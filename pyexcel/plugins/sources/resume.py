"""
pyexcel.plugins.sources.resume
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Resumable http downloads shared by :func:`pyexcel.get_sheet` and
:func:`pyexcel.get_book`.

A :class:`DownloadProgress` instance is the unit of one collection.
The caller decides which calls share the same collection by passing the
same instance. Each call performs at most one request against the actual
file address: routing endpoints are followed with plain GET requests and
the ``Range`` / ``If-Range`` headers are only sent once the known file
address is reached.

Every outcome that does not deliver a complete file is reported through
:class:`pyexcel.exceptions.DownloadError` and leaves a :class:`DownloadReport`
on both the raised exception and the progress object. A download that is
superseded by a concurrent or later download raises
:class:`pyexcel.exceptions.StaleDownload` without touching the current
progress.

:copyright: (c) 2014-2026 by C Wang
:license: New BSD License
"""

import threading
from urllib.parse import urljoin

from pyexcel._compact import request
from pyexcel.exceptions import (
    DownloadInterrupted,
    DownloadResponseMismatch,
    IncompleteDownload,
    StaleDownload,
)

REDIRECT_STATUSES = (301, 302, 303, 307, 308)
READ_CHUNK_SIZE = 8192

STATE_INTERRUPTED = "interrupted"
STATE_MISMATCH = "mismatch"
STATE_INCOMPLETE = "incomplete"
STATE_COMPLETE = "complete"
STATE_STALE = "stale"


class DownloadReport:
    """Progress information recorded after one download attempt."""

    def __init__(
        self,
        state,
        url=None,
        etag=None,
        total_length=None,
        resume_from=0,
        generation=0,
        message="",
    ):
        self.state = state
        self.url = url
        self.etag = etag
        self.total_length = total_length
        self.resume_from = resume_from
        self.generation = generation
        self.message = message

    def __repr__(self):
        return (
            "DownloadReport(state=%r, url=%r, etag=%r, total_length=%r, "
            "resume_from=%r, generation=%r, message=%r)"
            % (
                self.state,
                self.url,
                self.etag,
                self.total_length,
                self.resume_from,
                self.generation,
                self.message,
            )
        )


class _ProgressState:
    """Immutable snapshot of the progress for one download attempt."""

    def __init__(self, generation, url, etag, total_length, resume_from):
        self.generation = generation
        self.url = url
        self.etag = etag
        self.total_length = total_length
        self.resume_from = resume_from


class DownloadProgress:
    """Thread-safe progress of one resumable collection.

    Share one instance between the :func:`pyexcel.get_sheet` and
    :func:`pyexcel.get_book` calls that belong to the same collection.
    ``resume_from`` is the byte at which the next request has to continue
    while ``etag`` and ``total_length`` identify the file version the
    progress belongs to.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._buffer = bytearray()
        self._url = None
        self._etag = None
        self._total_length = None
        self._resume_from = 0
        self._generation = 0
        self._completed = False
        self._report = None
        self._reports = []

    @property
    def lock(self):
        """The reentrant lock guarding every state transition."""
        return self._lock

    @property
    def url(self):
        """The resolved actual file address, once known."""
        return self._url

    @property
    def etag(self):
        """Strong ETag of the file version currently held."""
        return self._etag

    @property
    def total_length(self):
        """Total length in bytes of the file version currently held."""
        return self._total_length

    @property
    def resume_from(self):
        """Next byte to request when continuing the collection."""
        return self._resume_from

    @property
    def generation(self):
        """Monotonic version of the progress state."""
        return self._generation

    @property
    def completed(self):
        """Whether the whole file has been received."""
        return self._completed

    @property
    def report(self):
        """The latest :class:`DownloadReport`."""
        return self._report

    @property
    def reports(self):
        """All reports produced for this collection, oldest first."""
        return list(self._reports)

    def snapshot(self):
        """Return an immutable snapshot for an upcoming attempt."""
        with self._lock:
            return _ProgressState(
                self._generation,
                self._url,
                self._etag,
                self._total_length,
                self._resume_from,
            )

    def _record(self, report):
        self._report = report
        self._reports.append(report)

    def _reset(self, url, etag, total_length, content):
        self._url = url
        self._etag = etag
        self._total_length = total_length
        self._buffer = bytearray(content)
        self._resume_from = len(self._buffer)
        self._completed = False
        self._generation += 1

    def _append(self, content):
        self._buffer.extend(content)
        self._resume_from = len(self._buffer)
        self._generation += 1

    def _content(self):
        return bytes(self._buffer)


def download_resumable(entry_url, progress):
    """Run one download attempt inside a collection.

    :param entry_url: the routing url the call always starts from, for
        example ``/dispatch``
    :param progress: the shared :class:`DownloadProgress`
    :returns: ``(content, info, file_url, report)`` where ``content`` is
        the complete file, ``info`` the response headers object,
        ``file_url`` the resolved actual file address and ``report`` the
        completion :class:`DownloadReport`
    :raises pyexcel.exceptions.DownloadError: when no complete file can be
        delivered by this attempt
    """
    snapshot = progress.snapshot()
    try:
        connection, file_url = _open_connection(entry_url, snapshot)
    except OSError as error:
        _raise_network_error(progress, snapshot, error)
    try:
        status = _response_status(connection)
        info = connection.info()
        if status == 206:
            return _handle_partial(
                connection, info, file_url, progress, snapshot
            )
        return _handle_full(
            connection, info, file_url, progress, snapshot
        )
    finally:
        connection.close()


def _raise_network_error(progress, snapshot, error):
    with progress.lock:
        report = DownloadReport(
            STATE_INTERRUPTED,
            url=snapshot.url,
            etag=snapshot.etag,
            total_length=snapshot.total_length,
            resume_from=progress.resume_from,
            generation=progress.generation,
            message=str(error),
        )
        progress._record(report)
    raise DownloadInterrupted(report) from error


def _open_connection(entry_url, snapshot):
    current_url = entry_url
    while True:
        headers = _build_headers(current_url, snapshot)
        if headers:
            connection = request.urlopen(
                request.Request(current_url, headers=headers),
            )
        else:
            connection = request.urlopen(current_url)
        status = _response_status(connection)
        location = _header(connection.info(), "Location")
        if status in REDIRECT_STATUSES and location:
            connection.close()
            current_url = urljoin(current_url, location)
            continue
        return connection, current_url


def _build_headers(current_url, snapshot):
    if not snapshot.resume_from:
        return {}
    if snapshot.url is None or current_url != snapshot.url:
        return {}
    headers = {"Range": "bytes=%s-" % snapshot.resume_from}
    if snapshot.etag:
        headers["If-Range"] = snapshot.etag
    return headers


def _handle_full(connection, info, file_url, progress, snapshot):
    etag = _normalize_etag(_header(info, "ETag"))
    total_length = _parse_int(_header(info, "Content-Length"))
    content = _read_content(connection, total_length)
    if total_length is not None and len(content) < total_length:
        report = _commit_interrupted_full(
            file_url, etag, total_length, content, progress
        )
        raise DownloadInterrupted(report)
    with progress.lock:
        progress._reset(file_url, etag, total_length, content)
        progress._completed = True
        report = DownloadReport(
            STATE_COMPLETE,
            url=file_url,
            etag=etag,
            total_length=total_length,
            resume_from=len(content),
            generation=progress.generation,
        )
        progress._record(report)
    return content, info, file_url, report


def _commit_interrupted_full(
    file_url, etag, total_length, content, progress
):
    with progress.lock:
        progress._reset(file_url, etag, total_length, content)
        report = DownloadReport(
            STATE_INTERRUPTED,
            url=file_url,
            etag=etag,
            total_length=total_length,
            resume_from=len(content),
            generation=progress.generation,
            message=(
                "connection ended after %s of %s bytes"
                % (len(content), total_length)
            ),
        )
        progress._record(report)
    return report


def _handle_partial(connection, info, file_url, progress, snapshot):
    if not snapshot.resume_from:
        return _mismatch(
            file_url,
            progress,
            snapshot,
            "206 response received without any resume offset",
        )
    etag = _normalize_etag(_header(info, "ETag"))
    if snapshot.etag is not None and etag != snapshot.etag:
        return _mismatch(
            file_url,
            progress,
            snapshot,
            "ETag %r does not match progress ETag %r"
            % (etag, snapshot.etag),
        )
    start, end, range_total = _parse_content_range(
        _header(info, "Content-Range"),
        file_url,
        progress,
        snapshot,
    )
    if start != snapshot.resume_from:
        return _mismatch(
            file_url,
            progress,
            snapshot,
            "Content-Range start %s does not match resume offset %s"
            % (start, snapshot.resume_from),
        )
    if snapshot.total_length is not None and range_total != (
        snapshot.total_length
    ):
        return _mismatch(
            file_url,
            progress,
            snapshot,
            "Content-Range total %s does not match file length %s"
            % (range_total, snapshot.total_length),
        )
    segment_length = end - start + 1
    content_length = _parse_int(_header(info, "Content-Length"))
    if content_length is not None and content_length != segment_length:
        return _mismatch(
            file_url,
            progress,
            snapshot,
            "Content-Length %s does not match Content-Range segment "
            "length %s" % (content_length, segment_length),
        )
    expected_total = (
        snapshot.total_length
        if snapshot.total_length is not None
        else range_total
    )
    content = _read_content(connection, segment_length)
    if len(content) != segment_length:
        with progress.lock:
            report = DownloadReport(
                STATE_INTERRUPTED,
                url=file_url,
                etag=snapshot.etag,
                total_length=expected_total,
                resume_from=snapshot.resume_from,
                generation=snapshot.generation,
                message=(
                    "connection ended after %s of %s expected bytes"
                    % (len(content), segment_length)
                ),
            )
            progress._record(report)
        raise DownloadInterrupted(report)
    return _commit_partial(
        content, end, expected_total, file_url, etag, progress, snapshot
    )


def _commit_partial(
    content,
    end,
    expected_total,
    file_url,
    etag,
    progress,
    snapshot,
):
    with progress.lock:
        current = progress.snapshot()
        if not _is_current_branch(current, snapshot):
            report = DownloadReport(
                STATE_STALE,
                url=file_url,
                etag=etag,
                total_length=expected_total,
                resume_from=current.resume_from,
                generation=current.generation,
                message=(
                    "download generation %s was superseded by %s"
                    % (snapshot.generation, current.generation)
                ),
            )
            progress._record(report)
            raise StaleDownload(report)
        progress._append(content)
        if progress._etag is None and etag is not None:
            progress._etag = etag
        if progress._total_length is None:
            progress._total_length = expected_total
        completed = progress.resume_from == expected_total
        if completed:
            progress._completed = True
            state = STATE_COMPLETE
            message = ""
        else:
            state = STATE_INCOMPLETE
            message = "resume from byte %s" % progress.resume_from
        report = DownloadReport(
            state,
            url=file_url,
            etag=etag,
            total_length=expected_total,
            resume_from=progress.resume_from,
            generation=progress.generation,
            message=message,
        )
        progress._record(report)
        full_content = progress._content() if completed else None
    if not completed:
        raise IncompleteDownload(report)
    return full_content, None, file_url, report


def _is_current_branch(current, snapshot):
    return (
        current.generation == snapshot.generation
        and current.url == snapshot.url
        and current.etag == snapshot.etag
        and current.total_length == snapshot.total_length
        and current.resume_from == snapshot.resume_from
    )


def _parse_content_range(value, file_url, progress, snapshot):
    if not value:
        _mismatch(
            file_url,
            progress,
            snapshot,
            "206 response is missing a Content-Range header",
        )
    text = value.strip()
    if not text.startswith("bytes "):
        _mismatch(
            file_url,
            progress,
            snapshot,
            "unsupported Content-Range unit in %r" % value,
        )
    body = text[len("bytes "):]
    try:
        range_part, total_part = body.split("/", 1)
        start_text, end_text = range_part.split("-", 1)
        start = int(start_text)
        end = int(end_text)
        range_total = int(total_part)
    except (ValueError, TypeError):
        _mismatch(
            file_url,
            progress,
            snapshot,
            "malformed Content-Range header %r" % value,
        )
    if end < start:
        _mismatch(
            file_url,
            progress,
            snapshot,
            "inverted byte range in Content-Range header %r" % value,
        )
    return start, end, range_total


def _mismatch(file_url, progress, snapshot, message):
    with progress.lock:
        current = progress.snapshot()
        report = DownloadReport(
            STATE_MISMATCH,
            url=file_url,
            etag=current.etag,
            total_length=current.total_length,
            resume_from=current.resume_from,
            generation=current.generation,
            message=message,
        )
        progress._record(report)
    raise DownloadResponseMismatch(report)


def _read_content(connection, length):
    if length is None:
        chunks = []
        try:
            while True:
                chunk = connection.read(READ_CHUNK_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
        except OSError:
            return b"".join(chunks)
        return b"".join(chunks)
    chunks = []
    remaining = length
    try:
        while remaining > 0:
            chunk = connection.read(min(remaining, READ_CHUNK_SIZE))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    except OSError:
        return b"".join(chunks)
    return b"".join(chunks)


def _response_status(connection):
    status = getattr(connection, "status", None)
    if status is None:
        getcode = getattr(connection, "getcode", None)
        if getcode is not None:
            status = getcode()
    return status


def _header(info, name):
    if info is None:
        return None
    value = info.get(name)
    if value is None:
        value = info.get(name.lower())
    return value


def _parse_int(value):
    if value is None:
        return None
    return int(value)


def _normalize_etag(etag):
    if etag is None:
        return None
    etag = etag.strip()
    if etag.startswith("W/"):
        return etag[2:].strip()
    return etag
