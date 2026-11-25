"""Resumable, publish-atomic downloads used by ``S3FileSystem.get_file``.

The optional transfer mode is enabled by passing a ``transfer_id`` to
``get_file``. A sidecar directory next to the destination (``<lpath>.s3dl``)
kept on the local filesystem carries the transfer state, so a download can be
resumed from a different process using the same task number.

Ownership model
---------------
Every accepted transfer records its ``transfer_id`` together with a random
``run_token`` in ``state.json``. A new task that is explicitly asked to take
over the same destination rotates the token. Before any state mutation and
before publishing, the running transfer re-reads the state and fences itself:
if the recorded owner no longer matches, it raises :class:`TransferSuperseded`
and never touches the destination. Fencing therefore stays effective even when
the previous task is still alive in another process.

Downloads are stored as one file per fully received, byte-accurate interval,
which lets a resume reuse completed bytes with a different chunk size: only the
uncovered byte ranges are requested again. The destination itself is replaced
last, with an atomic ``os.replace``, after a durable ``committed`` record has
been written, so readers either see the previous complete file or the new
complete file, never a partial one.
"""

import asyncio
import hashlib
import json
import os
import tempfile
import uuid

from s3fs.utils import _get_brange, ignoring

STATE_FILENAME = "state.json"
SIDECAR_SUFFIX = ".s3dl"
PART_PREFIX = "part-"
TMP_SUFFIX = ".tmp"

STATUS_RUNNING = "running"
STATUS_COMMITTED = "committed"
STATUS_PUBLISHED = "published"

_STATE_VERSION = 1


class ResumableTransferError(RuntimeError):
    """Base class for failures of a transfer-managed ``get_file`` call."""


class TransferSuperseded(ResumableTransferError):
    """Raised when another task has taken over the destination.

    The transfer identified by the caller's ``transfer_id`` is no longer the
    accepted owner, so it must not publish anything.
    """


class TransferUnrecoverable(ResumableTransferError):
    """Raised when a partial download can no longer be completed.

    The object version originally captured by the transfer is gone or changed
    beyond what the stored partial bytes can be combined with. The local
    destination is left untouched.
    """


def _sidecar_dir(lpath):
    return lpath + SIDECAR_SUFFIX


def _state_path(lpath):
    return os.path.join(_sidecar_dir(lpath), STATE_FILENAME)


def _fsync_directory(path):
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _load_state(lpath):
    try:
        with open(_state_path(lpath), "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None


def _write_state(lpath, state):
    """Persist the state atomically so crashes never leave a torn document."""
    sidecar = _sidecar_dir(lpath)
    os.makedirs(sidecar, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=STATE_FILENAME + ".", suffix=TMP_SUFFIX, dir=sidecar
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, _state_path(lpath))
        _fsync_directory(sidecar)
    except BaseException:
        with ignoring(OSError):
            os.unlink(tmp_path)
        raise


def _part_filename(start, end):
    return f"{PART_PREFIX}{start:010d}-{end:010d}"


def _parse_part_interval(filename):
    if not filename.startswith(PART_PREFIX):
        return None
    start_text, sep, end_text = filename[len(PART_PREFIX) :].partition("-")
    if not sep:
        return None
    try:
        return int(start_text), int(end_text)
    except ValueError:
        return None


def _completed_intervals(lpath, run_token):
    part_dir = os.path.join(_sidecar_dir(lpath), run_token)
    try:
        names = os.listdir(part_dir)
    except FileNotFoundError:
        return []
    intervals = [
        interval
        for name in names
        if (interval := _parse_part_interval(name)) is not None
    ]
    return sorted(intervals)


def _missing_intervals(length, completed):
    """Return the byte ranges of ``[0, length)`` not already covered."""
    missing = []
    cursor = 0
    for start, end in completed:
        if start > cursor:
            missing.append((cursor, start - 1))
        cursor = max(cursor, end + 1)
    if cursor < length:
        missing.append((cursor, length - 1))
    return missing


def _cleanup_staging(sidecar):
    for root, _dirs, files in os.walk(sidecar):
        for name in files:
            if name.endswith(TMP_SUFFIX):
                with ignoring(OSError):
                    os.unlink(os.path.join(root, name))


class ResumableDownload:
    """Run one transfer-managed download of ``rpath`` to ``lpath``."""

    def __init__(
        self,
        fs,
        rpath,
        lpath,
        transfer_id,
        *,
        chunksize,
        max_concurrency,
        version_id,
        callback,
        extra_kwargs,
        takeover=False,
    ):
        self.fs = fs
        self.rpath = rpath
        self.lpath = os.path.abspath(lpath)
        self.transfer_id = str(transfer_id)
        self.requested_chunksize = chunksize or fs.default_block_size
        self.max_concurrency = max_concurrency or fs.max_concurrency
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.requested_version_id = version_id
        self.callback = callback
        self.extra_kwargs = extra_kwargs
        self.takeover = takeover

        self.bucket, self.key, _ = fs.split_path(rpath)
        self.run_token = None
        self.state = None

    async def run(self):
        os.makedirs(os.path.dirname(self.lpath) or ".", exist_ok=True)
        sidecar = _sidecar_dir(self.lpath)
        os.makedirs(sidecar, exist_ok=True)
        _cleanup_staging(sidecar)

        existing = _load_state(self.lpath)
        if existing is not None:
            same_owner = existing.get("transfer_id") == self.transfer_id
            if not same_owner and not self.takeover:
                self._raise_rejected(existing)
            if (
                existing.get("status") == STATUS_PUBLISHED
                and same_owner
                and self._resume_published(existing)
            ):
                return

            if same_owner:
                # Re-validate the exact version this transfer pinned, even if a
                # newer object has since been published.
                head_version = existing.get("version_id") or self.requested_version_id
                head = await self._head_object(head_version)
                self.state = await self._acquire(existing, head)
            else:
                head = await self._head_object(self.requested_version_id)
                self.state = self._take_over(existing, head)
                _write_state(self.lpath, self.state)
        else:
            head = await self._head_object(self.requested_version_id)
            self.state = self._fresh_state(head)
            _write_state(self.lpath, self.state)

        if self.state["status"] == STATUS_PUBLISHED:
            if self._local_matches_published(self.state):
                return
            # The published file was lost or changed locally: rebuild it from
            # the retained parts without fetching anything again.
            self.state["status"] = STATUS_RUNNING
            self.state["candidate"] = None
            _write_state(self.lpath, self.state)

        await self._resume_committed_if_ready()
        if self.state["status"] == STATUS_PUBLISHED:
            return

        await self._download()
        await self._publish()

    def _raise_rejected(self, existing):
        raise TransferSuperseded(
            f"Destination {self.lpath!r} is owned by transfer "
            f"{existing.get('transfer_id')!r}; transfer {self.transfer_id!r} "
            "was rejected (pass takeover=True to take over the destination)"
        )

    async def _head_object(self, version_id):
        try:
            response = await self.fs._call_s3(
                "head_object",
                Bucket=self.bucket,
                Key=self.key,
                **self.extra_kwargs,
                **_version_kw(version_id),
            )
        except FileNotFoundError as exc:
            raise TransferUnrecoverable(
                f"Object version {version_id!r} for transfer "
                f"{self.transfer_id!r} no longer exists; the partial download "
                "cannot be resumed"
            ) from exc
        return {
            "version_id": response.get("VersionId"),
            "etag": response.get("ETag"),
            "size": response["ContentLength"],
        }

    def _fresh_state(self, head):
        self.run_token = uuid.uuid4().hex
        return {
            "state_version": _STATE_VERSION,
            "transfer_id": self.transfer_id,
            "run_token": self.run_token,
            "bucket": self.bucket,
            "key": self.key,
            "version_id": head["version_id"],
            "etag": head["etag"],
            "size": head["size"],
            "checksum": None,
            "chunksize": self.requested_chunksize,
            "status": STATUS_RUNNING,
            "completed": [],
            "candidate": None,
        }

    async def _acquire(self, existing, head):
        """Re-open a transfer owned by this same ``transfer_id``."""
        if (
            existing.get("bucket") != self.bucket
            or existing.get("key") != self.key
        ):
            raise ResumableTransferError(
                f"Transfer {self.transfer_id!r} was started for "
                f"{existing.get('bucket')}/{existing.get('key')!r}, not "
                f"{self.rpath!r}; reuse its transfer_id on the same source only"
            )
        status = existing.get("status")
        if status == STATUS_PUBLISHED:
            self.run_token = existing["run_token"]
            return existing
        if status == STATUS_COMMITTED:
            self._assert_source_matches(existing, head)
            self.run_token = existing["run_token"]
            return existing
        self._assert_source_matches(existing, head)
        self.run_token = existing["run_token"]
        existing["chunksize"] = self.requested_chunksize
        return existing

    def _take_over(self, existing, head):
        self.run_token = uuid.uuid4().hex
        self._discard_token_parts(existing.get("run_token"))
        stale_candidate = existing.get("candidate")
        if stale_candidate:
            with ignoring(OSError):
                os.unlink(stale_candidate)
        return {
            "state_version": _STATE_VERSION,
            "transfer_id": self.transfer_id,
            "run_token": self.run_token,
            "bucket": self.bucket,
            "key": self.key,
            "version_id": head["version_id"],
            "etag": head["etag"],
            "size": head["size"],
            "checksum": None,
            "chunksize": self.requested_chunksize,
            "status": STATUS_RUNNING,
            "completed": [],
            "candidate": None,
        }

    def _assert_source_matches(self, existing, head):
        if (
            head.get("etag") != existing.get("etag")
            or head.get("size") != existing.get("size")
        ):
            raise TransferUnrecoverable(
                f"Source object for transfer {self.transfer_id!r} changed since "
                f"the partial download (etag {existing.get('etag')!r} -> "
                f"{head.get('etag')!r}); the original content can no longer be "
                "resumed"
            )

    def _discard_token_parts(self, run_token):
        if not run_token:
            return
        part_dir = os.path.join(_sidecar_dir(self.lpath), run_token)
        try:
            names = os.listdir(part_dir)
        except FileNotFoundError:
            return
        for name in names:
            with ignoring(OSError):
                os.unlink(os.path.join(part_dir, name))
        with ignoring(OSError):
            os.rmdir(part_dir)

    def _resume_published(self, existing):
        if existing.get("transfer_id") != self.transfer_id:
            return False
        if self._local_matches_published(existing):
            self.run_token = existing["run_token"]
            self.state = existing
            return True
        return False

    def _local_matches_published(self, state):
        try:
            stat = os.stat(self.lpath)
        except OSError:
            return False
        checksum = state.get("checksum")
        if checksum is None:
            return stat.st_size == state["size"]
        digest = hashlib.sha256()
        with open(self.lpath, "rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest() == checksum

    async def _resume_committed_if_ready(self):
        if self.state.get("status") != STATUS_COMMITTED:
            return
        candidate = self.state.get("candidate")
        if candidate and os.path.exists(candidate):
            self._fence()
            self._replace_target(candidate)
            self._mark_published()
            return
        if self._local_matches_published(self.state):
            self._mark_published()
            return
        self.state["status"] = STATUS_RUNNING
        self.state["candidate"] = None
        _write_state(self.lpath, self.state)

    def _fence(self):
        current = _load_state(self.lpath)
        if (
            current is None
            or current.get("transfer_id") != self.transfer_id
            or current.get("run_token") != self.run_token
        ):
            owner = None if current is None else current.get("transfer_id")
            raise TransferSuperseded(
                f"Transfer {self.transfer_id!r} was superseded by "
                f"{owner!r} for {self.lpath!r}"
            )

    async def _download(self):
        completed = _completed_intervals(self.lpath, self.run_token)
        length = self.state["size"]
        planned = (
            list(_get_brange(length, self.requested_chunksize))
            if not completed
            else _missing_intervals(length, completed)
        )
        if not planned:
            return

        self.callback.set_size(length)
        batches = [
            planned[i : i + self.max_concurrency]
            for i in range(0, len(planned), self.max_concurrency)
        ]
        for batch in batches:
            self._fence()
            results = await asyncio.gather(
                *(self._fetch_interval(start, end) for start, end in batch)
            )
            completed = sorted(
                set(completed) | {(start, end) for start, end in results}
            )
            self.state["completed"] = completed
            self._fence()
            _write_state(self.lpath, self.state)

    async def _fetch_interval(self, start, end):
        kw = {**self.fs.req_kw, **self.extra_kwargs}
        kw["Range"] = f"bytes={start}-{end}"
        response = await self.fs._call_s3(
            "get_object",
            Bucket=self.bucket,
            Key=self.key,
            **_version_kw(self.state.get("version_id")),
            **kw,
        )
        body = response["Body"]
        try:
            data = await body.read()
        finally:
            close = getattr(body, "close", None)
            if close is not None:
                close()
        expected = end - start + 1
        if len(data) != expected:
            raise ResumableTransferError(
                f"Expected {expected} bytes for range {start}-{end}, got {len(data)}"
            )

        part_dir = os.path.join(_sidecar_dir(self.lpath), self.run_token)
        os.makedirs(part_dir, exist_ok=True)
        final_path = os.path.join(part_dir, _part_filename(start, end))
        fd, tmp_path = tempfile.mkstemp(dir=part_dir, suffix=TMP_SUFFIX)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, final_path)
            _fsync_directory(part_dir)
        except BaseException:
            with ignoring(OSError):
                os.unlink(tmp_path)
            raise
        self.callback.relative_update(len(data))
        return start, end

    async def _publish(self):
        self._fence()
        completed = _completed_intervals(self.lpath, self.run_token)
        if _missing_intervals(self.state["size"], completed):
            raise ResumableTransferError(
                f"Transfer {self.transfer_id!r} cannot publish incomplete data"
            )

        candidate = self._candidate_path()
        checksum = None
        if not os.path.exists(candidate):
            checksum = self._assemble_candidate(candidate, completed)
        else:
            checksum = self._hash_file(candidate)
        self.state["checksum"] = checksum
        self.state["status"] = STATUS_COMMITTED
        self.state["candidate"] = candidate
        _write_state(self.lpath, self.state)

        self._fence()
        self._replace_target(candidate)
        self._mark_published()

    def _candidate_path(self):
        directory = os.path.dirname(self.lpath)
        name = os.path.basename(self.lpath)
        return os.path.join(directory, f".{name}.{self.run_token}.tmp")

    def _assemble_candidate(self, candidate, completed):
        digest = hashlib.sha256()
        fd, tmp_path = tempfile.mkstemp(
            dir=os.path.dirname(candidate),
            prefix=os.path.basename(candidate) + ".",
            suffix=TMP_SUFFIX,
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                for start, end in completed:
                    part_path = os.path.join(
                        _sidecar_dir(self.lpath),
                        self.run_token,
                        _part_filename(start, end),
                    )
                    with open(part_path, "rb") as part:
                        chunk = part.read()
                        digest.update(chunk)
                        handle.seek(start)
                        handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, candidate)
            _fsync_directory(os.path.dirname(candidate))
        except BaseException:
            with ignoring(OSError):
                os.unlink(tmp_path)
            raise
        return digest.hexdigest()

    @staticmethod
    def _hash_file(path):
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def _replace_target(self, candidate):
        os.replace(candidate, self.lpath)
        _fsync_directory(os.path.dirname(self.lpath))

    def _mark_published(self):
        # Re-fence: a takeover accepted while we were replacing the target must
        # not be allowed to overwrite the new owner's state with ours.
        self._fence()
        self.state["status"] = STATUS_PUBLISHED
        self.state["candidate"] = None
        _write_state(self.lpath, self.state)


def _version_kw(version_id):
    return {"VersionId": version_id} if version_id else {}
