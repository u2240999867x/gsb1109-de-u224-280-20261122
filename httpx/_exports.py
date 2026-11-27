"""
Tracking for long running exports.

The export platform drives an asynchronous job through a small protocol:

* `202 Accepted` responses report that the job is still running. They carry
  the export identifier/revision, may carry a new relative progress address
  in `Location`, and may carry a `Retry-After` delay (in seconds).
* `303 See Other` responses point at a candidate artifact for the reported
  revision. A candidate can be withdrawn at any time by a later, higher
  revision, even while it is being downloaded.
* `200 OK` responses deliver an artifact, whose response headers must carry
  the same identifier/revision as the `303` that pointed at it.

Several progress requests for the same export may be in flight at once. They
share one view of the current revision per export identifier, each revision's
candidate is downloaded at most once, and only an artifact whose stream has
been fully read and whose revision has not been superseded is ever confirmed.
The confirmed file is moved into the archive directory atomically, so partial
downloads never appear there under their final name. Confirmation is cached:
late progress responses and failures of superseded pickup streams can neither
restart polling nor overwrite the archived result.
"""

from __future__ import annotations

import os
import tempfile
import time
import typing
from pathlib import Path

import anyio

from ._exceptions import ProtocolError, RequestError
from ._models import Request, Response
from ._urls import URL

if typing.TYPE_CHECKING:
    from ._client import AsyncClient

__all__ = [
    "AsyncExportManager",
    "ExportConfirmationError",
    "ExportError",
    "ExportProtocolError",
    "ExportResult",
]

# Header names are compared case-insensitively by `Headers`.
EXPORT_ID_HEADER = "X-Export-Id"
EXPORT_REVISION_HEADER = "X-Export-Revision"

_ACCEPTED = 202
_SEE_OTHER = 303
_OK = 200
_PROGRESS_STATUSES = frozenset({_ACCEPTED, _SEE_OTHER})

Sleep = typing.Callable[[float], typing.Awaitable[typing.Any]]
Clock = typing.Callable[[], float]


class ExportError(RequestError):
    """Base class for errors raised while tracking an export."""


class ExportProtocolError(ExportError, ProtocolError):
    """The export platform returned a response that violates the protocol."""


class ExportConfirmationError(ExportError):
    """A candidate artifact could not be confirmed for the requested export."""


class _Withdrawn(ExportError):
    """Internal control flow: a candidate revision was superseded."""


class ExportResult:
    """
    The confirmed outcome of an export.

    `response` is the fully read artifact response (headers and request remain
    available after the stream has closed), `revision` is the confirmed
    revision, and `path` is the archived file on disk.
    """

    def __init__(
        self, *, export_id: str, revision: int, response: Response, path: Path
    ) -> None:
        self.export_id = export_id
        self.revision = revision
        self.response = response
        self.path = path

    def __repr__(self) -> str:
        return (
            f"<ExportResult export_id={self.export_id!r} revision={self.revision} "
            f"path={str(self.path)!r}>"
        )


class _Export:
    """Shared state for one export identifier, guarded by the manager lock."""

    def __init__(self, export_id: str, progress_url: URL) -> None:
        self.export_id = export_id
        self.progress_url: URL = progress_url
        self.revision: int | None = None
        # Revisions whose pickup has been started. A repeated 303 for one of
        # these does not trigger a second download; the duplicate poller waits
        # on the matching event instead of hammering the progress endpoint.
        self.downloads: set[int] = set()
        self.download_events: dict[int, anyio.Event] = {}
        self.confirmed = False
        self.confirmed_revision: int | None = None
        self.confirmed_response: Response | None = None
        self.confirmed_path: Path | None = None


class _Poll:
    """The next progress request to send, optionally after a monotonic wait."""

    def __init__(self, request: Request, wake_at: float | None) -> None:
        self.request = request
        self.wake_at = wake_at


class _Await:
    """Wait for an in-flight download of another revision to finish."""

    def __init__(self, event: anyio.Event, request: Request) -> None:
        self.event = event
        self.request = request


class _Download:
    """A candidate artifact that should be downloaded for `revision`."""

    def __init__(self, revision: int, url: URL) -> None:
        self.revision = revision
        self.url = url


class AsyncExportManager:
    """
    Coordinates overlapping progress requests for export jobs on top of an
    `AsyncClient`.

    Several coroutines may call `archive()` concurrently for the same export
    identifier: they share one view of the current revision, any one revision
    is only downloaded once, and they all converge on the same cached
    confirmation. Different identifiers never interfere with each other.

    **Parameters:**

    * **client** - An `httpx.AsyncClient` used for all requests. Its ordinary
      redirect behaviour is left untouched; the export protocol's 202/303
      responses are handled here.
    * **id_header** / **revision_header** - Header names carrying the export
      identifier and revision.
    * **sleep** - Async sleep function, overridable in tests.
    * **clock** - Monotonic clock used to schedule `Retry-After` waits,
      overridable in tests. Wall clock skew never affects scheduling.
    """

    def __init__(
        self,
        client: AsyncClient,
        *,
        id_header: str = EXPORT_ID_HEADER,
        revision_header: str = EXPORT_REVISION_HEADER,
        sleep: Sleep | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._client: AsyncClient = client
        self._id_header = id_header
        self._revision_header = revision_header
        self._sleep: Sleep = anyio.sleep if sleep is None else sleep
        self._clock: Clock = time.perf_counter if clock is None else clock
        self._exports: dict[str, _Export] = {}
        self._lock = anyio.Lock()

    async def archive(
        self,
        export_id: str,
        progress_url: URL | str,
        destination: Path | str,
    ) -> ExportResult:
        """
        Drive the export identified by `export_id` until a complete artifact
        has been archived at `destination`, and return an `ExportResult`.

        Progress and candidate requests may overlap with requests from other
        coroutines tracking the same identifier. The destination file appears
        only once an artifact has been fully read and confirmed.

        A confirmed export is cached: further calls return the cached result
        without issuing requests, and stale responses observed by overlapping
        pollers are discarded.
        """
        destination = Path(destination)
        async with self._lock:
            export = self._exports.get(export_id)
            if export is None:
                export = _Export(export_id, self._as_url(progress_url))
                self._exports[export_id] = export
            elif export.confirmed:
                return self._confirmed_result(export)
            request = self._build_request(export, export.progress_url)

        return await self._poll(export, request, destination)

    def _build_request(self, export: _Export, url: URL) -> Request:
        # Stamp every request with the export identifier so the platform can
        # distinguish jobs that share a progress address.
        return self._client.build_request(
            "GET", url, headers={self._id_header: export.export_id}
        )

    async def _poll(
        self, export: _Export, request: Request, destination: Path
    ) -> ExportResult:
        while True:
            async with self._lock:
                if export.confirmed:
                    return self._confirmed_result(export)

            # `stream=True` keeps the response body under this manager's
            # control, and `follow_redirects=False` ensures the export
            # protocol's 202/303 responses are handled here rather than by the
            # client's ordinary redirect behaviour, which stays available to
            # regular requests.
            response = await self._client.send(
                request, stream=True, follow_redirects=False
            )
            try:
                action = await self._handle_progress(export, request, response)
            finally:
                await response.aclose()

            if isinstance(action, _Download):
                outcome = await self._download_candidate(
                    export, action.revision, action.url, destination
                )
                if isinstance(outcome, ExportResult):
                    return outcome
                # The candidate was withdrawn or its stream failed: go back to
                # polling the current progress address.
                action = _Poll(self._build_request(export, export.progress_url), None)
            elif isinstance(action, _Await):
                # Park until the in-flight download finishes instead of
                # issuing duplicate requests; the next loop iteration picks up
                # the resulting confirmation or resumes polling.
                await action.event.wait()
                request = action.request
                continue

            if action.wake_at is not None:
                # Scheduled against the monotonic clock, so an incorrect wall
                # clock on this machine cannot shift the polling interval.
                remaining = action.wake_at - self._clock()
                if remaining > 0:
                    await self._sleep(remaining)
            request = action.request

    async def _handle_progress(
        self, export: _Export, request: Request, response: Response
    ) -> _Poll | _Await | _Download:
        status = response.status_code
        if status not in _PROGRESS_STATUSES:
            raise ExportProtocolError(
                f"Unexpected export response status {status} for export "
                f"{export.export_id!r}.",
                request=request,
            )

        reported_id, revision = self._read_tracking_headers(export, response)
        self._check_identifier(export, reported_id, response)
        delay = self._retry_after(response)

        async with self._lock:
            if export.confirmed:
                return _Poll(self._build_request(export, export.progress_url), None)

            stale = export.revision is not None and revision < export.revision
            if not stale:
                export.revision = revision

            # Only a response that still represents the current revision is
            # allowed to schedule the wait. A stale or superseded response
            # must not reintroduce an obsolete `Retry-After`.
            wake_at = None if stale else self._wake_at(delay)

            if status == _ACCEPTED:
                location = response.headers.get("Location")
                if location is not None and not stale:
                    # Relative addresses resolve against the request that
                    # produced this response, not the original progress URL.
                    export.progress_url = self._resolve_location(request.url, location)
                return _Poll(
                    self._build_request(export, export.progress_url),
                    wake_at,
                )

            # 303 See Other: offer a candidate artifact for this revision.
            location = response.headers.get("Location")
            if location is None:
                raise ExportProtocolError(
                    f"303 response for export {export.export_id!r} is missing a "
                    "'Location' header.",
                    request=response.request,
                )
            if stale:
                # A newer revision already superseded this offer: keep polling
                # the current progress address without downloading it.
                return _Poll(self._build_request(export, export.progress_url), None)
            if revision in export.downloads:
                # A repeated 303 for a revision whose pickup is already in
                # flight (or has already run): do not download it twice. Park
                # on that download's completion event instead of busy polling.
                event = export.download_events[revision]
                return _Await(event, self._build_request(export, export.progress_url))

            export.downloads.add(revision)
            export.download_events[revision] = anyio.Event()
            candidate_url = self._resolve_location(response.request.url, location)
            return _Download(revision, candidate_url)

    async def _download_candidate(
        self,
        export: _Export,
        revision: int,
        candidate_url: URL,
        destination: Path,
    ) -> ExportResult | None:
        """
        Download one candidate.

        Returns the confirmed result, or `None` when the candidate was
        withdrawn or its stream failed and polling should continue. Fatal
        protocol violations (for example an identifier mismatch) are raised to
        the caller.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        # A hidden temporary name in the archive directory keeps the final
        # filename invisible until the artifact has been fully confirmed.
        tmp_fd, tmp_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)

        candidate_request = self._build_request(export, candidate_url)
        candidate: Response | None = None
        succeeded = False
        try:
            try:
                candidate = await self._client.send(
                    candidate_request, stream=True, follow_redirects=False
                )
            except RequestError:
                # A transient transport error opening the pickup is retried
                # from the progress endpoint rather than failing the export.
                return None

            if candidate.status_code != _OK:
                raise ExportProtocolError(
                    f"Pickup {revision} of export {export.export_id!r} returned "
                    f"unexpected status {candidate.status_code} from "
                    f"{candidate_url}.",
                    request=candidate_request,
                )

            candidate_id, candidate_revision = self._read_tracking_headers(
                export, candidate
            )
            if candidate_id != export.export_id:
                raise ExportConfirmationError(
                    f"Pickup {revision} for export {export.export_id!r} at "
                    f"{candidate_url} returned the wrong export identifier "
                    f"{candidate_id!r} (revision {candidate_revision}).",
                    request=candidate_request,
                )
            if candidate_revision != revision:
                raise ExportConfirmationError(
                    f"Pickup {revision} of export {export.export_id!r} returned "
                    f"revision {candidate_revision} from {candidate_url}.",
                    request=candidate_request,
                )

            # Stream straight to disk: reports may be larger than memory.
            # `aiter_bytes` streams chunk by chunk for a live stream and also
            # works for responses whose small body was buffered upfront.
            try:
                async with await anyio.open_file(tmp_path, "wb") as handle:
                    async for chunk in candidate.aiter_bytes():
                        await handle.write(chunk)
                        await self._ensure_current(export, revision, candidate_request)
            except _Withdrawn:
                raise
            except RequestError as exc:
                # The pickup stream broke (for example the platform withdrew
                # the candidate by closing the connection). Retry via polling.
                raise _Withdrawn(str(exc), request=candidate_request) from exc

            async with self._lock:
                if export.confirmed:
                    return self._confirmed_result(export)
                if export.revision is not None and revision < export.revision:
                    raise _Withdrawn(
                        f"Pickup {revision} of export {export.export_id!r} was "
                        f"withdrawn by revision {export.revision}.",
                        request=candidate_request,
                    )

                await anyio.to_thread.run_sync(os.replace, tmp_path, destination)

                export.confirmed = True
                export.confirmed_revision = revision
                export.confirmed_response = candidate
                export.confirmed_path = destination
                succeeded = True
                return ExportResult(
                    export_id=export.export_id,
                    revision=revision,
                    response=candidate,
                    path=destination,
                )
        except _Withdrawn:
            # Superseded candidates are quietly replaced by further polling.
            return None
        finally:
            if candidate is not None and not succeeded:
                await candidate.aclose()
            async with self._lock:
                export.downloads.discard(revision)
                event = export.download_events.pop(revision, None)
            if event is not None:
                # Wake any duplicate poller parked on this download.
                event.set()
            with anyio.CancelScope(shield=True):
                # A no-op after the successful atomic rename.
                await anyio.to_thread.run_sync(self._silently_remove, tmp_path)

    async def _ensure_current(
        self, export: _Export, revision: int, request: Request
    ) -> None:
        async with self._lock:
            if export.confirmed:
                raise _Withdrawn(
                    f"Pickup {revision} of export {export.export_id!r} finished "
                    "after a newer revision was confirmed.",
                    request=request,
                )
            if export.revision is not None and revision < export.revision:
                raise _Withdrawn(
                    f"Pickup {revision} of export {export.export_id!r} was "
                    f"withdrawn by revision {export.revision} before it "
                    "completed.",
                    request=request,
                )

    def _read_tracking_headers(
        self, export: _Export, response: Response
    ) -> tuple[str, int]:
        try:
            reported_id = response.headers[self._id_header]
        except KeyError:
            raise ExportProtocolError(
                f"Response for export {export.export_id!r} is missing the "
                f"{self._id_header!r} header.",
                request=response.request,
            ) from None
        try:
            raw_revision = response.headers[self._revision_header]
        except KeyError:
            raise ExportProtocolError(
                f"Response for export {export.export_id!r} is missing the "
                f"{self._revision_header!r} header.",
                request=response.request,
            ) from None
        try:
            revision = int(raw_revision)
        except ValueError:
            raise ExportProtocolError(
                f"Export revision {raw_revision!r} for export "
                f"{export.export_id!r} is not an integer.",
                request=response.request,
            ) from None
        return reported_id, revision

    def _check_identifier(
        self, export: _Export, reported_id: str, response: Response
    ) -> None:
        if reported_id != export.export_id:
            raise ExportConfirmationError(
                f"Response reports export {reported_id!r} but export "
                f"{export.export_id!r} was requested "
                f"({str(response.request.url)!r}).",
                request=response.request,
            )

    def _retry_after(self, response: Response) -> float:
        raw = response.headers.get("Retry-After")
        if raw is None:
            return 0.0
        try:
            return max(0.0, float(raw))
        except ValueError:
            # An HTTP-date form cannot be scheduled against the monotonic
            # clock; fall back to polling immediately.
            return 0.0

    def _wake_at(self, delay: float) -> float | None:
        # The deadline is measured on the monotonic clock, so a skewed wall
        # clock cannot affect when the next poll is sent.
        return self._clock() + delay if delay > 0 else None

    def _resolve_location(self, base_url: URL, location: str) -> URL:
        url = self._as_url(location)
        if url.is_relative_url:
            url = base_url.join(url)
        return url

    def _confirmed_result(self, export: _Export) -> ExportResult:
        assert (
            export.confirmed
            and export.confirmed_revision is not None
            and export.confirmed_response is not None
            and export.confirmed_path is not None
        )
        return ExportResult(
            export_id=export.export_id,
            revision=export.confirmed_revision,
            response=export.confirmed_response,
            path=export.confirmed_path,
        )

    @staticmethod
    def _as_url(url: URL | str) -> URL:
        return url if isinstance(url, URL) else URL(url)

    @staticmethod
    def _silently_remove(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
