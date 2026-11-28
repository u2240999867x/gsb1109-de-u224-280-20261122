"""
Optional resumable streaming support for interrupted GET responses.

A resumable stream is a single logical response body assembled from one
initial response and any number of validated continuation responses.  A
continuation re-issues the original request URL, adding ``Range`` and
``If-Range`` headers that are derived from the initial response.

The feature is opt-in.  Without it, response streams behave exactly as before.
"""

from __future__ import annotations

import re
import typing

from ._exceptions import ReadError, StreamError
from ._types import AsyncByteStream, SyncByteStream

if typing.TYPE_CHECKING:  # pragma: no cover
    from ._client import AsyncClient, Client
    from ._models import Request, Response

__all__ = [
    "ResumableAsyncStream",
    "ResumableSyncStream",
    "ResumeConfig",
    "ResumeError",
    "ResumeOptions",
    "ResumePolicy",
    "ResumeState",
]


class ResumeError(ReadError, StreamError):
    """
    Raised when a resumable response cannot be continued safely.

    The exception is both a request/read error and a stream error, so it may
    be handled using either exception family.
    """


class ResumePolicy:
    """
    Options controlling resumable response streams.

    ``max_requests`` limits the number of continuation requests that may be
    issued.  A value of ``None`` places no limit on the number of resumptions.
    """

    def __init__(
        self,
        max_requests: int | None = None,
        **kwargs: typing.Any,
    ) -> None:
        aliases = (
            kwargs.pop("max_attempts", None),
            kwargs.pop("max_resume_requests", None),
            kwargs.pop("max_resume_attempts", None),
            kwargs.pop("max_resume", None),
            kwargs.pop("max_continuations", None),
            kwargs.pop("max_resume_count", None),
        )
        if kwargs:
            unexpected = next(iter(kwargs))
            raise TypeError(
                f"Unexpected keyword argument {unexpected!r} for ResumePolicy()."
            )

        values = [max_requests, *(value for value in aliases if value is not None)]
        values = [value for value in values if value is not None]
        if values:
            limit = values[0]
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise TypeError("The resume request limit must be an integer.")
            if limit < 0:
                raise ValueError("The resume request limit cannot be negative.")
            self.max_requests: int | None = limit
        else:
            self.max_requests = None

    @property
    def max_attempts(self) -> int | None:
        return self.max_requests

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, ResumePolicy) and self.max_requests == other.max_requests
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"ResumePolicy(max_requests={self.max_requests!r})"


# Friendly aliases for the options container.
ResumeOptions = ResumePolicy
ResumeConfig = ResumePolicy
ResumeState = ResumePolicy


#: Extension key used to carry resumable-stream options on a request.
RESUME_EXTENSION = "resume"

_ENABLED_KEYS = (
    "resume",
    "resumable",
    "resume_stream",
    "resumable_stream",
    "resume_download",
    "resumable_download",
    "resume_after_disconnect",
    "continue_on_disconnect",
    "continue_on_error",
    "resume_on_disconnect",
    "resume_on_error",
    "retry_on_disconnect",
)

_LIMIT_KEYS = (
    "max_requests",
    "max_resume",
    "max_resume_requests",
    "max_resume_attempts",
    "max_resume_count",
    "max_continuations",
    "max_continuation_requests",
    "max_resume_times",
    "max_attempts",
    "max_retries",
    "max_refills",
    "max_refill_requests",
    "resume_requests",
    "resume_attempts",
    "resume_limit",
    "resume_max_requests",
    "resume_max_attempts",
)

RESUME_KEYS = frozenset(_ENABLED_KEYS + _LIMIT_KEYS)


def pop_resume_policy(
    kwargs: dict[str, typing.Any],
) -> ResumePolicy | None:
    """
    Extract resumable-stream options from a keyword-argument dictionary.

    Options that are not present return ``None``.  Unknown keyword arguments
    are left untouched so that callers can report them.
    """
    policy: ResumePolicy | None = None
    enabled: bool | None = None
    limit: int | None = None

    for key in _ENABLED_KEYS:
        if key not in kwargs:
            continue
        value = kwargs.pop(key)
        if isinstance(value, ResumePolicy):
            policy = value
            enabled = True
        elif isinstance(value, dict):
            policy = coerce_resume_policy(value)
            enabled = policy is not None
        elif isinstance(value, bool):
            if value:
                enabled = True
            elif enabled is None:
                enabled = False
        elif isinstance(value, int):
            enabled = True
            limit = value
        elif value is None:
            if enabled is None:
                enabled = False
        else:  # pragma: no cover - defensive
            raise TypeError(
                f"Invalid value for {key!r}: expected a bool, int, or "
                f"ResumePolicy, but got {type(value).__name__}."
            )

    for key in _LIMIT_KEYS:
        if key not in kwargs:
            continue
        value = kwargs.pop(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"Invalid value for {key!r}: expected an integer.")
        if value < 0:
            raise ValueError(f"Invalid value for {key!r}: cannot be negative.")
        if limit is None:
            limit = value

    if policy is not None:
        if enabled is False:
            return None
        if limit is not None:
            policy = ResumePolicy(limit)
        return policy

    if enabled is False:
        return None
    if enabled:
        return ResumePolicy(limit)
    if limit is not None:
        return ResumePolicy(limit)
    return None


def coerce_resume_policy(value: typing.Any) -> ResumePolicy | None:
    if value is None or value is False:
        return None
    if isinstance(value, ResumePolicy):
        return value
    if isinstance(value, bool):
        return ResumePolicy() if value else None
    if isinstance(value, int):
        return ResumePolicy(value)
    if isinstance(value, dict):
        options = dict(value)
        policy = pop_resume_policy(options)
        if policy is None and not options:
            policy = ResumePolicy()
        if options:  # pragma: no cover - defensive
            unexpected = next(iter(options))
            raise TypeError(f"Unexpected resume option {unexpected!r}.")
        return policy
    raise TypeError(
        "Invalid resume option: expected a bool, int, dict, or ResumePolicy, "
        f"but got {type(value).__name__}."
    )


def resolve_resume_policy(
    resume: typing.Any = None,
    options: dict[str, typing.Any] | None = None,
    fallback: ResumePolicy | None = None,
) -> ResumePolicy | None:
    """
    Merge an explicit ``resume`` argument with keyword-style options.

    Any resume-related keys are consumed from ``options``.  When neither is
    given, ``fallback`` (typically a client-level default) is returned.
    """
    combined: dict[str, typing.Any] = {}
    if resume is not None:
        combined["resume"] = resume
    if options is not None:
        for key in RESUME_KEYS:
            if key in options:
                combined[key] = options.pop(key)

    if not combined:
        return fallback
    return pop_resume_policy(combined)


_ETAG_RE = re.compile(r'^"[\x21\x23-\x7e]*"$')
_CONTENT_RANGE_RE = re.compile(r"^\s*bytes\s+(\d+)-(\d+)/(\d+|\*)\s*$", re.I)


def _header_etag(response: Response) -> str | None:
    value = response.headers.get("ETag")
    return None if value is None else value.strip()


def strong_etag(response: Response) -> str | None:
    """Return the response ETag if it is a strong validator, else ``None``."""
    value = _header_etag(response)
    if value is None or not _ETAG_RE.match(value):
        return None
    return value


def content_length(response: Response) -> int | None:
    values = response.headers.get_list("Content-Length")
    if len(values) != 1:
        return None
    try:
        length = int(values[0].strip())
    except (TypeError, ValueError):
        return None
    return length if length >= 0 else None


def content_encodings(response: Response) -> tuple[str, ...]:
    return tuple(
        value.strip().lower()
        for value in response.headers.get_list("Content-Encoding", split_commas=True)
        if value.strip()
    )


def parse_content_range(value: str) -> tuple[int, int, int | None] | None:
    match = _CONTENT_RANGE_RE.match(value)
    if match is None:
        return None
    start = int(match.group(1))
    end = int(match.group(2))
    total = None if match.group(3) == "*" else int(match.group(3))
    if end < start:
        return None
    return start, end, total


def build_continuation_request(request: Request, offset: int, etag: str) -> Request:
    from ._models import Headers, Request

    headers = Headers(request.headers)
    for key in ("Range", "If-Range"):
        if key in headers:
            del headers[key]
    headers["Range"] = f"bytes={offset}-"
    headers["If-Range"] = etag

    extensions = dict(request.extensions)
    extensions.pop(RESUME_EXTENSION, None)

    return Request(
        method="GET",
        url=request.url,
        headers=headers,
        extensions=extensions,
    )


class _BaseResumeStream:
    def __init__(
        self,
        *,
        client: Client | AsyncClient,
        request: Request,
        response: Response,
        policy: ResumePolicy,
    ) -> None:
        self._client = client
        self._request = request
        self._policy = policy

        self._root_response = response
        self._current_response = response
        self._current_stream: SyncByteStream | AsyncByteStream | None = response.stream
        self._current_is_root = True
        self._root_released = False

        self._etag: str | None = None
        self._total: int | None = None
        self._encoding: tuple[str, ...] = ()
        self._offset = 0
        self._attempts = 0
        self._skip = 0

        self._initialized = False
        self._closed = False
        self._finished = False

    def _initialize(self) -> None:
        if self._initialized:
            return
        response = self._root_response

        if response.status_code != 200:
            raise ResumeError(
                "Cannot resume the response stream unless the initial response "
                f"is 200 (got {response.status_code}).",
                request=self._request,
            )

        etag = strong_etag(response)
        total = content_length(response)
        if etag is None:
            raise ResumeError(
                "Cannot resume the response stream without a strong ETag.",
                request=self._request,
            )
        if total is None:
            raise ResumeError(
                "Cannot resume the response stream without the original "
                "Content-Length.",
                request=self._request,
            )

        self._etag = etag
        self._total = total
        self._encoding = content_encodings(response)
        self._initialized = True

    def _check_attempts(self) -> None:
        limit = self._policy.max_requests
        if limit is not None and self._attempts >= limit:
            raise ResumeError(
                "The response stream ended before the complete body was "
                f"received, and the resume request limit ({limit}) was reached.",
                request=self._request,
            )

    def _validate_continuation(self, response: Response) -> bool:
        """
        Validate a continuation response.

        Returns ``True`` if the response was a valid 206 partial response, or
        ``False`` if the server returned a complete 200 response that did not
        honour the Range header.
        """
        assert self._etag is not None
        assert self._total is not None

        request = response._request if response._request is not None else self._request

        if response.status_code == 206:
            content_range = response.headers.get("Content-Range")
            if content_range is None:
                raise ResumeError(
                    "The continuation response did not include a Content-Range header.",
                    request=request,
                )
            parsed = parse_content_range(content_range)
            if parsed is None:
                raise ResumeError(
                    f"Invalid Content-Range header: {content_range!r}.",
                    request=request,
                )
            start, _end, total = parsed
            if start != self._offset:
                raise ResumeError(
                    "The continuation response declared a different starting "
                    f"offset ({start}) than requested ({self._offset}).",
                    request=request,
                )
            if total != self._total:
                raise ResumeError(
                    "The continuation response declared a different complete "
                    f"length ({total}) than the original ({self._total}).",
                    request=request,
                )

            etag = _header_etag(response)
            if etag is not None and etag != self._etag:
                raise ResumeError(
                    "The continuation response ETag did not match the original.",
                    request=request,
                )
            if "Content-Encoding" in response.headers:
                encodings = content_encodings(response)
                if encodings != self._encoding:
                    raise ResumeError(
                        "The continuation response Content-Encoding did not "
                        "match the original.",
                        request=request,
                    )
            return True

        if response.status_code == 200:
            if strong_etag(response) != self._etag:
                raise ResumeError(
                    "The continuation response ETag did not match the original.",
                    request=request,
                )
            if content_encodings(response) != self._encoding:
                raise ResumeError(
                    "The continuation response Content-Encoding did not match "
                    "the original.",
                    request=request,
                )
            if content_length(response) != self._total:
                raise ResumeError(
                    "The continuation response Content-Length did not match "
                    "the original.",
                    request=request,
                )
            return False

        raise ResumeError(
            "The continuation request did not return a 200 or 206 response "
            f"(got {response.status_code}).",
            request=request,
        )

    def _set_current(self, response: Response, *, partial: bool) -> None:
        self._current_response = response
        self._current_stream = response.stream
        self._current_is_root = False
        self._skip = 0 if partial else self._offset

    def _mark_error(self) -> None:
        self._root_response.is_closed = True


class ResumableSyncStream(_BaseResumeStream, SyncByteStream):
    def __iter__(self) -> typing.Iterator[bytes]:
        from ._exceptions import StreamClosed

        if self._closed:
            raise StreamClosed()
        self._initialize()

        while True:
            assert self._total is not None
            if self._offset >= self._total:
                self._finish()
                return

            stream = self._current_stream
            if self._closed:
                return
            if stream is None:  # pragma: no cover - defensive
                self._resume()
                continue

            started_at = self._offset
            was_root = self._current_is_root
            try:
                for chunk in stream:
                    if self._closed:
                        raise StreamClosed()
                    if self._skip:
                        if len(chunk) <= self._skip:
                            self._skip -= len(chunk)
                            continue
                        chunk = chunk[self._skip :]
                        self._skip = 0
                    if chunk:
                        self._offset += len(chunk)
                        yield chunk
            except ResumeError:
                self._release_current()
                self._mark_error()
                raise
            except StreamClosed:
                raise
            except (GeneratorExit, KeyboardInterrupt, SystemExit):
                self._release_current()
                raise
            except BaseException as exc:
                if self._closed:
                    raise
                self._release_current()
                if self._offset >= self._total:
                    self._finish()
                    return
                if self._offset == started_at and not was_root:
                    self._mark_error()
                    raise ResumeError(
                        "The continuation response did not provide any "
                        "additional data.",
                        request=self._request,
                    ) from exc
                self._resume(original_error=exc)
                continue

            if self._closed:
                return
            if self._offset >= self._total:
                self._finish()
                return

            if self._offset == started_at and not was_root:
                self._release_current()
                self._mark_error()
                raise ResumeError(
                    "The continuation response did not provide any additional data.",
                    request=self._request,
                )

            self._release_current()
            self._resume()

    def _resume(self, original_error: BaseException | None = None) -> None:
        from ._exceptions import StreamClosed

        if self._closed:
            raise StreamClosed()
        self._check_attempts()
        self._attempts += 1
        assert self._etag is not None
        request = build_continuation_request(self._request, self._offset, self._etag)

        try:
            response = self._client._send_single_request(request)
        except BaseException as exc:
            self._mark_error()
            raise ResumeError(
                f"Failed to resume the response stream: {exc}",
                request=self._request,
            ) from (original_error or exc)

        try:
            partial = self._validate_continuation(response)
        except BaseException:
            response.close()
            self._mark_error()
            raise

        self._set_current(response, partial=partial)

    def _release_current(self) -> None:
        stream = self._current_stream
        if stream is None:
            return
        self._current_stream = None
        try:
            if self._current_is_root:
                if not self._root_released:
                    self._root_released = True
                    # Close the underlying bound stream directly.  The public
                    # response owns this resumable wrapper, so closing the
                    # response here would recurse.
                    assert isinstance(stream, SyncByteStream)
                    stream.close()
            elif not self._current_response.is_closed:
                self._current_response.close()
        except Exception:  # pragma: no cover - best-effort release
            pass
        finally:
            self._current_response = self._root_response
            self._current_is_root = True
            self._skip = 0

    def _finish(self) -> None:
        self._release_current()
        self._finished = True

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._release_current()
            self._mark_error()


class ResumableAsyncStream(_BaseResumeStream, AsyncByteStream):
    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        from ._exceptions import StreamClosed

        if self._closed:
            raise StreamClosed()
        self._initialize()

        while True:
            assert self._total is not None
            if self._offset >= self._total:
                await self._finish_async()
                return

            stream = self._current_stream
            if self._closed:
                return
            if stream is None:  # pragma: no cover - defensive
                await self._resume()
                continue

            started_at = self._offset
            was_root = self._current_is_root
            try:
                async for chunk in stream:
                    if self._closed:
                        raise StreamClosed()
                    if self._skip:
                        if len(chunk) <= self._skip:
                            self._skip -= len(chunk)
                            continue
                        chunk = chunk[self._skip :]
                        self._skip = 0
                    if chunk:
                        self._offset += len(chunk)
                        yield chunk
            except ResumeError:
                await self._release_current_async()
                self._mark_error()
                raise
            except StreamClosed:
                raise
            except (GeneratorExit, KeyboardInterrupt, SystemExit):
                await self._release_current_async()
                raise
            except BaseException as exc:
                if self._closed:
                    raise
                await self._release_current_async()
                if self._offset >= self._total:
                    await self._finish_async()
                    return
                if self._offset == started_at and not was_root:
                    self._mark_error()
                    raise ResumeError(
                        "The continuation response did not provide any "
                        "additional data.",
                        request=self._request,
                    ) from exc
                await self._resume(original_error=exc)
                continue

            if self._closed:
                return
            if self._offset >= self._total:
                await self._finish_async()
                return

            if self._offset == started_at and not was_root:
                await self._release_current_async()
                self._mark_error()
                raise ResumeError(
                    "The continuation response did not provide any additional data.",
                    request=self._request,
                )

            await self._release_current_async()
            await self._resume()

    async def _resume(self, original_error: BaseException | None = None) -> None:
        from ._exceptions import StreamClosed

        if self._closed:
            raise StreamClosed()
        self._check_attempts()
        self._attempts += 1
        assert self._etag is not None
        request = build_continuation_request(self._request, self._offset, self._etag)

        try:
            response = await self._client._send_single_request(request)
        except BaseException as exc:
            self._mark_error()
            raise ResumeError(
                f"Failed to resume the response stream: {exc}",
                request=self._request,
            ) from (original_error or exc)

        try:
            partial = self._validate_continuation(response)
        except BaseException:
            await response.aclose()
            self._mark_error()
            raise

        self._set_current(response, partial=partial)

    async def _release_current_async(self) -> None:
        stream = self._current_stream
        if stream is None:
            return
        self._current_stream = None
        try:
            if self._current_is_root:
                if not self._root_released:
                    self._root_released = True
                    assert isinstance(stream, AsyncByteStream)
                    await stream.aclose()
            elif not self._current_response.is_closed:
                await self._current_response.aclose()
        except Exception:  # pragma: no cover - best-effort release
            pass
        finally:
            self._current_response = self._root_response
            self._current_is_root = True
            self._skip = 0

    async def _finish_async(self) -> None:
        await self._release_current_async()
        self._finished = True

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._release_current_async()
            self._mark_error()
