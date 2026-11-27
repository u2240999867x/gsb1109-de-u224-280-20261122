"""
Optional sampling of the raw response bytes delivered while following
redirects.

When redirect following is enabled, the client normally drains each
intermediate response body before issuing the next request.  This module
adds an optional shared byte budget for that drainage, so that only a small
sample of each redirect body is retained instead of buffering whole bodies
that the caller will never inspect.

Two kinds of redirect bodies are supported:

* ``mandatory=True`` bodies are always read in full by the existing response
  event hooks (using ``response.read()`` / ``response.aread()``).  A stream
  tap captures the first raw, still-encoded bytes as they pass through.
* Regular bodies are pulled one raw chunk at a time, stopping as soon as the
  per-hop limit or the shared budget is reached.

The same budget is shared by every hop of a single fetch, including any
requests retried by the authentication flow.  Authentication challenge
responses are consumed by the auth flow itself and never draw from the
budget.
"""

from __future__ import annotations

import enum
import typing

from ._types import AsyncByteStream, SyncByteStream

if typing.TYPE_CHECKING:
    from ._models import Request, Response

__all__ = [
    "RedirectSample",
    "RedirectSampleReason",
    "RedirectSampleRule",
    "RedirectSampling",
]

# Given a request and the redirect response just received, return the
# per-hop sample limit and whether the body must be checked in full.
RedirectSampleRule = typing.Callable[
    ["Request", "Response"], typing.Tuple[int, bool]
]


class RedirectSampleReason(enum.Enum):
    """
    Why sampling stopped for a single hop.
    """

    # The whole response body was observed, i.e. the underlying stream ended.
    COMPLETE = "complete"
    # Pulling stopped because the per-hop limit or the shared budget was hit.
    # A hop that is never pulled because the budget was already exhausted is
    # also recorded as limited, since stream end was never observed.
    LIMITED = "limited"
    # Pulling the next raw chunk raised an exception.
    ERRORED = "errored"


class RedirectSample:
    """
    The sampling record for a single followed redirect response.

    Attributes:
        max_bytes: The per-hop sample limit requested by the caller.
        mandatory: Whether the body had to be read and checked in full.
        bytes_pulled: Number of raw, encoded bytes actually delivered by the
            transport while handling this hop.
        sample: The retained raw bytes (at most ``max_bytes`` long).
        stop_reason: Why pulling stopped for this hop.
        error: The original exception, when ``stop_reason`` is
            ``RedirectSampleReason.ERRORED``.
    """

    def __init__(
        self,
        *,
        max_bytes: int,
        mandatory: bool,
        bytes_pulled: int = 0,
        sample: bytes | None = None,
        stop_reason: RedirectSampleReason | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.max_bytes = max_bytes
        self.mandatory = mandatory
        self.bytes_pulled = bytes_pulled
        self.sample = b"" if sample is None else sample
        # Left unset until the hop is finalized, so an exception raised
        # before drainage can be recorded as ERRORED.
        self.stop_reason = stop_reason
        self.error = error
        # Raw bytes captured by the stream tap for mandatory hops.
        self._sink = bytearray()

    def __repr__(self) -> str:
        return (
            "RedirectSample("
            f"max_bytes={self.max_bytes!r}, mandatory={self.mandatory!r}, "
            f"bytes_pulled={self.bytes_pulled!r}, sample={self.sample!r}, "
            f"stop_reason={self.stop_reason!r})"
        )


class _TappedSyncStream(SyncByteStream):
    """
    Wraps a synchronous response stream, recording the first ``limit`` raw
    bytes that flow through it without otherwise altering iteration.
    """

    def __init__(self, stream: SyncByteStream, sink: bytearray, limit: int) -> None:
        self._stream = stream
        self._sink = sink
        self._limit = limit

    def __iter__(self) -> typing.Iterator[bytes]:
        for chunk in self._stream:
            if len(self._sink) < self._limit:
                room = self._limit - len(self._sink)
                self._sink.extend(chunk[:room])
            yield chunk

    def close(self) -> None:
        self._stream.close()


class _TappedAsyncStream(AsyncByteStream):
    """
    Wraps an asynchronous response stream, recording the first ``limit`` raw
    bytes that flow through it without otherwise altering iteration.
    """

    def __init__(self, stream: AsyncByteStream, sink: bytearray, limit: int) -> None:
        self._stream = stream
        self._sink = sink
        self._limit = limit

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        async for chunk in self._stream:
            if len(self._sink) < self._limit:
                room = self._limit - len(self._sink)
                self._sink.extend(chunk[:room])
            yield chunk

    async def aclose(self) -> None:
        await self._stream.aclose()


class RedirectSampling:
    """
    Configuration for retaining a small raw-byte sample of each followed
    redirect body.

    A single ``RedirectSampling`` instance may be shared by many fetches:
    every fetch gets its own byte budget, tracked by an internal sampler.

    Parameters:
        budget: Total number of raw bytes that one fetch may retain across
            all of its redirect hops, including authentication retries.
        rule: Callable invoked for every redirect response, returning the
            per-hop sample limit and whether that hop is mandatory and must
            be checked in full.
    """

    def __init__(self, budget: int, rule: RedirectSampleRule) -> None:
        if budget < 0:
            raise ValueError("The redirect sample budget must be zero or positive.")
        self.budget = budget
        self.rule = rule

    def start(self) -> "_Sampler":
        return _Sampler(budget=self.budget, rule=self.rule)


class _Sampler:
    """
    Per-fetch state: the shared raw-byte budget and every hop record.
    """

    def __init__(self, budget: int, rule: RedirectSampleRule) -> None:
        self.budget = budget
        self.remaining = budget
        self.rule = rule
        self.samples: list[RedirectSample] = []

    def begin_hop(self, response: "Response") -> RedirectSample:
        """
        Register a new redirect hop and, for mandatory checks, install the
        raw stream tap used to retain encoded bytes while the response hooks
        read the decoded body in full.
        """
        max_bytes, mandatory = self.rule(response.request, response)
        if max_bytes < 0:
            raise ValueError("The per-hop sample limit must be zero or positive.")

        record = RedirectSample(max_bytes=max_bytes, mandatory=mandatory)
        response.redirect_sample = record
        self.samples.append(record)

        if mandatory:
            response.stream = self._tap(
                response.stream, record._sink, max_bytes
            )

        return record

    def drain_sync(self, response: "Response") -> None:
        """
        Drain a regular, non-mandatory redirect response using at most the
        remaining shared budget, then close it before the next hop is sent.
        """
        record = response.redirect_sample
        assert record is not None
        limit = min(record.max_bytes, self.remaining)

        if limit == 0:
            # No budget left: do not pull even once, just release the body.
            # Stream end was never observed, so this counts as limited.
            record.stop_reason = RedirectSampleReason.LIMITED
            self._finish_sync(response, record)
            return

        try:
            for chunk in response.iter_raw():
                record.bytes_pulled += len(chunk)
                room = limit - len(record.sample)
                if room:
                    record.sample += chunk[:room]
                if len(record.sample) >= limit:
                    # Stopping exactly on a chunk boundary still counts as
                    # hitting the limit, as stream end was not observed.
                    record.stop_reason = RedirectSampleReason.LIMITED
                    break
            else:
                # A for/else only runs when iteration observed stream end.
                record.stop_reason = RedirectSampleReason.COMPLETE
        except BaseException as exc:
            record.stop_reason = RedirectSampleReason.ERRORED
            record.error = exc
            raise
        finally:
            self._finish_sync(response, record)

    async def drain_async(self, response: "Response") -> None:
        """
        Asynchronous counterpart of ``drain_sync``.
        """
        record = response.redirect_sample
        assert record is not None
        limit = min(record.max_bytes, self.remaining)

        if limit == 0:
            record.stop_reason = RedirectSampleReason.LIMITED
            await self._finish_async(response, record)
            return

        try:
            async for chunk in response.aiter_raw():
                record.bytes_pulled += len(chunk)
                room = limit - len(record.sample)
                if room:
                    record.sample += chunk[:room]
                if len(record.sample) >= limit:
                    record.stop_reason = RedirectSampleReason.LIMITED
                    break
            else:
                record.stop_reason = RedirectSampleReason.COMPLETE
        except BaseException as exc:
            record.stop_reason = RedirectSampleReason.ERRORED
            record.error = exc
            raise
        finally:
            await self._finish_async(response, record)

    def complete_mandatory(self, response: "Response") -> None:
        """
        Finalize a mandatory hop whose body was fully read by the response
        hooks, recording the tapped raw bytes.
        """
        record = response.redirect_sample
        assert record is not None
        record.sample = bytes(record._sink)
        record.bytes_pulled = response.num_bytes_downloaded
        record.stop_reason = RedirectSampleReason.COMPLETE
        self._charge(record)

    def mark_error(self, response: "Response", exc: BaseException) -> None:
        """
        Record an exception that interrupted a hop before it could be
        finalized. Already-finalized hops are left untouched. For mandatory
        hops, retain whatever the stream tap captured before the failure.
        """
        record = response.redirect_sample
        assert record is not None
        if record.stop_reason is not None:
            return
        if record.mandatory:
            record.sample = bytes(record._sink)
            record.bytes_pulled = response.num_bytes_downloaded
        record.stop_reason = RedirectSampleReason.ERRORED
        record.error = exc
        self._charge(record)

    def attach_error(self, exc: BaseException) -> None:
        """
        Expose the per-fetch sampler on a failed fetch, so callers can still
        inspect the records gathered before the original error was raised.
        """
        exc.redirect_sampler = self  # type: ignore[attr-defined]

    def _finish_sync(
        self, response: "Response", record: RedirectSample
    ) -> None:
        self._charge(record)
        response.close()

    async def _finish_async(
        self, response: "Response", record: RedirectSample
    ) -> None:
        self._charge(record)
        await response.aclose()

    def _charge(self, record: RedirectSample) -> None:
        self.remaining -= len(record.sample)
        if self.remaining < 0:
            self.remaining = 0

    def _tap(
        self,
        stream: SyncByteStream | AsyncByteStream,
        sink: bytearray,
        limit: int,
    ) -> SyncByteStream | AsyncByteStream:
        if isinstance(stream, SyncByteStream):
            return _TappedSyncStream(stream, sink, limit)
        return _TappedAsyncStream(stream, sink, limit)
