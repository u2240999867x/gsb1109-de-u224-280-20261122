"""Cross-validation request coordination.

Pandera validations are synchronous and often run in separate worker threads,
each with its own event loop.  A naïve per-validation semaphore therefore
cannot enforce one provider-wide concurrency budget, and an asyncio future
cannot be shared between those loops.

This module gives each provider one process-local broker.  The broker submits
real provider calls to one daemon event-loop thread, keys concurrent work by
the normal answer-cache key, and shares the resulting :class:`Reply` with every
caller.  Callers may stop waiting (for example because another row in their
validation failed) without cancelling the shared provider call.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

from pandera.decisions.execution import (
    StateTooLongError,
    TokenBucket,
    estimate_tokens,
)
from pandera.decisions.primitives import Decision

SOURCE_CACHE = "cache"
SOURCE_SHARED = "shared"
SOURCE_CALL = "call"


@dataclass(frozen=True)
class Reply:
    """An answer and how the caller obtained it."""

    value: Mapping[str, Decision]
    source: str


class _EventLoopThread:
    """One event loop shared by all provider brokers in this process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def submit(self, coroutine: Any) -> Future[Any]:
        """Submit a coroutine to the shared loop, starting it if needed."""
        with self._lock:
            if self._loop is None or self._thread is None:
                self._ready.clear()
                thread = threading.Thread(
                    target=self._run,
                    name="pandera-decision-loop",
                    daemon=True,
                )
                self._thread = thread
                thread.start()
                self._ready.wait()
            loop = self._loop
        assert loop is not None
        return asyncio.run_coroutine_threadsafe(coroutine, loop)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_forever()
        finally:  # pragma: no cover - daemon shutdown path
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.close()


_EVENT_LOOP = _EventLoopThread()


class ProviderBroker:
    """Coalesces identical in-flight calls for one provider."""

    _registry_lock = threading.Lock()
    _registry: dict[int, tuple[Any, ProviderBroker]] = {}
    _strong_registry: dict[int, ProviderBroker] = {}

    def __init__(self, provider: Any) -> None:
        self.provider = provider
        self._in_flight: dict[str, Future[Reply]] = {}
        # RLock because a future can already be done when add_done_callback is
        # called; concurrent.futures then invokes the removal callback inline.
        self._lock = threading.RLock()
        self._semaphore: asyncio.Semaphore | None = None
        self._bucket: TokenBucket | None = None
        self._limits_signature: tuple[Any, ...] | None = None

    @classmethod
    def for_provider(cls, provider: Any) -> ProviderBroker:
        """Return the process-local broker for ``provider``."""
        provider_id = id(provider)
        with cls._registry_lock:
            entry = cls._registry.get(provider_id)
            if entry is not None and entry[0]() is provider:
                return entry[1]

            broker = cls(provider)
            try:
                reference = weakref.ref(provider)
            except TypeError:
                # A provider using slots without __weakref__ cannot be weakly
                # referenced.  Keeping it for process lifetime is acceptable
                # for these unusual test/runtime doubles.
                cls._strong_registry[provider_id] = broker
                reference = lambda: provider  # noqa: E731
            cls._registry[provider_id] = (reference, broker)
            return broker

    def begin(
        self,
        key: str,
        state: Any,
        compiled: Any,
        cache: Any,
    ) -> Reply | tuple[Future[Reply], str]:
        """Return a cache reply or the shared future plus its source."""
        return self._begin(key, state, compiled, cache)

    async def resolve(self, future: Future[Reply]) -> Mapping[str, Decision]:
        """Wait for a shared call without allowing caller cancellation to
        cancel it."""
        # Shielding is essential: a validation can raise on another row and
        # cancel this caller task, but B may still be waiting for the same
        # provider call.  The underlying future belongs to _EVENT_LOOP.
        reply = await asyncio.shield(asyncio.wrap_future(future))
        return reply.value

    def _begin(
        self,
        key: str,
        state: Any,
        compiled: Any,
        cache: Any,
    ) -> Reply | tuple[Future[Reply], str]:
        with self._lock:
            existing = self._in_flight.get(key)
            if existing is not None:
                return existing, SOURCE_SHARED

            # Recheck under the lock.  The previous leader may have completed
            # between an earlier unlocked cache read and registration.
            if cache is not None:
                cached = cache.get(key)
                if cached is not None:
                    return Reply(cached, SOURCE_CACHE)

            future = _EVENT_LOOP.submit(
                self._call(key, state, compiled, cache)
            )
            self._in_flight[key] = future

            def _remove(done: Future[Reply]) -> None:
                with self._lock:
                    if self._in_flight.get(key) is done:
                        del self._in_flight[key]

            future.add_done_callback(_remove)
            return future, SOURCE_CALL

    async def _call(
        self,
        key: str,
        state: Any,
        compiled: Any,
        cache: Any,
    ) -> Reply:
        limits = self.provider.limits
        signature = (
            limits.max_concurrency,
            limits.requests_per_minute,
            limits.tokens_per_second,
            limits.max_state_tokens,
        )
        if self._limits_signature != signature:
            concurrency = max(1, int(limits.max_concurrency or 1))
            self._semaphore = asyncio.Semaphore(concurrency)
            self._bucket = TokenBucket(
                requests_per_minute=limits.requests_per_minute,
                tokens_per_second=limits.tokens_per_second,
            )
            self._limits_signature = signature

        tokens = estimate_tokens(state)
        if (
            limits.max_state_tokens is not None
            and tokens > limits.max_state_tokens
        ):
            raise StateTooLongError(
                f"the state is about {tokens} tokens, over the "
                f"{limits.max_state_tokens} this provider accepts. Shorten "
                "what the column reads from."
            )

        assert self._semaphore is not None
        assert self._bucket is not None
        async with self._semaphore:
            await self._bucket.acquire(tokens)
            answer = await self.provider.decide(state, compiled)

        if cache is not None:
            await asyncio.to_thread(cache.set, key, answer)
        return Reply(answer, SOURCE_CALL)
