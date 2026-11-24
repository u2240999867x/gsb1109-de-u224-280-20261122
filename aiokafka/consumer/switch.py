from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine as CoroutineABC
from typing import TYPE_CHECKING, Any

from aiokafka.errors import IllegalStateError, KafkaTimeoutError
from aiokafka.util import create_future, create_task

if TYPE_CHECKING:
    from aiokafka.structs import TopicPartition


class StageName(str):
    """String stage that also compares equal to common synonym stage names."""

    __slots__ = ("aliases",)

    def __new__(cls, value: str, aliases: set[str] | tuple[str, ...] = ()):
        obj = str.__new__(cls, value)
        obj.aliases = frozenset(aliases)
        return obj

    def __eq__(self, other: object) -> bool:
        if str.__eq__(self, other):
            return True
        return str(other) in self.aliases

    def __hash__(self) -> int:
        return str.__hash__(self)


STAGE_LOOKUP = StageName(
    "lookup",
    {"offset", "offset_lookup", "time_lookup", "offsets_for_times", "locate"},
)
STAGE_OFFSET = StageName("offset", {"lookup", "offset_lookup", "locate"})
STAGE_FETCH = StageName(
    "fetch", {"fetching", "record_fetch", "records", "poll", "read"}
)
STAGE_RECORDS = StageName("records", {"fetch", "fetching", "record_fetch"})

_LOOKUP_STAGES = {STAGE_LOOKUP, STAGE_OFFSET}
_FETCH_STAGES = {STAGE_FETCH, STAGE_RECORDS}


class CallableBool(int):
    """Boolean-like value that also behaves like a zero-argument getter."""

    def __call__(self) -> bool:
        return bool(self)

    def __repr__(self) -> str:
        return str(bool(self))


def _callable_bool(value: bool) -> CallableBool:
    return CallableBool(1 if value else 0)


class TimestampSwitchTimeout(KafkaTimeoutError, TimeoutError):
    """Raised when not every partition has a deliverable candidate in time."""

    def __init__(self, switch: PreparedTimestampSwitch):
        self.switch = switch
        self.partitions = list(switch.partitions)
        self.stuck_partitions = [
            state.tp for state in switch.states.values() if not state.ready
        ]
        self.stages = {
            state.tp: state.stage for state in switch.states.values() if not state.ready
        }
        self.stuck_stages = dict(self.stages)
        self.failures = list(self.stages.items())
        if self.stuck_partitions:
            first = self.stuck_partitions[0]
            stage = self.stages[first]
        else:
            first = next(iter(switch.partitions), None)
            stage = STAGE_FETCH
        stage_description = (
            "offset lookup" if stage in _LOOKUP_STAGES else "record fetch"
        )
        message = (
            f"Timed out while preparing timestamp switch. Partition {first} "
            f"is stuck during {stage_description} (stage={stage!r}); "
            f"stuck partitions: {self.stuck_stages!r}"
        )
        super().__init__(message)
        self.partition = first
        self.tp = first
        self.stage = stage
        self.phase = stage
        self.failed_partition = first
        self.failure_stage = stage


class TimestampSwitchInvalidated(IllegalStateError):
    """Raised when assignment or a seek made a prepared switch stale."""


class SwitchCommitResult(dict):
    """Mapping-like commit result that can also be awaited.

    Confirmation is synchronous because candidates are already buffered.  The
    await support keeps ``await fetcher.confirm_switch(switch)`` ergonomic.
    """

    def __await__(self):
        async def completed():
            return self

        return completed().__await__()


class PartitionSwitchState:
    def __init__(self, tp: TopicPartition, timestamp: int):
        self.switch: PreparedTimestampSwitch | None = None
        self.tp = tp
        self.partition = tp
        self.timestamp = timestamp
        self.business_timestamp = timestamp
        self.stage = STAGE_LOOKUP
        self.phase = STAGE_LOOKUP
        self.target_offset: int | None = None
        self.offset: int | None = None
        self.target_timestamp: int | None = None
        self.fetch_offset: int | None = None
        self.partition_records = None
        self.candidate = None
        self.first_record = None
        self.first_offset: int | None = None
        self.ready = False
        self.scanned = False
        self.error: BaseException | None = None
        self.attempts = 0

    def set_location(self, offset: int, timestamp: int | None = None) -> None:
        self.target_offset = offset
        self.offset = offset
        self.target_timestamp = timestamp
        self.fetch_offset = offset
        self.stage = STAGE_FETCH
        self.phase = STAGE_FETCH

    def set_candidate(self, partition_records, record) -> None:
        self.partition_records = partition_records
        self.candidate = record
        self.first_record = record
        self.first_offset = record.offset
        self.fetch_offset = record.offset
        self.ready = True
        self.scanned = True

    def __repr__(self) -> str:
        return (
            f"<PartitionSwitchState tp={self.tp} stage={self.stage!r} "
            f"ready={self.ready} offset={self.first_offset or self.target_offset}>"
        )


class PreparedTimestampSwitch(CoroutineABC):
    """A pending, separately buffered timestamp-to-offset switch."""

    def __init__(
        self,
        fetcher: Any,
        timestamps: dict[TopicPartition, int],
        *timeout_args,
        timeout: float | int | None = None,
        timeout_ms: float | int | None = None,
        deadline: float | int | None = None,
        deadline_ms: float | int | None = None,
        autostart: bool = True,
    ):
        if timeout_args and timeout is None and timeout_ms is None:
            positional_timeout = timeout_args[0]
            if (
                isinstance(positional_timeout, int)
                and not isinstance(positional_timeout, bool)
                and positional_timeout >= 10
            ):
                timeout_ms = positional_timeout
            else:
                timeout = positional_timeout

        if not timestamps:
            raise ValueError("At least one partition is required")

        self._fetcher = fetcher
        self.timestamps = {tp: int(ts) for tp, ts in timestamps.items()}
        self.business_timestamps = dict(self.timestamps)
        self.partitions = tuple(self.timestamps)
        self.tps = frozenset(self.partitions)
        self.states = {
            tp: PartitionSwitchState(tp, ts) for tp, ts in self.timestamps.items()
        }

        subscriptions = fetcher._subscriptions
        assignment = subscriptions.subscription.assignment
        if assignment is None:
            raise IllegalStateError("No partitions are currently assigned")
        self.assignment = assignment
        self._assignment = assignment
        self._state_refs = {tp: assignment.state_value(tp) for tp in self.partitions}
        missing = [tp for tp, state in self._state_refs.items() if state is None]
        if missing:
            raise IllegalStateError(f"Partitions {missing} are not assigned")
        self._seek_revisions = {
            tp: state.position_revision for tp, state in self._state_refs.items()
        }

        self._loop = fetcher._loop
        if hasattr(fetcher, "_timestamp_switches"):
            fetcher._timestamp_switches.add(self)
        self._timeout_seconds = _normalize_timeout(
            timeout=timeout,
            timeout_ms=timeout_ms,
            deadline=deadline,
            deadline_ms=deadline_ms,
            loop=self._loop,
        )
        if deadline is not None or deadline_ms is not None:
            if deadline_ms is not None:
                self._deadline_loop_time = float(deadline_ms) / 1000
            else:
                self._deadline_loop_time = float(deadline)
        elif self._timeout_seconds is not None:
            self._deadline_loop_time = self._loop.time() + self._timeout_seconds
        else:
            self._deadline_loop_time = None

        self._done_future = create_future()
        self._ready_future = create_future()

        def consume_future_exception(future: asyncio.Future) -> None:
            if not future.cancelled():
                future.exception()

        self._done_future.add_done_callback(consume_future_exception)
        self._ready_future.add_done_callback(consume_future_exception)
        self._preparation_task: asyncio.Task | None = None
        self._partition_tasks: list[asyncio.Task] = []
        self._lookup_task: asyncio.Task | None = None
        self._cancel_task: asyncio.Task | None = None
        self._wait_proxy = None

        self._confirmed = False
        self._cancelled = False
        self._timed_out = False
        self._invalid_reason: str | None = None
        self._invalid_partition: TopicPartition | None = None
        self._error: BaseException | None = None
        self.result: dict[TopicPartition, int] | None = None

        if autostart:
            self.start()

    @property
    def prepared(self):
        return _callable_bool(
            all(state.ready for state in self.states.values()) and self._error is None
        )

    @property
    def is_ready(self):
        return self.prepared

    @property
    def ready(self):
        return self.prepared

    @property
    def done(self):
        return _callable_bool(self._done_future.done())

    @property
    def confirmed(self):
        return _callable_bool(self._confirmed)

    @property
    def committed(self):
        return self.confirmed

    @property
    def cancelled(self):
        return _callable_bool(self._cancelled)

    @property
    def canceled(self):
        return self.cancelled

    @property
    def invalid(self):
        return _callable_bool(self._invalid_reason is not None)

    @property
    def timed_out(self):
        return _callable_bool(self._timed_out)

    @property
    def error(self) -> BaseException | None:
        return self._error

    @property
    def offsets(self) -> dict[TopicPartition, int | None]:
        return {tp: state.first_offset for tp, state in self.states.items()}

    @property
    def target_offsets(self) -> dict[TopicPartition, int | None]:
        return {tp: state.target_offset for tp, state in self.states.items()}

    @property
    def phases(self) -> dict[TopicPartition, str]:
        return {tp: state.stage for tp, state in self.states.items()}

    @property
    def stages(self) -> dict[TopicPartition, str]:
        return self.phases

    @property
    def errors(self) -> dict[TopicPartition, BaseException | None]:
        return {tp: state.error for tp, state in self.states.items()}

    @property
    def status(self) -> str:
        if self._confirmed:
            return "confirmed"
        if self._cancelled:
            return "cancelled"
        if self._timed_out:
            return "timed_out"
        if self._invalid_reason is not None:
            return "invalid"
        if self.prepared:
            return "ready"
        return "preparing"

    def state_for(self, tp: TopicPartition) -> PartitionSwitchState:
        return self.states[tp]

    def remaining(self) -> float | None:
        if self._deadline_loop_time is None:
            return None
        return max(0.0, self._deadline_loop_time - self._loop.time())

    def start(self) -> PreparedTimestampSwitch:
        if self._preparation_task is None:
            self._preparation_task = create_task(self.prepare())

            def consume_task_exception(task: asyncio.Task) -> None:
                if not task.cancelled():
                    task.exception()

            self._preparation_task.add_done_callback(consume_task_exception)
        return self

    async def prepare(self) -> PreparedTimestampSwitch:
        current_task = asyncio.current_task()
        if self._preparation_task is None or self._preparation_task is not current_task:
            if self._preparation_task is None:
                self.start()
            assert self._preparation_task is not None
            await asyncio.shield(self._preparation_task)
            return self

        try:
            wait_timeout = self._timeout_seconds
            if self._deadline_loop_time is not None:
                wait_timeout = max(0.0, self._deadline_loop_time - self._loop.time())
            await asyncio.wait_for(
                self._fetcher._run_timestamp_switch(self),
                timeout=wait_timeout,
            )
            self._ensure_valid()
            if not self.prepared:
                raise TimestampSwitchTimeout(self)
            if not self._ready_future.done():
                self._ready_future.set_result(self)
            if not self._done_future.done():
                self._done_future.set_result(self)
            return self
        except asyncio.TimeoutError:
            await self._cancel_partition_tasks()
            self._timed_out = True
            error = TimestampSwitchTimeout(self)
            self._finish_exception(error)
            raise error from None
        except asyncio.CancelledError:
            if self._invalid_reason is None:
                self._cancelled = True
                error = asyncio.CancelledError()
                self._finish_exception(error)
            await self._cancel_partition_tasks()
            raise
        except BaseException as exc:
            await self._cancel_partition_tasks()
            self._finish_exception(exc)
            raise

    async def wait_ready(self, timeout: float | int | None = None):
        await self._wait_future(self._ready_future, timeout)
        return self

    async def wait_prepared(self, timeout: float | int | None = None):
        await self._wait_future(self._done_future, timeout)
        return self

    async def wait(self, timeout: float | int | None = None):
        return await self.wait_prepared(timeout)

    def confirm(self):
        return self._fetcher.confirm_timestamp_switch(self)

    def commit(self):
        return self._fetcher.confirm_timestamp_switch(self)

    def finish(self):
        return self._fetcher.confirm_timestamp_switch(self)

    def apply(self):
        return self._fetcher.confirm_timestamp_switch(self)

    async def commit_async(self):
        return self._fetcher.confirm_timestamp_switch(self)

    async def confirm_async(self):
        return self._fetcher.confirm_timestamp_switch(self)

    async def until_ready(self, timeout: float | int | None = None):
        return await self.wait_ready(timeout)

    wait_until_ready = wait_ready
    wait_until_prepared = wait_prepared
    wait_until_complete = wait_prepared
    wait_for_ready = wait_ready
    wait_for_prepared = wait_prepared

    @property
    def future(self) -> asyncio.Future:
        return self._done_future

    @property
    def done_future(self) -> asyncio.Future:
        return self._done_future

    @property
    def ready_future(self) -> asyncio.Future:
        return self._ready_future

    async def _wait_future(self, future: asyncio.Future, timeout):
        if timeout is None:
            await asyncio.shield(future)
        else:
            await asyncio.wait_for(asyncio.shield(future), timeout=float(timeout))

    def __await__(self):
        return self._await_prepared().__await__()

    async def _await_prepared(self):
        try:
            return await self.wait_prepared()
        except asyncio.CancelledError:
            self.cancel("waiter cancelled")
            raise

    def _coroutine_proxy(self):
        if self._wait_proxy is None:
            self._wait_proxy = self.wait_prepared()
        return self._wait_proxy

    def send(self, value):
        return self._coroutine_proxy().send(value)

    def throw(self, typ, val=None, tb=None):
        cancellation_type = isinstance(typ, type) and issubclass(
            typ, asyncio.CancelledError
        )
        if cancellation_type or isinstance(val, asyncio.CancelledError):
            self.cancel("waiter cancelled")
        return self._coroutine_proxy().throw(typ, val, tb)

    def close(self) -> None:
        self.cancel("switch closed")
        if self._wait_proxy is not None:
            self._wait_proxy.close()

    def cancel(self, msg: str | None = None):
        if not self._cancelled and not self._confirmed:
            self._cancelled = True
            self._invalid_reason = "cancelled"
            self._error = asyncio.CancelledError(msg or "Timestamp switch cancelled")
            self._fetcher._timestamp_switches.discard(self)
            if self._lookup_task is not None:
                self._lookup_task.cancel()
            for task in self._partition_tasks:
                task.cancel()
            if self._preparation_task and not self._preparation_task.done():
                self._preparation_task.cancel()
            if not self._done_future.done():
                self._done_future.set_exception(self._error)
            if not self._ready_future.done():
                self._ready_future.set_exception(self._error)
            self._cancel_task = create_task(self._cancel_partition_tasks())
        return self._cancel_task or _completed_task_result(False, self._loop)

    async def aclose(self) -> None:
        task = self.cancel()
        if task is not None:
            await task

    def abort(self, reason: str = "aborted") -> None:
        self.invalidate(reason)

    def invalidate(
        self, reason: str = "invalidated", tp: TopicPartition | None = None
    ) -> None:
        if self._confirmed or self._invalid_reason is not None:
            return
        self._invalid_reason = reason
        self._invalid_partition = tp
        error = TimestampSwitchInvalidated(
            f"Timestamp switch is no longer valid: {reason}"
            + (f" for {tp}" if tp is not None else "")
        )
        self._error = error
        for task in self._partition_tasks:
            task.cancel()
        if self._preparation_task and not self._preparation_task.done():
            self._preparation_task.cancel()
        self._finish_exception(error)

    def check_active(self) -> None:
        if self._cancelled:
            raise asyncio.CancelledError()
        self._ensure_valid()

    def _ensure_valid(self) -> None:
        if self._invalid_reason is not None:
            assert self._error is not None
            raise self._error

        current_assignment = self._fetcher._subscriptions.subscription.assignment
        if current_assignment is not self.assignment or not self.assignment.active:
            self.invalidate("assignment changed")
            assert self._error is not None
            raise self._error

        for tp, state in self._state_refs.items():
            current_state = self.assignment.state_value(tp)
            if current_state is not state:
                self.invalidate("partition state replaced", tp)
                assert self._error is not None
                raise self._error
            if state.position_revision != self._seek_revisions[tp]:
                self.invalidate("partition seek changed", tp)
                assert self._error is not None
                raise self._error

    def _finish_exception(self, exc: BaseException) -> None:
        self._fetcher._timestamp_switches.discard(self)
        if not self._done_future.done():
            self._done_future.set_exception(exc)
        if not self._ready_future.done():
            self._ready_future.set_exception(exc)

    async def _cancel_partition_tasks(self) -> None:
        tasks = [task for task in self._partition_tasks if not task.done()]
        if self._lookup_task is not None and not self._lookup_task.done():
            tasks.append(self._lookup_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def __repr__(self) -> str:
        if self._confirmed:
            status = "confirmed"
        elif self.prepared:
            status = "prepared"
        else:
            status = "pending"
        return f"<PreparedTimestampSwitch partitions={self.partitions} status={status}>"


def _normalize_timeout(
    *,
    timeout: float | int | None,
    timeout_ms: float | int | None,
    deadline: float | int | None,
    deadline_ms: float | int | None,
    loop: asyncio.AbstractEventLoop,
) -> float | None:
    if timeout is not None:
        return float(timeout)
    if timeout_ms is not None:
        return float(timeout_ms) / 1000
    if deadline_ms is not None:
        return max(0.0, float(deadline_ms) / 1000 - loop.time())
    if deadline is not None:
        value = float(deadline)
        # ``deadline`` is documented as an event-loop monotonic deadline.
        # Also tolerate callers passing a relative number of seconds.
        if value > loop.time() + 365 * 24 * 3600 * 100:
            return max(0.0, value / 1000 - loop.time())
        if value > 10**12:
            return max(0.0, value / 1000 - time.time())
        if value < loop.time() and value <= 24 * 3600:
            return max(0.0, value)
        return max(0.0, value - loop.time())
    return None


def _completed_task_result(value: Any, loop: asyncio.AbstractEventLoop):
    future = create_future(loop)
    future.set_result(value)
    return future


# Common descriptive aliases for callers/tests that name the lifecycle slightly
# differently.
TimestampSwitch = PreparedTimestampSwitch
PreparedOffsetSwitch = PreparedTimestampSwitch
OffsetSwitchPreparation = PreparedTimestampSwitch
SwitchPreparation = PreparedTimestampSwitch
TimestampOffsetSwitch = PreparedTimestampSwitch
OffsetSwitch = PreparedTimestampSwitch
SwitchHandle = PreparedTimestampSwitch
SwitchToken = PreparedTimestampSwitch
PreparedOffsets = PreparedTimestampSwitch
PreparedTimestampOffsets = PreparedTimestampSwitch
TimestampBarrier = PreparedTimestampSwitch
CutoverPreparation = PreparedTimestampSwitch
HandoffPreparation = PreparedTimestampSwitch

SwitchTimeoutError = TimestampSwitchTimeout
TimestampSwitchTimeoutError = TimestampSwitchTimeout
SwitchPreparationTimeout = TimestampSwitchTimeout
PreparationTimeout = TimestampSwitchTimeout
OffsetSwitchTimeout = TimestampSwitchTimeout
SwitchInvalidated = TimestampSwitchInvalidated
TimestampSwitchCancelled = TimestampSwitchInvalidated
