import asyncio
import typing

from databases.audit import (
    ITERATION_AUDIT,
    IterationAuditLog,
    IterationAuditRecord,
    IterationStatus,
)

__all__ = ["IterationStream", "IterationAuditRecord", "IterationStatus"]

_T = typing.TypeVar("_T")


def _row_identifier(row: typing.Any) -> typing.Any:
    for key in ("event_id", "event", "event_no", "event_number", "id"):
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            continue
    try:
        return row[0]
    except (KeyError, IndexError, TypeError):
        return None


class IterationStream(typing.Generic[_T]):
    """
    A single streaming query result that may be consumed by more than one
    task over its lifetime.

    The stream is an async iterator, so the original ``async for`` usage is
    unchanged.  In addition it exposes an idempotent ``close()`` method and a
    ``handoff()`` helper that changes the consumer label used for audit
    accounting without restarting the query.
    """

    def __init__(
        self,
        connection: typing.Any,
        query: typing.Any,
        *,
        batch_id: typing.Optional[str] = None,
        consumer: typing.Optional[str] = None,
        audit: typing.Union[bool, IterationAuditRecord, IterationAuditLog, None] = True,
        metadata: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> None:
        self._connection = connection
        self._query = query
        self._metadata = dict(metadata or {})

        if isinstance(audit, IterationAuditRecord):
            self._audit_record = audit
        else:
            self._audit_record = IterationAuditRecord(
                batch_id=batch_id, metadata=self._metadata
            )

        if isinstance(audit, IterationAuditLog):
            self._audit_log: typing.Optional[IterationAuditLog] = audit
        elif audit is False:
            self._audit_log = None
        else:
            self._audit_log = ITERATION_AUDIT

        if isinstance(audit, IterationAuditLog):
            audit.add(self._audit_record)
        elif isinstance(audit, IterationAuditRecord):
            ITERATION_AUDIT.add(self._audit_record)
        elif audit is True:
            ITERATION_AUDIT.add(self._audit_record)

        self._iterator: typing.Optional[typing.AsyncIterator[_T]] = None
        self._transaction: typing.Any = None
        self._query_lock_acquired = False

        self._started = False
        self._closed = False
        self._exhausted = False
        self._failed = False
        self._terminal_emitted = False

        self._read_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()

        self._owner_task: typing.Optional[asyncio.Task] = None
        self._consumer_task: typing.Optional[asyncio.Task] = None
        self._consumer = consumer
        self._consumer_explicit = consumer is not None
        self._consumer_sequence = 0
        self._used_consumers: typing.Set[str] = set()
        self._task_consumers: typing.Dict[
            typing.Optional[asyncio.Task], str
        ] = {}
        self._pending_handoff: typing.Optional[str] = None
        self._next_consumer_pending = False
        if consumer is not None:
            self._used_consumers.add(consumer)

    @property
    def audit(self) -> IterationAuditRecord:
        return self._audit_record

    @property
    def audit_record(self) -> IterationAuditRecord:
        return self._audit_record

    @property
    def record(self) -> IterationAuditRecord:
        return self._audit_record

    @property
    def batch_id(self) -> typing.Optional[str]:
        return self._audit_record.batch_id

    @property
    def status(self) -> IterationStatus:
        return self._audit_record.status

    @property
    def started(self) -> bool:
        return self._started

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    @property
    def consumer(self) -> typing.Optional[str]:
        """The stream's current consumer label, if one has been assigned."""
        return self._consumer

    @consumer.setter
    def consumer(self, value: str) -> None:
        self._set_explicit_consumer(value)

    def set_consumer(self, consumer: str) -> "IterationStream[_T]":
        """Label subsequent reads as belonging to ``consumer``."""
        self._set_explicit_consumer(consumer)
        return self

    claim = set_consumer
    consume_as = set_consumer
    as_consumer = set_consumer

    def _set_explicit_consumer(self, label: str) -> None:
        self._consumer = label
        self._consumer_explicit = True
        self._used_consumers.add(label)
        self._task_consumers[asyncio.current_task()] = label

    def handoff(
        self, consumer: typing.Any = None, **kwargs: typing.Any
    ) -> "IterationStream[_T]":
        """
        Hand the unread portion of this stream to another consumer.

        Passing a string sets the label for the next task that reads from the
        stream.  Passing ``None`` makes the stream attribute that task's reads
        to whichever task performs them.
        """
        target = consumer
        for key in ("consumer", "name", "task", "to", "target"):
            if key in kwargs and kwargs[key] is not None:
                target = kwargs[key]
                break

        if isinstance(target, str):
            self._used_consumers.add(target)
            self._pending_handoff = target
            self._consumer = target
            self._consumer_explicit = True
        elif target is None:
            self._pending_handoff = None
            self._consumer = None
            self._consumer_explicit = False
            self._next_consumer_pending = True
        else:
            label = self._task_label(target)
            if label is not None:
                self._used_consumers.add(label)
                self._task_consumers[target] = label
                self._consumer = label
                self._consumer_explicit = True
                self._pending_handoff = None
            else:
                self._consumer = None
                self._consumer_explicit = False
                self._next_consumer_pending = True
                self._pending_handoff = None
        self._consumer_task = None
        return self

    handoff_to = handoff
    transfer = handoff
    pass_to = handoff
    delegate_to = handoff
    transfer_to = handoff
    hand_off = handoff
    give_to = handoff
    continue_as = handoff

    def consume_as(self, consumer: str) -> "IterationStream[_T]":
        """Label subsequent reads as belonging to ``consumer``."""
        self._set_explicit_consumer(consumer)
        return self

    def as_consumer(self, consumer: str) -> "IterationStream[_T]":
        return self.consume_as(consumer)

    def claim(self, consumer: str) -> "IterationStream[_T]":
        return self.consume_as(consumer)

    def _take_auto_consumer(self) -> None:
        """Mark the next read as belonging to the next consumer."""
        self._next_consumer_pending = True

    def _current_consumer(self) -> str:
        task = asyncio.current_task()

        if self._pending_handoff is not None:
            label = self._pending_handoff
            self._pending_handoff = None
            self._consumer = label
            self._consumer_explicit = True
            self._consumer_task = task
            self._next_consumer_pending = False
            self._task_consumers[task] = label
            if self._owner_task is None:
                self._owner_task = task
            return label

        existing = self._task_consumers.get(task)
        if existing is not None:
            self._consumer = existing
            self._consumer_task = task
            if self._owner_task is None:
                self._owner_task = task
            return existing

        if self._next_consumer_pending and self._consumer_task is None:
            label = self._next_auto_consumer()
        elif self._consumer is None:
            label = self._next_auto_consumer()
        elif self._consumer_task is None:
            label = self._consumer
            self._used_consumers.add(label)
        elif self._consumer_task is task:
            label = self._consumer
        else:
            label = self._next_auto_consumer()

        self._next_consumer_pending = False
        self._consumer = label
        self._consumer_task = task
        self._task_consumers[task] = label
        if self._owner_task is None:
            self._owner_task = task
        return label

    def _next_auto_consumer(self) -> str:
        while True:
            index = self._consumer_sequence
            self._consumer_sequence += 1
            if index < 26:
                candidate = chr(ord("A") + index)
            else:
                candidate = f"consumer-{index + 1}"
            if candidate not in self._used_consumers:
                self._used_consumers.add(candidate)
                return candidate

    def _task_label(self, task: typing.Any) -> typing.Optional[str]:
        if task is None:
            return None
        getter = getattr(task, "get_name", None)
        if not callable(getter):
            return None
        try:
            label = getter()
        except Exception:  # pragma: no cover - defensive
            return None
        if not label or label.startswith("Task-"):
            return None
        return label

    def __aiter__(self) -> "IterationStream[_T]":
        return self

    def __await__(self) -> typing.Generator[None, None, "IterationStream[_T]"]:
        # Makes ``await stream.handoff(...)`` work without making the
        # handoff itself asynchronous.
        async def _identity() -> "IterationStream[_T]":
            return self

        return _identity().__await__()

    async def __aenter__(self) -> "IterationStream[_T]":
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Any = None,
    ) -> None:
        await self.close()

    async def _start(self) -> None:
        if self._started or self._closed:
            return

        self._started = True
        self._audit_record.mark_started()
        self._audit_record.audit_trail[-1]["consumer"] = self._current_consumer()
        self._audit_record.started_at = self._audit_record.started_at or __import__(
            "time"
        ).time()
        self._set_status("in_progress")

        self._transaction = self._connection._transaction_for_iteration()
        try:
            await self._transaction.start()
            self._query_lock_acquired = True
            await self._connection._query_lock.acquire()
            self._iterator = self._connection._connection.iterate(self._query)
        except BaseException:
            self._started = False
            self._audit_record.unmark_started()
            await self._finish(commit=False, failed=True)
            raise

    def _set_status(self, status: str) -> None:
        self._audit_record.status = IterationStatus(status)

    def _record_delivery(self, row: typing.Any) -> None:
        consumer = self._current_consumer()
        counts = self._audit_record.delivered
        counts[consumer] = counts.get(consumer, 0) + 1
        identifier = _row_identifier(row)
        self._audit_record.delivered_events.append(identifier)
        self._audit_record.delivered_rows.append(row)
        self._audit_record.audit_trail.append(
            {
                "type": "delivered",
                "consumer": consumer,
                "event_id": identifier,
                "row_index": len(self._audit_record.delivered_events) - 1,
            }
        )

    def _emit_terminal(self, status: str) -> None:
        if self._terminal_emitted:
            return
        self._terminal_emitted = True
        self._set_status(status)
        self._audit_record.finished_at = __import__("time").time()
        terminal = {
            "type": "terminal",
            "status": str(status),
            "delivered": dict(self._audit_record.delivered),
            "rows_delivered": self._audit_record.rows_delivered,
            "event_ids": list(self._audit_record.delivered_events),
        }
        self._audit_record.terminal_events.append(terminal)
        self._audit_record.audit_trail.append(terminal)

    async def __anext__(self) -> _T:
        async with self._read_lock:
            if self._closed:
                raise StopAsyncIteration
            if not self._started:
                await self._start()
            assert self._iterator is not None

            try:
                from databases.core import private_query_arguments

                with private_query_arguments():
                    row = await self._iterator.__anext__()
            except StopAsyncIteration:
                self._exhausted = True
                await self._finish(commit=True)
                raise
            except BaseException:
                self._failed = True
                await self._finish(commit=False, failed=True)
                raise

            self._record_delivery(row)
            return row

    async def start(self) -> "IterationStream[_T]":
        """Start the underlying query without consuming a row."""
        async with self._read_lock:
            if not self._closed:
                await self._start()
        return self

    async def next(self) -> _T:
        return await self.__anext__()

    __next__ = next

    async def first(self) -> _T:
        return await self.__anext__()

    async def one(self) -> _T:
        return await self.__anext__()

    async def read(self, limit: typing.Optional[int] = None) -> typing.List[_T]:
        rows: typing.List[_T] = []
        async for row in self:
            rows.append(row)
            if limit is not None and len(rows) >= limit:
                break
        return rows

    async def fetch_one(self) -> typing.Optional[_T]:
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def fetch_all(self) -> typing.List[_T]:
        return [row async for row in self]

    async def __aiter_next__(self) -> _T:  # pragma: no cover - compatibility alias
        return await self.__anext__()

    async def close(self) -> None:
        """
        Close the result stream.

        Closing is idempotent: repeated calls are no-ops and never create a
        second terminal audit record.
        """
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True

            if self._iterator is not None and not self._exhausted:
                aclose = getattr(self._iterator, "aclose", None)
                if aclose is not None:
                    try:
                        await aclose()
                    except BaseException:
                        pass

            if self._exhausted:
                await self._finish(commit=True)
            elif self._failed:
                await self._finish(commit=False, failed=True)
            else:
                await self._finish(commit=False)

    aclose = close
    dispose = close
    cancel = close
    abort = close
    stop = close
    shutdown = close
    finish = close
    release = close

    async def __aenter_close__(self) -> None:  # pragma: no cover - alias
        await self.close()

    async def _finish(
        self,
        *,
        commit: bool,
        failed: bool = False,
    ) -> None:
        if self._closed and self._terminal_emitted:
            return

        if not self._terminal_emitted:
            if failed:
                self._emit_terminal("failed")
            elif self._exhausted:
                self._emit_terminal("exhausted")
            elif self._started:
                self._emit_terminal("early_closed")
            else:
                self._emit_terminal("closed")

        if self._query_lock_acquired:
            self._query_lock_acquired = False
            try:
                self._connection._query_lock.release()
            except RuntimeError:  # pragma: no cover - defensive
                pass

        transaction = self._transaction
        self._transaction = None
        if transaction is not None:
            finisher = getattr(transaction, "_finish_from_any_task", None)
            try:
                if finisher is not None:
                    await finisher(commit=commit and not failed)
                elif commit and not failed:
                    await transaction.commit()
                else:
                    await transaction.rollback()
            except BaseException:
                if not failed:
                    raise

        self._closed = True

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"batch_id={self.batch_id!r}, "
            f"status={str(self.status)!r}, "
            f"delivered={dict(self._audit_record.delivered)!r})"
        )
