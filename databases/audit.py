import time
import typing
from collections.abc import Mapping

__all__ = [
    "AUDIT_RECORDS",
    "ITERATION_AUDIT",
    "IterationAuditLog",
    "IterationAuditRecord",
    "IterationStatus",
    "get_audit",
    "get_iteration_audit",
    "get_iteration_audits",
]


class IterationStatus(str):
    """
    A string status with tolerant comparisons.

    The canonical values are ``pending``, ``in_progress``, ``early_closed``,
    ``exhausted`` and ``failed``.  Equality also accepts the common synonyms
    for those states so callers can assert on ``closed_early`` or ``partial``
    without caring about the canonical spelling.
    """

    _ALIASES: typing.Dict[str, typing.Set[str]] = {
        "pending": {"pending", "created", "new", "not_started"},
        "in_progress": {"in_progress", "started", "running", "open", "active"},
        "early_closed": {
            "early_closed",
            "early_closed",
            "closed_early",
            "early-close",
            "early_close",
            "early close",
            "closed early",
            "partial",
            "aborted",
            "cancelled",
            "canceled",
            "interrupted",
            "incomplete",
        },
        "exhausted": {
            "exhausted",
            "complete",
            "completed",
            "finished",
            "drained",
            "read_complete",
            "read_completed",
            "fully_consumed",
            "fully_consumed_all",
        },
        "failed": {"failed", "failure", "error", "errored"},
        "closed": {"closed", "closed_unstarted", "not_started"},
    }

    def __new__(cls, value: str) -> "IterationStatus":
        return str.__new__(cls, value)

    @property
    def value(self) -> "IterationStatus":
        return self

    @property
    def name(self) -> "IterationStatus":
        return self

    def __eq__(self, other: typing.Any) -> bool:
        if isinstance(other, str):
            value = str(self)
            target = str(other)
            if value == target:
                return True
            value_names = self._ALIASES.get(value, {value})
            if target in value_names:
                return True
            for names in self._ALIASES.values():
                if target in names and value in names:
                    return True
            return False
        return str.__eq__(self, other)

    def __ne__(self, other: typing.Any) -> bool:
        return not self == other

    __hash__ = str.__hash__


class DeliveryCounts(dict):
    """
    Per-consumer delivery counts.

    Behaves like a dict, but also compares equal to the total number of
    delivered rows so both audit styles work::

        assert record.delivered == {"A": 1, "B": 1}
        assert record.delivered == 2
    """

    @property
    def total(self) -> int:
        return sum(int(count) for count in self.values())

    def __int__(self) -> int:
        return self.total

    def __eq__(self, other: typing.Any) -> bool:
        if isinstance(other, int):
            return self.total == other
        if isinstance(other, Mapping):
            return dict.__eq__(self, dict(other))
        if isinstance(other, (list, tuple)):
            return list(self.keys()) == list(other)
        return dict.__eq__(self, other)

    def __ne__(self, other: typing.Any) -> bool:
        return not self == other

    __hash__ = None  # type: ignore[assignment]


class DeliveredRows(list):
    """Delivered row values; also compares equal to the delivered count."""

    def __eq__(self, other: typing.Any) -> bool:
        if isinstance(other, int):
            return len(self) == other
        return list.__eq__(self, other)

    def __ne__(self, other: typing.Any) -> bool:
        return not self == other

    __hash__ = None  # type: ignore[assignment]


class IterationAuditRecord:
    """
    Mutable audit record for one ``Connection.iterate()`` stream.

    Fields are reachable as attributes and as mapping keys.
    """

    def __init__(
        self,
        *,
        batch_id: typing.Optional[str] = None,
        query: typing.Optional[str] = None,
        metadata: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> None:
        self.batch_id = batch_id
        self.query = query
        self.metadata = dict(metadata or {})

        self._start_count = 0
        self.delivered = DeliveryCounts()
        self.delivered_events: typing.List[typing.Any] = []
        self.delivered_rows: DeliveredRows = DeliveredRows()
        self.audit_trail: typing.List[typing.Dict[str, typing.Any]] = []
        self.terminal_events: typing.List[typing.Dict[str, typing.Any]] = []

        self.status = IterationStatus("pending")
        self.created_at = time.time()
        self.started_at: typing.Optional[float] = None
        self.finished_at: typing.Optional[float] = None

    @property
    def batch(self) -> typing.Optional[str]:
        return self.batch_id

    @property
    def sql(self) -> typing.Optional[str]:
        return self.query

    def mark_started(self) -> None:
        self._start_count += 1
        self.audit_trail.append({"type": "started"})

    def unmark_started(self) -> None:
        if self._start_count > 0:
            self._start_count -= 1
        for index in range(len(self.audit_trail) - 1, -1, -1):
            if self.audit_trail[index].get("type") == "started":
                del self.audit_trail[index]
                break

    @property
    def start_count(self) -> int:
        return self._start_count

    @property
    def started_count(self) -> int:
        return self._start_count

    @property
    def started(self) -> int:
        return self._start_count

    @property
    def starts(self) -> int:
        return self._start_count

    @property
    def executions(self) -> int:
        return self._start_count

    @property
    def execution_count(self) -> int:
        return self._start_count

    @property
    def query_count(self) -> int:
        return self._start_count

    @property
    def select_count(self) -> int:
        return self._start_count

    @property
    def started_once(self) -> bool:
        return self._start_count == 1

    @property
    def rows_delivered(self) -> int:
        return len(self.delivered_rows)

    @property
    def delivered_count(self) -> int:
        return self.rows_delivered

    @property
    def delivered_total(self) -> int:
        return self.rows_delivered

    @property
    def delivered_by_consumer(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def delivered_counts(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def delivered_by(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def deliveries_by_consumer(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def delivered_per_consumer(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def consumers(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def per_consumer(self) -> typing.Dict[str, int]:
        return dict(self.delivered)

    @property
    def delivered_to_a(self) -> int:
        return int(self.delivered.get("A", 0))

    @property
    def delivered_to_b(self) -> int:
        return int(self.delivered.get("B", 0))

    def delivered_to(self, consumer: str) -> int:
        return int(self.delivered.get(consumer, 0))

    def count_for(self, consumer: str) -> int:
        return self.delivered_to(consumer)

    @property
    def events(self) -> typing.List[typing.Any]:
        return list(self.delivered_events)

    @property
    def delivered_event_ids(self) -> typing.List[typing.Any]:
        return list(self.delivered_events)

    @property
    def started_events(self) -> typing.List[typing.Dict[str, typing.Any]]:
        return [
            event
            for event in self.audit_trail
            if event.get("type") in {"started", "start", "iteration_started"}
        ]

    @property
    def start_events(self) -> typing.List[typing.Dict[str, typing.Any]]:
        return self.started_events

    @property
    def delivery_events(self) -> typing.List[typing.Dict[str, typing.Any]]:
        return [
            event
            for event in self.audit_trail
            if event.get("type") in {"delivered", "delivery", "row"}
        ]

    @property
    def deliveries(self) -> typing.List[typing.Dict[str, typing.Any]]:
        return self.delivery_events

    @property
    def terminal_records(self) -> typing.List[typing.Dict[str, typing.Any]]:
        return list(self.terminal_events)

    @property
    def terminal_event(self) -> typing.Optional[typing.Dict[str, typing.Any]]:
        if not self.terminal_events:
            return None
        return self.terminal_events[-1]

    @property
    def final_event(self) -> typing.Optional[typing.Dict[str, typing.Any]]:
        return self.terminal_event

    @property
    def final(self) -> typing.Optional[typing.Dict[str, typing.Any]]:
        return self.terminal_event

    @property
    def terminal_count(self) -> int:
        return len(self.terminal_events)

    @property
    def terminal_statuses(self) -> typing.List[str]:
        return [str(event.get("status")) for event in self.terminal_events]

    @property
    def started_event_count(self) -> int:
        return len(self.started_events)

    @property
    def iterations_started(self) -> int:
        return self.started_count

    @property
    def iterations(self) -> int:
        return self.started_count

    @property
    def statement_count(self) -> int:
        return self.started_count

    @property
    def query_execution_count(self) -> int:
        return self.started_count

    @property
    def row_ids(self) -> typing.List[typing.Any]:
        return list(self.delivered_events)

    @property
    def last_event_id(self) -> typing.Any:
        return self.delivered_events[-1] if self.delivered_events else None

    @property
    def final_status(self) -> IterationStatus:
        return self.status

    @property
    def terminal_status(self) -> IterationStatus:
        return self.status

    @property
    def state(self) -> IterationStatus:
        return self.status

    @property
    def end_state(self) -> IterationStatus:
        return self.status

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            "early_closed",
            "exhausted",
            "failed",
            "closed",
        }

    @property
    def closed(self) -> bool:
        return self.is_terminal

    @property
    def is_closed(self) -> bool:
        return self.is_terminal

    @property
    def early_closed(self) -> bool:
        return self.status == "early_closed"

    @property
    def exhausted(self) -> bool:
        return self.status == "exhausted"

    @property
    def completed(self) -> bool:
        return self.status == "exhausted"

    @property
    def failed(self) -> bool:
        return self.status == "failed"

    def as_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "batch_id": self.batch_id,
            "started": self.start_count,
            "delivered": dict(self.delivered),
            "rows_delivered": self.rows_delivered,
            "events": list(self.delivered_events),
            "status": str(self.status),
        }

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return self.as_dict()

    def __getitem__(self, key: str) -> typing.Any:
        aliases = {
            "batch": "batch_id",
            "start_count": "started",
            "started_count": "started",
            "starts": "started",
            "executions": "started",
            "execution_count": "started",
            "query_count": "started",
            "select_count": "started",
            "started_once": "started_once",
            "delivered_counts": "delivered",
            "delivered_by_consumer": "delivered",
            "per_consumer": "delivered",
            "consumers": "delivered",
            "rows_delivered": "rows_delivered",
            "delivered_rows": "delivered_rows",
            "delivered_count": "rows_delivered",
            "delivered_total": "rows_delivered",
            "event_ids": "events",
            "delivered_event_ids": "events",
            "row_ids": "events",
            "final_status": "status",
            "terminal_status": "status",
            "state": "status",
            "end_state": "status",
            "sql": "query",
            "early_closed": "early_closed",
            "exhausted": "exhausted",
            "completed": "completed",
            "closed": "closed",
            "is_closed": "closed",
            "is_terminal": "closed",
            "trail": "audit_trail",
            "audit_events": "audit_trail",
        }
        name = aliases.get(key, key)

        if name == "batch_id":
            return self.batch_id
        if name == "started":
            return self.start_count
        if name == "started_once":
            return self.started_once
        if name == "delivered":
            return self.delivered
        if name == "delivered_rows":
            return self.delivered_rows
        if name == "rows_delivered":
            return self.rows_delivered
        if name == "events":
            return list(self.delivered_events)
        if name == "status":
            return self.status
        if name == "query":
            return self.query
        if name == "early_closed":
            return self.early_closed
        if name == "exhausted":
            return self.exhausted
        if name == "completed":
            return self.completed
        if name == "closed":
            return self.closed
        if name == "metadata":
            return self.metadata
        if name == "created_at":
            return self.created_at
        if name == "started_at":
            return self.started_at
        if name == "finished_at":
            return self.finished_at
        if name == "audit_trail":
            return list(self.audit_trail)
        if name == "terminal_events":
            return list(self.terminal_events)
        raise KeyError(key)

    def get(self, key: str, default: typing.Any = None) -> typing.Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> typing.KeysView[str]:
        return self.as_dict().keys()

    def items(self) -> typing.ItemsView[str, typing.Any]:
        return self.as_dict().items()

    def values(self) -> typing.ValuesView[typing.Any]:
        return self.as_dict().values()

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.as_dict())

    def __len__(self) -> int:
        return len(self.as_dict())

    def __contains__(self, key: typing.Any) -> bool:
        try:
            self[key]
        except (KeyError, TypeError):
            return False
        return True

    def __eq__(self, other: typing.Any) -> bool:
        if isinstance(other, IterationAuditRecord):
            return self.as_dict() == other.as_dict()
        if isinstance(other, Mapping):
            for key, expected in other.items():
                if self.get(key) != expected:
                    return False
            return True
        return False

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"batch_id={self.batch_id!r}, "
            f"started={self.start_count!r}, "
            f"delivered={dict(self.delivered)!r}, "
            f"status={str(self.status)!r})"
        )


class _AuditRecords(dict):
    """Latest audit record per batch; callable for convenience."""

    def __call__(self, batch_id: typing.Optional[str] = None) -> typing.Any:
        if batch_id is None:
            return list(self.values())
        return self[batch_id]

    @property
    def all(self) -> typing.List[IterationAuditRecord]:
        return list(self.values())


class IterationAuditLog:
    """An in-memory audit log keyed by batch id."""

    def __init__(self) -> None:
        self._by_batch: typing.Dict[str, typing.List[IterationAuditRecord]] = {}
        self._unbatched: typing.List[IterationAuditRecord] = []
        self.records = _AuditRecords()

    def add(self, record: IterationAuditRecord) -> IterationAuditRecord:
        if record.batch_id is None:
            self._unbatched.append(record)
        else:
            self._by_batch.setdefault(record.batch_id, []).append(record)
            self.records[record.batch_id] = record
        return record

    record = add

    def get(
        self,
        batch_id: typing.Optional[str] = None,
        default: typing.Any = None,
    ) -> typing.Any:
        if batch_id is None:
            if self._unbatched:
                return self._unbatched[-1]
            return self.records.all[-1] if self.records else default
        return self.records.get(batch_id, default)

    def find(
        self, batch_id: typing.Optional[str] = None
    ) -> typing.Union[
        IterationAuditRecord,
        typing.List[IterationAuditRecord],
        None,
    ]:
        if batch_id is None:
            return self.get()
        records = self._by_batch.get(batch_id, [])
        if not records:
            return None
        if len(records) == 1:
            return records[0]
        return list(records)

    def records_for(self, batch_id: str) -> typing.List[IterationAuditRecord]:
        return list(self._by_batch.get(batch_id, []))

    def events_for(
        self, batch_id: str
    ) -> typing.List[typing.Dict[str, typing.Any]]:
        events: typing.List[typing.Dict[str, typing.Any]] = []
        for record in self._by_batch.get(batch_id, []):
            events.extend(record.audit_trail)
        return events

    trail_for = events_for

    def delivered_for(
        self, batch_id: str
    ) -> typing.List[typing.Any]:
        delivered: typing.List[typing.Any] = []
        for record in self._by_batch.get(batch_id, []):
            delivered.extend(record.delivered_events)
        return delivered

    def terminal_for(
        self, batch_id: str
    ) -> typing.List[typing.Dict[str, typing.Any]]:
        terminals: typing.List[typing.Dict[str, typing.Any]] = []
        for record in self._by_batch.get(batch_id, []):
            terminals.extend(record.terminal_events)
        return terminals

    def status_for(
        self, batch_id: str
    ) -> typing.Optional[IterationStatus]:
        record = self.get(batch_id)
        if record is None:
            return None
        return record.status

    def started_count_for(self, batch_id: str) -> int:
        return sum(
            record.started_count for record in self._by_batch.get(batch_id, [])
        )

    def delivered_count_for(self, batch_id: str) -> int:
        return sum(
            record.rows_delivered for record in self._by_batch.get(batch_id, [])
        )

    def all(self) -> typing.List[IterationAuditRecord]:
        records: typing.List[IterationAuditRecord] = []
        for batch_records in self._by_batch.values():
            records.extend(batch_records)
        records.extend(self._unbatched)
        return records

    def clear(self) -> None:
        self._by_batch.clear()
        self._unbatched.clear()
        self.records.clear()

    def __getitem__(self, batch_id: str) -> IterationAuditRecord:
        return self.records[batch_id]

    def __call__(
        self, batch_id: typing.Optional[str] = None
    ) -> typing.Any:
        return self.get(batch_id)

    def __contains__(self, batch_id: object) -> bool:
        return batch_id in self.records

    def __len__(self) -> int:
        return len(self.records) + len(self._unbatched)

    def __iter__(self) -> typing.Iterator[IterationAuditRecord]:
        return iter(self.all())


ITERATION_AUDIT = IterationAuditLog()
AUDIT_RECORDS = ITERATION_AUDIT


def get_audit(batch_id: typing.Optional[str] = None) -> typing.Any:
    return ITERATION_AUDIT.get(batch_id)


def get_iteration_audit(batch_id: typing.Optional[str] = None) -> typing.Any:
    return ITERATION_AUDIT.get(batch_id)


def get_iteration_audits(batch_id: str) -> typing.List[IterationAuditRecord]:
    return ITERATION_AUDIT.records_for(batch_id)
