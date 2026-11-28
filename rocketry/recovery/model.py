"""Data model for partition aware Retry recovery.

The model is intentionally small and serializable.  A recovery record keeps
the current declaration batch of every partition, the receipts seen for each
run and the position up to which the task log has been consumed.  It can be
stored in memory or written to disk and loaded by a new scheduler process.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
import time


# Actions ---------------------------------------------------------------

START_ACTIONS = {"run", "start", "started", "begin", "start_receipt"}
SUCCESS_ACTIONS = {"success", "succeed", "succeeded", "ok", "success_receipt"}
FAIL_ACTIONS = {"fail", "failed", "failure", "error", "fail_receipt"}
TERMINATE_ACTIONS = {"terminate", "terminated", "termination"}
INACTION_ACTIONS = {"inaction", "inact", "inaction_receipt"}
CRASH_ACTIONS = {"crash", "crashed", "crash_receipt"}
END_ACTIONS = SUCCESS_ACTIONS | FAIL_ACTIONS | TERMINATE_ACTIONS | INACTION_ACTIONS | CRASH_ACTIONS


class RunStatus(str):
    """Status string with a few accepted synonyms.

    The canonical value is the plain status (``"success"`` for a run that was
    backfilled successfully) while comparisons also accept the more explicit
    ``"backfill_success"`` / ``"backfilled"`` spellings.  This keeps the API
    forgiving for callers that describe the state differently.
    """

    _ALIASES = {
        "pending": {"pending", "unconfirmed", "pending_confirmation", "待确认"},
        "failed": {"failed", "fail", "failure", "失败"},
        "success": {"success", "succeeded", "ok", "backfill_success", "backfilled",
                    "backfill_succeeded", "recovered", "补采成功", "已补采"},
        "backfill_pending": {"backfill_pending", "pending_backfill", "backfilling",
                             "backfill_in_progress", "补采中", "补采待确认"},
    }

    def __new__(cls, value: str, explicit: str = None):
        obj = super().__new__(cls, value)
        obj.explicit = explicit or value
        return obj

    @property
    def aliases(self):
        return self._ALIASES.get(str.__str__(self), {str.__str__(self)})

    def matches(self, other: str) -> bool:
        canonical = str.__str__(self)
        return canonical == other or other in self.aliases

    def __eq__(self, other):
        if isinstance(other, str):
            return self.matches(str(other))
        return str.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return str.__hash__(self)

    def __repr__(self):
        return repr(self.explicit)

    def __str__(self):
        return self.explicit


def _status(value: str, explicit: str = None) -> RunStatus:
    return RunStatus(value, explicit=explicit)


def _get(obj: Any, *names: str, default: Any = None) -> Any:
    """Read a value from a mapping or an object using aliases."""
    if obj is None:
        return default
    for name in names:
        if isinstance(obj, dict):
            if name in obj:
                return obj[name]
        elif hasattr(obj, name):
            return getattr(obj, name)
    return default


def normalize_action(value: Any) -> Optional[str]:
    """Normalize the different spellings of a receipt action."""
    if value is None:
        return None
    value = str(value).lower()
    if value in START_ACTIONS:
        return "start"
    if value in SUCCESS_ACTIONS:
        return "success"
    if value in FAIL_ACTIONS:
        return "fail"
    if value in TERMINATE_ACTIONS:
        return "terminate"
    if value in INACTION_ACTIONS:
        return "inaction"
    if value in CRASH_ACTIONS:
        return "crash"
    return value


@dataclass(init=False)
class Receipt:
    """A single receipt from the source system.

    Parameters
    ----------
    partition : str
        Partition (for example ``"north"`` or ``"south"``).
    batch : str
        Declaration batch the receipt belongs to.
    run_id : str
        Identifier of the run the receipt is about.
    source_seq : int, optional
        Monotonically increasing sequence number assigned by the source.
        It is used to ignore duplicate deliveries and to advance the log
        cursor.  ``event_seq`` / ``source_event_seq`` are accepted aliases.
    action : str
        ``start``, ``success``, ``fail``, ``terminate``, ``inaction`` or
        ``crash``.  Common alternatives (``run``, ``failed`` etc.) are
        normalized.
    """

    partition: str
    batch: str
    run_id: str
    action: str = "start"
    source_seq: Optional[int] = None
    created: Optional[float] = None
    log_position: Any = None
    receipt_id: Optional[str] = None
    original_run_id: Optional[str] = None
    transaction_id: Optional[str] = None
    payload: Any = None
    source: Optional[str] = None

    def __init__(
        self,
        partition: str = None,
        batch: str = None,
        run_id: str = None,
        action: str = "start",
        source_seq: Optional[int] = None,
        created: Optional[float] = None,
        log_position: Any = None,
        receipt_id: Optional[str] = None,
        original_run_id: Optional[str] = None,
        transaction_id: Optional[str] = None,
        payload: Any = None,
        source: Optional[str] = None,
        **kwargs,
    ):
        # Aliases accepted by convenience for callers using the source
        # system's terminology.
        partition = partition if partition is not None else kwargs.pop("partition_id", None)
        partition = partition if partition is not None else kwargs.pop("zone", None)
        batch = batch if batch is not None else kwargs.pop("batch_id", None)
        run_id = run_id if run_id is not None else kwargs.pop("run", None)
        run_id = run_id if run_id is not None else kwargs.pop("run_identifier", None)
        action = action if action is not None else kwargs.pop("status", None)
        action = action if action is not None else kwargs.pop("kind", None)
        action = action if action is not None else kwargs.pop("type", None)
        if source_seq is None:
            source_seq = kwargs.pop("source_event_seq", None)
        if source_seq is None:
            source_seq = kwargs.pop("event_seq", None)
        if source_seq is None:
            source_seq = kwargs.pop("seq", None)
        if log_position is None:
            log_position = kwargs.pop("position", None)
        if log_position is None:
            log_position = kwargs.pop("offset", None)
        if log_position is None:
            log_position = kwargs.pop("cursor", None)
        if transaction_id is None:
            transaction_id = kwargs.pop("business_id", None)
        if transaction_id is None:
            transaction_id = kwargs.pop("flow_id", None)
        if original_run_id is None:
            original_run_id = kwargs.pop("backfilled_run_id", None)
        if original_run_id is None:
            original_run_id = kwargs.pop("source_run_id", None)
        if created is None:
            created = kwargs.pop("timestamp", None)

        missing = [
            name for name, value in (
                ("partition", partition), ("batch", batch), ("run_id", run_id)
            ) if not value
        ]
        if missing:
            raise ValueError(f"Receipt is missing required field(s): {', '.join(missing)}")

        self.partition = str(partition)
        self.batch = str(batch)
        self.run_id = str(run_id)
        self.action = normalize_action(action) or "start"
        try:
            self.source_seq = int(source_seq) if source_seq is not None else None
        except (TypeError, ValueError):
            self.source_seq = source_seq
        self.created = created
        self.log_position = log_position
        self.receipt_id = receipt_id if receipt_id is not None else self._make_receipt_id()
        self.original_run_id = None if original_run_id is None else str(original_run_id)
        self.transaction_id = None if transaction_id is None else str(transaction_id)
        self.payload = payload
        self.source = source

    def _make_receipt_id(self) -> str:
        if self.source_seq is not None:
            return f"{self.partition}:{self.batch}:{self.run_id}:{self.action}:{self.source_seq}"
        if self.log_position is not None:
            return f"{self.partition}:{self.batch}:{self.run_id}:{self.action}:{self.log_position}"
        return f"{self.partition}:{self.batch}:{self.run_id}:{self.action}"

    @classmethod
    def from_any(cls, value: Any, **overrides) -> "Receipt":
        """Create a receipt from a mapping, another receipt or an object.

        This is deliberately permissive so receipts can be supplied as
        plain dictionaries, Rocketry log records or small application
        specific objects.
        """
        if isinstance(value, Receipt):
            data = value.to_dict()
        else:
            data = {
                "partition": _get(value, "partition", "partition_id", "region", "zone"),
                "batch": _get(value, "batch", "batch_id", "declaration_batch"),
                "run_id": _get(value, "run_id", "run", "run_identifier"),
                "action": _get(value, "action", "status", "kind", "type", "receipt_type"),
                "source_seq": _get(
                    value, "source_seq", "source_event_seq", "event_seq", "seq", "sequence"
                ),
                "created": _get(value, "created", "timestamp", "time"),
                "log_position": _get(value, "log_position", "position", "offset", "cursor"),
                "receipt_id": _get(value, "receipt_id", "id"),
                "original_run_id": _get(
                    value, "original_run_id", "backfilled_run_id", "source_run_id"
                ),
                "transaction_id": _get(
                    value, "transaction_id", "business_id", "business_transaction_id",
                    "idempotency_key", "flow_id",
                ),
                "payload": _get(value, "payload", "result", "data"),
                "source": _get(value, "source", "source_system"),
            }
            # Rocketry LogRecord objects expose extras as attributes but a
            # pydantic record can also be converted to a dict.
            if data["partition"] is None:
                try:
                    extra = value.dict()
                except AttributeError:
                    extra = None
                if isinstance(extra, dict):
                    data.update({
                        key: extra.get(key)
                        for key in data
                        if data[key] is None and key in extra
                    })
        data.update(overrides)
        missing = [key for key in ("partition", "batch", "run_id") if not data.get(key)]
        if missing:
            raise ValueError(f"Receipt is missing required field(s): {', '.join(missing)}")
        if not data.get("action"):
            data["action"] = "start"
        return cls(**data)

    # Friendly aliases ---------------------------------------------------
    @property
    def event_seq(self):
        return self.source_seq

    @property
    def source_event_seq(self):
        return self.source_seq

    @property
    def position(self):
        return self.log_position

    @property
    def kind(self):
        return self.action

    @property
    def run(self):
        return self.run_id

    # Field aliases ------------------------------------------------------
    @property
    def business_id(self):
        return self.transaction_id

    @property
    def flow_id(self):
        return self.transaction_id

    @property
    def idempotency_key(self):
        return self.transaction_id

    @property
    def event_id(self):
        return self.source_seq

    @property
    def seq(self):
        return self.source_seq

    @property
    def offset(self):
        return self.log_position

    @property
    def source_position(self):
        return self.log_position

    @property
    def partition_id(self):
        return self.partition

    @property
    def batch_id(self):
        return self.batch

    @property
    def status(self):
        return self.action

    @property
    def receipt_type(self):
        return self.action

    @property
    def key(self) -> Tuple[str, str, str]:
        return (self.partition, self.batch, self.run_id)

    @property
    def is_start(self) -> bool:
        return self.action == "start"

    @property
    def is_success(self) -> bool:
        return self.action == "success"

    @property
    def is_fail(self) -> bool:
        return self.action == "fail"

    @property
    def is_end(self) -> bool:
        return self.action in END_ACTIONS

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition": self.partition,
            "batch": self.batch,
            "run_id": self.run_id,
            "action": self.action,
            "source_seq": self.source_seq,
            "created": self.created,
            "log_position": self.log_position,
            "receipt_id": self.receipt_id,
            "original_run_id": self.original_run_id,
            "transaction_id": self.transaction_id,
            "payload": self.payload,
            "source": self.source,
        }


@dataclass
class RunState:
    """State of one run inside a partition."""

    partition: str
    batch: str
    run_id: str
    receipts: List[Receipt] = field(default_factory=list)
    start_receipt: Optional[Receipt] = None
    end_receipt: Optional[Receipt] = None
    backfill_transaction_id: Optional[str] = None
    backfill_status: Optional[str] = None
    backfill_receipt: Optional[Receipt] = None
    archived: bool = False
    first_source_seq: Optional[int] = None
    last_source_seq: Optional[int] = None

    @property
    def key(self) -> Tuple[str, str, str]:
        return (self.partition, self.batch, self.run_id)

    @property
    def status(self) -> RunStatus:
        """Canonical display status of the run.

        A run without an end receipt is ``pending`` (待确认) even when the
        start receipt is known.  A successfully backfilled run reports
        ``success`` (also comparable to ``backfill_success``).
        """
        if self.backfill_status == "completed":
            return _status("success", "backfill_success")
        if self.backfill_status in (
            "registered", "started", "inserted", "receipt_pending"
        ):
            return _status("backfill_pending")
        if self.backfill_receipt is not None:
            # The result is stored but its receipt is not confirmed in the
            # task log yet.
            return _status("backfill_pending")
        if self.end_receipt is None:
            return _status("pending")
        action = self.end_receipt.action
        if action == "success":
            return _status("success")
        if action == "fail":
            return _status("failed")
        return _status(action)

    # Common aliases used by consumers of the model.
    @property
    def state(self) -> str:
        return self.status

    @property
    def display_status(self) -> str:
        return self.status

    @property
    def outcome(self) -> Optional[str]:
        if self.backfill_status == "completed":
            return "success"
        if self.end_receipt is not None:
            return "success" if self.end_receipt.action == "success" else self.end_receipt.action
        return None

    @property
    def is_pending(self) -> bool:
        return self.end_receipt is None and self.backfill_status not in (
            "registered", "started", "inserted", "receipt_pending"
        )

    @property
    def is_unconfirmed(self) -> bool:
        "Whether the run has no end receipt."
        return self.end_receipt is None

    @property
    def confirmed(self) -> bool:
        return self.end_receipt is not None

    @property
    def status_label(self) -> str:
        "Human friendly label. Pending runs are shown as 'pending (待确认)'."
        labels = {
            "pending": "pending (待确认)",
            "failed": "failed",
            "success": "success",
            "backfill_pending": "backfill pending",
            "backfill_success": "backfill success",
        }
        return labels.get(self.status, self.status)

    @property
    def is_confirmed_failed(self) -> bool:
        return self.end_receipt is not None and self.end_receipt.action == "fail"

    @property
    def is_success(self) -> bool:
        return self.status in ("success", "backfill_success")

    @property
    def is_backfilled(self) -> bool:
        return self.status == "backfill_success"

    def add_receipt(self, receipt: Receipt) -> bool:
        """Add a receipt. Return whether it was new.

        Receipts are idempotent by ``receipt_id``.  Delayed start receipts
        are accepted after a success/fail receipt and never downgrade the
        already known terminal state.  A backfill success receipt is a
        separate result: it completes the backfill but does not overwrite the
        original failure receipt kept for auditing.
        """
        if any(existing.receipt_id == receipt.receipt_id for existing in self.receipts):
            return False
        if receipt.source == "backfill" and receipt.is_success:
            self.receipts.append(receipt)
            if receipt.source_seq is not None:
                if self.first_source_seq is None or receipt.source_seq < self.first_source_seq:
                    self.first_source_seq = receipt.source_seq
                if self.last_source_seq is None or receipt.source_seq > self.last_source_seq:
                    self.last_source_seq = receipt.source_seq
            self.backfill_receipt = receipt
            if receipt.transaction_id is not None:
                self.backfill_transaction_id = receipt.transaction_id
            self.backfill_status = "completed"
            return True
        # A repeated terminal receipt with a different source sequence is
        # still the same logical delivery; only keep the first terminal one.
        if receipt.is_end and self.end_receipt is not None:
            same_action = receipt.action == self.end_receipt.action
            if same_action:
                return False
            # Do not let an older event overwrite a newer terminal state.
            if (
                receipt.source_seq is not None
                and self.end_receipt.source_seq is not None
                and receipt.source_seq < self.end_receipt.source_seq
            ):
                return False

        self.receipts.append(receipt)
        if receipt.source_seq is not None:
            if self.first_source_seq is None or receipt.source_seq < self.first_source_seq:
                self.first_source_seq = receipt.source_seq
            if self.last_source_seq is None or receipt.source_seq > self.last_source_seq:
                self.last_source_seq = receipt.source_seq

        if receipt.is_start:
            if self.start_receipt is None:
                self.start_receipt = receipt
        elif receipt.is_end:
            self.end_receipt = receipt
        return True

    def mark_backfill(self, transaction_id: str, status: str = "registered") -> None:
        self.backfill_transaction_id = transaction_id
        self.backfill_status = status

    def to_dict(self) -> Dict[str, Any]:
        status = self.status
        return {
            "partition": self.partition,
            "batch": self.batch,
            "run_id": self.run_id,
            "status": getattr(status, "explicit", str(status)),
            "outcome": self.outcome,
            "receipts": [receipt.to_dict() for receipt in self.receipts],
            "start_receipt": None if self.start_receipt is None else self.start_receipt.to_dict(),
            "end_receipt": None if self.end_receipt is None else self.end_receipt.to_dict(),
            "backfill_transaction_id": self.backfill_transaction_id,
            "backfill_status": self.backfill_status,
            "backfill_receipt": None if self.backfill_receipt is None else self.backfill_receipt.to_dict(),
            "archived": self.archived,
            "first_source_seq": self.first_source_seq,
            "last_source_seq": self.last_source_seq,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunState":
        run = cls(
            partition=data["partition"],
            batch=data["batch"],
            run_id=data["run_id"],
            archived=data.get("archived", False),
            backfill_transaction_id=data.get("backfill_transaction_id"),
            backfill_status=data.get("backfill_status"),
            first_source_seq=data.get("first_source_seq"),
            last_source_seq=data.get("last_source_seq"),
        )
        for receipt_data in data.get("receipts", []):
            run.receipts.append(Receipt.from_any(receipt_data))
        if data.get("start_receipt"):
            run.start_receipt = Receipt.from_any(data["start_receipt"])
        if data.get("end_receipt"):
            run.end_receipt = Receipt.from_any(data["end_receipt"])
        if data.get("backfill_receipt"):
            run.backfill_receipt = Receipt.from_any(data["backfill_receipt"])
        return run


@dataclass
class PartitionState:
    """The bookkeeping of one partition."""

    name: str
    current_batch: Optional[str] = None
    runs: Dict[str, RunState] = field(default_factory=dict)
    history: List[RunState] = field(default_factory=list)

    def ensure_run(self, batch: str, run_id: str) -> RunState:
        run = self.runs.get(run_id)
        if run is None:
            run = RunState(partition=self.name, batch=batch, run_id=run_id)
            self.runs[run_id] = run
        return run

    def set_batch(self, batch: str) -> None:
        """Start a new declaration batch.

        Runs of the previous batch are archived.  They remain queryable in
        ``history`` but are no longer eligible for a backfill.
        """
        batch = str(batch)
        if self.current_batch == batch:
            return
        if self.current_batch is not None:
            for run in self.runs.values():
                if len(run.receipts) or run.backfill_transaction_id:
                    run.archived = True
                    self.history.append(run)
        self.runs = {}
        self.current_batch = batch
        self.current_batch = batch

    def add_receipt(self, receipt: Receipt) -> RunState:
        if self.current_batch is None:
            self.current_batch = receipt.batch

        if receipt.batch != self.current_batch:
            # Old batch: keep it for audit purposes only.  It must never
            # become an active run that can be backfilled.
            run = self._get_archived(receipt)
            run.archived = True
            run.add_receipt(receipt)
            return run

        run = self.ensure_run(receipt.batch, receipt.run_id)
        run.archived = False
        run.add_receipt(receipt)
        return run

    def _get_archived(self, receipt: Receipt) -> RunState:
        for run in self.history:
            if run.batch == receipt.batch and run.run_id == receipt.run_id:
                return run
        run = RunState(
            partition=self.name,
            batch=receipt.batch,
            run_id=receipt.run_id,
            archived=True,
        )
        self.history.append(run)
        return run

    @property
    def batch(self):
        return self.current_batch

    def get_run(self, run_id: str, include_history: bool = False) -> Optional[RunState]:
        run = self.runs.get(run_id)
        if run is not None:
            return run
        if include_history:
            for run in self.history:
                if run.run_id == run_id:
                    return run
        return None

    def current_runs(self) -> Iterable[RunState]:
        return self.runs.values()

    def pending_runs(self) -> List[RunState]:
        return [run for run in self.runs.values() if run.is_pending]

    def failed_runs(self) -> List[RunState]:
        return [
            run for run in self.runs.values()
            if run.is_confirmed_failed and run.backfill_receipt is None
        ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "current_batch": self.current_batch,
            "batch": self.current_batch,
            "runs": {run_id: run.to_dict() for run_id, run in self.runs.items()},
            "history": [run.to_dict() for run in self.history],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionState":
        state = cls(
            name=data.get("name") or data.get("partition"),
            current_batch=data.get("current_batch", data.get("batch")),
        )
        for run_id, run_data in data.get("runs", {}).items():
            state.runs[run_id] = RunState.from_dict(run_data)
        for run_data in data.get("history", []):
            state.history.append(RunState.from_dict(run_data))
        return state


@dataclass
class StreamCursor:
    """Position up to which a receipt stream has been consumed."""

    stream: str = "task_log"
    position: Any = None
    source_seq: Optional[int] = None
    file: Optional[str] = None
    updated: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stream": self.stream,
            "position": self.position,
            "source_seq": self.source_seq,
            "file": self.file,
            "updated": self.updated,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamCursor":
        return cls(
            stream=data.get("stream", "task_log"),
            position=data.get("position"),
            source_seq=data.get("source_seq"),
            file=data.get("file"),
            updated=data.get("updated"),
        )


@dataclass
class RecoveryRecord:
    """Persistent recovery record shared by scheduler sessions.

    The record stores the current batch of every partition, all receipts
    seen, and the task log cursor.  It is safe to serialize to JSON.
    """

    partitions: Dict[str, PartitionState] = field(default_factory=dict)
    cursors: Dict[str, StreamCursor] = field(default_factory=dict)
    version: int = 1

    def ensure_partition(self, name: str) -> PartitionState:
        name = str(name)
        if name not in self.partitions:
            self.partitions[name] = PartitionState(name=name)
        return self.partitions[name]

    def declare_batch(self, partition: str, batch: str) -> PartitionState:
        state = self.ensure_partition(partition)
        state.set_batch(batch)
        return state

    # Aliases often convenient in application code.
    set_batch = declare_batch

    def add_receipt(self, receipt: Any) -> RunState:
        receipt = Receipt.from_any(receipt)
        state = self.ensure_partition(receipt.partition)
        return state.add_receipt(receipt)

    def get_partition(self, partition: str) -> Optional[PartitionState]:
        return self.partitions.get(str(partition))

    def get_run(self, partition: str, run_id: str, include_history: bool = True) -> Optional[RunState]:
        state = self.get_partition(partition)
        if state is None:
            return None
        return state.get_run(run_id, include_history=include_history)

    def cursor(self, stream: str = "task_log") -> StreamCursor:
        if stream not in self.cursors:
            self.cursors[stream] = StreamCursor(stream=stream)
        return self.cursors[stream]

    def advance_cursor(
        self,
        stream: str = "task_log",
        position: Any = None,
        source_seq: Optional[int] = None,
        file: Optional[str] = None,
    ) -> StreamCursor:
        cursor = self.cursor(stream)
        if source_seq is not None:
            if cursor.source_seq is None or source_seq > cursor.source_seq:
                cursor.source_seq = int(source_seq)
        if position is not None:
            if cursor.position is None or _position_greater(position, cursor.position):
                cursor.position = position
        if file is not None:
            cursor.file = file
        cursor.updated = time.time()
        return cursor

    @property
    def log_positions(self) -> Dict[str, Any]:
        return {name: cursor.position for name, cursor in self.cursors.items()}

    def pending_runs(self) -> List[RunState]:
        return [
            run for state in self.partitions.values()
            for run in state.pending_runs()
        ]

    def failed_runs(self) -> List[RunState]:
        return [
            run for state in self.partitions.values()
            for run in state.failed_runs()
        ]

    def history(self, partition: Optional[str] = None) -> List[RunState]:
        if partition is not None:
            state = self.get_partition(partition)
            return [] if state is None else list(state.history)
        return [run for state in self.partitions.values() for run in state.history]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "partitions": {
                name: state.to_dict() for name, state in self.partitions.items()
            },
            "cursors": {
                name: cursor.to_dict() for name, cursor in self.cursors.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RecoveryRecord":
        record = cls(version=data.get("version", 1))
        partitions = data.get("partitions", {})
        for name, state_data in partitions.items():
            if isinstance(state_data, str):
                # Shorthand: {"north": "A"} declares the current batch.
                state_data = {"name": name, "current_batch": state_data}
            elif not isinstance(state_data, dict):
                state_data = {"name": name, "current_batch": getattr(state_data, "current_batch", None)}
            state_data.setdefault("name", name)
            record.partitions[name] = PartitionState.from_dict(state_data)
        for cursor_name, cursor_data in data.get("cursors", {}).items():
            if not isinstance(cursor_data, dict):
                cursor_data = {"stream": cursor_name, "position": cursor_data}
            record.cursors[cursor_name] = StreamCursor.from_dict(cursor_data)
        # A single task-log cursor may be stored under a top-level key.
        if "cursor" in data and "task_log" not in record.cursors:
            cursor = data["cursor"]
            if not isinstance(cursor, dict):
                cursor = {"position": cursor}
            record.cursors["task_log"] = StreamCursor.from_dict(cursor)
        if "position" in data and "task_log" not in record.cursors:
            record.cursors["task_log"] = StreamCursor(
                stream="task_log",
                position=data["position"],
                source_seq=data.get("source_seq"),
            )
        # Tolerate receipts listed at the top level.
        for receipt in data.get("receipts", []):
            record.add_receipt(receipt)
        return record


def _position_greater(new: Any, old: Any) -> bool:
    """Best effort comparison for opaque log positions."""
    try:
        return new > old
    except TypeError:
        # File/offset positions are compared by their offset if possible.
        new_offset = _get(new, "offset", "position", default=None)
        old_offset = _get(old, "offset", "position", default=None)
        if new_offset is not None and old_offset is not None:
            try:
                return new_offset > old_offset
            except TypeError:
                return False
        return False
