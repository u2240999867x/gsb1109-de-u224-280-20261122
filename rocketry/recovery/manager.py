"""Partition aware recovery orchestration.

The manager ties together three things:

* a :class:`~rocketry.recovery.model.RecoveryRecord` holding partition batches,
  receipts and the task log cursor,
* a :class:`~rocketry.recovery.ledger.BackfillLedger` giving idempotent
  backfill dispatch/insert semantics, and
* optional integration with a Rockery task logger so a successful backfill can
  be written back to the task log (and therefore be seen by normal Retry
  evaluations and by the scheduler after a restart).

It is deliberately usable without a running scheduler; this makes recovery
deterministic and testable.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from .ledger import BackfillEntry, BackfillLedger, default_transaction_id, _first
from .model import (
    Receipt,
    RecoveryRecord,
    RunState,
    _get,
    normalize_action,
)
from .store import BaseStateStore, FileStateStore, RecoveryStore


class BackfillNotAllowed(RuntimeError):
    """Raised when a run is not eligible for a backfill."""


class PartitionRecovery:
    """Recovery bookkeeping and idempotent partition backfills.

    Parameters
    ----------
    record : RecoveryRecord, optional
        Recovery record to continue from.
    ledger : BackfillLedger, optional
        Ledger of backfill transactions.
    store : BaseStateStore, optional
        Shared persistence backend.  If neither ``record`` nor ``ledger`` is
        given, both are stored under this backend.
    path : str, optional
        Convenience for a JSON backed ``store``.
    task : Task, optional
        Rocketry task whose logger receives backfill success receipts.
    receipt_sink : callable, optional
        Alternative way to persist a backfill success receipt.  It is called
        with the :class:`Receipt` and its return value is used as the receipt
        stored on the entry.
    executor : callable, optional
        Callable performing the actual backfill work.  Called with the
        :class:`BackfillEntry`.  If omitted, backfill work is considered to
        succeed as soon as it is inserted.
    session : Session, optional
        Rocketry session used for time and for resolving ``task``.
    stream : str
        Name of the receipt stream whose position is stored.  Defaults to
        ``"task_log"``.
    """

    def __init__(
        self,
        record: RecoveryRecord = None,
        ledger: BackfillLedger = None,
        store: BaseStateStore = None,
        path=None,
        task=None,
        receipt_sink: Callable = None,
        executor: Callable = None,
        session=None,
        stream: str = "task_log",
        partitions: Optional[Sequence[str]] = None,
    ):
        if store is None and path is not None:
            store = FileStateStore(path)
        self.store = store
        self.task = task
        self.session = session
        self.receipt_sink = receipt_sink
        self.executor = executor
        self.stream = stream
        self._batch_depth = 0

        if store is not None:
            self.record_store = RecoveryStore(store=store, record=record)
            self.ledger = ledger if ledger is not None else BackfillLedger(store=store)
        else:
            self.record_store = RecoveryStore(record=record)
            self.ledger = ledger if ledger is not None else BackfillLedger()

        if record is not None:
            self.record_store.record = record
        if partitions:
            for partition in partitions:
                self.record.ensure_partition(partition)

    # Persistence --------------------------------------------------------
    @property
    def record(self) -> RecoveryRecord:
        return self.record_store.record

    @record.setter
    def record(self, value: RecoveryRecord):
        self.record_store.record = value

    def load(self) -> "PartitionRecovery":
        """Load the recovery record and ledger from the backing store."""
        self.record_store.load()
        self._sync_from_ledger()
        return self

    def reload(self) -> "PartitionRecovery":
        return self.load()

    def _sync_from_ledger(self) -> None:
        """Reflect ledger transactions that were committed before a crash.

        The ledger is written first when a backfill is inserted.  If the
        process was killed between the ledger write and the recovery record
        write, this reconciles the run state from the ledger on load.
        """
        try:
            entries = self.ledger.entries()
        except Exception:
            return
        changed = False
        for entry in entries:
            state = self.record.get_partition(entry.partition)
            if state is None:
                state = self.record.ensure_partition(entry.partition)
                changed = True
            is_current = entry.batch == state.current_batch
            run = state.get_run(entry.run_id, include_history=True)
            if run is None:
                run = RunState(
                    partition=entry.partition,
                    batch=entry.batch,
                    run_id=entry.run_id,
                    archived=not is_current,
                )
                if is_current:
                    state.runs[entry.run_id] = run
                else:
                    state.history.append(run)
                changed = True
            if run.backfill_transaction_id != entry.transaction_id:
                run.backfill_transaction_id = entry.transaction_id
                changed = True
            if run.backfill_status != entry.status:
                run.backfill_status = entry.status
                changed = True
            if entry.success_receipt is not None and run.backfill_receipt is None:
                run.backfill_receipt = entry.success_receipt
                changed = True
        if changed:
            self.record_store.save()

    def save(self) -> RecoveryRecord:
        if self._batch_depth == 0:
            self.record_store.save()
        return self.record

    def save_record(self) -> RecoveryRecord:
        return self.save()

    def snapshot(self) -> Dict[str, Any]:
        """JSON serializable recovery record plus partition view."""
        return {
            "record": self.record.to_dict(),
            "partitions": self.view(),
            "cursor": self.record.cursor(self.stream).to_dict(),
        }

    def export(self) -> Dict[str, Any]:
        return self.snapshot()

    def dump(self) -> Dict[str, Any]:
        return self.snapshot()

    def to_json(self) -> str:
        import json
        return json.dumps(self.snapshot(), ensure_ascii=False, default=str)

    def save_to(self, path) -> None:
        """Write the recovery record to ``path`` (and keep it loaded)."""
        from .store import FileStateStore, RecoveryStore
        store = FileStateStore(path)
        RecoveryStore(store=store, record=self.record_store.record).save()

    @classmethod
    def from_record(cls, record: RecoveryRecord, **kwargs) -> "PartitionRecovery":
        return cls(record=record, **kwargs)

    @classmethod
    def load_from(cls, path, **kwargs) -> "PartitionRecovery":
        """Load a recovery from a JSON file."""
        from .store import FileStateStore
        return cls(store=FileStateStore(path), **kwargs)

    @property
    def partitions(self) -> Dict[str, Any]:
        return self.record.partitions

    # Partition and batch bookkeeping ------------------------------------
    def ensure_partition(self, partition: str):
        return self.record.ensure_partition(partition)

    add_partition = ensure_partition

    def declare_batch(self, partition: str, batch: str):
        state = self.record.declare_batch(partition, batch)
        self.save()
        return state

    set_batch = declare_batch
    declare = declare_batch
    start_batch = declare_batch

    def current_batch(self, partition: str) -> Optional[str]:
        state = self.record.get_partition(partition)
        return None if state is None else state.current_batch

    @property
    def batches(self) -> Dict[str, Optional[str]]:
        return {
            name: state.current_batch for name, state in self.record.partitions.items()
        }

    # Receipts and log consumption --------------------------------------
    def process_receipt(self, receipt: Any = None, *args, **kwargs) -> RunState:
        """Apply one receipt and persist the new state.

        Accepts a :class:`Receipt`, a mapping/object with receipt fields, or
        keyword arguments (``partition=``, ``batch=``, ``run_id=``,
        ``action=``, ``source_seq=`` ...).
        """
        if receipt is None:
            receipt = kwargs
            kwargs = {}
        elif kwargs:
            if isinstance(receipt, Receipt):
                receipt = Receipt.from_any(receipt, **kwargs)
            else:
                data = dict(kwargs)
                data.update(Receipt.from_any(receipt).to_dict())
                receipt = data
        elif isinstance(receipt, (list, tuple)) and not isinstance(receipt, str):
            # Positional form: partition, batch, run_id, action, source_seq
            names = ("partition", "batch", "run_id", "action", "source_seq")
            receipt = dict(zip(names, receipt))
        parsed = Receipt.from_any(receipt)
        if parsed.log_position is None:
            parsed.log_position = self._default_position
        run = self.record.add_receipt(parsed)
        self._apply_transaction_receipt(parsed, run)
        self._advance_cursor(parsed)
        if self._should_save_each:
            self.save()
        return run

    @contextmanager
    def batching(self):
        """Defer recovery saves while a batch of operations is applied.

        The final state is persisted when the context exits (including on
        exceptions), so a crash mid-way only loses the in-flight batch.
        """
        previous = self._batch_depth
        self._batch_depth += 1
        try:
            yield self
        finally:
            self._batch_depth = previous
            if self._batch_depth == 0:
                self.save()
                self.ledger.flush()

    def process_receipts(self, receipts: Iterable[Any], source: str = None) -> List[RunState]:
        runs = []
        with self.batching():
            for receipt in receipts:
                if source is not None and not isinstance(receipt, Receipt):
                    try:
                        receipt = Receipt.from_any(receipt, source=source)
                    except TypeError:
                        pass
                runs.append(self.process_receipt(receipt))
        return runs

    add_receipt = process_receipt
    record_receipt = process_receipt
    apply_receipt = process_receipt
    ingest_receipt = process_receipt

    def ingest(self, receipts: Iterable[Any], source: str = None) -> List[RunState]:
        return self.process_receipts(receipts, source=source)

    consume = ingest
    feed = ingest
    replay = ingest

    def consume_log(
        self,
        records: Iterable[Any],
        position: Any = None,
        source: str = None,
    ) -> List[RunState]:
        """Consume a log segment starting at ``position``.

        If ``position`` is omitted the stored cursor is used.  Records whose
        position is at or before the cursor are ignored.  This models reading
        only the continuation segment of a rotated task log.
        """
        if position is not None:
            self.record.advance_cursor(self.stream, position=position)
            self.save()
        consumed = []
        for record in self.read_continuation(records):
            consumed.append(self.process_receipt(record))
        self.save()
        return consumed

    read_log = consume_log
    tail = consume_log
    consume_continuation = consume_log

    def read_continuation(
        self,
        records: Iterable[Any],
        position: Any = None,
        stream: str = None,
    ) -> List[Any]:
        """Return records strictly after the given (or stored) position.

        When records carry a source sequence, the sequence is authoritative.
        Records without a sequence but with a position are compared to the
        cursor position only when that position was set from positions (not
        from a bare sequence).
        """
        stream = stream or self.stream
        cursor = self.record.cursor(stream)
        if position is None:
            position = cursor.position
        result = []
        for record in records:
            seq = _get(record, "source_seq", "source_event_seq", "event_seq", "seq")
            if seq is not None and cursor.source_seq is not None:
                try:
                    if int(seq) <= int(cursor.source_seq):
                        continue
                except (TypeError, ValueError):
                    pass
                # The sequence is authoritative for sequenced records.
                result.append(record)
                continue
            record_position = self._record_position(record)
            if record_position is not None and position is not None:
                if not self._position_after(record_position, position):
                    continue
            result.append(record)
        return result

    # Cursor helpers -----------------------------------------------------
    @property
    def position(self):
        return self.record.cursor(self.stream).position

    @property
    def source_seq(self):
        return self.record.cursor(self.stream).source_seq

    @property
    def log_position(self):
        return self.position

    def advance_cursor(self, position: Any = None, source_seq: int = None, file: str = None):
        cursor = self.record.advance_cursor(self.stream, position, source_seq, file)
        self.save()
        return cursor

    def _advance_cursor(self, receipt: Receipt) -> None:
        # Backfill receipts are written to the Rocketry task log with a
        # timestamp position.  They must not move the source log cursor,
        # otherwise later source positions (small sequence numbers) would
        # look "older" and be skipped.
        if receipt.source == "backfill":
            if receipt.source_seq is not None:
                self.record.advance_cursor(self.stream, source_seq=receipt.source_seq)
            return
        position = receipt.log_position
        seq = receipt.source_seq
        if position is None and seq is None:
            return
        self.record.advance_cursor(self.stream, position=position, source_seq=seq)

    def _record_position(self, record: Any) -> Any:
        return _get(record, "log_position", "position", "offset", "cursor", "created")

    def _position_after(self, new: Any, old: Any) -> bool:
        try:
            return new > old
        except TypeError:
            pass
        new_offset = _get(new, "offset", "position", "index", default=None)
        old_offset = _get(old, "offset", "position", "index", default=None)
        if new_offset is not None and old_offset is not None:
            try:
                return new_offset > old_offset
            except TypeError:
                return False
        return False

    @property
    def _should_save_each(self) -> bool:
        # Saving after every receipt is cheap for memory/file-backed stores
        # and makes a kill during ingestion recoverable.  A batching context
        # defers saves to the end of the batch.
        return self._batch_depth == 0

    @property
    def _default_position(self):
        return None

    # Querying -----------------------------------------------------------
    def status(self, partition: str = None, run_id: str = None, include_history: bool = True):
        """Get the status of a run, or the view of a partition/all partitions.

        * ``status(partition, run_id)`` returns the run status string.
        * ``status(partition)`` returns the partition's serializable view.
        * ``status()`` returns all partitions' views.
        """
        if partition is not None and run_id is not None:
            run = self.record.get_run(partition, run_id, include_history=include_history)
            if run is None:
                return None
            return run.status
        if partition is not None:
            state = self.record.get_partition(partition)
            return None if state is None else self._partition_view(state)
        return self.view()

    get_status = status

    def get_run(self, partition: str, run_id: str, include_history: bool = True) -> Optional[RunState]:
        return self.record.get_run(partition, run_id, include_history=include_history)

    find_run = get_run

    def pending(self, partition: str = None) -> List[RunState]:
        if partition is not None:
            state = self.record.get_partition(partition)
            return [] if state is None else state.pending_runs()
        return self.record.pending_runs()

    pending_runs = pending
    unconfirmed = pending

    def failed(self, partition: str = None) -> List[RunState]:
        if partition is not None:
            state = self.record.get_partition(partition)
            return [] if state is None else state.failed_runs()
        return self.record.failed_runs()

    failed_runs = failed

    def history(self, partition: str = None) -> List[RunState]:
        return self.record.history(partition)

    def view(self) -> Dict[str, Any]:
        return {
            name: self._partition_view(state)
            for name, state in self.record.partitions.items()
        }

    to_dict = view
    state = view

    def _partition_view(self, state) -> Dict[str, Any]:
        return {
            "partition": state.name,
            "batch": state.current_batch,
            "current_batch": state.current_batch,
            "runs": {run_id: run.to_dict() for run_id, run in state.runs.items()},
            "pending": [run.to_dict() for run in state.pending_runs()],
            "failed": [run.to_dict() for run in state.failed_runs()],
            "history": [run.to_dict() for run in state.history],
        }

    # Eligibility and dispatch ------------------------------------------
    def can_backfill(self, partition: str, run_id: str) -> bool:
        """Whether a run is a confirmed failure in the partition's current batch."""
        state = self.record.get_partition(partition)
        if state is None:
            return False
        run = state.get_run(run_id, include_history=False)
        if run is None or run.archived:
            return False
        if run.batch != state.current_batch:
            return False
        if not run.is_confirmed_failed:
            return False
        if self.ledger.get_by_run(partition, run.batch, run_id) is not None:
            return False
        return True

    is_backfillable = can_backfill
    should_backfill = can_backfill

    def eligible(self, partition: str = None) -> List[RunState]:
        states = (
            [self.record.get_partition(partition)]
            if partition is not None
            else list(self.record.partitions.values())
        )
        result = []
        for state in states:
            if state is None:
                continue
            for run in state.failed_runs():
                if self.can_backfill(run.partition, run.run_id):
                    result.append(run)
        return result

    def register_backfill(
        self,
        partition: Optional[str] = None,
        batch: Optional[str] = None,
        run_id: Optional[str] = None,
        transaction_id: Optional[str] = None,
        receipt: Any = None,
        check: bool = True,
        payload: Any = None,
    ) -> BackfillEntry:
        """Register a backfill transaction.  Never creates a second one.

        The first call for an eligible failed run creates the transaction.
        Repeated calls (even with another transaction id) return the original
        entry.  Registering an archived/old-batch run raises
        :class:`BackfillNotAllowed` when ``check`` is true.
        """
        if receipt is not None:
            parsed = Receipt.from_any(receipt)
            partition = partition or parsed.partition
            batch = batch or parsed.batch
            run_id = run_id or parsed.run_id
            transaction_id = transaction_id or parsed.transaction_id
        if partition is None:
            raise ValueError("register_backfill requires partition and run_id")
        if run_id is None and batch is not None:
            # Support register_backfill(partition, run_id) in addition to
            # register_backfill(partition, batch, run_id).
            state = self.record.get_partition(partition)
            if state is not None and state.get_run(
                str(batch), include_history=True
            ) is not None:
                run_id, batch = batch, None
        if run_id is None:
            raise ValueError("register_backfill requires partition and run_id")
        state = self.record.get_partition(partition)
        if state is None:
            raise KeyError(f"Unknown partition: {partition!r}")
        run = state.get_run(run_id, include_history=True)
        if check:
            if not self.can_backfill(partition, run_id):
                existing = self.ledger.get_by_run(
                    partition,
                    batch or (run.batch if run is not None else state.current_batch),
                    run_id,
                )
                if existing is not None:
                    return existing
                raise BackfillNotAllowed(
                    f"Run {run_id!r} in partition {partition!r} is not a "
                    "confirmed failure of the current batch"
                )
        if batch is None:
            batch = run.batch if run is not None else state.current_batch
        if run is not None:
            batch = run.batch
        entry = self.ledger.register(
            transaction_id=transaction_id,
            partition=partition,
            batch=batch,
            run_id=run_id,
            payload=payload,
        )
        if run is not None:
            run.mark_backfill(entry.transaction_id, entry.status)
        self.save()
        return entry

    register = register_backfill
    request_backfill = register_backfill

    def dispatch(
        self,
        partition: Optional[str] = None,
        run_id: Optional[str] = None,
        transaction_id: Optional[str] = None,
        executor: Callable = None,
    ) -> Optional[BackfillEntry]:
        """Register and execute one backfill.

        If the run already has a transaction, the existing entry is returned
        and no new work is started.  The executor is called at most once for
        the transaction; if it has already been inserted the stored receipt
        is returned.
        """
        entry = self.register_backfill(
            partition=partition,
            run_id=run_id,
            transaction_id=transaction_id,
            check=False,
        )
        return self.run_backfill(entry, executor=executor)

    def dispatch_failed(self, executor: Callable = None, partition: str = None) -> List[BackfillEntry]:
        """Dispatch backfills for all eligible failed runs."""
        entries = []
        with self.batching(), self.ledger.batching():
            for run in self.eligible(partition):
                entries.append(self.dispatch(run.partition, run.run_id, executor=executor))
        return entries

    dispatch_backfills = dispatch_failed
    dispatch_pending = dispatch_failed
    backfill_failed = dispatch_failed
    run_backfills = dispatch_failed

    def run_backfill(self, entry: Any, executor: Callable = None) -> BackfillEntry:
        """Perform the work for an already registered backfill exactly once."""
        if not isinstance(entry, BackfillEntry):
            entry = self.ledger.get(transaction_id=str(entry))
            if entry is None:
                raise KeyError(f"Unknown backfill transaction: {entry!r}")
        if entry.success_receipt is not None:
            return entry
        executor = executor if executor is not None else self.executor
        if entry.status == "registered":
            entry = self.ledger.mark_started(entry.transaction_id)
        try:
            if executor is not None:
                result = executor(entry)
                receipt = self._receipt_from_result(result, entry)
            else:
                receipt = None
            receipt = self.ledger.insert(
                entry.transaction_id,
                partition=entry.partition,
                batch=entry.batch,
                run_id=entry.run_id,
                success_receipt=receipt,
            )
            entry = self.ledger.get(entry.transaction_id)
        except Exception as exc:
            self.ledger._update(entry.transaction_id, status="failed", error=str(exc))
            raise
        self._attach_backfill(entry)
        # Complete the transaction by making the success receipt visible.
        # Without an explicit sink/task the ledger itself is the sink.
        entry = self.ledger.get(entry.transaction_id)
        self._write_success_receipt(entry, writer=self.receipt_sink)
        self.save()
        return self.ledger.get(entry.transaction_id)

    execute_backfill = run_backfill
    perform_backfill = run_backfill

    def insert_backfill(
        self,
        transaction_id: Optional[str] = None,
        partition: Optional[str] = None,
        batch: Optional[str] = None,
        run_id: Optional[str] = None,
        success_receipt: Any = None,
        payload: Any = None,
        **kwargs,
    ):
        """Persist a backfill result idempotently and return its receipt."""
        transaction_id = _first(
            transaction_id,
            kwargs.pop("business_id", None),
            kwargs.pop("business_transaction_id", None),
            kwargs.pop("flow_id", None),
            kwargs.pop("idempotency_key", None),
        )
        partition = _first(
            partition, kwargs.pop("partition_id", None),
            kwargs.pop("region", None), kwargs.pop("zone", None),
        )
        batch = _first(batch, kwargs.pop("batch_id", None),
                       kwargs.pop("declaration_batch", None))
        run_id = _first(
            run_id, kwargs.pop("run", None),
            kwargs.pop("run_identifier", None),
            kwargs.pop("original_run_id", None),
        )
        if success_receipt is None:
            success_receipt = kwargs.pop("receipt", None)
        if partition is None and isinstance(transaction_id, (dict, Receipt)):
            source = Receipt.from_any(transaction_id)
            partition, batch, run_id = source.partition, source.batch, source.run_id
            transaction_id = source.transaction_id
            success_receipt = success_receipt or source
        entry = None
        if transaction_id is not None and not isinstance(transaction_id, (dict, Receipt)):
            entry = self.ledger.get_by_transaction(str(transaction_id))
        if entry is None and partition is not None:
            entry = self.ledger.get_by_run(partition, batch, run_id)
        if entry is None:
            entry = self.register_backfill(
                partition=partition,
                batch=batch,
                run_id=run_id,
                transaction_id=transaction_id,
                check=False,
            )
        receipt = self.ledger.insert(
            entry.transaction_id,
            partition=entry.partition,
            batch=entry.batch,
            run_id=entry.run_id,
            success_receipt=success_receipt,
            payload=payload,
        )
        self._attach_backfill(self.ledger.get(entry.transaction_id))
        self.save()
        return receipt

    save_backfill = insert_backfill
    record_backfill = insert_backfill
    store_backfill = insert_backfill

    def _receipt_from_result(self, result: Any, entry: BackfillEntry):
        if result is None:
            return None
        if isinstance(result, Receipt):
            return result
        if isinstance(result, dict) and any(
            key in result for key in ("partition", "batch", "run_id", "action", "status")
        ):
            return Receipt.from_any(result)
        # Treat an arbitrary return value as the receipt payload.
        return Receipt(
            partition=entry.partition,
            batch=entry.batch,
            run_id=entry.run_id,
            action="success",
            original_run_id=entry.run_id,
            transaction_id=entry.transaction_id,
            payload=result,
            source="backfill",
        )

    def _attach_backfill(self, entry: BackfillEntry) -> None:
        if entry is None:
            return
        run = self.record.get_run(entry.partition, entry.run_id, include_history=True)
        if run is None:
            return
        run.mark_backfill(entry.transaction_id, entry.status)
        if entry.success_receipt is not None:
            run.backfill_receipt = entry.success_receipt

    def _apply_transaction_receipt(self, receipt: Receipt, run: RunState) -> None:
        transaction_id = receipt.transaction_id
        if transaction_id is None and receipt.source == "backfill":
            # The deterministic id is used by the ledger when none was given.
            transaction_id = default_transaction_id(
                receipt.partition, receipt.batch, receipt.original_run_id or receipt.run_id
            )
        if transaction_id is None:
            return
        entry = self.ledger.get_by_transaction(transaction_id)
        if entry is None:
            return
        if receipt.is_success:
            if entry.success_receipt is None:
                self.ledger.insert(
                    entry.transaction_id,
                    partition=entry.partition,
                    batch=entry.batch,
                    run_id=entry.run_id,
                    success_receipt=receipt,
                )
            self.ledger.mark_receipt_logged(entry.transaction_id, receipt=receipt)
            run.backfill_receipt = receipt
            run.mark_backfill(entry.transaction_id, "completed")

    # Completing backfills after a restart ------------------------------
    def resume(self, executor: Callable = None, writer: Callable = None) -> Dict[str, List[BackfillEntry]]:
        """Finish dispatches interrupted by shutdown.

        * registered but not inserted/dispatched entries are executed,
        * inserted entries without a logged success receipt get the receipt
          written to the task log.
        """
        executor = executor if executor is not None else self.executor
        writer = writer if writer is not None else self.receipt_sink
        started, written = [], []

        for entry in self.ledger.pending_dispatches():
            self.run_backfill(entry, executor=executor)
            started.append(self.ledger.get(entry.transaction_id))

        for entry in self.ledger.pending_receipts():
            if self._write_success_receipt(entry, writer=writer):
                written.append(entry)

        self.save()
        return {"started": started, "written": written}

    recover = resume
    reconcile = resume
    recover_pending = resume
    complete_pending = resume

    def write_success_receipt(self, entry: Any, writer: Callable = None) -> bool:
        if not isinstance(entry, BackfillEntry):
            entry = self.ledger.get(transaction_id=str(entry))
        return self._write_success_receipt(entry, writer=writer)

    def _write_success_receipt(self, entry: BackfillEntry, writer: Callable = None) -> bool:
        if entry is None or entry.success_receipt is None:
            return False
        if entry.receipt_logged:
            return False
        writer = writer if writer is not None else self.receipt_sink
        if writer is not None:
            writer(entry.success_receipt)
        elif self.task is not None:
            self._write_to_task_log(entry.success_receipt)
        else:
            # No sink.  Mark completed so repeated observations do not try
            # to dispatch again; the in-memory state still records the
            # backfill receipt.
            entry = self.ledger.mark_receipt_logged(
                entry.transaction_id, receipt=entry.success_receipt
            )
            self._attach_backfill(entry)
            return True
        entry = self.ledger.mark_receipt_logged(
            entry.transaction_id, receipt=entry.success_receipt
        )
        self._attach_backfill(entry)
        return True

    def _write_to_task_log(self, receipt: Receipt) -> None:
        task = self.task
        if task is None and self.session is not None:
            task = getattr(self, "task_name", None)
            if task is not None:
                task = self.session[task]
        if task is None:
            raise RuntimeError(
                "Cannot write backfill receipt: no task or receipt_sink configured"
            )
        record = self._make_task_record(receipt, task)
        task.log_record(record)

    def _make_task_record(self, receipt: Receipt, task):
        import logging

        created = receipt.created if receipt.created is not None else self._now()
        message = (
            f"Backfill succeeded for run "
            f"'{receipt.original_run_id or receipt.run_id}'"
        )
        logger = getattr(task.logger, "logger", None)
        record = logging.LogRecord(
            name=getattr(logger, "name", "rocketry.task"),
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg=message,
            args=(),
            exc_info=None,
            func="backfill",
        )
        record.created = created
        record.msecs = (created - int(created)) * 1000
        record.task_name = task.name
        record.action = "success"
        record.run_id = receipt.run_id
        record.partition = receipt.partition
        record.batch = receipt.batch
        record.source_seq = receipt.source_seq
        record.source_event_seq = receipt.source_seq
        record.original_run_id = receipt.original_run_id or receipt.run_id
        record.transaction_id = receipt.transaction_id
        record.source = receipt.source or "backfill"
        if receipt.payload is not None:
            record.result = receipt.payload
        return record

    # Task log ingestion ------------------------------------------------
    def ingest_task_log(
        self,
        task=None,
        records: Iterable[Any] = None,
        position: Any = None,
    ) -> List[RunState]:
        """Consume new recovery receipts from a task's log.

        Only records carrying ``partition``, ``batch`` and ``run_id`` are
        treated as receipts.  Ordinary Rocketry actions are ignored.  The
        stored cursor ensures only the log continuation (after rotation) is
        read.
        """
        task = task if task is not None else self.task
        if records is None:
            if task is None:
                return []
            try:
                records = task.logger.get_records()
            except Exception:
                return []
        if position is not None:
            self.record.advance_cursor(self.stream, position=position)
            self.save()
        cursor = self.record.cursor(self.stream)
        consumed = []
        for record in records:
            receipt = self._receipt_from_task_record(record)
            if receipt is None:
                continue
            record_position = self._record_position(record)
            if record_position is None:
                record_position = receipt.log_position
            if record_position is not None and cursor.position is not None:
                if not self._position_after(record_position, cursor.position):
                    continue
            if receipt.source_seq is not None and cursor.source_seq is not None:
                if receipt.source_seq <= cursor.source_seq:
                    continue
            consumed.append(self.process_receipt(receipt))
        self.save()
        return consumed

    ingest_log = ingest_task_log
    consume_task_log = ingest_task_log
    pull_receipts = ingest_task_log

    def _receipt_from_task_record(self, record: Any):
        partition = _get(record, "partition", "partition_id", "region", "zone")
        batch = _get(record, "batch", "batch_id", "declaration_batch")
        run_id = _get(record, "run_id", "run", "run_identifier")
        if partition is None or batch is None or run_id is None:
            return None
        action = normalize_action(_get(record, "action", "status", "receipt", "receipt_action"))
        source_seq = _get(
            record, "source_seq", "source_event_seq", "event_seq", "seq", "sequence"
        )
        log_position = _get(record, "log_position", "position", "offset", "cursor")
        if log_position is None:
            log_position = _get(record, "created")
        return Receipt(
            partition=str(partition),
            batch=str(batch),
            run_id=str(run_id),
            action=action or "start",
            source_seq=source_seq,
            created=_get(record, "created", "timestamp", "time"),
            log_position=log_position,
            original_run_id=_get(record, "original_run_id", "backfilled_run_id"),
            transaction_id=_get(
                record, "transaction_id", "business_id", "flow_id"
            ),
            payload=_get(record, "result", "payload"),
            source=_get(record, "source", default="task_log"),
        )

    def _now(self) -> float:
        if self.session is not None:
            try:
                return self.session.get_time()
            except Exception:
                pass
        return time.time()

    # Retry integration --------------------------------------------------
    def retry_required(self, task=None, dispatch: bool = True) -> bool:
        """Whether normal scheduling should run a recovery backfill.

        This is the side-effect free part used by :class:`Retry`: after a
        failure has been confirmed for the current batch and no backfill has
        been registered yet, return True and (when ``dispatch`` is set)
        register + perform the backfill exactly once.

        If a previous process stopped after registering or inserting a
        backfill, the pending work is completed first.  A True return then
        signals that a retry action just happened; later rounds return False.
        """
        if task is not None and self.task is None:
            self.task = task
        resumed = False
        if dispatch and (
            self.ledger.pending_dispatches() or self.ledger.pending_receipts()
        ):
            # Finish interrupted work from the previous process.  A True
            # return tells the caller a recovery action happened in this
            # round; later rounds are False.
            self.resume(executor=self.executor)
            resumed = True

        eligible = self.eligible()
        if not eligible:
            return resumed
        if dispatch:
            for run in eligible:
                entry = self.register_backfill(run.partition, run.run_id)
                self.run_backfill(entry, executor=self.executor)
                # Write the success receipt when a task logger is available;
                # it will be consumed by the normal log ingestion path.
                entry = self.ledger.get(entry.transaction_id)
                if entry is not None and entry.success_receipt is not None:
                    self._write_success_receipt(entry)
            return True
        return True

    should_retry = retry_required
    needs_backfill = retry_required


# Friendly alternative names.
PartitionedRecovery = PartitionRecovery
RecoveryManager = PartitionRecovery
PartitionBackfill = PartitionRecovery
Recovery = PartitionRecovery


def Recovery(path=None, **kwargs) -> PartitionRecovery:
    """Create a :class:`PartitionRecovery` (optionally backed by ``path``)."""
    return PartitionRecovery(path=path, **kwargs)


# Alias used in some applications: recovery journal
RecoveryJournal = RecoveryRecord
