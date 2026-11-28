"""Idempotent ledger for partition backfill dispatches.

A backfill is identified by a business transaction id (``transaction_id``,
also called ``business_id`` / ``flow_id``).  Registering the same id twice
returns the original entry and never inserts a second row.  A run can only
have one backfill transaction regardless of which id is used later.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from contextlib import contextmanager
import time

from .model import Receipt
from .store import BaseStateStore, MemoryStateStore


def default_transaction_id(partition: str, batch: str, run_id: str) -> str:
    """Deterministic id used when the caller does not provide one."""
    return f"backfill:{partition}:{batch}:{run_id}"


def _run_key(partition: str, batch: str, run_id: str):
    return f"{partition}|{batch}|{run_id}"


def _first(*values):
    "Return the first non-None value."
    for value in values:
        if value is not None:
            return value
    return None


@dataclass
class BackfillEntry:
    """Persistent state of a single backfill transaction."""

    transaction_id: str
    partition: str
    batch: str
    run_id: str
    status: str = "registered"
    success_receipt: Optional[Receipt] = None
    receipt_logged: bool = False
    created: Optional[float] = None
    updated: Optional[float] = None
    error: Optional[str] = None
    payload: Any = None

    # Friendly aliases ---------------------------------------------------
    @property
    def id(self):
        return self.transaction_id

    @property
    def business_id(self):
        return self.transaction_id

    @property
    def flow_id(self):
        return self.transaction_id

    @property
    def original_run_id(self):
        return self.run_id

    @property
    def key(self):
        return _run_key(self.partition, self.batch, self.run_id)

    @property
    def is_completed(self) -> bool:
        return self.status == "completed" and self.receipt_logged

    @property
    def is_registered(self) -> bool:
        return self.status in ("registered", "started", "in_progress")

    @property
    def is_inserted(self) -> bool:
        return self.success_receipt is not None or self.status in (
            "inserted", "receipt_pending", "completed"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "partition": self.partition,
            "batch": self.batch,
            "run_id": self.run_id,
            "status": self.status,
            "success_receipt": None if self.success_receipt is None else self.success_receipt.to_dict(),
            "receipt_logged": self.receipt_logged,
            "created": self.created,
            "updated": self.updated,
            "error": self.error,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BackfillEntry":
        receipt = data.get("success_receipt")
        return cls(
            transaction_id=data["transaction_id"],
            partition=data["partition"],
            batch=data["batch"],
            run_id=data["run_id"],
            status=data.get("status", "registered"),
            success_receipt=None if receipt is None else Receipt.from_any(receipt),
            receipt_logged=data.get("receipt_logged", False),
            created=data.get("created"),
            updated=data.get("updated"),
            error=data.get("error"),
            payload=data.get("payload"),
        )

    @property
    def receipt(self):
        return self.success_receipt


# Alias commonly used in application code.
Backfill = BackfillEntry


class BackfillLedger:
    """Idempotent registry and result store for backfill transactions.

    Parameters
    ----------
    store : BaseStateStore, optional
        Persistence backend.  By default an in-memory store is used.
    """

    LEDGER_KEY = "backfill_ledger"

    def __init__(self, store: Optional[BaseStateStore] = None):
        self.store = store if store is not None else MemoryStateStore()
        self._cache: Optional[Dict[str, BackfillEntry]] = None
        self._dirty = False
        self._batch_depth = 0

    # Persistence --------------------------------------------------------
    def _load_raw(self) -> Dict[str, Any]:
        state = self.store.load()
        return state.get(self.LEDGER_KEY, {})

    def _save_raw(self, data: Dict[str, Any]) -> None:
        state = self.store.load()
        state[self.LEDGER_KEY] = data
        self.store.save(state)

    def _load(self) -> Dict[str, BackfillEntry]:
        if self._cache is None:
            self._cache = {
                transaction_id: BackfillEntry.from_dict(entry)
                for transaction_id, entry in self._load_raw().get("entries", {}).items()
            }
        return self._cache

    def _save_entries(self, entries: Dict[str, BackfillEntry]) -> None:
        self._cache = entries
        self._dirty = True
        if self._batch_depth == 0:
            self.flush()

    def flush(self) -> None:
        "Write cached entries to the store."
        if not self._dirty or self._cache is None:
            return
        raw = self._load_raw()
        raw["entries"] = {key: entry.to_dict() for key, entry in self._cache.items()}
        raw["run_index"] = {
            entry.key: entry.transaction_id for entry in self._cache.values()
        }
        self._save_raw(raw)
        self._dirty = False

    def reload(self) -> "BackfillLedger":
        self._cache = None
        self._dirty = False
        return self

    @contextmanager
    def batching(self):
        "Defer writes while a batch of entries is updated."
        self._batch_depth += 1
        try:
            yield self
        finally:
            self._batch_depth -= 1
            if self._batch_depth == 0:
                self.flush()

    # Querying -----------------------------------------------------------
    def entries(self) -> List[BackfillEntry]:
        return list(self._load().values())

    def __iter__(self):
        return iter(self.entries())

    def __len__(self):
        return len(self._load())

    def get(self, transaction_id: Optional[str] = None, **kwargs) -> Optional[BackfillEntry]:
        """Get an entry by transaction id or by run coordinates."""
        entries = self._load()
        if transaction_id is not None:
            entry = entries.get(str(transaction_id))
            if entry is not None:
                return entry
        partition = kwargs.get("partition")
        batch = kwargs.get("batch")
        run_id = kwargs.get("run_id")
        if partition is not None and batch is not None and run_id is not None:
            key = _run_key(partition, batch, run_id)
            raw = self._load_raw()
            transaction_id = raw.get("run_index", {}).get(key)
            if transaction_id is not None:
                return entries.get(transaction_id)
        filters = {
            name: value for name, value in kwargs.items()
            if value is not None and name in ("partition", "batch", "run_id", "status")
        }
        if filters:
            for entry in entries.values():
                if all(getattr(entry, name) == value for name, value in filters.items()):
                    return entry
        return None

    get_entry = get
    lookup = get

    def get_by_run(self, partition: str, batch: str, run_id: str) -> Optional[BackfillEntry]:
        return self.get(partition=partition, batch=batch, run_id=run_id)

    def get_by_transaction(self, transaction_id: str) -> Optional[BackfillEntry]:
        return self.get(transaction_id=transaction_id)

    def exists(self, transaction_id: Optional[str] = None, **kwargs) -> bool:
        return self.get(transaction_id=transaction_id, **kwargs) is not None

    # Mutation -----------------------------------------------------------
    def register(
        self,
        transaction_id: Optional[str] = None,
        partition: str = None,
        batch: str = None,
        run_id: str = None,
        payload: Any = None,
        **kwargs,
    ) -> BackfillEntry:
        """Register a backfill.  Idempotent by transaction and by run.

        If the run already has a transaction, that transaction is returned
        unchanged.  If the transaction id already exists, its original
        entry is returned.
        """
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
        if partition is None or batch is None or run_id is None:
            # Allow register(receipt) or register({...})
            source = transaction_id if isinstance(transaction_id, dict) else None
            if source is None:
                source = transaction_id if isinstance(transaction_id, Receipt) else None
            if source is not None:
                receipt = Receipt.from_any(source)
                partition = partition or receipt.partition
                batch = batch or receipt.batch
                run_id = run_id or receipt.run_id
                transaction_id = receipt.transaction_id or None
        if partition is None or batch is None or run_id is None:
            raise ValueError("register requires partition, batch and run_id")

        partition, batch, run_id = str(partition), str(batch), str(run_id)
        if transaction_id is None:
            transaction_id = default_transaction_id(partition, batch, run_id)
        transaction_id = str(transaction_id)

        entries = self._load()
        existing = entries.get(transaction_id)
        if existing is not None:
            return existing

        existing = self._find_by_run(entries, partition, batch, run_id)
        if existing is not None:
            # The run is already accounted for; never create a second
            # transaction for the same failed run.
            return existing

        now = time.time()
        entry = BackfillEntry(
            transaction_id=transaction_id,
            partition=partition,
            batch=batch,
            run_id=run_id,
            status="registered",
            created=now,
            updated=now,
            payload=payload,
        )
        entries[transaction_id] = entry
        self._save_entries(entries)
        return entry

    register_backfill = register
    submit = register

    def _find_by_run(self, entries: Dict[str, BackfillEntry], partition, batch, run_id):
        for entry in entries.values():
            if (
                entry.partition == partition
                and entry.batch == batch
                and entry.run_id == run_id
            ):
                return entry
        return None

    def start(self, transaction_id: str) -> BackfillEntry:
        return self._update(transaction_id, status="started")

    def mark_started(self, transaction_id: str) -> BackfillEntry:
        return self._update(transaction_id, status="started")

    def _update(self, transaction_id: str, **changes) -> BackfillEntry:
        entries = self._load()
        entry = entries.get(str(transaction_id))
        if entry is None:
            raise KeyError(f"Unknown backfill transaction: {transaction_id!r}")
        for key, value in changes.items():
            setattr(entry, key, value)
        entry.updated = time.time()
        self._save_entries(entries)
        return entry

    def insert(
        self,
        transaction_id: Optional[str] = None,
        *,
        partition: str = None,
        batch: str = None,
        run_id: str = None,
        success_receipt: Any = None,
        payload: Any = None,
        created: Optional[float] = None,
        **kwargs,
    ):
        """Insert a backfill result exactly once and return its success receipt.

        Repeated calls with the same business transaction id (or for the
        same run) return the receipt produced by the first insertion.

        ``success_receipt`` may be supplied by the caller; otherwise a
        success :class:`Receipt` is built with ``original_run_id`` pointing
        to the failed run being backfilled.
        """
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
        if success_receipt is None:
            success_receipt = kwargs.pop("result", None)
        # Accept insert(receipt) / insert(dict)
        if partition is None and isinstance(transaction_id, (dict, Receipt)):
            source = Receipt.from_any(transaction_id)
            partition, batch, run_id = source.partition, source.batch, source.run_id
            transaction_id = source.transaction_id
            success_receipt = success_receipt or source
        if partition is None and run_id is None and transaction_id is not None:
            # A transaction id alone is enough: use the coordinates from
            # the registered entry.
            existing = self.get(transaction_id=str(transaction_id))
            if existing is not None:
                partition, batch, run_id = (
                    existing.partition, existing.batch, existing.run_id
                )
                if existing.success_receipt is not None:
                    return existing.success_receipt
        if partition is None or batch is None or run_id is None:
            raise ValueError("insert requires partition, batch and run_id")
        if transaction_id is None:
            transaction_id = default_transaction_id(partition, batch, run_id)
        transaction_id = str(transaction_id)

        entries = self._load()
        existing = entries.get(transaction_id) or self._find_by_run(
            entries, str(partition), str(batch), str(run_id)
        )
        if existing is not None:
            if existing.success_receipt is not None:
                return existing.success_receipt
            # Registered but not inserted yet: perform the insertion on the
            # original transaction so the id stays stable.
            entry = existing
        else:
            now = time.time()
            entry = BackfillEntry(
                transaction_id=transaction_id,
                partition=str(partition),
                batch=str(batch),
                run_id=str(run_id),
                status="registered",
                created=now,
                updated=now,
                payload=payload,
            )
            entries[transaction_id] = entry

        now = time.time()
        if success_receipt is None:
            success_receipt = Receipt(
                partition=str(partition),
                batch=str(batch),
                run_id=str(run_id),
                action="success",
                created=now if created is None else created,
                original_run_id=str(run_id),
                transaction_id=transaction_id,
                payload=payload,
                source="backfill",
            )
        else:
            success_receipt = Receipt.from_any(
                success_receipt,
                partition=str(partition),
                batch=str(batch),
                run_id=success_receipt.run_id if isinstance(success_receipt, Receipt) else str(run_id),
                action="success",
                original_run_id=(
                    success_receipt.original_run_id
                    if isinstance(success_receipt, Receipt) and success_receipt.original_run_id
                    else str(run_id)
                ),
                transaction_id=transaction_id,
            )

        entry.success_receipt = success_receipt
        entry.status = "receipt_pending"
        entry.receipt_logged = False
        entry.updated = now
        self._save_entries(entries)
        return success_receipt

    insert_backfill = insert
    record_backfill = insert
    save = insert
    store_result = insert
    record = insert

    def load(self) -> "BackfillLedger":
        "Reload from the backing store (ledger is stateless otherwise)."
        self._load()
        return self

    def save_to(self, path) -> None:
        "Write the ledger to a standalone JSON file."
        from .store import FileStateStore
        FileStateStore(path).save({self.LEDGER_KEY: self._load_raw()})

    @classmethod
    def load_from(cls, path) -> "BackfillLedger":
        from .store import FileStateStore
        return cls(store=FileStateStore(path))

    def mark_receipt_logged(
        self,
        transaction_id: Optional[str] = None,
        receipt: Any = None,
        **kwargs,
    ) -> Optional[BackfillEntry]:
        """Mark that the success receipt has been written to the task log."""
        entries = self._load()
        entry = None
        if transaction_id is not None:
            entry = entries.get(str(transaction_id))
        if entry is None and receipt is not None:
            receipt = Receipt.from_any(receipt)
            entry = self._find_by_run(
                entries, receipt.partition, receipt.batch,
                receipt.original_run_id or receipt.run_id,
            )
        if entry is None:
            entry = self.get(partition=kwargs.get("partition"),
                             batch=kwargs.get("batch"),
                             run_id=kwargs.get("run_id"))
            if entry is None:
                return None
            entry = entries.get(entry.transaction_id)
        entry.receipt_logged = True
        entry.status = "completed"
        entry.updated = time.time()
        self._save_entries(entries)
        return entry

    complete = mark_receipt_logged
    mark_completed = mark_receipt_logged

    def complete_by_receipt(self, receipt: Any) -> Optional[BackfillEntry]:
        receipt = Receipt.from_any(receipt)
        return self.mark_receipt_logged(receipt=receipt)

    def pending_receipts(self) -> List[BackfillEntry]:
        """Inserted entries whose success receipt is not yet in the log."""
        return [
            entry for entry in self._load().values()
            if entry.success_receipt is not None and not entry.receipt_logged
        ]

    def pending_dispatches(self) -> List[BackfillEntry]:
        """Registered entries whose work has not produced an insert yet."""
        return [
            entry for entry in self._load().values()
            if entry.success_receipt is None and entry.status != "cancelled"
        ]

    def completed(self) -> List[BackfillEntry]:
        return [entry for entry in self._load().values() if entry.is_completed]

    def clear(self) -> None:
        self._save_raw({})
        self._cache = None
        self._dirty = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entries": {
                transaction_id: entry.to_dict()
                for transaction_id, entry in self._load().items()
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], store: BaseStateStore = None) -> "BackfillLedger":
        ledger = cls(store=store)
        ledger._save_raw({
            "entries": data.get("entries", {}),
            "run_index": {
                _run_key(entry["partition"], entry["batch"], entry["run_id"]): transaction_id
                for transaction_id, entry in data.get("entries", {}).items()
            },
        })
        ledger._cache = None
        return ledger


# Friendly aliases
BackfillRegistry = BackfillLedger
BackfillStore = BackfillLedger
BackfillJournal = BackfillLedger
