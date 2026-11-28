"""Persistence helpers for recovery records and the backfill ledger."""
from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from .model import RecoveryRecord


class BaseStateStore:
    """Minimal key/value store used by the recovery components.

    Implementations only need to provide :meth:`load` and :meth:`save`.
    """

    def load(self) -> Dict[str, Any]:
        raise NotImplementedError

    def save(self, state: Dict[str, Any]) -> None:
        raise NotImplementedError

    def clear(self) -> None:
        self.save({})


class MemoryStateStore(BaseStateStore):
    """State store held in memory (useful for tests and short processes)."""

    def __init__(self, state: Optional[Dict[str, Any]] = None):
        self._state = dict(state or {})
        self._lock = threading.RLock()

    def load(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._state, default=str))

    def save(self, state: Dict[str, Any]) -> None:
        with self._lock:
            self._state = json.loads(json.dumps(state, default=str))

    @property
    def state(self) -> Dict[str, Any]:
        return self.load()

    def __getstate__(self):
        return {"_state": self._state}

    def __setstate__(self, state):
        self._state = state.get("_state", {})
        self._lock = threading.RLock()

    def __repr__(self):
        return f"{type(self).__name__}({self._state!r})"


# Friendly alias: a memory store is often handed in as a plain mapping.
DictStateStore = MemoryStateStore


class InMemoryStateStore(MemoryStateStore):
    "Alias kept for readability in application code."


class FileStateStore(BaseStateStore):
    """JSON file backed state store.

    Writes are atomic (write to a temporary file then replace) so a process
    killed in the middle of a save does not leave a truncated file behind.
    """

    def __init__(self, path):
        self.path = Path(path)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as file:
            content = file.read()
        if not content.strip():
            return {}
        return json.loads(content)

    def save(self, state: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=self.path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(state, file, ensure_ascii=False, default=str)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_name, self.path)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise


class RecoveryStore:
    """Stores a :class:`~rocketry.recovery.model.RecoveryRecord`.

    The store can be used stand-alone or shared with a backfill ledger; the
    two components use different keys in the same underlying state store so
    a single JSON file contains everything needed after a restart.
    """

    RECORD_KEY = "recovery_record"

    def __init__(self, store: Optional[BaseStateStore] = None, record: RecoveryRecord = None):
        self.store = store if store is not None else MemoryStateStore()
        self._record = record

    @property
    def record(self) -> RecoveryRecord:
        if self._record is None:
            self.load()
        return self._record

    @record.setter
    def record(self, value: RecoveryRecord) -> None:
        self._record = value

    def load(self) -> RecoveryRecord:
        state = self.store.load()
        data = state.get(self.RECORD_KEY)
        if data is None:
            if self._record is None:
                self._record = RecoveryRecord()
        else:
            self._record = RecoveryRecord.from_dict(data)
        return self._record

    def save(self, record: Optional[RecoveryRecord] = None) -> RecoveryRecord:
        if record is not None:
            self._record = record
        if self._record is None:
            self._record = RecoveryRecord()
        state = self.store.load()
        state[self.RECORD_KEY] = self._record.to_dict()
        self.store.save(state)
        return self._record

    def snapshot(self) -> Dict[str, Any]:
        return self.record.to_dict()

    def clear(self) -> None:
        self._record = RecoveryRecord()
        state = self.store.load()
        state.pop(self.RECORD_KEY, None)
        self.store.save(state)


# Friendly aliases.  The feature is sometimes described as a recovery
# journal / checkpoint rather than a store.
MemoryRecoveryStore = RecoveryStore
InMemoryRecoveryStore = RecoveryStore


def FileRecoveryStore(path, record: RecoveryRecord = None) -> RecoveryStore:
    """Create a recovery store backed by a JSON file."""
    return RecoveryStore(store=FileStateStore(path), record=record)
