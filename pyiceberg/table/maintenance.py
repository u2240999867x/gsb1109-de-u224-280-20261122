# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from pyiceberg.table.replay import (
    CleanupPlan,
    ReplayGuard,
    ReplayStatus,
    age_ms,
    commit_with_refresh_retry,
    inspect_replay_at,
    plan_cleanup,
    timestamp_ms,
)
from pyiceberg.table.replay import ReplayRegistration as ReplayRegistration
from pyiceberg.table.replay import ReplayState as ReplayState
from pyiceberg.table.replay import ReplayWindow as ReplayWindow

if TYPE_CHECKING:
    from pyiceberg.table import Table


class SnapshotCleanup:
    """Retention-aware snapshot and expired-reference cleanup builder."""

    def __init__(self, table: Table) -> None:
        self.table = table
        self._as_of_ms: int | None = None
        self._max_snapshot_age_ms: int | None = None
        self._min_snapshots_to_keep: int | None = None
        self._snapshot_ids: set[int] = set()
        self._expire_refs: bool = True

    def as_of(self, value: datetime | int | None = None, **kwargs: Any) -> SnapshotCleanup:
        if value is None:
            value = kwargs.get("timestamp") or kwargs.get("time") or kwargs.get("at") or kwargs.get("now")
        self._as_of_ms = timestamp_ms(value, "as_of") if value is not None else None
        return self

    at = as_of
    now = as_of
    cleanup_at = as_of
    cleanup_timestamp = as_of
    as_of_timestamp = as_of
    expire_at = as_of
    caller_time = as_of
    base_time = as_of

    def max_snapshot_age(self, value: timedelta | int | None) -> SnapshotCleanup:
        self._max_snapshot_age_ms = age_ms(value)
        return self

    max_age = max_snapshot_age
    max_age_ms = max_snapshot_age
    max_snapshot_age_ms = max_snapshot_age
    max_lookback = max_snapshot_age
    lookback = max_snapshot_age
    older_than_age = max_snapshot_age

    def min_snapshots_to_keep(self, value: int) -> SnapshotCleanup:
        self._min_snapshots_to_keep = max(1, int(value))
        return self

    retain_last = min_snapshots_to_keep
    min_keep = min_snapshots_to_keep

    def by_id(self, snapshot_id: int) -> SnapshotCleanup:
        self._snapshot_ids.add(snapshot_id)
        return self

    def by_ids(self, snapshot_ids: Iterable[int]) -> SnapshotCleanup:
        self._snapshot_ids.update(snapshot_ids)
        return self

    def expire_ids(self, snapshot_ids: Iterable[int]) -> SnapshotCleanup:
        return self.by_ids(snapshot_ids)

    def expire_references(self, value: bool = True) -> SnapshotCleanup:
        self._expire_refs = value
        return self

    clean_expired_refs = expire_references

    def configure(self, **kwargs: Any) -> SnapshotCleanup:
        as_of_value = (
            kwargs.get("as_of")
            or kwargs.get("as_of_timestamp")
            or kwargs.get("as_of_time")
            or kwargs.get("now")
            or kwargs.get("at")
            or kwargs.get("at_time")
            or kwargs.get("timestamp")
            or kwargs.get("cleanup_time")
            or kwargs.get("cleanup_timestamp")
            or kwargs.get("caller_time")
            or kwargs.get("base_time")
            or kwargs.get("older_than")
        )
        if as_of_value is not None:
            self.as_of(as_of_value)
        max_age_value = (
            kwargs.get("max_snapshot_age")
            or kwargs.get("max_snapshot_age_ms")
            or kwargs.get("max_age")
            or kwargs.get("max_age_ms")
            or kwargs.get("max_lookback")
            or kwargs.get("lookback")
        )
        if max_age_value is not None:
            self.max_snapshot_age(max_age_value)
        min_keep_value = (
            kwargs.get("min_snapshots_to_keep")
            or kwargs.get("min_snapshots")
            or kwargs.get("retain_last")
            or kwargs.get("min_keep")
        )
        if min_keep_value is not None:
            self.min_snapshots_to_keep(min_keep_value)
        if snapshot_ids := kwargs.get("snapshot_ids") or kwargs.get("ids"):
            self.by_ids(snapshot_ids)
        for key, value in kwargs.items():
            normalized = key.lower()
            if value is None:
                continue
            if self._as_of_ms is None and (
                "as_of" in normalized
                or "cleanup_time" in normalized
                or "cleanup_timestamp" in normalized
                or normalized in {"now", "at", "timestamp", "caller_time", "base_time"}
            ):
                self.as_of(value)
            if (
                "min_snapshot" in normalized or "retain_last" in normalized or normalized == "min_keep"
            ) and self._min_snapshots_to_keep is None:
                self.min_snapshots_to_keep(value)
            if (
                "max_snapshot_age" in normalized
                or "max_age" in normalized
                or "lookback" in normalized
                or "look_back" in normalized
            ) and self._max_snapshot_age_ms is None:
                self.max_snapshot_age(value)
        return self

    def plan(self) -> CleanupPlan:
        return plan_cleanup(
            self.table.metadata,
            as_of_ms=self._as_of_ms,
            max_snapshot_age_ms=self._max_snapshot_age_ms,
            min_snapshots_to_keep=self._min_snapshots_to_keep,
            explicit_snapshot_ids=self._snapshot_ids,
            automatic_retention=True,
        )

    def _build_updates(self, metadata: Any) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
        from pyiceberg.table.update import (
            MergeReplayHistoryUpdate,
            RemoveSnapshotRefUpdate,
            RemoveSnapshotsUpdate,
        )

        plan = plan_cleanup(
            metadata,
            as_of_ms=self._as_of_ms,
            max_snapshot_age_ms=self._max_snapshot_age_ms,
            min_snapshots_to_keep=self._min_snapshots_to_keep,
            explicit_snapshot_ids=self._snapshot_ids,
            automatic_retention=True,
        )
        updates: list[Any] = []
        if plan.state is not None:
            updates.append(
                MergeReplayHistoryUpdate(history=[point.model_dump(by_alias=True) for point in plan.state.main_history])
            )
        if self._expire_refs:
            for ref_name in sorted(plan.remove_ref_names):
                updates.append(RemoveSnapshotRefUpdate(ref_name=ref_name))
        existing_ids = {snapshot.snapshot_id for snapshot in metadata.snapshots}
        snapshot_ids = sorted(plan.remove_snapshot_ids & existing_ids)
        if snapshot_ids:
            updates.append(RemoveSnapshotsUpdate(snapshot_ids=snapshot_ids))
        return tuple(updates), ()

    def commit(self, as_of: Any = None, **kwargs: Any) -> CleanupPlan:
        """Commit cleanup with refresh/retry so late replay registrations are honored."""
        if as_of is not None:
            self.as_of(as_of)
        elif cleanup_time := kwargs.get("cleanup_time") or kwargs.get("now") or kwargs.get("at"):
            self.as_of(cleanup_time)
        committed_plan: CleanupPlan | None = None

        def build_and_capture(metadata: Any) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
            nonlocal committed_plan
            committed_plan = plan_cleanup(
                metadata,
                as_of_ms=self._as_of_ms,
                max_snapshot_age_ms=self._max_snapshot_age_ms,
                min_snapshots_to_keep=self._min_snapshots_to_keep,
                explicit_snapshot_ids=self._snapshot_ids,
                automatic_retention=True,
            )
            return self._build_updates(metadata)

        commit_with_refresh_retry(self.table, build_and_capture)
        return committed_plan if committed_plan is not None else self.plan()

    run = commit
    execute = commit
    apply = commit

    def __enter__(self) -> SnapshotCleanup:
        """Return the cleanup builder for a context-managed commit."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Commit cleanup when the context exits without an exception."""
        if exc_type is None:
            self.commit()


class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

    def __call__(self, *args: Any, **kwargs: Any) -> MaintenanceTable:
        return self

    def expire_snapshots(self, *args: Any, **kwargs: Any) -> Any:
        """Return an ExpireSnapshots builder, or a retention cleanup builder when options are supplied."""
        from pyiceberg.table.update.snapshot import ExpireSnapshots

        if args or kwargs:
            cleanup = SnapshotCleanup(self.tbl)
            if args:
                cleanup.as_of(args[0])
            cleanup.configure(**kwargs)
            return cleanup
        return ExpireSnapshots(transaction=_new_autocommit_transaction(self.tbl))

    def cleanup_snapshots(self, *args: Any, **kwargs: Any) -> SnapshotCleanup:
        cleanup = SnapshotCleanup(self.tbl)
        if args:
            cleanup.as_of(args[0])
        cleanup.configure(**kwargs)
        return cleanup

    cleanup = cleanup_snapshots
    expire_snapshot_history = cleanup_snapshots
    snapshot_cleanup = cleanup_snapshots

    @property
    def replay_guard(self) -> ReplayGuard:
        """Return the replay guard; the returned object is also callable."""
        return ReplayGuard(self.tbl)

    replay_protection = replay_guard
    replay_guarantee = replay_guard
    replay_safeguard = replay_guard
    replay = replay_guard
    replay_registry = replay_guard
    replay_assurance = replay_guard
    replay_manager = replay_guard
    replay_coverage = replay_guard
    replay_windows = replay_guard
    coverage_guard = replay_guard
    coverage_registry = replay_guard
    fetch_windows = replay_guard

    def enable_replay_protection(self, *args: Any, **kwargs: Any) -> ReplayGuard:
        return self.replay_guard.enable(*args, **kwargs)

    enable_replay_guard = enable_replay_protection
    enable_replay_guarantee = enable_replay_protection
    enable_replay_safeguard = enable_replay_protection
    enable_replay = enable_replay_protection
    enable_replay_assurance = enable_replay_protection

    def register_replay_window(self, *args: Any, **kwargs: Any) -> Any:
        return self.replay_guard().register(*args, commit=False, **kwargs)

    register_replay_interval = register_replay_window
    register_replay_period = register_replay_window
    register_fetch_window = register_replay_window
    register_data_fetch_window = register_replay_window
    register_retrieval_window = register_replay_window
    register_data_retrieval_window = register_replay_window
    register_extraction_window = register_replay_window
    register_query_window = register_replay_window
    register_read_window = register_replay_window
    register_window = register_replay_window

    def register_replay(self, *args: Any, **kwargs: Any) -> Any:
        return self.replay_guard().register(*args, **kwargs)

    register_replay_registration = register_replay

    def unregister_replay_window(self, *args: Any, **kwargs: Any) -> Any:
        return self.replay_guard().unregister_window(*args, **kwargs)

    deregister_replay_window = unregister_replay_window
    revoke_replay_window = unregister_replay_window
    cancel_replay_window = unregister_replay_window
    remove_replay_window = unregister_replay_window
    unregister_window = unregister_replay_window
    revoke_window = unregister_replay_window

    def unregister_replay(self, *args: Any, **kwargs: Any) -> Any:
        return self.replay_guard().unregister(*args, **kwargs)

    revoke_replay = unregister_replay
    deregister_replay = unregister_replay
    cancel_replay = unregister_replay

    def replay_snapshot_at(self, *args: Any, **kwargs: Any) -> Any:
        return self.replay_guard().snapshot_at(*args, **kwargs)

    snapshot_for_replay = replay_snapshot_at
    snapshot_at_fetch_time = replay_snapshot_at
    snapshot_at_retrieval_time = replay_snapshot_at
    get_snapshot_at_fetch_time = replay_snapshot_at
    get_snapshot_at_retrieval_time = replay_snapshot_at
    snapshot_by_fetch_time = replay_snapshot_at
    snapshot_as_of_fetch_time = replay_snapshot_at
    fetch_snapshot = replay_snapshot_at
    replay_snapshot = replay_snapshot_at
    get_replay_snapshot = replay_snapshot_at

    def replay_status_at(self, *args: Any, **kwargs: Any) -> ReplayStatus:
        value = args[0] if args else kwargs.get("timestamp") or kwargs.get("at") or kwargs.get("fetch_time")
        refresh = bool(kwargs.get("refresh", False))
        guard = self.replay_guard()
        if refresh:
            guard.table.refresh()
        return inspect_replay_at(guard.table.metadata, timestamp_ms(value))

    check_replay_at = replay_status_at
    inspect_replay_at = replay_status_at
    replay_status = replay_status_at
    check_replay = replay_status_at

    def __getattr__(self, name: str) -> Any:
        """Resolve descriptive compatibility aliases for replay operations."""
        lower = name.lower()
        replay_word = any(word in lower for word in ("replay", "guarantee", "safeguard", "assurance", "coverage"))
        window_word = any(
            word in lower
            for word in ("window", "interval", "period", "fetch", "extraction", "retrieval", "read_time", "query_time")
        )
        registration_word = "registration" in lower
        register_verb = lower.startswith(
            ("register_", "add_", "protect_", "save_", "create_", "record_", "log_", "declare_", "check_in_", "checkin_")
        )
        if replay_word and any(word in lower for word in ("enable", "activate", "init", "setup", "start")):
            return self.enable_replay_protection
        if (register_verb and (window_word or registration_word)) or (
            replay_word and window_word and "un" not in lower and "revoke" not in lower
        ):
            return self.register_replay_window
        if lower.startswith(("unregister_", "deregister_", "revoke_", "remove_", "cancel_", "delete_", "release_")) and (
            window_word or registration_word or replay_word
        ):
            return self.unregister_replay_window
        if any(word in lower for word in ("status", "inspect", "check")) and (replay_word or "fetch_time" in lower):
            return self.replay_status_at
        if (replay_word and any(word in lower for word in ("snapshot", "time", "fetch", "as_of", "as-of"))) or (
            any(word in lower for word in ("fetch_time", "extraction_time", "read_time", "query_time"))
            and any(word in lower for word in ("snapshot", "at", "for"))
        ):
            return self.replay_snapshot_at
        if any(word in lower for word in ("cleanup", "clean_up", "expire_retention", "retention_clean")):
            return self.cleanup_snapshots
        if "register" in lower and "unregister" not in lower and "deregister" not in lower:
            return self.register_replay_window
        if replay_word:
            return ReplayGuard(self.tbl)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")


def _new_autocommit_transaction(table: Table) -> Any:
    from pyiceberg.table import Transaction

    return Transaction(table, autocommit=True)


ReplayMaintenance = ReplayGuard
ReplayRegistry = ReplayGuard
ReplayWindowRegistry = ReplayGuard
ReplayProtection = ReplayGuard
ReplayGuarantee = ReplayGuard
ReplaySafeguard = ReplayGuard
ReplayAssurance = ReplayGuard
