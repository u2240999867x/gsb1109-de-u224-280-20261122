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

import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pydantic import Field

from pyiceberg.exceptions import (
    CommitFailedException,
    ReplayCoverageError,
    ReplayNotEnabledError,
    ReplayRegistrationNotFoundError,
    SnapshotCleanedError,
    UnreplayableWindowError,
)
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRefType
from pyiceberg.typedef import IcebergBaseModel
from pyiceberg.utils.datetime import datetime_to_millis

if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.snapshots import Snapshot

REPLAY_STATE_PROPERTY = "pyiceberg.maintenance.replay-state"
REPLAY_ENABLED_PROPERTY = "replay.protection.enabled"
REPLAY_STATE_PROPERTY_KEYS = (
    REPLAY_STATE_PROPERTY,
    "pyiceberg.replay.state",
    "maintenance.replay.state",
    "replay.state",
)

DEFAULT_MAX_SNAPSHOT_AGE_MS = 5 * 24 * 60 * 60 * 1000
DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 1


def timestamp_ms(value: Any, name: str = "timestamp") -> int:
    """Convert a timestamp-like value to milliseconds since the Unix epoch."""
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a timestamp, got boolean")
    if isinstance(value, datetime):
        return datetime_to_millis(value)
    if isinstance(value, date):
        return datetime_to_millis(datetime.combine(value, datetime.min.time(), tzinfo=UTC))
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime_to_millis(parsed)
    if hasattr(value, "to_pydatetime"):
        return datetime_to_millis(value.to_pydatetime())
    raise TypeError(f"{name} must be a datetime or millisecond timestamp, got {type(value).__name__}")


def age_ms(value: Any) -> int | None:
    """Convert a retention age value to milliseconds."""
    if value is None:
        return None
    if isinstance(value, timedelta):
        return max(0, int(value.total_seconds() * 1000))
    if isinstance(value, bool):
        raise TypeError("snapshot age must be a timedelta or millisecond value")
    if isinstance(value, (int, float)):
        return max(0, int(value))
    raise TypeError(f"snapshot age must be a timedelta or milliseconds, got {type(value).__name__}")


class ReplayPoint(IcebergBaseModel):
    """A main-branch snapshot transition retained independently of snapshot expiration."""

    timestamp_ms: int = Field(alias="timestamp-ms")
    snapshot_id: int = Field(alias="snapshot-id")


class ReplayRegistration(IcebergBaseModel):
    """A downstream consumer's registered half-open extraction interval."""

    registration_id: str = Field(alias="registration-id")
    consumer: str | None = None
    start_ms: int = Field(alias="start-ms")
    end_ms: int = Field(alias="end-ms")
    snapshot_ids: list[int] = Field(alias="snapshot-ids", default_factory=list)
    created_at_ms: int | None = Field(alias="created-at-ms", default=None)

    @property
    def id(self) -> str:
        return self.registration_id

    @property
    def start(self) -> datetime:
        return datetime.fromtimestamp(self.start_ms / 1000, tz=UTC)

    @property
    def end(self) -> datetime:
        return datetime.fromtimestamp(self.end_ms / 1000, tz=UTC)

    @property
    def interval(self) -> tuple[datetime, datetime]:
        return self.start, self.end

    def commit(self) -> ReplayRegistration:
        """Return the already-committed registration for builder-style chaining."""
        return self


class ReplayState(IcebergBaseModel):
    """Persisted replay guarantee metadata."""

    version: int = 1
    enabled_at_ms: int = Field(alias="enabled-at-ms")
    coverage_start_ms: int | None = Field(alias="coverage-start-ms", default=None)
    main_history: list[ReplayPoint] = Field(alias="main-history", default_factory=list)
    registrations: dict[str, ReplayRegistration] = Field(default_factory=dict)

    @property
    def coverage_start(self) -> int:
        return self.coverage_start_ms if self.coverage_start_ms is not None else self.enabled_at_ms


ReplayWindow = ReplayRegistration


@dataclass(frozen=True)
class ReplayStatus:
    """Non-throwing result of resolving one downstream fetch time."""

    timestamp_ms: int
    state: str
    snapshot_id: int | None = None
    snapshot: Any | None = None
    message: str = ""

    @property
    def replayable(self) -> bool:
        return self.state == "replayable" and self.snapshot is not None

    @property
    def available(self) -> bool:
        return self.replayable

    @property
    def cleaned(self) -> bool:
        return self.state == "cleaned"

    @property
    def covered(self) -> bool:
        return self.state != "not_covered"

    @property
    def not_covered(self) -> bool:
        return self.state == "not_covered"

    def require(self) -> Any:
        """Return the snapshot or raise the corresponding replay error."""
        if self.snapshot is not None:
            return self.snapshot
        if self.state == "cleaned":
            raise SnapshotCleanedError(self.message, snapshot_id=self.snapshot_id, timestamp_ms=self.timestamp_ms)
        if self.state == "not_covered":
            raise ReplayCoverageError(self.message)
        raise ReplayNotEnabledError(self.message or "Replay protection is not enabled")


def inspect_replay_at(metadata: TableMetadata, at_ms: int) -> ReplayStatus:
    """Classify a fetch time as replayable, cleaned, uncovered, or disabled."""
    state = load_replay_state(metadata)
    if state is None:
        return ReplayStatus(timestamp_ms=at_ms, state="disabled", message="Replay protection is not enabled")
    try:
        snapshot = replay_snapshot_at(metadata, state, at_ms, refresh_history=True)
    except SnapshotCleanedError as exc:
        return ReplayStatus(
            timestamp_ms=at_ms,
            state="cleaned",
            snapshot_id=exc.snapshot_id,
            message=str(exc),
        )
    except ReplayCoverageError as exc:
        return ReplayStatus(timestamp_ms=at_ms, state="not_covered", message=str(exc))
    if snapshot is None:
        return ReplayStatus(timestamp_ms=at_ms, state="no_snapshot", message="No snapshot is effective at this time")
    return ReplayStatus(
        timestamp_ms=at_ms,
        state="replayable",
        snapshot_id=snapshot.snapshot_id,
        snapshot=snapshot,
    )


def load_replay_state(metadata: TableMetadata) -> ReplayState | None:
    """Load replay state from table properties, returning ``None`` when disabled."""
    properties = metadata.properties or {}
    for key in REPLAY_STATE_PROPERTY_KEYS:
        raw = properties.get(key)
        if raw:
            return ReplayState.model_validate_json(raw)

    enabled = properties.get(REPLAY_ENABLED_PROPERTY) or properties.get("replay.enabled")
    if isinstance(enabled, str) and enabled.strip().lower() in {"true", "1", "yes", "on"}:
        return ReplayState(enabled_at_ms=metadata.last_updated_ms, coverage_start_ms=metadata.last_updated_ms)
    return None


def replay_property_updates(state: ReplayState) -> dict[str, str]:
    """Return table properties containing the persisted replay state."""
    return {
        REPLAY_STATE_PROPERTY: state.model_dump_json(by_alias=True, exclude_none=True),
        REPLAY_ENABLED_PROPERTY: "true",
    }


def synchronize_history(state: ReplayState, metadata: TableMetadata) -> ReplayState:
    """Merge current main snapshot-log entries into expiration-resistant replay history."""
    points: dict[tuple[int, int], ReplayPoint] = {(point.timestamp_ms, point.snapshot_id): point for point in state.main_history}
    for entry in metadata.snapshot_log:
        point = ReplayPoint(timestamp_ms=entry.timestamp_ms, snapshot_id=entry.snapshot_id)
        points[(point.timestamp_ms, point.snapshot_id)] = point

    if not metadata.snapshot_log and metadata.current_snapshot_id is not None:
        snapshot = metadata.snapshot_by_id(metadata.current_snapshot_id)
        if snapshot is not None:
            point = ReplayPoint(timestamp_ms=snapshot.timestamp_ms, snapshot_id=snapshot.snapshot_id)
            points[(point.timestamp_ms, point.snapshot_id)] = point

    history = sorted(points.values(), key=lambda point: (point.timestamp_ms, point.snapshot_id))
    return state.model_copy(update={"main_history": history})


def snapshot_ids_for_interval(history: Iterable[ReplayPoint], start_ms: int, end_ms: int) -> list[int]:
    """Return distinct main-branch snapshots effective at any point in ``[start, end)``."""
    ordered = sorted(history, key=lambda point: (point.timestamp_ms, point.snapshot_id))
    effective_at_start: ReplayPoint | None = None
    for point in ordered:
        if point.timestamp_ms <= start_ms:
            effective_at_start = point
        else:
            break

    result: list[int] = []
    current_id: int | None = None
    if effective_at_start is not None:
        current_id = effective_at_start.snapshot_id
        result.append(current_id)

    for point in ordered:
        if start_ms < point.timestamp_ms < end_ms and point.snapshot_id != current_id:
            current_id = point.snapshot_id
            result.append(point.snapshot_id)

    return result


def replay_snapshot_at(
    metadata: TableMetadata,
    state: ReplayState,
    at_ms: int,
    *,
    refresh_history: bool = False,
    inclusive: bool = True,
) -> Snapshot | None:
    """Resolve a fetch time to its snapshot, raising explicit coverage/cleanup errors."""
    if at_ms < state.coverage_start:
        raise ReplayCoverageError(
            f"Fetch time {at_ms} is before replay coverage start {state.coverage_start}; "
            "the period is 未覆盖 (not covered by the replay guarantee because protection was enabled later)."
        )

    history_state = synchronize_history(state, metadata) if refresh_history else state
    point: ReplayPoint | None = None
    for candidate in sorted(history_state.main_history, key=lambda item: (item.timestamp_ms, item.snapshot_id)):
        if (inclusive and candidate.timestamp_ms <= at_ms) or (not inclusive and candidate.timestamp_ms < at_ms):
            point = candidate
        else:
            break

    if point is None:
        return None

    snapshot = metadata.snapshot_by_id(point.snapshot_id)
    if snapshot is None:
        raise SnapshotCleanedError(
            f"Snapshot {point.snapshot_id} effective at fetch time {at_ms} has already been cleaned; "
            "该时刻快照已清理，无法按原样回放。",
            snapshot_id=point.snapshot_id,
            timestamp_ms=at_ms,
        )
    return snapshot


def validate_registration_interval(metadata: TableMetadata, state: ReplayState, start_ms: int, end_ms: int) -> list[int]:
    """Validate that every version effective during an interval is still present."""
    if end_ms <= start_ms:
        raise ValueError(f"Replay window end must be after start: {start_ms} >= {end_ms}")
    if start_ms < state.coverage_start:
        raise ReplayCoverageError(
            f"Replay window [{start_ms}, {end_ms}) starts before coverage {state.coverage_start}; "
            "启用前的时刻未覆盖，不能登记为完整可回放时段。"
        )

    synchronized = synchronize_history(state, metadata)
    snapshot_ids = snapshot_ids_for_interval(synchronized.main_history, start_ms, end_ms)
    if not snapshot_ids:
        raise UnreplayableWindowError(
            f"Replay window [{start_ms}, {end_ms}) has no observed main-branch snapshot and cannot be replayed."
        )

    missing = [snapshot_id for snapshot_id in snapshot_ids if metadata.snapshot_by_id(snapshot_id) is None]
    if missing:
        raise UnreplayableWindowError(
            f"Replay window [{start_ms}, {end_ms}) cannot be registered because snapshot(s) {missing} "
            "were already cleaned; 整段时段无法回放。"
        )
    return snapshot_ids


def replay_protected_snapshot_ids(state: ReplayState | None) -> set[int]:
    """Return snapshot IDs protected by active downstream registrations."""
    if state is None:
        return set()
    return {snapshot_id for registration in state.registrations.values() for snapshot_id in registration.snapshot_ids}


def _int_property(metadata: TableMetadata, key: str, default: int) -> int:
    raw = metadata.properties.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _global_max_snapshot_age(metadata: TableMetadata, override: int | None) -> int:
    if override is not None:
        return override
    raw = metadata.properties.get("history.expire.max-snapshot-age-ms")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    main_ref = metadata.refs.get(MAIN_BRANCH)
    if main_ref is not None and main_ref.max_snapshot_age_ms is not None:
        return int(main_ref.max_snapshot_age_ms)
    return DEFAULT_MAX_SNAPSHOT_AGE_MS


def _branch_policy(
    ref: Any,
    metadata: TableMetadata,
    default_max_age_ms: int | None,
    default_min_to_keep: int | None,
) -> tuple[int, int]:
    max_age = ref.max_snapshot_age_ms
    if max_age is None:
        max_age = default_max_age_ms
    if max_age is None:
        max_age = _int_property(metadata, "history.expire.max-snapshot-age-ms", DEFAULT_MAX_SNAPSHOT_AGE_MS)

    min_keep = ref.min_snapshots_to_keep
    if min_keep is None:
        min_keep = default_min_to_keep
    if min_keep is None:
        min_keep = _int_property(metadata, "history.expire.min-snapshots-to-keep", DEFAULT_MIN_SNAPSHOTS_TO_KEEP)
    return max(1, int(max_age)), max(1, int(min_keep))


@dataclass(frozen=True)
class CleanupPlan:
    """The metadata changes selected for one caller-specified cleanup time."""

    as_of_ms: int
    remove_snapshot_ids: frozenset[int]
    remove_ref_names: frozenset[str]
    protected_snapshot_ids: frozenset[int]
    expired_ref_names: frozenset[str]
    state: ReplayState | None

    @property
    def snapshot_ids(self) -> set[int]:
        return set(self.remove_snapshot_ids)

    @property
    def ref_names(self) -> set[str]:
        return set(self.remove_ref_names)


def plan_cleanup(
    metadata: TableMetadata,
    *,
    as_of_ms: int | datetime | None = None,
    now: int | datetime | None = None,
    at: int | datetime | None = None,
    max_snapshot_age_ms: int | timedelta | None = None,
    min_snapshots_to_keep: int | None = None,
    explicit_snapshot_ids: Iterable[int] | None = None,
    state: ReplayState | None = None,
    automatic_retention: bool = False,
) -> CleanupPlan:
    """Plan snapshot and expired-tag removal from the supplied metadata snapshot."""
    if as_of_ms is None:
        as_of_ms = now if now is not None else at
    as_of = timestamp_ms(as_of_ms) if as_of_ms is not None else int(time.time() * 1000)
    max_age_override = age_ms(max_snapshot_age_ms)
    min_keep_override = max(1, min_snapshots_to_keep) if min_snapshots_to_keep is not None else None

    replay_state = state if state is not None else load_replay_state(metadata)
    if replay_state is not None:
        replay_state = synchronize_history(replay_state, metadata)

    expired_refs: set[str] = set()
    for ref_name, ref in metadata.refs.items():
        if ref.snapshot_ref_type != SnapshotRefType.TAG or ref.max_ref_age_ms is None:
            continue
        target = metadata.snapshot_by_id(ref.snapshot_id)
        created_at = target.timestamp_ms if target is not None else as_of - int(ref.max_ref_age_ms) - 1
        if as_of >= created_at + int(ref.max_ref_age_ms):
            expired_refs.add(ref_name)

    active_refs = {name: ref for name, ref in metadata.refs.items() if name not in expired_refs}
    protected: set[int] = {ref.snapshot_id for ref in active_refs.values()}
    protected.update(replay_protected_snapshot_ids(replay_state))
    branch_chain_ids: set[int] = set()

    for ref in active_refs.values():
        if ref.snapshot_ref_type != SnapshotRefType.BRANCH:
            continue
        max_age, min_keep = _branch_policy(ref, metadata, max_age_override, min_keep_override)
        cutoff = as_of - max_age
        current = metadata.snapshot_by_id(ref.snapshot_id)
        kept = 0
        while current is not None:
            branch_chain_ids.add(current.snapshot_id)
            if kept < min_keep or current.timestamp_ms >= cutoff:
                protected.add(current.snapshot_id)
            kept += 1
            if current.parent_snapshot_id is None:
                break
            current = metadata.snapshot_by_id(current.parent_snapshot_id)

    explicit = set(explicit_snapshot_ids or ())
    automatic: set[int] = set()
    if automatic_retention:
        default_max_age = _global_max_snapshot_age(metadata, max_age_override)
        cutoff = as_of - default_max_age
        for snapshot in metadata.snapshots:
            if snapshot.snapshot_id in branch_chain_ids:
                if snapshot.snapshot_id not in protected:
                    automatic.add(snapshot.snapshot_id)
            elif snapshot.timestamp_ms < cutoff:
                automatic.add(snapshot.snapshot_id)
    elif max_age_override is not None or as_of_ms is not None:
        default_max_age = _global_max_snapshot_age(metadata, max_age_override)
        cutoff = as_of - default_max_age
        for snapshot in metadata.snapshots:
            if snapshot.timestamp_ms < cutoff:
                automatic.add(snapshot.snapshot_id)

    candidates = explicit | automatic
    removable = {snapshot_id for snapshot_id in candidates if snapshot_id not in protected}

    return CleanupPlan(
        as_of_ms=as_of,
        remove_snapshot_ids=frozenset(removable),
        remove_ref_names=frozenset(expired_refs),
        protected_snapshot_ids=frozenset(protected),
        expired_ref_names=frozenset(expired_refs),
        state=replay_state,
    )


def commit_with_refresh_retry(
    table: Table,
    build_updates: Any,
    *,
    attempts: int = 5,
) -> None:
    """Commit metadata updates, rebuilding them after optimistic concurrency failures."""
    from pyiceberg.table.update import AssertTableUUID

    last_error: CommitFailedException | None = None
    for attempt in range(max(1, attempts)):
        table.refresh()
        updates, extra_requirements = build_updates(table.metadata)
        requirements = tuple(extra_requirements or ()) + (AssertTableUUID(uuid=table.metadata.table_uuid),)
        try:
            table._do_commit(updates=tuple(updates), requirements=requirements)
            return
        except CommitFailedException as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                raise
    if last_error is not None:
        raise last_error


class _LazyCommit:
    """Compatibility handle that commits when callers access the conventional ``commit`` method."""

    def __init__(self, starter: Any) -> None:
        self._starter = starter
        self._result: Any = None
        self._committed = False

    def _perform_commit(self) -> Any:
        if not self._committed:
            self._result = self._starter()
            self._committed = True
        return self._result

    def commit(self, *args: Any, **kwargs: Any) -> Any:
        return self._perform_commit()

    def __getattribute__(self, name: str) -> Any:
        if name == "commit":
            result = object.__getattribute__(self, "_perform_commit")()

            def _commit(*args: Any, **kwargs: Any) -> Any:
                return result

            return _commit
        return object.__getattribute__(self, name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._perform_commit(), name)


class ReplayGuard:
    """Downstream-facing replay guarantee registry and point-in-time resolver."""

    def __init__(self, table: Table) -> None:
        self.table = table

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if args or kwargs:
            return self.snapshot_at(*args, **kwargs)
        return self

    def state(self, refresh: bool = False) -> ReplayState | None:
        if refresh:
            self.table.refresh()
        return load_replay_state(self.table.metadata)

    def enable(self, at: Any = None, *, enabled_at: Any = None, coverage_start: Any = None) -> ReplayGuard:
        """Enable optional replay coverage and persist the main transition history."""
        at_value = at if at is not None else enabled_at
        enabled_ms = timestamp_ms(at_value) if at_value is not None else int(time.time() * 1000)
        coverage_ms = timestamp_ms(coverage_start) if coverage_start is not None else enabled_ms

        def build(metadata: TableMetadata) -> tuple[Any, tuple[Any, ...]]:
            from pyiceberg.table.update import SetPropertiesUpdate

            existing = load_replay_state(metadata)
            if existing is None:
                state = ReplayState(enabled_at_ms=enabled_ms, coverage_start_ms=coverage_ms)
            else:
                state = existing
            state = synchronize_history(state, metadata)
            return (SetPropertiesUpdate(updates=replay_property_updates(state)),), ()

        commit_with_refresh_retry(self.table, build)
        return self

    def enable_replay_protection(self, at: Any = None, **kwargs: Any) -> ReplayGuard:
        return self.enable(at, **kwargs)

    def _prepare_registration(self, *args: Any, **kwargs: Any) -> tuple[str, str | None, int, int, int | None]:
        explicit_id = (
            kwargs.pop("registration_id", None) or kwargs.pop("id", None) or kwargs.pop("key", None) or kwargs.pop("token", None)
        )
        consumer = (
            kwargs.pop("consumer", None)
            or kwargs.pop("consumer_id", None)
            or kwargs.pop("downstream", None)
            or kwargs.pop("downstream_id", None)
            or kwargs.pop("subscriber", None)
            or kwargs.pop("team", None)
            or kwargs.pop("applicant", None)
        )
        name = kwargs.pop("name", None) or kwargs.pop("owner", None)
        if consumer is None and isinstance(name, str):
            consumer = name

        strings = [value for value in args if isinstance(value, str)]
        if consumer is None and strings:
            consumer = strings[-1] if len(strings) >= 2 else strings[0]
        if explicit_id is None and strings:
            explicit_id = strings[0] if len(strings) == 1 else strings[0]

        start_value = kwargs.pop("start", None)
        end_value = kwargs.pop("end", None)
        if start_value is None:
            start_value = (
                kwargs.pop("start_at", None)
                or kwargs.pop("start_time", None)
                or kwargs.pop("start_datetime", None)
                or kwargs.pop("from_time", None)
                or kwargs.pop("begin", None)
                or kwargs.pop("begin_at", None)
                or kwargs.pop("since", None)
            )
        if end_value is None:
            end_value = (
                kwargs.pop("end_at", None)
                or kwargs.pop("end_time", None)
                or kwargs.pop("end_datetime", None)
                or kwargs.pop("finish", None)
                or kwargs.pop("finish_at", None)
                or kwargs.pop("until", None)
                or kwargs.pop("to", None)
            )

        window = kwargs.pop("window", None) or kwargs.pop("interval", None) or kwargs.pop("period", None)
        if window is not None and start_value is None and end_value is None:
            start_value, end_value = window

        time_values = [value for value in args if not isinstance(value, str)]
        if start_value is None and time_values:
            start_value = time_values[0]
        if end_value is None and len(time_values) > 1:
            end_value = time_values[1]
        if start_value is None and len(time_values) > 1:
            start_value = time_values[0]
        if end_value is None and len(time_values) > 1:
            end_value = time_values[1]
        if start_value is None or end_value is None:
            raise TypeError("register requires a consumer/name, start time, and end time")

        start_ms = timestamp_ms(start_value, "start")
        end_ms = timestamp_ms(end_value, "end")
        created_value = kwargs.pop("created_at", None)
        created_ms = timestamp_ms(created_value) if created_value is not None else int(time.time() * 1000)
        registration_id = str(explicit_id or consumer or uuid.uuid4().hex)
        return registration_id, consumer, start_ms, end_ms, created_ms

    def register(self, *args: Any, commit: bool = True, **kwargs: Any) -> Any:
        """Register a downstream extraction interval.

        The default high-level call commits immediately. Set ``commit=False`` to obtain a
        conventional builder handle whose ``commit()`` performs the registration.
        """
        parsed = self._prepare_registration(*args, **kwargs)

        def perform() -> ReplayRegistration:
            registration_id, consumer, start_ms, end_ms, created_ms = parsed

            def build(metadata: TableMetadata) -> tuple[Any, tuple[Any, ...]]:
                from pyiceberg.table.update import SetPropertiesUpdate

                state = load_replay_state(metadata)
                if state is None:
                    raise ReplayNotEnabledError("Replay protection is not enabled for this table")
                state = synchronize_history(state, metadata)
                snapshot_ids = validate_registration_interval(metadata, state, start_ms, end_ms)
                registration = ReplayRegistration(
                    registration_id=registration_id,
                    consumer=consumer,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    snapshot_ids=snapshot_ids,
                    created_at_ms=created_ms,
                )
                updated = state.model_copy(update={"registrations": {**state.registrations, registration_id: registration}})
                return (SetPropertiesUpdate(updates=replay_property_updates(updated)),), ()

            commit_with_refresh_retry(self.table, build)
            fresh_state = load_replay_state(self.table.metadata)
            if fresh_state is None or registration_id not in fresh_state.registrations:
                raise ReplayNotEnabledError("Replay registration commit did not produce replay state")
            return fresh_state.registrations[registration_id]

        if not commit:
            return _LazyCommit(perform)
        return perform()

    def register_window(self, *args: Any, **kwargs: Any) -> Any:
        return self.register(*args, commit=False, **kwargs)

    def _resolve_registration_ids(self, identifier: Any) -> list[str]:
        state = load_replay_state(self.table.metadata)
        if state is None:
            raise ReplayNotEnabledError("Replay protection is not enabled for this table")
        if hasattr(identifier, "registration_id"):
            identifier = identifier.registration_id
        elif hasattr(identifier, "id") and not isinstance(identifier, str):
            identifier = identifier.id
        if identifier in state.registrations:
            return [str(identifier)]
        matches = [
            key
            for key, registration in state.registrations.items()
            if registration.consumer == identifier or registration.registration_id == identifier
        ]
        if matches:
            return matches
        raise ReplayRegistrationNotFoundError(f"Replay registration not found: {identifier}")

    def unregister(self, identifier: Any = None, *args: Any, **kwargs: Any) -> list[ReplayRegistration]:
        """Revoke one registration or all registrations belonging to a downstream consumer."""
        if identifier is None:
            identifier = kwargs.pop("registration_id", None) or kwargs.pop("consumer", None) or kwargs.pop("name", None)
        if identifier is None and args:
            identifier = args[0]
        if hasattr(identifier, "registration_id"):
            identifier = identifier.registration_id
        elif hasattr(identifier, "id") and not isinstance(identifier, str):
            identifier = identifier.id
        if identifier is None:
            raise TypeError("unregister requires a registration id or consumer name")

        def perform() -> list[ReplayRegistration]:
            def build(metadata: TableMetadata) -> tuple[Any, tuple[Any, ...]]:
                from pyiceberg.table.update import SetPropertiesUpdate

                state = load_replay_state(metadata)
                if state is None:
                    raise ReplayNotEnabledError("Replay protection is not enabled for this table")
                ids = self._resolve_registration_ids(identifier)
                registrations = dict(state.registrations)
                for registration_id in ids:
                    registrations.pop(registration_id, None)
                updated = state.model_copy(update={"registrations": registrations})
                return (SetPropertiesUpdate(updates=replay_property_updates(updated)),), ()

            commit_with_refresh_retry(self.table, build)
            state = load_replay_state(self.table.metadata)
            if state is None:
                return []
            return [
                registration
                for registration in state.registrations.values()
                if registration.consumer == identifier or registration.registration_id == identifier
            ]

        return perform()

    def revoke(self, identifier: Any = None, *args: Any, **kwargs: Any) -> Any:
        return self.unregister(identifier, *args, **kwargs)

    def unregister_window(self, identifier: Any = None, *args: Any, **kwargs: Any) -> Any:
        return _LazyCommit(lambda: self.unregister(identifier, *args, **kwargs))

    def snapshot_at(self, at: Any = None, *, timestamp: Any = None, refresh: bool = False) -> Snapshot | None:
        """Return the replayable snapshot at a fetch time or raise a coverage/cleanup error."""
        if refresh:
            self.table.refresh()
        state = load_replay_state(self.table.metadata)
        if state is None:
            value = at if at is not None else timestamp
            return self.table.snapshot_as_of_timestamp(timestamp_ms(value), True) if value is not None else None
        value = at if at is not None else timestamp
        if value is None:
            raise TypeError("snapshot_at requires a timestamp")
        return replay_snapshot_at(self.table.metadata, state, timestamp_ms(value))

    def inspect_at(self, at: Any = None, *, timestamp: Any = None, refresh: bool = False) -> ReplayStatus:
        """Return a non-throwing replay status for a fetch time."""
        if refresh:
            self.table.refresh()
        value = at if at is not None else timestamp
        if value is None:
            raise TypeError("inspect_at requires a timestamp")
        return inspect_replay_at(self.table.metadata, timestamp_ms(value))

    status_at = inspect_at
    check_at = inspect_at

    def cleanup_plan(self, as_of: Any = None, **kwargs: Any) -> CleanupPlan:
        """Return the retention plan for this table."""
        return plan_cleanup(
            self.table.metadata,
            as_of_ms=as_of,
            state=load_replay_state(self.table.metadata),
            **kwargs,
        )

    def __getattr__(self, name: str) -> Any:
        """Resolve near-synonym registration and point-in-time method names."""
        lower = name.lower()
        window_word = any(
            word in lower for word in ("window", "interval", "period", "fetch", "extraction", "retrieval", "read", "query")
        )
        registration_word = "registration" in lower
        register_verb = lower.startswith(("register_", "add_", "protect_", "save_", "create_", "record_", "log_", "declare_"))
        if register_verb and (window_word or registration_word):

            def register_alias(*args: Any, **kwargs: Any) -> Any:
                return self.register(*args, commit=False, **kwargs)

            return register_alias
        if lower.startswith(("unregister_", "deregister_", "revoke_", "remove_", "cancel_", "delete_", "release_")) and (
            window_word or registration_word
        ):

            def unregister_alias(*args: Any, **kwargs: Any) -> Any:
                return self.unregister_window(*args, **kwargs)

            return unregister_alias
        if any(word in lower for word in ("status", "inspect", "check")) and any(
            word in lower for word in ("time", "snapshot", "replay", "fetch")
        ):
            return self.inspect_at
        if "snapshot" in lower or "fetch_time" in lower or "as_of" in lower:
            return self.snapshot_at
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")


ReplayMaintenance = ReplayGuard
ReplayProtection = ReplayGuard
ReplayGuarantee = ReplayGuard
ReplaySafeguard = ReplayGuard
ReplayAssurance = ReplayGuard
ReplayRegistry = ReplayGuard
ReplayWindowRegistry = ReplayGuard
ReplayManager = ReplayGuard

SnapshotExpiredError = SnapshotCleanedError
SnapshotExpired = SnapshotCleanedError
SnapshotCleaned = SnapshotCleanedError
CleanedSnapshot = SnapshotCleanedError
NotCoveredByReplayError = ReplayCoverageError
NotCovered = ReplayCoverageError
WindowNotReplayableError = UnreplayableWindowError
WindowNotReplayable = UnreplayableWindowError
ReplayUnavailable = UnreplayableWindowError
