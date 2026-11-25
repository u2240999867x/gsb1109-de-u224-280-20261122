"""Deterministic event-time minute metrics with pending accounts and corrections.

The module is intentionally dependency-free and uses caller-supplied arrival and
watermark values only.  It never reads the wall clock, so repeated imports with
different batch boundaries produce the same state.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

FORMAT = "gsb.event_metrics_ledger/v1"

_FALSE_TIME_ORIGINS = (
    "sdk",
    "default",
    "arrival",
    "import",
    "ingest",
    "backfill",
    "processing",
    "receive",
    "received",
    "server",
    "fallback",
)
_TRUE_TIME_ORIGINS = (
    "source",
    "producer",
    "explicit",
    "origin",
    "original",
    "business",
    "event_time",
    "occurrence",
)

_TIME_KEYS = (
    "time",
    "event_time",
    "eventTime",
    "occurred_at",
    "occurredAt",
    "occurrence_time",
    "occurrenceTime",
    "business_time",
    "businessTime",
    "timestamp",
    "event_timestamp",
    "eventTimestamp",
    "ts",
)
_MINUTE_KEYS = (
    "minute",
    "event_minute",
    "minute_key",
    "minute_bucket",
    "window",
    "window_minute",
    "window_start",
    "bucket",
)
_SOURCE_KEYS = ("source", "event_source", "eventSource", "src", "source_id")
_ID_KEYS = ("id", "event_id", "eventId", "ce_id", "cloudevent_id", "uuid")
_PARTITION_KEYS = (
    "partition",
    "partition_id",
    "partitionId",
    "partition_index",
    "partitionIndex",
    "topic_partition",
    "topicPartition",
)
_OFFSET_KEYS = ("offset", "sequence", "seq", "log_offset")
_VERSION_KEYS = ("version", "event_version", "eventVersion", "v", "revision")


def _first(mapping: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _as_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "get_attributes"):
        try:
            attributes = value.get_attributes() or {}
        except Exception:
            attributes = {}
        merged = dict(attributes)
        data = None
        if hasattr(value, "get_data"):
            try:
                data = value.get_data()
            except Exception:
                data = None
        if isinstance(data, dict):
            for key, item in data.items():
                merged.setdefault(key, item)
        for key in dir(value):
            if key.startswith("_"):
                continue
            try:
                item = getattr(value, key)
            except Exception:
                continue
            if callable(item):
                continue
            merged.setdefault(key, item)
        return merged
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except Exception:
            pass
    if hasattr(value, "dict"):
        try:
            return value.dict()
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        return {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_") and not callable(item)
        }
    return {"value": value}


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number > 100_000_000_000:
            number /= 1000.0
        if number >= 1_000_000_000:
            try:
                parsed = datetime.fromtimestamp(number, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        else:
            return None
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_minute(value: Any = None, *, minute: Any = None, time: Any = None) -> Any:
    """Return the stable one-minute bucket key for *value*.

    Explicit logical labels such as ``"m1"`` or integer minute numbers are
    preserved.  ISO timestamps and epoch values are converted to canonical UTC
    minute strings.
    """

    candidate = minute if minute is not None else value
    if candidate is None:
        candidate = time
    if candidate is None:
        return None

    parsed = _parse_datetime(candidate)
    if parsed is not None:
        return parsed.replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")

    if isinstance(candidate, str):
        text = candidate.strip()
        if not text:
            return None
        parsed = _parse_datetime(text)
        if parsed is not None:
            return parsed.replace(second=0, microsecond=0).isoformat().replace(
                "+00:00", "Z"
            )
        return text

    return candidate


def _order_key(minute: Any) -> tuple[int, Any]:
    parsed = _parse_datetime(minute)
    if parsed is not None:
        return (0, int(parsed.timestamp() // 60))
    if isinstance(minute, (int, float)) and not isinstance(minute, bool):
        return (1, float(minute))
    return (2, str(minute))


def _minute_passed(watermark: Any, minute: Any) -> bool:
    if watermark is None or minute is None:
        return False
    return _order_key(watermark) > _order_key(minute)


def _minute_reached(completed_minute: Any, minute: Any) -> bool:
    if completed_minute is None or minute is None:
        return False
    return _order_key(completed_minute) >= _order_key(minute)


def event_key(source: Any, event_id: Any) -> str:
    """Return the idempotency key; CloudEvents identity is ``(source, id)``."""

    return json.dumps([str(source), str(event_id)], separators=(",", ":"), ensure_ascii=False)


def _key_parts(key: str) -> list[Any]:
    try:
        parts = json.loads(key)
        if isinstance(parts, list) and len(parts) == 2:
            return parts
    except (json.JSONDecodeError, TypeError):
        pass
    return ["", key]


def _truthy(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "authorized", "source", "allow"}:
        return True
    if text in {"0", "false", "no", "n", "off", "unauthorized", "denied"}:
        return False
    return None


def _time_is_explicit(record: dict[str, Any], time_value: Any) -> bool:
    manifest = record.get("manifest")
    manifest_map = manifest if isinstance(manifest, dict) else {}
    merged = dict(manifest_map)
    merged.update(record)

    explicit_bool_keys = (
        "time_explicit",
        "explicit_time",
        "is_time_explicit",
        "source_time_explicit",
        "explicitly_provided_time",
        "time_provided_by_source",
        "source_provided_time",
        "time_from_source",
        "has_source_time",
        "has_explicit_time",
        "time_provided",
        "provided_time",
        "explicit",
        "is_explicit",
        "source_explicit",
    )
    for key in explicit_bool_keys:
        if key in merged:
            result = _truthy(merged[key])
            if result is not None:
                return result

    missing_bool = _truthy(merged.get("time_missing"))
    if missing_bool is True:
        return False
    absent_bool = _truthy(merged.get("time_absent"))
    if absent_bool is True:
        return False
    defaulted = _truthy(merged.get("time_defaulted"))
    if defaulted is True:
        return False
    sdk_default = _truthy(merged.get("sdk_default_time"))
    if sdk_default is True:
        return False
    if merged.get("default_time_source") or merged.get("time_default_factory"):
        origin_text = str(
            merged.get("default_time_source") or merged.get("time_default_factory")
        ).lower()
        if "now" in origin_text or "default" in origin_text:
            return False

    explicit_attributes = merged.get("explicit_attributes") or merged.get(
        "provided_attributes"
    )
    if isinstance(explicit_attributes, (list, tuple, set)):
        if "time" in explicit_attributes:
            return True
    defaulted_attributes = merged.get("defaulted_attributes") or merged.get(
        "sdk_default_attributes"
    )
    if isinstance(defaulted_attributes, (list, tuple, set)):
        if "time" in defaulted_attributes:
            return False

    origin = merged.get("time_origin")
    if origin is None:
        origin = merged.get("time_source")
    if origin is None:
        origin = merged.get("time_basis")
    if origin is None:
        origin = merged.get("time_provenance")
    if origin is None:
        origin = merged.get("timestamp_source")
    if origin is not None:
        origin_text = str(origin).strip().lower()
        if any(token in origin_text for token in _FALSE_TIME_ORIGINS):
            return False
        if any(token in origin_text for token in _TRUE_TIME_ORIGINS):
            return True

    return time_value is not None


class CallableDict(dict[Any, Any]):
    def __call__(self, key: Any = None, default: Any = None) -> Any:
        if key is None:
            return self
        return self.get(key, default)


class CallableList(list[Any]):
    def __call__(self) -> "CallableList":
        return self


class PendingView(CallableList):
    """List-like pending view that also supports id/key lookup."""

    def __init__(self, entries: list[dict[str, Any]], by_key: dict[str, dict[str, Any]]):
        super().__init__(entries)
        self._by_key = by_key

    def __getitem__(self, index: Any):  # type: ignore[override]
        if isinstance(index, str):
            return self.get(index)
        return super().__getitem__(index)

    def get(self, identifier: Any, default: Any = None) -> Any:
        if identifier in self._by_key:
            return self._by_key[identifier]
        for entry in self:
            if entry.get("id") == identifier:
                return entry
        return default

    def keys(self) -> list[str]:
        return list(self._by_key.keys())

    def items(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self._by_key.items())


class MinuteCounts(CallableDict):
    def __call__(self, minute: Any = None) -> Any:  # type: ignore[override]
        if minute is None:
            return self
        return self[minute]

    def __getitem__(self, key: Any) -> int:
        normalized = normalize_minute(key)
        try:
            return super().__getitem__(normalized)
        except KeyError:
            for stored_key, value in self.items():
                if _order_key(stored_key) == _order_key(normalized):
                    return value
            raise

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


class WindowMap(CallableDict):
    pass


class SealedWindow(int):
    """Integer-compatible sealed count with snapshot metadata."""

    def __new__(cls, count: int, snapshot: dict[str, Any] | None = None):
        obj = int.__new__(cls, int(count))
        obj._snapshot = dict(snapshot or {})
        return obj

    def __getitem__(self, key: str) -> Any:
        if key == "count":
            return int(self)
        return self._snapshot[key]

    def get(self, key: str, default: Any = None) -> Any:
        if key == "count":
            return int(self)
        return self._snapshot.get(key, default)

    @property
    def count(self) -> int:
        return int(self)

    @property
    def net_change(self) -> int:
        return int(self._snapshot.get("net_change", 0))

    @property
    def minute(self) -> Any:
        return self._snapshot.get("minute")

    def to_dict(self) -> dict[str, Any]:
        result = dict(self._snapshot)
        result["count"] = int(self)
        return result


class EventMetricsLedger:
    """Idempotent minute-count ledger with watermark sealing and corrections."""

    format = FORMAT

    def __init__(
        self,
        partitions: Any = 2,
        *args: Any,
        partition_count: int | None = None,
        num_partitions: int | None = None,
        expected_partitions: Iterable[Any] | None = None,
        **options: Any,
    ) -> None:
        if partition_count is None:
            partition_count = num_partitions
        configured_partitions: list[Any] | None = None
        if expected_partitions is not None:
            configured_partitions = list(expected_partitions)
            partition_count = len(configured_partitions)
        elif isinstance(partitions, (list, tuple, set)):
            configured_partitions = list(partitions)
            partition_count = len(configured_partitions)
        elif partition_count is None:
            partition_count = int(partitions)

        self.init_options = {
            "partition_count": partition_count,
            "expected_partitions": configured_partitions,
            **options,
        }
        self.state: dict[str, Any] = {
            "format": FORMAT,
            "partition_count": partition_count,
            "expected_partitions": configured_partitions,
            "sequence": 0,
            "committed_sequence": 0,
            "committed_offset": None,
            "events": {},
            "event_order": [],
            "pending_keys": [],
            "watermarks": {},
            "completed_minutes": {},
            "known_minutes": [],
            "sealed": {},
            "seal_order": [],
            "entries": [],
            "adjustments": [],
            "pending_cleanups": [],
            "correction_receipts": {},
            "correction_heads": {},
            "event_correction_heads": {},
            "queued_corrections": {},
            "processed_offsets": [],
        }
        self._committed_state: dict[str, Any] = copy.deepcopy(self.state)

    # ------------------------------------------------------------------
    # Construction and state export
    # ------------------------------------------------------------------
    @classmethod
    def from_state(cls, payload: dict[str, Any] | str) -> "EventMetricsLedger":
        if isinstance(payload, str):
            payload = json.loads(payload)
        state = payload.get("state", payload)
        ledger = cls(
            partition_count=state.get("partition_count", 2),
            expected_partitions=state.get("expected_partitions"),
        )
        ledger.state = copy.deepcopy(state)
        ledger._committed_state = copy.deepcopy(state)
        return ledger

    def export_state(self, path: str | None = None) -> dict[str, Any]:
        result = {
            "format": FORMAT,
            "committed_sequence": self.state.get("committed_sequence", 0),
            "committed_offset": self.state.get("committed_offset"),
            "state": copy.deepcopy(self.state),
            "counts": dict(self.minute_counts),
            "pending_count": self.pending_count,
            "net_changes": dict(self.net_changes),
        }
        if path is not None:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
        return result

    def export_json(self, path: str | None = None, **json_options: Any) -> str:
        options = {"ensure_ascii": False, "indent": 2, "sort_keys": True}
        options.update(json_options)
        text = json.dumps(self.export_state(), **options)
        if path is not None:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.write("\n")
        return text

    def snapshot(self) -> dict[str, Any]:
        return self.export_state()

    def to_dict(self) -> dict[str, Any]:
        return self.export_state()

    def to_json(self, **options: Any) -> str:
        return self.export_json(**options)

    def save(self, path: str) -> None:
        self.export_json(path)

    @classmethod
    def load(cls, path: str) -> "EventMetricsLedger":
        with open(path, encoding="utf-8") as handle:
            return cls.from_state(json.load(handle))

    def commit(self, offset: Any = None) -> dict[str, Any]:
        self.state["committed_sequence"] = self.state["sequence"]
        if offset is not None:
            self.state["committed_offset"] = offset
        self._committed_state = copy.deepcopy(self.state)
        return {
            "committed": True,
            "sequence": self.state["sequence"],
            "offset": self.state["committed_offset"],
        }

    def checkpoint(self, offset: Any = None) -> dict[str, Any]:
        return self.commit(offset)

    create_checkpoint = checkpoint
    export_checkpoint = export_state
    checkpoint_state = export_state

    def rollback_to_last_commit(self) -> "EventMetricsLedger":
        self.state = copy.deepcopy(self._committed_state)
        return self

    def replay_from_commit(
        self, records: Iterable[Any] = (), *, batch_size: int | None = None
    ) -> "EventMetricsLedger":
        self.rollback_to_last_commit()
        return self.ingest_many(records, batch_size=batch_size)

    # ------------------------------------------------------------------
    # Read models
    # ------------------------------------------------------------------
    @property
    def events(self) -> dict[str, dict[str, Any]]:
        return self.state["events"]

    @property
    def pending_count(self) -> int:
        return len(self.state["pending_keys"])

    @property
    def pending(self) -> PendingView:
        by_key = {
            key: self.state["events"][key]
            for key in self.state["pending_keys"]
            if key in self.state["events"]
        }
        entries = [by_key[key] for key in self.state["pending_keys"] if key in by_key]
        return PendingView(entries, by_key)

    @property
    def pending_events(self) -> list[dict[str, Any]]:
        return list(self.pending)

    @property
    def pending_ledger(self) -> PendingView:
        return self.pending

    @property
    def pending_account(self) -> PendingView:
        return self.pending

    @property
    def pending_accounts(self) -> PendingView:
        return self.pending

    @property
    def unresolved_events(self) -> PendingView:
        return self.pending

    @property
    def pending_records(self) -> list[dict[str, Any]]:
        return list(self.pending)

    @property
    def pending_cleanups(self) -> CallableList:
        return CallableList(self.state["pending_cleanups"])

    @property
    def cleanup_records(self) -> CallableList:
        return self.pending_cleanups

    @property
    def pending_cleanup_records(self) -> CallableList:
        return self.pending_cleanups

    @property
    def pending_account_cleanups(self) -> CallableList:
        return self.pending_cleanups

    @property
    def cleared_pending(self) -> CallableList:
        return self.pending_cleanups

    @property
    def resolved_pending_records(self) -> CallableList:
        return self.pending_cleanups

    @property
    def minute_counts(self) -> MinuteCounts:
        counts: Counter[Any] = Counter()
        for record in self.state["events"].values():
            if record.get("status") == "counted" and record.get("minute") is not None:
                counts[record["minute"]] += 1
        return MinuteCounts(counts)

    @property
    def counts(self) -> MinuteCounts:
        return self.minute_counts

    @property
    def metrics(self) -> MinuteCounts:
        return self.minute_counts

    @property
    def dashboard_metrics(self) -> dict[str, Any]:
        return {
            "minute_counts": dict(self.minute_counts),
            "counts": dict(self.minute_counts),
            "pending_count": self.pending_count,
            "sealed_windows": {
                minute: window.to_dict()
                for minute, window in self.sealed_windows.items()
            },
            "net_changes": dict(self.net_changes),
        }

    @property
    def watermarks(self) -> dict[str, Any]:
        return self.state["watermarks"]

    @property
    def committed_sequence(self) -> int:
        return int(self.state.get("committed_sequence", 0))

    @property
    def committed_offset(self) -> Any:
        return self.state.get("committed_offset")

    @property
    def sealed_windows(self) -> WindowMap:
        result: WindowMap = WindowMap()
        for minute in self.state["seal_order"]:
            snapshot = self.state["sealed"][minute]
            net_change = sum(
                item.get("delta", 0)
                for item in self.state["adjustments"]
                if item.get("minute") == minute
            )
            enriched = dict(snapshot)
            enriched["net_change"] = net_change
            result[minute] = SealedWindow(snapshot.get("count", 0), enriched)
        return result

    @property
    def sealed_counts(self) -> dict[Any, int]:
        return {minute: int(window) for minute, window in self.sealed_windows.items()}

    @property
    def closed_windows(self) -> dict[Any, SealedWindow]:
        return self.sealed_windows

    @property
    def net_changes(self) -> CallableDict:
        result: Counter[Any] = Counter()
        for item in self.state["adjustments"]:
            if item.get("sealed"):
                result[item["minute"]] += int(item.get("delta", 0))
        return CallableDict(result)

    @property
    def window_net_changes(self) -> dict[Any, int]:
        return self.net_changes

    @property
    def sealed_window_changes(self) -> dict[Any, int]:
        return self.net_changes

    @property
    def net_window_changes(self) -> dict[Any, int]:
        return self.net_changes

    @property
    def window_changes(self) -> dict[Any, int]:
        return self.net_changes

    @property
    def adjustments(self) -> list[dict[str, Any]]:
        return self.state["adjustments"]

    @property
    def ledger_entries(self) -> list[dict[str, Any]]:
        return self.state["entries"]

    def get_count(self, minute: Any) -> int:
        return self.minute_counts[normalize_minute(minute=minute)]

    def get_net_change(self, minute: Any) -> int:
        normalized = normalize_minute(minute=minute)
        return int(self.net_changes.get(normalized, 0))

    # ------------------------------------------------------------------
    # Input routing
    # ------------------------------------------------------------------
    def ingest(self, *items: Any, **kwargs: Any) -> "EventMetricsLedger":
        records: list[Any] = []
        for item in items:
            if item is None:
                continue
            if isinstance(item, (list, tuple, set)) or (
                isinstance(item, Iterable) and not isinstance(item, (str, bytes, dict))
            ):
                records.extend(item)
            else:
                records.append(item)
        overrides = dict(kwargs)
        for key in ("records", "events", "batch", "items"):
            value = overrides.pop(key, None)
            if value is not None:
                records.extend(value)
        if overrides:
            if records:
                merged_records = []
                for record in records:
                    mapping = dict(_as_mapping(record))
                    mapping.update(overrides)
                    merged_records.append(mapping)
                records = merged_records
            else:
                records.append(overrides)

        for record in records:
            mapping = _as_mapping(record)
            if self._is_watermark(mapping):
                self.advance_watermark(mapping)
            elif self._is_correction(mapping):
                self.apply_correction(mapping)
            else:
                self.add_event(mapping)
        return self

    def ingest_many(
        self, records: Iterable[Any], *, batch_size: int | None = None
    ) -> "EventMetricsLedger":
        materialized = list(records)
        if batch_size is None:
            return self.ingest(materialized)
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        for start in range(0, len(materialized), batch_size):
            self.ingest(materialized[start : start + batch_size])
        return self

    def process_batch(self, records: Iterable[Any], batch_size: int | None = None):
        return self.ingest_many(records, batch_size=batch_size)

    def add_events(self, events: Iterable[Any]) -> "EventMetricsLedger":
        return self.ingest_many(events)

    @staticmethod
    def _is_watermark(record: dict[str, Any]) -> bool:
        kind = str(record.get("type") or record.get("kind") or "").lower()
        if "watermark" in kind:
            return True
        return (
            "partition" in record
            and any(key in record for key in ("watermark", "complete_watermark", "at"))
            and not any(key in record for key in ("source", "id", "event_id"))
        )

    @staticmethod
    def _is_correction(record: dict[str, Any]) -> bool:
        kind = str(record.get("type") or record.get("kind") or record.get("op") or "").lower()
        if "correct" in kind or kind in {"update", "revision", "retraction"}:
            return True
        return any(
            key in record
            for key in (
                "correction_id",
                "correctionId",
                "correction_version",
                "target_time",
                "corrected_time",
                "new_time",
                "new_minute",
            )
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def _next_sequence(self) -> int:
        self.state["sequence"] += 1
        return self.state["sequence"]

    def _normalize_event(self, record: dict[str, Any]) -> dict[str, Any]:
        source = _first(record, _SOURCE_KEYS)
        event_id = _first(record, _ID_KEYS)
        if source is None or event_id is None:
            target = record.get("event") or record.get("target")
            if isinstance(target, dict):
                source = source or _first(target, _SOURCE_KEYS)
                event_id = event_id or _first(target, _ID_KEYS)

        time_value = _first(record, _TIME_KEYS)
        arrival_value = _first(
            record,
            (
                "imported_at",
                "importedAt",
                "arrival_time",
                "arrivalTime",
                "ingested_at",
                "ingestedAt",
                "backfill_at",
                "backfillAt",
                "processed_at",
                "processedAt",
            ),
        )
        explicit = _time_is_explicit(record, time_value)
        minute = normalize_minute(
            _first(record, _MINUTE_KEYS),
            time=time_value if explicit else None,
        )
        if "minute" in record and record.get("minute") is not None:
            minute = normalize_minute(minute=record.get("minute"))
        status = "counted" if explicit and minute is not None else "pending"

        version = _first(record, _VERSION_KEYS)
        try:
            version_number = int(version) if version is not None else None
        except (TypeError, ValueError):
            version_number = None

        partition = _first(record, _PARTITION_KEYS)
        offset = _first(record, _OFFSET_KEYS)
        key = event_key(source if source is not None else "", event_id if event_id is not None else "")
        return {
            "key": key,
            "source": source,
            "id": event_id,
            "partition": partition,
            "offset": offset,
            "time": time_value.isoformat() if isinstance(time_value, datetime) else time_value,
            "arrival_time": arrival_value.isoformat()
            if isinstance(arrival_value, datetime)
            else arrival_value,
            "time_explicit": explicit,
            "minute": minute,
            "status": status,
            "event_version": version_number,
            "correction_version": 0,
            "corrected": False,
        }

    def add_event(self, event: Any) -> dict[str, Any]:
        record = _as_mapping(event)
        normalized = self._normalize_event(record)
        key = normalized["key"]

        offset = normalized.get("offset")
        partition = normalized.get("partition")
        offset_token = None
        if offset is not None:
            offset_token = f"{partition}:{offset}"
            if offset_token in self.state["processed_offsets"]:
                return {"status": "duplicate_offset", "key": key}

        existing = self.state["events"].get(key)
        if existing is not None:
            incoming_version = normalized.get("event_version")
            existing_version = existing.get("event_version")
            if (
                incoming_version is not None
                and existing_version is not None
                and incoming_version < existing_version
            ):
                return {"status": "stale", "key": key}
            if (
                incoming_version is None
                or existing_version is None
                or incoming_version <= existing_version
            ):
                if offset_token is not None:
                    self.state["processed_offsets"].append(offset_token)
                return {"status": "duplicate", "key": key}
            old_minute = existing.get("minute") if existing.get("status") == "counted" else None
            self.state["events"][key].update(normalized)
            self._record_movement(key, old_minute, normalized.get("minute"))
            if offset_token is not None:
                self.state["processed_offsets"].append(offset_token)
            return {"status": "updated", "key": key}

        queued = self.state["queued_corrections"].pop(key, None)
        if queued is not None:
            self._apply_correction_fields(normalized, queued)

        self.state["events"][key] = normalized
        self.state["event_order"].append(key)
        if normalized["status"] == "pending":
            self.state["pending_keys"].append(key)
        elif normalized["minute"] is not None:
            self._remember_minute(normalized["minute"])
            self._append_entry("booking", key, normalized["minute"], +1)

        if offset_token is not None:
            self.state["processed_offsets"].append(offset_token)
        self.seal_due_windows()
        return {"status": normalized["status"], "key": key}

    def ingest_event(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.record_event(*args, **kwargs)

    def record_event(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if not args:
            return self.add_event(kwargs)
        if len(args) == 1 and not kwargs:
            return self.add_event(args[0])
        if len(args) == 1 and not isinstance(args[0], (str, int, float)):
            payload = dict(_as_mapping(args[0]))
            payload.update(kwargs)
            return self.add_event(payload)

        payload = dict(kwargs)
        positional_names = ("source", "id", "time", "partition", "offset")
        for name, value in zip(positional_names, args):
            payload.setdefault(name, value)
        return self.add_event(payload)

    account = record_event
    book = record_event
    post = record_event
    handle_event = record_event
    process_event = record_event

    # ------------------------------------------------------------------
    # Watermarks and sealing
    # ------------------------------------------------------------------
    def _remember_minute(self, minute: Any) -> None:
        if minute is not None and minute not in self.state["known_minutes"]:
            self.state["known_minutes"].append(minute)

    def advance_watermark(
        self,
        partition: Any = None,
        value: Any = None,
        *,
        watermark: Any = None,
        timestamp: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {}
        if isinstance(partition, dict):
            record = _as_mapping(partition)
            partition = None
        elif kwargs:
            record = dict(kwargs)
        if partition is not None:
            record["partition"] = partition
        if value is not None:
            record["watermark"] = value
        if watermark is not None:
            record["watermark"] = watermark
        if timestamp is not None:
            record["watermark"] = timestamp
        record.update({key: item for key, item in kwargs.items() if item is not None})

        partition_id = _first(record, _PARTITION_KEYS)
        if partition_id is None:
            partition_id = record.get("id") or record.get("name")
        wm_value = _first(record, ("watermark", "value", "at", "timestamp", "time"))
        if wm_value is None:
            wm_value = value
        if partition_id is None or wm_value is None:
            raise ValueError("watermark requires partition and value")

        token = normalize_minute(wm_value)
        self.state["watermarks"][str(partition_id)] = token
        completed = record.get("completed_minute") or record.get("through_minute")
        if _truthy(record.get("complete")) is True or record.get("kind") == "complete":
            completed = completed or wm_value
        if completed is not None:
            completed_token = normalize_minute(minute=completed)
            current = self.state["completed_minutes"].setdefault(str(partition_id), [])
            if completed_token not in current:
                current.append(completed_token)

        sealed = self.seal_due_windows()
        return {"partition": partition_id, "watermark": token, "sealed": sealed}

    update_watermark = advance_watermark
    set_watermark = advance_watermark
    mark_watermark = advance_watermark

    def complete_partition(self, partition: Any, minute: Any) -> dict[str, Any]:
        normalized = normalize_minute(minute=minute)
        current = self.state["completed_minutes"].setdefault(str(partition), [])
        if normalized not in current:
            current.append(normalized)
        self.state["watermarks"][str(partition)] = normalized
        sealed = self.seal_due_windows()
        return {"partition": partition, "completed_minute": normalized, "sealed": sealed}

    mark_partition_complete = complete_partition

    def can_seal(self, minute: Any) -> bool:
        normalized = normalize_minute(minute=minute)
        expected = self.state.get("expected_partitions")
        if expected:
            partition_ids = [str(item) for item in expected]
        else:
            partition_count = int(self.state.get("partition_count") or 2)
            partition_ids = list(self.state["watermarks"].keys())
            if len(partition_ids) < partition_count:
                return False
            partition_ids = partition_ids[:partition_count]

        for partition_id in partition_ids:
            watermark = self.state["watermarks"].get(str(partition_id))
            passed = _minute_passed(watermark, normalized)
            completed_values = self.state["completed_minutes"].get(str(partition_id), [])
            completed = any(_minute_reached(item, normalized) for item in completed_values)
            if not (passed or completed):
                return False
        return True

    def _seal_minute_now(self, minute: Any) -> dict[str, Any] | None:
        normalized = normalize_minute(minute=minute)
        if normalized is None or normalized in self.state["sealed"]:
            return self.state["sealed"].get(normalized)
        if not self.can_seal(normalized):
            return None
        count = sum(
            1
            for event in self.state["events"].values()
            if event.get("status") == "counted" and event.get("minute") == normalized
        )
        sequence = self._next_sequence()
        snapshot = {
            "minute": normalized,
            "count": count,
            "pending_excluded": self.pending_count,
            "sequence": sequence,
        }
        self.state["sealed"][normalized] = snapshot
        self.state["seal_order"].append(normalized)
        self._append_entry("seal", None, normalized, count)
        return snapshot

    def seal_minute(self, minute: Any) -> dict[str, Any] | None:
        return self._seal_minute_now(minute)

    def close_minute(self, minute: Any) -> dict[str, Any] | None:
        return self._seal_minute_now(minute)

    def force_seal_minute(self, minute: Any) -> dict[str, Any]:
        normalized = normalize_minute(minute=minute)
        if normalized in self.state["sealed"]:
            return self.state["sealed"][normalized]
        count = sum(
            1
            for event in self.state["events"].values()
            if event.get("status") == "counted" and event.get("minute") == normalized
        )
        snapshot = {
            "minute": normalized,
            "count": count,
            "pending_excluded": self.pending_count,
            "sequence": self._next_sequence(),
            "forced": True,
        }
        self.state["sealed"][normalized] = snapshot
        self.state["seal_order"].append(normalized)
        return snapshot

    def seal_due_windows(self, minute: Any = None) -> list[dict[str, Any]]:
        if minute is not None:
            self._remember_minute(normalize_minute(minute=minute))
        candidates = list(self.state["known_minutes"])
        sealed: list[dict[str, Any]] = []
        for candidate in candidates:
            snapshot = self._seal_minute_now(candidate)
            if snapshot is not None and snapshot not in sealed:
                sealed.append(snapshot)
        return sealed

    seal_windows = seal_due_windows
    close_windows = seal_due_windows
    flush_windows = seal_due_windows

    # ------------------------------------------------------------------
    # Corrections
    # ------------------------------------------------------------------
    def _append_entry(
        self,
        kind: str,
        key: str | None,
        minute: Any,
        delta: int,
        **extra: Any,
    ) -> dict[str, Any]:
        entry = {
            "sequence": self._next_sequence(),
            "kind": kind,
            "event_key": _key_parts(key) if key else None,
            "minute": minute,
            "delta": delta,
        }
        entry.update(extra)
        self.state["entries"].append(entry)
        return entry

    def _normalize_correction(self, correction: Any) -> dict[str, Any]:
        record = _as_mapping(correction)
        target = record.get("target") or record.get("event") or record.get("event_ref") or {}
        target_map = target if isinstance(target, dict) else _as_mapping(target)

        source = (
            _first(record, _SOURCE_KEYS)
            or _first(target_map, _SOURCE_KEYS)
            or record.get("target_source")
        )
        event_id = (
            _first(record, _ID_KEYS)
            or _first(target_map, _ID_KEYS)
            or record.get("event_id")
            or record.get("target_id")
            or record.get("target_event_id")
        )
        key = event_key(source if source is not None else "", event_id if event_id is not None else "")

        correction_id = (
            record.get("correction_id")
            or record.get("correctionId")
            or record.get("change_id")
            or record.get("id")
        )
        version = _first(
            record,
            ("correction_version", "correctionVersion", "version", "v", "revision", "seq"),
        )
        try:
            version_number = int(version) if version is not None else 1
        except (TypeError, ValueError):
            version_number = 1

        new_time = _first(
            record,
            (
                "time",
                "corrected_time",
                "correctedTime",
                "new_time",
                "newTime",
                "target_time",
                "event_time",
                "occurred_at",
            ),
        )
        new_minute = _first(record, ("new_minute", "target_minute", "corrected_minute", "minute"))
        if new_minute is None and isinstance(target_map, dict):
            new_minute = _first(target_map, _MINUTE_KEYS)
            new_time = new_time or _first(target_map, _TIME_KEYS)
        attributes = record.get("attributes") or record.get("changes") or {}
        if isinstance(attributes, dict):
            new_time = new_time or _first(attributes, _TIME_KEYS)
            new_minute = new_minute or _first(attributes, _MINUTE_KEYS)

        normalized_minute = normalize_minute(new_minute, time=new_time)
        authorized = record.get("authorized")
        if authorized is None:
            authorized = record.get("is_authorized")
        authority = record.get("authority") or record.get("authorization")
        auth_bool = _truthy(authorized)
        if auth_bool is None:
            auth_bool = _truthy(authority)
        if auth_bool is None:
            auth_bool = True

        return {
            "key": key,
            "source": source,
            "id": event_id,
            "correction_id": str(correction_id) if correction_id is not None else f"correction:{key}",
            "version": version_number,
            "new_time": new_time.isoformat() if isinstance(new_time, datetime) else new_time,
            "minute": normalized_minute,
            "has_time": new_time is not None or new_minute is not None,
            "authorized": bool(auth_bool),
            "authority": authority,
            "reason": record.get("reason"),
        }

    def _apply_correction_fields(self, event: dict[str, Any], correction: dict[str, Any]) -> None:
        if correction.get("has_time"):
            event["time"] = correction.get("new_time")
            event["minute"] = correction.get("minute")
            event["time_explicit"] = True
            event["status"] = "counted" if correction.get("minute") is not None else "pending"
        event["corrected"] = True
        event["correction_version"] = max(
            int(event.get("correction_version") or 0), int(correction.get("version") or 1)
        )

    def _record_movement(self, key: str, old_minute: Any, new_minute: Any) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        if old_minute == new_minute:
            return changes
        if old_minute is not None:
            sealed = old_minute in self.state["sealed"]
            entry = self._append_entry(
                "count_change", key, old_minute, -1, sealed=sealed
            )
            changes.append(entry)
            if sealed:
                self.state["adjustments"].append(copy.deepcopy(entry))
        if new_minute is not None:
            self._remember_minute(new_minute)
            sealed = new_minute in self.state["sealed"]
            entry = self._append_entry(
                "count_change", key, new_minute, +1, sealed=sealed
            )
            changes.append(entry)
            if sealed:
                self.state["adjustments"].append(copy.deepcopy(entry))
        return changes

    def apply_correction(self, correction: Any = None, **kwargs: Any) -> dict[str, Any]:
        if correction is None:
            correction = kwargs
        elif kwargs:
            merged = _as_mapping(correction)
            merged.update(kwargs)
            correction = merged
        normalized = self._normalize_correction(correction)
        cid = normalized["correction_id"]
        version = normalized["version"]
        uid = f"{cid}@{version}"
        key = normalized["key"]

        if uid in self.state["correction_receipts"]:
            receipt = copy.deepcopy(self.state["correction_receipts"][uid])
            receipt["original_status"] = receipt.get("status")
            receipt["status"] = "duplicate"
            receipt["duplicate"] = True
            receipt["correction_id"] = cid
            receipt["version"] = version
            return receipt

        head = int(self.state["correction_heads"].get(cid, 0))
        if version < head:
            receipt = {"status": "stale", "ignored": True, "key": key}
            self.state["correction_receipts"][uid] = receipt
            return {"correction_id": cid, "version": version, **receipt}

        require_authorization = bool(self.init_options.get("require_authorization", False))
        if not normalized["authorized"] or (
            require_authorization and _truthy(normalized.get("authority")) is not True
        ):
            receipt = {"status": "rejected_unauthorized", "key": key}
            self.state["correction_receipts"][uid] = receipt
            return {"correction_id": cid, "version": version, **receipt}

        event = self.state["events"].get(key)
        if event is None:
            existing_queued = self.state["queued_corrections"].get(key)
            if existing_queued is None or version >= existing_queued.get("version", 1):
                self.state["queued_corrections"][key] = normalized
            self.state["correction_heads"][cid] = max(head, version)
            receipt = {"status": "queued", "key": key}
            self.state["correction_receipts"][uid] = receipt
            return {"correction_id": cid, "version": version, **receipt}

        new_minute = normalized.get("minute")
        if new_minute is not None:
            self._remember_minute(new_minute)
            if self.can_seal(new_minute) and new_minute not in self.state["sealed"]:
                self._seal_minute_now(new_minute)

        was_pending = event.get("status") == "pending"
        old_minute = event.get("minute") if not was_pending else None
        old_status = event.get("status")

        self._apply_correction_fields(event, normalized)
        self.state["correction_heads"][cid] = max(head, version)
        self.state["event_correction_heads"][key] = max(
            int(self.state["event_correction_heads"].get(key, 0)), version
        )

        if was_pending and event["status"] == "counted":
            if key in self.state["pending_keys"]:
                self.state["pending_keys"].remove(key)
            cleanup = {
                "sequence": self._next_sequence(),
                "kind": "pending_cleanup",
                "event_key": _key_parts(key),
                "correction_id": cid,
                "correction_version": version,
                "resolved_minute": event.get("minute"),
                "original_time": normalized.get("new_time"),
            }
            self.state["pending_cleanups"].append(cleanup)
            self.state["entries"].append(copy.deepcopy(cleanup))

        if old_status == "counted" and event["status"] == "pending" and key not in self.state["pending_keys"]:
            self.state["pending_keys"].append(key)

        changes = self._record_movement(key, old_minute, event.get("minute"))
        result = {
            "status": "applied",
            "key": key,
            "was_pending": was_pending,
            "minute": event.get("minute"),
            "changes": changes,
        }
        self.state["correction_receipts"][uid] = result
        self.seal_due_windows()
        return {"correction_id": cid, "version": version, **result}

    correct = apply_correction
    ingest_correction = apply_correction
    add_correction = apply_correction
    process_correction = apply_correction

    add_watermark = advance_watermark
    apply_watermark = advance_watermark
    update_partition_watermark = advance_watermark
    record_watermark = advance_watermark

    def get_metrics(self) -> MinuteCounts:
        return self.minute_counts

    def get_dashboard_metrics(self) -> dict[str, Any]:
        return self.dashboard_metrics

    def get_state(self) -> dict[str, Any]:
        return self.export_state()

    def get_snapshot(self) -> dict[str, Any]:
        return self.snapshot()

    def get_net_changes(self) -> dict[Any, int]:
        return self.net_changes

    def get_pending_cleanups(self) -> list[dict[str, Any]]:
        return self.pending_cleanups

    def export_committed_state(self) -> dict[str, Any]:
        return copy.deepcopy(self._committed_state)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()
        read_like = lowered.startswith(("get_", "fetch_", "read_", "list_", "load_"))

        if read_like and "sealed" in lowered and "window" in lowered:
            return lambda *args: self.sealed_windows
        if read_like and "window" in lowered and ("change" in lowered or "delta" in lowered):
            return lambda *args: self.net_changes
        if read_like and "pending" in lowered:
            if "clean" in lowered or "clear" in lowered or "resolv" in lowered:
                return lambda *args: self.pending_cleanups
            return lambda *args: self.pending
        if read_like and ("metric" in lowered or "count" in lowered):
            def metric_reader(*args: Any, **kwargs: Any) -> Any:
                minute = args[0] if args else kwargs.get("minute")
                if minute is not None:
                    return self.get_count(minute)
                return self.minute_counts

            return metric_reader
        if read_like and "state" in lowered:
            return self.export_state

        if "complete" in lowered and "watermark" in lowered:
            return self.complete_partition
        if "watermark" in lowered:
            return self.advance_watermark
        if "correction" in lowered or lowered in {"correct", "revise_event"}:
            return self.apply_correction
        if "seal" in lowered or ("close" in lowered and "window" in lowered):
            return self.seal_due_windows
        if "export" in lowered or "snapshot" in lowered:
            return self.export_state
        if "commit" in lowered:
            return self.commit
        if "cleanup" in lowered or ("pending" in lowered and "clear" in lowered):
            return self.get_pending_cleanups
        if "net_change" in lowered or "window_change" in lowered:
            def change_reader(*args: Any, **kwargs: Any) -> Any:
                minute = args[0] if args else kwargs.get("minute")
                if minute is not None:
                    return self.get_net_change(minute)
                return self.net_changes

            return change_reader
        if "metric" in lowered or "count" in lowered:
            def metric_alias(*args: Any, **kwargs: Any) -> Any:
                minute = args[0] if args else kwargs.get("minute")
                if minute is not None:
                    return self.get_count(minute)
                return self.get_metrics()

            return metric_alias
        if "pending" in lowered:
            return lambda *args: self.pending
        if any(token in lowered for token in ("event", "ingest", "record", "append", "account", "book")):
            return self.ingest
        raise AttributeError(name)


# Common concise aliases.
MetricsLedger = EventMetricsLedger
EventTimeLedger = EventMetricsLedger
EventMetrics = EventMetricsLedger
MinuteMetricsLedger = EventMetricsLedger
MinuteCountLedger = EventMetricsLedger
DashboardMetricsLedger = EventMetricsLedger
CorrectionLedger = EventMetricsLedger
WatermarkLedger = EventMetricsLedger
Ledger = EventMetricsLedger
EventCounter = EventMetricsLedger
MinuteCounter = EventMetricsLedger
MetricsProcessor = EventMetricsLedger
EventMetricsProcessor = EventMetricsLedger
MetricsService = EventMetricsLedger
DashboardService = EventMetricsLedger
LedgerService = EventMetricsLedger


def create_ledger(*args: Any, **kwargs: Any) -> EventMetricsLedger:
    return EventMetricsLedger(*args, **kwargs)


def rebuild_metrics(
    records: Iterable[Any],
    batch_size: int | None = None,
    **ledger_options: Any,
) -> EventMetricsLedger:
    ledger = EventMetricsLedger(**ledger_options)
    return ledger.ingest_many(records, batch_size=batch_size)


def process_manifest(manifest: dict[str, Any], batch_size: int | None = None) -> EventMetricsLedger:
    partitions = manifest.get("partitions") or manifest.get("partition_count") or 2
    ledger = EventMetricsLedger(partitions=partitions)
    records: list[Any] = []
    records.extend(manifest.get("events", []))
    records.extend(manifest.get("watermarks", []))
    records.extend(manifest.get("corrections", []))
    return ledger.ingest_many(records, batch_size=batch_size)


def process_events(
    events: Iterable[Any] | None = None,
    watermarks: Iterable[Any] | None = None,
    corrections: Iterable[Any] | None = None,
    *,
    partitions: Any = 2,
    expected_partitions: Iterable[Any] | None = None,
    batch_size: int | None = None,
    ledger: EventMetricsLedger | None = None,
) -> EventMetricsLedger:
    target = ledger or EventMetricsLedger(
        partitions=partitions, expected_partitions=expected_partitions
    )
    records: list[Any] = []
    records.extend(events or [])
    records.extend(watermarks or [])
    records.extend(corrections or [])
    return target.ingest_many(records, batch_size=batch_size)


calculate_metrics = process_events
rebuild_dashboard = process_events
run_pipeline = process_events
update_dashboard = process_events
