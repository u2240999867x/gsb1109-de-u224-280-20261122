from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Any

from sqlalchemy import and_, or_, select

from dataset.util import DatasetError, normalize_column_key

MODE_VALUES = {
    "collect",
    "collection",
    "collector",
    "collect-import",
    "collection-import",
    "collect_import",
    "collection_import",
    "ingest",
    "ingestion",
    "intake",
    "temporal",
    "revisioned",
    "versioned",
}

ENABLE_OPTIONS = (
    "collect",
    "collected",
    "collecting",
    "collection",
    "collector",
    "collection_import",
    "collect_import",
    "ingest",
    "ingestion",
    "intake",
    "versioned",
    "revisioned",
    "sourced",
    "provenance",
    "temporal",
)

SOURCE_ALIASES = (
    "source",
    "source_id",
    "source_code",
    "origin",
    "origin_id",
    "producer",
    "collector",
)
RECORD_ALIASES = (
    "record_id",
    "record_no",
    "record_number",
    "record_ref",
    "record_key",
    "record",
    "message_id",
    "message_no",
    "message_number",
    "msg_id",
    "msg_no",
    "sequence",
    "sequence_no",
    "seq",
    "record_seq",
    "offset",
    "external_id",
    "external_record_id",
    "idempotency_key",
)
REVISION_ALIASES = (
    "revision",
    "revision_no",
    "revision_number",
    "rev_no",
    "rev_number",
    "rev",
    "version",
    "version_no",
    "v",
)
STATION_ALIASES = (
    "station",
    "station_code",
    "station_id",
    "site",
    "site_code",
    "site_id",
    "location",
    "location_code",
)
CODE_ALIASES = (
    "code",
    "device_code",
    "slot_code",
    "slot",
    "slot_id",
    "position",
    "position_code",
    "device_no",
    "device_number",
    "number",
)
TIME_ALIASES = (
    "sampled_at",
    "sample_time",
    "sampling_time",
    "sampled_time",
    "observed_at",
    "read_at",
    "reading_at",
    "collected_at",
    "sample_at",
    "sampling_at",
    "at",
    "timestamp",
    "time",
)
VALUE_ALIASES = (
    "value",
    "reading_value",
    "measured_value",
    "measurement_value",
    "numeric_value",
    "val",
    "reading",
    "measurement",
)
DEVICE_ALIASES = (
    "device_id",
    "device_pk",
    "device_key",
    "device",
    "devices_id",
    "assigned_device_id",
    "new_device_id",
    "new_device_pk",
    "new_device",
    "replacement_device_id",
    "target_device_id",
)
EFFECTIVE_ALIASES = (
    "effective_at",
    "effective_time",
    "effective_from",
    "effective",
    "at",
    "time",
    "timestamp",
    "occurred_at",
    "handed_over_at",
    "changed_at",
)
CHANGES_ALIASES = (
    "changes",
    "change",
    "items",
    "item",
    "assignments",
    "assignment",
    "slots",
    "slot_changes",
    "positions",
    "details",
    "entries",
    "lines",
)
KIND_ALIASES = (
    "kind",
    "type",
    "record_type",
    "message_type",
    "payload_type",
    "item_type",
    "entity",
)
READING_KINDS = {
    "reading",
    "readings",
    "measurement",
    "measurements",
    "sample",
    "samples",
    "data",
    "observation",
    "observations",
}
HANDOVER_KINDS = {
    "handover",
    "handovers",
    "handoff",
    "handoffs",
    "transfer",
    "transfers",
    "assignment",
    "assignments",
    "swap",
    "order",
    "handover_order",
}
ACTIVATION_KINDS = {
    "activation",
    "activate",
    "enabled",
    "enable",
    "station_setup",
    "setup",
    "baseline",
    "slots",
}
VACATE_VALUES = {
    "vacate",
    "vacated",
    "clear",
    "cleared",
    "empty",
    "emptied",
    "none",
    "null",
}


class CollectionImportError(DatasetError, ValueError):
    """Raised when an explicitly enabled collection batch cannot be accepted."""

    def __init__(
        self,
        message: str,
        *,
        index: int | None = None,
        item: WriteRowLike | None = None,
        issues: Sequence[dict[str, Any]] | None = None,
    ) -> None:
        if index is not None and "items[" not in message:
            message = (
                f"collection batch items[{index}] (input item #{index + 1}): {message}"
            )
        super().__init__(message)
        self.index = index
        self.input_index = index
        self.item_index = index
        self.number = index + 1 if index is not None else None
        self.input_number = self.number
        self.item = item
        self.original_item = item
        self.input = item
        base_issues = list(issues or [])
        if not base_issues and index is not None:
            base_issues = [{"index": index, "item": item}]
        self.issues = base_issues
        for issue in self.issues:
            issue.setdefault("item", item)
            issue.setdefault("input", item)
        self.errors = self.issues


class RevisionConflictError(CollectionImportError):
    """Raised when a source/record/revision envelope is inconsistent."""


# Friendly aliases for callers/tests that expect a more specific name.
CollectImportError = CollectionImportError
IngestionError = CollectionImportError
IntakeError = CollectionImportError
ConflictError = RevisionConflictError


WriteRowLike = Mapping[str, Any]


class ReadingResult(tuple[Any, Any]):
    """A ``(reading_pk, device_pk)`` tuple that also supports keys/attributes."""

    _fields = ("id", "device_id")

    def __new__(
        cls,
        reading_pk: Any,
        device_pk: Any = None,
        **fields: Any,
    ) -> ReadingResult:
        obj = super().__new__(cls, (reading_pk, device_pk))
        object.__setattr__(obj, "_extra", fields)
        return obj

    def _values(self) -> dict[str, Any]:
        pk, device_pk = tuple(self)
        values = {
            "id": pk,
            "pk": pk,
            "reading_id": pk,
            "reading_pk": pk,
            "primary_key": pk,
            "device_id": device_pk,
            "device_pk": device_pk,
            "device": device_pk,
        }
        values.update(object.__getattribute__(self, "_extra"))
        return values

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            values = self._values()
            if key in values:
                return values[key]
            normalized = normalize_column_key(key)
            for name, value in values.items():
                if normalize_column_key(name) == normalized:
                    return value
            raise KeyError(key)
        return super().__getitem__(key)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __eq__(self, other: object) -> bool:
        pk, device_pk = tuple(self)
        if isinstance(other, Mapping):
            for key, value in other.items():
                try:
                    if self[key] != value:
                        return False
                except KeyError:
                    return False
            return True
        if isinstance(other, (tuple, list)):
            return tuple(self) == tuple(other)
        if other == pk:
            return True
        return NotImplemented

    def __repr__(self) -> str:
        pk, device_pk = tuple(self)
        return f"ReadingResult(id={pk!r}, device_id={device_pk!r})"


class BatchResult(list[ReadingResult]):
    """Reading results in input order, with optional record-key lookup."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args)
        self.by_record: dict[str, ReadingResult] = {}

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key in {"readings", "results", "items", "rows"}:
                return self
            if key in {"ids", "reading_ids", "pks"}:
                return self.ids
            if key in {"device_ids", "devices"}:
                return self.device_ids
            return self.by_record[key]
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except (KeyError, IndexError, TypeError):
            return default

    def keys(self) -> list[str]:
        return list(self.by_record.keys())

    def items(self) -> list[tuple[str, ReadingResult]]:
        return list(self.by_record.items())

    def values(self) -> list[ReadingResult]:
        return list(self.by_record.values())

    @property
    def readings(self) -> BatchResult:
        return self

    @property
    def ids(self) -> list[Any]:
        return [item[0] for item in self]

    @property
    def device_ids(self) -> list[Any]:
        return [item[1] for item in self]


@dataclass(frozen=True)
class TimePoint:
    moment: datetime
    text: str
    raw: Any


@dataclass
class LedgerEntry:
    source: Any
    record: Any
    revision: int
    kind: str
    fingerprint: str
    target_table: str
    item_index: int | None = None
    payload: Any = None
    pk_value: str | None = None
    raw_hash: str | None = None
    persisted: bool = False


@dataclass
class ReadingItem:
    index: int
    raw: WriteRowLike
    source: Any
    record: Any
    revision: int
    station: Any
    code: Any
    time: TimePoint
    value: Any
    mapped: dict[str, Any]
    fingerprint: str = ""
    pk: Any = None
    is_current_candidate: bool = False
    skip_content_write: bool = False


@dataclass
class ChangeItem:
    code: Any
    device_id: Any
    ordinal: int = 0


@dataclass
class HandoveryItem:
    index: int
    raw: WriteRowLike
    source: Any
    record: Any
    revision: int
    station: Any
    time: TimePoint
    changes: list[ChangeItem]
    fingerprint: str = ""
    is_current_candidate: bool = False
    skip_content_write: bool = False


@dataclass
class ActivationItem:
    index: int
    station: Any
    changes: list[ChangeItem]


@dataclass
class SchemaInfo:
    columns: dict[str, str] = field(default_factory=dict)
    pk_column: str = "id"
    device_column: str = "device_id"


def collection_enabled(kwargs: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    """Return whether an explicit collection mode was requested."""
    options = dict(kwargs)
    if options.pop("_collection_handled", False):
        return False, {}
    for disabled_name in (
        "collect",
        "collected",
        "collection",
        "ingest",
        "intake",
        "enabled",
    ):
        if options.get(disabled_name) is False:
            return False, options

    explicit = options.get(
        "collect_mode", options.get("collection_mode", options.get("mode"))
    )
    if isinstance(explicit, str) and normalize_column_key(explicit) in {
        normalize_column_key(v) for v in MODE_VALUES
    }:
        return True, options
    if explicit is True:
        return True, options
    if isinstance(explicit, Mapping):
        return True, options

    for name in ENABLE_OPTIONS:
        value = options.get(name)
        if isinstance(value, Mapping):
            return True, options
        if value is True:
            return True, options
        if isinstance(value, str) and normalize_column_key(value) in {
            normalize_column_key(v) for v in MODE_VALUES
        }:
            return True, options

    # Any new keyword-only configuration is necessarily part of the new API.
    known = {
        "collect_mode",
        "collection_mode",
        "mode",
        "readings",
        "handovers",
        "handoffs",
        "items",
        "messages",
        "records",
        "batch",
        "device_table",
        "devices_table",
        "slots_table",
        "slot_table",
        "assignments_table",
        "handovers_table",
        "handoffs_table",
        "revisions_table",
        "slots",
        "initial_slots",
        "assignments",
        "slot_assignments",
        "baseline",
        "config",
        "spec",
    }
    extras = {k: v for k, v in options.items() if k not in known}
    if extras:
        return True, options
    return False, options


def is_collection_call(
    chunk_size: Any,
    kwargs: Mapping[str, Any],
) -> bool:
    if isinstance(chunk_size, bool):
        return True
    if isinstance(chunk_size, str) and normalize_column_key(chunk_size) in {
        normalize_column_key(v) for v in MODE_VALUES
    }:
        return True
    enabled, _ = collection_enabled(kwargs)
    return enabled


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )


def hash_content(value: Any) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, time, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def normalized_key(value: Any) -> str | None:
    if value is None:
        return None
    return normalize_column_key(str(value))


def make_key_lookup(row: Mapping[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for key in row:
        if not isinstance(key, str):
            continue
        norm = normalized_key(key)
        if norm is not None:
            lookup.setdefault(norm, key)
    return lookup


def find_value(
    row: Mapping[str, Any],
    aliases: Sequence[str],
    *,
    default: Any = None,
    present: bool = False,
) -> tuple[bool, Any]:
    lookup = make_key_lookup(row)
    for alias in aliases:
        actual = lookup.get(normalize_column_key(alias))  # type: ignore[arg-type]
        if actual is not None:
            return True, row[actual]
    if present:
        return False, None
    return False, default


def has_active_field(value: Any, seen: set[int] | None = None) -> bool:
    if seen is None:
        seen = set()
    if isinstance(value, Mapping):
        marker = id(value)
        if marker in seen:
            return False
        seen.add(marker)
        for key, child in value.items():
            if isinstance(key, str) and normalize_column_key(key) == "ACTIVE":
                return True
            if has_active_field(child, seen):
                return True
    elif isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in seen:
            return False
        seen.add(marker)
        for child in value:
            if has_active_field(child, seen):
                return True
    return False


def parse_time(value: Any, *, field: str, index: int | None = None) -> TimePoint:
    raw = value
    moment: datetime
    text: str

    if isinstance(value, datetime):
        moment = value
        if moment.tzinfo is not None:
            moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
        text = moment.isoformat(sep=" ")
    elif isinstance(value, time):
        moment = datetime.combine(date(1900, 1, 1), value)
        if moment.tzinfo is not None:
            moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
        text = moment.time().isoformat()
    elif isinstance(value, date):
        moment = datetime(value.year, value.month, value.day)
        text = moment.date().isoformat()
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number > 100_000_000:
            moment = datetime.fromtimestamp(number, tz=timezone.utc).replace(
                tzinfo=None
            )
            text = moment.isoformat(sep=" ")
        else:
            raise CollectionImportError(
                f"{field} is not a recognised time: {value!r}", index=index
            )
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise CollectionImportError(f"{field} must not be empty", index=index)
        iso_value = stripped.replace("Z", "+00:00")
        try:
            moment = datetime.fromisoformat(iso_value)
            if moment.tzinfo is not None:
                moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
            text = moment.isoformat(sep=" ")
        except ValueError:
            parsed = _parse_clock(stripped)
            if parsed is None:
                raise CollectionImportError(
                    f"{field} is not a recognised time: {value!r}", index=index
                ) from None
            hour, minute, second, micro = parsed
            moment = datetime(1900, 1, 1, hour, minute, second, micro)
            text = stripped if ":" in stripped else moment.time().isoformat()
    else:
        raise CollectionImportError(f"{field} is required", index=index)

    return TimePoint(moment=moment, text=text, raw=raw)


def _parse_clock(value: str) -> tuple[int, int, int, int] | None:
    parts = value.split(":")
    if len(parts) not in (2, 3):
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
        seconds = float(parts[2]) if len(parts) == 3 else 0.0
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= seconds < 60):
        return None
    whole_seconds = int(seconds)
    micro = int(round((seconds - whole_seconds) * 1_000_000))
    return hour, minute, whole_seconds, micro


def time_for_column(point: TimePoint, column_type: Any) -> Any:
    type_name = type(column_type).__name__.upper()
    if "DATETIME" in type_name or "TIMESTAMP" in type_name:
        return point.moment
    if type_name == "TIME":
        return point.moment.time()
    if "DATE" in type_name:
        return point.moment.date()
    if isinstance(point.raw, str):
        return point.raw
    return point.text


class CollectionImporter:
    """Apply an explicitly enabled collector import batch atomically."""

    def __init__(
        self,
        table: Any,
        rows: Sequence[WriteRowLike],
        keys: Sequence[str] | str | None,
        ensure: bool | None,
        types: dict[str, Any] | None,
        options: Mapping[str, Any],
    ) -> None:
        self.table = table
        self.db = table.db
        self.keys = keys
        self.ensure = ensure
        self.types = types or {}
        options_dict = dict(options)
        merged_options: dict[str, Any] = {}
        for config_name in (
            "config",
            "spec",
            "options",
            "collect",
            "collection",
            "intake",
            "ingest",
        ):
            config = options_dict.get(config_name)
            if isinstance(config, Mapping):
                merged_options.update(dict(config))
        merged_options.update(options_dict)
        self.options = merged_options
        reading_table_name = self._option_table_name(
            "readings_table",
            "reading_table",
            "observations_table",
            "target_readings_table",
            "target_table",
            "target",
            "table_name",
        )
        if reading_table_name and reading_table_name != table.name:
            self.table = self.db.load_table(reading_table_name)
        else:
            self.table = table
        self.raw_entries: list[tuple[WriteRowLike, str | None]] = [
            (row, None) for row in rows
        ]
        self._append_option_rows("readings", "reading")
        self._append_option_rows("messages", None)
        self._append_option_rows("records", None)
        self._append_option_rows("items", None)
        self._append_option_rows("batch", None)
        self._append_option_rows("handovers", "handover")
        self._append_option_rows("handoffs", "handover")

        self.schema = SchemaInfo()
        self.readings: list[ReadingItem] = []
        self.handovers: list[HandoveryItem] = []
        self.activations: list[ActivationItem] = []
        self.support_tables: dict[str, Any] = {}
        self.ledger_rows: dict[tuple[Any, Any, int], dict[str, Any]] = {}
        self.staged_ledger: dict[tuple[Any, Any, int], LedgerEntry] = {}
        self.existing_readings: dict[tuple[Any, Any], dict[str, Any]] = {}
        self.existing_handovers: dict[tuple[Any, Any], dict[str, Any]] = {}
        self.baseline: dict[tuple[Any, Any], Any] = {}
        self.external_events: list[dict[str, Any]] = []
        self.final_devices: dict[Any, Any] = {}

    def _append_option_rows(
        self, name: str, forced_kind: str | None, *aliases: str
    ) -> None:
        for option_name in (name, *aliases):
            value = self.options.get(option_name)
            if value is None:
                continue
            if isinstance(value, Mapping):
                value = [value]
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                for row in value:
                    if isinstance(row, Mapping):
                        self.raw_entries.append((row, forced_kind))

    def run(self) -> BatchResult:
        raw_rows = [row for row, _ in self.raw_entries]
        for index, row in enumerate(raw_rows):
            if not isinstance(row, Mapping):
                raise CollectionImportError(
                    "collection input rows must be mappings", index=index
                )
            if has_active_field(row):
                raise CollectionImportError(
                    "collection input must not contain an 'active' field",
                    index=index,
                    item=row,
                )

        self.schema = self._resolve_schema()
        self._parse_entries()
        self._sync_reading_schema()
        self.schema = self._resolve_schema()
        self._ensure_range_index()
        self._prepare_support_tables()
        self._load_baseline()
        self._load_existing_records()
        self._load_external_handovers()
        self._adopt_existing_revisions()
        self._reconcile_revisions()
        self._validate_devices()
        self._validate_effective_times()
        self._persist_activations()
        self._persist_ledger()
        self._persist_handovers()
        timeline = self._build_timeline(
            [item.station for item in self.readings + self.handovers]
        )
        self._persist_readings(timeline)
        self._recompute_attribution(timeline)
        return self._build_result()

    # ------------------------------------------------------------------ schema

    def _resolve_schema(self) -> SchemaInfo:
        columns: dict[str, str] = {}
        existing: list[str] = []
        if self.table.exists:
            existing = list(self.table._column_keys.values())

        alias_fields = {
            "source": SOURCE_ALIASES,
            "record": RECORD_ALIASES,
            "revision": REVISION_ALIASES,
            "station": STATION_ALIASES,
            "code": CODE_ALIASES,
            "time": TIME_ALIASES,
            "value": VALUE_ALIASES,
        }
        normalized_existing = {normalize_column_key(name): name for name in existing}
        for field_name, aliases in alias_fields.items():
            chosen: str | None = None
            for alias in aliases:
                actual = normalized_existing.get(normalize_column_key(alias))
                if actual is not None:
                    chosen = actual
                    break
            if chosen is None:
                chosen = aliases[0]
            columns[field_name] = chosen

        device_column: str | None = None
        if self.table._table is not None:
            for foreign_key in self.table.table.foreign_keys:
                if foreign_key.column.table.name == self._device_table_name():
                    device_column = foreign_key.parent.name
                    break
        if device_column is None:
            for alias in DEVICE_ALIASES:
                device_column = normalized_existing.get(normalize_column_key(alias))
                if device_column is not None:
                    break
        if device_column is None:
            device_column = "device_id"

        pk_column = self.table._primary_id or "id"
        if self.table._table is not None and self.table.table.primary_key is not None:
            pk_names = [column.name for column in self.table.table.primary_key.columns]
            if len(pk_names) == 1:
                pk_column = pk_names[0]

        if self.keys is not None:
            key_list = [self.keys] if isinstance(self.keys, str) else list(self.keys)
            key_names = [self.table._get_column_name(str(key)) for key in key_list]
            if len(key_names) >= 2:
                columns["source"] = key_names[0]
                columns["record"] = key_names[1]
            if len(key_names) >= 3:
                columns["revision"] = key_names[2]

        return SchemaInfo(
            columns=columns, pk_column=pk_column, device_column=device_column
        )

    def _sync_reading_schema(self) -> None:
        sample: dict[str, Any] = {
            self.schema.device_column: None,
            self.schema.columns["source"]: "",
            self.schema.columns["record"]: "",
            self.schema.columns["revision"]: 0,
            self.schema.columns["station"]: "",
            self.schema.columns["code"]: None,
            self.schema.columns["time"]: "",
            self.schema.columns["value"]: None,
        }
        for item in self.readings:
            sample.update(item.mapped)
            break
        type_overrides = dict(self.types)
        type_overrides.setdefault(self.schema.columns["revision"], self.db.types.bigint)
        type_overrides.setdefault(self.schema.device_column, self.db.types.bigint)
        self.table._sync_columns(sample, self.ensure, types=type_overrides)
        for item in self.readings:
            filtered = self.table._sync_columns(dict(item.mapped), False)
            item.mapped = filtered

    def _column(self, field_name: str) -> Any:
        name = self.schema.columns.get(field_name)
        if not name or not self.table.has_column(name):
            raise CollectionImportError(
                f"readings table is missing required column {name!r}"
            )
        return self.table.table.c[name]

    # ----------------------------------------------------------------- parsing

    def _parse_entries(self) -> None:
        for index, (raw, forced_kind) in enumerate(self.raw_entries):
            raw = self._unwrap_envelope(raw)
            kind = self._kind_of(raw, forced_kind)
            if kind == "activation":
                self.activations.append(self._parse_activation(index, raw))
            elif kind == "handover":
                self.handovers.append(self._parse_handover(index, raw))
            else:
                self.readings.append(self._parse_reading(index, raw))

    @staticmethod
    def _unwrap_envelope(raw: WriteRowLike) -> WriteRowLike:
        for envelope_key in (
            "envelope",
            "message",
            "payload",
            "body",
            "content",
            "data",
        ):
            nested = raw.get(envelope_key)
            if not isinstance(nested, Mapping):
                continue
            _, has_code = find_value(raw, CODE_ALIASES, present=True)
            _, has_time = find_value(raw, TIME_ALIASES, present=True)
            nested_has_shape = (
                find_value(nested, CODE_ALIASES, present=True)[0]
                or find_value(nested, CHANGES_ALIASES, present=True)[0]
            )
            if (not has_code and not has_time and nested_has_shape) or (
                find_value(nested, RECORD_ALIASES, present=True)[0]
                and find_value(nested, SOURCE_ALIASES, present=True)[0]
            ):
                merged: dict[str, Any] = dict(nested)
                merged.update(
                    {key: value for key, value in raw.items() if key != envelope_key}
                )
                return merged
        return raw

    def _kind_of(self, raw: WriteRowLike, forced_kind: str | None) -> str:
        if forced_kind:
            kind = normalize_column_key(forced_kind.lower())
        else:
            _, kind_value = find_value(raw, KIND_ALIASES)
            kind = (
                normalize_column_key(str(kind_value).lower())
                if kind_value is not None
                else None
            )

        if kind is not None:
            if kind in {normalize_column_key(v) for v in READING_KINDS}:
                return "reading"
            if kind in {normalize_column_key(v) for v in HANDOVER_KINDS}:
                return "handover"
            if kind in {normalize_column_key(v) for v in ACTIVATION_KINDS}:
                return "activation"

        has_changes, changes = find_value(raw, CHANGES_ALIASES)
        if has_changes and changes is not None:
            return "handover"
        has_code, _ = find_value(raw, CODE_ALIASES, present=True)
        has_time, _ = find_value(raw, TIME_ALIASES, present=True)
        has_value, _ = find_value(raw, VALUE_ALIASES, present=True)
        has_sampled_time, _ = find_value(
            raw,
            (
                "sampled_at",
                "sample_time",
                "sampling_time",
                "sampled_time",
                "observed_at",
                "read_at",
            ),
            present=True,
        )
        has_effective, _ = find_value(raw, EFFECTIVE_ALIASES, present=True)
        has_station, _ = find_value(raw, STATION_ALIASES, present=True)
        if has_value or has_sampled_time or (has_code and has_time):
            return "reading"
        if has_effective and (has_changes or has_station):
            return "handover"
        if has_station and has_changes:
            return "handover"
        if has_station and has_code:
            return "activation"
        if has_code:
            return "reading"
        raise CollectionImportError(
            "could not determine whether collection item is a reading or handover",
            item=raw,
        )

    def _identity(self, raw: WriteRowLike, index: int) -> tuple[Any, Any, int]:
        _, source = find_value(raw, SOURCE_ALIASES)
        _, record = find_value(raw, RECORD_ALIASES)
        has_revision, revision_value = find_value(raw, REVISION_ALIASES, present=True)
        if source is None:
            raise CollectionImportError(
                "collection item is missing source", index=index, item=raw
            )
        if record is None:
            raise CollectionImportError(
                "collection item is missing record number", index=index, item=raw
            )
        if not has_revision or revision_value is None:
            revision = 1
        else:
            try:
                revision = int(revision_value)
            except (TypeError, ValueError) as exc:
                raise CollectionImportError(
                    f"revision must be an integer, got {revision_value!r}",
                    index=index,
                    item=raw,
                ) from exc
        return source, record, revision

    def _parse_reading(self, index: int, raw: WriteRowLike) -> ReadingItem:
        source, record, revision = self._identity(raw, index)
        has_code, code = find_value(raw, CODE_ALIASES, present=True)
        if not has_code:
            raise CollectionImportError(
                "reading is missing the code field; "
                "use code=None for an unnumbered slot",
                index=index,
                item=raw,
            )
        _, station = find_value(raw, STATION_ALIASES)
        if station is None:
            raise CollectionImportError(
                "reading is missing station/site", index=index, item=raw
            )
        has_time, time_value = find_value(raw, TIME_ALIASES, present=True)
        if not has_time or time_value is None:
            raise CollectionImportError(
                "reading is missing sampling time", index=index, item=raw
            )
        point = parse_time(time_value, field="sampled_at", index=index)
        _, value = find_value(raw, VALUE_ALIASES)

        mapped = self._map_reading(raw)
        item = ReadingItem(
            index=index,
            raw=raw,
            source=source,
            record=record,
            revision=revision,
            station=station,
            code=code,
            time=point,
            value=value,
            mapped=mapped,
        )
        item.fingerprint = self.reading_fingerprint(
            source,
            record,
            revision,
            station,
            self._normalize_code_key(code),
            point,
            value,
        )
        return item

    def _map_reading(self, raw: WriteRowLike) -> dict[str, Any]:
        mapped: dict[str, Any] = {}
        alias_to_field: dict[str, str] = {}
        for field_name, aliases in (
            ("source", SOURCE_ALIASES),
            ("record", RECORD_ALIASES),
            ("revision", REVISION_ALIASES),
            ("station", STATION_ALIASES),
            ("code", CODE_ALIASES),
            ("time", TIME_ALIASES),
            ("value", VALUE_ALIASES),
        ):
            for alias in aliases:
                normalized_alias = normalize_column_key(alias)
                if normalized_alias is not None:
                    alias_to_field[normalized_alias] = field_name

        lookup = make_key_lookup(raw)
        kind_alias_keys = {normalize_column_key(alias) for alias in KIND_ALIASES}
        reading_kind_keys = {normalize_column_key(value) for value in READING_KINDS}
        for norm_key, raw_key in lookup.items():
            value = raw[raw_key]
            if norm_key in kind_alias_keys:
                value_kind = (
                    normalize_column_key(str(value).lower())
                    if value is not None
                    else None
                )
                if value_kind in reading_kind_keys | {
                    normalize_column_key(v) for v in HANDOVER_KINDS | ACTIVATION_KINDS
                }:
                    continue
            matched_field = alias_to_field.get(norm_key)
            if matched_field is not None:
                target_name = self.schema.columns[matched_field]
                if matched_field == "time":
                    value = (
                        parse_time(value, field="sampled_at").raw
                        if value is not None
                        else None
                    )
                mapped[target_name] = value
            else:
                target_name = self.table._get_column_name(raw_key)
                if target_name in {self.schema.pk_column, self.schema.device_column}:
                    continue
                mapped[target_name] = value

        for field_name in (
            "source",
            "record",
            "revision",
            "station",
            "code",
            "time",
            "value",
        ):
            target_name = self.schema.columns[field_name]
            if field_name == "code":
                _, value = find_value(raw, CODE_ALIASES, present=True)
                mapped[target_name] = value
            elif target_name not in mapped:
                _, value = find_value(
                    raw,
                    {
                        "source": SOURCE_ALIASES,
                        "record": RECORD_ALIASES,
                        "revision": REVISION_ALIASES,
                        "station": STATION_ALIASES,
                        "time": TIME_ALIASES,
                        "value": VALUE_ALIASES,
                    }[field_name],
                )
                mapped[target_name] = value
        mapped.pop(self.schema.pk_column, None)
        mapped.pop(self.schema.device_column, None)
        for alias in DEVICE_ALIASES:
            mapped.pop(alias, None)
        return mapped

    def _fingerprint_mapped(self, mapped: Mapping[str, Any]) -> dict[str, Any]:
        excluded = {
            normalize_column_key(self.schema.pk_column),
            normalize_column_key(self.schema.device_column),
        }
        result = {
            key: value
            for key, value in mapped.items()
            if normalize_column_key(key) not in excluded
        }
        return result

    def _parse_handover(self, index: int, raw: WriteRowLike) -> HandoveryItem:
        source, record, revision = self._identity(raw, index)
        _, station = find_value(raw, STATION_ALIASES)
        has_effective, effective_value = find_value(
            raw, EFFECTIVE_ALIASES, present=True
        )
        if not has_effective or effective_value is None:
            raise CollectionImportError(
                "handover is missing effective time", index=index, item=raw
            )
        point = parse_time(effective_value, field="effective_at", index=index)
        changes = self._parse_changes(index, raw, station)
        if station is None:
            raise CollectionImportError(
                "handover is missing station/site", index=index, item=raw
            )
        item = HandoveryItem(
            index=index,
            raw=raw,
            source=source,
            record=record,
            revision=revision,
            station=station,
            time=point,
            changes=changes,
        )
        item.fingerprint = self._handover_fingerprint(station, point, changes)
        return item

    def _parse_changes(
        self, index: int, raw: WriteRowLike, station: Any
    ) -> list[ChangeItem]:
        has_container, container = find_value(raw, CHANGES_ALIASES, present=True)
        changes: list[ChangeItem] = []
        if has_container and container is not None:
            if isinstance(container, Mapping):
                for ordinal, (code, device) in enumerate(container.items()):
                    if isinstance(device, Mapping):
                        device_id = self._device_from_change(device)
                        code = self._code_from_change(device, code)
                    else:
                        device_id = device
                    changes.append(
                        ChangeItem(self._normalize_code_key(code), device_id, ordinal)
                    )
            elif isinstance(container, Sequence) and not isinstance(
                container, (str, bytes)
            ):
                for ordinal, entry in enumerate(container):
                    if isinstance(entry, Mapping):
                        code = self._code_from_change(entry, None)
                        device_id = self._device_from_change(entry)
                        changes.append(ChangeItem(code, device_id, ordinal))
                    elif isinstance(entry, (tuple, list)) and len(entry) >= 2:
                        changes.append(
                            ChangeItem(
                                self._normalize_code_key(entry[0]), entry[1], ordinal
                            )
                        )
        else:
            has_code, code = find_value(raw, CODE_ALIASES, present=True)
            if has_code:
                changes.append(
                    ChangeItem(
                        self._normalize_code_key(code),
                        self._device_from_change(raw),
                        0,
                    )
                )
        if not changes:
            raise CollectionImportError(
                "handover does not list any changed device slots",
                index=index,
                item=raw,
            )
        seen: set[Any] = set()
        for change in changes:
            key = self._slot_key(station, change.code)
            if key in seen:
                raise CollectionImportError(
                    f"handover lists slot {change.code!r} more than once",
                    index=index,
                    item=raw,
                )
            seen.add(key)
        return changes

    def _code_from_change(self, entry: Mapping[str, Any], fallback: Any) -> Any:
        has_code, code = find_value(entry, CODE_ALIASES, present=True)
        if has_code:
            return self._normalize_code_key(code)
        return self._normalize_code_key(fallback)

    def _device_from_change(self, entry: Mapping[str, Any]) -> Any:
        has_device, device = find_value(entry, DEVICE_ALIASES, present=True)
        if has_device:
            return device
        _, action = find_value(entry, ("action", "op", "operation"))
        _, vacated = find_value(entry, ("vacated", "vacate", "clear", "empty"))
        action_key = (
            normalize_column_key(str(action).lower()) if action is not None else None
        )
        if vacated is True or action_key in {
            normalize_column_key(v) for v in VACATE_VALUES
        }:
            return None
        raise CollectionImportError(
            "handover item is missing new device primary key; "
            "use null to vacate a slot",
            item=entry,
        )

    def _parse_activation(self, index: int, raw: WriteRowLike) -> ActivationItem:
        _, station = find_value(raw, STATION_ALIASES)
        if station is None:
            raise CollectionImportError(
                "activation is missing station", index=index, item=raw
            )
        changes = self._parse_changes(index, raw, station)
        return ActivationItem(index=index, station=station, changes=changes)

    @staticmethod
    def _normalize_code_key(code: Any) -> Any:
        if code is None:
            return None
        if isinstance(code, str):
            stripped = code.strip()
            if stripped.lower() in {"none", "null", "unnumbered", ""}:
                return None
            return stripped
        return code

    @staticmethod
    def _station_key(station: Any) -> Any:
        return normalize_column_key(station) if station is not None else None

    def _slot_key(self, station: Any, code: Any) -> tuple[Any, Any]:
        return self._station_key(station), self._normalize_code_key(code)

    @staticmethod
    def _content_scalar(value: Any) -> Any:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, Decimal):
            return float(value)
        return value

    @classmethod
    def reading_fingerprint(
        cls,
        source: Any,
        record: Any,
        revision: int,
        station: Any,
        code: Any,
        point: TimePoint,
        value: Any,
    ) -> str:
        return hash_content(
            {
                "kind": "reading",
                "source": cls._content_scalar(source),
                "record": cls._content_scalar(record),
                "revision": int(revision),
                "station": cls._content_scalar(station),
                "code": cls._content_scalar(code),
                "time": point.moment.isoformat(),
                "value": cls._content_scalar(value),
            }
        )

    def _handover_fingerprint(
        self, station: Any, point: TimePoint, changes: Sequence[ChangeItem]
    ) -> str:
        return hash_content(
            {
                "station": station,
                "time": point.text,
                "changes": sorted(
                    [[self._normalize_code_key(c.code), c.device_id] for c in changes],
                    key=lambda value: canonical_json(value),
                ),
            }
        )

    # ------------------------------------------------------------- support DDL

    def _device_table_name(self) -> str:
        for option_name in (
            "device_table",
            "devices_table",
            "devices",
            "device",
        ):
            value = self.options.get(option_name)
            if isinstance(value, str):
                return value
            table_name = getattr(value, "name", None)
            if isinstance(table_name, str):
                return table_name
        return "devices"

    def _option_table_name(self, *option_names: str) -> str | None:
        for option_name in option_names:
            value = self.options.get(option_name)
            if isinstance(value, str):
                return value
            table_name = getattr(value, "name", None)
            if isinstance(table_name, str):
                return table_name
        return None

    def _support_table_name(self, kind: str, default: str) -> str:
        option_names = {
            "revisions": ("revisions_table", "revision_table", "received_table"),
            "handovers": (
                "handovers_table",
                "handover_table",
                "handoff_table",
                "handoffs_table",
            ),
            "items": (
                "handover_items_table",
                "handoff_items_table",
                "items_table",
            ),
            "slots": (
                "slots_table",
                "slot_table",
                "assignments_table",
                "assignment_table",
            ),
        }[kind]
        name = self._option_table_name(*option_names)
        return name or default

    def _prepare_support_tables(self) -> None:
        revision_name = self._support_table_name("revisions", "collection_revisions")
        handover_name = self._support_table_name("handovers", "collection_handovers")
        item_name = self._support_table_name("items", "collection_handover_items")
        slot_name = self._support_table_name("slots", "collection_slots")

        revisions = self.db.create_table(revision_name)
        revisions._sync_columns(
            {
                "target_table": self.table.name,
                "kind": "reading",
                "source": "",
                "record_no": "",
                "revision": 0,
                "fingerprint": "",
                "raw_hash": "",
                "content_hash": "",
                "payload": "",
                "item_index": 0,
                "pk_value": "",
            },
            True,
            types={
                "revision": self.db.types.bigint,
                "item_index": self.db.types.bigint,
                "payload": self.db.types.text,
            },
        )
        handovers = self.db.create_table(handover_name)
        handovers._sync_columns(
            {
                "target_table": self.table.name,
                "source": "",
                "record_no": "",
                "revision": 0,
                "station": "",
                "effective_text": "",
                "effective_raw": "",
                "fingerprint": "",
            },
            True,
            types={"revision": self.db.types.bigint},
        )
        items = self.db.create_table(item_name)
        items._sync_columns(
            {
                "target_table": self.table.name,
                "source": "",
                "record_no": "",
                "station": "",
                "code": None,
                "device_id": None,
                "ordinal": 0,
            },
            True,
            types={"device_id": self.db.types.bigint, "ordinal": self.db.types.bigint},
        )
        slots = self.db.create_table(slot_name)
        slots._sync_columns(
            {"station": "", "code": None, "device_id": None},
            True,
            types={"device_id": self.db.types.bigint},
        )
        self.support_tables = {
            "revisions": revisions,
            "handovers": handovers,
            "items": items,
            "slots": slots,
        }

    # ------------------------------------------------------------ load existing

    def _load_existing_records(self) -> None:
        rev_table = self.support_tables["revisions"].table
        identities = [
            self._identity_key(item.source, item.record) for item in self.readings
        ]
        identities += [
            self._identity_key(item.source, item.record) for item in self.handovers
        ]
        identity_set = list(dict.fromkeys(identities))
        if identity_set:
            conditions = [
                and_(
                    rev_table.c.target_table == self.table.name,
                    rev_table.c.source == source,
                    rev_table.c.record_no == record,
                )
                for source, record in identity_set
            ]
            result = self.db.executable.execute(
                select(rev_table).where(or_(*conditions))
            )
            for row in result.mappings():
                data = dict(row)
                key = (data["source"], data["record_no"], int(data["revision"]))
                self.ledger_rows[key] = data

        source_column = self.schema.columns["source"]
        record_column = self.schema.columns["record"]
        for source, record in identity_set:
            row = self.table.find_one(**{source_column: source, record_column: record})
            if row is not None:
                data = dict(row)
                self.existing_readings[(source, record)] = data

        ho_table = self.support_tables["handovers"].table
        for source, record in identity_set:
            row = (
                self.db.executable.execute(
                    select(ho_table).where(
                        and_(
                            ho_table.c.target_table == self.table.name,
                            ho_table.c.source == source,
                            ho_table.c.record_no == record,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is not None:
                self.existing_handovers[(source, record)] = dict(row)

    @staticmethod
    def _identity_key(source: Any, record: Any) -> tuple[Any, Any]:
        return source, record

    def _load_baseline(self) -> None:
        slot_table = self.support_tables["slots"].table
        rows = self.db.executable.execute(select(slot_table)).mappings()
        for row in rows:
            data = dict(row)
            self.baseline[
                self._slot_key(data["station"], self._normalize_code_key(data["code"]))
            ] = data["device_id"]

        for option_name in (
            "slots",
            "initial_slots",
            "assignments",
            "slot_assignments",
            "baseline",
        ):
            explicit = self.options.get(option_name)
            if isinstance(explicit, Mapping):
                default_station = self.options.get("station") or self.options.get(
                    "default_station"
                )
                for slot, device in explicit.items():
                    station = default_station
                    if isinstance(slot, tuple) and len(slot) == 2:
                        station, code = slot
                    else:
                        code = slot
                    self.baseline[self._slot_key(station, code)] = device
            elif isinstance(explicit, Sequence) and not isinstance(
                explicit, (str, bytes)
            ):
                for entry in explicit:
                    if isinstance(entry, Mapping):
                        _, station = find_value(entry, STATION_ALIASES)
                        code = self._code_from_change(entry, None)
                        device = self._device_from_change(entry)
                        self.baseline[self._slot_key(station, code)] = device

        external_name = self._option_table_name(
            "slots_table",
            "slot_table",
            "assignments_table",
            "slots",
            "assignments",
            "baseline_table",
        )
        if external_name is None:
            external_name = self._discover_baseline_table()
        if external_name and external_name in self.db.tables:
            self._load_external_baseline(external_name)

    def _discover_baseline_table(self) -> str | None:
        priority = (
            "slots",
            "station_slots",
            "site_slots",
            "device_slots",
            "station_devices",
            "site_devices",
            "device_assignments",
            "station_assignments",
            "assignments",
            "placements",
            "device_positions",
            "positions",
        )
        tables = set(self.db.tables)
        for name in priority:
            if name in tables:
                return name
        best: tuple[int, str] | None = None
        for name in self.db.tables:
            if name == self.table.name or name.startswith("collection_"):
                continue
            low = name.lower()
            if (
                "handover" in low
                or "handoff" in low
                or "revision" in low
                or "notification" in low
            ):
                continue
            try:
                columns = [c["name"] for c in self.db.inspect.get_columns(name)]
            except Exception:
                continue
            has_station = any(
                normalized_key(c) in {normalize_column_key(a) for a in STATION_ALIASES}
                for c in columns
            )
            has_code = any(
                normalized_key(c) in {normalize_column_key(a) for a in CODE_ALIASES}
                for c in columns
            )
            has_device = any(
                normalized_key(c) in {normalize_column_key(a) for a in DEVICE_ALIASES}
                for c in columns
            )
            if has_station and has_code and has_device:
                score = 1
                if "slot" in low or "assignment" in low or "device" in low:
                    score = 5
                if best is None or score > best[0]:
                    best = (score, name)
        return best[1] if best else None

    def _load_external_baseline(self, name: str) -> None:
        ext = self.db.load_table(name)
        columns = list(ext._column_keys.values())
        station_col = self._pick_alias_column(columns, STATION_ALIASES)
        code_col = self._pick_alias_column(columns, CODE_ALIASES)
        device_col = self._pick_alias_column(columns, DEVICE_ALIASES)
        time_col = self._pick_alias_column(columns, EFFECTIVE_ALIASES + TIME_ALIASES)
        if not (station_col and code_col and device_col):
            return
        stmt = select(ext.table)
        for row in self.db.executable.execute(stmt).mappings():
            data = dict(row)
            key = self._slot_key(data[station_col], data[code_col])
            if time_col is None:
                self.baseline[key] = data[device_col]
            else:
                point = parse_time(data[time_col], field="effective_at")
                self.external_events.append(
                    {
                        "station": data[station_col],
                        "code": self._normalize_code_key(data[code_col]),
                        "device_id": data[device_col],
                        "time": point,
                    }
                )

    def _load_external_handovers(self) -> None:
        explicit = self._option_table_name(
            "handovers_table", "handoff_table", "handovers", "handoffs"
        )
        name = explicit
        if name is None:
            for candidate in (
                "handovers",
                "handoffs",
                "handover_orders",
                "handoff_orders",
                "device_handovers",
                "station_handovers",
                "assignment_orders",
                "handover_history",
            ):
                if candidate in self.db.tables:
                    name = candidate
                    break
        if not name or name not in self.db.tables or name.startswith("collection_"):
            return

        table = self.db.load_table(name)
        columns = list(table._column_keys.values())
        source_col = self._pick_alias_column(columns, SOURCE_ALIASES)
        record_col = self._pick_alias_column(columns, RECORD_ALIASES)
        revision_col = self._pick_alias_column(columns, REVISION_ALIASES)
        station_col = self._pick_alias_column(columns, STATION_ALIASES)
        time_col = self._pick_alias_column(columns, EFFECTIVE_ALIASES + TIME_ALIASES)
        changes_col = self._pick_alias_column(columns, CHANGES_ALIASES)
        if not all((source_col, record_col, station_col, time_col)):
            return

        child_changes = self._load_external_handover_items()
        rows_by_logical: dict[tuple[Any, Any], dict[str, Any]] = {}
        for row in self.db.executable.execute(select(table.table)).mappings():
            data = dict(row)
            source = data[source_col]
            record = data[record_col]
            revision = (
                int(data[revision_col])
                if revision_col and data.get(revision_col) is not None
                else 1
            )
            point = parse_time(data[time_col], field="effective_at")
            changes: list[ChangeItem] = []
            if changes_col and data.get(changes_col) is not None:
                raw_changes = data[changes_col]
                if isinstance(raw_changes, str):
                    try:
                        raw_changes = json.loads(raw_changes)
                    except json.JSONDecodeError:
                        raw_changes = None
                if isinstance(raw_changes, Mapping):
                    raw_changes = [
                        {code_key: device_value}
                        for code_key, device_value in raw_changes.items()
                    ]
                if isinstance(raw_changes, Sequence) and not isinstance(
                    raw_changes, (str, bytes)
                ):
                    for ordinal, entry in enumerate(raw_changes):
                        if isinstance(entry, Mapping):
                            code = self._code_from_change(entry, None)
                            device = self._device_from_change(entry)
                            changes.append(ChangeItem(code, device, ordinal))
            if not changes:
                changes = child_changes.get((source, record), [])
                pk_value = data.get("id")
                if not changes and pk_value is not None:
                    changes = child_changes.get(("__id__", pk_value), [])
            fingerprint = self._handover_fingerprint(data[station_col], point, changes)
            normalized = {
                "id": data.get("id"),
                "source": source,
                "record_no": record,
                "revision": revision,
                "station": data[station_col],
                "effective_text": point.text,
                "fingerprint": fingerprint,
                "_changes": changes,
                "_external": True,
            }
            key = (source, record)
            previous = rows_by_logical.get(key)
            if previous is None or revision > int(previous["revision"]):
                rows_by_logical[key] = normalized

        for key, value in rows_by_logical.items():
            self.existing_handovers.setdefault(key, value)
            point = parse_time(value["effective_text"], field="effective_at")
            for change in value["_changes"]:
                self.external_events.append(
                    {
                        "source": value["source"],
                        "record": value["record_no"],
                        "station": value["station"],
                        "code": self._normalize_code_key(change.code),
                        "device_id": change.device_id,
                        "time": point,
                    }
                )

    def _load_external_handover_items(
        self,
    ) -> dict[tuple[Any, Any], list[ChangeItem]]:
        explicit = self._option_table_name(
            "handover_items_table", "handoff_items_table", "items_table"
        )
        name = explicit
        if name is None:
            for candidate in (
                "handover_items",
                "handoff_items",
                "handover_changes",
                "handoff_changes",
                "handover_lines",
            ):
                if candidate in self.db.tables:
                    name = candidate
                    break
        if not name or name not in self.db.tables or name.startswith("collection_"):
            return {}

        table = self.db.load_table(name)
        columns = list(table._column_keys.values())
        code_col = self._pick_alias_column(columns, CODE_ALIASES)
        device_col = self._pick_alias_column(columns, DEVICE_ALIASES)
        source_col = self._pick_alias_column(columns, SOURCE_ALIASES)
        record_col = self._pick_alias_column(columns, RECORD_ALIASES)
        parent_col = self._pick_alias_column(
            columns,
            (
                "handover_id",
                "handovers_id",
                "handoff_id",
                "order_id",
                "header_id",
                "handover_order_id",
            ),
        )
        if not (code_col and device_col):
            return {}

        result: dict[tuple[Any, Any], list[ChangeItem]] = {}
        for ordinal, row in enumerate(
            self.db.executable.execute(select(table.table)).mappings()
        ):
            data = dict(row)
            change = ChangeItem(
                self._normalize_code_key(data[code_col]),
                data[device_col],
                ordinal,
            )
            if source_col and record_col:
                key = (data[source_col], data[record_col])
            elif parent_col:
                key = ("__id__", data[parent_col])
            else:
                continue
            result.setdefault(key, []).append(change)
        return result

    @staticmethod
    def _pick_alias_column(
        columns: Sequence[str], aliases: Sequence[str]
    ) -> str | None:
        lookup = {normalize_column_key(c): c for c in columns}
        for alias in aliases:
            actual = lookup.get(normalize_column_key(alias))
            if actual is not None:
                return actual
        return None

    # ------------------------------------------------------------- revisions

    def _all_versioned_items(self) -> list[ReadingItem | HandoveryItem]:
        items: list[ReadingItem | HandoveryItem] = [*self.readings, *self.handovers]
        return sorted(items, key=lambda item: item.index)

    def _preexisting_revision(self, source: Any, record: Any) -> int | None:
        revisions: list[int] = []
        for (ledger_source, ledger_record, revision), _row in self.ledger_rows.items():
            if ledger_source == source and ledger_record == record:
                revisions.append(int(revision))
        reading = self.existing_readings.get((source, record))
        if reading is not None:
            revision_name = self.schema.columns["revision"]
            if reading.get(revision_name) is not None:
                revisions.append(int(reading[revision_name]))
        handover = self.existing_handovers.get((source, record))
        if handover is not None and handover.get("revision") is not None:
            revisions.append(int(handover["revision"]))
        return max(revisions) if revisions else None

    def _adopt_existing_revisions(self) -> None:
        time_name = self.schema.columns["time"]
        station_name = self.schema.columns["station"]
        code_name = self.schema.columns["code"]
        value_name = self.schema.columns["value"]
        revision_name = self.schema.columns["revision"]

        for (source, record), row in list(self.existing_readings.items()):
            if row.get(revision_name) is None or row.get(time_name) is None:
                continue
            revision = int(row[revision_name])
            triple = (source, record, revision)
            if triple in self.ledger_rows:
                continue
            point = parse_time(row[time_name], field="sampled_at")
            fingerprint = self.reading_fingerprint(
                source,
                record,
                revision,
                row.get(station_name),
                self._normalize_code_key(row.get(code_name)),
                point,
                row.get(value_name),
            )
            self.ledger_rows[triple] = {
                "fingerprint": fingerprint,
                "item_index": None,
            }
            self.staged_ledger[triple] = LedgerEntry(
                source=source,
                record=record,
                revision=revision,
                kind="reading",
                fingerprint=fingerprint,
                target_table=self.table.name,
                payload=dict(row),
            )

        for (source, record), row in list(self.existing_handovers.items()):
            revision = int(row.get("revision") or 0)
            triple = (source, record, revision)
            if triple in self.ledger_rows:
                continue
            handover_fingerprint = row.get("fingerprint")
            if not handover_fingerprint:
                point = parse_time(row["effective_text"], field="effective_at")
                changes = self._load_single_handover_changes(source, record)
                handover_fingerprint = self._handover_fingerprint(
                    row["station"], point, changes
                )
            self.ledger_rows[triple] = {
                "fingerprint": handover_fingerprint,
                "item_index": None,
            }
            self.staged_ledger[triple] = LedgerEntry(
                source=source,
                record=record,
                revision=revision,
                kind="handover",
                fingerprint=handover_fingerprint,
                target_table=self.table.name,
                payload=row,
            )

    def _load_single_handover_changes(
        self, source: Any, record: Any
    ) -> list[ChangeItem]:
        items = self.support_tables["items"].table
        rows = self.db.executable.execute(
            select(items).where(
                and_(
                    items.c.target_table == self.table.name,
                    items.c.source == source,
                    items.c.record_no == record,
                )
            )
        ).mappings()
        return [
            ChangeItem(
                self._normalize_code_key(row["code"]),
                row["device_id"],
                int(row.get("ordinal") or 0),
            )
            for row in rows
        ]

    def _reconcile_revisions(self) -> None:
        for item in self._all_versioned_items():
            triple = (item.source, item.record, item.revision)
            pre_revision = self._preexisting_revision(item.source, item.record)

            if triple in self.ledger_rows:
                row = self.ledger_rows[triple]
                incoming_raw_hash = hash_content(item.raw)
                raw_conflict = (
                    bool(row.get("raw_hash")) and row["raw_hash"] != incoming_raw_hash
                )
                if raw_conflict or row.get("fingerprint") != item.fingerprint:
                    raise RevisionConflictError(
                        "source/record/revision was already stored "
                        "with different content",
                        index=item.index,
                        item=item.raw,
                        issues=[
                            {
                                "index": item.index,
                                "source": item.source,
                                "record": item.record,
                                "revision": item.revision,
                                "original_item_index": row.get("item_index"),
                            }
                        ],
                    )
                item.skip_content_write = True
                continue

            if triple in self.staged_ledger:
                prior = self.staged_ledger[triple]
                raw_conflict = bool(prior.raw_hash) and prior.raw_hash != hash_content(
                    item.raw
                )
                if raw_conflict or prior.fingerprint != item.fingerprint:
                    raise RevisionConflictError(
                        "same source/record/revision occurs with "
                        "different content in this batch",
                        index=item.index,
                        item=item.raw,
                        issues=[
                            {
                                "index": item.index,
                                "first_index": prior.item_index,
                                "source": item.source,
                                "record": item.record,
                                "revision": item.revision,
                            }
                        ],
                    )
                item.skip_content_write = True
                continue

            matches_existing = self._adopt_matching_existing(item)
            if matches_existing:
                item.skip_content_write = True
            elif pre_revision is not None and item.revision < pre_revision:
                raise RevisionConflictError(
                    f"revision {item.revision} is below current "
                    f"revision {pre_revision}",
                    index=item.index,
                    item=item.raw,
                    issues=[
                        {
                            "index": item.index,
                            "source": item.source,
                            "record": item.record,
                            "revision": item.revision,
                            "current_revision": pre_revision,
                        }
                    ],
                )

            self.staged_ledger[triple] = LedgerEntry(
                source=item.source,
                record=item.record,
                revision=item.revision,
                kind="reading" if isinstance(item, ReadingItem) else "handover",
                fingerprint=item.fingerprint,
                raw_hash=hash_content(item.raw),
                target_table=self.table.name,
                item_index=item.index,
                payload=item.raw,
            )
            item.is_current_candidate = not item.skip_content_write

        max_revisions: dict[tuple[Any, Any], int] = {}
        for item in self._all_versioned_items():
            if item.is_current_candidate and not item.skip_content_write:
                key = (item.source, item.record)
                max_revisions[key] = max(
                    max_revisions.get(key, item.revision), item.revision
                )
        for item in self._all_versioned_items():
            key = (item.source, item.record)
            max_revision = max_revisions.get(key)
            if max_revision is None or item.revision != max_revision:
                item.is_current_candidate = False
                if key in max_revisions:
                    item.skip_content_write = True
            else:
                item.is_current_candidate = not item.skip_content_write

    def _adopt_matching_existing(self, item: ReadingItem | HandoveryItem) -> bool:
        key = (item.source, item.record)
        if isinstance(item, ReadingItem):
            row = self.existing_readings.get(key)
            if row is None:
                return False
            revision_name = self.schema.columns["revision"]
            if (
                row.get(revision_name) is not None
                and int(row[revision_name]) != item.revision
            ):
                return False
            for column, incoming in item.mapped.items():
                if column in {self.schema.pk_column, self.schema.device_column}:
                    continue
                if column not in row:
                    continue
                if not self._values_equal(incoming, row[column], column=column):
                    raise RevisionConflictError(
                        "source/record/revision was already stored "
                        "with different content",
                        index=item.index,
                        item=item.raw,
                    )
            return True

        row = self.existing_handovers.get(key)
        if row is None:
            return False
        if int(row.get("revision", -1)) != item.revision:
            return False
        if row.get("fingerprint") and row["fingerprint"] != item.fingerprint:
            raise RevisionConflictError(
                "source/record/revision was already stored "
                "with different handover content",
                index=item.index,
                item=item.raw,
            )
        return True

    def _values_equal(self, incoming: Any, stored: Any, *, column: str) -> bool:
        if incoming == stored:
            return True
        if isinstance(incoming, (int, float)) and isinstance(stored, (int, float)):
            return float(incoming) == float(stored)
        normalized_column = normalize_column_key(column)
        if normalized_column == normalize_column_key(self.schema.columns["time"]):
            try:
                left = parse_time(incoming, field="time").moment
                right = parse_time(stored, field="time").moment
                return left == right
            except CollectionImportError:
                return False
        if isinstance(incoming, str) or isinstance(stored, str):
            return str(incoming).strip() == str(stored).strip()
        try:
            return canonical_json(incoming) == canonical_json(stored)
        except Exception:
            return False

    # ------------------------------------------------------------- validation

    def _validate_devices(self) -> None:
        referenced: set[Any] = set()
        for handover in self.handovers:
            referenced.update(
                change.device_id
                for change in handover.changes
                if change.device_id is not None
            )
        for activation in self.activations:
            referenced.update(
                change.device_id
                for change in activation.changes
                if change.device_id is not None
            )
        if not referenced:
            return
        name = self._device_table_name()
        if name not in self.db.tables:
            raise CollectionImportError(f"device ledger table does not exist: {name}")
        device_table = self.db.load_table(name).table
        pk_columns = (
            list(device_table.primary_key.columns) if device_table.primary_key else []
        )
        pk_name = pk_columns[0].name if pk_columns else "id"
        rows = self.db.executable.execute(select(device_table.c[pk_name])).fetchall()
        existing = {self._scalar_identity(row[0]) for row in rows}
        for device_id in referenced:
            if self._scalar_identity(device_id) not in existing:
                raise CollectionImportError(
                    f"handover references non-existent device primary key {device_id!r}"
                )

    @staticmethod
    def _scalar_identity(value: Any) -> tuple[str, str]:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return (
                "number",
                str(int(value)) if float(value).is_integer() else repr(float(value)),
            )
        return ("scalar", type(value).__name__ + ":" + str(value))

    def _validate_effective_times(self) -> None:
        states: dict[tuple[Any, Any], dict[str, Any]] = {}
        ho_table = self.support_tables["handovers"].table
        for row in self.db.executable.execute(select(ho_table)).mappings():
            data = dict(row)
            if data.get("target_table") != self.table.name:
                continue
            states[(data["source"], data["record_no"])] = {
                "station": data["station"],
                "moment": parse_time(
                    data["effective_text"], field="effective_at"
                ).moment,
                "index": None,
            }
        for key, data in self.existing_handovers.items():
            if data.get("_external"):
                states[key] = {
                    "station": data["station"],
                    "moment": parse_time(
                        data["effective_text"], field="effective_at"
                    ).moment,
                    "index": None,
                }
        for handover in self.handovers:
            if handover.is_current_candidate:
                states[(handover.source, handover.record)] = {
                    "station": handover.station,
                    "moment": handover.time.moment,
                    "index": handover.index,
                }

        seen: dict[tuple[Any, Any], tuple[Any, Any]] = {}
        for key, state in states.items():
            group = (self._station_key(state["station"]), state["moment"])
            if group in seen and seen[group] != key:
                index = state.get("index")
                raise CollectionImportError(
                    "only one effective handover is allowed "
                    "for a station and timestamp",
                    index=index,
                )
            seen[group] = key

    # -------------------------------------------------------------- persistence

    def _persist_ledger(self) -> None:
        if not self.staged_ledger:
            return
        ledger = self.support_tables["revisions"]
        for entry in self.staged_ledger.values():
            self.db.executable.execute(
                ledger.table.insert().values(
                    target_table=entry.target_table,
                    kind=entry.kind,
                    source=entry.source,
                    record_no=entry.record,
                    revision=entry.revision,
                    fingerprint=entry.fingerprint,
                    raw_hash=entry.raw_hash,
                    content_hash=entry.fingerprint,
                    payload=canonical_json(entry.payload),
                    item_index=entry.item_index,
                    pk_value=entry.pk_value,
                )
            )

    def _persist_activations(self) -> None:
        slots = self.support_tables["slots"]
        for activation in self.activations:
            for change in activation.changes:
                key = self._slot_key(activation.station, change.code)
                self.baseline[key] = change.device_id
                existing = (
                    self.db.executable.execute(
                        select(slots.table).where(
                            and_(
                                slots.table.c.station == activation.station,
                                self._null_safe_eq(slots.table.c.code, change.code),
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if existing is None:
                    self.db.executable.execute(
                        slots.table.insert().values(
                            station=activation.station,
                            code=change.code,
                            device_id=change.device_id,
                        )
                    )
                else:
                    self.db.executable.execute(
                        slots.table.update()
                        .where(slots.table.c.id == existing["id"])
                        .values(device_id=change.device_id)
                    )

    @staticmethod
    def _null_safe_eq(column: Any, value: Any) -> Any:
        return column.is_(None) if value is None else column == value

    def _persist_handovers(self) -> None:
        handovers = self.support_tables["handovers"]
        items = self.support_tables["items"]
        for handover in self.handovers:
            if not handover.is_current_candidate:
                continue
            values = {
                "target_table": self.table.name,
                "source": handover.source,
                "record_no": handover.record,
                "revision": handover.revision,
                "station": handover.station,
                "effective_text": handover.time.text,
                "effective_raw": str(handover.time.raw),
                "fingerprint": handover.fingerprint,
            }
            existing = self.existing_handovers.get((handover.source, handover.record))
            if existing is None or existing.get("_external"):
                self.db.executable.execute(handovers.table.insert().values(**values))
            else:
                self.db.executable.execute(
                    handovers.table.update()
                    .where(handovers.table.c.id == existing["id"])
                    .values(**values)
                )

            self.db.executable.execute(
                items.table.delete().where(
                    and_(
                        items.table.c.target_table == self.table.name,
                        items.table.c.source == handover.source,
                        items.table.c.record_no == handover.record,
                    )
                )
            )
            for change in handover.changes:
                self.db.executable.execute(
                    items.table.insert().values(
                        target_table=self.table.name,
                        source=handover.source,
                        record_no=handover.record,
                        station=handover.station,
                        code=change.code,
                        device_id=change.device_id,
                        ordinal=change.ordinal,
                    )
                )

    def _ensure_range_index(self) -> None:
        if not self.table._check_ensure(self.ensure):
            return
        with suppress(Exception):
            self.table.create_index(
                [
                    self.schema.columns["station"],
                    self.schema.columns["code"],
                    self.schema.columns["time"],
                ]
            )
        with suppress(Exception):
            self.table.create_index(
                [self.schema.columns["source"], self.schema.columns["record"]]
            )

    # ---------------------------------------------------------------- timeline

    def _build_timeline(self, stations: Sequence[Any]) -> dict[str, Any]:
        events: list[dict[str, Any]] = []
        handovers = self.support_tables["handovers"].table
        items = self.support_tables["items"].table
        station_values = list(dict.fromkeys(stations))
        ho_query = select(handovers).where(handovers.c.target_table == self.table.name)
        item_query = select(items).where(items.c.target_table == self.table.name)
        if station_values:
            ho_query = ho_query.where(handovers.c.station.in_(station_values))
            item_query = item_query.where(items.c.station.in_(station_values))
        headers: dict[tuple[Any, Any], dict[str, Any]] = {}
        for row in self.db.executable.execute(ho_query).mappings():
            data = dict(row)
            headers[(data["source"], data["record_no"])] = data
        own_keys = set(headers)
        for event in self.external_events:
            event_key = (event.get("source"), event.get("record"))
            if event_key[0] is not None and event_key in own_keys:
                continue
            events.append(event)
        for row in self.db.executable.execute(item_query).mappings():
            item = dict(row)
            header = headers.get((item["source"], item["record_no"]))
            if header is None:
                continue
            point = parse_time(header["effective_text"], field="effective_at")
            events.append(
                {
                    "station": header["station"],
                    "code": self._normalize_code_key(item["code"]),
                    "device_id": item["device_id"],
                    "time": point,
                }
            )
        events.sort(key=lambda event: event["time"].moment)
        return {"baseline": dict(self.baseline), "events": events}

    def _resolve_device(
        self, timeline: Mapping[str, Any], station: Any, code: Any, moment: datetime
    ) -> Any:
        normalized_code = self._normalize_code_key(code)
        device = timeline["baseline"].get(self._slot_key(station, normalized_code))
        station_key = self._station_key(station)
        for event in timeline["events"]:
            if self._station_key(event["station"]) != station_key:
                continue
            if self._normalize_code_key(event["code"]) != normalized_code:
                continue
            if event["time"].moment <= moment:
                device = event["device_id"]
        return device

    # --------------------------------------------------------------- readings

    def _persist_readings(self, timeline: Mapping[str, Any]) -> None:
        time_column = self._column("time")
        reading_pks: dict[tuple[Any, Any], Any] = {}
        for item in self.readings:
            item.mapped[self.schema.columns["time"]] = time_for_column(
                item.time, time_column.type
            )

        for item in self.readings:
            key = (item.source, item.record)
            existing = self.existing_readings.get(key)
            if existing is not None:
                reading_pks[key] = existing[self.schema.pk_column]
            elif key in reading_pks:
                pass

            if not item.is_current_candidate:
                if existing is not None:
                    item.pk = existing[self.schema.pk_column]
                continue

            values = dict(item.mapped)
            desired = self._resolve_device(
                timeline, item.station, item.code, item.time.moment
            )
            values[self.schema.device_column] = desired
            pk: Any
            if existing is not None:
                pk = existing[self.schema.pk_column]
                self.db.executable.execute(
                    self.table.table.update()
                    .where(self.table.table.c[self.schema.pk_column] == pk)
                    .values(**item.mapped)
                )
                item.pk = pk
                reading_pks[key] = pk
            else:
                result = self.db.executable.execute(
                    self.table.table.insert().values(**values)
                )
                pk = None
                if result.inserted_primary_key:
                    pk = result.inserted_primary_key[0]
                if pk is None:
                    pk = getattr(result, "lastrowid", None)
                item.pk = pk
                reading_pks[key] = pk

        for item in self.readings:
            if item.pk is None:
                key = (item.source, item.record)
                item.pk = reading_pks.get(key)
                existing = self.existing_readings.get(key)
                if item.pk is None and existing is not None:
                    item.pk = existing[self.schema.pk_column]

    # ------------------------------------------------------------ recomputation

    def _load_old_handover_changes(self) -> dict[tuple[Any, Any], list[ChangeItem]]:
        result: dict[tuple[Any, Any], list[ChangeItem]] = {}
        items = self.support_tables["items"].table
        for key, header in self.existing_handovers.items():
            if header.get("_changes") is not None:
                result[key] = list(header["_changes"])
                continue
            rows = self.db.executable.execute(
                select(items).where(
                    and_(
                        items.c.target_table == self.table.name,
                        items.c.source == header["source"],
                        items.c.record_no == header["record_no"],
                    )
                )
            ).mappings()
            changes = []
            for row in rows:
                data = dict(row)
                changes.append(
                    ChangeItem(
                        self._normalize_code_key(data["code"]),
                        data["device_id"],
                        int(data.get("ordinal") or 0),
                    )
                )
            result[key] = changes
        return result

    def _affected_ranges(self, timeline: Mapping[str, Any]) -> list[dict[str, Any]]:
        old_changes = self._load_old_handover_changes()
        contributions: dict[tuple[Any, Any], dict[str, Any]] = {}

        def add(station: Any, code: Any, point: TimePoint) -> None:
            station_key = self._station_key(station)
            normalized_code = self._normalize_code_key(code)
            key = (station_key, normalized_code)
            group = contributions.setdefault(
                key,
                {"station": station, "code": normalized_code, "points": []},
            )
            group["points"].append(point)

        for handover in self.handovers:
            if not handover.is_current_candidate:
                continue
            key = (handover.source, handover.record)
            old = self.existing_handovers.get(key)
            if old is not None:
                old_point = parse_time(old["effective_text"], field="effective_at")
                for change in old_changes.get(key, []):
                    add(old["station"], change.code, old_point)
            for change in handover.changes:
                add(handover.station, change.code, handover.time)

        ranges: list[dict[str, Any]] = []
        for (station_key, code), group in contributions.items():
            points = group["points"]
            if not points:
                continue
            start = min(points, key=lambda point: point.moment)
            latest = max(points, key=lambda point: point.moment)
            end: TimePoint | None = None
            for event in timeline["events"]:
                if self._station_key(event["station"]) != station_key:
                    continue
                if self._normalize_code_key(event["code"]) != code:
                    continue
                if event["time"].moment > latest.moment and (
                    end is None or event["time"].moment < end.moment
                ):
                    end = event["time"]
            ranges.append(
                {
                    "station": group["station"],
                    "code": code,
                    "start": start,
                    "end": end,
                }
            )
        return ranges

    def _recompute_attribution(self, timeline: Mapping[str, Any]) -> None:
        pk_column = self.schema.pk_column
        device_column = self.schema.device_column
        time_column = self._column("time")
        candidate_rows: dict[Any, dict[str, Any]] = {}

        for range_spec in self._affected_ranges(timeline):
            station_column = self._column("station")
            code_column = self._column("code")
            clauses = [station_column == range_spec["station"]]
            code = range_spec["code"]
            clauses.append(
                code_column.is_(None) if code is None else code_column == code
            )
            clauses.append(
                time_column >= time_for_column(range_spec["start"], time_column.type)
            )
            end = range_spec["end"]
            if end is not None:
                clauses.append(time_column <= time_for_column(end, time_column.type))
            rows = self.db.executable.execute(
                select(self.table.table).where(and_(*clauses))
            ).mappings()
            for row in rows:
                data = dict(row)
                candidate_rows[data[pk_column]] = data

        for item in self.readings:
            if item.pk is None:
                continue
            row = (
                self.db.executable.execute(
                    select(self.table.table).where(
                        self.table.table.c[pk_column] == item.pk
                    )
                )
                .mappings()
                .first()
            )
            if row is not None:
                data = dict(row)
                candidate_rows[data[pk_column]] = data

        for pk, row in candidate_rows.items():
            station = row[self.schema.columns["station"]]
            code = row[self.schema.columns["code"]]
            point = parse_time(row[self.schema.columns["time"]], field="sampled_at")
            desired = self._resolve_device(timeline, station, code, point.moment)
            current = row.get(device_column)
            if self._scalar_identity(current) == self._scalar_identity(desired):
                self.final_devices[pk] = current
                continue
            self.db.executable.execute(
                self.table.table.update()
                .where(self.table.table.c[pk_column] == pk)
                .values({device_column: desired})
            )
            self.final_devices[pk] = desired

    def _build_result(self) -> BatchResult:
        result = BatchResult()
        for item in sorted(self.readings, key=lambda reading: reading.index):
            pk = item.pk
            device = self.final_devices.get(pk)
            if pk is not None and device is None:
                row = self.db.executable.execute(
                    select(self.table.table.c[self.schema.device_column]).where(
                        self.table.table.c[self.schema.pk_column] == pk
                    )
                ).first()
                if row is not None:
                    device = row[0]
            reading_result = ReadingResult(
                pk,
                device,
                source=item.source,
                record_id=item.record,
                record=item.record,
                revision=item.revision,
                index=item.index,
            )
            result.append(reading_result)
            result.by_record[str(item.record)] = reading_result
        return result
