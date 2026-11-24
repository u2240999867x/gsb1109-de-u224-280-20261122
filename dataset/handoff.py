"""Atomic import of related site and collector records.

The hand-off stream is read exactly once.  Legacy identifiers are only unique
within a vendor and record type, while the target database assigns its own
primary keys.  Records can refer to each other in either direction and in any
order.

:class:`Handoff` stages the stream, resolves every reference against both the
batch and rows already present in the database, and only then writes.  The
site and collector tables have mutually dependent foreign keys declared
``DEFERRABLE INITIALLY DEFERRED``, so records may point at rows that are
inserted later in the same transaction.  If any reference cannot be resolved,
nothing from the batch is written and :class:`UnresolvedReferenceError` reports
each offending original record together with the referenced vendor, type and
number.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    Unicode,
    UniqueConstraint,
    select,
    text,
)
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from sqlalchemy.schema import Table as SQLATable

from dataset.util import DatasetError, normalize_column_name

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from dataset.database import Database

log = logging.getLogger(__name__)

_SITE = "site"
_COLLECTOR = "collector"
_ENTITIES: tuple[str, str] = (_SITE, _COLLECTOR)

_ENTITY_ALIASES = {
    "site": _SITE,
    "sites": _SITE,
    "站点": _SITE,
    "collector": _COLLECTOR,
    "collectors": _COLLECTOR,
    "采集器": _COLLECTOR,
}
_ENTITY_PLURALS: dict[str, str] = {_SITE: "sites", _COLLECTOR: "collectors"}

#: Foreign-key column on each table that points at the other table.
_FK_COLUMNS: dict[str, str] = {_SITE: "collector_id", _COLLECTOR: "site_id"}
_TARGET_ENTITY: dict[str, str] = {_SITE: _COLLECTOR, _COLLECTOR: _SITE}

#: Input keys that describe the envelope rather than payload columns.
_RESERVED_KEYS = frozenset(
    {
        "id",
        "type",
        "kind",
        "entity",
        "table",
        "record_type",
        "vendor",
        "supplier",
        "source",
        "source_vendor",
        "供应商",
        "number",
        "legacy_id",
        "legacy_number",
        "编号",
        "old_id",
        "old_number",
        "external_id",
        "external_number",
        "source_id",
        "source_number",
        "asset_number",
        "asset_no",
        "asset_id",
        "permanent_asset_number",
        "permanent_asset_no",
        "permanent_asset",
        "permanent_asset_id",
        "永久资产号",
        "asset",
        "permanent_number",
        "permanent_id",
        "references",
        "refs",
        "relations",
        "data",
        "payload",
        "attributes",
        "site",
        "collector",
        "站点",
        "采集器",
        "site_id",
        "collector_id",
        "site_pk",
        "collector_pk",
    }
)

_NUMBER_ALIASES = (
    "number",
    "legacy_id",
    "legacy_number",
    "编号",
    "old_id",
    "old_number",
    "external_id",
    "external_number",
    "source_id",
    "source_number",
)
_ASSET_ALIASES = (
    "asset_number",
    "asset_no",
    "asset_id",
    "permanent_asset_number",
    "permanent_asset_no",
    "permanent_asset",
    "永久资产号",
    "permanent_asset_id",
    "permanent_number",
    "permanent_id",
    "asset",
)
_VENDOR_ALIASES = (
    "vendor",
    "supplier",
    "供应商",
    "vendor_name",
    "source_vendor",
    "source",
)
_TYPE_ALIASES = (
    "type",
    "kind",
    "entity",
    "table",
    "record_type",
    "source_type",
)


class HandoffError(DatasetError):
    """Base class for a rejected hand-off batch."""


class HandoffConflictError(HandoffError):
    """One legacy key was associated with incompatible target rows."""


@dataclass(frozen=True)
class AssetKey:
    """A legacy identifier scoped to a vendor and record type."""

    vendor: str
    type: str
    number: str

    def __str__(self) -> str:
        return f"{self.vendor}/{self.type}/{self.number}"


@dataclass(frozen=True)
class Reference:
    """A pointer at another record in the legacy numbering scheme."""

    vendor: str
    type: str
    number: Any = None
    asset_number: Any = None

    def matches_asset(self) -> bool:
        return self.asset_number is not None and self.number is None

    def __str__(self) -> str:
        target = self.number if not self.matches_asset() else self.asset_number
        return f"{self.vendor}/{self.type}/{target}"


@dataclass
class HandoffRecord:
    """A normalized record from the hand-off stream.

    ``type`` is ``"site"`` or ``"collector"`` and ``number`` is the legacy
    identifier.  Fields in ``data`` become payload columns on the target
    table.  ``references`` maps ``collector_id``/``site_id`` to the target's
    legacy identity.
    """

    type: str
    vendor: str
    number: Any
    data: dict[str, Any] = field(default_factory=dict)
    asset_number: Any | None = None
    references: dict[str, Reference] = field(default_factory=dict)
    raw: Mapping[str, Any] | None = None
    #: True when the input explicitly carried a legacy number.  Records that
    #: only carry a permanent asset number must not overwrite the number of
    #: the existing row they matched.
    number_explicit: bool = True

    @property
    def kind(self) -> str:
        return self.type


@dataclass(frozen=True)
class UnresolvedReference:
    """A dangling reference and the original record that contained it."""

    record: Mapping[str, Any]
    table: str
    column: str
    vendor: str
    type: str
    number: Any

    @property
    def referenced_vendor(self) -> str:
        return self.vendor

    @property
    def referenced_type(self) -> str:
        return self.type

    @property
    def referenced_number(self) -> Any:
        return self.number

    @property
    def original_record(self) -> Mapping[str, Any]:
        return self.record

    @property
    def original(self) -> Mapping[str, Any]:
        return self.record

    @property
    def reference(self) -> dict[str, Any]:
        return {"vendor": self.vendor, "type": self.type, "number": self.number}

    def to_dict(self) -> dict[str, Any]:
        return {
            "record": dict(self.record),
            "table": self.table,
            "column": self.column,
            "vendor": self.vendor,
            "type": self.type,
            "number": self.number,
        }

    as_dict = to_dict


class UnresolvedReferenceError(HandoffError):
    """A batch contains references with no target in the batch or database."""

    def __init__(self, references: Sequence[UnresolvedReference]):
        self.references = list(references)
        super().__init__(self._message())

    @property
    def unresolved(self) -> list[UnresolvedReference]:
        return self.references

    @property
    def unresolved_references(self) -> list[UnresolvedReference]:
        return self.references

    @property
    def failures(self) -> list[UnresolvedReference]:
        return self.references

    def _message(self) -> str:
        details = "; ".join(
            f"{ref.table}.{ref.column} -> {ref.vendor}/{ref.type}/{ref.number} "
            f"(record {dict(ref.record)!r})"
            for ref in self.references
        )
        return f"Unresolved hand-off references: {details}"

    def to_dict(self) -> dict[str, Any]:
        return {"references": [ref.to_dict() for ref in self.references]}

    as_dict = to_dict

    def __iter__(self) -> Any:
        return iter(self.references)

    def __len__(self) -> int:
        return len(self.references)

    def __getitem__(self, index: int) -> UnresolvedReference:
        return self.references[index]

    @property
    def details(self) -> list[UnresolvedReference]:
        return self.references

    @property
    def records(self) -> list[Mapping[str, Any]]:
        return [reference.record for reference in self.references]

    @property
    def ok(self) -> bool:
        return False


@dataclass
class HandoffResult:
    """Primary keys assigned to the legacy keys in a successful batch."""

    ids: dict[AssetKey, Any] = field(default_factory=dict)
    inserted: dict[str, list[Any]] = field(
        default_factory=lambda: {_SITE: [], _COLLECTOR: []}
    )
    updated: dict[str, list[Any]] = field(
        default_factory=lambda: {_SITE: [], _COLLECTOR: []}
    )
    errors: list[UnresolvedReference] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def success(self) -> bool:
        return self.ok

    @property
    def applied(self) -> bool:
        return self.ok

    @property
    def unresolved(self) -> list[UnresolvedReference]:
        return self.errors

    @property
    def old_to_new(self) -> dict[AssetKey, Any]:
        return self.ids

    @property
    def mapping(self) -> dict[AssetKey, Any]:
        return self.ids

    @property
    def new_to_old(self) -> dict[Any, AssetKey]:
        return {value: key for (key, value) in self.ids.items()}

    @property
    def new_ids(self) -> dict[AssetKey, Any]:
        return self.ids

    @property
    def id_map(self) -> dict[AssetKey, Any]:
        return self.ids

    def keys(self) -> Any:
        return self.ids.keys()

    def values(self) -> Any:
        return self.ids.values()

    def items(self) -> Any:
        return self.ids.items()

    def id_for(self, type: Any, vendor: Any = None, number: Any = None) -> Any:
        """Return the target primary key for a legacy identifier.

        Accepts ``id_for("site", "A", 7)`` or a pre-built
        :class:`AssetKey`.
        """
        if isinstance(type, AssetKey):
            return self.ids[type]
        entity = _normalize_entity(type)
        return self.ids[AssetKey(str(vendor), entity, _normalize_number(number))]

    def mapping_for(self, type: str) -> dict[AssetKey, Any]:
        entity = _normalize_entity(type)
        return {key: value for (key, value) in self.ids.items() if key.type == entity}

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, AssetKey):
            return self.ids[key]
        if isinstance(key, tuple) and len(key) == 3:
            return self.ids[
                AssetKey(
                    str(key[0]), _normalize_entity(key[1]), _normalize_number(key[2])
                )
            ]
        if isinstance(key, str) and key.count("/") == 2:
            vendor, type_, number = key.split("/", 2)
            return self.ids[AssetKey(vendor, _normalize_entity(type_), number)]
        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        try:
            self[key]
        except (KeyError, HandoffError):
            return False
        return True

    def __iter__(self) -> Any:
        return iter(self.ids)

    def __len__(self) -> int:
        return len(self.ids)

    def as_dict(self) -> dict[str, Any]:
        """Return the legacy-to-target mapping keyed by ``vendor/type/number``."""
        return {str(key): value for (key, value) in self.ids.items()}

    @property
    def site_ids(self) -> dict[AssetKey, Any]:
        return self.mapping_for(_SITE)

    @property
    def collector_ids(self) -> dict[AssetKey, Any]:
        return self.mapping_for(_COLLECTOR)


@dataclass
class _Handle:
    """A target row, either already present or scheduled for insertion."""

    entity: str
    id: Any | None = None
    existing: bool = False
    keys: set[AssetKey] = field(default_factory=set)
    #: Keys that came from the batch, excluding pre-existing legacy keys.
    batch_keys: set[AssetKey] = field(default_factory=set)
    data: dict[str, Any] = field(default_factory=dict)
    asset_number: Any | None = None
    references: dict[str, Reference] = field(default_factory=dict)
    touched: bool = False
    #: Set when an existing row is found by permanent asset number instead of
    #: by its legacy key.
    matched_by_asset: bool = False
    records: list[HandoffRecord] = field(default_factory=list)
    resolved: dict[str, _Handle] = field(default_factory=dict)

    @property
    def vendor(self) -> str | None:
        for key in self.keys:
            return key.vendor
        return None

    def add_key(self, key: AssetKey) -> None:
        self.keys.add(key)

    def merge(self, record: HandoffRecord, key: AssetKey) -> None:
        self.add_key(key)
        self.batch_keys.add(key)
        self.records.append(record)
        self.touched = True
        if record.asset_number is not None:
            self.asset_number = record.asset_number
        for name, value in record.data.items():
            self.data[name] = value
        for column, reference in record.references.items():
            previous = self.references.get(column)
            if previous is not None and previous != reference:
                raise HandoffConflictError(
                    f"Conflicting references for {column}: {previous} and {reference}"
                )
            self.references[column] = reference


@dataclass
class _EntityTable:
    entity: str
    name: str
    table: SQLATable
    #: logical name -> actual column name
    columns: dict[str, str]

    def column(self, logical: str) -> Any:
        return self.table.c[self.columns[logical]]


@dataclass
class _Schema:
    entities: dict[str, _EntityTable]


@dataclass
class _ExistingRows:
    handles: list[_Handle] = field(default_factory=list)
    by_key: dict[AssetKey, _Handle] = field(default_factory=dict)
    by_asset: dict[tuple[str, str], _Handle] = field(default_factory=dict)


class Handoff:
    """Land a read-once stream of sites and collectors into two tables.

    ::

        result = Handoff(db).load(
            [
                {
                    "type": "site",
                    "vendor": "A",
                    "number": 7,
                    "references": {
                        "collector_id": {
                            "vendor": "A",
                            "type": "collector",
                            "number": 7,
                        }
                    },
                },
                {
                    "type": "collector",
                    "vendor": "A",
                    "number": 7,
                    "asset_number": "A-100",
                    "references": {
                        "site_id": {"vendor": "A", "type": "site", "number": 7}
                    },
                },
            ]
        )
        site_id = result.id_for("site", "A", 7)

    Records may appear in any order.  A reference can target another record in
    the stream or one that already exists in the database.  Existing rows are
    matched by legacy key, or by ``(vendor, permanent asset number)`` when the
    incoming record carries an asset number.
    """

    def __init__(
        self,
        db: Database,
        *,
        site_table: str | None = None,
        collector_table: str | None = None,
        id_column: str = "id",
        vendor_column: str = "vendor",
        type_column: str = "type",
        number_column: str = "legacy_id",
        asset_number_column: str = "permanent_asset_number",
    ) -> None:
        self.db = db
        self._site_table_option = site_table
        self._collector_table_option = collector_table
        # ``number_column`` is the default legacy-id column.  If an existing
        # table uses an advertised alias, discovery will find that column.
        self.id_column = id_column
        self.vendor_column = vendor_column
        self.type_column = type_column
        self.number_column = number_column
        self.asset_number_column = asset_number_column

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def import_records(
        self, records: Iterable[HandoffRecord | Mapping[str, Any]]
    ) -> HandoffResult:
        """Alias for :meth:`load`."""
        return self.load(records)

    def import_stream(
        self, records: Iterable[HandoffRecord | Mapping[str, Any]]
    ) -> HandoffResult:
        """Alias for :meth:`load`."""
        return self.load(records)

    def create_tables(self) -> None:
        """Create the two tables and their deferred constraints if needed."""
        self._ensure_schema(())

    def load(
        self,
        records: Iterable[HandoffRecord | Mapping[str, Any] | Sequence[Any]],
        *,
        raise_on_error: bool = True,
    ) -> HandoffResult:
        """Consume ``records`` once and import them atomically.

        Every record and reference is validated before the first write.  On
        failure the batch contributes no rows and no legacy-to-target mapping;
        previously committed data is left untouched.  By default an
        :class:`UnresolvedReferenceError` is raised; pass
        ``raise_on_error=False`` to receive a result with ``ok == False`` and
        the unresolved references in ``result.errors`` instead.
        """
        if isinstance(records, (Mapping, HandoffRecord)):
            records = [records]
        staged = [self._normalize_record(record) for record in records]
        if not staged:
            return HandoffResult()

        connector = self.db.executable
        # ``db.in_transaction`` tracks explicit caller transactions only.  An
        # autobegin opened by the read phase contains no writes and can be
        # closed safely; an explicit transaction is preserved via a savepoint.
        caller_transaction = self.in_outer_transaction

        # Validation and writes share one batch transaction.  Rows and the
        # old-to-new mapping from a rejected batch are discarded; schema
        # objects may remain, which is harmless.
        try:
            with _BatchTransaction(connector, caller_transaction):
                schema = self._ensure_schema(staged)
                existing = self._read_existing(schema)
                handles, key_map = self._build_handles(staged, existing)
                self._resolve(schema, handles, key_map, existing)

                inserted = [handle for handle in handles if not handle.existing]
                updated = [
                    handle for handle in handles if handle.existing and handle.touched
                ]

                for handle in inserted:
                    self._insert(schema.entities[handle.entity], handle)
                for handle in inserted + updated:
                    self._write_links(schema.entities[handle.entity], handle)
                self._check_foreign_keys(schema, inserted + updated)
        except UnresolvedReferenceError as error:
            if raise_on_error:
                raise
            return HandoffResult(errors=list(error.references))
        except HandoffError as error:
            if raise_on_error:
                raise
            return HandoffResult(errors=[_error_as_reference(error)])

        result = HandoffResult()
        # Only report keys touched by this batch, never rows that merely
        # happened to exist in the database.
        reported = inserted + updated
        for handle in reported:
            if handle.id is None:
                continue
            for key in handle.batch_keys:
                result.ids[key] = handle.id
        for handle in inserted:
            result.inserted[handle.entity].append(handle.id)
        for handle in updated:
            result.updated[handle.entity].append(handle.id)
        return result

    @property
    def in_outer_transaction(self) -> bool:
        return bool(self.db.in_transaction)

    @property
    def site_table(self) -> SQLATable:
        return self._ensure_schema(()).entities[_SITE].table

    @property
    def collector_table(self) -> SQLATable:
        return self._ensure_schema(()).entities[_COLLECTOR].table

    # ------------------------------------------------------------------
    # Input normalization
    # ------------------------------------------------------------------
    def _normalize_record(
        self, value: HandoffRecord | Mapping[str, Any] | Sequence[Any]
    ) -> HandoffRecord:
        if isinstance(value, HandoffRecord):
            return value
        if isinstance(value, Mapping):
            return self._normalize_mapping(value)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return self._normalize_tuple(value)
        if hasattr(value, "_asdict"):
            return self._normalize_mapping(value._asdict())
        attributes = getattr(value, "__dict__", None)
        if isinstance(attributes, dict):
            return self._normalize_mapping(attributes)
        raise HandoffError(f"Unsupported record value: {value!r}")

    def _normalize_tuple(self, value: Sequence[Any]) -> HandoffRecord:
        """Accept ``(type, vendor, number, references, payload)`` tuples."""
        if not 3 <= len(value) <= 6:
            raise HandoffError(f"Invalid record tuple: {value!r}")
        entity = _normalize_entity(value[0])
        vendor = str(value[1])
        number = value[2]
        references = value[3] if len(value) > 3 and value[3] else {}
        payload = value[4] if len(value) > 4 and value[4] else {}
        asset_number = value[5] if len(value) > 5 else None
        if not isinstance(references, Mapping):
            raise HandoffError(f"Invalid record references: {value!r}")
        if not isinstance(payload, Mapping):
            raise HandoffError(f"Invalid record payload: {value!r}")
        return HandoffRecord(
            type=entity,
            vendor=vendor,
            number=number,
            data=dict(payload),
            asset_number=asset_number,
            references=self._normalize_references(
                {"type": entity, "vendor": vendor, "references": references},
                entity,
                vendor,
            ),
            raw={"type": entity, "vendor": vendor, "number": number},
        )

    def _normalize_mapping(self, raw: Mapping[str, Any]) -> HandoffRecord:
        type_value = _first(raw, *_TYPE_ALIASES)
        if type_value is None:
            raise HandoffError(f"Record has no type: {dict(raw)!r}")
        entity = _normalize_entity(type_value)

        vendor_value = _first(raw, *_VENDOR_ALIASES)
        if vendor_value is None:
            raise HandoffError(f"Record has no vendor: {dict(raw)!r}")
        vendor = str(vendor_value)

        asset_number = _first(raw, *_ASSET_ALIASES)
        number = _first(raw, *_NUMBER_ALIASES)
        number_explicit = number is not None
        if number is None:
            # A record for an already-known asset may carry only the
            # permanent asset number; use it as the lookup identity but do
            # not treat it as a new legacy number.
            if asset_number is None:
                raise HandoffError(f"Record has no legacy number: {dict(raw)!r}")
            number = asset_number

        payload = _first(raw, "data", "payload", "attributes")
        if payload is None:
            data = {
                key: value
                for (key, value) in raw.items()
                if str(key).lower() not in _RESERVED_KEYS
            }
        elif isinstance(payload, Mapping):
            data = dict(payload)
        else:
            raise HandoffError(f"Record data must be a mapping: {dict(raw)!r}")

        references = self._normalize_references(raw, entity, vendor)
        return HandoffRecord(
            type=entity,
            vendor=vendor,
            number=number,
            data=data,
            asset_number=asset_number,
            references=references,
            raw=_public_record(raw),
            number_explicit=number_explicit,
        )

    def _normalize_references(
        self, raw: Mapping[str, Any], entity: str, vendor: str
    ) -> dict[str, Reference]:
        references: dict[str, Reference] = {}
        source = _first(raw, "references", "refs", "relations")
        if source is not None and not isinstance(source, Mapping):
            source = self._normalize_reference_list(source)
        if source is not None:
            if isinstance(source, Mapping):
                entries: Iterable[tuple[Any, Any]] = source.items()
            elif isinstance(source, Iterable) and not isinstance(source, (str, bytes)):
                entries = []
                for item in source:
                    if not isinstance(item, Mapping):
                        raise HandoffError(
                            f"Invalid reference entry in record: {dict(raw)!r}"
                        )
                    column = _first(item, "column", "field", "name", "target")
                    if column is None:
                        raise HandoffError(f"Reference has no column: {dict(item)!r}")
                    entries.append((column, item))
            else:
                raise HandoffError(f"Invalid references in record: {dict(raw)!r}")

            for column, value in entries:
                target_column = self._reference_column(entity, column)
                references[target_column] = self._normalize_reference(
                    value, target_column, vendor
                )

        # Shorthand forms such as ``{"site": 7}`` and ``{"collector_id": 7}``.
        for key in (
            "site",
            "collector",
            "site_id",
            "collector_id",
            "site_pk",
            "collector_pk",
            "站点",
            "采集器",
        ):
            if key in raw and raw[key] is not None:
                target_column = self._reference_column(entity, key)
                if target_column not in references:
                    references[target_column] = self._normalize_reference(
                        raw[key], target_column, vendor
                    )

        return references

    @staticmethod
    def _normalize_reference_list(source: Any) -> list[Mapping[str, Any]]:
        """Expand compact string references such as ``"collector:A:7"``."""
        if isinstance(source, str):
            source = [source]
        if not isinstance(source, Iterable):
            raise HandoffError(f"Invalid references: {source!r}")
        normalized: list[Mapping[str, Any]] = []
        for item in source:
            if isinstance(item, str):
                parts = item.split(":", 2)
                if len(parts) != 3:
                    raise HandoffError(f"Invalid reference: {item!r}")
                column, vendor, number = parts
                normalized.append(
                    {"column": column, "vendor": vendor, "number": number}
                )
                continue
            normalized.append(item)
        return normalized

    def _normalize_reference(self, value: Any, column: str, vendor: str) -> Reference:
        target_entity = self._entity_for_column(column)
        if isinstance(value, Reference):
            return Reference(
                vendor=str(value.vendor),
                type=_normalize_entity(value.type),
                number=value.number,
                asset_number=value.asset_number,
            )
        if isinstance(value, Mapping):
            ref_type = _first(value, *_TYPE_ALIASES) or target_entity
            ref_vendor = _first(value, *_VENDOR_ALIASES) or vendor
            number = _first(value, "id", *_NUMBER_ALIASES)
            asset_number = _first(value, *_ASSET_ALIASES)
            if number is None and asset_number is None:
                raise HandoffError(f"Reference has no number: {dict(value)!r}")
            return Reference(
                vendor=str(ref_vendor),
                type=_normalize_entity(ref_type),
                number=number,
                asset_number=asset_number,
            )
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            if len(value) == 2:
                # (vendor, number) for the column's implied type.
                ref_vendor, number = value
                return Reference(
                    vendor=str(ref_vendor), type=target_entity, number=number
                )
            if len(value) == 3:
                ref_vendor, ref_type, number = value
                return Reference(
                    vendor=str(ref_vendor),
                    type=_normalize_entity(ref_type),
                    number=number,
                )
            raise HandoffError(f"Invalid reference tuple: {value!r}")
        return Reference(vendor=vendor, type=target_entity, number=value)

    def _reference_column(self, source_entity: str, column: Any) -> str:
        name = str(column).strip().lower()
        if name in ("site", "sites", "site_id", "site_pk", "站点"):
            target_entity = _SITE
        elif name in (
            "collector",
            "collectors",
            "collector_id",
            "collector_pk",
            "采集器",
        ):
            target_entity = _COLLECTOR
        else:
            raise HandoffError(f"Unknown reference column: {column!r}")
        if target_entity == source_entity:
            raise HandoffError(
                f"A {source_entity} cannot reference another {source_entity}"
            )
        return _FK_COLUMNS[source_entity]

    def _entity_for_column(self, column: str) -> str:
        # ``collector_id`` points at a collector and ``site_id`` at a site.
        for entity, fk_column in _FK_COLUMNS.items():
            if fk_column == column:
                return _TARGET_ENTITY[entity]
        raise HandoffError(f"Unknown reference column: {column!r}")

    # ------------------------------------------------------------------
    # Schema discovery / creation
    # ------------------------------------------------------------------
    def _table_name(self, entity: str) -> str:
        option = (
            self._site_table_option if entity == _SITE else self._collector_table_option
        )
        if option is not None:
            return option
        candidates = (
            ("sites", "site") if entity == _SITE else ("collectors", "collector")
        )
        available = set(self.db.tables)
        for candidate in candidates:
            if candidate in available:
                return candidate
        return candidates[0]

    def _ensure_schema(self, records: Sequence[HandoffRecord]) -> _Schema:
        conn = self.db.executable
        metadata = MetaData(schema=self.db.schema)
        tables: dict[str, SQLATable] = {}
        names = {entity: self._table_name(entity) for entity in _ENTITIES}

        for entity in _ENTITIES:
            name = names[entity]
            try:
                tables[entity] = SQLATable(
                    name,
                    metadata,
                    schema=self.db.schema,
                    autoload_with=conn,
                )
            except NoSuchTableError:
                tables[entity] = self._define_table(entity, name, metadata)

        payload_columns: dict[str, dict[str, Any]] = {_SITE: {}, _COLLECTOR: {}}
        for record in records:
            for column_name in record.data:
                safe_name = normalize_column_name(column_name)
                payload_columns[record.type].setdefault(
                    safe_name, record.data[column_name]
                )

        for entity in _ENTITIES:
            self._add_payload_columns(conn, tables[entity], payload_columns[entity])

        metadata.create_all(conn, checkfirst=True)

        entities: dict[str, _EntityTable] = {}
        for entity in _ENTITIES:
            name = names[entity]
            table = SQLATable(
                name,
                MetaData(schema=self.db.schema),
                schema=self.db.schema,
                autoload_with=conn,
            )
            columns = self._map_columns(entity, table)
            before = dict(columns)
            self._add_required_columns(conn, table, columns)
            self._add_link_column(conn, entity, table, columns)
            if columns != before:
                table = SQLATable(
                    name,
                    MetaData(schema=self.db.schema),
                    schema=self.db.schema,
                    autoload_with=conn,
                )
                columns = self._map_columns(entity, table)
            missing = self._missing_columns(entity, columns)
            if missing:
                raise HandoffError(
                    f"Table {name} is missing required columns: {sorted(missing)}"
                )
            entities[entity] = _EntityTable(
                entity=entity, name=name, table=table, columns=columns
            )
        self._invalidate_table_cache(names.values())
        return _Schema(entities=entities)

    def _invalidate_table_cache(self, names: Iterable[str]) -> None:
        """Make cached ``dataset.Table`` objects reflect altered columns."""
        for name in names:
            cached = self.db._tables.get(name)
            if cached is None:
                continue
            cached._table = None
            cached._columns = None
            cached._indexes = []

    def _define_table(self, entity: str, name: str, metadata: MetaData) -> SQLATable:
        other_entity = _TARGET_ENTITY[entity]
        other_name = self._table_name(other_entity)
        fk_column = _FK_COLUMNS[entity]
        target_schema = f"{self.db.schema}." if self.db.schema else ""
        # The target table may be created in this same ``create_all`` call;
        # SQLAlchemy resolves mutual string references fine, but using the
        # schema-qualified name keeps PostgreSQL search paths unambiguous.
        target = f"{target_schema}{other_name}.{self.id_column}"

        table = SQLATable(name, metadata, schema=self.db.schema)
        table.append_column(
            Column(self.id_column, Integer, primary_key=True, autoincrement=True)
        )
        table.append_column(Column(self.vendor_column, Unicode(255), nullable=False))
        table.append_column(Column(self.type_column, Unicode(32), nullable=False))
        table.append_column(Column(self.number_column, Unicode(255), nullable=False))
        table.append_column(
            Column(self.asset_number_column, Unicode(255), nullable=True)
        )
        table.append_column(
            Column(
                fk_column,
                Integer,
                ForeignKey(target, deferrable=True, initially="DEFERRED"),
                nullable=False,
            )
        )
        table.append_constraint(
            UniqueConstraint(
                self.vendor_column,
                self.type_column,
                self.number_column,
                name=f"uq_{name}_vendor_type_number",
            )
        )
        table.append_constraint(
            UniqueConstraint(
                self.vendor_column,
                self.asset_number_column,
                name=f"uq_{name}_vendor_asset_number",
            )
        )
        return table

    def _map_columns(self, entity: str, table: SQLATable) -> dict[str, str]:
        names = {column.name.lower(): column.name for column in table.columns}
        columns: dict[str, str] = {}

        if self.id_column.lower() in names:
            columns["id"] = names[self.id_column.lower()]
        else:
            primary = [column.name for column in table.primary_key.columns]
            if primary:
                columns["id"] = primary[0]

        for logical, aliases in (
            ("vendor", (self.vendor_column, *_VENDOR_ALIASES)),
            ("number", (self.number_column, *_NUMBER_ALIASES)),
            ("asset_number", (self.asset_number_column, *_ASSET_ALIASES)),
            ("type", (self.type_column, *_TYPE_ALIASES)),
        ):
            for candidate in aliases:
                if candidate.lower() in names:
                    columns[logical] = names[candidate.lower()]
                    break

        fk_column = _FK_COLUMNS[entity]
        other = _TARGET_ENTITY[entity]
        for candidate in (
            fk_column,
            f"{other}_pk",
            f"{other}_id",
            other,
            _ENTITY_PLURALS[other],
        ):
            if candidate.lower() in names:
                columns["foreign_key"] = names[candidate.lower()]
                break
        return columns

    def _missing_columns(self, entity: str, columns: dict[str, str]) -> set[str]:
        required = {"id", "vendor", "number", "asset_number", "foreign_key"}
        return required - set(columns)

    def _add_required_columns(
        self, conn: Connection, table: SQLATable, columns: dict[str, str]
    ) -> None:
        missing = self._missing_columns("", columns)
        additions = {
            "vendor": (self.vendor_column, Unicode(255)),
            "number": (self.number_column, Unicode(255)),
            "asset_number": (self.asset_number_column, Unicode(255)),
        }
        for logical, (name, type_) in additions.items():
            if logical not in missing:
                continue
            self._alter_add_column(conn, table, name, type_)
            columns[logical] = name

    def _add_link_column(
        self,
        conn: Connection,
        entity: str,
        table: SQLATable,
        columns: dict[str, str],
    ) -> None:
        """Add the deferred foreign-key column to a pre-existing table."""
        if "foreign_key" in columns or table.name not in self.db:
            return
        fk_column = _FK_COLUMNS[entity]
        other_name = self._table_name(_TARGET_ENTITY[entity])
        if self.db.engine is None:
            return
        dialect = self.db.engine.dialect
        qualified = _quote(table.name, dialect)
        if self.db.schema:
            qualified = _quote(self.db.schema, dialect) + "." + qualified
        target_table = _quote(other_name, dialect)
        target_column = _quote(self.id_column, dialect)
        integer_sql = Integer().compile(dialect=dialect)
        conn.execute(
            text(
                f"ALTER TABLE {qualified} ADD COLUMN {_quote(fk_column, dialect)} "
                f"{integer_sql} REFERENCES {target_table}({target_column}) "
                "DEFERRABLE INITIALLY DEFERRED"
            )
        )
        columns["foreign_key"] = fk_column

    def _add_payload_columns(
        self,
        conn: Connection,
        table: SQLATable,
        samples: Mapping[str, Any],
    ) -> None:
        existing = {column.name.lower() for column in table.columns}
        for name, sample in samples.items():
            if name.lower() in existing:
                continue
            column_type = self.db.types.guess(sample)
            table.append_column(Column(name, column_type))
            if table.name in self.db:
                self._alter_add_column(conn, table, name, column_type)

    def _alter_add_column(
        self, conn: Connection, table: SQLATable, name: str, type_: Any
    ) -> None:
        if table.name not in self.db:
            return
        if self.db.engine is None:
            raise HandoffError("Cannot alter a table without a database engine")
        compile_type = type_() if isinstance(type_, type) else type_
        type_sql = compile_type.compile(dialect=self.db.engine.dialect)
        qualified = _quote(table.name, self.db.engine.dialect)
        if self.db.schema:
            qualified = _quote(self.db.schema, self.db.engine.dialect) + "." + qualified
        column_sql = _quote(name, self.db.engine.dialect)
        conn.execute(
            text(f"ALTER TABLE {qualified} ADD COLUMN {column_sql} {type_sql}")
        )

    # ------------------------------------------------------------------
    # Existing rows and handles
    # ------------------------------------------------------------------
    def _read_existing(self, schema: _Schema) -> _ExistingRows:
        existing = _ExistingRows()
        for entity in _ENTITIES:
            entity_table = schema.entities[entity]
            rows = (
                self.db.executable.execute(select(entity_table.table)).mappings().all()
            )
            for row in rows:
                handle = _Handle(entity=entity, existing=True)
                handle.id = row[entity_table.columns["id"]]
                handle.data = dict(row)
                if "asset_number" in entity_table.columns:
                    handle.asset_number = row[entity_table.columns["asset_number"]]
                key = AssetKey(
                    vendor=str(row[entity_table.columns["vendor"]]),
                    type=entity,
                    number=_normalize_number(row[entity_table.columns["number"]]),
                )
                handle.add_key(key)
                existing.handles.append(handle)
                existing.by_key.setdefault(key, handle)
                if handle.asset_number is not None:
                    existing.by_asset.setdefault(
                        (key.vendor, _normalize_number(handle.asset_number)), handle
                    )
        return existing

    def _build_handles(
        self,
        staged: Sequence[HandoffRecord],
        existing: _ExistingRows,
    ) -> tuple[list[_Handle], dict[AssetKey, _Handle]]:
        handles: list[_Handle] = list(existing.handles)
        key_map: dict[AssetKey, _Handle] = dict(existing.by_key)

        def register(key: AssetKey, handle: _Handle) -> None:
            previous = key_map.get(key)
            if previous is not None and previous is not handle:
                raise HandoffConflictError(
                    f"Legacy key {key} maps to multiple target rows "
                    f"({previous.id!r} and {handle.id!r})"
                )
            key_map[key] = handle
            handle.add_key(key)

        for record in staged:
            key = AssetKey(
                vendor=record.vendor,
                type=record.type,
                number=_normalize_number(record.number),
            )
            handle: _Handle | None = None
            asset_match: _Handle | None = None
            if record.asset_number is not None:
                asset_match = existing.by_asset.get(
                    (record.vendor, _normalize_number(record.asset_number))
                )
            if asset_match is not None and (
                not record.number_explicit or key not in key_map
            ):
                # An asset-only record is identified by its permanent asset
                # number, not by a legacy key synthesized from that number.
                # When both a legacy and an asset identity are supplied, the
                # asset wins only if the legacy key is not already known.
                handle = asset_match
                handle.matched_by_asset = True
            else:
                handle = key_map.get(key)
            if handle is None:
                handle = _Handle(entity=record.type)
                handles.append(handle)
            handle.merge(record, key)
            # A batch record carrying a legacy number and an asset number can
            # also be the target of an asset-based reference.  Register the
            # asset BEFORE the legacy key so the batch's identity for that
            # asset wins over an unrelated pre-existing legacy row.
            if handle.asset_number is not None:
                asset_key = (
                    record.vendor,
                    _normalize_number(handle.asset_number),
                )
                previous = existing.by_asset.get(asset_key)
                if previous is not None and previous is not handle:
                    raise HandoffConflictError(
                        f"Permanent asset number {record.asset_number!r} maps "
                        f"to multiple target rows"
                    )
                existing.by_asset[asset_key] = handle
            register(key, handle)
        return handles, key_map

    # ------------------------------------------------------------------
    # Reference resolution
    # ------------------------------------------------------------------
    def _resolve(
        self,
        schema: _Schema,
        handles: Sequence[_Handle],
        key_map: dict[AssetKey, _Handle],
        existing: _ExistingRows,
    ) -> None:
        # Resolve explicit references first.  An explicit reference that does
        # not resolve is an error; it must not silently fall back to a
        # same-number or reciprocal guess.
        explicit: set[tuple[int, str]] = set()
        for handle in handles:
            if not handle.records:
                continue
            target_entity = _TARGET_ENTITY[handle.entity]
            column = _FK_COLUMNS[handle.entity]
            reference = handle.references.get(column)
            if reference is None:
                continue
            explicit.add((id(handle), column))
            target = self._find(reference, target_entity, key_map, existing)
            if target is not None:
                handle.resolved[column] = target

        # An existing asset may be submitted as data without its legacy
        # number while a site in the same batch refers to it by that number.
        # If exactly one asset-only record of the target type is available,
        # bind the dangling reference to it and register the alias key.
        for handle in handles:
            if not handle.records:
                continue
            column = _FK_COLUMNS[handle.entity]
            if column in handle.resolved:
                continue
            reference = handle.references.get(column)
            if (
                reference is None
                or reference.number is None
                or reference.asset_number is not None
            ):
                continue
            target_entity = reference.type
            if target_entity != _TARGET_ENTITY[handle.entity]:
                continue
            key = AssetKey(
                vendor=reference.vendor,
                type=target_entity,
                number=_normalize_number(reference.number),
            )
            if key in key_map:
                continue
            candidates = [
                candidate
                for candidate in handles
                if candidate.entity == target_entity
                and candidate.vendor == reference.vendor
                and candidate.asset_number is not None
                and not any(rec.number_explicit for rec in candidate.records)
                and candidate is not handle
            ]
            if len(candidates) != 1:
                continue
            target = candidates[0]
            handle.resolved[column] = target
            target.add_key(key)
            key_map[key] = target

        # Fill the reciprocal link: if the other side already points here, use
        # it without requiring a second explicit reference in the input.
        for handle in handles:
            if not handle.records:
                continue
            target_entity = _TARGET_ENTITY[handle.entity]
            column = _FK_COLUMNS[handle.entity]
            if column in handle.resolved or (id(handle), column) in explicit:
                continue
            reciprocal = self._reciprocal(handle, handles)
            if reciprocal is not None:
                handle.resolved[column] = reciprocal
                handle.references.setdefault(
                    column,
                    Reference(
                        vendor=reciprocal.vendor or handle.vendor or "",
                        type=reciprocal.entity,
                        number=next(iter(reciprocal.keys)).number
                        if reciprocal.keys
                        else None,
                    ),
                )

        # Fall back to the common convention that a site and its collector
        # share the same vendor and legacy number.  An asset-only record is
        # deliberately excluded: it is identified by its permanent asset
        # number, not by a legacy number synthesized from it.
        for handle in handles:
            if not handle.records:
                continue
            record = handle.records[0]
            if not record.number_explicit:
                continue
            target_entity = _TARGET_ENTITY[handle.entity]
            column = _FK_COLUMNS[handle.entity]
            if column in handle.resolved or (id(handle), column) in explicit:
                continue
            reference = Reference(
                vendor=record.vendor, type=target_entity, number=record.number
            )
            target = self._find(reference, target_entity, key_map, existing)
            if target is not None:
                handle.resolved[column] = target
                handle.references.setdefault(column, reference)

        unresolved: list[UnresolvedReference] = []
        for handle in handles:
            if not handle.records:
                continue
            target_entity = _TARGET_ENTITY[handle.entity]
            column = _FK_COLUMNS[handle.entity]
            if column in handle.resolved:
                continue
            record = handle.records[0]
            reference = handle.references.get(column)
            if reference is None:
                # An existing asset-only record may be submitted just to
                # refresh its payload; it does not imply a same-number link.
                if not record.number_explicit and handle.existing:
                    continue
                reference = Reference(
                    vendor=record.vendor,
                    type=target_entity,
                    number=record.number,
                )
            unresolved.append(
                UnresolvedReference(
                    record=(
                        record.raw
                        if record.raw is not None
                        else _record_to_mapping(record)
                    ),
                    table=_ENTITY_PLURALS[handle.entity],
                    column=column,
                    vendor=reference.vendor,
                    type=reference.type,
                    number=(
                        reference.number
                        if reference.number is not None
                        else reference.asset_number
                    ),
                )
            )

        if unresolved:
            raise UnresolvedReferenceError(unresolved)

    def _find(
        self,
        reference: Reference,
        target_entity: str,
        key_map: dict[AssetKey, _Handle],
        existing: _ExistingRows,
    ) -> _Handle | None:
        if reference.type != target_entity:
            # The reference names the wrong record type for this column.
            return None
        by_key: _Handle | None = None
        if reference.number is not None:
            key = AssetKey(
                vendor=reference.vendor,
                type=target_entity,
                number=_normalize_number(reference.number),
            )
            by_key = key_map.get(key) or existing.by_key.get(key)
        by_asset: _Handle | None = None
        if reference.asset_number is not None:
            asset_key = (
                reference.vendor,
                _normalize_number(reference.asset_number),
            )
            by_asset = existing.by_asset.get(asset_key)
            if by_asset is None:
                # An asset reference may target a row introduced later in
                # this same batch; consult the freshly registered handles.
                by_asset = next(
                    (
                        handle
                        for handle in key_map.values()
                        if handle.entity == target_entity
                        and handle.asset_number is not None
                        and (
                            handle.vendor,
                            _normalize_number(handle.asset_number),
                        )
                        == asset_key
                    ),
                    None,
                )
        if by_key is not None and by_asset is not None and by_key is not by_asset:
            # Both identifiers are present but disagree; do not guess.
            return None
        return by_key or by_asset

    def _reciprocal(
        self, handle: _Handle, handles: Sequence[_Handle]
    ) -> _Handle | None:
        for candidate in handles:
            if candidate is handle or candidate.entity == handle.entity:
                continue
            for target in candidate.resolved.values():
                if target is handle:
                    return candidate
        return None

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------
    def _insert(self, entity_table: _EntityTable, handle: _Handle) -> None:
        record = handle.records[0]
        row: dict[str, Any] = {}
        for name, value in handle.data.items():
            logical = name.lower()
            if logical in _RESERVED_KEYS:
                continue
            actual = entity_table.columns.get(logical, name)
            if actual in entity_table.table.c:
                row[actual] = value
        row[entity_table.columns["vendor"]] = record.vendor
        row[entity_table.columns["number"]] = str(record.number)
        if "type" in entity_table.columns:
            row[entity_table.columns["type"]] = handle.entity
        if "asset_number" in entity_table.columns:
            row[entity_table.columns["asset_number"]] = handle.asset_number
        # Placeholder id, replaced by ``_write_links`` once every row in the
        # cycle exists.  The deferred constraint allows it until commit.
        row[entity_table.columns["foreign_key"]] = 0

        result = self.db.executable.execute(entity_table.table.insert().values(row))
        if result.inserted_primary_key is None or not len(result.inserted_primary_key):
            raise HandoffError(
                f"Could not determine the new primary key for {handle.entity}"
            )
        handle.id = result.inserted_primary_key[0]

    def _write_links(self, entity_table: _EntityTable, handle: _Handle) -> None:
        if handle.id is None:
            raise HandoffError("Cannot write a row without a primary key")
        target = handle.resolved.get(_FK_COLUMNS[handle.entity])
        if target is None or target.id is None:
            if handle.existing:
                # An existing row may be submitted only to refresh payload.
                # Keep its current link unless this batch resolved a new one.
                self._update_payload(entity_table, handle)
                return
            raise HandoffError(f"No resolved target for {handle.entity} {handle.keys}")
        values: dict[str, Any] = {entity_table.columns["foreign_key"]: target.id}
        if handle.existing:
            for name, value in self._record_payload(handle).items():
                logical = name.lower()
                if logical in _RESERVED_KEYS or logical in _FK_COLUMNS.values():
                    continue
                actual = entity_table.columns.get(logical, name)
                if actual in entity_table.table.c:
                    values[actual] = value
            if (
                handle.asset_number is not None
                and "asset_number" in entity_table.columns
            ):
                values[entity_table.columns["asset_number"]] = handle.asset_number
        self.db.executable.execute(
            entity_table.table.update()
            .where(entity_table.table.c[entity_table.columns["id"]] == handle.id)
            .values(values)
        )

    def _update_payload(self, entity_table: _EntityTable, handle: _Handle) -> None:
        values: dict[str, Any] = {}
        for name, value in self._record_payload(handle).items():
            logical = name.lower()
            if logical in _RESERVED_KEYS or logical in _FK_COLUMNS.values():
                continue
            actual = entity_table.columns.get(logical, name)
            if actual in entity_table.table.c:
                values[actual] = value
        if handle.asset_number is not None and "asset_number" in entity_table.columns:
            values[entity_table.columns["asset_number"]] = handle.asset_number
        if not values:
            return
        self.db.executable.execute(
            entity_table.table.update()
            .where(entity_table.table.c[entity_table.columns["id"]] == handle.id)
            .values(values)
        )

    @staticmethod
    def _record_payload(handle: _Handle) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for record in handle.records:
            payload.update(record.data)
        return payload

    def _check_foreign_keys(self, schema: _Schema, written: Sequence[_Handle]) -> None:
        """Verify that every row this batch wrote points at a real target.

        Only the touched rows are inspected, so unrelated pre-existing data
        cannot fail an otherwise valid batch.
        """
        for handle in written:
            if handle.id is None:
                continue
            entity_table = schema.entities[handle.entity]
            target = handle.resolved.get(_FK_COLUMNS[handle.entity])
            if target is None or target.id is None:
                if handle.existing:
                    continue
                raise HandoffError(
                    f"{handle.entity} {handle.id} has no resolved target"
                )
            target_table = schema.entities[target.entity]
            fk_value = self.db.executable.execute(
                select(entity_table.table.c[entity_table.columns["foreign_key"]]).where(
                    entity_table.table.c[entity_table.columns["id"]] == handle.id
                )
            ).scalar()
            exists = self.db.executable.execute(
                select(target_table.table.c[target_table.columns["id"]]).where(
                    target_table.table.c[target_table.columns["id"]] == fk_value
                )
            ).first()
            if fk_value is None or exists is None:
                raise HandoffError(
                    f"{entity_table.name}.{entity_table.columns['foreign_key']} "
                    f"for row {handle.id} does not point at an existing target"
                )


class _BatchTransaction:
    """A transaction or savepoint that covers only this batch's writes.

    SQLite's ``PRAGMA foreign_keys`` switch cannot be toggled inside a
    transaction, and leaving it on globally would make unrelated operations
    (such as dropping both tables) order-dependent.  The loader therefore
    relies on the declared ``DEFERRABLE INITIALLY DEFERRED`` constraints,
    turns on SQLite's deferred mode for the batch, and runs a scoped target
    check in :meth:`Handoff._check_foreign_keys` before commit.  On other
    databases the deferred constraints are evaluated by the database itself
    at commit time.
    """

    def __init__(self, conn: Connection, in_outer_transaction: bool) -> None:
        self.conn = conn
        self.in_outer_transaction = in_outer_transaction
        self.transaction: Any = None
        self.is_sqlite = conn.dialect.name == "sqlite"

    def __enter__(self) -> _BatchTransaction:
        if self.in_outer_transaction:
            self.transaction = self.conn.begin_nested()
        else:
            if self.conn.in_transaction():
                # Only work from the read phase happened so far; start the
                # batch from a clean boundary so a failure rolls back exactly
                # the rows written by this batch.
                self.conn.commit()
            self.transaction = self.conn.begin()
        if self.is_sqlite:
            self.conn.execute(text("PRAGMA defer_foreign_keys=ON"))
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            if exc_type is None:
                self.transaction.commit()
            else:
                self.transaction.rollback()
        except SQLAlchemyError as error:
            self.transaction.rollback()
            if isinstance(exc_value, HandoffError):
                raise
            raise HandoffError(f"Hand-off batch failed: {error}") from error
        # ``PRAGMA defer_foreign_keys`` resets automatically at the end of the
        # surrounding transaction, so a caller-owned transaction is untouched.


def import_handoff(
    db: Database,
    records: Iterable[HandoffRecord | Mapping[str, Any]],
    *,
    raise_on_error: bool = True,
    **kwargs: Any,
) -> HandoffResult:
    """Import a hand-off stream in one atomic batch."""
    return Handoff(db, **kwargs).load(records, raise_on_error=raise_on_error)


def load_handoff(
    db: Database,
    records: Iterable[HandoffRecord | Mapping[str, Any]],
    **kwargs: Any,
) -> HandoffResult:
    """Alias for :func:`import_handoff`."""
    return import_handoff(db, records, **kwargs)


def try_import_handoff(
    db: Database,
    records: Iterable[HandoffRecord | Mapping[str, Any]],
    **kwargs: Any,
) -> HandoffResult:
    """Import a batch, returning failures in ``result.errors``."""
    loader_kwargs = {
        key: value
        for (key, value) in kwargs.items()
        if key
        in {
            "site_table",
            "collector_table",
            "id_column",
            "vendor_column",
            "type_column",
            "number_column",
            "asset_number_column",
        }
    }
    return Handoff(db, **loader_kwargs).load(records, raise_on_error=False)


def create_handoff_tables(db: Database, **kwargs: Any) -> Handoff:
    """Create the site/collector tables and return the loader."""
    loader = Handoff(db, **kwargs)
    loader.create_tables()
    return loader


def _error_as_reference(error: HandoffError) -> UnresolvedReference:
    return UnresolvedReference(
        record={},
        table="",
        column="",
        vendor="",
        type="",
        number=str(error),
    )


def _quote(identifier: str, dialect: Any) -> str:
    """Quote an identifier with the dialect's own rules."""
    return str(dialect.identifier_preparer.quote(identifier))


def _public_record(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return a plain, caller-visible copy of an original input record."""
    return dict(raw)


def _first(mapping: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    lowered = {str(key).lower(): value for (key, value) in mapping.items()}
    for name in names:
        value = lowered.get(name.lower())
        if value is not None:
            return value
    return default


def _normalize_entity(value: Any) -> str:
    entity = _ENTITY_ALIASES.get(str(value).strip().lower())
    if entity is None:
        raise HandoffError(f"Unknown record type: {value!r}")
    return entity


def _normalize_number(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    return str(value).strip()


def _record_to_mapping(record: HandoffRecord) -> dict[str, Any]:
    output: dict[str, Any] = {
        "type": record.type,
        "vendor": record.vendor,
        "number": record.number,
    }
    if record.asset_number is not None:
        output["asset_number"] = record.asset_number
    if record.references:
        output["references"] = {
            column: {
                "vendor": reference.vendor,
                "type": reference.type,
                "number": reference.number,
                **(
                    {"asset_number": reference.asset_number}
                    if reference.asset_number is not None
                    else {}
                ),
            }
            for (column, reference) in record.references.items()
        }
    output.update(record.data)
    return output


# Explicit loader aliases.
HandoffLoader = Handoff
HandoffImporter = Handoff
HandoffImportError = UnresolvedReferenceError

__all__ = [
    "AssetKey",
    "Handoff",
    "HandoffConflictError",
    "HandoffError",
    "HandoffImportError",
    "HandoffImporter",
    "HandoffLoader",
    "HandoffRecord",
    "HandoffResult",
    "Reference",
    "UnresolvedReference",
    "UnresolvedReferenceError",
    "create_handoff_tables",
    "import_handoff",
    "load_handoff",
    "try_import_handoff",
]
