"""Read-only table metadata used by static analysis rules.

SQLFluff deliberately has no database connection, so rules which need to reason
about uniqueness (for example whether a ``ROW_NUMBER()`` row pick has a
tie-breaker) rely on a *catalog* supplied by the user.  The catalog is a plain
JSON/YAML document.  It only describes what is known about the tables; the
analysis never changes it.

The canonical shape is::

    tables:
      site_catalog:
        primary_key: site_id
      event:
        primary_key: [site_id, event_time, record_no]
        nullable: [event_time]
      receipt:
        primary_key: receipt_no
        columns:
          record_no:
            nullable: false
    relationships:
      - name: receipt_per_record
        left: receipt
        right: event
        left_columns: [record_no]
        right_columns: [record_no]
        cardinality: many_to_one

A number of common spellings are accepted for the individual keys (for
example ``natural_key``/``keys``/``unique_keys``, ``relations``/``edges`` and
``from``/``to`` or ``parent``/``child`` for relationships).  The parser is
intentionally permissive: an unknown field is ignored rather than treated as
proof, so a misspelled constraint can only make the analysis more
conservative.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Iterable, Iterator, Optional

import yaml

from sqlfluff.core.errors import SQLFluffUserError

# Relationship cardinalities are expressed as "at most one row on this side
# for a key on the other side".
_ONE = "one"
_MANY = "many"
_BOTH = "both"


def _clean_name(value: Any) -> str:
    """Return a normalised identifier/table name."""
    if value is None:
        return ""
    text = str(value).strip()
    # Strip common quoting, then take the last part of a qualified name.
    text = text.strip("\"'`[]")
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.strip("\"'`[]").upper()


def _as_list(value: Any) -> list[Any]:
    """Coerce a scalar or iterable to a list, without splitting strings."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _as_column_list(value: Any) -> tuple[str, ...]:
    """Coerce a key/column declaration to a tuple of normalised names."""
    if value is None:
        return ()
    if isinstance(value, str):
        # A comma separated string is convenient in config files.
        if "," in value:
            return tuple(_clean_name(part) for part in value.split(",") if part.strip())
        return (_clean_name(value),)
    if isinstance(value, dict):
        # A mapping such as ``{site_id: site_id}`` names both sides; the keys
        # are the locally declared columns.
        return tuple(_clean_name(key) for key in value)
    if isinstance(value, (list, tuple)):
        return tuple(_clean_name(item) for item in value if str(item).strip())
    return (_clean_name(value),)


def _is_column_list(value: Any) -> bool:
    """Whether ``value`` looks like a single composite key."""
    if isinstance(value, str):
        # ``"a,b"`` or ``"a"`` is a single composite key.
        return True
    if isinstance(value, dict):
        return False
    if isinstance(value, (list, tuple)):
        return all(not isinstance(item, (list, tuple, dict)) for item in value)
    return False


@dataclass(frozen=True)
class UniqueKey:
    """A declared unique/natural key of a table.

    Attributes:
        columns: Normalised column names in the key.
        nullable_columns: Columns in the key which the catalog says may be
            null.  SQL uniqueness commonly permits multiple nulls, so such a
            key alone cannot adjudicate ties unless the nulls are filtered.
        name: Optional key name, used only in diagnostics.
        primary: Whether this key was declared as the primary key.
    """

    columns: tuple[str, ...]
    nullable_columns: frozenset[str] = frozenset()
    name: Optional[str] = None
    primary: bool = False

    def __hash__(self) -> int:  # pragma: no cover - dataclass default is fine
        return hash((self.columns, self.nullable_columns, self.name, self.primary))


@dataclass(frozen=True)
class ColumnSpec:
    """What the catalog says about a single column."""

    name: str
    nullable: Optional[bool] = None
    unique: bool = False


@dataclass(frozen=True)
class Relationship:
    """A declared relationship between two tables.

    ``left_max_one`` means that for a key on the right side there is at most
    one row on the left side.  ``right_max_one`` has the symmetric meaning.
    ``left_columns`` and ``right_columns`` hold the equated columns.
    """

    left_table: str
    right_table: str
    left_columns: tuple[str, ...]
    right_columns: tuple[str, ...]
    left_max_one: Optional[bool] = None
    right_max_one: Optional[bool] = None
    name: Optional[str] = None
    # Optional cardinality hints (for diagnostics; None means "unknown").
    left_min_rows: Optional[int] = None
    right_min_rows: Optional[int] = None

    def matches(
        self,
        left_table: str,
        left_columns: Iterable[str],
        right_table: str,
        right_columns: Iterable[str],
    ) -> bool:
        """Whether this relationship describes the given join, either way."""
        left_key = frozenset(_clean_name(c) for c in left_columns)
        right_key = frozenset(_clean_name(c) for c in right_columns)
        if self.left_table == _clean_name(left_table) and self.right_table == (
            _clean_name(right_table)
        ):
            return (
                frozenset(self.left_columns) <= left_key
                and frozenset(self.right_columns) <= right_key
            )
        if self.left_table == _clean_name(right_table) and self.right_table == (
            _clean_name(left_table)
        ):
            return (
                frozenset(self.left_columns) <= right_key
                and frozenset(self.right_columns) <= left_key
            )
        return False


@dataclass
class TableSpec:
    """Catalog metadata for one table."""

    name: str
    columns: dict[str, ColumnSpec] = field(default_factory=dict)
    keys: list[UniqueKey] = field(default_factory=list)

    def column(self, name: str) -> Optional[ColumnSpec]:
        """Return metadata for a column, if declared."""
        return self.columns.get(_clean_name(name))

    def nullable(self, name: str) -> Optional[bool]:
        """Return declared nullability, if known."""
        spec = self.column(name)
        return spec.nullable if spec else None

    def add_key(self, key: UniqueKey) -> None:
        """Add a key if an equivalent one is not already present."""
        for existing in self.keys:
            if existing.columns == key.columns:
                # Merge nullability information; a key is nullable if any
                # declaration says so.
                if key.nullable_columns - existing.nullable_columns:
                    existing = UniqueKey(
                        existing.columns,
                        existing.nullable_columns | key.nullable_columns,
                        existing.name or key.name,
                        existing.primary or key.primary,
                    )
                    self.keys.remove(
                        next(k for k in self.keys if k.columns == key.columns)
                    )
                    self.keys.append(existing)
                return
        self.keys.append(key)


@dataclass
class Catalog:
    """A read-only catalog of tables and relationships."""

    tables: dict[str, TableSpec] = field(default_factory=dict)
    relationships: list[Relationship] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------
    @classmethod
    def from_dict(cls, raw: Any) -> "Catalog":
        """Build a catalog from an already-parsed JSON/YAML mapping."""
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise SQLFluffUserError(
                "Row-pick catalog must be a mapping with a 'tables' section."
            )

        # Allow the catalog to be nested under a top level "catalog" key.
        if "tables" not in raw and isinstance(raw.get("catalog"), dict):
            raw = raw["catalog"]

        raw_tables = raw.get("tables", raw.get("entities", raw.get("table", {})))
        catalog = cls()

        # A top-level mapping of table -> nullable key columns is a convenient
        # shorthand used by some generated catalogs.
        top_level_nullable: dict[str, list[Any]] = {}
        for key in ("nullable_keys", "nullable_columns", "null_columns"):
            value = raw.get(key)
            if isinstance(value, dict):
                for table_name, columns in value.items():
                    top_level_nullable.setdefault(_clean_name(table_name), []).extend(
                        _as_list(columns)
                    )

        if isinstance(raw_tables, dict):
            for name, spec in raw_tables.items():
                clean_name = _clean_name(name)
                if clean_name in top_level_nullable and isinstance(spec, dict):
                    merged = dict(spec)
                    existing = merged.get("nullable_keys", merged.get("nullable"))
                    combined = _as_list(existing) + top_level_nullable[clean_name]
                    merged["nullable_keys"] = combined
                    spec = merged
                catalog.tables[clean_name] = cls._parse_table(name, spec)
        elif isinstance(raw_tables, list):
            for spec in raw_tables:
                if not isinstance(spec, dict):
                    continue
                name = spec.get("name") or spec.get("table")
                if not name:
                    continue
                catalog.tables[_clean_name(name)] = cls._parse_table(name, spec)

        raw_relationships = raw.get(
            "relationships",
            raw.get("relations", raw.get("edges", raw.get("foreign_keys", []))),
        )
        if isinstance(raw_relationships, dict):
            iterable: Iterable[Any] = [
                dict(value, name=value.get("name", key))
                if isinstance(value, dict)
                else value
                for key, value in raw_relationships.items()
            ]
        else:
            iterable = raw_relationships or []
        for relationship in iterable:
            parsed = cls._parse_relationship(relationship)
            if parsed:
                catalog.relationships.append(parsed)
        return catalog

    @classmethod
    def from_json(cls, text: str) -> "Catalog":
        """Build a catalog from JSON text."""
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as err:
            raise SQLFluffUserError(f"Invalid row-pick catalog JSON: {err}") from err
        return cls.from_dict(raw)

    @classmethod
    def from_yaml(cls, text: str) -> "Catalog":
        """Build a catalog from YAML text (JSON is valid YAML too)."""
        try:
            raw = yaml.safe_load(text)
        except yaml.YAMLError as err:
            raise SQLFluffUserError(f"Invalid row-pick catalog YAML: {err}") from err
        return cls.from_dict(raw)

    @classmethod
    def from_file(cls, path: str) -> "Catalog":
        """Read a catalog from a JSON or YAML file."""
        try:
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
        except OSError as err:
            raise SQLFluffUserError(
                f"Could not read row-pick catalog {path!r}: {err}"
            ) from err
        # JSON is a subset of YAML, so a single parser handles both.  The file
        # extension is deliberately not used to choose: users often store JSON
        # in a ``.yaml`` file and vice versa.
        return cls.from_yaml(text)

    @classmethod
    def from_value(cls, value: Any, *, base_path: Optional[str] = None) -> "Catalog":
        """Build a catalog from a config value.

        The value may be:

        * a :class:`Catalog` (returned as-is),
        * a mapping (inline catalog),
        * a string containing JSON/YAML, or
        * a path to a JSON/YAML file (relative paths resolve against
          ``base_path``, then the current working directory).
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls.from_dict(value)
        if value is None:
            return cls()
        text = str(value).strip()
        if not text:
            return cls()
        # Inline catalog: a mapping, or a document with a tables section.
        if text.startswith("{") or text.startswith("["):
            return cls.from_yaml(text)
        looks_like_yaml = "\n" in text or ": " in text or text.startswith("tables")
        if looks_like_yaml:
            return cls.from_yaml(text)
        path = text
        if not os.path.isabs(path) and base_path:
            candidate = os.path.join(base_path, path)
            if os.path.exists(candidate):
                path = candidate
        return cls.from_file(path)

    @classmethod
    def _parse_table(cls, name: Any, spec: Any) -> TableSpec:
        """Parse one table declaration."""
        table = TableSpec(_clean_name(name))
        if spec is None:
            return table
        if isinstance(spec, list):
            # A bare list of columns is treated as non-null column names.
            for column in spec:
                table.columns[_clean_name(column)] = ColumnSpec(
                    _clean_name(column), nullable=False
                )
            return table
        if not isinstance(spec, dict):
            return table

        nullable_columns: set[str] = set()
        non_null_columns: set[str] = set()

        # Table level nullability declarations.
        for key in (
            "nullable",
            "nullable_columns",
            "nullable_keys",
            "key_nullable",
            "key_may_be_null",
            "null_allowed",
            "optional",
            "optional_columns",
        ):
            for column in _as_list(spec.get(key)):
                if isinstance(column, dict):
                    for col_name, is_nullable in column.items():
                        if is_nullable:
                            nullable_columns.add(_clean_name(col_name))
                        else:
                            non_null_columns.add(_clean_name(col_name))
                elif _is_column_list(column):
                    for col_name in _as_column_list(column):
                        nullable_columns.add(col_name)
                else:
                    nullable_columns.add(_clean_name(column))
        for key in ("not_null", "non_nullable", "required", "mandatory"):
            for column in _as_column_list(spec.get(key)):
                non_null_columns.add(column)

        # Column declarations.  A scalar value is interpreted as nullability;
        # a mapping may set ``nullable``/``null``/``unique``/``primary_key``.
        raw_columns = spec.get("columns", spec.get("fields", spec.get("column", {})))
        if isinstance(raw_columns, dict):
            for col_name, col_spec in raw_columns.items():
                clean = _clean_name(col_name)
                nullable: Optional[bool] = None
                unique = False
                primary = False
                if isinstance(col_spec, bool):
                    nullable = col_spec
                elif isinstance(col_spec, dict):
                    if "nullable" in col_spec:
                        nullable = bool(col_spec["nullable"])
                    elif "null" in col_spec:
                        nullable = bool(col_spec["null"])
                    unique = bool(col_spec.get("unique", False))
                    primary = bool(col_spec.get("primary_key", False))
                elif col_spec is not None:
                    # A type or other descriptive value; don't infer
                    # nullability from it.
                    nullable = None
                table.columns[clean] = ColumnSpec(clean, nullable, unique)
                if nullable is True:
                    nullable_columns.add(clean)
                elif nullable is False:
                    non_null_columns.add(clean)
                if unique:
                    table.add_key(
                        UniqueKey(
                            (clean,),
                            frozenset({clean}) if nullable else frozenset(),
                            name=f"{table.name}.{clean}_unique",
                        )
                    )
                if primary:
                    table.add_key(
                        UniqueKey(
                            (clean,), frozenset(), name=f"{table.name}.pk", primary=True
                        )
                    )
                    non_null_columns.add(clean)
        elif isinstance(raw_columns, list):
            for col_name in raw_columns:
                clean = _clean_name(col_name)
                table.columns.setdefault(clean, ColumnSpec(clean))

        # Keys.
        key_fields = (
            "primary_key",
            "primary_keys",
            "pk",
            "natural_key",
            "natural_keys",
            "business_key",
            "business_keys",
            "unique_key",
            "unique_keys",
            "keys",
            "uniques",
            "unique",
            "identity",
            "identifiers",
        )
        for field_name in key_fields:
            if field_name not in spec:
                continue
            raw_key = spec[field_name]
            keys = cls._parse_keys(raw_key, field_name)
            for columns, declared_nullable, key_name in keys:
                primary = field_name in ("primary_key", "primary_keys", "pk")
                key_nulls = set(declared_nullable)
                key_nulls |= {
                    col
                    for col in columns
                    if col in nullable_columns
                    or (col in table.columns and table.columns[col].nullable is True)
                }
                if primary and not key_nulls:
                    non_null_columns.update(columns)
                table.add_key(
                    UniqueKey(
                        columns,
                        frozenset(key_nulls),
                        key_name or f"{table.name}.{field_name}",
                        primary,
                    )
                )

        # Any explicitly non-null column declaration upgrades metadata.
        for column in non_null_columns:
            existing = table.columns.get(column)
            if existing:
                table.columns[column] = ColumnSpec(
                    column, nullable=False, unique=existing.unique
                )
            else:
                table.columns[column] = ColumnSpec(column, nullable=False)
        for column in nullable_columns - non_null_columns:
            existing = table.columns.get(column)
            if existing:
                if existing.nullable is None:
                    table.columns[column] = ColumnSpec(
                        column, nullable=True, unique=existing.unique
                    )
            else:
                table.columns[column] = ColumnSpec(column, nullable=True)

        # Ensure every key column has at least metadata.
        for table_key in table.keys:
            for column in table_key.columns:
                table.columns.setdefault(column, ColumnSpec(column))
        return table

    @staticmethod
    def _parse_keys(
        raw_key: Any, field_name: str
    ) -> list[tuple[tuple[str, ...], set[str], Optional[str]]]:
        """Normalise the many ways a key can be written."""
        result: list[tuple[tuple[str, ...], set[str], Optional[str]]] = []
        if raw_key is None:
            return result
        if _is_column_list(raw_key):
            result.append((_as_column_list(raw_key), set(), None))
            return result
        if isinstance(raw_key, dict):
            # Either a single key mapping ``{columns: [...], nullable: [...]}``
            # or a mapping of names to key definitions.
            if any(k in raw_key for k in ("columns", "column", "key", "cols")):
                columns = _as_column_list(
                    raw_key.get(
                        "columns",
                        raw_key.get("column", raw_key.get("key", raw_key.get("cols"))),
                    )
                )
                nullable = set()
                for nullable_value in _as_list(
                    raw_key.get("nullable", raw_key.get("nullable_columns", []))
                ):
                    nullable |= set(_as_column_list(nullable_value))
                result.append(
                    (
                        columns,
                        nullable,
                        raw_key.get("name") or f"{field_name}",
                    )
                )
                return result
            for name, value in raw_key.items():
                if isinstance(value, dict):
                    columns = _as_column_list(
                        value.get(
                            "columns",
                            value.get("column", value.get("key", value.get("cols"))),
                        )
                    )
                    nullable = set()
                    for nullable_value in _as_list(
                        value.get("nullable", value.get("nullable_columns", []))
                    ):
                        nullable |= set(_as_column_list(nullable_value))
                    result.append((columns, nullable, str(name)))
                else:
                    result.append((_as_column_list(value), set(), str(name)))
            return result
        if isinstance(raw_key, (list, tuple)):
            for item in raw_key:
                if _is_column_list(item):
                    result.append((_as_column_list(item), set(), None))
                else:
                    result.extend(Catalog._parse_keys(item, field_name))
            return result
        result.append((_as_column_list(raw_key), set(), None))
        return result

    @classmethod
    def _parse_relationship(cls, raw: Any) -> Optional[Relationship]:
        """Parse one relationship declaration."""
        if not isinstance(raw, dict):
            return None

        def side_value(*names: str) -> Any:
            for name in names:
                if name in raw:
                    return raw[name]
            return None

        left_raw = side_value("left", "from", "parent", "source", "table_a", "fact")
        right_raw = side_value("right", "to", "child", "target", "table_b", "dim")
        if isinstance(left_raw, dict):
            left_table = left_raw.get("table") or left_raw.get("name")
            left_max_one = left_raw.get("max_one", left_raw.get("max_rows") == 1)
            left_min_rows = left_raw.get("min_rows")
            left_columns_raw = left_raw.get("columns")
        else:
            left_table = left_raw
            left_max_one = None
            left_min_rows = None
            left_columns_raw = None
        if isinstance(right_raw, dict):
            right_table = right_raw.get("table") or right_raw.get("name")
            right_max_one = right_raw.get("max_one", right_raw.get("max_rows") == 1)
            right_min_rows = right_raw.get("min_rows")
            right_columns_raw = right_raw.get("columns")
        else:
            right_table = right_raw
            right_max_one = None
            right_min_rows = None
            right_columns_raw = None

        if not left_table or not right_table:
            return None

        left_columns = _as_column_list(
            side_value(
                "left_columns",
                "from_columns",
                "parent_columns",
                "source_columns",
                "left_key",
                "left_keys",
                "from_key",
                "from_keys",
                "parent_key",
                "parent_keys",
            )
            or left_columns_raw
            or side_value("on", "columns", "keys")
        )
        right_columns = _as_column_list(
            side_value(
                "right_columns",
                "to_columns",
                "child_columns",
                "target_columns",
                "right_key",
                "right_keys",
                "to_key",
                "to_keys",
                "child_key",
                "child_keys",
            )
            or right_columns_raw
        )

        join_spec = raw.get("on", raw.get("columns", raw.get("keys")))
        if join_spec is not None:
            pairs = cls._mapping_items(join_spec)
            if pairs and not right_columns:
                # ``{left_column: right_column}``.
                left_columns = tuple(left for left, _ in pairs)
                right_columns = tuple(right for _, right in pairs)
            elif pairs and not left_columns:
                left_columns = tuple(left for left, _ in pairs)
                right_columns = tuple(right for _, right in pairs)
            if not pairs:
                left_columns = left_columns or _as_column_list(join_spec)
                right_columns = right_columns or left_columns

        if len(left_columns) != len(right_columns):
            # A same-name ``USING`` style relationship is common.
            if not right_columns:
                right_columns = left_columns
            elif not left_columns:
                left_columns = right_columns
            else:
                return None
        if not left_columns:
            return None

        # Cardinality spellings.
        cardinality_value = None
        for key in (
            "cardinality",
            "relation",
            "relationship",
            "type",
            "left_cardinality",
            "right_cardinality",
        ):
            if key in raw and raw[key] is not None:
                cardinality_value = raw[key]
                if key not in ("left_cardinality", "right_cardinality"):
                    break
        cardinality = str(cardinality_value or "").strip().lower()
        if cardinality:
            left_max_one_c, right_max_one_c = cls._parse_cardinality(cardinality)
            if left_max_one is None:
                left_max_one = left_max_one_c
            if right_max_one is None:
                right_max_one = right_max_one_c

        for key, side in (
            ("left_max_one", "left"),
            ("from_max_one", "left"),
            ("parent_max_one", "left"),
            ("source_max_one", "left"),
            ("left_unique", "left"),
            ("from_unique", "left"),
            ("right_max_one", "right"),
            ("to_max_one", "right"),
            ("child_max_one", "right"),
            ("target_max_one", "right"),
            ("right_unique", "right"),
            ("to_unique", "right"),
        ):
            if key in raw:
                if side == "left":
                    left_max_one = bool(raw[key])
                else:
                    right_max_one = bool(raw[key])

        at_most_one = raw.get("at_most_one", raw.get("max_one", None))
        if isinstance(at_most_one, str):
            unique_side_hint = at_most_one.strip().lower()
            if unique_side_hint in ("left", "from", "parent", "source"):
                left_max_one = True
            elif unique_side_hint in ("right", "to", "child", "target"):
                right_max_one = True
            elif unique_side_hint in ("both", "either", "all"):
                left_max_one = True
                right_max_one = True
        elif at_most_one is True:
            left_max_one = True
            right_max_one = True

        unique_side = (
            str(
                raw.get(
                    "unique_side",
                    raw.get("unique_on", raw.get("at_most_one_side", "")),
                )
            )
            .strip()
            .lower()
        )
        if unique_side in ("left", "from", "parent", "source"):
            left_max_one = True
        elif unique_side in ("right", "to", "child", "target"):
            right_max_one = True
        elif unique_side in ("both", "either", "all"):
            left_max_one = True
            right_max_one = True

        for key, side in (
            ("left_max_rows", "left"),
            ("from_max_rows", "left"),
            ("right_max_rows", "right"),
            ("to_max_rows", "right"),
        ):
            if key in raw and raw[key] == 1:
                if side == "left":
                    left_max_one = True
                else:
                    right_max_one = True

        return Relationship(
            left_table=_clean_name(left_table),
            right_table=_clean_name(right_table),
            left_columns=left_columns,
            right_columns=right_columns,
            left_max_one=left_max_one,
            right_max_one=right_max_one,
            name=raw.get("name"),
            left_min_rows=left_min_rows,
            right_min_rows=right_min_rows,
        )

    @staticmethod
    def _mapping_items(value: Any) -> list[tuple[str, str]]:
        """Return ``(left, right)`` pairs from a relationship column spec."""
        if isinstance(value, dict):
            return [(_clean_name(k), _clean_name(v)) for k, v in value.items()]
        if isinstance(value, (list, tuple)):
            pairs: list[tuple[str, str]] = []
            for item in value:
                if isinstance(item, dict) and len(item) == 1:
                    key, val = next(iter(item.items()))
                    pairs.append((_clean_name(key), _clean_name(val)))
                elif isinstance(item, (list, tuple)) and len(item) == 2:
                    pairs.append((_clean_name(item[0]), _clean_name(item[1])))
                elif isinstance(item, str) and "=" in item:
                    left, right = item.split("=", 1)
                    pairs.append((_clean_name(left), _clean_name(right)))
            return pairs
        if isinstance(value, str) and "=" in value:
            left, right = value.split("=", 1)
            return [(_clean_name(left), _clean_name(right))]
        return []

    @staticmethod
    def _parse_cardinality(cardinality: str) -> tuple[Optional[bool], Optional[bool]]:
        """Translate common cardinality spellings.

        Returns ``(left_max_one, right_max_one)``.  ``None`` means the text
        did not describe that side.
        """
        text = cardinality.replace(" ", "").replace("_", "").replace("-", "")
        one = {"1", "one", "single", "atmostone"}
        many = {"n", "*", "many", "0..*", "1..*", "atleastone", "multiple"}
        if text in ("1:1", "1..1", "onetoone", "oneone", "11"):
            return True, True
        if text in ("1:n", "1..n", "1..*", "onetomany", "one2many"):
            return True, False
        if text in ("n:1", "n..1", "n..1", "*..1", "manytoone", "many2one", "n1"):
            return False, True
        if text in ("n:n", "n..n", "*:*", "manytomany"):
            return False, False
        if text in ("oneto0..1", "onezeroone", "oneoptionalone", "0..1:1"):
            return True, True
        # Individual side names.
        if ":" in cardinality:
            left, right = cardinality.split(":", 1)
            left_value = (
                True if left.strip() in one else False if left.strip() in many else None
            )
            right_value = (
                True
                if right.strip() in one
                else False
                if right.strip() in many
                else None
            )
            return left_value, right_value
        if text in one:
            return True, None
        if text in many:
            return False, None
        return None, None

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------
    def table(self, name: str) -> Optional[TableSpec]:
        """Return a table by name (case and schema insensitive)."""
        return self.tables.get(_clean_name(name))

    def has_table(self, name: str) -> bool:
        """Whether the catalog declares a table."""
        return _clean_name(name) in self.tables

    def unique_keys(self, table: str) -> list[UniqueKey]:
        """Return declared unique keys for a table."""
        spec = self.table(table)
        return list(spec.keys) if spec else []

    def column_nullable(self, table: str, column: str) -> Optional[bool]:
        """Return declared nullability for a column, or ``None`` if unknown."""
        spec = self.table(table)
        if not spec:
            return None
        return spec.nullable(column)

    def relationship_for(
        self,
        left_table: str,
        left_columns: Iterable[str],
        right_table: str,
        right_columns: Iterable[str],
    ) -> Optional[Relationship]:
        """Return a relationship describing this join, if declared."""
        left_key = tuple(left_columns)
        right_key = tuple(right_columns)
        for relationship in self.relationships:
            if relationship.matches(left_table, left_key, right_table, right_key):
                return relationship
        return None

    def max_one_per_key(
        self,
        left_table: str,
        left_columns: Iterable[str],
        right_table: str,
        right_columns: Iterable[str],
    ) -> tuple[Optional[bool], Optional[bool]]:
        """Return ``(left_per_right, right_per_left)`` max-one evidence.

        ``left_per_right`` is True when each key in ``right_table`` matches at
        most one row of ``left_table``.  ``right_per_left`` is the symmetric
        statement.  A value of ``False`` is positive evidence of possible
        amplification; ``None`` means unknown.
        """
        relationship = self.relationship_for(
            left_table, left_columns, right_table, right_columns
        )
        left_max = relationship.left_max_one if relationship else None
        right_max = relationship.right_max_one if relationship else None
        if relationship and _clean_name(relationship.left_table) != _clean_name(
            left_table
        ):
            # The relationship was declared in the opposite direction.
            left_max, right_max = right_max, left_max

        # A declared unique key covered by the join columns also proves that
        # side matches at most once.  Nullable key columns only count when
        # they have been constrained; the caller handles that refinement, so
        # here a key with declared nullable columns is deliberately ignored.
        right_key = frozenset(_clean_name(c) for c in right_columns)
        left_key = frozenset(_clean_name(c) for c in left_columns)
        if right_max is None:
            if any(
                frozenset(key.columns) <= right_key and not key.nullable_columns
                for key in self.unique_keys(right_table)
            ):
                right_max = True
        if left_max is None:
            if any(
                frozenset(key.columns) <= left_key and not key.nullable_columns
                for key in self.unique_keys(left_table)
            ):
                left_max = True
        return left_max, right_max


@lru_cache(maxsize=32)
def _load_catalog_cached(path: str, mtime_ns: int) -> Catalog:
    """Cached catalog loader keyed by path and modification time."""
    return Catalog.from_file(path)


# Descriptive aliases used by callers which think in terms of a table catalog.
TableCatalog = Catalog
RowPickCatalog = Catalog
RelationshipSpec = Relationship


def load_catalog(value: Any, *, base_path: Optional[str] = None) -> Catalog:
    """Load a catalog from a config value.

    ``base_path`` is used to resolve relative file paths.  This function is
    intentionally side-effect free: the returned object is a plain snapshot
    and callers may not use it to modify table metadata.
    """
    if isinstance(value, Catalog):
        return value
    if isinstance(value, (dict, list)):
        return Catalog.from_dict(value)
    text = str(value).strip() if value is not None else ""
    if not text:
        return Catalog()
    if text.startswith("{") or text.startswith("[") or "\n" in text:
        return Catalog.from_yaml(text)
    # An empty path or an unset sentinel is not a catalog.
    if text in ("None", "none", "__unset__"):
        return Catalog()
    path = text
    if not os.path.isabs(path) and base_path:
        candidate = os.path.join(base_path, path)
        if os.path.exists(candidate):
            path = candidate
    if os.path.exists(path):
        try:
            mtime_ns = os.stat(path).st_mtime_ns
        except OSError:  # pragma: no cover - race with deletion
            mtime_ns = 0
        return _load_catalog_cached(path, mtime_ns)
    # A string containing ``tables:`` is an inline YAML document.
    if ": " in text:
        return Catalog.from_yaml(text)
    return Catalog.from_file(path)


def iter_tables(catalog: Catalog) -> Iterator[TableSpec]:
    """Iterate declared tables, mainly for diagnostics/tests."""
    return iter(catalog.tables.values())


__all__ = (
    "Catalog",
    "ColumnSpec",
    "Relationship",
    "RelationshipSpec",
    "RowPickCatalog",
    "TableCatalog",
    "TableSpec",
    "UniqueKey",
    "iter_tables",
    "load_catalog",
)
