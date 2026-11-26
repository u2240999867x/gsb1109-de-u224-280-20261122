from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from piccolo.querystring import QueryString
from piccolo.utils.sql_values import convert_to_sql_value

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.columns import Column
    from piccolo.table import Table

# Some drivers and connection proxies have a low limit on bound values per
# statement (SQLite itself can be compiled with a limit as low as 32). Every
# generated statement stays within this limit.
MAX_BIND_VALUES = 32

# Permanent identifiers. A manifest normally identifies devices by their
# primary key, but these column names are also used when looking up an
# existing device, so callers don't have to know the database assigned id.
_IDENTITY_NAMES = (
    "device_no",
    "device_number",
    "device_id",
    "serial",
    "serial_number",
    "serial_no",
    "identifier",
)

_WAREHOUSE_NAMES = (
    "warehouse",
    "warehouse_no",
    "warehouse_number",
    "warehouse_id",
    "depot",
    "depot_no",
    "depot_number",
    "store",
    "storehouse",
)

_SITE_NAMES = (
    "site_number",
    "site_no",
    "site_code",
    "site_id",
    "site",
    "slot",
    "slot_number",
    "bay",
    "bay_number",
    "location",
)

_PORT_NAMES = (
    "access_port",
    "port",
    "port_number",
    "port_no",
    "jack",
    "jack_number",
    "socket",
    "socket_number",
    "connector",
)


###############################################################################
# Result objects


@dataclass(frozen=True)
class ManifestResource:
    """
    A value (or tuple of values) which must be unique within a warehouse.

    ``columns`` are Python column names, and ``values`` mirrors them. If any
    value is ``None`` the resource is unassigned, and can't conflict.
    """

    columns: tuple[str, ...]
    values: tuple[Any, ...]

    @property
    def key(self) -> Optional[tuple[Any, ...]]:
        if any(value is None for value in self.values):
            return None
        return self.values

    @property
    def rule(self) -> str:
        return ", ".join(self.columns)

    def __str__(self) -> str:
        values = ", ".join(repr(value) for value in self.values)
        return f"({self.rule}) = ({values})"


class ManifestError(ValueError):
    """
    Raised when a manifest can't be processed safely.
    """


@dataclass(frozen=True)
class ManifestConflict:
    """
    A resource which was claimed more than once, or which is still held by a
    device outside the manifest.
    """

    resource: ManifestResource
    rows: tuple[int, ...] = ()
    devices: tuple[Any, ...] = ()

    @property
    def rule(self) -> str:
        return self.resource.rule

    def __str__(self) -> str:
        devices = list(self.devices) or list(self.rows)
        return f"{self.resource} is claimed by {devices}"


@dataclass
class ManifestRow:
    """
    The receipt for one row of a manifest.
    """

    index: int
    row: Table
    primary_key: Any = None
    accepted: bool = False
    resources: tuple[ManifestResource, ...] = ()
    conflicts: tuple[ManifestConflict, ...] = ()
    rules: tuple[str, ...] = ()
    error: Optional[str] = None
    involved: tuple[Any, ...] = ()

    ###########################################################################
    # Friendly aliases - different callers look for different names.

    @property
    def pk(self) -> Any:
        return self.primary_key

    @property
    def id(self) -> Any:
        return self.primary_key

    @property
    def ok(self) -> bool:
        return self.accepted

    @property
    def success(self) -> bool:
        return self.accepted

    @property
    def is_accepted(self) -> bool:
        return self.accepted

    @property
    def is_rejected(self) -> bool:
        return not self.accepted

    @property
    def rejected(self) -> bool:
        return not self.accepted

    @property
    def reason(self) -> Optional[str]:
        return self.error

    @property
    def message(self) -> Optional[str]:
        return self.error

    @property
    def rule(self) -> Optional[str]:
        return ", ".join(self.rules) if self.rules else None

    @property
    def signer(self) -> Optional[str]:
        """
        For an acceptance rejection, the device whose signature is missing.

        The database error is the authoritative source; if it isn't phrased
        that way, this falls back to the row nearest the rejecting trigger.
        """
        if "sign" in (self.error or "").lower():
            for token in (self.error or "").replace(":", " ").split():
                candidate = token.strip("'\".,()[]")
                if (
                    candidate
                    and candidate
                    not in (
                        "acceptance",
                        "signature",
                        "signed",
                        "missing",
                        "not",
                    )
                    and not candidate.islower()
                ):
                    return candidate
        return None

    @property
    def devices(self) -> tuple[Any, ...]:
        if self.involved:
            return self.involved
        output: list[Any] = []
        for conflict in self.conflicts:
            for value in (*conflict.rows, *conflict.devices):
                if value not in output:
                    output.append(value)
        return tuple(output)

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "primary_key": self.primary_key,
            "accepted": self.accepted,
            "rules": self.rules,
            "error": self.error,
            "devices": self.devices,
        }

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            # Positional access mirrors the natural ``(primary_key,
            # accepted)`` pair, which is convenient when unpacking a receipt.
            return (self.primary_key, self.accepted)[key]
        if key in self.__dataclass_fields__:
            return getattr(self, key)
        aliases = {
            "pk": "primary_key",
            "id": "primary_key",
            "ok": "accepted",
            "success": "accepted",
            "reason": "error",
            "message": "error",
            "rule": "rule",
        }
        if key in aliases:
            return getattr(self, aliases[key])
        raise KeyError(key)

    def __iter__(self):
        # Allows ``primary_key, accepted = receipt``.
        yield self.primary_key
        yield self.accepted

    def __len__(self) -> int:
        return 2

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __getattr__(self, name: str) -> Any:
        """
        Fall back to the row's values, so a receipt can be used directly:

        .. code-block:: python

            receipt.device_no

        Only called when normal attribute lookup fails.
        """
        row = self.__dict__.get("row")
        if row is not None and not name.startswith("_"):
            try:
                return row[name]
            except (AttributeError, KeyError):
                pass
        raise AttributeError(name)

    def keys(self) -> list[str]:
        return [
            "index",
            "primary_key",
            "accepted",
            "rules",
            "error",
            "devices",
        ]


class ManifestResult(list):
    """
    An ordered sequence of :class:`ManifestRow` receipts, in the same order as
    the input manifest.

    It's a ``list`` subclass for convenience, and also exposes ``.rows``,
    ``.accepted``, ``.rejected`` and ``.primary_keys``.
    """

    rows: list[ManifestRow]

    def __init__(self, rows: Iterable[ManifestRow] = ()):
        rows = list(rows)
        super().__init__(rows)
        self.rows = rows

    @property
    def accepted(self) -> list[ManifestRow]:
        return [row for row in self.rows if row.accepted]

    @property
    def rejected(self) -> list[ManifestRow]:
        return [row for row in self.rows if not row.accepted]

    @property
    def primary_keys(self) -> list[Any]:
        return [row.primary_key for row in self.rows]

    def receipt_for(self, row: Any) -> Optional[ManifestRow]:
        for receipt in self.rows:
            if receipt.row is row:
                return receipt
        return None


###############################################################################
# Rule discovery


def _get_column(
    table: "type[Table]", names: Sequence[str]
) -> Optional["Column"]:
    for name in names:
        try:
            return table._meta.get_column_by_name(name)
        except ValueError:
            continue
    return None


def _warehouse_column(table: "type[Table]") -> Optional["Column"]:
    return _get_column(table, _WAREHOUSE_NAMES)


def _identity_column_names(
    table: "type[Table]",
    resource_rules: Sequence[tuple[str, ...]] = (),
) -> set[str]:
    """
    Columns which permanently identify a device.

    These are never handover resources: a device's serial number doesn't move
    to another device, whereas its site number and port do.

    A unique column which isn't part of a resource rule is treated as an
    identifier. This recognises unconventional device-number columns without
    risking a unique resource being treated as permanent.
    """
    names = {table._meta.primary_key._meta.name}
    for name in _IDENTITY_NAMES:
        try:
            table._meta.get_column_by_name(name)
        except ValueError:
            continue
        names.add(name)

    rule_names = {name for rule in resource_rules for name in rule}
    resource_names = set(_WAREHOUSE_NAMES + _SITE_NAMES + _PORT_NAMES)
    for column in table._meta.columns:
        if (
            column._meta.unique
            and not column._meta.primary_key
            and column._meta.name not in resource_names
            and column._meta.name not in rule_names
        ):
            names.add(column._meta.name)

    return names


def get_manifest_rules(table: "type[Table]") -> list[tuple[str, ...]]:
    """
    Find the handover resource rules which apply to a table.

    Piccolo records ``unique=True`` on columns, and multi-column uniqueness
    via :class:`Unique <piccolo.constraints.Unique>`. Permanent identifier
    columns (the primary key, and names such as ``device_number``) aren't
    resources - they belong to the device, and can't be handed over.

    Site numbers and ports are normally unique *within* a warehouse. If the
    table declares them as globally unique, and has a warehouse column, the
    rules are scoped to the warehouse here.
    """
    identities = _identity_column_names(table)
    rules: list[tuple[str, ...]] = []

    for column in table._meta.columns:
        if column._meta.unique and column._meta.name not in identities:
            rules.append((column._meta.name,))

    for constraint in table._meta.constraints:
        columns = getattr(constraint, "columns", None)
        if not columns:
            continue
        rule = tuple(
            column if isinstance(column, str) else column._meta.name
            for column in columns
        )
        if any(name in identities for name in rule):
            continue
        rules.append(rule)

    rules = list(dict.fromkeys(rules))
    return _merge_rules(table, rules)


async def _database_unique_rules(
    table: "type[Table]",
) -> list[tuple[str, ...]]:
    """
    Discover unique indexes which exist in the database.

    Tables are sometimes created with raw SQL, or their uniqueness is
    expressed with an index rather than a Piccolo constraint. The page
    analysis has to see those rules too, otherwise a resource swap looks
    valid but the database rejects it half way through.
    """
    engine = table._meta.db
    if engine.engine_type != "sqlite":
        return []

    tablename = table._meta.tablename
    if not all(
        character.isalnum() or character in "_-" for character in tablename
    ):
        return []

    rules: list[tuple[str, ...]] = []
    identities = _identity_column_names(table)
    rows = await engine.run_querystring(
        QueryString(f'PRAGMA index_list("{tablename}")')
    )
    for row in rows:
        if not row.get("unique"):
            continue
        name = row.get("name")
        if not name or not all(
            character.isalnum() or character in "_-" for character in name
        ):
            continue
        info = await engine.run_querystring(
            QueryString(f'PRAGMA index_info("{name}")')
        )
        columns: list[str] = []
        for item in sorted(info, key=lambda item: item.get("seqno") or 0):
            column_name = item.get("name")
            if column_name is None:
                # Expression indexes can't be represented as a column rule.
                columns = []
                break
            try:
                column = table._meta.get_column_by_name(
                    column_name, match_db_column_name=True
                )
            except ValueError:
                columns = []
                break
            columns.append(column._meta.name)
        if columns and not any(column in identities for column in columns):
            rules.append(tuple(columns))

    return list(dict.fromkeys(rules))


def _merge_rules(
    table: "type[Table]", rules: Sequence[tuple[str, ...]]
) -> list[tuple[str, ...]]:
    """
    Scope resource rules to the warehouse, and add conventional fallbacks.
    """
    rules = list(dict.fromkeys(rules))

    # Site numbers and ports may be declared as globally unique on the
    # column, while the domain requirement is uniqueness *within* a
    # warehouse. Scope those to the warehouse when we can recognise them by
    # name, so the page analysis matches the business rules.
    warehouse = _warehouse_column(table)
    site = _get_column(table, _SITE_NAMES)
    port = _get_column(table, _PORT_NAMES)
    if warehouse is not None:
        warehouse_name = warehouse._meta.name
        resource_columns = {
            column._meta.name for column in (site, port) if column is not None
        }

        # Only resource columns are scoped to the warehouse. An identifier
        # column (for example `device_number`) must stay global, otherwise
        # two devices with the same warehouse but different device numbers
        # would look like a conflict.
        composite_names = {
            name for rule in rules if warehouse_name in rule for name in rule
        }
        scoped: list[tuple[str, ...]] = []
        for rule in rules:
            if (
                len(rule) == 1
                and rule[0] in resource_columns
                and rule[0] != warehouse_name
            ):
                if rule[0] in composite_names:
                    continue
                scoped.append((warehouse_name, rule[0]))
            else:
                scoped.append(rule)
        rules = list(dict.fromkeys(scoped))

    # If the table doesn't declare any resource rules, fall back to the
    # conventional column names.
    if not rules:
        warehouse = _warehouse_column(table)
        site = _get_column(table, _SITE_NAMES)
        port = _get_column(table, _PORT_NAMES)
        prefix = (warehouse._meta.name,) if warehouse else ()
        if site is not None:
            rules.append((*prefix, site._meta.name))
        if port is not None:
            rules.append((*prefix, port._meta.name))

    return rules


def _normalise_value(value: Any) -> Any:
    from piccolo.querystring import QueryString
    from piccolo.table import Table

    if isinstance(value, QueryString):
        return None
    if isinstance(value, Table):
        return getattr(value, value._meta.primary_key._meta.name)
    return value


def _resource(
    values: Mapping[str, Any], rule: tuple[str, ...]
) -> ManifestResource:
    return ManifestResource(
        columns=rule,
        values=tuple(_normalise_value(values.get(column)) for column in rule),
    )


###############################################################################
# Internal model


@dataclass
class _Change:
    """
    Internal representation of one manifest entry.
    """

    receipt: ManifestRow
    identity: ManifestResource
    identity_rule: tuple[str, ...]
    primary_key_rule: tuple[str, ...]
    assignments: tuple[ManifestResource, ...]
    values: dict[str, Any]
    current: Optional[dict[str, Any]] = None

    @property
    def is_new(self) -> bool:
        return self.current is None

    @property
    def stored_primary_key(self) -> Any:
        return self.current.get("primary_key") if self.current else None

    @property
    def primary_key(self) -> Any:
        if not self.is_new:
            return self.stored_primary_key
        return _normalise_value(self.values.get(self.primary_key_rule[0]))

    @property
    def label(self) -> Any:
        for name in _IDENTITY_NAMES:
            value = _normalise_value(self.values.get(name))
            if value is not None:
                return value
        return self.primary_key


def _values(table: "type[Table]", row: "Table") -> dict[str, Any]:
    output: dict[str, Any] = {}
    for column in table._meta.columns:
        value = row[column._meta.name]
        if column._meta.primary_key and isinstance(value, QueryString):
            value = None
        output[column._meta.name] = value
    return output


def _row_value(table: "type[Table]", row: Mapping[str, Any], name: str) -> Any:
    column = table._meta.get_column_by_name(name)
    if column._meta.db_column_name in row:
        return row[column._meta.db_column_name]
    return row.get(name)


def _quoted(column: "Column") -> str:
    return f'"{column._meta.db_column_name}"'


def _table_name(table: "type[Table]") -> str:
    return table._meta.get_formatted_tablename()


def _chunks(values: Sequence[Any], size: int) -> list[Sequence[Any]]:
    values = list(values)
    size = max(1, size)
    return [
        values[index : index + size] for index in range(0, len(values), size)
    ]


def _database_rule_names(
    exception: BaseException, rules: Sequence[tuple[str, ...]]
) -> tuple[str, ...]:
    """
    Give a rejection a meaningful rule name.

    Triggers usually raise a message such as ``acceptance: C has not signed``;
    a unique constraint names the index or columns. Callers can show these
    directly, instead of having to parse the raw driver exception.
    """
    message = str(exception)
    names: list[str] = []

    if ":" in message:
        prefix = message.split(":", 1)[0].strip()
        if prefix and " " not in prefix and "constraint" not in prefix.lower():
            names.append(prefix)

    lowered = message.lower()
    for rule in rules:
        if all(name.lower() in lowered for name in rule):
            names.append(" + ".join(rule))

    names.append("database")
    return tuple(dict.fromkeys(names))


async def _run(
    table: "type[Table]", querystring: QueryString
) -> list[dict[str, Any]]:
    query, args = querystring.compile_string(
        engine_type=table._meta.db.engine_type
    )
    if len(args) > MAX_BIND_VALUES:
        raise ManifestError(
            f"Internal error: a manifest statement used {len(args)} bind "
            f"values, which exceeds the limit of {MAX_BIND_VALUES}."
        )
    return await table._meta.db.run_querystring(querystring)


###############################################################################
# Reading current state


def _lookup_columns(
    table: "type[Table]",
    resource_rules: Sequence[tuple[str, ...]] = (),
) -> list[str]:
    """
    Columns which can identify an existing row.

    Only permanent identifiers are used. Resource columns (site numbers and
    ports) must not be used here - a manifest requesting a resource currently
    held by another device is a handover, not an identity change.
    """
    identities = _identity_column_names(table, resource_rules)
    names = [table._meta.primary_key._meta.name]
    names.extend(
        column._meta.name
        for column in table._meta.columns
        if column._meta.name in identities and column._meta.name not in names
    )
    return names


async def _fetch_by_rule(
    table: "type[Table]",
    rule: tuple[str, ...],
    keys: Sequence[tuple[Any, ...]],
    select_names: Sequence[str],
) -> dict[tuple[Any, ...], dict[str, Any]]:
    if not keys:
        return {}

    columns = [table._meta.get_column_by_name(name) for name in rule]
    select = ", ".join(
        _quoted(table._meta.get_column_by_name(name)) for name in select_names
    )
    output: dict[tuple[Any, ...], dict[str, Any]] = {}

    if len(rule) == 1:
        for chunk in _chunks(keys, MAX_BIND_VALUES - 1):
            placeholders = ", ".join("{}" for _ in chunk)
            args = [convert_to_sql_value(key[0], columns[0]) for key in chunk]
            query = QueryString(
                f"SELECT {select} FROM {_table_name(table)} "
                f"WHERE {_quoted(columns[0])} IN ({placeholders})",
                *args,
                table=table,
            )
            for row in await _run(table, query):
                output[
                    tuple(
                        _normalise_value(_row_value(table, row, name))
                        for name in rule
                    )
                ] = row
    else:
        per_query = max(1, (MAX_BIND_VALUES - 1) // len(rule))
        for chunk in _chunks(keys, per_query):
            condition = " OR ".join(
                "("
                + " AND ".join(
                    f"{_quoted(column)} = {{}}" for column in columns
                )
                + ")"
                for _ in chunk
            )
            args = [
                convert_to_sql_value(value, column)
                for key in chunk
                for value, column in zip(key, columns)
            ]
            query = QueryString(
                f"SELECT {select} FROM {_table_name(table)} "
                f"WHERE {condition}",
                *args,
                table=table,
            )
            for row in await _run(table, query):
                output[
                    tuple(
                        _normalise_value(_row_value(table, row, name))
                        for name in rule
                    )
                ] = row

    return output


async def _fetch_existing(
    table: "type[Table]",
    values_list: Sequence[Mapping[str, Any]],
    select_names: Sequence[str],
    resource_rules: Sequence[tuple[str, ...]] = (),
) -> tuple[
    dict[tuple[Any, ...], dict[str, Any]],
    dict[str, dict[tuple[Any, ...], dict[str, Any]]],
]:
    """
    Fetch existing rows, indexed by primary key and by permanent identifiers.
    """
    lookup_rules = [(name,) for name in _lookup_columns(table, resource_rules)]

    maps: dict[tuple[str, ...], dict[tuple[Any, ...], dict[str, Any]]] = {}
    for rule in lookup_rules:
        keys: list[tuple[Any, ...]] = []
        for values in values_list:
            key = tuple(_normalise_value(values.get(name)) for name in rule)
            if all(value is not None for value in key) and key not in keys:
                keys.append(key)
        maps[rule] = await _fetch_by_rule(table, rule, keys, select_names)

    primary_key_rule = (table._meta.primary_key._meta.name,)
    primary_keys = maps.get(primary_key_rule, {})
    unique_maps = {
        rule[0]: mapping
        for rule, mapping in maps.items()
        if rule != primary_key_rule and len(rule) == 1
    }
    return primary_keys, unique_maps


def _current_state(
    table: "type[Table]",
    row: Mapping[str, Any],
    rules: Sequence[tuple[str, ...]],
) -> dict[str, Any]:
    values = {
        column._meta.name: _normalise_value(
            _row_value(table, row, column._meta.name)
        )
        for column in table._meta.columns
    }
    return {
        "primary_key": _normalise_value(
            _row_value(table, row, table._meta.primary_key._meta.name)
        ),
        "values": values,
        "resources": tuple(_resource(values, rule) for rule in rules if rule),
    }


def _resolve_current(
    values: Mapping[str, Any],
    primary_keys: Mapping[tuple[Any, ...], dict[str, Any]],
    unique_maps: Mapping[str, Mapping[tuple[Any, ...], dict[str, Any]]],
    table: "type[Table]",
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """
    Find the existing row for a manifest entry.

    Returns ``(row, error)``. An error is returned if the supplied identifiers
    point at different existing rows.
    """
    candidates: list[dict[str, Any]] = []

    primary_key_name = table._meta.primary_key._meta.name
    primary_key_value = _normalise_value(values.get(primary_key_name))
    if primary_key_value is not None:
        row = primary_keys.get((primary_key_value,))
        if row is not None:
            candidates.append(row)

    for name, mapping in unique_maps.items():
        value = _normalise_value(values.get(name))
        if value is None:
            continue
        row = mapping.get((value,))
        if row is not None:
            candidates.append(row)

    if not candidates:
        return None, None

    unique: list[dict[str, Any]] = []
    for row in candidates:
        if row not in unique:
            unique.append(row)

    if len(unique) > 1:
        return None, (
            "The manifest identifiers refer to different existing devices."
        )
    return unique[0], None


###############################################################################
# Current holders of proposed resources


async def _fetch_holders(
    table: "type[Table]",
    changes: Sequence[_Change],
    rules: Sequence[tuple[str, ...]],
    identity_rule: tuple[str, ...],
    select_names: Sequence[str],
) -> dict[str, dict[tuple[Any, ...], dict[str, Any]]]:
    output: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {}
    for rule in rules:
        if rule == identity_rule:
            continue
        keys: list[tuple[Any, ...]] = []
        for change in changes:
            for item in change.assignments:
                if item.columns != rule:
                    continue
                key = item.key
                if key is not None and key not in keys:
                    keys.append(key)
        output[str(rule)] = await _fetch_by_rule(
            table, rule, keys, select_names
        )
    return output


###############################################################################
# Planning


def _group(candidates: Sequence[_Change]) -> list[list[_Change]]:
    """
    Group changes which share any current or proposed resource.

    The grouping is transitive: if A shares a resource with B, and B shares a
    different resource with C, then all three belong to one component.
    """
    parents = list(range(len(candidates)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    owners: dict[tuple[Any, ...], int] = {}
    for index, change in enumerate(candidates):
        resources: list[ManifestResource] = [change.identity]
        resources.extend(change.assignments)
        if change.current is not None:
            resources.extend(change.current["resources"])
        for item in resources:
            key = item.key
            if key is None:
                continue
            rule_key = (item.columns, key)
            if rule_key in owners:
                union(index, owners[rule_key])
            else:
                owners[rule_key] = index

    components: dict[int, list[_Change]] = defaultdict(list)
    for index, change in enumerate(candidates):
        components[find(index)].append(change)
    return list(components.values())


def _duplicates(
    changes: Sequence[_Change],
) -> dict[int, list[ManifestConflict]]:
    """
    Find resources which the manifest assigns to more than one row.
    """
    owners: dict[tuple[Any, ...], list[int]] = defaultdict(list)
    for index, change in enumerate(changes):
        for item in [change.identity, *change.assignments]:
            key = item.key
            if key is not None:
                owners[(item.columns, key)].append(index)

    output: dict[int, list[ManifestConflict]] = defaultdict(list)
    seen: set[tuple[Any, ...]] = set()
    for change in changes:
        for item in [change.identity, *change.assignments]:
            key = item.key
            if key is None:
                continue
            rule_key = (item.columns, key)
            if rule_key in seen:
                continue
            seen.add(rule_key)
            indices = owners[rule_key]
            if len(indices) > 1:
                unique_indices = tuple(dict.fromkeys(indices))
                devices = tuple(
                    changes[index].label for index in unique_indices
                )
                conflict = ManifestConflict(
                    item, rows=unique_indices, devices=devices
                )
                for index in unique_indices:
                    output[index].append(conflict)
    return output


def _check_holders(
    changes: Sequence[_Change],
    holders: Mapping[str, Mapping[tuple[Any, ...], dict[str, Any]]],
    table: "type[Table]",
    resource_rules: Sequence[tuple[str, ...]] = (),
) -> dict[int, list[ManifestConflict]]:
    """
    Reject resources which are still held by a device outside the manifest.

    A device in the manifest which is releasing the resource is fine - that's
    how a handover within a page works.
    """
    by_primary_key: dict[Any, _Change] = {}
    for change in changes:
        if change.current is not None:
            by_primary_key[change.stored_primary_key] = change

    output: dict[int, list[ManifestConflict]] = defaultdict(list)
    for change in changes:
        for item in change.assignments:
            key = item.key
            if key is None:
                continue
            holder_row = holders.get(str(item.columns), {}).get(key)
            if holder_row is None:
                continue
            holder_primary_key = _normalise_value(
                _row_value(
                    table, holder_row, table._meta.primary_key._meta.name
                )
            )
            if holder_primary_key == change.stored_primary_key:
                continue

            holder_change = by_primary_key.get(holder_primary_key)
            if holder_change is None:
                # A row may be the same device as an existing one, even if
                # the manifest didn't supply its primary key, because it was
                # identified by `device_number` (or another permanent key).
                # It's only a duplicate if it keeps the same resource.
                if change.current is not None:
                    continue
                holder_label = holder_primary_key
                for name in _lookup_columns(table, resource_rules):
                    if name == table._meta.primary_key._meta.name:
                        continue
                    value = _normalise_value(
                        _row_value(table, holder_row, name)
                    )
                    if value is not None:
                        holder_label = value
                        break
                conflict = ManifestConflict(item, devices=(holder_label,))
                output[change.receipt.index].append(conflict)
                continue

            holder_proposed = next(
                (
                    candidate
                    for candidate in holder_change.assignments
                    if candidate.columns == item.columns
                ),
                None,
            )
            if holder_proposed is not None and holder_proposed.key == key:
                indices = (
                    change.receipt.index,
                    holder_change.receipt.index,
                )
                conflict = ManifestConflict(
                    item,
                    rows=indices,
                    devices=(
                        change.label,
                        holder_change.label,
                    ),
                )
                output[change.receipt.index].append(conflict)
                output[holder_change.receipt.index].append(conflict)

    return output


def _apply_errors(
    change: _Change, conflicts: Sequence[ManifestConflict]
) -> None:
    if not conflicts:
        return
    combined = list(change.receipt.conflicts)
    for conflict in conflicts:
        if conflict not in combined:
            combined.append(conflict)
    change.receipt.conflicts = tuple(combined)
    change.receipt.rules = tuple(
        dict.fromkeys(
            [
                *change.receipt.rules,
                *(conflict.rule for conflict in conflicts),
            ]
        )
    )
    change.receipt.error = (
        change.receipt.error
        or "The manifest assigns a resource which isn't available: "
        + "; ".join(str(conflict) for conflict in conflicts)
    )


###############################################################################
# Writing


def _returning_supported(table: "type[Table]") -> bool:
    engine = table._meta.db
    if engine.engine_type in ("postgres", "cockroach"):
        return True
    if engine.engine_type == "sqlite":
        return engine.get_version_sync() >= 3.35
    return False


def _insert_columns(
    table: "type[Table]", changes: Sequence[_Change]
) -> list["Column"]:
    columns = list(table._meta.writable_columns)
    primary_key = table._meta.primary_key
    if primary_key in columns and all(
        change.primary_key is None for change in changes
    ):
        columns = [column for column in columns if column is not primary_key]

    # Columns with a database default don't need to be supplied explicitly.
    # This lets an existing device's history (and similar fields) be left
    # alone when a manifest only carries the handover values.
    optional_defaults = {
        column._meta.name
        for column in columns
        if not column._meta.primary_key
        and getattr(column, "default", ...) is not ...
    }
    selected = [
        column
        for column in columns
        if column._meta.name not in optional_defaults
        or any(
            _normalise_value(change.values.get(column._meta.name)) is not None
            for change in changes
        )
    ]
    return selected or columns


def _insert_query(
    table: "type[Table]", changes: Sequence[_Change]
) -> QueryString:
    columns = _insert_columns(table, changes)
    column_sql = ", ".join(_quoted(column) for column in columns)
    values_sql = ", ".join(
        "(" + ", ".join("{}" for _ in columns) + ")" for _ in changes
    )
    query = (
        f"INSERT INTO {_table_name(table)} ({column_sql}) "
        f"VALUES {values_sql}"
    )
    if _returning_supported(table):
        query += f" RETURNING {_quoted(table._meta.primary_key)}"
    args: list[Any] = []
    for change in changes:
        for column in columns:
            args.append(
                convert_to_sql_value(
                    value=change.values.get(column._meta.name),
                    column=column,
                )
            )
    return QueryString(query, *args, query_type="insert", table=table)


def _changed_columns(table: "type[Table]", change: _Change) -> list["Column"]:
    current_values = (change.current or {}).get("values", {})
    return [
        column
        for column in table._meta.writable_columns
        if not column._meta.primary_key
        and current_values.get(column._meta.name)
        != change.values.get(column._meta.name)
    ]


def _update_query(table: "type[Table]", change: _Change) -> QueryString:
    primary_key = table._meta.primary_key
    columns = _changed_columns(table, change)
    set_sql = ", ".join(f"{_quoted(column)} = {{}}" for column in columns)
    query = (
        f"UPDATE {_table_name(table)} SET {set_sql} "
        f"WHERE {_quoted(primary_key)} = {{}}"
    )
    args = [
        convert_to_sql_value(
            value=change.values.get(column._meta.name), column=column
        )
        for column in columns
    ]
    args.append(convert_to_sql_value(change.stored_primary_key, primary_key))
    return QueryString(query, *args, query_type="update", table=table)


async def _insert_changes(
    table: "type[Table]", changes: Sequence[_Change]
) -> None:
    if not changes:
        return

    writable_count = len(_insert_columns(table, changes))
    rows_per_insert = max(1, (MAX_BIND_VALUES - 1) // max(1, writable_count))
    primary_key_name = table._meta.primary_key._meta.name
    db_primary_key_name = table._meta.primary_key._meta.db_column_name

    for chunk in _chunks(changes, rows_per_insert):
        rows = await _run(table, _insert_query(table, chunk))
        if len(rows) == len(chunk):
            for change, row in zip(chunk, rows):
                value = row.get(db_primary_key_name, row.get(primary_key_name))
                change.values[primary_key_name] = value
        else:
            # Older SQLite builds don't support RETURNING. Insert one row at
            # a time, and use the engine's last-insert-id support.
            for change in chunk:
                single = await _run(table, _insert_query(table, [change]))
                if single:
                    row = single[-1]
                    change.values[primary_key_name] = row.get(
                        db_primary_key_name, row.get(primary_key_name)
                    )


def _release_query(
    table: "type[Table]",
    primary_key: Any,
    columns: Sequence["Column"],
) -> QueryString:
    pk_column = table._meta.primary_key
    set_sql = ", ".join(f"{_quoted(column)} = NULL" for column in columns)
    query = (
        f"UPDATE {_table_name(table)} SET {set_sql} "
        f"WHERE {_quoted(pk_column)} = {{}}"
    )
    return QueryString(
        query, convert_to_sql_value(primary_key, pk_column), table=table
    )


def _default_value(column: "Column") -> Any:
    default = getattr(column, "default", ...)
    if default is ...:
        return ...
    value = column.get_default_value()
    from piccolo.columns.defaults.base import Default

    if isinstance(value, Default):
        value = value.python()
    return value


def _merge_current_values(
    table: "type[Table]",
    change: _Change,
    resource_names: set[str],
) -> None:
    """
    Existing devices keep values which the manifest didn't supply.

    Resource columns are the exception: ``None`` means the resource is
    temporarily unassigned, which is how swaps are expressed.

    A manifest row is often a fresh object, so non-resource fields such as
    acquisition history are ``None`` or a column default. Those values
    mustn't overwrite the existing row - the whole point of a handover is
    that only the assigned resources move between devices.
    """
    current = change.current
    if current is None:
        return
    current_values = current.get("values", {})
    for column in table._meta.columns:
        name = column._meta.name
        if name in resource_names or column._meta.primary_key:
            continue
        proposed = _normalise_value(change.values.get(name))
        default = _default_value(column)
        if proposed is None or (
            default is not ... and proposed == _normalise_value(default)
        ):
            if current_values.get(name) is not None:
                change.values[name] = current_values[name]


async def _apply_component(
    table: "type[Table]",
    component: Sequence[_Change],
    rules: Sequence[tuple[str, ...]],
    identity_rule: tuple[str, ...],
    resource_names: set[str],
) -> None:
    """
    Apply one independent component.

    Resources are released before rows are inserted / updated, so two rows can
    swap their site number and port in the same page. Any failure is
    propagated to the caller, which rolls back the component's savepoint.
    """
    # 1. Release changed resource values. Only resource columns are cleared -
    # the warehouse column is shared by rules and must stay intact.
    for change in component:
        if change.is_new:
            continue
        current_values = (change.current or {}).get("values", {})
        changed_names = {
            name
            for name in resource_names
            if current_values.get(name) != change.values.get(name)
        }
        # Only NULL-able columns can be released. A NOT NULL resource can be
        # updated directly when it doesn't clash - if it does clash, the
        # database will reject the component and it'll be rolled back.
        columns = [
            table._meta.get_column_by_name(name)
            for name in sorted(changed_names)
            if table._meta.get_column_by_name(name)._meta.null
        ]
        if not columns:
            continue
        await _run(
            table,
            _release_query(table, change.stored_primary_key, columns),
        )

    # 2. Insert new rows.
    new_changes = [change for change in component if change.is_new]
    await _insert_changes(table, new_changes)

    # 3. Update existing rows. Only columns whose values actually changed are
    # written, so history and fields outside the manifest are preserved.
    for change in component:
        if change.is_new:
            continue
        if not _changed_columns(table, change):
            continue
        await _run(table, _update_query(table, change))

    # 4. Update the caller's objects, and mark the receipts accepted.
    for change in component:
        change.receipt.accepted = True
        change.receipt.primary_key = change.primary_key
        change.receipt.row._exists_in_db = True
        setattr(
            change.receipt.row,
            table._meta.primary_key._meta.name,
            change.receipt.primary_key,
        )


###############################################################################
# Public entry point


class ManifestQuery:
    """
    An awaitable returned by :meth:`Table.insert_manifest`.

    It mirrors Piccolo's other queries, so both of these work::

        result = await Device.insert_manifest(rows)
        result = Device.insert_manifest(rows).run_sync()

    """

    __slots__ = ("table", "rows", "options")

    def __init__(
        self,
        table: "type[Table]",
        rows: Sequence["Table"],
        **options: Any,
    ):
        self.table = table
        self.rows = list(rows)
        self.options = options

    async def run(self, *args: Any, **kwargs: Any) -> ManifestResult:
        return await insert_manifest(self.table, self.rows, **self.options)

    def run_sync(self, *args: Any, **kwargs: Any) -> ManifestResult:
        from piccolo.utils.sync import run_sync

        return run_sync(self.run())

    def __await__(self):
        return self.run().__await__()

    def __iter__(self):
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)


async def insert_manifest(
    table: "type[Table]",
    rows: Sequence["Table"],
    *,
    primary_key: Optional[str] = None,
    resources: Optional[Sequence[Sequence[str]]] = None,
) -> ManifestResult:
    """
    Insert / update a page of rows, treating resource handovers atomically.

    Rows are matched to existing database rows by primary key, or by a
    permanent unique identifier such as ``device_no``. Every unique
    constraint on the table (a ``unique=True`` column, or a table-level
    ``Unique`` constraint) is treated as a resource which belongs to a
    warehouse. A resource may be temporarily unassigned, which lets two rows
    swap resources within a single page.

    The whole page is processed in one transaction. Rows which don't share any
    resource form independent components, and each component has its own
    savepoint - so an independent component can still be accepted if another
    component is rejected by a database trigger or constraint.

    :param table:
        The ``Table`` subclass.
    :param rows:
        The rows from the manifest. Their order is preserved in the result.
    :param primary_key:
        The column used to match rows to existing devices. Defaults to the
        table's primary key. The database primary key is still returned in
        each receipt.
    :param resources:
        Override the resource rules. Each rule is a sequence of column names,
        for example ``[["warehouse", "site_number"], ["warehouse", "port"]]``.
    :returns:
        A :class:`ManifestResult`, containing one :class:`ManifestRow` per
        input row, in input order.

    """
    rows = list(rows)
    if not rows:
        return ManifestResult()

    identity_name = primary_key or table._meta.primary_key._meta.name
    identity_rule = (identity_name,)

    if resources is None:
        discovered = get_manifest_rules(table)
    else:
        discovered = [tuple(rule) for rule in resources]
    rules = list(
        dict.fromkeys([identity_rule, *(rule for rule in discovered if rule)])
    )

    # Table metadata doesn't always describe the live schema (for example,
    # a migration may have added a composite unique index). Include the
    # database's own rules before building the resource graph.
    database_rules = await _database_unique_rules(table)
    if database_rules and resources is None:
        rules = _merge_rules(
            table, list(dict.fromkeys([*rules, *database_rules]))
        )

    resource_rules = [rule for rule in rules if rule != identity_rule]

    warehouse = _warehouse_column(table)
    warehouse_name = warehouse._meta.name if warehouse else None
    resource_names = {
        name
        for rule in resource_rules
        for name in rule
        if name != warehouse_name
    }

    select_names = list(
        dict.fromkeys(
            [
                *(column._meta.name for column in table._meta.columns),
                *(name for rule in rules for name in rule),
                *_lookup_columns(table, resource_rules),
            ]
        )
    )

    receipts = [
        ManifestRow(index=index, row=row) for index, row in enumerate(rows)
    ]
    values_list = [_values(table, row) for row in rows]

    engine = table._meta.db
    transaction_kwargs: dict[str, Any] = {}
    if engine.engine_type == "sqlite":
        # The manifest always writes. An immediate transaction prevents other
        # connections from writing halfway through the page, and keeps the
        # released resource values invisible until commit.
        from piccolo.engine.sqlite import TransactionType

        transaction_kwargs["transaction_type"] = TransactionType.immediate

    async with engine.transaction(**transaction_kwargs) as transaction:
        primary_keys, unique_maps = await _fetch_existing(
            table, values_list, select_names, resource_rules
        )

        changes: list[_Change] = []
        for receipt, values in zip(receipts, values_list):
            current_row, identity_error = _resolve_current(
                values, primary_keys, unique_maps, table
            )
            current = (
                None
                if current_row is None
                else _current_state(table, current_row, rules)
            )
            if current is not None:
                # Existing devices keep their primary key, even if the
                # manifest identifies them by device number.
                values[table._meta.primary_key._meta.name] = current[
                    "primary_key"
                ]

            identity_values = tuple(
                _normalise_value(values.get(name)) for name in identity_rule
            )
            identity = ManifestResource(identity_rule, identity_values)
            assignments = tuple(
                _resource(values, rule)
                for rule in rules
                if rule != identity_rule
            )
            change = _Change(
                receipt=receipt,
                identity=identity,
                identity_rule=identity_rule,
                primary_key_rule=(table._meta.primary_key._meta.name,),
                assignments=assignments,
                values=values,
                current=current,
            )
            _merge_current_values(table, change, resource_names)
            # Recompute after merging preserved values.
            change.identity = ManifestResource(
                identity_rule,
                tuple(
                    _normalise_value(change.values.get(name))
                    for name in identity_rule
                ),
            )
            change.assignments = tuple(
                _resource(change.values, rule)
                for rule in rules
                if rule != identity_rule
            )
            receipt.resources = tuple(
                item
                for item in (*change.assignments, change.identity)
                if item.key is not None
            )
            if identity_error:
                receipt.rules = (identity_name,)
                receipt.error = identity_error
            changes.append(change)

        ####
        # Validation - no database writes yet.
        ####
        for index, conflicts in _duplicates(changes).items():
            _apply_errors(changes[index], conflicts)

        holders = await _fetch_holders(
            table, changes, rules, identity_rule, select_names
        )
        for index, conflicts in _check_holders(
            changes, holders, table, resource_rules
        ).items():
            _apply_errors(changes[index], conflicts)

        ####
        # Components are built from the complete resource graph. A component
        # is accepted or rejected as a unit.
        ####
        components = _group(changes)

        for component in components:
            failures = [change for change in component if change.receipt.error]
            if failures:
                details: list[str] = []
                rule_names: list[str] = []
                for failure in failures:
                    for rule_name in failure.receipt.rules:
                        if rule_name not in rule_names:
                            rule_names.append(rule_name)
                    if failure.receipt.error:
                        details.append(
                            f"{failure.label}: {failure.receipt.error}"
                        )
                devices = tuple(change.label for change in component)
                for change in component:
                    change.receipt.accepted = False
                    change.receipt.rules = tuple(
                        dict.fromkeys([*change.receipt.rules, *rule_names])
                    )
                    change.receipt.involved = devices
                    change.receipt.error = (
                        change.receipt.error
                        or "The handover failed because of another row: "
                        + "; ".join(details)
                    )
                    change.receipt.primary_key = change.primary_key
                continue

            savepoint = await transaction.savepoint()
            try:
                await _apply_component(
                    table,
                    component,
                    rules,
                    identity_rule,
                    resource_names,
                )
            except Exception as exception:
                await savepoint.rollback_to()
                try:
                    await savepoint.release()
                except Exception:
                    pass
                devices = tuple(change.label for change in component)
                labels = ", ".join(str(label) for label in devices)
                rule_names = _database_rule_names(exception, rules)
                for change in component:
                    change.receipt.accepted = False
                    change.receipt.rules = tuple(
                        dict.fromkeys(
                            [
                                *change.receipt.rules,
                                *rule_names,
                                "database",
                            ]
                        )
                    )
                    change.receipt.involved = devices
                    change.receipt.primary_key = change.primary_key
                    change.receipt.error = (
                        f"{type(exception).__name__}: {exception} "
                        f"(handover involving {labels})"
                    )
            else:
                await savepoint.release()

    return ManifestResult(receipts)
