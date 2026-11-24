"""Committed change tracking for dataset tables.

When change tracking is enabled on a database (``track_changes=True``),
every insert, update and delete performed through :py:class:`dataset.Table`
is recorded in a change log table inside the same transaction as the data
write itself. This means that change records commit and roll back together
with the data they describe, including nested (savepoint) transactions.

Readers use :py:meth:`dataset.Database.changes` or
:py:meth:`dataset.Table.changes` to retrieve the net row-level differences
produced by each committed outer transaction. Values are read back from the
database after each write, so generated primary keys, column defaults and
trigger-modified values are reflected in the reported changes.
"""

import json
from base64 import b64decode, b64encode
from collections import OrderedDict
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import BigInteger, Column, Integer, MetaData, String, Text
from sqlalchemy import Table as SQLATable
from sqlalchemy.sql import select

if TYPE_CHECKING:
    from dataset.database import Database

CHANGE_TABLE_NAME = "_dataset_changes"

OP_INSERT = "insert"
OP_UPDATE = "update"
OP_DELETE = "delete"

ChangeOp = Literal["insert", "update", "delete"]
Change = dict[str, Any]
# A (primary key, row) pair as stored in the change log. Either may be None
# for tables without a primary key, or rows that could not be read back.
ChangeEntry = tuple[dict[str, Any] | None, dict[str, Any] | None]

_TYPE_KEY = "__dataset_type__"
_TYPE_VALUE = "value"


def _encode_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return {_TYPE_KEY: "datetime", _TYPE_VALUE: value.isoformat()}
    if isinstance(value, date):
        return {_TYPE_KEY: "date", _TYPE_VALUE: value.isoformat()}
    if isinstance(value, Decimal):
        return {_TYPE_KEY: "decimal", _TYPE_VALUE: str(value)}
    if isinstance(value, bytes):
        return {_TYPE_KEY: "bytes", _TYPE_VALUE: b64encode(value).decode("ascii")}
    return value


def _decode_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value.keys()) == {_TYPE_KEY, _TYPE_VALUE}:
        kind = value[_TYPE_KEY]
        raw = value[_TYPE_VALUE]
        if kind == "datetime":
            return datetime.fromisoformat(raw)
        if kind == "date":
            return date.fromisoformat(raw)
        if kind == "decimal":
            return Decimal(raw)
        if kind == "bytes":
            return b64decode(raw.encode("ascii"))
    return value


def encode_row(row: dict[str, Any] | None) -> str | None:
    """Serialise a row to JSON, preserving common SQL value types."""
    if row is None:
        return None
    return json.dumps({k: _encode_value(v) for k, v in row.items()}, default=str)


def decode_row(data: str | None) -> dict[str, Any] | None:
    """Deserialise a row previously encoded with :py:func:`encode_row`."""
    if data is None:
        return None
    return {k: _decode_value(v) for k, v in json.loads(data).items()}


class ChangeSet:
    """The net committed changes read from the change log.

    ``changes`` is a list of dicts with the keys ``table``, ``op`` (one of
    ``insert``, ``update`` or ``delete``), ``pk`` (the primary key values of
    the affected row, or ``None``) and ``row`` (the row values as finally
    stored in the database; for deletes, the values just before deletion).

    ``bookmark`` is an opaque position in the change log. Once a read has
    been confirmed, the bookmark can be handed to another reader which
    passes it as ``since`` to resume exactly after these changes.
    """

    def __init__(self, changes: list[Change], bookmark: int) -> None:
        self.changes = changes
        self.bookmark = bookmark

    def __iter__(self) -> Iterator[Change]:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    def __getitem__(self, index: int) -> Change:
        return self.changes[index]

    def __bool__(self) -> bool:
        return bool(self.changes)

    def __repr__(self) -> str:
        return f"<ChangeSet({len(self.changes)} changes, bookmark={self.bookmark})>"


def ensure_change_table(db: "Database") -> SQLATable:
    """Create the change log table if it does not exist yet."""
    if db._changes_table is not None:
        return db._changes_table
    with db.lock:
        if db._changes_table is None:
            metadata = MetaData(schema=db.schema)
            table = SQLATable(
                CHANGE_TABLE_NAME,
                metadata,
                Column(
                    "id",
                    BigInteger().with_variant(Integer, "sqlite"),
                    primary_key=True,
                ),
                Column("tx_id", String(64), nullable=False),
                Column("table_name", String(255), nullable=False),
                Column("op", String(16), nullable=False),
                Column("pk", Text, nullable=True),
                Column("row", Text, nullable=True),
            )
            table.create(db.executable, checkfirst=True)
            db._changes_table = table
            db._auto_commit()
    return db._changes_table


def log_changes(
    db: "Database",
    table_name: str,
    op: ChangeOp,
    entries: list[ChangeEntry],
) -> None:
    """Record change entries inside the currently open transaction.

    All entries of a single write operation share one transaction id, so
    that readers can group them into the commit they belong to.
    """
    if not entries:
        return
    table = ensure_change_table(db)
    tx_id = db._change_tx_id()
    db.executable.execute(
        table.insert(),
        [
            {
                "tx_id": tx_id,
                "table_name": table_name,
                "op": op,
                "pk": encode_row(pk),
                "row": encode_row(row),
            }
            for pk, row in entries
        ],
    )


def read_changes(
    db: "Database",
    since: "int | ChangeSet | None" = None,
    table_name: str | None = None,
) -> ChangeSet:
    """Read committed changes, collapsed to net row differences per commit.

    Only changes from transactions that have been committed are visible:
    changes written by the caller's own still-open transaction are excluded,
    and other connections cannot see uncommitted entries at all.
    """
    if isinstance(since, ChangeSet):
        since = since.bookmark
    table = db._changes_table
    if table is None:
        if not db.has_table(CHANGE_TABLE_NAME):
            return ChangeSet([], since or 0)
        table = ensure_change_table(db)
    stmt = select(table).order_by(table.c.id)
    if since:
        stmt = stmt.where(table.c.id > since)
    if table_name is not None:
        stmt = stmt.where(table.c.table_name == table_name)
    current_tx = db._current_change_tx_id()
    if current_tx is not None:
        stmt = stmt.where(table.c.tx_id != current_tx)
    rp = db.executable.execute(stmt)

    bookmark = since or 0
    groups: dict[str, "OrderedDict[tuple[str, str], list[Any]]"] = OrderedDict()
    for row in rp.mappings():
        bookmark = max(bookmark, row["id"])
        tx_groups = groups.setdefault(row["tx_id"], OrderedDict())
        row_key = row["pk"] if row["pk"] is not None else f"row:{row['row']}"
        key = (row["table_name"], row_key)
        tx_groups.setdefault(key, []).append(row)

    changes: list[Change] = []
    for tx_groups in groups.values():
        for entries in tx_groups.values():
            first, last = entries[0], entries[-1]
            if first["op"] == OP_INSERT and last["op"] == OP_DELETE:
                # Created and removed within a single commit: no net change.
                continue
            op: ChangeOp
            if first["op"] == OP_INSERT:
                op = OP_INSERT
            elif last["op"] == OP_DELETE:
                op = OP_DELETE
            else:
                op = OP_UPDATE
            changes.append(
                {
                    "table": last["table_name"],
                    "op": op,
                    "pk": decode_row(last["pk"]),
                    "row": decode_row(last["row"]),
                }
            )
    return ChangeSet(changes, bookmark)
