"""Durable batch bookkeeping in SQLite.

The ledger stores one row per ``(batch, operation, resource)``.  Local row
writes and ledger registration happen in one database transaction so a crash
cannot leave a written row that is invisible to recovery.

The ledger itself does not assume the shape of the data table.  It reflects
the configured table and stores JSON snapshots of the row before and after the
batch write.  Those snapshots are what make a later withdrawal decide whether
the row still belongs to the batch.
"""
import datetime
import decimal
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional

from redbird.batch.model import BatchEntry, BatchItem, BatchOutcome, Intent, LocalStatus, RemoteStatus
from redbird.packages import import_exists, sqlalchemy

_SQLALCHEMY_AVAILABLE = import_exists("sqlalchemy")


_JSON_FIELDS = {
    "planned",
    "applied",
    "previous",
    "expected_version",
    "remote_version",
    "remote_batch",
    "remote_operation",
}


def _require_sqlalchemy():
    if not _SQLALCHEMY_AVAILABLE:  # pragma: no cover
        from redbird.utils.importing import get_import_error
        raise get_import_error("sqlalchemy")


def _jsonable(value):
    """Convert a value to a stable JSON-compatible representation.

    Values are normalised both when stored and when compared so the ledger can
    be used with dates, decimals and other non-JSON Python values.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"__bytes__": value.hex()}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(val) for val in value]
    return str(value)


def _json_dumps(value):
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)


def _json_loads(value):
    if value is None:
        return None
    return json.loads(value)


def _key(value):
    return _json_dumps(value)


def _unkey(value):
    return _json_loads(value)


def _normalise_row(row: Optional[Mapping]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {str(key): _jsonable(value) for key, value in dict(row).items()}


def _coerce(value, column):
    if value is None:
        return None
    try:
        py_type = column.type.python_type
    except (AttributeError, NotImplementedError):
        return value
    if isinstance(value, py_type):
        return value
    if py_type is datetime.datetime and isinstance(value, str):
        return datetime.datetime.fromisoformat(value)
    if py_type is datetime.date and isinstance(value, str):
        return datetime.date.fromisoformat(value)
    if py_type is datetime.time and isinstance(value, str):
        return datetime.time.fromisoformat(value)
    if py_type is bool and isinstance(value, str):
        return value.lower() in ("1", "true", "yes")
    if py_type is bytes and isinstance(value, dict) and "__bytes__" in value:
        return bytes.fromhex(value["__bytes__"])
    try:
        return py_type(value)
    except (TypeError, ValueError):
        return value


class SQLiteLedger:
    """Batch ledger backed by a SQLite database.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine, optional
        Engine connected to the SQLite database.
    conn_string : str, optional
        Connection string, for example ``sqlite:///sync.db``.
    session : sqlalchemy.orm.Session, optional
        Session whose bind is used when ``engine`` is omitted.
    target_table : str
        Name of the table that holds the synchronised rows.
    id_field : str
        Primary key field of ``target_table``.
    ledger_table : str
        Name of the ledger table.  Defaults to
        ``_redbird_batch_entries``.
    """

    def __init__(
        self,
        engine=None,
        conn_string: Optional[str] = None,
        session=None,
        target_table: str = "items",
        id_field: str = "id",
        ledger_table: str = "_redbird_batch_entries",
    ):
        _require_sqlalchemy()
        if conn_string is not None:
            engine = sqlalchemy.create_engine(conn_string)
        if engine is None and session is not None:
            engine = session.get_bind()
        if engine is None:
            raise TypeError("Pass either engine, conn_string or session")
        if not isinstance(engine, sqlalchemy.engine.Engine):
            engine = engine.engine
        self.engine = engine
        self.target_table = target_table
        self.id_field = id_field
        self.ledger_table = ledger_table
        self._target = None
        self._table = None
        self._metadata = sqlalchemy.MetaData()

    @classmethod
    def from_repo(cls, repo, **kwargs):
        """Create a ledger using the database of a :class:`SQLRepo`."""
        model_orm = getattr(repo, "model_orm", None)
        table = kwargs.pop("target_table", None)
        if table is None:
            if model_orm is not None:
                table = model_orm.__table__.name
            else:
                table = repo.table
        if table is None:
            raise TypeError("Could not determine the target table from the repository")
        kwargs.setdefault("id_field", repo.id_field)
        engine = repo.session.get_bind()
        return cls(engine=engine, target_table=table, **kwargs)

    @property
    def table(self):
        if self._table is None:
            self._table = self._define_ledger_table()
        return self._table

    def _define_ledger_table(self):
        _require_sqlalchemy()
        metadata = self._metadata
        return sqlalchemy.Table(
            self.ledger_table,
            metadata,
            sqlalchemy.Column("batch_id", sqlalchemy.Text, primary_key=True),
            sqlalchemy.Column("operation_id", sqlalchemy.Text, primary_key=True),
            sqlalchemy.Column("resource_id", sqlalchemy.Text, primary_key=True),
            sqlalchemy.Column("sequence", sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column("local_status", sqlalchemy.Text, nullable=False),
            sqlalchemy.Column("remote_status", sqlalchemy.Text, nullable=False),
            sqlalchemy.Column("outcome", sqlalchemy.Text, nullable=False),
            sqlalchemy.Column("intent", sqlalchemy.Text, nullable=False, server_default=Intent.MARK.value),
            sqlalchemy.Column("planned", sqlalchemy.Text),
            sqlalchemy.Column("applied", sqlalchemy.Text),
            sqlalchemy.Column("previous", sqlalchemy.Text),
            sqlalchemy.Column("previous_existed", sqlalchemy.Boolean, nullable=False),
            sqlalchemy.Column("expected_version", sqlalchemy.Text),
            sqlalchemy.Column("remote_version", sqlalchemy.Text),
            sqlalchemy.Column("remote_batch", sqlalchemy.Text),
            sqlalchemy.Column("remote_operation", sqlalchemy.Text),
            sqlalchemy.Column("message", sqlalchemy.Text),
            sqlalchemy.Column("updated_at", sqlalchemy.Text),
        )

    def ensure_schema(self):
        """Create the ledger table if it does not exist."""
        self.table.create(bind=self.engine, checkfirst=True)
        return self

    def create_target_table(self, columns, primary_key: Optional[str] = None):
        """Create the target table for tests and small integrations.

        ``columns`` is a mapping of column names to Python types or SQLAlchemy
        types.  The primary key defaults to ``id_field``.
        """
        _require_sqlalchemy()
        if primary_key is None:
            primary_key = self.id_field
        sql_columns = []
        for name, type_ in columns.items():
            if isinstance(type_, sqlalchemy.types.TypeEngine):
                sql_type = type_
            else:
                sql_type = {
                    str: sqlalchemy.String,
                    int: sqlalchemy.Integer,
                    float: sqlalchemy.Float,
                    bool: sqlalchemy.Boolean,
                    datetime.date: sqlalchemy.Date,
                    datetime.datetime: sqlalchemy.DateTime,
                    dict: sqlalchemy.JSON,
                }.get(type_, sqlalchemy.String)
            sql_columns.append(
                sqlalchemy.Column(name, sql_type, primary_key=(name == primary_key))
            )
        table = sqlalchemy.Table(
            self.target_table,
            self._metadata,
            *sql_columns
        )
        table.create(bind=self.engine, checkfirst=True)
        self._target = table
        return self

    def _target_table(self):
        if self._target is None:
            self._target = sqlalchemy.Table(
                self.target_table,
                sqlalchemy.MetaData(),
                autoload_with=self.engine,
            )
        return self._target

    def _select_row(self, conn, resource_id):
        target = self._target_table()
        column = target.c[self.id_field]
        return conn.execute(
            sqlalchemy.select(target).where(column == resource_id)
        ).mappings().first()

    def read_current(self, resource_id) -> Optional[Dict[str, Any]]:
        """Read the current target row as a dictionary."""
        target = self._target_table()
        column = target.c[self.id_field]
        with self.engine.connect() as conn:
            row = conn.execute(
                sqlalchemy.select(target).where(column == resource_id)
            ).mappings().first()
            return None if row is None else dict(row)

    def _next_sequence(self, conn, batch_key, operation_key):
        table = self.table
        value = conn.execute(
            sqlalchemy.select(sqlalchemy.func.max(table.c.sequence)).where(
                (table.c.batch_id == batch_key) & (table.c.operation_id == operation_key)
            )
        ).scalar()
        return (value or 0) + 1

    def _entry_values(self, entry: BatchEntry, sequence: int):
        return {
            "batch_id": _key(entry.batch_id),
            "operation_id": _key(entry.operation_id),
            "resource_id": _key(entry.resource_id),
            "sequence": sequence,
            "local_status": entry.local_status,
            "remote_status": entry.remote_status,
            "outcome": entry.outcome,
            "intent": entry.intent,
            "planned": _json_dumps(entry.planned),
            "applied": _json_dumps(entry.applied),
            "previous": _json_dumps(entry.previous),
            "previous_existed": bool(entry.previous_existed),
            "expected_version": _json_dumps(entry.expected_version),
            "remote_version": _json_dumps(entry.remote_version),
            "remote_batch": _json_dumps(entry.remote_batch),
            "remote_operation": _json_dumps(entry.remote_operation),
            "message": entry.message,
            "updated_at": datetime.datetime.utcnow().isoformat(),
        }

    def _row_to_entry(self, row) -> BatchEntry:
        data = dict(row)
        return BatchEntry(
            batch_id=_unkey(data["batch_id"]),
            operation_id=_unkey(data["operation_id"]),
            resource_id=_unkey(data["resource_id"]),
            local_status=data["local_status"],
            remote_status=data["remote_status"],
            outcome=data["outcome"],
            intent=data.get("intent", Intent.MARK.value),
            sequence=data.get("sequence", 0),
            planned=_json_loads(data["planned"]),
            applied=_json_loads(data["applied"]),
            previous=_json_loads(data["previous"]),
            previous_existed=bool(data["previous_existed"]),
            expected_version=_json_loads(data["expected_version"]),
            remote_version=_json_loads(data["remote_version"]),
            remote_batch=_json_loads(data["remote_batch"]),
            remote_operation=_json_loads(data["remote_operation"]),
            message=data["message"],
        )

    def apply(
        self,
        batch_id,
        operation_id,
        items: Iterable[BatchItem],
        if_exists: str = "upsert",
    ) -> List[BatchEntry]:
        """Write local rows and register the operation atomically.

        ``if_exists`` is ``upsert`` (default), ``update`` or ``raise``.  A
        resource already registered for this batch/operation is returned
        untouched, making retries safe.
        """
        items = list(items)
        if not items:
            return []
        target = self._target_table()
        table = self.table
        id_column = target.c[self.id_field]
        batch_key = _key(batch_id)
        operation_key = _key(operation_id)

        with self.engine.begin() as conn:
            entries = []
            for item in items:
                resource_key = _key(item.resource_id)
                existing = conn.execute(
                    sqlalchemy.select(table).where(
                        (table.c.batch_id == batch_key)
                        & (table.c.operation_id == operation_key)
                        & (table.c.resource_id == resource_key)
                    )
                ).mappings().first()

                if existing is not None:
                    # The local write and the ledger entry commit together, so
                    # an existing entry means this resource was already
                    # recorded.  Never rewrite it: the row may have been
                    # corrected manually since.
                    entries.append(self._row_to_entry(existing))
                    continue
                sequence = self._next_sequence(conn, batch_key, operation_key)

                row = self._select_row(conn, item.resource_id)
                previous = None if row is None else dict(row)
                previous_existed = row is not None

                values = dict(item.values)
                if previous_existed:
                    if if_exists == "raise":
                        raise KeyError(
                            "Item {} already exists in the target table".format(item.resource_id)
                        )
                    values.pop(self.id_field, None)
                    if values:
                        conn.execute(
                            target.update().where(id_column == item.resource_id).values(**values)
                        )
                else:
                    if if_exists == "update":
                        raise KeyError(
                            "Item {} was not found in the target table".format(item.resource_id)
                        )
                    values.setdefault(self.id_field, item.resource_id)
                    conn.execute(target.insert().values(**values))

                current = self._select_row(conn, item.resource_id)
                applied = None if current is None else dict(current)

                entry = BatchEntry(
                    batch_id=batch_id,
                    operation_id=operation_id,
                    resource_id=item.resource_id,
                    local_status=LocalStatus.APPLIED.value,
                    remote_status=RemoteStatus.PENDING.value,
                    outcome=BatchOutcome.PENDING.value,
                    intent=Intent.MARK.value,
                    planned=_jsonable(item.values),
                    applied=_jsonable(applied),
                    previous=_jsonable(previous),
                    previous_existed=previous_existed,
                    expected_version=item.expected_version,
                )
                payload = self._entry_values(entry, sequence)
                if existing is None:
                    conn.execute(table.insert().values(**payload))
                else:
                    conn.execute(
                        table.update().where(
                            (table.c.batch_id == batch_key)
                            & (table.c.operation_id == operation_key)
                            & (table.c.resource_id == resource_key)
                        ).values(**payload)
                    )
                entries.append(entry)
            return entries

    def update_entry(self, batch_id, operation_id, resource_id, **fields) -> BatchEntry:
        """Persist fields of one ledger entry and return the updated entry."""
        table = self.table
        unknown = set(fields) - set(c for c in table.c.keys() if c not in ("batch_id", "operation_id", "resource_id"))
        if unknown:
            raise KeyError("Unknown ledger fields: {}".format(sorted(unknown)))
        values = {}
        for name, value in fields.items():
            if name in _JSON_FIELDS:
                values[name] = _json_dumps(value)
            elif name == "previous_existed":
                values[name] = bool(value)
            else:
                values[name] = value
        values["updated_at"] = datetime.datetime.utcnow().isoformat()

        where = (
            (table.c.batch_id == _key(batch_id))
            & (table.c.operation_id == _key(operation_id))
            & (table.c.resource_id == _key(resource_id))
        )
        with self.engine.begin() as conn:
            result = conn.execute(table.update().where(where).values(**values))
            if result.rowcount == 0:
                raise KeyError("Batch entry not found")
            row = conn.execute(sqlalchemy.select(table).where(where)).mappings().first()
            return self._row_to_entry(row)

    def entry(self, batch_id, operation_id, resource_id) -> Optional[BatchEntry]:
        table = self.table
        with self.engine.connect() as conn:
            row = conn.execute(
                sqlalchemy.select(table).where(
                    (table.c.batch_id == _key(batch_id))
                    & (table.c.operation_id == _key(operation_id))
                    & (table.c.resource_id == _key(resource_id))
                )
            ).mappings().first()
            return None if row is None else self._row_to_entry(row)

    def entries(
        self,
        batch_id=None,
        operation_id=None,
        outcome: Optional[str] = None,
    ) -> List[BatchEntry]:
        """Query ledger entries, optionally filtered by batch/operation/outcome."""
        table = self.table
        query = sqlalchemy.select(table)
        if batch_id is not None:
            query = query.where(table.c.batch_id == _key(batch_id))
        if operation_id is not None:
            query = query.where(table.c.operation_id == _key(operation_id))
        if outcome is not None:
            query = query.where(table.c.outcome == outcome)
        query = query.order_by(table.c.sequence, table.c.resource_id)
        with self.engine.connect() as conn:
            return [self._row_to_entry(row) for row in conn.execute(query).mappings()]

    def outcome(self, batch_id, operation_id, resource_id) -> Optional[str]:
        """Return the outcome of one resource, or ``None`` if not in the batch."""
        entry = self.entry(batch_id, operation_id, resource_id)
        return None if entry is None else entry.outcome

    def summary(self, batch_id=None, operation_id=None) -> Dict[str, int]:
        """Count final outcomes for a batch/operation.

        The result always contains ``retained``, ``withdrawn``, ``conflict``
        and ``pending`` keys.
        """
        counts = {
            BatchOutcome.RETAINED.value: 0,
            BatchOutcome.WITHDRAWN.value: 0,
            BatchOutcome.CONFLICT.value: 0,
            BatchOutcome.PENDING.value: 0,
        }
        for entry in self.entries(batch_id=batch_id, operation_id=operation_id):
            counts[entry.outcome] = counts.get(entry.outcome, 0) + 1
        return counts

    def _batch_changed_fields(self, entry: BatchEntry):
        applied = _normalise_row(entry.applied) or {}
        previous = _normalise_row(entry.previous) or {}
        if not entry.previous_existed:
            return [name for name in applied if name != self.id_field]
        return [
            name for name in applied
            if name != self.id_field and applied.get(name) != previous.get(name)
        ]

    def local_owned(self, entry: BatchEntry) -> bool:
        """Whether the local row still contains the batch's changes.

        Only fields the batch actually changed are compared.  A manual change
        to an unrelated field does not make the batch's own changes invalid.
        """
        current = _normalise_row(self.read_current(entry.resource_id))
        applied = _normalise_row(entry.applied)
        if current is None:
            return applied is None
        if applied is None:
            return False
        if not entry.previous_existed:
            return current == applied
        previous_norm = _normalise_row(entry.previous) or {}
        for name in self._batch_changed_fields(entry):
            value = current.get(name)
            if value not in (applied.get(name), previous_norm.get(name)):
                return False
        return True

    def withdraw_local(self, entry: BatchEntry, conflict_message: str = None) -> BatchEntry:
        """Revert the local row and record the withdrawal atomically."""
        target = self._target_table()
        id_column = target.c[self.id_field]
        applied = _normalise_row(entry.applied)
        previous = entry.previous
        previous_norm = _normalise_row(previous)
        table = self.table
        where = (
            (table.c.batch_id == _key(entry.batch_id))
            & (table.c.operation_id == _key(entry.operation_id))
            & (table.c.resource_id == _key(entry.resource_id))
        )

        with self.engine.begin() as conn:
            row = self._select_row(conn, entry.resource_id)
            current = _normalise_row(row)

            if current is None:
                result = "already" if not entry.previous_existed else "conflict"
            elif previous_norm is not None and current == previous_norm:
                result = "already"
            elif not entry.previous_existed:
                result = "reverted" if current == applied else "conflict"
            else:
                changed = self._batch_changed_fields(entry)
                if any(
                    current.get(name) not in (applied.get(name), previous_norm.get(name))
                    for name in changed
                ):
                    result = "conflict"
                else:
                    result = "reverted"

            if result == "conflict":
                values = {
                    "outcome": BatchOutcome.CONFLICT.value,
                    "local_status": LocalStatus.PRESERVED.value,
                    "message": conflict_message or "Local row was superseded",
                    "updated_at": datetime.datetime.utcnow().isoformat(),
                }
            else:
                if result == "reverted":
                    if entry.previous_existed:
                        changed = self._batch_changed_fields(entry)
                        values_to_restore = {
                            column.name: _coerce(previous_norm[column.name], column)
                            for column in target.columns
                            if column.name in changed
                            and column.name in previous_norm
                            and current.get(column.name) == applied.get(column.name)
                        }
                        if values_to_restore:
                            conn.execute(
                                target.update().where(id_column == entry.resource_id).values(**values_to_restore)
                            )
                    else:
                        conn.execute(target.delete().where(id_column == entry.resource_id))
                values = {
                    "outcome": BatchOutcome.WITHDRAWN.value,
                    "local_status": LocalStatus.REVERTED.value if result == "reverted" else entry.local_status,
                    "message": None,
                    "updated_at": datetime.datetime.utcnow().isoformat(),
                }

            conn.execute(table.update().where(where).values(**values))
            row = conn.execute(sqlalchemy.select(table).where(where)).mappings().first()
            return self._row_to_entry(row)
