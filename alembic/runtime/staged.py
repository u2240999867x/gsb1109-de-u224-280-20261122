"""Staged, restart-safe offline migration artifacts.

A staged artifact is compiled without any target database connection.  It
splits each revision into declared phases:

``transaction``
    Ordinary DDL.  When a revision contains no autocommit phase, its version
    bookkeeping is included in this same transaction, so a failure rolls
    back both the DDL and the progress update.

``autocommit``
    DDL that must run outside of a transaction, such as PostgreSQL
    ``CREATE INDEX CONCURRENTLY``.  The preceding transaction is committed
    first, and the following transaction is started only after the phase
    completes.

``commit``
    Version bookkeeping in a new transaction, emitted after an autocommit
    phase.

Each phase declares read-only checks derived from the compiled definition.
A check returns a single scalar value and classifies as one of:

``not_applied``
    The effect is absent.
``applied``
    The effect is present and, for a ``definition`` check, matches exactly.
``conflict``
    Something with the same identity exists but does not match the
    definition in the artifact, or the version table does not match any
    boundary declared by the artifact.

The runner evaluates all checks of a revision before its first write.  It
skips applied phases, completes an effect whose DDL landed but whose
progress was not written (only where the backend cannot make DDL
transactional), and aborts on conflicts.  The same artifact is therefore
safe to replay after a process stop at any phase boundary.
"""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
import json
import os
from typing import Any
from typing import Iterator
from typing import Literal
from typing import TYPE_CHECKING

from sqlalchemy import schema
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from .. import util
from ..util import sqla_compat

if TYPE_CHECKING:
    from .migration import MigrationContext

ARTIFACT_FORMAT = "alembic-staged-artifact"
ARTIFACT_VERSION = 1

PhaseKind = Literal["transaction", "autocommit", "commit"]
CheckKind = Literal["exists", "absent", "definition", "version"]
PhaseState = Literal["not_applied", "applied", "conflict"]


@dataclass
class PhaseCheck:
    """A read-only check for a database effect."""

    sql: str
    """A single ``SELECT`` statement returning one scalar value."""

    kind: CheckKind
    expected: str | None = None
    before: str | None = None
    """For ``version`` checks, the version value before the revision."""

    def to_json(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "kind": self.kind,
            "expected": self.expected,
            "before": self.before,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> PhaseCheck:
        return cls(
            sql=data["sql"],
            kind=data["kind"],
            expected=data.get("expected"),
            before=data.get("before"),
        )

    def classify(self, value: Any) -> tuple[PhaseState, str | None]:
        if self.kind == "exists":
            if value is None or value is False or value == 0 or value == "":
                return "not_applied", "effect not present"
            return "applied", None
        elif self.kind == "absent":
            if value is None or value is False or value == 0 or value == "":
                return "applied", None
            return "not_applied", "effect still present"
        elif self.kind == "version":
            normalized = "" if value is None else str(value)
            if normalized == (self.expected or ""):
                return "applied", None
            elif normalized == (self.before or ""):
                return "not_applied", "version not written"
            else:
                return (
                    "conflict",
                    "version is %r, expected %r"
                    % (normalized, self.expected or ""),
                )
        else:
            if value is None or value == "":
                return "not_applied", "effect not present"
            elif str(value) == self.expected:
                return "applied", None
            else:
                return (
                    "conflict",
                    "found %r, expected %r" % (value, self.expected),
                )


@dataclass
class StagedStatement:
    """One SQL statement and the checks that describe its effect."""

    sql: str
    role: Literal["ddl", "version", "setup"] = "ddl"
    checks: Sequence[PhaseCheck] = field(default_factory=tuple)

    def to_json(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "role": self.role,
            "checks": [c.to_json() for c in self.checks],
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> StagedStatement:
        return cls(
            sql=data["sql"],
            role=data["role"],
            checks=[PhaseCheck.from_json(c) for c in data.get("checks") or ()],
        )


@dataclass
class StagedPhase:
    """One executable unit of a staged revision."""

    index: int
    kind: PhaseKind
    statements: Sequence[StagedStatement] = field(default_factory=tuple)
    version_check: PhaseCheck | None = None
    label: str = ""

    @property
    def name(self) -> str:
        suffix = " - %s" % self.label if self.label else ""
        return "phase %d (%s)%s" % (self.index, self.kind, suffix)

    @property
    def sql(self) -> list[str]:
        return [s.sql for s in self.statements]

    @property
    def ddl_statements(self) -> list[StagedStatement]:
        return [s for s in self.statements if s.role == "ddl"]

    @property
    def version_statements(self) -> list[StagedStatement]:
        return [s for s in self.statements if s.role == "version"]

    @property
    def setup_statements(self) -> list[StagedStatement]:
        return [s for s in self.statements if s.role == "setup"]

    @property
    def write_statements(self) -> list[StagedStatement]:
        """DDL and setup statements, in execution order."""

        return [s for s in self.statements if s.role in ("ddl", "setup")]

    @property
    def ddl_checks(self) -> list[PhaseCheck]:
        """Checks describing user DDL effects (not setup/progress)."""

        return [c for s in self.ddl_statements for c in s.checks]

    def to_json(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "kind": self.kind,
            "label": self.label,
            "statements": [s.to_json() for s in self.statements],
            "version_check": (
                self.version_check.to_json() if self.version_check else None
            ),
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> StagedPhase:
        version_check = data.get("version_check")
        return cls(
            index=data["index"],
            kind=data["kind"],
            label=data.get("label") or "",
            statements=[
                StagedStatement.from_json(s) for s in data["statements"]
            ],
            version_check=(
                PhaseCheck.from_json(version_check) if version_check else None
            ),
        )


@dataclass
class StagedRevision:
    """A revision split into phases."""

    revision: str
    direction: Literal["upgrade", "downgrade"]
    source_revisions: Sequence[str] = field(default_factory=tuple)
    destination_revisions: Sequence[str] = field(default_factory=tuple)
    heads_before: Sequence[str] = field(default_factory=tuple)
    heads_after: Sequence[str] = field(default_factory=tuple)
    phases: Sequence[StagedPhase] = field(default_factory=list)
    doc: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "direction": self.direction,
            "source_revisions": list(self.source_revisions),
            "destination_revisions": list(self.destination_revisions),
            "heads_before": list(self.heads_before),
            "heads_after": list(self.heads_after),
            "phases": [p.to_json() for p in self.phases],
            "doc": self.doc,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> StagedRevision:
        return cls(
            revision=data["revision"],
            direction=data["direction"],
            source_revisions=tuple(data.get("source_revisions") or ()),
            destination_revisions=tuple(
                data.get("destination_revisions") or ()
            ),
            heads_before=tuple(data.get("heads_before") or ()),
            heads_after=tuple(data.get("heads_after") or ()),
            phases=[StagedPhase.from_json(p) for p in data["phases"]],
            doc=data.get("doc"),
        )


@dataclass
class StagedArtifact:
    """A compiled, replayable offline migration artifact."""

    dialect: str
    expected_start: Sequence[str] = field(default_factory=tuple)
    revisions: list[StagedRevision] = field(default_factory=list)
    version_table: str = "alembic_version"
    version_table_schema: str | None = None
    transactional_ddl: bool = True

    def to_json(self) -> dict[str, Any]:
        return {
            "format": ARTIFACT_FORMAT,
            "format_version": ARTIFACT_VERSION,
            "dialect": self.dialect,
            "transactional_ddl": self.transactional_ddl,
            "expected_start": list(self.expected_start),
            "version_table": self.version_table,
            "version_table_schema": self.version_table_schema,
            "revisions": [r.to_json() for r in self.revisions],
        }

    def dumps(self) -> str:
        return json.dumps(self.to_json(), indent=2) + "\n"

    def write(self, path: str | os.PathLike[str]) -> None:
        with open(path, "w") as f:
            f.write(self.dumps())

    def render_sql(self) -> str:
        """Render the phases as an executable SQL script.

        Transaction and commit phases are bracketed by ``BEGIN`` and
        ``COMMIT``.  Autocommit phases appear between the ``COMMIT`` of the
        preceding transaction and the ``BEGIN`` of the next one.
        """

        lines: list[str] = []
        for revision in self.revisions:
            lines.append(
                "-- revision %s (%s)"
                % (revision.revision, revision.direction)
            )
            for phase in revision.phases:
                if phase.kind in ("transaction", "commit"):
                    lines.append("BEGIN;")
                    lines.extend(
                        _terminate(s.sql) for s in phase.statements
                    )
                    lines.append("COMMIT;")
                else:
                    lines.extend(
                        _terminate(s.sql) for s in phase.statements
                    )
            lines.append("")
        return "\n".join(lines)

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> StagedArtifact:
        with open(path) as f:
            return cls.from_json(json.load(f))

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> StagedArtifact:
        if data.get("format") != ARTIFACT_FORMAT:
            raise util.CommandError(
                "Not an Alembic staged artifact: %r" % (data.get("format"),)
            )
        if data.get("format_version") != ARTIFACT_VERSION:
            raise util.CommandError(
                "Unsupported staged artifact format version %r"
                % (data.get("format_version"),)
            )
        return cls(
            dialect=data["dialect"],
            transactional_ddl=data.get("transactional_ddl", True),
            expected_start=tuple(data.get("expected_start") or ()),
            version_table=data.get("version_table", "alembic_version"),
            version_table_schema=data.get("version_table_schema"),
            revisions=[
                StagedRevision.from_json(r) for r in data["revisions"]
            ],
        )


class StagedConflictError(util.CommandError):
    """Raised when a staged replay finds conflicting database state."""


def _clean_sql(sql: str) -> str:
    """Normalize captured SQL to a plain executable statement."""

    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    # version DML is captured with a RETURNING clause in SQLAlchemy 2;
    # offline scripts and exec_driver_sql need a plain statement
    upper = sql.upper()
    idx = upper.find(" RETURNING ")
    if idx >= 0:
        sql = sql[:idx].rstrip()
    return sql


def _version_create_checks(ctx: MigrationContext) -> list[PhaseCheck]:
    return [
        _table_exists_check(
            ctx.dialect.name,
            ctx.version_table,
            ctx.version_table_schema,
        )
    ]


def _terminate(sql: str) -> str:
    stripped = sql.rstrip()
    if stripped.endswith(";"):
        return stripped
    return stripped + ";"


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def _q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _qualified_table(table_name: str, schema_name: str | None) -> str:
    if schema_name:
        return "%s.%s" % (_qident(schema_name), _qident(table_name))
    return _qident(table_name)


def _normalize(sql: str) -> str:
    return " ".join(sql.split()).rstrip(";")


def _table_exists_check(
    dialect: str,
    table_name: str,
    schema_name: str | None = None,
) -> PhaseCheck:
    if dialect == "sqlite":
        sql = (
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = %s" % _q(table_name)
        )
    elif dialect == "postgresql":
        schema_pred = (
            "table_schema = %s" % _q(schema_name)
            if schema_name
            else "table_schema = current_schema()"
        )
        sql = (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = %s AND %s" % (_q(table_name), schema_pred)
        )
    else:
        sql = (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = %s" % _q(table_name)
        )
    return PhaseCheck(sql=sql, kind="exists")


def _table_columns_check(
    dialect: str,
    table_name: str,
    expected: str,
    schema_name: str | None = None,
) -> PhaseCheck:
    if dialect == "sqlite":
        sql = (
            "SELECT (SELECT group_concat(name, ',') FROM "
            "(SELECT name FROM pragma_table_info(%s) ORDER BY cid))"
            % _q(table_name)
        )
    elif dialect == "postgresql":
        schema_pred = (
            "n.nspname = %s" % _q(schema_name)
            if schema_name
            else "n.nspname = current_schema()"
        )
        sql = (
            "SELECT array_to_string(ARRAY(SELECT a.attname "
            "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = %s AND %s AND a.attnum > 0 "
            "AND NOT a.attisdropped ORDER BY a.attnum), ',')"
            % (_q(table_name), schema_pred)
        )
    else:
        sql = (
            "SELECT group_concat(column_name, ',') FROM ("
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position)"
            % _q(table_name)
        )
    return PhaseCheck(sql=sql, kind="definition", expected=expected)


def _index_definition_check(
    dialect: str,
    index: schema.Index,
    create_sql: str,
) -> PhaseCheck:
    table = index.table
    assert table is not None
    table_name = table.name
    schema_name = table.schema
    index_name = str(index.name)
    columns = _index_expected_columns(index)

    if columns is not None and dialect == "sqlite":
        sql = (
            "SELECT (SELECT group_concat(name, ',') FROM "
            "(SELECT name FROM pragma_index_info(%s) ORDER BY seqno)) "
            "|| '|' || (SELECT \"unique\" FROM pragma_index_list(%s) "
            "WHERE name = %s LIMIT 1)"
            % (_q(index_name), _q(table_name), _q(index_name))
        )
        return PhaseCheck(
            sql=sql,
            kind="definition",
            expected="%s|%d" % (columns, 1 if index.unique else 0),
        )
    elif columns is not None and dialect == "postgresql":
        schema_pred = (
            "n.nspname = %s" % _q(schema_name)
            if schema_name
            else "n.nspname = current_schema()"
        )
        sql = (
            "SELECT array_to_string(ARRAY("
            "SELECT COALESCE(a.attname, "
            "pg_get_indexdef(i.indexrelid, k.ord::int, true)) "
            "FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
            "LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid "
            "AND a.attnum = k.attnum ORDER BY k.ord), ',') "
            "|| '|' || (CASE WHEN i.indisunique THEN '1' ELSE '0' END) "
            "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = %s AND %s"
            % (_q(index_name), schema_pred)
        )
        return PhaseCheck(
            sql=sql,
            kind="definition",
            expected="%s|%d" % (columns, 1 if index.unique else 0),
        )
    elif dialect == "sqlite":
        sql = (
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'index' AND name = %s" % _q(index_name)
        )
    else:
        schema_pred = (
            "n.nspname = %s" % _q(schema_name)
            if schema_name
            else "n.nspname = current_schema()"
        )
        sql = (
            "SELECT pg_get_indexdef(i.indexrelid) "
            "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = %s AND %s"
            % (_q(index_name), schema_pred)
        )
    return PhaseCheck(
        sql=sql, kind="definition", expected=_normalize(create_sql)
    )


def _index_expected_columns(index: schema.Index) -> str | None:
    parts = []
    for expr in index.expressions:
        name = getattr(expr, "name", None)
        if name is None:
            return None
        parts.append(str(name))
    return ",".join(parts)


def _index_absent_check(
    dialect: str, index_name: str, schema_name: str | None = None
) -> PhaseCheck:
    if dialect == "sqlite":
        sql = (
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'index' AND name = %s" % _q(index_name)
        )
    elif dialect == "postgresql":
        schema_pred = (
            "n.nspname = %s" % _q(schema_name)
            if schema_name
            else "n.nspname = current_schema()"
        )
        sql = (
            "SELECT 1 FROM pg_class c JOIN pg_namespace n "
            "ON n.oid = c.relnamespace WHERE c.relname = %s AND %s"
            % (_q(index_name), schema_pred)
        )
    else:
        sql = (
            "SELECT 1 FROM information_schema.statistics "
            "WHERE index_name = %s" % _q(index_name)
        )
    return PhaseCheck(sql=sql, kind="absent")


def _column_exists_check(
    dialect: str,
    table_name: str,
    column_name: str,
    schema_name: str | None = None,
) -> PhaseCheck:
    if dialect == "sqlite":
        sql = (
            "SELECT 1 FROM pragma_table_info(%s) WHERE name = %s"
            % (_q(table_name), _q(column_name))
        )
    elif dialect == "postgresql":
        schema_pred = (
            "table_schema = %s" % _q(schema_name)
            if schema_name
            else "table_schema = current_schema()"
        )
        sql = (
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = %s AND %s"
            % (_q(table_name), _q(column_name), schema_pred)
        )
    else:
        sql = (
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = %s"
            % (_q(table_name), _q(column_name))
        )
    return PhaseCheck(sql=sql, kind="exists")


def _column_absent_check(
    dialect: str,
    table_name: str,
    column_name: str,
    schema_name: str | None = None,
) -> PhaseCheck:
    check = _column_exists_check(
        dialect, table_name, column_name, schema_name
    )
    check.kind = "absent"
    return check


def _constraint_exists_check(
    dialect: str,
    table_name: str,
    constraint_name: str,
    schema_name: str | None = None,
) -> PhaseCheck:
    if dialect == "sqlite":
        sql = (
            "SELECT 1 FROM sqlite_master WHERE type = 'index' "
            "AND name = %s" % _q(constraint_name)
        )
    elif dialect == "postgresql":
        schema_pred = (
            "n.nspname = %s" % _q(schema_name)
            if schema_name
            else "n.nspname = current_schema()"
        )
        sql = (
            "SELECT 1 FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = %s AND con.conname = %s AND %s"
            % (_q(table_name), _q(constraint_name), schema_pred)
        )
    else:
        sql = (
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_name = %s AND constraint_name = %s"
            % (_q(table_name), _q(constraint_name))
        )
    return PhaseCheck(sql=sql, kind="exists")


def _version_check(
    dialect: str,
    version_table: str,
    schema_name: str | None,
    heads_before: Iterable[str],
    heads_after: Iterable[str],
) -> PhaseCheck:
    qualified = _qualified_table(version_table, schema_name)
    if dialect == "sqlite":
        sql = (
            "SELECT group_concat(version_num, ',') FROM "
            "(SELECT version_num FROM %s ORDER BY version_num)" % qualified
        )
    elif dialect == "postgresql":
        sql = (
            "SELECT string_agg(version_num, ',' ORDER BY version_num) "
            "FROM %s" % qualified
        )
    else:
        sql = (
            "SELECT group_concat(version_num, ',') FROM "
            "(SELECT version_num FROM %s ORDER BY version_num)" % qualified
        )
    return PhaseCheck(
        sql=sql,
        kind="version",
        before=",".join(sorted(heads_before)),
        expected=",".join(sorted(heads_after)),
    )


# ---------------------------------------------------------------------------
# Checks derived from constructs
# ---------------------------------------------------------------------------


def _checks_for_construct(
    ctx: MigrationContext, construct: Any, sql: str
) -> list[PhaseCheck]:
    dialect = ctx.dialect.name
    checks: list[PhaseCheck] = []
    if isinstance(construct, schema.CreateTable):
        table = construct.element
        if table.name == ctx.version_table:
            return checks
        checks.append(
            _table_exists_check(dialect, table.name, table.schema)
        )
        checks.append(
            _table_columns_check(
                dialect,
                table.name,
                ",".join(c.name for c in table.columns),
                table.schema,
            )
        )
    elif isinstance(construct, schema.DropTable):
        table = construct.element
        check = _table_exists_check(dialect, table.name, table.schema)
        check.kind = "absent"
        checks.append(check)
    elif isinstance(construct, schema.CreateIndex):
        checks.append(
            _index_definition_check(dialect, construct.element, sql)
        )
    elif isinstance(construct, schema.DropIndex):
        index = construct.element
        index_table = index.table
        checks.append(
            _index_absent_check(
                dialect,
                str(index.name),
                index_table.schema if index_table is not None else None,
            )
        )
    elif isinstance(construct, schema.AddConstraint):
        constraint = construct.element
        con_table = getattr(constraint, "table", None)
        if con_table is not None and constraint.name is not None:
            checks.append(
                _constraint_exists_check(
                    dialect,
                    con_table.name,
                    str(constraint.name),
                    con_table.schema,
                )
            )
    elif isinstance(construct, schema.DropConstraint):
        constraint = construct.element
        con_table = getattr(constraint, "table", None)
        if con_table is not None and constraint.name is not None:
            check = _constraint_exists_check(
                dialect,
                con_table.name,
                str(constraint.name),
                con_table.schema,
            )
            check.kind = "absent"
            checks.append(check)
    else:
        checks.extend(_checks_for_alembic_ddl(dialect, construct))
    return checks


def _checks_for_alembic_ddl(dialect: str, construct: Any) -> list[PhaseCheck]:
    from ..ddl import base as ddl_base

    if isinstance(construct, ddl_base.AddColumn):
        return [
            _column_exists_check(
                dialect,
                construct.table_name,
                construct.column.name,
                construct.schema,
            )
        ]
    elif isinstance(construct, ddl_base.DropColumn):
        return [
            _column_absent_check(
                dialect,
                construct.table_name,
                construct.column.name,
                construct.schema,
            )
        ]
    elif isinstance(construct, ddl_base.AlterColumn):
        return [
            _column_exists_check(
                dialect,
                construct.table_name,
                construct.column_name,
                construct.schema,
            )
        ]
    return []


def _raw_sql_checks(ctx: MigrationContext, sql: str) -> list[PhaseCheck]:
    """Derive conservative checks for simple hand-written DDL."""

    dialect = ctx.dialect.name
    normalized = _normalize(sql)
    upper = normalized.upper()
    if upper.startswith("CREATE TABLE "):
        name = _identifier_after(normalized, "CREATE TABLE ")
        if name is not None:
            return [_table_exists_check(dialect, name)]
    elif upper.startswith("DROP TABLE "):
        name = _identifier_after(normalized, "DROP TABLE ")
        if name is not None:
            check = _table_exists_check(dialect, name)
            check.kind = "absent"
            return [check]
    elif upper.startswith("CREATE UNIQUE INDEX "):
        name = _identifier_after(normalized, "CREATE UNIQUE INDEX ")
        if name is not None:
            return [_raw_index_exists_check(dialect, name)]
    elif upper.startswith("CREATE INDEX "):
        name = _identifier_after(normalized, "CREATE INDEX ")
        if name is not None:
            return [_raw_index_exists_check(dialect, name)]
    elif upper.startswith("DROP INDEX "):
        name = _identifier_after(normalized, "DROP INDEX ")
        if name is not None:
            check = _raw_index_exists_check(dialect, name)
            check.kind = "absent"
            return [check]
    return []


def _raw_index_exists_check(dialect: str, name: str) -> PhaseCheck:
    if dialect == "sqlite":
        sql = (
            "SELECT 1 FROM sqlite_master WHERE type = 'index' "
            "AND name = %s" % _q(name)
        )
    elif dialect == "postgresql":
        sql = (
            "SELECT 1 FROM pg_class c JOIN pg_namespace n "
            "ON n.oid = c.relnamespace WHERE c.relname = %s "
            "AND n.nspname = current_schema()" % _q(name)
        )
    else:
        sql = (
            "SELECT 1 FROM information_schema.statistics "
            "WHERE index_name = %s" % _q(name)
        )
    return PhaseCheck(sql=sql, kind="exists")


def _identifier_after(statement: str, prefix: str) -> str | None:
    upper = statement.upper()
    pos = upper.find(prefix.upper())
    if pos < 0:
        return None
    rest = statement[pos + len(prefix) :].strip()
    if not rest:
        return None
    if rest[0] in "\"`[":
        close = {"\"": "\"", "`": "`", "[": "]"}[rest[0]]
        end = rest.find(close, 1)
        if end < 0:
            return None
        return rest[1:end]
    token = rest.split(None, 1)[0]
    return token.split("(", 1)[0].rstrip(",") or None


# ---------------------------------------------------------------------------
# Capture during offline compilation
# ---------------------------------------------------------------------------


@dataclass
class _Event:
    kind: Literal["ddl", "version_create", "version", "ac_start", "ac_end"]
    construct: Any = None
    sql: str = ""


class _Buffer:
    """A write-only buffer used while compiling; SQL is also recorded."""

    def __init__(self) -> None:
        self.value = ""

    def write(self, text: str) -> int:
        self.value += text
        return len(text)

    def flush(self) -> None:
        pass

    def reset(self) -> None:
        self.value = ""


class _Capture:
    def __init__(self, ctx: MigrationContext) -> None:
        self.ctx = ctx
        self.pending: list[_Event] = []
        self.declared: list[PhaseCheck] = []
        self.buffer = _Buffer()

    def declare_check(self, check: PhaseCheck) -> None:
        self.declared.append(check)

    def take_declared(self) -> list[PhaseCheck]:
        checks = self.declared
        self.declared = []
        return checks

    def record(self, construct: Any, sql: str) -> None:
        sql = _clean_sql(sql)
        if isinstance(construct, schema.CreateTable) and getattr(
            construct.element, "name", None
        ) == self.ctx.version_table:
            self.pending.append(
                _Event("version_create", construct=construct, sql=sql)
            )
            return
        table = getattr(construct, "table", None)
        if table is None:
            element = getattr(construct, "element", None)
            table = getattr(element, "table", None)
            if table is None and isinstance(element, schema.Table):
                table = element
        if (
            table is not None
            and getattr(table, "name", None) == self.ctx.version_table
        ):
            self.pending.append(
                _Event("version", construct=construct, sql=sql)
            )
            return
        self.pending.append(
            _Event("ddl", construct=construct, sql=sql)
        )

    def marker(self, kind: Literal["ac_start", "ac_end"]) -> None:
        self.pending.append(_Event(kind))

    def take_events(self) -> list[_Event]:
        events = self.pending
        self.pending = []
        return events


@contextmanager
def _capture_context(
    ctx: MigrationContext,
) -> Iterator[_Capture]:
    capture = _Capture(ctx)
    impl = ctx.impl
    original_exec = impl._exec
    original_buffer = impl.output_buffer
    original_autocommit = ctx.autocommit_block
    had_instance_autocommit = "autocommit_block" in ctx.__dict__

    impl.output_buffer = capture.buffer  # type: ignore[assignment]
    had_staged_capture = hasattr(ctx, "_staged_capture")
    ctx._staged_capture = capture  # type: ignore[attr-defined]

    def exec_wrapper(*args: Any, **kw: Any) -> Any:
        capture.buffer.reset()
        construct = args[0] if args else kw.get("construct")
        result = original_exec(*args, **kw)
        sql = capture.buffer.value.strip()
        if sql:
            capture.record(construct, sql)
        return result

    setattr(impl, "_exec", exec_wrapper)

    @contextmanager
    def autocommit_wrapper() -> Iterator[None]:
        capture.marker("ac_start")
        with original_autocommit():
            yield
        capture.marker("ac_end")

    setattr(ctx, "autocommit_block", autocommit_wrapper)

    try:
        yield capture
    finally:
        setattr(impl, "_exec", original_exec)
        impl.output_buffer = original_buffer
        if not had_staged_capture:
            delattr(ctx, "_staged_capture")
        if had_instance_autocommit:
            setattr(ctx, "autocommit_block", original_autocommit)
        else:
            delattr(ctx, "autocommit_block")


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------


def compile_staged_artifact(
    migration_context: MigrationContext,
) -> StagedArtifact:
    """Compile an offline migration context into a staged artifact.

    The context must be configured with ``as_sql=True`` and have the
    ``op``/``context`` proxies installed, as when called from
    :meth:`.EnvironmentContext.run_migrations`.
    """

    ctx = migration_context
    if not ctx.as_sql:
        raise util.CommandError(
            "Staged artifacts may only be compiled in offline (--sql) mode"
        )

    heads = tuple(sorted(ctx.get_current_heads()))
    steps = list(ctx._migrations_fn(heads, ctx))  # type: ignore[misc]
    if len(steps) != len(set(step.revision.revision for step in steps)):
        raise util.CommandError("Duplicate migration steps are not supported")

    artifact = StagedArtifact(
        dialect=ctx.dialect.name,
        transactional_ddl=bool(ctx.impl.transactional_ddl),
        expected_start=heads,
        version_table=ctx.version_table,
        version_table_schema=ctx.version_table_schema,
    )

    revisions: list[list[Any]] = []

    def observer(
        phase: str, step: Any, heads: set[str]
    ) -> None:
        if phase == "start":
            revisions.append(
                [
                    step,
                    tuple(sorted(heads)),
                    tuple(sorted(heads)),
                    [],
                    [],
                ]
            )
        elif phase == "end":
            entry = revisions[-1]
            entry[2] = tuple(sorted(heads))
            entry[3].extend(capture.take_events())
            declared = capture.take_declared()
            if declared:
                entry[4].extend(declared)

    with _capture_context(ctx) as capture:
        old_observer = ctx.opts.get("_step_observer")
        ctx.opts["_step_observer"] = observer
        try:
            ctx.run_migrations()
        finally:
            if old_observer is None:
                ctx.opts.pop("_step_observer", None)
            else:
                ctx.opts["_step_observer"] = old_observer

    for step, heads_before, heads_after, events, declared in revisions:
        revision = StagedRevision(
            revision=str(step.revision.revision),
            direction="upgrade" if step.is_upgrade else "downgrade",
            source_revisions=tuple(step.from_revisions_no_deps),
            destination_revisions=tuple(step.to_revisions_no_deps),
            heads_before=heads_before,
            heads_after=heads_after,
            phases=_split_phases(
                ctx, events, heads_before, heads_after, declared
            ),
            doc=step.doc,
        )
        _validate_revision(revision)
        artifact.revisions.append(revision)

    return artifact


def _split_phases(
    ctx: MigrationContext,
    events: Sequence[_Event],
    heads_before: Sequence[str],
    heads_after: Sequence[str],
    declared: Sequence[PhaseCheck] = (),
) -> list[StagedPhase]:
    phases: list[StagedPhase] = []
    declared = list(declared)

    current: list[_Event] = []
    current_kind: PhaseKind = "transaction"
    after_autocommit = False

    def flush() -> None:
        nonlocal current, after_autocommit
        if not current:
            return
        statements: list[StagedStatement] = []
        version_seen = False
        for event in current:
            if event.kind == "version":
                version_seen = True
                statements.append(
                    StagedStatement(sql=event.sql, role="version")
                )
            elif event.kind == "version_create":
                statements.append(
                    StagedStatement(
                        sql=event.sql,
                        role="setup",
                        checks=_version_create_checks(ctx),
                    )
                )
            else:
                if declared:
                    checks = [declared.pop(0)]
                elif event.construct is not None:
                    checks = _checks_for_construct(
                        ctx, event.construct, event.sql
                    )
                else:
                    checks = _raw_sql_checks(ctx, event.sql)
                statements.append(
                    StagedStatement(
                        sql=event.sql, role="ddl", checks=checks
                    )
                )

        if declared:
            # declared checks that were not consumed apply to the phase
            for statement in statements:
                if statement.role == "ddl" and not statement.checks:
                    statement.checks = [declared.pop(0)]
                    break

        has_ddl = any(s.role == "ddl" for s in statements)
        has_setup = any(s.role == "setup" for s in statements)
        if version_seen and not has_ddl and not has_setup:
            kind: PhaseKind = "commit"
        elif after_autocommit:
            kind = "transaction"
        else:
            kind = current_kind

        version_check = None
        if version_seen:
            version_check = _version_check(
                ctx.dialect.name,
                ctx.version_table,
                ctx.version_table_schema,
                heads_before,
                heads_after,
            )

        phases.append(
            StagedPhase(
                index=len(phases),
                kind=kind,
                statements=statements,
                version_check=version_check,
                label=_phase_label(statements, kind),
            )
        )
        current = []
        after_autocommit = False

    for event in events:
        if event.kind == "ac_start":
            flush()
            current_kind = "autocommit"
        elif event.kind == "ac_end":
            flush()
            current_kind = "transaction"
            after_autocommit = True
        else:
            current.append(event)

    flush()
    return phases


def _phase_label(
    statements: Sequence[StagedStatement], kind: PhaseKind
) -> str:
    if kind == "commit":
        return "version"
    labels = []
    for statement in statements:
        if statement.role != "ddl":
            continue
        labels.append(_describe_sql(statement.sql))
    return ", ".join(label for label in labels if label)


def _describe_sql(sql: str) -> str:
    normalized = _normalize(sql)
    upper = normalized.upper()
    if upper.startswith("CREATE UNIQUE INDEX CONCURRENTLY "):
        name = _identifier_after(
            normalized, "CREATE UNIQUE INDEX CONCURRENTLY "
        )
        return "create unique index concurrently %s" % name if name else ""
    if upper.startswith("CREATE INDEX CONCURRENTLY "):
        name = _identifier_after(normalized, "CREATE INDEX CONCURRENTLY ")
        return "create index concurrently %s" % name if name else ""
    for prefix, word in (
        ("CREATE TABLE ", "create table"),
        ("DROP TABLE ", "drop table"),
        ("CREATE UNIQUE INDEX ", "create unique index"),
        ("CREATE INDEX ", "create index"),
        ("DROP INDEX ", "drop index"),
        ("ALTER TABLE ", "alter table"),
    ):
        if upper.startswith(prefix):
            name = _identifier_after(normalized, prefix)
            return "%s %s" % (word, name) if name else word
    return ""


def _validate_revision(revision: StagedRevision) -> None:
    for phase in revision.phases:
        for statement in phase.ddl_statements:
            if not statement.checks:
                raise util.CommandError(
                    "Cannot build a recoverable staged artifact for "
                    "revision %s %s: no read-only check could be derived "
                    "from %r. Declare the operation with a supported "
                    "Alembic directive or use a checkable DDL statement."
                    % (revision.revision, phase.name, statement.sql)
                )
        if phase.version_check is not None and phase.ddl_statements:
            for statement in phase.ddl_statements:
                if not statement.checks:
                    raise util.CommandError(
                        "Revision %s %s advances the version without "
                        "effect checks" % (revision.revision, phase.name)
                    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class PhaseResult:
    def __init__(
        self,
        revision: str,
        phase: StagedPhase,
        state: Literal["applied", "not_applied", "conflict", "executed"],
        detail: str | None = None,
    ) -> None:
        self.revision = revision
        self.phase = phase
        self.state = state
        self.detail = detail

    def to_json(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "phase": self.phase.index,
            "kind": self.phase.kind,
            "label": self.phase.label,
            "state": self.state,
            "detail": self.detail,
        }

    def __str__(self) -> str:
        detail = " (%s)" % self.detail if self.detail else ""
        return "%s %s: %s%s" % (
            self.revision,
            self.phase.name,
            self.state,
            detail,
        )


class StagedRunReport:
    def __init__(self) -> None:
        self.results: list[PhaseResult] = []
        self.stopped_after: int | None = None

    def add(self, result: PhaseResult) -> None:
        self.results.append(result)

    @property
    def stopped(self) -> bool:
        return self.stopped_after is not None

    def to_json(self) -> dict[str, Any]:
        return {
            "stopped": self.stopped,
            "stopped_after": self.stopped_after,
            "phases": [r.to_json() for r in self.results],
        }

    def dumps(self) -> str:
        return json.dumps(self.to_json(), indent=2) + "\n"


def _read_heads(
    conn: Connection, artifact: StagedArtifact
) -> tuple[str, ...] | None:
    if not sqla_compat._connectable_has_table(
        conn, artifact.version_table, artifact.version_table_schema
    ):
        return None
    result = conn.exec_driver_sql(
        "SELECT version_num FROM %s"
        % _qualified_table(
            artifact.version_table, artifact.version_table_schema
        )
    )
    return tuple(sorted(row[0] for row in result))


def _evaluate_check(
    conn: Connection, check: PhaseCheck
) -> tuple[PhaseState, str | None]:
    try:
        value = conn.exec_driver_sql(check.sql).scalar()
    except DBAPIError:
        # the catalog object (including the version table) is absent
        return "not_applied", "catalog object not present"
    return check.classify(value)


def _evaluate_phase(
    conn: Connection, phase: StagedPhase
) -> tuple[Any, str | None]:
    """Classify a phase.

    Returns ``(state, detail)`` where state is one of ``"applied"``,
    ``"not_applied"``, ``"partial"`` (DDL is present but the version has not
    advanced) or ``"conflict"``.
    """

    details: list[str] = []
    saw_conflict = False
    applied_checks = 0
    total_checks = len(phase.ddl_checks)

    for check in phase.ddl_checks:
        state, detail = _evaluate_check(conn, check)
        if state == "conflict":
            saw_conflict = True
            details.append(detail or "conflict")
        elif state == "applied":
            applied_checks += 1
        else:
            details.append(detail or "effect not present")

    version_state: PhaseState | None = None
    if phase.version_check is not None:
        version_state, detail = _evaluate_check(
            conn, phase.version_check
        )
        if version_state == "conflict":
            saw_conflict = True
            details.append(detail or "version conflict")
        elif version_state == "not_applied":
            details.append("version not written")

    if saw_conflict:
        return "conflict", "; ".join(details)
    if total_checks == 0:
        if version_state == "applied":
            return "applied", None
        return "not_applied", "; ".join(details) or "version not written"
    if applied_checks == total_checks:
        if version_state == "not_applied":
            return "partial", "effect present, version not written"
        return "applied", None
    if applied_checks == 0:
        return "not_applied", "; ".join(details) or "effect not present"
    return "conflict", "partially applied phase: " + "; ".join(details)


def describe_staged_artifact(
    artifact: StagedArtifact, engine: Engine
) -> StagedRunReport:
    """Report each phase's current state without writing anything."""

    report = StagedRunReport()
    with engine.connect() as conn:
        for revision in artifact.revisions:
            for phase in revision.phases:
                state, detail = _evaluate_phase(conn, phase)
                report.add(
                    PhaseResult(revision.revision, phase, state, detail)
                )
    return report


def _allowed_states(artifact: StagedArtifact) -> set[tuple[str, ...]]:
    states = {tuple(sorted(artifact.expected_start))}
    for revision in artifact.revisions:
        states.add(tuple(sorted(revision.heads_before)))
        states.add(tuple(sorted(revision.heads_after)))
    return states


def _resolve_start_index(
    artifact: StagedArtifact, actual: tuple[str, ...]
) -> int:
    expected_start = tuple(sorted(artifact.expected_start))
    if actual == expected_start:
        return 0
    start_index = 0
    for i, revision in enumerate(artifact.revisions):
        if tuple(sorted(revision.heads_after)) == actual:
            start_index = i + 1
    return start_index


def _execute_sqlite_transaction(
    conn: Connection, statements: Sequence[str]
) -> None:
    if conn.in_transaction():
        conn.rollback()
    conn.exec_driver_sql("BEGIN")
    try:
        for statement in statements:
            conn.exec_driver_sql(statement)
        conn.exec_driver_sql("COMMIT")
    except Exception:
        conn.exec_driver_sql("ROLLBACK")
        raise


def _execute_transaction(
    conn: Connection, statements: Sequence[str], dialect: str
) -> None:
    if dialect == "sqlite":
        _execute_sqlite_transaction(conn, statements)
    else:
        with conn.begin():
            for statement in statements:
                conn.exec_driver_sql(statement)


def _execute_autocommit(
    engine: Engine, statements: Sequence[str]
) -> None:
    with engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        for statement in statements:
            conn.exec_driver_sql(statement)


def _ddl_checks_pass(
    conn: Connection, revision: StagedRevision, phase: StagedPhase
) -> None:
    if not phase.ddl_checks:
        return
    for check in phase.ddl_checks:
        state, detail = _evaluate_check(conn, check)
        if state != "applied":
            raise StagedConflictError(
                "Revision %s %s: effect checks failed before version "
                "advancement: %s"
                % (revision.revision, phase.name, detail or state)
            )


def _write_statements_for(
    conn: Connection, artifact: StagedArtifact, phase: StagedPhase
) -> list[str]:
    statements: list[str] = []
    version_table_present = sqla_compat._connectable_has_table(
        conn, artifact.version_table, artifact.version_table_schema
    )
    if not version_table_present:
        statements.extend(s.sql for s in phase.setup_statements)
    statements.extend(s.sql for s in phase.ddl_statements)
    return statements


def _execute_transaction_phase(
    engine: Engine,
    artifact: StagedArtifact,
    revision: StagedRevision,
    phase: StagedPhase,
) -> None:
    if artifact.dialect == "sqlite":
        with engine.connect() as conn:
            if conn.in_transaction():
                conn.rollback()
            conn.exec_driver_sql("BEGIN")
            try:
                for sql in _write_statements_for(conn, artifact, phase):
                    conn.exec_driver_sql(sql)
                _ddl_checks_pass(conn, revision, phase)
                for statement in phase.version_statements:
                    conn.exec_driver_sql(statement.sql)
                conn.exec_driver_sql("COMMIT")
            except Exception:
                conn.exec_driver_sql("ROLLBACK")
                raise
    else:
        with engine.begin() as conn:
            for sql in _write_statements_for(conn, artifact, phase):
                conn.exec_driver_sql(sql)
            _ddl_checks_pass(conn, revision, phase)
            for statement in phase.version_statements:
                conn.exec_driver_sql(statement.sql)


def run_staged_artifact(
    artifact: StagedArtifact,
    engine: Engine,
    stop_after: int | None = None,
) -> StagedRunReport:
    """Replay *artifact* against the database behind *engine*.

    All checks for a revision are evaluated before that revision's first
    write.  Applied phases are skipped.  A phase whose DDL is confirmed but
    whose version was not written is completed only when the backend cannot
    make DDL transactional; otherwise it is a conflict.  Conflicting state
    aborts before any write and names the offending phase.

    ``stop_after`` limits the number of phases *executed* (skipped phases
    do not count) and then returns, simulating a process stop at a phase
    boundary.
    """

    report = StagedRunReport()
    executed = 0

    with engine.connect() as conn:
        actual = tuple(sorted(_read_heads(conn, artifact) or ()))
    allowed = _allowed_states(artifact)
    if actual not in allowed:
        raise StagedConflictError(
            "Database version %r is not the artifact start %r or any "
            "declared revision boundary; refusing to write."
            % (actual, tuple(sorted(artifact.expected_start)))
        )

    start_index = _resolve_start_index(artifact, actual)

    if start_index >= len(artifact.revisions):
        # the database is already at the artifact's final boundary; report
        # every phase as applied without evaluating or writing
        for revision in artifact.revisions:
            for phase in revision.phases:
                report.add(
                    PhaseResult(revision.revision, phase, "applied")
                )
        return report

    for revision in artifact.revisions[start_index:]:
        with engine.connect() as conn:
            states = [_evaluate_phase(conn, p) for p in revision.phases]

        for phase, (state, detail) in zip(revision.phases, states):
            if state == "conflict":
                raise StagedConflictError(
                    "Conflict at revision %s %s: %s"
                    % (revision.revision, phase.name, detail)
                )

        for phase, (state, detail) in zip(revision.phases, states):
            if state == "applied":
                report.add(
                    PhaseResult(revision.revision, phase, "applied")
                )
                continue

            if stop_after is not None and executed >= stop_after:
                report.stopped_after = executed
                return report

            if state == "partial":
                # DDL is confirmed present but progress was not written.
                if artifact.transactional_ddl:
                    raise StagedConflictError(
                        "Conflict at revision %s %s: DDL is present but the "
                        "version was not written; a transactional phase "
                        "cannot leave partial progress."
                        % (revision.revision, phase.name)
                    )
                _execute_version_only(engine, artifact, phase)
            elif phase.kind == "autocommit":
                _execute_autocommit(engine, phase.sql)
            else:
                _execute_transaction_phase(
                    engine, artifact, revision, phase
                )

            with engine.connect() as conn:
                after_state, after_detail = _evaluate_phase(conn, phase)
            if after_state != "applied":
                raise StagedConflictError(
                    "After executing revision %s %s, its checks report %r: "
                    "%s"
                    % (
                        revision.revision,
                        phase.name,
                        after_state,
                        after_detail or "",
                    )
                )

            report.add(
                PhaseResult(
                    revision.revision, phase, "executed", after_detail
                )
            )
            executed += 1
            if stop_after is not None and executed >= stop_after:
                report.stopped_after = executed
                return report

        with engine.connect() as conn:
            final = tuple(sorted(_read_heads(conn, artifact) or ()))
        expected = tuple(sorted(revision.heads_after))
        if final != expected:
            raise StagedConflictError(
                "Revision %s completed but database version is %r, "
                "expected %r"
                % (revision.revision, final, expected)
            )

    return report


def _execute_version_only(
    engine: Engine, artifact: StagedArtifact, phase: StagedPhase
) -> None:
    with engine.connect() as conn:
        # re-confirm the effect checks immediately before advancing the
        # version, so that progress never runs ahead of the database state
        for check in phase.ddl_checks:
            state, detail = _evaluate_check(conn, check)
            if state != "applied":
                raise StagedConflictError(
                    "Effect check failed before completing progress: %s"
                    % (detail or state)
                )
        statements: list[str] = []
        version_table_present = sqla_compat._connectable_has_table(
            conn, artifact.version_table, artifact.version_table_schema
        )
        if not version_table_present:
            statements.extend(s.sql for s in phase.setup_statements)
        statements.extend(s.sql for s in phase.version_statements)
        if not statements:
            return
        _execute_transaction(conn, statements, artifact.dialect)
