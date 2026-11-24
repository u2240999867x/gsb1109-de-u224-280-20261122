import asyncio
import contextvars
import logging
import threading
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse
from weakref import WeakSet

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Connection, Engine, create_engine, event, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import Executable

from dataset.table import Table
from dataset.types import ColumnType, Types
from dataset.util import (
    QUERY_STEP,
    ResultIter,
    ResultLease,
    RowFactory,
    normalize_table_name,
    row_factory,
    safe_url,
)

log = logging.getLogger(__name__)


class _TransactionScope:
    """Transaction and connection state owned by one task or thread."""

    __slots__ = (
        "owner_task",
        "thread_id",
        "connection",
        "transactions",
        "lease",
        "__weakref__",
    )

    def __init__(
        self,
        thread_id: int,
        owner_task: asyncio.Task[Any] | None,
    ) -> None:
        self.thread_id = thread_id
        self.owner_task = owner_task
        self.connection: Connection | None = None
        self.transactions: list[Any] = []
        self.lease = ResultLease()


class Database:
    """A database object represents a SQL database with multiple tables."""

    def __init__(
        self,
        url: str,
        schema: str | None = None,
        engine_kwargs: dict[str, Any] | None = None,
        ensure_schema: bool = True,
        row_type: RowFactory = row_factory,
        sqlite_wal_mode: bool = True,
        on_connect_statements: list[str] | None = None,
    ) -> None:
        """Configure and connect to the database."""
        if engine_kwargs is None:
            engine_kwargs = {}

        parsed_url = urlparse(url)
        # if parsed_url.scheme.lower() in 'sqlite':
        #     # ref: https://github.com/pudo/dataset/issues/163
        #     if 'poolclass' not in engine_kwargs:
        #         engine_kwargs['poolclass'] = StaticPool

        self.lock = threading.RLock()
        self.local = threading.local()
        self.connections: dict[int, Connection] = {}
        self._scope_var: contextvars.ContextVar[_TransactionScope | None] = (
            contextvars.ContextVar(f"dataset_database_{id(self)}_scope", default=None)
        )
        self._scopes: WeakSet[_TransactionScope] = WeakSet()

        if len(parsed_url.query):
            query = parse_qs(parsed_url.query)
            if schema is None:
                schema_qs = query.get("schema", query.get("searchpath", []))
                if len(schema_qs):
                    schema = schema_qs.pop()

        self.schema = schema
        self.engine: Engine | None = create_engine(url, **engine_kwargs)
        assert self.engine is not None
        self.is_postgres = self.engine.dialect.name == "postgresql"
        self.is_sqlite = self.engine.dialect.name == "sqlite"
        self.is_mysql = "mysql" in self.engine.dialect.name
        if on_connect_statements is None:
            on_connect_statements = []

        def _run_on_connect(dbapi_con: Any, con_record: Any) -> None:
            # reference:
            # https://stackoverflow.com/questions/9671490/how-to-set-sqlite-pragma-statements-with-sqlalchemy
            # https://stackoverflow.com/a/7831210/1890086
            for statement in on_connect_statements:
                dbapi_con.execute(statement)

        if self.is_sqlite and parsed_url.path != "" and sqlite_wal_mode:
            # we only enable WAL mode for sqlite databases that are not in-memory
            on_connect_statements.append("PRAGMA journal_mode=WAL")

        if len(on_connect_statements):
            event.listen(self.engine, "connect", _run_on_connect)

        self.types = Types(is_postgres=self.is_postgres)
        self.url = url
        self.row_type: RowFactory = row_type
        self.ensure_schema = ensure_schema
        self._tables: dict[str, Table] = {}

    def _current_task(self) -> asyncio.Task[Any] | None:
        try:
            return asyncio.current_task()
        except RuntimeError:
            return None

    def _scope(self) -> _TransactionScope:
        """Return the scope shared by callers in the same task or thread."""
        thread_id = threading.get_ident()
        owner_task = self._current_task()
        scope = self._scope_var.get()

        if scope is not None and scope.thread_id == thread_id:
            if scope.owner_task is owner_task:
                pass
            elif scope.owner_task is None:
                # A synchronous setup scope can be adopted by the event-loop
                # task that starts running in this same call context.
                scope.owner_task = owner_task
            elif scope.owner_task.done() and not scope.transactions:
                self._release_connection(scope)
                scope.owner_task = owner_task
            else:
                if not scope.transactions:
                    # The parent context may still own an idle autocommit
                    # connection. Return it before this new task checks one out;
                    # an active transaction must remain open and isolated.
                    self._release_connection(scope)
                scope = None

        if scope is None or scope.thread_id != thread_id:
            scope = _TransactionScope(thread_id, owner_task)
            self._scope_var.set(scope)

        self._scopes.add(scope)
        return scope

    @property
    def executable(self) -> Connection:
        """Connection against which statements will be executed."""
        scope = self._scope()
        connection = scope.connection
        if connection is None:
            if self.engine is None:
                raise RuntimeError("Database is closed")
            # Check out outside the database lock: with a tiny pool, another
            # thread may need that lock to release its connection first.
            connection = self.engine.connect()
            with self.lock:
                scope.connection = connection
                self.connections[id(scope)] = connection
        return connection

    @property
    def op(self) -> Operations:
        """Get an alembic operations context."""
        ctx = MigrationContext.configure(self.executable)
        return Operations(ctx)

    @property
    def inspect(self) -> Inspector:
        """Get a SQLAlchemy inspector."""
        return inspect(self.executable)

    def has_table(self, name: str) -> bool:
        return self.inspect.has_table(name, schema=self.schema)

    @property
    def metadata(self) -> MetaData:
        """Return a SQLAlchemy schema cache object."""
        return MetaData(schema=self.schema)

    @property
    def in_transaction(self) -> bool:
        """Check if this database is in a transactional context."""
        return bool(self._scope().transactions)

    def _detach_result_lease(self, scope: _TransactionScope) -> ResultLease:
        lease = scope.lease
        scope.lease = ResultLease()
        lease.end()
        return lease

    def _release_connection(self, scope: _TransactionScope | None = None) -> None:
        """Close and release a scope's connection back to the pool."""
        if scope is None:
            scope = self._scope()

        lease = scope.lease
        scope.lease = ResultLease()
        try:
            lease.end()
        finally:
            with self.lock:
                connection = scope.connection
                scope.connection = None
                self.connections.pop(id(scope), None)

            if connection is not None:
                connection.close()

    def _flush_tables(self) -> None:
        """Clear the table metadata after transaction rollbacks."""
        for table in self._tables.values():
            table._table = None

    def _auto_commit(self) -> None:
        """Commit pending changes when not in an explicit transaction.

        In SQLAlchemy 2.x, connections use "autobegin" which starts a
        transaction on first use. This method commits that transaction
        after each write operation when the user has not started an
        explicit transaction via ``begin()``/``with db:``.
        """
        scope = self._scope()
        if not scope.transactions:
            self.executable.commit()
            if not scope.lease.has_active_results():
                self._release_connection(scope)

    def begin(self) -> None:
        """Enter a transaction explicitly.

        No data will be written until the transaction has been committed.
        Nested calls use SQL savepoints so an inner failure can be rolled
        back without discarding the caller's transaction.
        """
        scope = self._scope()
        connection = self.executable
        if scope.transactions:
            scope.transactions.append(connection.begin_nested())
            return

        if not connection.in_transaction():
            connection.begin()
        scope.transactions.append(True)

    def commit(self) -> None:
        """Commit the current transaction.

        Make all statements executed since the transaction was begun permanent.
        """
        scope = self._scope()
        if not scope.transactions:
            return

        transaction = scope.transactions.pop()
        if scope.transactions:
            transaction.commit()
            return

        try:
            if scope.connection is not None:
                self._detach_result_lease(scope)
                scope.connection.commit()
        finally:
            self._release_connection(scope)

    def rollback(self) -> None:
        """Roll back the current transaction.

        Discard all statements executed since the transaction was begun.
        """
        scope = self._scope()
        if not scope.transactions:
            return

        transaction = scope.transactions.pop()
        is_outermost = not scope.transactions
        try:
            if is_outermost:
                if scope.connection is not None:
                    self._detach_result_lease(scope)
                    scope.connection.rollback()
            else:
                transaction.rollback()
        finally:
            if is_outermost:
                self._release_connection(scope)
            self._flush_tables()

    def __enter__(self) -> "Database":
        """Start a transaction."""
        self.begin()
        return self

    def __exit__(
        self, error_type: object, error_value: object, traceback: object
    ) -> None:
        """End a transaction by committing or rolling back."""
        if error_type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                raise
        else:
            self.rollback()

    def close(self) -> None:
        """Close all database connections and dispose of the engine.

        Releases all pooled connections and makes this object unusable.
        This should be called when the database is no longer needed,
        especially in multi-threaded or connection-pooled setups.
        """
        scopes = list(self._scopes)
        for scope in scopes:
            scope.lease.end()

        with self.lock:
            for conn in self.connections.values():
                conn.close()
            self.connections.clear()
            for scope in scopes:
                scope.connection = None

        if self.engine is not None:
            self.engine.dispose()
        self._tables = {}
        self.engine = None

    @property
    def tables(self) -> list[str]:
        """Get a listing of all tables that exist in the database."""
        return self.inspect.get_table_names(schema=self.schema)

    @property
    def views(self) -> list[str]:
        """Get a listing of all views that exist in the database."""
        return self.inspect.get_view_names(schema=self.schema)

    def __contains__(self, table_name: str) -> bool:
        """Check if the given table name exists in the database."""
        try:
            table_name = normalize_table_name(table_name)
            if table_name in self.tables:
                return True
            return table_name in self.views
        except ValueError:
            return False

    def create_table(
        self,
        table_name: str,
        primary_id: str | Literal[False] | None = None,
        primary_type: ColumnType | None = None,
        primary_increment: bool | None = None,
    ) -> Table:
        """Create a new table.

        Either loads a table or creates it if it doesn't exist yet. You can
        define the name and type of the primary key field, if a new table is to
        be created. The default is to create an auto-incrementing integer,
        ``id``. You can also set the primary key to be a string or big integer.
        The caller will be responsible for the uniqueness of ``primary_id`` if
        it is defined as a text type. You can disable auto-increment behaviour
        for numeric primary keys by setting `primary_increment` to `False`.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.create_table('population')

            # custom id and type
            table2 = db.create_table('population2', 'age')
            table3 = db.create_table('population3',
                                     primary_id='city',
                                     primary_type=db.types.text)
            # custom length of String
            table4 = db.create_table('population4',
                                     primary_id='city',
                                     primary_type=db.types.string(25))
            # no primary key
            table5 = db.create_table('population5',
                                     primary_id=False)
        """
        assert not isinstance(primary_type, str), (
            "Text-based primary_type support is dropped, use db.types."
        )
        table_name = normalize_table_name(table_name)
        with self.lock:
            if table_name not in self._tables:
                self._tables[table_name] = Table(
                    self,
                    table_name,
                    primary_id=primary_id,
                    primary_type=primary_type,
                    primary_increment=primary_increment,
                    auto_create=True,
                )
            return self._tables[table_name]

    def load_table(self, table_name: str) -> Table:
        """Load a table.

        This will fail if the tables does not already exist in the database. If
        the table exists, its columns will be reflected and are available on
        the :py:class:`Table <dataset.Table>` object.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.load_table('population')
        """
        table_name = normalize_table_name(table_name)
        with self.lock:
            if table_name not in self._tables:
                self._tables[table_name] = Table(self, table_name)
            return self._tables[table_name]

    def get_table(
        self,
        table_name: str,
        primary_id: str | Literal[False] | None = None,
        primary_type: ColumnType | None = None,
        primary_increment: bool | None = None,
    ) -> Table:
        """Load or create a table.

        This is now the same as ``create_table``.
        ::

            table = db.get_table('population')
            # you can also use the short-hand syntax:
            table = db['population']
        """
        if not self.ensure_schema:
            return self.load_table(table_name)
        return self.create_table(
            table_name, primary_id, primary_type, primary_increment
        )

    def __getitem__(self, table_name: str) -> Table:
        """Get a given table."""
        return self.get_table(table_name)

    def _ipython_key_completions_(self) -> list[str]:
        """Completion for table names with IPython."""
        return self.tables

    def query(self, query: str | Executable, **kwargs: Any) -> ResultIter:
        """Run a statement on the database directly.

        Allows for the execution of arbitrary read/write queries. A query can
        either be a plain text string, or a `SQLAlchemy expression
        <https://docs.sqlalchemy.org/en/21/tutorial/data_select.html#tutorial-selecting-data>`_.
        If a plain string is passed in, it will be converted to an expression
        automatically.

        Keyword arguments will be used for parameter binding. Use a named bind
        parameter in the query (i.e. ``SELECT * FROM tbl WHERE a = :foo``) and
        pass the value as a keyword argument (i.e. ``foo='bar'``).
        ::

            statement = 'SELECT user, COUNT(*) c FROM photos GROUP BY user'
            for row in db.query(statement):
                print(row['user'], row['c'])

        The returned iterator will yield each result sequentially.
        """
        if isinstance(query, str):
            query = text(query)
        _step = kwargs.pop("_step", QUERY_STEP)
        if _step is False or _step == 0:
            _step = None
        scope = self._scope()
        if kwargs:
            rp = self.executable.execute(query, kwargs)
        else:
            rp = self.executable.execute(query)
        return ResultIter(
            rp,
            row_type=self.row_type,
            step=_step,
            lease=scope.lease,
        )

    def __repr__(self) -> str:
        """Text representation contains the URL."""
        return f"<Database({safe_url(self.url)})>"
