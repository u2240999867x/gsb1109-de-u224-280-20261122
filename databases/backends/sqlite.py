import asyncio
import logging
import sqlite3
import typing
import uuid
from urllib.parse import urlencode

import aiosqlite
from sqlalchemy.dialects.sqlite import pysqlite
from sqlalchemy.engine.cursor import CursorResultMetaData
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement

from databases.backends.common.records import Record, Row, create_column_maps
from databases.core import LOG_EXTRA, DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


class SQLiteBackend(DatabaseBackend):
    def __init__(
        self, database_url: typing.Union[DatabaseURL, str], **options: typing.Any
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = pysqlite.dialect(paramstyle="qmark")
        # aiosqlite does not support decimals
        self._dialect.supports_native_decimal = False
        self._pool = SQLitePool(self._database_url, **self._options)

    async def connect(self) -> None:
        await self._pool.connect()

    async def disconnect(self) -> None:
        await self._pool.disconnect()

    def prepare_disconnect(self) -> None:
        self._pool.prepare_disconnect()

    def connection(self) -> "SQLiteConnection":
        return SQLiteConnection(self._pool, self._dialect)


class SQLiteSession:
    """
    A single connect() .. disconnect() lifetime of the SQLite pool.

    Shared-cache in-memory databases only exist while at least one SQLite
    connection to them is open.  A session therefore owns an optional keeper
    connection, and tracks how many connections have been handed out so that
    shutdown can wait until all of them have actually been closed.

    Bookkeeping is deliberately synchronous: it must stay correct even when
    the task releasing a connection is cancelled.
    """

    def __init__(self, keeper: typing.Optional[sqlite3.Connection]) -> None:
        self.keeper = keeper
        self.active = 0
        self.closing = False
        self._idle = asyncio.Event()
        self._idle.set()

    def enter(self) -> None:
        self.active += 1
        self._idle.clear()

    def leave(self) -> None:
        self.active -= 1
        if self.active == 0:
            self._idle.set()

    async def wait_for_borrowers(self) -> None:
        await self._idle.wait()


class SQLitePool:
    def __init__(self, url: DatabaseURL, **options: typing.Any) -> None:
        self._database = url.database
        self._memref: typing.Optional[sqlite3.Connection] = None
        # add query params to database connection string
        if url.options:
            self._database += "?" + urlencode(url.options)
        self._options = options

        # Only in-memory shared-cache databases need a keeper connection.
        # Ordinary file databases (including file-backed shared caches) must
        # not leave an additional connection open.
        mode = url.options.get("mode", "").lower()
        cache = url.options.get("cache", "").lower()
        self._is_shared_memory = cache == "shared" and (
            mode == "memory"
            or url.database == ":memory:"
            or url.database == "file::memory:"
        )

        self._session: typing.Optional[SQLiteSession] = None
        # "idle", "open", "closing" or "closed".  A closed pool refuses new
        # borrows until connect() starts a fresh session.
        self._state = "idle"
        # Set when shutdown is requested while startup is still in flight, so
        # the freshly created session already refuses new borrows.
        self._close_requested = False

    async def connect(self) -> None:
        # If a previous shutdown did not manage to finish (for example a
        # disconnect task reported an error), make sure its resources are
        # fully drained before starting a new session.
        if self._session is not None:
            await self.disconnect()

        close_requested = self._close_requested
        self._close_requested = False

        keeper = None
        if self._is_shared_memory:
            keeper_options = dict(self._options)
            keeper_options.setdefault("check_same_thread", False)
            try:
                keeper = sqlite3.connect(self._database, **keeper_options)
            except BaseException:
                self._state = "closed"
                raise

        session = SQLiteSession(keeper)
        if close_requested:
            session.closing = True
        self._memref = keeper
        self._session = session
        self._state = "closing" if close_requested else "open"

    def prepare_disconnect(self) -> None:
        if self._session is not None:
            # Runs synchronously inside Database.disconnect(), so a borrow
            # that is attempted before the shared shutdown task is scheduled
            # is already refused.
            self._session.closing = True
        else:
            # Startup may still be in flight; make sure the session it is
            # about to create rejects borrows as well.
            self._close_requested = True
        self._state = "closing"

    async def disconnect(self) -> None:
        session = self._session
        if session is None:
            self._state = "closed"
            # A shutdown request that raced a failed startup must not make
            # the next session start out already closing.
            self._close_requested = False
            return

        # Reject new borrows immediately, then let operations that have
        # already been borrowed run to completion.
        session.closing = True
        self._state = "closing"
        await session.wait_for_borrowers()

        try:
            if session.keeper is not None:
                session.keeper.close()
        finally:
            # Whatever happened, this session is over.  A later connect()
            # must never observe (or resurrect) its resources.
            if self._memref is session.keeper:
                self._memref = None
            if self._session is session:
                self._session = None
            self._close_requested = False
            self._state = "closed"

    async def acquire(self) -> aiosqlite.Connection:
        session = self._session
        if session is not None:
            if session.closing:
                raise RuntimeError("SQLite database session is closing")
            # The increment and the closing check happen without yielding,
            # so shutdown cannot slip in between them.
            session.enter()
            try:
                connection = await self._open()
            except BaseException:
                session.leave()
                raise
            connection._databases_session = session  # type: ignore[attr-defined]
            return connection

        if self._state in ("closing", "closed"):
            raise RuntimeError("SQLite database session is closing")

        # Lazy use without an explicit connect() keeps working for regular
        # databases, but has no session lifetime to manage.
        return await self._open()

    async def release(self, connection: aiosqlite.Connection) -> None:
        session: typing.Optional[SQLiteSession] = getattr(
            connection, "_databases_session", None
        )
        if session is None:
            await connection.__aexit__(None, None, None)
            return

        close_task = asyncio.ensure_future(connection.__aexit__(None, None, None))

        def closed(task: "asyncio.Future") -> None:
            # Retrieve any exception so it is not reported as unhandled,
            # then stop counting this connection against shutdown.
            try:
                task.exception()
            except asyncio.CancelledError:
                pass
            session.leave()

        close_task.add_done_callback(closed)

        # A cancelled borrower must not leave a half-closed connection
        # behind.  Closing continues in the background; shutdown only
        # proceeds once the connection is really gone.
        await asyncio.shield(close_task)

    async def _open(self) -> aiosqlite.Connection:
        connection = aiosqlite.connect(
            database=self._database, isolation_level=None, **self._options
        )
        await connection.__aenter__()
        return connection


class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


class SQLiteConnection(ConnectionBackend):
    def __init__(self, pool: SQLitePool, dialect: Dialect):
        self._pool = pool
        self._dialect = dialect
        self._connection: typing.Optional[aiosqlite.Connection] = None

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        self._connection = await self._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        connection = self._connection
        try:
            await self._pool.release(connection)
        finally:
            # Even a cancelled release must not leave this backend thinking
            # the (now closing) connection is still owned by the caller.
            self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect

        async with self._connection.execute(query_str, args) as cursor:
            rows = await cursor.fetchall()
            metadata = CursorResultMetaData(context, cursor.description)
            rows = [
                Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    row,
                )
                for row in rows
            ]
            return [Record(row, result_columns, dialect, column_maps) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[Record]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect

        async with self._connection.execute(query_str, args) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            metadata = CursorResultMetaData(context, cursor.description)
            row = Row(
                metadata,
                metadata._processors,
                metadata._keymap,
                row,
            )
            return Record(row, result_columns, dialect, column_maps)

    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        async with self._connection.cursor() as cursor:
            await cursor.execute(query_str, args)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        for single_query in queries:
            await self.execute(single_query)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect

        async with self._connection.execute(query_str, args) as cursor:
            metadata = CursorResultMetaData(context, cursor.description)
            async for row in cursor:
                record = Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    row,
                )
                yield Record(record, result_columns, dialect, column_maps)

    def transaction(self) -> TransactionBackend:
        return SQLiteTransaction(self)

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(
            dialect=self._dialect, compile_kwargs={"render_postcompile": True}
        )
        execution_context = self._dialect.execution_ctx_cls()
        execution_context.dialect = self._dialect

        args = []
        result_map = None

        if not isinstance(query, DDLElement):
            compiled_params = sorted(compiled.params.items())

            params = compiled.construct_params()
            for key in compiled.positiontup:
                raw_val = params[key]
                if key in compiled._bind_processors:
                    val = compiled._bind_processors[key](raw_val)
                else:
                    val = raw_val
                args.append(val)

            execution_context.result_column_struct = (
                compiled._result_columns,
                compiled._ordered_columns,
                compiled._textual_ordered_columns,
                compiled._ad_hoc_textual,
                compiled._loose_column_name_matching,
            )

            mapping = {
                key: "$" + str(i) for i, (key, _) in enumerate(compiled_params, start=1)
            }
            compiled_query = compiled.string % mapping
            result_map = compiled._result_columns

        else:
            compiled_query = compiled.string

        query_message = compiled_query.replace(" \n", " ").replace("\n", " ")
        logger.debug(
            "Query: %s Args: %s", query_message, repr(tuple(args)), extra=LOG_EXTRA
        )
        return compiled.string, args, result_map, CompilationContext(execution_context)

    @property
    def raw_connection(self) -> aiosqlite.core.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


class SQLiteTransaction(TransactionBackend):
    def __init__(self, connection: SQLiteConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        if self._is_root:
            async with self._connection._connection.execute("BEGIN") as cursor:
                await cursor.close()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            async with self._connection._connection.execute(
                f"SAVEPOINT {self._savepoint_name}"
            ) as cursor:
                await cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            async with self._connection._connection.execute("COMMIT") as cursor:
                await cursor.close()
        else:
            async with self._connection._connection.execute(
                f"RELEASE SAVEPOINT {self._savepoint_name}"
            ) as cursor:
                await cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            async with self._connection._connection.execute("ROLLBACK") as cursor:
                await cursor.close()
        else:
            async with self._connection._connection.execute(
                f"ROLLBACK TO SAVEPOINT {self._savepoint_name}"
            ) as cursor:
                await cursor.close()
