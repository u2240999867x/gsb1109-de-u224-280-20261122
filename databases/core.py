import asyncio
import contextlib
import functools
import logging
import typing
import weakref
from contextvars import ContextVar
from types import TracebackType
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit

from sqlalchemy import text
from sqlalchemy.sql import ClauseElement

from databases.importer import import_from_string
from databases.interfaces import DatabaseBackend, Record, TransactionBackend

try:  # pragma: no cover
    import click

    # Extra log info for optional coloured terminal outputs.
    LOG_EXTRA = {
        "color_message": "Query: " + click.style("%s", bold=True) + " Args: %s"
    }
    CONNECT_EXTRA = {
        "color_message": "Connected to database " + click.style("%s", bold=True)
    }
    DISCONNECT_EXTRA = {
        "color_message": "Disconnected from database " + click.style("%s", bold=True)
    }
except ImportError:  # pragma: no cover
    LOG_EXTRA = {}
    CONNECT_EXTRA = {}
    DISCONNECT_EXTRA = {}


logger = logging.getLogger("databases")


def _consume_task_exception(task: "asyncio.Future") -> None:
    # Start and shutdown failures are reported to the coroutines awaiting the
    # shared task.  If every waiter was cancelled there may be nobody left to
    # retrieve the exception, so consume it here to avoid a spurious warning.
    try:
        task.exception()
    except asyncio.CancelledError:
        pass


_ACTIVE_TRANSACTIONS: ContextVar[
    typing.Optional["weakref.WeakKeyDictionary['Transaction', 'TransactionBackend']"]
] = ContextVar("databases:active_transactions", default=None)


class Database:
    SUPPORTED_BACKENDS = {
        "postgresql": "databases.backends.postgres:PostgresBackend",
        "postgresql+aiopg": "databases.backends.aiopg:AiopgBackend",
        "postgres": "databases.backends.postgres:PostgresBackend",
        "mysql": "databases.backends.mysql:MySQLBackend",
        "mysql+asyncmy": "databases.backends.asyncmy:AsyncMyBackend",
        "sqlite": "databases.backends.sqlite:SQLiteBackend",
    }

    _connection_map: "weakref.WeakKeyDictionary[asyncio.Task, 'Connection']"

    def __init__(
        self,
        url: typing.Union[str, "DatabaseURL"],
        *,
        force_rollback: bool = False,
        **options: typing.Any,
    ):
        self.url = DatabaseURL(url)
        self.options = options
        self.is_connected = False
        self._connection_map = weakref.WeakKeyDictionary()

        # Connection startup and shutdown are shared by all callers.  Keeping
        # the operations in tasks means that cancelling any one caller does
        # not abort the work that other callers are waiting for.
        self._connect_task: typing.Optional[asyncio.Task] = None
        self._disconnect_task: typing.Optional[asyncio.Task] = None

        self._force_rollback = force_rollback

        backend_str = self._get_backend()
        backend_cls = import_from_string(backend_str)
        assert issubclass(backend_cls, DatabaseBackend)
        self._backend = backend_cls(self.url, **self.options)

        # When `force_rollback=True` is used, we use a single global
        # connection, within a transaction that always rolls back.
        self._global_connection: typing.Optional[Connection] = None
        self._global_transaction: typing.Optional[Transaction] = None

    @property
    def _current_task(self) -> asyncio.Task:
        task = asyncio.current_task()
        if not task:
            raise RuntimeError("No currently active asyncio.Task found")
        return task

    @property
    def _connection(self) -> typing.Optional["Connection"]:
        return self._connection_map.get(self._current_task)

    @_connection.setter
    def _connection(
        self, connection: typing.Optional["Connection"]
    ) -> typing.Optional["Connection"]:
        task = self._current_task

        if connection is None:
            self._connection_map.pop(task, None)
        else:
            self._connection_map[task] = connection

        return self._connection

    async def connect(self) -> None:
        """
        Establish the connection pool.
        """
        while True:
            # A shutdown always has to fully complete before a new session
            # may be started.  Wait for the shared shutdown operation so
            # that a failed shutdown is reported to every waiter.  This is
            # checked before `is_connected`, because that flag stays true
            # until the shutdown has actually finished.
            disconnect_task = self._disconnect_task
            if disconnect_task is not None:
                if disconnect_task.done():
                    # A previous shutdown has already settled.  Its failure
                    # was reported to the callers that were waiting on it;
                    # this call is free to start a fresh session.
                    if self._disconnect_task is disconnect_task:
                        self._disconnect_task = None
                    continue

                try:
                    await asyncio.shield(disconnect_task)
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    if self._disconnect_task is disconnect_task:
                        self._disconnect_task = None
                    raise
                if self._disconnect_task is disconnect_task:
                    self._disconnect_task = None
                continue

            if self.is_connected:
                logger.debug("Already connected, skipping connection")
                return None

            connect_task = self._connect_task
            if connect_task is not None and connect_task.done():
                # A previous startup attempt either failed or was cancelled.
                # Do not reuse it; a fresh call is allowed to retry.
                connect_task = None
                self._connect_task = None

            if connect_task is None:
                connect_task = asyncio.ensure_future(self._connect())
                self._connect_task = connect_task

            try:
                # Shield the shared startup task: cancelling one caller must
                # not abort the startup that other callers are waiting for.
                await asyncio.shield(connect_task)
            except asyncio.CancelledError:
                if connect_task.done() and self._connect_task is connect_task:
                    self._connect_task = None
                elif self._connect_task is connect_task:
                    connect_task.add_done_callback(_consume_task_exception)
                raise
            except BaseException:
                if self._connect_task is connect_task:
                    self._connect_task = None
                raise
            return

    async def _connect(self) -> None:
        global_connection: typing.Optional[Connection] = None
        global_transaction: typing.Optional[Transaction] = None
        try:
            await self._backend.connect()
            logger.info(
                "Connected to database %s",
                self.url.obscure_password,
                extra=CONNECT_EXTRA,
            )

            if self._force_rollback:
                assert self._global_connection is None
                assert self._global_transaction is None

                global_connection = Connection(self, self._backend)
                global_transaction = global_connection.transaction(
                    force_rollback=True
                )
                await global_transaction.__aenter__()

                self._global_connection = global_connection
                self._global_transaction = global_transaction

            self.is_connected = True
        except BaseException:
            # Do not leave a half-started session behind (notably the
            # SQLite shared-memory keeper connection).
            self._global_connection = None
            self._global_transaction = None
            if global_connection is not None and global_transaction is not None:
                with contextlib.suppress(Exception):
                    if global_connection._transaction_stack:
                        await global_transaction.__aexit__(None, None, None)
                    elif global_connection._connection_counter:
                        await global_connection.__aexit__(None, None, None)
            with contextlib.suppress(Exception):
                await self._backend.disconnect()
            raise

    async def disconnect(self) -> None:
        """
        Close all connections in the connection pool.
        """
        # Drop this task's task-local connection reference in the caller's
        # context (the shared shutdown runs in its own task).
        if self._global_connection is None and self._connection is not None:
            self._connection = None

        if self._disconnect_task is None and (
            self.is_connected
            or (self._connect_task is not None and not self._connect_task.done())
        ):
            # Reject new borrows from the moment shutdown is requested, even
            # before the shared shutdown task gets scheduled.
            self._backend.prepare_disconnect()

        while True:
            disconnect_task = self._disconnect_task
            if disconnect_task is not None:
                try:
                    await asyncio.shield(disconnect_task)
                except asyncio.CancelledError:
                    if disconnect_task.done() and (
                        self._disconnect_task is disconnect_task
                    ):
                        self._disconnect_task = None
                    raise
                except BaseException:
                    if self._disconnect_task is disconnect_task:
                        self._disconnect_task = None
                    raise
                if self._disconnect_task is disconnect_task:
                    self._disconnect_task = None
                return

            if not self.is_connected:
                # If startup is still in flight, let it settle (or fail)
                # before deciding that there is nothing to shut down.
                connect_task = self._connect_task
                if connect_task is not None and not connect_task.done():
                    try:
                        await asyncio.shield(connect_task)
                    except asyncio.CancelledError:
                        raise
                    except BaseException:
                        if self._connect_task is connect_task:
                            self._connect_task = None
                        return
                if not self.is_connected:
                    if self._connect_task is connect_task:
                        self._connect_task = None
                    logger.debug("Already disconnected, skipping disconnection")
                    return None

            disconnect_task = asyncio.ensure_future(self._disconnect())
            self._disconnect_task = disconnect_task
            try:
                await asyncio.shield(disconnect_task)
            except asyncio.CancelledError:
                if disconnect_task.done() and (
                    self._disconnect_task is disconnect_task
                ):
                    self._disconnect_task = None
                elif self._disconnect_task is disconnect_task:
                    disconnect_task.add_done_callback(_consume_task_exception)
                raise
            except BaseException:
                if self._disconnect_task is disconnect_task:
                    self._disconnect_task = None
                raise
            if self._disconnect_task is disconnect_task:
                self._disconnect_task = None
            return

    async def _disconnect(self) -> None:
        error: typing.Optional[BaseException] = None
        try:
            if self._force_rollback:
                global_connection = self._global_connection
                global_transaction = self._global_transaction
                # Clear these even if the rollback fails: the session is no
                # longer usable and a later connect() must start clean.
                self._global_transaction = None
                self._global_connection = None

                if global_transaction is not None:
                    try:
                        await global_transaction.__aexit__()
                    except BaseException as exc:
                        error = exc
                        # Make sure the underlying connection is returned so
                        # that shutdown can still drain the session.
                        if global_connection is not None and (
                            global_connection._connection_counter
                        ):
                            with contextlib.suppress(Exception):
                                await global_connection.__aexit__(None, None, None)

            try:
                await self._backend.disconnect()
            except BaseException as exc:
                if error is None:
                    error = exc

            if error is None:
                logger.info(
                    "Disconnected from database %s",
                    self.url.obscure_password,
                    extra=DISCONNECT_EXTRA,
                )
        finally:
            # Even a failed shutdown must leave the Database object in a
            # state where a later connect() can retry the whole session.
            self.is_connected = False
            self._connect_task = None
            if self._disconnect_task is asyncio.current_task():
                # The failure is reported by the waiter below.
                self._disconnect_task = None

        if error is not None:
            raise error

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        await self.disconnect()

    async def fetch_all(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.List[Record]:
        async with self.connection() as connection:
            return await connection.fetch_all(query, values)

    async def fetch_one(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.Optional[Record]:
        async with self.connection() as connection:
            return await connection.fetch_one(query, values)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.fetch_val(query, values, column=column)

    async def execute(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.Any:
        async with self.connection() as connection:
            return await connection.execute(query, values)

    async def execute_many(
        self, query: typing.Union[ClauseElement, str], values: list
    ) -> None:
        async with self.connection() as connection:
            return await connection.execute_many(query, values)

    async def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.AsyncGenerator[typing.Mapping, None]:
        async with self.connection() as connection:
            async for record in connection.iterate(query, values):
                yield record

    def connection(self) -> "Connection":
        if self._global_connection is not None:
            return self._global_connection

        if not self._connection:
            self._connection = Connection(self, self._backend)

        return self._connection

    def transaction(
        self, *, force_rollback: bool = False, **kwargs: typing.Any
    ) -> "Transaction":
        return Transaction(self.connection, force_rollback=force_rollback, **kwargs)

    @contextlib.contextmanager
    def force_rollback(self) -> typing.Iterator[None]:
        initial = self._force_rollback
        self._force_rollback = True
        try:
            yield
        finally:
            self._force_rollback = initial

    def _get_backend(self) -> str:
        return self.SUPPORTED_BACKENDS.get(
            self.url.scheme, self.SUPPORTED_BACKENDS[self.url.dialect]
        )


class Connection:
    def __init__(self, database: Database, backend: DatabaseBackend) -> None:
        self._database = database
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack: typing.List[Transaction] = []

        self._query_lock = asyncio.Lock()

    async def __aenter__(self) -> "Connection":
        async with self._connection_lock:
            self._connection_counter += 1
            try:
                if self._connection_counter == 1:
                    await self._connection.acquire()
            except BaseException as e:
                self._connection_counter -= 1
                raise e
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        async with self._connection_lock:
            assert self._connection is not None
            self._connection_counter -= 1
            if self._connection_counter == 0:
                try:
                    await self._connection.release()
                finally:
                    # A release may finish after a later session has already
                    # installed a new connection for this task.  Never clear
                    # that newer connection as a side effect of the old one.
                    if self._database._connection is self:
                        self._database._connection = None

    async def fetch_all(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.List[Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_all(built_query)

    async def fetch_one(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.Optional[Record]:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_one(built_query)

    async def fetch_val(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        column: typing.Any = 0,
    ) -> typing.Any:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.fetch_val(built_query, column)

    async def execute(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.Any:
        built_query = self._build_query(query, values)
        async with self._query_lock:
            return await self._connection.execute(built_query)

    async def execute_many(
        self, query: typing.Union[ClauseElement, str], values: list
    ) -> None:
        queries = [self._build_query(query, values_set) for values_set in values]
        async with self._query_lock:
            await self._connection.execute_many(queries)

    async def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        built_query = self._build_query(query, values)
        async with self.transaction():
            async with self._query_lock:
                async for record in self._connection.iterate(built_query):
                    yield record

    def transaction(
        self, *, force_rollback: bool = False, **kwargs: typing.Any
    ) -> "Transaction":
        def connection_callable() -> Connection:
            return self

        return Transaction(connection_callable, force_rollback, **kwargs)

    @property
    def raw_connection(self) -> typing.Any:
        return self._connection.raw_connection

    @staticmethod
    def _build_query(
        query: typing.Union[ClauseElement, str], values: typing.Optional[dict] = None
    ) -> ClauseElement:
        if isinstance(query, str):
            query = text(query)

            return query.bindparams(**values) if values is not None else query
        elif values:
            return query.values(**values)  # type: ignore

        return query


_CallableType = typing.TypeVar("_CallableType", bound=typing.Callable)


class Transaction:
    def __init__(
        self,
        connection_callable: typing.Callable[[], Connection],
        force_rollback: bool,
        **kwargs: typing.Any,
    ) -> None:
        self._connection_callable = connection_callable
        self._force_rollback = force_rollback
        self._extra_options = kwargs
        # Backend created by the most recent start().  Normally commit() and
        # rollback() find it in the task-local ContextVar; this reference
        # additionally covers shutdown, which runs in a shared task where the
        # initiating task's context is not available.
        self._started_transaction: typing.Optional["TransactionBackend"] = None

    @property
    def _connection(self) -> "Connection":
        # Returns the same connection if called multiple times
        return self._connection_callable()

    @property
    def _transaction(self) -> typing.Optional["TransactionBackend"]:
        transactions = _ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            return None

        return transactions.get(self, None)

    @_transaction.setter
    def _transaction(
        self, transaction: typing.Optional["TransactionBackend"]
    ) -> typing.Optional["TransactionBackend"]:
        transactions = _ACTIVE_TRANSACTIONS.get()
        if transactions is None:
            transactions = weakref.WeakKeyDictionary()
        else:
            transactions = transactions.copy()

        if transaction is None:
            transactions.pop(self, None)
        else:
            transactions[self] = transaction

        _ACTIVE_TRANSACTIONS.set(transactions)
        return transactions.get(self, None)

    def _active_transaction(self) -> typing.Optional["TransactionBackend"]:
        transaction = self._transaction
        if transaction is not None:
            return transaction
        return self._started_transaction

    async def __aenter__(self) -> "Transaction":
        """
        Called when entering `async with database.transaction()`
        """
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]] = None,
        exc_value: typing.Optional[BaseException] = None,
        traceback: typing.Optional[TracebackType] = None,
    ) -> None:
        """
        Called when exiting `async with database.transaction()`
        """
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> typing.Generator[None, None, "Transaction"]:
        """
        Called if using the low-level `transaction = await database.transaction()`
        """
        return self.start().__await__()

    def __call__(self, func: _CallableType) -> _CallableType:
        """
        Called if using `@database.transaction()` as a decorator.
        """

        @functools.wraps(func)
        async def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper  # type: ignore

    async def start(self) -> "Transaction":
        # The same Transaction instance may be used concurrently (for example
        # by a decorator).  Keep the backend local until it is fully started.
        started_transaction = self._connection._connection.transaction()

        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack
            await self._connection.__aenter__()
            await started_transaction.start(
                is_root=is_root, extra_options=self._extra_options
            )
            self._connection._transaction_stack.append(self)
            self._started_transaction = started_transaction
            self._transaction = started_transaction
        return self

    async def commit(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            transaction = self._active_transaction()
            assert transaction is not None
            await transaction.commit()
            await self._connection.__aexit__()
            self._transaction = None
            self._started_transaction = None

    async def rollback(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            transaction = self._active_transaction()
            assert transaction is not None
            await transaction.rollback()
            await self._connection.__aexit__()
            self._transaction = None
            self._started_transaction = None


class _EmptyNetloc(str):
    def __bool__(self) -> bool:
        return True


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL"]):
        if isinstance(url, DatabaseURL):
            self._url: str = url._url
        elif isinstance(url, str):
            self._url = url
        else:
            raise TypeError(
                f"Invalid type for DatabaseURL. Expected str or DatabaseURL, got {type(url)}"
            )

    @property
    def components(self) -> SplitResult:
        if not hasattr(self, "_components"):
            self._components = urlsplit(self._url)
        return self._components

    @property
    def scheme(self) -> str:
        return self.components.scheme

    @property
    def dialect(self) -> str:
        return self.components.scheme.split("+")[0]

    @property
    def driver(self) -> str:
        if "+" not in self.components.scheme:
            return ""
        return self.components.scheme.split("+", 1)[1]

    @property
    def userinfo(self) -> typing.Optional[bytes]:
        if self.components.username:
            info = self.components.username
            if self.components.password:
                info += ":" + self.components.password
            return info.encode("utf-8")
        return None

    @property
    def username(self) -> typing.Optional[str]:
        if self.components.username is None:
            return None
        return unquote(self.components.username)

    @property
    def password(self) -> typing.Optional[str]:
        if self.components.password is None:
            return None
        return unquote(self.components.password)

    @property
    def hostname(self) -> typing.Optional[str]:
        return (
            self.components.hostname
            or self.options.get("host")
            or self.options.get("unix_sock")
        )

    @property
    def port(self) -> typing.Optional[int]:
        return self.components.port

    @property
    def netloc(self) -> typing.Optional[str]:
        return self.components.netloc

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]
        return unquote(path)

    @property
    def options(self) -> dict:
        if not hasattr(self, "_options"):
            self._options = dict(parse_qsl(self.components.query))
        return self._options

    def replace(self, **kwargs: typing.Any) -> "DatabaseURL":
        if (
            "username" in kwargs
            or "password" in kwargs
            or "hostname" in kwargs
            or "port" in kwargs
        ):
            hostname = kwargs.pop("hostname", self.hostname)
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", self.components.username)
            password = kwargs.pop("password", self.components.password)

            netloc = hostname
            if port is not None:
                netloc += f":{port}"
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f":{password}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            kwargs["path"] = "/" + kwargs.pop("database")

        if "dialect" in kwargs or "driver" in kwargs:
            dialect = kwargs.pop("dialect", self.dialect)
            driver = kwargs.pop("driver", self.driver)
            kwargs["scheme"] = f"{dialect}+{driver}" if driver else dialect

        if not kwargs.get("netloc", self.netloc):
            # Using an empty string that evaluates as True means we end up
            # with URLs like `sqlite:///database` instead of `sqlite:/database`
            kwargs["netloc"] = _EmptyNetloc()

        components = self.components._replace(**kwargs)
        return self.__class__(components.geturl())

    @property
    def obscure_password(self) -> str:
        if self.password:
            return self.replace(password="********")._url
        return self._url

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.obscure_password)})"

    def __eq__(self, other: typing.Any) -> bool:
        return str(self) == str(other)
