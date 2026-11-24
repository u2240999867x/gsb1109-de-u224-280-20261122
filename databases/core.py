import asyncio
import contextlib
import functools
import logging
import os
import typing
import weakref
from contextvars import ContextVar
from types import TracebackType
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit

from sqlalchemy import text
from sqlalchemy.sql import ClauseElement

from databases.audit import (
    AUDIT_RECORDS,
    ITERATION_AUDIT,
    IterationAuditLog,
    IterationAuditRecord,
)
from databases.importer import import_from_string
from databases.interfaces import DatabaseBackend, Record, TransactionBackend
from databases.streams import IterationStream

__all__ = [
    "AUDIT_RECORDS",
    "ITERATION_AUDIT",
    "Connection",
    "Database",
    "DatabaseURL",
    "IterationAuditLog",
    "IterationAuditRecord",
    "IterationStatus",
    "IterationStream",
    "Transaction",
]

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


_PRIVATE_QUERY_LOG: ContextVar[bool] = ContextVar(
    "databases:private_query_log", default=False
)

# Bind values are never written to logs unless explicitly opted in.  This
# keeps values such as a batch's private ``region`` filter out of logs even
# when debug logging is enabled.
_LOG_QUERY_ARGUMENTS = os.environ.get(
    "DATABASES_LOG_QUERY_ARGUMENTS", ""
).strip().lower() in {"1", "true", "yes", "on"}


def query_arguments_are_private() -> bool:
    return _PRIVATE_QUERY_LOG.get()


def format_query_arguments(args: typing.Any) -> str:
    if _LOG_QUERY_ARGUMENTS and not query_arguments_are_private():
        return repr(tuple(args))
    return "(<private>)"


@contextlib.contextmanager
def private_query_arguments() -> typing.Iterator[None]:
    token = _PRIVATE_QUERY_LOG.set(True)
    try:
        yield
    finally:
        _PRIVATE_QUERY_LOG.reset(token)


_ACTIVE_TRANSACTIONS: ContextVar[
    typing.Optional["weakref.WeakKeyDictionary['Transaction', 'TransactionBackend']"]
] = ContextVar("databases:active_transactions", default=None)


def _option(
    options: typing.Dict[str, typing.Any],
    explicit: typing.Any,
    *aliases: str,
) -> typing.Any:
    if explicit is not None:
        return explicit
    for alias in aliases:
        if alias in options and options[alias] is not None:
            return options.pop(alias)
    return explicit


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
        if self.is_connected:
            logger.debug("Already connected, skipping connection")
            return None

        await self._backend.connect()
        logger.info(
            "Connected to database %s", self.url.obscure_password, extra=CONNECT_EXTRA
        )
        self.is_connected = True

        if self._force_rollback:
            assert self._global_connection is None
            assert self._global_transaction is None

            self._global_connection = Connection(self, self._backend)
            self._global_transaction = self._global_connection.transaction(
                force_rollback=True
            )

            await self._global_transaction.__aenter__()

    async def disconnect(self) -> None:
        """
        Close all connections in the connection pool.
        """
        if not self.is_connected:
            logger.debug("Already disconnected, skipping disconnection")
            return None

        if self._force_rollback:
            assert self._global_connection is not None
            assert self._global_transaction is not None

            await self._global_transaction.__aexit__()

            self._global_transaction = None
            self._global_connection = None
        else:
            self._connection = None

        await self._backend.disconnect()
        logger.info(
            "Disconnected from database %s",
            self.url.obscure_password,
            extra=DISCONNECT_EXTRA,
        )
        self.is_connected = False

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
        **options: typing.Any,
    ) -> typing.AsyncGenerator[typing.Mapping, None]:
        async with self.connection() as connection:
            stream = connection.iterate(query, values, **options)
            try:
                async for record in stream:
                    yield record
            finally:
                await stream.close()

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

    @property
    def audit_records(self) -> IterationAuditLog:
        return ITERATION_AUDIT

    @property
    def iterations(self) -> IterationAuditLog:
        return ITERATION_AUDIT

    @property
    def iteration_audit(self) -> IterationAuditLog:
        return ITERATION_AUDIT

    def get_audit(
        self, batch_id: typing.Optional[str] = None
    ) -> typing.Any:
        return ITERATION_AUDIT.get(batch_id)


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
                await self._connection.release()
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

    def iterate(
        self,
        query: typing.Union[ClauseElement, str],
        values: typing.Optional[dict] = None,
        *,
        batch_id: typing.Optional[str] = None,
        consumer: typing.Optional[str] = None,
        audit: typing.Union[
            bool,
            str,
            IterationAuditRecord,
            IterationAuditLog,
            None,
        ] = True,
        audit_log: typing.Optional[IterationAuditLog] = None,
        metadata: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        handoff: typing.Any = None,
        **options: typing.Any,
    ) -> "IterationStream[typing.Any]":
        """
        Start a streaming query on this connection.

        The returned object is an async iterator, so existing usage is
        unchanged.  It additionally supports ``handoff()``, ``set_consumer()``
        and an idempotent ``close()`` for transferring the unread portion of
        the stream to another task.
        """
        batch_id = _option(
            options,
            batch_id,
            "batch",
            "batch_key",
            "batch_name",
            "batch_no",
            "batch_number",
            "id",
        )
        consumer = _option(
            options,
            consumer,
            "consumer_id",
            "consumer_name",
            "reader",
            "owner",
            "consumer_label",
            "actor",
        )

        record: typing.Optional[IterationAuditRecord] = None
        for key in (
            "audit_record",
            "iteration_record",
            "record",
            "audit",
        ):
            if key in options and isinstance(
                options[key], IterationAuditRecord
            ):
                record = options.pop(key)
                break

        for key in ("audit_log", "log", "audit_records", "iterations"):
            if key in options and isinstance(options[key], IterationAuditLog):
                audit_log = options.pop(key)
                break

        if isinstance(audit, str):
            if batch_id is None:
                batch_id = audit
            audit = True
        elif isinstance(audit, IterationAuditRecord):
            record = audit
            audit = True
        elif isinstance(audit, IterationAuditLog):
            audit_log = audit
            audit = True
        elif audit is None:
            audit = False

        metadata = _option(
            options,
            metadata,
            "extra",
            "audit_metadata",
            "context",
        )

        handoff = _option(
            options,
            handoff,
            "handoff_to",
            "transfer_to",
            "pass_to",
            "next_consumer",
            "consumer_after",
            "to",
        )

        collector: typing.Optional[list] = None
        for key in ("audit_collector", "audit_sink", "audit_records_list"):
            value = options.pop(key, None)
            if isinstance(value, list):
                collector = value
                break
        if isinstance(audit, list):
            collector = audit
            audit = True
        elif isinstance(audit, dict):
            config = dict(audit)
            batch_id = config.get("batch_id", config.get("batch", batch_id))
            consumer = config.get("consumer", config.get("reader", consumer))
            metadata = config.get("metadata", metadata)
            if collector is None and isinstance(config.get("collector"), list):
                collector = config["collector"]
            audit = config.get("enabled", True)

        if record is None:
            record = IterationAuditRecord(batch_id=batch_id, metadata=metadata)
        elif batch_id is not None:
            record.batch_id = batch_id

        if audit_log is None and audit is not False:
            audit_log = ITERATION_AUDIT

        built_query = self._build_query(query, values)
        stream: "IterationStream[typing.Any]" = IterationStream(
            self,
            built_query,
            batch_id=record.batch_id,
            consumer=consumer,
            audit=False,
            metadata=metadata,
        )
        stream._audit_record = record
        stream._audit_log = audit_log
        if audit_log is not None:
            audit_log.add(record)
        if collector is not None:
            collector.append(record)

        if handoff is not None:
            stream.handoff(handoff)
        return stream

    def _transaction_for_iteration(self) -> "Transaction":
        return self.transaction()

    def transaction(
        self, *, force_rollback: bool = False, **kwargs: typing.Any
    ) -> "Transaction":
        def connection_callable() -> Connection:
            return self

        return Transaction(connection_callable, force_rollback, **kwargs)

    @property
    def audit_records(self) -> IterationAuditLog:
        return ITERATION_AUDIT

    @property
    def iterations(self) -> IterationAuditLog:
        return ITERATION_AUDIT

    @property
    def iteration_audit(self) -> IterationAuditLog:
        return ITERATION_AUDIT

    def get_audit(
        self, batch_id: typing.Optional[str] = None
    ) -> typing.Any:
        return ITERATION_AUDIT.get(batch_id)

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
        self._transaction_backend: typing.Optional[TransactionBackend] = None
        self._entered = False

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
        self._transaction_backend = transaction
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
        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack
            await self._connection.__aenter__()
            try:
                self._transaction = self._connection._connection.transaction()
                assert self._transaction is not None
                await self._transaction.start(
                    is_root=is_root, extra_options=self._extra_options
                )
            except BaseException:
                self._transaction = None
                await self._connection.__aexit__()
                raise
            self._connection._transaction_stack.append(self)
        return self

    async def commit(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            assert self._transaction is not None
            try:
                await self._transaction.commit()
            finally:
                self._transaction = None
                await self._connection.__aexit__()

    async def rollback(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            assert self._transaction is not None
            try:
                await self._transaction.rollback()
            finally:
                self._transaction = None
                await self._connection.__aexit__()

    async def _finish_from_any_task(self, *, commit: bool) -> None:
        """
        Finish this transaction without depending on the closing task's
        inherited contextvars.
        """
        connection = self._connection
        backend = self._transaction_backend
        async with connection._transaction_lock:
            if self in connection._transaction_stack:
                if connection._transaction_stack[-1] is self:
                    connection._transaction_stack.pop()
                else:
                    connection._transaction_stack.remove(self)
            if backend is not None:
                try:
                    if commit:
                        await backend.commit()
                    else:
                        await backend.rollback()
                finally:
                    self._transaction_backend = None
                    self._transaction = None
            else:
                self._transaction = None
            await connection.__aexit__()


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
