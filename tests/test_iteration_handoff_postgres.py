import asyncio
import logging
import sys
import types

import pytest
import sqlalchemy

try:  # pragma: no cover - asyncpg is an optional dependency
    import asyncpg  # noqa: F401
except ImportError:  # pragma: no cover
    _stub = types.ModuleType("asyncpg")
    _stub.connection = types.SimpleNamespace(Connection=object)
    _stub.transaction = types.SimpleNamespace(Transaction=object)
    sys.modules["asyncpg"] = _stub

from databases.audit import ITERATION_AUDIT
from databases.backends.postgres import PostgresBackend, PostgresConnection
from databases.streams import IterationStream


class _FakeRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return dict.__getitem__(self, key)


class _PrefetchCursor:
    """
    Mimics asyncpg's cursor, which issues one portal bind and prefetches the
    remaining rows (including the final row) into an internal buffer.
    """

    def __init__(self, connection):
        self._connection = connection
        self._buffer = []
        self._position = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._connection.bound:
            self._connection.bound = True
            self._connection.cursor_calls += 1
            self._buffer.extend(self._connection.rows)

        if self._position >= len(self._buffer):
            raise StopAsyncIteration

        row = self._buffer[self._position]
        self._position += 1
        return row

    async def aclose(self):
        self.closed = True


class _FakeTransaction:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    async def start(self):
        pass

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


class _FakeAsyncpgConnection:
    def __init__(self):
        self.rows = [
            _FakeRow(event_id=20, region="苏州"),
            _FakeRow(event_id=21, region="苏州"),
            _FakeRow(event_id=22, region="苏州"),
        ]
        self.cursor_calls = 0
        self.bound = False
        self.query = None
        self.args = None
        self.transactions = []

    def cursor(self, query, *args):
        self.query = query
        self.args = args
        return _PrefetchCursor(self)

    def transaction(self, **kwargs):
        transaction = _FakeTransaction()
        self.transactions.append(transaction)
        return transaction


class _FakeTransactionWrapper:
    def __init__(self, backend):
        self._backend = backend
        self.commits = 0
        self.rollbacks = 0

    async def start(self):
        await self._backend.start()

    async def commit(self):
        self.commits += 1
        await self._backend.commit()

    async def rollback(self):
        self.rollbacks += 1
        await self._backend.rollback()

    async def _finish_from_any_task(self, *, commit):
        if commit:
            await self.commit()
        else:
            await self.rollback()


class _ConnectionShim:
    """
    The subset of ``databases.core.Connection`` used by ``IterationStream``,
    backed by a fake asyncpg connection instead of a live server.
    """

    def __init__(self, backend_connection):
        self._connection = backend_connection
        self._query_lock = asyncio.Lock()
        self._transaction_lock = asyncio.Lock()
        self._transaction_stack = []
        self.transactions = []

    def transaction(self):
        transaction = _FakeTransactionWrapper(_FakeTransaction())
        self.transactions.append(transaction)
        return transaction

    def _transaction_for_iteration(self):
        return self.transaction()


@pytest.fixture(autouse=True)
def clear_audit_log():
    ITERATION_AUDIT.clear()
    yield
    ITERATION_AUDIT.clear()


@pytest.fixture
def fake_postgres(monkeypatch):
    stub = types.ModuleType("asyncpg")
    stub.connection = types.SimpleNamespace(Connection=object)
    stub.transaction = types.SimpleNamespace(Transaction=object)
    monkeypatch.setitem(__import__("sys").modules, "asyncpg", stub)

    backend = PostgresBackend("postgresql://localhost/test")
    pg_connection = _FakeAsyncpgConnection()
    connection = PostgresConnection(backend, backend._dialect)
    connection._connection = pg_connection
    return connection, pg_connection, _ConnectionShim(connection)


def _query():
    return sqlalchemy.text(
        "SELECT event_id, region FROM events "
        "WHERE region = :region ORDER BY event_id"
    ).bindparams(region="苏州")


def test_postgres_prefetch_handoff_early_close(fake_postgres):
    _, pg_connection, shim = fake_postgres

    async def run():
        stream = IterationStream(
            shim, _query(), batch_id="SZ-42", consumer="A"
        )
        assert (await stream.next())["event_id"] == 20
        stream.handoff("B")
        assert (await stream.next())["event_id"] == 21

        # The fake driver has already fetched row 22 into its buffer.
        await stream.close()
        await stream.close()

        record = ITERATION_AUDIT.get("SZ-42")
        assert pg_connection.cursor_calls == 1
        assert record.started_count == 1
        assert record.delivered == {"A": 1, "B": 1}
        assert record.rows_delivered == 2
        assert record.delivered_events == [20, 21]
        assert record.status == "early_closed"
        assert len(record.terminal_events) == 1
        assert shim.transactions[0].rollbacks == 1

    asyncio.run(run())


def test_postgres_prefetch_handoff_exhausted(fake_postgres):
    _, pg_connection, shim = fake_postgres

    async def run():
        stream = IterationStream(
            shim, _query(), batch_id="SZ-42", consumer="A"
        )
        assert (await stream.next())["event_id"] == 20
        stream.handoff("B")
        remaining = [row["event_id"] async for row in stream]
        assert remaining == [21, 22]

        record = ITERATION_AUDIT.get("SZ-42")
        assert pg_connection.cursor_calls == 1
        assert record.delivered == {"A": 1, "B": 2}
        assert record.rows_delivered == 3
        assert record.delivered_events == [20, 21, 22]
        assert record.status == "exhausted"
        assert shim.transactions[0].commits == 1

        await stream.close()
        assert len(record.terminal_events) == 1

    asyncio.run(run())


def test_postgres_iterate_masks_bind_values(monkeypatch):
    stub = types.ModuleType("asyncpg")
    stub.connection = types.SimpleNamespace(Connection=object)
    stub.transaction = types.SimpleNamespace(Transaction=object)
    monkeypatch.setitem(__import__("sys").modules, "asyncpg", stub)

    backend = PostgresBackend("postgresql://localhost/test")
    connection = PostgresConnection(backend, backend._dialect)
    connection._connection = _FakeAsyncpgConnection()

    messages = []

    class Handler(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())

    logger = logging.getLogger("databases")
    handler = Handler()
    logger.addHandler(handler)
    previous_level = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        compiled = connection._compile(_query())
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)

    assert compiled[1] == ["苏州"]
    assert all("苏州" not in message for message in messages)
