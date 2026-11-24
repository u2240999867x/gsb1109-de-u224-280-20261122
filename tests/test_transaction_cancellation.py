import asyncio
import functools
import typing

import pytest

from databases import Database

CREATE_TABLE = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
INSERT_ITEM = "INSERT INTO items (name) VALUES (:name)"
SELECT_ITEMS = "SELECT name FROM items ORDER BY id"


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.new_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


class StatementGate:
    """
    A one-shot gate for SQL statements matching a prefix.

    With `block_return` the statement is executed by the driver, but the
    await does not return until the gate is released. With `block_send`
    the statement is held back before it is sent to the database at all.
    """

    def __init__(self, prefix: str, *, block_send=False, block_return=False):
        self.prefix = prefix
        self.block_send = block_send
        self.block_return = block_return
        self.armed = True
        self.send_attempted = asyncio.Event()
        self.executed = asyncio.Event()
        self.release = asyncio.Event()


class StatementGates:
    def __init__(self):
        self._gates: typing.List[StatementGate] = []

    def add(self, gate: StatementGate) -> StatementGate:
        self._gates.append(gate)
        return gate

    def match(self, sql: str) -> typing.Optional[StatementGate]:
        for gate in self._gates:
            if gate.armed and sql.startswith(gate.prefix):
                gate.armed = False
                return gate
        return None


class GatedCursor:
    """
    Async context manager mimicking the awaitable cursor returned by
    `aiosqlite.Connection.execute()`, with gating around the statement.
    """

    def __init__(self, connection, sql, parameters, gates):
        self._connection = connection
        self._sql = sql
        self._parameters = parameters
        self._gates = gates
        self._cursor = None

    async def __aenter__(self):
        gate = self._gates.match(self._sql)
        if gate is not None and gate.block_send:
            gate.send_attempted.set()
            await gate.release.wait()
        self._cursor = await self._connection.execute(self._sql, self._parameters)
        if gate is not None and gate.block_return:
            gate.executed.set()
            await gate.release.wait()
        return self._cursor

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._cursor is not None:
            await self._cursor.close()


class GatedConnection:
    """
    Wraps a raw aiosqlite connection, gating statements through `execute()`.
    """

    def __init__(self, connection, gates):
        self._connection = connection
        self._gates = gates

    def execute(self, sql, parameters=None):
        return GatedCursor(self._connection, sql, parameters, self._gates)

    def __getattr__(self, name):
        return getattr(self._connection, name)


def gated_database(tmp_path) -> typing.Tuple[Database, StatementGates]:
    """
    A `Database` whose raw connections are wrapped by `GatedConnection`.
    """
    database = Database(f"sqlite:///{tmp_path}/test.db")
    gates = StatementGates()

    pool = database._backend._pool
    original_acquire = pool.acquire

    async def gated_acquire():
        connection = await original_acquire()
        return GatedConnection(connection, gates)

    pool.acquire = gated_acquire
    return database, gates


async def insert_item(connection, name):
    await connection.execute(INSERT_ITEM, {"name": name})


async def item_names(database):
    rows = await database.fetch_all(SELECT_ITEMS)
    return [row["name"] for row in rows]


@pytest.mark.parametrize("style", ["explicit", "context_manager"])
@async_adapter
async def test_cancelled_begin_leaves_no_transaction_or_connection_reference(
    tmp_path, style
):
    """
    Cancelling a task while the driver has executed BEGIN but the await
    has not returned must settle the transaction: no transaction or
    connection references are left behind, and the same connection keeps
    working without being reconnected.
    """
    database, gates = gated_database(tmp_path)
    begin_gate = gates.add(StatementGate("BEGIN", block_return=True))

    async with database:
        await database.execute(CREATE_TABLE)
        async with database.connection() as connection:
            transaction = connection.transaction()
            if style == "explicit":
                start = transaction.start()
            else:
                start = transaction.__aenter__()

            task = asyncio.create_task(start)
            # The database has executed BEGIN, the await has not returned.
            await begin_gate.executed.wait()
            task.cancel()
            begin_gate.release.set()
            with pytest.raises(asyncio.CancelledError):
                await task

            # No transaction or connection references are left behind.
            assert transaction._transaction is None
            assert connection._transaction_stack == []
            assert connection._connection_counter == 1

            # The same connection keeps working without reconnecting.
            async with connection.transaction():
                await insert_item(connection, "A")

        assert await item_names(database) == ["A"]


@pytest.mark.parametrize("style", ["explicit", "context_manager"])
@async_adapter
async def test_cancelled_commit_after_release_savepoint_merges_inner_transaction(
    tmp_path, style
):
    """
    Cancelling a task while the driver has executed RELEASE SAVEPOINT but
    the await has not returned must judge the inner transaction as merged:
    the cancellation propagates only after the result and the cleanup are
    settled, and the outer commit persists A, B and C.
    """
    database, gates = gated_database(tmp_path)
    release_gate = gates.add(StatementGate("RELEASE SAVEPOINT", block_return=True))

    async with database:
        await database.execute(CREATE_TABLE)
        async with database.connection() as connection:
            outer = connection.transaction()
            await outer.start()
            await insert_item(connection, "A")

            if style == "explicit":
                inner = connection.transaction()
                await inner.start()
                await insert_item(connection, "B")
                inner_task = asyncio.create_task(inner.commit())
            else:

                async def run_inner():
                    async with connection.transaction():
                        await insert_item(connection, "B")

                inner_task = asyncio.create_task(run_inner())

            # The database has executed RELEASE SAVEPOINT, the await has
            # not returned.
            await release_gate.executed.wait()
            inner_task.cancel()
            release_gate.release.set()

            # The original cancellation reaches the caller.
            with pytest.raises(asyncio.CancelledError):
                await inner_task

            # The cancellation propagated only after the cleanup settled.
            assert connection._transaction_stack == [outer]
            assert connection._connection_counter == 2

            await insert_item(connection, "C")
            await outer.commit()

            # Subsequent transactions do not trip the stack assertion.
            async with connection.transaction():
                pass

        # A new connection observes the merged inner transaction.
        assert await item_names(database) == ["A", "B", "C"]


@pytest.mark.parametrize("style", ["explicit", "context_manager"])
@async_adapter
async def test_cancelled_commit_before_release_savepoint_rolls_back_inner_transaction(
    tmp_path, style
):
    """
    Cancelling a task before the RELEASE SAVEPOINT statement is sent must
    roll the inner transaction back: the cancellation propagates only
    after the cleanup is settled, and the outer commit persists A and C.
    """
    database, gates = gated_database(tmp_path)
    release_gate = gates.add(StatementGate("RELEASE SAVEPOINT", block_send=True))

    async with database:
        await database.execute(CREATE_TABLE)
        async with database.connection() as connection:
            outer = connection.transaction()
            await outer.start()
            await insert_item(connection, "A")

            if style == "explicit":
                inner = connection.transaction()
                await inner.start()
                await insert_item(connection, "B")
                inner_task = asyncio.create_task(inner.commit())
            else:

                async def run_inner():
                    async with connection.transaction():
                        await insert_item(connection, "B")

                inner_task = asyncio.create_task(run_inner())

            # The RELEASE SAVEPOINT statement has not been sent yet.
            await release_gate.send_attempted.wait()
            inner_task.cancel()
            release_gate.release.set()

            # The original cancellation reaches the caller.
            with pytest.raises(asyncio.CancelledError):
                await inner_task

            # The statement was never sent to the database.
            assert not release_gate.executed.is_set()

            # The cancellation propagated only after the cleanup settled.
            assert connection._transaction_stack == [outer]
            assert connection._connection_counter == 2

            await insert_item(connection, "C")
            await outer.commit()

            # Subsequent transactions do not trip the stack assertion.
            async with connection.transaction():
                pass

        # A new connection observes the outer transaction without B.
        assert await item_names(database) == ["A", "C"]
