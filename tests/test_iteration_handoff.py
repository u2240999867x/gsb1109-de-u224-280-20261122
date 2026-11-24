import asyncio
import logging

import pytest

from databases import Database, DatabaseURL
from databases.audit import ITERATION_AUDIT
from tests.test_databases import DATABASE_URLS, async_adapter

BATCH_ID = "SZ-42"
REGION = "苏州"


@pytest.fixture(autouse=True)
def clear_audit_log():
    ITERATION_AUDIT.clear()
    yield
    ITERATION_AUDIT.clear()


async def _setup_events(connection):
    await connection.execute(
        "CREATE TEMPORARY TABLE events ("
        "event_id INTEGER PRIMARY KEY, region TEXT NOT NULL)"
    )
    await connection.execute(
        "INSERT INTO events(event_id, region) VALUES (20, :region), "
        "(21, :region), (22, :region)",
        {"region": REGION},
    )


async def _select_events(connection, **options):
    return connection.iterate(
        "SELECT event_id, region FROM events "
        "WHERE region = :region ORDER BY event_id",
        {"region": REGION},
        batch_id=BATCH_ID,
        **options,
    )


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_handoff_early_close(database_url):
    """
    A consumer can read the first row, hand the stream to another task, and
    the second task can close it early without restarting the query.
    """
    if DatabaseURL(database_url).dialect == "mysql":
        pytest.skip("MySQL requires a temporary table on the same connection")

    async with Database(database_url) as database:
        async with database.connection() as connection:
            await _setup_events(connection)

            async with connection.transaction():
                stream = await _select_events(connection, consumer="A")
                first = await stream.next()
                assert first["event_id"] == 20

                handed_off = asyncio.Event()
                read = asyncio.Event()
                received = []

                async def cleaner():
                    handed_off.set()
                    await read.wait()
                    stream.handoff("B")
                    row = await stream.next()
                    received.append(row["event_id"])
                    await stream.close()
                    await stream.close()

                task = asyncio.create_task(cleaner())
                await handed_off.wait()
                stream.handoff("B")
                read.set()
                await task

                assert received == [21]

                rows = await connection.fetch_all(
                    "SELECT event_id FROM events ORDER BY event_id"
                )
                assert [row["event_id"] for row in rows] == [20, 21, 22]

            record = ITERATION_AUDIT.get(BATCH_ID)
            assert record.started_count == 1
            assert record.delivered == {"A": 1, "B": 1}
            assert record.rows_delivered == 2
            assert record.delivered_events == [20, 21]
            assert record.status == "early_closed"
            assert len(record.terminal_events) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_handoff_exhausted(database_url):
    """
    Handing off and then draining the stream records all rows and ends in the
    exhausted state.
    """
    if DatabaseURL(database_url).dialect == "mysql":
        pytest.skip("MySQL requires a temporary table on the same connection")

    async with Database(database_url) as database:
        async with database.connection() as connection:
            await _setup_events(connection)

            stream = await _select_events(connection, consumer="A")
            assert (await stream.next())["event_id"] == 20
            stream.handoff("B")

            remaining = [row["event_id"] async for row in stream]
            assert remaining == [21, 22]

            record = ITERATION_AUDIT.get(BATCH_ID)
            assert record.started_count == 1
            assert record.delivered == {"A": 1, "B": 2}
            assert record.rows_delivered == 3
            assert record.delivered_events == [20, 21, 22]
            assert record.status == "exhausted"

            await stream.close()
            assert len(record.terminal_events) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_bind_values_are_not_logged(database_url, caplog):
    """
    Iteration bind values remain private in library debug logs while the
    query still receives and uses them.
    """
    if DatabaseURL(database_url).dialect == "mysql":
        pytest.skip("MySQL requires a temporary table on the same connection")

    async with Database(database_url) as database:
        async with database.connection() as connection:
            await _setup_events(connection)

            with caplog.at_level(logging.DEBUG, logger="databases"):
                caplog.clear()
                stream = await _select_events(connection, consumer="A")
                rows = [row async for row in stream]

            assert [row["event_id"] for row in rows] == [20, 21, 22]
            assert REGION not in caplog.text


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_legacy_async_for(database_url):
    """
    Existing single-task ``async for`` consumption still works without any
    handoff or audit options.
    """
    async with Database(database_url) as database:
        query = "SELECT * FROM (VALUES (1), (2), (3)) as t"
        results = []
        async for row in database.connection().iterate(query):
            results.append(row[0])

        assert results == [1, 2, 3]
