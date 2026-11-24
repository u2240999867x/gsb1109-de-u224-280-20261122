import asyncio
import functools

import pytest

from databases import Database
from databases.extraction import (
    DatabaseSnapshot,
    ExtractionRound,
    RoundUnavailableError,
    start_extraction,
)


def async_test(function):
    @functools.wraps(function)
    def runner(*args, **kwargs):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(function(*args, **kwargs))
        finally:
            loop.close()

    return runner


def snapshot_source():
    return {
        "east": [
            {"id": 12, "change_seq": 100, "station": "east"},
            {"id": 18, "change_seq": 150, "station": "east"},
        ],
        "west": [{"id": 72, "change_seq": 120, "station": "west"}],
    }


async def static_snapshot_loader(station, after_id, limit, **kwargs):
    rows = snapshot_source()[station]
    return [row for row in rows if after_id is None or row["id"] > after_id][:limit]


@async_test
async def test_handoff_refences_old_ack_and_keeps_snapshot_rows():
    round_instance = ExtractionRound(
        loader=static_snapshot_loader,
        lower_bound=100,
        upper_bound=199,
        page_size=1,
        snapshot_id="snapshot-one",
        auto_snapshot=False,
    )

    first = await round_instance.fetch_page("east", worker_id="east-1")
    assert first.ids == [12]

    second = await round_instance.fetch_page("east", worker_id="east-2")
    assert second.ids == [12]
    assert second.token != first.token

    stale_receipt = await first.ack()
    assert stale_receipt.accepted is False
    assert round_instance.position("east") is None

    current_receipt = await second.ack()
    assert current_receipt.accepted is True
    assert round_instance.position("east") == 12

    next_east = await round_instance.fetch_page("east")
    assert next_east.ids == [18]

    west = await round_instance.fetch_page("west")
    assert west.ids == [72]
    assert round_instance.position("west") is None


@async_test
async def test_explicit_release_then_handoff_revokes_old_token():
    round_instance = ExtractionRound(
        loader=static_snapshot_loader,
        lower_bound=100,
        upper_bound=199,
        page_size=1,
        auto_snapshot=False,
    )
    first = await round_instance.fetch_page("east", "worker-1")
    released = await round_instance.release(first)
    assert released.accepted is True

    second = await round_instance.handoff("east", new_worker_id="worker-2")
    assert second.ids == [12]
    assert second.token != first.token
    assert (await first.ack()).accepted is False
    assert round_instance.position("east") is None
    assert (await second.ack()).accepted is True


@async_test
async def test_worker_identity_is_fenced_after_handoff():
    round_instance = ExtractionRound(
        loader=static_snapshot_loader,
        lower_bound=100,
        upper_bound=199,
        page_size=1,
        auto_snapshot=False,
    )
    await round_instance.fetch_page("east", worker_id="east-1")
    await round_instance.handoff("east", new_worker_id="east-2")

    stale = await round_instance.ack("east", worker_id="east-1")
    assert stale.accepted is False
    assert round_instance.position("east") is None

    current = await round_instance.ack("east", worker_id="east-2")
    assert current.accepted is True
    assert round_instance.position("east") == 12


class ClosedSnapshot:
    snapshot_id = "snapshot-closed"

    def __init__(self):
        self.closed = False

    async def verify(self):
        if self.closed:
            raise RoundUnavailableError("Extraction round cannot continue")


@async_test
async def test_lost_snapshot_fences_all_stations():
    snapshot = ClosedSnapshot()
    round_instance = ExtractionRound(
        loader=static_snapshot_loader,
        lower_bound=100,
        upper_bound=199,
        page_size=1,
        snapshot=snapshot,
        auto_snapshot=False,
    )
    east_page = await round_instance.fetch_page("east")
    west_page = await round_instance.fetch_page("west")

    snapshot.closed = True
    round_instance.mark_fatal(RoundUnavailableError("snapshot gone"))

    with pytest.raises(RoundUnavailableError):
        await round_instance.handoff("east")
    with pytest.raises(RoundUnavailableError):
        await round_instance.fetch_page("west")

    east_receipt = await east_page.ack()
    west_receipt = await west_page.ack()
    assert east_receipt.accepted is False
    assert west_receipt.accepted is False
    assert round_instance.position("east") is None
    assert round_instance.position("west") is None


class PoolTransaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def rollback(self):
        return None


class PoolConnection:
    def __init__(self, database):
        self.database = database

    async def __aenter__(self):
        await self.database.acquire()
        return self

    async def __aexit__(self, *exc):
        self.database.release()
        return False

    def transaction(self, **kwargs):
        return PoolTransaction(self)

    async def execute(self, query, values=None):
        return None

    async def fetch_val(self, query, values=None):
        if "pg_export_snapshot" in str(query):
            return "snapshot-pool"
        return 1

    async def fetch_all(self, query, values=None):
        values = values or {}
        self.database.reads_started += 1
        if self.database.reads_started == 2:
            self.database.both_reads_started.set()
        await asyncio.wait_for(self.database.both_reads_started.wait(), 2)
        self.database.max_active_in_read = max(
            self.database.max_active_in_read, self.database.active
        )
        if values["station"] == "east":
            return [{"id": 12}]
        return [{"id": 72}]


class PoolDatabase:
    def __init__(self):
        self.active = 0
        self.max_active = 0
        self.reads_started = 0
        self.max_active_in_read = 0
        self.both_reads_started = asyncio.Event()

        class Url:
            scheme = "fake"

        self.url = Url()

    def connection(self):
        return PoolConnection(self)

    async def acquire(self):
        if self.active >= 3:
            raise RuntimeError("pool exhausted")
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)

    def release(self):
        self.active -= 1

    async def fetch_val(self, query):
        async with self.connection() as connection:
            return await connection.fetch_val(query)


@async_test
async def test_three_connection_pool_reads_concurrently_and_releases_pages():
    database = PoolDatabase()
    round_instance = await start_extraction(database, 100, 199, page_size=1)
    assert database.active == 1

    east = asyncio.create_task(round_instance.fetch_page("east"))
    west = asyncio.create_task(round_instance.fetch_page("west"))
    east_page, west_page = await asyncio.gather(east, west)

    assert east_page.ids == [12]
    assert west_page.ids == [72]
    assert database.max_active_in_read == 3
    assert database.active == 1

    assert await asyncio.wait_for(database.fetch_val("SELECT 1"), 0.2) == 1
    assert database.active == 1
    assert database.max_active == 3

    await round_instance.close()
    assert database.active == 0


@async_test
async def test_normal_query_in_start_task_does_not_reuse_snapshot_connection(tmp_path):
    database = Database(f"sqlite:///{tmp_path}/context.db")
    await database.connect()
    try:
        await database.execute(
            "CREATE TABLE tickets (id INTEGER PRIMARY KEY, value TEXT)"
        )
        await database.execute("INSERT INTO tickets (id, value) VALUES (12, 'old')")

        snapshot = DatabaseSnapshot(
            database, export_sql="SELECT 1", health_sql=None, ping=False
        )
        await snapshot.start()
        assert snapshot.connection is not database._connection

        await database.execute("UPDATE tickets SET value = 'new' WHERE id = 12")
        assert (
            await database.fetch_val("SELECT value FROM tickets WHERE id = 12") == "new"
        )

        await snapshot.close()
    finally:
        await database.disconnect()
