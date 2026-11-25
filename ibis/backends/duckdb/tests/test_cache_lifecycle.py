from __future__ import annotations

import gc

import pyarrow as pa
import pytest

import ibis


def cached_tables(con):
    return {name for name in con.list_tables() if name.startswith("ibis_cached_")}


def batches(reader):
    return [batch["x"].to_pylist() for batch in reader]


@pytest.fixture
def con():
    con = ibis.duckdb.connect()
    try:
        yield con
    finally:
        con.disconnect()


def make_expr(con, values):
    con.create_table("ints", pa.table({"x": list(values)}), overwrite=True)
    return con.table("ints").order_by("x")


def test_cache_readers_outlive_handle_release(con):
    expr = make_expr(con, range(1, 7))

    first = expr.cache()
    second = expr.cache()
    # both handles refer to the same physical cached table
    assert first.op() is second.op()
    name = first.op().name
    assert name in cached_tables(con)

    a = first.to_pyarrow_batches(chunk_size=2)
    b = second.to_pyarrow_batches(chunk_size=2)
    assert b.read_next_batch()["x"].to_pylist() == [1, 2]

    # releasing the first handle twice is harmless and leaves the table alone
    first.release()
    first.release()
    assert name in cached_tables(con)
    assert second.to_pyarrow_batches(chunk_size=6).read_all()["x"].to_pylist() == [
        1,
        2,
        3,
        4,
        5,
        6,
    ]

    # dropping the second handle frees the table from the cache, but the open
    # readers keep it alive
    del second
    gc.collect()
    assert name in cached_tables(con)

    assert [a.read_next_batch()["x"].to_pylist() for _ in range(3)] == [
        [1, 2],
        [3, 4],
        [5, 6],
    ]
    a.close()
    b.close()
    assert name not in cached_tables(con)


def test_recache_after_refresh_keeps_old_readers(con):
    expr = make_expr(con, range(1, 7))

    first = expr.cache()
    old_name = first.op().name
    a = first.to_pyarrow_batches(chunk_size=2)
    # do not read `a` yet

    second = expr.cache()
    b = second.to_pyarrow_batches(chunk_size=2)
    assert b.read_next_batch()["x"].to_pylist() == [1, 2]

    first.release()
    first.release()
    assert b.read_next_batch()["x"].to_pylist() == [3, 4]
    del first, second
    gc.collect()

    # refresh the input and materialize a new cache round
    refreshed = make_expr(con, range(101, 107))
    third = refreshed.cache()
    new_name = third.op().name
    assert new_name != old_name
    c = third.to_pyarrow_batches(chunk_size=2)

    # closing the second reader early must not affect the first
    b.close()

    assert [a.read_next_batch()["x"].to_pylist() for _ in range(3)] == [
        [1, 2],
        [3, 4],
        [5, 6],
    ]
    # the old table is now dropped, and the new one survives
    assert old_name not in cached_tables(con)
    assert new_name in cached_tables(con)

    assert [c.read_next_batch()["x"].to_pylist() for _ in range(3)] == [
        [101, 102],
        [103, 104],
        [105, 106],
    ]

    # the new table was only kept alive by the new reader (and its handle);
    # once both are gone it is cleaned up as well
    third.release()
    c.close()
    assert new_name not in cached_tables(con)


def test_reader_keeps_pinned_table_after_handle_gc(con):
    expr = make_expr(con, range(1, 7))

    handle = expr.cache()
    name = handle.op().name
    reader = handle.to_pyarrow_batches(chunk_size=2)
    del handle
    gc.collect()

    # the reader still pins the table even though the last handle is gone
    assert name in cached_tables(con)
    assert reader.read_all()["x"].to_pylist() == [1, 2, 3, 4, 5, 6]
    assert name not in cached_tables(con)


def test_reader_closed_before_reading_releases_pin(con):
    expr = make_expr(con, range(1, 7))

    handle = expr.cache()
    name = handle.op().name
    reader = handle.to_pyarrow_batches(chunk_size=2)
    reader.close()
    handle.release()
    assert name not in cached_tables(con)


def test_memtable_batches_still_work(con):
    table = ibis.memtable(pa.table({"x": [1, 2, 3, 4, 5]}))
    reader = table.to_pyarrow_batches(chunk_size=2)
    assert batches(reader) == [[1, 2], [3, 4], [5]]
