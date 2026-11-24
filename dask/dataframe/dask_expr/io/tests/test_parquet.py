from __future__ import annotations

import os
import pickle

import numpy as np
import pandas as pd
import pytest
from pyarrow import fs

import dask
from dask._compatibility import WINDOWS
from dask.dataframe.dask_expr import from_array, from_graph, from_pandas, read_parquet
from dask.dataframe.dask_expr._expr import Filter, Lengths, Literal
from dask.dataframe.dask_expr._reductions import Len
from dask.dataframe.dask_expr.io import FusedParquetIO, ReadParquet
from dask.dataframe.dask_expr.io.parquet import (
    ReadParquetFSSpec,
    _aggregate_statistics_to_file,
    _combine_stats,
    _extract_stats,
)
from dask.dataframe.io.parquet.utils import (
    PROVENANCE_FILE_COLUMN,
    PROVENANCE_ROW_GROUP_COLUMN,
    PROVENANCE_ROW_POSITION_COLUMN,
)
from dask.dataframe.utils import assert_eq
from dask.utils import key_split

PROVENANCE_COLS = [
    PROVENANCE_FILE_COLUMN,
    PROVENANCE_ROW_GROUP_COLUMN,
    PROVENANCE_ROW_POSITION_COLUMN,
]


def _make_file(dir, df=None, filename="myfile.parquet", **kwargs):
    fn = os.path.join(str(dir), filename)
    if df is None:
        df = pd.DataFrame({c: range(10) for c in "abcde"})
    df.to_parquet(fn, **kwargs)
    return fn


@pytest.fixture
def parquet_file(tmpdir):
    return _make_file(tmpdir)


@pytest.fixture(params=["arrow", "fsspec"])
def filesystem(request):
    return request.param


def test_parquet_len(tmpdir, filesystem):
    df = read_parquet(_make_file(tmpdir), filesystem=filesystem)
    pdf = df.compute()

    assert len(df[df.a > 5]) == len(pdf[pdf.a > 5])

    s = (df["b"] + 1).astype("Int32")
    assert len(s) == len(pdf)

    assert isinstance(Len(s.expr).optimize(), Literal)
    assert isinstance(Lengths(s.expr).optimize(), Literal)


def test_parquet_missing_stats(tmpdir, filesystem):
    _make_file(tmpdir)
    _make_file(tmpdir, write_statistics=["a", "b"], filename="bla.parquet")

    result = read_parquet(tmpdir, filesystem=filesystem)
    expected = pd.concat(
        [
            pd.DataFrame({c: range(10) for c in "abcde"}),
            pd.DataFrame({c: range(10) for c in "abcde"}),
        ]
    )
    assert_eq(result, expected, check_index=False)


@pytest.mark.parametrize("val", [np.nan, 1])
def test_parquet_all_na_column(tmpdir, filesystem, val):
    pdf = pd.DataFrame({"a": [np.nan] * 299 + [val], "b": [1, 2, 3] * 100})
    _make_file(tmpdir, df=pdf, filename="bla.parquet", row_group_size=100)
    result = read_parquet(tmpdir, filesystem=filesystem)
    assert_eq(result, pdf)


def test_parquet_len_filter(tmpdir, filesystem):
    df = read_parquet(_make_file(tmpdir), filesystem=filesystem)
    expr = Len(df[df.c > 0].expr)
    result = expr.simplify()
    for rp in result.find_operations(ReadParquet):
        assert rp.operand("columns") == ["c"] or rp.operand("columns") == []


def test_parquet_len_empty_dir():
    assert len(read_parquet(["empty_dir/*.parquet"])) == 0


@pytest.mark.skipif(WINDOWS, reason="directory empty on windows")
@pytest.mark.parametrize("write_metadata_file", [True, False])
def test_to_parquet(tmpdir, write_metadata_file):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)

    # Check basic parquet round trip
    df.to_parquet(tmpdir, write_metadata_file=write_metadata_file)
    df2 = read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df2)

    # Check overwrite behavior
    df["new"] = df["x"] + 1
    df.to_parquet(tmpdir, overwrite=True, write_metadata_file=write_metadata_file)
    df2 = read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df2)

    # Check that we cannot overwrite a path we are
    # reading from in the same graph
    with pytest.raises(ValueError, match="Cannot overwrite"):
        df2.to_parquet(tmpdir, overwrite=True)


def test_pyarrow_filesystem(parquet_file):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(parquet_file, filesystem=filesystem)
    df = read_parquet(parquet_file)
    assert assert_eq(df, df_pa)
    assert all(tsk.data_producer for tsk in df.dask.values())
    assert all(tsk.data_producer for tsk in df.optimize().dask.values())


@pytest.mark.parametrize("dtype_backend", ["pyarrow", "numpy_nullable", None])
def test_pyarrow_filesystem_dtype_backend(parquet_file, dtype_backend):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(
        parquet_file, filesystem=filesystem, dtype_backend=dtype_backend
    )
    df = read_parquet(parquet_file, dtype_backend=dtype_backend)
    assert assert_eq(df, df_pa)


@pytest.mark.parametrize("types_mapper", [None, lambda x: None])
def test_pyarrow_filesystem_types_mapper(parquet_file, types_mapper):
    # This test isn't doing much other than ensuring the stuff is not raising
    # anywhere
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(
        parquet_file,
        filesystem=filesystem,
        arrow_to_pandas={"types_mapper": types_mapper},
    )
    df = read_parquet(parquet_file, arrow_to_pandas={"types_mapper": types_mapper})
    assert assert_eq(df, df_pa)


def test_pyarrow_filesystem_serialize(parquet_file):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(parquet_file, filesystem=filesystem)

    roundtripped = pickle.loads(pickle.dumps(df_pa.optimize().dask))
    roundtripped_df = from_graph(
        roundtripped,
        df_pa._meta,
        df_pa.divisions,
        df_pa.__dask_keys__(),
        key_split(df_pa._name),
    )
    assert assert_eq(df_pa, roundtripped_df)


def test_pyarrow_filesystem_filters(parquet_file):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(parquet_file, filesystem=filesystem)
    df_pa = df_pa[df_pa.c == 1]
    expected = read_parquet(
        parquet_file, filesystem=filesystem, filters=[[("c", "==", 1)]]
    )
    assert df_pa.optimize()._name == expected.optimize()._name
    assert len(df_pa.compute()) == 1


second_parquet_file = parquet_file


def test_pyarrow_filesystem_list_of_files(parquet_file, second_parquet_file):
    filesystem = fs.LocalFileSystem()

    result = read_parquet([parquet_file, second_parquet_file], filesystem=filesystem)
    expected = pd.read_parquet([parquet_file, second_parquet_file])
    assert_eq(result, expected, check_index=False)


def test_partition_pruning(tmpdir):
    with dask.config.set({"dataframe.parquet.minimum-partition-size": 1}):
        filesystem = fs.LocalFileSystem()
        df = from_pandas(
            pd.DataFrame(
                {
                    "a": [1, 2, 3, 4, 5] * 10,
                    "b": range(50),
                }
            ),
            npartitions=2,
        )
        df.to_parquet(tmpdir, partition_on=["a"])
        ddf = read_parquet(tmpdir, filesystem=filesystem)
        ddf_filtered = read_parquet(
            tmpdir, filters=[[("a", "==", 1)]], filesystem=filesystem
        )
        assert ddf_filtered.npartitions == ddf.npartitions // 5

        ddf_optimize = read_parquet(tmpdir, filesystem=filesystem)
        ddf_optimize = ddf_optimize[ddf_optimize.a == 1].optimize()
        assert ddf_optimize.npartitions == ddf.npartitions // 5
        assert_eq(
            ddf_filtered,
            ddf_optimize,
            # FIXME ?
            check_names=False,
        )


def test_predicate_pushdown(tmpdir):
    original = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, df=original)
    df = read_parquet(fn, filesystem="arrow")
    assert_eq(df, original)
    x = df[df.a == 5][df.c > 20]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr.frame, FusedParquetIO)
    assert ("a", "==", 5) in y.expr.frame.operands[0].operand("filters")[0]
    assert ("c", ">", 20) in y.expr.frame.operands[0].operand("filters")[0]
    assert list(y.columns) == ["b"]

    # Check computed result
    y_result = y.compute()
    assert y_result.name == "b"
    assert len(y_result) == 6
    assert (y_result == 4).all()

    # Don't push down if replace is in there
    x = df[df.replace(5, 50).a == 5]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr, Filter)
    assert len(y.compute()) == 0


def test_predicate_pushdown_compound(tmpdir):
    pdf = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, df=pdf)
    df = read_parquet(fn, filesystem="arrow")

    # Test AND
    x = df[(df.a == 5) & (df.c > 20)]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr.frame, FusedParquetIO)
    assert {("c", ">", 20), ("a", "==", 5)} == set(y.expr.frame.operands[0].filters[0])
    assert_eq(
        y,
        pdf[(pdf.a == 5) & (pdf.c > 20)]["b"],
        check_index=False,
    )

    # Test OR
    x = df[(df.a == 5) | (df.c > 20)]
    x = x[x.b != 0]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr.frame, FusedParquetIO)
    filters = [
        set(y.expr.frame.operands[0].filters[0]),
        set(y.expr.frame.operands[0].filters[1]),
    ]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    expect = pdf[(pdf.a == 5) | (pdf.c > 20)]
    expect = expect[expect.b != 0]["b"]
    assert_eq(
        y,
        expect,
        check_index=False,
    )

    # Test OR and AND
    x = df[((df.a == 5) | (df.c > 20)) & (df.b != 0)]["b"]
    z = x.optimize(fuse=False)
    assert isinstance(z.expr.frame, FusedParquetIO)
    filters = [
        set(z.expr.frame.operands[0].filters[0]),
        set(z.expr.frame.operands[0].filters[1]),
    ]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    assert_eq(y, z)


def test_aggregate_rg_stats_to_file(tmpdir):
    filesystem = fs.LocalFileSystem()
    fn = str(tmpdir)
    ddf = from_pandas(pd.DataFrame({"a": range(10)}), npartitions=1)
    ddf.to_parquet(fn)
    ddf = read_parquet(fn, filesystem=filesystem)
    frag = ddf._expr.fragments[0]
    # Make sure this doesn't raise. We'll test the actual aggregation below
    _aggregate_statistics_to_file([frag.metadata.to_dict()])
    # In reality, we'll strip the metadata down
    assert (
        len(_aggregate_statistics_to_file([_extract_stats(frag.metadata.to_dict())]))
        > 0
    )


def test_aggregate_statistics_to_file():
    file_in = {
        "top-level-file-stat": "not-interested",
        "row_groups": [
            # RG 1
            {
                "num_rows": 100,
                "total_byte_size": 1000,
                "columns": [
                    {
                        "total_compressed_size": 10,
                        "total_uncompressed_size": 15,
                        "statistics": {
                            "min": 0,
                            "max": 10,
                            "null_count": 5,
                            "distinct_count": None,
                            "new_powerful_yet_unknown_stat": 42,
                        },
                        "path_in_schema": "a",
                    },
                    {
                        "total_compressed_size": 7,
                        "total_uncompressed_size": 23,
                        "statistics": {
                            "min": 11,
                            "max": 20,
                            "null_count": 1,
                            "distinct_count": 12,
                            "new_powerful_yet_unknown_stat": 42,
                        },
                        "path_in_schema": "b",
                    },
                ],
            },
            # RG 2
            {
                "num_rows": 50,
                "total_byte_size": 500,
                "columns": [
                    {
                        "total_compressed_size": 5,
                        "total_uncompressed_size": 7,
                        "statistics": {
                            "min": 40,
                            "max": 50,
                            "null_count": 0,
                            "distinct_count": None,
                            "new_powerful_yet_unknown_stat": 42,
                        },
                        "path_in_schema": "a",
                    },
                    {
                        "total_compressed_size": 7,
                        "total_uncompressed_size": 23,
                        "statistics": {
                            "min": 0,
                            "max": 20,
                            "null_count": 0,
                            "distinct_count": None,
                            "new_powerful_yet_unknown_stat": 42,
                        },
                        "path_in_schema": "b",
                    },
                ],
            },
        ],
    }

    expected = {
        "top-level-file-stat": "not-interested",
        "num_rows": 150,
        "total_byte_size": 1500,
        "columns": [
            {
                "path_in_schema": "a",
                "total_compressed_size": 15,
                "total_uncompressed_size": 22,
                "statistics": {
                    "min": 0,
                    "max": 50,
                },
            },
            {
                "path_in_schema": "b",
                "total_compressed_size": 14,
                "total_uncompressed_size": 46,
                "statistics": {
                    "min": 0,
                    "max": 20,
                },
            },
        ],
    }
    actual = _aggregate_statistics_to_file([file_in])
    assert len(actual) == 1
    assert actual[0] == expected


def test_combine_statistics():
    file_in = [
        # File 1
        {
            "top-level-file-stat": "not-interested",
            "num_row_groups": 2,
            "row_groups": [
                # RG 1
                {
                    "num_rows": 200,
                    "total_byte_size": 1000,
                    "columns": [
                        {
                            "total_compressed_size": 10,
                            "total_uncompressed_size": 15,
                            "statistics": {
                                "min": 0,
                                "max": 10,
                                "null_count": 5,
                                "distinct_count": None,
                                "new_powerful_yet_unknown_stat": 42,
                            },
                            "path_in_schema": "a",
                        },
                        {
                            "total_compressed_size": 7,
                            "total_uncompressed_size": 23,
                            "statistics": {
                                "min": 11,
                                "max": 20,
                                "null_count": 1,
                                "distinct_count": 12,
                                "new_powerful_yet_unknown_stat": 42,
                            },
                            "path_in_schema": "b",
                        },
                    ],
                },
                # RG 2
                {
                    "num_rows": 50,
                    "total_byte_size": 500,
                    "columns": [
                        {
                            "total_compressed_size": 5,
                            "total_uncompressed_size": 7,
                            "statistics": {
                                "min": 40,
                                "max": 50,
                                "null_count": 0,
                                "distinct_count": None,
                                "new_powerful_yet_unknown_stat": 42,
                            },
                            "path_in_schema": "a",
                        },
                        {
                            "total_compressed_size": 7,
                            "total_uncompressed_size": 23,
                            "statistics": {
                                "min": 0,
                                "max": 20,
                                "null_count": 0,
                                "distinct_count": None,
                                "new_powerful_yet_unknown_stat": 42,
                            },
                            "path_in_schema": "b",
                        },
                    ],
                },
            ],
        },
        # File 2
        {
            "top-level-file-stat": "not-interested",
            "num_row_groups": 1,
            "row_groups": [
                # RG 1
                {
                    "num_rows": 100,
                    "total_byte_size": 2000,
                    "columns": [
                        {
                            "total_compressed_size": 10,
                            "total_uncompressed_size": 15,
                            "statistics": {
                                "min": 0,
                                "max": 10,
                                "null_count": 5,
                                "distinct_count": None,
                                "new_powerful_yet_unknown_stat": 42,
                            },
                            "path_in_schema": "a",
                        },
                        {
                            "total_compressed_size": 7,
                            "total_uncompressed_size": 23,
                            "statistics": {
                                "min": 11,
                                "max": 20,
                                "null_count": 1,
                                "distinct_count": 12,
                                "new_powerful_yet_unknown_stat": 42,
                            },
                            "path_in_schema": "b",
                        },
                    ],
                },
            ],
        },
    ]
    actual = _combine_stats(file_in)
    expected = {
        "num_rows": ((200 + 50) + 100) // 2,
        "total_byte_size": ((1000 + 500) + 2000) // 2,
        "num_row_groups": 1.5,
        "columns": [
            {
                "total_compressed_size": ((10 + 5) + 10) / 2,
                "total_uncompressed_size": ((15 + 7) + 15) / 2,
                "path_in_schema": "a",
            },
            {
                "total_compressed_size": ((7 + 7) + 7) / 2,
                "total_uncompressed_size": ((23 + 23) + 23) / 2,
                "path_in_schema": "b",
            },
        ],
    }
    assert actual == expected


def test_index_only_from_parquet(tmpdir):
    pdf = pd.DataFrame({"foo": range(5)}, index=range(50, 55))
    pdf.to_parquet(f"{tmpdir}/test.parquet")
    result = read_parquet(f"{tmpdir}/test.parquet").index
    assert_eq(result, pdf.index)


def test_timestamp_divisions(tmpdir):
    pdf = pd.DataFrame.from_dict(
        {
            "Date": ["11/26/2017", "11/26/2017"],
            "Time": ["17:00:00.067", "17:00:00.102"],
            "Volume": [403, 3],
        }
    )
    pdf["Timestamp"] = pd.to_datetime(pdf.Date) + pd.to_timedelta(pdf.Time)
    pdf.to_parquet(f"{tmpdir}/test.parquet")
    df = read_parquet(
        f"{tmpdir}/test.parquet", index="Timestamp", calculate_divisions=True
    )["Volume"]
    assert df.optimize().divisions == (
        pd.Timestamp("2017-11-26 17:00:00.067000"),
        pd.Timestamp("2017-11-26 17:00:00.102000"),
    )


def test_read_parquet_index_projection(tmpdir):
    df = from_array(
        np.zeros((201 * 10, 8), dtype=np.int64),
        columns=["val1", "val2", "val3", "val4", "val5", "val6", "val7", "tsprv"],
    )
    df = df.repartition(npartitions=201)
    df.to_parquet(f"{tmpdir}/index", write_index=False)

    df = read_parquet(f"{tmpdir}/index", calculate_divisions=True)
    result = df.assign(dts=df.index - df.tsprv)
    expected = df.compute()
    expected = expected.assign(dts=expected.index - expected.tsprv)

    assert_eq(result.dts.min(), expected.dts.min())


@pytest.mark.filterwarnings(
    # The stats collection encounters the async client and falls back to a
    # sync scheduler. Irrelevant for this test.
    "ignore:.*Client.*asynchronous:UserWarning",
)
def test_ensure_plan_computed_during_optimization(tmpdir, filesystem):

    pytest.importorskip("distributed")
    from distributed.utils_test import gen_cluster

    @gen_cluster(
        client=True,
        clean_kwargs={"threads": False},
        config={"admin.async-client-fallback": "sync"},
    )
    async def _test(c, s, *workers):
        # https://github.com/dask/dask/issues/11932

        df = read_parquet(_make_file(tmpdir), filesystem=filesystem)
        df = df[df.a == 1]
        res = await c.compute(df.size)
        assert res == 5

    _test()


def test_tune_optimization_disabled(tmpdir):
    """Test that optimization.tune.active=False preserves the original partition structure."""
    n_partitions = 10
    pdf = pd.DataFrame(
        {
            "x": np.random.randn(1000),
            "y": np.random.randn(1000),
        }
    )
    ddf = from_pandas(pdf, npartitions=n_partitions)

    # Write parquet with multiple partitions
    path = str(tmpdir.join("test.parquet"))
    ddf.to_parquet(path)

    # Read with tune optimization disabled via config
    with dask.config.set({"optimization.tune.active": False}):
        result = read_parquet(path)

        # Verify that the number of partitions is preserved
        assert result.npartitions == n_partitions

        # Verify that map_partitions(len) gives consistent results
        # for both the full DataFrame and a column selection
        full_lens = result.map_partitions(len).compute()
        col_lens = result["x"].map_partitions(len).compute()

        assert len(full_lens) == n_partitions
        assert len(col_lens) == n_partitions
        assert_eq(full_lens, col_lens)


def _make_provenance_file(dir, filename="data.parquet", biz_prefix="A"):
    """Write a single parquet file with two row groups.

    The first row group contains no matching rows at all. The second row
    group contains a single matching row (``status == "hit"``) sandwiched
    between two non-matching rows. Note that the ``amount`` column (and,
    when splitting by row-group, also the pandas index) contains
    duplicates across the row groups, so the provenance columns are the
    only way to tell some rows apart.
    """
    fn = os.path.join(str(dir), filename)
    pdf = pd.DataFrame(
        {
            "biz_id": [f"{biz_prefix}{i}" for i in range(6)],
            "amount": [100.0, 200.0, 300.0, 100.0, 200.0, 300.0],
            "status": ["miss", "miss", "miss", "miss", "hit", "miss"],
            "flag": [0, 0, 0, 0, 1, 0],
        }
    )
    pdf.to_parquet(fn, row_group_size=3, index=False)
    return fn


def _read_original_row(file_path, row_group, row_position):
    """Read a single row back from the source file using provenance coordinates."""
    import pyarrow.parquet as pq

    table = pq.ParquetFile(file_path).read_row_group(row_group)
    return table.slice(row_position, 1).to_pandas()


def test_read_parquet_row_provenance_columns(tmpdir, filesystem):
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True, filesystem=filesystem)
    assert list(df.columns) == ["biz_id", "amount", "status", "flag"] + PROVENANCE_COLS

    result = df.compute()
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [0, 0, 0, 1, 1, 1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [0, 1, 2, 0, 1, 2]
    assert {os.path.realpath(p) for p in result[PROVENANCE_FILE_COLUMN]} == {
        os.path.realpath(fn)
    }
    assert result[PROVENANCE_ROW_GROUP_COLUMN].dtype == "int64"
    assert result[PROVENANCE_ROW_POSITION_COLUMN].dtype == "int64"


def test_read_parquet_row_provenance_filter_pushdown(tmpdir, filesystem):
    # The first row group does not match at all; the matching row sits
    # between two non-matching rows in the second row group. After filter
    # pushdown and column projection, the reported position must still be
    # the original zero-based position within the second row group.
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True, filesystem=filesystem)
    result = df[df.status == "hit"][["biz_id", "amount"] + PROVENANCE_COLS].compute()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["biz_id"] == "A4"
    assert row["amount"] == 200.0
    assert row[PROVENANCE_ROW_GROUP_COLUMN] == 1
    # Zero-based position within the original row group, not within
    # the filtered result (which would be 0)
    assert row[PROVENANCE_ROW_POSITION_COLUMN] == 1

    # The coordinates must retrieve the same row from the source file
    original = _read_original_row(
        row[PROVENANCE_FILE_COLUMN],
        row[PROVENANCE_ROW_GROUP_COLUMN],
        row[PROVENANCE_ROW_POSITION_COLUMN],
    )
    assert original["biz_id"].iloc[0] == row["biz_id"]
    assert original["amount"].iloc[0] == row["amount"]


@pytest.mark.parametrize("split_row_groups", [False, True])
def test_read_parquet_row_provenance_split_row_groups(tmpdir, split_row_groups):
    # Whole-file partitions and per-row-group partitions must report
    # identical provenance for the same data
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True, split_row_groups=split_row_groups)
    result = df[df.status == "hit"].compute()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["biz_id"] == "A4"
    assert row[PROVENANCE_ROW_GROUP_COLUMN] == 1
    assert row[PROVENANCE_ROW_POSITION_COLUMN] == 1


def test_read_parquet_row_provenance_filter_projection_order(tmpdir, filesystem):
    # Filtering first and then requesting the positions must give the same
    # provenance as requesting the positions first and then filtering
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True, filesystem=filesystem)
    cols = ["biz_id", "amount"] + PROVENANCE_COLS

    filter_first = df[df.status == "hit"][cols]
    project_first = df[cols + ["status"]]
    project_first = project_first[project_first.status == "hit"][cols]
    assert_eq(filter_first, project_first)

    result = filter_first.compute()
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [1]


def test_read_parquet_row_provenance_follows_rows(tmpdir, filesystem):
    # Re-ordering the business rows must not detach the provenance
    # columns from their rows
    _make_provenance_file(tmpdir, filename="part.0.parquet", biz_prefix="A")
    _make_provenance_file(tmpdir, filename="part.1.parquet", biz_prefix="B")
    df = read_parquet(str(tmpdir), add_row_provenance=True, filesystem=filesystem)

    base = df.compute().set_index("biz_id")
    reordered = df.shuffle("biz_id").compute().set_index("biz_id")
    reordered = reordered.loc[base.index]
    assert_eq(
        base[PROVENANCE_COLS],
        reordered[PROVENANCE_COLS],
        check_index=False,
    )


def test_read_parquet_row_provenance_distinguishes_duplicate_rows(tmpdir):
    # Two original rows can share the same pandas index and the same
    # amount, and only differ in the column used for filtering. The
    # provenance columns must still identify each of them uniquely.
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True, split_row_groups=True)
    result = df[df.amount == 200.0].compute()

    # Both rows have amount == 200.0 and (after filtering) the same
    # pandas index within their respective partitions
    assert len(result) == 2
    assert result.index.tolist() == [0, 0]
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [0, 1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [1, 1]
    for _, row in result.iterrows():
        original = _read_original_row(
            row[PROVENANCE_FILE_COLUMN],
            row[PROVENANCE_ROW_GROUP_COLUMN],
            row[PROVENANCE_ROW_POSITION_COLUMN],
        )
        assert original["biz_id"].iloc[0] == row["biz_id"]


def test_read_parquet_row_provenance_row_group_skipping(tmpdir):
    # Row groups that cannot match the filters according to their
    # statistics must actually be skipped, i.e. never be read
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True)
    df = df[df.flag == 1]

    (read_op,) = df.optimize().expr.find_operations(ReadParquetFSSpec)
    row_groups = []
    for part in read_op._plan["parts"]:
        for piece in [part] if isinstance(part, dict) else part:
            row_groups.extend(piece["piece"][1])
    # The first row group only contains flag == 0 and must have been
    # excluded based on the parquet statistics
    assert row_groups == [1]

    result = df.compute()
    assert len(result) == 1
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [1]


def test_read_parquet_row_provenance_column_projection(tmpdir):
    # Only the business columns, the provenance columns, and the columns
    # needed to evaluate the filters may be read
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True)
    df = df[df.status == "hit"][["biz_id", "amount"] + PROVENANCE_COLS]

    (read_op,) = df.optimize().expr.find_operations(ReadParquetFSSpec)
    assert set(read_op.columns) == {"biz_id", "amount"} | set(PROVENANCE_COLS)
    assert read_op.filters == [[("status", "==", "hit")]]

    result = df.compute()
    assert list(result.columns) == ["biz_id", "amount"] + PROVENANCE_COLS
    assert result["biz_id"].tolist() == ["A4"]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [1]


def test_read_parquet_row_provenance_only_provenance_columns(tmpdir, filesystem):
    # Selecting only the provenance columns must not read any data column
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True, filesystem=filesystem)
    result = df[PROVENANCE_COLS].compute()

    assert list(result.columns) == PROVENANCE_COLS
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [0, 0, 0, 1, 1, 1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [0, 1, 2, 0, 1, 2]


def test_read_parquet_row_provenance_explicit_columns(tmpdir):
    # Provenance columns are appended to an explicit column projection
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, columns=["biz_id", "amount"], add_row_provenance=True)
    assert list(df.columns) == ["biz_id", "amount"] + PROVENANCE_COLS

    result = df.compute()
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [0, 0, 0, 1, 1, 1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [0, 1, 2, 0, 1, 2]


def test_read_parquet_row_provenance_multiple_files(tmpdir, filesystem):
    _make_provenance_file(tmpdir, filename="part.0.parquet")
    _make_provenance_file(tmpdir, filename="part.1.parquet")
    df = read_parquet(str(tmpdir), add_row_provenance=True, filesystem=filesystem)

    result = df[df.status == "hit"].compute()
    assert len(result) == 2
    assert {os.path.basename(p) for p in result[PROVENANCE_FILE_COLUMN]} == {
        "part.0.parquet",
        "part.1.parquet",
    }
    for _, row in result.iterrows():
        original = _read_original_row(
            row[PROVENANCE_FILE_COLUMN],
            row[PROVENANCE_ROW_GROUP_COLUMN],
            row[PROVENANCE_ROW_POSITION_COLUMN],
        )
        assert original["biz_id"].iloc[0] == row["biz_id"]


def test_read_parquet_row_provenance_filter_on_provenance_column(tmpdir):
    # Predicates on the provenance columns cannot be pushed into the
    # parquet scan, but they must still work as ordinary filters
    fn = _make_provenance_file(tmpdir)
    df = read_parquet(fn, add_row_provenance=True)
    result = df[df[PROVENANCE_ROW_GROUP_COLUMN] == 1].compute()

    assert len(result) == 3
    assert result[PROVENANCE_ROW_GROUP_COLUMN].tolist() == [1, 1, 1]
    assert result[PROVENANCE_ROW_POSITION_COLUMN].tolist() == [0, 1, 2]
