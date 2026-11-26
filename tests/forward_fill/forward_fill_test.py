from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

import narwhals as nw
from tests.utils import assert_equal_data

if TYPE_CHECKING:
    from collections.abc import Callable

    from narwhals._native import IntoDataFrame


def _pandas(data: dict[str, list[Any]]) -> IntoDataFrame:
    import pandas as pd

    return pd.DataFrame(data)


def _arrow(data: dict[str, list[Any]]) -> IntoDataFrame:
    import pyarrow as pa

    return pa.table(data)


def _typed_null_pandas(data: dict[str, list[Any]]) -> IntoDataFrame:
    import pandas as pd

    frame = pd.DataFrame(data)
    frame["value"] = pd.array(frame["value"], dtype="Float64")
    return frame


def _typed_null_arrow(data: dict[str, list[Any]]) -> IntoDataFrame:
    import pyarrow as pa

    return pa.table({**data, "value": pa.array(data["value"], type=pa.float64())})


BACKENDS: list[Callable[[dict[str, list[Any]]], IntoDataFrame]] = [
    pytest.param(_pandas, id="pandas"),
    pytest.param(_arrow, id="pyarrow"),
]


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_forward_fill_limit_is_shared_across_shards(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    # B has no local signal in its head: those rows stay pending until A lands.
    a = nw.from_native(
        native_factory({"device": ["x"], "value": [11], "seq": [10], "shutdown": [False]})
    )
    b = nw.from_native(
        native_factory(
            {
                "device": ["x"] * 5,
                "value": [None, None, None, 0, None],
                "seq": [20, 21, 22, 23, 24],
                "shutdown": [False, False, True, False, False],
            }
        )
    )
    result_a = nw.forward_fill(
        a,
        "value",
        partition_by="device",
        order_by="seq",
        limit=2,
        block_on="shutdown",
        shard="A",
    )
    result_b = nw.forward_fill(
        b,
        "value",
        partition_by="device",
        order_by="seq",
        limit=2,
        block_on="shutdown",
        shard="B",
    )
    assert_equal_data(
        result_b.frame,
        {
            "device": ["x"] * 5,
            "value": [None, None, None, 0, 0],
            "seq": [20, 21, 22, 23, 24],
            "shutdown": [False, False, True, False, False],
        },
    )
    assert result_b.provenance["pending"].to_list() == [True, True, False, False, False]

    resolved = result_b.resolve(result_a.state)
    assert_equal_data(
        resolved.frame,
        {
            "device": ["x"] * 5,
            "value": [11, 11, None, 0, 0],
            "seq": [20, 21, 22, 23, 24],
            "shutdown": [False, False, True, False, False],
        },
    )
    assert resolved.provenance["source_shard"].to_list()[:2] == ["A", "A"]
    assert resolved.provenance["source_shard"].to_list()[3:] == ["B", "B"]
    assert resolved.provenance["source_gap"].to_list()[:2] == [1, 2]
    assert resolved.provenance["source_gap"].to_list()[3:] == [0, 1]
    assert resolved.provenance["source_kind"].to_list() == [
        "fill",
        "fill",
        "none",
        "signal",
        "fill",
    ]


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_late_sample_revokes_only_affected_prefix(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    a = {"device": ["x"], "value": [11], "seq": [10], "shutdown": [False]}
    b = {
        "device": ["x"] * 5,
        "value": [None, None, None, 0, None],
        "seq": [20, 21, 22, 23, 24],
        "shutdown": [False, False, True, False, False],
    }
    # A late sample at seq 12 spends the second fill in A itself.
    a_late = {
        "device": ["x"] * 2,
        "value": [11, None],
        "seq": [10, 12],
        "shutdown": [False, False],
    }
    result_b = nw.forward_fill(
        nw.from_native(native_factory(b)),
        "value",
        partition_by="device",
        order_by="seq",
        block_on="shutdown",
        shard="B",
    )
    before = nw.forward_fill(
        nw.from_native(native_factory(a)),
        "value",
        partition_by="device",
        order_by="seq",
        shard="A",
    ).state
    after = nw.forward_fill(
        nw.from_native(native_factory(a_late)),
        "value",
        partition_by="device",
        order_by="seq",
        shard="A",
    ).state
    assert_equal_data(
        result_b.resolve(before).frame,
        {
            "device": ["x"] * 5,
            "value": [11, 11, None, 0, 0],
            "seq": [20, 21, 22, 23, 24],
            "shutdown": [False, False, True, False, False],
        },
    )
    # Only the first row keeps 11; row two was the one the extra sample revoked.
    assert_equal_data(
        result_b.resolve(after).frame,
        {
            "device": ["x"] * 5,
            "value": [11, None, None, 0, 0],
            "seq": [20, 21, 22, 23, 24],
            "shutdown": [False, False, True, False, False],
        },
    )


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_shutdown_blocks_until_next_real_signal(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    df = nw.from_native(
        native_factory(
            {
                "value": [11, None, None, None, 0, None],
                "shutdown": [False, False, True, False, False, False],
            }
        )
    )
    result = nw.forward_fill(df, "value", block_on="shutdown", limit=None)
    assert_equal_data(
        result.frame,
        {
            "value": [11, 11, None, None, 0, 0],
            "shutdown": [False, False, True, False, False, False],
        },
    )


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_devices_are_independent(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    df = nw.from_native(
        native_factory(
            {
                "device": ["A", "A", "B", "B", "A"],
                "value": [11, None, None, None, None],
                "seq": [0, 1, 1, 2, 2],
            }
        )
    )
    result = nw.forward_fill(df, "value", partition_by="device", order_by="seq")
    assert_equal_data(
        result.frame,
        {
            "device": ["A", "A", "B", "B", "A"],
            "value": [11, 11, None, None, 11],
            "seq": [0, 1, 1, 2, 2],
        },
    )
    assert set(result.state.keys()) == {("A",), ("B",)}


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_state_merge_is_associative_and_skips_rereads(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    one = nw.from_native(native_factory({"value": [11.0, None], "seq": [0, 1]}))
    two = nw.from_native(native_factory({"value": [None, None, 0.0], "seq": [2, 3, 4]}))
    typed_null = _typed_null_pandas if native_factory is _pandas else _typed_null_arrow
    three = nw.from_native(typed_null({"value": [None, None], "seq": [5, 6]}))
    s1 = nw.forward_fill(one, "value", order_by="seq", shard="1").state
    s2 = nw.forward_fill(two, "value", order_by="seq", shard="2").state
    s3 = nw.forward_fill(three, "value", order_by="seq", shard="3").state
    assert (s1.merge(s2)).merge(s3) == s1.merge(s2.merge(s3))
    result = nw.forward_fill(three, "value", order_by="seq", shard="3").resolve(
        s1.merge(s2)
    )
    assert result.frame["value"].to_list() == [0.0, 0.0]
    assert result.frame["seq"].to_list() == [5, 6]


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_no_signal_before_shard_stays_null(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    df = nw.from_native(native_factory({"value": [None, None], "seq": [0, 1]}))
    result = nw.forward_fill(df, "value", order_by="seq")
    assert_equal_data(result.frame, {"value": [None, None], "seq": [0, 1]})
    assert result.provenance["pending"].to_list() == [True, True]
    assert len(result.state.keys()) == 1


def test_merge_rejects_mismatched_configuration() -> None:
    pytest.importorskip("pandas")
    import pandas as pd

    df = nw.from_native(pd.DataFrame({"value": [1, None], "seq": [0, 1]}))
    partitioned = nw.forward_fill(df, "value", partition_by="seq", order_by="seq").state
    unpartitioned = nw.forward_fill(df, "value", order_by="seq").state
    with pytest.raises(ValueError, match="different `partition_by`"):
        partitioned.merge(unpartitioned)


def test_invalid_options() -> None:
    pytest.importorskip("pandas")
    import pandas as pd

    df = nw.from_native(pd.DataFrame({"value": [1, None]}))
    with pytest.raises(ValueError, match="non-negative integer"):
        nw.forward_fill(df, "value", limit=-1)
    with pytest.raises(ValueError, match="non-negative integer"):
        nw.forward_fill(df, "value", limit=True)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="`column` must be a string"):
        nw.forward_fill(df, 1)  # type: ignore[arg-type]
    with pytest.raises(nw.exceptions.ColumnNotFoundError):
        nw.forward_fill(df, "missing")


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_shutdown_blocks_cross_shard_carry(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    earlier = nw.from_native(
        native_factory(
            {"device": ["x"], "value": [11.0], "seq": [0], "shutdown": [False]}
        )
    )
    later = nw.from_native(
        native_factory(
            {
                "device": ["x"] * 4,
                "value": [None, None, None, None],
                "seq": [1, 2, 3, 4],
                "shutdown": [False, True, False, False],
            }
        )
    )
    prefix = nw.forward_fill(
        earlier,
        "value",
        partition_by="device",
        order_by="seq",
        limit=None,
        block_on="shutdown",
        shard="earlier",
    ).state
    batch = nw.forward_fill(
        later,
        "value",
        partition_by="device",
        order_by="seq",
        limit=None,
        block_on="shutdown",
        shard="later",
    )
    # Nothing after the shutdown is pending: an earlier signal must not leak
    # across it, even without a `limit`.
    assert batch.provenance["pending"].to_list() == [True, False, False, False]
    resolved = batch.resolve(prefix).frame["value"].to_list()
    assert resolved[0] == 11.0
    assert all(value is None or value != value for value in resolved[1:])  # noqa: PLR0124


@pytest.mark.parametrize("native_factory", BACKENDS)
def test_empty_frame_round_trips(
    native_factory: Callable[[dict[str, list[Any]]], IntoDataFrame],
) -> None:
    empty = nw.from_native(native_factory({"device": [], "value": [], "seq": []}))
    result = nw.forward_fill(
        empty, "value", partition_by="device", order_by="seq", shard="empty"
    )
    assert result.frame.shape == (0, 3)
    assert result.provenance.shape == (0, 8)
    assert result.state.keys() == ((),)
    assert result.state.merge(result.state) == result.state
