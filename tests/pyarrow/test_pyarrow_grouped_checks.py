"""PyArrow-specific grouped admission check tests."""

import pyarrow as pa
import pytest

import pandera.pyarrow as pap
from pandera.api.grouped import GroupedCheck, GroupedCheckLengthError

COLUMNS = ["region", "device", "traffic", "delay"]
ROWS = [
    (None, "X", 4, 12),
    ("None", "X", 2, 8),
    ("None", "Y", 5, 8),
    (None, "W", 5, 8),
    ("west", "M", 5, 8),
    ("west", "N", 5, 8),
    (None, "Z", 5, 8),
]


def _table(chunk_sizes=None):
    columns = list(zip(*ROWS))
    types = [pa.string(), pa.string(), pa.int64(), pa.int64()]
    arrays = []
    for values, dtype in zip(columns, types):
        if chunk_sizes is None:
            arrays.append(pa.array(values, type=dtype))
        else:
            start = 0
            chunks = []
            for size in chunk_sizes:
                chunks.append(list(values[start : start + size]))
                start += size
            arrays.append(pa.chunked_array(chunks, type=dtype))
    return pa.table(dict(zip(COLUMNS, arrays)))


def _schema():
    return pap.DataFrameSchema(
        grouped_checks=[
            pap.GroupedCheck(
                lambda group: group["delay"] <= 10,
                groupby="region",
                name="delay",
            ),
            pap.GroupedCheck(
                lambda group: group["traffic"].sum() >= 5,
                groupby="device",
                name="device",
            ),
            pap.GroupedCheck(
                lambda group: group["device"].nunique() == 2,
                groupby="region",
                name="dual",
            ),
        ]
    )


@pytest.mark.parametrize(
    "chunk_sizes",
    [None, [3, 4], [1, 2, 4], [2, 5], [7]],
)
def test_chunking_does_not_change_outcome(chunk_sizes):
    result = _schema().validate_grouped(_table(chunk_sizes))
    assert result.table.to_pylist() == [
        {"region": "west", "device": "M", "traffic": 5, "delay": 8},
        {"region": "west", "device": "N", "traffic": 5, "delay": 8},
    ]
    assert {
        rejection.position: (rejection.round, set(rejection.rules))
        for rejection in result.rejections
    } == {
        0: (1, {"delay", "dual"}),
        3: (1, {"dual"}),
        6: (1, {"dual"}),
        1: (2, {"device"}),
        2: (3, {"dual"}),
    }


def test_accepted_rows_are_a_pyarrow_table():
    result = _schema().validate_grouped(_table())
    assert isinstance(result.table, pa.Table)


def test_accepted_rows_keep_original_column_order():
    result = _schema().validate_grouped(_table())
    assert result.table.column_names == COLUMNS


def test_all_rows_rejected_returns_empty_table():
    schema = pap.DataFrameSchema(
        grouped_checks=[
            pap.GroupedCheck(lambda group: False, "region", name="never")
        ]
    )
    result = schema.validate_grouped(_table())
    assert result.table.num_rows == 0
    assert len(result.rejections) == len(ROWS)


def test_wrong_length_reports_rule_group_round():
    def bad(group):
        return [True]

    table = pa.table({"region": ["west", "west"], "v": [1, 2]})
    schema = pap.DataFrameSchema(
        grouped_checks=[GroupedCheck(bad, "region", name="bad")]
    )
    with pytest.raises(GroupedCheckLengthError) as exc_info:
        schema.validate_grouped(table)
    assert exc_info.value.rule == "bad"
    assert exc_info.value.group_key == "west"
    assert exc_info.value.round == 1


def test_returned_pyarrow_boolean_vector():
    def per_row(group):
        return pa.array([value <= 1 for value in group["v"]])

    table = pa.table({"region": ["a", "a"], "v": [1, 2]})
    result = pap.DataFrameSchema(
        grouped_checks=[GroupedCheck(per_row, "region", name="v")]
    ).validate_grouped(table)
    assert result.table.to_pylist() == [{"region": "a", "v": 1}]


def test_returned_pyarrow_boolean_scalar():
    table = pa.table({"region": ["a", "a"], "v": [1, 2]})
    result = pap.DataFrameSchema(
        grouped_checks=[
            GroupedCheck(lambda group: pa.scalar(False), "region", name="v")
        ]
    ).validate_grouped(table)
    assert result.table.num_rows == 0
