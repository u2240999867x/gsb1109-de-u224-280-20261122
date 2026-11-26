"""Tests for round-based grouped admission checks.

The same callbacks are exercised against pandas and pyarrow inputs so that the
shared-callback contract, row identity, round semantics, and rejection list
stay backend-independent.
"""

import pandas as pd
import pyarrow as pa
import pytest

import pandera.pandas as pp
import pandera.pyarrow as pap
from pandera.api.grouped import GroupedCheck, GroupedCheckLengthError


ROWS = [
    (None, "X", 4, 12),  # A
    ("None", "X", 2, 8),  # B
    ("None", "Y", 5, 8),  # C
    (None, "W", 5, 8),  # D
    ("west", "M", 5, 8),  # E
    ("west", "N", 5, 8),  # F
    (None, "Z", 5, 8),  # G
]
COLUMNS = ["region", "device", "traffic", "delay"]


def _delay_rule(records):
    return records["delay"] <= 10


def _device_rule(records):
    return records["traffic"].sum() >= 5


def _dual_rule(records):
    return records["device"].nunique() == 2


def _schema(df_backend):
    """Build the three-rule example contract for the requested API."""
    api = pp if df_backend == "pandas" else pap
    return api.DataFrameSchema(
        grouped_checks=[
            api.GroupedCheck(_delay_rule, groupby="region", name="delay"),
            api.GroupedCheck(_device_rule, groupby="device", name="device"),
            api.GroupedCheck(_dual_rule, groupby="region", name="dual"),
        ]
    )


def _pandas_frame():
    frame = pd.DataFrame(ROWS, columns=COLUMNS)
    # D (position 3) and E (position 4) share the display index 2.
    frame.index = [0, 1, 2, 2, 3, 4, 5]
    return frame


def _pyarrow_table(chunked=False):
    columns = list(zip(*ROWS))
    types = [pa.string(), pa.string(), pa.int64(), pa.int64()]
    if chunked:
        arrays = [
            pa.chunked_array(
                [list(value[:3]), list(value[3:])], type=dtype
            )
            for value, dtype in zip(columns, types)
        ]
    else:
        arrays = [
            pa.array(value, type=dtype)
            for value, dtype in zip(columns, types)
        ]
    return pa.table(dict(zip(COLUMNS, arrays)))


def _rejection_summary(result, backend="pandas"):
    if backend == "pandas":
        positions = [r.position for r in result.rejections]
    else:
        positions = [r.position for r in result.rejections]
    return {
        position: (rejection.round, set(rejection.rules))
        for position, rejection in zip(positions, result.rejections)
    }


@pytest.mark.parametrize("chunked", [False, True])
def test_example_rounds_and_final_rows(chunked):
    """A/D/G go in round 1, B in round 2, C in round 3; E/F survive."""
    if chunked is None:  # pragma: no cover - parametrization placeholder
        return
    table = _pyarrow_table(chunked)
    result = _schema("pyarrow").validate_grouped(table)

    assert result.table.to_pylist() == [
        {
            "region": "west",
            "device": "M",
            "traffic": 5,
            "delay": 8,
        },
        {
            "region": "west",
            "device": "N",
            "traffic": 5,
            "delay": 8,
        },
    ]

    summary = _rejection_summary(result)
    assert summary[0] == (1, {"delay", "dual"})
    assert summary[3] == (1, {"dual"})
    assert summary[6] == (1, {"dual"})
    assert summary[1] == (2, {"device"})
    assert summary[2] == (3, {"dual"})
    assert result.rounds == 3


def test_pandas_matches_pyarrow():
    pandas_result = _schema("pandas").validate_grouped(_pandas_frame())
    arrow_result = _schema("pyarrow").validate_grouped(_pyarrow_table())

    # Accepted rows preserve original order and equal values.
    assert pandas_result.table.reset_index(drop=True).to_dict("records") == (
        arrow_result.table.to_pylist()
    )
    assert _rejection_summary(pandas_result) == _rejection_summary(
        arrow_result
    )


def test_rules_share_the_round_start_snapshot():
    """Within a round every rule sees the rows present at round start."""
    frame = _pandas_frame()
    seen = {}

    def device(group):
        seen["device"] = tuple(group["device"])
        return group["traffic"].sum() >= 5

    def dual(group):
        # The X group still has both rows in round 1 because the delay rule's
        # round-1 eliminations have not been applied yet.
        seen.setdefault("dual_round1", tuple(group["device"]))
        return group["device"].nunique() == 2

    schema = pp.DataFrameSchema(
        grouped_checks=[
            GroupedCheck(_delay_rule, "region", name="delay"),
            GroupedCheck(dual, "region", name="dual"),
        ]
    )
    schema.validate_grouped(frame)
    assert seen["dual_round1"] == ("X", "W", "Z")


def test_accepted_table_keeps_column_order():
    frame = _pandas_frame()
    result = _schema("pandas").validate_grouped(frame)
    assert list(result.table.columns) == COLUMNS


def test_passing_group_reruns_after_membership_change():
    frame = pd.DataFrame(
        {"region": ["a", "a", "a"], "v": [1, 2, 3], "size": [3, 3, 3]}
    )
    calls = []

    def drop_big(group):
        # Keep only the first row each round: membership shrinks over time.
        return [index == 0 for index in range(len(group))]

    def needs_all(group):
        calls.append(len(group))
        return len(group) >= 4

    schema = pp.DataFrameSchema(
        grouped_checks=[
            GroupedCheck(drop_big, "region", name="drop"),
            GroupedCheck(needs_all, "region", name="all"),
        ]
    )
    result = schema.validate_grouped(frame)
    # Round 1: 3 members (fails needs_all, all three removed by drop_big too)
    assert calls == [3]
    assert len(result.table) == 0



def test_same_callback_object_shared_across_backends():
    """Identical callback objects run unchanged on pandas and pyarrow."""
    seen = []

    def shared(group):
        seen.append(tuple(group["device"]))
        return group["traffic"].sum() >= 5

    pandas_schema = pp.DataFrameSchema(
        grouped_checks=[GroupedCheck(shared, "device", name="d")]
    )
    arrow_schema = pap.DataFrameSchema(
        grouped_checks=[GroupedCheck(shared, "device", name="d")]
    )
    pandas_result = pandas_schema.validate_grouped(
        pd.DataFrame(ROWS, columns=COLUMNS)
    )
    first_run = list(seen)
    seen.clear()
    arrow_result = arrow_schema.validate_grouped(_pyarrow_table())

    pandas_rows = pandas_result.table[["device", "traffic", "delay"]].to_dict(
        "records"
    )
    arrow_rows = [
        {key: row[key] for key in ("device", "traffic", "delay")}
        for row in arrow_result.table.to_pylist()
    ]
    assert pandas_rows == arrow_rows
    # The same ``GroupRecords`` shape and original row order reach the shared
    # callback on both backends.
    expected = [("X", "X"), ("Y",), ("W",), ("M",), ("N",), ("Z",)]
    assert first_run == expected
    assert seen == expected


def test_null_and_text_none_are_distinct_groups():
    """The null region key and the text ``None`` form separate groups."""
    frame = pd.DataFrame(
        [(None, "X", 4, 8), ("None", "X", 4, 8)], columns=COLUMNS
    )
    seen = []

    def record(records):
        seen.append((records[0]["region"], len(records)))
        return True

    schema = pp.DataFrameSchema(
        grouped_checks=[GroupedCheck(record, "region")]
    )
    schema.validate_grouped(frame)
    assert (None, 1) in seen
    assert ("None", 1) in seen


def test_duplicate_display_index_does_not_merge_rows():
    frame = _pandas_frame()
    result = _schema("pandas").validate_grouped(frame)
    # Only E and F survive; they carry the original labels 3 and 4.
    assert list(result.table.index) == [3, 4]
    assert len(frame) == 7


def test_rule_order_does_not_change_outcome():
    reordered = pp.DataFrameSchema(
        grouped_checks=[
            GroupedCheck(_dual_rule, "region", name="dual"),
            GroupedCheck(_device_rule, "device", name="device"),
            GroupedCheck(_delay_rule, "region", name="delay"),
        ]
    )
    result = reordered.validate_grouped(_pandas_frame())
    assert [r.position for r in result.rejections] == [0, 1, 2, 3, 6]
    assert result.rejections[0].rules == ("delay", "dual")


def test_groups_only_rerun_when_membership_changes():
    """The west region and M/N device callbacks run exactly once."""
    frame = _pandas_frame()
    calls = {"region": {}, "device": {}}

    def delay(records):
        region = records[0]["region"]
        calls["region"][region] = calls["region"].get(region, 0) + 1
        return records["delay"] <= 10

    def device(records):
        name = records[0]["device"]
        calls["device"][name] = calls["device"].get(name, 0) + 1
        return records["traffic"].sum() >= 5

    schema = pp.DataFrameSchema(
        grouped_checks=[
            GroupedCheck(delay, "region", name="delay"),
            GroupedCheck(device, "device", name="device"),
        ]
    )
    schema.validate_grouped(frame)

    assert calls["region"]["west"] == 1
    assert calls["device"]["M"] == 1
    assert calls["device"]["N"] == 1


def test_empty_group_is_not_called_again():
    calls = []

    def always_fail(records):
        calls.append(tuple(records))
        return False

    frame = pd.DataFrame({"region": ["a", "a"], "v": [1, 2]})
    schema = pp.DataFrameSchema(
        grouped_checks=[GroupedCheck(always_fail, "region", name="r")]
    )
    schema.validate_grouped(frame)
    assert calls == [(frame.iloc[0].to_dict(), frame.iloc[1].to_dict())]


def test_group_scalar_true_passes_all_rows():
    frame = pd.DataFrame({"region": ["a", "a"], "v": [1, 2]})
    result = _schema_with(
        GroupedCheck(lambda records: True, "region", name="ok")
    ).validate_grouped(frame)
    assert len(result.table) == 2
    assert result.rejections == []


def _schema_with(check):
    return pp.DataFrameSchema(grouped_checks=[check])


@pytest.mark.parametrize("returned", [["ok"], [True]])
def test_wrong_length_reports_rule_group_and_round(returned):
    def bad(records):
        return returned

    frame = pd.DataFrame({"region": ["west", "west"], "v": [1, 2]})
    schema = pp.DataFrameSchema(
        grouped_checks=[GroupedCheck(bad, "region", name="bad")]
    )
    with pytest.raises(GroupedCheckLengthError) as exc_info:
        schema.validate_grouped(frame)
    error = exc_info.value
    assert error.rule == "bad"
    assert error.group_key == "west"
    assert error.round == 1
    assert error.expected == 2


def test_diagnostic_cap_does_not_affect_elimination():
    frame = _pandas_frame()
    capped = pp.DataFrameSchema(
        grouped_checks=[
            GroupedCheck(
                _delay_rule, "region", name="delay", n_failure_cases=1
            ),
            GroupedCheck(
                _device_rule, "device", name="device", n_failure_cases=1
            ),
            GroupedCheck(
                _dual_rule, "region", name="dual", n_failure_cases=1
            ),
        ]
    )
    result = capped.validate_grouped(frame)
    assert [r.position for r in result.rejections] == [0, 1, 2, 3, 6]
    assert result.table.reset_index(drop=True).to_dict("records") == [
        {"region": "west", "device": "M", "traffic": 5, "delay": 8},
        {"region": "west", "device": "N", "traffic": 5, "delay": 8},
    ]
    # The diagnostic cap only truncates this sampling view.
    assert len(result.failure_cases) == 1
    assert result.rejections == result.rejection_list
    assert len(result.rejections) == 5


def test_row_order_seen_by_callback():
    order = []

    def record(records):
        order.extend(row["v"] for row in records)
        return True

    frame = pd.DataFrame(
        {"region": ["a", "b", "a", "b"], "v": [10, 20, 30, 40]}
    )
    pp.DataFrameSchema(
        grouped_checks=[GroupedCheck(record, "region")]
    ).validate_grouped(frame)
    assert order == [10, 30, 20, 40]


def test_existing_validate_is_unchanged():
    schema = pp.DataFrameSchema(
        {"region": pp.Column(str, nullable=True), "v": pp.Column(int)}
    )
    frame = pd.DataFrame({"region": ["a"], "v": [1]})
    assert schema.validate(frame) is not None
