import pandas as pd
import pytest

import pandera.pandas as pa
from pandera.errors import SchemaErrors, SchemaInitError


def _order_parser(calls: list[int]):
    def parse_order_no(series: pd.Series) -> pd.Series:
        calls.append(len(calls) + 1)
        return (
            series.astype("string")
            .str.replace(r"^ORD-", "", regex=True)
            .astype("int64")
        )

    return pa.Parser(parse_order_no)


def _contracts(calls: list[int]):
    inbound = pa.DataFrameSchema(
        columns={
            "warehouse": pa.Column(str),
            "order_no": pa.Column(int, parsers=_order_parser(calls)),
        },
        index=pa.MultiIndex(
            [
                pa.Index(str, name="batch"),
                pa.Index(str, name="scan_ref"),
            ]
        ),
        unique=[
            ["warehouse", "order_no"],
            ["warehouse", "scan_ref"],
        ],
    )

    archive = (
        inbound.set_index(["order_no"], append=True, drop=False)
        .rename_columns({"order_no": "retained_no"})
        .reset_index(["scan_ref"])
        .rename_columns({"scan_ref": "order_no"})
    )
    archive.columns["retained_no"].checks = [
        pa.Check(
            lambda series: series.eq(
                pd.Series(
                    series.index.get_level_values("order_no"),
                    index=series.index,
                )
            ),
            error="retained_no does not match order_no index",
        )
    ]

    reentry = (
        archive.rename_columns({"order_no": "scan_ref"})
        .set_index(["scan_ref"], append=True)
        .remove_columns(["retained_no"])
        .reset_index(["order_no"])
    )
    return inbound, archive, reentry


def _archive_frame():
    rows = [
        ("b1", "ORD-007", "V91", "7"),
        ("b2", "7", "V92", "ORD-007"),
        ("b3", "8", "V91", "8"),
        ("b4", "9", "V94", "10"),
        ("b5", "10", "V93", "10"),
    ]
    return pd.DataFrame(
        {
            "warehouse": ["A"] * 5,
            "order_no": [row[2] for row in rows],
            "retained_no": [row[3] for row in rows],
        },
        index=pd.MultiIndex.from_tuples(
            [(row[0], row[1]) for row in rows],
            names=["batch", "order_no"],
        ),
    )


def _raw_frame():
    rows = [
        ("b1", "ORD-007", "V91"),
        ("b2", "7", "V92"),
        ("b3", "8", "V91"),
        ("b4", "9", "V94"),
        ("b5", "10", "V93"),
    ]
    return pd.DataFrame(
        {
            "warehouse": ["A"] * 5,
            "order_no": [row[1] for row in rows],
        },
        index=pd.MultiIndex.from_tuples(
            [(row[0], row[2]) for row in rows],
            names=["batch", "scan_ref"],
        ),
    )


def test_unique_keys_follow_fields_through_archive_layout():
    calls: list[int] = []
    _, archive, _ = _contracts(calls)

    with pytest.raises(SchemaErrors) as exc_info:
        archive.validate(_archive_frame(), lazy=True)

    errors = exc_info.value.schema_errors
    messages = [str(error) for error in errors]
    unique_messages = [
        message
        for error, message in zip(errors, messages)
        if error.check == "multiple_fields_uniqueness"
    ]

    assert len(errors) == 3
    assert any("('A', 7)" in message for message in unique_messages)
    assert any("('A', 'V91')" in message for message in unique_messages)
    assert not any("('A', 10)" in message for message in unique_messages)
    assert any(
        "retained_no does not match order_no index" in message
        for message in messages
    )
    assert calls == [1, 2]


def test_corrected_archive_cleans_both_order_representations():
    calls: list[int] = []
    _, archive, _ = _contracts(calls)
    frame = pd.DataFrame(
        {
            "warehouse": ["A"] * 3,
            "order_no": ["V91", "V94", "V93"],
            "retained_no": ["7", "ORD-009", "10"],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("b1", "ORD-007"),
                ("b4", "9"),
                ("b5", "10"),
            ],
            names=["batch", "order_no"],
        ),
    )

    result = archive.validate(frame, lazy=True)

    assert result.index.get_level_values("order_no").tolist() == [
        7,
        9,
        10,
    ]
    assert result["retained_no"].tolist() == [7, 9, 10]
    assert result["order_no"].tolist() == ["V91", "V94", "V93"]
    assert calls == [1, 2]


def test_archive_reset_index_reports_name_collision_and_reentry_works():
    calls: list[int] = []
    inbound, archive, reentry = _contracts(calls)

    with pytest.raises(SchemaInitError, match="order_no"):
        archive.reset_index("order_no")

    assert archive.index.names == ["batch", "order_no"]
    assert list(archive.columns) == [
        "warehouse",
        "retained_no",
        "order_no",
    ]

    with pytest.raises(SchemaErrors) as exc_info:
        reentry.validate(_raw_frame(), lazy=True)

    assert len(exc_info.value.schema_errors) == 2
    assert exc_info.value.data["order_no"].tolist() == [
        7,
        7,
        8,
        9,
        10,
    ]

    with pytest.raises(SchemaErrors):
        inbound.validate(_raw_frame(), lazy=True)
    assert calls == [1, 2]


def test_column_only_unique_contract_still_works():
    schema = pa.DataFrameSchema(
        {"warehouse": pa.Column(str), "order_no": pa.Column(int)},
        unique=["warehouse", "order_no"],
    )

    valid = pd.DataFrame({"warehouse": ["A", "A"], "order_no": [7, 9]})
    assert schema.validate(valid).order_no.tolist() == [7, 9]

    invalid = pd.DataFrame({"warehouse": ["A", "A"], "order_no": [7, 7]})
    with pytest.raises(SchemaErrors):
        schema.validate(invalid, lazy=True)
