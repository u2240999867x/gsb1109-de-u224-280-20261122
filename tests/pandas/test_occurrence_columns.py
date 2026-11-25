"""Tests for occurrence-pinned column rules on duplicated column labels."""

import pandas as pd
import pytest

from pandera.pandas import (
    Check,
    Column,
    DataFrameSchema,
    Parser,
    errors,
)

OCCURRENCE_2_LABEL = "qty (occurrence 2, column position 3)"


def _supplier_frame(second_qty=-2):
    """Supplier frame with two physical columns both labelled ``qty``."""
    return pd.DataFrame(
        [[7, "drop-me", second_qty, 3]],
        columns=["qty", "unknown", "qty", "qty_adj"],
    )


def _supplier_schema(**second_qty_kwargs):
    """Schema pinning the second ``qty`` (returns) with its own rule."""
    return DataFrameSchema(
        columns=[
            # outbound quantity: no negative values allowed
            Column(int, name="qty", checks=Check.ge(0)),
            # returned quantity: sign is flipped to positive
            Column(int, name="qty", occurrence=2, **second_qty_kwargs),
            # same-prefix fields go through the general regex rule
            Column(int, name=r"qty_.*", regex=True),
        ],
        strict="filter",
    )


def test_occurrence_pinning_end_to_end():
    """Pinned slots keep physical identity through strict='filter'."""
    schema = _supplier_schema(parsers=Parser(lambda s: s.abs()))

    out = schema.validate(_supplier_frame())

    assert out.columns.tolist() == ["qty", "qty", "qty_adj"]
    assert out.iloc[0].tolist() == [7, 2, 3]


def test_occurrence_pinning_preserves_input():
    """Validation must not mutate the caller's dataframe."""
    schema = _supplier_schema(parsers=Parser(lambda s: s.abs()))
    df = _supplier_frame()

    schema.validate(df)

    assert df.columns.tolist() == ["qty", "unknown", "qty", "qty_adj"]
    assert df.iloc[0].tolist() == [7, "drop-me", -2, 3]


def test_occurrence_slot_supports_own_check():
    """Each pinned slot independently takes parser, dtype, and Check."""
    schema = _supplier_schema(
        parsers=Parser(lambda s: s.abs()),
        checks=Check.le(2),
    )
    # returned quantity of 5 violates the per-slot Check.le(2)
    df = _supplier_frame(second_qty=-5)

    with pytest.raises(errors.SchemaError) as exc_info:
        schema.validate(df)

    assert exc_info.value.column_name == OCCURRENCE_2_LABEL

    out = schema.validate(_supplier_frame())
    assert out.iloc[0].tolist() == [7, 2, 3]


def test_occurrence_error_reports_physical_position():
    """Diagnostics locate the original 3rd column / 2nd ``qty`` only."""
    schema = _supplier_schema()
    df = _supplier_frame(second_qty="illegal")

    with pytest.raises(errors.SchemaError) as exc_info:
        schema.validate(df)

    err = exc_info.value
    assert err.column_name == OCCURRENCE_2_LABEL
    message = str(err)
    assert "occurrence 2" in message
    assert "position 3" in message
    # the first `qty` (position 1) must not be implicated
    assert "occurrence 1" not in message


def test_occurrence_error_reports_physical_position_lazy():
    """Lazy reports carry the physical locator in column and failure cases."""
    schema = _supplier_schema()
    df = _supplier_frame(second_qty="illegal")

    with pytest.raises(errors.SchemaErrors) as exc_info:
        schema.validate(df, lazy=True)

    schema_errors = exc_info.value.schema_errors
    assert len(schema_errors) == 1
    assert schema_errors[0].column_name == OCCURRENCE_2_LABEL
    failure_cases = exc_info.value.failure_cases
    assert OCCURRENCE_2_LABEL in failure_cases["column"].tolist()
    # the first `qty` (value 7) must not show up as a failure case
    assert 7 not in failure_cases["failure_case"].tolist()
    assert not any("occurrence 1" in str(err) for err in schema_errors)


def test_literal_suffixed_column_uses_normal_rules():
    """A literal ``qty_2`` column is not an occurrence and uses regex rules."""
    schema = DataFrameSchema(
        columns=[
            Column(int, name="qty", occurrence=2),
            Column(int, name=r"qty.*", regex=True),
        ],
        strict="filter",
    )
    df = pd.DataFrame(
        [[1, 2, -3, 4]], columns=["qty", "qty_2", "qty", "qty_adj"]
    )

    out = schema.validate(df)

    # `qty_2` sits between the two `qty` columns and passes through the
    # ordinary regex rule; occurrence 2 of `qty` is the 3rd physical column.
    assert out.columns.tolist() == ["qty", "qty_2", "qty", "qty_adj"]
    assert out.iloc[0].tolist() == [1, 2, -3, 4]


def test_literal_suffixed_column_failure_is_reported_plainly():
    """Failures on a literal ``qty_2`` column are reported as ``qty_2``."""
    schema = DataFrameSchema(
        columns=[
            Column(int, name="qty", occurrence=2),
            Column(int, name=r"qty.*", regex=True, checks=Check.ge(0)),
        ],
        strict="filter",
    )
    df = pd.DataFrame(
        [[1, -5, -3, 4]], columns=["qty", "qty_2", "qty", "qty_adj"]
    )

    with pytest.raises(errors.SchemaError) as exc_info:
        schema.validate(df)

    assert exc_info.value.column_name == "qty_2"


def test_missing_declared_occurrence_is_reported():
    """Requiring the 2nd ``qty`` when only one exists fails clearly."""
    schema = DataFrameSchema(
        columns=[
            Column(int, name="qty", occurrence=2),
            Column(int, name=r"qty_.*", regex=True),
        ],
        strict="filter",
    )
    # the first `qty` was dropped by the supplier: only one remains
    df = pd.DataFrame([[7, 3]], columns=["qty", "qty_adj"])

    with pytest.raises(errors.SchemaError) as exc_info:
        schema.validate(df)

    message = str(exc_info.value)
    assert "occurrence 2" in message
    assert "1 occurrence(s)" in message
    assert exc_info.value.reason_code.name == "COLUMN_NOT_IN_DATAFRAME"


def test_missing_declared_occurrence_is_reported_lazy():
    """Lazy validation also reports the missing declared position."""
    schema = DataFrameSchema(
        columns=[Column(int, name="qty", occurrence=2)],
    )
    df = pd.DataFrame([[7]], columns=["qty"])

    with pytest.raises(errors.SchemaErrors) as exc_info:
        schema.validate(df, lazy=True)

    assert any(
        err.reason_code.name == "COLUMN_NOT_IN_DATAFRAME"
        and "occurrence 2" in str(err)
        for err in exc_info.value.schema_errors
    )


def test_optional_occurrence_column_may_be_absent():
    """A non-required pinned column passes silently when absent."""
    schema = DataFrameSchema(
        columns=[
            Column(int, name="qty"),
            Column(int, name="qty", occurrence=2, required=False),
        ]
    )
    df = pd.DataFrame([[7]], columns=["qty"])

    out = schema.validate(df)

    assert out.columns.tolist() == ["qty"]


def test_duplicate_occurrence_slot_raises_at_init():
    """Two explicit rules claiming the same slot are rejected."""
    with pytest.raises(errors.SchemaInitError, match="occurrence 2"):
        DataFrameSchema(
            columns=[
                Column(int, name="qty", occurrence=2),
                Column(float, name="qty", occurrence=2),
            ]
        )


def test_duplicate_plain_names_in_list_raise_at_init():
    """A list of columns may not repeat a name without occurrence."""
    with pytest.raises(errors.SchemaInitError, match="duplicate columns"):
        DataFrameSchema(
            columns=[Column(int, name="qty"), Column(float, name="qty")]
        )


@pytest.mark.parametrize("occurrence", [0, -1, 1.5, "2", True])
def test_invalid_occurrence_raises_at_init(occurrence):
    """occurrence must be a positive (1-based) integer."""
    with pytest.raises(errors.SchemaInitError, match="positive integer"):
        Column(int, name="qty", occurrence=occurrence)


def test_occurrence_with_regex_raises_at_init():
    """occurrence cannot be combined with regex=True."""
    with pytest.raises(errors.SchemaInitError, match="regex"):
        Column(int, name="qty", occurrence=2, regex=True)


def test_dict_input_hoists_occurrence_columns():
    """Dict-style schemas can carry a single occurrence-pinned column."""
    schema = DataFrameSchema({"qty": Column(int, occurrence=2)})

    assert schema.columns == {}
    assert len(schema.occurrence_columns) == 1
    pinned = schema.occurrence_columns[0]
    assert pinned.name == "qty"
    assert pinned.occurrence == 2

    df = pd.DataFrame([[1, -2]], columns=["qty", "qty"])
    out = schema.validate(df)
    assert out.columns.tolist() == ["qty", "qty"]
    assert out.iloc[0].tolist() == [1, -2]


def test_unique_column_names_still_enforced():
    """Schemas rejecting duplicated labels keep doing so."""
    schema = DataFrameSchema({"qty": Column(int)}, unique_column_names=True)
    df = pd.DataFrame([[1, 2]], columns=["qty", "qty"])

    with pytest.raises(errors.SchemaErrors, match="multiple columns"):
        schema.validate(df, lazy=True)
