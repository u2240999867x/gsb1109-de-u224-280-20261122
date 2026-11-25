import pandas as pd

import pyranges as pr


def _scenario_tables():
    left = pr.PyRanges(
        pd.DataFrame(
            {
                "Chromosome": ["W"],
                "Start": [-6],
                "End": [8],
                "Strand": ["+"],
                "source_id": ["L"],
                "supplier": ["A"],
                "Start_b": [100],
                "End_b": [110],
                "negative_mileage": [-7],
                "numeric_minus_one": [-1],
                "text_minus_one": ["-1"],
                "true_null": pd.Series([None], dtype="object"),
            }
        )
    )
    right = pr.PyRanges(
        pd.DataFrame(
            {
                "Chromosome": ["W", "W", "W", "E"],
                "Start": [-3, 9, -6, 9007199254740993],
                "End": [12, 10, 8, 9007199254741000],
                "Strand": ["-", "-", "+", "-"],
                "source_id": ["R1", "R2", "R3", "E1"],
                "supplier": ["B", "B", "B", "B"],
            }
        )
    )
    return left, right


def test_reconciliation_workflow_survives_dataframe_handoffs():
    left, right = _scenario_tables()

    candidates = left.join(
        right,
        how="outer",
        strandedness="opposite",
        slack=2,
        suffix="_b",
        reconcile=True,
    )

    assert len(candidates) == 4
    envelope = candidates.new_position("union").df
    huge_start = envelope.loc[envelope.source_id_b == "E1", "Start"].iloc[0]
    assert int(huge_start) == 9007199254740993

    west = pr.PyRanges(envelope[envelope.Chromosome == "W"])
    sheet = pr.PyRanges(pd.DataFrame({"Chromosome": ["W"], "Start": [-5], "End": [11]}))
    holes = pr.PyRanges(
        pd.DataFrame({"Chromosome": ["W", "W"], "Start": [-4, 4], "End": [-1, 6]})
    )
    clipped = west.intersect(sheet).subtract(holes).df

    r1_clipped = clipped[clipped.source_id_b == "R1"].sort_values("Start")
    assert list(zip(r1_clipped.Start, r1_clipped.End)) == [
        (-5, -4),
        (-1, 4),
        (6, 11),
    ]

    finance_table = clipped.sample(frac=1, random_state=7).reset_index(drop=True)
    billing = pr.PyRanges(finance_table).new_position("intersection").df
    r1_billing = billing[billing.source_id_b == "R1"].sort_values("Start")
    assert list(zip(r1_billing.Start, r1_billing.End)) == [(-1, 4), (6, 8)]
    assert int((r1_billing.End - r1_billing.Start).sum()) == 7

    reviewer_table = billing.sample(frac=1, random_state=9).reset_index(drop=True)
    reviewed = pr.PyRanges(reviewer_table).new_position("union").df
    r1_reviewed = reviewed[reviewed.source_id_b == "R1"].sort_values("Start")
    assert list(zip(r1_reviewed.Start, r1_reviewed.End)) == [(-1, 4), (6, 11)]
    assert list(zip(r1_reviewed.LeftStart, r1_reviewed.LeftEnd)) == [(-6, 8), (-6, 8)]
    assert list(zip(r1_reviewed.RightStart, r1_reviewed.RightEnd)) == [(-3, 12), (-3, 12)]
    assert 100 in reviewed["Start_b"].values
    assert 110 in reviewed["End_b"].values

    left_row = candidates.df[candidates.df.source_id == "L"].iloc[0]
    assert left_row.negative_mileage == -7
    assert left_row.numeric_minus_one == -1
    assert left_row.text_minus_one == "-1"
    assert pd.isna(left_row.true_null)


def test_reconciliation_empty_declared_table_keeps_left_once():
    left, _right = _scenario_tables()
    empty_right = pr.PyRanges(
        pd.DataFrame(
            {
                "Chromosome": pd.Series(dtype="category"),
                "Start": pd.Series(dtype="int64"),
                "End": pd.Series(dtype="int64"),
                "Strand": pd.Series(dtype="category"),
                "source_id": pd.Series(dtype="object"),
            }
        )
    )

    candidates = pr.reconcile_join(
        left[["Chromosome", "Start", "End", "Strand"]],
        empty_right,
        strandedness="opposite",
    )

    assert len(candidates) == 1
    billing = candidates.new_position("union").new_position("intersection")
    assert len(billing) == 0


def test_default_new_position_remains_unchanged_without_lineage():
    gr = pr.from_dict({"Chromosome": ["1"], "Start": [3], "End": [6]})
    other = pr.from_dict({"Chromosome": ["1"], "Start": [5], "End": [7]})

    joined = gr.join(other)

    result = joined.new_position("intersection").df
    assert list(zip(result.Start, result.End)) == [(5, 6)]
