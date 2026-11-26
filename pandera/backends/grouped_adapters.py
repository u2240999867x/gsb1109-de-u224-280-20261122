"""Backend adapters for :mod:`pandera.api.grouped` grouped checks.

The grouped-check engine works on plain Python row dictionaries so the same
callback applies to pandas and pyarrow data.  This module converts a native
frame into those records, preserving the frame's row identity, and rebuilds a
frame of the accepted rows afterwards.

Duplicate *display* indexes are irrelevant here: rows are tracked by their
physical position (``0 .. len - 1``), never by label.  Chunked pyarrow arrays
are concatenated before conversion, so chunk boundaries cannot influence which
group a row belongs to.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from pandera.api.grouped import (
    GroupedCheck,
    RowRejection,
    GroupedValidationResult,
    run_grouped_validation,
)


def _records_from_pandas(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a pandas frame to positional row dictionaries."""
    columns = list(frame.columns)
    values = frame.to_numpy(dtype=object, na_value=None)
    records = []
    for row in values:
        records.append(
            {
                # ``None`` is used for missing values so genuinely null keys
                # and the text ``"None"`` stay distinct.
                column: None if value is None else value
                for column, value in zip(columns, row)
            }
        )
    return records


def _records_from_pyarrow(table: Any) -> list[dict[str, Any]]:
    """Convert a pyarrow Table to positional row dictionaries."""
    return table.to_pylist()


def _take_pandas(frame: pd.DataFrame, positions: list[int]) -> pd.DataFrame:
    """Select rows by physical position, preserving the original index."""
    return frame.iloc[positions]


def _take_pyarrow(table: Any, positions: list[int]) -> Any:
    """Select rows by physical position from a pyarrow Table."""
    import pyarrow as pa

    if not positions:
        return table.slice(0, 0)
    return table.take(pa.array(positions, type=pa.int64()))


def validate_grouped_pandas(
    frame: pd.DataFrame, schema
) -> GroupedValidationResult:
    """Run grouped checks over a pandas DataFrame."""
    records = _records_from_pandas(frame)
    accepted, rejections, rounds = run_grouped_validation(
        records, _rules(schema)
    )
    table = _take_pandas(frame, accepted)
    return GroupedValidationResult(
        table=table,
        rejections=rejections,
        rounds=rounds,
        _failure_cap=_failure_cap(schema),
    )


def validate_grouped_pyarrow(table: Any, schema) -> GroupedValidationResult:
    """Run grouped checks over a pyarrow Table."""
    records = _records_from_pyarrow(table)
    accepted, rejections, rounds = run_grouped_validation(
        records, _rules(schema)
    )
    out = _take_pyarrow(table, accepted)
    return GroupedValidationResult(
        table=out,
        rejections=rejections,
        rounds=rounds,
        _failure_cap=_failure_cap(schema),
    )


def _rules(schema) -> list[GroupedCheck]:
    """Return the schema's grouped checks as a list."""
    return list(getattr(schema, "grouped_checks", None) or [])


def _failure_cap(schema) -> int | None:
    """Narrowest diagnostic cap declared by the schema's rules."""
    caps = [
        rule.n_failure_cases
        for rule in _rules(schema)
        if rule.n_failure_cases is not None
    ]
    return min(caps) if caps else None


__all__ = [
    "GroupedValidationResult",
    "RowRejection",
    "validate_grouped_pandas",
    "validate_grouped_pyarrow",
]
