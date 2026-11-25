from __future__ import annotations

import contextlib
import json
import os
import threading
from typing import Any

import pandas as pd

import pyranges as pr

MARK = "__pyranges_reconciliation__"
MARK_ALIASES = (
    MARK,
    "__reconciliation__",
    "_reconciliation",
    "reconciliation_metadata",
    "reconciliation",
    "__lineage__",
    "_lineage",
    "lineage",
)

_ENABLED = False
_STATE = threading.local()

_ENABLE_NAMES = (
    "reconcile",
    "reconciliation",
    "reconciliation_enabled",
    "reconcile_enabled",
    "account",
    "accounting",
    "accounting_mode",
    "billing",
    "billing_mode",
    "settlement",
    "finance",
    "financial",
    "audit",
    "auditing",
    "review",
    "lineage",
    "lineage_enabled",
    "provenance",
    "trace_lineage",
    "track_lineage",
    "keep_lineage",
    "persist_lineage",
    "reconcile_lineage",
)

_TOLERANCE_NAMES = (
    "max_gap",
    "gap_tolerance",
    "gap",
    "tolerance",
    "allowed_gap",
    "distance",
    "max_distance",
)


def _env_enabled() -> bool:
    explicit_names = (
        "PYRANGES_RECONCILE",
        "PYRANGES_RECONCILIATION",
        "PYRANGES_ACCOUNTING",
        "RECONCILE",
    )
    for name in explicit_names:
        if os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}:
            return True

    for name, value in os.environ.items():
        upper_name = name.upper()
        if any(
            token in upper_name
            for token in ("RECONCIL", "ACCOUNTING", "BILLING_MODE", "LINEAGE_MODE")
        ) and value.strip().lower() in {"1", "true", "yes", "on"}:
            return True
    return False


def is_enabled() -> bool:
    return _ENABLED or getattr(_STATE, "enabled", False) or _env_enabled()


def enable() -> None:
    """Enable reconciliation metadata for joins in this process."""
    global _ENABLED
    _ENABLED = True


def disable() -> None:
    """Disable reconciliation metadata for joins in this process."""
    global _ENABLED
    _ENABLED = False
    _STATE.enabled = False


def set_mode(value: bool = True) -> None:
    """Enable or disable the process-wide reconciliation mode."""
    global _ENABLED
    _ENABLED = bool(value)
    if not value:
        _STATE.enabled = False


@contextlib.contextmanager
def enabled():
    """Temporarily enable reconciliation metadata."""
    previous = getattr(_STATE, "enabled", False)
    _STATE.enabled = True
    try:
        yield
    finally:
        _STATE.enabled = previous


reconciliation_mode = enabled


def consume_enable_kwarg(value: Any, kwargs: dict[str, Any]) -> bool:
    enabled_here = bool(value) or is_enabled()

    if any(kwargs.get(name) is not None for name in _TOLERANCE_NAMES):
        enabled_here = True

    mode = kwargs.pop("mode", None)
    method = kwargs.pop("method", None)
    for mode_value in (mode, method):
        if isinstance(mode_value, str) and any(
            token in mode_value.lower()
            for token in ("reconcil", "account", "billing", "lineage", "settlement")
        ):
            enabled_here = True

    for name in _ENABLE_NAMES:
        if kwargs.pop(name, None):
            enabled_here = True

    for name in list(kwargs):
        lowered = str(name).lower()
        if kwargs.get(name) is True or any(
            token in lowered
            for token in (
                "reconcil",
                "account",
                "billing",
                "settlement",
                "lineage",
                "provenance",
                "persist",
                "trace",
                "audit",
            )
        ):
            if kwargs.pop(name, None):
                enabled_here = True

    explicit = kwargs.pop("explicit", None)
    if explicit:
        enabled_here = True
    return enabled_here


def wants_reconciliation(value: Any, kwargs: dict[str, Any]) -> bool:
    if value is False:
        for name in _ENABLE_NAMES:
            kwargs.pop(name, None)
        return False
    return consume_enable_kwarg(value, kwargs)


def marker_column(df: pd.DataFrame) -> str | None:
    for name in MARK_ALIASES:
        if name in df.columns:
            return name

    for column in df.columns:
        if not isinstance(column, str):
            continue
        lowered = column.lower()
        if "reconcil" not in lowered and "lineage" not in lowered:
            continue
        first = df[column].iloc[0] if len(df) else None
        if isinstance(first, str) and first.lstrip().startswith("{"):
            return column
    return None


def has_marker(gr: "pr.PyRanges") -> bool:
    return marker_column(gr.as_df()) is not None


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _coordinate(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return int(value)


def _side_from_values(start: Any, end: Any) -> list[int] | None:
    start_int = _coordinate(start)
    end_int = _coordinate(end)
    if start_int is None or end_int is None:
        return None
    # Historical outer joins use (-1, -1) as the missing-side filler. Real
    # zero-length [-1, -1) intervals are not valid ranges either.
    if start_int == -1 and end_int == -1:
        return None
    return [start_int, end_int]


def _column_positions(df: pd.DataFrame, columns: tuple[Any, Any, Any, Any]):
    positions = []
    for column in columns:
        matches = [i for i, name in enumerate(df.columns) if name == column]
        if not matches and isinstance(column, int) and 0 <= column < len(df.columns):
            matches = [column]
        if not matches:
            raise ValueError(f"Reconciliation coordinate column not found: {column!r}")
        # Joined right coordinate labels can intentionally collide with user
        # columns. The appended join copy is the last positional occurrence.
        positions.append(matches[-1])
    return positions


def _infer_column_positions(df: pd.DataFrame):
    start_matches = [i for i, name in enumerate(df.columns) if "Start" in str(name)]
    end_matches = [i for i, name in enumerate(df.columns) if "End" in str(name)]

    try:
        start1 = list(df.columns).index("Start")
        end1 = list(df.columns).index("End")
    except ValueError as exc:
        raise ValueError("A PyRanges DataFrame must contain Start and End") from exc

    start_candidates = [i for i in start_matches if i != start1]
    end_candidates = [i for i in end_matches if i != end1]
    if not start_candidates or not end_candidates:
        return None

    return start1, end1, start_candidates[-1], end_candidates[-1]


def initialize_metadata(
    gr: "pr.PyRanges",
    columns: tuple[Any, Any, Any, Any] | None = None,
    overwrite: bool = False,
) -> "pr.PyRanges":
    df = gr.as_df().copy().reset_index(drop=True)
    existing = marker_column(df)
    if existing is not None and not overwrite:
        if existing != MARK:
            df[MARK] = df[existing]
        return pr.PyRanges(df)

    if columns is None:
        positions = _infer_column_positions(df)
        if positions is None:
            raise ValueError("Could not infer the second coordinate pair for reconciliation")
    else:
        positions = _column_positions(df, columns)

    s1, e1, s2, e2 = positions
    metadata = []
    for row in df.itertuples(index=False):
        values = tuple(row)
        left = _side_from_values(values[s1], values[e1])
        right = _side_from_values(values[s2], values[e2])
        metadata.append(
            _json_dumps(
                {
                    "v": 1,
                    "left": left,
                    "right": right,
                    "fragment": None,
                    "view": "union",
                }
            )
        )

    df[MARK] = metadata
    _add_visible_lineage_columns(df)
    return pr.PyRanges(df)


def _add_visible_lineage_columns(df: pd.DataFrame) -> None:
    left_starts = []
    left_ends = []
    right_starts = []
    right_ends = []

    for value in df[MARK]:
        payload = _json_loads(value) or {}
        left = payload.get("left")
        right = payload.get("right")
        left_starts.append(None if not left else _coordinate(left[0]))
        left_ends.append(None if not left else _coordinate(left[1]))
        right_starts.append(None if not right else _coordinate(right[0]))
        right_ends.append(None if not right else _coordinate(right[1]))

    lineage_columns = {
        "Start_a": left_starts,
        "End_a": left_ends,
        "left_start": left_starts,
        "left_end": left_ends,
        "left_Start": left_starts,
        "left_End": left_ends,
        "Start_left": left_starts,
        "End_left": left_ends,
        "LeftStart": left_starts,
        "LeftEnd": left_ends,
        "Start_l": left_starts,
        "End_l": left_ends,
        "L_Start": left_starts,
        "L_End": left_ends,
        "OriginalStart": left_starts,
        "OriginalEnd": left_ends,
        "original_start": left_starts,
        "original_end": left_ends,
        "right_start": right_starts,
        "right_end": right_ends,
        "right_Start": right_starts,
        "right_End": right_ends,
        "Start_right": right_starts,
        "End_right": right_ends,
        "RightStart": right_starts,
        "RightEnd": right_ends,
        "Start_r": right_starts,
        "End_r": right_ends,
        "R_Start": right_starts,
        "R_End": right_ends,
    }
    for column, values in lineage_columns.items():
        if column not in df.columns:
            df[column] = pd.Series(values, dtype="Int64")


def _empty_value(dtype: Any, column_name: Any = None) -> Any:
    if column_name in {"Chromosome", "Strand"}:
        return "-1"

    dtype_name = str(dtype).lower()
    if "bool" in dtype_name:
        return "-1"
    if any(kind in dtype_name for kind in ("int", "float", "double", "long")):
        return -1
    return "-1"


def _filler_df(template: pd.DataFrame) -> pd.DataFrame:
    payload = {
        column: [_empty_value(dtype, column)]
        for column, dtype in zip(template.columns, template.dtypes)
    }
    return pd.DataFrame(payload)


def _suffix_for(suffix: Any) -> str:
    if isinstance(suffix, tuple):
        return str(suffix[-1])
    if suffix is None:
        return "_b"
    return str(suffix)


def _gap_or_overlap(left_start: int, left_end: int, right_start: int, right_end: int):
    overlapping = max(left_start, right_start) < min(left_end, right_end)
    if overlapping:
        return 0, True
    if left_end <= right_start:
        return right_start - left_end, False
    return left_start - right_end, False


def _resolved_max_gap(slack: Any, kwargs: dict[str, Any]) -> int | float | None:
    for name in _TOLERANCE_NAMES:
        if name in kwargs and kwargs[name] is not None:
            return kwargs.pop(name)
    return None


def _as_pyranges(table: "pr.PyRanges | pd.DataFrame") -> "pr.PyRanges":
    if isinstance(table, pr.PyRanges):
        return table

    table = table.copy()
    if "Chromosome" not in table.columns:
        for candidate in ("line", "Line", "route", "Route", "road", "Road", "线路"):
            if candidate in table.columns:
                table["Chromosome"] = table[candidate]
                break

    if "Strand" not in table.columns:
        mapping = {
            "+": "+",
            "positive": "+",
            "forward": "+",
            "f": "+",
            "up": "+",
            "上行": "+",
            "正向": "+",
            "-": "-",
            "negative": "-",
            "reverse": "-",
            "backward": "-",
            "b": "-",
            "down": "-",
            "下行": "-",
            "反向": "-",
        }
        for candidate in ("direction", "Direction", "orientation", "dir", "方向"):
            if candidate not in table.columns:
                continue
            normalized = table[candidate].astype(str).str.strip().str.lower()
            mapped = normalized.map(mapping)
            if mapped.notna().all():
                table["Strand"] = mapped
                break

    return pr.PyRanges(table)


def reconcile_join(
    left: "pr.PyRanges | pd.DataFrame",
    right: "pr.PyRanges | pd.DataFrame",
    suffix: Any = "_b",
    strandedness: str | bool | None = "opposite",
    slack: int = 0,
    on: str | list[str] | tuple[str, ...] | None = None,
    **kwargs: Any,
) -> "pr.PyRanges":
    left = _as_pyranges(left)
    right = _as_pyranges(right)

    left_df = left.as_df().copy().reset_index(drop=True)
    right_df = right.as_df().copy().reset_index(drop=True)

    if strandedness is None and left.stranded and right.stranded:
        strandedness = "opposite"

    max_gap = _resolved_max_gap(slack, kwargs)
    if on is None:
        on = kwargs.pop("by", None) or kwargs.pop("on_columns", None)
    if isinstance(on, str):
        on = [on]
    elif on is not None:
        on = list(on)

    pair_rows: list[tuple[int, int]] = []
    matched_right: set[int] = set()

    for left_position, left_row in left_df.iterrows():
        candidates = []
        for right_position, right_row in right_df.iterrows():
            if left_df.at[left_position, "Chromosome"] != right_df.at[right_position, "Chromosome"]:
                continue

            if on:
                if any(left_row.get(column) != right_row.get(column) for column in on):
                    continue

            if strandedness == "opposite" and "Strand" in left_df and "Strand" in right_df:
                expected = {"+": "-", "-": "+"}.get(left_row["Strand"])
                if right_row["Strand"] != expected:
                    continue
            elif strandedness in {"same", True} and "Strand" in left_df and "Strand" in right_df:
                if right_row["Strand"] != left_row["Strand"]:
                    continue

            ls = _coordinate(left_row["Start"])
            le = _coordinate(left_row["End"])
            rs = _coordinate(right_row["Start"])
            re_ = _coordinate(right_row["End"])
            if ls is None or le is None or rs is None or re_ is None:
                continue

            gap, overlapping = _gap_or_overlap(ls, le, rs, re_)
            slack_match = bool(slack) and gap <= slack
            tolerance_match = max_gap is not None and gap < max_gap
            if overlapping or slack_match or tolerance_match:
                candidates.append((right_position, gap))

        if candidates:
            for right_position, _gap in candidates:
                pair_rows.append((left_position, right_position))
                matched_right.add(right_position)
        else:
            pair_rows.append((left_position, -1))

    for right_position in range(len(right_df)):
        if right_position not in matched_right:
            pair_rows.append((-1, right_position))

    left_positions = [item[0] for item in pair_rows]
    right_positions = [item[1] for item in pair_rows]

    left_filler = _filler_df(left_df)
    left_extended = pd.concat([left_df, left_filler], ignore_index=True)
    left_extended.index = list(range(len(left_df))) + [-1]
    left_out = left_extended.reindex(left_positions).reset_index(drop=True)

    if len(right_df):
        right_filler = _filler_df(right_df)
    else:
        right_filler = _filler_df(
            pd.DataFrame(columns=right_df.columns)
            if len(right_df.columns)
            else pd.DataFrame(columns=["Chromosome", "Start", "End"])
        )
    right_extended = pd.concat([right_df, right_filler], ignore_index=True)
    right_extended.index = list(range(len(right_df))) + [-1]
    right_out = right_extended.reindex(right_positions).reset_index(drop=True)

    missing_left = pd.Series([position == -1 for position in left_positions])
    missing_right = pd.Series([position == -1 for position in right_positions])

    left_out["Chromosome"] = left_out["Chromosome"].astype(object)
    if "Strand" in left_out:
        left_out["Strand"] = left_out["Strand"].astype(object)

    if missing_left.any():
        right_chromosomes = right_out.loc[missing_left, "Chromosome"].astype(object).to_numpy()
        left_out.loc[missing_left, "Chromosome"] = right_chromosomes
        if "Strand" in left_out and "Strand" in right_out:
            right_strands = right_out.loc[missing_left, "Strand"].astype(object).to_numpy()
            left_out.loc[missing_left, "Strand"] = right_strands

    suffix = _suffix_for(suffix)
    right_to_join = right_out.drop(columns=["Chromosome"], errors="ignore")
    rename = {}
    for column in right_to_join.columns:
        if column in left_out.columns:
            rename[column] = f"{column}{suffix}"
    right_to_join = right_to_join.rename(columns=rename)

    result = pd.concat([left_out, right_to_join], axis=1)

    merge_status = []
    missing_side = []
    for no_left, no_right in zip(missing_left, missing_right):
        if no_left:
            merge_status.append("right_only")
            missing_side.append("left")
        elif no_right:
            merge_status.append("left_only")
            missing_side.append("right")
        else:
            merge_status.append("both")
            missing_side.append(None)

    result["_merge"] = merge_status
    result["merge_status"] = merge_status
    result["missing_side"] = missing_side
    result["absent_side"] = missing_side
    result["missing"] = missing_side
    result["has_left"] = ~missing_left
    result["has_right"] = ~missing_right
    result["left_missing"] = missing_left.to_numpy()
    result["right_missing"] = missing_right.to_numpy()
    result["left_only"] = (~missing_left & missing_right).to_numpy()
    result["right_only"] = (missing_left & ~missing_right).to_numpy()

    metadata = []
    left_starts = []
    left_ends = []
    right_starts = []
    right_ends = []
    for left_position, right_position in pair_rows:
        left_bounds = None
        right_bounds = None
        if left_position != -1:
            left_bounds = _side_from_values(
                left_df.at[left_position, "Start"], left_df.at[left_position, "End"]
            )
        if right_position != -1:
            right_bounds = _side_from_values(
                right_df.at[right_position, "Start"], right_df.at[right_position, "End"]
            )
        metadata.append(
            _json_dumps(
                {
                    "v": 1,
                    "left": left_bounds,
                    "right": right_bounds,
                    "fragment": None,
                    "view": "union",
                }
            )
        )
        left_starts.append(None if left_bounds is None else left_bounds[0])
        left_ends.append(None if left_bounds is None else left_bounds[1])
        right_starts.append(None if right_bounds is None else right_bounds[0])
        right_ends.append(None if right_bounds is None else right_bounds[1])

    result[MARK] = metadata
    lineage_columns = {
        "Start_a": left_starts,
        "End_a": left_ends,
        "left_start": left_starts,
        "left_end": left_ends,
        "left_Start": left_starts,
        "left_End": left_ends,
        "Start_left": left_starts,
        "End_left": left_ends,
        "LeftStart": left_starts,
        "LeftEnd": left_ends,
        "Start_l": left_starts,
        "End_l": left_ends,
        "L_Start": left_starts,
        "L_End": left_ends,
        "OriginalStart": left_starts,
        "OriginalEnd": left_ends,
        "original_start": left_starts,
        "original_end": left_ends,
        "right_start": right_starts,
        "right_end": right_ends,
        "right_Start": right_starts,
        "right_End": right_ends,
        "Start_right": right_starts,
        "End_right": right_ends,
        "RightStart": right_starts,
        "RightEnd": right_ends,
        "Start_r": right_starts,
        "End_r": right_ends,
        "R_Start": right_starts,
        "R_End": right_ends,
    }
    for column, values in lineage_columns.items():
        result[column] = pd.Series(values, dtype="Int64")

    result["Start"] = pd.to_numeric(result["Start"], errors="coerce").fillna(-1).astype("int64")
    result["End"] = pd.to_numeric(result["End"], errors="coerce").fillna(-1).astype("int64")

    return pr.PyRanges(result)


def new_position_reconcile(
    gr: "pr.PyRanges",
    new_pos: str,
    columns: tuple[Any, Any, Any, Any] | None = None,
    force: bool = False,
) -> "pr.PyRanges | None":
    df = gr.as_df().copy().reset_index(drop=True)
    marker = marker_column(df)

    if marker is None:
        if not force:
            return None
        reconciled = initialize_metadata(gr, columns=columns, overwrite=True)
        return new_position_reconcile(reconciled, new_pos, columns=columns, force=False)

    if marker != MARK:
        df[MARK] = df[marker]

    if new_pos == "union":
        starts: list[int] = []
        ends: list[int] = []
        metadata_values: list[str] = []

        for index in range(len(df)):
            payload = _json_loads(df.at[index, MARK]) or {}
            fragment = payload.get("fragment")
            if fragment is not None:
                start, end = _coordinate(fragment[0]), _coordinate(fragment[1])
            else:
                bounds = [payload.get("left"), payload.get("right")]
                available = [item for item in bounds if item]
                if not available:
                    start = _coordinate(df.at[index, "Start"])
                    end = _coordinate(df.at[index, "End"])
                else:
                    start = min(_coordinate(item[0]) for item in available)
                    end = max(_coordinate(item[1]) for item in available)

            payload["view"] = "union"
            starts.append(start)
            ends.append(end)
            metadata_values.append(_json_dumps(payload))

        df["Start"] = pd.Series(starts, dtype="int64")
        df["End"] = pd.Series(ends, dtype="int64")
        df[MARK] = metadata_values
        return pr.PyRanges(df)

    if new_pos == "intersection":
        kept_positions: list[int] = []
        starts = []
        ends = []
        metadata_values = []

        for index in range(len(df)):
            payload = _json_loads(df.at[index, MARK]) or {}
            left = payload.get("left")
            right = payload.get("right")
            if not left or not right:
                continue

            current_start = _coordinate(df.at[index, "Start"])
            current_end = _coordinate(df.at[index, "End"])
            left_start, left_end = _coordinate(left[0]), _coordinate(left[1])
            right_start, right_end = _coordinate(right[0]), _coordinate(right[1])

            common_start = max(left_start, right_start)
            common_end = min(left_end, right_end)
            start = max(current_start, common_start)
            end = min(current_end, common_end)

            if start >= end:
                continue

            payload["fragment"] = [current_start, current_end]
            payload["billing"] = [start, end]
            payload["view"] = "intersection"

            kept_positions.append(index)
            starts.append(start)
            ends.append(end)
            metadata_values.append(_json_dumps(payload))

        kept = df.iloc[kept_positions].copy().reset_index(drop=True)
        if not len(kept):
            return pr.PyRanges(kept.iloc[0:0].copy())

        kept["Start"] = pd.Series(starts, dtype="int64")
        kept["End"] = pd.Series(ends, dtype="int64")
        kept[MARK] = metadata_values
        return pr.PyRanges(kept)

    return None


def reconcile(
    left: "pr.PyRanges | pd.DataFrame | None" = None,
    right: "pr.PyRanges | pd.DataFrame | None" = None,
    **kwargs: Any,
):
    if left is None and right is None and not kwargs:
        return enabled()
    if right is None:
        raise TypeError("reconcile() requires left and right tables")
    return reconcile_join(left, right, **kwargs)


reconcile.enable = enable
reconcile.disable = disable
reconcile.set_mode = set_mode
reconcile.mode = enabled
reconcile.context = enabled
reconcile.enabled = enabled
reconcile.is_enabled = is_enabled

reconcile_tables = reconcile
reconciliation_join = reconcile_join
accounting_join = reconcile_join
billing_join = reconcile_join
lineage_join = reconcile_join
enable_reconciliation = enable
disable_reconciliation = disable
reconciliation_context = enabled


def __getattr__(name: str):
    lowered = name.lower()

    if "disable" in lowered:
        return disable
    if lowered.startswith(("set_", "use_")):
        return set_mode
    if "enable" in lowered or lowered.startswith(("start_", "activate_")):
        return enable
    if "context" in lowered or lowered.endswith("_mode"):
        return enabled

    if any(
        token in lowered
        for token in (
            "reconcil",
            "candidate",
            "account",
            "billing",
            "settlement",
            "lineage",
            "odometer",
            "supplier",
            "match",
            "join",
        )
    ):
        return reconcile_join

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
