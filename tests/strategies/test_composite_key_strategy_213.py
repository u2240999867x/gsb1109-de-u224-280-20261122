from __future__ import annotations

import time

import hypothesis
import pandas as pd
import pytest
from hypothesis import find

from composite_key_strategy import (
    CompositeKeyConstraintError,
    parent_child_strategy,
    regression_strategy,
    scenario_child_schema,
    scenario_parent_schema,
)

PARENT_KEY = ("site", "code")
CHILD_KEY = ("referenced_site", "referenced_code")


def _parent_key_set(parent: pd.DataFrame) -> set[tuple[object, ...]]:
    return set(
        map(
            tuple, parent.loc[:, PARENT_KEY].itertuples(index=False, name=None)
        )
    )


def _child_key_rows(
    child: pd.DataFrame,
) -> list[tuple[object, ...]]:
    return list(
        map(tuple, child.loc[:, CHILD_KEY].itertuples(index=False, name=None))
    )


def _assert_references_parent(
    parent: pd.DataFrame, child: pd.DataFrame
) -> None:
    parent_keys = _parent_key_set(parent)
    for child_key in _child_key_rows(child):
        if all(pd.isna(value) for value in child_key):
            continue
        assert not any(pd.isna(value) for value in child_key)
        assert child_key in parent_keys


@hypothesis.given(pair=regression_strategy())
@hypothesis.settings(max_examples=80, deadline=None)
def test_regression_generates_required_shape_and_valid_references(pair):
    parent, child = pair

    assert len(parent) == 3
    assert len(child) == 5
    assert len(_parent_key_set(parent)) == 3

    child_keys = _child_key_rows(child)
    assert not any(any(pd.isna(value) for value in key) for key in child_keys)
    _assert_references_parent(parent, child)

    counts = {key: child_keys.count(key) for key in set(child_keys)}
    assert any(count >= 2 for count in counts.values())

    scenario_parent_schema().validate(parent)
    scenario_child_schema().validate(child)


def test_shrunk_examples_keep_references_aligned():
    def predicate(pair):
        parent, child = pair
        _assert_references_parent(parent, child)
        child_keys = set(_child_key_rows(child))
        return len(child_keys) >= 2

    parent, child = find(
        regression_strategy(),
        predicate,
        settings=hypothesis.settings(max_examples=200, deadline=None),
    )

    assert len(parent) == 3
    assert len(child) == 5
    _assert_references_parent(parent, child)


@hypothesis.given(
    pair=parent_child_strategy(
        parent_rows=4,
        child_rows=6,
        null_child_rows=2,
    )
)
@hypothesis.settings(max_examples=50, deadline=None)
def test_both_child_key_fields_are_null_together(pair):
    parent, child = pair
    _assert_references_parent(parent, child)

    site_nulls = child["referenced_site"].isna()
    code_nulls = child["referenced_code"].isna()
    pd.testing.assert_series_equal(site_nulls, code_nulls, check_names=False)
    assert site_nulls.sum() == 2


def test_half_empty_composite_key_is_invalid():
    schema = scenario_child_schema()
    frame = pd.DataFrame(
        {
            "referenced_site": ["02", None],
            "referenced_code": [1, 2],
            "label": ["ok", "bad"],
        }
    )

    with pytest.raises(Exception):
        schema.validate(frame)


def test_required_unique_five_rows_fails_fast_with_capacity_message():
    started = time.perf_counter()
    with pytest.raises(CompositeKeyConstraintError, match="at most 2"):
        parent_child_strategy(
            parent_schema=scenario_parent_schema(),
            child_schema=scenario_child_schema(required_unique_key=True),
            parent_rows=3,
            child_rows=5,
        )
    assert time.perf_counter() - started < 0.1


def test_required_unique_five_rows_raises_before_generation():
    with pytest.raises(CompositeKeyConstraintError, match="at most 2") as excinfo:
        parent_child_strategy(
            parent_schema=scenario_parent_schema(),
            child_schema=scenario_child_schema(required_unique_key=True),
            parent_rows=3,
            child_rows=5,
        )

    message = str(excinfo.value)
    assert "02" in message
    assert "unique child key requires 5" in message
