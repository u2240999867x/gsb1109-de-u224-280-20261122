"""Compatibility entry point for the composite-key Hypothesis regression."""

import pandas as pd

from composite_key_strategy import *  # noqa: F403
from composite_key_strategy import (  # noqa: F401
    CompositeKeyConstraintError,
    CompositeKeyStrategy,
    child_schema,
    composite_key_strategy,
    impossible_unique_child_regression_strategy,
    parent_child_strategy,
    parent_schema,
    regression_strategy,
    related_dataframes_strategy,
    scenario_child_schema,
    scenario_parent_schema,
)


def _main() -> None:
    parent, child = regression_strategy().example()
    parent_keys = set(zip(parent["site"], parent["code"]))
    child_keys = list(zip(child["referenced_site"], child["referenced_code"]))
    assert len(parent) == 3
    assert len(child) == 5
    assert len(parent_keys) == 3
    assert all(key in parent_keys for key in child_keys)
    assert any(child_keys.count(key) >= 2 for key in set(child_keys))

    print("parent:")
    print(parent)
    print("\nchild:")
    print(child)
    print("\ncomposite-key regression generated successfully")

    try:
        impossible_unique_child_regression_strategy()
    except CompositeKeyConstraintError as exc:
        print(f"\nexpected static rejection: {exc}")
    else:
        raise AssertionError("impossible unique child request was accepted")


if __name__ == "__main__":
    _main()
