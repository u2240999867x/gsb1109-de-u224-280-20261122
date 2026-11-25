"""Compatibility import for the composite-key relation strategy."""

from pandera.strategies.composite_key_strategy import *  # noqa: F403
from pandera.strategies.composite_key_strategy import (  # noqa: F401
    CHILD_KEY,
    PARENT_KEY,
    CompositeKeyConstraintError,
    CompositeKeyStrategy,
    RelatedFrames,
    child_schema,
    composite_key_strategy,
    create_composite_key_strategy,
    impossible_unique_child_regression_strategy,
    parent_child_dataframes_strategy,
    parent_child_strategy,
    parent_schema,
    regression_strategy,
    related_dataframes_strategy,
    scenario_child_schema,
    scenario_parent_schema,
)
