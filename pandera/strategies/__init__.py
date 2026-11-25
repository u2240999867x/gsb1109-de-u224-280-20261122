"""Hypothesis-based data synthesis (import backend modules explicitly).

Example: ``import pandera.strategies.pandas_strategies`` or
``import pandera.strategies.xarray_strategies``.
"""

from typing import Any

_TENSORDICT_STRATEGIES = ("tensordict_strategy", "tensorclass_strategy")

_COMPOSITE_KEY_STRATEGIES = (
    "CompositeKeyConstraintError",
    "CompositeKeyStrategy",
    "RelatedFrames",
    "composite_key_strategy",
    "create_composite_key_strategy",
    "foreign_key_dataframes_strategy",
    "parent_child_dataframes_strategy",
    "parent_child_strategy",
    "regression_strategy",
    "related_dataframes_strategy",
)


def __getattr__(name: str) -> Any:
    # Lazily expose tensordict strategies so that importing this package
    # doesn't require torch/tensordict to be installed.
    if name in _TENSORDICT_STRATEGIES:
        from pandera.strategies import tensordict_strategies

        return getattr(tensordict_strategies, name)
    if name in _COMPOSITE_KEY_STRATEGIES:
        import importlib

        module = importlib.import_module(
            "pandera.strategies.composite_key_strategy"
        )

        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
