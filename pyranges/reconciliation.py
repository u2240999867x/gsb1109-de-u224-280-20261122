from pyranges.reconcile import *  # noqa: F401,F403
from pyranges.reconcile import (  # noqa: F401
    MARK,
    disable,
    enable,
    enabled,
    initialize_metadata,
    is_enabled,
    marker_column,
    new_position_reconcile,
    reconcile,
    reconcile_join,
)


def __getattr__(name):
    lowered = name.lower()

    if "disable" in lowered:
        return disable
    if "enable" in lowered or lowered in {"start", "activate"}:
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
