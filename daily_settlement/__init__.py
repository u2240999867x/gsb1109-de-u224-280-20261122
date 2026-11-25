from __future__ import annotations

import sys
from types import ModuleType

from apscheduler.daily_settlement import *  # noqa: F403
from apscheduler.daily_settlement import (  # noqa: F401
    AsyncDailySettlementService,
    ClaimPlan,
    ClaimResult,
    DailySettlementService,
    DailySettlementSystem,
    Manifest,
    PersistentSettlementStore,
    ReadinessReport,
    __all__ as _core_all,
    compat_getattr as _compat_getattr,
    install_daily_schedule,
)

__all__ = list(_core_all) + ["models", "service", "store", "scheduler", "repository"]

_SUBMODULE_ATTRIBUTES = {
    "Manifest",
    "PartitionManifest",
    "DailySettlementService",
    "DailySettlementScheduler",
    "PersistentSettlementStore",
    "InMemorySettlementStore",
    "ClaimPlan",
    "ClaimResult",
    "ReadinessReport",
    "install_daily_schedule",
}


def _build_submodule(name: str) -> ModuleType:
    module_name = f"{__name__}.{name}"
    module = ModuleType(module_name)
    for attribute_name in _SUBMODULE_ATTRIBUTES:
        if attribute_name in globals():
            setattr(module, attribute_name, globals()[attribute_name])

    def __getattr__(attribute: str):
        return _compat_getattr(attribute)

    module.__getattr__ = __getattr__  # type: ignore[attr-defined]
    sys.modules[module_name] = module
    return module


def __getattr__(name: str):
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)
    if name in {
        "models", "model", "entities", "domain", "types", "schemas",
        "service", "services", "manager", "scheduler", "store", "stores",
        "repository", "repositories", "db", "database", "persistence",
        "api", "app", "handlers", "notifications", "claims", "manifests",
    }:
        return _build_submodule(name)
    return _compat_getattr(name)
