from __future__ import annotations

import importlib
from typing import Any


def __getattr__(name: str) -> Any:
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)
    handoff = importlib.import_module("zarr.metadata.handoff")
    if name in handoff.__all__:
        return getattr(handoff, name)
    return handoff.__getattr__(name)
