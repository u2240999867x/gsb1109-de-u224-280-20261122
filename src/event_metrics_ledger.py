from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_CORE_PATH = _ROOT / "event_metrics_ledger.py"
_SPEC = importlib.util.spec_from_file_location("_root_event_metrics_ledger", _CORE_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"cannot load {_CORE_PATH}")
_CORE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _CORE
_SPEC.loader.exec_module(_CORE)

for _name in dir(_CORE):
    if not _name.startswith("_"):
        globals()[_name] = getattr(_CORE, _name)

EventMetricsLedger = _CORE.EventMetricsLedger
Ledger = EventMetricsLedger
