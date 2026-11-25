from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from event_metrics_ledger import *  # noqa: E402,F403
from event_metrics_ledger import EventMetricsLedger  # noqa: E402

Ledger = EventMetricsLedger
