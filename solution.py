from __future__ import annotations

from event_metrics_ledger import *  # noqa: F403
from event_metrics_ledger import (
    FORMAT,
    EventMetricsLedger,
    Ledger,
    MetricsLedger,
    create_ledger,
    event_key,
    normalize_minute,
    process_manifest,
    rebuild_metrics,
)
from event_metrics_scenario import build_report

__all__ = [
    "EventMetricsLedger",
    "MetricsLedger",
    "Ledger",
    "FORMAT",
    "create_ledger",
    "event_key",
    "normalize_minute",
    "process_manifest",
    "rebuild_metrics",
    "build_report",
    "solve",
    "run",
]


def solve(*args, **kwargs):
    if args or kwargs:
        return EventMetricsLedger(*args, **kwargs)
    return build_report(kwargs.pop("batch_size", 1))


def run(*args, **kwargs):
    return solve(*args, **kwargs)


if __name__ == "__main__":
    import json

    print(json.dumps(build_report(), ensure_ascii=False, indent=2, sort_keys=True))
