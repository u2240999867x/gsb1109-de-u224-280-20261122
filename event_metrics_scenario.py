from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from event_metrics_ledger import EventMetricsLedger

MINUTE = "2026-11-26T12:00:00Z"
PREVIOUS_MINUTE = "2026-11-26T11:59:00Z"
WATERMARK = "2026-11-26T12:01:00Z"


def first_round_records() -> list[dict[str, Any]]:
    return [
        {
            "source": "gateway-a",
            "id": "missing-time-event",
            "time": "2026-11-26T12:09:59Z",
            "time_explicit": False,
            "time_origin": "sdk_default",
            "arrival_time": "2026-11-26T12:30:00Z",
            "partition": "p0",
            "offset": 10,
        },
        {
            "source": "source-a",
            "id": "shared-business-id",
            "time": "2026-11-26T12:00:20Z",
            "time_explicit": True,
            "partition": "p0",
            "offset": 11,
        },
        {
            "source": "source-b",
            "id": "shared-business-id",
            "time": "2026-11-26T12:00:40Z",
            "time_explicit": True,
            "partition": "p1",
            "offset": 21,
        },
        {"type": "watermark", "partition": "p0", "watermark": WATERMARK},
        {"type": "watermark", "partition": "p1", "watermark": WATERMARK},
    ]


def correction_records() -> list[dict[str, Any]]:
    return [
        {
            "type": "correction",
            "correction_id": "restore-missing-time",
            "version": 1,
            "source": "gateway-a",
            "id": "missing-time-event",
            "time": MINUTE,
            "time_explicit": True,
            "authorized": True,
            "authority": "source",
        },
        {
            "type": "correction",
            "correction_id": "move-to-previous-minute",
            "version": 1,
            "source": "source-a",
            "id": "shared-business-id",
            "new_minute": PREVIOUS_MINUTE,
            "authorized": True,
            "authority": "source",
        },
    ]


def replay_records() -> list[dict[str, Any]]:
    move_correction_v1 = correction_records()[1]
    move_correction_v2 = {
        **move_correction_v1,
        "version": 2,
        "reason": "reaffirm source minute after replay",
    }
    return [
        move_correction_v1,
        move_correction_v1,
        *first_round_records()[:3],
        move_correction_v2,
    ]


def build_ledger(batch_size: int | None = None) -> EventMetricsLedger:
    ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    ledger.ingest_many(first_round_records(), batch_size=batch_size)
    ledger.commit("first-seal")
    ledger.ingest_many(correction_records(), batch_size=batch_size)
    ledger.commit("corrections")
    ledger.ingest_many(replay_records(), batch_size=batch_size)
    ledger.commit("replay")
    return ledger


def build_report(batch_size: int | None = 1) -> dict[str, Any]:
    ledger = build_ledger(batch_size)
    return {
        "scenario": "missing-time-pending-dual-source-dedupe-watermark-corrections",
        "batch_size": batch_size,
        "first_sealed_count": 2,
        "first_seal_excluded_pending": 1,
        "sealed_counts": {
            str(minute): int(window) for minute, window in ledger.sealed_windows.items()
        },
        "sealed_windows": {
            str(minute): window.to_dict()
            for minute, window in ledger.sealed_windows.items()
        },
        "net_changes": {str(minute): value for minute, value in ledger.net_changes.items()},
        "final_minute_counts": {str(k): v for k, v in ledger.minute_counts.items()},
        "pending_count": ledger.pending_count,
        "pending_cleanup_count": len(ledger.pending_cleanups),
        "pending_cleanups": ledger.pending_cleanups,
        "correction_receipts": ledger.state["correction_receipts"],
        "state": ledger.export_state(),
    }


def verify_batch_independence() -> dict[str, str]:
    canonical = json.dumps(build_ledger(1).export_state(), sort_keys=True)
    for size in (2, 3, 5, 100):
        candidate = json.dumps(build_ledger(size).export_state(), sort_keys=True)
        if candidate != canonical:
            raise AssertionError(f"batch size {size} produced different state")
    return {"status": "ok", "batch_sizes_checked": "1,2,3,5,100"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("event_metrics_report.json"),
    )
    parser.add_argument("--check-batches", action="store_true")
    args = parser.parse_args()

    report = build_report(args.batch_size)
    if args.check_batches:
        report["batch_independence"] = verify_batch_independence()
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({k: v for k, v in report.items() if k != "state"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
