from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from reconcile import reconcile

ROOT = Path(__file__).resolve().parents[1]
BATCH = ROOT / "samples" / "reconciliation" / "two_station_batch.json"
CORRECTION = ROOT / "samples" / "reconciliation" / "count_correction.json"


def load_sample() -> dict:
    return json.loads(BATCH.read_text(encoding="utf-8"))


def test_first_station_has_exactly_two_matchings() -> None:
    report = reconcile(load_sample())
    station_one = next(
        station for station in report.stations if station["id"] == "ST-1"
    )
    assert station_one["matching_count"] == 2


def test_endpoint_evidence_selects_one_chain_and_keeps_resend_observation() -> None:
    report = reconcile(load_sample())
    warehouse = next(
        row for row in report.warehouse_results if row["warehouse_row"] == "DWH-W1"
    )

    assert warehouse["unique_source"] is True
    assert warehouse["unique_original_event"] == "EVT-A1"
    assert warehouse["chains"][0]["nodes"] == [
        "EVT-A1",
        "MID-B1",
        "OUT-C1",
        "DWH-W1",
    ]
    assert "OBS-S2-RESEND-7" in warehouse["chains"][0]["resend_observation_ids"]
    assert warehouse["chains"][0]["resend_groups"] == ["RG-213-7"]


def test_second_subgraph_retains_both_candidate_sources() -> None:
    report = reconcile(load_sample())
    warehouse = next(
        row for row in report.warehouse_results if row["warehouse_row"] == "DWH-W2"
    )

    assert warehouse["ambiguous"] is True
    assert warehouse["candidate_sources"] == ["ORIG-7A", "ORIG-7B"]
    assert len(warehouse["chains"]) == 2
    assert warehouse["selection_policy"] == (
        "enumerate_all_global_solutions; never order-based"
    )
    assert warehouse["verifiable_constraints"]


def test_count_correction_is_located_and_other_results_remain_readable() -> None:
    correction = json.loads(CORRECTION.read_text(encoding="utf-8"))
    report = reconcile(load_sample(), correction)

    assert report.status == "completed_with_contradictions"
    issue = report.issues[0]
    assert issue.code == "COUNT_CONTRADICTION"
    assert issue.station_id == "ST-1"
    assert issue.observation_id == "OBS-CORRECTION-COUNT-1"
    assert issue.source_ref == "logs/corrections.jsonl"
    assert issue.line == 64

    warehouse_ids = {
        row["warehouse_row"] for row in report.warehouse_results
    }
    assert warehouse_ids == {"DWH-W1", "DWH-W2"}


def test_root_cli_shim_runs_json_report() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "offline_reconcile.py"), str(BATCH), "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["summary"]["unique_source_rows"] == 1
    assert payload["summary"]["ambiguous_rows"] == 1
