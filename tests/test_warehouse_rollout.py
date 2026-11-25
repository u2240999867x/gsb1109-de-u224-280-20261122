from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import warehouse_ingest as wi  # noqa: E402


def test_v1_and_v2_amounts_are_distinct() -> None:
    assert wi.money_receipt_amount(1250, "v1") == "12.50"
    assert wi.money_receipt_amount(1250, "v2") == "1250.00"
    assert wi.warehouse_ingest("B-1", 1250, "v1")["receipt_text"] == "12.50 欧元"
    assert wi.warehouse_ingest("B-2", 1250, version="v2")["amount_numeric"] == 1250.0


def test_legacy_and_threadpool_classification() -> None:
    protocol, routing, _ = wi.classify_job(labels=["legacy_ingest"])
    assert protocol == "v1"
    assert routing == "ready"

    protocol, routing, reason = wi.classify_job(labels=["threadpool"])
    assert protocol is None
    assert routing == "manual_review"
    assert "ambiguous" in reason

    protocol, routing, _ = wi.classify_job(
        labels=["threadpool"], metadata={"warehouse_protocol": "v2"}
    )
    assert protocol == "v2"
    assert routing == "ready"


def test_rolling_upgrade_scenario_matches_contract() -> None:
    report = wi.run_rolling_upgrade_scenario()

    assert report["status"] == "passed"
    checks = report["pre_completion_checks"]
    assert checks["b_claim_while_a_active_is_none"] is True
    assert checks["v2_not_acquired_by_b"] is True
    assert checks["v2_still_queued"] is True
    assert checks["no_writes_before_release"] is True
    assert checks["a_lease_valid"] is True

    receipts = report["receipts_by_original_job_id"]
    assert receipts["job-a-inflight-v1"]["receipt_text"] == "12.50 欧元"
    assert receipts["job-legacy-ingest"]["protocol_used"] == "v1"
    assert receipts["job-v2-switch"]["amount_display"] == "1250.00"
    assert receipts["job-v2-switch"]["protocol_used"] == "v2"
    assert receipts["job-unlabeled-after-rollback"]["amount"] == "12.50"
    assert receipts["job-old-add-v1"]["amount"] == "12.50"
    assert receipts["job-old-schedule-v1"]["amount"] == "12.50"

    review = report["review_by_original_job_id"]["job-threadpool-ambiguous"]
    assert review["status"] == "awaiting_manual_verification"
    assert review["original_args"] == ["B-203-003", 1250]

    written_ids = [write["job_id"] for write in report["writes"]]
    assert written_ids == [
        "job-a-inflight-v1",
        "job-legacy-ingest",
        "job-old-add-v1",
        "job-old-schedule-v1",
        "job-v2-switch",
        "job-unlabeled-after-rollback",
    ]
    assert "job-threadpool-ambiguous" not in written_ids
    assert report["max_concurrent_writes"] == 1
    assert report["final_claims"] == {"worker_a": None, "worker_b": None}
