import pytest

from pyranges.acceptance import (
    ACCEPTED,
    CONFLICT,
    FAILED,
    AcceptanceLedger,
)


def build_example_ledger():
    ledger = AcceptanceLedger()
    scope = {"route": "K1", "work_order": "WO-TARGET"}
    documents = [
        {"id": "REV-A", "line": "现场复核", "start": 0, "end": 20, "verdict": "pass", "review": True, **scope},
        {"id": "REV-B", "line": "现场复核", "start": 100, "end": 120, "verdict": "pass", "review": True, **scope},
        {"id": "L3-A", "line": "third", "start": 0, "end": 20, "verdict": "fail", **scope},
        {"id": "L3-B", "line": "third", "start": 100, "end": 120, "verdict": "fail", **scope},
        {"id": "A0", "line": "line-1", "start": 0, "end": 20, "verdict": "pass", **scope},
        {"id": "K", "kind": "check", "start": 204, "end": 208, **scope},
        {
            "id": "A1", "line": "line-1", "start": 0, "end": 20, "verdict": "fail",
            "replaces": ["A0"], "requires": [("K", 204, 208)], **scope,
        },
        {"id": "A2", "line": "line-1", "start": 5, "end": 10, "verdict": "fail", "replaces": ["A1"], **scope},
        {"id": "B0", "line": "line-1", "start": 100, "end": 120, "verdict": "fail", **scope},
        {
            "id": "B1", "line": "line-1", "start": 100, "end": 120, "verdict": "pass",
            "replaces": ["B0"], "requires": [("A1", 7, 8)], **scope,
        },
    ]
    for document in documents:
        ledger.add_document(document, track=False)
    return ledger


def segment_map(report):
    return {(item["start"], item["end"]): item for item in report}


def test_complete_sources_use_latest_effective_evidence():
    ledger = build_example_ledger()
    segments = segment_map(ledger.evaluate(route="K1", work_order="WO-TARGET"))

    assert segments[(5, 10)]["status"] == FAILED
    assert segments[(5, 10)]["line_references"]["line-1"] == ["A2"]
    assert segments[(100, 120)]["status"] == ACCEPTED
    assert segments[(100, 120)]["line_references"]["line-1"] == ["B1"]
    assert ledger.is_qualified("A1", 7, 8)
    assert ledger.is_qualified("A1", 7.5)


def test_partial_void_propagates_through_dependency_chain():
    ledger = build_example_ledger()
    changes = ledger.add_void({"id": "V-K", "target": "K", "start": 205, "end": 206})
    changed = segment_map(changes)

    assert set(changed) == {(0, 5), (5, 10), (10, 20), (100, 120)}
    assert changed[(0, 5)]["after"]["status"] == ACCEPTED
    assert changed[(0, 5)]["after"]["line_references"]["line-1"] == ["A0"]
    assert changed[(10, 20)]["after"]["line_references"]["line-1"] == ["A0"]

    middle = changed[(5, 10)]["after"]
    assert middle["status"] == CONFLICT
    assert middle["conflicts"][0]["parties"] == ["A0", "A2"]

    assert changed[(100, 120)]["after"]["status"] == FAILED
    assert changed[(100, 120)]["after"]["line_references"]["line-1"] == ["B0"]
    assert not ledger.is_qualified("A1", 7, 8)
    assert not ledger.is_qualified("B1", 110)


def test_release_restores_original_state_and_evidence():
    ledger = build_example_ledger()
    ledger.add_void({"id": "V-K", "target": "K", "start": 205, "end": 206})
    changes = ledger.add_release({"id": "R-V-K", "void_id": "V-K"})
    changed = segment_map(changes)

    assert changed[(5, 10)]["after"]["line_references"]["line-1"] == ["A2"]
    assert changed[(100, 120)]["after"]["status"] == ACCEPTED
    assert changed[(100, 120)]["after"]["line_references"]["line-1"] == ["B1"]


def test_fixed_event_ids_are_idempotent():
    ledger = build_example_ledger()
    first = ledger.add_void({"id": "V-DUP", "target": "K", "start": 205, "end": 206})
    second = ledger.add_void({"id": "V-DUP", "target": "K", "start": 205, "end": 206})

    assert len(first) == 4
    assert second.duplicate
    assert len(second) == 0
    assert ledger.add_release({"id": "R-DUP", "void_id": "V-DUP"}).duplicate is False
    repeated = ledger.add_release({"id": "R-DUP", "void_id": "V-DUP"})
    assert repeated.duplicate
    assert len(repeated) == 0


def test_correction_only_recomputes_referenced_work_order():
    ledger = build_example_ledger()
    for index in range(2000):
        ledger.add_document({
            "id": f"NOISE-{index}",
            "line": f"other-line-{index % 7}",
            "route": "K1",
            "work_order": f"WO-OTHER-{index % 13}",
            "start": index,
            "end": index + 1,
            "verdict": "pass" if index % 2 else "fail",
        }, track=False)

    changes = ledger.add_void({"id": "V-PERF", "target": "K", "start": 205, "end": 206})

    assert len(changes) == 4
    assert ledger.last_recomputed_work_orders == {"WO-TARGET"}
    assert ledger.recomputed_document_count <= 12


def test_missing_reference_is_reported_separately_from_failure():
    ledger = AcceptanceLedger()
    scope = {"route": "R", "work_order": "WO"}
    ledger.add_document({"id": "REV", "line": "review", "start": 0, "end": 10, "verdict": "pass", "review": True, **scope}, track=False)
    ledger.add_document({
        "id": "S1", "line": "line-1", "start": 0, "end": 10, "verdict": "fail",
        "requires": [("MISSING-K", 1, 2)], **scope,
    }, track=False)

    segment = ledger.evaluate(route="R", work_order="WO")[0]
    assert "MISSING-K" in segment["missing_documents"]
    assert segment["failed_lines"] == []
