import json

import pytest

from cloudevents.archive import (
    LANE_INTEGER,
    LANE_STRING,
    STATUS_ASSIGNED,
    STATUS_CONFLICTED,
    STATUS_PENDING,
    STATUS_WITHDRAWN,
    Archive,
    CommitConflictError,
    IngestAMQP,
    IngestHTTP,
    IngestJSON,
    LineChange,
    Operation,
    RegisterManifest,
    RevokeManifest,
    UnknownCommitError,
    UnknownReceiptError,
    WithdrawReceipt,
)
from cloudevents.core.bindings.amqp import AMQPMessage
from cloudevents.core.bindings.http import HTTPMessage

SOURCE = "/warehouse/inbound"
EVENT_TYPE = "com.example.receipt"

# Receipt ids assigned by the archive in ingestion order.
A, C, D, B, E = 1, 2, 3, 4, 5

INT_LINE = 1
STRING_LINE = 2
BINARY_LINE = 3


def amqp_receipt(event_id: str) -> AMQPMessage:
    """A: AMQP binary mode, integer lane=7, text body OK."""
    return AMQPMessage(
        properties={"content-type": "text/plain"},
        application_properties={
            "cloudEvents_specversion": "1.0",
            "cloudEvents_id": event_id,
            "cloudEvents_source": SOURCE,
            "cloudEvents_type": EVENT_TYPE,
            "cloudEvents_lane": 7,
        },
        application_data=b"OK",
    )


def legacy_http_receipt(event_id: str) -> HTTPMessage:
    """B/E: legacy HTTP binary mode, ce-lane: 7 of unknown type, body OK."""
    return HTTPMessage(
        headers={
            "ce-specversion": "1.0",
            "ce-id": event_id,
            "ce-source": SOURCE,
            "ce-type": EVENT_TYPE,
            "ce-lane": "7",
            "content-type": "text/plain",
        },
        body=b"OK",
    )


def json_receipt(event_id: str, base64_body: bool = False) -> bytes:
    """C/D: structured JSON, string lane="7", text OK or data_base64 bytes OK."""
    document = {
        "specversion": "1.0",
        "id": event_id,
        "source": SOURCE,
        "type": EVENT_TYPE,
        "lane": "7",
        "datacontenttype": "text/plain",
    }
    if base64_body:
        document["data_base64"] = "T0s="
    else:
        document["data"] = "OK"
    return json.dumps(document).encode("utf-8")


def commit_ingest_acd() -> tuple[str, list[Operation]]:
    return "c1-ingest", [
        IngestAMQP(amqp_receipt("a-1")),
        IngestJSON(json_receipt("c-1")),
        IngestJSON(json_receipt("d-1", base64_body=True)),
    ]


def commit_ingest_be() -> tuple[str, list[Operation]]:
    return "c2-ingest-legacy", [
        IngestHTTP(legacy_http_receipt("b-1")),
        IngestHTTP(legacy_http_receipt("e-1")),
    ]


def commit_confirm_be() -> tuple[str, list[Operation]]:
    return "c3-confirm", [
        RegisterManifest("m-b-int", B, LANE_INTEGER),
        RegisterManifest("m-e-str", E, LANE_STRING),
    ]


def commit_conflict_b() -> tuple[str, list[Operation]]:
    return "c4-conflict", [RegisterManifest("m-b-str", B, LANE_STRING)]


def commit_revoke_conflict() -> tuple[str, list[Operation]]:
    return "c5-revoke", [RevokeManifest("m-b-str")]


def commit_swap_and_withdraw() -> tuple[str, list[Operation]]:
    return "c6-swap", [
        RevokeManifest("m-b-int"),
        RegisterManifest("m-b-str-2", B, LANE_STRING),
        RevokeManifest("m-e-str"),
        RegisterManifest("m-e-int-2", E, LANE_INTEGER),
        WithdrawReceipt(A),
        WithdrawReceipt(C),
    ]


def all_commits() -> list[tuple[str, list[Operation]]]:
    return [
        commit_ingest_acd(),
        commit_ingest_be(),
        commit_confirm_be(),
        commit_conflict_b(),
        commit_revoke_conflict(),
        commit_swap_and_withdraw(),
    ]


def build_archive() -> Archive:
    archive = Archive()
    for commit_id, operations in all_commits():
        archive.submit(commit_id, operations)
    return archive


def test_initial_receipts_get_separate_lines() -> None:
    archive = Archive()
    result = archive.submit(*commit_ingest_acd())

    assert result.receipt_ids == (A, C, D)
    assert {change.line_no for change in result.changes} == {
        INT_LINE,
        STRING_LINE,
        BINARY_LINE,
    }
    assert all(change.created for change in result.changes)

    assert archive.receipt(A).line_no == INT_LINE
    assert archive.receipt(A).lane_type == LANE_INTEGER
    assert archive.receipt(A).lane_value == 7
    assert archive.receipt(C).line_no == STRING_LINE
    assert archive.receipt(C).lane_type == LANE_STRING
    assert archive.receipt(C).lane_value == "7"
    # Same bytes as the text body, but explicitly binary-delivered.
    assert archive.receipt(D).line_no == BINARY_LINE
    assert archive.line(BINARY_LINE).key.body_kind == "binary"
    assert archive.line(BINARY_LINE).key.body_value == b"OK"
    assert archive.line(STRING_LINE).key.body_kind == "text"
    assert archive.line(STRING_LINE).key.body_value == "OK"


def test_legacy_receipts_pending_until_manifest_confirmation() -> None:
    archive = Archive()
    archive.submit(*commit_ingest_acd())
    legacy_message = legacy_http_receipt("b-1")
    result = archive.submit(*commit_ingest_be())

    assert result.receipt_ids == (B, E)
    assert result.changes == ()

    view = archive.receipt(B)
    assert view.status == STATUS_PENDING
    assert view.line_no is None
    # Raw content of the legacy receipt stays queryable while pending.
    assert view.raw == legacy_message

    result = archive.submit(*commit_confirm_be())
    assert result.changes == (
        LineChange(line_no=INT_LINE, added=(B,), removed=(), created=False),
        LineChange(line_no=STRING_LINE, added=(E,), removed=(), created=False),
    )
    # B shares the integer line with A, E shares the string line with C,
    # across bindings and event ids. D stays on its own line.
    assert archive.line(INT_LINE).members == (A, B)
    assert archive.line(STRING_LINE).members == (C, E)
    assert archive.line(BINARY_LINE).members == (D,)


def test_conflicting_manifests_suspend_and_restore_assignment() -> None:
    archive = Archive()
    for commit in (commit_ingest_acd(), commit_ingest_be(), commit_confirm_be()):
        archive.submit(*commit)

    result = archive.submit(*commit_conflict_b())
    assert result.changes == (
        LineChange(line_no=INT_LINE, added=(), removed=(B,), created=False),
    )

    view = archive.receipt(B)
    assert view.status == STATUS_CONFLICTED
    assert view.line_no is None
    assert {m.manifest_id for m in view.manifests} == {"m-b-int", "m-b-str"}
    assert {m.lane_type for m in view.manifests} == {LANE_INTEGER, LANE_STRING}

    # Other receipts are not affected by the conflict.
    assert archive.line(INT_LINE).members == (A,)
    assert archive.line(STRING_LINE).members == (C, E)
    assert archive.receipt(E).line_no == STRING_LINE

    result = archive.submit(*commit_revoke_conflict())
    assert result.changes == (
        LineChange(line_no=INT_LINE, added=(B,), removed=(), created=False),
    )
    assert archive.receipt(B).status == STATUS_ASSIGNED
    assert archive.line(INT_LINE).members == (A, B)


def test_final_commit_swaps_members_and_withdraws_atomically() -> None:
    archive = build_archive()

    # Line numbers survive the commit; members become E and B.
    assert archive.line(INT_LINE).members == (E,)
    assert archive.line(STRING_LINE).members == (B,)
    # D's line and members are untouched and not part of the change.
    assert archive.line(BINARY_LINE).members == (D,)

    assert archive.receipt(B).line_no == STRING_LINE
    assert archive.receipt(E).line_no == INT_LINE

    # Withdrawn receipts keep their raw content queryable.
    for receipt_id, expected_raw in (
        (A, amqp_receipt("a-1")),
        (C, json_receipt("c-1")),
    ):
        view = archive.receipt(receipt_id)
        assert view.status == STATUS_WITHDRAWN
        assert view.line_no is None
        assert view.raw == expected_raw

    final = archive.commit("c6-swap")
    assert final.changes == (
        LineChange(line_no=INT_LINE, added=(E,), removed=(A, B), created=False),
        LineChange(line_no=STRING_LINE, added=(B,), removed=(C, E), created=False),
    )
    assert all(change.line_no != BINARY_LINE for change in final.changes)
    # The lines were updated in place, not deleted and recreated.
    assert all(not change.created for change in final.changes)


def test_commit_resend_with_same_content_is_recorded_once() -> None:
    archive = build_archive()
    before_lines = archive.lines()

    commit_id, operations = commit_swap_and_withdraw()
    replayed = archive.submit(commit_id, operations)

    assert replayed.replayed is True
    assert replayed.receipt_ids == ()
    assert archive.commit_ids.count(commit_id) == 1
    assert archive.lines() == before_lines


def test_commit_id_reuse_with_different_content_fails() -> None:
    archive = Archive()
    archive.submit(*commit_ingest_acd())

    with pytest.raises(CommitConflictError):
        archive.submit("c1-ingest", [IngestJSON(json_receipt("other"))])

    # The failed commit left no trace.
    assert archive.commit_ids == ("c1-ingest",)
    assert archive.line(INT_LINE).members == (A,)


def test_replay_from_empty_archive_reproduces_state_and_changes() -> None:
    original = build_archive()
    commits = all_commits()

    replayed = Archive.replay(commits)

    assert replayed.commit_ids == original.commit_ids
    assert replayed.lines() == original.lines()
    for receipt_id in (A, C, D, B, E):
        assert replayed.receipt(receipt_id) == original.receipt(receipt_id)
    for commit_id, _ in commits:
        assert replayed.commit(commit_id) == original.commit(commit_id)


def test_unknown_receipt_and_commit_raise() -> None:
    archive = Archive()
    with pytest.raises(UnknownReceiptError):
        archive.receipt(999)
    with pytest.raises(UnknownCommitError):
        archive.commit("nope")
