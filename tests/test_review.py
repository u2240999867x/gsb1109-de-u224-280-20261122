"""Tests for review sessions that keep sorted-list hits stable across edits."""

from __future__ import annotations

import warnings

import pytest

from jsonpath_ng.ext import parse
from jsonpath_ng.ext.review import ReviewError
from jsonpath_ng.ext.review import ReviewSession
from jsonpath_ng.ext.review import ReviewTicket
from jsonpath_ng.ext.review import StaleHitError


def _document() -> dict[str, object]:
    return {
        "items": [
            {"score": 9, "scans": [3], "mark": "待办"},
            {"score": 1, "scans": [7, 7], "mark": "待办"},
            {"score": 1, "scans": [7, 7], "mark": "待办"},
            {"score": 4, "scans": [8], "mark": "待办"},
        ]
    }


def test_sorted_hits_record_rank_and_original_position() -> None:
    session = ReviewSession(_document())

    hits = session.find("$.items[/score][0].mark")
    assert len(hits) == 1
    hit = hits[0]
    assert hit.original_path == "$.items[1].mark"
    assert hit.rank == 0
    assert hit.value == "待办"

    second = session.find("$.items[/score][1].scans[1]")[0]
    assert second.original_path == "$.items[2].scans[1]"
    assert second.rank == 1
    assert second.value == 7


def test_identical_rows_keep_document_order_in_sorted_rank() -> None:
    session = ReviewSession(_document())

    hits = session.find("$.items[/score]")
    assert len(hits) == 1
    # The whole sorted list still reads as an ordered list.
    assert [row["score"] for row in hits[0].value] == [1, 1, 4, 9]

    rows = session.find("$.items[/score][*]")
    assert [hit.original_path for hit in rows] == [
        "$.items[1]",
        "$.items[2]",
        "$.items[3]",
        "$.items[0]",
    ]
    assert [hit.rank for hit in rows] == [0, 1, 2, 3]


def test_duplicate_scalar_occurrences_are_distinct_hits() -> None:
    session = ReviewSession(_document())

    first_seven = session.find("$.items[/score][1].scans[0]")[0]
    second_seven = session.find("$.items[/score][1].scans[1]")[0]

    assert first_seven.original_path == "$.items[2].scans[0]"
    assert second_seven.original_path == "$.items[2].scans[1]"
    assert first_seven.token_id != second_seven.token_id


def test_full_concurrent_review_scenario() -> None:
    data = _document()
    session = ReviewSession(data)

    # First review ticket.
    first_hits = session.find("$.items[/score][0].mark") + session.find(
        "$.items[/score][1].scans[1]"
    )
    first_ticket = ReviewTicket(session)
    first_ticket.add_replace(first_hits[0], "通过")
    first_ticket.add_replace(first_hits[1], 9)

    # Second review ticket.
    second_hits = session.find("$.items[/score][1].scans[0]") + session.find(
        "$.items[/score][2].mark"
    )
    second_ticket = ReviewTicket(session)
    second_ticket.add_replace(second_hits[0], 99)
    second_ticket.add_replace(second_hits[1], "通过")

    # A colleague edits the same document through the same review entry point.
    edits = ReviewTicket(session)
    edits.add_delete(session.find("$.items[0]")[0])
    edits.submit()

    edits = ReviewTicket(session)
    edits.add_delete(session.find("$.items[1].scans[0]")[0])
    edits.submit()

    edits = ReviewTicket(session)
    edits.add_replace(session.find("$.items[1].score")[0], 10)
    edits.submit()

    assert [row["score"] for row in data["items"]] == [1, 10, 4]

    # Submitting the first ticket lands on the same occurrences, not old rows.
    first_ticket.submit()
    assert data["items"][0]["mark"] == "通过"
    assert data["items"][1]["scans"] == [9]
    assert [row["score"] for row in data["items"]] == [1, 10, 4]

    # The old ticket still reports the rank and position captured at selection.
    assert [hit.rank for hit in first_hits] == [0, 1]
    assert second_hits[0].rank == 1
    assert second_hits[0].original_path == "$.items[2].scans[0]"
    assert second_hits[1].rank == 2

    statuses = [session.refresh(hit) for hit in second_hits]
    assert statuses[0].found is False
    assert statuses[1].found is True
    assert statuses[1].path == "$.items[2].mark"

    # The whole ticket is rejected; the valid mark edit is not applied.
    with pytest.raises(StaleHitError) as exc_info:
        second_ticket.submit()
    assert exc_info.value.hit is second_hits[0]
    assert data["items"][2]["mark"] == "待办"

    # Re-submitting only the still-valid hit succeeds.
    retry = ReviewTicket(session)
    retry.add_replace(second_hits[1], "通过")
    retry.submit()
    assert data["items"][2]["mark"] == "通过"


def test_refresh_tracks_occurrence_after_sibling_deletion() -> None:
    data = _document()
    session = ReviewSession(data)

    surviving_seven = session.find("$.items[2].scans[1]")[0]
    deletion = ReviewTicket(session)
    deletion.add_delete(session.find("$.items[2].scans[0]")[0])
    deletion.submit()

    status = session.refresh(surviving_seven)
    assert status.found is True
    # The surviving occurrence moved to index 0 without becoming the
    # deleted one.
    assert status.path == "$.items[2].scans[0]"
    assert status.value == 7
    assert data["items"][2]["scans"] == [7]


def test_replacing_a_slot_retokenizes_and_stales_old_descendants() -> None:
    data = _document()
    session = ReviewSession(data)

    old_child = session.find("$.items[0].scans[0]")[0]
    ticket = ReviewTicket(session)
    ticket.add_replace(
        session.find("$.items[0]")[0], {"score": 7, "scans": [1, 2]}
    )
    ticket.submit()

    assert data["items"][0] == {"score": 7, "scans": [1, 2]}
    assert session.refresh(old_child).found is False

    new_hit = session.find("$.items[0].scans[1]")[0]
    assert new_hit.value == 2


def test_batch_is_atomic_when_any_hit_is_stale() -> None:
    data = _document()
    session = ReviewSession(data)

    valid_hit = session.find("$.items[3].mark")[0]
    doomed_hit = session.find("$.items[0].scans[0]")[0]

    deletion = ReviewTicket(session)
    deletion.add_delete(session.find("$.items[0]")[0])
    deletion.submit()

    ticket = ReviewTicket(session)
    ticket.add_replace(valid_hit, "通过")
    ticket.add_delete(doomed_hit)
    with pytest.raises(StaleHitError):
        ticket.submit()

    assert data["items"][2]["mark"] == "待办"


def test_review_supports_other_selectors() -> None:
    data = {
        "items": [
            {"score": 1, "tag": "a"},
            {"score": 2, "tag": "b"},
            {"score": 3, "tag": "a"},
        ],
        "weird key": 1,
    }
    session = ReviewSession(data)

    assert [
        hit.original_path for hit in session.find("$.items[*].score")
    ] == [
        "$.items[0].score",
        "$.items[1].score",
        "$.items[2].score",
    ]
    assert [hit.original_path for hit in session.find("$.items[0:2]")] == [
        "$.items[0]",
        "$.items[1]",
    ]
    assert [
        hit.original_path for hit in session.find("$.items[0,2].tag")
    ] == ["$.items[0].tag", "$.items[2].tag"]
    descendants = session.find("$..score")
    assert len(descendants) == 3
    filtered = session.find("$.items[?@.score > 1]")
    assert [hit.original_path for hit in filtered] == [
        "$.items[1]",
        "$.items[2]",
    ]
    assert session.find('$["weird key"]')[0].original_path == '$["weird key"]'


def test_review_deletes_dict_members_and_list_elements() -> None:
    data = {"object": {"keep": 1, "drop": 2}, "array": [1, 2, 3]}
    session = ReviewSession(data)

    ticket = ReviewTicket(session)
    ticket.add_delete(session.find("$.object.drop")[0])
    ticket.add_delete(session.find("$.array[0]")[0])
    ticket.submit()

    assert data == {"object": {"keep": 1}, "array": [2, 3]}
    assert session.find("$.array[0]")[0].value == 2


def test_sorted_filters_slices_and_unions_keep_real_locations() -> None:
    data = {
        "items": [
            {"score": 1, "tag": "a", "scans": [7, 7]},
            {"score": 2, "tag": "b", "scans": [3]},
            {"score": 1, "tag": "c", "scans": [9]},
        ]
    }
    session = ReviewSession(data)

    reverse = session.find("$.items[\\score][*]")
    assert [hit.value["score"] for hit in reverse] == [2, 1, 1]

    filtered = session.find("$.items[/score][?@.tag == 'c']")
    assert len(filtered) == 1
    assert filtered[0].original_path == "$.items[2]"
    assert filtered[0].rank == 1

    sliced = session.find("$.items[/score][0:2]")
    assert [hit.rank for hit in sliced] == [0, 1]

    multi_key = ReviewSession(
        {
            "rows": [
                {"a": 1, "b": 2},
                {"a": 1, "b": 1},
                {"a": 0, "b": 9},
            ]
        }
    )
    ordered = multi_key.find("$.rows[/a,/b][*]")
    assert [hit.original_path for hit in ordered] == [
        "$.rows[2]",
        "$.rows[1]",
        "$.rows[0]",
    ]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        where = session.find("$.items[*] where scans")
        assert [hit.original_path for hit in where] == [
            "$.items[0]",
            "$.items[1]",
            "$.items[2]",
        ]
        wherenot = session.find("$.items[*] wherenot missing")
        assert len(wherenot) == 3

        # The deprecated union operator has low precedence in the grammar;
        # the review layer must match the native query's interpretation.
        union_expression = "$.items[0] | $.items[2]"
        native = parse(union_expression).find(data)
        union = session.find(union_expression)
        assert [hit.value for hit in union] == [match.value for match in native]
        assert all(hit.original_path for hit in union)


def test_stale_error_names_the_original_location() -> None:
    data = _document()
    session = ReviewSession(data)
    hit = session.find("$.items[0].scans[0]")[0]

    deletion = ReviewTicket(session)
    deletion.add_delete(session.find("$.items[0]")[0])
    deletion.submit()

    with pytest.raises(StaleHitError, match=r"\$\.items\[0\]\.scans\[0\]"):
        ticket = ReviewTicket(session)
        ticket.add_delete(hit)
        ticket.submit()


def test_requery_reflects_new_rank_while_old_hit_keeps_its_rank() -> None:
    data = _document()
    session = ReviewSession(data)
    saved = session.find("$.items[/score][2].mark")[0]
    assert saved.rank == 2

    edits = ReviewTicket(session)
    edits.add_delete(session.find("$.items[0]")[0])
    edits.submit()

    edits = ReviewTicket(session)
    edits.add_delete(session.find("$.items[1].scans[0]")[0])
    edits.submit()

    edits = ReviewTicket(session)
    edits.add_replace(session.find("$.items[1].score")[0], 10)
    edits.submit()

    rows = session.find("$.items[/score][*]")
    assert [row.value["score"] for row in rows] == [1, 4, 10]
    assert rows[-1].original_path == "$.items[1]"

    assert saved.rank == 2
    status = session.refresh(saved)
    assert status.found is True
    assert status.path == "$.items[2].mark"


def test_ordinary_query_and_direct_edit_are_unchanged() -> None:
    data = _document()

    matches = parse("$.items[/score]").find(data)
    assert [row["score"] for row in matches[0].value] == [1, 1, 4, 9]

    parse("$.items[0].score").update(data, 5)
    assert data["items"][0]["score"] == 5


def test_root_cannot_be_replaced_or_deleted() -> None:
    session = ReviewSession({"a": 1})
    root = session.find("$")[0]
    ticket = ReviewTicket(session)
    ticket.add_replace(root, {"b": 2})
    with pytest.raises(ReviewError):
        ticket.submit()

    ticket = ReviewTicket(session)
    ticket.add_delete(root)
    with pytest.raises(ReviewError):
        ticket.submit()
