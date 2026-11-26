"""Tests for resumable, paginated JSONPath queries."""

from __future__ import annotations

import copy
import json
import typing

import pytest

from jsonpath_ng import parse
from jsonpath_ng.jsonpath import Descendants
from jsonpath_ng.jsonpath import Fields
from jsonpath_ng.jsonpath import Root
from jsonpath_ng.jsonpath import This
from jsonpath_ng.jsonpath import Where
from jsonpath_ng.pagination import CursorQueryMismatchError
from jsonpath_ng.pagination import DataChangedError
from jsonpath_ng.pagination import InvalidCursorError
from jsonpath_ng.pagination import PagedFind
from jsonpath_ng.pagination import SourceUnavailableError
from jsonpath_ng.pagination import UnsupportedPathError

G1 = [
    {"id": 7, "name": "zero"},
    {"id": 7, "name": "one"},
]
# The same values as G1 but with the two adjacent orders swapped.
G2 = list(reversed(copy.deepcopy(G1)))
TASK_EXPRESSION = Descendants(Root(), Fields("id", "id"))


def _drain(finder: PagedFind, page_size: int) -> list:
    matches = []
    while True:
        page = finder.fetch(page_size)
        if not page:
            assert finder.exhausted
            break
        matches.extend(page)
    return matches


def _paths(matches: list) -> list[str]:
    return [str(match.full_path) for match in matches]


def test_find_equivalence_across_supported_paths() -> None:
    documents = [
        G1,
        {"store": {"book": [{"id": 1}, {"id": 2}], "id": 9}},
        {"a": {"b": [10, 20, {"id": 3}]}, "id": 5},
        [1, 2, [3, [4, 5]]],
        {"x": [{"y": [{"id": 1}, {"id": 1}]}, {"id": 2}]},
    ]
    expressions = [
        TASK_EXPRESSION,
        parse("$..id"),
        parse("$.id"),
        parse("$[0]"),
        parse("$[*]"),
        parse("$..[*]"),
        parse("$.a.b[1]"),
        parse("$.a.b[*]"),
        parse("$..b[0]"),
        parse("$[0:2]"),
        parse("$..x[*].y[*].id"),
        parse("$.store..id"),
        parse("$..a..id"),
        parse("$.a.b[::2]"),
        parse("$[-1]"),
        parse("$[0,2]"),
    ]
    for document in documents:
        for expression in expressions:
            expected = expression.find(copy.deepcopy(document))
            for page_size in (1, 2, 5):
                finder = PagedFind(document, expression, limit=page_size)
                actual = _drain(finder, page_size)
                assert _paths(actual) == _paths(expected)
                assert [match.value for match in actual] == [
                    match.value for match in expected
                ]
                for got, want in zip(actual, expected):
                    assert got.context == want.context


def test_duplicate_fields_match_twice_each_in_find_order() -> None:
    matches = TASK_EXPRESSION.find(G1)
    assert [match.value for match in matches] == [7, 7, 7, 7]
    assert _paths(matches) == ["[0].id", "[0].id", "[1].id", "[1].id"]

    finder = PagedFind(G1, TASK_EXPRESSION, limit=1)
    assert _paths(_drain(finder, 1)) == [
        "[0].id",
        "[0].id",
        "[1].id",
        "[1].id",
    ]


def test_resume_across_processes_keeps_parent_context(tmp_path: typing.Any) -> None:
    document_path = tmp_path / "version.json"
    receipt_path = tmp_path / "receipt.json"
    document_path.write_text(json.dumps(G1), encoding="utf-8")

    # First process: take only the first match and persist the receipt.
    first = PagedFind.from_file(document_path, TASK_EXPRESSION, limit=1)
    first_page = first.fetch()
    assert len(first_page) == 1
    assert _paths(first_page) == ["[0].id"]
    first.save_receipt(receipt_path)

    # Second process: reopen the original JSON with the same expression.
    receipt = PagedFind.load_receipt(receipt_path)
    resumed = PagedFind.resume_from_file(
        document_path, TASK_EXPRESSION, receipt, limit=1
    )
    next_match = resumed.fetch()[0]
    # The remaining duplicate of item 0 comes first, not anything from item 1.
    assert str(next_match.full_path) == "[0].id"
    assert next_match.value == 7
    # The datum still carries its real parent record.
    assert next_match.context.value == G1[0]
    assert str(next_match.context.path) == "[0]"

    rest = _drain(resumed, 2)
    assert _paths(rest) == ["[1].id", "[1].id"]
    assert resumed.emitted == 4


def test_resume_from_every_page_boundary_matches_find(tmp_path: typing.Any) -> None:
    expression = parse("$..id")
    document = {"a": {"id": 1, "b": {"id": 2}}, "c": [{"id": 3}, {"id": 4}]}
    expected = _paths(expression.find(document))

    for stop_after in range(1, len(expected) + 1):
        finder = PagedFind(document, expression, limit=1)
        for _ in range(stop_after):
            finder.fetch()
        receipt = finder.receipt
        resumed = PagedFind.resume(document, expression, receipt, limit=1)
        collected = resumed
        assert [str(match.full_path) for match in _drain(collected, 1)] == expected[
            stop_after:
        ]


def test_swapped_version_is_rejected_on_resume(tmp_path: typing.Any) -> None:
    g1_path = tmp_path / "g1.json"
    g2_path = tmp_path / "g2.json"
    receipt_path = tmp_path / "receipt.json"
    g1_path.write_text(json.dumps(G1), encoding="utf-8")
    g2_path.write_text(json.dumps(G2), encoding="utf-8")

    finder = PagedFind.from_file(g1_path, TASK_EXPRESSION, limit=1)
    finder.fetch()
    finder.save_receipt(receipt_path)

    with pytest.raises(DataChangedError):
        PagedFind.resume_from_file(
            g2_path, TASK_EXPRESSION, PagedFind.load_receipt(receipt_path)
        )


def test_different_query_with_same_cursor_is_rejected() -> None:
    finder = PagedFind(G1, TASK_EXPRESSION, limit=1)
    finder.fetch()
    with pytest.raises(CursorQueryMismatchError):
        PagedFind.resume(G1, parse("$..name"), finder.receipt)


def test_unavailable_or_invalid_source_is_rejected(tmp_path: typing.Any) -> None:
    with pytest.raises(SourceUnavailableError):
        PagedFind.from_file(tmp_path / "missing.json", TASK_EXPRESSION)

    broken = tmp_path / "broken.json"
    broken.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(SourceUnavailableError):
        PagedFind.from_file(broken, TASK_EXPRESSION)


def test_malformed_receipts_are_rejected() -> None:
    with pytest.raises(InvalidCursorError):
        PagedFind.resume(G1, TASK_EXPRESSION, {"version": 99})

    valid_finder = PagedFind(G1, TASK_EXPRESSION, limit=1)
    valid_finder.fetch()
    corrupted_state = valid_finder.receipt
    corrupted_state["state"] = {}
    with pytest.raises(InvalidCursorError):
        PagedFind.resume(G1, TASK_EXPRESSION, corrupted_state)

    corrupted_frame = valid_finder.receipt
    corrupted_frame["state"]["stack"] = [["bad"]]
    with pytest.raises(InvalidCursorError):
        PagedFind.resume(G1, TASK_EXPRESSION, corrupted_frame)

    # A frame referencing a child of a non-composite node is invalid.
    bad_ref = valid_finder.receipt
    bad_ref["state"]["stack"] = [
        [[0, 0], [], "eval", 1, -1, [], []],
    ]
    with pytest.raises(InvalidCursorError):
        PagedFind.resume(G1, TASK_EXPRESSION, bad_ref)


def test_auto_id_field_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("jsonpath_ng.jsonpath.auto_id_field", "id")
    with pytest.raises(UnsupportedPathError):
        PagedFind(G1, TASK_EXPRESSION)


def test_receipt_with_dangling_path_is_rejected() -> None:
    finder = PagedFind(G1, TASK_EXPRESSION, limit=1)
    finder.fetch()
    dangling = finder.receipt
    dangling["state"]["stack"][0][1] = [99]
    with pytest.raises(InvalidCursorError):
        PagedFind.resume(G1, TASK_EXPRESSION, dangling)


def test_multifield_and_slice_selectors() -> None:
    document = {"a": 1, "b": 2, "c": 3}
    fields_matches = _drain(PagedFind(document, Fields("a", "c"), limit=1), 1)
    assert [match.value for match in fields_matches] == [1, 3]

    array = [10, 20, 30, 40]
    slice_matches = _drain(PagedFind(array, parse("[1:4:2]"), limit=1), 1)
    assert [match.value for match in slice_matches] == [20, 40]
    assert str(slice_matches[0].context.path) == "$"



def test_unsupported_expressions_are_rejected() -> None:
    from jsonpath_ng.ext.parser import parse as ext_parse

    for expression in (
        ext_parse("$.a[?@.x > 1]"),
        Where(Fields("a"), Fields("b")),
        parse("`parent`"),
    ):
        with pytest.raises(UnsupportedPathError):
            PagedFind(G1, expression)


def test_limit_validation() -> None:
    finder = PagedFind(G1, TASK_EXPRESSION)
    with pytest.raises(ValueError):
        finder.fetch()
    with pytest.raises(ValueError):
        finder.fetch(0)
    with pytest.raises(ValueError):
        finder.fetch(True)


def test_constructor_limit_and_exhausted_receipt() -> None:
    expression = parse("$..id")
    document = {"a": {"id": 1}, "b": {"id": 2}}
    finder = PagedFind(document, expression, limit=1)
    first = finder.fetch()
    assert len(first) == 1
    assert finder.emitted == 1

    # Resume and drain to the end.
    resumed = PagedFind.resume(document, expression, finder.receipt, limit=1)
    assert resumed.emitted == 1
    resumed.fetch()
    assert resumed.fetch() == []
    assert resumed.exhausted

    # An already exhausted receipt still resumes cleanly and yields nothing.
    finished = PagedFind.resume(document, expression, resumed.receipt, limit=1)
    assert finished.fetch() == []
    assert finished.exhausted
    assert finished.emitted == 2


def test_receipt_file_round_trip(tmp_path: typing.Any) -> None:
    expression = parse("$..id")
    document = {"a": {"id": 1}, "b": {"id": 2}}
    finder = PagedFind(document, expression, limit=1)
    finder.fetch()
    target = tmp_path / "receipt.json"
    finder.save_receipt(target)

    loaded = PagedFind.load_receipt(target)
    assert json.loads(target.read_text(encoding="utf-8")) == loaded
    resumed = PagedFind.resume(document, expression, loaded, limit=1)
    assert [match.value for match in resumed.fetch()] == [2]

    with pytest.raises(InvalidCursorError):
        PagedFind.load_receipt(tmp_path / "missing.json")


def test_bare_this_and_root_remain_faithful() -> None:
    root_matches = _drain(PagedFind(G1, Root(), limit=1), 1)
    assert root_matches[0].value == G1
    this_matches = _drain(PagedFind(G1, This(), limit=1), 1)
    assert this_matches[0].value == G1
    assert str(this_matches[0].path) == "`this`"


def test_finished_subtree_is_not_retraversed_on_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from jsonpath_ng import pagination

    plain = [
        {"id": 1, "child": {"id": 2}},
        {"id": 3, "child": {"id": 4}},
    ]
    expression = parse("$..id")

    # Record every structural DFS node the descendant loop schedules,
    # i.e. nodes actually traversed rather than context materialization.
    scheduled: list[tuple] = []
    original = pagination._Engine._continue_descendants

    def spy(self: object, node: object, frame: object) -> None:
        if frame.mode == "desc" and frame.phase == 2 and frame.queue:
            scheduled.append(frame.queue[-1])
        original(self, node, frame)

    monkeypatch.setattr(pagination._Engine, "_continue_descendants", spy)

    first = PagedFind(plain, expression, limit=2)
    first_page = first.fetch()
    assert [match.value for match in first_page] == [1, 2]
    first_visits = set(scheduled)
    # The first batch structurally finished the whole [0] container lineage.
    assert {(0,), (0, "id"), (0, "child")} <= first_visits

    scheduled.clear()
    resumed = PagedFind.resume(plain, expression, first.receipt, limit=2)
    remaining = _drain(resumed, 2)
    assert [match.value for match in remaining] == [3, 4]

    second_visits = set(scheduled)
    # No structurally visited node is ever walked a second time.
    assert first_visits & second_visits == set()
    # Already finished ancestors of the first subtree are not revisited.
    assert (0,) not in second_visits
    assert (0, "id") not in second_visits
    assert (0, "child") not in second_visits
    # The still-pending leaf of [0] is consumed once, then traversal moves on.
    assert (0, "child", "id") in second_visits
    assert (1,) in second_visits
