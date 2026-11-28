import io

import pytest

from check_jsonschema.handoff import (
    DUPLICATE_KEY,
    NON_FINITE_NUMBER,
)
from check_jsonschema.handoff.yaml_parser import load

WAREHOUSE = """\
north:
  <<: &common
    bins: &bins
      1: {count: .inf}
      "1": {count: 9}
      true: {count: 4}
  bins:
    <<: *bins
    "1": {count: 8}
east:
  <<: [*common, {bins: {"1": {count: 3}}}]
west:
  bins: *bins
"""


def _load(text):
    return load(io.BytesIO(text.encode()))


def _issue_map(doc):
    return {(issue.category, issue.pointer): issue for issue in doc.issues}


def test_field_name_rules():
    doc = _load("1: a\ntrue: b\nplain: c\n")
    assert doc.data == {"1": "a", "True": "b", "plain": "c"}
    assert doc.issues == []


def test_decoded_string_key_name():
    doc = _load('"1": a\n')
    assert list(doc.data) == ["1"]


def test_warehouse_delivered_data():
    doc = _load(WAREHOUSE)
    assert doc.data["north"]["bins"] == {"1": {"count": 8}, "True": {"count": 4}}
    assert doc.data["east"]["bins"] == {"1": {"count": 9}, "True": {"count": 4}}
    assert doc.data["west"]["bins"] == {"1": {"count": 9}, "True": {"count": 4}}


def test_north_has_no_handoff_errors():
    doc = _load(WAREHOUSE)
    north_pointers = [i.pointer for i in doc.issues if i.pointer.startswith("/north")]
    assert north_pointers == []


def test_east_and_west_each_get_duplicate_and_nonfinite_receipts():
    doc = _load(WAREHOUSE)
    issues = _issue_map(doc)
    for section, alias_line in (("east", 11), ("west", 13)):
        duplicate = issues[(DUPLICATE_KEY, f"/{section}/bins/1")]
        assert [(loc.line, loc.column) for loc in duplicate.locations] == [
            (4, 7),
            (5, 7),
        ]
        assert len(duplicate.alias_uses) == 1
        assert duplicate.alias_uses[0].line == alias_line

        non_finite = issues[(NON_FINITE_NUMBER, f"/{section}/bins/1/count")]
        assert [(loc.line, loc.column) for loc in non_finite.locations] == [(4, 18)]
        assert len(non_finite.alias_uses) == 1
        assert non_finite.alias_uses[0].line == alias_line


def test_alias_use_anchor_names_per_section():
    doc = _load(WAREHOUSE)
    issues = _issue_map(doc)
    assert issues[(DUPLICATE_KEY, "/east/bins/1")].alias_uses[0].anchor == "common"
    assert issues[(DUPLICATE_KEY, "/west/bins/1")].alias_uses[0].anchor == "bins"


def test_shared_default_reports_separate_field_paths():
    doc = _load(WAREHOUSE)
    pointers = sorted(issue.pointer for issue in doc.issues)
    assert all(p.startswith("/east") or p.startswith("/west") for p in pointers)
    assert any(p.startswith("/east/bins/1") for p in pointers)
    assert any(p.startswith("/west/bins/1") for p in pointers)


def test_local_override_replaces_default_and_suppresses_its_errors():
    text = """\
base: &b
  bins:
    1: {count: .inf}
    "1": {count: 9}
section:
  <<: *b
  bins:
    "1": {count: 8}
"""
    doc = _load(text)
    assert doc.data["section"]["bins"] == {"1": {"count": 8}}
    assert all(
        not issue.pointer.startswith("/section") for issue in doc.issues
    )


def test_first_merge_mapping_provides_field():
    text = """\
a: &a
  k: 1
b: &b
  k: 2
  only_b: 3
section:
  <<: [*a, *b]
"""
    doc = _load(text)
    # the first mapping in the merge list to provide 'k' wins
    assert doc.data["section"]["k"] == 1
    assert doc.data["section"]["only_b"] == 3
    assert doc.issues == []


def test_local_entry_overrides_merge_provider():
    text = """\
a: &a
  k: 1
section:
  <<: *a
  k: 2
"""
    doc = _load(text)
    assert doc.data["section"]["k"] == 2
    assert doc.issues == []


def test_losing_duplicate_value_still_checked_for_nonfinite():
    text = """\
section:
  bins:
    1: {count: .inf}
    "1": {count: 8}
"""
    doc = _load(text)
    assert doc.data["section"]["bins"]["1"] == {"count": 8}
    categories = {issue.category for issue in doc.issues}
    assert DUPLICATE_KEY in categories
    assert NON_FINITE_NUMBER in categories
    non_finite = next(
        issue
        for issue in doc.issues
        if issue.category == NON_FINITE_NUMBER
    )
    assert non_finite.pointer == "/section/bins/1/count"
    assert [(loc.line, loc.column) for loc in non_finite.locations] == [(3, 16)]


def test_nonfinite_float_variants():
    text = "a: .nan\nb: .Inf\nc: -.inf\n"
    doc = _load(text)
    pointers = sorted(issue.pointer for issue in doc.issues)
    assert pointers == ["/a", "/b", "/c"]
    assert all(issue.category == NON_FINITE_NUMBER for issue in doc.issues)


def test_empty_document():
    assert _load("").data is None
    assert _load("").issues == []


def test_root_nonfinite_scalar():
    doc = _load(".inf\n")
    assert doc.issues[0].category == NON_FINITE_NUMBER
    assert doc.issues[0].pointer == ""
    assert (doc.issues[0].locations[0].line, doc.issues[0].locations[0].column) == (
        1,
        1,
    )


def test_alias_as_key_is_resolved_and_located():
    text = "a: &k 1\nm:\n  *k : 1\n  \"1\": 2\n"
    doc = _load(text)
    assert doc.data["m"]["1"] == 2
    duplicate = next(issue for issue in doc.issues if issue.pointer == "/m/1")
    assert [(loc.line, loc.column) for loc in duplicate.locations] == [(3, 3), (4, 3)]
    assert duplicate.alias_uses[0].anchor == "k"
    assert (duplicate.alias_uses[0].line, duplicate.alias_uses[0].column) == (3, 3)


def test_invalid_yaml_raises_parse_error():
    from ruamel.yaml import YAMLError

    with pytest.raises(YAMLError):
        _load("a: {b\n")


def test_reused_anchor_does_not_warn_or_fail(recwarn):
    text = "a: &anchor\n  - 1\nb: &anchor 2\nm:\n  x: *anchor\n"
    doc = _load(text)
    assert doc.data == {"a": [1], "b": 2, "m": {"x": 2}}
    assert doc.issues == []
    assert recwarn.list == []


def test_multiple_documents_raise_like_the_regular_loader():
    from ruamel.yaml import YAMLError

    with pytest.raises(YAMLError):
        _load("a: 1\n---\nb: 2\n")


def test_sequence_with_nonfinite():
    doc = _load("v: [.inf, 1, .nan]\n")
    assert [(i.pointer, i.category) for i in doc.issues] == [
        ("/v/0", NON_FINITE_NUMBER),
        ("/v/2", NON_FINITE_NUMBER),
    ]
