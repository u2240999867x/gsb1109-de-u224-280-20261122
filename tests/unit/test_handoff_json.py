import pytest

from check_jsonschema.handoff import DUPLICATE_KEY, NON_FINITE_NUMBER
from check_jsonschema.handoff.json_parser import JsonParseError, load


def _issue_map(doc):
    return {(issue.category, issue.pointer): issue for issue in doc.issues}


def test_simple_data_and_null_passthrough():
    doc = load('{"a": null, "b": [1, 2.5], "c": {"x": true}}')
    assert doc.data == {"a": None, "b": [1, 2.5], "c": {"x": True}}
    assert doc.issues == []


def test_duplicate_decoded_keys_both_reported():
    doc = load('{"bins":{"1":{"count":1},"\\u0031":{"count":9}}}')
    issue = _issue_map(doc)[(DUPLICATE_KEY, "/bins/1")]
    assert [(loc.line, loc.column) for loc in issue.locations] == [(1, 10), (1, 26)]
    # last value wins in the delivered data
    assert doc.data["bins"]["1"] == {"count": 9}


def test_overflow_float_is_nonfinite_independent_of_optional_parser():
    doc = load('{"bins":{"1":{"count":1e999},"\\u0031":{"count":9}}}')
    non_finite = _issue_map(doc)[(NON_FINITE_NUMBER, "/bins/1/count")]
    assert [(loc.line, loc.column) for loc in non_finite.locations] == [(1, 23)]


def test_line_and_column_tracking_multiline():
    text = '{\n  "a": 1,\n  "a": 2,\n  "b": {\n    "x": 1e400\n  }\n}'
    doc = load(text)
    duplicate = _issue_map(doc)[(DUPLICATE_KEY, "/a")]
    assert [(loc.line, loc.column) for loc in duplicate.locations] == [(2, 3), (3, 3)]
    non_finite = _issue_map(doc)[(NON_FINITE_NUMBER, "/b/x")]
    assert [(loc.line, loc.column) for loc in non_finite.locations] == [(5, 10)]


def test_array_index_pointer():
    doc = load('[1, 1e999]')
    issue = _issue_map(doc)[(NON_FINITE_NUMBER, "/1")]
    assert issue.locations[0].line == 1


def test_integers_are_not_flagged():
    doc = load('{"big": 100000000000000000000, "neg": -5}')
    assert doc.issues == []
    assert isinstance(doc.data["big"], int)


def test_escaped_pointer_token():
    doc = load('{"a/b": {"c~d": 1e999}}')
    issue = _issue_map(doc)[(NON_FINITE_NUMBER, "/a~1b/c~0d")]
    assert issue is not None


@pytest.mark.parametrize(
    "text",
    [
        "{",
        '{"a":}',
        '{"a" 1}',
        '{"a": 1 "b": 2}',
        "tru",
        "[1,]",
        '{"a": 1} extra',
    ],
)
def test_malformed_json_raises(text):
    with pytest.raises(JsonParseError):
        load(text)


def test_bytes_input():
    doc = load(b'{"1": 1e999}')
    assert doc.issues[0].category == NON_FINITE_NUMBER
