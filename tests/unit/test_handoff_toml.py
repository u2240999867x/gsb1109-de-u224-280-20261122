import pytest

from check_jsonschema.handoff import NON_FINITE_NUMBER
from check_jsonschema.handoff.toml_parser import load


def _issue_map(doc):
    return {issue.pointer: issue for issue in doc.issues}


def test_nan_value_position():
    doc = load(b"[bins]\ncount = nan\n")
    issue = _issue_map(doc)["/bins/count"]
    assert issue.category == NON_FINITE_NUMBER
    assert [(loc.line, loc.column) for loc in issue.locations] == [(2, 9)]


def test_inf_variants_and_pointer_paths():
    text = 'a = nan\nb = inf\nc = -inf\n[s]\nx = +inf\n'
    doc = load(text.encode())
    issues = _issue_map(doc)
    assert set(issues) == {"/a", "/b", "/c", "/s/x"}
    assert [(loc.line, loc.column) for loc in issues["/s/x"].locations] == [(5, 5)]


def test_array_nonfinite_index_pointer():
    doc = load(b"a = [1, nan, 2, inf]\n")
    issues = _issue_map(doc)
    assert set(issues) == {"/a/1", "/a/3"}
    assert [(loc.line, loc.column) for loc in issues["/a/1"].locations] == [(1, 9)]
    assert [(loc.line, loc.column) for loc in issues["/a/3"].locations] == [(1, 17)]


@pytest.mark.parametrize(
    "text",
    [
        'nan = 1\ninf = "x"\n',
        "a.nan = 1\n",
        "[nan.inf]\nx = 1\n",
        "t = {nan = 1, inf = 2}\n",
        'a = "nan and inf"\n',
        "a = 1 # nan here\n",
        'a = """\nnan\ninf\n"""\n',
        '["nan"]\nx = 1\n',
    ],
)
def test_keys_strings_comments_are_not_values(text):
    assert load(text.encode()).issues == []


def test_data_is_parsed_normally():
    doc = load(b'[a]\nb = "x"\nc = [1, 2]\n')
    assert doc.data == {"a": {"b": "x", "c": [1, 2]}}
    assert doc.issues == []


def test_malformed_toml_raises():
    import tomllib

    with pytest.raises(tomllib.TOMLDecodeError):
        load(b"a = [\n")
