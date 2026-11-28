import json

import pytest

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

NORTH_ONLY = """\
north:
  <<: &common
    bins: &bins
      1: {count: .inf}
      "1": {count: 9}
      true: {count: 4}
  bins:
    <<: *bins
    "1": {count: 8}
"""

BINS_SCHEMA = {
    "type": "object",
    "properties": {
        "north": {
            "type": "object",
            "properties": {
                "bins": {
                    "type": "object",
                    "minProperties": 2,
                    "maxProperties": 2,
                    "properties": {
                        "1": {
                            "type": "object",
                            "properties": {"count": {"const": 8}},
                        },
                        "True": {
                            "type": "object",
                            "properties": {"count": {"const": 4}},
                        },
                    },
                    "additionalProperties": False,
                }
            },
        }
    },
}


@pytest.fixture
def schemafile(tmp_path):
    path = tmp_path / "schema.json"
    path.write_text("{}")
    return path


def _handoff_errors(result):
    report = json.loads(result.output)
    return report["handoff_errors"]


def test_full_handoff_run_fails_with_empty_contract(
    run_line, tmp_path, schemafile
):
    warehouse = tmp_path / "warehouse.yaml"
    warehouse.write_text(WAREHOUSE)
    handin = tmp_path / "handin.json"
    handin.write_text('{"bins":{"1":{"count":1e999},"\\u0031":{"count":9}}}')
    toml_doc = tmp_path / "handin.toml"
    toml_doc.write_text("[bins]\ncount = nan\n")
    null_doc = tmp_path / "null.json"
    null_doc.write_text('{"bins": {"1": {"count": null}}}')

    result = run_line(
        [
            "check-jsonschema",
            "--handoff",
            "-o",
            "json",
            "--schemafile",
            str(schemafile),
            str(warehouse),
            str(handin),
            str(toml_doc),
            str(null_doc),
        ]
    )
    assert result.exit_code == 1

    errors = _handoff_errors(result)
    by_file: dict[str, list[dict]] = {}
    for error in errors:
        by_file.setdefault(error["filename"], []).append(error)

    # the null document is clean and is left to the (empty) contract
    assert str(null_doc) not in by_file

    yaml_errors = by_file[str(warehouse)]
    east = [e for e in yaml_errors if e["pointer"].startswith("/east")]
    west = [e for e in yaml_errors if e["pointer"].startswith("/west")]
    north = [e for e in yaml_errors if e["pointer"].startswith("/north")]
    assert north == []

    for section, alias_line in (("east", 11), ("west", 13)):
        group = east if section == "east" else west
        duplicate = next(e for e in group if e["category"] == "duplicate_key")
        assert duplicate["pointer"] == f"/{section}/bins/1"
        assert duplicate["locations"] == [
            {"line": 4, "column": 7},
            {"line": 5, "column": 7},
        ]
        assert duplicate["alias_uses"] == [
            {"anchor": "common" if section == "east" else "bins",
             "line": alias_line, "column": 8 if section == "east" else 9}
        ]
        non_finite = next(
            e for e in group if e["category"] == "non_finite_number"
        )
        assert non_finite["pointer"] == f"/{section}/bins/1/count"
        assert non_finite["locations"] == [{"line": 4, "column": 18}]

    json_errors = by_file[str(handin)]
    assert {
        (e["category"], e["pointer"]) for e in json_errors
    } == {
        ("non_finite_number", "/bins/1/count"),
        ("duplicate_key", "/bins/1"),
    }

    toml_errors = by_file[str(toml_doc)]
    assert toml_errors == [
        {
            "filename": str(toml_doc),
            "category": "non_finite_number",
            "pointer": "/bins/count",
            "locations": [{"line": 2, "column": 9}],
        }
    ]


def test_clean_handoff_file_is_contract_checked(run_line, tmp_path):
    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps(BINS_SCHEMA))
    north = tmp_path / "north.yaml"
    north.write_text(NORTH_ONLY)

    result = run_line(
        [
            "check-jsonschema",
            "--handoff",
            "--schemafile",
            str(schema),
            str(north),
        ]
    )
    assert result.exit_code == 0


def test_handoff_error_file_skips_contract_validation(run_line, tmp_path):
    # a schema that would reject the data; the handoff error must fail first
    # and never reach the contract
    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({"type": "string"}))
    handin = tmp_path / "handin.json"
    handin.write_text('{"1": 1e999}')

    result = run_line(
        [
            "check-jsonschema",
            "--handoff",
            "-o",
            "json",
            "--schemafile",
            str(schema),
            str(handin),
        ]
    )
    assert result.exit_code == 1
    report = json.loads(result.output)
    assert report["errors"] == []
    assert len(report["handoff_errors"]) == 1
    assert report["handoff_errors"][0]["category"] == "non_finite_number"


def test_handoff_off_keeps_regular_loading(run_line, tmp_path, schemafile):
    doc = tmp_path / "doc.json"
    doc.write_text('{"a": 1}')
    result = run_line(
        [
            "check-jsonschema",
            "-o",
            "json",
            "--schemafile",
            str(schemafile),
            str(doc),
        ]
    )
    assert result.exit_code == 0
    report = json.loads(result.output)
    assert "handoff_errors" not in report


def test_text_reporter_includes_positions(run_line, tmp_path, schemafile):
    toml_doc = tmp_path / "handin.toml"
    toml_doc.write_text("count = nan\n")
    result = run_line(
        [
            "check-jsonschema",
            "--handoff",
            "--schemafile",
            str(schemafile),
            str(toml_doc),
        ]
    )
    assert result.exit_code == 1
    assert "non_finite_number" in result.output
    assert "line 1, column 9" in result.output
