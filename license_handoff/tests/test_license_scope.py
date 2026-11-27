"""单元级保证：只同步许可，其余字段与显式/省略区别不被破坏。"""

from __future__ import annotations

import json
from pathlib import Path

from license_handoff import (
    PROVENANCE_PROPERTY,
    HandoffPaths,
    LicenseState,
    Side,
    initial_migration,
    merge_handoff,
    normalize_license,
)

from .factories import (
    ITEM_IDS,
    build_legacy_package,
    copy_package,
    set_current_item_license,
)


def test_non_license_fields_are_preserved_through_migration(tmp_path: Path) -> None:
    source = tmp_path / "source"
    build_legacy_package(
        source,
        default_license="various",
        item_licenses={"A": None, "B": None, "C": "various", "D": None},
        absolute_link_target=source / "collections" / "delta.json",
    )
    custom_a = source / "items" / "A.json"
    document = json.loads(custom_a.read_text("utf-8"))
    document["properties"]["custom:field"] = "keep-me"
    document["properties"]["platform"] = "sat-x"
    custom_a.write_text(json.dumps(document), "utf-8")

    output = HandoffPaths(tmp_path / "legacy", tmp_path / "current")
    initial_migration(source, output)

    legacy_a = json.loads((output.legacy_dir / "items" / "A.json").read_text("utf-8"))
    current_a = json.loads((output.current_dir / "items" / "A.json").read_text("utf-8"))
    assert legacy_a["properties"]["custom:field"] == "keep-me"
    assert legacy_a["properties"]["platform"] == "sat-x"
    assert current_a["properties"]["custom:field"] == "keep-me"
    assert current_a["properties"]["platform"] == "sat-x"
    assert "license" not in legacy_a["properties"]
    assert "license" not in current_a["properties"]
    assert PROVENANCE_PROPERTY in current_a["properties"]


def test_explicit_and_omitted_distinction_survives_merge(tmp_path: Path) -> None:
    source = tmp_path / "source"
    build_legacy_package(
        source,
        default_license="various",
        item_licenses={"A": None, "B": None, "C": "various", "D": None},
        absolute_link_target=source / "collections" / "delta.json",
    )
    base = HandoffPaths(tmp_path / "h1" / "legacy", tmp_path / "h1" / "current")
    initial_migration(source, base)
    sides = HandoffPaths(tmp_path / "s" / "legacy", tmp_path / "s" / "current")
    copy_package(base.legacy_dir, sides.legacy_dir)
    copy_package(base.current_dir, sides.current_dir)
    set_current_item_license(sides.current_dir, "A", "CC-BY-4.0")
    set_current_item_license(sides.current_dir, "B", "CC-BY-4.0")
    set_current_item_license(sides.current_dir, "C", "CC0-1.0")
    output = HandoffPaths(tmp_path / "h2" / "legacy", tmp_path / "h2" / "current")
    result, _ = merge_handoff(base, sides, output)
    assert result.conflicts == {}
    assert set(result.winners.values()) == {Side.CURRENT}

    for item_id in ITEM_IDS:
        legacy_props = json.loads(
            (output.legacy_dir / "items" / f"{item_id}.json").read_text("utf-8")
        )["properties"]
        current_props = json.loads(
            (output.current_dir / "items" / f"{item_id}.json").read_text("utf-8")
        )["properties"]
        if item_id == "D":
            assert "license" not in legacy_props
            assert "license" not in current_props
        else:
            assert "license" in legacy_props
            assert "license" in current_props


def test_normalize_rules_match_pystac_migration() -> None:
    assert normalize_license("various") == "other"
    assert normalize_license("proprietary") == "other"
    assert normalize_license("CC-BY-4.0") == "CC-BY-4.0"
    assert normalize_license("CC0-1.0") == "CC0-1.0"
    assert LicenseState.omitted().explicit is False
    assert LicenseState.written("various").explicit is True
