"""三轮许可交接的端到端验收测试。"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, cast

import pytest

import pystac
from license_handoff import (
    PROVENANCE_PROPERTY,
    HandoffPaths,
    LicenseConflictError,
    Side,
    initial_migration,
    merge_handoff,
    read_current_package,
    read_legacy_package,
)

from .factories import (
    ITEM_IDS,
    build_legacy_package,
    copy_package,
    set_current_item_license,
)


def _legacy_item_props(path: Path) -> dict[str, Any]:
    document = cast(dict[str, Any], json.loads(path.read_text("utf-8")))
    return cast(dict[str, Any], document["properties"])


def _legacy_collection(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text("utf-8")))


def _current_item(path: Path) -> pystac.Item:
    return cast(pystac.Item, pystac.read_file(str(path)))


def _current_provenance(path: Path) -> dict[str, str]:
    return cast(dict[str, str], _current_item(path).properties[PROVENANCE_PROPERTY])


def test_initial_migration_pair(tmp_path: Path) -> None:
    source = tmp_path / "source"
    build_legacy_package(
        source,
        default_license="various",
        item_licenses={"A": None, "B": None, "C": "various", "D": None},
        absolute_link_target=source / "collections" / "delta.json",
    )
    base = HandoffPaths(
        legacy_dir=tmp_path / "handoff1" / "legacy",
        current_dir=tmp_path / "handoff1" / "current",
    )
    initial_migration(source, base)

    legacy_collection_path = base.legacy_dir / "collections" / "delta.json"
    legacy_collection = _legacy_collection(legacy_collection_path)
    assert legacy_collection["stac_version"] == "0.9.0"
    assert legacy_collection["stac_extensions"] == ["commons"]
    assert legacy_collection["properties"]["license"] == "various"

    for item_id in ITEM_IDS:
        props = _legacy_item_props(base.legacy_dir / "items" / f"{item_id}.json")
        if item_id == "C":
            assert props["license"] == "various"
        else:
            assert "license" not in props

    links = {
        item_id: json.loads(
            (base.legacy_dir / "items" / f"{item_id}.json").read_text("utf-8")
        )["links"]
        for item_id in ITEM_IDS
    }
    assert next(link for link in links["A"] if link["rel"] == "collection")[
        "href"
    ] == str((source / "collections" / "delta.json").resolve())
    for item_id in ["B", "C", "D"]:
        assert (
            next(link for link in links[item_id] if link["rel"] == "collection")["href"]
            == "../collections/delta.json"
        )

    snapshot = read_current_package(base.current_dir)
    assert snapshot.collection_license == "other"
    for item_id in ITEM_IDS:
        fact = snapshot.items[item_id]
        assert fact.effective_license() == "other"
        if item_id == "C":
            assert fact.item.explicit is True
            assert fact.provenance.source.value == "item"
            assert fact.provenance.literal == "various"
        else:
            assert fact.item.explicit is False
            assert fact.provenance.source.value == "collection"
            assert fact.provenance.literal == "various"
        assert fact.provenance.resolved == "other"

    # 搬迁前 A 的包外绝对链接保留原输入目录的字面值（0.9.0 Collection 不由 PySTAC 打开）
    relocated = tmp_path / "relocated"
    shutil.move(base.current_dir, relocated / "current")
    shutil.move(base.legacy_dir, relocated / "legacy")
    shutil.rmtree(source)
    for item_id in ITEM_IDS:
        item_path = relocated / "current" / "items" / f"{item_id}.json"
        item = cast(pystac.Item, pystac.read_file(str(item_path)))
        collection = item.get_collection()
        assert collection is not None
        assert collection.id == "delta"
        assert collection.license == "other"
    for item_id in ["B", "C", "D"]:
        legacy_item_path = relocated / "legacy" / "items" / f"{item_id}.json"
        item = cast(pystac.Item, pystac.read_file(str(legacy_item_path)))
        assert item.common_metadata.license == "other"
    # 包外绝对地址照旧保留字面值，不被改写成包内相对链接
    relocated_a_links = json.loads(
        (relocated / "legacy" / "items" / "A.json").read_text("utf-8")
    )["links"]
    assert next(link for link in relocated_a_links if link["rel"] == "collection")[
        "href"
    ].endswith("/source/collections/delta.json")


def test_second_handoff_conflicts_then_resolution(tmp_path: Path) -> None:
    source = tmp_path / "source"
    build_legacy_package(
        source,
        default_license="various",
        item_licenses={"A": None, "B": None, "C": "various", "D": None},
        absolute_link_target=source / "collections" / "delta.json",
    )
    base = HandoffPaths(tmp_path / "h1" / "legacy", tmp_path / "h1" / "current")
    initial_migration(source, base)

    sides = HandoffPaths(tmp_path / "sides" / "legacy", tmp_path / "sides" / "current")
    copy_package(base.legacy_dir, sides.legacy_dir)
    copy_package(base.current_dir, sides.current_dir)

    side_collection = sides.legacy_dir / "collections" / "delta.json"
    collection_dict = json.loads(side_collection.read_text("utf-8"))
    collection_dict["properties"]["license"] = "proprietary"
    side_collection.write_text(json.dumps(collection_dict), "utf-8")
    b_path = sides.legacy_dir / "items" / "B.json"
    b_dict = json.loads(b_path.read_text("utf-8"))
    b_dict["properties"]["license"] = "various"
    b_path.write_text(json.dumps(b_dict), "utf-8")

    set_current_item_license(sides.current_dir, "A", "CC-BY-4.0")
    set_current_item_license(sides.current_dir, "B", "CC-BY-4.0")
    set_current_item_license(sides.current_dir, "C", "CC0-1.0")

    blocked_output = HandoffPaths(
        tmp_path / "blocked" / "legacy", tmp_path / "blocked" / "current"
    )
    with pytest.raises(LicenseConflictError) as exc_info:
        merge_handoff(base, sides, blocked_output)
    assert set(exc_info.value.conflicts) == {"A", "B"}
    assert not blocked_output.legacy_dir.exists()
    assert not blocked_output.current_dir.exists()

    conflict_a = exc_info.value.conflicts["A"]
    assert any(
        change.scope.value == "collection" for change in conflict_a.legacy_changes
    )
    assert "various" in conflict_a.describe() and "proprietary" in conflict_a.describe()
    conflict_b = exc_info.value.conflicts["B"]
    assert any(change.scope.value == "item" for change in conflict_b.legacy_changes)
    assert "明确填写 various" in conflict_b.describe()

    output = HandoffPaths(tmp_path / "h2" / "legacy", tmp_path / "h2" / "current")
    result, _ = merge_handoff(
        base, sides, output, {"A": Side.CURRENT, "B": Side.LEGACY}
    )
    assert result.winners["C"] is Side.CURRENT
    assert result.winners["D"] is Side.LEGACY

    merged_collection_path = output.legacy_dir / "collections" / "delta.json"
    merged_collection = _legacy_collection(merged_collection_path)
    assert merged_collection["properties"]["license"] == "proprietary"
    expected_legacy = {
        "A": "CC-BY-4.0",
        "B": "various",
        "C": "CC0-1.0",
    }
    for item_id, literal in expected_legacy.items():
        assert (
            _legacy_item_props(output.legacy_dir / "items" / f"{item_id}.json")[
                "license"
            ]
            == literal
        )
    assert "license" not in _legacy_item_props(output.legacy_dir / "items" / "D.json")
    merged_legacy_snapshot = read_legacy_package(output.legacy_dir)
    d_fact = merged_legacy_snapshot.items["D"]
    assert d_fact.item.explicit is False
    assert d_fact.effective_literal() == "proprietary"

    merged_current_snapshot = read_current_package(output.current_dir)
    effective = {
        item_id: merged_current_snapshot.items[item_id].effective_license()
        for item_id in ITEM_IDS
    }
    assert effective == {
        "A": "CC-BY-4.0",
        "B": "other",
        "C": "CC0-1.0",
        "D": "other",
    }
    b_provenance = _current_provenance(output.current_dir / "items" / "B.json")
    assert b_provenance == {
        "source": "item",
        "literal": "various",
        "resolved": "other",
    }
    d_provenance = _current_provenance(output.current_dir / "items" / "D.json")
    assert d_provenance == {
        "source": "collection",
        "literal": "proprietary",
        "resolved": "other",
    }


def test_third_handoff_default_reverts_in_fresh_process(tmp_path: Path) -> None:
    source = tmp_path / "source"
    build_legacy_package(
        source,
        default_license="various",
        item_licenses={"A": None, "B": None, "C": "various", "D": None},
        absolute_link_target=source / "collections" / "delta.json",
    )
    base1 = HandoffPaths(tmp_path / "h1" / "legacy", tmp_path / "h1" / "current")
    initial_migration(source, base1)

    sides1 = HandoffPaths(tmp_path / "s1" / "legacy", tmp_path / "s1" / "current")
    copy_package(base1.legacy_dir, sides1.legacy_dir)
    copy_package(base1.current_dir, sides1.current_dir)
    collection_path = sides1.legacy_dir / "collections" / "delta.json"
    collection_dict = json.loads(collection_path.read_text("utf-8"))
    collection_dict["properties"]["license"] = "proprietary"
    collection_path.write_text(json.dumps(collection_dict), "utf-8")
    b_path = sides1.legacy_dir / "items" / "B.json"
    b_dict = json.loads(b_path.read_text("utf-8"))
    b_dict["properties"]["license"] = "various"
    b_path.write_text(json.dumps(b_dict), "utf-8")
    set_current_item_license(sides1.current_dir, "A", "CC-BY-4.0")
    set_current_item_license(sides1.current_dir, "B", "CC-BY-4.0")
    set_current_item_license(sides1.current_dir, "C", "CC0-1.0")
    base2 = HandoffPaths(tmp_path / "h2" / "legacy", tmp_path / "h2" / "current")
    merge_handoff(base1, sides1, base2, {"A": Side.CURRENT, "B": Side.LEGACY})

    sides2 = HandoffPaths(tmp_path / "s2" / "legacy", tmp_path / "s2" / "current")
    copy_package(base2.legacy_dir, sides2.legacy_dir)
    copy_package(base2.current_dir, sides2.current_dir)
    reverted_path = sides2.legacy_dir / "collections" / "delta.json"
    reverted = json.loads(reverted_path.read_text("utf-8"))
    reverted["properties"]["license"] = "various"
    reverted_path.write_text(json.dumps(reverted), "utf-8")

    output = HandoffPaths(tmp_path / "h3" / "legacy", tmp_path / "h3" / "current")
    result, _ = merge_handoff(base2, sides2, output)
    assert result.conflicts == {}

    b_item = _current_item(output.current_dir / "items" / "B.json")
    d_item = _current_item(output.current_dir / "items" / "D.json")
    assert b_item.common_metadata.license == "other"
    assert b_item.properties[PROVENANCE_PROPERTY] == {
        "source": "item",
        "literal": "various",
        "resolved": "other",
    }
    assert d_item.common_metadata.license is None
    d_collection = d_item.get_collection()
    assert d_collection is not None
    assert d_collection.license == "other"
    assert d_item.properties[PROVENANCE_PROPERTY] == {
        "source": "collection",
        "literal": "various",
        "resolved": "other",
    }

    third_legacy = read_legacy_package(output.legacy_dir)
    assert third_legacy.items["D"].item.explicit is False
    assert third_legacy.items["D"].effective_literal() == "various"
    assert third_legacy.items["B"].effective_literal() == "various"

    fresh_snapshot = read_current_package(output.current_dir)
    assert fresh_snapshot.items["B"].provenance.source.value == "item"
    assert fresh_snapshot.items["D"].provenance.source.value == "collection"
    assert fresh_snapshot.items["B"].effective_license() == "other"
    assert fresh_snapshot.items["D"].effective_license() == "other"


def test_handoff_state_survives_separate_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    build_legacy_package(
        source,
        default_license="various",
        item_licenses={"A": None, "B": None, "C": "various", "D": None},
        absolute_link_target=source / "collections" / "delta.json",
    )
    base1 = HandoffPaths(tmp_path / "h1" / "legacy", tmp_path / "h1" / "current")
    initial_migration(source, base1)
    sides1 = HandoffPaths(tmp_path / "s1" / "legacy", tmp_path / "s1" / "current")
    copy_package(base1.legacy_dir, sides1.legacy_dir)
    copy_package(base1.current_dir, sides1.current_dir)
    collection_path = sides1.legacy_dir / "collections" / "delta.json"
    collection_dict = json.loads(collection_path.read_text("utf-8"))
    collection_dict["properties"]["license"] = "proprietary"
    collection_path.write_text(json.dumps(collection_dict), "utf-8")
    b_path = sides1.legacy_dir / "items" / "B.json"
    b_dict = json.loads(b_path.read_text("utf-8"))
    b_dict["properties"]["license"] = "various"
    b_path.write_text(json.dumps(b_dict), "utf-8")
    set_current_item_license(sides1.current_dir, "A", "CC-BY-4.0")
    set_current_item_license(sides1.current_dir, "B", "CC-BY-4.0")
    set_current_item_license(sides1.current_dir, "C", "CC0-1.0")
    base2 = HandoffPaths(tmp_path / "h2" / "legacy", tmp_path / "h2" / "current")
    merge_handoff(base1, sides1, base2, {"A": Side.CURRENT, "B": Side.LEGACY})

    sides2 = HandoffPaths(tmp_path / "s2" / "legacy", tmp_path / "s2" / "current")
    copy_package(base2.legacy_dir, sides2.legacy_dir)
    copy_package(base2.current_dir, sides2.current_dir)
    reverted_path = sides2.legacy_dir / "collections" / "delta.json"
    reverted = json.loads(reverted_path.read_text("utf-8"))
    reverted["properties"]["license"] = "various"
    reverted_path.write_text(json.dumps(reverted), "utf-8")
    output = HandoffPaths(tmp_path / "h3" / "legacy", tmp_path / "h3" / "current")

    script = f"""
import json
from license_handoff import HandoffPaths, merge_handoff, read_current_package
base = HandoffPaths({str(base2.legacy_dir)!r}, {str(base2.current_dir)!r})
sides = HandoffPaths({str(sides2.legacy_dir)!r}, {str(sides2.current_dir)!r})
out = HandoffPaths({str(output.legacy_dir)!r}, {str(output.current_dir)!r})
result, _ = merge_handoff(base, sides, out)
assert result.conflicts == {{}}, result.conflicts
snapshot = read_current_package(out.current_dir)
print(json.dumps({{
    item_id: {{
        "effective": fact.effective_license(),
        "provenance": fact.provenance.to_dict(),
    }}
    for item_id, fact in snapshot.items.items()
}}))
"""
    import subprocess
    import sys

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=True,
    )
    report = json.loads(completed.stdout.strip().splitlines()[-1])
    assert report["B"] == {
        "effective": "other",
        "provenance": {"source": "item", "literal": "various", "resolved": "other"},
    }
    assert report["D"] == {
        "effective": "other",
        "provenance": {
            "source": "collection",
            "literal": "various",
            "resolved": "other",
        },
    }
