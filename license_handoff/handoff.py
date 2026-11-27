"""交接编排：初始迁移与后续三方合并，每次成功都交付一对目录。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .merge import (
    LicenseConflictError,
    MergeResult,
    Side,
    three_way_merge,
)
from .model import (
    LicenseState,
    Provenance,
    normalize_license,
)
from .readers import read_current_package, read_legacy_package
from .writers import write_current_package, write_legacy_package


@dataclass(frozen=True)
class HandoffPaths:
    """一次成功交接对应的一对目录。"""

    legacy_dir: Path
    current_dir: Path


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return cast(dict[str, Any], json.load(handle))


def _load_raw_legacy(
    package_dir: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    raw_collection = _load_json(package_dir / "collections" / "delta.json")
    raw_items = {
        path.stem: _load_json(path)
        for path in sorted((package_dir / "items").glob("*.json"))
    }
    return raw_collection, raw_items


def initial_migration(source_dir: str | Path, output: HandoffPaths) -> HandoffPaths:
    """把 0.9.0 源目录迁移成第一对交接目录。

    旧包保持 commons 约定（显式填写与省略的区别原样保留），
    新包用 PySTAC 写为 STAC 1.1.0；``various`` / ``proprietary`` 归并为 ``other``。
    """
    source_dir = Path(source_dir)
    snapshot = read_legacy_package(source_dir)
    raw_collection, raw_items = _load_raw_legacy(source_dir)

    legacy_states: dict[str, LicenseState] = {}
    current_states: dict[str, LicenseState] = {}
    provenances: dict[str, Provenance] = {}
    for item_id, fact in snapshot.items.items():
        legacy_states[item_id] = fact.item
        if fact.item.explicit:
            assert fact.item.literal is not None
            current_states[item_id] = LicenseState.written(
                normalize_license(fact.item.literal)
            )
            provenances[item_id] = Provenance.for_item(fact.item.literal)
        else:
            current_states[item_id] = LicenseState.omitted()
            provenances[item_id] = Provenance.for_collection(
                snapshot.collection_default
            )

    write_legacy_package(
        output.legacy_dir,
        raw_collection,
        raw_items,
        legacy_states,
        snapshot.collection_default,
        snapshot.collection_links,
    )
    write_current_package(
        output.current_dir,
        raw_collection,
        raw_items,
        current_states,
        provenances,
        normalize_license(snapshot.collection_default),
    )
    return output


def merge_handoff(
    base: HandoffPaths,
    sides: HandoffPaths,
    output: HandoffPaths,
    choices: dict[str, Side] | None = None,
) -> tuple[MergeResult, HandoffPaths]:
    """以基准对为准，合并旧方与新方的后续许可改动。

    存在未解决冲突时不写任何目录（保留上一对成功交付），抛出
    :class:`LicenseConflictError`；调用方做完选择后用同一对基准重试。
    """
    base_legacy = read_legacy_package(base.legacy_dir)
    base_current = read_current_package(base.current_dir)
    side_legacy = read_legacy_package(sides.legacy_dir)
    side_current = read_current_package(sides.current_dir)

    result = three_way_merge(base_legacy, base_current, side_legacy, side_current)
    if result.conflicts and not choices:
        raise LicenseConflictError(result.conflicts)

    merged = result.resolve(choices or {})

    raw_collection, raw_items = _load_raw_legacy(Path(sides.legacy_dir))
    legacy_states = {
        item_id: item.legacy_state for item_id, item in merged.items.items()
    }
    current_states = {
        item_id: item.current_state for item_id, item in merged.items.items()
    }
    provenances = {item_id: item.provenance for item_id, item in merged.items.items()}

    write_legacy_package(
        output.legacy_dir,
        raw_collection,
        raw_items,
        legacy_states,
        merged.legacy_default,
        side_legacy.collection_links,
    )
    write_current_package(
        output.current_dir,
        raw_collection,
        raw_items,
        current_states,
        provenances,
        merged.current_collection_license,
    )
    return result, output
