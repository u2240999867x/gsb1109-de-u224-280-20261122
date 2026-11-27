"""从旧包（0.9.0）与新包（1.1.0）读取许可事实。

旧包保持 STAC 0.9.0 + commons 写法，直接解析原始 JSON：
PySTAC 1.x 无法完整读取带 ``properties.license`` 的 0.9.0 Collection，
而本工具必须保留“显式填写 vs 省略”的原始区别，不能依赖迁移后的对象。

新包是入仓程序实际消费的 STAC 1.1.0 目录，按约定使用 ``pystac.read_file`` 打开。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pystac

from .model import (
    PROVENANCE_PROPERTY,
    CurrentItemFact,
    CurrentSnapshot,
    LegacyItemFact,
    LegacySnapshot,
    LicenseState,
    Provenance,
)

COLLECTION_REL = "collection"


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return cast(dict[str, Any], json.load(handle))


def _find_collection_link(item_dict: dict[str, Any]) -> str:
    for link in item_dict.get("links", []):
        if link.get("rel") == COLLECTION_REL:
            href = link.get("href")
            if href is not None:
                return cast(str, href)
    raise KeyError(f"Item {item_dict.get('id')} 缺少 collection 链接")


def read_legacy_package(package_dir: str | Path) -> LegacySnapshot:
    """读取旧包目录的许可事实（不做任何迁移）。"""
    package_dir = Path(package_dir)
    collection_path = package_dir / "collections" / "delta.json"
    collection_dict = _load_json(collection_path)
    if collection_dict.get("stac_version") != "0.9.0":
        raise ValueError(f"{collection_path} 不是 STAC 0.9.0 Collection")

    collection_default = collection_dict.get("properties", {}).get("license")
    if collection_default is None:
        raise ValueError(f"{collection_path} 缺少 properties.license 共享默认值")
    collection_id = collection_dict["id"]

    items: dict[str, LegacyItemFact] = {}
    collection_links: dict[str, str] = {}
    items_dir = package_dir / "items"
    for item_path in sorted(items_dir.glob("*.json")):
        item_dict = _load_json(item_path)
        if item_dict.get("stac_version") != "0.9.0":
            raise ValueError(f"{item_path} 不是 STAC 0.9.0 Item")
        if "commons" not in item_dict.get("stac_extensions", []):
            raise ValueError(f"{item_path} 未声明 commons 扩展")
        item_id = item_dict["id"]
        items[item_id] = LegacyItemFact(
            item_id=item_id,
            item=LicenseState.from_properties(item_dict.get("properties", {})),
            collection_default=collection_default,
        )
        collection_links[item_id] = _find_collection_link(item_dict)

    return LegacySnapshot(
        collection_id=collection_id,
        collection_default=collection_default,
        items=items,
        collection_links=collection_links,
    )


def read_current_package(package_dir: str | Path) -> CurrentSnapshot:
    """用 ``pystac.read_file`` 打开新包并读取许可事实。"""
    package_dir = Path(package_dir)
    items_dir = package_dir / "items"
    item_paths = sorted(items_dir.glob("*.json"))
    if not item_paths:
        raise ValueError(f"{items_dir} 下没有 Item 文件")

    items: dict[str, CurrentItemFact] = {}
    collection_license: str | None = None
    collection_id: str | None = None
    for item_path in item_paths:
        item = pystac.read_file(str(item_path))
        if not isinstance(item, pystac.Item):
            raise ValueError(f"{item_path} 读取结果不是 pystac.Item")
        provenance_data = item.properties.get(PROVENANCE_PROPERTY)
        if provenance_data is None:
            raise ValueError(f"{item_path} 缺少 {PROVENANCE_PROPERTY} 来源记录")
        provenance = Provenance.from_dict(provenance_data)

        collection = item.get_collection()
        if not isinstance(collection, pystac.Collection):
            raise ValueError(f"{item_path} 无法解析到 pystac.Collection")
        if collection_license is None:
            collection_license = collection.license
            collection_id = collection.id
        elif collection.license != collection_license:
            raise ValueError("新包内各 Item 解析到的 Collection 许可不一致")

        items[item.id] = CurrentItemFact(
            item_id=item.id,
            item=LicenseState.from_properties(item.properties),
            collection_license=collection.license,
            provenance=provenance,
        )

    assert collection_license is not None and collection_id is not None
    return CurrentSnapshot(
        collection_id=collection_id,
        collection_license=collection_license,
        items=items,
    )
