"""写出一对交接目录。

* 旧包：手写 STAC 0.9.0 + commons JSON，保留“显式填写 / 省略”的原始区别，
  以及 A 的包外绝对 collection 链接、其余 Item 的包内相对链接。
* 新包：用 PySTAC 构建 STAC 1.1.0 对象并落盘，布局与旧包一致
  （``collections/``、``items/`` 平铺），全部使用相对链接，
  去掉 self 链接，保证目录整体搬迁后仍可被 ``pystac.read_file`` 打开。
"""

from __future__ import annotations

import json
import posixpath
from copy import deepcopy
from pathlib import Path
from typing import Any

import pystac
from pystac import CatalogType
from pystac.layout import HrefLayoutStrategy
from pystac.serialization import identify_stac_object
from pystac.serialization.migrate import migrate_to_latest
from pystac.validation import validate_dict

from .model import (
    PROVENANCE_PROPERTY,
    LicenseState,
    Provenance,
    normalize_license,
)

STAC_VERSION_LEGACY = "0.9.0"
COMMONS_EXTENSION = "commons"
COLLECTION_ID = "delta"
RELATIVE_COLLECTION_HREF = "../collections/delta.json"


class _PackageLayout(HrefLayoutStrategy):
    """把对象固定到 ``collections/`` 与 ``items/`` 平铺目录。

    返回锚定包根目录的绝对 self HREF；保存时选用 SELF_CONTAINED，
    PySTAC 会据此生成相对层级链接且不写 self 链接。
    """

    def __init__(self, root_href: str) -> None:
        self.root_href = root_href.rstrip("/")

    def get_catalog_href(self, cat: Any, parent_dir: str, is_root: bool) -> str:
        return posixpath.join(self.root_href, cat.DEFAULT_FILE_NAME)

    def get_collection_href(self, col: Any, parent_dir: str, is_root: bool) -> str:
        return posixpath.join(self.root_href, "collections", f"{col.id}.json")

    def get_item_href(self, item: Any, parent_dir: str) -> str:
        return posixpath.join(self.root_href, "items", f"{item.id}.json")


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_legacy_package(
    out_dir: str | Path,
    raw_collection: dict[str, Any],
    raw_items: dict[str, dict[str, Any]],
    item_states: dict[str, LicenseState],
    collection_default: str,
    collection_links: dict[str, str],
) -> None:
    """写出旧交接流程使用的 0.9.0 包。"""
    out_dir = Path(out_dir)
    collection_dict = deepcopy(raw_collection)
    collection_dict["stac_version"] = STAC_VERSION_LEGACY
    collection_dict["stac_extensions"] = [COMMONS_EXTENSION]
    collection_dict.setdefault("properties", {})["license"] = collection_default
    _write_json(out_dir / "collections" / "delta.json", collection_dict)

    for item_id, raw_item in raw_items.items():
        item_dict = deepcopy(raw_item)
        item_dict["stac_version"] = STAC_VERSION_LEGACY
        item_dict["stac_extensions"] = [COMMONS_EXTENSION]
        item_dict["collection"] = COLLECTION_ID
        properties = deepcopy(item_dict.get("properties", {}))
        properties.pop("license", None)
        properties.update(item_states[item_id].to_legacy_properties())
        item_dict["properties"] = properties
        item_dict["links"] = [
            link
            for link in item_dict.get("links", [])
            if link.get("rel") != "collection"
        ]
        item_dict["links"].append(
            {
                "rel": "collection",
                "href": collection_links[item_id],
                "type": "application/json",
            }
        )
        _write_json(out_dir / "items" / f"{item_id}.json", item_dict)


def _migrated_collection(
    raw_collection: dict[str, Any], collection_license: str
) -> pystac.Collection:
    info = identify_stac_object(raw_collection)
    migrated = migrate_to_latest(raw_collection, info)
    properties = migrated.get("properties")
    if isinstance(properties, dict):
        properties.pop("license", None)
        if not properties:
            migrated.pop("properties", None)
    migrated["license"] = collection_license
    return pystac.Collection.from_dict(migrated)


def _migrated_item(
    raw_item: dict[str, Any],
    state: LicenseState,
    provenance: Provenance,
) -> pystac.Item:
    info = identify_stac_object(raw_item)
    migrated = migrate_to_latest(raw_item, info)
    properties = migrated.setdefault("properties", {})
    properties.pop("license", None)
    if state.explicit:
        assert state.literal is not None
        properties["license"] = normalize_license(state.literal)
    properties[PROVENANCE_PROPERTY] = provenance.to_dict()
    migrated["links"] = [
        link for link in migrated.get("links", []) if link.get("rel") != "collection"
    ]
    return pystac.Item.from_dict(migrated)


def write_current_package(
    out_dir: str | Path,
    raw_collection: dict[str, Any],
    raw_items: dict[str, dict[str, Any]],
    item_states: dict[str, LicenseState],
    provenances: dict[str, Provenance],
    collection_license: str,
) -> None:
    """用 PySTAC 写出入仓程序使用的 STAC 1.1.0 包并离线校验。"""
    out_dir = Path(out_dir)
    root_href = str(out_dir.resolve())
    collection = _migrated_collection(raw_collection, collection_license)
    for item_id in sorted(raw_items):
        item = _migrated_item(
            raw_items[item_id], item_states[item_id], provenances[item_id]
        )
        collection.add_item(item)

    collection.normalize_hrefs(root_href, strategy=_PackageLayout(root_href))
    collection.save(catalog_type=CatalogType.SELF_CONTAINED)
    _validate_outputs(out_dir)


def _validate_outputs(out_dir: Path) -> None:
    collection_path = out_dir / "collections" / "delta.json"
    collection_dict = json.loads(collection_path.read_text(encoding="utf-8"))
    validate_dict(collection_dict, pystac.STACObjectType.COLLECTION)
    for item_path in sorted((out_dir / "items").glob("*.json")):
        item_dict = json.loads(item_path.read_text(encoding="utf-8"))
        validate_dict(item_dict, pystac.STACObjectType.ITEM)
