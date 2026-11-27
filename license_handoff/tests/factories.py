"""构造 0.9.0 / 1.1.0 测试目录的夹具。"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, cast

import pystac

ITEM_IDS = ["A", "B", "C", "D"]
DATETIME = "2020-01-01T00:00:00Z"
EXTENT = {
    "spatial": {"bbox": [[0.0, 0.0, 1.0, 1.0]]},
    "temporal": {"interval": [[None, None]]},
}


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", "utf-8")


def build_legacy_package(
    package_dir: Path,
    default_license: str,
    item_licenses: dict[str, str | None],
    absolute_link_target: Path | None = None,
) -> Path:
    """构造 0.9.0 + commons 包。

    ``item_licenses`` 的值为 None 时省略 ``properties.license``。
    A 的 collection 链接用包外绝对地址，其余用包内相对地址。
    """
    package_dir = Path(package_dir)
    collection = {
        "stac_version": "0.9.0",
        "stac_extensions": ["commons"],
        "id": "delta",
        "description": "delta test collection",
        "properties": {"license": default_license},
        "extent": EXTENT,
        "links": [],
    }
    _write(package_dir / "collections" / "delta.json", collection)

    for item_id in ITEM_IDS:
        literal = item_licenses[item_id]
        properties: dict[str, Any] = {"datetime": DATETIME}
        if literal is not None:
            properties["license"] = literal
        if item_id == "A" and absolute_link_target is not None:
            collection_href = str(absolute_link_target.resolve())
        else:
            collection_href = "../collections/delta.json"
        item = {
            "stac_version": "0.9.0",
            "stac_extensions": ["commons"],
            "type": "Feature",
            "id": item_id,
            "collection": "delta",
            "properties": properties,
            "geometry": None,
            "bbox": [0.0, 0.0, 1.0, 1.0],
            "links": [
                {
                    "rel": "collection",
                    "href": collection_href,
                    "type": "application/json",
                }
            ],
            "assets": {},
        }
        _write(package_dir / "items" / f"{item_id}.json", item)
    return package_dir


def copy_package(src: Path, dst: Path) -> Path:
    shutil.copytree(src, dst)
    return dst


def set_current_item_license(package_dir: Path, item_id: str, license: str) -> None:
    """模拟入仓同事用 PySTAC 独立修改新包 Item 许可。"""
    item_path = Path(package_dir) / "items" / f"{item_id}.json"
    item = cast(pystac.Item, pystac.read_file(str(item_path)))
    item.common_metadata.license = license
    item.save_object(include_self_link=False)
