"""许可交接的数据模型。

只同步两类许可信息：

* Item 的 ``properties.license``（显式填写 vs 省略必须区分）；
* 旧 Collection 在 ``properties.license`` 提供的 commons 共享默认值。

旧写法（``various`` / ``proprietary``）与“是否改成明细自己填写”都属于业务信息，
因此模型保留原始字面值，归一化到 STAC 1.1.0 的结果单独记录。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

# 与 pystac.serialization.migrate._migrate_license 的规则保持一致：
# 0.9.0 的两个废弃写法在 1.1.0 都归并为 other。
LEGACY_LICENSE_TO_CURRENT: dict[str, str] = {
    "various": "other",
    "proprietary": "other",
}

# 随 1.1.0 Item 一起交付的来源属性，保证换进程后仍可追溯许可来源。
PROVENANCE_PROPERTY = "license_handoff:provenance"


def normalize_license(literal: str) -> str:
    """把旧版许可字面值归一化为 1.1.0 写法，SPDX 标识原样透传。"""
    return LEGACY_LICENSE_TO_CURRENT.get(literal, literal)


class LicenseSource(str, Enum):
    """新包 Item 许可的来源。"""

    ITEM = "item"
    COLLECTION = "collection"


@dataclass(frozen=True)
class LicenseState:
    """``properties.license`` 的书写状态。

    ``explicit=False`` 表示键被省略（commons 场景下继承 Collection 默认值），
    ``explicit=True`` 时 ``literal`` 是文件里的原字面值。
    """

    literal: str | None
    explicit: bool

    @classmethod
    def omitted(cls) -> "LicenseState":
        return cls(literal=None, explicit=False)

    @classmethod
    def written(cls, literal: str) -> "LicenseState":
        return cls(literal=literal, explicit=True)

    @classmethod
    def from_properties(cls, properties: dict[str, Any]) -> "LicenseState":
        if "license" in properties:
            return cls.written(properties["license"])
        return cls.omitted()

    def to_legacy_properties(self) -> dict[str, Any]:
        """生成旧包 Item 的 properties 片段：省略时不写入 license 键。"""
        if self.explicit:
            return {"license": self.literal}
        return {}


@dataclass(frozen=True)
class Provenance:
    """新包 Item 许可的来源记录。"""

    source: LicenseSource
    literal: str
    resolved: str

    @classmethod
    def for_item(cls, literal: str) -> "Provenance":
        return cls(LicenseSource.ITEM, literal, normalize_license(literal))

    @classmethod
    def for_collection(cls, literal: str) -> "Provenance":
        return cls(LicenseSource.COLLECTION, literal, normalize_license(literal))

    def to_dict(self) -> dict[str, str]:
        return {
            "source": self.source.value,
            "literal": self.literal,
            "resolved": self.resolved,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "Provenance":
        return cls(
            source=LicenseSource(data["source"]),
            literal=data["literal"],
            resolved=data["resolved"],
        )


@dataclass(frozen=True)
class LegacyItemFact:
    """旧包（0.9.0 + commons）中单个 Item 的许可事实。"""

    item_id: str
    item: LicenseState
    collection_default: str

    def effective_literal(self) -> str:
        """按 commons 约定生效的旧版字面值。"""
        if self.item.explicit:
            assert self.item.literal is not None
            return self.item.literal
        return self.collection_default

    def inherits_default(self) -> bool:
        return not self.item.explicit


@dataclass(frozen=True)
class CurrentItemFact:
    """新包（STAC 1.1.0）中单个 Item 的许可事实。"""

    item_id: str
    item: LicenseState
    collection_license: str
    provenance: Provenance

    def effective_license(self) -> str:
        if self.item.explicit:
            assert self.item.literal is not None
            return self.item.literal
        return self.collection_license


@dataclass(frozen=True)
class LegacySnapshot:
    """一份旧包目录读出的全部许可事实。"""

    collection_id: str
    collection_default: str
    items: dict[str, LegacyItemFact]
    # 每个 Item 原始 collection 链接的字面值（A 是包外绝对地址，其余是相对地址）。
    collection_links: dict[str, str]


@dataclass(frozen=True)
class CurrentSnapshot:
    """一份新包目录读出的全部许可事实。"""

    collection_id: str
    collection_license: str
    items: dict[str, CurrentItemFact]
