"""三方合并引擎。

以“上一对成功交付的目录”为基准（base），比较两条并行改动线：

* 旧方（legacy）：供应商继续改 0.9.0 目录；
* 新方（current）：入仓同事独立改 1.1.0 目录。

每个 Item 分别在各自表示法内与基准比较，避免拿 ``various`` 和 ``other``
这类跨版本字面值直接判等。Item 自身许可的变化与 Collection 默认值变化
（影响省略许可、按 commons 继承的 Item）都单独记录，作为冲突解释的业务信息。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .model import (
    CurrentItemFact,
    CurrentSnapshot,
    LegacyItemFact,
    LegacySnapshot,
    LicenseState,
    Provenance,
    normalize_license,
)


class Side(str, Enum):
    LEGACY = "legacy"
    CURRENT = "current"


class ChangeScope(str, Enum):
    ITEM = "item"
    COLLECTION = "collection"


@dataclass(frozen=True)
class ChangeRecord:
    """单条业务级改动说明。"""

    scope: ChangeScope
    message: str


@dataclass(frozen=True)
class Conflict:
    item_id: str
    legacy_changes: list[ChangeRecord]
    current_changes: list[ChangeRecord]

    def describe(self) -> str:
        legacy = "；".join(change.message for change in self.legacy_changes)
        current = "；".join(change.message for change in self.current_changes)
        return (
            f"Item {self.item_id} 冲突：旧方改动（{legacy}） vs 新方改动（{current}）"
        )


class LicenseConflictError(Exception):
    """存在未解决冲突时抛出，携带全部冲突供调用方决策。"""

    def __init__(self, conflicts: dict[str, Conflict]) -> None:
        self.conflicts = conflicts
        messages = "\n".join(conflict.describe() for conflict in conflicts.values())
        super().__init__(f"检测到 {len(conflicts)} 处许可冲突：\n{messages}")


@dataclass(frozen=True)
class MergedItem:
    item_id: str
    legacy_state: LicenseState
    current_state: LicenseState
    provenance: Provenance


@dataclass(frozen=True)
class MergedPackage:
    legacy_default: str
    current_collection_license: str
    items: dict[str, MergedItem]


@dataclass
class MergeResult:
    conflicts: dict[str, Conflict]
    winners: dict[str, Side]
    merged_legacy_default: str
    merged_current_collection_license: str
    legacy_changed_ids: set[str]
    current_changed_ids: set[str]
    _base_legacy: LegacySnapshot = field(repr=False)
    _base_current: CurrentSnapshot = field(repr=False)
    _side_legacy: LegacySnapshot = field(repr=False)
    _side_current: CurrentSnapshot = field(repr=False)

    @property
    def conflicted_ids(self) -> list[str]:
        return sorted(self.conflicts)

    def resolve(self, choices: dict[str, Side]) -> MergedPackage:
        unresolved = set(self.conflicts) - set(choices)
        if unresolved:
            raise LicenseConflictError(
                {item_id: self.conflicts[item_id] for item_id in sorted(unresolved)}
            )
        extra = set(choices) - set(self.conflicts)
        if extra:
            raise KeyError(f"以下 Item 没有冲突，无需选择：{sorted(extra)}")

        items: dict[str, MergedItem] = {}
        for item_id in self._side_legacy.items:
            if item_id in self.conflicts:
                side = choices[item_id]
            else:
                side = self.winners[item_id]
            items[item_id] = self._derive_item(item_id, side)
        return MergedPackage(
            legacy_default=self.merged_legacy_default,
            current_collection_license=self.merged_current_collection_license,
            items=items,
        )

    def _derive_item(self, item_id: str, side: Side) -> MergedItem:
        unchanged = (
            item_id not in self.legacy_changed_ids
            and item_id not in self.current_changed_ids
        )
        if unchanged:
            base_fact = self._base_current.items[item_id]
            return MergedItem(
                item_id,
                legacy_state=self._side_legacy.items[item_id].item,
                current_state=base_fact.item,
                provenance=base_fact.provenance,
            )

        if side is Side.LEGACY:
            state = self._side_legacy.items[item_id].item
            if state.explicit:
                assert state.literal is not None
                return MergedItem(
                    item_id,
                    legacy_state=state,
                    current_state=LicenseState.written(
                        normalize_license(state.literal)
                    ),
                    provenance=Provenance.for_item(state.literal),
                )
            return MergedItem(
                item_id,
                legacy_state=state,
                current_state=LicenseState.omitted(),
                provenance=Provenance.for_collection(self.merged_legacy_default),
            )

        current_fact = self._side_current.items[item_id]
        state = current_fact.item
        if state.explicit:
            assert state.literal is not None
            return MergedItem(
                item_id,
                legacy_state=LicenseState.written(state.literal),
                current_state=state,
                provenance=Provenance.for_item(state.literal),
            )
        return MergedItem(
            item_id,
            legacy_state=LicenseState.omitted(),
            current_state=state,
            provenance=Provenance.for_collection(self.merged_legacy_default),
        )


def _legacy_item_changes(
    fact: LegacyItemFact,
    base: LegacyItemFact,
    base_default: str,
    side_default: str,
) -> list[ChangeRecord]:
    changes: list[ChangeRecord] = []
    if fact.item != base.item:
        if not base.item.explicit and fact.item.explicit:
            assert fact.item.literal is not None
            changes.append(
                ChangeRecord(
                    ChangeScope.ITEM,
                    f"Item {fact.item_id} 由省略许可改为明确填写 {fact.item.literal}",
                )
            )
        elif base.item.explicit and not fact.item.explicit:
            assert base.item.literal is not None
            changes.append(
                ChangeRecord(
                    ChangeScope.ITEM,
                    f"Item {fact.item_id} 由明确填写 {base.item.literal} 改为省略许可",
                )
            )
        else:
            changes.append(
                ChangeRecord(
                    ChangeScope.ITEM,
                    f"Item {fact.item_id} 显式许可由 {base.item.literal} "
                    f"改为 {fact.item.literal}",
                )
            )
    elif not fact.item.explicit and side_default != base_default:
        changes.append(
            ChangeRecord(
                ChangeScope.COLLECTION,
                f"Collection 默认许可由 {base_default} 改为 {side_default}，"
                f"Item {fact.item_id} 省略许可、按 commons 继承新默认值",
            )
        )
    return changes


def _current_item_changes(
    fact: CurrentItemFact,
    base: CurrentItemFact,
    base_collection_license: str,
    side_collection_license: str,
) -> list[ChangeRecord]:
    changes: list[ChangeRecord] = []
    if fact.item != base.item:
        if not base.item.explicit and fact.item.explicit:
            assert fact.item.literal is not None
            changes.append(
                ChangeRecord(
                    ChangeScope.ITEM,
                    f"新包 Item {fact.item_id} 由省略许可改为明确填写 "
                    f"{fact.item.literal}",
                )
            )
        elif base.item.explicit and not fact.item.explicit:
            assert base.item.literal is not None
            changes.append(
                ChangeRecord(
                    ChangeScope.ITEM,
                    f"新包 Item {fact.item_id} 由明确填写 {base.item.literal} "
                    f"改为省略许可",
                )
            )
        else:
            changes.append(
                ChangeRecord(
                    ChangeScope.ITEM,
                    f"新包 Item {fact.item_id} 显式许可由 {base.item.literal} "
                    f"改为 {fact.item.literal}",
                )
            )
    elif not fact.item.explicit and side_collection_license != base_collection_license:
        changes.append(
            ChangeRecord(
                ChangeScope.COLLECTION,
                f"新包 Collection 许可由 {base_collection_license} 改为 "
                f"{side_collection_license}，Item {fact.item_id} 省略许可、"
                "继承新默认值",
            )
        )
    return changes


def three_way_merge(
    base_legacy: LegacySnapshot,
    base_current: CurrentSnapshot,
    side_legacy: LegacySnapshot,
    side_current: CurrentSnapshot,
) -> MergeResult:
    """计算三方合并结果；冲突项不给定赢家，其余项自动定赢家。"""
    if set(base_legacy.items) != set(side_legacy.items):
        raise KeyError("旧方 Item 集合与基准不一致")
    if set(base_current.items) != set(side_current.items):
        raise KeyError("新方 Item 集合与基准不一致")
    if set(base_legacy.items) != set(base_current.items):
        raise KeyError("基准两份目录的 Item 集合不一致")

    legacy_default_changed = (
        side_legacy.collection_default != base_legacy.collection_default
    )
    current_collection_changed = (
        side_current.collection_license != base_current.collection_license
    )
    if legacy_default_changed and current_collection_changed:
        raise NotImplementedError(
            "双方同时修改 Collection 许可的场景未在本次交接范围内"
        )

    merged_legacy_default = side_legacy.collection_default
    merged_current_collection_license = normalize_license(merged_legacy_default)
    if side_current.collection_license != base_current.collection_license:
        merged_current_collection_license = side_current.collection_license

    conflicts: dict[str, Conflict] = {}
    winners: dict[str, Side] = {}
    legacy_changed_ids: set[str] = set()
    current_changed_ids: set[str] = set()
    for item_id in sorted(base_legacy.items):
        legacy_changes = _legacy_item_changes(
            side_legacy.items[item_id],
            base_legacy.items[item_id],
            base_legacy.collection_default,
            side_legacy.collection_default,
        )
        current_changes = _current_item_changes(
            side_current.items[item_id],
            base_current.items[item_id],
            base_current.collection_license,
            side_current.collection_license,
        )
        if legacy_changes:
            legacy_changed_ids.add(item_id)
        if current_changes:
            current_changed_ids.add(item_id)
        if legacy_changes and current_changes:
            conflicts[item_id] = Conflict(item_id, legacy_changes, current_changes)
        elif legacy_changes:
            winners[item_id] = Side.LEGACY
        elif current_changes:
            winners[item_id] = Side.CURRENT
        else:
            winners[item_id] = Side.CURRENT

    return MergeResult(
        conflicts=conflicts,
        winners=winners,
        merged_legacy_default=merged_legacy_default,
        merged_current_collection_license=merged_current_collection_license,
        legacy_changed_ids=legacy_changed_ids,
        current_changed_ids=current_changed_ids,
        _base_legacy=base_legacy,
        _base_current=base_current,
        _side_legacy=side_legacy,
        _side_current=side_current,
    )
