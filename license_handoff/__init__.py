"""STAC 0.9.0 → 1.1.0 影像档案许可迁移与三方交接工具。"""

from __future__ import annotations

from .handoff import HandoffPaths, initial_migration, merge_handoff
from .merge import (
    Conflict,
    LicenseConflictError,
    MergeResult,
    Side,
    three_way_merge,
)
from .model import (
    PROVENANCE_PROPERTY,
    LicenseSource,
    LicenseState,
    Provenance,
    normalize_license,
)
from .readers import read_current_package, read_legacy_package

__all__ = [
    "Conflict",
    "HandoffPaths",
    "LicenseConflictError",
    "LicenseSource",
    "LicenseState",
    "MergeResult",
    "PROVENANCE_PROPERTY",
    "Provenance",
    "Side",
    "initial_migration",
    "merge_handoff",
    "normalize_license",
    "read_current_package",
    "read_legacy_package",
    "three_way_merge",
]
