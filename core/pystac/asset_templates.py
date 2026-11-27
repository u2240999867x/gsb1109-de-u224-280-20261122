"""
Asset template registration and versioned template updates for ingested assets.

Ingest builds :class:`~pystac.Asset` instances from a Collection's
``item_assets`` definitions. Each generated asset is *registered* against the
asset key and template version it was generated from. Reviewers then edit the
asset directly -- ordinary attribute assignment, list operations and nested
dict edits -- and those edits are recorded as local decisions. When the
publishing side releases a new template version, registered assets are updated
with a three-way merge between the template version they were generated from,
the new template version and the accumulated local decisions:

* fields the reviewer never touched follow the new template, including fields
  that disappear from the template;
* any explicit write counts as a local decision, including writing back a
  value identical to the template's value;
* explicitly deleted fields stay missing, even if a later template version
  reintroduces them;
* nested dicts are merged recursively, so editing one sub-key (or deleting one
  sub-key) does not lock the whole object;
* assets that were never registered are left untouched and can be reported
  with :func:`unregistered_assets`.

The registration bookkeeping is stored in the owning Item's ``extra_fields``
under :data:`REGISTRY_KEY`, so it survives saving and re-reading the Item.
"""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any, Callable, Iterable, SupportsIndex, cast

from pystac import Asset, Item
from pystac.item_assets import (
    ASSET_DESC_PROP,
    ASSET_ROLES_PROP,
    ASSET_TITLE_PROP,
    ASSET_TYPE_PROP,
)

if TYPE_CHECKING:
    from pystac.collection import Collection

#: Item ``extra_fields`` key holding the asset template registry.
REGISTRY_KEY = "asset_templates"

#: Sentinel stored in a decision tree to mark an explicitly deleted field.
_MISSING = "__missing__"

_ASSET_NAMED_FIELDS = {
    ASSET_TITLE_PROP,
    ASSET_DESC_PROP,
    ASSET_TYPE_PROP,
    ASSET_ROLES_PROP,
}

Path = tuple[str, ...]


def _is_missing(value: Any) -> bool:
    return isinstance(value, str) and value == _MISSING


def _is_plain_dict(value: Any) -> bool:
    """Only plain dicts participate in recursive merging."""
    return isinstance(value, dict)


def _coerce_template(template: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(template, dict):
        raise TypeError(f"asset template must be a dict, got {type(template).__name__}")
    return template


def _deep_diff(base: Any, current: Any) -> Any:
    """Return the sub-tree where ``current`` differs from ``base``.

    Nested plain dicts are compared recursively; every other type is compared
    as a whole. A missing key is represented with the :data:`_MISSING`
    sentinel, so explicit deletions stay explicit across template versions.
    """
    if _is_plain_dict(base) and _is_plain_dict(current):
        diff: dict[str, Any] = {}
        for key in base.keys() | current.keys():
            if key not in current:
                diff[key] = _MISSING
            elif key not in base:
                diff[key] = deepcopy(current[key])
            else:
                nested = _deep_diff(base[key], current[key])
                if nested:
                    diff[key] = nested
        return diff

    if base != current:
        return deepcopy(current)
    return {}


def _merge_decision_trees(old: Any, fresh: Any) -> Any:
    """Accumulate newly recorded decisions on top of earlier ones.

    Leaf decisions are replaced by the fresh value; nested dicts are merged
    recursively, so a decision recorded against an older template shape (for
    example a deleted sub-key) survives a round in which the template changed
    shape and the fresh diff only mentions part of the object.
    """
    if fresh is None or (isinstance(fresh, dict) and not fresh):
        return deepcopy(old)
    if _is_plain_dict(old) and _is_plain_dict(fresh):
        merged = deepcopy(old)
        for key, value in fresh.items():
            if key in merged:
                merged[key] = _merge_decision_trees(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged
    return deepcopy(fresh)


def _merge_with_template(base: Any, new_template: Any, decisions: Any) -> Any:
    """Three-way merge of one template field with recorded local decisions."""
    if decisions is None:
        return deepcopy(new_template)

    if _is_missing(decisions):
        return _MISSING

    if _is_plain_dict(decisions):
        if not _is_plain_dict(new_template):
            # The template changed shape (e.g. dict -> scalar). Local edits to
            # individual sub-keys cannot be projected onto the new shape, so
            # the locally decided structure wins as a whole.
            return _reify(decisions)
        merged: dict[str, Any] = {}
        for key, decision in decisions.items():
            merged[key] = _merge_with_template(
                base.get(key) if _is_plain_dict(base) else None,
                new_template.get(key),
                decision,
            )
        for key, value in new_template.items():
            if key not in merged:
                merged[key] = deepcopy(value)
        return merged

    return deepcopy(decisions)


def _reify(value: Any) -> Any:
    """Turn a decision tree back into a plain value, dropping deletions."""
    if _is_missing(value):
        return _MISSING
    if _is_plain_dict(value):
        reified: dict[str, Any] = {}
        for key, sub in value.items():
            resolved = _reify(sub)
            if not _is_missing(resolved):
                reified[key] = resolved
        return reified
    return deepcopy(value)


def _value_at_path(value: Any, path: Path) -> Any:
    for key in path:
        if not _is_plain_dict(value) or key not in value:
            return _MISSING
        value = value[key]
    return deepcopy(value)


def _set_path(tree: dict[str, Any], path: Path, value: Any) -> None:
    node = tree
    for key in path[:-1]:
        existing: Any = node.get(key)
        if not _is_plain_dict(existing):
            existing = {}
            node[key] = existing
        node = existing
    node[path[-1]] = value


def _apply_template_dict(asset: Asset, template: dict[str, Any]) -> None:
    """Write a merged template dict onto an asset in place.

    The asset object itself is never replaced, so code holding a reference to
    ``item.assets[key]`` keeps working. Fields absent from ``template`` are
    removed from the asset.
    """
    asset.title = template.get(ASSET_TITLE_PROP)
    asset.description = template.get(ASSET_DESC_PROP)
    asset.media_type = template.get(ASSET_TYPE_PROP)
    asset.roles = template.get(ASSET_ROLES_PROP)

    asset.extra_fields = {
        key: value for key, value in template.items() if key not in _ASSET_NAMED_FIELDS
    }


class TrackingList(list[Any]):
    """List that reports any mutation to a touch callback."""

    def __init__(
        self,
        values: Iterable[Any],
        touch: Callable[[], None],
    ) -> None:
        super().__init__(values)
        self._touch = touch

    def append(self, value: Any) -> None:
        super().append(value)
        self._touch()

    def extend(self, values: Iterable[Any]) -> None:
        super().extend(values)
        self._touch()

    def insert(self, index: SupportsIndex, value: Any) -> None:
        super().insert(index, value)
        self._touch()

    def remove(self, value: Any) -> None:
        super().remove(value)
        self._touch()

    def pop(self, index: SupportsIndex = -1) -> Any:
        value = super().pop(index)
        self._touch()
        return value

    def clear(self) -> None:
        super().clear()
        self._touch()

    def sort(self, *args: Any, **kwargs: Any) -> None:
        super().sort(*args, **kwargs)
        self._touch()

    def reverse(self) -> None:
        super().reverse()
        self._touch()

    def __setitem__(self, index: Any, value: Any) -> None:
        super().__setitem__(index, value)
        self._touch()

    def __delitem__(self, index: Any) -> None:
        super().__delitem__(index)
        self._touch()

    def __iadd__(self, other: Iterable[Any]) -> TrackingList:  # type: ignore[misc]
        super().__iadd__(other)
        self._touch()
        return self

    def __imul__(self, other: SupportsIndex) -> TrackingList:
        super().__imul__(other)
        self._touch()
        return self

    def __copy__(self) -> list[Any]:
        return list(self)

    def __deepcopy__(self, memo: dict[int, Any]) -> list[Any]:
        return [deepcopy(value, memo) for value in self]


class TrackingDict(dict[str, Any]):
    """Dict recording every touched path, including through nested dicts."""

    def __init__(
        self,
        data: dict[str, Any],
        touch: Callable[[Path], None],
        prefix: Path = (),
    ) -> None:
        super().__init__()
        self._touch = touch
        self._prefix = prefix
        for key, value in data.items():
            super().__setitem__(key, self._wrap(key, value))

    def _wrap(self, key: str, value: Any) -> Any:
        path = self._prefix + (key,)
        if _is_plain_dict(value):
            return TrackingDict(value, self._touch, path)
        if isinstance(value, list):
            return TrackingList(value, lambda: self._touch(path))
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        path = self._prefix + (key,)
        super().__setitem__(key, self._wrap(key, value))
        self._touch(path)

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self._touch(self._prefix + (key,))

    def setdefault(self, key: str, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
        return self[key]

    def pop(self, key: str, default: Any = None) -> Any:
        existed = key in self
        value = super().pop(key, default)
        if existed:
            self._touch(self._prefix + (key,))
        return value

    def update(self, *args: Any, **kwargs: Any) -> None:
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def clear(self) -> None:
        for key in list(self.keys()):
            del self[key]

    def __copy__(self) -> dict[str, Any]:
        return dict(self)

    def __deepcopy__(self, memo: dict[int, Any]) -> dict[str, Any]:
        return {key: deepcopy(value, memo) for key, value in self.items()}


class TrackedAsset(Asset):
    """Asset that records reviewer edits as explicit local decisions.

    Ordinary attribute assignment (including a write-back of the template's
    own value), list operations on :attr:`roles` and nested dict edits on
    :attr:`extra_fields` all mark the touched path. The owning
    :class:`AssetTemplateRegistry` turns the touched paths into decisions when
    :meth:`AssetTemplateRegistry.record_decisions` runs.
    """

    _touched: set[Path]
    _tracking: bool

    _ATTR_PATHS: dict[str, Path] = {
        "title": ("title",),
        "description": ("description",),
        "media_type": ("type",),
        "roles": ("roles",),
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._touched: set[Path] = set()
        self._tracking = False
        super().__init__(*args, **kwargs)
        if isinstance(self.roles, list) and not isinstance(self.roles, TrackingList):
            self.__dict__["roles"] = TrackingList(
                self.roles, lambda: self._mark(("roles",))
            )
        self.__dict__["extra_fields"] = TrackingDict(self.extra_fields, self._mark)
        self._tracking = True

    @classmethod
    def from_asset(cls, asset: Asset) -> TrackedAsset:
        return cls(
            href=asset.href,
            title=asset.title,
            description=asset.description,
            media_type=asset.media_type,
            roles=deepcopy(asset.roles),
            extra_fields=deepcopy(asset.extra_fields),
        )

    def _mark(self, path: Path) -> None:
        if self._tracking:
            self._touched.add(path)

    def __setattr__(self, name: str, value: Any) -> None:
        path = self._ATTR_PATHS.get(name)
        if path is None or not self._tracking:
            super().__setattr__(name, value)
            return
        if name == "roles" and value is not None:
            # Always rewrap so clones never share a callback bound to another
            # asset instance.
            value = TrackingList(value, lambda: self._mark(path))
        super().__setattr__(name, value)
        self._mark(path)

    def begin_bulk_update(self) -> None:
        """Suspend tracking while a template update rewrites the asset."""
        self._tracking = False

    def end_bulk_update(self) -> None:
        """Rewrap containers and clear touched paths after a template update."""
        if isinstance(self.roles, list) and not isinstance(self.roles, TrackingList):
            self.__dict__["roles"] = TrackingList(
                self.roles, lambda: self._mark(("roles",))
            )
        if not isinstance(self.extra_fields, TrackingDict):
            self.__dict__["extra_fields"] = TrackingDict(self.extra_fields, self._mark)
        self._touched.clear()
        self._tracking = True


class AssetTemplateRegistry:
    """Per-Item registry mapping asset keys to template generation metadata.

    Thin, typed wrapper over the JSON-serializable structure stored in the
    owning Item's ``extra_fields``::

        {
            "asset_templates": {
                "<asset-key>": {
                    "version": "<template version>",
                    "snapshot": { ...template fields at generation time... },
                    "decisions": { ...accumulated reviewer decisions... }
                }
            }
        }
    """

    def __init__(self, item: Item) -> None:
        self.item = item
        registry = item.extra_fields.get(REGISTRY_KEY)
        if registry is None:
            registry = {}
            item.extra_fields[REGISTRY_KEY] = registry
        elif not isinstance(registry, dict):
            raise TypeError(
                f"item extra field {REGISTRY_KEY!r} must be a dict, "
                f"got {type(registry).__name__}"
            )
        self._registry: dict[str, Any] = registry

    @classmethod
    def from_item(cls, item: Item) -> AssetTemplateRegistry:
        return cls(item)

    def _entry(self, asset_key: str) -> dict[str, Any]:
        entry = self._registry.get(asset_key)
        if entry is None:
            raise KeyError(f"asset {asset_key!r} is not registered to a template")
        return cast(dict[str, Any], entry)

    def is_registered(self, asset_key: str) -> bool:
        return asset_key in self._registry

    def registered_keys(self) -> list[str]:
        return list(self._registry.keys())

    def version(self, asset_key: str) -> str:
        return cast(str, self._entry(asset_key)["version"])

    def snapshot(self, asset_key: str) -> dict[str, Any]:
        return cast(dict[str, Any], deepcopy(self._entry(asset_key)["snapshot"]))

    def decisions(self, asset_key: str) -> dict[str, Any]:
        return deepcopy(self._entry(asset_key).get("decisions", {}))

    def register(
        self,
        asset_key: str,
        asset: Asset,
        version: str,
        template: dict[str, Any],
    ) -> None:
        """Record that ``asset`` was generated from a template version."""
        self._registry[asset_key] = {
            "version": version,
            "snapshot": deepcopy(_coerce_template(template)),
            "decisions": {},
        }
        asset.owner = self.item

    def record_decisions(self, asset_key: str) -> dict[str, Any]:
        """Persist the reviewer's edits on one asset as local decisions.

        The asset is diffed against its generation snapshot, and every touched
        path (including same-value write-backs recorded by
        :class:`TrackedAsset`, and explicit deletions) is overlaid on the
        diff. Decisions accumulate across template versions.
        """
        entry = self._entry(asset_key)
        asset = self.item.assets[asset_key]
        # ``href`` is instance data, not part of an item_assets template, so it
        # never participates in template diffing or decisions.
        current = {k: v for k, v in asset.to_dict().items() if k != "href"}

        fresh = _deep_diff(entry["snapshot"], current)
        touched = getattr(asset, "_touched", None)
        if touched:
            for path in touched:
                _set_path(fresh, path, _value_at_path(current, path))
            touched.clear()

        entry["decisions"] = _merge_decision_trees(entry.get("decisions", {}), fresh)
        return deepcopy(entry["decisions"])

    def update(
        self,
        asset_key: str,
        version: str,
        new_template: dict[str, Any],
    ) -> Asset:
        """Update one registered asset to a new template version.

        Accumulated local decisions are preserved; everything else follows the
        new template. The asset is modified in place and returned.
        """
        entry = self._entry(asset_key)
        new_template = _coerce_template(new_template)
        asset = self.item.assets[asset_key]

        if getattr(asset, "_touched", None):
            self.record_decisions(asset_key)

        merged = _merge_with_template(
            base=entry["snapshot"],
            new_template=new_template,
            decisions=entry.get("decisions", {}),
        )
        resolved = _reify(merged)

        if isinstance(asset, TrackedAsset):
            asset.begin_bulk_update()
            _apply_template_dict(asset, resolved)
            asset.end_bulk_update()
        else:
            _apply_template_dict(asset, resolved)

        entry["version"] = version
        entry["snapshot"] = deepcopy(new_template)
        return asset

    def update_all(
        self,
        templates: dict[str, dict[str, Any]],
        version: str,
    ) -> list[str]:
        """Update every registered asset whose key has a new template.

        Returns the list of asset keys that were updated. Registered keys
        missing from ``templates`` are left alone.
        """
        updated: list[str] = []
        for asset_key in list(self._registry.keys()):
            template = templates.get(asset_key)
            if template is None:
                continue
            self.update(asset_key, version, template)
            updated.append(asset_key)
        return updated


def unregistered_assets(items: Iterable[Item]) -> list[tuple[str, str]]:
    """Return ``(item id, asset key)`` pairs for assets with no registration.

    Assets listed here are never touched by template updates and stay exactly
    as they were ingested.
    """
    missing: list[tuple[str, str]] = []
    for item in items:
        registry = item.extra_fields.get(REGISTRY_KEY, {}) or {}
        for asset_key in item.assets:
            if asset_key not in registry:
                missing.append((item.id, asset_key))
    return missing


def enable_asset_tracking(item: Item) -> Item:
    """Replace registered assets with tracking copies after a re-read.

    Saving an Item turns :class:`TrackedAsset` instances back into plain JSON,
    so a re-loaded Item holds ordinary :class:`~pystac.Asset` objects. This
    rewraps every registered asset in place so subsequent reviewer edits are
    captured again. Unregistered assets are left untouched. The Item is
    returned for convenience.
    """
    registry = item.extra_fields.get(REGISTRY_KEY, {}) or {}
    for asset_key in list(registry):
        asset = item.assets.get(asset_key)
        if isinstance(asset, Asset) and not isinstance(asset, TrackedAsset):
            tracked = TrackedAsset.from_asset(asset)
            tracked.owner = item
            item.assets[asset_key] = tracked
    return item


def ingest_asset(
    item: Item,
    asset_key: str,
    collection: Collection,
    version: str,
    href: str,
) -> TrackedAsset:
    """Build an asset from a Collection ``item_assets`` entry and register it.

    Mirrors the ingest pipeline: the Asset is generated from
    :attr:`~pystac.Collection.item_assets`, wrapped in a
    :class:`TrackedAsset` so reviewer edits are captured, added to the Item,
    and registered against ``version`` so later template releases update it.
    """
    try:
        definition = collection.item_assets[asset_key]
    except KeyError:
        raise KeyError(
            f"collection {collection.id!r} has no item_assets entry for {asset_key!r}"
        ) from None

    asset = TrackedAsset.from_asset(definition.create_asset(href))
    item.add_asset(asset_key, asset)
    AssetTemplateRegistry(item).register(
        asset_key, asset, version, definition.to_dict()
    )
    return asset
