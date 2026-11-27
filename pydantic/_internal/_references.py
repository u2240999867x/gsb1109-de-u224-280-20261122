"""Opt-in export of model graphs where shared model instances are referenced.

The regular serializers emit a full copy of a model instance at every place it is
reachable from. With the ``serialize_references`` argument of the dump methods, shared
instances are emitted once and referenced everywhere else:

* the first occurrence of a shared instance is emitted inline, augmented with an
  ``'$id'`` entry identifying the subject;
* every later occurrence is replaced with ``{'$ref': <id>}``.

Only instances that are reachable more than once are rewritten; every other value is
left exactly as produced by the regular serializer, so the ordinary tree export does
not change.
"""

from __future__ import annotations

from typing import Any

__all__ = ('share_references',)

_REF_KEY = '$ref'
_ID_KEY = '$id'


def _occurrence_counts(root: Any) -> dict[int, int]:
    """Count how many times each model instance is reachable from ``root``."""
    from ..main import BaseModel

    counts: dict[int, int] = {}
    expanded: set[int] = set()

    def visit(value: Any) -> None:
        if isinstance(value, BaseModel):
            key = id(value)
            counts[key] = counts.get(key, 0) + 1
            if key in expanded:
                return
            expanded.add(key)
            for child in _model_children(value):
                visit(child)
        elif isinstance(value, (list, tuple, set, frozenset)):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for item in value.values():
                visit(item)

    visit(root)
    return counts


def _model_children(value: Any) -> Any:
    instance_dict = getattr(value, '__dict__', None)
    if instance_dict is None:
        return ()
    return instance_dict.values()


class _Sharer:
    def __init__(self, counts: dict[int, int], by_alias: bool) -> None:
        self.counts = counts
        self.by_alias = by_alias
        self.ids: dict[int, str] = {}
        self.delivered: set[int] = set()

    def ref_id(self, value: Any) -> str:
        key = id(value)
        if key not in self.ids:
            self.ids[key] = f'ref_{len(self.ids)}'
        return self.ids[key]

    def walk(self, value: Any, node: Any) -> Any:
        from ..main import BaseModel

        if isinstance(value, BaseModel):
            return self._walk_model(value, node)
        if isinstance(value, (list, tuple)) and isinstance(node, list) and len(value) == len(node):
            return [self.walk(item, child) for item, child in zip(value, node, strict=True)]
        if isinstance(value, dict) and isinstance(node, dict):
            result = dict(node)
            for key, child in value.items():
                if key in result and isinstance(child, (BaseModel, list, tuple, dict)):
                    result[key] = self.walk(child, result[key])
            return result
        return node

    def _walk_model(self, value: Any, node: Any) -> Any:
        from ..main import BaseModel

        shared = self.counts.get(id(value), 0) > 1
        if shared and id(value) in self.delivered:
            return {_REF_KEY: self.ref_id(value)}

        fields = getattr(type(value), '__pydantic_fields__', {})
        decorators = getattr(type(value), '__pydantic_decorators__', None)
        has_model_serializer = bool(decorators and decorators.model_serializers)
        instance_dict = getattr(value, '__dict__', {})

        if not isinstance(node, dict) or has_model_serializer:
            # Can't safely map the serialized node back to fields.
            return node

        result = dict(node)
        field_serialized: set[str] = set()
        if decorators is not None:
            for decorator in decorators.field_serializers.values():
                field_serialized.update(decorator.info.fields)

        for name, field_info in fields.items():
            if name not in instance_dict or name in field_serialized:
                continue
            child = instance_dict[name]
            if not isinstance(child, (BaseModel, list, tuple, dict)):
                continue
            key = self._key_for(name, field_info)
            if key in result:
                result[key] = self.walk(child, result[key])

        if shared:
            self.delivered.add(id(value))
            result[_ID_KEY] = self.ref_id(value)
        return result

    def _key_for(self, name: str, field_info: Any) -> str:
        if self.by_alias:
            alias = getattr(field_info, 'serialization_alias', None) or getattr(field_info, 'alias', None)
            if isinstance(alias, str):
                return alias
        return name


def share_references(tree: Any, root: Any, *, by_alias: bool) -> Any:
    """Rewrite ``tree`` so shared model instances are emitted once and referenced elsewhere."""
    from ..main import BaseModel
    from ..root_model import RootModel

    if not isinstance(root, BaseModel):
        return tree
    if isinstance(root, RootModel):
        # A root model serializes as its root value, so align the walk with that value.
        value = root.root
    else:
        value = root
    counts = _occurrence_counts(root)
    return _Sharer(counts, by_alias).walk(value, tree)
