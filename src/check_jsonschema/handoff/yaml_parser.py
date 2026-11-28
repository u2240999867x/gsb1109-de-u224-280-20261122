"""
Position-aware YAML parsing for strict handoff mode.

The regular YAML loader normalizes all mapping keys to strings with no memory
of where they came from. Handoff mode instead walks the composed node tree
itself so that it can

- apply the YAML ``<<`` merge rules while remembering which mapping supplied
  each field name and where every source key was written,
- report two source keys of the same selected mapping which resolve to the
  same JSON field name (e.g. the integer key ``1`` and the string key ``"1"``),
- report non-finite floats such as ``.inf`` at the position of the literal,
- follow aliases back to their original text, recording each alias usage
  position on the way.

The constructed values are produced by the same ruamel constructor used by
the regular loader, so scalar semantics (including dates loaded as strings)
match the non-handoff behavior.
"""

from __future__ import annotations

import dataclasses
import io
import math
import typing as t
import warnings

import ruamel.yaml
from ruamel.yaml.composer import Composer, ComposerError
from ruamel.yaml.events import AliasEvent
from ruamel.yaml.nodes import MappingNode, ScalarNode, SequenceNode

from ..parsers.yaml import construct_yaml_implementation
from . import (
    DUPLICATE_KEY,
    NON_FINITE_NUMBER,
    HandoffAliasUse,
    HandoffDocument,
    HandoffIssue,
    HandoffLocation,
    build_pointer,
)

_MERGE_TAG = "tag:yaml.org,2002:merge"


class _AliasRef:
    """A composed alias: the node it points at plus the usage position."""

    def __init__(
        self, node: ruamel.yaml.Node, anchor: str, line: int, column: int
    ) -> None:
        self.node = node
        self.anchor = anchor
        self.line = line
        self.column = column


@dataclasses.dataclass(frozen=True)
class _Candidate:
    """A field-name entry considered while resolving one mapping."""

    field_name: str
    key_node: t.Any
    key_refs: tuple[_AliasRef, ...]
    value_node: ruamel.yaml.Node
    value_refs: tuple[_AliasRef, ...]
    is_local: bool


class _AliasRecordingComposer(Composer):
    def compose_node(self, parent: t.Any, index: t.Any) -> t.Any:
        if self.parser.check_event(AliasEvent):
            event = self.parser.get_event()
            if event.anchor not in self.anchors:
                raise ComposerError(
                    None,
                    None,
                    f"found undefined alias {event.anchor!r}",
                    event.start_mark,
                )
            return _AliasRef(
                self.anchors[event.anchor],
                event.anchor,
                event.start_mark.line + 1,
                event.start_mark.column + 1,
            )
        return super().compose_node(parent, index)


def _field_name(key: t.Any) -> str:
    # booleans must be checked before int because ``bool`` is a subclass of int
    if isinstance(key, bool):
        return str(key)
    return str(key)


def _node_location(node: ruamel.yaml.Node) -> HandoffLocation:
    if isinstance(node, _AliasRef):
        # an alias written in key/value position is located where the author
        # wrote the alias, not where its anchor was defined
        return HandoffLocation(line=node.line, column=node.column)
    return HandoffLocation(
        line=node.start_mark.line + 1, column=node.start_mark.column + 1
    )


class _YamlHandoffBuilder:
    def __init__(self, constructor: ruamel.yaml.BaseConstructor) -> None:
        self._constructor = constructor
        self.issues: list[HandoffIssue] = []

    def build(self, root: ruamel.yaml.Node) -> t.Any:
        return self._build(root, (), ())

    def _resolve(self, node: t.Any) -> tuple[t.Any, tuple[_AliasRef, ...]]:
        alias_refs: list[_AliasRef] = []
        while isinstance(node, _AliasRef):
            alias_refs.append(node)
            node = node.node
        return node, tuple(alias_refs)

    def _alias_uses(
        self, alias_refs: tuple[_AliasRef, ...]
    ) -> tuple[HandoffAliasUse, ...]:
        return tuple(
            HandoffAliasUse(
                anchor=ref.anchor, line=ref.line, column=ref.column
            )
            for ref in alias_refs
        )

    def _build(
        self,
        node: t.Any,
        pointer_parts: tuple[str, ...],
        alias_refs: tuple[_AliasRef, ...],
    ) -> t.Any:
        node, extra_refs = self._resolve(node)
        alias_refs = alias_refs + extra_refs
        if isinstance(node, MappingNode):
            return self._build_mapping(node, pointer_parts, alias_refs)
        if isinstance(node, SequenceNode):
            return self._build_sequence(node, pointer_parts, alias_refs)
        if isinstance(node, ScalarNode):
            return self._build_scalar(node, pointer_parts, alias_refs)
        return self._construct(node)

    def _build_scalar(
        self,
        node: ScalarNode,
        pointer_parts: tuple[str, ...],
        alias_refs: tuple[_AliasRef, ...],
    ) -> t.Any:
        value = self._construct(node)
        if isinstance(value, float) and not math.isfinite(value):
            self.issues.append(
                HandoffIssue(
                    category=NON_FINITE_NUMBER,
                    pointer=build_pointer(pointer_parts),
                    locations=(_node_location(node),),
                    alias_uses=self._alias_uses(alias_refs),
                )
            )
        return value

    def _build_sequence(
        self,
        node: SequenceNode,
        pointer_parts: tuple[str, ...],
        alias_refs: tuple[_AliasRef, ...],
    ) -> list[t.Any]:
        result: list[t.Any] = []
        for index, child in enumerate(node.value):
            result.append(
                self._build(child, (*pointer_parts, str(index)), alias_refs)
            )
        return result

    def _build_mapping(
        self,
        node: MappingNode,
        pointer_parts: tuple[str, ...],
        alias_refs: tuple[_AliasRef, ...],
    ) -> dict[str, t.Any]:
        # Candidates run in merge order (first mapping to provide a field name
        # wins) followed by local entries. Group the local entries per field
        # name to detect same-layer collisions.
        candidates = self._collect_candidates(node)
        local_by_field: dict[str, list[_Candidate]] = {}
        for candidate in candidates:
            if candidate.is_local:
                local_by_field.setdefault(
                    candidate.field_name, []
                ).append(candidate)

        # The selected candidate per field name: a local entry always wins,
        # otherwise the first merge entry providing the field name.
        selected: dict[str, _Candidate] = {}
        for candidate in candidates:
            if candidate.is_local:
                selected[candidate.field_name] = candidate
            else:
                selected.setdefault(candidate.field_name, candidate)

        result: dict[str, t.Any] = {}
        # walk in first-seen order so the delivered mapping keeps a stable,
        # document-derived ordering rather than set ordering
        seen_fields: set[str] = set()
        for candidate in candidates:
            field_name = candidate.field_name
            if field_name in seen_fields:
                continue
            seen_fields.add(field_name)
            field_parts = (*pointer_parts, field_name)
            local_candidates = local_by_field.get(field_name, [])

            # two original keys written at this same layer which collapse onto
            # the same field name: report every key position
            if len(local_candidates) > 1:
                self.issues.append(
                    HandoffIssue(
                        category=DUPLICATE_KEY,
                        pointer=build_pointer(field_parts),
                        locations=tuple(
                            _node_location(entry.key_node)
                            for entry in local_candidates
                        ),
                        alias_uses=self._alias_uses(
                            alias_refs
                            + tuple(
                                ref
                                for entry in local_candidates
                                for ref in entry.key_refs
                            )
                        ),
                    )
                )
                # the losing local values are not delivered, but their
                # non-finite numbers still have to be located; the selected
                # value is checked by the normal recursive build below
                for losing in local_candidates[:-1]:
                    self._scan_nonfinite(
                        losing.value_node,
                        field_parts,
                        alias_refs + losing.key_refs + losing.value_refs,
                    )

            chosen = selected[field_name]
            result[field_name] = self._build(
                chosen.value_node,
                field_parts,
                alias_refs + chosen.value_refs,
            )
        return result

    def _key_field_name(
        self, key_node: t.Any
    ) -> tuple[str, tuple[_AliasRef, ...]]:
        resolved, key_refs = self._resolve(key_node)
        return _field_name(self._construct(resolved)), key_refs

    def _collect_candidates(
        self, node: MappingNode
    ) -> list[_Candidate]:
        merge_entries: list[
            tuple[ruamel.yaml.Node, t.Any, tuple[_AliasRef, ...]]
        ] = []
        local_pairs: list[tuple[t.Any, t.Any]] = []
        for key_node, value_node in node.value:
            resolved_key, _ = self._resolve(key_node)
            if resolved_key.tag == _MERGE_TAG:
                self._collect_merge(value_node, merge_entries, (), set())
            else:
                local_pairs.append((key_node, value_node))

        candidates: list[_Candidate] = []
        for key_node, value_node, entry_refs in merge_entries:
            field_name, key_refs = self._key_field_name(key_node)
            resolved, value_refs = self._resolve(value_node)
            candidates.append(
                _Candidate(
                    field_name=field_name,
                    key_node=key_node,
                    key_refs=key_refs,
                    value_node=resolved,
                    value_refs=entry_refs + value_refs,
                    is_local=False,
                )
            )
        for key_node, value_node in local_pairs:
            field_name, key_refs = self._key_field_name(key_node)
            resolved, value_refs = self._resolve(value_node)
            candidates.append(
                _Candidate(
                    field_name=field_name,
                    key_node=key_node,
                    key_refs=key_refs,
                    value_node=resolved,
                    value_refs=value_refs,
                    is_local=True,
                )
            )
        return candidates

    def _scan_nonfinite(
        self,
        node: t.Any,
        pointer_parts: tuple[str, ...],
        alias_refs: tuple[_AliasRef, ...],
    ) -> None:
        node, extra_refs = self._resolve(node)
        alias_refs = alias_refs + extra_refs
        if isinstance(node, MappingNode):
            for key_node, child_value in node.value:
                resolved_key, key_refs = self._resolve(key_node)
                if resolved_key.tag == _MERGE_TAG:
                    continue
                field_name = _field_name(self._construct(resolved_key))
                self._scan_nonfinite(
                    child_value,
                    (*pointer_parts, field_name),
                    alias_refs + key_refs,
                )
        elif isinstance(node, SequenceNode):
            for index, child in enumerate(node.value):
                self._scan_nonfinite(
                    child, (*pointer_parts, str(index)), alias_refs
                )
        elif isinstance(node, ScalarNode):
            value = self._construct(node)
            if isinstance(value, float) and not math.isfinite(value):
                self.issues.append(
                    HandoffIssue(
                        category=NON_FINITE_NUMBER,
                        pointer=build_pointer(pointer_parts),
                        locations=(_node_location(node),),
                        alias_uses=self._alias_uses(alias_refs),
                    )
                )

    def _collect_merge(
        self,
        value_node: t.Any,
        merge_entries: list[
            tuple[ruamel.yaml.Node, t.Any, tuple[_AliasRef, ...]]
        ],
        alias_refs: tuple[_AliasRef, ...],
        seen: set[int],
    ) -> None:
        node, refs = self._resolve(value_node)
        alias_refs = alias_refs + refs
        if id(node) in seen:
            return
        seen.add(id(node))
        if isinstance(node, MappingNode):
            for key_node, child_value in node.value:
                resolved_key, _ = self._resolve(key_node)
                if resolved_key.tag == _MERGE_TAG:
                    self._collect_merge(
                        child_value, merge_entries, alias_refs, seen
                    )
                else:
                    merge_entries.append((key_node, child_value, alias_refs))
        elif isinstance(node, SequenceNode):
            # earlier mappings take precedence, so walk in order
            for child in node.value:
                self._collect_merge(child, merge_entries, alias_refs, seen)
        else:
            raise ruamel.yaml.constructor.ConstructorError(
                "while constructing a mapping",
                None,
                "expected a mapping or list of mappings for merging, "
                f"but found {node.id!s}",
                node.start_mark,
            )

    def _construct(self, node: ruamel.yaml.Node) -> t.Any:
        return self._constructor.construct_object(node, deep=True)


def _make_yaml() -> ruamel.yaml.YAML:
    implementation = construct_yaml_implementation()
    implementation.Composer = _AliasRecordingComposer
    return implementation


def load(stream: t.IO[bytes] | bytes) -> HandoffDocument:
    if isinstance(stream, bytes):
        stream = io.BytesIO(stream)
    stream_bytes = stream.read()
    implementation = _make_yaml()
    with warnings.catch_warnings():
        # mirror the regular loader: reusing an anchor is allowed and only
        # means later definitions take over the name
        warnings.simplefilter("ignore", ruamel.yaml.error.ReusedAnchorWarning)
        root = implementation.compose(io.BytesIO(stream_bytes))
    if root is None:
        return HandoffDocument(data=None, issues=[])
    builder = _YamlHandoffBuilder(implementation.constructor)
    data = builder.build(root)
    return HandoffDocument(data=data, issues=builder.issues)
