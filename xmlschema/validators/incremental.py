#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Incremental revalidation of XSD 1.1 assertions on an XML tree that was already
validated.  The normal validation evaluates every *xs:assert* while decoding an
element.  When only a small part of a large document changes, re-running the
whole validation is expensive.  This module re-evaluates only the assertions
whose owner is on a changed path and leaves untouched subtrees alone.

Two assertion shapes are supported incrementally:

* the identifier of the first non-cancelled item, compared with an attribute of
  the assertion owner;
* the sum of an item amount attribute, filtered by a cancellation predicate,
  compared with a total attribute of the assertion owner.

For other XPath expressions a :class:`FullRevalidationRequirement` is returned
instead of guessing the result, so that the caller can run a complete validation.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Union

from xmlschema.aliases import ElementType, NsmapType, SchemaType
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.namespaces import NamespaceMapper
from xmlschema.resources import XMLResource
from xmlschema.translation import gettext as _
from xmlschema.utils.etree import etree_getpath, is_etree_element
from xmlschema.utils.qnames import local_name

from .assertions import XsdAssert
from .exceptions import XMLSchemaValidationError
from .validation import ValidationContext

__all__ = [
    'IncrementalAssertionValidator', 'AssertionRevalidator',
    'AssertionRevalidationSession', 'AssertionRevalidationResult',
    'FullRevalidationRequirement', 'FullRevalidationRequired',
    'revalidate_assertions',
]

_NAME = r'[A-Za-z_][\w.\-]*'
_STEP_NAME = rf'(?:[A-Za-z_][\w.\-]*:)?{_NAME}'
_ATTRIBUTE = rf'@{_STEP_NAME}'
_LITERAL = r"(?:true\(\)|false\(\)|'(?:[^']*)'|\"(?:[^\"]*)\"|-?\d+(?:\.\d+)?)"


def _split_comparison(path: str) -> Optional[tuple[str, str, str]]:
    """Splits a top-level comparison, ignoring operators inside predicates."""
    depth = 0
    quote: Optional[str] = None
    index = 0
    while index < len(path):
        char = path[index]

        if quote is not None:
            if char == quote:
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char in '([':
            depth += 1
        elif char in ')]':
            depth -= 1
        elif depth == 0:
            for operator in ('!=', 'eq', 'ne', '='):
                if not path.startswith(operator, index):
                    continue
                if operator in ('eq', 'ne'):
                    before = path[index - 1:index]
                    after = path[index + len(operator):index + len(operator) + 1]
                    if before and not before.isspace():
                        continue
                    if after and not after.isspace():
                        continue

                left = path[:index].strip()
                right = path[index + len(operator):].strip()
                if left and right:
                    return left, operator, right
                break

        index += 1
    return None


def _resolve_name(name: str, namespaces: Mapping[str, str],
                  default_namespace: Optional[str]) -> str:
    """Resolves a prefixed or default-namespace QName used in an assertion path."""
    if name.startswith('{'):
        return name
    if ':' in name:
        prefix, local = name.split(':', 1)
        uri = namespaces.get(prefix)
        if uri is None:
            return name
        return f'{{{uri}}}{local}' if uri else local
    elif default_namespace:
        return f'{{{default_namespace}}}{name}'
    else:
        return name


def _match_tag(tag: str, step: str, namespaces: Mapping[str, str],
               default_namespace: Optional[str]) -> bool:
    if step == '*':
        return True
    return _resolve_name(step, namespaces, default_namespace) == tag


def _get_attribute(elem: ElementType, attribute: str,
                   namespaces: Mapping[str, str],
                   default_namespace: Optional[str]) -> Optional[str]:
    name = attribute[1:] if attribute.startswith('@') else attribute
    resolved = _resolve_name(name, namespaces, default_namespace)
    if resolved in elem.attrib:
        return elem.attrib[resolved]

    # An unprefixed attribute in an XPath expression is always in no
    # namespace, also when the expression has a default element namespace.
    if ':' not in name:
        return elem.attrib.get(name)
    return None


def _literal(value: str) -> Any:
    value = value.strip()
    if value == 'true()':
        return True
    elif value == 'false()':
        return False
    elif value[:1] in ("'", '"') and value[-1:] == value[:1]:
        return value[1:-1]
    else:
        try:
            return Decimal(value)
        except InvalidOperation:
            return value


@dataclass(frozen=True)
class _Predicate:
    """A cancellation predicate extracted from an assertion path."""
    kind: str
    attribute: str
    value: Any = None

    def matches(self, elem: ElementType, namespaces: Mapping[str, str],
                default_namespace: Optional[str]) -> bool:
        raw = _get_attribute(elem, self.attribute, namespaces, default_namespace)

        if self.kind == 'not_attribute':
            return raw is None

        expected = self.value
        if isinstance(expected, bool):
            if raw is None:
                current_bool: Optional[bool] = None
            elif raw in ('true', '1'):
                current_bool = True
            elif raw in ('false', '0'):
                current_bool = False
            else:
                current_bool = None
            if self.kind == 'eq':
                return current_bool == expected
            elif self.kind == 'ne':
                return current_bool is not None and current_bool != expected
            else:
                return current_bool != expected
        elif isinstance(expected, Decimal):
            if raw is None:
                current_num: Any = None
            else:
                try:
                    current_num = Decimal(raw)
                except (InvalidOperation, TypeError):
                    current_num = raw
            if self.kind == 'eq':
                return current_num is not None and current_num == expected
            elif self.kind == 'ne':
                return current_num is not None and current_num != expected
            else:
                return current_num != expected
        else:
            if self.kind == 'eq':
                return raw is not None and raw == expected
            elif self.kind == 'ne':
                return raw is not None and raw != expected
            else:
                return raw != expected


_PREDICATE = re.compile(
    rf'^\s*(?:'
    rf'not\(\s*(?P<not_attribute>{_ATTRIBUTE})\s*(?:=|eq)\s*(?P<not_value>{_LITERAL})\s*\)'
    rf'|not\(\s*(?P<not_presence>{_ATTRIBUTE})\s*\)'
    rf'|(?P<ne_attribute>{_ATTRIBUTE})\s*(?:!=|ne)\s*(?P<ne_value>{_LITERAL})'
    rf'|(?P<eq_attribute>{_ATTRIBUTE})\s*(?:=|eq)\s*(?P<eq_value>{_LITERAL})'
    rf')\s*$'
)


def _parse_predicate(text: str) -> Optional[_Predicate]:
    match = _PREDICATE.match(text)
    if match is None:
        return None
    elif match.group('not_attribute') is not None:
        return _Predicate('not_eq', match.group('not_attribute'),
                          _literal(match.group('not_value')))
    elif match.group('not_presence') is not None:
        return _Predicate('not_attribute', match.group('not_presence'))
    elif match.group('ne_attribute') is not None:
        return _Predicate('ne', match.group('ne_attribute'),
                          _literal(match.group('ne_value')))
    else:
        return _Predicate('eq', match.group('eq_attribute'),
                          _literal(match.group('eq_value')))


@dataclass(frozen=True)
class _PathStep:
    name: str
    predicates: tuple[str, ...] = ()


def _parse_steps(path: str) -> Optional[list[_PathStep]]:
    """
    Parses a simple child path with optional predicates.  Returns `None` for
    paths outside the supported subset (axes, absolute paths, wildcards, etc.).
    """
    path = path.strip()
    if not path or path.startswith(('/', '.')) or '//' in path or '::' in path:
        return None
    elif ')' in path.replace('()', '') and 'not(' not in path:
        return None

    steps: list[_PathStep] = []
    for chunk in path.split('/'):
        chunk = chunk.strip()
        if not chunk:
            return None

        match = re.fullmatch(rf'(?P<name>{_STEP_NAME})(?P<predicates>(?:\[[^\[\]]*\])*)', chunk)
        if match is None:
            return None

        predicates = re.findall(r'\[([^\[\]]*)\]', match.group('predicates'))
        for predicate in predicates:
            predicate = predicate.strip()
            if predicate != '1' and _parse_predicate(predicate) is None:
                return None

        steps.append(_PathStep(match.group('name'), tuple(predicates)))

    return steps or None


@dataclass
class _Plan:
    assertion: XsdAssert
    elem: ElementType
    supported: bool
    reason: Optional[str] = None
    kind: str = 'unsupported'

    @property
    def key(self) -> tuple[int, int]:
        return id(self.assertion), id(self.elem)

    @property
    def default_namespace(self) -> Optional[str]:
        return self.assertion.xpath_default_namespace or None

    @property
    def namespaces(self) -> Mapping[str, str]:
        """The static namespace context of the assertion expression."""
        return self.assertion.schema.namespaces


@dataclass
class _FirstPlan(_Plan):
    steps: list[_PathStep] = field(default_factory=list)
    item_attribute: str = ''
    owner_attribute: str = ''
    equality: bool = True
    predicate: Optional[_Predicate] = None
    items: list[ElementType] = field(default_factory=list)

    def rebuild(self, validator: 'IncrementalAssertionValidator') -> None:
        """Indexes the candidate items along the assertion path."""
        self.items.clear()

        nodes = [self.elem]
        for step in self.steps:
            next_nodes: list[ElementType] = []
            for node in nodes:
                for child in node:
                    if _match_tag(child.tag, step.name, self.namespaces,
                                  self.default_namespace):
                        next_nodes.append(child)
            nodes = next_nodes

        self.items.extend(nodes)

    def first_included(self, validator: 'IncrementalAssertionValidator') \
            -> Optional[ElementType]:
        for item in self.items:
            if self.predicate is None or self.predicate.matches(
                    item, self.namespaces, self.default_namespace):
                return item
        return None

    def is_item_ancestor(self, validator: 'IncrementalAssertionValidator',
                         elem: ElementType) -> bool:
        """Returns `True` if *elem* may contain or be one of the indexed items."""
        for item in self.items:
            node: Optional[ElementType] = item
            while node is not None:
                if node is elem:
                    return True
                node = validator.parent_map.get(node)
        return False


@dataclass
class _SumPlan(_Plan):
    steps: list[_PathStep] = field(default_factory=list)
    item_attribute: str = ''
    owner_attribute: str = ''
    equality: bool = True
    predicate: Optional[_Predicate] = None
    items: dict[ElementType, tuple[Optional[Decimal], bool]] = field(default_factory=dict)
    total: Optional[Decimal] = None
    invalid: bool = False

    def rebuild(self, validator: 'IncrementalAssertionValidator') -> None:
        """Indexes the current items of the sum.  Untouched items are not revisited."""
        self.items.clear()
        self.invalid = False

        nodes = [self.elem]
        for step in self.steps:
            next_nodes: list[ElementType] = []
            for node in nodes:
                for child in node:
                    if _match_tag(child.tag, step.name, self.namespaces,
                                  self.default_namespace):
                        next_nodes.append(child)
            nodes = next_nodes

        for item in nodes:
            self.update_item(validator, item)

        self.recompute()

    def update_item(self, validator: 'IncrementalAssertionValidator',
                    item: ElementType) -> None:
        raw = _get_attribute(item, self.item_attribute, self.namespaces,
                             self.default_namespace)

        amount: Optional[Decimal]
        if raw is None:
            amount = None
        else:
            try:
                amount = Decimal(raw.strip())
            except (InvalidOperation, ValueError):
                amount = None
                self.invalid = True

        if self.predicate is None:
            included = True
        else:
            included = self.predicate.matches(item, self.namespaces,
                                              self.default_namespace)

        self.items[item] = (amount, included)

    def recompute(self) -> None:
        if self.invalid:
            self.total = None
            return

        total = Decimal(0)
        for amount, included in self.items.values():
            if included and amount is not None:
                total += amount
        self.total = total

    def is_item_ancestor(self, validator: 'IncrementalAssertionValidator',
                         elem: ElementType) -> bool:
        """Returns `True` if *elem* may contain or be one of the indexed items."""
        for item in self.items:
            node: Optional[ElementType] = item
            while node is not None:
                if node is elem:
                    return True
                node = validator.parent_map.get(node)
        return False


@dataclass(frozen=True)
class FullRevalidationRequirement:
    """An assertion that can't be safely re-evaluated incrementally."""
    assertion: XsdAssert
    elem: Optional[ElementType]
    reason: str
    path: Optional[str] = None

    @property
    def assertion_path(self) -> str:
        return self.assertion.path

    @property
    def element_path(self) -> Optional[str]:
        return self.path

    def __str__(self) -> str:
        location = f' at {self.path}' if self.path else ''
        return (f'{self.assertion!r}{location} requires a full revalidation: '
                f'{self.reason}')


FullRevalidationRequired = FullRevalidationRequirement


class AssertionRevalidationResult:
    """
    The outcome of an incremental assertion revalidation.

    :ivar errors: the current validation errors, including unrelated errors \
    that were present in the input.
    :ivar removed: assertion errors that were fixed by the changes.
    :ivar added: new assertion errors.
    :ivar retained: previous errors that are still present.
    :ivar revalidated: assertions that have been evaluated again.
    :ivar skipped: known assertions that were not affected by the changes.
    :ivar full_revalidation: assertions that need a complete validation because \
    their XPath expression is outside the incrementally supported subset.
    """
    def __init__(self, errors: list[XMLSchemaValidationError],
                 removed: list[XMLSchemaValidationError],
                 added: list[XMLSchemaValidationError],
                 retained: list[XMLSchemaValidationError],
                 revalidated: list[XsdAssert],
                 skipped: list[XsdAssert],
                 full_revalidation: list[FullRevalidationRequirement]) -> None:
        self.errors = errors
        self.removed = removed
        self.added = added
        self.retained = retained
        self.revalidated = revalidated
        self.skipped = skipped
        self.full_revalidation = full_revalidation

    @property
    def current_errors(self) -> list[XMLSchemaValidationError]:
        return self.errors

    @property
    def removed_errors(self) -> list[XMLSchemaValidationError]:
        return self.removed

    @property
    def added_errors(self) -> list[XMLSchemaValidationError]:
        return self.added

    @property
    def retained_errors(self) -> list[XMLSchemaValidationError]:
        return self.retained

    @property
    def revalidated_assertions(self) -> list[XsdAssert]:
        return self.revalidated

    @property
    def skipped_assertions(self) -> list[XsdAssert]:
        return self.skipped

    @property
    def full_revalidation_requirements(self) -> list[FullRevalidationRequirement]:
        return self.full_revalidation

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)

    @property
    def requires_full_revalidation(self) -> bool:
        return bool(self.full_revalidation)

    @property
    def needs_full_revalidation(self) -> bool:
        return bool(self.full_revalidation)

    @property
    def full_revalidation_required(self) -> bool:
        return bool(self.full_revalidation)

    @property
    def is_complete(self) -> bool:
        return not self.full_revalidation

    @property
    def is_valid(self) -> bool:
        return not self.errors

    def __iter__(self) -> Iterator[XMLSchemaValidationError]:
        return iter(self.errors)

    def __len__(self) -> int:
        return len(self.errors)

    def __bool__(self) -> bool:
        return self.is_valid and self.is_complete

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(errors={len(self.errors)}, '
            f'removed={len(self.removed)}, added={len(self.added)}, '
            f'revalidated={len(self.revalidated)}, skipped={len(self.skipped)}, '
            f'full_revalidation={len(self.full_revalidation)})'
        )


class IncrementalAssertionValidator:
    """
    Re-evaluates only the XSD 1.1 assertions affected by a batch of XML changes.

    The instance is created after an XML document has been validated.  Pass the
    errors of that validation through *errors*; otherwise an initial assertion-only
    pass is executed.  The XML tree is not decoded again by this class.

    Changes are provided as XML elements (or as ``(element, attribute)`` pairs)
    after the tree has reached its final state.  A batch is intentional: all the
    corrections of a batch are applied before evaluating the assertions, so a
    cancellation and a following amount correction are checked together and no
    intermediate value is used.

    :param schema: the XSD 1.1 schema instance.
    :param source: an :class:`XMLResource` or an XML source already loaded in \
    memory. Lazy resources are not supported because their untouched subtrees \
    are not available for a later correction.
    :param errors: the errors of the previous validation.  If omitted, an initial \
    assertion-only validation pass is performed.
    :param namespaces: an optional namespace mapping.
    """
    def __init__(self, schema: SchemaType, source: Union[XMLResource, Any],
                 errors: Optional[Iterable[XMLSchemaValidationError]] = None,
                 namespaces: Optional[NsmapType] = None) -> None:

        self.schema = schema
        resource = schema.maps.settings.get_xml_resource(source)
        if resource.is_lazy():
            raise XMLSchemaValueError(_(
                "incremental assertion validation requires a non-lazy XML resource"
            ))

        self.source = resource
        self.namespaces = resource.get_namespaces(namespaces, root_only=False)
        if namespaces:
            self.namespaces.update(namespaces)

        self._parent_map = resource.parent_map
        self._plans: dict[tuple[int, int], _Plan] = {}
        self._plans_by_owner: dict[ElementType, list[_Plan]] = {}
        self._known_assertions: dict[tuple[int, int], XsdAssert] = {}
        self._pending: list[tuple[ElementType, Optional[str]]] = []

        # Index the assertion owners and the lightweight per-item state once.
        # The revalidation of a later attribute-only batch doesn't have to walk
        # the subtrees that are not on the changed paths.
        self._discover_document()

        self._initial_errors_supplied = errors is not None
        self._errors: list[XMLSchemaValidationError] = list(errors or ())
        self._cache_assertion_error_paths()

        if not self._initial_errors_supplied:
            self._errors = self._collect_initial_errors()

    @property
    def errors(self) -> list[XMLSchemaValidationError]:
        """The current validation errors."""
        return self._errors

    @property
    def assertion_errors(self) -> list[XMLSchemaValidationError]:
        """The current errors produced by XSD assertions."""
        return [e for e in self._errors if isinstance(e.validator, XsdAssert)]

    @property
    def assertions(self) -> list[XsdAssert]:
        """The assertions known by this validator."""
        return list(self._known_assertions.values())

    @property
    def parent_map(self) -> dict[ElementType, Optional[ElementType]]:
        return self._parent_map

    @property
    def default_namespace(self) -> Optional[str]:
        default = self.namespaces.get(None)
        if default is None:
            default = self.namespaces.get('')
        return default

    def mark_changed(self, elem: Union[ElementType, str],
                     attribute: Optional[str] = None) -> None:
        """Queues an element (or one of its attributes) as changed."""
        if isinstance(elem, str):
            change = self._resolve_path_change(elem)
            if change is None:
                raise XMLSchemaValueError(_(
                    "the provided path doesn't select an element of the XML data"
                ))
            self._pending.append(change)
            return
        elif not is_etree_element(elem):
            raise TypeError(_("a changed element must be an Element instance"))

        self._pending.append((elem, attribute))

    # Common aliases, useful for callers that prefer a different wording.
    update = mark_changed
    changed = mark_changed
    track = mark_changed
    notify = mark_changed

    def revalidate(self, changes: Optional[Any] = None,
                   namespaces: Optional[NsmapType] = None) -> AssertionRevalidationResult:
        """
        Re-evaluates the assertions affected by the provided changes.

        :param changes: an element, an ``(element, attribute)`` pair, a path, an \
        iterable of those, or a mapping from elements to changed attributes/values. \
        If omitted, the changes queued with :meth:`mark_changed` are used.
        :param namespaces: an optional namespace mapping used only for this call.
        """
        if namespaces:
            self.namespaces = dict(self.namespaces)
            self.namespaces.update(namespaces)

        if changes is None:
            batch = self._pending[:]
            self._pending.clear()
        else:
            batch = self._normalize_changes(changes)

        old_errors = self._errors
        if not batch:
            return AssertionRevalidationResult(
                errors=old_errors,
                removed=[], added=[], retained=[],
                revalidated=[],
                skipped=[plan.assertion for plan in self._plans.values()],
                full_revalidation=[],
            )

        # A structural change invalidates cached positions and the parent map.
        # Attribute-only batches don't need a document-wide rescan.
        structural = any(attribute is None for _, attribute in batch)
        if structural:
            self._parent_map = self.source.parent_map

        # Index assertions on newly inserted elements.  This only visits the
        # new subtrees, not the untouched part of the document.
        self._discover_new_elements(batch)

        # Discard XPath node caches: attribute values and text may have changed.
        self.source._xpath_root = None  # noqa: SLF001

        owners = self._affected_owners(batch)
        affected_plans: list[_Plan] = []
        requirements: list[FullRevalidationRequirement] = []

        for owner in owners:
            for plan in self._get_plans(owner):
                affected_plans.append(plan)
                if not plan.supported:
                    requirements.append(self._make_requirement(plan))

        # Evaluate first, after all batch changes have been applied.
        results: dict[tuple[int, int], list[XMLSchemaValidationError]] = {}
        revalidated: list[XsdAssert] = []
        for plan in affected_plans:
            if not plan.supported:
                continue

            revalidated.append(plan.assertion)
            if isinstance(plan, _SumPlan):
                self._refresh_sum_plan(plan, batch, structural)
                if self._sum_is_false(plan):
                    results[plan.key] = self._make_assertion_errors(plan)
                else:
                    results[plan.key] = []
            elif isinstance(plan, _FirstPlan):
                self._refresh_first_plan(plan, batch, structural)
                if self._first_is_false(plan):
                    results[plan.key] = self._make_assertion_errors(plan)
                else:
                    results[plan.key] = []
            else:  # pragma: no cover - unsupported plans are filtered above
                results[plan.key] = self._evaluate_assertion(plan.assertion, plan.elem)

        replaced = {plan.key for plan in affected_plans if plan.supported}
        current: list[XMLSchemaValidationError] = []
        removed: list[XMLSchemaValidationError] = []
        retained: list[XMLSchemaValidationError] = []
        reused: dict[tuple[int, int], XMLSchemaValidationError] = {}

        for error in old_errors:
            key = self._assertion_key(error)
            if key is not None and key in replaced:
                if results.get(key):
                    current.append(error)
                    retained.append(error)
                    reused[key] = error
                else:
                    removed.append(error)
            elif key is not None and not self._element_in_tree(error.elem):
                # A removed element cannot retain a path and its assertion has
                # disappeared with it: the error is stale.
                removed.append(error)
            else:
                current.append(error)
                retained.append(error)

        added: list[XMLSchemaValidationError] = []
        for plan in affected_plans:
            if not plan.supported:
                continue

            errors = results.get(plan.key, ())
            if not errors or plan.key in reused:
                continue

            for error in errors:
                current.append(error)
                added.append(error)

        self._errors = current

        affected_keys = {plan.key for plan in affected_plans}
        skipped = [
            plan.assertion for key, plan in self._plans.items()
            if key not in affected_keys
        ]

        return AssertionRevalidationResult(
            errors=current,
            removed=removed,
            added=added,
            retained=retained,
            revalidated=revalidated,
            skipped=skipped,
            full_revalidation=requirements,
        )

    # Compatibility aliases for one-shot call styles.
    revalidate_assertions = revalidate

    def iter_errors(self, changes: Optional[Any] = None) \
            -> Iterator[XMLSchemaValidationError]:
        yield from self.revalidate(changes).errors

    #
    # Change normalization and affected owner discovery

    def _normalize_changes(self, changes: Any) -> list[tuple[ElementType, Optional[str]]]:
        if changes is None:
            return []
        elif is_etree_element(changes):
            return [(changes, None)]
        elif isinstance(changes, str):
            change = self._resolve_path_change(changes)
            return [] if change is None else [change]
        elif isinstance(changes, tuple) and len(changes) == 2 \
                and is_etree_element(changes[0]):
            return [(changes[0], changes[1])]

        if isinstance(changes, Mapping):
            result: list[tuple[ElementType, Optional[str]]] = []
            for elem, value in changes.items():
                if isinstance(elem, str):
                    change = self._resolve_path_change(elem)
                    if change is None:
                        continue
                    elem = change[0]

                if not is_etree_element(elem):
                    continue

                if value is None:
                    result.append((elem, None))
                elif isinstance(value, str):
                    result.append((elem, value))
                elif isinstance(value, Mapping):
                    for name, val in value.items():
                        elem.set(name, str(val))
                    result.append((elem, None))
                elif isinstance(value, tuple) and len(value) == 2 \
                        and isinstance(value[0], str):
                    elem.set(value[0], str(value[1]))
                    result.append((elem, value[0]))
                else:
                    try:
                        result.extend((elem, str(name)) for name in value)
                    except TypeError:
                        result.append((elem, None))
            return result

        result = []
        for change in changes:
            if isinstance(change, str):
                resolved = self._resolve_path_change(change)
                if resolved is not None:
                    result.append(resolved)
            elif is_etree_element(change):
                result.append((change, None))
            elif isinstance(change, tuple):
                if len(change) == 3 and is_etree_element(change[0]):
                    elem, attribute, value = change
                    elem.set(attribute, str(value))
                    result.append((elem, attribute))
                elif len(change) == 2 and is_etree_element(change[0]):
                    result.append((change[0], change[1]))
            elif hasattr(change, 'elem'):
                result.append((change.elem, getattr(change, 'attribute', None)))
        return result

    def _resolve_path_change(self, path: str) -> Optional[tuple[ElementType, Optional[str]]]:
        attribute: Optional[str] = None
        stripped = path.strip()
        if '/@' in stripped:
            stripped, attribute = stripped.rsplit('/@', 1)
            attribute = '@' + attribute

        elem = self.source.find(stripped, self.namespaces)
        if elem is None:
            return None
        return elem, attribute

    def _affected_owners(self, batch: list[tuple[ElementType, Optional[str]]]) \
            -> list[ElementType]:
        owners: list[ElementType] = []
        seen: set[int] = set()

        for elem, _ in batch:
            node: Optional[ElementType] = elem
            while node is not None:
                if id(node) not in seen:
                    seen.add(id(node))
                    owners.append(node)
                node = self._parent_map.get(node)

        return owners

    def _discover_new_elements(self, batch: list[tuple[ElementType, Optional[str]]]) \
            -> None:
        """Indexes assertions on elements inserted by the current batch."""
        pending: list[ElementType] = []
        for elem, attribute in batch:
            if attribute is not None:
                continue
            for child in elem:
                if child not in self._plans_by_owner:
                    pending.append(child)

        while pending:
            elem = pending.pop()
            if elem in self._plans_by_owner:
                continue

            xsd_element = self._get_xsd_element(elem)
            self._get_plans(elem, xsd_element)
            pending.extend(elem)

    def _get_plans(self, elem: ElementType, xsd_element: Optional[Any] = None) \
            -> list[_Plan]:
        plans = self._plans_by_owner.get(elem)
        if plans is not None:
            return plans

        plans = []
        if xsd_element is None:
            xsd_element = self._get_xsd_element(elem)
        xsd_type = getattr(xsd_element, 'type', None)
        for assertion in getattr(xsd_type, 'assertions', ()):
            plan = self._build_plan(assertion, elem)
            if isinstance(plan, _FirstPlan):
                plan.rebuild(self)
            elif isinstance(plan, _SumPlan):
                plan.rebuild(self)

            self._plans[plan.key] = plan
            self._known_assertions[plan.key] = assertion
            plans.append(plan)

        self._plans_by_owner[elem] = plans
        return plans

    def _discover_document(self) -> None:
        """Indexes assertions and their local item paths without validating."""
        root = self.source.root
        root_xsd = self._get_xsd_element(root)
        stack: list[tuple[ElementType, Optional[Any]]] = [(root, root_xsd)]

        while stack:
            elem, xsd_element = stack.pop()
            self._get_plans(elem, xsd_element)

            if not len(elem):
                continue

            children_map = self._children_map(elem, xsd_element)
            for child in reversed(elem):
                stack.append((child, children_map.get(child)))

    # Compatibility aliases for one-shot call styles.
    revalidate_assertions = revalidate

    def iter_errors(self, changes: Optional[Any] = None) \
            -> Iterator[XMLSchemaValidationError]:
        yield from self.revalidate(changes).errors

    def _get_xsd_element(self, elem: ElementType) -> Optional[Any]:
        if elem is self.source.root:
            xsd_element = self.schema.get_element(elem.tag)
            if xsd_element is not None:
                return xsd_element
            return self.schema.find(f'/{elem.tag}', self.namespaces)

        path = etree_getpath(elem, self.source.root, self.namespaces,
                             relative=False, add_position=True)
        if path is None:
            return None

        xsd_element = self.schema.find(path, self.namespaces)
        if xsd_element is None or not hasattr(xsd_element, 'type'):
            xsd_element = self.schema.get_element(elem.tag)
        return xsd_element if hasattr(xsd_element, 'type') else None

    #
    # Assertion plan construction

    def _build_plan(self, assertion: XsdAssert, elem: ElementType) -> _Plan:
        path = assertion.path.strip()
        split = _split_comparison(path)
        unsupported_reason = _(
            "the XPath expression is outside the incrementally supported subset "
            "(first non-cancelled item equality and filtered attribute sum)"
        )

        if split is None:
            return _Plan(assertion, elem, False, unsupported_reason)

        left, operator, right = split
        equality = operator in ('=', 'eq')

        kind, operand, owner_attribute, steps, predicate = \
            self._parse_operands(left, right)
        if kind == 'first':
            item_attribute = self._last_attribute(operand)
            if item_attribute is None or not isinstance(predicate, _Predicate):
                return _Plan(assertion, elem, False, unsupported_reason)
            return _FirstPlan(
                assertion, elem, True, kind='first', steps=steps,
                item_attribute=item_attribute,
                owner_attribute=owner_attribute, equality=equality,
                predicate=predicate,
            )
        elif kind == 'sum':
            item_attribute = self._last_attribute(operand)
            if item_attribute is None:
                return _Plan(assertion, elem, False, unsupported_reason)
            return _SumPlan(
                assertion, elem, True, kind='sum', steps=steps,
                item_attribute=item_attribute,
                owner_attribute=owner_attribute, equality=equality,
                predicate=predicate if isinstance(predicate, _Predicate) else None,
            )

        return _Plan(assertion, elem, False, unsupported_reason)

    def _parse_operands(self, left: str, right: str) \
            -> tuple[str, str, str, list[_PathStep], Any]:
        left = left.strip()
        right = right.strip()

        for operand, other in ((left, right), (right, left)):
            if operand.startswith('sum(') and operand.endswith(')'):
                expression = operand[4:-1].strip()
                owner = other
                if not self._is_attribute(owner):
                    return 'unsupported', operand, '', [], None

                sum_match = re.fullmatch(
                    rf'(?P<path>.+)\s*/\s*(?P<attribute>{_ATTRIBUTE})', expression
                )
                if sum_match is None:
                    return 'unsupported', operand, '', [], None

                steps = _parse_steps(sum_match.group('path'))
                if steps is None:
                    return 'unsupported', operand, '', [], None

                predicate = self._predicate_for_steps(steps)
                if predicate is False:
                    return 'unsupported', operand, '', [], None

                return ('sum', expression, owner, steps, predicate)

        for operand, other in ((left, right), (right, left)):
            if self._is_attribute(other):
                match = re.fullmatch(
                    rf'(?P<path>.+)\s*/\s*(?P<attribute>{_ATTRIBUTE})', operand
                )
                if match is None:
                    continue

                path = match.group('path').strip()
                if not re.search(r'\[\s*1\s*\]\s*$', path):
                    continue

                path = re.sub(r'\[\s*1\s*\]\s*$', '', path).strip()
                steps = _parse_steps(path)
                if steps is None:
                    continue

                predicate = self._predicate_for_steps(steps)
                if predicate is False:
                    continue

                return 'first', operand, other, steps, predicate

        return 'unsupported', left, '', [], None

    @staticmethod
    def _is_attribute(operand: str) -> bool:
        return re.fullmatch(_ATTRIBUTE, operand.strip()) is not None

    @staticmethod
    def _last_attribute(operand: str) -> Optional[str]:
        match = re.search(rf'({_ATTRIBUTE})\s*$', operand.strip())
        return match.group(1) if match is not None else None

    @staticmethod
    def _predicate_for_steps(steps: list[_PathStep]) -> Any:
        """
        Returns the cancellation predicate, `None` if there is no predicate, or
        `False` if predicates are outside the supported subset.
        """
        predicate: Optional[_Predicate] = None
        for pos, step in enumerate(steps):
            for text in step.predicates:
                text = text.strip()
                if pos != len(steps) - 1:
                    return False
                elif text == '1':
                    continue

                parsed = _parse_predicate(text)
                if parsed is None or predicate is not None:
                    return False
                predicate = parsed
        return predicate

    #
    # Initial assertion-only pass

    def _collect_initial_errors(self) -> list[XMLSchemaValidationError]:
        errors: list[XMLSchemaValidationError] = []
        for plans in self._plans_by_owner.values():
            for plan in plans:
                errors.extend(self._evaluate_plan(plan))
        return errors

    def _children_map(self, elem: ElementType, xsd_element: Optional[Any]) \
            -> dict[ElementType, Optional[Any]]:
        result: dict[ElementType, Optional[Any]] = {}
        candidates: list[Any] = []
        content = getattr(getattr(xsd_element, 'type', None), 'content', None)
        if content is not None and hasattr(content, 'iter_elements'):
            candidates = list(content.iter_elements())

        for child in elem:
            for candidate in candidates:
                try:
                    if candidate.is_matching(child.tag):
                        result[child] = candidate
                        break
                except (AttributeError, TypeError):
                    continue
            else:
                path = etree_getpath(child, self.source.root, self.namespaces,
                                     relative=False, add_position=True)
                xsd_element = self.schema.find(path, self.namespaces) \
                    if path is not None else None
                if xsd_element is None or not hasattr(xsd_element, 'type'):
                    xsd_element = self.schema.get_element(child.tag)
                result[child] = xsd_element if hasattr(xsd_element, 'type') else None

        return result

    def _evaluate_plan(self, plan: _Plan) -> list[XMLSchemaValidationError]:
        if not plan.supported:
            # The initial pass can use the real XPath: only an incremental
            # re-evaluation is unsupported, not the engine itself.
            return self._evaluate_assertion(plan.assertion, plan.elem)

        if isinstance(plan, _SumPlan):
            if plan.total is None and not plan.items:
                plan.rebuild(self)
            return self._make_assertion_errors(plan) if self._sum_is_false(plan) else []
        elif isinstance(plan, _FirstPlan):
            if not plan.items:
                plan.rebuild(self)
            return self._make_assertion_errors(plan) if self._first_is_false(plan) else []
        else:  # pragma: no cover - guarded by _build_plan
            return self._evaluate_assertion(plan.assertion, plan.elem)

    #
    # First-item update and evaluation

    def _refresh_first_plan(self, plan: _FirstPlan,
                            batch: list[tuple[ElementType, Optional[str]]],
                            structural: bool = False) -> None:
        if not plan.items:
            plan.rebuild(self)
            return
        elif not structural:
            # Attribute changes cannot add or remove candidate items.  A
            # predicate based on a changed attribute is re-evaluated by
            # _first_is_false().
            return

        for elem, attribute in batch:
            if elem is plan.elem:
                if attribute is None and structural:
                    # Children of the owner may have been added or removed.
                    plan.rebuild(self)
                    return
                continue
            elif self._is_descendant(elem, plan.elem):
                # A child was inserted or removed below the owner: rebuild the
                # candidate list of just this assertion.
                plan.rebuild(self)
                return

    def _first_is_false(self, plan: _FirstPlan) -> bool:
        first = plan.first_included(self)
        item_raw = _get_attribute(first, plan.item_attribute, plan.namespaces,
                                  plan.default_namespace) if first is not None else None
        owner_raw = _get_attribute(plan.elem, plan.owner_attribute, plan.namespaces,
                                   plan.default_namespace)

        if item_raw is None or owner_raw is None:
            # An empty operand makes the comparison false for both '=' and '!='.
            return True

        equal = self._values_equal(item_raw, owner_raw,
                                   self._xsd_attribute_type(first, plan.item_attribute,
                                                            plan))
        return not equal if plan.equality else equal

    def _xsd_attribute_type(self, elem: ElementType, attribute: str,
                            plan: Optional[_Plan] = None) -> Optional[Any]:
        xsd_element = self._get_xsd_element(elem)
        xsd_type = getattr(xsd_element, 'type', None)
        attributes = getattr(xsd_type, 'attributes', None)
        if not attributes:
            return None

        name = attribute[1:] if attribute.startswith('@') else attribute
        namespaces = plan.namespaces if plan is not None else self.namespaces
        xsd_attribute = attributes.get(_resolve_name(name, namespaces, None))
        if xsd_attribute is None:
            local = local_name(name)
            for qname, item in attributes.items():
                if qname is not None and local_name(qname) == local:
                    xsd_attribute = item
                    break

        return getattr(xsd_attribute, 'type', None)

    @staticmethod
    def _values_equal(value1: Optional[str], value2: Optional[str],
                      xsd_type: Optional[Any] = None) -> bool:
        if value1 is None or value2 is None:
            return False
        elif xsd_type is not None and xsd_type.is_simple() \
                and getattr(xsd_type, 'name', None) != 'xs:string':
            try:
                return xsd_type.decode(value1) == xsd_type.decode(value2)
            except (ValueError, TypeError):
                return value1 == value2
        else:
            return value1 == value2

    #
    # Sum update and evaluation

    def _refresh_sum_plan(self, plan: _SumPlan,
                          batch: list[tuple[ElementType, Optional[str]]],
                          structural: bool = False) -> None:
        if not plan.items and plan.total is None:
            plan.rebuild(self)
            return

        rebuild = False
        for elem, attribute in batch:
            if elem is plan.elem:
                if attribute is None and structural:
                    rebuild = True
                continue
            elif elem in plan.items:
                plan.update_item(self, elem)
            elif structural and self._is_descendant(elem, plan.elem):
                # A new/removed item, or a changed nested structure.
                rebuild = True

        if rebuild:
            plan.rebuild(self)
        else:
            plan.recompute()

    def _is_descendant(self, elem: ElementType, ancestor: ElementType) -> bool:
        node = self._parent_map.get(elem)
        while node is not None:
            if node is ancestor:
                return True
            node = self._parent_map.get(node)
        return False

    def _sum_is_false(self, plan: _SumPlan) -> bool:
        if plan.invalid:
            return bool(self._evaluate_assertion(plan.assertion, plan.elem))

        raw = _get_attribute(plan.elem, plan.owner_attribute, plan.namespaces,
                             plan.default_namespace)
        if raw is None:
            return bool(self._evaluate_assertion(plan.assertion, plan.elem))

        try:
            owner_value = Decimal(raw.strip())
        except (InvalidOperation, ValueError):
            return bool(self._evaluate_assertion(plan.assertion, plan.elem))

        total = plan.total if plan.total is not None else Decimal(0)
        return (total != owner_value) if plan.equality else (total == owner_value)

    #
    # Evaluation and error helpers

    def _new_context(self) -> ValidationContext:
        return ValidationContext(
            source=self.source,
            converter=NamespaceMapper(self.namespaces, source=self.source),
            level=0,
            check_identities=False,
        )

    def _evaluate_assertion(self, assertion: XsdAssert, elem: ElementType) \
            -> list[XMLSchemaValidationError]:
        if not assertion.built:
            try:
                assertion.build()
            except Exception:
                pass

        context = self._new_context()
        try:
            assertion(elem, 'lax', context)
        except Exception as err:
            if not isinstance(err, XMLSchemaValidationError):
                context.validation_error('lax', assertion, err, elem)
        return context.errors[:]

    def _make_assertion_errors(self, plan: _Plan) -> list[XMLSchemaValidationError]:
        context = self._new_context()
        context.validation_error('lax', plan.assertion,
                                 'assertion test is false', plan.elem)
        return context.errors[:]

    def _make_requirement(self, plan: _Plan) -> FullRevalidationRequirement:
        path = etree_getpath(plan.elem, self.source.root, self.namespaces,
                             relative=False, add_position=True)
        return FullRevalidationRequirement(
            assertion=plan.assertion, elem=plan.elem,
            reason=plan.reason or _("unsupported XPath expression"),
            path=path,
        )

    def _cache_assertion_error_paths(self) -> None:
        for error in self._errors:
            if isinstance(error.validator, XsdAssert):
                try:
                    _ = error.path
                except Exception:
                    pass

    @staticmethod
    def _assertion_key(error: XMLSchemaValidationError) \
            -> Optional[tuple[int, int]]:
        if isinstance(error.validator, XsdAssert) and error.elem is not None:
            return id(error.validator), id(error.elem)
        return None

    def _element_in_tree(self, elem: Optional[ElementType]) -> bool:
        while elem is not None:
            if elem is self.source.root:
                return True
            elem = self._parent_map.get(elem)  # type: ignore[assignment]
        return False


# Useful aliases for callers that prefer a shorter or more explicit name.
AssertionRevalidator = IncrementalAssertionValidator
AssertionRevalidationSession = IncrementalAssertionValidator


def revalidate_assertions(schema: SchemaType, source: Union[XMLResource, Any],
                          changes: Optional[Any] = None,
                          errors: Optional[Iterable[XMLSchemaValidationError]] = None,
                          namespaces: Optional[NsmapType] = None) \
        -> AssertionRevalidationResult:
    """
    Convenience function for a one-shot incremental assertion revalidation.

    :param schema: the XSD 1.1 schema instance.
    :param source: the XML resource/tree containing the corrected document.
    :param changes: the changed elements, attributes or paths.
    :param errors: the errors of the previous validation.
    :param namespaces: an optional namespace mapping.
    """
    validator = IncrementalAssertionValidator(
        schema, source, errors=errors, namespaces=namespaces
    )
    return validator.revalidate(changes)
