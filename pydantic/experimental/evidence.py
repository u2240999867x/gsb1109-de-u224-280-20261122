"""Experimental validation evidence API.

This module provides an *opt-in* way to collect evidence about what Pydantic actually does
during a validation call:

* which branch of a smart union was actually adopted, and which branches were only tried as
  candidates (including the errors they produced before another branch succeeded),
* which nested models / fields were entered, with their position inside the input,
* which nodes were never reached (e.g. the inner model of a ``None`` nullable field),
* evidence produced by auxiliary :class:`~pydantic.TypeAdapter` checks, attributed to the
  validation that triggered them.

The feature is explicitly enabled through :func:`evidence_enabled` (validator construction
time) and evidence is collected through :func:`evidence_context` (validation call time).
When neither is active, validation behaves exactly as before and the instrumentation wrappers
add a single context-variable lookup.

The public API below is experimental and subject to change.
"""

from __future__ import annotations

import copy
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast

from pydantic_core import ErrorDetails, InitErrorDetails, ValidationError
from pydantic_core import core_schema as _cs

if TYPE_CHECKING:
    from pydantic import TypeAdapter

__all__ = (
    'NodeId',
    'NodeAttempt',
    'ValidationEvidenceReport',
    'evidence_enabled',
    'evidence_context',
    'set_evidence_enabled',
    'evidence_is_enabled',
    'instrument_core_schema',
    'check_type_adapter',
)


# Status of a single node validation attempt.
STATUS_SUCCESS = 'success'
STATUS_ERROR = 'error'
STATUS_EXCEPTION = 'exception'

# Kinds of instrumented nodes.
KIND_MODEL = 'model'
KIND_UNION = 'union'
KIND_CHOICE = 'choice'
KIND_LIST = 'list'
KIND_ITEM = 'item'
KIND_TYPE_ADAPTER = 'type_adapter'

# Whether newly constructed schema validators should be instrumented. Consulted from
# `pydantic.plugin._schema_validator.create_schema_validator`.
_gate: ContextVar[bool] = ContextVar('pydantic_evidence_gate', default=False)
# The collector associated with the currently running evidence context, if any.
_active_collector: ContextVar[EvidenceCollector | None] = ContextVar('pydantic_evidence_collector', default=None)


class NodeId(NamedTuple):
    """Stable identity of an instrumented node inside a core schema.

    Attributes:
        kind: One of the ``KIND_*`` constants.
        name: Human readable name (model class name, choice label, ...).
        ref: The core schema ``ref`` when available, used to disambiguate repeated names.
    """

    kind: str
    name: str
    ref: str | None = None

    def __str__(self) -> str:
        return f'{self.kind}:{self.name}'


@dataclass(slots=True)
class NodeAttempt:
    """One recorded execution of an instrumented node.

    Attributes:
        node: The identity of the instrumented node.
        status: One of :data:`STATUS_SUCCESS`, :data:`STATUS_ERROR`, :data:`STATUS_EXCEPTION`.
        adopted: Whether a union choice was the branch actually returned by the surrounding
            union. ``False`` for branches that were only tried, ``None`` outside of unions.
        loc: Position of the validated value inside the top-level input.
        duration_ns: Wall-clock duration of the wrapped validator, in nanoseconds.
        errors: Sanitized validation errors produced by this attempt (candidate errors keep
            their original, user-facing locations).
        exception_type: Name of the exception type for ``status == STATUS_EXCEPTION``.
        source: Name of the auxiliary :class:`~pydantic.TypeAdapter` check this attempt belongs
            to, if any (attribution of local-state validation).
        sequence: Monotonic sequence number of the attempt within the report.
        adopted_choice: For union attempts, the :class:`NodeId` of the choice that was adopted.
    """

    node: NodeId
    status: str
    loc: tuple[Any, ...]
    duration_ns: int
    adopted: bool | None = None
    errors: list[ErrorDetails] = field(default_factory=list)
    exception_type: str | None = None
    source: str | None = None
    sequence: int = 0
    adopted_choice: NodeId | None = None


@dataclass(slots=True)
class ValidationEvidenceReport:
    """The evidence collected for one batch (one :func:`evidence_context`)."""

    batch_id: str
    attempts: list[NodeAttempt] = field(default_factory=list)

    def attempts_for(self, node: NodeId) -> list[NodeAttempt]:
        """Return all attempts recorded for ``node`` in execution order."""
        return [attempt for attempt in self.attempts if attempt.node == node]

    def attempts_named(self, name: str) -> list[NodeAttempt]:
        """Return all attempts whose node is called ``name`` in execution order."""
        return [attempt for attempt in self.attempts if attempt.node.name == name]

    def union_attempts(self) -> list[NodeAttempt]:
        """Return all union attempts in execution order."""
        return [attempt for attempt in self.attempts if attempt.node.kind == KIND_UNION]

    def adopted_choices(self) -> list[NodeId]:
        """Return the choice adopted by each union attempt, in execution order."""
        return [
            attempt.adopted_choice
            for attempt in self.attempts
            if attempt.node.kind == KIND_UNION and attempt.adopted_choice is not None
        ]

    def unreachable_nodes(self, all_nodes: list[NodeId]) -> list[NodeId]:
        """Return nodes from ``all_nodes`` that were never entered during the batch."""
        visited = {attempt.node for attempt in self.attempts}
        return [node for node in all_nodes if node not in visited]


@dataclass(slots=True)
class _Frame:
    node: NodeId
    loc: tuple[Any, ...]
    source: str | None
    attempt: NodeAttempt
    choice_attempts: list[NodeAttempt] = field(default_factory=list)
    choice_results: list[tuple[NodeAttempt, Any]] = field(default_factory=list)


@dataclass(slots=True)
class _SequenceFrame(_Frame):
    """Frame for positional containers (lists / tuples), tracking the running item index."""

    next_index: int = 0


class EvidenceCollector:
    """Collects node attempts for a single report, driven by instrumentation wrappers."""

    __slots__ = ('report', '_stack', '_sequence')

    def __init__(self, report: ValidationEvidenceReport) -> None:
        self.report = report
        self._stack: list[_Frame] = []
        self._sequence = 0

    def enter(
        self,
        node: NodeId,
        info: Any | None,
        *,
        positional: bool = False,
        source: str | None = None,
    ) -> _Frame:
        parent = self._stack[-1] if self._stack else None
        parent_loc = parent.loc if parent is not None else ()
        inherited_source = source if source is not None else (parent.source if parent is not None else None)

        if positional:
            if isinstance(parent, _SequenceFrame):
                loc = parent_loc + (parent.next_index,)
                parent.next_index += 1
            else:
                loc = parent_loc
        else:
            # `info.field_name` stays set across the whole field subtree, so emit it only when
            # crossing a field boundary, i.e. descending directly from a model / dataclass frame
            # (or from the root). Nodes reached through a container or union already inherit the
            # field name emitted by the outermost node of that field.
            field_name = getattr(info, 'field_name', None)
            at_field_boundary = parent is None or parent.node.kind == KIND_MODEL
            if field_name is not None and at_field_boundary and (not parent_loc or parent_loc[-1] != field_name):
                loc = parent_loc + (field_name,)
            else:
                loc = parent_loc

        attempt = NodeAttempt(
            node=node,
            status=STATUS_SUCCESS,
            loc=loc,
            duration_ns=0,
            source=inherited_source,
            sequence=self._sequence,
        )
        self._sequence += 1
        self.report.attempts.append(attempt)

        frame_cls = _SequenceFrame if node.kind == KIND_LIST else _Frame
        frame = frame_cls(node=node, loc=loc, source=inherited_source, attempt=attempt)
        self._stack.append(frame)
        if parent is not None and parent.node.kind == KIND_UNION and node.kind == KIND_CHOICE:
            parent.choice_attempts.append(attempt)
        return frame

    def exit(
        self,
        frame: _Frame,
        *,
        status: str,
        duration_ns: int,
        errors: list[ErrorDetails] | None = None,
        exception: BaseException | None = None,
        result: Any = None,
    ) -> None:
        attempt = frame.attempt
        attempt.status = status
        attempt.duration_ns = duration_ns
        if errors is not None:
            attempt.errors = errors
        if exception is not None:
            attempt.exception_type = type(exception).__name__

        popped = self._stack.pop()
        assert popped is frame

        if frame.node.kind == KIND_UNION:
            if status == STATUS_SUCCESS:
                for choice_attempt in frame.choice_attempts:
                    adopted_result = next(
                        (choice_result for attempt, choice_result in frame.choice_results if attempt is choice_attempt),
                        None,
                    )
                    is_adopted = adopted_result is result
                    choice_attempt.adopted = is_adopted
                    if is_adopted:
                        attempt.adopted_choice = choice_attempt.node
            else:
                for choice_attempt in frame.choice_attempts:
                    choice_attempt.adopted = False
        elif (
            self._stack
            and self._stack[-1].node.kind == KIND_UNION
            and frame.node.kind == KIND_CHOICE
            and status == STATUS_SUCCESS
        ):
            self._stack[-1].choice_results.append((frame.attempt, result))


# -----------------------------------------------------------------------------------------------
# Enablement
# -----------------------------------------------------------------------------------------------


def set_evidence_enabled(enabled: bool) -> None:
    """Globally enable or disable instrumentation of newly built schema validators."""
    _gate.set(enabled)


def evidence_is_enabled() -> bool:
    """Whether schema validators built from now on should be instrumented."""
    return _gate.get()


@contextmanager
def evidence_enabled() -> Iterator[None]:
    """Context manager enabling instrumentation for validators built inside the ``with`` block."""
    token = _gate.set(True)
    try:
        yield
    finally:
        _gate.reset(token)


@contextmanager
def evidence_context(batch_id: str) -> Iterator[ValidationEvidenceReport]:
    """Run validation calls while collecting evidence into a fresh :class:`ValidationEvidenceReport`.

    The report (and the underlying collector) is stored in a :class:`~contextvars.ContextVar`,
    so two batch contexts running concurrently (e.g. in different threads) never share events.
    """
    report = ValidationEvidenceReport(batch_id=batch_id)
    collector = EvidenceCollector(report)
    token = _active_collector.set(collector)
    try:
        yield report
    finally:
        _active_collector.reset(token)


def check_type_adapter(
    adapter: TypeAdapter[Any],
    value: Any,
    *,
    name: str,
    **validate_kwargs: Any,
) -> Any:
    """Run ``adapter.validate_python(value)`` as a named, evidence-tracked local-state check.

    Any node visited by this check is attributed to ``name`` (see :attr:`NodeAttempt.source`),
    so evidence can tell local-state validation apart from the validation that triggered it.
    When no evidence context is active, the adapter is called directly with no extra behavior.
    """
    collector = _active_collector.get()
    if collector is None:
        return adapter.validate_python(value, **validate_kwargs)

    node = NodeId(KIND_TYPE_ADAPTER, name)
    frame = collector.enter(node, None, source=name)
    start = time.perf_counter_ns()
    try:
        result = adapter.validate_python(value, **validate_kwargs)
    except ValidationError as error:
        collector.exit(
            frame,
            status=STATUS_ERROR,
            duration_ns=time.perf_counter_ns() - start,
            errors=cast(list[ErrorDetails], _sanitize_errors(error.errors(include_url=False), {})),
            exception=None,
        )
        raise
    except Exception as exception:
        collector.exit(
            frame,
            status=STATUS_EXCEPTION,
            duration_ns=time.perf_counter_ns() - start,
            exception=exception,
        )
        raise
    else:
        collector.exit(
            frame,
            status=STATUS_SUCCESS,
            duration_ns=time.perf_counter_ns() - start,
            result=result,
        )
        return result


# -----------------------------------------------------------------------------------------------
# Core schema instrumentation
# -----------------------------------------------------------------------------------------------


# Child keys holding nested core schemas, mirroring pydantic-core's `schema_gather.rs`.
_SINGLE_CHILD_KEYS = (
    'schema',
    'items_schema',
    'keys_schema',
    'values_schema',
    'json_schema',
    'python_schema',
    'lax_schema',
    'strict_schema',
    'return_schema',
    'extras_schema',
    'var_args_schema',
    'var_kwargs_schema',
)
_LIST_CHILD_KEYS = ('steps', 'definitions', 'computed_fields')
_FIELD_DICT_KEY = 'fields'
_TAGGED_CHOICES_KEY = 'choices'
_ARGUMENTS_SCHEMA_KEY = 'arguments_schema'


def instrument_core_schema(schema: _cs.CoreSchema) -> _cs.CoreSchema:
    """Return a deep-copied, instrumented version of ``schema``.

    The returned schema must be built with ``_use_prebuilt=False`` so that the wrappers placed
    on nested models are not bypassed by pydantic-core's prebuilt-validator reuse.
    """
    instrumented = copy.deepcopy(schema)
    state = _InstrumentState()
    instrumented = state.visit(instrumented)
    return instrumented


class _InstrumentState:
    def __init__(self) -> None:
        self._union_counter = 0
        self._container_counter = 0
        # Maps a core-schema `ref` to a human-readable model name, used to label union
        # choices that reference a shared definition instead of inlining the model schema.
        self._ref_labels: dict[str, str] = {}

    def visit(self, node: Any) -> Any:
        if not isinstance(node, dict):
            return node
        node_type = node.get('type')

        if node_type == 'definition-ref':
            # Shared definitions are instrumented once in the `definitions` block.
            return node

        if node_type == 'definitions':
            for definition in node['definitions']:
                ref = definition.get('ref')
                if ref is not None:
                    self._ref_labels[ref] = _choice_label(definition)
            node['schema'] = self.visit(node['schema'])
            node['definitions'] = [self.visit(definition) for definition in node['definitions']]
            return node

        if node_type == 'union':
            return self._visit_union(node)

        if node_type == 'tagged-union':
            choices = node[_TAGGED_CHOICES_KEY]
            # `choices` is the only child container of a tagged union; visit it once here and
            # return to avoid wrapping the same branches twice.
            for tag, choice in list(choices.items()):
                choices[tag] = self.visit(choice)
            return node

        self._visit_children(node)

        if node_type in ('model', 'dataclass'):
            return self._wrap_model(node)
        if node_type in ('list', 'deque', 'set', 'frozenset', 'generator', 'tuple'):
            return self._wrap_container(node)
        return node

    def _visit_union(self, node: dict[str, Any]) -> Any:
        union_id = self._union_counter
        self._union_counter += 1
        union_node = NodeId(KIND_UNION, f'union-{union_id}')

        label_map: dict[str, str] = {}
        wrapped_choices: list[Any] = []
        for choice_index, choice in enumerate(node['choices']):
            original_label = self._choice_label(choice)
            choice = self.visit(choice)
            choice_node = NodeId(KIND_CHOICE, original_label, choice.get('ref') or choice.get('schema_ref'))
            wrapper_name = f'__pyd_evidence_choice_{union_id}_{choice_index}__'
            label_map[f'function-wrap[{wrapper_name}()]'] = original_label
            wrapped_choices.append(self._wrap(choice, choice_node, wrapper_name))
        node['choices'] = wrapped_choices

        return self._wrap(node, union_node, f'__pyd_evidence_union_{union_id}__', label_map=label_map)

    def _visit_children(self, node: dict[str, Any]) -> None:
        for key in _SINGLE_CHILD_KEYS:
            child = node.get(key)
            if isinstance(child, dict):
                node[key] = self.visit(child)

        for key in _LIST_CHILD_KEYS:
            children = node.get(key)
            if isinstance(children, list):
                node[key] = [self.visit(child) if isinstance(child, dict) else child for child in children]

        fields = node.get(_FIELD_DICT_KEY)
        if isinstance(fields, dict):
            for field_node in fields.values():
                if isinstance(field_node, dict) and isinstance(field_node.get('schema'), dict):
                    field_node['schema'] = self.visit(field_node['schema'])
        elif isinstance(fields, list):
            # `dataclass-args` / `named-tuple` expose fields as a list of schema dicts.
            node[_FIELD_DICT_KEY] = [self.visit(field_node) for field_node in fields]

        choices = node.get(_TAGGED_CHOICES_KEY)
        if isinstance(choices, list):
            node[_TAGGED_CHOICES_KEY] = [self.visit(choice) for choice in choices]
        elif isinstance(choices, dict):
            for tag, choice in list(choices.items()):
                choices[tag] = self.visit(choice)

        tuple_items = node.get('items_schema')
        if isinstance(tuple_items, list):
            node['items_schema'] = [self.visit(item) for item in tuple_items]

        # `call` exposes a single `arguments_schema` dict, while `arguments` / `arguments-v3`
        # expose it as a list of parameter schemas, each carrying its own `schema`.
        arguments = node.get(_ARGUMENTS_SCHEMA_KEY)
        if isinstance(arguments, dict):
            node[_ARGUMENTS_SCHEMA_KEY] = self.visit(arguments)
        elif isinstance(arguments, list):
            for parameter in arguments:
                if isinstance(parameter, dict) and isinstance(parameter.get('schema'), dict):
                    parameter['schema'] = self.visit(parameter['schema'])

    def _choice_label(self, node: dict[str, Any]) -> str:
        if node.get('type') == 'definition-ref':
            referenced = self._ref_labels.get(node['schema_ref'])
            if referenced is not None:
                return referenced
        return _choice_label(node)

    def _wrap_model(self, node: dict[str, Any]) -> Any:
        cls = node.get('cls')
        name = getattr(cls, '__name__', None) or node.get('model_name') or node['type']
        model_node = NodeId(KIND_MODEL, name, node.get('ref'))
        return self._wrap(node, model_node, f'__pyd_evidence_model_{id(node)}__')

    def _wrap_container(self, node: dict[str, Any]) -> Any:
        container_id = self._container_counter
        self._container_counter += 1
        items = node.get('items_schema')
        if isinstance(items, dict):
            item_node = NodeId(KIND_ITEM, f'item-{container_id}')
            node['items_schema'] = self._wrap(
                items, item_node, f'__pyd_evidence_item_{container_id}__', positional=True
            )
        elif isinstance(items, list):
            node['items_schema'] = [
                self._wrap(
                    item,
                    NodeId(KIND_ITEM, f'item-{container_id}-{index}'),
                    f'__pyd_evidence_item_{container_id}_{index}__',
                    positional=True,
                )
                for index, item in enumerate(items)
            ]
        container_node = NodeId(KIND_LIST, f'{node["type"]}-{container_id}')
        return self._wrap(node, container_node, f'__pyd_evidence_container_{container_id}__')

    def _wrap(
        self,
        node: dict[str, Any],
        evidence_node: NodeId,
        wrapper_name: str,
        *,
        positional: bool = False,
        label_map: dict[str, str] | None = None,
    ) -> _cs.WrapValidatorFunctionSchema:
        def wrapper(value: Any, handler: Any, info: Any) -> Any:
            collector = _active_collector.get()
            if collector is None:
                return handler(value)

            frame = collector.enter(evidence_node, info, positional=positional)
            start = time.perf_counter_ns()
            try:
                result = handler(value)
            except ValidationError as error:
                sanitized = _sanitize_error(error, label_map or {}, info)
                collector.exit(
                    frame,
                    status=STATUS_ERROR,
                    duration_ns=time.perf_counter_ns() - start,
                    errors=sanitized.errors(include_url=False),
                )
                raise sanitized
            except Exception as exception:
                collector.exit(
                    frame,
                    status=STATUS_EXCEPTION,
                    duration_ns=time.perf_counter_ns() - start,
                    exception=exception,
                )
                raise
            else:
                collector.exit(
                    frame,
                    status=STATUS_SUCCESS,
                    duration_ns=time.perf_counter_ns() - start,
                    result=result,
                )
                return result

        wrapper.__name__ = wrapper_name
        wrapped = _cs.with_info_wrap_validator_function(wrapper, node, ref=node.get('ref'))
        return wrapped


def _choice_label(node: dict[str, Any]) -> str:
    cls = node.get('cls')
    if cls is not None:
        return getattr(cls, '__name__', node['type'])
    if node.get('model_name'):
        return node['model_name']
    return node['type']


def _sanitize_errors(errors: list[ErrorDetails], label_map: dict[str, str]) -> list[InitErrorDetails]:
    sanitized: list[InitErrorDetails] = []
    for error in errors:
        line_error: InitErrorDetails = {
            'type': error['type'],
            'loc': tuple(label_map.get(item, item) if isinstance(item, str) else item for item in error['loc']),
            'input': error['input'],
        }
        if 'ctx' in error:
            line_error['ctx'] = error['ctx']
        sanitized.append(line_error)
    return sanitized


def _sanitize_error(error: ValidationError, label_map: dict[str, str], info: Any) -> ValidationError:
    """Rewrite evidence-wrapper loc items back to the original choice labels.

    Only plain unions add their choices' validator name to error locations, so without a
    label map (or without any affected loc) the original error can be re-raised untouched.
    """
    raw_errors = error.errors(include_url=False)
    affected = any(
        isinstance(item, str) and item in label_map for line_error in raw_errors for item in line_error['loc']
    )
    if not label_map or not affected:
        return error

    line_errors = _sanitize_errors(raw_errors, label_map)
    input_type = cast(Literal['python', 'json'], 'json' if getattr(info, 'mode', None) == 'json' else 'python')
    config = getattr(info, 'config', None) or {}
    hide_input = bool(config.get('hide_input_in_errors'))
    rebuilt = ValidationError.from_exception_data(
        error.title, line_errors, input_type=input_type, hide_input=hide_input
    )
    rebuilt.with_traceback(error.__traceback__)
    return rebuilt
