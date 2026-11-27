"""Tests for the experimental validation evidence API.

The evidence API makes observable, for an explicitly enabled validation call:

* which branch of a smart union was actually adopted vs. only tried as a candidate,
* candidate-only errors that never reach the final ``ValidationError``,
* nested node positions (batch index, field) and nullable nodes that are never entered,
* auxiliary ``TypeAdapter`` checks attributed to the validation that triggered them,
* per-node timings, while preserving Pydantic's native single-execution policy and error shape.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any

import pytest
from pydantic_core import ValidationError

import pydantic.plugin._loader as plugin_loader
from pydantic import BaseModel, TypeAdapter, field_validator, validate_call
from pydantic.experimental.evidence import (
    KIND_CHOICE,
    KIND_MODEL,
    KIND_TYPE_ADAPTER,
    check_type_adapter,
    evidence_context,
    evidence_enabled,
    instrument_core_schema,
)


def build_batch_validator(local_state: set[int]):
    id_adapter = TypeAdapter(int)

    class InvoiceA(BaseModel):
        doc_type: str
        invoice_id: int

        @field_validator('invoice_id')
        @classmethod
        def _check_local_state(cls, value: int) -> int:
            # The model's own validator delegates the local-state lookup to a TypeAdapter.
            check_type_adapter(id_adapter, value, name='local-invoice-id')
            if value not in local_state:
                raise ValueError('unknown invoice id')
            return value

    class InvoiceB(BaseModel):
        doc_type: str
        note: str

    return TypeAdapter(list[InvoiceA | InvoiceB]), InvoiceA, InvoiceB


def choice_attempts(report, index: int):
    return [attempt for attempt in report.attempts if attempt.node.kind == KIND_CHOICE and attempt.loc == (index,)]


def test_adopted_branch_vs_tried_candidates() -> None:
    with evidence_enabled():
        validator, invoice_a, invoice_b = build_batch_validator({1})

    batch = [
        {'doc_type': 'invoice', 'invoice_id': 1, 'note': 'extra'},  # both pass, union picks one
        {'doc_type': 'invoice', 'note': 'hello'},  # InvoiceA fails, InvoiceB accepted
        {'doc_type': 'nope'},  # both fail
    ]
    with evidence_context('batch-1') as report:
        with pytest.raises(ValidationError):
            validator.validate_python(batch)

    # Item 0: both candidates succeed; exactly one is adopted (smart union picks the first
    # choice when both are equally good), the other is a non-adopted, error-free candidate.
    item_0 = choice_attempts(report, 0)
    assert {attempt.node.name for attempt in item_0} == {'InvoiceA', 'InvoiceB'}
    assert [attempt.adopted for attempt in item_0] == [True, False]
    assert all(attempt.status == 'success' and not attempt.errors for attempt in item_0)

    # Item 1: InvoiceA is only tried (and failed), InvoiceB is the adopted branch.
    item_1 = choice_attempts(report, 1)
    by_name = {attempt.node.name: attempt for attempt in item_1}
    assert by_name['InvoiceA'].status == 'error'
    assert by_name['InvoiceA'].adopted is False
    assert by_name['InvoiceA'].errors and by_name['InvoiceA'].errors[0]['type'] == 'missing'
    assert by_name['InvoiceB'].status == 'success'
    assert by_name['InvoiceB'].adopted is True

    # Item 2: every candidate failed and none was adopted; the row cannot be received.
    item_2 = choice_attempts(report, 2)
    assert {attempt.status for attempt in item_2} == {'error'}
    assert all(attempt.adopted is False for attempt in item_2)

    assert [choice.name for choice in report.adopted_choices()] == ['InvoiceA', 'InvoiceB']


def test_candidate_errors_do_not_pollute_final_error() -> None:
    with evidence_enabled():
        validator, _invoice_a, _invoice_b = build_batch_validator(set())

    with evidence_context('batch-1') as report:
        # InvoiceA fails (unknown id), InvoiceB succeeds: the overall call succeeds.
        result = validator.validate_python([{'doc_type': 'invoice', 'note': 'hello'}])

    assert result[0].note == 'hello'
    candidate = next(
        attempt for attempt in report.attempts if attempt.node.kind == KIND_CHOICE and attempt.node.name == 'InvoiceA'
    )
    assert candidate.adopted is False and candidate.status == 'error' and candidate.errors
    # The successful candidate has no errors.
    adopted = report.adopted_choices()
    assert [choice.name for choice in adopted] == ['InvoiceB']


def test_final_validation_error_matches_native_loc() -> None:
    with evidence_enabled():
        instrumented, _, _ = build_batch_validator({1})
    native, _, _ = build_batch_validator({1})

    bad_batch = [{'doc_type': 'nope'}]
    with evidence_context('batch-1'):
        with pytest.raises(ValidationError) as instrumented_exc:
            instrumented.validate_python(bad_batch)
    with pytest.raises(ValidationError) as native_exc:
        native.validate_python(bad_batch)

    assert instrumented_exc.value.errors(include_url=False) == native_exc.value.errors(include_url=False)


def test_nested_events_keep_batch_index_and_field_location() -> None:
    with evidence_enabled():
        validator, _, _ = build_batch_validator({7})

    with evidence_context('batch-1') as report:
        validator.validate_python(
            [
                {'doc_type': 'invoice', 'invoice_id': 7},
                {'doc_type': 'invoice', 'note': 'second'},
            ]
        )

    model_attempts = [attempt for attempt in report.attempts if attempt.node.kind == KIND_MODEL]
    # Every nested event carries the index of its row.
    assert {attempt.loc[0] for attempt in model_attempts} == {0, 1}
    invoice_a_locations = {attempt.loc for attempt in model_attempts if attempt.node.name == 'InvoiceA'}
    assert invoice_a_locations == {(0,), (1,)}


def test_nested_model_union_choices_keep_model_labels_and_locs() -> None:
    with evidence_enabled():

        class Alpha(BaseModel):
            a: int

        class Beta(BaseModel):
            b: str

        class Container(BaseModel):
            item: Alpha | Beta

        validator = TypeAdapter(list[Container])

    with evidence_context('batch-1') as report:
        validator.validate_python([{'item': {'a': 1}}, {'item': {'b': 'x'}}])

    choice_names = {attempt.node.name for attempt in report.attempts if attempt.node.kind == KIND_CHOICE}
    # Choices that reference shared definitions are labelled with the model name, not
    # `definition-ref`.
    assert choice_names == {'Alpha', 'Beta'}
    inner_union = next(attempt for attempt in report.union_attempts())
    assert inner_union.loc == (0, 'item')
    assert [choice.name for choice in report.adopted_choices()] == ['Alpha', 'Beta']


def test_nested_list_locations_keep_every_index() -> None:
    with evidence_enabled():

        class Cell(BaseModel):
            value: int

        class Row(BaseModel):
            cells: list[Cell]

        validator = TypeAdapter(list[Row])

    with evidence_context('batch-1') as report:
        validator.validate_python([{'cells': [{'value': 1}, {'value': 2}]}, {'cells': [{'value': 3}]}])

    cell_locations = {
        attempt.loc for attempt in report.attempts if attempt.node.kind == KIND_MODEL and attempt.node.name == 'Cell'
    }
    assert cell_locations == {(0, 'cells', 0), (0, 'cells', 1), (1, 'cells', 0)}


def test_nullable_node_is_not_entered_for_none() -> None:
    with evidence_enabled():

        class Detail(BaseModel):
            value: int

        class Document(BaseModel):
            required: Detail
            optional: Detail | None = None

        validator = TypeAdapter(list[Document])

    with evidence_context('batch-1') as report:
        validator.validate_python(
            [
                {'required': {'value': 1}, 'optional': None},
                {'required': {'value': 2}, 'optional': {'value': 3}},
                {'required': {'value': 4}},
            ]
        )

    detail_attempts = [
        attempt for attempt in report.attempts if attempt.node.kind == KIND_MODEL and attempt.node.name == 'Detail'
    ]
    # Required Detail for each row + optional Detail only on the middle row: 4 executions.
    assert len(detail_attempts) == 4
    assert {attempt.loc for attempt in detail_attempts} == {
        (0, 'required'),
        (1, 'required'),
        (1, 'optional'),
        (2, 'required'),
    }


def test_type_adapter_check_is_attributed_and_indexed() -> None:
    with evidence_enabled():
        validator, _, _ = build_batch_validator({1})

    with evidence_context('batch-1') as report:
        validator.validate_python(
            [
                {'doc_type': 'invoice', 'invoice_id': 1},
                {'doc_type': 'invoice', 'note': 'no id'},
            ]
        )

    local_checks = [attempt for attempt in report.attempts if attempt.node.kind == KIND_TYPE_ADAPTER]
    assert len(local_checks) == 1  # only the InvoiceA row reaches the local-state check
    check = local_checks[0]
    assert check.node.name == 'local-invoice-id'
    assert check.source == 'local-invoice-id'
    assert check.loc == (0,)  # nested event keeps the index of its batch row
    assert check.status == 'success' and check.duration_ns >= 0


def test_each_model_conversion_runs_once() -> None:
    conversions: list[int] = []

    with evidence_enabled():

        class InvoiceA(BaseModel):
            invoice_id: int

            @field_validator('invoice_id')
            @classmethod
            def _count(cls, value: int) -> int:
                conversions.append(value)
                return value

        class InvoiceB(BaseModel):
            note: str

        validator = TypeAdapter(list[InvoiceA | InvoiceB])

        with evidence_context('batch-1') as report:
            validator.validate_python(
                [
                    {'invoice_id': 1, 'note': 'x'},
                    {'note': 'y'},
                    {'invoice_id': 2},
                ]
            )

    # Pydantic's native policy is unchanged: InvoiceA converts once per row that actually
    # reaches its field validator, evidence wrappers do not re-run validation. The failed
    # candidate on row 1 (missing `invoice_id`) never reaches the validator, so it is not
    # counted as a conversion but still appears as an error attempt.
    assert sorted(conversions) == [1, 2]
    invoice_a_attempts = [
        attempt for attempt in report.attempts if attempt.node.name == 'InvoiceA' and attempt.node.kind == KIND_MODEL
    ]
    successful_invoice_a = [attempt for attempt in invoice_a_attempts if attempt.status == 'success']
    assert len(successful_invoice_a) == len(conversions) == 2
    assert sum(attempt.status == 'error' for attempt in invoice_a_attempts) == 1


def test_validation_result_identical_with_and_without_evidence() -> None:
    class InvoiceA(BaseModel):
        invoice_id: int

    class InvoiceB(BaseModel):
        note: str

    payload = [{'invoice_id': 1, 'note': 'x'}, {'note': 'y'}]
    native = TypeAdapter(list[InvoiceA | InvoiceB]).validate_python(payload)
    with evidence_enabled():
        instrumented = TypeAdapter(list[InvoiceA | InvoiceB])
    with evidence_context('batch-1'):
        observed = instrumented.validate_python(payload)
    assert observed == native


def test_validate_call_arguments_are_instrumented() -> None:
    # `validate_call` builds its validator lazily; calling it once inside the gate forces the
    # instrumented build, after which union argument choices produce evidence.
    with evidence_enabled():

        @validate_call
        def echo(value: int | str) -> int | str:
            return value

        echo('warmup')

    with evidence_context('batch-1') as report:
        assert echo('hello') == 'hello'
        assert echo(1) == 1

    assert [choice.name for choice in report.adopted_choices()] == ['str', 'int']
    # Both calls succeed overall even though the rejected `int` candidate is tried (and fails)
    # for the string argument.
    assert all(attempt.node.kind != 'union' or attempt.status == 'success' for attempt in report.attempts)


def test_disabled_by_default_adds_no_wrappers() -> None:
    class Model(BaseModel):
        value: int

    schema = Model.__pydantic_core_schema__

    def contains_wrapper(node: Any) -> bool:
        if isinstance(node, dict):
            if node.get('type') == 'function-wrap' and node['function']['function'].__name__.startswith(
                '__pyd_evidence_'
            ):
                return True
            return any(contains_wrapper(child) for child in node.values())
        if isinstance(node, list):
            return any(contains_wrapper(item) for item in node)
        return False

    assert not contains_wrapper(schema)
    with evidence_context('batch-1') as report:
        Model(value=1)
    assert report.attempts == []


def test_instrument_core_schema_is_opt_in_and_deep_copied() -> None:
    class Model(BaseModel):
        value: int

    original = Model.__pydantic_core_schema__
    instrumented = instrument_core_schema(original)
    assert instrumented is not original
    # The original schema handed to us is not mutated.
    assert Model.__pydantic_core_schema__ is original


def test_exception_status_propagates_and_unwinds_stack() -> None:
    with evidence_enabled():

        class Boom(BaseModel):
            value: int

            @field_validator('value')
            @classmethod
            def _explode(cls, value: int) -> int:
                raise RuntimeError('kaboom')

        validator = TypeAdapter(list[Boom])

    with evidence_context('batch-1') as report:
        with pytest.raises(RuntimeError, match='kaboom'):
            validator.validate_python([{'value': 1}])

    statuses = {(attempt.node.name, attempt.status) for attempt in report.attempts}
    assert ('Boom', 'exception') in statuses
    boom_attempt = next(attempt for attempt in report.attempts if attempt.node.name == 'Boom')
    assert boom_attempt.exception_type == 'RuntimeError'
    # The enclosing list frame is marked as an exception too, leaving no dangling frames.
    list_attempt = next(attempt for attempt in report.attempts if attempt.node.kind == 'list')
    assert list_attempt.status == 'exception'


def test_two_batches_run_concurrently_with_isolated_reports() -> None:
    with evidence_enabled():
        validator, _, _ = build_batch_validator({1})

    outcomes: dict[str, Any] = {}

    def run_batch(batch_id: str, payload: list[dict[str, Any]]) -> None:
        with evidence_context(batch_id) as report:
            try:
                validator.validate_python(payload)
                outcomes[batch_id] = ('ok', report)
            except Exception as exc:  # noqa: BLE001
                outcomes[batch_id] = (type(exc).__name__, report)

    thread_a = threading.Thread(target=run_batch, args=('batch-a', [{'doc_type': 'invoice', 'invoice_id': 1}]))
    thread_b = threading.Thread(target=run_batch, args=('batch-b', [{'doc_type': 'invoice', 'note': 'b'}]))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    report_a = outcomes['batch-a'][1]
    report_b = outcomes['batch-b'][1]
    assert report_a.batch_id == 'batch-a'
    assert report_b.batch_id == 'batch-b'
    # No event leaks across batch contexts: row-indexed events stay on index 0, and the only
    # empty-loc event is the shared root container.
    indexed_locations_a = {attempt.loc for attempt in report_a.attempts if attempt.loc}
    indexed_locations_b = {attempt.loc for attempt in report_b.attempts if attempt.loc}
    assert all(loc[0] == 0 for loc in indexed_locations_a)
    assert all(loc[0] == 0 for loc in indexed_locations_b)
    # Each batch owns its own report with an independent sequence.
    assert report_a is not report_b
    assert [attempt.sequence for attempt in report_a.attempts] == list(range(len(report_a.attempts)))
    assert [attempt.sequence for attempt in report_b.attempts] == list(range(len(report_b.attempts)))


@contextlib.contextmanager
def install_plugin(plugin: Any):
    if plugin_loader._plugins is None:
        plugin_loader._plugins = {}
    plugin_loader._plugins[plugin.__class__.__qualname__] = plugin
    try:
        yield
    finally:
        plugin_loader._plugins.clear()


def test_plugin_exception_ends_call_without_touching_other_batch() -> None:
    class FailingObserver:
        def __init__(self) -> None:
            self.local = threading.local()

        def new_schema_validator(self, *args: Any) -> Any:
            return _ObserverHandler(self.local), None, None

    class _ObserverHandler:
        # Nested validators (e.g. the local-state `TypeAdapter`) reuse the same plugin and
        # fire their own callbacks without a context, so the observer keeps a per-thread stack
        # instead of a single overwritten value; the bottom frame is the top-level validation.
        def __init__(self, local: threading.local) -> None:
            self.local = local

        def _stack(self) -> list[str | None]:
            stack = getattr(self.local, 'stack', None)
            if stack is None:
                stack = []
                self.local.stack = stack
            return stack

        def on_enter(self, input: Any, *, context: Any = None, **kwargs: Any) -> None:
            self._stack().append((context or {}).get('batch'))

        def on_success(self, result: Any) -> None:
            stack = self._stack()
            batch_id = stack.pop()
            if not stack and batch_id == 'batch-a':
                raise RuntimeError('observer failure')

        def on_error(self, error: Any) -> None:
            self._stack().pop()

        def on_exception(self, exception: Any) -> None:
            self._stack().pop()

    with install_plugin(FailingObserver()), evidence_enabled():
        validator, _, _ = build_batch_validator({1})

    outcomes: dict[str, Any] = {}

    def run_batch(batch_id: str, payload: list[dict[str, Any]]) -> None:
        with evidence_context(batch_id) as report:
            try:
                validator.validate_python(payload, context={'batch': batch_id})
            except Exception as exc:  # noqa: BLE001
                outcomes[batch_id] = (type(exc).__name__, str(exc), report)
                return
            outcomes[batch_id] = ('ok', None, report)

    thread_a = threading.Thread(target=run_batch, args=('batch-a', [{'doc_type': 'invoice', 'invoice_id': 1}]))
    thread_b = threading.Thread(target=run_batch, args=('batch-b', [{'doc_type': 'invoice', 'note': 'b'}]))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    # The existing plugin error convention is respected: the throwing batch's call ends with
    # the plugin's exception, while the other batch still succeeds.
    assert outcomes['batch-a'][0] == 'RuntimeError'
    assert outcomes['batch-a'][1] == 'observer failure'
    assert outcomes['batch-b'][0] == 'ok'
    # Batch B's events are never associated with batch A's failing observer.
    assert outcomes['batch-b'][2].batch_id == 'batch-b'
    assert all(attempt.source is None for attempt in outcomes['batch-b'][2].attempts)
