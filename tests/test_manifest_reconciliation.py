from dataclasses import InitVar
from typing import Optional

import pytest
from pydantic import TypeAdapter, model_validator
from pydantic_core import ValidationError

import pydantic
from pydantic.errors import PydanticUserError
from pydantic.experimental.manifest_reconciliation import (
    ManifestReconciliation,
    ManifestReconciliationEntry,
    ManifestReconciliationResult,
)

COLD_ZONE = 'cold'
COLD_CAPACITY = 12

spec_validations: list[int] = []
post_init_calls: list[Optional[dict]] = []


@pydantic.dataclasses.dataclass(frozen=True, slots=True)
class BoxLine:
    """Shared, already-releasable row type: frozen and slots-based."""

    declaration: str
    zone: str = COLD_ZONE
    box_spec: InitVar[Optional[dict]] = None

    @pydantic.field_validator('box_spec')
    @classmethod
    def _validate_box_spec(cls, value: Optional[dict]) -> Optional[dict]:
        if value is not None:
            spec_validations.append(value['cells'])
        return value

    def __post_init__(self, box_spec: Optional[dict]) -> None:
        post_init_calls.append(box_spec)


def _resolve(line: BoxLine, init_only: tuple[object, ...]) -> tuple[str, int]:
    (box_spec,) = init_only
    return line.zone, box_spec['cells']


def _select_lines(manifest: object) -> list[BoxLine]:
    return manifest.lines


def _reconciliation(capacity: dict[str, int] | None = None) -> ManifestReconciliation[BoxLine]:
    return ManifestReconciliation(
        BoxLine,
        resolve=_resolve,
        select_rows=_select_lines,
        capacity=capacity if capacity is not None else {COLD_ZONE: COLD_CAPACITY},
    )


@pydantic.dataclasses.dataclass
class PackingManifest:
    lines: list[BoxLine]

    @model_validator(mode='wrap')
    @classmethod
    def _wrap(cls, data, handler, info):
        manifest = handler(data)
        # Preview row built directly while filtering: independent construction,
        # only shown to the operator, never part of the final manifest.
        preview = BoxLine('H', box_spec={'cells': 9})
        assert preview.declaration == 'H'
        return manifest


@pydantic.dataclasses.dataclass
class DuplicatingManifest:
    lines: list[BoxLine]

    @model_validator(mode='wrap')
    @classmethod
    def _wrap(cls, data, handler, info):
        manifest = handler(data)
        line_b, line_a = manifest.lines
        # Two inbound stations reuse one validated `A` instance at two positions.
        manifest.lines = [line_b, line_a, line_a]
        BoxLine('H', box_spec={'cells': 9})
        return manifest


@pydantic.dataclasses.dataclass
class PreviewInFinalManifest:
    lines: list[BoxLine]

    @model_validator(mode='wrap')
    @classmethod
    def _wrap(cls, data, handler, info):
        manifest = handler(data)
        preview = BoxLine('H', box_spec={'cells': 9})
        previous_batch = BoxLine('A', box_spec={'cells': 4})
        manifest.lines = [*manifest.lines, preview, previous_batch]
        return manifest


@pydantic.dataclasses.dataclass
class SealedPackingManifest:
    lines: list[BoxLine]

    @model_validator(mode='wrap')
    @classmethod
    def _wrap(cls, data, handler, info):
        manifest = handler(data)
        if info.context is not None and info.context.get('sealed') is False:
            raise ValueError('seal broken')
        BoxLine('H', box_spec={'cells': 9})
        return manifest


A_DECLARATION = {'declaration': 'A', 'box_spec': {'cells': 4}}
B_DECLARATION = {'declaration': 'B', 'box_spec': {'cells': 6}}


@pytest.fixture(autouse=True)
def _reset_counters():
    spec_validations.clear()
    post_init_calls.clear()
    yield
    spec_validations.clear()
    post_init_calls.clear()


def test_reconciles_final_manifest_by_final_index() -> None:
    adapter = TypeAdapter(PackingManifest, manifest_reconciliation=_reconciliation())

    manifest, result = adapter.validate_python_with_manifest_reconciliation(
        {'lines': [B_DECLARATION, A_DECLARATION]}
    )

    assert [line.declaration for line in manifest.lines] == ['B', 'A']
    assert isinstance(result, ManifestReconciliationResult)
    assert result.zone_usage == {COLD_ZONE: 10}
    assert result.total_cells == 10
    assert result.entries == (
        ManifestReconciliationEntry(index=0, zone=COLD_ZONE, cells=6),
        ManifestReconciliationEntry(index=1, zone=COLD_ZONE, cells=4),
    )


def test_equal_rows_are_independent_but_both_have_a_basis() -> None:
    adapter = TypeAdapter(PackingManifest, manifest_reconciliation=_reconciliation())

    manifest, result = adapter.validate_python_with_manifest_reconciliation(
        {'lines': [A_DECLARATION, A_DECLARATION]}
    )

    first, second = manifest.lines
    assert first == second
    assert first is not second
    assert [entry.cells for entry in result.entries] == [4, 4]


def test_reused_instance_counts_at_every_final_position() -> None:
    adapter = TypeAdapter(DuplicatingManifest, manifest_reconciliation=_reconciliation(capacity={COLD_ZONE: 20}))

    manifest, result = adapter.validate_python_with_manifest_reconciliation(
        {'lines': [B_DECLARATION, A_DECLARATION]}
    )

    assert [line.declaration for line in manifest.lines] == ['B', 'A', 'A']
    assert manifest.lines[1] is manifest.lines[2]
    assert result.zone_usage == {COLD_ZONE: 14}
    assert [entry.cells for entry in result.entries] == [6, 4, 4]
    assert [entry.index for entry in result.entries] == [0, 1, 2]


def test_capacity_exceeded_raises_with_zone_totals() -> None:
    adapter = TypeAdapter(DuplicatingManifest, manifest_reconciliation=_reconciliation())

    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python_with_manifest_reconciliation({'lines': [B_DECLARATION, A_DECLARATION]})

    errors = exc_info.value.errors()
    assert len(errors) == 1
    error = errors[0]
    assert error['type'] == 'manifest_capacity_exceeded'
    assert error['loc'] == (COLD_ZONE,)
    assert error['ctx'] == {'zone': COLD_ZONE, 'used': 14, 'capacity': COLD_CAPACITY}


def test_preview_and_previous_batch_rows_report_missing_basis_at_final_index() -> None:
    adapter = TypeAdapter(PreviewInFinalManifest, manifest_reconciliation=_reconciliation())

    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python_with_manifest_reconciliation({'lines': [B_DECLARATION, A_DECLARATION]})

    errors = exc_info.value.errors()
    assert [error['loc'] for error in errors] == [(2,), (3,)]
    assert {error['type'] for error in errors} == {'manifest_missing_basis'}
    # The previously constructed row compares equal to the validated `A` row, yet
    # equality does not grant it a construction basis.
    assert errors[1]['input'] == BoxLine('A', box_spec={'cells': 4})


def test_seal_failure_surfaces_original_error_without_capacity_result() -> None:
    adapter = TypeAdapter(SealedPackingManifest, manifest_reconciliation=_reconciliation())

    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python_with_manifest_reconciliation(
            {'lines': [B_DECLARATION, A_DECLARATION]}, context={'sealed': False}
        )
    assert exc_info.value.errors()[0]['type'] == 'value_error'
    assert 'seal broken' in exc_info.value.errors()[0]['msg']

    # The same adapter can still reconcile a later batch.
    manifest, result = adapter.validate_python_with_manifest_reconciliation(
        {'lines': [B_DECLARATION, A_DECLARATION]}, context={'sealed': True}
    )
    assert [line.declaration for line in manifest.lines] == ['B', 'A']
    assert result.zone_usage == {COLD_ZONE: 10}


def test_hook_call_counts_match_plain_validation() -> None:
    plain_adapter = TypeAdapter(SealedPackingManifest)
    reconciled_adapter = TypeAdapter(SealedPackingManifest, manifest_reconciliation=_reconciliation())
    payload = {'lines': [B_DECLARATION, A_DECLARATION]}

    plain_adapter.validate_python(payload, context={'sealed': True})
    plain_spec_count = len(spec_validations)
    plain_post_init_count = len(post_init_calls)

    reconciled_adapter.validate_python_with_manifest_reconciliation(payload, context={'sealed': True})
    assert len(spec_validations) == plain_spec_count * 2
    assert len(post_init_calls) == plain_post_init_count * 2

    # Reusing the adapter reuses the instrumented validator and stays equivalent.
    reconciled_adapter.validate_python_with_manifest_reconciliation(payload, context={'sealed': True})
    assert len(spec_validations) == plain_spec_count * 3
    assert len(post_init_calls) == plain_post_init_count * 3


def test_reconciliation_does_not_touch_plain_validation_entry_points() -> None:
    adapter = TypeAdapter(PackingManifest, manifest_reconciliation=_reconciliation())

    manifest = adapter.validate_python({'lines': [B_DECLARATION, A_DECLARATION]})
    assert [line.declaration for line in manifest.lines] == ['B', 'A']

    # A row built directly by the caller has no basis from the manifest validation.
    direct = BoxLine('H', box_spec={'cells': 9})
    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python_with_manifest_reconciliation({'lines': [B_DECLARATION, A_DECLARATION, direct]})
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]['type'] == 'manifest_missing_basis'
    assert errors[0]['loc'] == (2,)


def test_json_validation_reconciles() -> None:
    adapter = TypeAdapter(PackingManifest, manifest_reconciliation=_reconciliation())

    manifest, result = adapter.validate_json_with_manifest_reconciliation(
        '{"lines": [{"declaration": "B", "box_spec": {"cells": 6}},'
        ' {"declaration": "A", "box_spec": {"cells": 4}}]}'
    )
    assert [line.declaration for line in manifest.lines] == ['B', 'A']
    assert result.zone_usage == {COLD_ZONE: 10}
    assert [entry.cells for entry in result.entries] == [6, 4]


def test_validate_strings_reconciles() -> None:
    adapter = TypeAdapter(PackingManifest, manifest_reconciliation=_reconciliation())

    manifest, result = adapter.validate_strings_with_manifest_reconciliation(
        {'lines': [{'declaration': 'B', 'box_spec': {'cells': 6}}, {'declaration': 'A', 'box_spec': {'cells': 4}}]}
    )
    assert [line.declaration for line in manifest.lines] == ['B', 'A']
    assert result.zone_usage == {COLD_ZONE: 10}
    assert [entry.cells for entry in result.entries] == [6, 4]


def test_reconciliation_method_requires_configuration() -> None:
    adapter = TypeAdapter(PackingManifest)

    with pytest.raises(PydanticUserError, match='ManifestReconciliation'):
        adapter.validate_python_with_manifest_reconciliation({'lines': []})


def test_row_type_must_be_a_dataclass() -> None:
    with pytest.raises(PydanticUserError) as exc_info:
        ManifestReconciliation(
            int,
            resolve=lambda row, init_only: (COLD_ZONE, 0),
            select_rows=lambda manifest: [],
            capacity={COLD_ZONE: 1},
        )
    assert exc_info.value.code == 'manifest-reconciliation-row-type'


def test_class_validator_and_direct_construction_are_untouched() -> None:
    class_validator = BoxLine.__pydantic_validator__
    adapter = TypeAdapter(PackingManifest, manifest_reconciliation=_reconciliation())

    assert BoxLine.__pydantic_validator__ is class_validator
    # Direct construction never registers a basis, even inside the instrumented run.
    preview = BoxLine('H', box_spec={'cells': 9})
    assert BoxLine.__pydantic_validator__ is class_validator

    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python_with_manifest_reconciliation({'lines': [B_DECLARATION, preview]})
    assert exc_info.value.errors()[0]['loc'] == (1,)


def test_multiple_zones_are_checked_independently() -> None:
    warm_zone = 'warm'

    def resolve_by_line(line: BoxLine, init_only: tuple[object, ...]) -> tuple[str, int]:
        (box_spec,) = init_only
        return line.zone, box_spec['cells']

    adapter = TypeAdapter(
        PackingManifest,
        manifest_reconciliation=ManifestReconciliation(
            BoxLine,
            resolve=resolve_by_line,
            select_rows=_select_lines,
            capacity={COLD_ZONE: 12, warm_zone: 2},
        ),
    )
    payload = {
        'lines': [
            {'declaration': 'W', 'zone': warm_zone, 'box_spec': {'cells': 3}},
            B_DECLARATION,
        ]
    }

    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python_with_manifest_reconciliation(payload)
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]['type'] == 'manifest_capacity_exceeded'
    assert errors[0]['loc'] == (warm_zone,)
    assert errors[0]['ctx'] == {'zone': warm_zone, 'used': 3, 'capacity': 2}


def test_generic_row_type_is_supported() -> None:
    from typing import Generic, TypeVar

    row_t = TypeVar('row_t')

    @pydantic.dataclasses.dataclass(frozen=True, slots=True)
    class GenericLine(Generic[row_t]):
        declaration: row_t
        box_spec: InitVar[Optional[dict]] = None

    @pydantic.dataclasses.dataclass
    class GenericManifest:
        lines: list[GenericLine[str]]

    reconciliation = ManifestReconciliation(
        GenericLine,
        resolve=lambda line, init_only: (COLD_ZONE, init_only[0]['cells']),
        select_rows=lambda manifest: manifest.lines,
        capacity={COLD_ZONE: 12},
    )
    adapter = TypeAdapter(GenericManifest, manifest_reconciliation=reconciliation)

    manifest, result = adapter.validate_python_with_manifest_reconciliation(
        {'lines': [{'declaration': 'A', 'box_spec': {'cells': 4}}]}
    )
    assert manifest.lines[0].declaration == 'A'
    assert result.zone_usage == {COLD_ZONE: 4}


def test_row_nested_in_base_model_keeps_definition_refs() -> None:
    class Cart(pydantic.BaseModel):
        rows: list[BoxLine]
        extra: BoxLine

    adapter = TypeAdapter(
        Cart,
        manifest_reconciliation=ManifestReconciliation(
            BoxLine,
            resolve=_resolve,
            select_rows=lambda manifest: [*manifest.rows, manifest.extra],
            capacity={COLD_ZONE: 12},
        ),
    )

    cart, result = adapter.validate_python_with_manifest_reconciliation(
        {
            'rows': [{'declaration': 'A', 'box_spec': {'cells': 4}}],
            'extra': {'declaration': 'B', 'box_spec': {'cells': 6}},
        }
    )
    assert [line.declaration for line in [*cart.rows, cart.extra]] == ['A', 'B']
    assert result.zone_usage == {COLD_ZONE: 10}
    assert [entry.index for entry in result.entries] == [0, 1]
