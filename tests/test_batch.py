"""Tests for the experimental batch validation API."""

from __future__ import annotations

from typing import Annotated, Any

import pytest
from pydantic_core import ValidationError

from pydantic import AliasChoices, AliasPath, BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter
from pydantic.experimental.batch import Batch, validate_python


class ObjectDetail:
    def __init__(self, readings: list[dict[str, Any]] | None = None) -> None:
        self._readings = readings if readings is not None else [{'qty': '007', 'fallback': '011'}]
        self.reads = {'readings': 0, 'mirror': 0, 'emergency_qty': 0}

    @property
    def readings(self) -> list[dict[str, Any]]:
        self.reads['readings'] += 1
        return self._readings

    @property
    def mirror(self) -> dict[str, Any]:
        self.reads['mirror'] += 1
        return {'lines': self._readings}

    @property
    def emergency_qty(self) -> str:
        self.reads['emergency_qty'] += 1
        raise ValueError('business boom')


class OrmRow:
    def __init__(self, detail: ObjectDetail) -> None:
        self._detail = detail
        self.reads = {'primary': 0, 'backup': 0}

    @property
    def primary(self) -> ObjectDetail:
        self.reads['primary'] += 1
        return self._detail

    @property
    def backup(self) -> ObjectDetail:
        self.reads['backup'] += 1
        return self._detail

    @property
    def emergency_qty(self) -> str:
        raise ValueError('business boom')


def _drop_first_qty(v: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if v and isinstance(v[0], dict):
        v[0].pop('qty', None)
    return v


class CleanRole(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    readings: Annotated[list[dict[str, Any]], BeforeValidator(_drop_first_qty)] = Field(
        validation_alias=AliasPath('primary', 'readings')
    )
    qty: int = Field(
        validation_alias=AliasChoices(
            AliasPath('backup', 'mirror', 'lines', 0, 'qty'),
            AliasPath('backup', 'mirror', 'lines', 0, 'fallback'),
            AliasPath('backup', 'emergency_qty'),
        )
    )
    note: str = Field(default='missing-note', validation_alias=AliasPath('primary', 'note'))


class ReconcileRole(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    qty: str = Field(
        validation_alias=AliasChoices(
            AliasPath('backup', 'mirror', 'lines', 0, 'qty'),
            AliasPath('backup', 'mirror', 'lines', 0, 'fallback'),
            AliasPath('backup', 'emergency_qty'),
        )
    )


@pytest.mark.parametrize(
    'entries',
    [
        [(CleanRole, 'r'), (ReconcileRole, 'r'), (CleanRole, 'r')],
        [(ReconcileRole, 'r'), (CleanRole, 'r'), (CleanRole, 'r')],
    ],
    ids=['clean-first', 'reconcile-first'],
)
def test_batch_snapshot_shares_reads_and_isolates_roles(entries: list[tuple[type[BaseModel], str]]) -> None:
    detail = ObjectDetail()
    row = OrmRow(detail)
    adapted = [(model, row) for model, _ in entries]

    result = validate_python(adapted)

    if isinstance(result[0], CleanRole):
        assert [model.qty for model in result] == [11, '007', 11]
        assert isinstance(result[1], ReconcileRole)
        assert result[0].readings == result[2].readings
    else:
        assert [model.qty for model in result] == ['007', 11, 11]
        assert isinstance(result[1], CleanRole)

    assert detail._readings == [{'qty': '007', 'fallback': '011'}]
    assert row.reads == {'primary': 1, 'backup': 1}
    assert detail.reads == {'readings': 1, 'mirror': 1, 'emergency_qty': 0}


def test_batch_snapshot_allows_shared_columns_in_same_role() -> None:
    class SharedRole(BaseModel):
        model_config = ConfigDict(from_attributes=True)
        readings: Annotated[list[dict[str, Any]], BeforeValidator(_drop_first_qty)] = Field(
            validation_alias=AliasPath('primary', 'readings')
        )
        mirror: Any = Field(validation_alias=AliasPath('backup', 'mirror'))

    detail = ObjectDetail()
    row = OrmRow(detail)
    result = validate_python([(SharedRole, row)])[0]

    assert result.readings == [{'fallback': '011'}]
    assert result.mirror == {'lines': [{'fallback': '011'}]}
    assert detail._readings == [{'qty': '007', 'fallback': '011'}]


def test_batch_is_fresh_and_supports_equal_identifiers() -> None:
    class QtyRole(BaseModel):
        model_config = ConfigDict(from_attributes=True)
        qty: str = Field(validation_alias=AliasPath('detail', 'mirror', 'lines', 0, 'qty'))

    class Row:
        def __init__(self, qty: str) -> None:
            self._detail = ObjectDetail([{'qty': qty}])

        @property
        def detail(self) -> ObjectDetail:
            return self._detail

    first = validate_python([(QtyRole, Row('007')), (QtyRole, Row('011'))])
    assert [item.qty for item in first] == ['007', '011']

    second = validate_python([(QtyRole, Row('999'))])
    assert [item.qty for item in second] == ['999']


def test_batch_missing_attribute_uses_default() -> None:
    class NoteRole(BaseModel):
        model_config = ConfigDict(from_attributes=True)
        note: str = 'default-note'

    class Row:
        pass

    assert validate_python([(NoteRole, Row())])[0].note == 'default-note'


def test_batch_error_location_and_cause() -> None:
    class FailingRole(BaseModel):
        model_config = ConfigDict(from_attributes=True)
        qty: str = Field(validation_alias=AliasPath('detail', 'emergency_qty'))

    class HealthyRole(BaseModel):
        model_config = ConfigDict(from_attributes=True)
        qty: str = 'ok'

    class Row:
        def __init__(self, detail: ObjectDetail) -> None:
            self._detail = detail

        @property
        def detail(self) -> ObjectDetail:
            return self._detail

    with pytest.raises(ValidationError) as exc_info:
        validate_python([(HealthyRole, Row(ObjectDetail())), (FailingRole, Row(ObjectDetail()))])

    errors = exc_info.value.errors(include_url=False)
    assert errors == [
        {
            'type': 'get_attribute_error',
            'loc': (1, 'qty'),
            'msg': 'Error extracting attribute: ValueError: business boom',
            'input': errors[0]['input'],
            'ctx': {'error': 'ValueError: business boom'},
        }
    ]
    assert isinstance(errors[0]['input'], Row)
    assert 'business boom' in str(exc_info.value)


def test_type_adapter_validate_batch() -> None:
    class Row:
        def __init__(self, detail: ObjectDetail) -> None:
            self._detail = detail

        @property
        def detail(self) -> ObjectDetail:
            return self._detail

    class QtyRole(BaseModel):
        model_config = ConfigDict(from_attributes=True)
        qty: str = Field(validation_alias=AliasPath('detail', 'mirror', 'lines', 0, 'qty'))

    adapter = TypeAdapter(QtyRole)
    assert [item.qty for item in adapter.validate_batch([Row(ObjectDetail()), Row(ObjectDetail())])] == [
        '007',
        '007',
    ]


@pytest.mark.parametrize(
    'roles',
    [
        (CleanRole, ReconcileRole, CleanRole),
        (ReconcileRole, CleanRole, CleanRole),
    ],
    ids=['clean-first', 'reconcile-first'],
)
def test_type_adapter_tuple_batch_snapshot(roles: tuple[type[BaseModel], ...]) -> None:
    """A heterogeneous tuple validated in one call shares a single snapshot."""
    detail = ObjectDetail()
    row = OrmRow(detail)
    adapter = TypeAdapter(tuple[roles])  # type: ignore[valid-type]
    result = adapter.validate_python((row, row, row), from_attributes=True, snapshot_reads=True)

    if issubclass(roles[0], CleanRole):
        assert [item.qty for item in result] == [11, '007', 11]
    else:
        assert [item.qty for item in result] == ['007', 11, 11]

    assert detail._readings == [{'qty': '007', 'fallback': '011'}]
    assert row.reads == {'primary': 1, 'backup': 1}
    assert detail.reads == {'readings': 1, 'mirror': 1, 'emergency_qty': 0}


def test_batch_context_manager() -> None:
    with Batch(snapshot_reads=True) as batch:
        assert batch.validate_python([(CleanRole, OrmRow(ObjectDetail())), (CleanRole, OrmRow(ObjectDetail()))])
