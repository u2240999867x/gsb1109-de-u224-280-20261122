"""Tests for the `ValidateAs` round-trip declaration support."""

from typing import Annotated, ClassVar, Literal

import pytest
from pydantic_core import core_schema

from pydantic import (
    BaseModel,
    BeforeValidator,
    DeclarationStore,
    Field,
    StrictInt,
    TypeAdapter,
    ValidateAs,
    ValidationError,
    ValidationInfo,
)


class Quantity:
    """A measurement-library quantity, interned by piece count."""

    _pool: ClassVar[dict[int, 'Quantity']] = {}

    def __init__(self, pieces: int) -> None:
        self.pieces = pieces

    @classmethod
    def intern(cls, pieces: int) -> 'Quantity':
        return cls._pool.setdefault(pieces, cls(pieces))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Quantity) and self.pieces == other.pieces

    def __hash__(self) -> int:
        return hash(self.pieces)

    def __repr__(self) -> str:
        return f'Quantity({self.pieces})'

    @classmethod
    def __get_pydantic_core_schema__(cls, source: type, handler) -> core_schema.CoreSchema:
        return core_schema.is_instance_schema(
            cls,
            serialization=core_schema.plain_serializer_function_ser_schema(lambda q: q.pieces),
        )


class CaseDeclaration(BaseModel):
    kind: Literal['case']
    count: StrictInt = Field(alias='n')
    per: int


class PackDeclaration(BaseModel):
    kind: Literal['pack']
    count: str = Field(alias='n', pattern=r'\d{2}')
    per: int


Declaration = Annotated[CaseDeclaration | PackDeclaration, Field(discriminator='kind')]


@pytest.fixture(name='cargo_adapter')
def cargo_adapter_fixture():
    calls = []
    produced = []

    def to_quantity(declaration: CaseDeclaration | PackDeclaration) -> Quantity:
        calls.append(declaration)
        quantity = Quantity.intern(int(declaration.count) * declaration.per)
        produced.append(quantity)
        return quantity

    def clean_key(key: str, info: ValidationInfo) -> str:
        cleaned = key.lower()
        if isinstance(info.context, DeclarationStore):
            info.context.track_key(cleaned)
        return cleaned

    Bin = Annotated[str, BeforeValidator(clean_key)]
    Cargo = Annotated[Quantity, ValidateAs(Declaration, to_quantity, round_trip=True)]
    adapter = TypeAdapter(dict[Bin, Cargo])
    return adapter, calls, produced


def test_round_trip_declaration(cargo_adapter) -> None:
    ta, calls, produced = cargo_adapter

    store1 = DeclarationStore()
    batch1 = ta.validate_python(
        {
            'A': {'kind': 'case', 'n': 2, 'per': 6},
            'B': {'kind': 'case', 'n': 2, 'per': 6},
            'a': {'kind': 'pack', 'n': '03', 'per': 4},
        },
        context=store1,
    )
    # keys are cleaned to lowercase, the last declaration for a bin wins:
    assert batch1 == {'a': Quantity.intern(12), 'b': Quantity.intern(12)}
    # every conversion reused the same interned 12-piece quantity:
    assert produced[0] is produced[1] is produced[2] is batch1['a'] is batch1['b']

    # second batch only swaps the declarations of `A` and `a`:
    store2 = DeclarationStore()
    batch2 = ta.validate_python(
        {
            'A': {'kind': 'pack', 'n': '03', 'per': 4},
            'B': {'kind': 'case', 'n': 2, 'per': 6},
            'a': {'kind': 'case', 'n': 2, 'per': 6},
        },
        context=store2,
    )
    assert batch2 == batch1
    assert len(calls) == 6

    # the second batch is exported first, with its own credentials:
    assert ta.dump_python(store2.view(), by_alias=True) == {
        'a': {'kind': 'case', 'n': 2, 'per': 6},
        'b': {'kind': 'case', 'n': 2, 'per': 6},
    }
    assert ta.dump_json(store2.view(), by_alias=True) == (
        b'{"a":{"kind":"case","n":2,"per":6},"b":{"kind":"case","n":2,"per":6}}'
    )

    # then a single entry of the first batch is selected by its cleaned key:
    assert ta.dump_python(store1.view('a'), by_alias=True) == {'a': {'kind': 'pack', 'n': '03', 'per': 4}}
    assert ta.dump_json(store1.view('a'), by_alias=True) == b'{"a":{"kind":"pack","n":"03","per":4}}'

    # exporting and selecting declarations does not re-run the conversion:
    assert len(calls) == 6

    # both Python and JSON outputs are accepted back by the same adapter:
    assert ta.validate_python(ta.dump_python(store2.view(), by_alias=True)) == batch2
    assert ta.validate_json(ta.dump_json(store2.view(), by_alias=True)) == batch2
    assert ta.validate_python(ta.dump_python(store1.view('a'), by_alias=True)) == {'a': Quantity.intern(12)}
    assert ta.validate_json(ta.dump_json(store1.view('a'), by_alias=True)) == {'a': Quantity.intern(12)}


def test_round_trip_plain_export_unchanged(cargo_adapter) -> None:
    ta, _, _ = cargo_adapter

    batch = ta.validate_python(
        {'A': {'kind': 'case', 'n': 2, 'per': 6}},
        context=DeclarationStore(),
    )
    # plain export keeps the existing piece-count representation:
    assert ta.dump_python(batch) == {'a': 12}
    assert ta.dump_json(batch) == b'{"a":12}'

    # a dict that only carries piece counts and was never validated against a
    # declaration can be exported plainly as well:
    plain = {'a': Quantity.intern(12)}
    assert ta.dump_python(plain) == {'a': 12}
    assert ta.dump_json(plain) == b'{"a":12}'

    # ... but a declaration export points out the selected key lacking a basis:
    with pytest.raises(KeyError, match="No declaration recorded for key 'a'"):
        DeclarationStore().view('a')


def test_round_trip_declaration_contract_enforced(cargo_adapter) -> None:
    ta, _, _ = cargo_adapter

    # `case` declarations still reject a string `n` per the old contract:
    with pytest.raises(ValidationError) as exc_info:
        ta.validate_python({'A': {'kind': 'case', 'n': '03', 'per': 6}}, context=DeclarationStore())
    assert exc_info.value.errors(include_url=False)[0]['type'] == 'int_type'

    # `pack` declarations require a two-digit string `n`:
    with pytest.raises(ValidationError):
        ta.validate_python({'A': {'kind': 'pack', 'n': '3', 'per': 4}}, context=DeclarationStore())
