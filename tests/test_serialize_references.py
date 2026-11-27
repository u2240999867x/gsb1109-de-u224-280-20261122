"""Tests for the opt-in ``serialize_references`` export."""

from __future__ import annotations

import json

from pydantic import BaseModel, Field


class Tray(BaseModel):
    warehouse: str
    code: str
    pieces: int = Field(serialization_alias='pieces')


class SealedTray(Tray):
    seal: str


class Row(BaseModel):
    kind: str
    tray: Tray


class Manifest(BaseModel):
    code: str
    review: Tray
    rows: list[Row]


def test_default_export_is_unchanged() -> None:
    east = Tray(warehouse='east', code='P-17', pieces=5)
    m = Manifest(code='C-41', review=east, rows=[Row(kind='formal', tray=east)])
    assert m.model_dump() == {
        'code': 'C-41',
        'review': {'warehouse': 'east', 'code': 'P-17', 'pieces': 5},
        'rows': [{'kind': 'formal', 'tray': {'warehouse': 'east', 'code': 'P-17', 'pieces': 5}}],
    }


def test_shared_instance_is_referenced() -> None:
    east = Tray(warehouse='east', code='P-17', pieces=5)
    m = Manifest(code='C-41', review=east, rows=[Row(kind='formal', tray=east)])

    dumped = m.model_dump(serialize_references=True)
    assert dumped['review'] == {'warehouse': 'east', 'code': 'P-17', 'pieces': 5, '$id': 'ref_0'}
    assert dumped['rows'][0]['tray'] == {'$ref': 'ref_0'}

    as_json = json.loads(m.model_dump_json(serialize_references=True))
    assert as_json == dumped


def test_distinct_instances_are_kept_independent() -> None:
    east = Tray(warehouse='east', code='P-17', pieces=5)
    west = Tray(warehouse='west', code='P-17', pieces=5)
    m = Manifest(code='C-41', review=east, rows=[Row(kind='formal', tray=west)])

    dumped = m.model_dump(serialize_references=True)
    assert '$id' not in dumped['review']
    assert dumped['review'] == {'warehouse': 'east', 'code': 'P-17', 'pieces': 5}
    assert dumped['rows'][0]['tray'] == {'warehouse': 'west', 'code': 'P-17', 'pieces': 5}


def test_aliases_and_polymorphism_are_respected() -> None:
    east = SealedTray(warehouse='east', code='P-17', pieces=5, seal='SECRET')
    m = Manifest(code='C-41', review=east, rows=[Row(kind='formal', tray=east)])

    dumped = m.model_dump(
        by_alias=True, polymorphic_serialization=True, serialize_references=True
    )
    assert dumped['review'] == {
        'warehouse': 'east',
        'code': 'P-17',
        'pieces': 5,
        'seal': 'SECRET',
        '$id': 'ref_0',
    }
    assert dumped['rows'][0]['tray'] == {'$ref': 'ref_0'}
