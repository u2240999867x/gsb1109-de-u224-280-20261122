"""Tests for batch cleaning, the coordinate receipt and the return view."""

from __future__ import annotations

import json

import pytest

from pydantic import RootModel
from warehouse import (
    Batch,
    BatchCoord,
    Coord,
    Receipt,
    ReturnBatch,
    ReturnCoord,
    build_return,
    clean_from_dicts,
    clean_from_json,
    clean_from_json_file,
    get_value,
    parse_coord,
)


def test_cleaned_batch_is_root_model_sorted_by_station(raw_batch: list[dict]) -> None:
    batch, _ = clean_from_dicts(raw_batch)

    assert isinstance(batch, RootModel)
    assert isinstance(batch, Batch)
    assert [row.station for row in batch.root] == ['A', 'B']


def test_b_row_pre_cleaning_overwrites_code_and_reverses_legacy(raw_batch: list[dict]) -> None:
    batch, _ = clean_from_dicts(raw_batch)
    b_row = batch.root[1]

    assert b_row.station == 'B'
    assert b_row.code == 'B-007'
    assert [item.label for item in b_row.legacy] == ['007', '007']
    assert [item.code for item in b_row.legacy] == ['OLD-B1', 'OLD-B0']
    assert [item.unit for item in b_row.legacy] == ['piece', 'box']
    assert b_row.model == 'B-007'
    assert b_row.unit == 'box'


def test_a_row_keeps_gateway_code_and_legacy_order(raw_batch: list[dict]) -> None:
    batch, _ = clean_from_dicts(raw_batch)
    a_row = batch.root[0]

    assert a_row.code == 'A-201'
    assert [item.code for item in a_row.legacy] == ['OLD-A0', 'OLD-A1']
    assert [item.unit for item in a_row.legacy] == ['crate', 'pallet']
    assert a_row.model == 'A-201'
    assert a_row.unit == 'pallet'


def test_receipt_is_root_model_of_plain_coordinates(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    assert isinstance(receipt, RootModel)
    assert isinstance(receipt, Receipt)
    assert json.loads(receipt.model_dump_json()) == receipt.model_dump()
    assert all(isinstance(entry.target, str) for entry in receipt.root)
    assert all(isinstance(source, str) for entry in receipt.root for source in entry.sources)


def test_receipt_records_station_moves_with_original_row_numbers(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    assert receipt.entries_for('batch[0].station') == [
        entry for entry in receipt.root if entry.target == 'batch[0].station'
    ]
    assert receipt.source_keys('batch[0].station') == ['original[1].station']
    assert receipt.source_keys('batch[1].station') == ['original[0].station']


def test_receipt_records_b_legacy_reversal_moves(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    assert receipt.source_keys('batch[1].legacy[0].unit') == ['original[0].legacy[1].unit']
    assert receipt.source_keys('batch[1].legacy[1].unit') == ['original[0].legacy[0].unit']
    assert receipt.source_keys('batch[1].legacy[0].label') == ['original[0].legacy[1].label']
    assert receipt.source_keys('batch[1].legacy[1].note') == ['original[0].legacy[0].note']


def test_receipt_has_no_legacy_moves_for_a_row(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    assert not [entry for entry in receipt.root if entry.kind == 'move' and entry.target.startswith('batch[0].legacy')]


def test_b_model_receipt_names_station_and_tail_label_sources(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    entry = receipt.locate('batch[1].model')[0]
    assert entry.kind == 'generated'
    assert entry.sources == ['original[0].station', 'original[0].legacy[0].label']


def test_b_unit_receipt_names_original_first_legacy_unit(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    assert receipt.source_keys('batch[1].unit') == ['original[0].legacy[0].unit']


def test_a_model_and_unit_receipt_use_directly_adopted_sources(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)

    assert receipt.source_keys('batch[0].model') == ['original[1].code']
    assert receipt.source_keys('batch[0].unit') == ['original[1].legacy[1].unit']


def test_b_return_only_drops_the_three_consumed_fragments(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)
    returned = build_return(raw_batch, receipt).model_dump()

    assert isinstance(build_return(raw_batch, receipt), ReturnBatch)
    assert returned[0] == {
        'code': 'B-007',
        'legacy': [
            {'code': 'OLD-B0', 'note': 'b-note-0'},
            {'label': '007', 'code': 'OLD-B1', 'unit': 'piece', 'note': 'b-note-1'},
        ],
    }


def test_a_return_keeps_sorting_station_and_drops_only_adopted_fragments(
    raw_batch: list[dict],
) -> None:
    _, receipt = clean_from_dicts(raw_batch)
    returned = build_return(raw_batch, receipt).model_dump()

    assert returned[1] == {
        'station': 'A',
        'legacy': [
            {'label': '010', 'code': 'OLD-A0', 'unit': 'crate', 'note': 'a-note-0'},
            {'label': '011', 'code': 'OLD-A1', 'note': 'a-note-1'},
        ],
    }


def test_return_keeps_original_row_numbers_and_legacy_indices(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)
    returned = build_return(raw_batch, receipt).root

    assert returned[0].root['legacy'][0] == {'code': 'OLD-B0', 'note': 'b-note-0'}
    assert returned[1].root['station'] == 'A'
    assert len(returned[0].root['legacy']) == 2
    assert len(returned[1].root['legacy']) == 2


def test_original_batch_stays_usable_for_reconciliation(raw_batch: list[dict]) -> None:
    snapshot = json.dumps(raw_batch)
    _, receipt = clean_from_dicts(raw_batch)
    build_return(raw_batch, receipt)

    assert json.dumps(raw_batch) == snapshot
    assert raw_batch[0]['code'] == 'B-007'
    assert raw_batch[0]['legacy'][0]['unit'] == 'box'
    assert raw_batch[1]['legacy'][1]['unit'] == 'pallet'


def test_dict_and_json_gateways_share_the_same_coordinates(raw_batch: list[dict]) -> None:
    dict_batch, dict_receipt = clean_from_dicts(raw_batch)
    json_batch, json_receipt = clean_from_json(json.dumps(raw_batch))

    assert json_receipt.model_dump() == dict_receipt.model_dump()
    assert json_batch.model_dump() == dict_batch.model_dump()

    dict_return = build_return(raw_batch, dict_receipt).model_dump()
    json_return = build_return(json.loads(json.dumps(raw_batch)), json_receipt).model_dump()
    assert json_return == dict_return


def test_json_file_gateway_matches_dict_gateway(tmp_path, raw_batch: list[dict]) -> None:
    payload = tmp_path / 'batch.json'
    payload.write_text(json.dumps(raw_batch), encoding='utf-8')

    file_batch, file_receipt = clean_from_json_file(payload)
    dict_batch, dict_receipt = clean_from_dicts(raw_batch)

    assert file_receipt.model_dump() == dict_receipt.model_dump()
    assert file_batch.model_dump() == dict_batch.model_dump()


def test_receiver_resolves_intake_positions_back_to_originals(raw_batch: list[dict]) -> None:
    _, receipt = clean_from_dicts(raw_batch)
    returned = build_return(raw_batch, receipt).model_dump()

    sources = receipt.source_keys('batch[1].model')
    assert [get_value(raw_batch, source) for source in sources] == ['B', '007']
    assert get_value(raw_batch, 'original[0].legacy[0].unit') == 'box'

    assert get_value(returned, 'return[0].code') == 'B-007'
    assert get_value(returned, 'return[1].station') == 'A'
    with pytest.raises(KeyError, match=r'return\[0\]\.legacy\[0\]\.unit'):
        get_value(returned, 'return[0].legacy[0].unit')
    assert get_value(returned, 'return[0].legacy[0].unit', default=None) is None


def test_model_falls_back_to_legacy_one_code_without_top_level_code() -> None:
    raw_rows = [
        {
            'station': 'C',
            'legacy': [
                {'label': 'x', 'code': 'L0', 'unit': 'u0'},
                {'label': 'y', 'code': 'L1', 'unit': 'u1'},
            ],
        }
    ]

    batch, receipt = clean_from_dicts(raw_rows)

    assert batch.root[0].model == 'L1'
    assert batch.root[0].unit == 'u1'
    assert receipt.source_keys('batch[0].model') == ['original[0].legacy[1].code']


def test_coord_rendering_and_parsing_round_trip() -> None:
    coord = BatchCoord(2, 'legacy.unit', 0)
    assert coord.key() == 'batch[2].legacy[0].unit'
    assert parse_coord(coord.key()) == coord

    origin = ReturnCoord(0, 'legacy.label', 1, view='original')
    assert origin.key() == 'original[0].legacy[1].label'
    assert origin.in_return().key() == 'return[0].legacy[1].label'
    assert origin.in_original() == origin

    top_level = Coord('return', 3, 'station')
    assert top_level.key() == 'return[3].station'
    assert parse_coord(top_level.key()) == top_level


def test_coord_rejects_legacy_index_mismatch() -> None:
    with pytest.raises(ValueError, check=lambda err: 'legacy_index' in str(err)):
        BatchCoord(0, 'legacy.unit')
    with pytest.raises(ValueError, check=lambda err: 'legacy_index' in str(err)):
        Coord('batch', 0, 'station', legacy_index=1)
