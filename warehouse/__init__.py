"""Warehouse intake cleaning with coordinate receipts.

The package models an arrival batch sent by a gateway. Gateways hand over a
list of raw rows; :func:`clean_batch` returns both the cleaned Pydantic
:class:`~warehouse.batch.Batch` model and a :class:`~warehouse.batch.Receipt`
explaining, with coordinates, which fragments were moved, which values were
generated, and where every selected value originally lived.

Two gateway entry points share the same coordinate vocabulary:
:func:`clean_from_dicts` for Python dictionary lists and
:func:`clean_from_json` for JSON payloads. Receivers holding only the original
batch and the receipt rebuild the return view with :func:`build_return`.
"""

from warehouse.batch import (
    Batch,
    InboundRow,
    LegacyItem,
    Receipt,
    ReceiptEntry,
    clean_batch,
    clean_from_dicts,
    clean_from_json,
    clean_from_json_file,
)
from warehouse.coordinates import (
    BatchCoord,
    Coord,
    ReturnCoord,
    Segment,
    coordinate_key,
    get_value,
    parse_coord,
)
from warehouse.returns import ReturnBatch, ReturnRow, build_return

__all__ = [
    'Batch',
    'BatchCoord',
    'Coord',
    'InboundRow',
    'LegacyItem',
    'Receipt',
    'ReceiptEntry',
    'ReturnBatch',
    'ReturnCoord',
    'ReturnRow',
    'Segment',
    'build_return',
    'clean_batch',
    'clean_from_dicts',
    'clean_from_json',
    'clean_from_json_file',
    'coordinate_key',
    'get_value',
    'parse_coord',
]
