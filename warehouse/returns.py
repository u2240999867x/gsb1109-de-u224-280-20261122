"""Return view derived from the original batch and the receipt.

The receiver of an intake result only holds the original batch and the
:class:`~warehouse.batch.Receipt`. The return view is rebuilt from those two:
every original coordinate named as a source of a ``generated`` receipt entry
was actually consumed by intake and is removed; everything else is kept.

Return rows preserve the original row numbering and legacy array indices.
"""

from __future__ import annotations

import copy
from typing import Any

from pydantic import RootModel
from warehouse.batch import Receipt
from warehouse.coordinates import Coord, parse_coord


class ReturnRow(RootModel[dict[str, Any]]):
    """One row of the return view, keyed by the original field names.

    The underlying mapping keeps the original row numbering (row ``i`` of the
    return corresponds to row ``i`` of the original batch) and the original
    legacy array indices; consumed fragments are simply absent.
    """


class ReturnBatch(RootModel[list[ReturnRow]]):
    """The full return view, indexed by original row number."""

    def get(self, row_index: int) -> ReturnRow:
        """Return the return row for original row ``row_index``."""
        return self.root[row_index]


def _consumed_coordinates(receipt: Receipt) -> set[Coord]:
    consumed: set[Coord] = set()
    for entry in receipt.root:
        if entry.kind != 'generated':
            continue
        for source in entry.sources:
            coord = parse_coord(source)
            if coord.view == 'original':
                consumed.add(coord)
    return consumed


def build_return(original_rows: list[dict], receipt: Receipt) -> ReturnBatch:
    """Build the return view from the original batch and its receipt.

    Args:
        original_rows: The raw gateway rows, unchanged since they were handed
            to the cleaner.
        receipt: The receipt produced while cleaning those rows.

    Returns:
        A :class:`ReturnBatch` whose rows and legacy arrays keep the original
        numbering and indices, minus the fragments intake consumed.
    """
    consumed = _consumed_coordinates(receipt)
    rows = copy.deepcopy(original_rows)
    for coord in consumed:
        row = rows[coord.row_index]
        if coord.segment in ('station', 'code'):
            row.pop(coord.segment, None)
        elif coord.segment.startswith('legacy.'):
            field = coord.segment.split('.', 1)[1]
            legacy = row.get('legacy')
            if isinstance(legacy, list) and coord.legacy_index is not None and coord.legacy_index < len(legacy):
                item = legacy[coord.legacy_index]
                if isinstance(item, dict):
                    item.pop(field, None)
    return ReturnBatch.model_validate(rows)
