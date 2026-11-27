"""Coordinate vocabulary shared by the receipt, the return and the gateways.

Coordinates are plain, JSON-serialisizable strings so that a receipt produced
from a list of Python dictionaries and one produced from JSON text use exactly
the same vocabulary. Every coordinate names a *fragment* (a scalar value)
inside either the cleaned batch or the original/return view of a row:

* ``batch[0].model``              - cleaned top-level ``code`` of row 0
* ``batch[0].legacy[1].code``     - a scalar inside a cleaned legacy item
* ``original[1].legacy[0].label`` - the same fragment in the original batch
* ``return[1].station``           - the same fragment in the return view

The original batch and the return view keep the original row numbering and the
original legacy array indices, even after cleaning has reordered rows or
reversed a legacy array.
"""

from __future__ import annotations

from typing import Any, Literal, TypeAlias

_MISSING = object()

#: A single scalar fragment that can participate in intake or be returned.
Segment: TypeAlias = Literal[
    'station',
    'code',
    'model',
    'unit',
    'legacy.label',
    'legacy.code',
    'legacy.unit',
    'legacy.note',
]

#: Which view of the data a coordinate points at.
View: TypeAlias = Literal['batch', 'original', 'return']


class Coord:
    """Coordinate of a scalar fragment inside a row.

    Args:
        view: ``'batch'`` for the cleaned batch, ``'original'`` for the raw
            batch and ``'return'`` for the return view.
        row_index: Row number. In ``'original'`` and ``'return'`` views this is
            always the original row number; the ``'batch'`` view uses the
            cleaned (post-sort) row number.
        segment: Which scalar fragment the coordinate names. Legacy fragments
            additionally need ``legacy_index``.
        legacy_index: Original array index inside the row's ``legacy`` list.
            Required for ``legacy.*`` segments, forbidden otherwise.
    """

    __slots__ = ('legacy_index', 'row_index', 'segment', 'view')

    def __init__(
        self,
        view: View,
        row_index: int,
        segment: Segment,
        legacy_index: int | None = None,
    ) -> None:
        is_legacy = segment.startswith('legacy.')
        if is_legacy and legacy_index is None:
            raise ValueError(f'{segment!r} requires a legacy_index')
        if not is_legacy and legacy_index is not None:
            raise ValueError(f'{segment!r} must not carry a legacy_index')
        self.view = view
        self.row_index = row_index
        self.segment = segment
        self.legacy_index = legacy_index

    def key(self) -> str:
        """Render the coordinate as its canonical string key."""
        if self.legacy_index is None:
            return f'{self.view}[{self.row_index}].{self.segment}'
        field = self.segment.split('.', 1)[1]
        return f'{self.view}[{self.row_index}].legacy[{self.legacy_index}].{field}'

    def with_view(self, view: View) -> Coord:
        """Return the same coordinate expressed against another view."""
        return Coord(view, self.row_index, self.segment, self.legacy_index)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Coord):
            return NotImplemented
        return (
            self.view == other.view
            and self.row_index == other.row_index
            and self.segment == other.segment
            and self.legacy_index == other.legacy_index
        )

    def __hash__(self) -> int:
        return hash((self.view, self.row_index, self.segment, self.legacy_index))

    def __repr__(self) -> str:
        return f'Coord({self.key()!r})'


class BatchCoord(Coord):
    """Coordinate of a fragment inside the cleaned batch."""

    def __init__(self, row_index: int, segment: Segment, legacy_index: int | None = None) -> None:
        super().__init__('batch', row_index, segment, legacy_index)


class ReturnCoord(Coord):
    """Coordinate of a fragment inside the original batch or the return view.

    Both views keep the original row numbering and original legacy indices, so
    one coordinate type covers them; use :meth:`in_return` / :meth:`in_original`
    to switch the rendered prefix.
    """

    def __init__(
        self,
        row_index: int,
        segment: Segment,
        legacy_index: int | None = None,
        *,
        view: View = 'return',
    ) -> None:
        if view not in ('original', 'return'):
            raise ValueError('ReturnCoord only supports the original and return views')
        super().__init__(view, row_index, segment, legacy_index)

    def in_return(self) -> ReturnCoord:
        """Return this coordinate rendered with the ``return`` prefix."""
        return ReturnCoord(self.row_index, self.segment, self.legacy_index, view='return')

    def in_original(self) -> ReturnCoord:
        """Return this coordinate rendered with the ``original`` prefix."""
        return ReturnCoord(self.row_index, self.segment, self.legacy_index, view='original')


def coordinate_key(coord: Coord | str) -> str:
    """Return the canonical string key of a coordinate (idempotent on strings)."""
    if isinstance(coord, Coord):
        return coord.key()
    return coord


def parse_coord(text: str) -> Coord:
    """Parse a canonical coordinate key back into a :class:`Coord`.

    Args:
        text: A key previously produced by :meth:`Coord.key`, e.g.
            ``'original[0].legacy[1].unit'``.

    Returns:
        The coordinate object.
    """
    view, remainder = text.split('[', 1)
    if view not in ('batch', 'original', 'return'):
        raise ValueError(f'unknown coordinate view in {text!r}')
    row_text, tail = remainder.split(']', 1)
    path = tail.lstrip('.')
    if path.startswith('legacy['):
        index_text, field_text = path[len('legacy[') :].split(']', 1)
        segment: Segment = f'legacy.{field_text.lstrip(".")}'  # type: ignore[assignment]
        return Coord(view, int(row_text), segment, int(index_text))
    return Coord(view, int(row_text), path)  # type: ignore[arg-type]


def get_value(view_rows: list[dict], coord: Coord | str, default: Any = _MISSING) -> Any:
    """Read the scalar value at ``coord`` from a batch-shaped list of rows.

    The same helper works against the original raw rows, the cleaned batch
    dump or the return view, as long as ``view_rows`` uses the row numbering
    of the coordinate's view. Missing fragments return ``default`` (or raise
    :class:`KeyError` when no default is given), which lets reconciliation
    code distinguish "returned to supplier" from a present-but-falsy value.

    Args:
        view_rows: Rows of the matching view (e.g. the original dictionaries
            for an ``original[...]`` coordinate).
        coord: A :class:`Coord` or its canonical string key.
        default: Value to return when the fragment is absent.

    Returns:
        The scalar value at the coordinate.
    """
    parsed = parse_coord(coord) if isinstance(coord, str) else coord
    if parsed.row_index >= len(view_rows):
        if default is not _MISSING:
            return default
        raise KeyError(coord if isinstance(coord, str) else coord.key())
    row = view_rows[parsed.row_index]
    if parsed.legacy_index is None:
        value: Any = row.get(parsed.segment, default)
        if value is _MISSING:
            raise KeyError(coord if isinstance(coord, str) else coord.key())
        return value
    legacy = row.get('legacy')
    if not isinstance(legacy, list) or parsed.legacy_index >= len(legacy):
        if default is not _MISSING:
            return default
        raise KeyError(coord if isinstance(coord, str) else coord.key())
    field = parsed.segment.split('.', 1)[1]
    value = legacy[parsed.legacy_index].get(field, default)
    if value is _MISSING:
        raise KeyError(coord if isinstance(coord, str) else coord.key())
    return value
