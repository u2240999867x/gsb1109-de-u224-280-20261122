"""Pydantic cleaning of arrival batches and the coordinate receipt.

Cleaning happens in two stages:

1. **Batch-level pre-cleaning** (in :func:`clean_batch`): rows are sorted by
   ``station`` and the ``legacy`` array of ``B`` station rows is reversed.
   Every relocation is recorded on the :class:`ReceiptBuilder` as a ``move``
   entry.
2. **Row-level pre-cleaning** (the ``model_validator(mode='before')`` on
   :class:`InboundRow`): a ``B`` row overwrites its top-level ``code`` with
   ``"<station>-<legacy[-1].label>"``; other rows keep the gateway ``code``.
   The intake ``model`` (top-level ``code``, falling back to
   ``legacy[1].code``) and ``unit`` (``legacy[-1].unit``) selections are
   recorded as ``generated`` entries naming every source fragment.

The cleaned batch is a :class:`Batch` (a :class:`~pydantic.RootModel` of
:class:`InboundRow`); the :class:`Receipt` is also a ``RootModel`` holding
plain string coordinates, so it round-trips through JSON unchanged.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, RootModel, ValidationInfo, computed_field, model_validator
from warehouse.coordinates import BatchCoord, Coord, ReturnCoord

#: Origin row index marker attached to a row while it travels through validators.
_ORIGIN_KEY = '_warehouse_origin_index'
#: Cleaned row index marker attached to a row while it travels through validators.
_CLEANED_KEY = '_warehouse_cleaned_index'
#: Mapping of cleaned legacy index -> original legacy index.
_LEGACY_ORIGIN_KEY = '_warehouse_legacy_origin_indices'

_LEGACY_FIELDS: tuple[str, ...] = ('label', 'code', 'unit', 'note')


class LegacyItem(BaseModel):
    """One entry of a row's ``legacy`` array."""

    label: str
    code: str | None = None
    unit: str | None = None
    note: str | None = None


class InboundRow(BaseModel):
    """One cleaned, inbound row.

    ``model`` and ``unit`` are the fields selected for intake; ``station``,
    ``code`` and ``legacy`` retain the cleaned pre-intake values behind them.
    """

    station: str
    code: str | None = None
    legacy: list[LegacyItem]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def model(self) -> str:
        """Intake model: the cleaned top-level ``code``, else ``legacy[1].code``."""
        if self.code is not None:
            return self.code
        return self.legacy[1].code  # type: ignore[return-value]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def unit(self) -> str:
        """Intake unit: the unit of the last cleaned legacy item."""
        return self.legacy[-1].unit  # type: ignore[return-value]

    @model_validator(mode='before')
    @classmethod
    def _pre_clean_row(cls, data: object, info: ValidationInfo) -> object:
        if not isinstance(data, dict) or _ORIGIN_KEY not in data:
            return data
        builder: ReceiptBuilder = info.context
        origin = data.pop(_ORIGIN_KEY)
        cleaned_index = data.pop(_CLEANED_KEY)
        legacy_origin_indices: list[int] = data.pop(_LEGACY_ORIGIN_KEY)

        if data['station'] == 'B':
            tail_origin = legacy_origin_indices[-1]
            data['code'] = f'{data["station"]}-{data["legacy"][-1]["label"]}'
            model_sources = (
                ReturnCoord(origin, 'station', view='original'),
                ReturnCoord(origin, 'legacy.label', tail_origin, view='original'),
            )
        elif data.get('code') is not None:
            model_sources = (ReturnCoord(origin, 'code', view='original'),)
        else:
            fallback_origin = legacy_origin_indices[1]
            model_sources = (ReturnCoord(origin, 'legacy.code', fallback_origin, view='original'),)
        builder.record_generated(BatchCoord(cleaned_index, 'model'), *model_sources)

        tail_origin = legacy_origin_indices[-1]
        builder.record_generated(
            BatchCoord(cleaned_index, 'unit'),
            ReturnCoord(origin, 'legacy.unit', tail_origin, view='original'),
        )
        return data


class Batch(RootModel[list[InboundRow]]):
    """The cleaned arrival batch, in cleaned row order."""


@dataclass(frozen=True)
class _RecordedEntry:
    kind: Literal['move', 'generated']
    target: Coord
    sources: tuple[Coord, ...]


class ReceiptBuilder:
    """Mutable accumulator passed to Pydantic through the validation context."""

    def __init__(self) -> None:
        self._entries: list[_RecordedEntry] = []

    def record_move(self, target: BatchCoord, source: ReturnCoord) -> None:
        """Record that an unchanged fragment moved from ``source`` to ``target``."""
        self._entries.append(_RecordedEntry('move', target, (source,)))

    def record_generated(self, target: BatchCoord, *sources: ReturnCoord) -> None:
        """Record that ``target`` was selected/computed using ``sources``."""
        self._entries.append(_RecordedEntry('generated', target, tuple(sources)))

    def build(self) -> Receipt:
        """Freeze the accumulated entries into a validated :class:`Receipt`."""
        return Receipt.model_validate(
            [
                {
                    'kind': entry.kind,
                    'target': entry.target.key(),
                    'sources': [source.key() for source in entry.sources],
                }
                for entry in self._entries
            ]
        )


class ReceiptEntry(BaseModel):
    """One receipt line: a cleaned fragment and the coordinates it came from."""

    kind: Literal['move', 'generated']
    target: str
    sources: list[str]


class Receipt(RootModel[list[ReceiptEntry]]):
    """Coordinate receipt produced while cleaning a batch.

    A ``move`` entry says a fragment kept its value but changed position
    (a row sort or a legacy reversal); a ``generated`` entry says an intake
    field was selected or computed and names every source fragment used.
    """

    def entries_for(self, target: str) -> list[ReceiptEntry]:
        """Return all receipt entries whose ``target`` equals ``target``."""
        return [entry for entry in self.root if entry.target == target]

    def source_keys(self, target: str) -> list[str]:
        """Return every original/return coordinate backing ``target``."""
        return [source for entry in self.entries_for(target) for source in entry.sources]

    def locate(self, target: str) -> list[ReceiptEntry]:
        """Alias for :meth:`entries_for` used by intake-result receivers."""
        return self.entries_for(target)


def _present_legacy_fields(item: object) -> tuple[str, ...]:
    if not isinstance(item, dict):
        return ()
    return tuple(field for field in _LEGACY_FIELDS if field in item)


def clean_batch(raw_rows: list[dict]) -> tuple[Batch, Receipt]:
    """Clean a raw arrival batch and return both the model and its receipt.

    The input is deep-copied first, so the caller's original dictionaries stay
    untouched and can keep being used for reconciliation.

    Batch-level pre-cleaning sorts the rows by ``station`` and reverses the
    ``legacy`` array of ``B`` station rows; row-level pre-cleaning and the
    intake field selection then run inside the Pydantic validators.

    Args:
        raw_rows: Raw gateway rows, each with ``station``, optional ``code``
            and a ``legacy`` array.

    Returns:
        A ``(batch, receipt)`` tuple. ``batch`` is the cleaned
        :class:`Batch` model and ``receipt`` is the coordinate :class:`Receipt`.
    """
    rows = copy.deepcopy(raw_rows)
    builder = ReceiptBuilder()

    ordered = sorted(enumerate(rows), key=lambda pair: pair[1]['station'])
    cleaned_rows: list[dict] = []
    for cleaned_index, (origin, row) in enumerate(ordered):
        builder.record_move(
            BatchCoord(cleaned_index, 'station'),
            ReturnCoord(origin, 'station', view='original'),
        )

        legacy = row.get('legacy')
        legacy_origin_indices = list(range(len(legacy))) if isinstance(legacy, list) else []
        if row['station'] == 'B' and isinstance(legacy, list) and legacy:
            reordered = list(reversed(legacy))
            row['legacy'] = reordered
            for new_index, item in enumerate(reordered):
                old_index = len(reordered) - 1 - new_index
                legacy_origin_indices[new_index] = old_index
                for field in _present_legacy_fields(item):
                    builder.record_move(
                        BatchCoord(cleaned_index, f'legacy.{field}', new_index),
                        ReturnCoord(origin, f'legacy.{field}', old_index, view='original'),
                    )

        row[_ORIGIN_KEY] = origin
        row[_CLEANED_KEY] = cleaned_index
        row[_LEGACY_ORIGIN_KEY] = legacy_origin_indices
        cleaned_rows.append(row)

    batch = Batch.model_validate(cleaned_rows, context=builder)
    return batch, builder.build()


def clean_from_dicts(raw_rows: list[dict]) -> tuple[Batch, Receipt]:
    """Dictionary-list gateway entry point."""
    return clean_batch(raw_rows)


def clean_from_json(payload: str | bytes) -> tuple[Batch, Receipt]:
    """JSON gateway entry point: parse the payload then clean it."""
    return clean_batch(json.loads(payload))


def clean_from_json_file(path: str | Path) -> tuple[Batch, Receipt]:
    """JSON-file gateway entry point."""
    with Path(path).open(encoding='utf-8') as handle:
        return clean_from_json(handle.read())
