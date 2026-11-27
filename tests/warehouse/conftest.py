from __future__ import annotations

import pytest


@pytest.fixture
def raw_batch() -> list[dict]:
    """The two-row arrival batch: B first (original row 0), A second (row 1)."""
    return [
        {
            'station': 'B',
            'code': 'B-007',
            'legacy': [
                {'label': '007', 'code': 'OLD-B0', 'unit': 'box', 'note': 'b-note-0'},
                {'label': '007', 'code': 'OLD-B1', 'unit': 'piece', 'note': 'b-note-1'},
            ],
        },
        {
            'station': 'A',
            'code': 'A-201',
            'legacy': [
                {'label': '010', 'code': 'OLD-A0', 'unit': 'crate', 'note': 'a-note-0'},
                {'label': '011', 'code': 'OLD-A1', 'unit': 'pallet', 'note': 'a-note-1'},
            ],
        },
    ]
