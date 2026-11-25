import pytest

import pyranges as pr
from pyranges import (
    BatchMismatchError,
    PaginatedApplyPair,
    SQLiteWarehouse,
)


def example_tables(reordered=False):
    samples = [
        ("L1", "+", 90, 110, "A1"),
        ("L1", "+", 90, 110, "A2"),
    ]
    permits = [
        ("L1", "+", 0, 200, "P"),
        ("L1", "+", 100, 101, "Q"),
        ("L1", "+", 110, 130, "R"),
        ("L1", "-", 100, 101, "QREV"),
    ]
    if reordered:
        samples = list(reversed(samples))
        permits = [permits[2], permits[0], permits[3], permits[1]]

    columns = ("Chromosome", "Strand", "Start", "End", "stable_id")
    sample_frame = pr.from_dict(
        {
            "Chromosome": [row[0] for row in samples],
            "Strand": [row[1] for row in samples],
            "Start": [row[2] for row in samples],
            "End": [row[3] for row in samples],
            "sample_id": [row[4] for row in samples],
        }
    )
    permit_frame = pr.from_dict(
        {
            "Chromosome": [row[0] for row in permits],
            "Strand": [row[1] for row in permits],
            "Start": [row[2] for row in permits],
            "End": [row[3] for row in permits],
            "permit_id": [row[4] for row in permits],
        }
    )
    return sample_frame, permit_frame


def result_for(key):
    return {"sample_id": key.sample_id, "permit_id": key.permit_id, "stored": True}


def test_narrative_partial_page_then_restart_with_smaller_page(tmp_path):
    index_path = tmp_path / "pairs.index.sqlite3"
    warehouse_path = tmp_path / "warehouse.sqlite3"

    warehouse = SQLiteWarehouse(warehouse_path, batch_id="batch-1")
    runner = PaginatedApplyPair(
        *example_tables(),
        batch_id="batch-1",
        page_size=2,
        warehouse=warehouse,
        index_path=index_path,
    )

    first = runner.next_page()
    assert [tuple(key) for key in first.expected_keys] == [
        ("A1", "P"),
        ("A1", "Q"),
    ]
    receipt = first.confirm([result_for(key) for key in first.expected_keys], sink=warehouse)
    assert [tuple(key) for key in receipt.keys] == [("A1", "P"), ("A1", "Q")]
    runner.last_receipt = receipt

    second = runner.next_page()
    assert [tuple(key) for key in second.expected_keys] == [
        ("A2", "P"),
        ("A2", "Q"),
    ]
    verification = second.verify([result_for(second.expected_keys[0])])
    assert not verification
    assert verification.missing == [second.expected_keys[1]]
    with pytest.raises(Exception):
        second.confirm(sink=warehouse)

    # Simulate a new process: no in-memory runner state is reused.
    restarted_warehouse = SQLiteWarehouse(warehouse_path)
    restarted = PaginatedApplyPair(
        *example_tables(reordered=True),
        batch_id="batch-1",
        page_size=1,
        warehouse=restarted_warehouse,
        index_path=index_path,
    )
    assert restarted.last_receipt.sequence == 1

    delivered = []
    while True:
        page = restarted.next_page()
        if page is None:
            break
        assert len(page) == 1
        page.confirm([result_for(page.expected_keys[0])], sink=restarted_warehouse)
        restarted.last_receipt = page.receipt
        delivered.extend(page.expected_keys)

    report = restarted.complete()
    assert report.completed is True
    assert report.total_pairs == 4
    assert [tuple(key) for key in delivered] == [("A2", "P"), ("A2", "Q")]
    assert restarted_warehouse.count_results("batch-1") == 4
    assert [
        (row["sample_id"], row["permit_id"])
        for row in restarted_warehouse.results("batch-1")
    ] == [
        ("A1", "P"),
        ("A1", "Q"),
        ("A2", "P"),
        ("A2", "Q"),
    ]


def test_json_receipts_reject_other_batch_and_empty_group_completes(tmp_path):
    receipt_path = tmp_path / "receipts.json"
    index_path = tmp_path / "receipts.index.sqlite3"
    samples, permits = example_tables()

    runner = PaginatedApplyPair(
        samples,
        permits,
        batch_id="json-batch",
        page_size=2,
        receipt_path=receipt_path,
        index_path=index_path,
    )
    report = runner.run(lambda page: [result_for(key) for key in page.expected_keys])
    assert report.completed is True
    assert report.total_pairs == 4

    with pytest.raises(BatchMismatchError):
        PaginatedApplyPair(
            samples,
            permits,
            batch_id="other-batch",
            receipt_path=receipt_path,
            index_path=index_path,
        )

    empty_receipt = tmp_path / "empty.json"
    empty_index = tmp_path / "empty.index.sqlite3"
    empty_samples = pr.from_dict(
        {"Chromosome": ["L1"], "Strand": ["+"], "Start": [300], "End": [301], "sample_id": ["S"]}
    )
    report = PaginatedApplyPair(
        empty_samples,
        permits,
        batch_id="empty",
        receipt_path=empty_receipt,
        index_path=empty_index,
    ).run(lambda page: [])
    assert report.completed is True
    assert report.total_pairs == 0

    restarted = PaginatedApplyPair(
        empty_samples,
        permits,
        batch_id="empty",
        receipt_path=empty_receipt,
        index_path=empty_index,
    )
    assert restarted.completed is True
    assert restarted.next_page() is None


def test_pages_are_bounded_without_materializing_all_matches(tmp_path):
    n = 5000
    samples = pr.from_dict(
        {"Chromosome": ["L"], "Strand": ["+"], "Start": [50], "End": [60], "sample_id": ["S"]}
    )
    permits = pr.from_dict(
        {
            "Chromosome": ["L"] * n,
            "Strand": ["+"] * n,
            "Start": [0] * n,
            "End": [100] * n,
            "permit_id": [f"P{i:05d}" for i in range(n)],
        }
    )
    runner = PaginatedApplyPair(
        samples,
        permits,
        batch_id="many",
        page_size=37,
        index_path=tmp_path / "many.sqlite3",
    )

    seen = 0
    pages = 0
    while True:
        page = runner.next_page()
        if page is None:
            break
        assert len(page) <= 37
        seen += len(page)
        pages += 1
        page.confirm([None] * len(page))
        runner.last_receipt = page.receipt
    assert seen == n
    assert pages == (n + 36) // 37
