import sqlite3
from typing import Any, Dict, List, Mapping, Optional

import pytest

from databases import Database
from databases.backends.reconcile import (
    BatchConflictError,
    DocumentIntake,
    Reconciliation,
    RunNotFoundError,
    unchanged,
    written,
)

pytestmark = pytest.mark.asyncio

SOURCE = "north"

BUSINESS_DDL = [
    """
    CREATE TABLE business_rows (
        id INTEGER PRIMARY KEY,
        business_key TEXT NOT NULL UNIQUE,
        version INTEGER NOT NULL,
        content TEXT NOT NULL CHECK (content <> 'Y')
    )
    """,
    """
    CREATE TABLE ledger_position (
        source TEXT PRIMARY KEY,
        last_seq INTEGER NOT NULL
    )
    """,
]


def document(
    source_seq: int,
    business_key: str = "X",
    version: Optional[int] = None,
    content: Optional[str] = None,
    source: str = SOURCE,
) -> Dict[str, Any]:
    if version is None:
        version = source_seq
    if content is None:
        content = business_key
    return {
        "source": source,
        "source_seq": source_seq,
        "business_key": business_key,
        "version": version,
        "content": content,
    }


async def make_databases(tmp_path, *, connect: bool = True):
    intake_url = f"sqlite:///{tmp_path}/intake.db"
    business_url = f"sqlite:///{tmp_path}/business.db"
    intake_db = Database(intake_url)
    business_db = Database(business_url)
    if connect:
        await intake_db.connect()
        await business_db.connect()
        for statement in BUSINESS_DDL:
            await business_db.execute(statement)
        await business_db.execute(
            "INSERT INTO business_rows (id, business_key, version, content) "
                "VALUES (7, 'X', 3, 'X-3')"
        )
        await business_db.execute(
            "INSERT INTO ledger_position (source, last_seq) "
                "VALUES (:source, 39)",
            {"source": SOURCE},
        )
    return intake_db, business_db


async def apply_document(connection, document: Mapping[str, Any]):
    await connection.execute(
        "UPDATE ledger_position SET last_seq = :source_seq "
            "WHERE source = :source",
        {
            "source_seq": document["source_seq"],
            "source": document["source"],
        },
    )
    row = await connection.fetch_one(
        "SELECT id, version FROM business_rows "
            "WHERE business_key = :business_key",
        {"business_key": document["business_key"]},
    )
    if row is not None and document["version"] <= row["version"]:
        return unchanged()
    if row is None:
        row_id = await connection.execute(
            "INSERT INTO business_rows (business_key, version, content) "
                "VALUES (:business_key, :version, :content)",
            {
                "business_key": document["business_key"],
                "version": document["version"],
                "content": document["content"],
            },
        )
    else:
        row_id = row["id"]
        await connection.execute(
            "UPDATE business_rows SET version = :version, content = :content "
                "WHERE id = :id",
            {
                "version": document["version"],
                "content": document["content"],
                "id": row_id,
            },
        )
    return written(row_id, document["version"])


def receipt_index(receipts: List[Any]) -> Dict[int, Any]:
    return {receipt.source_seq: receipt for receipt in receipts}


async def business_state(business_db: Database):
    row = await business_db.fetch_one(
        "SELECT id, version FROM business_rows WHERE business_key = 'X'"
    )
    position = await business_db.fetch_val(
        "SELECT last_seq FROM ledger_position WHERE source = :source",
        {"source": SOURCE},
    )
    return row["id"], row["version"], position


async def test_rollback_then_retry_reuses_original_range_and_receipts(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)

    result = await intake.receive_many([document(42, version=6, content="X-6")])
    assert result.accepted_count == 1
    await intake.receive_many(
        [
            document(40, version=4, content="X-4"),
            document(41, business_key="Y", version=1, content="Y"),
        ]
    )

    reconciliation = Reconciliation(intake_db, business_db)
    end_seq = await reconciliation.open_run("A", 40, source=SOURCE)
    assert end_seq == 42

    async with business_db.transaction(force_rollback=True):
        receipts = await reconciliation.process_run(
            "A", source=SOURCE, apply=apply_document
        )
        assert [receipt.source_seq for receipt in receipts] == [40, 41, 42]
        pending = receipt_index(receipts)
        assert pending[40].status == "written"
        assert pending[41].status == "rejected"
        assert pending[41].reason
        assert pending[42].status == "written"

        # The caller's uncommitted transaction is not visible to other connections.
        assert await reconciliation.get_receipt(SOURCE, 40) is None
        run = await reconciliation.get_run("A", source=SOURCE)
        assert run["status"] == "open"

        # Intake keeps working while the business transaction stays open.
        await intake.receive_many([document(43, version=8, content="X-8")])

    # A resupply that re-sends an already received sequence with different
    # content must reject the whole batch, including the brand new sequence.
    with pytest.raises(BatchConflictError) as exc_info:
        await intake.receive_many(
            [
                document(44, version=9, content="X-9"),
                document(42, version=7, content="X-7"),
            ]
        )
    assert exc_info.value.item_count == 2
    assert await intake.get_document(SOURCE, 43) is not None
    assert await intake.get_document(SOURCE, 44) is None
    assert (await intake.get_document(SOURCE, 42))["version"] == 6

    # Reconnect with fresh handles on the same files.
    await intake_db.disconnect()
    await business_db.disconnect()
    intake_db, business_db = await make_databases(tmp_path, connect=False)
    await intake_db.connect()
    await business_db.connect()
    intake = DocumentIntake(intake_db)
    reconciliation = Reconciliation(intake_db, business_db)

    row_id, version, position = await business_state(business_db)
    assert (row_id, version, position) == (7, 3, 39)
    run = await reconciliation.get_run("A", source=SOURCE)
    assert run["status"] == "open"
    assert await reconciliation.get_receipts("A", source=SOURCE) == []

    # Run B takes the new upper bound and commits.
    assert await reconciliation.open_run("B", 40, source=SOURCE) == 43
    async with business_db.transaction():
        receipts_b = await reconciliation.process_run(
            "B", source=SOURCE, apply=apply_document
        )

    by_seq = receipt_index(receipts_b)
    assert list(by_seq) == [40, 41, 42, 43]
    assert (by_seq[40].status, by_seq[40].row_id, by_seq[40].version) == (
        "written",
        7,
        4,
    )
    assert by_seq[41].status == "rejected"
    assert "CHECK" in by_seq[41].reason
    assert (by_seq[42].status, by_seq[42].row_id, by_seq[42].version) == (
        "written",
        7,
        6,
    )
    assert (by_seq[43].status, by_seq[43].row_id, by_seq[43].version) == (
        "written",
        7,
        8,
    )
    assert await business_state(business_db) == (7, 8, 43)

    # Retrying A reuses its original range and the existing receipts.
    assert await reconciliation.open_run("A", 40, source=SOURCE) == 42
    async with business_db.transaction():
        receipts_a = await reconciliation.process_run(
            "A", source=SOURCE, apply=apply_document
        )
    assert [receipt.source_seq for receipt in receipts_a] == [40, 41, 42]
    assert all(receipt.run_no == "B" for receipt in receipts_a)
    run_a = await reconciliation.get_run("A", source=SOURCE)
    assert run_a["status"] == "completed"
    assert await business_state(business_db) == (7, 8, 43)


async def test_committed_run_is_visible_and_follow_up_only_processes_new_sequences(
    tmp_path,
):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)
    await intake.receive_many(
        [
            document(40, version=4, content="X-4"),
            document(41, business_key="Y", version=1, content="Y"),
            document(42, version=6, content="X-6"),
        ]
    )
    reconciliation = Reconciliation(intake_db, business_db)
    assert await reconciliation.open_run("A", 40, source=SOURCE) == 42

    async with business_db.transaction():
        receipts_a = await reconciliation.process_run(
            "A", source=SOURCE, apply=apply_document
        )

    assert await reconciliation.get_run("A", source=SOURCE)
    committed = receipt_index(
        await reconciliation.get_receipts("A", source=SOURCE)
    )
    assert committed[40].status == "written"
    assert committed[41].status == "rejected"
    assert committed[42].status == "written"
    assert await business_state(business_db) == (7, 6, 42)

    await intake.receive_many([document(43, version=8, content="X-8")])
    assert await reconciliation.open_run("B", 40, source=SOURCE) == 43
    async with business_db.transaction():
        receipts_b = await reconciliation.process_run(
            "B", source=SOURCE, apply=apply_document
        )

    by_seq = receipt_index(receipts_b)
    assert [receipt.run_no for receipt in receipts_b] == ["A", "A", "A", "B"]
    assert (by_seq[43].status, by_seq[43].row_id, by_seq[43].version) == (
        "written",
        7,
        8,
    )
    assert await business_state(business_db) == (7, 8, 43)


async def test_identical_resend_is_accepted_once(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)
    doc = document(50, version=2, content="same")

    first = await intake.receive_many([doc])
    assert first.accepted_count == 1
    second = await intake.receive_many([dict(doc), dict(doc)])
    assert second.item_count == 2
    assert second.accepted_count == 1

    stored = await intake.get_document(SOURCE, 50)
    assert stored["version"] == 2
    assert stored["content"] == "same"


async def test_conflict_inside_one_batch_rejects_everything(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)

    with pytest.raises(BatchConflictError):
        await intake.receive_many(
            [
                document(60, version=1, content="one"),
                document(60, version=2, content="two"),
            ]
        )
    assert await intake.get_document(SOURCE, 60) is None


async def test_missing_sequence_stops_the_contiguous_range(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)
    await intake.receive_many([document(70), document(72)])

    reconciliation = Reconciliation(intake_db, business_db)
    assert await reconciliation.open_run("R1", 70, source=SOURCE) == 70
    assert await reconciliation.open_run("R2", 71, source=SOURCE) == 70

    await intake.receive_many([document(71)])
    assert await reconciliation.open_run("R3", 71, source=SOURCE) == 72


async def test_unchanged_receipt_when_version_does_not_advance(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)
    await intake.receive_many([document(80, version=3, content="X-3")])

    reconciliation = Reconciliation(intake_db, business_db)
    assert await reconciliation.open_run("R", 80, source=SOURCE) == 80
    async with business_db.transaction():
        receipts = await reconciliation.process_run(
            "R", source=SOURCE, apply=apply_document
        )

    assert receipts[0].status == "unchanged"
    assert receipts[0].row_id is None
    assert receipts[0].version is None
    assert await business_state(business_db) == (7, 3, 80)


async def test_received_documents_survive_reconnect(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)
    await intake.receive_many([document(90, version=5, content="durable")])
    await intake_db.disconnect()

    intake_db = Database(f"sqlite:///{tmp_path}/intake.db")
    await intake_db.connect()
    intake = DocumentIntake(intake_db)
    stored = await intake.get_document(SOURCE, 90)
    assert stored["business_key"] == "X"
    assert stored["version"] == 5


async def test_streaming_batch_and_paged_range_scan(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    intake = DocumentIntake(intake_db)

    async def stream(count: int):
        for source_seq in range(1, count + 1):
            yield document(source_seq, version=1)

    result = await intake.receive_batch(stream(1000))
    assert result.item_count == 1000
    assert result.accepted_count == 1000

    await intake.receive_many([document(1002, version=1)])
    reconciliation = Reconciliation(intake_db, business_db)
    assert await reconciliation.open_run("big", 1, source=SOURCE) == 1000
    assert await reconciliation.open_run("gap", 1001, source=SOURCE) == 1000

    await intake.receive_many([document(1001, version=1)])
    assert await reconciliation.open_run("filled", 1001, source=SOURCE) == 1002


async def test_processing_unknown_run_raises(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    reconciliation = Reconciliation(intake_db, business_db)
    with pytest.raises(RunNotFoundError):
        await reconciliation.process_run(
            "missing", source=SOURCE, apply=apply_document
        )


async def test_execute_many_still_works(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    await business_db.execute_many(
        "INSERT INTO ledger_position (source, last_seq) "
            "VALUES (:source, :last_seq)",
        [
            {"source": "a", "last_seq": 1},
            {"source": "b", "last_seq": 2},
        ],
    )
    rows = await business_db.fetch_all(
        "SELECT source, last_seq FROM ledger_position ORDER BY source"
    )
    assert [(row["source"], row["last_seq"]) for row in rows] == [
        ("a", 1),
        ("b", 2),
        ("north", 39),
    ]


async def test_check_failure_is_sqlite_integrity_error(tmp_path):
    intake_db, business_db = await make_databases(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        await business_db.execute(
            "INSERT INTO business_rows (business_key, version, content) "
                "VALUES ('Z', 1, 'Y')"
        )
