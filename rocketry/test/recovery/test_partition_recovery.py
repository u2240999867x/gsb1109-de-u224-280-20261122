"""Behavioural tests for partition aware Retry recovery.

These tests use the recovery model directly (no scheduler loop) because the
recovery machinery is intentionally independent of an active scheduler.  Each
test mirrors one step of the nightly partition collection incident.
"""
import logging

import pytest

from rocketry import Session
from rocketry.conditions import Retry
from rocketry.recovery import (
    BackfillLedger,
    FileStateStore,
    MemoryStateStore,
    PartitionRecovery,
    Receipt,
    RecoveryRecord,
)

NORTH = "north"
SOUTH = "south"


def receipt(partition, batch, run_id, action, seq, position=None):
    return Receipt(
        partition=partition,
        batch=batch,
        run_id=run_id,
        action=action,
        source_seq=seq,
        log_position=position if position is not None else seq,
    )


def make_recovery(store=None):
    recovery = PartitionRecovery(store=store)
    recovery.declare_batch(NORTH, "A")
    recovery.declare_batch(SOUTH, "B")
    return recovery


def initial_record(recovery):
    """First session: A succeeded (start in flight), B only started."""
    recovery.process_receipts([
        receipt(NORTH, "A", "run-a", "success", 10),
        receipt(SOUTH, "B", "run-b", "start", 11),
    ])
    recovery.advance_cursor(position=11, source_seq=11)
    recovery.save()
    return recovery


def assert_first_recovery(recovery):
    assert recovery.status(NORTH, "run-a") == "success"
    assert recovery.status(SOUTH, "run-b") == "pending"
    assert recovery.pending() == [recovery.get_run(SOUTH, "run-b")]
    assert recovery.failed() == []
    assert recovery.eligible() == []


def test_first_recovery_pending_without_backfill():
    recovery = initial_record(make_recovery())
    assert_first_recovery(recovery)


def test_recovery_record_round_trip():
    store = MemoryStateStore()
    recovery = initial_record(make_recovery(store))

    restarted = PartitionRecovery(store=store)
    restarted.load()
    assert_first_recovery(restarted)
    assert restarted.current_batch(NORTH) == "A"
    assert restarted.current_batch(SOUTH) == "B"
    assert restarted.position == 11
    assert restarted.source_seq == 11


def test_continuation_after_rotation_backfills_only_current_failure():
    recovery = initial_record(make_recovery())

    continuation = [
        # Late start and duplicate success for north batch A.
        receipt(NORTH, "A", "run-a", "start", 12, position=12),
        receipt(NORTH, "A", "run-a", "success", 13, position=13),
        # South batch B confirms failure.
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
        # North's previous batch failed but is no longer current.
        receipt(NORTH, "A-old", "run-a-old", "fail", 15, position=15),
    ]
    consumed = recovery.consume_log(continuation)
    assert len(consumed) == 4

    assert recovery.status(NORTH, "run-a") == "success"
    assert recovery.get_run(NORTH, "run-a").start_receipt is not None
    assert recovery.status(SOUTH, "run-b") == "failed"
    assert recovery.status(NORTH, "run-a-old") == "failed"

    # Only B of the current batch is eligible.
    assert [run.run_id for run in recovery.eligible()] == ["run-b"]
    assert recovery.can_backfill(NORTH, "run-a-old") is False

    entries = recovery.dispatch_failed()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.run_id == "run-b"
    assert entry.partition == SOUTH
    assert entry.success_receipt.original_run_id == "run-b"
    assert recovery.status(SOUTH, "run-b") == "backfill_success"

    # North's old failure is only available in history.
    assert recovery.status(NORTH, "run-a-old") == "failed"
    assert recovery.get_run(NORTH, "run-a-old").archived is True
    assert len(recovery.history(NORTH)) == 1
    assert recovery.ledger.get_by_run(NORTH, "A-old", "run-a-old") is None


def test_log_cursor_reads_only_continuation():
    recovery = initial_record(make_recovery())
    old_records = [
        receipt(NORTH, "A", "run-a", "success", 10, position=10),
        receipt(SOUTH, "B", "run-b", "start", 11, position=11),
    ]
    new_records = [
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ]
    # Replaying the old segment must not change anything.
    assert recovery.read_continuation(old_records + new_records) == new_records
    recovery.consume_log(old_records + new_records)
    assert recovery.position == 14
    assert recovery.status(SOUTH, "run-b") == "failed"


def test_duplicate_receipts_are_ignored():
    recovery = initial_record(make_recovery())
    before = len(recovery.get_run(NORTH, "run-a").receipts)
    recovery.process_receipts([
        receipt(NORTH, "A", "run-a", "success", 10, position=10),
        receipt(NORTH, "A", "run-a", "start", 12, position=12),
    ])
    run = recovery.get_run(NORTH, "run-a")
    assert len(run.receipts) == before + 1
    assert run.status == "success"


def test_new_batch_archives_old_runs():
    recovery = initial_record(make_recovery())
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    recovery.declare_batch(SOUTH, "C")
    assert recovery.current_batch(SOUTH) == "C"
    assert recovery.status(SOUTH, "run-b") == "failed"
    assert recovery.get_run(SOUTH, "run-b").archived is True
    assert recovery.can_backfill(SOUTH, "run-b") is False
    assert recovery.eligible(SOUTH) == []


def test_backfill_ledger_is_idempotent():
    ledger = BackfillLedger()
    first = ledger.register(
        transaction_id="biz-1", partition=SOUTH, batch="B", run_id="run-b"
    )
    second = ledger.register(
        transaction_id="biz-1", partition=SOUTH, batch="B", run_id="run-b"
    )
    assert first.transaction_id == second.transaction_id
    assert len(ledger) == 1

    # A different business id for the same run still points to the original.
    third = ledger.register(
        transaction_id="biz-2", partition=SOUTH, batch="B", run_id="run-b"
    )
    assert third.transaction_id == "biz-1"
    assert len(ledger) == 1

    receipt_1 = ledger.insert("biz-1")
    receipt_2 = ledger.insert("biz-1")
    assert receipt_1.receipt_id == receipt_2.receipt_id
    assert receipt_1.original_run_id == "run-b"
    assert len(ledger) == 1


def test_kill_after_registration_before_work():
    store = MemoryStateStore()
    recovery = initial_record(make_recovery(store))
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    entry = recovery.register_backfill(SOUTH, "run-b", transaction_id="biz-b")
    assert entry.status == "registered"
    assert entry.success_receipt is None

    # "Restart": a brand-new manager reading the same store, no old workers.
    restarted = PartitionRecovery(store=store)
    restarted.load()
    assert restarted.status(SOUTH, "run-b") == "backfill_pending"

    # The failure is re-delivered by the log.
    restarted.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
        receipt(SOUTH, "B", "run-b", "fail", 16, position=16),
    ])
    assert restarted.status(SOUTH, "run-b") == "backfill_pending"
    assert restarted.ledger.get_by_transaction("biz-b").success_receipt is None

    result = restarted.resume()
    assert len(result["started"]) == 1
    entry = restarted.ledger.get_by_transaction("biz-b")
    assert entry.success_receipt is not None
    assert entry.success_receipt.original_run_id == "run-b"
    assert restarted.status(SOUTH, "run-b") == "backfill_success"
    assert len(restarted.ledger) == 1

    # Later scheduling rounds must not create a second transaction.
    assert restarted.dispatch_failed() == []
    assert restarted.ledger.get_by_run(SOUTH, "B", "run-b").transaction_id == "biz-b"


def test_kill_after_insert_before_task_log_receipt():
    store = MemoryStateStore()
    recovery = initial_record(make_recovery(store))
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    entry = recovery.register_backfill(SOUTH, "run-b", transaction_id="biz-b")
    inserted = recovery.insert_backfill("biz-b")
    assert inserted.original_run_id == "run-b"
    assert recovery.ledger.get_by_transaction("biz-b").receipt_logged is False

    restarted = PartitionRecovery(store=store)
    restarted.load()
    assert restarted.status(SOUTH, "run-b") == "backfill_pending"

    # Re-delivered original failure does not trigger a second insertion.
    restarted.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 16, position=16),
    ])
    assert len(restarted.ledger) == 1

    written = []
    result = restarted.resume(writer=written.append)
    assert len(result["written"]) == 1
    assert written[0].original_run_id == "run-b"
    assert restarted.ledger.get_by_transaction("biz-b").receipt_logged is True
    assert restarted.status(SOUTH, "run-b") == "backfill_success"
    assert len(restarted.ledger) == 1


def test_retry_with_recovery_dispatches_once(session):
    store = MemoryStateStore()
    recovery = initial_record(make_recovery(store))
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])

    task = session.create_task(func=lambda: None, name="collect", execution="main")
    cond = Retry(recovery=recovery)
    assert cond.observe(task=task) is True
    assert recovery.status(SOUTH, "run-b") == "backfill_success"
    # The condition itself asks for a retry; the actual recovery has already
    # been accounted for hence later observations are False.
    assert cond.observe(task=task) is False
    assert len(recovery.ledger) == 1


def test_retry_without_recovery_is_unchanged(mock_datetime_now, session):
    from rocketry.testing.log import create_task_record

    session.config.force_status_from_logs = True
    task = session.create_task(func=lambda: None, name="collect", execution="main")
    for created, action in [
        ("2020-01-01 07:10", "run"),
        ("2020-01-01 07:15", "fail"),
    ]:
        task.logger.handle(create_task_record(
            task_name="collect", action=action, created=created, msg="x", args=(), exc_info=None
        ))
    mock_datetime_now("2020-01-01 07:30")
    assert Retry(1).observe(task=task) is True
    assert Retry(0).observe(task=task) is False


def test_old_batch_cannot_start_new_backfill():
    from rocketry.recovery import BackfillNotAllowed

    recovery = initial_record(make_recovery())
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    recovery.declare_batch(SOUTH, "C")
    with pytest.raises(BackfillNotAllowed):
        recovery.register_backfill(SOUTH, "run-b")
    assert recovery.ledger.get_by_run(SOUTH, "B", "run-b") is None

    # An old batch may still receive a late result for audit purposes.
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "success", 20, position=20),
    ])
    run = recovery.get_run(SOUTH, "run-b")
    assert run.status == "success"
    assert run.archived is True


def test_restart_resumes_pending_work_via_retry(session):
    store = MemoryStateStore()
    recovery = initial_record(make_recovery(store))
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    recovery.register_backfill(SOUTH, "run-b", transaction_id="biz-b")

    task = session.create_task(func=lambda: None, name="collect", execution="main")
    restarted = PartitionRecovery(store=store, task=task, session=session)
    cond = Retry(recovery=restarted)
    assert cond.observe(task=task) is True
    entry = restarted.ledger.get_by_transaction("biz-b")
    assert entry.receipt_logged is True
    assert restarted.status(SOUTH, "run-b") == "backfill_success"
    assert cond.observe(task=task) is False
    assert len(restarted.ledger) == 1


def test_file_store_survives_new_manager(tmpdir):
    path = tmpdir.join("recovery.json")
    recovery = initial_record(make_recovery(store=None))
    recovery.save_to(str(path))

    restarted = PartitionRecovery.load_from(str(path))
    assert_first_recovery(restarted)
    assert restarted.current_batch(NORTH) == "A"
    assert restarted.source_seq == 11

    restarted.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    restarted.insert_backfill(partition=SOUTH, batch="B", run_id="run-b",
                              transaction_id="biz-b")
    first_receipt = restarted.ledger.get_by_transaction("biz-b").success_receipt

    again = PartitionRecovery.load_from(str(path))
    again.load()
    second_receipt = again.insert_backfill("biz-b")
    assert second_receipt.receipt_id == first_receipt.receipt_id
    assert second_receipt.transaction_id == first_receipt.transaction_id == "biz-b"
    assert again.status(SOUTH, "run-b") == "backfill_pending"

    written = []
    again.resume(writer=written.append)
    assert written[0].receipt_id == first_receipt.receipt_id
    assert again.status(SOUTH, "run-b") == "backfill_success"
    assert len(again.ledger) == 1


def test_insert_is_idempotent_by_business_id():
    ledger = BackfillLedger()
    first = ledger.insert(
        partition=SOUTH, batch="B", run_id="run-b", transaction_id="biz-b"
    )
    second = ledger.insert(
        partition=SOUTH, batch="B", run_id="run-b", transaction_id="biz-b"
    )
    assert first.receipt_id == second.receipt_id
    assert first.original_run_id == "run-b"
    assert second.transaction_id == "biz-b"
    assert len(ledger) == 1


def test_status_aliases():
    recovery = initial_record(make_recovery())
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    run = recovery.get_run(SOUTH, "run-b")
    assert run.status == "failed"
    assert run.status == "fail"
    recovery.dispatch_failed()
    run = recovery.get_run(SOUTH, "run-b")
    assert run.status == "success"
    assert run.status == "backfill_success"
    assert run.status == "backfilled"
    # The original failure receipt is retained for auditing.
    assert run.end_receipt.action == "fail"
    assert run.backfill_receipt.action == "success"


def test_duplicate_backfill_receipt_does_not_overwrite_failure():
    recovery = initial_record(make_recovery())
    recovery.process_receipts([
        receipt(SOUTH, "B", "run-b", "fail", 14, position=14),
    ])
    entry = recovery.register_backfill(SOUTH, "run-b", transaction_id="biz-b")
    receipt_1 = recovery.insert_backfill("biz-b")
    receipt_2 = recovery.insert_backfill("biz-b")
    assert receipt_1.receipt_id == receipt_2.receipt_id

    # Delivering the same backfill success as a log record is idempotent.
    recovery.process_receipt(Receipt(
        partition=SOUTH, batch="B", run_id="run-b", action="success",
        source_seq=99, original_run_id="run-b", transaction_id="biz-b",
        source="backfill",
    ))
    run = recovery.get_run(SOUTH, "run-b")
    assert run.end_receipt.action == "fail"
    assert run.status == "backfill_success"
    assert len(recovery.ledger) == 1
    assert recovery.ledger.get_by_transaction("biz-b").receipt_logged is True


def test_batch_mapping_shorthand():
    recovery = PartitionRecovery()
    recovery.declare_batch(NORTH, "A")
    recovery.declare_batch(SOUTH, "B")
    assert recovery.batches == {NORTH: "A", SOUTH: "B"}

    record = RecoveryRecord.from_dict({
        "partitions": {NORTH: "A", SOUTH: "B"},
        "position": 11,
        "source_seq": 11,
        "receipts": [
            {"partition": NORTH, "batch": "A", "run_id": "run-a",
             "action": "success", "source_seq": 10},
            {"partition": SOUTH, "batch": "B", "run_id": "run-b",
             "action": "start", "source_seq": 11},
        ],
    })
    assert record.get_partition(NORTH).current_batch == "A"
    assert record.get_run(NORTH, "run-a").status == "success"
    assert record.get_run(SOUTH, "run-b").status == "pending"
    assert record.cursor().position == 11
