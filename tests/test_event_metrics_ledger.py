from __future__ import annotations

import json

import pytest

from event_metrics_ledger import EventMetricsLedger
from event_metrics_scenario import (
    MINUTE,
    PREVIOUS_MINUTE,
    WATERMARK,
    build_ledger,
    correction_records,
    first_round_records,
    replay_records,
)


def test_first_seal_waits_for_both_partitions_and_excludes_pending() -> None:
    ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    ledger.ingest_many(first_round_records()[:3])

    assert ledger.pending_count == 1
    assert ledger.minute_counts[MINUTE] == 2

    ledger.advance_watermark("p0", WATERMARK)
    assert ledger.sealed_counts == {}

    ledger.advance_watermark("p1", WATERMARK)
    assert ledger.sealed_counts == {MINUTE: 2}
    assert ledger.sealed_windows[MINUTE]["pending_excluded"] == 1


def test_source_plus_id_is_deduplication_key() -> None:
    ledger = EventMetricsLedger(partition_count=2)
    event = {"id": "same", "time": MINUTE, "time_explicit": True}

    ledger.ingest({**event, "source": "source-a"})
    ledger.ingest({**event, "source": "source-b"})
    ledger.ingest({**event, "source": "source-a"})

    assert ledger.minute_counts[MINUTE] == 2
    assert len(ledger.events) == 2


def test_corrections_create_pending_cleanup_and_sealed_net_changes() -> None:
    ledger = build_ledger(1)

    assert ledger.pending_count == 0
    assert len(ledger.pending_cleanups) == 1
    assert ledger.pending_cleanups[0]["resolved_minute"] == MINUTE
    assert ledger.sealed_counts[MINUTE] == 2
    assert ledger.sealed_counts[PREVIOUS_MINUTE] == 0
    assert ledger.net_changes == {MINUTE: 0, PREVIOUS_MINUTE: 1}
    assert ledger.minute_counts[MINUTE] == 2
    assert ledger.minute_counts[PREVIOUS_MINUTE] == 1


def test_duplicate_and_stale_corrections_do_not_rebook() -> None:
    ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    ledger.ingest_many(first_round_records())
    missing_correction, move_correction = correction_records()

    first = ledger.apply_correction(missing_correction)
    duplicate = ledger.apply_correction(missing_correction)
    assert first["status"] == "applied"
    assert duplicate["status"] == "duplicate"

    ledger.apply_correction(move_correction)
    repeated = ledger.apply_correction(move_correction)
    stale = ledger.apply_correction({**move_correction, "new_minute": MINUTE})
    assert repeated["status"] == "duplicate"
    assert stale["status"] == "duplicate"

    ledger.apply_correction({**move_correction, "version": 2})
    late_old = ledger.apply_correction({**move_correction, "version": 1, "new_minute": MINUTE})
    assert late_old["status"] == "duplicate"
    assert ledger.minute_counts[MINUTE] == 2
    assert ledger.minute_counts[PREVIOUS_MINUTE] == 1


def test_higher_version_first_blocks_lower_version() -> None:
    ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    ledger.ingest_many(first_round_records())
    correction = correction_records()[1]

    newer = ledger.apply_correction({**correction, "version": 2})
    older = ledger.apply_correction({**correction, "version": 1, "new_minute": MINUTE})

    assert newer["status"] == "applied"
    assert older["status"] == "stale"
    assert ledger.minute_counts[PREVIOUS_MINUTE] == 1
    assert ledger.minute_counts[MINUTE] == 1


@pytest.mark.parametrize("batch_size", [1, 2, 3, 5, 100])
def test_batch_size_does_not_change_exported_state(batch_size: int) -> None:
    canonical = json.dumps(build_ledger(1).export_state(), sort_keys=True)
    rerun = json.dumps(build_ledger(batch_size).export_state(), sort_keys=True)
    assert rerun == canonical


@pytest.mark.parametrize("batch_size", [1, 2, 4, 7, 100])
def test_batch_boundaries_can_cross_stream_stages(batch_size: int) -> None:
    records = [
        *first_round_records(),
        *correction_records(),
        *replay_records(),
    ]
    canonical_ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    canonical_ledger.ingest_many(records, batch_size=1)
    canonical = json.dumps(canonical_ledger.export_state()["state"], sort_keys=True)

    rerun_ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    rerun_ledger.ingest_many(records, batch_size=batch_size)
    rerun = json.dumps(rerun_ledger.export_state()["state"], sort_keys=True)
    assert rerun == canonical


def test_export_can_restore_and_replay_after_commit() -> None:
    ledger = EventMetricsLedger(expected_partitions=["p0", "p1"])
    ledger.ingest_many(first_round_records())
    commit = ledger.commit("offset-1")
    exported = ledger.export_state()

    restored = EventMetricsLedger.from_state(exported)
    assert restored.committed_sequence == commit["sequence"]
    restored.ingest_many(correction_records())
    restored.commit("corrections")
    restored.ingest_many(replay_records())
    restored.commit("replay")

    expected = build_ledger(7)
    assert restored.export_state()["state"] == expected.export_state()["state"]
