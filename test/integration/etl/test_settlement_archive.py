"""
Tests for the multi-result settlement archive.

A controllable fake DBAPI cursor stands in for the SQL Server procedure cursor
so result order, row-count-only results and chain failures can be scripted.
The target is a local SQLite database built with the reference schema that
ships with the settlement_archive module.
"""
import sqlite3
import tempfile

import pytest

from etlhelper.exceptions import ETLHelperArchiveError
from etlhelper.settlement_archive import (
    DETAIL_COLUMNS,
    MANIFEST_COLUMNS,
    STATUS_APPLIED,
    STATUS_EMPTY,
    STATUS_NOT_RETURNED,
    STATUS_REJECTED,
    STATUS_SKIPPED,
    TARGET_SCHEMA_SQL,
    archive_settlement,
)

DETAIL_DESCRIPTION = (("event_id", None), ("valid_batch_no", None))
MANIFEST_DESCRIPTION = tuple((name, None) for name in MANIFEST_COLUMNS)


class FakeResult:
    """One entry in the scripted result chain of a FakeCursor."""

    def __init__(self, description, rows=(), rowcount=-1):
        self.description = description
        self.rows = list(rows)
        self.position = 0
        self.rowcount = rowcount


class FakeCursor:
    """
    DBAPI-like cursor whose result chain is fully scripted.

    ``failures`` maps a step name (``"nextset_<index>"`` or
    ``"fetch_<result-index>_<fetch-count>"``) to the exception to raise.
    """

    def __init__(self, results, failures=None):
        self._results = results
        self._failures = failures or {}
        # A real DBAPI cursor already points at the first result set once the
        # procedure has been executed.
        self._index = 0
        self._fetch_counts = {}
        self.execute_calls = 0

    @property
    def description(self):
        if self._index < 0 or self._index >= len(self._results):
            return None
        return self._results[self._index].description

    def execute(self, *args, **kwargs):
        self.execute_calls += 1
        self._index = 0
        self._fetch_counts = {}

    def nextset(self):
        step = f"nextset_{self._index}"
        if step in self._failures:
            raise self._failures[step]
        self._index += 1
        if self._index >= len(self._results):
            return None
        return True

    def fetchmany(self, size):
        fetch_count = self._fetch_counts.get(self._index, 0)
        step = f"fetch_{self._index}_{fetch_count}"
        if step in self._failures:
            raise self._failures[step]
        self._fetch_counts[self._index] = fetch_count + 1

        result = self._results[self._index]
        chunk = result.rows[result.position:result.position + size]
        result.position += len(chunk)
        return chunk

    def close(self):
        pass


def detail(rows=()):
    return FakeResult(DETAIL_DESCRIPTION, rows)


def rowcount_only(rowcount):
    return FakeResult(None, (), rowcount=rowcount)


def manifest(rows):
    return FakeResult(MANIFEST_DESCRIPTION, rows)


def make_target(tmp_path, initial_status=()):
    """Create a local SQLite target with the reference business schema."""
    conn = sqlite3.connect(tmp_path / "target.db")
    conn.executescript(TARGET_SCHEMA_SQL)
    conn.execute(
        "CREATE TABLE site_correction (id INTEGER PRIMARY KEY, note TEXT)"
    )
    for batch_no, status in initial_status:
        conn.execute(
            "INSERT INTO batch_status (valid_batch_no, status) VALUES (?, ?)",
            (batch_no, status),
        )
    conn.commit()
    return conn


def _normalize_event_id(value):
    # event_id is a TEXT column, so SQLite stores numeric ids as strings.
    if isinstance(value, str) and value.lstrip("-").isdigit():
        return int(value)
    return value


def journal_events(conn):
    return [
        (_normalize_event_id(event_id), event_type, batch_no)
        for event_id, event_type, batch_no in conn.execute(
            "SELECT event_id, event_type, valid_batch_no "
            "FROM event_journal ORDER BY seq"
        ).fetchall()
    ]


def batch_status(conn, batch_no):
    return conn.execute(
        "SELECT status FROM batch_status WHERE valid_batch_no = ?",
        (batch_no,),
    ).fetchone()[0]


def prompt_cursor():
    """
    Build the exact chain described in the task narrative.

    Return order: C cancellation, A production, B cancellation, C production,
    an empty detail, with row-count results interspersed.  The manifest is
    returned in the independent order C, B, D, A.
    """
    results = [
        detail([(50, "K")]),                              # detail 1: C cancel
        rowcount_only(7),
        detail([(10, "K"), (30, "Z")]),                  # detail 2: A produce
        detail([(20, "K")]),                             # detail 3: B cancel
        rowcount_only(2),
        detail([(40, "K")]),                             # detail 4: C produce
        detail(),                                        # detail 5: empty
        manifest([
            ("C", 1, 4, 1, 1, 1),
            ("B", 1, None, 0, 3, 1),
            ("D", 1, None, 0, 5, 0),
            ("A", 1, 2, 2, None, 0),
        ]),
        rowcount_only(0),
    ]
    return FakeCursor(results)


def test_archive_prompt_scenario(tmp_path):
    target = make_target(tmp_path, initial_status=[("Z", "effective")])
    cursor = prompt_cursor()

    receipt = archive_settlement(cursor, target, chunk_size=2)
    target.commit()

    # The source procedure is never re-executed.
    assert cursor.execute_calls == 0

    by_plant = {batch.plant: batch for batch in receipt.batches}
    assert [batch.plant for batch in receipt.batches] == ["C", "B", "D", "A"]

    accepted = {batch.plant for batch in receipt.accepted}
    rejected = {batch.plant for batch in receipt.rejected}
    assert accepted == {"C", "D"}
    assert rejected == {"A", "B"}

    batch_a = by_plant["A"]
    assert batch_a.accepted is False
    assert batch_a.rejected_event_id == 30
    assert "already effective" in batch_a.rejection_reason

    batch_b = by_plant["B"]
    assert batch_b.rejected_event_id == 20
    assert "not effective" in batch_b.rejection_reason
    assert batch_b.production_status == STATUS_NOT_RETURNED
    assert batch_b.cancellation_status == STATUS_REJECTED

    batch_c = by_plant["C"]
    assert batch_c.accepted is True
    assert batch_c.production_status == STATUS_APPLIED
    assert batch_c.cancellation_status == STATUS_APPLIED

    batch_d = by_plant["D"]
    assert batch_d.accepted is True
    assert batch_d.production_status == STATUS_NOT_RETURNED
    assert batch_d.cancellation_status == STATUS_EMPTY

    # Only C's events are written, in event-id order; the rejected batches and
    # the empty/not-returned details leave no business rows and no journal rows.
    assert journal_events(target) == [
        (40, "production", "K"),
        (50, "cancellation", "K"),
    ]
    assert target.execute(
        "SELECT COUNT(*) FROM production_events"
    ).fetchone()[0] == 1
    assert target.execute(
        "SELECT COUNT(*) FROM cancellation_events"
    ).fetchone()[0] == 1
    assert batch_status(target, "Z") == "effective"
    assert batch_status(target, "K") == "cancelled"


def test_rowcount_only_results_do_not_take_detail_numbers(tmp_path):
    target = make_target(tmp_path)
    results = [
        rowcount_only(3),
        detail([(1, "K")]),
        rowcount_only(0),
        detail(),
        manifest([("P", 1, 1, 1, 2, 0)]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    batch = receipt.batches[0]
    assert batch.production_detail == 1
    assert batch.cancellation_detail == 2
    assert batch.production_status == STATUS_APPLIED
    assert batch.cancellation_status == STATUS_EMPTY
    assert journal_events(target) == [(1, "production", "K")]


def test_unreleased_batch_is_skipped_and_does_not_write(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),                  # detail 1: released plant P
        detail([(2, "Z")]),                  # detail 2: unreleased plant Q
        manifest([
            ("P", 1, 1, 1, None, 0),
            ("Q", 0, 2, 1, None, 0),
        ]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    assert [batch.plant for batch in receipt.skipped] == ["Q"]
    skipped = receipt.batches[1]
    assert skipped.accepted is False
    assert skipped.production_status == STATUS_SKIPPED
    assert journal_events(target) == [(1, "production", "K")]


def test_expected_row_count_mismatch_fails_without_target_writes(tmp_path):
    target = make_target(tmp_path)
    target.execute(
        "INSERT INTO site_correction (id, note) VALUES (1, 'correction')"
    )
    results = [
        detail([(1, "K"), (2, "K")]),
        manifest([("P", 1, 1, 99, None, 0)]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="expected 99"):
        archive_settlement(FakeCursor(results), target)

    # Fatal failure leaves no archive writes but the caller can still commit
    # its own earlier change.
    target.commit()
    assert journal_events(target) == []
    assert target.execute(
        "SELECT note FROM site_correction WHERE id = 1"
    ).fetchone()[0] == "correction"


def test_described_result_after_manifest_fails(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 1, 1, None, 0)]),
        detail([(2, "Z")]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="after the manifest"):
        archive_settlement(FakeCursor(results), target)

    assert journal_events(target) == []


def test_chain_without_manifest_fails(tmp_path):
    target = make_target(tmp_path)
    results = [detail([(1, "K")]), rowcount_only(1)]

    with pytest.raises(ETLHelperArchiveError, match="without a manifest"):
        archive_settlement(FakeCursor(results), target)


def test_unreferenced_detail_fails(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        detail([(2, "Z")]),
        manifest([("P", 1, 1, 1, None, 0)]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="not assigned"):
        archive_settlement(FakeCursor(results), target)


def test_detail_referenced_twice_fails(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        manifest([
            ("P", 1, 1, 1, None, 0),
            ("Q", 1, 1, 1, None, 0),
        ]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="exactly one"):
        archive_settlement(FakeCursor(results), target)


def test_duplicate_event_id_among_released_details_fails(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        detail([(1, "Z")]),
        manifest([
            ("P", 1, 1, 1, None, 0),
            ("Q", 1, 2, 1, None, 0),
        ]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="Event ids"):
        archive_settlement(FakeCursor(results), target)


def test_duplicate_event_id_in_unreleased_detail_is_allowed(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        detail([(1, "Z")]),
        manifest([
            ("P", 1, 1, 1, None, 0),
            ("Q", 0, 2, 1, None, 0),
        ]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    assert [batch.plant for batch in receipt.accepted] == ["P"]
    assert journal_events(target) == [(1, "production", "K")]


def test_manifest_reference_to_missing_detail_fails(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 7, 1, None, 0)]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="no such detail"):
        archive_settlement(FakeCursor(results), target)


def test_chain_break_after_manifest_leaves_no_business_rows(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 1, 1, None, 0)]),
        rowcount_only(1),
    ]
    # The manifest is fully read, then advancing past the row-count result
    # raises before the chain is confirmed complete.
    cursor = FakeCursor(results, failures={"nextset_2": OSError("connection lost")})

    with pytest.raises(ETLHelperArchiveError, match="advance"):
        archive_settlement(cursor, target)

    target.commit()
    assert journal_events(target) == []
    assert target.execute(
        "SELECT COUNT(*) FROM production_events"
    ).fetchone()[0] == 0


def test_fetch_failure_mid_detail_fails_without_writes(tmp_path):
    target = make_target(tmp_path)
    results = [
        detail([(1, "K"), (2, "K"), (3, "K")]),
        manifest([("P", 1, 3, None, 0)]),
    ]
    cursor = FakeCursor(
        results, failures={"fetch_0_1": OSError("connection lost")}
    )

    with pytest.raises(ETLHelperArchiveError, match="Failed to fetch"):
        archive_settlement(cursor, target, chunk_size=2)

    target.commit()
    assert journal_events(target) == []


def test_non_business_write_error_fails_and_keeps_caller_changes(tmp_path):
    target = make_target(tmp_path)
    target.execute(
        "INSERT INTO site_correction (id, note) VALUES (1, 'correction')"
    )
    # Removing the target table turns every business insert into an
    # OperationalError, which is not a business-rule rejection.
    target.execute("DROP TABLE production_events")

    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 1, 1, None, 0)]),
    ]

    with pytest.raises(ETLHelperArchiveError, match="Non-business write error"):
        archive_settlement(FakeCursor(results), target)

    target.commit()
    assert journal_events(target) == []
    assert target.execute(
        "SELECT note FROM site_correction WHERE id = 1"
    ).fetchone()[0] == "correction"


def test_successful_archive_shares_caller_transaction(tmp_path):
    target = make_target(tmp_path)
    target.execute(
        "INSERT INTO site_correction (id, note) VALUES (1, 'correction')"
    )
    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 1, 1, None, 0)]),
    ]

    archive_settlement(FakeCursor(results), target)

    # Before the caller commits, nothing is visible on another connection.
    other = sqlite3.connect(tmp_path / "target.db")
    assert other.execute("SELECT COUNT(*) FROM event_journal").fetchone()[0] == 0
    assert other.execute(
        "SELECT COUNT(*) FROM site_correction"
    ).fetchone()[0] == 0
    other.close()

    target.commit()

    other = sqlite3.connect(tmp_path / "target.db")
    assert other.execute("SELECT COUNT(*) FROM event_journal").fetchone()[0] == 1
    assert other.execute(
        "SELECT note FROM site_correction WHERE id = 1"
    ).fetchone()[0] == "correction"
    other.close()


def test_caller_rollback_discards_archive_and_correction(tmp_path):
    target = make_target(tmp_path)
    target.execute(
        "INSERT INTO site_correction (id, note) VALUES (1, 'correction')"
    )
    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 1, 1, None, 0)]),
    ]

    archive_settlement(FakeCursor(results), target)
    target.rollback()

    assert journal_events(target) == []
    assert target.execute(
        "SELECT COUNT(*) FROM site_correction"
    ).fetchone()[0] == 0


def test_chunking_keeps_correctness_across_many_rows(tmp_path):
    target = make_target(tmp_path)
    rows = [(event_id, f"K{event_id}") for event_id in range(100, 140)]
    results = [
        detail(rows),
        manifest([("P", 1, 1, 40, None, 0)]),
    ]

    receipt = archive_settlement(FakeCursor(results), target, chunk_size=7)
    target.commit()

    assert receipt.batches[0].production_actual_rows == 40
    assert target.execute(
        "SELECT COUNT(*) FROM event_journal"
    ).fetchone()[0] == 40
    # Event order, not result fetch order, drives the journal sequence.
    first = _normalize_event_id(target.execute(
        "SELECT event_id FROM event_journal ORDER BY seq LIMIT 1"
    ).fetchone()[0])
    last = _normalize_event_id(target.execute(
        "SELECT event_id FROM event_journal ORDER BY seq DESC LIMIT 1"
    ).fetchone()[0])
    assert (first, last) == (100, 139)


def test_same_plant_two_batches_are_distinct(tmp_path):
    target = make_target(tmp_path, initial_status=[("Z", "effective")])
    results = [
        detail([(1, "K")]),                  # detail 1: batch one production
        detail([(2, "Z")]),                  # detail 2: batch two production
        manifest([
            ("P", 1, 1, 1, None, 0),
            ("P", 1, 2, 1, None, 0),
        ]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    assert len(receipt.batches) == 2
    assert receipt.batches[0].accepted is True
    assert receipt.batches[1].accepted is False
    assert receipt.batches[1].rejected_event_id == 2
    assert journal_events(target) == [(1, "production", "K")]


def test_rejection_rejudges_remaining_batches_from_initial_state(tmp_path):
    # K starts absent.  Batch B cancels K (would fail alone), batch C produces
    # K then cancels it.  Because B is judged against the pre-archive state,
    # it must still be rejected even though C would make K effective first.
    target = make_target(tmp_path)
    results = [
        detail([(20, "K")]),                 # detail 1: B cancellation
        detail([(40, "K")]),                 # detail 2: C production
        detail([(50, "K")]),                 # detail 3: C cancellation
        manifest([
            ("B", 1, None, 0, 1, 1),
            ("C", 1, 2, 1, 3, 1),
        ]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    by_plant = {batch.plant: batch for batch in receipt.batches}
    assert by_plant["B"].accepted is False
    assert by_plant["B"].rejected_event_id == 20
    assert by_plant["C"].accepted is True
    assert journal_events(target) == [
        (40, "production", "K"),
        (50, "cancellation", "K"),
    ]
    assert batch_status(target, "K") == "cancelled"


def test_rejected_batch_rolls_back_its_earlier_successes(tmp_path):
    # Event 1 would succeed, but event 5 fails the production rule against the
    # pre-archive state (Z already effective).  The whole batch is rejected, so
    # the earlier insert and its journal entry must be undone as well.
    target = make_target(tmp_path, initial_status=[("Z", "effective")])
    results = [
        detail([(1, "K"), (5, "Z")]),
        manifest([("P", 1, 1, 2, None, 0)]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    assert len(receipt.rejected) == 1
    rejected = receipt.rejected[0]
    assert rejected.rejected_event_id == 5
    assert journal_events(target) == []
    assert target.execute(
        "SELECT COUNT(*) FROM production_events"
    ).fetchone()[0] == 0
    assert target.execute(
        "SELECT COUNT(*) FROM batch_status"
    ).fetchone()[0] == 1  # only the pre-existing Z


def test_all_batches_rejected_and_remaining_batch_rejudged(tmp_path):
    # Q's cancellation of K would only succeed because P makes K effective in
    # the same round.  Re-judging Q from the pre-archive state must reject it,
    # and P is rejected by its own later event: nothing is written at all.
    target = make_target(tmp_path, initial_status=[("Z", "effective")])
    results = [
        detail([(1, "K"), (5, "Z")]),       # detail 1: P production
        detail([(2, "K")]),                 # detail 2: Q cancellation
        manifest([
            ("P", 1, 1, 2, None, 0),
            ("Q", 1, None, 0, 2, 1),
        ]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    assert {batch.plant for batch in receipt.rejected} == {"P", "Q"}
    q_rejection = next(
        batch for batch in receipt.batches if batch.plant == "Q"
    )
    assert q_rejection.rejected_event_id == 2
    assert journal_events(target) == []
    assert batch_status(target, "Z") == "effective"
    assert target.execute(
        "SELECT COUNT(*) FROM batch_status WHERE valid_batch_no = 'K'"
    ).fetchone()[0] == 0


def test_rejected_batch_keeps_empty_distinct_from_not_returned(tmp_path):
    # The production rule rejects the batch (Z already effective), but its
    # cancellation detail was returned with zero rows and its other detail was
    # explicitly not returned.  The two states must stay distinct.
    target = make_target(tmp_path, initial_status=[("Z", "effective")])
    results = [
        detail([(5, "Z")]),                  # detail 1: production (rejected)
        detail(),                            # detail 2: empty cancellation
        manifest([("P", 1, 1, 1, 2, 0)]),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    batch = receipt.batches[0]
    assert batch.accepted is False
    assert batch.production_status == STATUS_REJECTED
    assert batch.cancellation_status == STATUS_EMPTY
    assert journal_events(target) == []


def test_chain_can_start_with_a_rowcount_result(tmp_path):
    target = make_target(tmp_path)
    results = [
        rowcount_only(5),
        detail([(1, "K")]),
        rowcount_only(0),
        manifest([("P", 1, 1, 1, None, 0)]),
        rowcount_only(3),
    ]

    receipt = archive_settlement(FakeCursor(results), target)
    target.commit()

    assert receipt.batches[0].production_detail == 1
    assert journal_events(target) == [(1, "production", "K")]


def test_spool_files_are_cleaned_up(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    target = make_target(tmp_path)
    results = [
        detail([(1, "K")]),
        manifest([("P", 1, 1, 1, None, 0)]),
    ]

    archive_settlement(FakeCursor(results), target)

    leftovers = list(tmp_path.glob("etlhelper_settlement_*.db"))
    assert leftovers == []


def test_empty_manifest_fails(tmp_path):
    target = make_target(tmp_path)
    results = [detail(), manifest([])]

    with pytest.raises(ETLHelperArchiveError, match="manifest result set is empty"):
        archive_settlement(FakeCursor(results), target)


def test_unexpected_columns_fail(tmp_path):
    target = make_target(tmp_path)
    weird = FakeResult((("event_id", None), ("other", None)), [(1, 2)])
    results = [weird, manifest([])]

    with pytest.raises(ETLHelperArchiveError, match="Unexpected result set"):
        archive_settlement(FakeCursor(results), target)


def test_detail_column_constants():
    assert DETAIL_COLUMNS == ("event_id", "valid_batch_no")
    assert MANIFEST_COLUMNS == (
        "plant",
        "released",
        "production_detail",
        "production_expected_rows",
        "cancellation_detail",
        "cancellation_expected_rows",
    )
