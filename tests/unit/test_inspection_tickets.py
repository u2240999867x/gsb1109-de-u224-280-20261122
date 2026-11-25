import time

import pytest

from inspection.tickets import (
    LocalTicketIndex,
    VersionedTicketStore,
    cluster_alarms_with_pyranges,
)


def initial_updates():
    return [
        {"sequence": 1, "business_version": 1, "alarm_id": "A", "start": 0, "end": 4, "first_reported_at": 9},
        {"sequence": 1, "business_version": 1, "alarm_id": "B", "start": 8, "end": 12, "first_reported_at": 3},
        {"sequence": 1, "business_version": 1, "alarm_id": "C", "start": 10, "end": 14, "first_reported_at": 6},
        {"sequence": 2, "business_version": 2, "alarm_id": "X", "start": 3, "end": 9, "first_reported_at": 4},
        {"sequence": 3, "business_version": 3, "alarm_id": "X", "operation": "revoke"},
        {"sequence": 4, "business_version": 4, "alarm_id": "B", "operation": "revoke"},
    ]


def correction():
    return {
        "sequence": 5,
        "business_version": 2,
        "alarm_id": "X",
        "operation": "correct",
        "start": 3,
        "end": 8,
    }


def test_published_version_two_report_uses_original_cutoff():
    store = VersionedTicketStore()
    store.ingest(initial_updates())

    ticket = store.get_ticket(version=2, ledger_cutoff=4, ticket_id="B")

    assert ticket.ticket_id == "B"
    assert ticket.members == {"A", "B", "C", "X"}


def test_late_correction_splits_ticket_at_touching_endpoint():
    store = VersionedTicketStore()
    store.ingest(initial_updates())
    store.ingest(correction())

    old_report = store.get_daily_report(2, 4, "B")
    assert old_report.members == ("A", "B", "C", "X")

    retained = store.get_ticket(2, 5, "B")
    left = store.get_ticket(2, 5, alarm_id="A")

    assert retained.members == {"B", "C"}
    assert left.ticket_id == "X"
    assert left.members == {"A", "X"}


def test_later_business_versions_are_unchanged_by_old_correction():
    store = VersionedTicketStore()
    store.ingest(initial_updates())
    before_v3 = [(t.ticket_id, t.members) for t in store.tickets_at(3, 4)]
    before_v4 = [(t.ticket_id, t.members) for t in store.tickets_at(4, 4)]

    store.ingest(correction())

    after_v3 = [(t.ticket_id, t.members) for t in store.tickets_at(3, 5)]
    after_v4 = [(t.ticket_id, t.members) for t in store.tickets_at(4, 5)]
    assert before_v3 == after_v3
    assert before_v4 == after_v4
    assert store.get_ticket(4, 5, alarm_id="C").ticket_id == "C"


def test_reconcile_reports_only_changed_version_and_split_successors():
    store = VersionedTicketStore()
    store.ingest(initial_updates())
    store.publish_daily_report(2, 4)
    store.ingest(correction())

    changes = store.reconcile(4, 5)

    assert [change.business_version for change in changes] == [2]
    version_change = changes[0]
    assert version_change.lineage == [
        {
            "change_type": "split",
            "old_ticket_id": "B",
            "successor_ids": ["B", "X"],
        }
    ]
    by_new_id = {change.ticket_id: change for change in version_change.changes}
    assert by_new_id["B"].members == ("B", "C")
    assert by_new_id["B"].removed == ("A", "X")
    assert by_new_id["X"].members == ("A", "X")
    assert set(by_new_id["X"].predecessor_ids) == {"B"}

    for ticket_id in version_change.lineage[0]["successor_ids"]:
        assert store.get_ticket(2, 5, ticket_id=ticket_id)


def test_duplicate_import_and_split_import_are_idempotent_and_equivalent():
    whole = VersionedTicketStore()
    whole.ingest(initial_updates())
    whole.ingest(correction())
    whole.ingest(correction())

    split = VersionedTicketStore()
    split.ingest(initial_updates()[:3])
    split.ingest(initial_updates()[3:])
    split.ingest(correction())
    split.ingest(correction())

    coordinates = [(1, 1), (2, 2), (2, 4), (2, 5), (3, 5), (4, 5)]
    for version, cutoff in coordinates:
        assert [t.to_dict() for t in whole.tickets_at(version, cutoff)] == [
            t.to_dict() for t in split.tickets_at(version, cutoff)
        ]


def test_same_business_version_uses_last_entry_before_cutoff():
    store = VersionedTicketStore()
    store.ingest(
        [
            {"sequence": 1, "business_version": 1, "alarm_id": "A", "start": 0, "end": 4, "first_reported_at": 9},
            {"sequence": 2, "business_version": 2, "alarm_id": "X", "start": 3, "end": 9, "first_reported_at": 4},
            {"sequence": 5, "business_version": 2, "alarm_id": "X", "start": 3, "end": 8, "first_reported_at": 4},
        ]
    )

    assert "B" not in store.get_ticket(2, 2, alarm_id="X").members
    assert store.get_ticket(2, 2, alarm_id="X").members == {"A", "X"}
    assert store.get_ticket(2, 5, alarm_id="X").members == {"A", "X"}


def test_local_index_only_reclusters_affected_component():
    index = LocalTicketIndex()
    for number in range(2000):
        index.upsert(
            _alarm(
                f"N{number}",
                100_000 + number * 10,
                100_004 + number * 10,
                number,
            )
        )
    index.upsert(_alarm("A", 0, 4, 9))
    index.upsert(_alarm("B", 8, 12, 3))
    index.upsert(_alarm("C", 10, 14, 6))
    index.upsert(_alarm("X", 3, 9, 4))

    result = index.upsert(_alarm("X", 3, 8, 4))

    assert {ticket.ticket_id for ticket in result.old_tickets} == {"B"}
    assert {ticket.ticket_id for ticket in result.new_tickets} == {"B", "X"}
    assert index.ticket_for_alarm("A").ticket_id == "X"
    assert index.ticket_for_alarm("B").ticket_id == "B"
    assert len(index.alarms) == 2004


def test_local_million_alarm_correction_is_fast():
    index = LocalTicketIndex()
    alarms = [_alarm(f"N{i}", 10_000 + i * 3, 10_002 + i * 3, i) for i in range(100_000)]
    index.rebuild(alarms)
    index.upsert(_alarm("A", 0, 4, 9))
    index.upsert(_alarm("X", 3, 9, 4))

    started = time.perf_counter()
    result = index.upsert(_alarm("X", 3, 8, 4))
    elapsed = time.perf_counter() - started

    assert {ticket.ticket_id for ticket in result.new_tickets} == {"X"}
    assert elapsed < 0.2


def test_pyranges_backend_matches_strict_semantics():
    store = VersionedTicketStore(use_pyranges=True)
    store.ingest(initial_updates())
    store.ingest(correction())

    assert store.get_ticket(2, 4, "B").members == {"A", "B", "C", "X"}
    assert store.get_ticket(2, 5, "B").members == {"B", "C"}
    assert store.get_ticket(2, 5, alarm_id="A").ticket_id == "X"


def _alarm(alarm_id, start, end, reported_at):
    from inspection.tickets import Alarm

    return Alarm(
        alarm_id=alarm_id,
        start=start,
        end=end,
        first_reported_at=reported_at,
        line="L",
        direction="+",
    )
