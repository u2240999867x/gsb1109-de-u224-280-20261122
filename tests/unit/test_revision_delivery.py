from __future__ import annotations

import pytest

from revision_delivery import (
    CategoryCountShard,
    CategoryReceipt,
    ChangeSide,
    Correction,
    PublicationGate,
    RevisionDeliveryService,
    RevisionReorderBuffer,
    WrongShardError,
    category_net_changes,
    plan_revisions,
)


def test_category_move_has_two_correlated_deterministic_legs() -> None:
    [delivery] = plan_revisions(
        [
            Correction(
                item_id="sku-1",
                revision=12,
                old_category="A",
                new_category="B",
            )
        ]
    )

    assert delivery.correlation_id == "sku-1@r12"
    assert [change.category for change in delivery.changes] == ["A", "B"]
    assert [change.delta for change in delivery.changes] == [-1, 1]
    assert {change.correlation_id for change in delivery.changes} == {"sku-1@r12"}
    assert len({change.receipt_id for change in delivery.changes}) == 2
    assert delivery.net_by_category() == {"A": -1, "B": 1}

    first, second = delivery.changes
    assert first.side is ChangeSide.FROM
    assert second.side is ChangeSide.TO
    assert first.to_message()["receipt_id"].endswith(":from:A")
    assert second.to_message()["receipt_id"].endswith(":to:B")


def test_revision_13_is_buffered_until_revision_12_arrives() -> None:
    buffer = RevisionReorderBuffer(expected_revision=12)

    assert (
        buffer.submit(
            {
                "item_id": "sku-1",
                "revision": 13,
                "from_category": "B",
                "to_category": "A",
            }
        )
        == []
    )
    assert [ref.revision for ref in buffer.pending_revisions()] == [13]

    deliveries = buffer.submit(
        {
            "item_id": "sku-1",
            "revision": 12,
            "old_category": "A",
            "new_category": "B",
        }
    )

    assert [delivery.revision for delivery in deliveries] == [12, 13]
    assert deliveries[0].net_by_category() == {"A": -1, "B": 1}
    assert deliveries[1].net_by_category() == {"B": -1, "A": 1}


def test_unmarked_buffer_waits_for_late_predecessor() -> None:
    buffer = RevisionReorderBuffer()

    assert buffer.submit(Correction("sku-1", 13, "B", "A")) == []
    deliveries = buffer.submit(Correction("sku-1", 12, "A", "B"))

    assert [delivery.revision for delivery in deliveries] == [12, 13]


def test_unordered_batch_is_calculated_in_revision_order() -> None:
    deliveries = plan_revisions(
        [
            {"item_id": "sku-1", "revision": 13, "category": "A"},
            {"item_id": "sku-1", "revision": 12, "category": "B"},
        ],
        categories={"sku-1": "A"},
    )

    assert [delivery.revision for delivery in deliveries] == [12, 13]
    assert deliveries[0].net_by_category() == {"A": -1, "B": 1}
    assert deliveries[1].net_by_category() == {"B": -1, "A": 1}
    assert category_net_changes(
        [
            {"item_id": "sku-1", "revision": 13, "category": "A"},
            {"item_id": "sku-1", "revision": 12, "category": "B"},
        ],
        categories={"sku-1": "A"},
    ) == {"A": [-1, 1], "B": [1, -1]}


def test_pending_replay_does_not_add_b_twice() -> None:
    service = RevisionDeliveryService(
        initial_counts={"A": 1}, release_first_observed=True
    )
    result = service.submit(
        Correction(
            item_id="sku-1",
            revision=12,
            old_category="A",
            new_category="B",
        )
    )
    b_leg = next(change for change in result.changes if change.category == "B")

    first_receipt = service.apply(b_leg)
    assert first_receipt.duplicate is False
    assert service.counts == {"A": 1, "B": 1}

    pending = service.gate.pending_changes((b_leg.item_id, b_leg.revision))
    assert [change.category for change in pending] == ["A"]

    replay_receipt = service.shard("B").apply(b_leg)
    assert replay_receipt.duplicate is True
    assert service.counts["B"] == 1

    service.replay_pending()
    assert service.counts == {"A": 0, "B": 1}


def test_return_revision_arrives_before_older_a_receipt() -> None:
    service = RevisionDeliveryService(
        initial_counts={"A": 1}, release_first_observed=True
    )
    revision_12 = service.submit(Correction("sku-1", 12, "A", "B")).delivery
    assert revision_12 is not None
    b_leg = next(change for change in revision_12.changes if change.category == "B")
    service.apply(b_leg)

    revision_13 = service.submit(Correction("sku-1", 13, "B", "A")).delivery
    assert revision_13 is not None
    assert service.publications == []
    assert service.counts == {"A": 1, "B": 1}

    service.replay_pending()
    assert [item.revision for item in service.publications] == [12, 13]
    assert service.counts == {"A": 1, "B": 0}


def test_gate_publishes_only_when_both_legs_are_present_and_in_order() -> None:
    service = RevisionDeliveryService(
        initial_counts={"A": 1}, release_first_observed=True
    )
    result = service.submit(Correction("sku-1", 12, "A", "B"))
    rev12 = result.delivery.ref

    assert service.gate.state(rev12).value == "deliverable"

    a_leg, b_leg = result.changes
    service.apply(b_leg)
    assert service.gate.state(rev12).value == "partial"
    assert service.publications == []

    service.apply(a_leg)
    assert service.gate.state(rev12).value == "published"
    assert [publication.revision for publication in service.publications] == [12]
    assert service.published_counts == {"A": 0, "B": 1}


def test_newer_complete_revision_waits_for_missing_older_publication() -> None:
    gate = PublicationGate()
    deliveries = plan_revisions(
        [
            Correction("sku-1", 12, "A", "B"),
            Correction("sku-1", 13, "B", "A"),
        ]
    )
    rev12, rev13 = (gate.expect(delivery) for delivery in deliveries)

    for change in rev13.changes:
        gate.acknowledge(CategoryReceipt.for_change(change))

    assert gate.drain_publications() == []
    assert gate.is_complete(rev13.ref)
    assert not gate.is_published(rev13.ref)

    for change in rev12.changes:
        if change.category == "A":
            gate.acknowledge(CategoryReceipt.for_change(change))
    assert gate.drain_publications() == []

    for change in rev12.changes:
        if change.category == "B":
            gate.acknowledge(CategoryReceipt.for_change(change))

    published = gate.drain_publications()
    assert [item.revision for item in published] == [12, 13]


def test_attribute_correction_has_no_other_category_shard_work() -> None:
    [delivery] = plan_revisions(
        [
            {
                "item_id": "sku-2",
                "revision": 7,
                "category": "C",
                "attributes": {"color": "red"},
            }
        ],
        categories={"sku-2": "C"},
    )

    assert delivery.kind == "attribute"
    assert len(delivery.changes) == 1
    attribute_leg = delivery.changes[0]
    assert attribute_leg.category == "C"
    assert attribute_leg.delta == 0
    assert delivery.expected_categories == ("C",)
    assert delivery.net_by_category() == {"C": 0}

    shard_c = CategoryCountShard("C")
    receipt = shard_c.apply(attribute_leg)
    assert receipt.duplicate is False
    assert shard_c.count == 0
    assert shard_c.apply(attribute_leg).duplicate is True

    gate = PublicationGate()
    gate.expect(delivery)
    assert gate.drain_publications() == []

    gate.acknowledge(CategoryReceipt.for_change(attribute_leg))
    published = gate.drain_publications()
    assert len(published) == 1


def test_shard_rejects_other_category_and_deduplicates_replay() -> None:
    shard_a = CategoryCountShard("A", initial_count=1)
    [delivery] = plan_revisions([Correction("sku-1", 12, "A", "B")])
    a_leg, b_leg = delivery.changes

    receipt = shard_a.apply(a_leg)
    assert receipt.duplicate is False
    assert shard_a.count == 0

    duplicate = shard_a.apply(a_leg)
    assert duplicate.duplicate is True
    assert shard_a.count == 0

    with pytest.raises(WrongShardError):
        shard_a.apply(b_leg)


def test_full_13_12_scenario_has_expected_net_history_and_counts() -> None:
    service = RevisionDeliveryService(
        initial_counts={"A": 1},
        expected_revision={"sku-1": 12},
    )

    assert service.submit(Correction("sku-1", 13, "B", "A")).deliveries == ()
    result = service.submit(Correction("sku-1", 12, "A", "B"))
    assert [delivery.revision for delivery in result.deliveries] == [12, 13]

    b12 = next(
        change
        for change in result.changes
        if change.revision == 12 and change.category == "B"
    )
    service.apply(b12)
    assert service.counts["B"] == 1

    service.replay_pending()
    duplicate = service.shard("B").apply(b12)
    assert duplicate.duplicate
    service.acknowledge(duplicate)

    assert service.counts == {"A": 1, "B": 0}
    assert service.published_counts == {"A": 1, "B": 0}
    assert service.net_changes("A") == [-1, 1]
    assert service.net_changes("B") == [1, -1]
    assert [(p.item_id, p.revision) for p in service.publications] == [
        ("sku-1", 12),
        ("sku-1", 13),
    ]
