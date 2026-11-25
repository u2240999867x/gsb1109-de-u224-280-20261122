"""Faust wiring for ordered, idempotent category revision delivery.

Run locally with a Kafka broker via::

    python examples/category_revision_delivery.py worker -l info

The pure ordering and receipt state machine lives in
:mod:`faust.revision_delivery` and is unit tested without Kafka.
"""

from __future__ import annotations

from typing import Any

import faust

from faust.revision_delivery import Correction, RevisionReorderBuffer


app = faust.App(
    "category-revision-delivery",
    broker="kafka://localhost:9092",
    value_serializer="json",
    topic_partitions=6,
)

corrections_topic = app.topic("supplier-category-corrections", value_type=dict)
changes_topic = app.topic("category-revision-changes", value_type=dict)
receipts_topic = app.topic("category-revision-receipts", value_type=dict)
publications_topic = app.topic("published-category-revisions", value_type=dict)

category_counts = app.Table("category_counts", default=int)
applied_receipts = app.Table("applied_change_receipts", default=int)
receipt_state = app.Table("revision_receipt_state", default=dict)
published_revisions = app.Table("published_revisions", default=dict)

order_buffers: dict[str, RevisionReorderBuffer] = {}


def buffer_for(item_id: str) -> RevisionReorderBuffer:
    if item_id not in order_buffers:
        order_buffers[item_id] = RevisionReorderBuffer()
    return order_buffers[item_id]


@app.agent(corrections_topic)
async def repartition_corrections(events: faust.Stream[dict[str, Any]]) -> None:
    async for event in events:
        correction = Correction.coerce(event)
        deliveries = buffer_for(correction.item_id).submit(correction)

        for delivery in deliveries:
            if not delivery.changes:
                message = delivery.to_message()
                message["published_without_shard_work"] = True
                await publications_topic.send(
                    key=delivery.item_id,
                    value=message,
                )
                published_revisions[delivery.correlation_id] = message
                continue

            for change in delivery.changes:
                await changes_topic.send(
                    key=change.category,
                    value=change.to_message(),
                )


@app.agent(changes_topic)
async def apply_category_changes(changes: faust.Stream[dict[str, Any]]) -> None:
    async for change in changes:
        receipt_id = str(change["receipt_id"])
        duplicate = receipt_id in applied_receipts

        if not duplicate:
            category = str(change["category"])
            category_counts[category] += int(change["delta"])
            applied_receipts[receipt_id] = int(change["revision"])

        receipt = {
            "receipt_id": receipt_id,
            "correlation_id": change["correlation_id"],
            "item_id": change["item_id"],
            "revision": change["revision"],
            "category": change["category"],
            "delta": change["delta"],
            "duplicate": duplicate,
            "expected_receipt_ids": change.get("expected_receipt_ids", []),
        }
        await receipts_topic.send(
            key=str(change["correlation_id"]),
            value=receipt,
        )


@app.agent(receipts_topic)
async def publish_complete_revisions(
    receipts: faust.Stream[dict[str, Any]],
) -> None:
    async for receipt in receipts:
        correlation_id = str(receipt["correlation_id"])
        state = dict(
            receipt_state.get(correlation_id)
            or {
                "expected_receipt_ids": receipt.get("expected_receipt_ids", []),
                "received_receipt_ids": [],
            }
        )

        expected = set(
            state.get("expected_receipt_ids")
            or receipt.get("expected_receipt_ids")
            or []
        )
        received = set(state.get("received_receipt_ids") or [])
        received.add(str(receipt["receipt_id"]))

        state["expected_receipt_ids"] = sorted(expected)
        state["received_receipt_ids"] = sorted(received)
        receipt_state[correlation_id] = state

        if not expected.issubset(received):
            continue
        if correlation_id in published_revisions:
            continue

        publication = {
            "correlation_id": correlation_id,
            "item_id": receipt["item_id"],
            "revision": receipt["revision"],
            "receipts": sorted(received),
        }
        published_revisions[correlation_id] = publication
        await publications_topic.send(
            key=str(receipt["item_id"]),
            value=publication,
        )


if __name__ == "__main__":
    app.main()
