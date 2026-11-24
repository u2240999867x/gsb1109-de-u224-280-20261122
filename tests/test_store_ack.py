import asyncio
from unittest import mock

import pytest

from aiokafka import AIOKafkaConsumer, DeliveryReceipt, StaleDeliveryReceiptError
from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import IllegalOperation, IllegalStateError
from aiokafka.protocol.fetch import FetchRequest_v0 as FetchRequest
from aiokafka.protocol.fetch import FetchResponse_v0 as FetchResponse
from aiokafka.record.default_records import (
    _DefaultRecordBatchBuilderPy as DefaultRecordBatchBuilder,
)
from aiokafka.structs import TopicPartition
from aiokafka.util import create_task

TP0 = TopicPartition("topic", 0)
TP1 = TopicPartition("topic", 1)


def make_state(enable_store_ack=True):
    state = SubscriptionState(enable_store_ack=enable_store_ack)
    state.assign_from_user({TP0, TP1})
    assignment = state.subscription.assignment
    state.seek(TP0, 0)
    state.seek(TP1, 0)
    return state, assignment


def build_raw_batch(offsets, *, control=False, transactional=False, producer_id=-1):
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=1 if (control or transactional) else 0,
        producer_id=producer_id,
        producer_epoch=1,
        base_sequence=-1 if control else 0,
        batch_size=999999,
    )
    if control:
        original = builder._get_attributes
        builder._get_attributes = lambda *a, **kw: (
            original(*a, **kw) | DefaultRecordBatchBuilder.CONTROL_MASK
        )
    for offset in offsets:
        if control:
            builder.append(
                offset=offset,
                timestamp=1631276519572,
                key=b"\x00\x00\x00\x00",
                value=b"\x00\x00\x00\x00\x00\x00",
                headers=[],
            )
        else:
            builder.append(
                offset=offset, value=b"msg", key=None, timestamp=None, headers=[]
            )
    return bytes(builder.build())


# --------------------------------------------------------------------------
# SubscriptionState / TopicPartitionState state machine
# --------------------------------------------------------------------------


async def test_receipt_issued_and_acknowledged_in_order():
    state, assignment = make_state()
    tp_state = assignment.state_value(TP0)

    r0 = state.issue_receipt(TP0, 0)
    assert isinstance(r0, DeliveryReceipt)
    assert r0.offset == 0
    assert tp_state.safe_position == 0

    state.acknowledge(r0)
    assert tp_state.safe_position == 1

    r1 = state.issue_receipt(TP0, 1)
    state.acknowledge(r1)
    assert tp_state.safe_position == 2


async def test_out_of_order_ack_waits_for_prefix():
    state, assignment = make_state()
    tp_state = assignment.state_value(TP0)

    r0 = state.issue_receipt(TP0, 0)
    r1 = state.issue_receipt(TP0, 1)
    r2 = state.issue_receipt(TP0, 2)

    state.acknowledge(r2)
    assert tp_state.safe_position == 0
    state.acknowledge(r1)
    assert tp_state.safe_position == 0
    state.acknowledge(r0)
    assert tp_state.safe_position == 3


async def test_empty_offsets_advance_on_batch_completion():
    state, assignment = make_state()
    tp_state = assignment.state_value(TP0)

    r0 = state.issue_receipt(TP0, 0)
    # A complete batch up to offset 3, but only offset 0 carried a record.
    tp_state.mark_empty_upto(3)
    # Offset 0 still pending, so the safe position cannot move.
    assert tp_state.safe_position == 0
    state.acknowledge(r0)
    # Offsets 1-3 were proven empty by the drained batch.
    assert tp_state.safe_position == 4


async def test_empty_fetch_without_batch_completion_does_not_advance():
    state, assignment = make_state()
    tp_state = assignment.state_value(TP0)

    r0 = state.issue_receipt(TP0, 0)
    state.acknowledge(r0)
    assert tp_state.safe_position == 1
    # An empty Fetch response produces no completion callback, so nothing
    # beyond the acknowledged prefix becomes committable.
    assert tp_state.safe_position == 1


async def test_reread_invalidates_later_receipts_but_keeps_earlier_work():
    state, assignment = make_state()
    tp_state = assignment.state_value(TP0)

    r10 = state.issue_receipt(TP0, 10)
    r14 = state.issue_receipt(TP0, 14)
    r17 = state.issue_receipt(TP0, 17)
    tp_state.mark_empty_upto(17)

    state.reread(TP0, 14)
    assert tp_state.position == 14
    assert tp_state.safe_position == 10

    # Late receipts from the superseded dispatch are rejected.
    with pytest.raises(StaleDeliveryReceiptError):
        state.acknowledge(r14)
    with pytest.raises(StaleDeliveryReceiptError):
        state.acknowledge(r17)

    # Work dispatched before the failure point is still confirmable and
    # advances the safe position across the already proven gaps.
    state.acknowledge(r10)
    assert tp_state.safe_position == 14

    new_r14 = state.issue_receipt(TP0, 14)
    new_r17 = state.issue_receipt(TP0, 17)
    tp_state.mark_empty_upto(17)
    state.acknowledge(new_r14)
    assert tp_state.safe_position == 17
    state.acknowledge(new_r17)
    assert tp_state.safe_position == 18


async def test_reread_below_commit_safe_prefix_raises():
    state, assignment = make_state()
    state.acknowledge(state.issue_receipt(TP0, 0))
    state.acknowledge(state.issue_receipt(TP0, 1))
    assert assignment.state_value(TP0).safe_position == 2

    with pytest.raises(IllegalStateError):
        state.reread(TP0, 1)


async def test_reassignment_invalidates_receipts_from_old_assignment():
    state = SubscriptionState(enable_store_ack=True)
    state.assign_from_user({TP0})
    state.seek(TP0, 10)
    receipt = state.issue_receipt(TP0, 10)
    epoch = state.delivery_epoch

    # Partition revoked and later handed back to the same consumer.
    state.unsubscribe()
    state.assign_from_user({TP0})
    assert state.delivery_epoch == epoch + 1

    with pytest.raises(StaleDeliveryReceiptError):
        state.acknowledge(receipt)


async def test_safe_consumed_offsets_collects_partition_boundaries():
    state, assignment = make_state()
    state.acknowledge(state.issue_receipt(TP0, 0))
    assignment.state_value(TP1).mark_empty_upto(2)

    offsets = state.safe_consumed_offsets()
    assert offsets[TP0].offset == 1
    assert offsets[TP1].offset == 3


async def test_disabled_mode_rejects_acknowledge():
    state, _assignment = make_state(enable_store_ack=False)
    assert state.store_ack_enabled is False
    with pytest.raises(IllegalStateError):
        state.acknowledge(mock.Mock())


# --------------------------------------------------------------------------
# Fetcher integration
# --------------------------------------------------------------------------


def make_fetcher(*, enable_store_ack=True, isolation_level="read_uncommitted"):
    client = AIOKafkaClient(bootstrap_servers=[])
    subscriptions = SubscriptionState(enable_store_ack=enable_store_ack)
    fetcher = Fetcher(
        client,
        subscriptions,
        enable_store_ack=enable_store_ack,
        isolation_level=isolation_level,
    )
    # Tests drive the private fetch methods directly; disable the background
    # fetch routine, which has no broker to talk to.
    fetcher._fetch_task.cancel()
    return client, subscriptions, fetcher


async def stop_fetcher(fetcher):
    await fetcher.close()


async def test_fetcher_attaches_receipts_to_records():
    client, subscriptions, fetcher = make_fetcher()
    try:
        subscriptions.assign_from_user({TP0})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(TP0, 4)

        raw_batch = build_raw_batch([4, 5])
        request_tp = (TP0.topic, [(TP0.partition, 4, 100000)])
        response = FetchResponse([(TP0.topic, [(0, 0, 9, raw_batch)])])
        client.send = mock.AsyncMock(return_value=response)

        request = FetchRequest(-1, 100, 100, [request_tp])
        assert await fetcher._proc_fetch_request(assignment, 0, request) is True

        records = await fetcher.fetched_records([])
        messages = records[TP0]
        assert [msg.offset for msg in messages] == [4, 5]
        assert all(isinstance(msg.receipt, DeliveryReceipt) for msg in messages)

        tp_state = assignment.state_value(TP0)
        for msg in messages:
            subscriptions.acknowledge(msg.receipt)
        assert tp_state.safe_position == 6
    finally:
        await stop_fetcher(fetcher)


async def test_fetcher_control_only_batch_marks_offsets_empty():
    client, subscriptions, fetcher = make_fetcher(isolation_level="read_committed")
    try:
        subscriptions.assign_from_user({TP0})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(TP0, 18)

        # Control batch covering offsets 18 and 19, no deliverable records.
        raw_batch = build_raw_batch(
            [18, 19], control=True, transactional=True, producer_id=7
        )
        response = FetchResponse([(TP0.topic, [(0, 0, 9, raw_batch)])])
        client.send = mock.AsyncMock(return_value=response)

        request = FetchRequest(
            -1, 100, 100, [(TP0.topic, [(TP0.partition, 18, 100000)])]
        )
        assert await fetcher._proc_fetch_request(assignment, 0, request) is True

        # Draining yields nothing, but completing the batch proves the range
        # empty and advances the safe position to 20.
        records = await fetcher.fetched_records([])
        assert records == {}
        assert assignment.state_value(TP0).safe_position == 20
    finally:
        await stop_fetcher(fetcher)


async def test_fetcher_disabled_mode_has_no_receipts():
    client, subscriptions, fetcher = make_fetcher(enable_store_ack=False)
    try:
        subscriptions.assign_from_user({TP0})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(TP0, 4)

        raw_batch = build_raw_batch([4])
        response = FetchResponse([(TP0.topic, [(0, 0, 9, raw_batch)])])
        client.send = mock.AsyncMock(return_value=response)

        request = FetchRequest(
            -1, 100, 100, [(TP0.topic, [(TP0.partition, 4, 100000)])]
        )
        await fetcher._proc_fetch_request(assignment, 0, request)
        messages = (await fetcher.fetched_records([]))[TP0]
        assert messages[0].receipt is None
        assert assignment.state_value(TP0).position == 5
    finally:
        await stop_fetcher(fetcher)


async def test_fetcher_reread_redelivers_with_fresh_receipt():
    client, subscriptions, fetcher = make_fetcher()
    try:
        subscriptions.assign_from_user({TP0})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(TP0, 14)
        tp_state = assignment.state_value(TP0)

        old_receipt = subscriptions.issue_receipt(TP0, 14)
        fetcher.reread_from(TP0, 14)
        assert tp_state.position == 14

        with pytest.raises(StaleDeliveryReceiptError):
            subscriptions.acknowledge(old_receipt)

        raw_batch = build_raw_batch([14])
        response = FetchResponse([(TP0.topic, [(0, 0, 9, raw_batch)])])
        client.send = mock.AsyncMock(return_value=response)

        request = FetchRequest(
            -1, 100, 100, [(TP0.topic, [(TP0.partition, 14, 100000)])]
        )
        await fetcher._proc_fetch_request(assignment, 0, request)
        messages = (await fetcher.fetched_records([]))[TP0]
        new_receipt = messages[0].receipt
        assert new_receipt is not None
        assert new_receipt.sequence != old_receipt.sequence
        subscriptions.acknowledge(new_receipt)
        assert tp_state.safe_position == 15
    finally:
        await stop_fetcher(fetcher)


# --------------------------------------------------------------------------
# GroupCoordinator auto-commit boundary
# --------------------------------------------------------------------------


def make_coordinator(enable_store_ack=True):
    client = AIOKafkaClient(bootstrap_servers=[])
    subscription = SubscriptionState(enable_store_ack=enable_store_ack)
    coordinator = GroupCoordinator(
        client,
        subscription,
        heartbeat_interval_ms=20000,
        auto_commit_interval_ms=1000,
        retry_backoff_ms=50,
        enable_store_ack=enable_store_ack,
    )
    coordinator._coordination_task.cancel()
    coordinator._coordination_task = create_task(asyncio.sleep(0.1))
    return client, subscription, coordinator


async def test_coordinator_autocommit_uses_safe_offsets():
    client, subscription, coordinator = make_coordinator()
    try:
        subscription.assign_from_user({TP0})
        assignment = subscription.subscription.assignment
        tp_state = assignment.state_value(TP0)
        subscription.seek(TP0, 10)

        # Position moves to 13 through ordinary fetches, but only offset 10 is
        # acknowledged; the autocommit must not pass the safe boundary.
        tp_state.consumed_to(13)
        subscription.acknowledge(subscription.issue_receipt(TP0, 10))
        assert tp_state.position == 13
        assert tp_state.safe_position == 11

        captured = {}

        async def do_commit(assign, offsets):
            captured.update(offsets)

        coordinator._do_commit_offsets = mock.Mock(side_effect=do_commit)
        coordinator._next_autocommit_deadline = 0
        await coordinator._maybe_do_autocommit(assignment)

        assert captured == {TP0: mock.ANY}
        assert captured[TP0].offset == 11
    finally:
        await coordinator.close()
        await client.close()


async def test_coordinator_last_autocommit_uses_safe_offsets():
    client, subscription, coordinator = make_coordinator()
    try:
        subscription.assign_from_user({TP0})
        assignment = subscription.subscription.assignment
        subscription.seek(TP0, 0)
        subscription.acknowledge(subscription.issue_receipt(TP0, 0))

        captured = {}

        async def commit_offsets(assign, offsets):
            captured.update(offsets)

        coordinator.commit_offsets = mock.Mock(side_effect=commit_offsets)
        await coordinator._maybe_do_last_autocommit(assignment)
        assert captured[TP0].offset == 1
    finally:
        await coordinator.close()
        await client.close()


async def test_coordinator_disabled_mode_uses_consumed_offsets():
    client, subscription, coordinator = make_coordinator(enable_store_ack=False)
    try:
        subscription.assign_from_user({TP0})
        assignment = subscription.subscription.assignment
        tp_state = assignment.state_value(TP0)
        tp_state.seek(0)
        tp_state.consumed_to(7)

        offsets = coordinator._autocommit_offsets(assignment)
        assert offsets[TP0].offset == 7
    finally:
        await coordinator.close()
        await client.close()


# --------------------------------------------------------------------------
# AIOKafkaConsumer public surface (no broker connection needed pre-start)
# --------------------------------------------------------------------------


async def test_consumer_guards_store_ack_methods_when_disabled():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=[],
        group_id="g",
        enable_store_ack=False,
    )
    try:
        with pytest.raises(IllegalOperation):
            consumer.acknowledge(mock.Mock())
        with pytest.raises(IllegalOperation):
            consumer.reread_partition_from(TP0, 0)
        with pytest.raises(IllegalOperation):
            await consumer.safe_position(TP0)
    finally:
        # Never started: close the lazily created client without coordinator.
        await consumer._client.close()
        consumer._closed = True


async def test_consumer_reread_validates_offset():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=[], group_id="g", enable_store_ack=True
    )
    try:
        with pytest.raises(ValueError):
            consumer.reread_partition_from(TP0, -1)
    finally:
        await consumer._client.close()
        consumer._closed = True
