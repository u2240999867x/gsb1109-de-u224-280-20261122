import asyncio
import collections
from unittest import mock

import pytest

from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.consumer.fetcher import (
    READ_UNCOMMITTED,
    Fetcher,
    FetchResult,
    OffsetResetStrategy,
    PartitionRecords,
)
from aiokafka.protocol.fetch import FetchResponse_v0
from aiokafka.protocol.offset import OffsetResponse_v1
from aiokafka.record.default_records import (
    _DefaultRecordBatchBuilderPy as DefaultRecordBatchBuilder,
)
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import TopicPartition

TOPIC = "topic"
LEADER_A = 0
LEADER_B = 1


class FakeBroker:
    """Stand-in for the network boundary of the Fetcher.

    ListOffsets replies are gated by per-request events so a test can decide
    when each lookup result is released. Several lookups for the same
    partition queue up and each consume the next queued gate in order.
    Fetch requests get empty responses, which simply make the partition
    fetchable again on the next routine spin.
    """

    def __init__(self, leaders):
        self.leaders = leaders
        # (node_id, tp) -> deque of (release_event, offset)
        self._gates = collections.defaultdict(collections.deque)
        self.request_log = []

    def gate(self, node_id, tp, offset, event=None):
        event = event or asyncio.Event()
        self._gates[(node_id, tp)].append((event, offset))
        return event

    async def send(self, node_id, request):
        self.request_log.append((node_id, request))
        if getattr(request, "API_KEY", None) == 2:
            return await self._handle_offset_request(node_id, request)
        # Pace the otherwise tight empty-fetch loop a little.
        await asyncio.sleep(0.01)
        return self._handle_fetch_request(request)

    @staticmethod
    def _topics_of(request):
        topics = getattr(request, "topics", None)
        return request._topics if topics is None else topics

    async def _handle_offset_request(self, node_id, request):
        topics = []
        for topic, partitions in self._topics_of(request):
            part_data = []
            for partition, _timestamp in partitions:
                tp = TopicPartition(topic, partition)
                event, offset = self._gates[(node_id, tp)].popleft()
                await event.wait()
                part_data.append((partition, 0, -1, offset))
            topics.append((topic, part_data))
        return OffsetResponse_v1(topics)

    def _handle_fetch_request(self, request):
        topics = []
        for topic, partitions in self._topics_of(request):
            part_data = [
                (partition, 0, 100000, b"") for partition, *_ in partitions
            ]
            topics.append((topic, part_data))
        return FetchResponse_v0(topics)

    def offset_requests_for(self, tp):
        result = []
        for node_id, request in self.request_log:
            if getattr(request, "API_KEY", None) != 2:
                continue
            for _topic, partitions in self._topics_of(request):
                if any(partition == tp.partition for partition, *_ in partitions):
                    result.append((node_id, request))
        return result

    def fetch_offsets_for(self, tp):
        offsets = []
        for _node_id, request in self.request_log:
            if getattr(request, "API_KEY", None) == 2:
                continue
            for _topic, partitions in self._topics_of(request):
                for partition, position, *_ in partitions:
                    if partition == tp.partition:
                        offsets.append(position)
        return offsets


def _buffered_result(tp, assignment, start_offset, values):
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        batch_size=99999999,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=0,
    )
    for offset, value in enumerate(values, start=start_offset):
        builder.append(
            offset=offset, value=value, key=None, timestamp=None, headers=[]
        )
    records = MemoryRecords(bytes(builder.build()))
    partition_records = PartitionRecords(
        tp,
        records,
        [],
        start_offset,
        None,
        None,
        True,
        READ_UNCOMMITTED,
    )
    return FetchResult(
        tp,
        assignment=assignment,
        partition_records=partition_records,
        backoff=0,
    )


async def _wait_until(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached in time")
        await asyncio.sleep(0.01)


class _StubCoordinator:
    def check_errors(self):
        return None

    async def close(self):
        return None


async def _build_consumer(partitions, broker):
    consumer = AIOKafkaConsumer(group_id=None, bootstrap_servers=[])
    consumer.assign(list(partitions))

    cluster = mock.Mock()
    cluster.leader_for_partition.side_effect = lambda tp: broker.leaders[tp]
    cluster.broker_metadata.return_value = mock.Mock()
    consumer._client.cluster = cluster
    consumer._client.send = broker.send

    # Give every partition a valid position before the fetcher starts, so its
    # first spin does not issue auto-reset ListOffsets requests.
    assignment = consumer._subscription.subscription.assignment
    for tp in partitions:
        assignment.state_value(tp).seek(0)

    consumer._fetcher = Fetcher(consumer._client, consumer._subscription)
    consumer._coordinator = _StubCoordinator()

    # Let the background routine complete its first spin, which clears any
    # buffered records. Test state is only seeded after this point.
    await asyncio.sleep(0.05)
    return consumer


@pytest.fixture
def partitions():
    return (
        TopicPartition(TOPIC, 0),
        TopicPartition(TOPIC, 1),
        TopicPartition(TOPIC, 2),
    )


@pytest.mark.asyncio
async def test_concurrent_seek_supersession_and_cancellation(partitions):
    p0, p1, p2 = partitions
    broker = FakeBroker({p0: LEADER_A, p1: LEADER_A, p2: LEADER_B})
    consumer = await _build_consumer(partitions, broker)
    try:
        assignment = consumer._subscription.subscription.assignment
        state0 = assignment.state_value(p0)
        state1 = assignment.state_value(p1)
        state2 = assignment.state_value(p2)

        # P0 has prefetched offsets 40 and 41, not yet handed to the user.
        state0.seek(40)
        buffered = _buffered_result(p0, assignment, 40, [b"40", b"41"])
        consumer._fetcher._records[p0] = buffered
        assert consumer._fetcher._buffered_position(p0) == 40

        first_release = asyncio.Event()
        broker.gate(LEADER_A, p0, 30, first_release)
        broker.gate(LEADER_A, p1, 70, first_release)
        p2_end_release = broker.gate(LEADER_B, p2, 90)
        p0_end_release = broker.gate(LEADER_A, p0, 120)

        # First call: seek P0 and P1 to their beginning, one shared request.
        first_call = asyncio.ensure_future(
            consumer.seek_to_beginning(p0, p1)
        )
        await _wait_until(
            lambda: len(broker.offset_requests_for(p0)) >= 1
            and len(broker.offset_requests_for(p1)) >= 1
        )

        # Second call supersedes the P0 part of the first one.
        second_call = asyncio.ensure_future(consumer.seek_to_end(p0, p2))
        await _wait_until(lambda: broker.offset_requests_for(p2))

        # Release the first lookup: P1 moves to 70, the stale P0 result (30)
        # must be discarded because the second reset superseded it.
        first_release.set()
        result = await asyncio.wait_for(first_call, timeout=2.0)
        assert result is False
        assert state1.position == 70
        assert not state0.has_valid_position
        assert state0.awaiting_reset

        # The second call moves P2 to 90 while its P0 lookup stays held.
        p2_end_release.set()
        await _wait_until(lambda: state2.has_valid_position)
        assert state2.position == 90
        assert await consumer.position(p2) == 90
        assert not second_call.done()

        # Cancelling the second call must restore P0's buffered read point
        # instead of letting the held end-offset reply move it later.
        second_call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second_call

        assert state0.has_valid_position
        assert state0.position == 40
        assert await consumer.position(p0) == 40
        # The prefetched records are served again from the local buffer.
        assert consumer._fetcher._records.get(p0) is buffered
        assert not consumer._fetcher._shelved_records

        # Now release the held end offset (120): it must be ignored.
        p0_end_release.set()
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert state0.position == 40
        assert not state0.awaiting_reset

        # Consumption continues with the buffered records, never visiting 30
        # or 120, even though the broker could no longer retain them.
        assert (await consumer.getone()).offset == 40
        assert (await consumer.getone()).offset == 41
        assert await consumer.position(p0) == 42
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_cancelled_seek_with_manual_seek_uses_manual_offset(partitions):
    p0, _p1, p2 = partitions
    broker = FakeBroker({p0: LEADER_A, _p1: LEADER_A, p2: LEADER_B})
    consumer = await _build_consumer(partitions, broker)
    try:
        assignment = consumer._subscription.subscription.assignment
        state0 = assignment.state_value(p0)
        state2 = assignment.state_value(p2)

        state0.seek(40)
        buffered = _buffered_result(p0, assignment, 40, [b"40", b"41"])
        consumer._fetcher._records[p0] = buffered

        p2_end_release = broker.gate(LEADER_B, p2, 90)
        p0_end_release = broker.gate(LEADER_A, p0, 120)

        second_call = asyncio.ensure_future(consumer.seek_to_end(p0, p2))
        await _wait_until(lambda: broker.offset_requests_for(p2))

        p2_end_release.set()
        await _wait_until(lambda: state2.has_valid_position)
        assert state2.position == 90

        # A manual seek before cancellation wins over the buffered restore.
        consumer.seek(p0, 55)
        assert state0.position == 55
        assert p0 not in consumer._fetcher._records

        second_call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second_call

        p0_end_release.set()
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert state0.position == 55
        assert await consumer.position(p0) == 55

        # The next fetch is issued from the manually seeked position.
        await _wait_until(lambda: 55 in broker.fetch_offsets_for(p0))
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_single_successful_seek_to_beginning_returns_true(partitions):
    p0, _p1, _p2 = partitions
    broker = FakeBroker({p0: LEADER_A, _p1: LEADER_A, _p2: LEADER_B})
    consumer = await _build_consumer(partitions, broker)
    try:
        assignment = consumer._subscription.subscription.assignment
        assignment.state_value(p0).seek(0)
        release = broker.gate(LEADER_A, p0, 30)

        call = asyncio.ensure_future(consumer.seek_to_beginning(p0))
        await _wait_until(lambda: broker.offset_requests_for(p0))
        assert not call.done()

        release.set()
        assert await asyncio.wait_for(call, timeout=2.0) is True
        assert await consumer.position(p0) == 30
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_single_successful_seek_to_end_returns_true(partitions):
    p0, _p1, _p2 = partitions
    broker = FakeBroker({p0: LEADER_A, _p1: LEADER_A, _p2: LEADER_B})
    consumer = await _build_consumer(partitions, broker)
    try:
        assignment = consumer._subscription.subscription.assignment
        assignment.state_value(p0).seek(0)
        release = broker.gate(LEADER_A, p0, 120)

        call = asyncio.ensure_future(consumer.seek_to_end(p0))
        await _wait_until(lambda: broker.offset_requests_for(p0))
        release.set()

        assert await asyncio.wait_for(call, timeout=2.0) is True
        assert await consumer.position(p0) == 120
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_reset_generation_rejects_stale_result(partitions):
    p0, _p1, _p2 = partitions
    broker = FakeBroker({p0: LEADER_A, _p1: LEADER_A, _p2: LEADER_B})
    consumer = await _build_consumer(partitions, broker)
    try:
        assignment = consumer._subscription.subscription.assignment
        state0 = assignment.state_value(p0)
        state0.seek(40)

        first_gate = broker.gate(LEADER_A, p0, 30)
        second_gate = broker.gate(LEADER_A, p0, 120)

        first_call = asyncio.ensure_future(consumer.seek_to_beginning(p0))
        await _wait_until(lambda: len(broker.offset_requests_for(p0)) == 1)
        first_generation = state0.reset_generation

        # The superseding reset bumps the generation at once, even though the
        # fetcher can only issue its ListOffsets request once the in-flight
        # one for this leader returns.
        second_call = asyncio.ensure_future(consumer.seek_to_end(p0))
        await _wait_until(
            lambda: state0.reset_generation == first_generation + 1
        )
        assert len(broker.offset_requests_for(p0)) == 1

        # The held earliest reply arrives after the superseding reset and must
        # not move the read point; the first call then resolves to False.
        first_gate.set()
        result = await asyncio.wait_for(first_call, timeout=2.0)
        assert result is False
        assert not state0.has_valid_position
        assert not second_call.done()

        # Its return lets the fetcher issue the superseding latest lookup.
        await _wait_until(lambda: len(broker.offset_requests_for(p0)) == 2)
        second_gate.set()
        assert await asyncio.wait_for(second_call, timeout=2.0) is True
        assert state0.position == 120
        assert state0.reset_strategy is None
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_topic_partition_state_restore_point():
    from aiokafka.consumer.subscription_state import (
        PartitionStatus,
        SubscriptionState,
    )

    subscriptions = SubscriptionState()
    tp = TopicPartition(TOPIC, 0)
    subscriptions.assign_from_user({tp})
    state = subscriptions.subscription.assignment.state_value(tp)

    state.seek(40)
    state.await_reset(OffsetResetStrategy.EARLIEST, restore_position=40)
    assert state.awaiting_reset
    assert not state.has_valid_position
    assert state.restore_position == 40

    assert state.restore_pending_reset() == 40
    assert state.position == 40
    assert state._status is PartitionStatus.CONSUMING
    assert not state.awaiting_reset

    # A stale generation cannot overwrite a newer position.
    state.await_reset(OffsetResetStrategy.LATEST, restore_position=40)
    generation = state.reset_generation
    state.await_reset(OffsetResetStrategy.EARLIEST, restore_position=40)
    assert state.reset_to(120, generation=generation) is False
    assert state.reset_to(30, generation=state.reset_generation) is True
    assert state.position == 30
