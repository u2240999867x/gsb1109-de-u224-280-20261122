"""Broker-less end-to-end scenarios for cooperative rebalancing.

A scripted group coordinator drives the real :class:`GroupCoordinator` and
:class:`Fetcher` over several JoinGroup/SyncGroup generations. Every group
response, committed offset and member ownership claim is recorded so the
tests can prove *which generation* each action happened in.

Scenarios:

* cooperative expansion where the old member A keeps P0 (paused) and P1
  (with prefetched, partially delivered records) and only P2 moves to the
  new member B, after a two-phase handover and a deliberately suspended
  revocation callback;
* a second membership change during a later round;
* an eager protocol control sample that keeps the classic revoke-all
  behaviour.
"""

import asyncio
from unittest import mock

import pytest

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import (
    Fetcher,
    FetchResult,
)
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.coordinator.assignors.cooperative import (
    RangeCooperativeAssignor,
    decode_owned_partitions,
)
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.coordinator.protocol import ConsumerProtocol
from aiokafka.errors import NoError
from aiokafka.protocol.commit import (
    OffsetCommitResponse_v2,
    OffsetFetchResponse_v0,
)
from aiokafka.protocol.fetch import FetchResponse_v0 as FetchResponse
from aiokafka.protocol.group import (
    HeartbeatResponse_v0,
    JoinGroupResponse_v0,
    LeaveGroupResponse_v0,
    SyncGroupResponse_v0,
)
from aiokafka.protocol.offset import OffsetResponse_v1
from aiokafka.structs import ConsumerRecord, TopicPartition
from aiokafka.util import create_future

TOPIC = "topic"
P0 = TopicPartition(TOPIC, 0)
P1 = TopicPartition(TOPIC, 1)
P2 = TopicPartition(TOPIC, 2)
ALL = {P0, P1, P2}

NO_ERROR = NoError.errno


class FakePartitionRecords:
    """Minimal PartitionRecords stand-in that tracks the next fetch offset
    as records are delivered, mirroring the real iterator semantics."""

    def __init__(self, records, start_offset):
        self._records = list(records)
        self._index = 0
        self.next_fetch_offset = start_offset

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._records):
            raise StopIteration
        record = self._records[self._index]
        self._index += 1
        self.next_fetch_offset = record.offset + 1
        return record


def make_record(tp, offset):
    return ConsumerRecord(
        topic=tp.topic,
        partition=tp.partition,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=f"msg-{tp.partition}-{offset}".encode(),
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=8,
        headers=[],
    )


class RecordingListener(ConsumerRebalanceListener):
    def __init__(self, revoke_gate=None):
        self.revoked = []
        self.assigned = []
        # Optional future the revoked callback awaits, letting the test hang
        # the revocation mid-rebalance.
        self._revoke_gate = revoke_gate

    async def on_partitions_revoked(self, revoked):
        self.revoked.append(set(revoked))
        if self._revoke_gate is not None:
            await self._revoke_gate

    async def on_partitions_assigned(self, assigned):
        self.assigned.append(set(assigned))


class ScriptedCoordinator:
    """Models just enough broker group coordination for one local member."""

    def __init__(self, assignor, local_member_id, topic):
        self.assignor = assignor
        self.local_member_id = local_member_id
        self.topic = topic
        # member_id -> partitions delivered in the last SyncGroup
        self.delivered = {}
        # member_id -> currently connected (membership snapshot)
        self.members = []
        self.generation = 0
        # Observability, keyed by generation
        self.join_requests = []
        self.sync_assignments = []
        self.committed_by_generation = []
        self._join_index = 0

    def remote_metadata(self, member_id):
        owned = self.delivered.get(member_id, set())
        if hasattr(self.assignor, "metadata_with_ownership"):
            metadata = self.assignor.metadata_with_ownership(
                {self.topic}, owned
            )
        else:
            metadata = self.assignor.metadata({self.topic})
        return metadata.encode()

    async def send(self, node_id, request, group=None):
        key = request.API_KEY
        if key == 11:  # JoinGroup
            return self._handle_join(request)
        if key == 14:  # SyncGroup
            return self._handle_sync(request)
        if key == 12:  # Heartbeat
            return HeartbeatResponse_v0(NO_ERROR)
        if key == 13:  # LeaveGroup
            return LeaveGroupResponse_v0(NO_ERROR)
        if key == 8:  # OffsetCommit
            self._record_commit(request)
            topics = [
                (topic, [(p, NO_ERROR) for p, _off, _meta in parts])
                for topic, parts in request._topics
            ]
            return OffsetCommitResponse_v2(topics)
        if key == 9:  # OffsetFetch: nothing committed
            topics = [
                (
                    topic,
                    [(p, -1, "", NO_ERROR) for p in parts],
                )
                for topic, parts in request._partitions
            ]
            return OffsetFetchResponse_v0(topics)
        if key == 2:  # ListOffsets -> latest offset 0
            topics = [
                (topic, [(p, NO_ERROR, -1, 0) for p, _strategy in parts])
                for topic, parts in request._topics
            ]
            return OffsetResponse_v1(topics)
        if key == 1:  # Fetch -> always empty
            topics = [
                (topic, [(p, NO_ERROR, 0, b"") for p, _off, _max in parts])
                for topic, parts in request.topics
            ]
            return FetchResponse(topics)
        raise AssertionError(f"unexpected request {request!r}")

    def _handle_join(self, request):
        self.generation += 1
        gen = self.generation
        # Record the protocols the local member advertised and what it claims.
        claims = {}
        for name, metadata_bytes in request._group_protocols:
            metadata = ConsumerProtocol.METADATA.decode(metadata_bytes)
            claims[name] = {
                "version": metadata.version,
                "owned": decode_owned_partitions(metadata.user_data),
            }
        self.join_requests.append(
            {
                "generation": gen,
                "member_id": request._member_id,
                "protocols": claims,
            }
        )

        member_entries = []
        for member_id in self.members:
            if member_id == self.local_member_id:
                # The local member's own metadata is the first protocol entry.
                local_name, local_bytes = request._group_protocols[0]
                member_entries.append((member_id, local_bytes))
            else:
                member_entries.append((member_id, self.remote_metadata(member_id)))

        protocol_name = request._group_protocols[0][0]
        return JoinGroupResponse_v0(
            NO_ERROR,
            gen,
            protocol_name,
            self.local_member_id,  # local member is always leader
            self.local_member_id,
            member_entries,
        )

    def _handle_sync(self, request):
        gen = request._generation_id
        assignments = {}
        for member_id, assignment_bytes in request._group_assignment:
            assignment = ConsumerProtocol.ASSIGNMENT.decode(assignment_bytes)
            assignments[member_id] = set(assignment.partitions())
        self.sync_assignments.append({"generation": gen, "assignments": assignments})
        self.delivered.update(assignments)
        local_bytes = dict(request._group_assignment)[self.local_member_id]
        return SyncGroupResponse_v0(NO_ERROR, local_bytes)

    def _record_commit(self, request):
        offsets = {}
        for topic, parts in request._topics:
            for partition, offset, _meta in parts:
                offsets[TopicPartition(topic, partition)] = offset
        self.committed_by_generation.append(
            {
                "generation": request._consumer_group_generation_id,
                "offsets": offsets,
            }
        )


def make_cluster():
    client = AIOKafkaClient(bootstrap_servers=[])
    # Populate cluster metadata without a broker round-trip.
    client.cluster._brokers[0] = mock.Mock(nodeId=0)
    client.cluster._partitions[TOPIC] = {
        p: mock.Mock(leader=0) for p in range(3)
    }
    client.cluster._broker_partitions[0] = set(ALL)
    return client


async def wait_until(predicate, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met before timeout")


async def start_member(assignor, *, cooperative, listener=None):
    client = make_cluster()
    client.ready = mock.AsyncMock(return_value=True)
    client.force_metadata_update = mock.Mock(
        return_value=done_future()
    )
    client.set_topics = mock.Mock(return_value=done_future())
    client._maybe_wait_metadata = mock.AsyncMock(return_value=None)

    subscription = SubscriptionState()
    subscription.subscribe({TOPIC}, listener=listener)

    coordinator = ScriptedCoordinator(assignor, "A", TOPIC)
    coordinator.members = ["A"]
    client.coordinator_lookup = mock.AsyncMock(return_value=0)
    client.send = coordinator.send

    fetcher = Fetcher(client, subscription)
    group = GroupCoordinator(
        client,
        subscription,
        group_id="coop-group",
        assignors=(assignor,),
        cooperative_rebalance=cooperative,
        enable_auto_commit=True,
        auto_commit_interval_ms=10_000,
        heartbeat_interval_ms=30_000,
        retry_backoff_ms=10,
        fetcher=fetcher,
    )
    return client, subscription, fetcher, group, coordinator


def done_future():
    fut = create_future()
    fut.set_result(True)
    return fut


def prime_p1_buffer(fetcher, subscription, records):
    assignment = subscription.subscription.assignment
    partition_records = FakePartitionRecords(records, start_offset=0)
    fetcher._records[P1] = FetchResult(
        P1,
        assignment=assignment,
        partition_records=partition_records,
        backoff=0,
    )
    return partition_records


@pytest.mark.asyncio
async def test_cooperative_two_phase_handover_keeps_state_and_buffers():
    revoke_gate = create_future()
    listener = RecordingListener(revoke_gate=revoke_gate)
    client, subscription, fetcher, group, broker = await start_member(
        RangeCooperativeAssignor, cooperative=True, listener=listener
    )

    try:
        # Generation 1: A owns everything.
        await wait_until(lambda: subscription.assigned_partitions() == ALL)
        assert broker.generation == 1

        # P0 is paused for business reasons; P1 has a prefetched batch.
        subscription.pause(P0)
        subscription.seek(P1, 0)
        p1_records = [make_record(P1, 0), make_record(P1, 1), make_record(P1, 2)]
        prime_p1_buffer(fetcher, subscription, p1_records)

        # Deliver the first buffered record before any rebalance.
        first = await fetcher.next_record([])
        assert first.offset == 0

        # New member B joins; trigger a rejoin.
        broker.members = ["A", "B"]
        group.request_rejoin()

        # Wait until the revocation callback for P2 is parked on the gate.
        await wait_until(lambda: listener.revoked and not revoke_gate.done())
        # The first cooperative round only revokes P2.
        assert listener.revoked[-1] == {P2}

        # While the callback is suspended A still owns all partitions, so P2
        # has not been handed to B yet. B requested no ownership and received
        # nothing in this generation.
        gen2_join = next(j for j in broker.join_requests if j["generation"] == 2)
        assert gen2_join["protocols"][RangeCooperativeAssignor.name]["owned"] == ALL
        gen2_sync = next(s for s in broker.sync_assignments if s["generation"] == 2)
        assert gen2_sync["assignments"]["B"] == set()
        # No partition is owned by two members in the same generation.
        owners = gen2_sync["assignments"]
        all_assigned = [tp for tps in owners.values() for tp in tps]
        assert len(all_assigned) == len(set(all_assigned))
        assert P2 not in all_assigned

        # P0 remains paused and P1 keeps being read from its own undelivered
        # buffer (next record is offset 1, not re-fetched from 0).
        assert subscription.is_assigned(P0)
        second = await fetcher.next_record([])
        assert second.offset == 1
        # P2's (absent here) and P0's state are untouched; P1 buffer survives.
        assert P1 in fetcher._records

        # Release the suspended revocation and let the next generation finish.
        revoke_gate.set_result(None)

        # Generation 3: A no longer claims P2, so B finally receives it.
        await wait_until(
            lambda: any(s["generation"] == 3 for s in broker.sync_assignments)
        )
        gen3_sync = next(s for s in broker.sync_assignments if s["generation"] == 3)
        assert gen3_sync["assignments"]["B"] == {P2}
        assert gen3_sync["assignments"]["A"] == {P0, P1}
        # Still no double ownership.
        owners3 = gen3_sync["assignments"]
        all3 = [tp for tps in owners3.values() for tp in tps]
        assert len(all3) == len(set(all3)) == 3

        # A's retained partitions keep their state across the new assignment.
        assert subscription.assigned_partitions() == {P0, P1}
        assert P0 in subscription.paused_partitions()
        p1_state = subscription.subscription.assignment.state_value(P1)
        assert p1_state.position == 2  # offsets 0 and 1 delivered
        # The retained buffer continues from the undelivered record.
        third = await fetcher.next_record([])
        assert third.offset == 2

        # Retained partitions never re-trigger the assigned callback: A was
        # only notified once (the initial full assignment), never again.
        assert listener.assigned == [ALL]
        # Revocation only ever mentioned P2.
        assert all(event == {P2} for event in listener.revoked)

        # Offset commits carry the generation they happened in, proving the
        # handover spanned distinct rounds.
        commit_generations = {c["generation"] for c in broker.committed_by_generation}
        assert 1 in commit_generations
    finally:
        await group.close()
        await fetcher.close()
        await client.close()


@pytest.mark.asyncio
async def test_cooperative_second_membership_change_readds_partition():
    client, subscription, fetcher, group, broker = await start_member(
        RangeCooperativeAssignor, cooperative=True, listener=RecordingListener()
    )
    try:
        await wait_until(lambda: subscription.assigned_partitions() == ALL)

        # First change: B joins, P2 moves away (two rounds).
        broker.members = ["A", "B"]
        group.request_rejoin()
        await wait_until(
            lambda: any(
                s["generation"] >= 3
                and s["assignments"].get("B") == {P2}
                for s in broker.sync_assignments
            )
        )
        assert subscription.assigned_partitions() == {P0, P1}

        # Second change during a later round: B leaves; A gets P2 back.
        broker.members = ["A"]
        group.request_rejoin()
        await wait_until(lambda: subscription.assigned_partitions() == ALL)

        latest = broker.sync_assignments[-1]
        # A is the only member and owns everything again, including P2 which
        # is a genuinely new assignment for it in this generation.
        assert latest["assignments"] == {"A": ALL}
    finally:
        await group.close()
        await fetcher.close()
        await client.close()


@pytest.mark.asyncio
async def test_eager_control_sample_revokes_everything_and_resets_state():
    listener = RecordingListener()
    client, subscription, fetcher, group, broker = await start_member(
        RangePartitionAssignor, cooperative=False, listener=listener
    )
    try:
        await wait_until(lambda: subscription.assigned_partitions() == ALL)

        subscription.pause(P0)
        subscription.seek(P1, 0)
        prime_p1_buffer(
            fetcher, subscription, [make_record(P1, 0), make_record(P1, 1)]
        )
        assert P1 in fetcher._records

        # B arrives under the eager protocol.
        broker.members = ["A", "B"]
        group.request_rejoin()

        await wait_until(
            lambda: subscription.assigned_partitions() == {P0, P1}
        )

        # Eager prepare revoked the whole previous assignment upfront. The
        # initial join also fires an empty-set revoke callback, so the
        # rebalance revoke is the event containing every partition.
        assert listener.revoked[0] == set()
        assert ALL in listener.revoked
        # The new assignment re-notifies even partitions A effectively keeps.
        assert {P0, P1} in listener.assigned
        # Classic behaviour: fresh partition state, pause and buffers gone.
        assert subscription.paused_partitions() == set()
        assert fetcher._records == {}
        # Range assignor with 3 partitions / 2 members gives B partition 2.
        latest = broker.sync_assignments[-1]["assignments"]
        assert latest["B"] == {P2}
        assert latest["A"] == {P0, P1}
    finally:
        await group.close()
        await fetcher.close()
        await client.close()


@pytest.mark.asyncio
async def test_cooperative_flag_with_eager_assignor_falls_back_to_eager():
    # The cooperative flag must not force cooperative semantics when the only
    # configured assignor is eager. The join metadata carries no ownership
    # payload and a rebalance still revokes the whole assignment.
    listener = RecordingListener()
    client, subscription, fetcher, group, broker = await start_member(
        RangePartitionAssignor, cooperative=True, listener=listener
    )
    try:
        await wait_until(lambda: subscription.assigned_partitions() == ALL)

        # The advertised eager metadata has version 0 and empty user_data.
        join = broker.join_requests[0]
        protocol_claim = join["protocols"][RangePartitionAssignor.name]
        assert protocol_claim["version"] == 0
        assert protocol_claim["owned"] == set()

        subscription.pause(P0)
        broker.members = ["A", "B"]
        group.request_rejoin()
        await wait_until(lambda: subscription.assigned_partitions() == {P0, P1})

        # Eager fallback still revokes everything before the join.
        assert ALL in listener.revoked
        assert subscription.paused_partitions() == set()
    finally:
        await group.close()
        await fetcher.close()
        await client.close()
