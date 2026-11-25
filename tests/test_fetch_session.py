"""Tests for KIP-227 incremental fetch sessions.

A controllable in-memory broker records the exact wire fields of every
FetchRequest and serves scripted FetchResponses, so each round can assert
the session id, epoch, changed partitions and forgotten list, as well as
how many records the consumer actually accepted.
"""

import asyncio
import collections
import contextlib
from unittest import mock

from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import FetchSessionIdNotFound
from aiokafka.protocol.fetch import (
    FetchRequest_v4,
    FetchRequest_v7,
    FetchResponse_v4,
    FetchResponse_v7,
)
from aiokafka.record.default_records import (
    _DefaultRecordBatchBuilderPy as DefaultRecordBatchBuilder,
)
from aiokafka.structs import TopicPartition
from aiokafka.util import create_future, create_task

P0 = TopicPartition("topic", 0)
P1 = TopicPartition("topic", 1)
P2 = TopicPartition("topic", 2)

SESSIONLESS = 0


def make_batch(offset, value=b"msg"):
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        batch_size=99999999,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=0,
    )
    builder.append(offset=offset, value=value, key=None, timestamp=None, headers=[])
    return bytes(builder.build())


def fetch_response_v7(session_id, partitions, error_code=0):
    # partitions: {tp: (error_code, highwater, raw_batch)}
    topics = collections.defaultdict(list)
    for tp, (err, hw, batch) in partitions.items():
        topics[tp.topic].append((tp.partition, err, hw, -1, -1, None, batch))
    return FetchResponse_v7(0, error_code, session_id, list(topics.items()))


def empty_partition(tp, hw=0):
    return (0, hw, b"")


def data_partition(tp, offset, hw):
    return (0, hw, make_batch(offset))


class FakeBroker:
    """Records wire fields of each request and replays scripted responses."""

    def __init__(self, api_version=7):
        assert api_version in (4, 7)
        self.api_version = api_version
        self.sent = []  # request structs as dicts, exactly as encoded
        self._responses = collections.deque()

    def queue_response(self, response):
        self._responses.append(response)

    async def send(self, node_id, request):
        if self.api_version >= 7:
            struct = request.build(FetchRequest_v7)
        else:
            struct = request.build(FetchRequest_v4)
        # Encode and decode for real to prove the wire format is valid
        decoded = type(struct).decode(struct.encode())
        self.sent.append(decoded.to_object())
        return self._responses.popleft()

    def wire(self, round_no):
        return self.sent[round_no]


async def make_fetcher(leaders):
    client = AIOKafkaClient(bootstrap_servers=[])
    subscriptions = SubscriptionState()
    fetcher = Fetcher(client, subscriptions, auto_offset_reset="latest")
    # Disable background routine; we drive requests manually
    fetcher._fetch_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await fetcher._fetch_task
    fetcher._fetch_task = create_task(asyncio.sleep(1000000))

    client.cluster = mock.Mock()
    client.cluster.leader_for_partition.side_effect = lambda tp: leaders[tp]

    def force_metadata_update():
        fut = create_future()
        fut.set_result(False)
        return fut

    client.force_metadata_update = mock.MagicMock()
    client.force_metadata_update.side_effect = force_metadata_update
    return fetcher, client, subscriptions


def get_requests(fetcher, assignment):
    fetch_requests, _, _, _, _ = fetcher._get_actions_per_node(assignment)
    return dict(fetch_requests)


async def run_round(fetcher, assignment, brokers):
    """Build requests for all nodes and process the queued responses."""
    requests = get_requests(fetcher, assignment)
    wakeup = {}
    for node_id, req in requests.items():
        wakeup[node_id] = await fetcher._proc_fetch_request(assignment, node_id, req)
    return requests, wakeup


def drain(fetcher):
    """Consume all buffered records, returns {tp: [ConsumerRecord]}."""
    drained = {}
    for tp in list(fetcher._records.keys()):
        res = fetcher._records[tp]
        records = res.getall()
        if records:
            drained[tp] = records
        del fetcher._records[tp]
    return drained


def wire_partitions(wire):
    """{partition: fetch_offset} explicitly listed in the request."""
    return {
        part["partition"]: part.get("fetch_offset", part.get("offset"))
        for topic in wire["topics"]
        for part in topic["partitions"]
    }


def wire_forgotten(wire):
    return {
        (topic["topic"], part)
        for topic in wire["forgotten_topics_data"]
        for part in topic["partitions"]
    }


async def test_incremental_fetch_session_lifecycle():
    leaders = {P0: 1, P1: 1, P2: 1}
    fetcher, client, subscriptions = await make_fetcher(leaders)
    try:
        broker1 = FakeBroker(api_version=7)
        broker2 = FakeBroker(api_version=7)
        brokers = {1: broker1, 2: broker2}

        async def send(node_id, request):
            return await brokers[node_id].send(node_id, request)

        client.send = mock.MagicMock()
        client.send.side_effect = send

        subscriptions.assign_from_user({P0, P1, P2})
        assignment = subscriptions.subscription.assignment
        for tp in (P0, P1, P2):
            subscriptions.seek(tp, 0)

        # -- Round 0: full fetch establishes the session on broker 1 ------
        broker1.queue_response(
            fetch_response_v7(
                555,
                {
                    P0: data_partition(P0, 0, hw=1),
                    P1: empty_partition(P1),
                    P2: empty_partition(P2),
                },
            )
        )
        requests, _ = await run_round(fetcher, assignment, brokers)
        assert set(requests) == {1}
        wire = broker1.wire(0)
        assert wire["session_id"] == SESSIONLESS
        assert wire["session_epoch"] == 0  # INITIAL_EPOCH
        assert wire_partitions(wire) == {0: 0, 1: 0, 2: 0}
        assert wire_forgotten(wire) == set()
        assert fetcher._fetch_sessions[1].session_id == 555

        drained = drain(fetcher)
        assert [r.offset for r in drained[P0]] == [0]
        tp_state = {tp: assignment.state_value(tp) for tp in (P0, P1, P2)}
        assert tp_state[P0].position == 1
        assert tp_state[P1].position == 0
        assert tp_state[P2].position == 0

        # -- Round 1: incremental; P2 moved to broker 2 -------------------
        # P0 advanced (sent), P1 unchanged (not sent), P2 forgotten here and
        # starts an independent session on broker 2.
        leaders[P2] = 2
        # Broker 1 returns P1 data although P1 was not explicitly requested:
        # it is still part of the session.
        broker1.queue_response(
            fetch_response_v7(
                555,
                {
                    P0: empty_partition(P0, hw=1),
                    P1: data_partition(P1, 0, hw=1),
                },
            )
        )
        broker2.queue_response(
            fetch_response_v7(777, {P2: data_partition(P2, 0, hw=1)})
        )
        requests, _ = await run_round(fetcher, assignment, brokers)
        assert set(requests) == {1, 2}

        wire = broker1.wire(1)
        assert wire["session_id"] == 555
        assert wire["session_epoch"] == 1
        assert wire_partitions(wire) == {0: 1}  # only P0 changed
        assert wire_forgotten(wire) == {("topic", 2)}

        wire = broker2.wire(0)
        assert wire["session_id"] == SESSIONLESS
        assert wire["session_epoch"] == 0
        assert wire_partitions(wire) == {2: 0}
        assert wire_forgotten(wire) == set()

        drained = drain(fetcher)
        # P1's in-session record is accepted at the position valid when the
        # request was built
        assert [r.offset for r in drained[P1]] == [0]
        assert [r.offset for r in drained[P2]] == [0]
        assert tp_state[P1].position == 1
        assert tp_state[P2].position == 1
        assert fetcher._fetch_sessions[2].session_id == 777

        # -- Round 2: P1 paused -> forgotten on broker 1 ------------------
        subscriptions.pause(P1)
        broker1.queue_response(fetch_response_v7(555, {P0: empty_partition(P0, hw=1)}))
        broker2.queue_response(fetch_response_v7(777, {P2: empty_partition(P2, hw=1)}))
        await run_round(fetcher, assignment, brokers)

        wire = broker1.wire(2)
        assert wire["session_id"] == 555
        assert wire["session_epoch"] == 2
        assert wire_partitions(wire) == {}  # nothing changed
        assert wire_forgotten(wire) == {("topic", 1)}

        wire = broker2.wire(1)
        assert wire["session_id"] == 777
        assert wire["session_epoch"] == 1
        assert wire_partitions(wire) == {2: 1}  # P2 advanced on its new session
        assert wire_forgotten(wire) == set()
        assert drain(fetcher) == {}

        # -- Round 3: broker 1 dropped the session ------------------------
        broker1.queue_response(
            fetch_response_v7(SESSIONLESS, {}, error_code=FetchSessionIdNotFound.errno)
        )
        broker2.queue_response(fetch_response_v7(777, {P2: empty_partition(P2, hw=1)}))
        _, wakeup = await run_round(fetcher, assignment, brokers)
        assert wakeup[1] is False
        wire = broker1.wire(3)
        assert wire["session_id"] == 555
        assert wire["session_epoch"] == 3
        # Only broker 1's session is rebuilt; positions are untouched
        assert fetcher._fetch_sessions[1].session_id == SESSIONLESS
        assert fetcher._fetch_sessions[2].session_id == 777
        assert tp_state[P0].position == 1
        assert tp_state[P1].position == 1
        assert tp_state[P2].position == 1

        # -- Round 4: full fetch rebuilds broker 1's session only ---------
        broker1.queue_response(fetch_response_v7(999, {P0: empty_partition(P0, hw=1)}))
        broker2.queue_response(fetch_response_v7(777, {P2: empty_partition(P2, hw=1)}))
        await run_round(fetcher, assignment, brokers)
        wire = broker1.wire(4)
        assert wire["session_id"] == SESSIONLESS
        assert wire["session_epoch"] == 0
        # Paused P1 and migrated P2 are not revived by the rebuild
        assert wire_partitions(wire) == {0: 1}
        assert wire_forgotten(wire) == set()
        assert fetcher._fetch_sessions[1].session_id == 999

        # -- Round 5: resuming P1 re-adds it at its current position ------
        subscriptions.resume(P1)
        broker1.queue_response(
            fetch_response_v7(
                999,
                {
                    P0: empty_partition(P0, hw=1),
                    P1: data_partition(P1, 1, hw=2),
                },
            )
        )
        broker2.queue_response(fetch_response_v7(777, {P2: empty_partition(P2, hw=1)}))
        await run_round(fetcher, assignment, brokers)
        wire = broker1.wire(5)
        assert wire["session_id"] == 999
        assert wire["session_epoch"] == 1
        assert wire_partitions(wire) == {1: 1}
        assert wire_forgotten(wire) == set()

        drained = drain(fetcher)
        assert [r.offset for r in drained[P1]] == [1]
        assert tp_state[P1].position == 2

        # -- Late response from P2's old broker is ignored ----------------
        # P2 is no longer in broker 1's session. Even a response whose
        # offset matches P2's current position must not be applied.
        late_response = fetch_response_v7(555, {P2: data_partition(P2, 1, hw=2)})
        broker1.queue_response(late_response)
        stale_req = requests[1]  # any request object addressed to node 1
        needs_wakeup = await fetcher._proc_fetch_request(assignment, 1, stale_req)
        assert needs_wakeup is False
        assert P2 not in fetcher._records
        assert tp_state[P2].position == 1
    finally:
        subscriptions.unsubscribe()
        await fetcher.close()


async def test_full_fetch_preserved_when_broker_has_no_session_support():
    """Brokers without Fetch v7+ never see session parameters and always
    receive the complete partition list."""
    leaders = {P0: 1, P1: 1}
    fetcher, client, subscriptions = await make_fetcher(leaders)
    try:
        broker1 = FakeBroker(api_version=4)

        async def send(node_id, request):
            return await broker1.send(node_id, request)

        client.send = mock.MagicMock()
        client.send.side_effect = send

        subscriptions.assign_from_user({P0, P1})
        assignment = subscriptions.subscription.assignment
        for tp in (P0, P1):
            subscriptions.seek(tp, 0)

        def v4_response(partitions):
            topics = collections.defaultdict(list)
            for tp, (err, hw, batch) in partitions.items():
                topics[tp.topic].append((tp.partition, err, hw, -1, None, batch))
            return FetchResponse_v4(0, list(topics.items()))

        # Round 0: full list, P0 advances
        broker1.queue_response(
            v4_response({P0: data_partition(P0, 0, hw=1), P1: empty_partition(P1)})
        )
        await run_round(fetcher, assignment, {1: broker1})
        wire = broker1.wire(0)
        # v4 schema carries no session fields at all
        assert "session_id" not in wire
        assert "session_epoch" not in wire
        assert "forgotten_topics_data" not in wire
        assert wire_partitions(wire) == {0: 0, 1: 0}
        drained = drain(fetcher)
        assert [r.offset for r in drained[P0]] == [0]

        # Round 1: still the complete list, no session was established
        broker1.queue_response(
            v4_response({P0: empty_partition(P0, hw=1), P1: empty_partition(P1)})
        )
        await run_round(fetcher, assignment, {1: broker1})
        wire = broker1.wire(1)
        assert wire_partitions(wire) == {0: 1, 1: 0}
        assert fetcher._fetch_sessions[1].session_id == SESSIONLESS
    finally:
        subscriptions.unsubscribe()
        await fetcher.close()
