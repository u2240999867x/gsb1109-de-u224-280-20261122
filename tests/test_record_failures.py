"""Tests for per-record deserialization failure handling.

Covers the archive-program scenario:

* P0 holds normal records 20, 21 (and later 22).
* P1 holds a compacted compressed batch with offsets 10, 12, 14, 16 and a
  next batch offset of 19; the gaps were compacted away.
* 10 and 16 decode fine.
* 12's key decodes, but its value needs a new dictionary version.
* 14's key itself is broken.
"""

import asyncio
import contextlib
import itertools
import struct
import unittest
import warnings
from unittest import mock

import pytest

from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.records import RecordDecodeFailure
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import CorruptRecordException, IllegalStateError
from aiokafka.protocol.fetch import FetchRequest_v0 as FetchRequest
from aiokafka.protocol.fetch import FetchResponse_v0 as FetchResponse
from aiokafka.record.default_records import (
    _DefaultRecordBatchBuilderPy as DefaultRecordBatchBuilder,
)
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.record.util import calc_crc32c
from aiokafka.structs import ConsumerRecord, TopicPartition
from aiokafka.util import create_task

from ._testutil import run_until_complete

TOPIC = "archive"
P0 = TopicPartition(TOPIC, 0)
P1 = TopicPartition(TOPIC, 1)

# Offset of the LastOffsetDelta field inside the v2 batch header
LAST_OFFSET_DELTA_OFFSET = struct.calcsize(">qiibIh")


class TrackingKeyDeserializer:
    """Appends the registration id of the current attempt to decoded keys."""

    def __init__(self):
        self.calls = []
        self._counter = itertools.count(1)
        self._current = 0

    def register(self):
        self._current = next(self._counter)
        return self._current

    def __call__(self, raw):
        if raw is None:
            return None
        if raw.startswith(b"BADKEY"):
            raise ValueError(f"broken key: {raw!r}")
        self.calls.append(raw)
        return (raw.decode(), self._current)


class ValueDeserializer:
    def __init__(self):
        self.calls = []
        self.dictionary_loaded = False

    def __call__(self, raw):
        if raw is None:
            return None
        self.calls.append(raw)
        if raw.startswith(b"NEED-DICT") and not self.dictionary_loaded:
            raise ValueError("dictionary version too old")
        return raw.decode()


def build_p1_batch():
    """A gzip-compressed magic-2 batch with compacted-out gaps.

    Offsets 10, 12, 14, 16 are present; the last offset delta is rewritten so
    the batch's next offset is 19 (positions 11, 13, 15, 17, 18 were
    compacted away).
    """
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=1,  # gzip
        batch_size=99999999,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=0,
    )
    payload = b"x" * 256
    builder.append(offset=10, timestamp=0, key=b"k10", value=payload, headers=[])
    builder.append(
        offset=12,
        timestamp=0,
        key=b"k12",
        value=b"NEED-DICT" + payload,
        headers=[],
    )
    builder.append(
        offset=14,
        timestamp=0,
        key=b"BADKEY-14",
        value=payload,
        headers=[],
    )
    builder.append(offset=16, timestamp=0, key=b"k16", value=payload, headers=[])
    raw = bytearray(builder.build())
    # BaseOffset is 0 in a client-built batch; delta 18 makes next_offset 19.
    struct.pack_into(">i", raw, LAST_OFFSET_DELTA_OFFSET, 18)
    # The header rewrite invalidates the CRC, recompute it.
    crc = calc_crc32c(bytes(raw[builder.ATTRIBUTES_OFFSET :]))
    struct.pack_into(">I", raw, builder.CRC_OFFSET, crc)

    records = MemoryRecords(bytes(raw))
    batch = records.next_batch()
    assert batch.compression_type == 1, "test batch must stay compressed"
    assert batch.next_offset == 19
    return bytes(raw)


def build_simple_batch(records):
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        batch_size=99999999,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=0,
    )
    for offset, key, value in records:
        builder.append(offset=offset, timestamp=0, key=key, value=value, headers=[])
    return bytes(builder.build())


@pytest.mark.usefixtures("setup_test_class_serverless")
class TestRecordDecodeFailures(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._cleanup = []

    def tearDown(self):
        super().tearDown()
        for coro, args, kw in reversed(self._cleanup):
            task = asyncio.wait_for(coro(*args, **kw), 30)
            self.loop.run_until_complete(task)

    def add_cleanup(self, cb_or_coro, *args, **kw):
        self._cleanup.append((cb_or_coro, args, kw))

    async def _make_fetcher(self, key_deser, value_deser, handler=True):
        client = AIOKafkaClient(bootstrap_servers=[])
        subscriptions = SubscriptionState()
        fetcher = Fetcher(
            client,
            subscriptions,
            key_deserializer=key_deser,
            value_deserializer=value_deser,
            record_error_handler=handler,
        )
        self.add_cleanup(fetcher.close)
        # Disable the background fetch routine; tests feed responses directly
        fetcher._fetch_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await fetcher._fetch_task
        fetcher._fetch_task = create_task(asyncio.sleep(1000000))
        return fetcher, client, subscriptions

    def _assign(self, subscriptions):
        subscriptions.assign_from_user({P0, P1})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(P0, 20)
        subscriptions.seek(P1, 10)
        return assignment

    async def _feed(self, fetcher, assignment, response, request_offsets=None):
        request_offsets = request_offsets or {0: 20, 1: 10}
        req = FetchRequest(
            -1,  # replica_id
            100,
            100,
            [
                (
                    TOPIC,
                    [
                        (partition, offset, 100000)
                        for partition, offset in request_offsets.items()
                    ],
                )
            ],
        )

        async def send(node, request):
            return response

        fetcher._client.send = mock.MagicMock(side_effect=send)
        await fetcher._proc_fetch_request(assignment, 0, req)

    @run_until_complete
    async def test_full_archive_scenario(self):
        key_deser = TrackingKeyDeserializer()
        value_deser = ValueDeserializer()
        registration = key_deser.register()
        reported = []
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser, handler=reported.append
        )
        assignment = self._assign(subscriptions)

        batch_p1 = build_p1_batch()
        batch_p0 = build_simple_batch([(20, b"k20", b"v20"), (21, b"k21", b"v21")])
        response = FetchResponse(
            [(TOPIC, [(0, 0, 30, batch_p0), (1, 0, 30, batch_p1)])]
        )
        await self._feed(fetcher, assignment, response)

        # First batch read delivers good rows from both partitions, in
        # whichever buffer order; 20, 21 and 10 must all be delivered once.
        first = await fetcher.fetched_records([])
        delivered = [
            (tp.partition, rec.offset)
            for tp, recs in first.items()
            for rec in recs
            if isinstance(rec, ConsumerRecord)
        ]
        assert (0, 20) in delivered
        assert (0, 21) in delivered
        assert (1, 10) in delivered
        # The P1 list stops at the bad row 12, exposed as a failure report
        assert isinstance(first[P1][-1], RecordDecodeFailure)

        p1_result = fetcher._records[P1]
        failure_12 = p1_result.held_failure
        assert isinstance(failure_12, RecordDecodeFailure)
        assert failure_12.failed_part == "value"
        assert failure_12.topic == TOPIC
        assert failure_12.partition == 1
        assert failure_12.offset == 12
        assert failure_12.raw_key == b"k12"
        assert failure_12.raw_value == b"NEED-DICT" + b"x" * 256
        assert isinstance(failure_12.exception, ValueError)
        assert reported[-1] is failure_12
        # Position on P1 is parked at 12; P0 fully consumed
        assert assignment.state_value(P1).position == 12
        assert assignment.state_value(P0).position == 22

        # getone on P1 keeps handing back the same held report, never a new row
        assert await fetcher.next_record([P1]) is failure_12

        # Pause P1 while waiting for the new dictionary; P0 keeps reading.
        subscriptions.pause(P1)
        batch_p0_more = build_simple_batch([(22, b"k22", b"v22")])
        response_22 = FetchResponse([(TOPIC, [(0, 0, 30, batch_p0_more)])])
        await self._feed(fetcher, assignment, response_22, request_offsets={0: 22})
        rec_22 = await fetcher.next_record([P0])
        assert rec_22.offset == 22
        # The paused P1 keeps its already-fetched batch and held report
        assert fetcher._records[P1].held_failure is failure_12
        assert assignment.state_value(P1).position == 12

        # The broker can no longer resend the old P1 batch: while the report
        # is unresolved no P1 fetch may be requested.
        def no_resend(node, request):
            for _topic, partitions in request.topics:
                for partition, *_ in partitions:
                    if partition == 1:
                        raise AssertionError("P1 batch must not be refetched")
            return response_22

        fetcher._client.send = mock.MagicMock(side_effect=no_resend)

        # New dictionary loaded; resume and retry 12. Only the value
        # deserializer runs again.
        value_deser.dictionary_loaded = True
        subscriptions.resume(P1)
        value_calls_before = len(value_deser.calls)
        key_calls_before = len(key_deser.calls)
        rec_12 = await failure_12.retry()
        assert rec_12.offset == 12
        assert rec_12.value.startswith("NEED-DICT")
        assert len(value_deser.calls) == value_calls_before + 1
        # The key was decoded once during the original pass; its registration
        # number is preserved on delivery.
        assert len(key_deser.calls) == key_calls_before
        assert rec_12.key == ("k12", registration)
        assert assignment.state_value(P1).position == 13

        # Continue with the same in-memory batch: next is the 14 key failure.
        failure_14 = await fetcher.next_record([P1])
        assert isinstance(failure_14, RecordDecodeFailure)
        assert failure_14.failed_part == "key"
        assert failure_14.offset == 14
        assert failure_14.raw_key == b"BADKEY-14"
        assert failure_14.raw_value == b"x" * 256

        # Re-issuing the request for 12 via its old report is rejected:
        # a report can only resolve the record it was created for.
        with pytest.raises(IllegalStateError):
            await failure_12.retry()
        with pytest.raises(IllegalStateError):
            await failure_12.skip()

        # 14 can only be skipped through its own report.
        await failure_14.skip()
        assert assignment.state_value(P1).position == 15

        # 16 is delivered exactly once.
        rec_16 = await fetcher.next_record([P1])
        assert rec_16.offset == 16
        assert rec_16.value == "x" * 256
        # After the compacted batch is drained the next position jumps to 19.
        assert fetcher._records[P1].getone() is None
        assert not fetcher._records[P1].has_more()
        assert assignment.state_value(P1).position == 19

        # Every delivered row was decoded exactly once; the retry only
        # re-invoked the value deserializer for 12.
        assert key_deser.calls == [
            b"k20",
            b"k21",
            b"k10",
            b"k12",
            b"k22",
            b"k16",
        ]
        assert value_deser.calls == [
            b"v20",
            b"v21",
            b"x" * 256,
            b"NEED-DICT" + b"x" * 256,
            b"v22",
            b"NEED-DICT" + b"x" * 256,
            b"x" * 256,
        ]

    @run_until_complete
    async def test_getmany_max_records_one(self):
        key_deser = TrackingKeyDeserializer()
        key_deser.register()
        value_deser = ValueDeserializer()
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser
        )
        assignment = self._assign(subscriptions)
        response = FetchResponse(
            [
                (
                    TOPIC,
                    [
                        (
                            0,
                            0,
                            30,
                            build_simple_batch(
                                [(20, b"k20", b"v20"), (21, b"k21", b"v21")]
                            ),
                        ),
                        (1, 0, 30, build_p1_batch()),
                    ],
                )
            ]
        )
        await self._feed(fetcher, assignment, response)

        # max_records=1: every call returns at most one row, but repeated
        # calls still collect all good rows.
        seen = []
        for _ in range(3):
            batch = await fetcher.fetched_records([], max_records=1)
            assert all(len(recs) == 1 for recs in batch.values())
            for tp, recs in batch.items():
                seen.append((tp.partition, recs[0].offset))
        assert sorted(seen) == [(0, 20), (0, 21), (1, 10)]

        # The following call parks the bad row 12 as a failure report.
        batch = await fetcher.fetched_records([], max_records=1)
        (only,) = batch[P1]
        assert isinstance(only, RecordDecodeFailure)
        held = fetcher._records[P1].held_failure
        assert held is only
        assert held.offset == 12 and held.failed_part == "value"

    @run_until_complete
    async def test_failed_retry_keeps_report(self):
        key_deser = TrackingKeyDeserializer()
        key_deser.register()
        value_deser = ValueDeserializer()
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser
        )
        assignment = self._assign(subscriptions)
        response = FetchResponse([(TOPIC, [(1, 0, 30, build_p1_batch())])])
        await self._feed(fetcher, assignment, response)

        assert (await fetcher.next_record([P1])).offset == 10
        failure = await fetcher.next_record([P1])
        assert failure.offset == 12 and failure.failed_part == "value"

        # Dictionary still old: retry raises and the same report stays valid.
        with pytest.raises(ValueError):
            await failure.retry()
        assert fetcher._records[P1].held_failure is failure
        assert assignment.state_value(P1).position == 12

        # After the dictionary reload the very same report retries fine.
        value_deser.dictionary_loaded = True
        rec_12 = await failure.retry()
        assert rec_12.offset == 12
        assert rec_12.value.startswith("NEED-DICT")
        assert fetcher._records[P1].held_failure is None
        assert assignment.state_value(P1).position == 13

    @run_until_complete
    async def test_skip_value_and_key_failures(self):
        key_deser = TrackingKeyDeserializer()
        key_deser.register()
        value_deser = ValueDeserializer()
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser
        )
        assignment = self._assign(subscriptions)
        response = FetchResponse([(TOPIC, [(1, 0, 30, build_p1_batch())])])
        await self._feed(fetcher, assignment, response)

        assert (await fetcher.next_record([P1])).offset == 10
        failure_12 = await fetcher.next_record([P1])
        assert failure_12.failed_part == "value"
        await failure_12.skip()
        assert assignment.state_value(P1).position == 13

        failure_14 = await fetcher.next_record([P1])
        assert failure_14.failed_part == "key" and failure_14.offset == 14
        await failure_14.skip()
        assert assignment.state_value(P1).position == 15

        rec_16 = await fetcher.next_record([P1])
        assert rec_16.offset == 16
        assert fetcher._records[P1].getone() is None
        assert assignment.state_value(P1).position == 19

    @run_until_complete
    async def test_paused_partition_keeps_held_batch(self):
        key_deser = TrackingKeyDeserializer()
        key_deser.register()
        value_deser = ValueDeserializer()
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser
        )
        assignment = self._assign(subscriptions)
        response = FetchResponse(
            [
                (
                    TOPIC,
                    [
                        (
                            0,
                            0,
                            30,
                            build_simple_batch(
                                [(20, b"k20", b"v20"), (21, b"k21", b"v21")]
                            ),
                        ),
                        (1, 0, 30, build_p1_batch()),
                    ],
                )
            ]
        )
        await self._feed(fetcher, assignment, response)
        await fetcher.fetched_records([])  # drain good rows, P1 parks on 12
        failure_12 = fetcher._records[P1].held_failure
        assert failure_12 is not None

        # While paused the partition surfaces nothing and keeps the batch.
        subscriptions.pause(P1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(fetcher.next_record([P1]), timeout=0.1)
        assert fetcher._records[P1].held_failure is failure_12

        # The other partition keeps reading independently.
        batch_p0_more = build_simple_batch([(22, b"k22", b"v22")])
        response_22 = FetchResponse([(TOPIC, [(0, 0, 30, batch_p0_more)])])
        await self._feed(fetcher, assignment, response_22, request_offsets={0: 22})
        rec_22 = await fetcher.next_record([P0])
        assert rec_22.offset == 22
        assert fetcher._records[P1].held_failure is failure_12

        # After resume the same in-memory report is available again.
        subscriptions.resume(P1)
        assert await fetcher.next_record([P1]) is failure_12

    @run_until_complete
    async def test_legacy_behavior_without_handler(self):
        key_deser = TrackingKeyDeserializer()
        key_deser.register()
        value_deser = ValueDeserializer()
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser, handler=None
        )
        assignment = self._assign(subscriptions)
        response = FetchResponse([(TOPIC, [(1, 0, 30, build_p1_batch())])])
        await self._feed(fetcher, assignment, response)

        # 10 comes through, then the value error on 12 propagates and kills
        # the buffered batch (it would have to be refetched from the broker).
        assert (await fetcher.next_record([P1])).offset == 10
        with pytest.raises(ValueError):
            await fetcher.next_record([P1])
        assert fetcher._records[P1].held_failure is None

    @run_until_complete
    async def test_crc_failure_has_no_per_record_entry(self):
        key_deser = TrackingKeyDeserializer()
        key_deser.register()
        value_deser = ValueDeserializer()
        fetcher, _client, subscriptions = await self._make_fetcher(
            key_deser, value_deser
        )
        assignment = self._assign(subscriptions)

        corrupt = bytearray(build_p1_batch())
        corrupt[-1] ^= 0xFF  # invalidate the batch CRC
        response = FetchResponse([(TOPIC, [(1, 0, 30, bytes(corrupt))])])
        await self._feed(fetcher, assignment, response)

        # Batch-level corruption keeps raising and offers no skip/retry API.
        with pytest.raises(CorruptRecordException):
            await fetcher.next_record([P1])
        assert fetcher._records[P1].held_failure is None

    @run_until_complete
    async def test_consumer_constructor_wires_handler(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            consumer = AIOKafkaConsumer(
                bootstrap_servers=[],
                consumer_record_error_handler=True,
            )
        try:
            assert consumer._consumer_record_error_handler is True
        finally:
            consumer._closed = True  # silence the unclosed-connector warning
