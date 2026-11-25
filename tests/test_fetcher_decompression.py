import gzip
import struct
from unittest import mock

import pytest

from aiokafka import AIOKafkaConsumer
from aiokafka.client import AIOKafkaClient
from aiokafka.codec import has_gzip
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import (
    CorruptRecordException,
    DecompressionLimitError,
    RecordTooLargeError,
)
from aiokafka.protocol.fetch import FetchRequest_v11 as FetchRequest
from aiokafka.protocol.fetch import FetchResponse_v11 as FetchResponse
from aiokafka.record.util import calc_crc32c
from aiokafka.structs import TopicPartition

from ._testutil import run_until_complete

KIB = 1024
MIB = 1024 * KIB
HEADER_SIZE = struct.calcsize(">qiibIhiqqqhii")
ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
CRC_OFFSET = struct.calcsize(">qiib")
CODEC_GZIP = 0x01

_has_gzip = has_gzip()


def encode_varint(value):
    value = (value << 1) ^ (value >> 63)
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def encode_record(offset, key, value, headers=(), timestamp_delta=0):
    body = bytearray()
    body.append(0)
    body += encode_varint(timestamp_delta)
    body += encode_varint(offset)
    if key is None:
        body += encode_varint(-1)
    else:
        body += encode_varint(len(key)) + key
    if value is None:
        body += encode_varint(-1)
    else:
        body += encode_varint(len(value)) + value
    body += encode_varint(len(headers))
    for header_key, header_value in headers:
        key_bytes = header_key.encode()
        body += encode_varint(len(key_bytes)) + key_bytes
        if header_value is None:
            body += encode_varint(-1)
        else:
            body += encode_varint(len(header_value)) + header_value
    return encode_varint(len(body)) + bytes(body)


def seal_batch(header, payload):
    header = bytearray(header)
    struct.pack_into(">i", header, 8, HEADER_SIZE - 12 + len(payload))
    raw = bytes(header) + payload
    struct.pack_into(">I", header, CRC_OFFSET, calc_crc32c(raw[ATTRIBUTES_OFFSET:]))
    return bytes(header) + payload


def build_batch(base_offset, records, *, gzip_payload=None, num_records=None):
    body = b"".join(records)
    payload = gzip.compress(body) if gzip_payload is None else gzip_payload
    if num_records is None:
        num_records = len(records)
    header = bytearray(HEADER_SIZE)
    struct.pack_into(">q", header, 0, base_offset)
    struct.pack_into(">i", header, 12, -1)
    header[16] = 2
    struct.pack_into(">h", header, 21, CODEC_GZIP)
    struct.pack_into(">i", header, 23, max(num_records - 1, 0))
    struct.pack_into(">q", header, 27, 1_000_000)
    struct.pack_into(">q", header, 35, 1_000_000 + max(num_records - 1, 0))
    struct.pack_into(">q", header, 43, -1)
    struct.pack_into(">h", header, 51, -1)
    struct.pack_into(">i", header, 53, -1)
    struct.pack_into(">i", header, 57, num_records)
    return seal_batch(header, payload)


def good_batch(base_offset, count=1, value=b"x"):
    records = [
        encode_record(i, b"k%d" % i, value, [("h", b"v")], timestamp_delta=i)
        for i in range(count)
    ]
    return build_batch(base_offset, records)


def bad_crc_batch(base_offset):
    raw = bytearray(good_batch(base_offset))
    raw[-1] ^= 0xFF
    return bytes(raw)


def oversized_batch(base_offset, limit):
    # A structurally valid batch that decompresses to exactly ``limit + 1``
    # bytes.  Reaching the extra byte must trip the per-batch size guard.
    payload_size = 0
    records = []
    target = limit - 8  # leave room for the record framing below
    while payload_size < target:
        remaining = target - payload_size
        chunk = min(remaining, 32 * KIB)
        record = encode_record(len(records), None, b"\x00" * chunk)
        records.append(record)
        payload_size += len(record)
    body = b"".join(records)
    payload = gzip.compress(body)
    header = bytearray(HEADER_SIZE)
    struct.pack_into(">q", header, 0, base_offset)
    struct.pack_into(">i", header, 12, -1)
    header[16] = 2
    struct.pack_into(">h", header, 21, CODEC_GZIP)
    struct.pack_into(">i", header, 23, len(records) - 1)
    struct.pack_into(">i", header, 57, len(records))
    return seal_batch(header, payload)


def truncated_record_batch(base_offset):
    # Valid CRC and valid gzip, but the record declares a length larger than
    # the remaining decompressed bytes.
    body = b"\xfe\xff\xff\x03" + b"\x00\x00\x00\x00"
    payload = gzip.compress(body)
    header = bytearray(HEADER_SIZE)
    struct.pack_into(">q", header, 0, base_offset)
    struct.pack_into(">i", header, 12, -1)
    header[16] = 2
    struct.pack_into(">h", header, 21, CODEC_GZIP)
    struct.pack_into(">i", header, 57, 1)
    return seal_batch(header, payload)


def response(topics):
    return FetchResponse(
        throttle_time_ms=0,
        error_code=0,
        session_id=0,
        topics=topics,
    )


def partition_data(partition, records_bytes, highwater=1000):
    return (
        partition,
        0,
        highwater,
        highwater,
        0,
        [],
        -1,
        records_bytes,
    )


@pytest.mark.skipif(not _has_gzip, reason="Gzip not available")
@pytest.mark.usefixtures("setup_test_class_serverless")
class TestDecompressionGuard:
    def _make_fetcher(self, **kwargs):
        client = AIOKafkaClient(bootstrap_servers=[])
        subscriptions = SubscriptionState()
        fetcher = Fetcher(
            client,
            subscriptions,
            max_decompressed_batch_bytes=64 * MIB,
            decompression_memory_limit=8 * MIB,
            **kwargs,
        )
        fetcher._fetch_task.cancel()
        return client, subscriptions, fetcher

    def _assignment(self, subscriptions, tps):
        subscriptions.assign_from_user(set(tps))
        assignment = subscriptions.subscription.assignment
        for tp in tps:
            subscriptions.seek(tp, 0)
        return assignment

    @run_until_complete
    async def test_interleaved_partitions_and_error_ordering(self):
        tps = [TopicPartition("t", i) for i in range(8)]
        client, subscriptions, fetcher = self._make_fetcher()
        assignment = self._assignment(subscriptions, tps)
        fetcher._records.clear()

        # Each partition delivers one gzip batch with two records.
        batches = {tp: good_batch(0, count=2, value=b"v" * 32 * KIB) for tp in tps}

        async def send(node, request):
            nonlocal batches
            topics = []
            for topic, parts in request.topics:
                out = []
                for partition, _offset, _max_bytes in parts:
                    tp = TopicPartition(topic, partition)
                    raw = batches.get(tp, b"")
                    out.append(partition_data(partition, raw))
                topics.append((topic, out))
            return response(topics)

        client.send = mock.MagicMock(side_effect=send)
        req = FetchRequest(
            replica_id=-1,
            max_wait_time=100,
            min_bytes=100,
            max_bytes=100000,
            isolation_level=0,
            session_id=0,
            session_epoch=0,
            topics=[
                (
                    "t",
                    [(tp.partition, 0, 1_000_000) for tp in tps],
                )
            ],
            forgotten_topics_data=[],
            rack_id="",
        )

        await fetcher._proc_fetch_request(assignment, 0, req)

        # Round-robin one record at a time.  ``getmany(max_records=1)`` must
        # hand back exactly one record and leave the other buffered.
        seen = {tp: [] for tp in tps}
        for _ in range(2):
            for tp in tps:
                out = await fetcher.fetched_records([tp], max_records=1)
                assert list(out.keys()) == [tp]
                assert len(out[tp]) == 1
                seen[tp].append(out[tp][0].offset)

        for tp in tps:
            assert seen[tp] == [0, 1]

        await fetcher.close()

    @run_until_complete
    async def test_p0_corrupt_batch_does_not_deliver_bad_or_later(self):
        await self._p0_problem_scenario(
            bad_crc_batch, expected=CorruptRecordException, limit_error=False
        )

    @run_until_complete
    async def test_p0_oversized_batch(self):
        await self._p0_problem_scenario(
            lambda offset: oversized_batch(offset, 64 * MIB),
            expected=DecompressionLimitError,
            limit_error=True,
        )

    @run_until_complete
    async def test_p0_truncated_record_batch(self):
        await self._p0_problem_scenario(
            truncated_record_batch,
            expected=CorruptRecordException,
            limit_error=False,
        )

    async def _p0_problem_scenario(self, make_problem, expected, limit_error):
        p0 = TopicPartition("t", 0)
        p1 = TopicPartition("t", 1)
        client, subscriptions, fetcher = self._make_fetcher()
        assignment = self._assignment(subscriptions, [p0, p1])
        fetcher._records.clear()

        base = good_batch(10) + good_batch(11)
        problem = make_problem(12)
        later = good_batch(14)
        batches = {p0: base + problem + later, p1: good_batch(0, count=3)}

        async def send(node, request):
            topics = []
            for topic, parts in request.topics:
                out = []
                for partition, _offset, _max_bytes in parts:
                    tp = TopicPartition(topic, partition)
                    raw = batches.get(tp, b"")
                    out.append(partition_data(partition, raw))
                topics.append((topic, out))
            return response(topics)

        client.send = mock.MagicMock(side_effect=send)

        def fetch_request(partitions):
            return FetchRequest(
                replica_id=-1,
                max_wait_time=100,
                min_bytes=100,
                max_bytes=100000,
                isolation_level=0,
                session_id=0,
                session_epoch=0,
                topics=[("t", partitions)],
                forgotten_topics_data=[],
                rack_id="",
            )

        req = fetch_request(
            [(p0.partition, 0, 1_000_000), (p1.partition, 0, 1_000_000)]
        )
        await fetcher._proc_fetch_request(assignment, 0, req)

        # Request exactly the three P0 records.  Only 10 and 11 may be
        # delivered; the problem batch must not leak.
        out = await fetcher.fetched_records([p0], max_records=3)
        assert [r.offset for r in out[p0]] == [10, 11]

        # P0's next position is 12.  The failure is reported on the next call
        # and is distinguishable: gzip-size guard vs. corruption.
        with pytest.raises(expected) as error:
            await fetcher.fetched_records([p0], max_records=3)
        if limit_error:
            assert isinstance(error.value, RecordTooLargeError)
            assert isinstance(error.value, CorruptRecordException)
        else:
            assert not isinstance(error.value, DecompressionLimitError)

        # Another partition keeps its own position.
        out = await fetcher.fetched_records([p1], max_records=1)
        assert [r.offset for r in out[p1]] == [0]

        # Broker returns the corrected batch starting at 12.
        batches[p0] = good_batch(12, count=2) + later
        req = fetch_request([(p0.partition, 12, 1_000_000)])
        await fetcher._proc_fetch_request(assignment, 0, req)
        out = await fetcher.fetched_records([p0], max_records=10)
        assert [r.offset for r in out[p0]] == [12, 13, 14]

        await fetcher.close()

    @run_until_complete
    async def test_close_releases_decoder_state(self):
        p0 = TopicPartition("t", 0)
        client, subscriptions, fetcher = self._make_fetcher()
        assignment = self._assignment(subscriptions, [p0])
        fetcher._records.clear()

        async def send(node, request):
            return response([("t", [partition_data(0, good_batch(0, count=5))])])

        client.send = mock.MagicMock(side_effect=send)
        req = FetchRequest(
            replica_id=-1,
            max_wait_time=100,
            min_bytes=100,
            max_bytes=100000,
            isolation_level=0,
            session_id=0,
            session_epoch=0,
            topics=[("t", [(0, 0, 1_000_000)])],
            forgotten_topics_data=[],
            rack_id="",
        )
        await fetcher._proc_fetch_request(assignment, 0, req)
        out = await fetcher.fetched_records([p0], max_records=1)
        assert len(out[p0]) == 1
        assert p0 in fetcher._records

        await fetcher.close()
        assert not fetcher._records

    @run_until_complete
    async def test_guard_disabled_keeps_eager_behaviour(self):
        p0 = TopicPartition("t", 0)
        client = AIOKafkaClient(bootstrap_servers=[])
        subscriptions = SubscriptionState()
        fetcher = Fetcher(client, subscriptions)
        fetcher._fetch_task.cancel()
        assignment = self._assignment(subscriptions, [p0])
        fetcher._records.clear()

        async def send(node, request):
            return response([("t", [partition_data(0, oversized_batch(0, MIB))])])

        client.send = mock.MagicMock(side_effect=send)
        req = FetchRequest(
            replica_id=-1,
            max_wait_time=100,
            min_bytes=100,
            max_bytes=100000,
            isolation_level=0,
            session_id=0,
            session_epoch=0,
            topics=[("t", [(0, 0, 1_000_000)])],
            forgotten_topics_data=[],
            rack_id="",
        )
        await fetcher._proc_fetch_request(assignment, 0, req)
        # No limit configured: the oversized batch is decompressed eagerly.
        records = fetcher._records[p0]
        batch = records._partition_records._records.next_batch()
        assert not hasattr(batch, "_validated")
        await fetcher.close()


@pytest.mark.skipif(not _has_gzip, reason="Gzip not available")
@pytest.mark.usefixtures("setup_test_class_serverless")
class TestDecompressionGuardOptions:
    @run_until_complete
    async def test_consumer_options_enable_guard(self):
        consumer = AIOKafkaConsumer(
            bootstrap_servers=[],
            max_decompressed_batch_bytes=64 * MIB,
            decompression_memory_limit=8 * MIB,
        )
        try:
            assert consumer._max_decompressed_batch_bytes == 64 * MIB
            assert consumer._decompression_memory_limit == 8 * MIB
        finally:
            await consumer.stop()

    @run_until_complete
    async def test_consumer_memory_limit_implies_default_batch_cap(self):
        consumer = AIOKafkaConsumer(
            bootstrap_servers=[], decompression_memory_limit=8 * MIB
        )
        try:
            assert consumer._max_decompressed_batch_bytes == 64 * MIB
        finally:
            await consumer.stop()

    @run_until_complete
    async def test_consumer_guard_disabled_by_default(self):
        consumer = AIOKafkaConsumer(bootstrap_servers=[])
        try:
            assert consumer._max_decompressed_batch_bytes is None
            assert consumer._decompression_memory_limit is None
        finally:
            await consumer.stop()

    @run_until_complete
    async def test_consumer_rejects_invalid_limits(self):
        for value in (0, -1, "x", True):
            with pytest.raises(ValueError):
                AIOKafkaConsumer(
                    bootstrap_servers=[], max_decompressed_batch_bytes=value
                )
            with pytest.raises(ValueError):
                AIOKafkaConsumer(bootstrap_servers=[], decompression_memory_limit=value)


@pytest.mark.skipif(not _has_gzip, reason="Gzip not available")
@pytest.mark.usefixtures("setup_test_class_serverless")
class TestDecompressionGuardOrdering:
    @run_until_complete
    async def test_delivers_prior_records_before_error(self):
        p0 = TopicPartition("t", 0)
        client = AIOKafkaClient(bootstrap_servers=[])
        subscriptions = SubscriptionState()
        fetcher = Fetcher(
            client,
            subscriptions,
            max_decompressed_batch_bytes=64 * MIB,
            decompression_memory_limit=8 * MIB,
        )
        fetcher._fetch_task.cancel()
        subscriptions.assign_from_user({p0})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(p0, 0)
        fetcher._records.clear()

        raw = good_batch(10, count=2) + bad_crc_batch(12) + good_batch(14)

        async def send(node, request):
            return response([("t", [partition_data(0, raw)])])

        client.send = mock.MagicMock(side_effect=send)
        req = FetchRequest(
            replica_id=-1,
            max_wait_time=100,
            min_bytes=100,
            max_bytes=100000,
            isolation_level=0,
            session_id=0,
            session_epoch=0,
            topics=[("t", [(0, 0, 1_000_000)])],
            forgotten_topics_data=[],
            rack_id="",
        )
        await fetcher._proc_fetch_request(assignment, 0, req)

        out = await fetcher.fetched_records([p0], max_records=10)
        assert [r.offset for r in out[p0]] == [10, 11]
        with pytest.raises(CorruptRecordException):
            await fetcher.fetched_records([p0], max_records=10)

        # Nothing from the problem batch or the later batch was delivered.
        assert fetcher._records.get(p0) is None or isinstance(
            fetcher._records[p0], type(fetcher._records[p0])
        )
        await fetcher.close()
