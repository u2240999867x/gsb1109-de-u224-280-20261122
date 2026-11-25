import json
import struct

import pytest

from aiokafka.consumer.backfill import (
    AiokafkaBackfillGateway,
    BackfillStatus,
    BackfillToken,
    Gap,
    KafkaBackfill,
    RawFetch,
    RetainedBatch,
)
from aiokafka.protocol.fetch import (
    FetchResponse_v0,
    FetchResponse_v4,
    FetchResponse_v11,
)
from aiokafka.record.default_records import (
    _DefaultRecordBatchBuilderPy as DefaultRecordBatchBuilder,
)
from aiokafka.structs import TopicPartition

TP0 = TopicPartition("archive", 0)
TP1 = TopicPartition("archive", 1)


def build_batch(base_offset, record_offsets, *, compression_type=0, timestamp=1000):
    """Build a magic v2 batch and stamp a broker-like base offset on it.

    The builder only knows in-batch sequence numbers, so records are appended
    with deltas matching their position inside the batch; the absolute base
    offset is written into the header the same way a broker would.
    """
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=compression_type,
        batch_size=99999999,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=0,
    )
    for index, offset in enumerate(record_offsets):
        builder.append(
            offset=offset - base_offset,
            timestamp=timestamp + index,
            key=None,
            value=f"v{offset}".encode(),
            headers=[],
        )
    buffer = bytearray(builder.build())
    struct.pack_into(">q", buffer, 0, base_offset)
    return bytes(buffer)


class ScriptedGateway:
    """In-memory BackfillGateway recording every broker interaction."""

    def __init__(self, earliest, fetch_script, *, end_offset=999):
        if isinstance(earliest, int):
            self._earliest_values = iter([earliest])
            self._earliest_default = earliest
        else:
            self._earliest_values = iter(earliest)
            self._earliest_default = earliest[-1]
        self._fetch_script = dict(fetch_script)
        self._end_offset = end_offset
        self.earliest_calls = []
        self.fetch_calls = []
        self.end_calls = []
        self.commits = 0

    async def earliest_offset(self, tp):
        self.earliest_calls.append(tp)
        try:
            return next(self._earliest_values)
        except StopIteration:
            return self._earliest_default

    async def end_offset(self, tp):
        self.end_calls.append(tp)
        return self._end_offset

    async def fetch(self, tp, offset):
        self.fetch_calls.append(offset)
        response = self._fetch_script[offset]
        if callable(response):
            response = response()
        return response

    async def commit(self, *_args):
        # The backfill must never call this; it exists to make accidental
        # group commits loudly visible in the call log.
        self.commits += 1
        raise AssertionError("backfill must never commit consumer offsets")


def fetch_with(records, *, log_start_offset, highwater=22):
    return RawFetch.records(
        TP0,
        records,
        log_start_offset=log_start_offset,
        highwater=highwater,
    )


@pytest.mark.asyncio
async def test_token_roundtrip_is_json_safe():
    token = BackfillToken(
        topic="archive",
        partition=0,
        start_offset=10,
        end_offset=16,
        position=11,
        retained=RetainedBatch(base_offset=10, next_offset=13, offsets=(12,)),
        gaps=(Gap(12, 13),),
    )

    raw = token.to_json()
    json.loads(raw)
    restored = BackfillToken.from_json(raw)

    assert restored == token
    assert BackfillToken.from_dict(token.to_dict()) == token


@pytest.mark.asyncio
async def test_token_rejects_unknown_version_and_bad_offsets():
    with pytest.raises(ValueError, match="version"):
        BackfillToken.from_dict({"version": 999, "topic": "t", "partition": 0})
    with pytest.raises(ValueError, match="non-negative"):
        BackfillToken.initial(TP0, -1, 5)
    with pytest.raises(ValueError, match="not exceed"):
        BackfillToken.initial(TP0, 10, 9)


@pytest.mark.asyncio
async def test_p1_is_complete_on_first_page_and_stays_closed():
    gateway = ScriptedGateway(
        earliest=40,
        fetch_script={40: fetch_with(build_batch(40, [40, 41]), log_start_offset=40)},
    )
    backfill = await KafkaBackfill.open(gateway, TP1, 40, 40)

    result = await backfill.read_page()
    assert result.status is BackfillStatus.COMPLETE
    assert result.record is None
    assert gateway.fetch_calls == []

    # New transactions becoming readable must not reopen the partition.
    again = await backfill.read_page()
    assert again.status is BackfillStatus.COMPLETE
    assert again.token.finished
    assert gateway.fetch_calls == []


@pytest.mark.asyncio
async def test_full_p0_scenario_delivery_resume_gap_and_completion():
    batch_10_12 = build_batch(10, [10, 12])

    # First process: fetch at 10 returns the compacted batch whose tail is 12.
    gateway = ScriptedGateway(
        earliest=10,
        fetch_script={10: fetch_with(batch_10_12, log_start_offset=10)},
    )
    backfill = await KafkaBackfill.open(gateway, TP0, 10, 16)

    first_page = await backfill.read_page()
    assert first_page.status is BackfillStatus.DELIVERED
    assert first_page.record.offset == 10
    assert first_page.record.value == b"v10"
    assert first_page.token.position == 11
    assert first_page.token.retained == RetainedBatch(
        base_offset=10, next_offset=13, offsets=(12,)
    )
    saved_credential = first_page.token.to_json()
    assert gateway.commits == 0

    # Recovery A: log start advanced to 12; 11 was proven absent by the
    # retained batch, so the next page delivers 12 without reporting 11.
    gateway_a = ScriptedGateway(
        earliest=12,
        fetch_script={12: fetch_with(batch_10_12, log_start_offset=12)},
    )
    backfill_a = KafkaBackfill.resume(
        gateway_a, BackfillToken.from_json(saved_credential)
    )
    page_a = await backfill_a.read_page()
    assert page_a.status is BackfillStatus.DELIVERED
    assert page_a.record.offset == 12
    assert page_a.gaps == ()
    assert page_a.token.position == 13
    assert gateway_a.fetch_calls == [12]
    assert gateway_a.commits == 0

    # Recovery B: same first-page credential, but log start is already 13.
    gateway_b = ScriptedGateway(earliest=13, fetch_script={})
    backfill_b = KafkaBackfill.resume(
        gateway_b, BackfillToken.from_json(saved_credential)
    )
    page_b = await backfill_b.read_page()
    assert page_b.status is BackfillStatus.GAP
    assert page_b.record is None
    assert page_b.gaps == (Gap(12, 13),)
    assert page_b.log_start_offset == 13
    assert page_b.token.position == 13
    assert gateway_b.fetch_calls == []

    # Quiet partition from position 13: empty fetch while the fixed end is 16
    # and the high watermark is 22 stays resumably unconfirmed.
    gateway_quiet = ScriptedGateway(
        earliest=13,
        fetch_script={13: fetch_with(b"", log_start_offset=13, highwater=22)},
    )
    quiet = KafkaBackfill.resume(gateway_quiet, page_a.token)
    quiet_page = await quiet.read_page()
    assert quiet_page.status is BackfillStatus.UNCONFIRMED
    assert quiet_page.token.position == 13
    assert quiet_page.log_start_offset == 13

    # Broker evidence: log still starts at 13, first complete batch at 18.
    gateway_done = ScriptedGateway(
        earliest=13,
        fetch_script={13: fetch_with(build_batch(18, [18, 19]), log_start_offset=13)},
    )
    done = KafkaBackfill.resume(gateway_done, quiet_page.token)
    done_page = await done.read_page()
    assert done_page.status is BackfillStatus.COMPLETE
    assert done_page.record is None
    assert done_page.token.finished
    # Records at 18+ are never delivered to the archive.
    assert gateway_done.fetch_calls == [13]


@pytest.mark.asyncio
async def test_retention_past_fixed_end_reports_unrecoverable_interval():
    batch_10_12 = build_batch(10, [10, 12])
    gateway = ScriptedGateway(
        earliest=10,
        fetch_script={10: fetch_with(batch_10_12, log_start_offset=10)},
    )
    backfill = await KafkaBackfill.open(gateway, TP0, 10, 16)
    first_page = await backfill.read_page()
    assert first_page.record.offset == 10

    gateway_a = ScriptedGateway(
        earliest=12,
        fetch_script={12: fetch_with(batch_10_12, log_start_offset=12)},
    )
    page_12 = await KafkaBackfill.resume(gateway_a, first_page.token).read_page()
    assert page_12.record.offset == 12

    # Retention boundary crosses the fixed end before compaction evidence
    # arrives: [13, 16) must be reported as unrecoverable.
    gateway_gap = ScriptedGateway(earliest=16, fetch_script={})
    gap_page = await KafkaBackfill.resume(gateway_gap, page_12.token).read_page()
    assert gap_page.status is BackfillStatus.GAP
    assert gap_page.gaps == (Gap(13, 16),)
    assert gap_page.log_start_offset == 16
    assert gap_page.token.finished
    assert gateway_gap.fetch_calls == []


@pytest.mark.asyncio
async def test_empty_fetch_without_log_start_evidence_is_unconfirmed():
    # Fetch API older than v5 carries no log start offset; with an empty
    # message set there is no basis to distinguish deletion from skipping.
    gateway = ScriptedGateway(
        earliest=13,
        fetch_script={
            13: RawFetch.records(
                TP0, b"", log_start_offset=None, highwater=22, api_version=4
            )
        },
    )
    token = BackfillToken.initial(TP0, 13, 16)
    result = await KafkaBackfill.resume(gateway, token).read_page()
    assert result.status is BackfillStatus.UNCONFIRMED
    assert result.token.position == 13


@pytest.mark.asyncio
async def test_fixed_end_offset_is_immutable_across_broker_growth():
    gateway = ScriptedGateway(
        earliest=10,
        fetch_script={10: fetch_with(build_batch(10, [10, 12]), log_start_offset=10)},
        end_offset=16,
    )
    backfill = await KafkaBackfill.open(gateway, TP0, 10, 16)
    assert backfill.token.end_offset == 16

    gateway._end_offset = 40
    result = await backfill.read_page()
    assert result.token.end_offset == 16


@pytest.mark.asyncio
async def test_compressed_batch_is_parsed_after_base_offset_stamp():
    compressed = build_batch(10, [10, 12], compression_type=1)
    gateway = ScriptedGateway(
        earliest=10,
        fetch_script={10: fetch_with(compressed, log_start_offset=10)},
    )
    backfill = await KafkaBackfill.open(gateway, TP0, 10, 16)
    page = await backfill.read_page()
    assert page.status is BackfillStatus.DELIVERED
    assert page.record.offset == 10
    assert page.token.retained.offsets == (12,)


@pytest.mark.asyncio
async def test_out_of_range_is_retried_through_log_start_classification():
    gateway = ScriptedGateway(
        earliest=[10, 12],
        fetch_script={
            11: RawFetch.out_of_range(TP0),
            12: fetch_with(build_batch(10, [10, 12]), log_start_offset=12),
        },
    )
    token = BackfillToken(
        topic=TP0.topic,
        partition=TP0.partition,
        start_offset=10,
        end_offset=16,
        position=11,
        retained=RetainedBatch(10, 13, (12,)),
    )
    page = await KafkaBackfill.resume(gateway, token).read_page()
    assert page.status is BackfillStatus.DELIVERED
    assert page.record.offset == 12
    assert page.gaps == ()
    # The out-of-range fetch at 11 happened before retention was detected.
    assert gateway.fetch_calls == [11, 12]


@pytest.mark.asyncio
async def test_deserializers_are_applied_to_delivered_record():
    gateway = ScriptedGateway(
        earliest=10,
        fetch_script={10: fetch_with(build_batch(10, [10]), log_start_offset=10)},
    )
    backfill = await KafkaBackfill.open(
        gateway,
        TP0,
        10,
        16,
        value_deserializer=lambda value: value.decode().upper(),
    )
    page = await backfill.read_page()
    assert page.record.value == "V10"


def test_gateway_parses_v11_fetch_response_log_start():
    message_set = build_batch(18, [18])
    response = FetchResponse_v11(
        throttle_time_ms=0,
        error_code=0,
        session_id=0,
        topics=[
            (
                "archive",
                [
                    (
                        0,  # partition
                        0,  # error code
                        22,  # highwater
                        20,  # last stable offset
                        13,  # log start offset
                        [],  # aborted transactions
                        -1,  # preferred read replica
                        message_set,
                    )
                ],
            )
        ],
    )

    gateway = AiokafkaBackfillGateway.__new__(AiokafkaBackfillGateway)
    raw = gateway._parse_fetch_response(TP0, response)
    assert raw.log_start_offset == 13
    assert raw.highwater == 22
    assert raw.api_version == 11
    assert raw.message_set == message_set
    assert raw.aborted_transactions == ()


def test_gateway_parses_legacy_v0_and_v4_responses():
    message_set = build_batch(10, [10])

    v0 = FetchResponse_v0([("archive", [(0, 0, 22, message_set)])])
    raw_v0 = AiokafkaBackfillGateway.__new__(
        AiokafkaBackfillGateway
    )._parse_fetch_response(TP0, v0)
    assert raw_v0.api_version == 0
    assert raw_v0.log_start_offset is None
    assert raw_v0.aborted_transactions == ()
    assert raw_v0.message_set == message_set

    v4 = FetchResponse_v4(
        0,  # throttle time
        [
            (
                "archive",
                [
                    (
                        0,  # partition
                        0,  # error code
                        22,  # highwater
                        20,  # last stable offset
                        [],  # aborted transactions
                        message_set,
                    )
                ],
            )
        ],
    )
    raw_v4 = AiokafkaBackfillGateway.__new__(
        AiokafkaBackfillGateway
    )._parse_fetch_response(TP0, v4)
    assert raw_v4.api_version == 4
    assert raw_v4.log_start_offset is None
    assert raw_v4.aborted_transactions == ()
    assert raw_v4.message_set == message_set


def test_classify_deleted_separates_evidence_from_retention():
    backfill = KafkaBackfill.__new__(KafkaBackfill)
    retained = RetainedBatch(base_offset=10, next_offset=13, offsets=(12,))

    # 11 is inside the fetched batch but absent (compaction): not a gap.
    assert backfill._classify_deleted(11, 12, retained) == []
    # 12 was present and never delivered: it is a retention gap.
    assert backfill._classify_deleted(11, 13, retained) == [Gap(12, 13)]
    # Beyond batch evidence everything is assumed deleted.
    assert backfill._classify_deleted(13, 16, retained) == [Gap(13, 16)]
    # No evidence at all: the whole jump is a gap.
    assert backfill._classify_deleted(13, 16, None) == [Gap(13, 16)]
