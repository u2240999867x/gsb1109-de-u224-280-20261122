"""Paged Kafka archive backfill with resumable continuation credentials.

The reader walks one topic-partition from a caller supplied start offset up to
a *fixed* end offset captured when the backfill job starts. It never joins a
consumer group, never commits offsets and therefore cannot disturb online
consumers or their group positions.

Four page statuses let callers tell the three interesting situations apart:

* ``DELIVERED`` -- one record was handed to the archive.
* ``COMPLETE`` -- every offset before the fixed end was either delivered or
  proven absent by broker evidence (a complete, valid batch starting past the
  offset while the log still covers that offset, i.e. compaction).
* ``UNCONFIRMED`` -- resumable state: the available evidence cannot tell
  "not written yet" apart from retention deletion (empty fetch, or a broker
  response without a log start offset).
* ``GAP`` -- retention moved the log start over offsets that were not proven
  absent; the deleted half-open interval and the current earliest offset are
  reported. A GAP page is itself resumable unless the fixed end is reached.

Continuation credentials (:class:`BackfillToken`) contain plain JSON-able
metadata, never record payloads, so they can be persisted and handed to a
different process via :meth:`KafkaBackfill.resume`.
"""

from __future__ import annotations

import abc
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from enum import StrEnum

import aiokafka.errors as Errors
from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import READ_UNCOMMITTED
from aiokafka.errors import KafkaError, NodeNotReadyError
from aiokafka.protocol.fetch import FetchRequest
from aiokafka.protocol.offset import OffsetRequest
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import ConsumerRecord, TopicPartition

log = logging.getLogger(__name__)

_TOKEN_VERSION = 1
_DEFAULT_FETCH_MAX_BYTES = 10 * 1024 * 1024
# Internal state machine bound: every iteration strictly advances the
# position, so a runaway loop would indicate a broker/protocol violation.
_MAX_INTERNAL_STEPS = 100

# ListOffsets timestamp constants (see OffsetResetStrategy in the fetcher).
_LIST_OFFSET_EARLIEST = -2
_LIST_OFFSET_LATEST = -1


class BackfillStatus(StrEnum):
    """Lifecycle state reported by :meth:`KafkaBackfill.read_page`."""

    DELIVERED = "delivered"
    COMPLETE = "complete"
    UNCONFIRMED = "unconfirmed"
    GAP = "gap"


@dataclass(frozen=True)
class Gap:
    """A half-open ``[start, end)`` interval lost to retention."""

    start: int
    end: int

    def to_dict(self) -> dict[str, int]:
        return {"start": self.start, "end": self.end}

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> Gap:
        return cls(start=int(data["start"]), end=int(data["end"]))


@dataclass(frozen=True)
class RetainedBatch:
    """Offset evidence of a fully fetched batch not yet fully delivered.

    Only offset metadata is retained so the credential stays payload free.
    ``offsets`` are the record offsets actually present in the batch, pruned
    to offsets at or above the current position.
    """

    base_offset: int
    next_offset: int
    offsets: tuple[int, ...]

    def covers(self, offset: int) -> bool:
        return self.base_offset <= offset < self.next_offset


@dataclass(frozen=True)
class BackfillToken:
    """JSON-serializable continuation credential for one partition."""

    topic: str
    partition: int
    start_offset: int
    end_offset: int
    position: int
    finished: bool = False
    retained: RetainedBatch | None = None
    gaps: tuple[Gap, ...] = ()

    @property
    def topic_partition(self) -> TopicPartition:
        return TopicPartition(self.topic, self.partition)

    def to_dict(self) -> dict[str, object]:
        return {
            "version": _TOKEN_VERSION,
            "topic": self.topic,
            "partition": self.partition,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "position": self.position,
            "finished": self.finished,
            "retained": None
            if self.retained is None
            else {
                "base_offset": self.retained.base_offset,
                "next_offset": self.retained.next_offset,
                "offsets": list(self.retained.offsets),
            },
            "gaps": [gap.to_dict() for gap in self.gaps],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def initial(
        cls, tp: TopicPartition, start_offset: int, end_offset: int
    ) -> BackfillToken:
        if start_offset < 0 or end_offset < 0:
            raise ValueError("offsets must be non-negative")
        if start_offset > end_offset:
            raise ValueError("start_offset must not exceed end_offset")
        return cls(
            topic=tp.topic,
            partition=tp.partition,
            start_offset=start_offset,
            end_offset=end_offset,
            position=start_offset,
            finished=start_offset == end_offset,
        )

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> BackfillToken:
        version = data.get("version")
        if version != _TOKEN_VERSION:
            raise ValueError(f"Unsupported backfill token version: {version!r}")
        retained_data = data.get("retained")
        retained = None
        if isinstance(retained_data, dict):
            retained = RetainedBatch(
                base_offset=int(retained_data["base_offset"]),  # type: ignore[index]
                next_offset=int(retained_data["next_offset"]),  # type: ignore[index]
                offsets=tuple(
                    int(offset)
                    for offset in retained_data.get("offsets", ())  # type: ignore[arg-type]
                ),
            )
        gaps = tuple(
            Gap.from_dict(gap_data)  # type: ignore[arg-type]
            for gap_data in data.get("gaps", ())
        )
        return cls(
            topic=str(data["topic"]),
            partition=int(data["partition"]),  # type: ignore[arg-type]
            start_offset=int(data["start_offset"]),  # type: ignore[arg-type]
            end_offset=int(data["end_offset"]),  # type: ignore[arg-type]
            position=int(data["position"]),  # type: ignore[arg-type]
            finished=bool(data["finished"]),
            retained=retained,
            gaps=gaps,
        )

    @classmethod
    def from_json(cls, raw: str) -> BackfillToken:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class RawFetch:
    """One raw Fetch response for a single partition."""

    topic_partition: TopicPartition
    error_type: type[KafkaError] | None
    highwater: int | None
    log_start_offset: int | None
    aborted_transactions: tuple[tuple[int, int], ...]
    message_set: bytes
    api_version: int

    @classmethod
    def records(
        cls,
        tp: TopicPartition,
        message_set: bytes,
        *,
        log_start_offset: int | None,
        highwater: int | None = None,
        api_version: int = 11,
        aborted_transactions: tuple[tuple[int, int], ...] = (),
    ) -> RawFetch:
        return cls(
            topic_partition=tp,
            error_type=None,
            highwater=highwater,
            log_start_offset=log_start_offset,
            aborted_transactions=aborted_transactions,
            message_set=message_set,
            api_version=api_version,
        )

    @classmethod
    def out_of_range(cls, tp: TopicPartition, *, api_version: int = 11) -> RawFetch:
        return cls(
            topic_partition=tp,
            error_type=Errors.OffsetOutOfRangeError,
            highwater=None,
            log_start_offset=None,
            aborted_transactions=(),
            message_set=b"",
            api_version=api_version,
        )


@dataclass(frozen=True)
class FetchedBatch:
    base_offset: int
    next_offset: int
    records: tuple[ConsumerRecord, ...]


@dataclass(frozen=True)
class PartitionBackfillResult:
    """Result of a single :meth:`KafkaBackfill.read_page` call."""

    topic_partition: TopicPartition
    status: BackfillStatus
    token: BackfillToken
    record: ConsumerRecord | None = None
    gaps: tuple[Gap, ...] = ()
    log_start_offset: int | None = None

    @property
    def finished(self) -> bool:
        return self.status is BackfillStatus.COMPLETE


class BackfillGateway(abc.ABC):
    """Broker operations required by the backfill reader."""

    @abc.abstractmethod
    async def earliest_offset(self, tp: TopicPartition) -> int:
        """Return the current earliest retained (log start) offset."""

    @abc.abstractmethod
    async def end_offset(self, tp: TopicPartition) -> int:
        """Return the current end offset (offset of the next written record)."""

    @abc.abstractmethod
    async def fetch(self, tp: TopicPartition, offset: int) -> RawFetch:
        """Fetch one partition starting at ``offset`` without blocking."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Release broker resources held by the gateway."""


class AiokafkaBackfillGateway(BackfillGateway):
    """:class:`BackfillGateway` backed by a shared :class:`AIOKafkaClient`."""

    def __init__(
        self,
        client: AIOKafkaClient,
        *,
        max_partition_fetch_bytes: int = _DEFAULT_FETCH_MAX_BYTES,
        isolation_level: int = READ_UNCOMMITTED,
    ) -> None:
        self._client = client
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._isolation_level = isolation_level

    async def _leader(self, tp: TopicPartition) -> int:
        node_id = self._client.cluster.leader_for_partition(tp)
        if node_id is None:
            self._client.add_topic(tp.topic)
            raise Errors.StaleMetadata(tp)
        if node_id == -1:
            raise Errors.LeaderNotAvailableError(tp)
        if not await self._client.ready(node_id):
            await self._client.force_metadata_update()
            node_id = self._client.cluster.leader_for_partition(tp)
            if (
                node_id is None
                or node_id == -1
                or not await self._client.ready(node_id)
            ):
                raise NodeNotReadyError(tp)
        return node_id

    async def _list_offset(self, tp: TopicPartition, timestamp: int) -> int:
        node_id = await self._leader(tp)
        request = OffsetRequest(
            -1,
            self._isolation_level,
            [(tp.topic, [(tp.partition, timestamp)])],
        )
        response = await self._client.send(node_id, request)
        for _topic, partitions in response.topics:
            for partition, error_code, _timestamp, offset in partitions:
                if partition != tp.partition:
                    continue
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    raise error_type(tp)
                return int(offset)
        raise Errors.KafkaError(f"ListOffsets response missing {tp}")

    async def earliest_offset(self, tp: TopicPartition) -> int:
        return await self._list_offset(tp, _LIST_OFFSET_EARLIEST)

    async def end_offset(self, tp: TopicPartition) -> int:
        return await self._list_offset(tp, _LIST_OFFSET_LATEST)

    async def fetch(self, tp: TopicPartition, offset: int) -> RawFetch:
        node_id = await self._leader(tp)
        request = FetchRequest(
            max_wait_time=0,
            min_bytes=0,
            max_bytes=self._max_partition_fetch_bytes,
            isolation_level=self._isolation_level,
            topics=[
                (tp.topic, [(tp.partition, offset, self._max_partition_fetch_bytes)])
            ],
            rack_id="",
        )
        response = await self._client.send(node_id, request)
        return self._parse_fetch_response(tp, response)

    def _parse_fetch_response(self, tp: TopicPartition, response: object) -> RawFetch:
        for topic, partitions in response.topics:  # type: ignore[attr-defined]
            if topic != tp.topic:
                continue
            for partition, error_code, highwater, *part_data in partitions:
                if partition != tp.partition:
                    continue
                api_version = response.API_VERSION  # type: ignore[attr-defined]
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    return RawFetch(
                        topic_partition=tp,
                        error_type=error_type,
                        highwater=highwater,
                        log_start_offset=None,
                        aborted_transactions=(),
                        message_set=b"",
                        api_version=api_version,
                    )
                # v5+ part_data:
                #   v5..v10: [lso, log_start, aborted_txns, message_set]
                #   v11:     [lso, log_start, aborted_txns,
                #             preferred_read_replica, message_set]
                # v4:      [lso, aborted_txns, message_set]
                # v0..v3:  [message_set] only
                if api_version >= 11:
                    log_start_offset = part_data[1]
                    aborted_transactions = part_data[2]
                elif api_version >= 5:
                    log_start_offset = part_data[1]
                    aborted_transactions = part_data[-2]
                elif api_version == 4:
                    log_start_offset = None
                    aborted_transactions = part_data[-2]
                else:
                    log_start_offset = None
                    aborted_transactions = ()
                return RawFetch(
                    topic_partition=tp,
                    error_type=None,
                    highwater=highwater,
                    log_start_offset=log_start_offset,
                    aborted_transactions=tuple(aborted_transactions),
                    message_set=part_data[-1],
                    api_version=api_version,
                )
        raise Errors.KafkaError(f"Fetch response missing {tp}")

    async def close(self) -> None:
        await self._client.close()


Deserializer = Callable[[bytes], object]


@dataclass
class KafkaBackfill:
    """Paged, resumable archive backfill reader for one partition."""

    gateway: BackfillGateway
    token: BackfillToken
    key_deserializer: Deserializer | None = None
    value_deserializer: Deserializer | None = None
    _tp: TopicPartition = field(init=False)

    def __post_init__(self) -> None:
        self._tp = self.token.topic_partition

    @classmethod
    async def open(
        cls,
        gateway: BackfillGateway,
        tp: TopicPartition,
        start_offset: int,
        end_offset: int | None = None,
        *,
        key_deserializer: Deserializer | None = None,
        value_deserializer: Deserializer | None = None,
    ) -> KafkaBackfill:
        """Start a job; the end offset is fixed for the job's lifetime."""
        if end_offset is None:
            end_offset = await gateway.end_offset(tp)
        token = BackfillToken.initial(tp, start_offset, end_offset)
        return cls(
            gateway=gateway,
            token=token,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )

    @classmethod
    def resume(
        cls,
        gateway: BackfillGateway,
        token: BackfillToken,
        *,
        key_deserializer: Deserializer | None = None,
        value_deserializer: Deserializer | None = None,
    ) -> KafkaBackfill:
        """Continue a job in another process from a saved credential."""
        return cls(
            gateway=gateway,
            token=token,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )

    @property
    def topic_partition(self) -> TopicPartition:
        return self._tp

    async def read_page(self) -> PartitionBackfillResult:
        """Advance at most one delivered record and return the new state."""
        token = self.token
        if token.finished or token.position >= token.end_offset:
            token = replace(token, finished=True, retained=None)
            self.token = token
            return self._result(BackfillStatus.COMPLETE, token)

        for _ in range(_MAX_INTERNAL_STEPS):
            if token.position >= token.end_offset:
                token = replace(token, finished=True, retained=None)
                self.token = token
                return self._result(BackfillStatus.COMPLETE, token)

            earliest = await self.gateway.earliest_offset(self._tp)

            if earliest > token.position:
                result = self._handle_retention_jump(token, earliest)
                if result is None:
                    # The jump skipped only offsets proven absent; keep
                    # advancing within the same page.
                    token = self.token
                    continue
                return result

            raw = await self.gateway.fetch(self._tp, token.position)
            if raw.error_type is Errors.OffsetOutOfRangeError:
                # The ListOffsets call above raced with retention; the next
                # iteration re-reads the log start and classifies the jump.
                log.debug("Offset out of range at %s, re-reading log start", self._tp)
                continue
            if raw.error_type is not None:
                raise raw.error_type(self._tp)

            log_start = raw.log_start_offset
            if log_start is None:
                # Fetch API < 5 does not report the log start; the fresh
                # ListOffsets value is the best available evidence.
                log_start = earliest

            batches = self._parse_batches(raw)
            if not batches:
                # Quiet partition: no data up to the fetch response and the
                # log still covers the position. Deletion vs. "not written
                # yet" cannot be decided from this evidence.
                return self._result(BackfillStatus.UNCONFIRMED, token, log_start)

            batch = self._first_relevant_batch(batches, token.position)
            if batch is None:
                return self._result(BackfillStatus.UNCONFIRMED, token, log_start)

            if batch.base_offset > token.position:
                result = self._handle_batch_gap(token, batch, log_start)
                if result is not None:
                    self.token = result.token
                    return result
                token = self.token
                continue

            result = self._deliver_or_skip(token, batch, log_start)
            self.token = result.token
            return result

        raise Errors.KafkaError(
            f"Backfill for {self._tp} did not advance within "
            f"{_MAX_INTERNAL_STEPS} internal steps"
        )

    def _handle_retention_jump(
        self, token: BackfillToken, earliest: int
    ) -> PartitionBackfillResult | None:
        """Classify a log start that moved past the current position.

        Returns ``None`` when the jump skipped only offsets proven absent;
        the caller continues advancing within the same page.
        """
        jump_end = min(earliest, token.end_offset)
        gaps = self._classify_deleted(token.position, jump_end, token.retained)
        retained = self._retain_after_jump(token.retained, earliest)
        finished = jump_end >= token.end_offset
        new_token = replace(
            token,
            position=jump_end,
            retained=None if finished else retained,
            finished=finished,
            gaps=self._merge_gaps(token.gaps, tuple(gaps)),
        )
        self.token = new_token
        if gaps:
            return self._result(
                BackfillStatus.GAP,
                new_token,
                log_start_offset=earliest,
                gaps=tuple(gaps),
            )
        if finished:
            return self._result(BackfillStatus.COMPLETE, new_token)
        return None

    def _handle_batch_gap(
        self,
        token: BackfillToken,
        batch: FetchedBatch,
        log_start: int | None,
    ) -> PartitionBackfillResult | None:
        """Decide what a first batch starting past the position means.

        Returns ``None`` when the in-between offsets are proven absent by
        compaction and the reader can continue within the same page.
        """
        position = token.position
        if log_start is None:
            # No way to distinguish retention deletion from compaction.
            return self._result(BackfillStatus.UNCONFIRMED, token, None)
        if log_start <= position:
            # The log still covers the position while the first complete
            # batch starts later: compaction provably removed the offsets in
            # between, so skip them and continue internally.
            self.token = replace(
                token,
                position=batch.base_offset,
                retained=self._retained_from_batch(batch, batch.base_offset),
            )
            return None
        jump_end = min(log_start, token.end_offset)
        gaps = self._classify_deleted(position, jump_end, token.retained)
        finished = jump_end >= token.end_offset
        new_token = replace(
            token,
            position=jump_end,
            retained=None
            if finished
            else self._retain_after_jump(token.retained, log_start),
            finished=finished,
            gaps=self._merge_gaps(token.gaps, tuple(gaps)),
        )
        self.token = new_token
        return self._result(
            BackfillStatus.GAP,
            new_token,
            log_start_offset=log_start,
            gaps=tuple(gaps),
        )

    def _deliver_or_skip(
        self,
        token: BackfillToken,
        batch: FetchedBatch,
        log_start: int | None,
    ) -> PartitionBackfillResult:
        pending = tuple(
            record
            for record in batch.records
            if token.position <= record.offset < token.end_offset
        )
        if not pending:
            if batch.next_offset <= token.position:
                # Defensive: stale batch ending at the position.
                return self._result(BackfillStatus.UNCONFIRMED, token, log_start)
            # Records exist only at/after the fixed end: nothing more to
            # deliver, the partition is complete up to the end offset.
            new_token = replace(token, finished=True, retained=None)
            return self._result(BackfillStatus.COMPLETE, new_token, log_start)

        record = pending[0]
        next_position = record.offset + 1
        retained = self._retained_from_batch(batch, next_position)
        new_token = replace(token, position=next_position, retained=retained)
        return self._result(
            BackfillStatus.DELIVERED,
            new_token,
            record=record,
            log_start_offset=log_start,
        )

    def _parse_batches(self, raw: RawFetch) -> list[FetchedBatch]:
        if not raw.message_set:
            return []
        memory_records = MemoryRecords(raw.message_set)
        aborted_producers = {
            producer_id for producer_id, _first_offset in raw.aborted_transactions
        }
        batches: list[FetchedBatch] = []
        while memory_records.has_next():
            batch = memory_records.next_batch()
            if batch is None:
                break
            if batch.is_control_batch:
                continue
            if batch.is_transactional and batch.producer_id in aborted_producers:
                continue
            consumer_records = tuple(
                self._to_consumer_record(batch_record) for batch_record in batch
            )
            base_offset = getattr(batch, "base_offset", None)
            if base_offset is None:
                # Legacy message sets expose no batch base offset; the first
                # surviving record marks the start of the covered range.
                base_offset = (
                    consumer_records[0].offset
                    if consumer_records
                    else batch.next_offset - 1
                )
            batches.append(
                FetchedBatch(
                    base_offset=base_offset,
                    next_offset=batch.next_offset,
                    records=consumer_records,
                )
            )
        return batches

    def _to_consumer_record(self, record: object) -> ConsumerRecord:
        key = record.key  # type: ignore[attr-defined]
        value = record.value  # type: ignore[attr-defined]
        key_size = len(key) if key is not None else -1
        value_size = len(value) if value is not None else -1
        if self.key_deserializer is not None:
            key = self.key_deserializer(key)  # type: ignore[misc]
        if self.value_deserializer is not None:
            value = self.value_deserializer(value)  # type: ignore[misc]
        return ConsumerRecord(
            self._tp.topic,
            self._tp.partition,
            record.offset,  # type: ignore[attr-defined]
            record.timestamp,  # type: ignore[attr-defined]
            record.timestamp_type,  # type: ignore[attr-defined]
            key,
            value,
            record.checksum,  # type: ignore[attr-defined]
            key_size,
            value_size,
            tuple(record.headers),  # type: ignore[attr-defined]
        )

    @staticmethod
    def _first_relevant_batch(
        batches: list[FetchedBatch], position: int
    ) -> FetchedBatch | None:
        for batch in batches:
            if batch.next_offset > position:
                return batch
        return None

    @staticmethod
    def _retained_from_batch(
        batch: FetchedBatch, position: int
    ) -> RetainedBatch | None:
        if position >= batch.next_offset:
            return None
        offsets = tuple(
            record.offset for record in batch.records if record.offset >= position
        )
        return RetainedBatch(
            base_offset=batch.base_offset,
            next_offset=batch.next_offset,
            offsets=offsets,
        )

    @staticmethod
    def _retain_after_jump(
        retained: RetainedBatch | None, log_start: int
    ) -> RetainedBatch | None:
        if retained is None or log_start >= retained.next_offset:
            return None
        offsets = tuple(offset for offset in retained.offsets if offset >= log_start)
        return replace(retained, offsets=offsets)

    def _classify_deleted(
        self,
        start: int,
        end: int,
        retained: RetainedBatch | None,
    ) -> list[Gap]:
        """Split ``[start, end)`` into intervals lost to retention.

        Offsets covered by a fully fetched batch are decided by evidence:
        records present in the batch but never delivered are gaps, while
        offsets proven absent by compaction are silently skipped. Anything
        outside retained evidence is reported as a gap.
        """
        if start >= end:
            return []
        if retained is None:
            return [Gap(start, end)]

        gaps: list[Gap] = []

        # No evidence before the retained batch: assume retention deletion.
        uncovered_end = min(end, retained.base_offset)
        if start < uncovered_end:
            gaps.append(Gap(start, uncovered_end))

        # Inside the retained batch, present (undelivered) records are gaps;
        # the in-between offsets were proven absent by compaction.
        overlap_start = max(start, retained.base_offset)
        overlap_end = min(end, retained.next_offset)
        present = sorted(
            offset
            for offset in retained.offsets
            if overlap_start <= offset < overlap_end
        )
        gaps.extend(Gap(offset, offset + 1) for offset in present)

        # No evidence past the retained batch either.
        uncovered_start = max(start, retained.next_offset)
        if uncovered_start < end:
            gaps.append(Gap(uncovered_start, end))

        return self._normalize_gaps(gaps)

    @staticmethod
    def _normalize_gaps(gaps: list[Gap]) -> list[Gap]:
        normalized: list[Gap] = []
        for gap in sorted(gaps, key=lambda item: item.start):
            if gap.start >= gap.end:
                continue
            if normalized and normalized[-1].end >= gap.start:
                normalized[-1] = Gap(
                    normalized[-1].start, max(normalized[-1].end, gap.end)
                )
            else:
                normalized.append(gap)
        return normalized

    @staticmethod
    def _merge_gaps(
        existing: tuple[Gap, ...], new_gaps: tuple[Gap, ...]
    ) -> tuple[Gap, ...]:
        return tuple(KafkaBackfill._normalize_gaps([*existing, *new_gaps]))

    def _result(
        self,
        status: BackfillStatus,
        token: BackfillToken,
        log_start_offset: int | None = None,
        *,
        record: ConsumerRecord | None = None,
        gaps: tuple[Gap, ...] = (),
    ) -> PartitionBackfillResult:
        return PartitionBackfillResult(
            topic_partition=self._tp,
            status=status,
            token=token,
            record=record,
            gaps=gaps,
            log_start_offset=log_start_offset,
        )
