from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, NamedTuple, TypeVar

from aiokafka.errors import KafkaError

__all__ = [
    "BrokerMetadata",
    "ConsumerRecord",
    "OffsetAndMetadata",
    "PartitionMetadata",
    "RecordDelivery",
    "RecordErrorDetail",
    "RecordStatus",
    "RecordMetadata",
    "TopicPartition",
]


class TopicPartition(NamedTuple):
    """A topic and partition tuple"""

    topic: str
    "A topic name"

    partition: int
    "A partition id"


class BrokerMetadata(NamedTuple):
    """A Kafka broker metadata used by admin tools"""

    # FIXME: consider updating implementation (https://github.com/aio-libs/aiokafka/issues/1050)
    nodeId: int | str
    "The Kafka broker id"

    host: str
    "The Kafka broker hostname"

    port: int
    "The Kafka broker port"

    rack: str | None
    """The rack of the broker, which is used to in rack aware partition
    assignment for fault tolerance.
    Examples: `RACK1`, `us-east-1d`. Default: None
    """


class PartitionMetadata(NamedTuple):
    """A topic partition metadata describing the state in the MetadataResponse"""

    topic: str
    "The topic name of the partition this metadata relates to"

    partition: int
    "The id of the partition this metadata relates to"

    leader: int
    "The id of the broker that is the leader for the partition"

    replicas: list[int]
    "The ids of all brokers that contain replicas of the partition"
    isr: list[int]
    "The ids of all brokers that contain in-sync replicas of the partition"

    error: KafkaError | None
    "A KafkaError object associated with the request for this partition metadata"


class OffsetAndMetadata(NamedTuple):
    """The Kafka offset commit API

    The Kafka offset commit API allows users to provide additional metadata
    (in the form of a string) when an offset is committed. This can be useful
    (for example) to store information about which node made the commit,
    what time the commit was made, etc.
    """

    offset: int
    "The offset to be committed"

    metadata: str
    "Non-null metadata"

    # TODO add leaderEpoch:


class RecordStatus(str, Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    REJECTED = "rejected"
    FAILED = "failed"
    UNKNOWN = "unknown"
    DUPLICATE = "duplicate"
    OFFSET_UNKNOWN = "offset_unknown"
    WRITTEN_OFFSET_UNKNOWN = "written_offset_unknown"


@dataclass
class RecordDelivery:
    """Final or current delivery state of one original batch record."""

    index: int
    topic: str | None = None
    partition: int | None = None
    topic_partition: TopicPartition | None = None
    offset: int | None = None
    timestamp: int | None = None
    timestamp_type: int | None = None
    log_start_offset: int | None = None
    error: BaseException | None = None
    error_message: str | None = None
    status: RecordStatus = RecordStatus.PENDING
    offset_known: bool = True
    duplicate: bool = False
    sequence_number: int | None = None
    original_sequence_number: int | None = None
    metadata: "RecordMetadata | None" = None
    key: bytes | None = None
    value: bytes | None = None
    headers: Sequence[tuple[str, bytes | None]] = field(default_factory=tuple)

    @property
    def batch_index(self) -> int:
        return self.index

    @property
    def original_index(self) -> int:
        return self.index

    @property
    def record_index(self) -> int:
        return self.index

    @property
    def reason(self) -> str | None:
        if self.error_message is not None:
            return self.error_message
        if self.error is not None:
            return str(self.error)
        return None

    @property
    def exception(self) -> BaseException | None:
        return self.error

    @property
    def offset_unknown(self) -> bool:
        return not self.offset_known

    @property
    def unknown_offset(self) -> bool:
        return not self.offset_known

    @property
    def has_offset(self) -> bool:
        return self.offset_known and self.offset is not None

    @property
    def sequence(self) -> int | None:
        return self.sequence_number

    @property
    def original_sequence(self) -> int | None:
        return self.original_sequence_number

    @property
    def error_code(self) -> int | None:
        return getattr(self.error, "errno", None)

    def __getitem__(self, key):
        if isinstance(key, int) and key == 0:
            return self.index
        if isinstance(key, str):
            if hasattr(self, key):
                return getattr(self, key)
            aliases = {"reason": "reason", "seq": "sequence_number"}
            if key in aliases:
                return getattr(self, aliases[key])
        raise KeyError(key)

    @property
    def written(self) -> bool | None:
        if self.status in (
            RecordStatus.DELIVERED,
            RecordStatus.DUPLICATE,
            RecordStatus.OFFSET_UNKNOWN,
            RecordStatus.WRITTEN_OFFSET_UNKNOWN,
        ):
            return True
        if self.status in (RecordStatus.REJECTED, RecordStatus.FAILED):
            return False
        return None

    @property
    def delivered(self) -> bool:
        return self.status in (
            RecordStatus.DELIVERED,
            RecordStatus.DUPLICATE,
            RecordStatus.OFFSET_UNKNOWN,
            RecordStatus.WRITTEN_OFFSET_UNKNOWN,
        )

    @property
    def rejected(self) -> bool:
        return self.status is RecordStatus.REJECTED


class RecordErrorDetail(tuple):
    """A ``(batch_index, error_message)`` tuple with attribute aliases."""

    def __new__(cls, batch_index, error_message, error=None, sequence_number=None):
        if error_message is None and error is not None:
            error_message = str(error)
        obj = super().__new__(cls, (batch_index, error_message))
        obj._error = error
        obj._sequence_number = sequence_number
        return obj

    @property
    def batch_index(self) -> int:
        return self[0]

    @property
    def index(self) -> int:
        return self[0]

    @property
    def original_index(self) -> int:
        return self[0]

    @property
    def error_message(self) -> str | None:
        return self[1]

    @property
    def batch_index_error_message(self) -> str | None:
        return self[1]

    @property
    def message(self) -> str | None:
        return self[1]

    @property
    def reason(self) -> str | None:
        return self[1]

    @property
    def error(self):
        return self._error

    @property
    def sequence_number(self):
        return self._sequence_number

    @property
    def original_sequence_number(self):
        return self._sequence_number


class RecordMetadata(NamedTuple):
    """Returned when a :class:`~.AIOKafkaProducer` sends a message"""

    topic: str
    "The topic name"

    partition: int
    "The partition number"

    topic_partition: TopicPartition
    ""

    offset: int | None
    """The unique offset of the message in this partition.

    See :ref:`Offsets and Consumer Position <offset_and_position>` for more
    details on offsets.
    """

    timestamp: int | None
    "Timestamp in millis, None for older Brokers"

    timestamp_type: int
    """The timestamp type of this record.

    If the broker respected the timestamp passed to
    :meth:`.AIOKafkaProducer.send`, ``0`` will be returned (``CreateTime``).

    If the broker set it's own timestamp, ``1`` will be returned (``LogAppendTime``).
    """

    log_start_offset: int | None
    ""

    record_results: tuple[RecordDelivery, ...] = ()

    @property
    def records(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def results(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def deliveries(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def record_delivery_results(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def per_record_results(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def record_result(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def record_metadata(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def per_record_metadata(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def record_metadata_list(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def metadata_list(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def record_metas(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def metas(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def metadatas(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def outcomes(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def record_outcomes(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def delivery_outcomes(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def send_results(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def ack_results(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def items(self) -> tuple[RecordDelivery, ...]:
        return self.record_results

    @property
    def has_offsets(self) -> bool:
        return all(result.offset is not None for result in self.delivered_records)

    @property
    def results_by_index(self) -> dict[int, RecordDelivery]:
        return {result.index: result for result in self.record_results}

    @property
    def metadata_by_index(self) -> dict[int, RecordDelivery]:
        return self.results_by_index

    @property
    def record_errors(self) -> tuple[RecordDelivery, ...]:
        return tuple(
            result for result in self.record_results if result.error is not None
        )

    @property
    def record_error_details(self) -> tuple[RecordErrorDetail, ...]:
        return tuple(
            RecordErrorDetail(
                result.index,
                result.error_message,
                result.error,
                result.original_sequence_number or result.sequence_number,
            )
            for result in self.record_results
            if result.error is not None or result.error_message is not None
        )

    @property
    def rejected_records(self) -> tuple[RecordDelivery, ...]:
        return tuple(
            result
            for result in self.record_results
            if result.status is RecordStatus.REJECTED
        )

    @property
    def failed_records(self) -> tuple[RecordDelivery, ...]:
        return tuple(
            result
            for result in self.record_results
            if result.status in (RecordStatus.REJECTED, RecordStatus.FAILED)
        )

    @property
    def successful_records(self) -> tuple[RecordDelivery, ...]:
        return self.delivered_records

    @property
    def delivered_records(self) -> tuple[RecordDelivery, ...]:
        return tuple(result for result in self.record_results if result.delivered)

    @property
    def offsets(self) -> tuple[int | None, ...]:
        return tuple(result.offset for result in self.record_results)

    def result_for_index(self, index: int) -> RecordDelivery | None:
        for result in self.record_results:
            if result.index == index:
                return result
        return None

    def result_for(self, index_or_metadata) -> RecordDelivery | None:
        index = getattr(index_or_metadata, "offset", index_or_metadata)
        return self.result_for_index(index)


RecordResult = RecordDelivery
RecordSendResult = RecordDelivery
SendResult = RecordDelivery
RecordAck = RecordDelivery
BatchRecordMetadata = RecordMetadata
BatchResult = RecordMetadata
BatchSendResult = RecordMetadata
UNKNOWN_OFFSET = None


KT = TypeVar("KT")
VT = TypeVar("VT")


@dataclass
class ConsumerRecord(Generic[KT, VT]):
    topic: str
    "The topic this record is received from"

    partition: int
    "The partition from which this record is received"

    offset: int
    "The position of this record in the corresponding Kafka partition."

    timestamp: int
    "The timestamp of this record"

    timestamp_type: int
    "The timestamp type of this record"

    key: KT | None
    "The key (or `None` if no key is specified)"

    value: VT | None
    "The value"

    checksum: int | None
    "Deprecated"

    serialized_key_size: int
    "The size of the serialized, uncompressed key in bytes."

    serialized_value_size: int
    "The size of the serialized, uncompressed value in bytes."

    headers: Sequence[tuple[str, bytes]]
    "The headers"


class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: int | None  # Only None if used with old broker version
