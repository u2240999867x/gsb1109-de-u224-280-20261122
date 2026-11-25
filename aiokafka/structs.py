from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Generic, NamedTuple, TypeVar

from aiokafka.errors import KafkaError

__all__ = [
    "BrokerMetadata",
    "ConsumerRecord",
    "OffsetAndMetadata",
    "PartitionMetadata",
    "PartitionRecoveryCredential",
    "PartitionRecoveryResult",
    "RecordMetadata",
    "RecoveryDivergence",
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


class RecordMetadata(NamedTuple):
    """Returned when a :class:`~.AIOKafkaProducer` sends a message"""

    topic: str
    "The topic name"

    partition: int
    "The partition number"

    topic_partition: TopicPartition
    ""

    offset: int
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

    leader_epoch: int | None = None
    """The partition leader epoch of the batch the record was fetched from.

    ``None`` for the pre-v2 (magic 0/1) message format, which predates leader
    epochs (KIP-101). Such records cannot produce a verifiable recovery
    credential.
    """


class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: int | None  # Only None if used with old broker version


@dataclass(frozen=True)
class PartitionRecoveryCredential:
    """A persistable, verifiable recovery point for one topic partition.

    It binds the offset of the *next* unprocessed record to the leader epoch
    of the last record that was actually processed, so an archiver can detect
    log truncation / replacement after a leader change instead of trusting a
    bare offset.

    Attributes:
        topic: Topic name.
        partition: Partition id.
        next_offset: Offset to resume reading from, i.e. one past the last
            processed record.
        leader_epoch: Leader epoch of the last processed record. ``-1`` means
            the broker did not attach an epoch to the record batch and the
            credential cannot be verified against the current leader.
        verified: ``False`` when ``leader_epoch`` is unavailable. A credential
            with ``verified=False`` cannot be validated with
            OffsetsForLeaderEpoch and must not be used to move the read
            position automatically.
    """

    topic: str
    partition: int
    next_offset: int
    leader_epoch: int
    verified: bool = True

    @property
    def topic_partition(self) -> TopicPartition:
        return TopicPartition(self.topic, self.partition)

    def to_dict(self) -> dict[str, object]:
        """Return a plain mapping suitable for JSON-style persistence."""
        return {
            "topic": self.topic,
            "partition": self.partition,
            "next_offset": self.next_offset,
            "leader_epoch": self.leader_epoch,
            "verified": self.verified,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> PartitionRecoveryCredential:
        """Rebuild a credential previously produced by :meth:`to_dict`."""
        return cls(
            topic=str(data["topic"]),
            partition=int(data["partition"]),
            next_offset=int(data["next_offset"]),
            leader_epoch=int(data["leader_epoch"]),
            verified=bool(data.get("verified", True)),
        )


@dataclass(frozen=True)
class RecoveryDivergence:
    """An offset range whose records cannot be re-delivered unchanged.

    Attributes:
        start_offset: First offset that needs manual reconciliation
            (inclusive).
        end_offset: One past the last offset that needs reconciliation.
        reason: Human-readable explanation of why the range is suspect
            (log truncation, deletion or replacement).
    """

    start_offset: int
    end_offset: int
    reason: str

    def __bool__(self) -> bool:
        return self.end_offset > self.start_offset


@dataclass(frozen=True)
class PartitionRecoveryResult:
    """Outcome of validating a :class:`PartitionRecoveryCredential`.

    Attributes:
        topic_partition: The partition that was validated.
        recoverable: Whether the read position was (or can be) moved to the
            credential's ``next_offset``.
        position: The read position that should be kept. Equals the previous
            position when recovery was not performed.
        divergences: Offset ranges in the old log that must be reconciled
            manually before the archived data can be trusted.
        error: Set when no verification evidence could be obtained from the
            leader. The previous position is kept in that case.
    """

    topic_partition: TopicPartition
    recoverable: bool
    position: int
    divergences: Sequence[RecoveryDivergence] = field(default_factory=tuple)
    error: KafkaError | None = None
