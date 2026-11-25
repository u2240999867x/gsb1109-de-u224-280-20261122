from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, NamedTuple, TypeVar

from aiokafka.errors import KafkaError

__all__ = [
    "BrokerMetadata",
    "ConsumerGroupMetadata",
    "ConsumerRecord",
    "OffsetAndMetadata",
    "PartitionMetadata",
    "RecordMetadata",
    "TopicPartition",
]


class TopicPartition(NamedTuple):
    """A topic and partition tuple"""

    topic: str
    "A topic name"

    partition: int
    "A partition id"


class ConsumerGroupMetadata(NamedTuple):
    """Immutable consumer group membership used by transactional commits."""

    group_id: str
    generation_id: int = -1
    member_id: str = ""
    group_instance_id: str | None = None

    @classmethod
    def coerce(cls, value=None, **kwargs):
        if value is None:
            metadata = None
        elif isinstance(value, ConsumerGroupMetadata):
            metadata = value
        elif isinstance(value, str):
            metadata = cls(group_id=value)
        elif isinstance(value, dict):
            values = dict(value)
            values.update({k: v for k, v in kwargs.items() if v is not None})
            return cls(
                group_id=values.get("group_id") or values.get("group"),
                generation_id=values.get("generation_id", values.get("generation", -1)),
                member_id=values.get("member_id", values.get("member", "")),
                group_instance_id=values.get(
                    "group_instance_id", values.get("instance_id")
                ),
            )
        elif isinstance(value, (tuple, list)):
            metadata = cls(*value)
        else:
            metadata = cls(
                group_id=getattr(value, "group_id", None)
                or getattr(value, "group", None),
                generation_id=getattr(
                    value,
                    "generation_id",
                    getattr(value, "generation", -1),
                ),
                member_id=getattr(value, "member_id", getattr(value, "member", "")),
                group_instance_id=getattr(
                    value,
                    "group_instance_id",
                    getattr(value, "instance_id", None),
                ),
            )

        if not kwargs:
            return metadata

        fields = metadata._asdict() if metadata is not None else {}
        if kwargs.get("group_id") is not None:
            fields["group_id"] = kwargs["group_id"]
        for name in (
            "generation_id",
            "generation",
            "member_id",
            "group_instance_id",
            "instance_id",
        ):
            if name in kwargs and kwargs[name] is not None:
                target = (
                    "generation_id"
                    if name in {"generation_id", "generation"}
                    else "group_instance_id"
                    if name == "instance_id"
                    else name
                )
                fields[target] = kwargs[name]
        if metadata is None and fields.get("group_id") is None:
            return None
        return cls(**fields)

    @property
    def generation(self) -> int:
        return self.generation_id

    @property
    def has_member_identity(self) -> bool:
        return (
            self.generation_id >= 0
            or bool(self.member_id)
            or self.group_instance_id is not None
        )


GroupMetadata = ConsumerGroupMetadata
ConsumerGroupMembership = ConsumerGroupMetadata
GroupMembership = ConsumerGroupMetadata
GroupIdentity = ConsumerGroupMetadata
ConsumerGroupMemberMetadata = ConsumerGroupMetadata


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


class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: int | None  # Only None if used with old broker version
