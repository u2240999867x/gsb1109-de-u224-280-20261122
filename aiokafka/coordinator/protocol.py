from collections.abc import Iterable
from typing import NamedTuple

from aiokafka.protocol.struct import Struct
from aiokafka.protocol.types import Array, Bytes, Int16, Int32, Schema, String
from aiokafka.structs import TopicPartition


class ConsumerProtocolMemberMetadata(Struct):
    version: int
    subscription: list[str]
    user_data: bytes

    SCHEMA = Schema(
        ("version", Int16),
        ("subscription", Array(String("utf-8"))),
        ("user_data", Bytes),
    )


class ConsumerProtocolMemberAssignment(Struct):
    class Assignment(NamedTuple):
        topic: str
        partitions: list[int]

    version: int
    assignment: list[Assignment]
    user_data: bytes

    SCHEMA = Schema(
        ("version", Int16),
        ("assignment", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
        ("user_data", Bytes),
    )

    def partitions(self) -> list[TopicPartition]:
        return [
            TopicPartition(topic, partition)
            for topic, partitions in self.assignment
            for partition in partitions
        ]


class ConsumerProtocolMemberUserData(Struct):
    """Cooperative protocol ``user_data`` payload.

    Carries the partitions the member still claims ownership of. The group
    leader uses it to avoid moving a partition to another member before the
    current owner has revoked it (the two-phase KIP-429 handover).
    """

    SCHEMA = Schema(
        (
            "owned_partitions",
            Array(("topic", String("utf-8")), ("partitions", Array(Int32))),
        )
    )

    @classmethod
    def from_partitions(
        cls, partitions: Iterable[TopicPartition]
    ) -> "ConsumerProtocolMemberUserData":
        by_topic: dict[str, list[int]] = {}
        for tp in partitions:
            by_topic.setdefault(tp.topic, []).append(tp.partition)
        return cls(sorted((topic, sorted(parts)) for topic, parts in by_topic.items()))

    def partitions(self) -> list[TopicPartition]:
        return [
            TopicPartition(topic, partition)
            for topic, partitions in self.owned_partitions
            for partition in partitions
        ]


class ConsumerProtocol:
    PROTOCOL_TYPE = "consumer"
    ASSIGNMENT_STRATEGIES = ("range", "roundrobin")
    METADATA = ConsumerProtocolMemberMetadata
    ASSIGNMENT = ConsumerProtocolMemberAssignment

    # Version 0 is used by the classic "eager" rebalance protocol, where
    # every member revokes all of its partitions before joining the group.
    # Version 1 (KIP-429) is used by the cooperative (incremental cooperative
    # rebalancing) protocol, where a member only revokes the partitions it has
    # to give up and keeps fetching from, and pauses, the partitions it
    # retains across generations.
    COOPERATIVE_PROTOCOL_VERSION = 1
