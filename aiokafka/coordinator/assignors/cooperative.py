"""Cooperative (incremental cooperative rebalancing, KIP-429) partition
assignors.

The wrappers reuse the classic assignor logic to compute the *target*
assignment, but advertise the cooperative protocol version in member
metadata and member assignments.

Ownership handover is two-phase and is driven jointly by the group leader
and :class:`~aiokafka.consumer.group_coordinator.GroupCoordinator`:

* every member advertises the partitions it still claims ownership of in
  the join metadata ``user_data``;
* the leader computes the target assignment with the wrapped assignor and
  then removes every partition whose owner would change from *all*
  assignments of the current generation. The old owner therefore receives
  an assignment without that partition and revokes it, while the prospective
  new owner receives nothing yet;
* on the next generation the old owner no longer claims the partition, so
  the leader assigns it to the new owner.

Partitions a member keeps stay assigned and retain their fetch position,
pause state and prefetched records; only the revoked partitions trigger the
user callback and buffer cleanup.

A cooperative assignor must be configured on every member of the group,
otherwise the broker coordinator will not select the cooperative protocol
and the group falls back to an eager protocol.
"""

import logging
from collections.abc import Iterable, Mapping

from aiokafka.cluster import ClusterMetadata
from aiokafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyPartitionAssignor,
)
from aiokafka.coordinator.protocol import (
    ConsumerProtocol,
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
    ConsumerProtocolMemberUserData,
)
from aiokafka.structs import TopicPartition

log = logging.getLogger(__name__)


def encode_owned_partitions(partitions: Iterable[TopicPartition]) -> bytes:
    """Encode the partitions a cooperative member still claims."""
    return ConsumerProtocolMemberUserData.from_partitions(partitions).encode()


def decode_owned_partitions(user_data: bytes) -> set[TopicPartition]:
    """Decode claimed ownership, tolerating empty/foreign ``user_data``."""
    if not user_data:
        return set()
    try:
        return set(ConsumerProtocolMemberUserData.decode(user_data).partitions())
    except Exception:  # noqa: BLE001 - foreign assignor payload, treat as no claim
        log.warning("Could not parse cooperative member user data, ignoring")
        return set()


class _CooperativeAssignorMixin:
    """Shared cooperative metadata/assignment encoding for wrapped assignors.

    Subclasses must set ``_eager_assignor`` to the assignor performing the
    actual partition placement and define ``name``.
    """

    _eager_assignor: type[AbstractPartitionAssignor]
    name: str

    protocol_type: str = "cooperative"

    @classmethod
    def assign(
        cls,
        cluster: ClusterMetadata,
        members: Mapping[str, ConsumerProtocolMemberMetadata],
    ) -> dict[str, ConsumerProtocolMemberAssignment]:
        # Partitions each member still claims in the current generation.
        claimed = {
            member_id: decode_owned_partitions(metadata.user_data)
            for member_id, metadata in members.items()
        }

        # The wrapped assignor only reads ``subscription`` (and, for sticky,
        # its own user_data), so feed it the plain cooperative metadata with
        # ownership payload stripped.
        eager_members = {
            member_id: ConsumerProtocolMemberMetadata(
                ConsumerProtocol.COOPERATIVE_PROTOCOL_VERSION,
                metadata.subscription,
                b"",
            )
            for member_id, metadata in members.items()
        }
        target = cls._eager_assignor.assign(cluster, eager_members)

        # Two-phase handover: a partition that a member still claims but the
        # target assigns elsewhere is removed from every assignment this
        # generation. It is only given to the new owner once the old owner
        # has revoked it and stops claiming it.
        target_owner: dict[TopicPartition, str] = {}
        for member_id, assignment in target.items():
            for tp in assignment.partitions():
                target_owner[tp] = member_id

        blocked: set[TopicPartition] = set()
        for tp, prospective_owner in target_owner.items():
            for owner, owned_partitions in claimed.items():
                if tp in owned_partitions and owner != prospective_owner:
                    blocked.add(tp)
                    break

        cooperative_assignments = {}
        for member_id, assignment in target.items():
            kept = []
            for topic, partitions in assignment.assignment:
                kept_partitions = [
                    p for p in partitions if TopicPartition(topic, p) not in blocked
                ]
                if kept_partitions:
                    kept.append((topic, kept_partitions))
            cooperative_assignments[member_id] = ConsumerProtocolMemberAssignment(
                ConsumerProtocol.COOPERATIVE_PROTOCOL_VERSION,
                kept,
                assignment.user_data,
            )
        return cooperative_assignments

    @classmethod
    def metadata(cls, topics: Iterable[str]) -> ConsumerProtocolMemberMetadata:
        # Standalone metadata without ownership; the coordinator normally
        # calls ``metadata_with_ownership`` with the partitions the member
        # currently owns.
        return cls.metadata_with_ownership(topics, set())

    @classmethod
    def metadata_with_ownership(
        cls,
        topics: Iterable[str],
        owned_partitions: Iterable[TopicPartition],
    ) -> ConsumerProtocolMemberMetadata:
        return ConsumerProtocolMemberMetadata(
            ConsumerProtocol.COOPERATIVE_PROTOCOL_VERSION,
            list(topics),
            encode_owned_partitions(owned_partitions),
        )

    @classmethod
    def on_assignment(cls, assignment: ConsumerProtocolMemberAssignment) -> None:
        cls._eager_assignor.on_assignment(assignment)


class RoundRobinCooperativeAssignor(
    _CooperativeAssignorMixin, AbstractPartitionAssignor
):
    """Round-robin placement advertised through the cooperative protocol."""

    name = "roundrobin-cooperative"
    _eager_assignor = RoundRobinPartitionAssignor


class RangeCooperativeAssignor(_CooperativeAssignorMixin, AbstractPartitionAssignor):
    """Range placement advertised through the cooperative protocol."""

    name = "range-cooperative"
    _eager_assignor = RangePartitionAssignor


class StickyCooperativeAssignor(_CooperativeAssignorMixin, AbstractPartitionAssignor):
    """Sticky placement advertised through the cooperative protocol.

    Matches the protocol name of the Java client's cooperative sticky
    assignor, so groups mixing both client implementations can agree on it.
    """

    name = "cooperative-sticky"
    _eager_assignor = StickyPartitionAssignor
