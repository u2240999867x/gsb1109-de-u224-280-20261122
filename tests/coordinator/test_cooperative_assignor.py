"""Unit tests for the cooperative (KIP-429) assignors.

These tests are broker-less: they drive the assignor directly with mocked
cluster metadata and member metadata to prove the two-phase ownership
handover performed by the group leader.
"""

from unittest.mock import MagicMock

import pytest

from aiokafka.coordinator.assignors.cooperative import (
    RangeCooperativeAssignor,
    RoundRobinCooperativeAssignor,
    StickyCooperativeAssignor,
    decode_owned_partitions,
    encode_owned_partitions,
)
from aiokafka.coordinator.protocol import (
    ConsumerProtocol,
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)
from aiokafka.structs import TopicPartition

TOPIC = "topic"
P0 = TopicPartition(TOPIC, 0)
P1 = TopicPartition(TOPIC, 1)
P2 = TopicPartition(TOPIC, 2)

ALL_PARTITIONS = {P0, P1, P2}


def make_cluster():
    cluster = MagicMock()
    cluster.topics.return_value = {TOPIC}
    cluster.partitions_for_topic.return_value = {0, 1, 2}
    return cluster


def member_metadata(assignor, topics, owned):
    return assignor.metadata_with_ownership(topics, set(owned))


def as_partitions(assignment):
    return set(assignment.partitions())


@pytest.mark.parametrize(
    "assignor",
    [
        RoundRobinCooperativeAssignor,
        RangeCooperativeAssignor,
        StickyCooperativeAssignor,
    ],
)
def test_cooperative_metadata_advertises_v1_and_ownership(assignor):
    metadata = member_metadata(assignor, {TOPIC}, [P0, P2])
    assert metadata.version == ConsumerProtocol.COOPERATIVE_PROTOCOL_VERSION
    assert metadata.subscription == [TOPIC]
    assert decode_owned_partitions(metadata.user_data) == {P0, P2}
    assert assignor.protocol_type == "cooperative"


def test_ownership_user_data_roundtrip():
    encoded = encode_owned_partitions([P0, P1, P2])
    assert decode_owned_partitions(encoded) == ALL_PARTITIONS
    assert decode_owned_partitions(b"") == set()
    assert decode_owned_partitions(b"\x00not-a-real-payload") == set()


def _target_owner(assignor, cluster, members):
    """Recompute where each partition ends up ignoring ownership claims."""
    eager_members = {
        member_id: ConsumerProtocolMemberMetadata(
            ConsumerProtocol.COOPERATIVE_PROTOCOL_VERSION,
            metadata.subscription,
            b"",
        )
        for member_id, metadata in members.items()
    }
    target = assignor._eager_assignor.assign(cluster, eager_members)
    owner = {}
    for member_id, assignment in target.items():
        for tp in assignment.partitions():
            owner[tp] = member_id
    return owner


@pytest.mark.parametrize(
    "assignor",
    [RoundRobinCooperativeAssignor, StickyCooperativeAssignor]
)
def test_two_phase_handover_blocks_partition_until_revoked(assignor):
    cluster = make_cluster()

    # Round 1: A owns everything and still claims P2; B is a new member with
    # no current ownership. The target wants to move some partition(s) to B,
    # but any partition A still claims must be blocked this generation.
    members_round1 = {
        "A": member_metadata(assignor, {TOPIC}, [P0, P1, P2]),
        "B": member_metadata(assignor, {TOPIC}, []),
    }
    target_owner = _target_owner(assignor, cluster, members_round1)
    moving = {tp for tp, owner in target_owner.items() if owner == "B"}
    assert moving, "the target assignment must move at least one partition to B"

    round1 = assignor.assign(cluster, members_round1)
    a_round1 = as_partitions(round1["A"])
    b_round1 = as_partitions(round1["B"])

    # No partition is owned by two members in the same generation.
    assert a_round1.isdisjoint(b_round1)
    # The partitions that must move but are still claimed by A are withheld
    # from B until A revokes them.
    assert b_round1.isdisjoint(moving)
    assert moving.isdisjoint(a_round1)
    # A keeps everything that is not moving and never receives a moving
    # partition back during the blocking generation.
    assert a_round1 == ALL_PARTITIONS - moving

    # Round 2: A has revoked the moving partitions and no longer claims them.
    members_round2 = {
        "A": member_metadata(assignor, {TOPIC}, a_round1),
        "B": member_metadata(assignor, {TOPIC}, b_round1),
    }
    round2 = assignor.assign(cluster, members_round2)
    a_round2 = as_partitions(round2["A"])
    b_round2 = as_partitions(round2["B"])

    assert a_round2.isdisjoint(b_round2)
    # The handover completes: B now receives exactly the target partitions.
    target_b = {tp for tp, owner in target_owner.items() if owner == "B"}
    target_a = {tp for tp, owner in target_owner.items() if owner == "A"}
    assert b_round2 == target_b
    assert a_round2 == target_a
    # Stable once the handover is done (a third identical round must not move
    # anything again).
    members_round3 = {
        "A": member_metadata(assignor, {TOPIC}, a_round2),
        "B": member_metadata(assignor, {TOPIC}, b_round2),
    }
    round3 = assignor.assign(cluster, members_round3)
    assert as_partitions(round3["A"]) == a_round2
    assert as_partitions(round3["B"]) == b_round2


@pytest.mark.parametrize(
    "assignor",
    [RoundRobinCooperativeAssignor, StickyCooperativeAssignor]
)
def test_assignment_independent_of_member_dict_order(assignor):
    cluster = make_cluster()
    members = {
        "A": member_metadata(assignor, {TOPIC}, [P0, P1, P2]),
        "B": member_metadata(assignor, {TOPIC}, []),
    }
    first = assignor.assign(cluster, dict(reversed(list(members.items()))))
    second = assignor.assign(cluster, dict(list(members.items())))
    assert first.keys() == second.keys()
    for member_id in first:
        assert first[member_id].encode() == second[member_id].encode()


def test_assignments_are_encoded_with_cooperative_version():
    assignor = RoundRobinCooperativeAssignor
    cluster = make_cluster()
    members = {
        "A": member_metadata(assignor, {TOPIC}, ALL_PARTITIONS),
    }
    result = assignor.assign(cluster, members)
    assignment = result["A"]
    assert isinstance(assignment, ConsumerProtocolMemberAssignment)
    assert assignment.version == ConsumerProtocol.COOPERATIVE_PROTOCOL_VERSION
    # Encoded assignment must be decodable through the shared protocol type.
    decoded = ConsumerProtocol.ASSIGNMENT.decode(assignment.encode())
    assert set(decoded.partitions()) == ALL_PARTITIONS
