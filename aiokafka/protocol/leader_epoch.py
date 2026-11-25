from typing import TypeAlias

from .api import Request, RequestStruct, Response
from .types import Array, Int16, Int32, Int64, Schema, String


class OffsetsForLeaderEpochResponse_v0(Response):
    API_KEY = 23
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("leader_epoch", Int32),
                        ("end_offset", Int64),
                    ),
                ),
            ),
        ),
    )


class OffsetsForLeaderEpochResponse_v1(Response):
    """
    Add per-partition error_code (KIP-320), e.g. FENCED_LEADER_EPOCH and
    UNKNOWN_LEADER_EPOCH.
    """

    API_KEY = 23
    API_VERSION = 1
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("error_code", Int16),
                        ("partition", Int32),
                        ("leader_epoch", Int32),
                        ("end_offset", Int64),
                    ),
                ),
            ),
        ),
    )


class OffsetsForLeaderEpochRequest_v0(RequestStruct):
    API_KEY = 23
    API_VERSION = 0
    RESPONSE_TYPE = OffsetsForLeaderEpochResponse_v0
    SCHEMA = Schema(
        ("replica_id", Int32),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("leader_epoch", Int32),
                    ),
                ),
            ),
        ),
    )
    DEFAULTS = {"replica_id": -1}


class OffsetsForLeaderEpochRequest_v1(RequestStruct):
    API_KEY = 23
    API_VERSION = 1
    RESPONSE_TYPE = OffsetsForLeaderEpochResponse_v1
    SCHEMA = OffsetsForLeaderEpochRequest_v0.SCHEMA
    DEFAULTS = {"replica_id": -1}


OffsetsForLeaderEpochRequestStruct: TypeAlias = (
    OffsetsForLeaderEpochRequest_v0 | OffsetsForLeaderEpochRequest_v1
)


class OffsetsForLeaderEpochRequest(Request[OffsetsForLeaderEpochRequestStruct]):
    """Ask the partition leader for the log end offset of a given leader epoch.

    ``topics`` maps topic names to ``(partition, leader_epoch)`` pairs. The
    leader replies with the end offset of that epoch: the first offset that is
    not part of the requested leader epoch's log anymore.
    """

    API_KEY = 23

    def __init__(
        self,
        topics: list[tuple[str, list[tuple[int, int]]]],
        replica_id: int = -1,
    ):
        self._topics = topics
        self._replica_id = replica_id

    @property
    def topics(self) -> list[tuple[str, list[tuple[int, int]]]]:
        return self._topics

    def build(
        self,
        request_struct_class: type[OffsetsForLeaderEpochRequestStruct],
    ) -> OffsetsForLeaderEpochRequestStruct:
        return request_struct_class(self._replica_id, self._topics)
