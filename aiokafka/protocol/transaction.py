from typing import TypeAlias

from aiokafka.errors import IncompatibleBrokerVersion
from aiokafka.structs import ConsumerGroupMetadata

from .api import Request, RequestStruct, Response
from .types import (
    Array,
    Boolean,
    CompactArray,
    CompactString,
    Int16,
    Int32,
    Int64,
    Schema,
    String,
)


class InitProducerIdResponse_v0(Response):
    API_KEY = 22
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
    )


class InitProducerIdRequest_v0(RequestStruct):
    API_KEY = 22
    API_VERSION = 0
    RESPONSE_TYPE = InitProducerIdResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")), ("transaction_timeout_ms", Int32)
    )


InitProducerIdRequestStruct: TypeAlias = InitProducerIdRequest_v0


class InitProducerIdRequest(Request[InitProducerIdRequestStruct]):
    API_KEY = 22
    CLASSES = [InitProducerIdRequest_v0]

    def __init__(self, transactional_id: str, transaction_timeout_ms: int):
        self._transactional_id = transactional_id
        self._transaction_timeout_ms = transaction_timeout_ms

    def build(
        self, request_struct_class: type[InitProducerIdRequestStruct]
    ) -> InitProducerIdRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._transaction_timeout_ms,
        )


class AddPartitionsToTxnResponse_v0(Response):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            Array(
                ("topic", String("utf-8")),
                (
                    "partition_errors",
                    Array(("partition", Int32), ("error_code", Int16)),
                ),
            ),
        ),
    )


class AddPartitionsToTxnRequest_v0(RequestStruct):
    API_KEY = 24
    API_VERSION = 0
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("topics", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
    )


AddPartitionsToTxnRequestStruct: TypeAlias = AddPartitionsToTxnRequest_v0


class AddPartitionsToTxnRequest(Request[AddPartitionsToTxnRequestStruct]):
    API_KEY = 24

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        topics: list[tuple[str, list[int]]],
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._topics = topics

    def build(
        self, request_struct_class: type[AddPartitionsToTxnRequestStruct]
    ) -> AddPartitionsToTxnRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._topics,
        )


class AddOffsetsToTxnResponse_v0(Response):
    API_KEY = 25
    API_VERSION = 0
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class AddOffsetsToTxnRequest_v0(RequestStruct):
    API_KEY = 25
    API_VERSION = 0
    RESPONSE_TYPE = AddOffsetsToTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("group_id", String("utf-8")),
    )


AddOffsetsToTxnRequestStruct: TypeAlias = AddOffsetsToTxnRequest_v0


class AddOffsetsToTxnRequest(Request[AddOffsetsToTxnRequestStruct]):
    API_KEY = 25

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        group_id: str,
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._group_id = group_id

    def build(
        self, request_struct_class: type[AddOffsetsToTxnRequestStruct]
    ) -> AddOffsetsToTxnRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._group_id,
        )


class EndTxnResponse_v0(Response):
    API_KEY = 26
    API_VERSION = 0
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class EndTxnRequest_v0(RequestStruct):
    API_KEY = 26
    API_VERSION = 0
    RESPONSE_TYPE = EndTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("transaction_result", Boolean),
    )


EndTxnRequestStruct: TypeAlias = EndTxnRequest_v0


class EndTxnRequest(Request[EndTxnRequestStruct]):
    API_KEY = 26

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        transaction_result: bool,
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._transaction_result = transaction_result

    def build(
        self, request_struct_class: type[EndTxnRequestStruct]
    ) -> EndTxnRequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._transaction_result,
        )


class TxnOffsetCommitResponse_v0(Response):
    API_KEY = 28
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            Array(
                ("topic", String("utf-8")),
                (
                    "partition_errors",
                    Array(("partition", Int32), ("error_code", Int16)),
                ),
            ),
        ),
    )


class TxnOffsetCommitResponse_v3(Response):
    API_KEY = 28
    API_VERSION = 3
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            CompactArray(
                ("topic", CompactString("utf-8")),
                (
                    "partition_errors",
                    CompactArray(
                        ("partition", Int32),
                        ("error_code", Int16),
                        tagged_fields=(),
                    ),
                ),
                tagged_fields=(),
            ),
        ),
        tagged_fields=(),
    )


class TxnOffsetCommitRequest_v0(RequestStruct):
    API_KEY = 28
    API_VERSION = 0
    RESPONSE_TYPE = TxnOffsetCommitResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("group_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                    ),
                ),
            ),
        ),
    )


class TxnOffsetCommitRequest_v1(RequestStruct):
    API_KEY = 28
    API_VERSION = 1
    RESPONSE_TYPE = TxnOffsetCommitResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("group_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("generation_id", Int32),
        ("member_id", String("utf-8")),
        ("group_instance_id", String("utf-8")),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                    ),
                ),
            ),
        ),
    )

    def __init__(self, *args, **kwargs):
        if not args:
            if "consumer_group_generation_id" in kwargs:
                kwargs["generation_id"] = kwargs.pop("consumer_group_generation_id")
            if "generation" in kwargs:
                kwargs["generation_id"] = kwargs.pop("generation")
            if "consumer_id" in kwargs:
                kwargs["member_id"] = kwargs.pop("consumer_id")
            if "consumer_group_member_id" in kwargs:
                kwargs["member_id"] = kwargs.pop("consumer_group_member_id")
        super().__init__(*args, **kwargs)

    @property
    def generation(self):
        return self.generation_id

    @property
    def consumer_group_generation_id(self):
        return self.generation_id

    @property
    def consumer_id(self):
        return self.member_id

    @property
    def consumer_group_member_id(self):
        return self.member_id


class TxnOffsetCommitRequest_v2(TxnOffsetCommitRequest_v1):
    API_VERSION = 2


class TxnOffsetCommitRequest_v3(TxnOffsetCommitRequest_v1):
    API_VERSION = 3
    RESPONSE_TYPE = TxnOffsetCommitResponse_v3
    SCHEMA = Schema(
        ("transactional_id", CompactString("utf-8")),
        ("group_id", CompactString("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("generation_id", Int32),
        ("member_id", CompactString("utf-8")),
        ("group_instance_id", CompactString("utf-8")),
        (
            "topics",
            CompactArray(
                ("topic", CompactString("utf-8")),
                (
                    "partitions",
                    CompactArray(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", CompactString("utf-8")),
                        tagged_fields=(),
                    ),
                ),
                tagged_fields=(),
            ),
        ),
        tagged_fields=(),
    )


TxnOffsetCommitRequestStruct: TypeAlias = (
    TxnOffsetCommitRequest_v0
    | TxnOffsetCommitRequest_v1
    | TxnOffsetCommitRequest_v2
    | TxnOffsetCommitRequest_v3
)


class TxnOffsetCommitRequest(Request[TxnOffsetCommitRequestStruct]):
    API_KEY = 28

    def __init__(self, *args, **kwargs):
        self._group_metadata = kwargs.pop("group_metadata", None)
        if self._group_metadata is None:
            self._group_metadata = kwargs.pop("metadata", None)
        if self._group_metadata is None:
            self._group_metadata = kwargs.pop("consumer_group_metadata", None)

        self._generation_id = kwargs.pop("generation_id", None)
        if self._generation_id is None:
            self._generation_id = kwargs.pop("consumer_group_generation_id", None)
        if self._generation_id is None:
            self._generation_id = kwargs.pop("generation", None)

        self._member_id = kwargs.pop("member_id", None)
        if self._member_id is None:
            self._member_id = kwargs.pop("consumer_id", None)
        if self._member_id is None:
            self._member_id = kwargs.pop("consumer_group_member_id", None)

        self._group_instance_id = kwargs.pop("group_instance_id", None)
        if self._group_instance_id is None:
            self._group_instance_id = kwargs.pop("instance_id", None)

        if len(args) >= 5 and isinstance(args[4], list | tuple):
            (
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                topics,
            ) = args[:5]
            extra = args[5:]
            generation_id = extra[0] if len(extra) > 0 else None
            member_id = extra[1] if len(extra) > 1 else None
            group_instance_id = extra[2] if len(extra) > 2 else None
        elif len(args) == 8:
            (
                transactional_id,
                group_id,
                producer_id,
                producer_epoch,
                generation_id,
                member_id,
                group_instance_id,
                topics,
            ) = args
        else:
            transactional_id = kwargs.pop("transactional_id", None)
            group_id = kwargs.pop("group_id", None)
            producer_id = kwargs.pop("producer_id", None)
            producer_epoch = kwargs.pop("producer_epoch", None)
            topics = kwargs.pop("topics", None)
            generation_id = kwargs.pop("generation_id", None)
            member_id = kwargs.pop("member_id", None)
            group_instance_id = kwargs.pop("group_instance_id", None)

            if args:
                positional = [
                    transactional_id,
                    group_id,
                    producer_id,
                    producer_epoch,
                    topics,
                ]
                arg_index = 0
                for index, value in enumerate(positional):
                    if value is None and arg_index < len(args):
                        positional[index] = args[arg_index]
                        arg_index += 1
                if arg_index != len(args):
                    raise TypeError(
                        f"Unexpected positional arguments: {args[arg_index:]!r}"
                    )
                (
                    transactional_id,
                    group_id,
                    producer_id,
                    producer_epoch,
                    topics,
                ) = positional
            if kwargs:
                raise TypeError(f"Unexpected keyword arguments: {kwargs!r}")

        self._init_values(
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            topics,
            generation_id,
            member_id,
            group_instance_id,
        )

    def _init_values(
        self,
        transactional_id,
        group_id,
        producer_id,
        producer_epoch,
        topics,
        generation_id=None,
        member_id=None,
        group_instance_id=None,
    ):
        if self._group_metadata is not None:
            metadata = self._group_metadata
            self._group_id = getattr(metadata, "group_id", group_id)
            self._generation_id = getattr(
                metadata, "generation_id", self._generation_id
            )
            self._member_id = getattr(metadata, "member_id", self._member_id)
            self._group_instance_id = getattr(
                metadata,
                "group_instance_id",
                self._group_instance_id,
            )
        else:
            self._group_id = group_id

        if generation_id is not None:
            self._generation_id = generation_id
        if member_id is not None:
            self._member_id = member_id
        if group_instance_id is not None:
            self._group_instance_id = group_instance_id

        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._topics = topics
        if self._generation_id is None:
            self._generation_id = -1
        if self._member_id is None:
            self._member_id = ""

    @property
    def group_metadata(self):
        return ConsumerGroupMetadata(
            group_id=self._group_id,
            generation_id=self._generation_id,
            member_id=self._member_id,
            group_instance_id=self._group_instance_id,
        )

    def build(
        self, request_struct_class: type[TxnOffsetCommitRequestStruct]
    ) -> TxnOffsetCommitRequestStruct:
        metadata = self.group_metadata
        if request_struct_class.API_VERSION == 0:
            if metadata.has_member_identity:
                raise IncompatibleBrokerVersion(
                    "TxnOffsetCommitRequest with consumer group generation, "
                    "member id, or group instance id requires version >= 1"
                )
            return TxnOffsetCommitRequest_v0(
                self._transactional_id,
                self._group_id,
                self._producer_id,
                self._producer_epoch,
                self._topics,
            )

        return request_struct_class(
            self._transactional_id,
            self._group_id,
            self._producer_id,
            self._producer_epoch,
            self._generation_id,
            self._member_id,
            self._group_instance_id,
            self._topics,
        )
