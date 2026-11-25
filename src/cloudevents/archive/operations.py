from dataclasses import dataclass
from typing import Final, Union

from cloudevents.core.bindings.amqp import AMQPMessage
from cloudevents.core.bindings.http import HTTPMessage

LANE_INTEGER: Final[str] = "integer"
LANE_STRING: Final[str] = "string"


@dataclass(frozen=True)
class IngestHTTP:
    """
    Ingest a raw HTTP message (binary content mode with ce- headers, or
    structured content mode) as a new receipt.

    Extension values carried by HTTP headers are plain strings, so the
    original type of the 'lane' extension is unconfirmed for binary mode
    receipts and stays pending until source manifests confirm it.
    """

    message: HTTPMessage


@dataclass(frozen=True)
class IngestAMQP:
    """
    Ingest a raw AMQP message as a new receipt.

    AMQP application properties preserve native types, so the type of the
    'lane' extension is confirmed directly from the binding.
    """

    message: AMQPMessage


@dataclass(frozen=True)
class IngestJSON:
    """
    Ingest a structured JSON-format CloudEvent as a new receipt.

    JSON types are preserved, so the type of the 'lane' extension is
    confirmed directly from the document. Data delivered via 'data_base64'
    is treated as binary, data delivered via 'data' as text.
    """

    body: bytes


@dataclass(frozen=True)
class RegisterManifest:
    """
    Register a source manifest declaring the original 'lane' type of a
    receipt whose lane type is unconfirmed (legacy HTTP receipts).

    Manifest ids are numbered independently and must not be reused.
    """

    manifest_id: str
    receipt_id: int
    lane_type: str


@dataclass(frozen=True)
class RevokeManifest:
    """
    Revoke a previously registered manifest. Once the remaining active
    manifests of a receipt agree, the receipt regains a determined
    business line.
    """

    manifest_id: str


@dataclass(frozen=True)
class WithdrawReceipt:
    """
    Withdraw a receipt from the current ingestion scope. The receipt
    leaves its business line, but its raw content stays queryable.
    """

    receipt_id: int


Operation = Union[
    IngestHTTP,
    IngestAMQP,
    IngestJSON,
    RegisterManifest,
    RevokeManifest,
    WithdrawReceipt,
]
