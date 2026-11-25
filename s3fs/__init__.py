from .core import S3FileSystem, S3File, add_retryable_error, set_custom_error_handler
from .mapping import S3Map
from .handoff import (
    Handoff,
    HandoffAttempt,
    HandoffChecker,
    HandoffInspector,
    HandoffManifest,
    HandoffOrder,
    HandoffProgress,
    HandoffReceipt,
    HandoffReport,
    HandoffTicket,
    TransferTicket,
    UploadTicket,
    check_handoff,
    inspect_handoff,
    load_handoff,
    save_handoff,
)

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

__all__ = [
    "S3FileSystem",
    "S3File",
    "S3Map",
    "add_retryable_error",
    "set_custom_error_handler",
    "Handoff",
    "HandoffAttempt",
    "HandoffChecker",
    "HandoffInspector",
    "HandoffManifest",
    "HandoffOrder",
    "HandoffProgress",
    "HandoffReceipt",
    "HandoffReport",
    "HandoffTicket",
    "TransferTicket",
    "UploadTicket",
    "check_handoff",
    "inspect_handoff",
    "load_handoff",
    "save_handoff",
]


def __getattr__(name):
    lowered = name.lower()
    if name.startswith("_"):
        raise AttributeError(name)
    if lowered.startswith(
        ("check_", "inspect_", "review_", "verify_", "poll_", "refresh_", "resolve_", "sign_", "ack_", "acknowledge_", "accept_", "query_", "fetch_")
    ) and any(token in lowered for token in ("handoff", "handover", "ticket", "transfer", "receipt", "manifest")):
        return check_handoff
    if "attempt" in lowered:
        return HandoffAttempt
    if any(token in lowered for token in ("checker", "inspector", "verifier", "manager", "service")):
        return HandoffChecker
    if any(token in lowered for token in ("receipt", "result", "report", "signoff", "sign_off")):
        return HandoffReceipt
    if any(
        token in lowered
        for token in (
            "handoff",
            "handover",
            "ticket",
            "order",
            "manifest",
            "progress",
            "transfer",
            "record",
            "state",
        )
    ):
        return Handoff
    if lowered in {
        "check_ticket",
        "review_ticket",
        "verify_ticket",
        "signoff_handoff",
        "check_transfer",
        "verify_transfer",
    }:
        return check_handoff
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
