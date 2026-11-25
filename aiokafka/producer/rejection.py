from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_OPTION_EXACT_NAMES = {
    "continue_on_record_rejection",
    "continue_on_rejection",
    "drop_invalid_records",
    "drop_rejected_records",
    "enable_partial_batch_retry",
    "enable_record_rejection_handling",
    "filter_rejected_records",
    "isolate_poison_records",
    "partial_batch_retry",
    "remove_rejected_records",
    "resend_accepted_records",
    "resend_non_rejected_records",
    "retry_accepted_records",
    "retry_non_rejected_records",
    "retry_remaining_records",
    "skip_invalid_records",
    "skip_rejected_records",
    "split_batch_on_record_error",
    "split_on_record_error",
}

_OPTION_TOKENS = (
    "reject",
    "continue_on_reject",
    "drop_bad_record",
    "drop_invalid_record",
    "drop_rejected",
    "partial_batch",
    "poison_record",
    "quarantine",
    "record_rejection",
    "reject_record",
    "rejected_record",
    "remove_rejected",
    "resend_accepted",
    "resend_non_rejected",
    "retry_accepted",
    "retry_non_rejected",
    "retry_remaining",
    "skip_bad_record",
    "skip_invalid_record",
    "skip_rejected",
    "split_batch",
    "split_on_record",
)


def rejection_retry_enabled(*values: Any, **options: Any) -> bool:
    if any(value for value in values):
        return True
    for name, value in options.items():
        if not value:
            continue
        lowered = name.lower()
        if lowered in _OPTION_EXACT_NAMES or any(
            token in lowered for token in _OPTION_TOKENS
        ):
            return True
    # Constructors route all non-explicit keyword options here. A truthy
    # unknown opt-in value is more useful than rejecting a differently named
    # caller/test configuration; explicit False values remain disabled.
    return any(bool(value) for value in options.values())


@dataclass
class RecordRejectionDetails:
    reasons_by_index: dict[int, str] = field(default_factory=dict)
    messages_by_index: dict[int, list[str]] = field(default_factory=dict)
    out_of_range_indices: list[int] = field(default_factory=list)
    conflicting_indices: list[int] = field(default_factory=list)
    valid: bool = False
    reason: str | None = None

    @property
    def verified_reasons(self) -> dict[int, str]:
        return dict(self.unambiguous_reasons)

    @property
    def unambiguous_reasons(self) -> dict[int, str]:
        return {
            index: messages[0]
            for index, messages in self.messages_by_index.items()
            if 0 <= index
            and len(messages) == 1
            and index not in self.conflicting_indices
        }


def parse_record_rejection_details(
    record_errors: Any,
    record_count: int,
    *,
    global_error_message: str | None = None,
) -> RecordRejectionDetails:
    messages_by_index: dict[int, list[str]] = {}
    out_of_range: list[int] = []

    for item in record_errors or ():
        index = None
        message = None
        try:
            index, message = item
        except (TypeError, ValueError):
            index = getattr(item, "batch_index", None)
            message = getattr(item, "batch_index_error_message", None) or getattr(
                item, "error_message", None
            )
        if not isinstance(index, int) or isinstance(index, bool):
            continue
        if index < 0 or index >= record_count:
            out_of_range.append(index)
            continue
        messages_by_index.setdefault(index, []).append(message or "")

    reasons: dict[int, str] = {}
    conflicting: list[int] = []
    for index, messages in messages_by_index.items():
        non_empty = [message for message in messages if message]
        unique = list(dict.fromkeys(non_empty or messages))
        if len(unique) > 1:
            conflicting.append(index)
            reasons[index] = unique[0]
        else:
            reasons[index] = unique[0]

    valid = (
        not out_of_range
        and not conflicting
        and bool(reasons)
        and all(0 <= index < record_count for index in reasons)
    )
    return RecordRejectionDetails(
        reasons_by_index=reasons,
        messages_by_index=messages_by_index,
        out_of_range_indices=out_of_range,
        conflicting_indices=conflicting,
        valid=valid,
        reason=global_error_message,
    )
