"""Models shared by the batch synchronisation helpers."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping, Optional


class BatchOutcome(str, Enum):
    """Final disposition of one row in a batch."""

    PENDING = "pending"
    RETAINED = "retained"
    WITHDRAWN = "withdrawn"
    CONFLICT = "conflict"


class LocalStatus(str, Enum):
    PENDING = "pending"
    APPLIED = "applied"
    PRESERVED = "preserved"
    REVERTED = "reverted"
    ABSENT = "absent"
    CONFLICT = "conflict"


class RemoteStatus(str, Enum):
    PENDING = "pending"
    MARKED = "marked"
    CLEARING = "clearing"
    CLEARED = "cleared"
    ABSENT = "absent"
    CONFLICT = "conflict"


@dataclass
class BatchItem:
    """A resource and the local values a batch wants to write."""

    resource_id: Any
    values: Mapping[str, Any] = field(default_factory=dict)
    expected_version: Any = None


class Intent(str, Enum):
    MARK = "mark"
    WITHDRAW = "withdraw"


@dataclass
class BatchEntry:
    """Durable state of one resource in one batch operation."""

    batch_id: Any
    operation_id: Any
    resource_id: Any
    local_status: str = LocalStatus.PENDING.value
    remote_status: str = RemoteStatus.PENDING.value
    outcome: str = BatchOutcome.PENDING.value
    intent: str = Intent.MARK.value
    sequence: int = 0
    planned: Optional[Dict[str, Any]] = None
    applied: Optional[Dict[str, Any]] = None
    previous: Optional[Dict[str, Any]] = None
    previous_existed: bool = False
    expected_version: Any = None
    remote_version: Any = None
    remote_batch: Any = None
    remote_operation: Any = None
    message: Optional[str] = None

    @property
    def retained(self) -> bool:
        return self.outcome == BatchOutcome.RETAINED.value

    @property
    def withdrawn(self) -> bool:
        return self.outcome == BatchOutcome.WITHDRAWN.value

    @property
    def conflict(self) -> bool:
        return self.outcome == BatchOutcome.CONFLICT.value

    @property
    def pending(self) -> bool:
        return self.outcome == BatchOutcome.PENDING.value

    @property
    def final(self) -> bool:
        return not self.pending

    @property
    def owner(self):
        if self.remote_batch is None and self.remote_operation is None:
            return None
        return (self.remote_batch, self.remote_operation)
