"""Optional recovery support for Rocketry's ``Retry`` condition.

The module provides partition aware bookkeeping for nightly backfills.  A
task opts in either through the dedicated condition::

    from rocketry.recovery import PartitionRetry

    @app.task(PartitionRetry(partitions=["north", "south"]))
    def collect():
        ...

or by attaching a recovery to the ordinary ``Retry``::

    from rocketry.conditions import Retry
    from rocketry.recovery import Recovery

    retry = Retry(3)
    retry.recovery = Recovery(path="recovery.json")
    retry.recovery.declare_batch("north", "A")
    retry.recovery.declare_batch("south", "B")

Without a ``recovery`` attribute ``Retry`` behaves exactly as before.
"""
from .model import (
    Receipt,
    RecoveryRecord,
    RunState,
    PartitionState,
    StreamCursor,
)
from .store import (
    BaseStateStore,
    MemoryStateStore,
    FileStateStore,
    RecoveryStore,
    FileRecoveryStore,
)
from .ledger import (
    BackfillEntry,
    BackfillLedger,
    BackfillRegistry,
    BackfillStore,
    BackfillJournal,
    default_transaction_id,
)
from .manager import (
    PartitionRecovery,
    PartitionedRecovery,
    RecoveryManager,
    PartitionBackfill,
    BackfillNotAllowed,
    Recovery,
    RecoveryJournal,
)
from .retry import (
    PartitionRetry,
    RecoveryRetry,
    PartitionRetryWrapper,
    partition_retry,
)

__all__ = [
    "Receipt",
    "RecoveryRecord",
    "RunState",
    "PartitionState",
    "StreamCursor",
    "BaseStateStore",
    "MemoryStateStore",
    "FileStateStore",
    "RecoveryStore",
    "FileRecoveryStore",
    "BackfillEntry",
    "BackfillLedger",
    "BackfillRegistry",
    "BackfillStore",
    "BackfillJournal",
    "default_transaction_id",
    "PartitionRecovery",
    "PartitionedRecovery",
    "RecoveryManager",
    "PartitionBackfill",
    "BackfillNotAllowed",
    "Recovery",
    "RecoveryJournal",
    "PartitionRetry",
    "RecoveryRetry",
    "PartitionRetryWrapper",
    "partition_retry",
]
