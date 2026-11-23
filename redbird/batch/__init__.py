"""Batch synchronisation helpers.

This package adds a small orchestration layer on top of Red Bird's REST and
SQL repositories for the common "stage a batch locally, then mark it in the
source" workflow.

Main entry points:

- :class:`~redbird.batch.core.BatchSynchronizer`: record, resume and withdraw
  a batch operation.
- :class:`~redbird.batch.sqlite.SQLiteLedger`: durable SQLite ledger for
  operations and local rows.
- :class:`~redbird.batch.rest.RESTBatchStore`: conditional, version-aware REST
  marker client.
- :class:`~redbird.batch.model.BatchItem`: one resource to stage.
"""
from redbird.batch.model import (
    BatchEntry,
    BatchItem,
    BatchOutcome,
    LocalStatus,
    RemoteStatus,
)
from redbird.batch.sqlite import SQLiteLedger
from redbird.batch.rest import (
    BatchRequestError,
    BatchResourceNotFound,
    BatchSourceError,
    BatchTransportError,
    BatchVersionConflict,
    RESTBatchStore,
    RemoteMarker,
)
from redbird.batch.core import BatchSynchronizer, ResumeReport, WithdrawReport

__all__ = [
    "BatchSynchronizer",
    "BatchItem",
    "BatchEntry",
    "BatchOutcome",
    "BatchSourceError",
    "BatchRequestError",
    "BatchResourceNotFound",
    "BatchTransportError",
    "BatchVersionConflict",
    "LocalStatus",
    "RemoteMarker",
    "RemoteStatus",
    "RESTBatchStore",
    "ResumeReport",
    "SQLiteLedger",
    "WithdrawReport",
]
