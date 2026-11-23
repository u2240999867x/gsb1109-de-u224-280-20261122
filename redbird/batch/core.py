"""Resumable batch synchronisation between a REST source and SQLite.

The synchroniser separates the two sides of a batch operation:

1. Local rows are written and the operation is registered in the SQLite
   ledger in one transaction.
2. Each resource's marker is written to REST with a conditional versioned
   update.

A process may stop between any two steps.  On the next run the ledger is used
to continue from the exact state of every resource; no marker is written twice
and no local value is silently overwritten.
"""
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from redbird.batch.model import (
    BatchEntry,
    BatchItem,
    BatchOutcome,
    Intent,
    LocalStatus,
    RemoteStatus,
)
from redbird.batch.rest import (
    BatchResourceNotFound,
    BatchSourceError,
    BatchTransportError,
    BatchVersionConflict,
    RemoteMarker,
    RESTBatchStore,
)
from redbird.batch.sqlite import SQLiteLedger, _normalise_row


def _row_matches(current: Optional[Mapping], snapshot: Optional[Mapping]) -> bool:
    if current is None and snapshot is None:
        return True
    if current is None or snapshot is None:
        return False
    return _normalise_row(current) == _normalise_row(snapshot)


class ResumeReport:
    """Result of a resume pass."""

    def __init__(self, entries: Sequence[BatchEntry]):
        self.entries = list(entries)

    @property
    def retained(self) -> List[BatchEntry]:
        return [entry for entry in self.entries if entry.outcome == BatchOutcome.RETAINED.value]

    @property
    def conflict(self) -> List[BatchEntry]:
        return [entry for entry in self.entries if entry.outcome == BatchOutcome.CONFLICT.value]

    @property
    def pending(self) -> List[BatchEntry]:
        return [entry for entry in self.entries if entry.outcome == BatchOutcome.PENDING.value]

    @property
    def withdrawn(self) -> List[BatchEntry]:
        return [entry for entry in self.entries if entry.outcome == BatchOutcome.WITHDRAWN.value]

    def by_resource(self) -> Dict[Any, BatchEntry]:
        return {entry.resource_id: entry for entry in self.entries}


class WithdrawReport(ResumeReport):
    """Result of a withdrawal pass."""


class BatchSynchronizer:
    """Coordinate the local ledger and the versioned REST markers.

    Parameters
    ----------
    ledger : SQLiteLedger
        Ledger (and local row access) for the batch.
    source : RESTBatchStore
        Version-aware REST source.

    Examples
    --------
    .. code-block:: python

        ledger = SQLiteLedger(conn_string="sqlite:///sync.db", target_table="items")
        ledger.ensure_schema()
        source = RESTBatchStore("https://example.test/items", id_field="id")

        sync = BatchSynchronizer(ledger=ledger, source=source)
        sync.record("batch-1", "op-1", [
            BatchItem("a", {"name": "Anna"}),
            BatchItem("b", {"name": "Bob"}),
        ])
        sync.resume("batch-1")
    """

    def __init__(self, ledger: SQLiteLedger, source: RESTBatchStore):
        self.ledger = ledger
        self.source = source

    @classmethod
    def from_repos(cls, local_repo, remote_repo, **kwargs):
        """Build from a :class:`SQLRepo` and a :class:`RESTRepo`."""
        ledger_kwargs = dict(kwargs.pop("ledger", {}))
        source_kwargs = dict(kwargs.pop("source", {}))
        if kwargs:
            raise TypeError("Unknown arguments: {}".format(sorted(kwargs)))
        ledger = SQLiteLedger.from_repo(local_repo, **ledger_kwargs)
        ledger.ensure_schema()
        source = RESTBatchStore.from_repo(remote_repo, **source_kwargs)
        return cls(ledger=ledger, source=source)

    def record(self, batch_id, operation_id, items: Iterable[BatchItem], if_exists="upsert") -> List[BatchEntry]:
        """Write local rows and register the operation atomically.

        Calling this again for the same batch/operation is safe.  A row whose
        value was manually changed after the batch wrote it is left alone.
        """
        items = [
            item if isinstance(item, BatchItem) else BatchItem(item, {})
            for item in items
        ]
        return self.ledger.apply(
            batch_id,
            operation_id,
            items,
            if_exists=if_exists,
        )

    def _entries(self, batch_id, operation_id=None, resource_ids=None) -> List[BatchEntry]:
        entries = self.ledger.entries(batch_id=batch_id, operation_id=operation_id)
        if resource_ids is not None:
            wanted = {str(value) for value in resource_ids}
            entries = [entry for entry in entries if str(entry.resource_id) in wanted]
        return entries

    def _marker(self, resource_id) -> RemoteMarker:
        return self.source.read_marker(resource_id)

    def _local_status(self, entry: BatchEntry) -> str:
        current = self.ledger.read_current(entry.resource_id)
        if _row_matches(current, entry.applied):
            return LocalStatus.APPLIED.value
        if _row_matches(current, entry.previous):
            return LocalStatus.REVERTED.value
        if current is None:
            return LocalStatus.ABSENT.value
        if self.ledger.local_owned(entry):
            return LocalStatus.APPLIED.value
        return LocalStatus.PRESERVED.value

    def _read_remote_after_conflict(self, resource_id) -> Optional[RemoteMarker]:
        try:
            return self._marker(resource_id)
        except BatchResourceNotFound:
            return None
        except BatchSourceError:
            return None

    def _apply_remote_marker(self, entry: BatchEntry) -> BatchEntry:
        """Bring one entry's remote marker to the marked state.

        The remote read is the source of truth.  A marker already owned by
        this batch/operation is not written again.
        """
        try:
            marker = self._marker(entry.resource_id)
        except BatchResourceNotFound:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.ABSENT.value,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource no longer exists in the REST source",
            )
        except BatchTransportError as exc:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                outcome=BatchOutcome.PENDING.value,
                message=str(exc),
            )

        if marker.owned_by(entry.batch_id, entry.operation_id):
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.MARKED.value,
                remote_version=marker.version,
                remote_batch=marker.batch_id,
                remote_operation=marker.operation_id,
                outcome=self._outcome_for_local(entry),
                message=None,
            )

        if marker.present:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CONFLICT.value,
                remote_version=marker.version,
                remote_batch=marker.batch_id,
                remote_operation=marker.operation_id,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource is marked by another batch or operation",
            )

        try:
            result = self.source.mark(
                entry.resource_id,
                entry.batch_id,
                entry.operation_id,
                expected_version=marker.version,
            )
        except BatchVersionConflict:
            latest = self._read_remote_after_conflict(entry.resource_id)
            if latest is not None and latest.owned_by(entry.batch_id, entry.operation_id):
                return self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    remote_status=RemoteStatus.MARKED.value,
                    remote_version=latest.version,
                    remote_batch=latest.batch_id,
                    remote_operation=latest.operation_id,
                    outcome=self._outcome_for_local(entry),
                    message=None,
                )
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CONFLICT.value,
                remote_version=None if latest is None else latest.version,
                remote_batch=None if latest is None else latest.batch_id,
                remote_operation=None if latest is None else latest.operation_id,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource version changed before it could be marked",
            )
        except BatchTransportError as exc:
            # The request may or may not have reached the source.  Keep the
            # entry pending; the next resume reads the marker first.
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=entry.remote_status,
                outcome=BatchOutcome.PENDING.value,
                message=str(exc),
            )
        except BatchSourceError as exc:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CONFLICT.value,
                outcome=BatchOutcome.CONFLICT.value,
                message=str(exc),
            )

        return self.ledger.update_entry(
            entry.batch_id,
            entry.operation_id,
            entry.resource_id,
            remote_status=RemoteStatus.MARKED.value,
            remote_version=result.version,
            remote_batch=result.batch_id,
            remote_operation=result.operation_id,
            outcome=self._outcome_for_local(entry),
            message=None,
        )

    def _outcome_for_local(self, entry: BatchEntry) -> str:
        local = self._local_status(entry)
        if local in (LocalStatus.APPLIED.value, LocalStatus.PRESERVED.value):
            return BatchOutcome.RETAINED.value
        if local in (LocalStatus.REVERTED.value, LocalStatus.ABSENT.value):
            return BatchOutcome.CONFLICT.value
        return BatchOutcome.PENDING.value

    def resume(self, batch_id, operation_id=None, resource_ids=None) -> ResumeReport:
        """Continue a batch after an interruption.

        The pass is idempotent.  It reads every remote marker before writing,
        skips markers already owned by this operation, records markers held by
        another operation as conflicts, and leaves manually corrected local
        values untouched.
        """
        entries = self._entries(batch_id, operation_id=operation_id, resource_ids=resource_ids)
        result = []
        for entry in entries:
            if entry.outcome == BatchOutcome.WITHDRAWN.value:
                result.append(entry)
                continue
            if entry.intent == Intent.WITHDRAW.value:
                # A previous withdrawal stopped between the REST and SQLite
                # steps.  Continue the withdrawal; resuming the mark would
                # re-claim a resource the operation was giving back.
                entry = self._continue_withdraw(entry)
                result.append(entry)
                continue

            local = self._local_status(entry)
            update = {}
            if local != entry.local_status:
                update["local_status"] = local
            if local in (LocalStatus.APPLIED.value, LocalStatus.PRESERVED.value):
                if entry.outcome == BatchOutcome.CONFLICT.value:
                    # A conflict is final unless a later explicit operation
                    # changes ownership.  Do not rewrite the remote marker.
                    result.append(entry)
                    continue
                entry = self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    **update
                ) if update else entry
                entry = self._apply_remote_marker(entry)
            else:
                entry = self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    local_status=local,
                    outcome=BatchOutcome.CONFLICT.value,
                    message="Local row no longer matches the batch write",
                )
            result.append(entry)
        return ResumeReport(result)

    def _clear_remote(self, entry: BatchEntry) -> BatchEntry:
        """Bring one entry's remote marker to the cleared state.

        ``clearing`` is persisted before the conditional request.  If the
        response is lost, the next pass can distinguish "we may have cleared
        it" from "someone else owns it" and continue.
        """
        try:
            marker = self._marker(entry.resource_id)
        except BatchResourceNotFound:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.ABSENT.value,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource no longer exists in the REST source",
            )
        except BatchTransportError as exc:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                outcome=BatchOutcome.PENDING.value,
                message=str(exc),
            )

        if marker.owned_by(entry.batch_id, entry.operation_id):
            entry = self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CLEARING.value,
                remote_version=marker.version,
                remote_batch=marker.batch_id,
                remote_operation=marker.operation_id,
            )
        elif marker.present:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CONFLICT.value,
                remote_version=marker.version,
                remote_batch=marker.batch_id,
                remote_operation=marker.operation_id,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource is marked by another batch or operation",
            )
        elif entry.remote_status in (RemoteStatus.CLEARING.value, RemoteStatus.CLEARED.value):
            # The previous pass may have cleared the marker and crashed before
            # recording the response.
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CLEARED.value,
                remote_version=marker.version,
                remote_batch=marker.batch_id,
                remote_operation=marker.operation_id,
            )
        elif entry.remote_status in (RemoteStatus.PENDING.value, RemoteStatus.ABSENT.value):
            # Nothing was ever confirmed as marked.  There is nothing to clear;
            # the local row can be reverted without touching the source.
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CLEARED.value,
                remote_version=marker.version,
                remote_batch=marker.batch_id,
                remote_operation=marker.operation_id,
                message=None,
            )
        else:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.ABSENT.value,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource has no marker owned by this batch",
            )

        try:
            result = self.source.clear_marker(
                entry.resource_id,
                entry.batch_id,
                entry.operation_id,
                expected_version=entry.remote_version,
            )
        except BatchVersionConflict:
            latest = self._read_remote_after_conflict(entry.resource_id)
            if latest is None:
                return self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    remote_status=RemoteStatus.CLEARED.value,
                    remote_version=None,
                )
            if latest.owned_by(entry.batch_id, entry.operation_id):
                return self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    remote_status=RemoteStatus.CLEARING.value,
                    remote_version=latest.version,
                    message="Marker version changed; retry withdrawal",
                )
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CONFLICT.value,
                remote_version=latest.version,
                remote_batch=latest.batch_id,
                remote_operation=latest.operation_id,
                outcome=BatchOutcome.CONFLICT.value,
                message="Resource was taken over before it could be cleared",
            )
        except BatchTransportError as exc:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                outcome=BatchOutcome.PENDING.value,
                message=str(exc),
            )
        except BatchSourceError as exc:
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                remote_status=RemoteStatus.CONFLICT.value,
                outcome=BatchOutcome.CONFLICT.value,
                message=str(exc),
            )

        return self.ledger.update_entry(
            entry.batch_id,
            entry.operation_id,
            entry.resource_id,
            remote_status=RemoteStatus.CLEARED.value,
            remote_version=result.version,
            remote_batch=result.batch_id,
            remote_operation=result.operation_id,
        )

    def withdraw(self, batch_id, operation_id=None, resource_ids=None) -> WithdrawReport:
        """Withdraw rows still owned by the batch.

        A row is withdrawn only when both sides still belong to this batch:

        - the REST marker is owned by ``batch_id``/``operation_id`` (or was
          already cleared by this operation), and
        - the local row still equals the value written by the batch.

        If either side was superseded, the entry is recorded as a conflict and
        neither side is changed.  All state transitions are persisted so an
        interruption between the REST and SQLite steps can be resumed.
        """
        entries = self._entries(batch_id, operation_id=operation_id, resource_ids=resource_ids)
        result = []
        for entry in entries:
            if entry.outcome == BatchOutcome.WITHDRAWN.value:
                result.append(entry)
                continue
            if entry.outcome == BatchOutcome.CONFLICT.value:
                result.append(entry)
                continue

            local = self._local_status(entry)
            if local not in (LocalStatus.APPLIED.value, LocalStatus.REVERTED.value):
                # A manual correction or another writer superseded the batch's
                # local change.  Do not clear the marker either: the two sides
                # no longer form a single reversible operation.
                result.append(self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    local_status=local,
                    outcome=BatchOutcome.CONFLICT.value,
                    message="Local row was changed after the batch write",
                ))
                continue

            # Persist the withdrawal intent before touching either side.  If
            # the process stops now, resume() continues the withdrawal.
            if entry.intent != Intent.WITHDRAW.value:
                entry = self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    intent=Intent.WITHDRAW.value,
                )
            result.append(self._continue_withdraw(entry))
        return WithdrawReport(result)

    def _continue_withdraw(self, entry: BatchEntry) -> BatchEntry:
        """Continue a persisted withdrawal from its last durable stage."""
        if entry.outcome == BatchOutcome.WITHDRAWN.value:
            return entry

        local = self._local_status(entry)
        if local not in (LocalStatus.APPLIED.value, LocalStatus.REVERTED.value):
            # The local row changed after the withdrawal started.  Do not
            # clear a marker based on a value the batch no longer owns.
            return self.ledger.update_entry(
                entry.batch_id,
                entry.operation_id,
                entry.resource_id,
                local_status=local,
                outcome=BatchOutcome.CONFLICT.value,
                message="Local row was changed after the batch write",
            )

        if entry.remote_status in (RemoteStatus.CLEARING.value, RemoteStatus.CLEARED.value):
            # The remote side is already clear (or may be).  Verify it before
            # reverting locally; a resource taken over remotely is a conflict.
            try:
                marker = self._marker(entry.resource_id)
            except BatchResourceNotFound:
                marker = None
            except BatchTransportError as exc:
                return self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    outcome=BatchOutcome.PENDING.value,
                    message=str(exc),
                )
            if marker is not None and marker.present:
                # CLEARING may mean the clear request never reached the
                # source.  If the marker is still ours, finish clearing.
                entry = self._clear_remote(entry)
                if entry.remote_status != RemoteStatus.CLEARED.value:
                    return entry
            else:
                entry = self.ledger.update_entry(
                    entry.batch_id,
                    entry.operation_id,
                    entry.resource_id,
                    remote_status=RemoteStatus.CLEARED.value,
                    remote_version=None if marker is None else marker.version,
                    message=None,
                )
        else:
            entry = self._clear_remote(entry)
            if entry.remote_status != RemoteStatus.CLEARED.value:
                return entry

        return self.ledger.withdraw_local(
            entry,
            conflict_message="Local row was changed after the batch write",
        )

    def status(self, batch_id, operation_id=None) -> Dict[Any, BatchEntry]:
        """Return the last state of every resource in a batch."""
        return {
            entry.resource_id: entry
            for entry in self.ledger.entries(batch_id=batch_id, operation_id=operation_id)
        }

    def outcome(self, batch_id, resource_id, operation_id=None) -> Optional[str]:
        """Return ``retained``, ``withdrawn``, ``conflict`` or ``pending``.

        ``None`` means the resource was never part of the batch.
        """
        return self.ledger.outcome(batch_id, operation_id, resource_id)

    def summary(self, batch_id, operation_id=None) -> Dict[str, int]:
        """Count retained, withdrawn, conflict and pending entries."""
        return self.ledger.summary(batch_id=batch_id, operation_id=operation_id)
