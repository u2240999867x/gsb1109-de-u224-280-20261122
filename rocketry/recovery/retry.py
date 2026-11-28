"""Optional partition-aware recovery for :class:`rocketry.conditions.Retry`.

The public entry point is :class:`PartitionRetry` which is a ``BaseCondition``
and therefore a drop-in ``start_cond``::

    from rocketry.recovery import PartitionRetry

    task.start_cond = PartitionRetry(partitions=["north", "south"])

Tasks that do not opt in keep using the plain :class:`~rocketry.conditions.Retry`
behaviour.
"""
from __future__ import annotations

from typing import Optional, Sequence

from rocketry.core.condition import BaseCondition
from rocketry.args import Task, Session

from .manager import PartitionRecovery


class PartitionRetry(BaseCondition):
    """A Retry condition that only backfills confirmed failures.

    Parameters
    ----------
    partitions : sequence of str, optional
        Partitions the task is split into.  Each partition is accounted for
        separately.
    recovery : PartitionRecovery, optional
        Recovery manager holding the persisted record and backfill ledger.
    n : int, optional
        Maximum attempts for the legacy retry fallback.  Only used for
        partitions/runs not governed by a recovery record.
    dispatch : bool, default True
        Whether to register and perform a backfill when one is required.
    task : str, optional
        Task name (otherwise the task being observed).
    dispatch_only : bool, default False
        Do not fall back to the plain Retry condition; only report whether a
        partition backfill is required.
    """

    def __init__(
        self,
        n: Optional[int] = 1,
        partitions: Optional[Sequence[str]] = None,
        recovery: Optional[PartitionRecovery] = None,
        task=None,
        dispatch: bool = True,
        dispatch_only: bool = False,
        legacy: bool = True,
    ):
        self.n = int(n) if n is not None else -1
        self.partitions = list(partitions) if partitions is not None else None
        self.recovery = recovery
        self.task = task
        self.dispatch = dispatch
        self.dispatch_only = dispatch_only
        self.legacy = legacy
        super().__init__()

    def get_recovery(self, task=None, session=None, create: bool = True) -> Optional[PartitionRecovery]:
        if self.recovery is not None:
            recovery = self.recovery
            if recovery.task is None and task is not None:
                recovery.task = task
            if recovery.session is None and session is not None:
                recovery.session = session
        elif create:
            recovery = PartitionRecovery(partitions=self.partitions, task=task, session=session)
            self.recovery = recovery
        else:
            recovery = None
        if recovery is not None and self.partitions:
            for partition in self.partitions:
                recovery.ensure_partition(partition)
        return recovery

    def get_state(self, task=Task(default=None), session=Session()):
        actual_task = session[self.task] if self.task is not None else task
        recovery = self.get_recovery(task=actual_task, session=session)
        if recovery is None:
            return False

        try:
            recovery.ingest_task_log(task=actual_task)
        except Exception:
            if not session.config.silence_cond_check:
                raise

        # Only confirmed failures of the current batch are candidates.  With
        # dispatch enabled the backfill is registered and performed (once)
        # during this observation.
        return bool(recovery.retry_required(
            task=actual_task,
            dispatch=self.dispatch,
        ))

    def __str__(self):
        return f"partition retry({self.partitions})"


# Friendly alias: ``RecoveryRetry`` reads naturally in an application.
RecoveryRetry = PartitionRetry


def partition_retry(
    n: Optional[int] = 1,
    partitions: Optional[Sequence[str]] = None,
    recovery: Optional[PartitionRecovery] = None,
    **kwargs,
) -> PartitionRetry:
    return PartitionRetry(n=n, partitions=partitions, recovery=recovery, **kwargs)


# Condition API wrapper compatible with ``retry(...)``.
class PartitionRetryWrapper:
    def __init__(self, partitions=None, recovery=None, **kwargs):
        self.partitions = partitions
        self.recovery = recovery
        self.kwargs = kwargs

    def __call__(self, n: int = 1, **kwargs):
        options = dict(self.kwargs)
        options.update(kwargs)
        return PartitionRetry(
            n=n,
            partitions=self.partitions,
            recovery=self.recovery,
            **options,
        )

    def get_cond(self):
        return PartitionRetry(
            partitions=self.partitions,
            recovery=self.recovery,
            **self.kwargs,
        )

    def observe(self, **kwargs):
        return self.get_cond().observe(**kwargs)
