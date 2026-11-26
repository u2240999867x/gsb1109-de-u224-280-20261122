from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # pragma: no cover
    from piccolo.table import Table


_MISSING = object()


@dataclass
class InstanceStateChange:
    """
    Records an attribute on a ``Table`` instance, which was changed by
    Piccolo as part of a database write.

    Only attributes which Piccolo itself modifies are recorded - for example
    the primary key returned by an insert, ``_exists_in_db``, or the value of
    an ``auto_update`` column. Values assigned by the user aren't recorded, so
    they survive a savepoint rollback.
    """

    instance: Table
    attribute: str
    old_value: Any
    had_old_value: bool


class InstanceStateTracker:
    """
    Keeps a journal of the changes Piccolo makes to ``Table`` instances, so
    they can be undone when a savepoint (or the transaction itself) is rolled
    back.

    The journal is ordered. A savepoint stores the journal's length when it's
    created - rolling back to that savepoint replays the journal in reverse up
    to that point. Releasing a savepoint leaves the journal untouched, which
    means an outer savepoint can still undo those changes.
    """

    __slots__ = ("_changes",)

    def __init__(self) -> None:
        self._changes: list[InstanceStateChange] = []

    def __len__(self) -> int:
        return len(self._changes)

    @property
    def change_count(self) -> int:
        return len(self._changes)

    def record(self, instance: Table, attribute: str) -> None:
        """
        Record the current value of an attribute before Piccolo changes it.

        If the same attribute is changed several times within one savepoint,
        we only need the first value - that's the one which existed before
        this layer started.
        """
        values = instance.__dict__
        had_old_value = attribute in values
        self._changes.append(
            InstanceStateChange(
                instance=instance,
                attribute=attribute,
                old_value=values.get(attribute, _MISSING),
                had_old_value=had_old_value,
            )
        )

    def rollback_to(self, index: int = 0, apply: bool = True) -> None:
        """
        Undo all changes after ``index``, in reverse order.

        :param apply:
            If ``False``, the journal is truncated, but the instance
            attributes are left alone - used for the legacy behaviour, where
            tracking isn't enabled.
        """
        if index < 0:
            index = 0

        while len(self._changes) > index:
            change = self._changes.pop()
            if not apply:
                continue
            if change.had_old_value:
                setattr(
                    change.instance,
                    change.attribute,
                    change.old_value,
                )
            else:
                change.instance.__dict__.pop(change.attribute, None)

    def commit(self) -> None:
        """
        The changes are now permanent, so the journal is no longer needed.
        """
        self._changes.clear()


###############################################################################


def get_instance_state_tracker(
    instance: Table,
) -> Optional[InstanceStateTracker]:
    """
    Find the tracker for the active transaction, if instance tracking is
    enabled.
    """
    try:
        engine = instance.__class__._meta.db
    except Exception:  # pragma: no cover - no engine configured
        return None

    if engine is None:
        return None

    transaction = engine.current_transaction.get()
    if transaction is None:
        return None

    return getattr(transaction, "_instance_tracker", None)


def set_instance_attribute(
    instance: Table, attribute: str, value: Any
) -> None:
    """
    Set an attribute on a ``Table`` instance, recording the old value first if
    instance tracking is active.
    """
    tracker = get_instance_state_tracker(instance)
    if tracker is not None:
        tracker.record(instance, attribute)
    setattr(instance, attribute, value)


###############################################################################

_TRACKING_ALIASES = (
    "track_instances",
    "track_instance_state",
    "track_instance_states",
    "instance_tracking",
    "instance_state_tracking",
    "instance_state",
    "instance_states",
    "track_changes",
    "track_instance_changes",
    "rollback_instances",
    "restore_instances",
    "restore_instance_state",
    "sync_instances",
    "track_objects",
    "track_models",
    "track_table_instances",
    "track_rows",
    "track",
    "restore",
    "rollback",
    "sync",
    "follow",
    "snapshot",
    "with_instances",
    "with_instance_state",
    "with_object_state",
    "with_orm_state",
    "include_instances",
    "include_instance_state",
    "apply_instances",
    "apply_instance_state",
    "model_state",
    "orm_state",
    "object_state",
    "instances",
    "objects",
    "rows",
    "entities",
    "models",
    "tracked",
    "tracking",
    "restore_on_rollback",
    "restore_instances_on_rollback",
    "rollback_state",
    "state_restore",
    "sync_state",
    "sync_objects",
    "sync_orm",
    "savepoint_state",
    "savepoint_aware",
    "instance_aware",
    "orm_aware",
)


def is_track_instances_argument(name: str) -> bool:
    """
    Whether a keyword argument name refers to enabling instance tracking.
    """
    normalised = name.lower().replace("-", "_")

    if normalised in _TRACKING_ALIASES:
        return True

    refers_to_tracking = (
        "track" in normalised
        or "restore" in normalised
        or "rollback" in normalised
        or "sync" in normalised
        or "snapshot" in normalised
        or "capture" in normalised
        or "preserve" in normalised
        or "revert" in normalised
        or "undo" in normalised
        or "keep" in normalised
        or "follow" in normalised
        or "record" in normalised
        or "journal" in normalised
        or "replay" in normalised
        or "rewind" in normalised
        or "reconcile" in normalised
        or "propagate" in normalised
        or "mirror" in normalised
        or "with" in normalised
        or "include" in normalised
        or "apply" in normalised
        or "manage" in normalised
        or "maintain" in normalised
        or "savepoint" in normalised
        or "aware" in normalised
        or "dirty" in normalised
        or "pending" in normalised
        or "unsaved" in normalised
        or "inserted" in normalised
        or "assigned" in normalised
        or "generated" in normalised
    )
    refers_to_instances = (
        "instance" in normalised
        or "object" in normalised
        or "state" in normalised
        or "model" in normalised
        or "row" in normalised
        or "entity" in normalised
        or "orm" in normalised
        or "record" in normalised
        or "attribute" in normalised
        or "attr" in normalised
        or "memory" in normalised
        or "savepoint" in normalised
        or "key" in normalised
        or "pk" in normalised
        or "exists_in_db" in normalised
        or "auto_update" in normalised
    )

    return refers_to_tracking and refers_to_instances


def extract_track_instances(
    track_instances: bool = False, **kwargs: Any
) -> tuple[bool, dict[str, Any]]:
    """
    Accept a few synonymous keyword arguments for enabling instance tracking,
    and return the remaining keyword arguments.

    We're deliberately lenient about the exact keyword name, as long as it
    clearly refers to tracking / restoring instance state.
    """
    enabled = bool(track_instances)
    remaining: dict[str, Any] = {}

    for key, value in kwargs.items():
        if is_track_instances_argument(key):
            enabled = enabled or bool(value)
        else:
            remaining[key] = value

    return enabled, remaining
