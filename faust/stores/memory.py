"""In-memory table storage."""

from typing import Any, Callable, Iterable, MutableMapping, Optional, Set, Tuple, Union

from faust.types import TP, EventT
from faust.types.stores import KT, VT
from faust.types.tables import TableLookup

from . import base


class Store(base.Store, base.StoreT[KT, VT]):
    """Table storage using an in-memory dictionary."""

    def __post_init__(self) -> None:
        self.data: MutableMapping = {}
        self._key_partition: MutableMapping[KT, int] = {}
        self._key_offsets: MutableMapping[KT, int] = {}
        self._applied_offsets: MutableMapping[int, int] = {}
        self._tombstones: Set[KT] = set()
        #: Last position a key was deleted at: (partition, offset).  Kept
        #: apart from ``_key_partition`` so a deleted key is not reported as
        #: live state while a 404 can still carry the position it was
        #: deleted at.
        self._deleted_positions: MutableMapping[KT, Tuple[int, int]] = {}
        self._version = 0

    def _clear(self) -> None:
        self.data.clear()
        self._key_partition.clear()
        self._key_offsets.clear()
        self._applied_offsets.clear()
        self._tombstones.clear()
        self._deleted_positions.clear()
        self._version += 1

    @property
    def version(self) -> int:
        """Return a counter incremented every time state is applied."""
        return self._version

    def key_partition(self, key: KT) -> Optional[int]:
        """Return the changelog partition last applied for ``key``."""
        return self._key_partition.get(key)

    def deleted_positions(self, key: KT) -> Optional[Tuple[int, int]]:
        """Return the ``(partition, offset)`` a key was last deleted at."""
        return self._deleted_positions.get(key)

    def applied_offset(self, tp: TP) -> Optional[int]:
        """Return the last changelog offset applied for topic partition."""
        return self._applied_offsets.get(tp.partition)

    def is_tombstone(self, key: KT) -> bool:
        """Return :const:`True` if ``key`` is deleted in the local state."""
        return key in self._tombstones

    def lookup(self, key: KT, partition: Optional[int] = None) -> TableLookup:
        """Return a value together with the position it was read at.

        A tombstoned key has a value of :const:`None`; an applied value of
        ``0`` is a real value and is returned as such.
        """
        deleted_at = self._deleted_positions.get(key)
        key_partition = self._key_partition.get(key)
        if key_partition is None and deleted_at is not None:
            key_partition = deleted_at[0]
        if key_partition is None:
            key_partition = partition
        if key_partition is None:
            key_partition = -1
        # The offset reported for a key is the one it was last written
        # (or deleted) at, which is what a reader of that key has observed.
        # For a key that was never seen in this worker, the partition's
        # applied position is the only position that says anything.
        offset = self._key_offsets.get(key)
        if deleted_at is not None and deleted_at[0] == key_partition:
            offset = deleted_at[1]
        if offset is None:
            offset = self._applied_offsets.get(key_partition, -1)
        if key in self._tombstones:
            value: Any = None
        else:
            value = self.data.get(key)
        return TableLookup(None, key, key_partition, value, offset)

    def apply_changelog_batch(
        self,
        batch: Iterable[EventT],
        to_key: Callable[[Any], Any],
        to_value: Callable[[Any], Any],
    ) -> None:
        """Apply batch of changelog events to in-memory table."""
        # default store does not do serialization, so we need
        # to convert these raw json serialized keys to proper structures
        # (E.g. regenerate tuples in WindowedKeys etc).
        events = list(batch)
        # Entries within one changelog partition must be applied in offset
        # order, so a batch that arrives out of order produces the same
        # result as the same entries delivered separately.
        if all(self._has_offset(event) for event in events):
            events.sort(
                key=lambda event: (event.message.partition, event.message.offset)
            )
        for event in events:
            message = event.message
            tp_partition = message.partition
            offset = message.offset if self._has_offset(event) else None
            key = to_key(event.key)
            if offset is not None:
                last_key_offset = self._key_offsets.get(key)
                if last_key_offset is not None and offset <= last_key_offset:
                    # Replay (or out of order delivery) of an entry already
                    # applied for this key: applying it again would move the
                    # key back in time, e.g. resurrect a key that was deleted
                    # at a later offset.  Entries for other keys at older
                    # offsets must still be applied.
                    continue
            # to delete keys in the table we set the raw value to None
            if message.value is None:
                self._tombstones.add(key)
                self.data.pop(key, None)
                self._key_partition.pop(key, None)
                self._deleted_positions[key] = (
                    tp_partition,
                    offset if offset is not None else -1,
                )
                if offset is not None:
                    # Keep the deletion as the key's last applied offset:
                    # an older entry redelivered afterwards must not be
                    # able to resurrect the key.
                    self._key_offsets[key] = offset
            else:
                self._tombstones.discard(key)
                self._deleted_positions.pop(key, None)
                self.data[key] = to_value(event.value)
                self._key_partition[key] = tp_partition
                if offset is not None:
                    self._key_offsets[key] = offset
            if offset is not None:
                last_partition_offset = self._applied_offsets.get(tp_partition)
                if last_partition_offset is None or offset > last_partition_offset:
                    self._applied_offsets[tp_partition] = offset
        self._version += 1

    @staticmethod
    def _has_offset(event: EventT) -> bool:
        offset = event.message.offset
        return isinstance(offset, int) and not isinstance(offset, bool)

    def _create_batch_iterator(
        self,
        mark_as_delete: Callable[[Any], None],
        to_key: Callable[[Any], Any],
        to_value: Callable[[Any], Any],
        batch: Iterable[EventT],
    ) -> Iterable[Tuple[Any, Any]]:
        for event in batch:
            key = to_key(event.key)
            # to delete keys in the table we set the raw value to None
            if event.message.value is None:
                mark_as_delete(key)
            self._key_partition[key] = event.message.partition
            yield key, to_value(event.value)

    async def on_recovery_completed(
        self, active_tps: Set[TP], standby_tps: Set[TP]
    ) -> None:
        partitions = {partition for _, partition in active_tps}

        delete_keys = []
        revoked_partitions = set(self._applied_offsets).difference(partitions)

        for k, p in self._key_partition.items():
            if p not in partitions:
                delete_keys.append(k)

        for k in delete_keys:
            self.data.pop(k, None)
            self._key_partition.pop(k, None)
            self._key_offsets.pop(k, None)
            self._deleted_positions.pop(k, None)
            self._tombstones.discard(k)

        # A partition that is no longer active has to be replayed from
        # scratch when it comes back: forgetting its applied position makes
        # the next recovery apply every entry again instead of skipping
        # them as already seen.
        for partition in revoked_partitions:
            self._applied_offsets.pop(partition, None)
        if delete_keys or revoked_partitions:
            self._version += 1

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the persisted offset.

        This always returns :const:`None` when using the in-memory store.
        """
        return None

    def reset_state(self) -> None:
        """Remove local file system state.

        This does nothing when using the in-memory store.

        """
        self._clear()

    async def backup_partition(
        self, tp: Union[TP, int], flush: bool = True, purge: bool = False, keep: int = 1
    ) -> None:
        """Backup partition from this store.

        This does nothing when using the in-memory store.

        """
        ...

    def restore_backup(
        self, tp: Union[TP, int], latest: bool = True, backup_id: int = 0
    ) -> None:
        """Restore partition backup from this store.

        This does nothing when using the in-memory store.

        """
        ...
