"""
Manifest-backed storage and resize support.

A :class:`ManifestStore` wraps an ordinary :class:`~zarr.abc.store.Store` and
keeps a *manifest* object in storage that records which keys have been
tombstoned. Reads consult the manifest, so a tombstoned key is reported as
absent even though its bytes are still physically present. This lets a
shrinking resize hide the chunks outside the new shape without deleting them,
which means a later resize that grows the array back over the same chunk
coordinates cannot read the old bytes back as if they were current data.

The manifest itself is replaced through the store's versioned-replace
capability (:class:`~zarr.abc.store.SupportsVersionedReplace`) when one is
available, so committing a manifest is an atomic compare-and-swap: a lost
update raises :class:`~zarr.abc.store.ConcurrentUpdateError` and is retried
instead of silently overwriting a newer manifest. On stores without that
capability the manifest still works for single-writer use, falling back to
ordinary writes, and plain stores keep their existing resize semantics.

Physical reclamation of the bytes left behind by a shrinking resize
(:meth:`ManifestStore.reclaim`) is deliberately separate from the resize. It
only ever deletes keys the live manifest still marks as tombstoned, so chunks
written after the shrink -- for example the first frame written after growing
the array again -- are never reclaimed, even when reclamation runs later.

A :meth:`snapshot` is a read-only, pinned view: it loads the manifest once and
never observes tombstones committed afterwards. That is what lets an archive
reader opened with the old shape, before a shrink, keep reading the old bytes
(the bytes are retained until reclamation).
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

from zarr.abc.store import (
    ConcurrentUpdateError,
    Store,
    SupportsVersionedReplace,
    versioned_replace,
)
from zarr.core._json import buffer_to_json_object, json_to_buffer
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import sync
from zarr.storage._memory import MemoryStore
from zarr.storage._wrapper import WrapperStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterable, Sequence

    from zarr.abc.store import ByteRequest
    from zarr.core.buffer import Buffer, BufferPrototype

__all__ = ["MANIFEST_KEY", "ManifestStore", "VersionedMemoryStore"]

#: Reserved key of the manifest object within the wrapped store.
MANIFEST_KEY: str = ".zarr-manifest.json"

_MANIFEST_DOCUMENT_VERSION = 1


@dataclass(slots=True)
class _Manifest:
    """The in-memory view of the manifest object.

    ``tombstones`` maps a hidden store key to the resize generation that
    tombstoned it. The generation only identifies which shrinking resize a
    tombstone belongs to; reads treat every tombstone identically. ``version``
    is the opaque object version the backing store last reported, or None when
    the backing store has no versioned-replace capability.
    """

    version: object | None
    generation: int
    tombstones: dict[str, int] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "zarr_manifest_version": _MANIFEST_DOCUMENT_VERSION,
            "generation": self.generation,
            "tombstones": dict(sorted(self.tombstones.items())),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any], version: object | None) -> Self:
        return cls(
            version=version,
            generation=int(payload["generation"]),
            tombstones={str(key): int(gen) for key, gen in payload["tombstones"].items()},
        )


@dataclass(slots=True)
class _ManifestState:
    """Shared, mutable manifest bookkeeping.

    Read-only copies produced by :meth:`ManifestStore.with_read_only` and
    :meth:`ManifestStore.snapshot` share one state. A pinned state loads the
    manifest once and freezes its tombstone view forever; an unpinned state
    caches the manifest until a compare-and-swap conflict forces a fresh read
    (see :meth:`ManifestStore._commit_unlocked`), so writers stay consistent
    without paying a manifest read on every key access.
    """

    lock: asyncio.Lock
    pinned: bool = False
    loaded: bool = False
    manifest: _Manifest | None = None


class ManifestStore(WrapperStore[Store]):
    """
    Store wrapper that tombstones keys through a versioned manifest object.

    The wrapped store holds the actual array bytes unchanged. This wrapper adds
    one reserved manifest object (:data:`MANIFEST_KEY`) listing tombstoned keys.
    Tombstoned keys read as missing (``get`` returns ``None``, ``exists``
    returns ``False`` and they are filtered from listings) while their bytes
    remain in the wrapped store until :meth:`reclaim` deletes them.

    Parameters
    ----------
    store : Store
        The store holding the data objects. When it implements
        :class:`~zarr.abc.store.SupportsVersionedReplace`, manifest commits are
        atomic compare-and-swap operations; otherwise they fall back to
        ordinary writes.
    pinned : bool, optional
        If True, freeze the manifest view at first load. Use
        :meth:`snapshot` rather than constructing a pinned store directly.

    Notes
    -----
    The manifest is an implementation detail of this wrapper. Accessing the
    wrapped store directly bypasses the tombstones, so all writers and readers
    of a dataset should go through the wrapper.
    """

    _state: _ManifestState

    def __init__(self, store: Store, *, pinned: bool = False) -> None:
        super().__init__(store)
        if isinstance(store, ManifestStore):
            # Composing manifest wrappers must not create a second, divergent
            # view of the same manifest object. Share the inner wrapper's state
            # (including its pinned flag and lock) so the outer wrapper observes
            # exactly the same tombstones, e.g. when wrapping a snapshot.
            self._state = store._state
        else:
            self._state = _ManifestState(lock=asyncio.Lock(), pinned=pinned)

    def _with_store(self, store: Store) -> Self:
        new = type(self)(store)
        # Share the manifest cache and lock with the wrapper we derive from.
        new._state = self._state
        return new

    def with_read_only(self, read_only: bool = False) -> Self:
        return self._with_store(self._store.with_read_only(read_only))

    @classmethod
    async def snapshot(cls, store: Store, *, read_only: bool = True) -> Self:
        """
        Return a pinned, point-in-time view of ``store``.

        The returned wrapper loads the manifest eagerly and never observes
        later tombstones, so an archive reader opened before a shrinking resize
        keeps reading the bytes the resize hid. The underlying bytes are only
        removed by an explicit :meth:`reclaim`, which snapshot readers are
        expected to outlive.

        Parameters
        ----------
        store : Store
            The store to snapshot. May already be a :class:`ManifestStore`.
        read_only : bool, optional
            Open the snapshot read-only (default). The snapshot never writes.
        """
        backing = store
        if isinstance(store, ManifestStore):
            backing = store._store
        if read_only:
            backing = backing.with_read_only(True)
        snapshot_store = cls(backing, pinned=True)
        await snapshot_store._load_manifest()
        return snapshot_store

    @property
    def _supports_sync_io(self) -> bool:
        # Tombstone filtering lives in the async get/set/delete overrides; the
        # sync methods inherited from WrapperStore delegate straight to the
        # wrapped store and would bypass it. Opt out, mirroring CacheStore, so
        # consumers take the async path (the sync API drives it via threads).
        return False

    @property
    def supports_versioned_replace(self) -> bool:
        """Whether manifest commits use atomic compare-and-swap semantics."""
        return isinstance(self._store, SupportsVersionedReplace)

    @property
    def pinned(self) -> bool:
        """Whether this wrapper froze its manifest view at first load."""
        return self._state.pinned

    # ------------------------------------------------------------------
    # Manifest loading and versioned commits
    # ------------------------------------------------------------------

    async def _current_version(self) -> object | None:
        if isinstance(self._store, SupportsVersionedReplace):
            return await self._store.get_version(MANIFEST_KEY)
        return None

    async def _load_manifest(self, *, force: bool = False) -> _Manifest:
        state = self._state
        if (
            state.loaded
            and (state.pinned or not force)
            and state.manifest is not None
        ):
            return state.manifest

        buffer = await self._store.get(MANIFEST_KEY, default_buffer_prototype())
        if buffer is None:
            manifest = _Manifest(version=None, generation=0)
        else:
            manifest = _Manifest.from_payload(
                buffer_to_json_object(buffer), await self._current_version()
            )
        state.manifest = manifest
        state.loaded = True
        return manifest

    async def _commit_unlocked(self, mutate: Callable[[_Manifest], bool]) -> _Manifest:
        """Commit ``mutate`` under the caller-held lock, retrying lost updates.

        ``mutate`` edits its argument in place and returns whether anything
        changed. On a :class:`ConcurrentUpdateError` the manifest is reloaded
        from storage (bypassing the cache) and the mutation re-applied until it
        commits.
        """
        state = self._state
        force = False
        while True:
            manifest = await self._load_manifest(force=force)
            working = _Manifest(
                version=manifest.version,
                generation=manifest.generation,
                tombstones=dict(manifest.tombstones),
            )
            if not mutate(working):
                state.manifest = working
                return working
            new_version: object = uuid.uuid4().hex
            payload = json_to_buffer(working.to_payload(), prototype=default_buffer_prototype())
            try:
                await versioned_replace(
                    self._store,
                    MANIFEST_KEY,
                    payload,
                    expected_version=manifest.version,
                    new_version=new_version,
                )
            except ConcurrentUpdateError:
                # Another writer committed first; drop our edit, re-read the
                # current manifest and retry.
                force = True
                continue
            working.version = (
                new_version if isinstance(self._store, SupportsVersionedReplace) else None
            )
            state.manifest = working
            return working

    async def _commit(self, mutate: Callable[[_Manifest], bool]) -> _Manifest:
        async with self._state.lock:
            return await self._commit_unlocked(mutate)

    # ------------------------------------------------------------------
    # Manifest-driven operations used by resize and reclamation
    # ------------------------------------------------------------------

    async def tombstone(self, keys: Iterable[str]) -> None:
        """Hide ``keys`` through the manifest without touching their bytes.

        All keys are recorded under one new resize generation, in a single
        versioned commit. Bytes already present in the wrapped store are left
        in place for later :meth:`reclaim`.
        """
        key_list = [key for key in keys if key != MANIFEST_KEY]
        if not key_list:
            return

        def _apply(manifest: _Manifest) -> bool:
            generation = manifest.generation + 1
            changed = False
            for key in key_list:
                if manifest.tombstones.get(key) != generation:
                    manifest.tombstones[key] = generation
                    changed = True
            if changed:
                manifest.generation = generation
            return changed

        await self._commit(_apply)

    async def reclaim(self) -> int:
        """Delete the wrapped-store bytes of keys still marked as tombstones.

        Only keys the live manifest still hides are deleted, so a key that was
        tombstoned by a shrinking resize and then written again after the array
        grew back is never touched. Returns the number of objects deleted.

        The tombstone removal commits under a versioned replace before any
        object is deleted, and the whole operation holds the per-instance
        manifest lock. The deleted set is recomputed on every compare-and-swap
        retry, so a key another writer un-tombstoned while a reclaim was in
        flight (for example the first frame written after regrowth) is never
        deleted. Reclaim is idempotent: a second call with no live tombstones
        returns 0.
        """
        state = self._state
        async with state.lock:
            force = False
            while True:
                manifest = await self._load_manifest(force=force)
                if not manifest.tombstones:
                    return 0
                # `keys` are exactly the tombstones in the manifest about to be
                # committed. On a lost update we reload and recompute, so keys a
                # concurrent writer already superseded never reach this set.
                keys = tuple(manifest.tombstones)
                new_manifest = _Manifest(
                    version=manifest.version,
                    generation=manifest.generation,
                    tombstones={},
                )
                new_version: object = uuid.uuid4().hex
                payload = json_to_buffer(
                    new_manifest.to_payload(), prototype=default_buffer_prototype()
                )
                try:
                    await versioned_replace(
                        self._store,
                        MANIFEST_KEY,
                        payload,
                        expected_version=manifest.version,
                        new_version=new_version,
                    )
                except ConcurrentUpdateError:
                    # Another writer committed first; re-read and skip anything
                    # no longer tombstoned (e.g. a frame written after regrowth).
                    force = True
                    continue
                new_manifest.version = (
                    new_version
                    if isinstance(self._store, SupportsVersionedReplace)
                    else None
                )
                state.manifest = new_manifest
                state.loaded = True
                for key in keys:
                    await self._store.delete(key)
                return len(keys)

    async def _is_hidden(self, key: str) -> bool:
        if key == MANIFEST_KEY:
            return False
        return key in (await self._load_manifest()).tombstones

    # ------------------------------------------------------------------
    # Synchronous conveniences
    # ------------------------------------------------------------------

    def tombstone_sync(self, keys: Iterable[str]) -> None:
        """Synchronous counterpart of :meth:`tombstone`."""
        sync(self.tombstone(keys))

    def reclaim_sync(self) -> int:
        """Synchronous counterpart of :meth:`reclaim`."""
        return sync(self.reclaim())

    @classmethod
    def snapshot_sync(cls, store: Store, *, read_only: bool = True) -> Self:
        """Synchronous counterpart of :meth:`snapshot`."""
        return sync(cls.snapshot(store, read_only=read_only))

    # ------------------------------------------------------------------
    # Store overrides
    # ------------------------------------------------------------------

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        if await self._is_hidden(key):
            return None
        return await self._store.get(key, prototype, byte_range)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        async def _get(key: str, byte_range: ByteRequest | None) -> Buffer | None:
            return await self.get(key, prototype=prototype, byte_range=byte_range)

        return [await _get(key, byte_range) for key, byte_range in key_ranges]

    async def _get_many(
        self, requests: Iterable[tuple[str, BufferPrototype, ByteRequest | None]]
    ) -> AsyncGenerator[tuple[str, Buffer | None], None]:
        for key, prototype, byte_range in requests:
            yield (key, await self.get(key, prototype, byte_range))

    async def get_ranges(
        self,
        key: str,
        byte_ranges: Sequence[ByteRequest | None],
        *,
        prototype: BufferPrototype,
        max_concurrency: int | None = None,
        max_gap_bytes: int | None = None,
        max_coalesced_bytes: int | None = None,
    ) -> AsyncIterator[Sequence[tuple[int, Buffer | None]]]:
        # Build on Store.get_ranges, which dispatches through self.get (and
        # therefore the tombstone check), rather than WrapperStore's delegation
        # to the wrapped store.
        kwargs: dict[str, int] = {}
        if max_concurrency is not None:
            kwargs["max_concurrency"] = max_concurrency
        if max_gap_bytes is not None:
            kwargs["max_gap_bytes"] = max_gap_bytes
        if max_coalesced_bytes is not None:
            kwargs["max_coalesced_bytes"] = max_coalesced_bytes
        async for group in Store.get_ranges(self, key, byte_ranges, prototype=prototype, **kwargs):
            yield group

    async def exists(self, key: str) -> bool:
        if await self._is_hidden(key):
            return False
        return await self._store.exists(key)

    async def set(self, key: str, value: Buffer) -> None:
        self._check_writable()
        await self._ensure_open()
        if key == MANIFEST_KEY:
            await self._store.set(key, value)
            return
        if not await self._is_hidden(key):
            await self._store.set(key, value)
            return
        # Superseding a tombstoned key is a lock-held transaction so reclamation
        # cannot delete the freshly written bytes while they are being written.
        async with self._state.lock:
            manifest = await self._load_manifest()
            if key not in manifest.tombstones:
                # Another writer superseded it first.
                await self._store.set(key, value)
                return

            def _apply(working: _Manifest) -> bool:
                return working.tombstones.pop(key, None) is not None

            await self._store.set(key, value)
            await self._commit_unlocked(_apply)

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        # docstring inherited
        self._check_writable()
        await self._ensure_open()
        if key == MANIFEST_KEY or not await self._is_hidden(key):
            await self._store.set_if_not_exists(key, value)
            return
        # A tombstoned key reads as absent in the manifest view even though its
        # bytes remain physically, so the "not exists" branch writes and clears
        # the tombstone via set().
        await self.set(key, value)

    async def _set_many(self, values: Iterable[tuple[str, Buffer]]) -> None:
        # Route every write through set() so superseding a tombstoned key clears
        # its tombstone under the lock, rather than bypassing the manifest.
        for key, value in values:
            await self.set(key, value)

    async def delete(self, key: str) -> None:
        self._check_writable()
        await self._store.delete(key)
        if key == MANIFEST_KEY:
            # The source of truth is gone; drop the cached view so the next read
            # observes an empty manifest instead of stale tombstones.
            self._state.manifest = None
            self._state.loaded = False
            return
        if await self._is_hidden(key):

            def _apply(manifest: _Manifest) -> bool:
                return manifest.tombstones.pop(key, None) is not None

            await self._commit(_apply)

    async def clear(self) -> None:
        await self._store.clear()
        self._state.manifest = _Manifest(version=None, generation=0)
        self._state.loaded = True

    async def delete_dir(self, prefix: str) -> None:
        if prefix != "" and not prefix.endswith("/"):
            prefix += "/"

        def _apply(manifest: _Manifest) -> bool:
            changed = False
            for key in [k for k in manifest.tombstones if k.startswith(prefix)]:
                del manifest.tombstones[key]
                changed = True
            return changed

        await self._store.delete_dir(prefix)
        await self._commit(_apply)

    async def list(self) -> AsyncIterator[str]:
        hidden = (await self._load_manifest()).tombstones
        async for key in self._store.list():
            if key != MANIFEST_KEY and key not in hidden:
                yield key

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        hidden = (await self._load_manifest()).tombstones
        async for key in self._store.list_prefix(prefix):
            if key != MANIFEST_KEY and key not in hidden:
                yield key

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        hidden = (await self._load_manifest()).tombstones
        async for key in self._store.list_dir(prefix):
            full_key = f"{prefix.rstrip('/')}/{key}" if prefix else key
            if key != MANIFEST_KEY and full_key not in hidden:
                yield key

    def __repr__(self) -> str:
        return f"ManifestStore({self._store!r})"


class VersionedMemoryStore(WrapperStore[MemoryStore]):
    """
    In-memory store that supports atomic, version-checked object replacement.

    Every committed object is paired with an opaque version token. Reads through
    the usual store API behave exactly like the wrapped
    :class:`~zarr.storage.MemoryStore`; :meth:`versioned_replace` additionally
    performs the compare-and-swap that
    :class:`~zarr.abc.store.SupportsVersionedReplace` promises. This is the
    reference backing store for :class:`ManifestStore` and a template for remote
    stores whose object API already offers conditional writes (e.g. S3
    ``If-Match``).

    Parameters
    ----------
    store : MemoryStore, optional
        The memory store holding the objects. A fresh empty one is created when
        omitted.
    """

    def __init__(self, store: MemoryStore | None = None) -> None:
        super().__init__(store if store is not None else MemoryStore())
        self._versions: dict[str, object] = {}

    def _with_store(self, store: MemoryStore) -> Self:
        new = type(self)(store)
        # Versions describe the same backing dict; derived stores share them.
        new._versions = self._versions
        return new

    async def get_version(self, key: str) -> object | None:
        """Return the current version of the object at ``key``, or None if absent."""
        return self._versions.get(key)

    async def versioned_replace(
        self,
        key: str,
        value: Buffer,
        *,
        expected_version: object | None,
        new_version: object,
    ) -> None:
        """Replace the object at ``key`` only when its version still matches.

        Raises
        ------
        ConcurrentUpdateError
            If the object's current version differs from
            ``expected_version``, including when one of the two expects the key
            to be absent while it is present.
        """
        self._check_writable()
        current = self._versions.get(key)
        if current != expected_version:
            raise ConcurrentUpdateError(
                f"Version conflict replacing {key!r}: expected {expected_version!r}, "
                f"found {current!r}."
            )
        await self._store.set(key, value)
        self._versions[key] = new_version

    async def set(self, key: str, value: Buffer) -> None:
        await super().set(key, value)
        # Plain writes are unconditional and advance the version, invalidating
        # any expected version a concurrent reader is still holding.
        self._versions[key] = uuid.uuid4().hex

    async def delete(self, key: str) -> None:
        await super().delete(key)
        self._versions.pop(key, None)

    async def delete_dir(self, prefix: str) -> None:
        if prefix != "" and not prefix.endswith("/"):
            prefix += "/"
        await super().delete_dir(prefix)
        for key in [k for k in self._versions if k.startswith(prefix)]:
            del self._versions[key]

    async def clear(self) -> None:
        await super().clear()
        self._versions.clear()

    def set_sync(self, key: str, value: Buffer) -> None:
        super().set_sync(key, value)
        self._versions[key] = uuid.uuid4().hex

    def delete_sync(self, key: str) -> None:
        super().delete_sync(key)
        self._versions.pop(key, None)

