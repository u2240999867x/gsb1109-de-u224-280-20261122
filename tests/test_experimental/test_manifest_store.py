"""Tests for the manifest-backed store and versioned replacement."""

import pytest

from zarr.abc.store import (
    ConcurrentUpdateError,
    Store,
    SupportsVersionedReplace,
    versioned_replace,
)
from zarr.core.buffer import default_buffer_prototype
from zarr.core.buffer.cpu import Buffer as CPUBuffer
from zarr.experimental import MANIFEST_KEY, ManifestStore, VersionedMemoryStore
from zarr.storage import MemoryStore


def _buf(data: bytes) -> CPUBuffer:
    return CPUBuffer.from_bytes(data)


async def _read_bytes(store: Store, key: str) -> bytes:
    buffer = await store.get(key, default_buffer_prototype())
    assert buffer is not None
    return buffer.to_bytes()


class TestVersionedMemoryStore:
    async def test_implements_protocol(self) -> None:
        store = VersionedMemoryStore()
        assert isinstance(store, SupportsVersionedReplace)
        assert ManifestStore(store).supports_versioned_replace

    async def test_versioned_replace_lifecycle(self) -> None:
        store = VersionedMemoryStore()
        assert await store.get_version("k") is None

        await store.versioned_replace("k", _buf(b"v1"), expected_version=None, new_version="ver1")
        assert await store.get_version("k") == "ver1"
        assert await _read_bytes(store, "k") == b"v1"

        # A fresh expected version allows the next swap.
        await store.versioned_replace("k", _buf(b"v2"), expected_version="ver1", new_version="ver2")
        assert await store.get_version("k") == "ver2"

        # A stale expected version is rejected and leaves the object untouched.
        with pytest.raises(ConcurrentUpdateError):
            await store.versioned_replace("k", _buf(b"v3"), expected_version="ver1", new_version="ver3")
        assert await _read_bytes(store, "k") == b"v2"
        assert await store.get_version("k") == "ver2"

        # Creating over an existing key while expecting absence is a conflict.
        with pytest.raises(ConcurrentUpdateError):
            await store.versioned_replace("k", _buf(b"v4"), expected_version=None, new_version="ver4")

    async def test_plain_set_advances_version(self) -> None:
        store = VersionedMemoryStore()
        await store.set("k", _buf(b"a"))
        version = await store.get_version("k")
        assert version is not None
        await store.set("k", _buf(b"b"))
        assert await store.get_version("k") != version

    async def test_delete_clears_version(self) -> None:
        store = VersionedMemoryStore()
        await store.set("k", _buf(b"a"))
        await store.delete("k")
        assert await store.get_version("k") is None
        # After deletion, an absence-expecting replace succeeds again.
        await store.versioned_replace("k", _buf(b"c"), expected_version=None, new_version="v")
        assert await store.get_version("k") == "v"


class TestManifestStore:
    async def test_tombstone_hides_but_retains_bytes(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        prototype = default_buffer_prototype()
        await store.set("c/0", _buf(b"a"))
        await store.set("c/1", _buf(b"b"))

        await store.tombstone(["c/1"])

        # The wrapper reports the key as missing in every read path.
        assert await store.get("c/1", prototype) is None
        assert not await store.exists("c/1")
        assert "c/1" not in [k async for k in store.list()]
        assert "c/1" not in [k async for k in store.list_prefix("c/")]
        # The manifest object itself is never listed.
        assert MANIFEST_KEY not in [k async for k in store.list()]

        # The bytes are still physically present in the backing store.
        assert await backing.exists("c/1")
        # Untombstoned keys read through unchanged.
        assert await _read_bytes(store, "c/0") == b"a"

    async def test_list_dir_filters_hidden(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("a/c/0", _buf(b"x"))
        await store.set("a/c/1", _buf(b"y"))
        await store.tombstone(["a/c/1"])
        assert [k async for k in store.list_dir("a")] == ["c"]
        assert "1" not in [k async for k in store.list_dir("a/c")]
        assert "0" in [k async for k in store.list_dir("a/c")]

    async def test_reclaim_deletes_only_live_tombstones(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"a"))
        await store.set("c/1", _buf(b"b"))
        await store.tombstone(["c/0", "c/1"])

        # Writing one tombstoned key supersedes its tombstone.
        await store.set("c/1", _buf(b"new"))
        assert await backing.exists("c/1")

        deleted = await store.reclaim()
        assert deleted == 1
        assert not await backing.exists("c/0")  # reclaimed
        assert await backing.exists("c/1")  # protected by the later write
        assert await store.reclaim() == 0  # idempotent

    async def test_set_clears_tombstone_transactionally(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"old"))
        await store.tombstone(["c/0"])
        await store.set("c/0", _buf(b"new"))
        assert await _read_bytes(store, "c/0") == b"new"
        # Nothing left to reclaim: the tombstone was removed before the write.
        assert await store.reclaim() == 0

    async def test_set_many_clears_tombstones(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"old-0"))
        await store.set("c/1", _buf(b"old-1"))
        await store.tombstone(["c/0", "c/1"])
        await store._set_many([("c/0", _buf(b"new-0")), ("c/1", _buf(b"new-1"))])
        assert await _read_bytes(store, "c/0") == b"new-0"
        assert await _read_bytes(store, "c/1") == b"new-1"
        # Both tombstones were cleared through the set() routing, so reclaim is a no-op.
        assert await store.reclaim() == 0

    async def test_delete_clears_tombstone(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"a"))
        await store.tombstone(["c/0"])
        await store.delete("c/0")
        assert await store.reclaim() == 0
        assert not await backing.exists("c/0")

    async def test_set_if_not_exists_treats_hidden_key_as_absent(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"old"))
        await store.tombstone(["c/0"])
        await store.set_if_not_exists("c/0", _buf(b"new"))
        assert await _read_bytes(store, "c/0") == b"new"

    async def test_clear_resets_manifest(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"a"))
        await store.tombstone(["c/0"])
        await store.clear()
        assert not await backing.exists(MANIFEST_KEY)
        # A fresh wrapper over the same store sees no tombstones.
        fresh = ManifestStore(backing)
        assert await fresh.get("c/0", default_buffer_prototype()) is None

    async def test_deleting_manifest_object_drops_cached_tombstones(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"a"))
        await store.tombstone(["c/0"])
        assert await store.get("c/0", default_buffer_prototype()) is None
        # Remove the manifest object directly through the wrapper.
        await store.delete(MANIFEST_KEY)
        # The cached view was reset, so the retained bytes are visible again.
        assert await _read_bytes(store, "c/0") == b"a"
        # A subsequent tombstone works from a clean manifest.
        await store.tombstone(["c/0"])
        assert await store.get("c/0", default_buffer_prototype()) is None

    async def test_delete_dir_drops_matching_tombstones(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("a/c/0", _buf(b"a"))
        await store.set("b/c/0", _buf(b"b"))
        await store.tombstone(["a/c/0", "b/c/0"])
        await store.delete_dir("a")
        manifest = await store._load_manifest(force=True)
        assert "a/c/0" not in manifest.tombstones
        assert "b/c/0" in manifest.tombstones

    async def test_snapshot_is_pinned(self) -> None:
        backing = VersionedMemoryStore()
        writer = ManifestStore(backing)
        await writer.set("c/0", _buf(b"old"))
        # Snapshot before the tombstone: pinned to the empty-manifest view.
        snapshot = await ManifestStore.snapshot(backing)
        assert snapshot.pinned

        await writer.tombstone(["c/0"])
        # The snapshot still reads the bytes the writer now hides.
        assert await _read_bytes(snapshot, "c/0") == b"old"
        assert await snapshot.exists("c/0")
        # A fresh unpinned wrapper sees the tombstone.
        assert await ManifestStore(backing).get("c/0", default_buffer_prototype()) is None

    async def test_snapshot_of_manifest_store_shares_backing(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        snapshot = await ManifestStore.snapshot(store, read_only=False)
        assert snapshot._store is backing
        assert snapshot.pinned
        ro_snapshot = await ManifestStore.snapshot(store)
        assert ro_snapshot.read_only

    async def test_cas_retry_merges_concurrent_commits(self) -> None:
        backing = VersionedMemoryStore()
        first = ManifestStore(backing)
        second = ManifestStore(backing)
        await first.tombstone(["x"])  # second's cache is still the empty manifest
        await second.tombstone(["y"])  # must conflict once, reload, and merge
        manifest = await first._load_manifest(force=True)
        assert set(manifest.tombstones) == {"x", "y"}

    async def test_reclaim_skips_key_another_writer_superseded(self) -> None:
        # Two independent wrappers over the same backing store. A stale reclaimer
        # must lose its compare-and-swap, reload, and never delete the frame the
        # other writer un-tombstoned after the reclaimer loaded its manifest.
        backing = VersionedMemoryStore()
        writer = ManifestStore(backing)
        for coord in range(5):
            await writer.set(f"c/{coord}", _buf(bytes([coord])))
        await writer.tombstone([f"c/{coord}" for coord in range(5)])

        reclaimer = ManifestStore(backing)
        await reclaimer._load_manifest()  # cache the five-tombstone manifest

        # The writer grows back and overwrites frame c/2, clearing its tombstone.
        await writer.set("c/2", _buf(b"new"))

        # The reclaimer's cached manifest still lists c/2; the CAS must reject it.
        deleted = await reclaimer.reclaim()
        assert deleted == 4
        assert await backing.exists("c/2")  # the freshly written frame survives
        for coord in (0, 1, 3, 4):
            assert not await backing.exists(f"c/{coord}")
        # And a fresh reader sees the new frame rather than a reclaimed hole.
        fresh = ManifestStore(backing)
        assert await _read_bytes(fresh, "c/2") == b"new"
        assert await fresh.reclaim() == 0

    async def test_plain_store_fallback(self) -> None:
        backing = MemoryStore()
        store = ManifestStore(backing)
        assert not store.supports_versioned_replace
        assert not isinstance(backing, SupportsVersionedReplace)
        await store.set("c/0", _buf(b"a"))
        await store.tombstone(["c/0"])
        assert await store.get("c/0", default_buffer_prototype()) is None
        assert await backing.exists("c/0")
        assert await store.reclaim() == 1
        assert not await backing.exists("c/0")

    async def test_versioned_replace_helper_falls_back_to_set(self) -> None:
        plain = MemoryStore()
        await versioned_replace(
            plain, "k", _buf(b"v"), expected_version=None, new_version="ignored"
        )
        assert await _read_bytes(plain, "k") == b"v"

    async def test_read_only_wrapper_shares_state(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"a"))
        reader = store.with_read_only(True)
        assert reader._state is store._state
        assert reader.read_only
        with pytest.raises(ValueError, match="read-only"):
            await reader.set("c/1", _buf(b"b"))

    async def test_with_read_only_roundtrip_keeps_tombstones(self) -> None:
        backing = VersionedMemoryStore()
        store = ManifestStore(backing)
        await store.set("c/0", _buf(b"a"))
        await store.tombstone(["c/0"])
        reader = store.with_read_only(True)
        assert await reader.get("c/0", default_buffer_prototype()) is None
