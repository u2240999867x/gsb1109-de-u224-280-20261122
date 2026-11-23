"""Integration tests for tombstone-mode resize through a ManifestStore."""

from collections.abc import Generator
from typing import Any, cast

import numpy as np
import pytest

from zarr.abc.store import Store
from zarr.core.array import AsyncArray, ResizeMode, create_array
from zarr.core.buffer import default_buffer_prototype
from zarr.core.buffer.cpu import Buffer as CPUBuffer
from zarr.core.common import ZarrFormat
from zarr.core.config import config
from zarr.experimental import ManifestStore, VersionedMemoryStore
from zarr.storage import MemoryStore


@pytest.fixture(autouse=True)
def _enable_rectilinear_chunks() -> Generator[None, None, None]:
    with config.set({"array.rectilinear_chunks": True}):
        yield


async def _open(store: Store) -> AsyncArray[Any]:
    # ManifestStore(ManifestStore) shares manifest state, so pinning survives.
    return await AsyncArray.open(store=ManifestStore(store))


def _manifest(array: AsyncArray[Any]) -> ManifestStore:
    return cast(ManifestStore, array.store)


async def test_tombstone_resize_scenario() -> None:
    # The summer-raster scenario: 17 frames, five appended by mistake.
    backing = VersionedMemoryStore()
    store = ManifestStore(backing)
    array = await create_array(
        store=store, shape=(17,), chunks=(1,), dtype="int32", fill_value=0
    )
    data = (np.arange(17, dtype="int32") + 1) * 10
    await array.setitem(..., data)

    # An archive reader opened against the OLD shape, pinned before the shrink.
    archive_store = await ManifestStore.snapshot(backing)
    archive_reader = await _open(archive_store)
    assert archive_reader.shape == (17,)

    # Shrink to 12 frames via tombstones; bytes for frames 12..16 are retained.
    await array.resize((12,), mode=ResizeMode.tombstone)
    assert array.shape == (12,)
    for coord in range(12, 17):
        assert await backing.exists(f"c/{coord}")

    # The old-shape archive reader still reads every original frame.
    np.testing.assert_array_equal(await archive_reader.getitem(...), data)

    # A reopened reader follows the new shape and sees only the retained frames.
    reader = await _open(backing)
    assert reader.shape == (12,)
    np.testing.assert_array_equal(await reader.getitem(...), data[:12])

    # Grow back to 17: hidden frames must not resurrect, they read as fill.
    await reader.resize((17,), mode=ResizeMode.tombstone)
    assert reader.shape == (17,)
    grown = np.asarray(await reader.getitem(...))
    np.testing.assert_array_equal(grown[:12], data[:12])
    np.testing.assert_array_equal(grown[12:], np.zeros(5, dtype="int32"))

    # Write the new 13th frame (index 12).
    await reader.setitem(12, np.int32(130))
    assert await backing.exists("c/12")

    # Reclaim the shrink leftovers after the grow + write completed.
    reclaimed = await _manifest(reader).reclaim()
    assert reclaimed == 4
    # The freshly written frame survives reclamation; the other four are gone.
    assert await backing.exists("c/12")
    for coord in (13, 14, 15, 16):
        assert not await backing.exists(f"c/{coord}")

    # A reopened reader gets 12 old frames, one new frame, four fill frames.
    final_reader = await _open(backing)
    final = np.asarray(await final_reader.getitem(...))
    expected = np.concatenate(
        [data[:12], np.array([130], dtype="int32"), np.zeros(4, dtype="int32")]
    )
    np.testing.assert_array_equal(final, expected)


async def test_tombstone_does_not_physically_delete_bytes() -> None:
    backing = VersionedMemoryStore()
    array = await create_array(
        store=ManifestStore(backing), shape=(8,), chunks=(1,), dtype="int32"
    )
    await array.setitem(..., np.arange(8, dtype="int32"))
    await array.resize((5,), mode=ResizeMode.tombstone)
    for coord in range(5, 8):
        assert await backing.exists(f"c/{coord}")
    # The manifest wrapper itself hides them.
    wrapper = ManifestStore(backing)
    for coord in range(5, 8):
        assert not await wrapper.exists(f"c/{coord}")


async def test_tombstone_then_delete_mode_equivalence_on_read() -> None:
    # Tombstoned chunks read identically to deleted chunks through the wrapper.
    tomb_backing = VersionedMemoryStore()
    tomb_array = await create_array(
        store=ManifestStore(tomb_backing), shape=(6,), chunks=(1,), dtype="int32", fill_value=7
    )
    await tomb_array.setitem(..., np.arange(6, dtype="int32"))
    await tomb_array.resize((3,), mode=ResizeMode.tombstone)
    await tomb_array.resize((6,), mode=ResizeMode.tombstone)

    del_store = MemoryStore()
    del_array = await create_array(
        store=del_store, shape=(6,), chunks=(1,), dtype="int32", fill_value=7
    )
    await del_array.setitem(..., np.arange(6, dtype="int32"))
    await del_array.resize((3,))
    await del_array.resize((6,))

    np.testing.assert_array_equal(
        await tomb_array.getitem(...), await del_array.getitem(...)
    )


async def test_tombstone_requires_manifest_store() -> None:
    array = await create_array(
        store=MemoryStore(), shape=(4,), chunks=(1,), dtype="int32"
    )
    with pytest.raises(TypeError, match="ManifestStore"):
        await array.resize((2,), mode=ResizeMode.tombstone)
    # A bad mode does not mutate the array.
    assert array.shape == (4,)


async def test_legacy_boolean_maps_to_modes() -> None:
    backing = VersionedMemoryStore()
    array = await create_array(
        store=ManifestStore(backing), shape=(6,), chunks=(1,), dtype="int32"
    )
    await array.setitem(..., np.arange(6, dtype="int32"))
    # delete_outside_chunks=False behaves like preserve: bytes stay and resurrect.
    await array.resize((3,), delete_outside_chunks=False)
    await array.resize((6,))
    np.testing.assert_array_equal(
        await array.getitem(...), np.arange(6, dtype="int32")
    )


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_tombstone_resize_basic_formats(zarr_format: ZarrFormat) -> None:
    backing = VersionedMemoryStore()
    array = await create_array(
        store=ManifestStore(backing),
        shape=(6,),
        chunks=(1,),
        dtype="int32",
        fill_value=0,
        zarr_format=zarr_format,
    )
    await array.setitem(..., np.arange(6, dtype="int32") + 1)
    await array.resize((3,), mode=ResizeMode.tombstone)
    await array.resize((6,), mode=ResizeMode.tombstone)
    out = np.asarray(await array.getitem(...))
    np.testing.assert_array_equal(out[:3], np.arange(3, dtype="int32") + 1)
    np.testing.assert_array_equal(out[3:], np.zeros(3, dtype="int32"))


@pytest.mark.parametrize("zarr_format", [3])
async def test_tombstone_resize_sharded(zarr_format: ZarrFormat) -> None:
    backing = VersionedMemoryStore()
    array = await create_array(
        store=ManifestStore(backing),
        shape=(8, 4),
        chunks=(1, 2),
        shards=(2, 4),
        dtype="int32",
        zarr_format=zarr_format,
    )
    data = np.arange(32, dtype="int32").reshape(8, 4)
    await array.setitem(..., data)
    await array.resize((4, 4), mode=ResizeMode.tombstone)
    assert array.shape == (4, 4)
    np.testing.assert_array_equal(await array.getitem(...), data[:4])

    # Grow back and confirm no resurrection of the hidden shard bytes.
    await array.resize((8, 4), mode=ResizeMode.tombstone)
    grown = np.asarray(await array.getitem(...))
    np.testing.assert_array_equal(grown[:4], data[:4])
    assert np.all(grown[4:] == 0)


async def test_reclaim_after_later_write_keeps_new_chunk() -> None:
    backing = VersionedMemoryStore()
    array = await create_array(
        store=ManifestStore(backing), shape=(6,), chunks=(1,), dtype="int32", fill_value=0
    )
    await array.setitem(..., np.arange(1, 7, dtype="int32"))
    await array.resize((3,), mode=ResizeMode.tombstone)  # tombstones c/3..c/5
    await array.resize((6,), mode=ResizeMode.tombstone)
    # Overwrite one of the hidden coordinates; its tombstone must be cleared.
    await array.setitem(4, np.int32(999))
    reclaimed = await _manifest(array).reclaim()
    # Only c/3 and c/5 remain tombstoned; c/4 was superseded by the new write.
    assert reclaimed == 2
    assert await backing.exists("c/4")
    result = np.asarray(await (await _open(backing)).getitem(...))
    assert result[4] == 999


def test_sync_tombstone_resize_and_reclaim() -> None:
    import zarr

    backing = VersionedMemoryStore()
    array = zarr.create_array(
        store=ManifestStore(backing), shape=(10,), chunks=(1,), dtype="int32", fill_value=0
    )
    array[:] = np.arange(10, dtype="int32")
    array.resize((6,), mode=ResizeMode.tombstone)
    array.resize((10,), mode=ResizeMode.tombstone)
    out = np.asarray(array[:])
    np.testing.assert_array_equal(out[:6], np.arange(6, dtype="int32"))
    np.testing.assert_array_equal(out[6:], np.zeros(4, dtype="int32"))
    assert _manifest(array.async_array).reclaim_sync() == 4


async def test_tombstone_resize_under_nested_group_path() -> None:
    import zarr

    backing = VersionedMemoryStore()
    root = await zarr.api.asynchronous.create_group(store=ManifestStore(backing))
    array = await root.create_array(
        "grp/raster", shape=(8,), chunks=(1,), dtype="int32", fill_value=0
    )
    await array.setitem(..., np.arange(8, dtype="int32"))
    await array.resize((5,), mode=ResizeMode.tombstone)
    # The tombstoned key carries the full nested path and stays physically present.
    assert await backing.exists("grp/raster/c/7")
    reopened = await zarr.api.asynchronous.open_array(store=ManifestStore(backing), path="grp/raster")
    assert reopened.shape == (5,)
    np.testing.assert_array_equal(await reopened.getitem(...), np.arange(5, dtype="int32"))
    await reopened.resize((8,), mode=ResizeMode.tombstone)
    np.testing.assert_array_equal(
        await reopened.getitem(...),
        np.concatenate([np.arange(5, dtype="int32"), np.zeros(3, dtype="int32")]),
    )
    assert await _manifest(reopened).reclaim() == 3


async def test_get_partial_values_masks_hidden_keys() -> None:
    backing = VersionedMemoryStore()
    store = ManifestStore(backing)
    await store.set("c/0", CPUBuffer.from_bytes(b"visible"))
    await store.set("c/1", CPUBuffer.from_bytes(b"hidden"))
    await store.tombstone(["c/1"])
    results = await store.get_partial_values(
        default_buffer_prototype(), [("c/0", None), ("c/1", None)]
    )
    assert results[0] is not None
    assert results[0].to_bytes() == b"visible"
    assert results[1] is None
