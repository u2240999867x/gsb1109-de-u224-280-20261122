import asyncio
import collections
import concurrent.futures
import io
import os
import tarfile
import threading
import time
from contextlib import nullcontext
from itertools import cycle

import pytest

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.local import LocalFileSystem

from .test_local import csv_files, filetexts


class LockedFileSystem(AsyncFileSystem):
    """
    A mock file system that simulates a synchronous locking file systems with delays.
    """

    def __init__(
        self,
        asynchronous: bool = False,
        delays=None,
    ) -> None:
        self.lock = asyncio.Lock()
        self.delays = cycle((0.03, 0.01) if delays is None else delays)

        super().__init__(asynchronous=asynchronous)

    async def _cat_file(self, path, start=None, end=None) -> bytes:
        await self._simulate_io_operation(path)
        return path.encode()

    async def _await_io(self) -> None:
        await asyncio.sleep(next(self.delays))

    async def _simulate_io_operation(self, path) -> None:
        await self._check_active()
        async with self.lock:
            await self._await_io()

    async def _check_active(self) -> None:
        if self.lock.locked():
            raise RuntimeError("Concurrent requests!")


@pytest.mark.asyncio
async def test_is_async_default():
    fs = fsspec.filesystem("file")
    async_fs = AsyncFileSystemWrapper(fs)
    assert async_fs.async_impl
    assert async_fs.asynchronous
    async_fs = AsyncFileSystemWrapper(fs, asynchronous=False)
    assert not async_fs.asynchronous


def test_class_wrapper():
    fs_cls = LocalFileSystem
    async_fs_cls = AsyncFileSystemWrapper.wrap_class(fs_cls)
    assert async_fs_cls.__name__ == "AsyncLocalFileSystemWrapper"
    async_fs = async_fs_cls()
    assert async_fs.async_impl


@pytest.mark.asyncio
async def test_cats():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        result = await async_fs._cat(".test.fakedata.1.csv")
        assert result == b"a,b\n1,2\n"

        out = set(
            (
                await async_fs._cat([".test.fakedata.1.csv", ".test.fakedata.2.csv"])
            ).values()
        )
        assert out == {b"a,b\n1,2\n", b"a,b\n3,4\n"}

        result = await async_fs._cat(".test.fakedata.1.csv", None, None)
        assert result == b"a,b\n1,2\n"

        result = await async_fs._cat(".test.fakedata.1.csv", start=1, end=6)
        assert result == b"a,b\n1,2\n"[1:6]

        result = await async_fs._cat(".test.fakedata.1.csv", start=-1)
        assert result == b"a,b\n1,2\n"[-1:]

        result = await async_fs._cat(".test.fakedata.1.csv", start=1, end=-2)
        assert result == b"a,b\n1,2\n"[1:-2]

        # test synchronous API is available as expected
        async_fs = AsyncFileSystemWrapper(fs, asynchronous=False)
        result = async_fs.cat(".test.fakedata.1.csv", start=1, end=-2)
        assert result == b"a,b\n1,2\n"[1:-2]

        out = set(
            (
                await async_fs._cat(
                    [".test.fakedata.1.csv", ".test.fakedata.2.csv"], start=1, end=-1
                )
            ).values()
        )
        assert out == {b"a,b\n1,2\n"[1:-1], b"a,b\n3,4\n"[1:-1]}


@pytest.mark.asyncio
async def test_basic_crud_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        await async_fs._touch(".test.fakedata.3.csv")
        assert await async_fs._exists(".test.fakedata.3.csv")

        data = await async_fs._cat(".test.fakedata.1.csv")
        assert data == b"a,b\n1,2\n"

        await async_fs._pipe(".test.fakedata.1.csv", b"a,b\n5,6\n")
        data = await async_fs._cat(".test.fakedata.1.csv")
        assert data == b"a,b\n5,6\n"

        await async_fs._rm(".test.fakedata.1.csv")
        assert not await async_fs._exists(".test.fakedata.1.csv")


@pytest.mark.asyncio
async def test_error_handling():
    fs = fsspec.filesystem("file")
    async_fs = AsyncFileSystemWrapper(fs)

    with pytest.raises(FileNotFoundError):
        await async_fs._cat(".test.non_existent.csv")

    with pytest.raises(FileNotFoundError):
        await async_fs._rm(".test.non_existent.csv")


@pytest.mark.asyncio
async def test_concurrent_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        async def read_file(file_path):
            return await async_fs._cat(file_path)

        results = await asyncio.gather(
            read_file(".test.fakedata.1.csv"),
            read_file(".test.fakedata.2.csv"),
            read_file(".test.fakedata.1.csv"),
        )

        assert results == [b"a,b\n1,2\n", b"a,b\n3,4\n", b"a,b\n1,2\n"]


@pytest.mark.asyncio
async def test_directory_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        await async_fs._makedirs("new_directory")
        assert await async_fs._isdir("new_directory")

        files = await async_fs._ls(".")
        filenames = [os.path.basename(file) for file in files]

        assert ".test.fakedata.1.csv" in filenames
        assert ".test.fakedata.2.csv" in filenames
        assert "new_directory" in filenames


@pytest.mark.asyncio
async def test_batch_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        await async_fs._rm([".test.fakedata.1.csv", ".test.fakedata.2.csv"])
        assert not await async_fs._exists(".test.fakedata.1.csv")
        assert not await async_fs._exists(".test.fakedata.2.csv")


def test_open(tmpdir):
    fn = f"{tmpdir}/afile"
    with open(fn, "wb") as f:
        f.write(b"hello")
    of = fsspec.open(
        "dir://afile::async_wrapper::file",
        mode="rb",
        async_wrapper={"asynchronous": False},
        dir={"path": str(tmpdir)},
    )
    with of as f:
        assert f.read() == b"hello"


@pytest.mark.parametrize("mode", ["wb", "w", "ab", "a"])
def test_open_persists_writes(tmp_path, mode):
    path = tmp_path / "output"
    path.write_bytes(b"original")
    fs = AsyncFileSystemWrapper(LocalFileSystem(), asynchronous=False)
    data = b"\x00hello\xff" if "b" in mode else "hello λ"
    with fs.open(path, mode, encoding="utf-8") as stream:
        assert stream.write(data) == len(data)

    expected = data if isinstance(data, bytes) else data.encode("utf-8")
    if "a" in mode:
        expected = b"original" + expected
    assert path.read_bytes() == expected
    with fs.open(path, "rb") as stream:
        assert stream.read() == expected


@pytest.mark.parametrize("mode", ["wb", "w"])
def test_chained_open_persists_writes(tmp_path, mode):
    path = tmp_path / "output"
    data = b"hello" if "b" in mode else "hello λ"
    with fsspec.open(
        f"async_wrapper::file://{path}",
        mode=mode,
        encoding="utf-8",
        async_wrapper={"asynchronous": False},
    ) as stream:
        stream.write(data)

    expected = data if isinstance(data, bytes) else data.encode("utf-8")
    assert path.read_bytes() == expected


def test_open_preserves_backend_errors(tmp_path):
    path = tmp_path / "output"
    path.write_bytes(b"original")
    fs = AsyncFileSystemWrapper(LocalFileSystem(), asynchronous=False)
    with pytest.raises(FileExistsError):
        fs.open(path, "xb")
    with pytest.raises(FileNotFoundError):
        fs.open(tmp_path / "missing" / "output", "wb")
    assert path.read_bytes() == b"original"


@pytest.mark.parametrize("commit", [True, False])
def test_open_transaction_persists_only_on_commit(tmp_path, commit):
    path = tmp_path / "output"
    path.write_bytes(b"original")
    fs = AsyncFileSystemWrapper(LocalFileSystem(), asynchronous=False)
    error = nullcontext() if commit else pytest.raises(RuntimeError, match="rollback")
    with error:
        with fs.transaction:
            with fs.open(path, "wb") as stream:
                stream.write(b"replacement")
            assert path.read_bytes() == b"original"
            if not commit:
                raise RuntimeError("rollback")
    assert path.read_bytes() == (b"replacement" if commit else b"original")


@pytest.mark.asyncio
async def test_semaphore_synchronous():
    fs = AsyncFileSystemWrapper(
        LockedFileSystem(), asynchronous=False, semaphore=asyncio.Semaphore(1)
    )

    paths = [f"path_{i}" for i in range(1, 3)]
    results = await asyncio.gather(*(fs._cat_file(path) for path in paths))

    assert set(results) == {path.encode() for path in paths}


@pytest.mark.asyncio
async def test_deadlock_when_asynchronous():
    fs = AsyncFileSystemWrapper(
        LockedFileSystem(), asynchronous=False, semaphore=asyncio.Semaphore(3)
    )
    paths = [f"path_{i}" for i in range(1, 3)]

    with pytest.raises(RuntimeError, match="Concurrent requests!"):
        await asyncio.gather(*(fs._cat_file(path) for path in paths))


def test_get_does_not_write_above_destination(tmp_path):
    # The async copy builds its destinations the same way the sync one does,
    # so a source name holding ".." must not escape the destination there
    # either. tar is synchronous, so it is wrapped to exercise
    # AsyncFileSystem._get.
    archive = tmp_path / "traversal.tar"
    with tarfile.open(archive, "w") as t:
        for name, data in [("readme.txt", b"ok"), ("../escaped.txt", b"escaped")]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))

    dest = tmp_path / "dest"
    dest.mkdir()
    outside = tmp_path / "escaped.txt"

    fs = AsyncFileSystemWrapper(fsspec.filesystem("tar", fo=str(archive)))
    try:
        fs.get("*", str(dest), recursive=True)
    except ValueError:
        pass

    assert not outside.exists(), f"async copy wrote {outside}, above {dest}"


def test_paths_normalized_like_wrapped_fs():
    fs = fsspec.filesystem("memory")
    fs.pipe({"/norm/a": b"a", "/norm/sub/b": b"b"})
    async_fs = AsyncFileSystemWrapper(fs, asynchronous=False)

    assert async_fs._strip_protocol("memory://norm/a") == "/norm/a"
    assert async_fs._parent("memory://norm/a") == "/norm"

    dirfs = fsspec.filesystem("dir", path="memory://norm", fs=async_fs)
    assert dirfs.ls("", detail=False) == ["a", "sub"]
    assert dirfs.find("") == ["a", "sub/b"]


@pytest.mark.asyncio
async def test_walk():
    fs = fsspec.filesystem("memory")
    fs.pipe({"/walk/a": b"a", "/walk/sub/b": b"b"})
    async_fs = AsyncFileSystemWrapper(fs, asynchronous=True)

    walked = [item async for item in async_fs._walk("/walk")]
    assert walked == list(fs.walk("/walk"))

    walked = [item async for item in async_fs._walk("/walk", maxdepth=1)]
    assert walked == [("/walk", ["sub"], ["a"])]


class StallableSyncFS(LocalFileSystem):
    """A synchronous filesystem whose downloads stall mid-write until released."""

    def __init__(self):
        super().__init__()
        self.entered = collections.defaultdict(threading.Event)
        self.release = collections.defaultdict(threading.Event)
        self.finished = collections.defaultdict(threading.Event)
        self.calls = []
        self.fail_on_release = set()
        self.fail_immediately = set()

    def get_file(self, rpath, lpath, **kwargs):
        if rpath in self.fail_immediately:
            raise ValueError(f"backend error for {rpath}")
        self.calls.append(rpath)
        try:
            with open(lpath, "wb") as stream:
                stream.write(b"chunk-")
                stream.flush()
                self.entered[rpath].set()
                if not self.release[rpath].wait(timeout=30):
                    raise TimeoutError(f"test did not release {rpath}")
                if rpath in self.fail_on_release:
                    raise OSError(f"sync write failure for {rpath}")
                stream.write(b"rest")
        finally:
            self.finished[rpath].set()


class DownloadRig:
    """One mirror site behind two wrappers sharing a loop and a semaphore."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        self.loop.set_default_executor(self.executor)
        self.thread = threading.Thread(
            target=self.loop.run_forever, name="test-loop", daemon=True
        )
        self.thread.start()
        self.semaphore = asyncio.Semaphore(2)
        self.mirror = StallableSyncFS()
        self.wrappers = [
            AsyncFileSystemWrapper(
                fs=self.mirror,
                asynchronous=False,
                loop=self.loop,
                semaphore=self.semaphore,
            )
            for _ in range(2)
        ]

    def stop(self):
        for gate in self.mirror.release.values():
            gate.set()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=10)
        self.executor.shutdown(wait=True)


def _wait_for(condition, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


@pytest.mark.parametrize("a_mode", ["sync", "sync-write-error", "coroutine"])
def test_mixed_sync_async_cancellation_and_quota(tmp_path, a_mode):
    rig = DownloadRig()
    mirror, loop, sem = rig.mirror, rig.loop, rig.semaphore
    unrelated_release = threading.Event()
    try:
        a_part = tmp_path / "a.part"

        # two of the three default-executor workers do unrelated work
        unrelated = [
            rig.executor.submit(lambda: unrelated_release.wait(timeout=30))
            for _ in range(2)
        ]

        # A: submitted by the legacy sync scheduler, or directly by a coroutine
        a_outcome = {}
        if a_mode == "coroutine":
            a_tasks = []

            async def submit_a():
                a_tasks.append(asyncio.current_task())
                return await rig.wrappers[0]._get_file("a", str(a_part))

            fut_a = asyncio.run_coroutine_threadsafe(submit_a(), loop)
        else:
            if a_mode == "sync-write-error":
                mirror.fail_on_release.add("a")

            def submit_a():
                try:
                    fsspec.asyn.sync(
                        loop, rig.wrappers[0]._get_file, "a", str(a_part), timeout=1
                    )
                    a_outcome["result"] = "ok"
                except Exception as exc:
                    a_outcome["exc"] = exc

            a_thread = threading.Thread(target=submit_a, daemon=True)
            a_thread.start()

        assert mirror.entered["a"].wait(timeout=10)
        a_stalled_since = time.monotonic()
        assert a_part.read_bytes() == b"chunk-"

        # B: async download via the other wrapper; takes the second permit and
        # queues in the executor, since every worker thread is busy
        fut_b = asyncio.run_coroutine_threadsafe(
            rig.wrappers[1]._get_file("b", str(tmp_path / "b.part")), loop
        )
        assert _wait_for(sem.locked)
        fut_b.cancel()
        with pytest.raises(concurrent.futures.CancelledError):
            fut_b.result(timeout=10)

        # the scheduler can borrow and return a permit from the same semaphore
        async def borrow_permit():
            async with sem:
                return True

        assert asyncio.run_coroutine_threadsafe(borrow_permit(), loop).result(
            timeout=10
        )

        # keep A stalled across the 1s deadline of the sync call
        remaining = 3.0 - (time.monotonic() - a_stalled_since)
        if remaining > 0:
            time.sleep(remaining)

        # free the unrelated work; B must still never reach the mirror
        unrelated_release.set()
        for fut in unrelated:
            fut.result(timeout=10)

        # C: async download, enters the backend and stalls there
        fut_c = asyncio.run_coroutine_threadsafe(
            rig.wrappers[0]._get_file("c", str(tmp_path / "c.part")), loop
        )
        assert mirror.entered["c"].wait(timeout=10)
        assert "b" not in mirror.calls

        # D: waits for a permit although a worker thread is free
        fut_d = asyncio.run_coroutine_threadsafe(
            rig.wrappers[1]._get_file("d", str(tmp_path / "d.part")), loop
        )
        # E: cancelled while waiting for a permit, must never reach the mirror
        fut_e = asyncio.run_coroutine_threadsafe(
            rig.wrappers[0]._get_file("e", str(tmp_path / "e.part")), loop
        )
        time.sleep(0.5)
        assert "d" not in mirror.calls
        assert not fut_d.done()
        fut_e.cancel()
        with pytest.raises(concurrent.futures.CancelledError):
            fut_e.result(timeout=10)

        if a_mode == "coroutine":
            # cancel A twice mid-write; it only finishes once the stalled
            # thread is done, and then with CancelledError
            loop.call_soon_threadsafe(a_tasks[0].cancel)
            loop.call_soon_threadsafe(a_tasks[0].cancel)
            time.sleep(0.5)
            assert not fut_a.done()
            mirror.release["a"].set()
            with pytest.raises(concurrent.futures.CancelledError):
                fut_a.result(timeout=10)
        else:
            # A's sync call is still waiting, past the 1s deadline
            assert a_outcome == {}
            assert a_thread.is_alive()
            mirror.release["a"].set()
            a_thread.join(timeout=10)
            assert isinstance(a_outcome.get("exc"), fsspec.FSTimeoutError)
        # the path is recyclable immediately: the writer thread is done
        assert mirror.finished["a"].is_set()
        a_part.unlink()
        a_part.write_bytes(b"reused")

        # D enters the backend once A is over; C and D finish normally
        assert mirror.entered["d"].wait(timeout=10)
        assert "e" not in mirror.calls
        mirror.release["c"].set()
        mirror.release["d"].set()
        assert fut_c.result(timeout=10) is None
        assert fut_d.result(timeout=10) is None
    finally:
        unrelated_release.set()
        rig.stop()


def test_download_result_and_backend_errors(tmp_path):
    rig = DownloadRig()
    mirror, loop = rig.mirror, rig.loop
    try:
        # normal downloads return through both call styles
        mirror.release["ok"].set()
        target = tmp_path / "ok.part"
        assert (
            fsspec.asyn.sync(
                loop, rig.wrappers[0]._get_file, "ok", str(target), timeout=5
            )
            is None
        )
        assert target.read_bytes() == b"chunk-rest"

        mirror.release["ok2"].set()

        async def normal():
            return await rig.wrappers[1]._get_file("ok2", str(tmp_path / "ok2.part"))

        assert (
            asyncio.run_coroutine_threadsafe(normal(), loop).result(timeout=10) is None
        )
        assert (tmp_path / "ok2.part").read_bytes() == b"chunk-rest"

        # backend errors raised within the deadline reach the caller as-is
        mirror.fail_immediately.add("bad")
        with pytest.raises(ValueError, match="backend error"):
            fsspec.asyn.sync(
                loop,
                rig.wrappers[0]._get_file,
                "bad",
                str(tmp_path / "bad.part"),
                timeout=5,
            )

        async def failing():
            return await rig.wrappers[1]._get_file("bad", str(tmp_path / "bad2.part"))

        with pytest.raises(ValueError, match="backend error"):
            asyncio.run_coroutine_threadsafe(failing(), loop).result(timeout=10)
    finally:
        rig.stop()
