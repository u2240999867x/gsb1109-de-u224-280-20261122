import asyncio
import hashlib
import json
import os
import subprocess
import sys
from io import BytesIO

import pytest

from s3fs import S3FileSystem
from s3fs.resumable import (
    STATUS_PUBLISHED,
    STATUS_RUNNING,
    TransferSuperseded,
    TransferUnrecoverable,
    _load_state,
    _sidecar_dir,
)


V1 = b"AAAABBBBCCCC"
V2 = b"DDDDEEEEFFFF"
V3 = b"GGGGHHHHIIII"
PREVIOUS = b"000011112222"


class Body:
    def __init__(self, data):
        self.stream = BytesIO(data)
        self.closed = False

    async def read(self, size=-1):
        return self.stream.read(size)

    def close(self):
        self.closed = True
        self.stream.close()


class KilledBody:
    """Dies only once ``parts_ready`` reports enough sibling parts on disk."""

    def __init__(self, parts_ready):
        self.parts_ready = parts_ready
        self.closed = False

    async def read(self, size=-1):
        while not self.parts_ready():
            await asyncio.sleep(0)
        raise ProcessKilled("process terminated mid-download")

    def close(self):
        self.closed = True


class ProcessKilled(RuntimeError):
    """Stands in for ``SIGKILL`` / a hard process termination."""


class FakeS3:
    """In-memory S3 supporting explicit object versions and range reads."""

    def __init__(self, bucket, key, versioned=True):
        self.bucket = bucket
        self.key = key
        self.versioned = versioned
        self.versions = []  # newest last: [(version_id, data)]
        self.range_calls = []
        self.head_calls = []
        # ranges (inclusive) that must fail as if the process was terminated
        self.kill_ranges = set()
        self.request_options_seen = []
        self._sidecar_root = None
        self._gate_completions = 0

    def arm(self, lpath, gate_completions):
        """Arm the kill gate: die only after this many parts are on disk."""
        self._sidecar_root = _sidecar_dir(str(lpath))
        self._gate_completions = gate_completions

    def _parts_on_disk(self):
        if self._sidecar_root is None:
            return 0
        count = 0
        for _root, _dirs, files in os.walk(self._sidecar_root):
            count += sum(1 for name in files if name.startswith("part-"))
        return count

    def put(self, data, version_id):
        if self.versioned:
            self.versions.append((version_id, data))
        else:
            self.versions = [(None, data)]

    def _resolve(self, version_id):
        vid = version_id or self.versions[-1][0]
        for stored_id, data in self.versions:
            if stored_id == vid:
                return vid, data
        raise FileNotFoundError(f"NoSuchKey: {self.key} version {version_id}")

    def head_object(self, Bucket, Key, VersionId=None, **kwargs):
        self.head_calls.append(VersionId)
        self.request_options_seen.append(kwargs)
        vid, data = self._resolve(VersionId)
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        return {
            "ContentLength": len(data),
            "ETag": etag,
            "VersionId": vid,
        }

    def get_object(self, Bucket, Key, Range=None, VersionId=None, **kwargs):
        self.request_options_seen.append(kwargs)
        vid, data = self._resolve(VersionId)
        if Range:
            start, end = (int(part) for part in Range.removeprefix("bytes=").split("-"))
            self.range_calls.append((start, end))
            if (start, end) in self.kill_ranges:
                return {
                    "Body": KilledBody(
                        lambda: self._parts_on_disk() >= self._gate_completions
                    ),
                    "ContentLength": 0,
                }
            data = data[start : end + 1]
            return {"Body": Body(data), "ContentLength": len(data)}
        return {"Body": Body(data), "ContentLength": len(data), "VersionId": vid}

    async def __call__(self, method, **kwargs):
        handler = getattr(self, method)
        return handler(**kwargs)


def make_fs(backend):
    fs = S3FileSystem(asynchronous=True, skip_instance_cache=True)
    fs._call_s3 = backend
    return fs


def get_file(fs, *args, **kwargs):
    return asyncio.run(fs._get_file(*args, **kwargs))


def state_for(target):
    return _load_state(str(target))


def test_without_transfer_id_downloads_directly(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V1, "v1")
    fs = make_fs(backend)
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)

    get_file(
        fs,
        "bucket/incoming/daily.dat",
        str(target),
        chunksize=4,
        max_concurrency=1,
    )

    assert target.read_bytes() == V1
    assert not os.path.exists(_sidecar_dir(str(target)))
    # The legacy single-stream path issues one unranged GET, no HEAD.
    assert backend.range_calls == []
    assert backend.head_calls == []


def test_takeover_fences_old_task_and_preserves_reader_view(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V1, "v1")
    backend.put(V2, "v2")
    fs = make_fs(backend)
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    # Task A starts on v1 and is killed after its first 4-byte part landed.
    backend.arm(target, gate_completions=1)
    backend.kill_ranges = {(4, 7)}
    with pytest.raises(ProcessKilled):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            max_concurrency=2,
            transfer_id="A",
        )
    assert target.read_bytes() == PREVIOUS
    assert state_for(target)["status"] == STATUS_RUNNING
    assert state_for(target)["transfer_id"] == "A"

    # A different task number without an explicit takeover is rejected.
    with pytest.raises(TransferSuperseded):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            transfer_id="sneaky",
        )

    # Task B explicitly takes over; three parts are requested concurrently, the
    # middle one is interrupted, while the first and last land on disk.
    backend.arm(target, gate_completions=2)
    backend.kill_ranges = {(4, 7)}
    with pytest.raises(ProcessKilled):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            max_concurrency=3,
            transfer_id="B",
            takeover=True,
        )
    assert target.read_bytes() == PREVIOUS
    state = state_for(target)
    assert state["transfer_id"] == "B"
    assert state["version_id"] == "v2"

    # Releasing A now must fail: B owns the destination and A is fenced off.
    with pytest.raises(TransferSuperseded):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            transfer_id="A",
        )
    assert target.read_bytes() == PREVIOUS


def test_resume_uses_only_missing_bytes_and_switches_atomically(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V1, "v1")
    backend.put(V2, "v2")
    fs = make_fs(backend)
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    # Recreate B's partial run: parts 0-3 and 8-11 of v2 are on disk.
    backend.arm(target, gate_completions=2)
    backend.kill_ranges = {(4, 7)}
    with pytest.raises(ProcessKilled):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            max_concurrency=3,
            transfer_id="B",
            takeover=True,
        )
    backend.kill_ranges.clear()
    backend.range_calls.clear()

    # Resume with B's number and a new chunk size of 6: only bytes 4-7 remain.
    get_file(
        fs,
        rpath,
        str(target),
        chunksize=6,
        max_concurrency=2,
        transfer_id="B",
    )
    assert backend.range_calls == [(4, 7)]
    assert target.read_bytes() == V2
    assert state_for(target)["status"] == STATUS_PUBLISHED

    # A same-length v3 is published afterwards; v2 remains readable by version.
    backend.put(V3, "v3")
    latest_head = backend.head_object(Bucket="bucket", Key="incoming/daily.dat")
    v2_head = backend.head_object(
        Bucket="bucket", Key="incoming/daily.dat", VersionId="v2"
    )
    assert latest_head["VersionId"] == "v3"
    assert v2_head["ContentLength"] == len(V2)

    # A repeated B call confirms the publication without any new download.
    backend.range_calls.clear()
    get_file(fs, rpath, str(target), chunksize=6, transfer_id="B")
    assert backend.range_calls == []
    assert target.read_bytes() == V2


def test_resume_after_commit_replaces_target_exactly_once(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V2, "v2")
    fs = make_fs(backend)
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    from s3fs import resumable

    replacements = []
    original_replace = resumable.ResumableDownload._replace_target

    def crash_after_replace(self, candidate):
        original_replace(self, candidate)
        replacements.append(candidate)
        raise ProcessKilled("result lost before the scheduler was notified")

    resumable.ResumableDownload._replace_target = crash_after_replace
    try:
        with pytest.raises(ProcessKilled):
            get_file(
                fs,
                rpath,
                str(target),
                chunksize=4,
                transfer_id="B",
            )
    finally:
        resumable.ResumableDownload._replace_target = original_replace

    # The destination already carries v2; state still says committed because
    # the process vanished before recording the publication.
    assert target.read_bytes() == V2
    assert state_for(target)["status"] == "committed"

    # Resume confirms the publication without re-downloading or re-replacing.
    backend.range_calls.clear()
    get_file(fs, rpath, str(target), chunksize=4, transfer_id="B")
    assert backend.range_calls == []
    assert len(replacements) == 1
    assert target.read_bytes() == V2
    assert state_for(target)["status"] == STATUS_PUBLISHED


def test_resume_finishes_committed_candidate_left_on_disk(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V2, "v2")
    fs = make_fs(backend)
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    from s3fs import resumable

    original_replace = resumable.ResumableDownload._replace_target

    def die_before_rename(self, candidate):
        raise ProcessKilled("killed after commit record, before the rename")

    resumable.ResumableDownload._replace_target = die_before_rename
    try:
        with pytest.raises(ProcessKilled):
            get_file(fs, rpath, str(target), chunksize=4, transfer_id="B")
    finally:
        resumable.ResumableDownload._replace_target = original_replace

    # The commit record and assembled candidate survived; target is untouched.
    assert target.read_bytes() == PREVIOUS
    state = state_for(target)
    assert state["status"] == "committed"
    assert os.path.exists(state["candidate"])

    # Resume performs the pending rename without any new S3 range request.
    backend.range_calls.clear()
    get_file(fs, rpath, str(target), chunksize=4, transfer_id="B")
    assert backend.range_calls == []
    assert target.read_bytes() == V2
    assert state_for(target)["status"] == STATUS_PUBLISHED
    assert not os.path.exists(state["candidate"])


def test_unversioned_overwrite_makes_partial_transfer_unrecoverable(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat", versioned=False)
    backend.put(V2, None)
    fs = make_fs(backend)
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    backend.arm(target, gate_completions=2)
    backend.kill_ranges = {(4, 7)}
    with pytest.raises(ProcessKilled):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            max_concurrency=3,
            transfer_id="B",
        )

    # The bucket only keeps the current object; v3 overwrites the partial v2.
    backend.put(V3, None)

    with pytest.raises(TransferUnrecoverable):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=6,
            transfer_id="B",
        )
    assert target.read_bytes() == PREVIOUS
    assert state_for(target)["status"] == STATUS_RUNNING


def test_transfer_mode_forwards_request_options(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V1, "v1")
    fs = make_fs(backend)
    target = tmp_path / "daily.dat"
    options = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": "key"}

    get_file(
        fs,
        "bucket/incoming/daily.dat",
        str(target),
        chunksize=4,
        transfer_id="A",
        **options,
    )

    assert target.read_bytes() == V1
    for seen in backend.request_options_seen:
        assert seen["SSECustomerAlgorithm"] == "AES256"
        assert seen["SSECustomerKey"] == "key"


def test_transfer_mode_rejects_partial_object_options(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V1, "v1")
    fs = make_fs(backend)
    target = tmp_path / "daily.dat"
    with pytest.raises(ValueError, match="Range"):
        get_file(
            fs,
            "bucket/incoming/daily.dat",
            str(target),
            transfer_id="A",
            Range="bytes=0-3",
        )
    assert backend.head_calls == []


def test_resume_from_fresh_filesystem_instance(tmp_path):
    # All transfer state lives in the sidecar directory, so a process restart
    # (here: a new S3FileSystem and a new event loop) resumes with no context.
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V2, "v2")
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    fs = make_fs(backend)
    backend.arm(target, gate_completions=2)
    backend.kill_ranges = {(4, 7)}
    with pytest.raises(ProcessKilled):
        get_file(
            fs,
            rpath,
            str(target),
            chunksize=4,
            max_concurrency=3,
            transfer_id="B",
        )
    backend.kill_ranges.clear()

    restarted_fs = make_fs(backend)
    get_file(
        restarted_fs,
        rpath,
        str(target),
        chunksize=6,
        max_concurrency=2,
        transfer_id="B",
    )
    assert target.read_bytes() == V2
    assert state_for(target)["status"] == STATUS_PUBLISHED


def test_takeover_of_published_target_republishes_new_version(tmp_path):
    backend = FakeS3("bucket", "incoming/daily.dat")
    backend.put(V2, "v2")
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)
    rpath = "bucket/incoming/daily.dat"

    fs = make_fs(backend)
    get_file(fs, rpath, str(target), chunksize=4, transfer_id="B")
    assert target.read_bytes() == V2

    # A same-length v3 is published after B completed.
    backend.put(V3, "v3")

    # A new task number takes over the already-published destination.
    get_file(
        fs,
        rpath,
        str(target),
        chunksize=4,
        transfer_id="C",
        takeover=True,
    )
    assert target.read_bytes() == V3
    state = state_for(target)
    assert state["transfer_id"] == "C"
    assert state["version_id"] == "v3"
    assert state["status"] == STATUS_PUBLISHED

    # The old task number is now fenced off, including its idempotent re-calls.
    with pytest.raises(TransferSuperseded):
        get_file(fs, rpath, str(target), chunksize=4, transfer_id="B")
    assert target.read_bytes() == V3


def _run_worker(target, stage, chunksize):
    worker = os.path.join(os.path.dirname(__file__), "transfer_worker.py")
    env = dict(os.environ, PYTHONPATH="")
    completed = subprocess.run(
        [sys.executable, worker, "--target", str(target), "--stage", stage,
         "--chunksize", str(chunksize)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    return completed.returncode, payload


def test_resume_across_real_processes(tmp_path):
    # Different OS processes, no shared memory: only the destination and its
    # sidecar directory carry the transfer between invocations.
    target = tmp_path / "ready" / "daily.dat"
    target.parent.mkdir(parents=True)
    target.write_bytes(PREVIOUS)

    code, first = _run_worker(target, "partial", chunksize=4)
    assert code == 2 and first["killed"] is True
    assert target.read_bytes() == PREVIOUS
    assert state_for(target)["status"] == STATUS_RUNNING

    # A fresh process resumes with a new chunk size and only the missing part.
    code, second = _run_worker(target, "resume", chunksize=6)
    assert code == 0 and second["killed"] is False
    assert second["ranges"] == [[4, 7]]
    assert target.read_bytes() == V2
    assert state_for(target)["status"] == STATUS_PUBLISHED

    # Another fresh process confirms the publication and downloads nothing.
    code, third = _run_worker(target, "confirm", chunksize=6)
    assert code == 0 and third["ranges"] == []
    assert target.read_bytes() == V2
