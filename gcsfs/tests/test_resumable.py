"""Tests for resumable, generation-pinned downloads (``gcsfs.resumable``).

The GCS side is fully scripted in-process: object metadata is served from a
fake ``_info`` and every media range request goes through a controllable
aiohttp-style session that returns prepared (possibly overlapping or wrong
generation) responses. No network access is required.
"""

import os

import pytest

from gcsfs import GCSFileSystem
from gcsfs.resumable import (
    GenerationMismatchResponseError,
    ObjectGenerationUnavailable,
    ResumableDownloadCredential,
    ResumableDownloadError,
    merge_intervals,
    missing_ranges,
    uncovered_pieces,
)
from gcsfs.retry import ChecksumError

G1_GENERATION = "1111111111111111"
G2_GENERATION = "2222222222222222"
OBJECT = "test-bucket/nightly-archive.bin"


def crc32c_b64(data):
    from gcsfs.resumable import Crc32c

    hasher = Crc32c()
    hasher.update(data)
    return hasher.b64digest()


G1 = bytes(range(16))
G2 = bytes((byte + 7) % 256 for byte in range(16))
assert len(G1) == len(G2) and G1 != G2
G1_CRC = crc32c_b64(G1)
G2_CRC = crc32c_b64(G2)
OLD_PUBLISH = b"previous-published-generation"


class FakeResponse:
    """Minimal async context manager mirroring the aiohttp response surface."""

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = dict(headers)
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def read(self):
        return self._body


class ScriptedRangeSession:
    """Serves scripted range responses, then canonical object bytes.

    Scripted entries are keyed by the requested ``[start, end)`` range, so the
    order in which concurrent requests arrive does not matter. Each range holds
    a queue of one-shot responses; once the queue is empty, canonical object
    bytes are returned. This lets a range fail once (e.g. wrong generation) and
    then succeed on retry.
    """

    def __init__(self, content, generation, scripted, serve_remaining_correct=True):
        self.content = content
        self.generation = generation
        self.scripted = {}
        for start, end, responses in scripted:
            self.scripted[(start, end)] = list(responses)
        self.serve_remaining_correct = serve_remaining_correct
        self.requests = []

    def get(self, url, params=None, headers=None, timeout=None):
        range_header = headers["Range"]
        _, bounds = range_header.split("=")
        start_text, end_text = bounds.split("-")
        start, end = int(start_text), int(end_text) + 1
        self.requests.append((start, end))

        queue = self.scripted.get((start, end))
        if queue:
            actual_start, body, generation, status = queue.pop(0)
        elif self.serve_remaining_correct:
            actual_start, body, generation, status = (
                start,
                self.content[start:end],
                self.generation,
                206,
            )
        else:
            raise AssertionError(f"unexpected range request [{start}, {end})")

        response_headers = {"x-goog-generation": generation}
        if status == 206:
            response_headers["Content-Range"] = (
                f"bytes {actual_start}-{actual_start + len(body) - 1}/{len(self.content)}"
            )
        return FakeResponse(status, response_headers, body)


def make_fs(monkeypatch, current_meta, pinned_meta=None, pinned_error=None, session=None):
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)

    async def fake_info(path, generation=None, **kwargs):
        if generation is not None:
            if pinned_error is not None:
                raise pinned_error
            return dict(pinned_meta)
        return dict(current_meta)

    monkeypatch.setattr(fs, "_info", fake_info)
    if session is not None:
        fs._session = session
    return fs


def object_meta(content, generation, crc=None):
    return {
        "name": OBJECT,
        "bucket": "test-bucket",
        "type": "file",
        "size": len(content),
        "generation": generation,
        "crc32c": crc if crc is not None else crc32c_b64(content),
    }


def write_credential(path, part_path, generation, size, crc, segments):
    credential = ResumableDownloadCredential(
        object_path=OBJECT,
        generation=generation,
        size=size,
        crc32c=crc,
        part_path=part_path,
        segments=segments,
    )
    credential.save(path)


def corrupt_part(part_path, offset):
    with open(part_path, "r+b") as handle:
        handle.seek(offset)
        handle.write((G1[offset] ^ 0xFF).to_bytes(1, "big"))


@pytest.fixture
def paths(tmp_path):
    lpath = tmp_path / "published.bin"
    part_path = tmp_path / "published.bin.part"
    credential_path = tmp_path / "published.bin.resume.json"
    return str(lpath), str(part_path), str(credential_path)


def seed_interrupted_download(part_path, credential_path, corrupt=True):
    """Recreate the interrupted G1 download: [0,4) and [8,12) persisted."""
    with open(part_path, "wb") as handle:
        handle.truncate(len(G1))
        handle.seek(0)
        handle.write(G1[0:4])
        handle.seek(8)
        handle.write(G1[8:12])
        handle.flush()
        os.fsync(handle.fileno())
    if corrupt:
        corrupt_part(part_path, 9)
    segments = [
        (0, 4, crc32c_b64(G1[0:4])),
        (8, 12, crc32c_b64(G1[8:12])),
    ]
    write_credential(
        credential_path,
        part_path,
        G1_GENERATION,
        len(G1),
        G1_CRC,
        segments,
    )


def test_resume_repairs_gap_and_corruption_then_publishes(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    seed_interrupted_download(part_path, credential_path)
    with open(lpath, "wb") as handle:
        handle.write(OLD_PUBLISH)

    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    g2_meta = object_meta(G2, G2_GENERATION, G2_CRC)

    # Round one, keyed by requested range: an overlapping fragment answering
    # [4,8), one wrong-generation fragment answering [8,12), then the G1 tail.
    # Exhausted script queues fall back to canonical G1 bytes, so the rejected
    # [8,12) response is retried correctly within the same worker.
    scripted = [
        (4, 8, [(2, G1[2:6], G1_GENERATION, 206)]),
        (8, 12, [(8, b"\x99" * 4, G2_GENERATION, 206)]),
        (12, 16, [(12, G1[12:16], G1_GENERATION, 206)]),
    ]
    session = ScriptedRangeSession(G1, G1_GENERATION, scripted)
    fs = make_fs(
        monkeypatch,
        current_meta=g2_meta,
        pinned_meta=g1_meta,
        session=session,
    )

    result = fs.get_file_resumable(
        OBJECT,
        lpath,
        credential_path=credential_path,
        part_path=part_path,
        chunk_size=4,
        concurrency=4,
    )

    assert result == lpath
    with open(lpath, "rb") as handle:
        assert handle.read() == G1
    assert not os.path.exists(part_path)
    assert not os.path.exists(credential_path)

    # [0,4) was already verified locally and must never be re-requested; the
    # corrupted [8,12) segment is requested twice: the wrong-generation attempt
    # is rejected and drained, then retried against the pinned generation.
    assert all(start >= 4 for start, _ in session.requests)
    assert session.requests.count((8, 12)) == 2
    # The overlapping [4,8) answer leaves a residual [6,8) gap for pass two.
    assert sorted(set(session.requests)) == [(4, 8), (6, 8), (8, 12), (12, 16)]


def test_wrong_generation_bytes_never_persisted(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    seed_interrupted_download(part_path, credential_path)
    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    g2_meta = object_meta(G2, G2_GENERATION, G2_CRC)

    scripted = [
        (4, 8, [(2, G1[2:6], G1_GENERATION, 206)]),
        (8, 12, [(8, b"\x99" * 4, G2_GENERATION, 206)]),
        (12, 16, [(12, G1[12:16], G1_GENERATION, 206)]),
    ]
    session = ScriptedRangeSession(G1, G1_GENERATION, scripted)
    fs = make_fs(monkeypatch, g2_meta, pinned_meta=g1_meta, session=session)

    fs.get_file_resumable(
        OBJECT,
        lpath,
        credential_path=credential_path,
        part_path=part_path,
        chunk_size=4,
        concurrency=1,
    )

    with open(lpath, "rb") as handle:
        published = handle.read()
    assert published == G1
    assert b"\x99" * 4 not in published


def test_persistent_wrong_generation_aborts_and_keeps_publish(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    seed_interrupted_download(part_path, credential_path)
    with open(lpath, "wb") as handle:
        handle.write(OLD_PUBLISH)

    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    g2_meta = object_meta(G2, G2_GENERATION, G2_CRC)
    # Every retry of the first gap claims G2: attempts exhaust without writing.
    scripted = [
        (
            4,
            8,
            [
                (4, G2[4:8], G2_GENERATION, 206),
                (4, G2[4:8], G2_GENERATION, 206),
                (4, G2[4:8], G2_GENERATION, 206),
            ],
        ),
    ]
    session = ScriptedRangeSession(
        G1, G1_GENERATION, scripted, serve_remaining_correct=False
    )
    fs = make_fs(monkeypatch, g2_meta, pinned_meta=g1_meta, session=session)

    with pytest.raises(GenerationMismatchResponseError):
        fs.get_file_resumable(
            OBJECT,
            lpath,
            credential_path=credential_path,
            part_path=part_path,
            chunk_size=4,
            concurrency=1,
        )

    with open(lpath, "rb") as handle:
        assert handle.read() == OLD_PUBLISH
    assert os.path.exists(part_path)
    assert os.path.exists(credential_path)


def test_generation_disappeared_keeps_published_file(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    seed_interrupted_download(part_path, credential_path)
    with open(lpath, "wb") as handle:
        handle.write(OLD_PUBLISH)

    g2_meta = object_meta(G2, G2_GENERATION, G2_CRC)
    session = ScriptedRangeSession(G1, G1_GENERATION, scripted=[])
    fs = make_fs(
        monkeypatch,
        current_meta=g2_meta,
        pinned_error=FileNotFoundError(OBJECT),
        session=session,
    )

    with pytest.raises(ObjectGenerationUnavailable):
        fs.get_file_resumable(
            OBJECT,
            lpath,
            credential_path=credential_path,
            part_path=part_path,
            chunk_size=4,
        )

    with open(lpath, "rb") as handle:
        assert handle.read() == OLD_PUBLISH
    assert os.path.exists(part_path)
    assert os.path.exists(credential_path)
    assert session.requests == []


def test_generation_vanishes_mid_download_keeps_published_file(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    seed_interrupted_download(part_path, credential_path)
    with open(lpath, "wb") as handle:
        handle.write(OLD_PUBLISH)

    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    g2_meta = object_meta(G2, G2_GENERATION, G2_CRC)
    # Metadata still serves G1, but the media endpoint has already dropped it.
    scripted = [
        (4, 8, [(4, b"", G1_GENERATION, 404)]),
    ]
    session = ScriptedRangeSession(
        G1, G1_GENERATION, scripted, serve_remaining_correct=False
    )
    fs = make_fs(monkeypatch, g2_meta, pinned_meta=g1_meta, session=session)

    with pytest.raises(ObjectGenerationUnavailable):
        fs.get_file_resumable(
            OBJECT,
            lpath,
            credential_path=credential_path,
            part_path=part_path,
            chunk_size=4,
            concurrency=1,
        )

    with open(lpath, "rb") as handle:
        assert handle.read() == OLD_PUBLISH
    assert os.path.exists(part_path)
    assert os.path.exists(credential_path)


def test_unpersisted_credential_bytes_are_not_trusted(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    # Credential claims [0,4) and [4,8), but only [0,4) ever reached disk.
    with open(part_path, "wb") as handle:
        handle.truncate(len(G1))
        handle.seek(0)
        handle.write(G1[0:4])
        handle.flush()
        os.fsync(handle.fileno())
    write_credential(
        credential_path,
        part_path,
        G1_GENERATION,
        len(G1),
        G1_CRC,
        [
            (0, 4, crc32c_b64(G1[0:4])),
            (4, 8, crc32c_b64(G1[4:8])),
        ],
    )

    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    session = ScriptedRangeSession(G1, G1_GENERATION, scripted=[])
    fs = make_fs(monkeypatch, g1_meta, pinned_meta=g1_meta, session=session)

    fs.get_file_resumable(
        OBJECT,
        lpath,
        credential_path=credential_path,
        part_path=part_path,
        chunk_size=4,
        concurrency=1,
    )

    with open(lpath, "rb") as handle:
        assert handle.read() == G1
    assert (4, 8) in session.requests
    assert all(start >= 4 for start, _ in session.requests)


def test_corrupted_segment_is_refetched_even_without_gap(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    # Every byte was persisted, but one byte in [8,12) is wrong.
    with open(part_path, "wb") as handle:
        handle.write(G1)
        handle.flush()
        os.fsync(handle.fileno())
    corrupt_part(part_path, 10)
    write_credential(
        credential_path,
        part_path,
        G1_GENERATION,
        len(G1),
        G1_CRC,
        [
            (0, 4, crc32c_b64(G1[0:4])),
            (4, 8, crc32c_b64(G1[4:8])),
            (8, 12, crc32c_b64(G1[8:12])),
            (12, 16, crc32c_b64(G1[12:16])),
        ],
    )

    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    session = ScriptedRangeSession(G1, G1_GENERATION, scripted=[])
    fs = make_fs(monkeypatch, g1_meta, pinned_meta=g1_meta, session=session)

    fs.get_file_resumable(
        OBJECT,
        lpath,
        credential_path=credential_path,
        part_path=part_path,
        chunk_size=4,
        concurrency=1,
    )

    with open(lpath, "rb") as handle:
        assert handle.read() == G1
    assert session.requests == [(8, 12)]


def test_full_object_checksum_mismatch_blocks_publish(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    with open(lpath, "wb") as handle:
        handle.write(OLD_PUBLISH)
    g1_meta = object_meta(G1, G1_GENERATION, G1_CRC)
    # Server hands back G2 bytes while metadata still promises G1's CRC32C.
    session = ScriptedRangeSession(G2, G1_GENERATION, scripted=[])
    fs = make_fs(monkeypatch, g1_meta, pinned_meta=g1_meta, session=session)

    with pytest.raises(ChecksumError):
        fs.get_file_resumable(
            OBJECT,
            lpath,
            credential_path=credential_path,
            part_path=part_path,
            chunk_size=4,
            concurrency=1,
        )

    with open(lpath, "rb") as handle:
        assert handle.read() == OLD_PUBLISH
    assert os.path.exists(part_path)
    assert os.path.exists(credential_path)


def test_missing_crc_metadata_aborts(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    meta = object_meta(G1, G1_GENERATION)
    meta.pop("crc32c")
    session = ScriptedRangeSession(G1, G1_GENERATION, scripted=[])
    fs = make_fs(monkeypatch, meta, pinned_meta=meta, session=session)

    with pytest.raises(ResumableDownloadError, match="crc32c"):
        fs.get_file_resumable(
            OBJECT,
            lpath,
            credential_path=credential_path,
            part_path=part_path,
        )
    assert not os.path.exists(lpath)


def test_legacy_get_file_creates_no_credential(paths, monkeypatch):
    lpath, part_path, credential_path = paths
    fs = GCSFileSystem(token="anon", skip_instance_cache=True)

    async def fake_get_file_request(rpath, target, *args, **kwargs):
        with open(target, "wb") as handle:
            handle.write(G1)
        return 200, {}, None, G1

    monkeypatch.setattr(fs, "_get_file_request", fake_get_file_request)
    fs.get_file(OBJECT, lpath, concurrency=1)

    with open(lpath, "rb") as handle:
        assert handle.read() == G1
    assert not os.path.exists(part_path)
    assert not os.path.exists(credential_path)
    assert list(os.listdir(os.path.dirname(lpath))) == [os.path.basename(lpath)]


def test_credential_round_trip(tmp_path):
    credential_path = tmp_path / "resume.json"
    credential = ResumableDownloadCredential(
        object_path=OBJECT,
        generation=G1_GENERATION,
        size=len(G1),
        crc32c=G1_CRC,
        part_path=str(tmp_path / "x.part"),
        segments=[(0, 4, crc32c_b64(G1[0:4]))],
    )
    credential.save(str(credential_path))
    loaded = ResumableDownloadCredential.load(str(credential_path))

    assert loaded.object_path == OBJECT
    assert loaded.generation == G1_GENERATION
    assert loaded.size == len(G1)
    assert loaded.crc32c == G1_CRC
    assert loaded.segments == [(0, 4, crc32c_b64(G1[0:4]))]


def test_unreadable_credential_is_ignored(tmp_path):
    credential_path = tmp_path / "resume.json"
    credential_path.write_text("{not valid json")
    assert ResumableDownloadCredential.load(str(credential_path)) is None


def test_interval_helpers():
    assert merge_intervals([(0, 4), (2, 6), (8, 12)]) == [(0, 6), (8, 12)]
    assert missing_ranges([(0, 4), (8, 12)], 16) == [(4, 8), (12, 16)]
    assert missing_ranges([(2, 6)], 8) == [(0, 2), (6, 8)]
    assert uncovered_pieces((0, 8), [(2, 6)]) == [(0, 2), (6, 8)]
    assert uncovered_pieces((4, 8), [(0, 6)]) == [(6, 8)]
    assert uncovered_pieces((2, 6), [(0, 16)]) == []
