"""
Resumable, generation-pinned downloads to a fixed local publish path.

A download writes into a private, pre-allocated ``*.part`` file next to the
publish target and records its progress in a ``*.resume.json`` credential.
The credential pins the exact object generation, the full object size and
CRC32C, and the byte intervals that have already been *durably* persisted
(each with its own CRC32C). The publish path is touched exactly once, via an
atomic ``os.replace``, and only after the reassembled part matches the
service's whole-object CRC32C.

Recovery rules:

* only intervals whose on-disk bytes still match their recorded CRC32C are
  trusted, so corrupted or never-durably-written bytes are re-requested;
* every range request is pinned to the credential's generation, and any
  response carrying a different ``x-goog-generation`` is rejected before being
  written;
* overlapping responses extend the covered interval set through a union, so
  progress is never counted twice;
* if the pinned generation can no longer be read, the existing publish file,
  the part file and the credential are all left in place.
"""

import asyncio
import base64
import json
import logging
import os
import re

from .retry import ChecksumError, validate_response

logger = logging.getLogger("gcsfs.resumable")

try:
    import crcmod

    class Crc32c:
        """Streaming CRC32C backed by ``crcmod``."""

        def __init__(self):
            self._crc = crcmod.Crc(0x11EDC6F41, initCrc=0, xorOut=0xFFFFFFFF)

        def update(self, data):
            self._crc.update(data)

        def b64digest(self):
            return base64.b64encode(self._crc.digest()).decode()

except ImportError:
    import google_crc32c

    class Crc32c:
        """Streaming CRC32C backed by ``google_crc32c``."""

        def __init__(self):
            self._crc = google_crc32c.Checksum()

        def update(self, data):
            self._crc.update(data)

        def b64digest(self):
            return base64.b64encode(self._crc.digest()).decode()


CREDENTIAL_VERSION = 1
_MAX_GENERATION_MISMATCH_RETRIES = 3
_CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(?:\d+|\*)")


class ResumableDownloadError(OSError):
    """Raised when a resumable download cannot be completed safely."""


class ObjectGenerationUnavailable(ResumableDownloadError):
    """Raised when the pinned object generation can no longer be read."""

    def __init__(self, path, generation):
        self.path = path
        self.generation = generation
        super().__init__(
            f"{path}: object generation {generation} is no longer available; "
            "keeping the existing published file"
        )


class GenerationMismatchResponseError(ResumableDownloadError):
    """Raised when a range response does not belong to the pinned generation."""


def merge_intervals(intervals):
    """Return the sorted, disjoint union of ``[start, end)`` intervals."""
    merged = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def missing_ranges(covered, size):
    """Return the uncovered ``[start, end)`` gaps inside ``[0, size)``."""
    gaps = []
    cursor = 0
    for start, end in merge_intervals(covered):
        if start > cursor:
            gaps.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < size:
        gaps.append((cursor, size))
    return gaps


def uncovered_pieces(interval, covered):
    """Return the parts of ``interval`` not already inside ``covered``."""
    start, end = interval
    pieces = []
    cursor = start
    for covered_start, covered_end in merge_intervals(covered):
        if covered_end <= cursor:
            continue
        if covered_start >= end:
            break
        if covered_start > cursor:
            pieces.append((cursor, min(covered_start, end)))
        cursor = max(cursor, covered_end)
        if cursor >= end:
            break
    if cursor < end:
        pieces.append((cursor, end))
    return pieces


def _parse_content_range(header, fallback_start):
    """Extract the inclusive start offset from a ``Content-Range`` header."""
    if not header:
        return fallback_start
    match = _CONTENT_RANGE_RE.fullmatch(header.strip())
    if not match:
        return fallback_start
    return int(match.group(1))


class ResumableDownloadCredential:
    """Persistent resume state for a single object generation."""

    def __init__(
        self,
        object_path,
        generation,
        size,
        crc32c,
        part_path,
        segments=None,
    ):
        self.object_path = object_path
        self.generation = str(generation) if generation is not None else None
        self.size = int(size)
        self.crc32c = crc32c
        self.part_path = part_path
        self.segments = list(segments or [])

    def to_dict(self):
        return {
            "version": CREDENTIAL_VERSION,
            "object": self.object_path,
            "generation": self.generation,
            "size": self.size,
            "crc32c": self.crc32c,
            "part_path": self.part_path,
            "segments": [
                {"start": start, "end": end, "crc32c": crc}
                for start, end, crc in self.segments
            ],
        }

    @classmethod
    def from_dict(cls, data):
        segments = [
            (int(item["start"]), int(item["end"]), item["crc32c"])
            for item in data.get("segments", [])
        ]
        return cls(
            object_path=data["object"],
            generation=data.get("generation"),
            size=int(data["size"]),
            crc32c=data.get("crc32c"),
            part_path=data["part_path"],
            segments=segments,
        )

    def save(self, credential_path):
        """Atomically persist the credential so a partial write can never survive."""
        tmp_path = f"{credential_path}.tmp"
        with open(tmp_path, "w") as handle:
            json.dump(self.to_dict(), handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, credential_path)

    @classmethod
    def load(cls, credential_path):
        """Return the credential, or ``None`` if it is missing or unreadable."""
        try:
            with open(credential_path) as handle:
                data = json.load(handle)
            if data.get("version") != CREDENTIAL_VERSION:
                logger.warning("Ignoring resume credential with unknown version")
                return None
            return cls.from_dict(data)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            logger.warning("Ignoring unreadable resume credential: %s", exc)
            return None


class ResumableDownloader:
    """Downloads one object into a part file and publishes it after full validation."""

    def __init__(
        self,
        fs,
        rpath,
        lpath,
        credential_path=None,
        part_path=None,
        chunk_size=8 * 1024 * 1024,
        concurrency=4,
        generation=None,
    ):
        self.fs = fs
        self.rpath = rpath
        self.lpath = lpath
        self.part_path = part_path or f"{lpath}.part"
        self.credential_path = credential_path or f"{lpath}.resume.json"
        self.chunk_size = max(1, int(chunk_size))
        self.concurrency = max(1, int(concurrency))
        self.explicit_generation = str(generation) if generation is not None else None
        self.covered = []
        # CRC32C of every disjoint piece already persisted to the part file,
        # keyed by ``[start, end)``. Keys never repeat: coverage only grows.
        self._segment_crc = {}
        self._lock = None
        self._part_fd = None
        self.credential = None
        self.target = None
        self.target_generation = None

    async def run(self):
        """Execute (or resume) the download; return the published local path."""
        self._lock = asyncio.Lock()
        credential = ResumableDownloadCredential.load(self.credential_path)
        self.target, self.target_generation = await self._resolve_target(credential)
        target_crc = self.target.get("crc32c")
        if not target_crc:
            raise ResumableDownloadError(
                f"{self.rpath}: object metadata has no crc32c; cannot validate "
                "a resumable download"
            )

        os.makedirs(os.path.dirname(self.lpath) or os.curdir, exist_ok=True)
        self._part_fd = os.open(
            self.part_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0), 0o600
        )
        try:
            os.ftruncate(self._part_fd, self.target["size"])
            self.covered = self._verify_local_segments(credential)
            self.credential = ResumableDownloadCredential(
                object_path=self.rpath,
                generation=self.target_generation,
                size=self.target["size"],
                crc32c=target_crc,
                part_path=self.part_path,
                segments=self._credential_segments(),
            )
            self._checkpoint()

            await self._fetch_missing()

            if missing_ranges(self.covered, self.target["size"]):
                raise ResumableDownloadError(
                    f"{self.rpath}: download ended with uncovered byte ranges"
                )
            self._validate_full_object(target_crc)
        except BaseException:
            if self._part_fd is not None:
                os.close(self._part_fd)
                self._part_fd = None
            raise

        os.close(self._part_fd)
        self._part_fd = None
        os.replace(self.part_path, self.lpath)
        try:
            os.remove(self.credential_path)
        except FileNotFoundError:
            pass
        return self.lpath

    async def _resolve_target(self, credential):
        """Pin the download to one object generation, following old credentials."""
        current = await self.fs._info(self.rpath)
        wanted = self.explicit_generation or (
            credential.generation if credential else None
        )
        current_generation = (
            str(current["generation"]) if current.get("generation") is not None else None
        )
        if wanted and wanted != current_generation:
            try:
                pinned = await self.fs._info(self.rpath, generation=wanted)
            except FileNotFoundError as exc:
                raise ObjectGenerationUnavailable(self.rpath, wanted) from exc
            return pinned, wanted
        return current, current_generation

    def _verify_local_segments(self, credential):
        """Keep only persisted intervals whose CRC32C still matches the credential."""
        if not credential or credential.size != self.target["size"]:
            return []
        if (
            credential.object_path != self.rpath
            or credential.part_path != self.part_path
        ):
            logger.warning(
                "Ignoring resume credential written for another object or part file"
            )
            return []
        recorded = {
            (start, end): segment_crc
            for start, end, segment_crc in credential.segments
        }
        trusted = []
        for start, end, segment_crc in credential.segments:
            if not segment_crc or end <= start or end > credential.size:
                logger.warning("Discarding malformed resume segment [%s, %s)", start, end)
                continue
            os.lseek(self._part_fd, start, os.SEEK_SET)
            data = os.read(self._part_fd, end - start)
            if len(data) != end - start:
                logger.warning("Discarding unpersisted resume segment [%s, %s)", start, end)
                continue
            hasher = Crc32c()
            hasher.update(data)
            if hasher.b64digest() != segment_crc:
                logger.warning("Discarding corrupted resume segment [%s, %s)", start, end)
                continue
            trusted.append((start, end))
        merged = merge_intervals(trusted)
        # Keep piece CRCs only where the on-disk range is still fully trusted.
        self._segment_crc = {
            (start, end): crc
            for (start, end), crc in recorded.items()
            if any(start >= low and end <= high for low, high in merged)
        }
        return merged

    async def _fetch_missing(self):
        # Recompute gaps after every pass: short or overlapping responses may
        # leave part of a scheduled gap for a later round.
        while True:
            gaps = []
            for start, end in missing_ranges(self.covered, self.target["size"]):
                for offset in range(start, end, self.chunk_size):
                    gaps.append((offset, min(offset + self.chunk_size, end)))
            if not gaps:
                return

            covered_before = sum(end - start for start, end in self.covered)
            semaphore = asyncio.Semaphore(self.concurrency)

            async def worker(gap_start, gap_end):
                async with semaphore:
                    fragment_start, data = await self._fetch_range(gap_start, gap_end)
                    await self._absorb(fragment_start, data)

            tasks = [
                asyncio.ensure_future(worker(start, end)) for start, end in gaps
            ]
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

            covered_after = sum(end - start for start, end in self.covered)
            if covered_after <= covered_before:
                raise ResumableDownloadError(
                    f"{self.rpath}: range responses made no download progress"
                )

    async def _fetch_range(self, start, end):
        await self.fs._set_session()
        url = self.fs.url(self.rpath, generation=self.target_generation)
        range_header = {
            "Range": await self.fs._process_limits(self.rpath, start, end)
        }
        headers = self.fs._get_headers(range_header)
        params = self.fs._get_params({})
        for attempt in range(_MAX_GENERATION_MISMATCH_RETRIES):
            async with self.fs.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.fs.requests_timeout,
            ) as response:
                if response.status >= 400:
                    body = await response.read()
                    if response.status == 404:
                        raise ObjectGenerationUnavailable(
                            self.rpath, self.target_generation
                        )
                    validate_response(response.status, body, url)
                response_generation = response.headers.get("x-goog-generation")
                if (
                    self.target_generation
                    and response_generation is not None
                    and str(response_generation) != str(self.target_generation)
                ):
                    await response.read()
                    logger.warning(
                        "Rejecting range [%s, %s) from generation %s (want %s)",
                        start,
                        end,
                        response_generation,
                        self.target_generation,
                    )
                    continue
                data = await response.read()
                actual_start = _parse_content_range(
                    response.headers.get("Content-Range"), start
                )
                return actual_start, data
        raise GenerationMismatchResponseError(
            f"{self.rpath}: range [{start}, {end}) kept returning a different "
            f"generation than {self.target_generation}"
        )

    async def _absorb(self, start, data):
        end = start + len(data)
        async with self._lock:
            for piece_start, piece_end in uncovered_pieces((start, end), self.covered):
                piece = data[piece_start - start : piece_end - start]
                self._pwrite_all(piece, piece_start)
                hasher = Crc32c()
                hasher.update(piece)
                self._segment_crc[(piece_start, piece_end)] = hasher.b64digest()
            os.fsync(self._part_fd)
            self.covered = merge_intervals(self.covered + [(start, end)])
            self._checkpoint()

    def _pwrite_all(self, data, offset):
        written = 0
        view = memoryview(data)
        while written < len(view):
            written += os.pwrite(self._part_fd, view[written:], offset + written)

    def _credential_segments(self):
        # Written pieces are disjoint by construction; keep them separate so
        # each one keeps its own CRC32C instead of merging away the lookup keys.
        return [
            (start, end, self._segment_crc[(start, end)])
            for start, end in sorted(self._segment_crc)
        ]

    def _checkpoint(self):
        self.credential.segments = self._credential_segments()
        self.credential.save(self.credential_path)

    def _validate_full_object(self, expected_crc):
        hasher = Crc32c()
        os.lseek(self._part_fd, 0, os.SEEK_SET)
        while True:
            block = os.read(self._part_fd, 1024 * 1024)
            if not block:
                break
            hasher.update(block)
        actual_crc = hasher.b64digest()
        if actual_crc != expected_crc:
            raise ChecksumError(
                f"{self.rpath}: whole-object CRC32C mismatch "
                f"(expected {expected_crc}, got {actual_crc}); publish path untouched"
            )
