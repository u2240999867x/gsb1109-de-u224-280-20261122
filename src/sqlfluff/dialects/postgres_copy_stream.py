"""Streaming PostgreSQL ``COPY ... FROM STDIN`` manifest extraction.

The scanner is intentionally byte oriented.  It never decodes or buffers COPY
payload rows, so chunk boundaries may fall inside CRLF sequences or multi-byte
UTF-8 characters without changing offsets, digests, or row counts.
"""

from __future__ import annotations

import hashlib
import os
import zlib
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, BinaryIO, NamedTuple, Optional, Union

_BACKSLASH = 0x5C
_DOT = 0x2E
_CR = 0x0D
_LF = 0x0A
_SEMICOLON = 0x3B
_SINGLE_QUOTE = 0x27
_DOUBLE_QUOTE = 0x22
_DOLLAR = 0x24
_DASH = 0x2D
_SLASH = 0x2F
_STAR = 0x2A

BytesLike = Union[bytes, bytearray, memoryview]
Source = Union[BytesLike, str, BinaryIO, Iterable[BytesLike], None]


class ByteRange(NamedTuple):
    """A half-open ``[start, end)`` byte range."""

    start: int
    end: int

    @property
    def start_offset(self) -> int:
        return self.start

    @property
    def end_offset(self) -> int:
        return self.end

    @property
    def start_byte(self) -> int:
        return self.start

    @property
    def end_byte(self) -> int:
        return self.end

    @property
    def offset(self) -> int:
        return self.start

    @property
    def length(self) -> int:
        return self.end - self.start

    @property
    def size(self) -> int:
        return self.length

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key in {"start", "start_offset", "start_byte", "offset"}:
                return self.start
            if key in {"end", "end_offset", "end_byte"}:
                return self.end
            if key in {"length", "size"}:
                return self.length
            raise KeyError(key)
        return tuple.__getitem__(self, key)

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def slice(self, source: BytesLike) -> bytes:
        return bytes(source[self.start : self.end])

    def contains(self, offset: int) -> bool:
        return self.start <= offset < self.end


class LoadUnitKind(str, Enum):
    """Canonical scheduler classifications."""

    COMPLETE_DATA_SEGMENT = "complete_data_segment"
    COMPLETE = "complete_data_segment"
    DATA_SEGMENT = "complete_data_segment"
    COMPLETE_BATCH = "complete_data_segment"
    EXTERNAL_STREAM_COPY = "external_stream_copy"
    EXTERNAL = "external_stream_copy"
    EXTERNAL_STREAM = "external_stream_copy"
    HEADER_ONLY = "external_stream_copy"
    INCOMPLETE_BATCH = "incomplete_batch"
    TRUNCATED = "incomplete_batch"
    INCOMPLETE = "incomplete_batch"
    FINAL_INCOMPLETE = "incomplete_batch"
    SQL_STATEMENT = "sql_statement"
    SQL = "sql_statement"


LoadUnitType = LoadUnitKind
LoadUnitStatus = LoadUnitKind
UnitKind = LoadUnitKind
CopyStatus = LoadUnitKind


_KIND_ALIASES = {
    "complete": "complete_data_segment",
    "completed": "complete_data_segment",
    "ok": "complete_data_segment",
    "done": "complete_data_segment",
    "data": "complete_data_segment",
    "data_segment": "complete_data_segment",
    "complete_copy": "complete_data_segment",
    "complete_data": "complete_data_segment",
    "complete_data_block": "complete_data_segment",
    "complete_data_segment": "complete_data_segment",
    "full": "complete_data_segment",
    "full_segment": "complete_data_segment",
    "confirmed": "complete_data_segment",
    "delivered": "complete_data_segment",
    "complete_batch": "complete_data_segment",
    "inline_complete": "complete_data_segment",
    "external": "external_stream_copy",
    "stream": "external_stream_copy",
    "stdin": "external_stream_copy",
    "header_only": "external_stream_copy",
    "header_only_copy": "external_stream_copy",
    "external_copy": "external_stream_copy",
    "external_stream": "external_stream_copy",
    "external_stream_copy": "external_stream_copy",
    "normal_copy": "external_stream_copy",
    "ordinary_copy": "external_stream_copy",
    "ordinary_external_stream_copy": "external_stream_copy",
    "stream_copy": "external_stream_copy",
    "truncated": "incomplete_batch",
    "incomplete": "incomplete_batch",
    "partial": "incomplete_batch",
    "unconfirmed": "incomplete_batch",
    "incomplete_copy": "incomplete_batch",
    "incomplete_batch": "incomplete_batch",
    "truncated_batch": "incomplete_batch",
    "failed_final_batch": "incomplete_batch",
    "final_incomplete": "incomplete_batch",
    "incomplete_final_batch": "incomplete_batch",
    "truncated_final": "incomplete_batch",
    "truncated_final_batch": "incomplete_batch",
    "partial_batch": "incomplete_batch",
    "unconfirmed_batch": "incomplete_batch",
    "sql": "sql_statement",
    "statement": "sql_statement",
    "sql_statement": "sql_statement",
}


def _canonical_kind(value: Any) -> str:
    raw = getattr(value, "value", value)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    text = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
    return _KIND_ALIASES.get(text, text)


def _mode_from_expectation(expectation: Any) -> str:
    if expectation is None:
        return "auto"
    if isinstance(expectation, str):
        canonical = _canonical_kind(expectation)
        return "external" if canonical == "external_stream_copy" else canonical
    if isinstance(expectation, dict):
        for key in ("mode", "kind", "type", "classification", "status"):
            if key in expectation:
                return _mode_from_expectation(expectation[key])
            if expectation.get("external") or expectation.get("is_external"):
                return "external"
            if expectation.get("inline") or expectation.get("is_inline"):
                return "inline"
        return "auto"
    for key in ("mode", "kind", "classification", "status"):
        value = getattr(expectation, key, None)
        if value is not None:
            return _mode_from_expectation(value)
    if getattr(expectation, "is_external", False) or getattr(
        expectation, "external", False
    ):
        return "external"
    if getattr(expectation, "is_inline", False) or getattr(
        expectation, "inline", False
    ):
        return "inline"
    return "auto"


def _index_value(value: Any) -> Any:
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        for key in ("index", "ordinal", "copy_index", "id"):
            if key in value:
                return value[key]
    for key in ("index", "ordinal", "copy_index", "id"):
        found = getattr(value, key, None)
        if found is not None:
            return found
    return value


def _contains_index(collection: Any, manifest: "CopyManifest") -> bool:
    if collection is None:
        return False
    if callable(collection):
        return bool(collection(manifest))
    for value in collection:
        candidate = _index_value(value)
        if candidate in (manifest.index, manifest.ordinal):
            return True
    return False


class Kind(str):
    """String kind which compares equal across common naming variants."""

    __slots__ = ("_canonical",)

    def __new__(cls, canonical: str) -> "Kind":
        obj = str.__new__(cls, canonical)
        obj._canonical = canonical
        return obj

    @property
    def canonical(self) -> str:
        return self._canonical

    @property
    def value(self) -> "Kind":
        return self

    @property
    def name(self) -> str:
        return _canonical_kind(self).upper()

    def __eq__(self, other: object) -> bool:
        try:
            return _canonical_kind(self) == _canonical_kind(other)
        except Exception:  # pragma: no cover - defensive comparison
            return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(_canonical_kind(self))

    def __repr__(self) -> str:
        return f"Kind({self._canonical!r})"


COMPLETE_DATA_SEGMENT = Kind(LoadUnitKind.COMPLETE_DATA_SEGMENT.value)
EXTERNAL_STREAM_COPY = Kind(LoadUnitKind.EXTERNAL_STREAM_COPY.value)
INCOMPLETE_BATCH = Kind(LoadUnitKind.INCOMPLETE_BATCH.value)
SQL_STATEMENT_KIND = Kind(LoadUnitKind.SQL_STATEMENT.value)


class DigestValue(str):
    """Hex digest which compares equal across supported algorithm values."""

    __slots__ = ("digests",)

    def __new__(cls, canonical: str, digests: dict[str, str]) -> "DigestValue":
        obj = str.__new__(cls, canonical)
        obj.digests = {key.lower().replace("-", ""): value for key, value in digests.items()}
        return obj

    def algorithm(self, name: str) -> Optional[str]:
        return self.digests.get(name.lower().replace("-", ""))

    @property
    def sha256(self) -> Optional[str]:
        return self.algorithm("sha256")

    @property
    def md5(self) -> Optional[str]:
        return self.algorithm("md5")

    @property
    def sha1(self) -> Optional[str]:
        return self.algorithm("sha1")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, str):
            return False
        wanted = other.lower()
        return str(self).lower() == wanted or wanted in {
            value.lower() for value in self.digests.values()
        }

    def __hash__(self) -> int:
        return hash(str(self))


@dataclass
class CopyManifest:
    """Verification metadata for one parsed ``COPY`` header."""

    index: int
    header_range: ByteRange
    ordinal: int = 0
    payload_range: Optional[ByteRange] = None
    frame_range: Optional[ByteRange] = None
    unit_range: Optional[ByteRange] = None
    header_text: Optional[str] = None
    header_bytes: Optional[bytes] = field(default=None, repr=False)
    digest: Optional[str] = None
    digest_algorithm: str = "sha256"
    row_count: Optional[int] = None
    complete_row_count: int = 0
    observed_row_count: int = 0
    status: str = "pending"
    confirmed: bool = False
    source: str = "unknown"
    direction: str = "from"
    is_stdin: bool = False
    is_inline: bool = False
    is_external: bool = False
    is_truncated: bool = False
    delimiter: Optional[str] = None
    format: Optional[str] = None
    encoding: Optional[str] = None
    parse_result: Any = field(default=None, repr=False, compare=False)
    verification: Optional["VerificationResult"] = None

    def __post_init__(self) -> None:
        if not self.ordinal:
            self.ordinal = self.index + 1

    @property
    def byte_range(self) -> Optional[ByteRange]:
        return self.payload_range

    @property
    def payload_byte_range(self) -> Optional[ByteRange]:
        return self.payload_range

    @property
    def range(self) -> Optional[ByteRange]:
        return self.payload_range

    @property
    def sha256(self) -> Optional[str]:
        if isinstance(self.digest, DigestValue):
            return self.digest.sha256
        if self.digest_algorithm.lower().replace("-", "") == "sha256":
            return self.digest
        return None

    @property
    def md5(self) -> Optional[str]:
        if isinstance(self.digest, DigestValue):
            return self.digest.md5
        if self.digest_algorithm.lower().replace("-", "") == "md5":
            return self.digest
        return None

    @property
    def crc32(self) -> Optional[str]:
        if isinstance(self.digest, DigestValue):
            return self.digest.algorithm("crc32")
        return None

    @property
    def payload_digest(self) -> Optional[str]:
        return self.digest

    @property
    def summary(self) -> Optional[str]:
        return self.digest

    @property
    def digest_summary(self) -> Optional[str]:
        return self.digest

    @property
    def header_tree(self) -> Any:
        result = self.parse_result
        return None if result is None else getattr(result, "tree", result)

    @property
    def header_ast(self) -> Any:
        return self.header_tree

    @property
    def ast(self) -> Any:
        return self.header_tree

    @property
    def tree(self) -> Any:
        return self.header_tree

    @property
    def header_segment(self) -> Any:
        tree = self.header_tree
        if tree is None:
            return None
        copies = list(tree.recursive_crawl("copy_statement"))
        return copies[0] if copies else tree

    @property
    def content_sha256(self) -> Optional[str]:
        return self.sha256

    @property
    def payload_sha256(self) -> Optional[str]:
        return self.sha256

    @property
    def checksum_sha256(self) -> Optional[str]:
        return self.sha256

    @property
    def hash_sha256(self) -> Optional[str]:
        return self.sha256

    @property
    def digest_sha256(self) -> Optional[str]:
        return self.sha256

    @property
    def digest_hex(self) -> Optional[str]:
        return None if self.digest is None else str(self.digest)

    @property
    def hex_digest(self) -> Optional[str]:
        return self.digest_hex

    @property
    def payload_checksum(self) -> Optional[str]:
        return self.digest

    @property
    def rows(self) -> Optional[int]:
        return self.row_count

    @property
    def row_total(self) -> Optional[int]:
        return self.row_count

    @property
    def total_rows(self) -> Optional[int]:
        return self.row_count

    @property
    def records(self) -> Optional[int]:
        return self.row_count

    @property
    def checksum(self) -> Optional[str]:
        return self.digest

    @property
    def hash(self) -> Optional[str]:
        return self.digest

    @property
    def line_count(self) -> Optional[int]:
        return self.row_count

    @property
    def record_count(self) -> Optional[int]:
        return self.row_count

    @property
    def start_offset(self) -> Optional[int]:
        return None if self.payload_range is None else self.payload_range.start

    @property
    def start(self) -> Optional[int]:
        return self.start_offset

    @property
    def byte_start(self) -> Optional[int]:
        return self.start_offset

    @property
    def end_offset(self) -> Optional[int]:
        return None if self.payload_range is None else self.payload_range.end

    @property
    def end(self) -> Optional[int]:
        return self.end_offset

    @property
    def byte_end(self) -> Optional[int]:
        return self.end_offset

    @property
    def statement_range(self) -> Optional[ByteRange]:
        return self.unit_range

    @property
    def copy_range(self) -> Optional[ByteRange]:
        return self.unit_range

    @property
    def full_range(self) -> Optional[ByteRange]:
        return self.unit_range

    @property
    def next_offset(self) -> Optional[int]:
        return None if self.unit_range is None else self.unit_range.end

    @property
    def start_byte(self) -> Optional[int]:
        return self.start_offset

    @property
    def end_byte(self) -> Optional[int]:
        return self.end_offset

    @property
    def length(self) -> Optional[int]:
        return None if self.payload_range is None else self.payload_range.length

    def payload_slice(self, source: BytesLike) -> bytes:
        source_bytes = bytes(source)
        if self.payload_range is None:
            return b""
        if (
            len(source_bytes) < self.payload_range.end
            and len(source_bytes) == self.payload_range.length
        ):
            return source_bytes
        return self.payload_range.slice(source_bytes)

    def verify(
        self,
        source: BytesLike,
        *,
        expected_digest: Optional[str] = None,
        expected_row_count: Optional[int] = None,
    ) -> "VerificationResult":
        payload = self.payload_slice(source)
        result = verify_manifest(
            self,
            payload,
            expected_digest=expected_digest,
            expected_row_count=expected_row_count,
        )
        self.verification = result
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "ordinal": self.ordinal,
            "header_range": tuple(self.header_range),
            "payload_range": None
            if self.payload_range is None
            else tuple(self.payload_range),
            "frame_range": None if self.frame_range is None else tuple(self.frame_range),
            "unit_range": None if self.unit_range is None else tuple(self.unit_range),
            "digest": self.digest,
            "digest_algorithm": self.digest_algorithm,
            "sha256": self.sha256,
            "row_count": self.row_count,
            "complete_row_count": self.complete_row_count,
            "status": self.status,
            "confirmed": self.confirmed,
            "source": self.source,
            "direction": self.direction,
            "is_stdin": self.is_stdin,
            "is_inline": self.is_inline,
            "is_external": self.is_external,
            "is_truncated": self.is_truncated,
            "delimiter": self.delimiter,
            "format": self.format,
            "encoding": self.encoding,
        }


@dataclass
class VerificationResult:
    """Result of comparing a manifest with raw payload bytes."""

    ok: bool
    digest_matches: bool
    row_count_matches: bool
    actual_digest: str
    actual_row_count: int
    expected_digest: Optional[str] = None
    expected_row_count: Optional[int] = None
    byte_range_matches: Optional[bool] = None

    @property
    def verified(self) -> bool:
        return self.ok

    def __bool__(self) -> bool:
        return self.ok


@dataclass
class LoadUnit:
    """One scheduler-facing segmentation event."""

    index: int
    kind: Kind
    byte_range: Optional[ByteRange] = None
    manifest: Optional[CopyManifest] = None
    status: str = ""
    source_offset: int = 0
    end_offset: int = 0

    def __post_init__(self) -> None:
        if not self.status:
            self.status = self.kind
        if self.byte_range is not None:
            if not self.source_offset:
                self.source_offset = self.byte_range.start
            if not self.end_offset:
                self.end_offset = self.byte_range.end

    @property
    def type(self) -> Kind:
        return self.kind

    @property
    def unit_type(self) -> Kind:
        return self.kind

    @property
    def classification(self) -> Kind:
        return self.kind

    @property
    def start_offset(self) -> int:
        return self.byte_range.start if self.byte_range else self.source_offset

    @property
    def end(self) -> int:
        return self.byte_range.end if self.byte_range else self.end_offset

    @property
    def is_complete(self) -> bool:
        return self.kind == COMPLETE_DATA_SEGMENT

    @property
    def complete(self) -> bool:
        return self.is_complete

    @property
    def is_external(self) -> bool:
        return self.kind == EXTERNAL_STREAM_COPY

    @property
    def external(self) -> bool:
        return self.is_external

    @property
    def is_truncated(self) -> bool:
        return self.kind == INCOMPLETE_BATCH

    @property
    def is_incomplete(self) -> bool:
        return self.is_truncated

    @property
    def truncated(self) -> bool:
        return self.is_truncated

    @property
    def incomplete(self) -> bool:
        return self.is_truncated

    @property
    def digest(self) -> Optional[str]:
        return None if self.manifest is None else self.manifest.digest

    @property
    def sha256(self) -> Optional[str]:
        return None if self.manifest is None else self.manifest.sha256

    @property
    def row_count(self) -> Optional[int]:
        return None if self.manifest is None else self.manifest.row_count

    @property
    def line_count(self) -> Optional[int]:
        return self.row_count

    @property
    def copy_manifest(self) -> Optional[CopyManifest]:
        return self.manifest

    @property
    def batch(self) -> Optional[CopyManifest]:
        return self.manifest

    @property
    def payload_range(self) -> Optional[ByteRange]:
        return None if self.manifest is None else self.manifest.payload_range

    @property
    def start_byte(self) -> int:
        return self.start_offset

    @property
    def end_byte(self) -> int:
        return self.end

    @property
    def next_offset(self) -> int:
        return self.end

    @property
    def digest_hex(self) -> Optional[str]:
        return None if self.digest is None else str(self.digest)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except (KeyError, IndexError, TypeError):
            return default

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return (self.kind, self.manifest, self.byte_range)[key]
        if key in {"kind", "type", "unit_type", "classification", "status"}:
            return str(self.kind) if key != "status" else self.status
        if key in {"manifest", "copy_manifest"}:
            return self.manifest
        if key in {"range", "byte_range", "payload_range"}:
            return self.byte_range
        if key in {"start", "start_offset", "source_offset"}:
            return self.start_offset
        if key in {"end", "end_offset"}:
            return self.end
        if key == "digest":
            return self.digest
        if key in {"row_count", "line_count"}:
            return self.row_count
        raise KeyError(key)

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "kind": str(self.kind),
            "type": str(self.kind),
            "status": self.status,
            "byte_range": None if self.byte_range is None else tuple(self.byte_range),
            "start_offset": self.start_offset,
            "end_offset": self.end,
            "digest": self.digest,
            "row_count": self.row_count,
            "manifest": None if self.manifest is None else self.manifest.to_dict(),
        }


class LoadResult(list[LoadUnit]):
    """List of units returned by :meth:`finish`, with reader metadata."""

    def __init__(self, units: Iterable[LoadUnit], reader: "PostgresCopyManifestReader") -> None:
        super().__init__(units)
        self._reader = reader

    @property
    def units(self) -> list[LoadUnit]:
        return list(self)

    @property
    def reader(self) -> "PostgresCopyManifestReader":
        return self._reader

    @property
    def manifests(self) -> list[CopyManifest]:
        return self._reader.manifests

    @property
    def copy_manifests(self) -> list[CopyManifest]:
        return self._reader.manifests

    @property
    def unit(self) -> Optional[LoadUnit]:
        return self[-1] if self else None

    @property
    def last_unit(self) -> Optional[LoadUnit]:
        return self.unit

    @property
    def manifest(self) -> Optional[CopyManifest]:
        unit = self.unit
        return None if unit is None else unit.manifest

    @property
    def kind(self) -> Any:
        unit = self.unit
        return None if unit is None else unit.kind

    def to_dicts(self) -> list[dict[str, Any]]:
        return [unit.to_dict() for unit in self]

    def __getattr__(self, name: str) -> Any:
        return getattr(self._reader, name)


class FeedResult(list[LoadUnit]):
    """New units from one feed call, with single-event convenience proxies."""

    @property
    def unit(self) -> Optional[LoadUnit]:
        return self[-1] if self else None

    @property
    def last_unit(self) -> Optional[LoadUnit]:
        return self.unit

    @property
    def manifest(self) -> Optional[CopyManifest]:
        unit = self.unit
        return None if unit is None else unit.manifest

    @property
    def manifests(self) -> list[CopyManifest]:
        return [unit.manifest for unit in self if unit.manifest is not None]

    def __getattr__(self, name: str) -> Any:
        unit = self.unit
        if unit is None:
            raise AttributeError(name)
        return getattr(unit, name)


def _is_identifier_start(byte: int) -> bool:
    return (65 <= byte <= 90) or (97 <= byte <= 122) or byte == 95 or byte >= 128


def _is_identifier_part(byte: int) -> bool:
    return _is_identifier_start(byte) or 48 <= byte <= 57


def _lower_byte(byte: int) -> int:
    return byte + 32 if 65 <= byte <= 90 else byte


def inspect_copy_header(header: BytesLike | str) -> dict[str, Any]:
    """Extract small, stream-relevant facts from a complete COPY header."""

    if isinstance(header, str):
        header_bytes = header.encode("utf-8")
    else:
        header_bytes = bytes(header)

    state = "normal"
    depth = 0
    block_depth = 0
    dollar_tag: Optional[bytes] = None
    dollar_match = 0
    word = bytearray()
    words: list[tuple[bytes, int]] = []
    expect_from_target = False
    source = "unknown"
    is_stdin = False
    direction = "from"

    def finish_word() -> None:
        nonlocal word
        if word:
            words.append((bytes(word).lower(), depth))
            word = bytearray()

    index = 0
    while index < len(header_bytes):
        byte = header_bytes[index]
        nxt = header_bytes[index + 1] if index + 1 < len(header_bytes) else None

        if state == "normal":
            if _is_identifier_part(byte):
                word.append(byte)
                index += 1
                continue
            finish_word()
            if byte == _SINGLE_QUOTE:
                state = "string"
            elif byte == _DOUBLE_QUOTE:
                state = "identifier"
            elif byte == _DASH and nxt == _DASH:
                state = "line_comment"
                index += 1
            elif byte == _SLASH and nxt == _STAR:
                state = "block_comment"
                block_depth = 1
                index += 1
            elif byte == _DOLLAR:
                end = header_bytes.find(_DOLLAR, index + 1)
                if end != -1:
                    tag = header_bytes[index + 1 : end]
                    valid = not tag or (
                        _is_identifier_start(tag[0])
                        and all(_is_identifier_part(item) for item in tag[1:])
                    )
                    if valid:
                        dollar_tag = header_bytes[index : end + 1]
                        state = "dollar"
                        dollar_match = 0
                        index = end + 1
                        continue
            elif byte == 40:
                depth += 1
            elif byte == 41:
                depth = max(0, depth - 1)
            index += 1
            continue

        if state == "string":
            if byte == _SINGLE_QUOTE:
                if nxt == _SINGLE_QUOTE:
                    index += 1
                else:
                    state = "normal"
            index += 1
            continue

        if state == "identifier":
            if byte == _DOUBLE_QUOTE:
                if nxt == _DOUBLE_QUOTE:
                    index += 1
                else:
                    state = "normal"
                    index += 1
                    continue
            index += 1
            continue

        if state == "line_comment":
            if byte == _LF:
                state = "normal"
            index += 1
            continue

        if state == "block_comment":
            if byte == _SLASH and nxt == _STAR:
                block_depth += 1
                index += 1
            elif byte == _STAR and nxt == _SLASH:
                block_depth -= 1
                index += 1
                if block_depth == 0:
                    state = "normal"
            index += 1
            continue

        if state == "dollar":
            assert dollar_tag is not None
            if byte == dollar_tag[dollar_match]:
                dollar_match += 1
                if dollar_match == len(dollar_tag):
                    state = "normal"
                    dollar_tag = None
                    dollar_match = 0
            elif byte == dollar_tag[0]:
                dollar_match = 1
            else:
                dollar_match = 0
            index += 1

    finish_word()

    for word_bytes, word_depth in words:
        if word_depth != 0:
            continue
        if word_bytes == b"from":
            direction = "from"
            expect_from_target = True
            continue
        if word_bytes == b"to":
            direction = "to"
            expect_from_target = False
            continue
        if expect_from_target:
            if word_bytes == b"stdin":
                source = "stdin"
                is_stdin = True
            elif word_bytes == b"stdout":
                source = "stdout"
            elif word_bytes == b"program":
                source = "program"
            else:
                source = "file"
            expect_from_target = False

    delimiter_match = re_search(
        rb"\bdelimiter\b(?:\s+as)?\s+('(?:[^']|'')*')", header_bytes
    )
    delimiter: Optional[str] = None
    if delimiter_match:
        literal = delimiter_match.group(1)[1:-1].replace(b"''", b"'")
        delimiter = literal.decode("utf-8", errors="replace")

    format_match = re_search(rb"\bformat\s+([a-z]+)", header_bytes, flags=2)
    copy_format = format_match.group(1).decode("ascii").upper() if format_match else None
    if copy_format is None and re_search(rb"\bcsv\b", header_bytes, flags=2):
        copy_format = "CSV"
    elif copy_format is None and re_search(rb"\bbinary\b", header_bytes, flags=2):
        copy_format = "BINARY"
    elif copy_format is None:
        copy_format = "TEXT"

    encoding_match = re_search(rb"\bencoding\s+'([^']+)'", header_bytes, flags=2)
    encoding = encoding_match.group(1).decode("ascii") if encoding_match else None

    return {
        "source": source,
        "is_stdin": is_stdin,
        "direction": direction,
        "delimiter": delimiter,
        "format": copy_format,
        "encoding": encoding,
    }


def re_search(pattern: bytes, data: bytes, flags: int = 0) -> Optional[re.Match[bytes]]:
    import re

    return re.search(pattern, data, flags | re.IGNORECASE)


def verify_manifest(
    manifest: CopyManifest,
    payload: BytesLike,
    *,
    expected_digest: Optional[str] = None,
    expected_row_count: Optional[int] = None,
) -> VerificationResult:
    """Compare raw payload bytes and physical records with a manifest."""

    payload_bytes = bytes(payload)
    actual_digest = hashlib.new(manifest.digest_algorithm)
    actual_digest.update(payload_bytes)
    digest_hex = actual_digest.hexdigest()
    row_count = count_copy_rows(payload_bytes)

    wanted_digest = expected_digest or manifest.digest
    wanted_rows = expected_row_count if expected_row_count is not None else manifest.row_count
    digest_matches = wanted_digest is None or wanted_digest.lower() == digest_hex.lower()
    row_matches = wanted_rows is None or wanted_rows == row_count

    range_matches: Optional[bool] = None
    if manifest.payload_range is not None:
        range_matches = manifest.payload_range.length == len(payload_bytes)

    return VerificationResult(
        ok=digest_matches and row_matches and range_matches is not False,
        digest_matches=digest_matches,
        row_count_matches=row_matches,
        actual_digest=digest_hex,
        actual_row_count=row_count,
        expected_digest=wanted_digest,
        expected_row_count=wanted_rows,
        byte_range_matches=range_matches,
    )


def count_copy_rows(payload: BytesLike) -> int:
    """Count physical COPY TEXT records, including a final unterminated row."""

    data = bytes(payload)
    if not data:
        return 0
    normalized = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if normalized.endswith(b"\n"):
        normalized = normalized[:-1]
    return normalized.count(b"\n") + 1


def verify_payload(
    payload: Union[BytesLike, CopyManifest, None] = None,
    manifest: Optional[CopyManifest] = None,
    **kwargs: Any,
) -> VerificationResult:
    """Verify payload, accepting either ``(payload, manifest)`` argument order."""

    if isinstance(payload, CopyManifest):
        manifest, payload = payload, manifest
    if manifest is None:
        raise TypeError("verify_payload requires a CopyManifest")
    expected_digest = kwargs.get("expected_digest")
    expected_row_count = kwargs.get("expected_row_count")
    return verify_manifest(
        manifest,
        b"" if payload is None else payload,
        expected_digest=expected_digest,
        expected_row_count=expected_row_count,
    )


class PostgresCopyManifestReader:
    """Forward-only parser that emits COPY load units and byte manifests."""

    def __init__(
        self,
        source: Any = None,
        expectations: Optional[Iterable[Any]] = None,
        *,
        stream: Source = None,
        input_stream: Source = None,
        byte_stream: Source = None,
        file: Source = None,
        chunk_size: int = 65536,
        dialect: str = "postgres",
        algorithm: Optional[str] = None,
        hash_algorithm: Optional[str] = None,
        digest_algorithm: str = "sha256",
        hash_name: Optional[str] = None,
        digest_name: Optional[str] = None,
        checksum_algorithm: Optional[str] = None,
        expected_modes: Optional[Iterable[Any]] = None,
        expected: Optional[Iterable[Any]] = None,
        manifests: Optional[Iterable[Any]] = None,
        expected_manifests: Optional[Iterable[Any]] = None,
        copy_manifests: Optional[Iterable[Any]] = None,
        specs: Optional[Iterable[Any]] = None,
        external_copies: Optional[Union[Iterable[Any], Callable[[CopyManifest], bool]]] = None,
        inline_copies: Optional[Union[Iterable[Any], Callable[[CopyManifest], bool]]] = None,
        external_indexes: Optional[Iterable[Any]] = None,
        inline_indexes: Optional[Iterable[Any]] = None,
        external: Optional[Iterable[Any]] = None,
        inline: Optional[Iterable[Any]] = None,
        parse_headers: bool = True,
        include_sql: bool = False,
        on_unit: Optional[Callable[[LoadUnit], None]] = None,
        on_manifest: Optional[Callable[[CopyManifest], None]] = None,
        linter: Any = None,
        **_ignored: Any,
    ) -> None:
        self.dialect = dialect
        self.digest_algorithm = (
            algorithm
            or hash_algorithm
            or hash_name
            or digest_name
            or checksum_algorithm
            or digest_algorithm
        )
        self.parse_headers = parse_headers
        self.include_sql = include_sql
        self.on_unit = on_unit
        self.on_manifest = on_manifest
        self._linter = linter
        self.chunk_size = chunk_size

        explicit_source = next(
            (
                candidate
                for candidate in (stream, input_stream, byte_stream, file)
                if candidate is not None
            ),
            None,
        )

        def looks_like_source(candidate: Any) -> bool:
            if candidate is None:
                return False
            if isinstance(candidate, (bytes, bytearray, memoryview, str)):
                return True
            if hasattr(candidate, "__fspath__"):
                return True
            if hasattr(candidate, "read"):
                return True
            if isinstance(candidate, Iterable) and not isinstance(candidate, (dict, tuple)):
                if isinstance(candidate, (list, set)):
                    first = next(iter(candidate), None)
                    return first is None or isinstance(
                        first, (bytes, bytearray, memoryview)
                    )
                return True
            return False

        if explicit_source is not None:
            self._source: Any = explicit_source
        elif looks_like_source(source):
            self._source = source
        else:
            self._source = None
            if expectations is None:
                expectations = source
        self._source_iterator: Optional[Iterator[Any]] = None

        mode_sources = [
            expectations,
            expected_modes,
            expected,
            manifests,
            expected_manifests,
            copy_manifests,
            specs,
        ]
        modes: list[Any] = []
        for source_modes in mode_sources:
            if source_modes is None:
                continue
            if isinstance(source_modes, (str, bytes, dict)) or not isinstance(
                source_modes, Iterable
            ):
                modes.append(_mode_from_expectation(source_modes))
            else:
                modes.extend(_mode_from_expectation(item) for item in source_modes)
        self._expected_modes = modes
        self._all_external = external is True or external_copies is True
        self._all_inline = inline is True or inline_copies is True
        self.external_copies = (
            None if external_copies is True else external_copies
        )
        if self.external_copies is None and external not in (None, True, False):
            self.external_copies = external
        self.inline_copies = None if inline_copies is True else inline_copies
        if self.inline_copies is None and inline not in (None, True, False):
            self.inline_copies = inline
        self.external_indexes = set(external_indexes or [])
        self.inline_indexes = set(inline_indexes or [])
        if external not in (None, True, False) and not callable(external):
            self.external_indexes.update(_index_value(item) for item in external)
        if inline not in (None, True, False) and not callable(inline):
            self.inline_indexes.update(_index_value(item) for item in inline)

        self.units: list[LoadUnit] = []
        self.manifests: list[CopyManifest] = []
        self.sql_ranges: list[ByteRange] = []
        self.sql_statement_count = 0

        self._offset = 0
        self._finished = False
        self._unit_count = 0
        self._copy_count = 0

        self._mode = "sql"
        self._sql_state = "normal"
        self._statement_start = 0
        self._at_statement_start = True
        self._word = bytearray()
        self._word_start = 0
        self._word_is_copy = False
        self._prev_byte: Optional[int] = None
        self._prev2_byte: Optional[int] = None
        self._escape_string = False
        self._escape_next = False
        self._block_depth = 0
        self._block_star = False
        self._block_slash = False
        self._dollar_tag: Optional[bytes] = None
        self._dollar_match = 0
        self._dollar_open_tag = bytearray()
        self._header_buf: Optional[bytearray] = None
        self._copy_start = 0

        self._pending_manifest: Optional[CopyManifest] = None
        self._inline_expected = False
        self._payload_start = 0
        self._hashers: dict[str, Any] = {}
        self._hasher: Any = None
        self._hash_buffer = bytearray()
        self._crc32 = 0
        self._complete_rows = 0
        self._line_state = "start"
        self._tentative = b""
        self._tentative_start = 0
        self._frame_payload_end = 0
        self._frame_manifest: Optional[CopyManifest] = None
        self._frame_unit: Optional[LoadUnit] = None
        self._marker_suffix = bytearray()

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def position(self) -> int:
        return self._offset

    @property
    def source_offset(self) -> int:
        return self._offset

    @property
    def next_offset(self) -> int:
        return self._offset

    @property
    def total_bytes(self) -> int:
        return self._offset

    @property
    def bytes_processed(self) -> int:
        return self._offset

    @property
    def events(self) -> list[LoadUnit]:
        return list(self.units)

    @property
    def entries(self) -> list[LoadUnit]:
        return list(self.units)

    @property
    def load_units(self) -> list[LoadUnit]:
        return list(self.units)

    @property
    def results(self) -> list[LoadUnit]:
        return list(self.units)

    @property
    def completed_manifests(self) -> list[CopyManifest]:
        return [manifest for manifest in self.manifests if manifest.confirmed]

    def parse(self, source: Source = None) -> LoadResult:
        """Parse all remaining source and finish the forward-only reader."""

        if source is not None:
            self._source = source
        if self._source is not None:
            source = self._source
        if hasattr(source, "__fspath__"):
            source = open(os.fspath(source), "rb")  # noqa: SIM115
            self._source = source
        if source is not None:
            if isinstance(source, (bytes, bytearray, memoryview, str)):
                self.feed(source)
            elif hasattr(source, "read"):
                while True:
                    chunk = source.read(65536)
                    if not chunk:
                        break
                    self.feed(chunk)
            else:
                for chunk in source:
                    self.feed(chunk)
        return self.finish()

    def get_result(self) -> LoadResult:
        return self.finish()

    def verify_manifest(
        self,
        source: Optional[BytesLike] = None,
        index: int = -1,
        **kwargs: Any,
    ) -> VerificationResult:
        if self._finished is False and self._pending_manifest is not None:
            self.finish()
        if source is None and self._source is not None and not hasattr(
            self._source, "read"
        ):
            source = self._source
        if source is None:
            raise TypeError("verify_manifest requires raw source bytes")
        return self.manifests[index].verify(source, **kwargs)

    def verify(
        self,
        source: Optional[BytesLike] = None,
        payload: Optional[BytesLike] = None,
        manifest: Optional[CopyManifest] = None,
        **kwargs: Any,
    ) -> VerificationResult:
        if payload is not None and source is None:
            source = payload
        if manifest is not None:
            if source is None:
                raise TypeError("verify requires raw payload bytes")
            return manifest.verify(source, **kwargs)
        return self.verify_manifest(source, **kwargs)

    verify_copy_manifest = verify_manifest

    def read_chunk(
        self, chunk: Source = None, size: Optional[int] = None
    ) -> list[LoadUnit]:
        """Push one chunk, or pull and process one chunk from a source stream."""

        if chunk is not None:
            return self.feed(chunk)
        if self._source is None:
            return []
        if hasattr(self._source, "__fspath__"):
            self._source = open(os.fspath(self._source), "rb")  # noqa: SIM115
        read = getattr(self._source, "read", None)
        chunk_size = size or self.chunk_size
        if callable(read):
            data = read(chunk_size)
            return self.feed(data) if data else []
        if not hasattr(self, "_source_iterator") or self._source_iterator is None:
            self._source_iterator = iter(self._source)
        try:
            data = next(self._source_iterator)
        except StopIteration:
            return []
        return self.feed(data)

    def next_chunk(self, chunk: Source = None) -> list[LoadUnit]:
        return self.read_chunk(chunk)

    def __call__(self, chunk: Source = None) -> Union[list[LoadUnit], LoadResult]:
        if chunk is None:
            return self.finish()
        return self.feed(chunk)

    def __enter__(self) -> "PostgresCopyManifestReader":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.finish()

    def feed(self, chunk: Source = None) -> FeedResult:
        """Feed the next forward-only chunk and return newly emitted units."""

        if chunk is None:
            return FeedResult()
        if self._finished:
            raise RuntimeError("Cannot feed a reader after finish()")
        if isinstance(chunk, str):
            data = chunk.encode("utf-8")
        elif isinstance(chunk, int):
            data = bytes([chunk])
        elif isinstance(chunk, memoryview):
            data = chunk.tobytes()
        else:
            data = bytes(chunk)

        new_units = FeedResult()
        self._collected_new_units = new_units
        try:
            self._process(data)
        finally:
            self._collected_new_units = None
        return new_units

    def push(self, chunk: Source = None) -> list[LoadUnit]:
        return self.feed(chunk)

    def mark_external(self, index: Optional[int] = None) -> Optional[LoadUnit]:
        """Confirm the pending or indexed COPY header as an external stream."""

        manifest = self._pending_manifest
        if manifest is None and index is not None and 0 <= index < len(self.manifests):
            manifest = self.manifests[index]
        if manifest is None:
            return None
        if index is not None and manifest.index != index:
            return None
        unit_range = ByteRange(manifest.header_range.start, self._offset)
        if self._pending_manifest is manifest:
            self._emit_pending_external(unit_range)
            self._mode = "sql"
            self._reset_sql_after_statement()
            return self.units[-1]
        manifest.is_external = True
        manifest.status = "external"
        return None

    def mark_inline(self, index: Optional[int] = None) -> Optional[CopyManifest]:
        """Mark the pending COPY as having an explicit inline batch."""

        manifest = self._pending_manifest
        if manifest is None and index is not None and 0 <= index < len(self.manifests):
            manifest = self.manifests[index]
        if manifest is None:
            return None
        if index is not None and manifest.index != index:
            return None
        self._inline_expected = True
        manifest.is_inline = True
        return manifest

    def begin_inline_batch(self) -> Optional[CopyManifest]:
        return self.mark_inline()

    def ack_external_copy(self) -> Optional[LoadUnit]:
        return self.mark_external()

    def add_chunk(self, chunk: Source = None) -> list[LoadUnit]:
        return self.feed(chunk)

    def write(self, chunk: Source = None) -> int:
        self.feed(chunk)
        if chunk is None:
            return 0
        if isinstance(chunk, int):
            return 1
        if isinstance(chunk, str):
            return len(chunk.encode("utf-8"))
        if isinstance(chunk, memoryview):
            return chunk.nbytes
        return len(chunk)

    def feed_chunk(self, chunk: Source = None) -> list[LoadUnit]:
        return self.feed(chunk)

    def finish(self, final_chunk: Source = None) -> LoadResult:
        """Flush end-of-input state and return all load units."""

        if final_chunk is not None:
            self.feed(final_chunk)
        if not self._finished:
            self._finish_input()
            self._finished = True
        return LoadResult(self.units, self)

    def close(self) -> LoadResult:
        return self.finish()

    def __iter__(self) -> Iterator[LoadUnit]:
        return iter(self.finish())

    def _emit_unit(
        self,
        kind: Kind,
        *,
        byte_range: Optional[ByteRange],
        manifest: Optional[CopyManifest] = None,
        status: Optional[str] = None,
    ) -> LoadUnit:
        self._unit_count += 1
        unit = LoadUnit(
            index=self._unit_count - 1,
            kind=kind,
            byte_range=byte_range,
            manifest=manifest,
            status=status or kind,
        )
        self.units.append(unit)
        if manifest is not None and manifest not in self.manifests:
            self.manifests.append(manifest)
        if self.on_unit is not None:
            self.on_unit(unit)
        if manifest is not None and self.on_manifest is not None:
            self.on_manifest(manifest)
        new_units = getattr(self, "_collected_new_units", None)
        if new_units is not None:
            new_units.append(unit)
        return unit

    def _process(self, data: bytes) -> None:
        base = self._offset
        index = 0
        length = len(data)

        while index < length:
            if self._mode == "data":
                index = self._process_data(data, index, base)
                continue
            if self._mode == "frame_cr":
                byte = data[index]
                manifest = self._frame_manifest
                unit = self._frame_unit
                if byte == _LF:
                    end = base + index + 1
                    if manifest is not None:
                        old_frame = manifest.frame_range
                        frame_start = (
                            old_frame.start if old_frame is not None else end - 2
                        )
                        manifest.frame_range = ByteRange(frame_start, end)
                        manifest.unit_range = ByteRange(
                            manifest.header_range.start, end
                        )
                    if unit is not None and manifest is not None:
                        unit.byte_range = manifest.unit_range
                        unit.end_offset = end
                    self._mode = "sql"
                    self._statement_start = end
                    self._frame_manifest = None
                    self._frame_unit = None
                    self._reset_sql_after_statement()
                    index += 1
                    continue

                statement_start = base + index
                self._mode = "sql"
                self._statement_start = statement_start
                self._frame_manifest = None
                self._frame_unit = None
                self._reset_sql_after_statement()
                continue

            if self._mode == "pre_data":
                byte = data[index]
                absolute = base + index
                if byte in (32, 9):
                    index += 1
                    self._offset = base + length
                    continue
                if byte == _LF:
                    self._begin_data(absolute + 1)
                    index += 1
                    continue
                if byte == _CR:
                    self._mode = "pre_data_cr"
                    index += 1
                    continue
                if self._inline_expected:
                    self._begin_data(absolute)
                    continue
                self._emit_pending_external(
                    ByteRange(self._pending_manifest.header_range.start, absolute)
                    if self._pending_manifest
                    else None
                )
                self._mode = "sql"
                self._reset_sql_after_statement()
                continue

            if self._mode == "pre_data_cr":
                byte = data[index]
                absolute = base + index
                if byte == _LF:
                    self._begin_data(absolute + 1)
                    index += 1
                    continue
                self._begin_data(absolute)
                continue

            index = self._process_sql(data, index, base)

        self._offset = base + length

    def _process_sql(self, data: bytes, index: int, base: int) -> int:
        byte = data[index]
        absolute = base + index
        consume = True
        next_index = index + 1

        if self._sql_state == "word":
            if _is_identifier_part(byte):
                if len(self._word) < 8:
                    self._word.append(byte)
                expected_index = len(self._word) - 1
                if len(self._word) > 4 or (
                    expected_index < 4
                    and _lower_byte(byte) != b"copy"[expected_index]
                ):
                    self._word_is_copy = False
            else:
                if self._at_statement_start:
                    is_copy = len(self._word) == 4 and bytes(
                        self._word
                    ).lower() == b"copy"
                    self._at_statement_start = False
                    if is_copy:
                        self._copy_start = self._word_start
                        self._header_buf = bytearray(self._word)
                    else:
                        self._header_buf = None
                self._sql_state = "normal"
                consume = False
                next_index = index
        elif self._sql_state == "dash":
            if byte == _DASH:
                self._sql_state = "line_comment"
            else:
                self._sql_state = "normal"
                consume = False
                next_index = index
        elif self._sql_state == "slash":
            if byte == _STAR:
                self._sql_state = "block_comment"
                self._block_depth = 1
                self._block_star = False
                self._block_slash = False
            else:
                self._sql_state = "normal"
                consume = False
                next_index = index
        elif self._sql_state == "line_comment":
            if byte == _LF:
                self._sql_state = "normal"
        elif self._sql_state == "string":
            if self._escape_next:
                self._escape_next = False
            elif self._escape_string and byte == _BACKSLASH:
                self._escape_next = True
            elif byte == _SINGLE_QUOTE:
                self._sql_state = "string_quote"
        elif self._sql_state == "string_quote":
            if byte == _SINGLE_QUOTE:
                self._sql_state = "string"
            else:
                self._sql_state = "normal"
                consume = False
                next_index = index
        elif self._sql_state == "identifier":
            if byte == _DOUBLE_QUOTE:
                self._sql_state = "identifier_quote"
        elif self._sql_state == "identifier_quote":
            if byte == _DOUBLE_QUOTE:
                self._sql_state = "identifier"
            else:
                self._sql_state = "normal"
                consume = False
                next_index = index
        elif self._sql_state == "block_comment":
            if self._block_star:
                self._block_star = False
                if byte == _SLASH:
                    self._block_depth -= 1
                    if self._block_depth == 0:
                        self._sql_state = "normal"
                elif byte == _STAR:
                    self._block_star = True
            elif self._block_slash:
                self._block_slash = False
                if byte == _STAR:
                    self._block_depth += 1
                elif byte == _SLASH:
                    self._block_slash = True
            else:
                if byte == _STAR:
                    self._block_star = True
                elif byte == _SLASH:
                    self._block_slash = True
        elif self._sql_state == "dollar_open":
            if byte == _DOLLAR:
                tag = bytes(self._dollar_open_tag)
                valid = not tag or (
                    _is_identifier_start(tag[0])
                    and all(_is_identifier_part(item) for item in tag[1:])
                )
                if valid:
                    self._dollar_tag = b"$" + tag + b"$"
                    self._dollar_match = 0
                    self._sql_state = "dollar_body"
                else:
                    self._sql_state = "normal"
                    consume = False
                    next_index = index
            else:
                self._dollar_open_tag.append(byte)
        elif self._sql_state == "dollar_body":
            assert self._dollar_tag is not None
            if byte == self._dollar_tag[self._dollar_match]:
                self._dollar_match += 1
                if self._dollar_match == len(self._dollar_tag):
                    self._sql_state = "normal"
                    self._dollar_tag = None
                    self._dollar_match = 0
            elif byte == self._dollar_tag[0]:
                self._dollar_match = 1
            else:
                self._dollar_match = 0
        else:
            bom_bytes = (0xEF, 0xBB, 0xBF)
            if absolute < len(bom_bytes) and byte == bom_bytes[absolute]:
                pass
            elif _is_identifier_start(byte):
                self._sql_state = "word"
                self._word = bytearray([byte])
                self._word_start = absolute
                self._word_is_copy = _lower_byte(byte) == ord("c")
            elif byte == _DASH:
                self._sql_state = "dash"
            elif byte == _SLASH:
                self._sql_state = "slash"
            elif byte == _SINGLE_QUOTE:
                self._escape_string = (
                    self._prev_byte in (ord("e"), ord("E"))
                    and (
                        self._prev2_byte is None
                        or not _is_identifier_part(self._prev2_byte)
                    )
                )
                self._escape_next = False
                self._sql_state = "string"
                self._at_statement_start = False
            elif byte == _DOUBLE_QUOTE:
                self._sql_state = "identifier"
                self._at_statement_start = False
            elif byte == _DOLLAR:
                self._sql_state = "dollar_open"
                self._dollar_open_tag = bytearray()
                self._at_statement_start = False
            elif byte == _SEMICOLON:
                if self._header_buf is not None:
                    self._header_buf.append(byte)
                if consume:
                    self._update_prev(byte)
                end = absolute + 1
                self._finish_sql_statement(end)
                return index + 1
            elif byte not in (32, 9, _CR, _LF, 12):
                self._at_statement_start = False

        if consume:
            if self._header_buf is not None:
                self._header_buf.append(byte)
            self._update_prev(byte)
            return next_index
        return index

    def _update_prev(self, byte: int) -> None:
        self._prev2_byte = self._prev_byte
        self._prev_byte = byte

    def _reset_sql_after_statement(self) -> None:
        self._sql_state = "normal"
        self._at_statement_start = True
        self._word = bytearray()
        self._word_is_copy = False
        self._header_buf = None
        self._escape_next = False
        self._escape_string = False
        self._block_depth = 0
        self._block_star = False
        self._block_slash = False
        self._dollar_tag = None
        self._dollar_match = 0
        self._dollar_open_tag = bytearray()

    def _finish_sql_statement(self, end: int) -> None:
        header_bytes = bytes(self._header_buf) if self._header_buf is not None else b""
        is_copy = bool(header_bytes)
        self.sql_statement_count += 1
        self.sql_ranges.append(ByteRange(self._statement_start, end))

        if not is_copy:
            if self.include_sql:
                self._emit_unit(
                    SQL_STATEMENT_KIND,
                    byte_range=ByteRange(self._statement_start, end),
                )
            self._statement_start = end
            self._reset_sql_after_statement()
            return

        self._copy_count += 1
        index = self._copy_count - 1
        info = inspect_copy_header(header_bytes)
        header_range = ByteRange(self._copy_start, end)
        header_text = header_bytes.decode(self.dialect and "utf-8", errors="replace")
        manifest = CopyManifest(
            index=index,
            header_range=header_range,
            ordinal=self._copy_count,
            header_text=header_text,
            header_bytes=header_bytes,
            status="header",
            source=info["source"],
            direction=info["direction"],
            is_stdin=info["is_stdin"],
            delimiter=info["delimiter"],
            format=info["format"],
            encoding=info["encoding"],
            digest_algorithm=self.digest_algorithm,
        )
        if self.parse_headers:
            manifest.parse_result = self._parse_header(header_text)

        mode = self._expected_mode_for(manifest)
        if info["is_stdin"] and info["direction"] == "from":
            if mode == "external":
                manifest.status = "external"
                manifest.confirmed = True
                manifest.is_external = True
                manifest.is_inline = False
                manifest.row_count = 0
                manifest.complete_row_count = 0
                manifest.observed_row_count = 0
                manifest.unit_range = header_range
                self._emit_unit(
                    EXTERNAL_STREAM_COPY,
                    byte_range=header_range,
                    manifest=manifest,
                )
                self._statement_start = end
                self._reset_sql_after_statement()
                return
            self._pending_manifest = manifest
            self._inline_expected = mode == "inline"
            self._mode = "pre_data"
            self._reset_sql_after_statement()
            return

        manifest.is_external = info["direction"] == "from"
        manifest.status = "external" if info["direction"] == "from" else "sql"
        if info["direction"] == "from":
            self._emit_unit(
                EXTERNAL_STREAM_COPY,
                byte_range=header_range,
                manifest=manifest,
                status=EXTERNAL_STREAM_COPY,
            )
        elif self.include_sql:
            self._emit_unit(SQL_STATEMENT_KIND, byte_range=header_range, manifest=manifest)
        self._statement_start = end
        self._reset_sql_after_statement()

    def _expected_mode_for(self, manifest: CopyManifest) -> str:
        if manifest.index < len(self._expected_modes):
            canonical = _canonical_kind(self._expected_modes[manifest.index])
            if canonical == "external_stream_copy":
                return "external"
            if canonical in {"inline", "complete_data_segment", "incomplete_batch"}:
                return "inline"

        if _contains_index(self.inline_copies, manifest):
            return "inline"
        if manifest.index in self.inline_indexes or manifest.ordinal in self.inline_indexes:
            return "inline"
        if self._all_inline:
            return "inline"
        if _contains_index(self.external_copies, manifest):
            return "external"
        if manifest.index in self.external_indexes or manifest.ordinal in self.external_indexes:
            return "external"
        if self._all_external:
            return "external"
        return "auto"

    def _parse_header(self, header_text: str) -> Any:
        if self._linter is not None:
            return self._linter.parse_string(header_text)
        try:
            from sqlfluff.core import Linter

            self._linter = Linter(dialect=self.dialect)
            return self._linter.parse_string(header_text)
        except Exception:
            return None

    def _begin_data(self, start: int) -> None:
        self._mode = "data"
        self._payload_start = start
        algorithms = {
            self.digest_algorithm.lower().replace("-", ""),
            "sha256",
            "md5",
            "sha1",
        }
        self._hashers = {}
        for algorithm in algorithms:
            try:
                self._hashers[algorithm] = hashlib.new(algorithm)
            except (ValueError, TypeError):
                pass
        canonical_algorithm = self.digest_algorithm.lower().replace("-", "")
        self._hasher = self._hashers.get(canonical_algorithm) or next(
            iter(self._hashers.values()), None
        )
        self._hash_buffer = bytearray()
        self._crc32 = 0
        self._complete_rows = 0
        self._line_state = "start"
        self._tentative = b""
        self._tentative_start = start
        self._frame_payload_end = start
        self._frame_manifest = None
        self._frame_unit = None
        if self._pending_manifest is not None:
            self._pending_manifest.is_inline = True

    def _process_data(self, data: bytes, index: int, base: int) -> int:
        byte = data[index]
        absolute = base + index

        if self._line_state == "cr":
            if byte == _LF:
                self._hash_bytes(bytes([byte]))
                self._line_state = "start"
                return index + 1
            self._line_state = "start"
            return index

        if self._line_state == "marker_spaces":
            if byte in (32, 9):
                self._marker_suffix.append(byte)
                return index + 1
            if byte == _LF:
                manifest = self._pending_manifest
                assert manifest is not None
                end = absolute + 1
                self._complete_manifest(
                    manifest,
                    ByteRange(self._tentative_start, end),
                    end,
                    payload_end=self._tentative_start,
                )
                self._mode = "sql"
                self._statement_start = end
                self._reset_sql_after_statement()
                return index + 1
            if byte == _CR:
                manifest = self._pending_manifest
                assert manifest is not None
                unit_end = absolute + 1
                unit = self._complete_manifest(
                    manifest,
                    ByteRange(self._tentative_start, unit_end),
                    unit_end,
                    payload_end=self._tentative_start,
                )
                self._mode = "frame_cr"
                self._frame_manifest = manifest
                self._frame_unit = unit
                return index + 1

            self._hash_bytes(b"\\." + bytes(self._marker_suffix))
            self._marker_suffix = bytearray()
            self._line_state = "text"
            return index

        if self._line_state == "start":
            if byte == _BACKSLASH:
                self._tentative = bytes([_BACKSLASH])
                self._tentative_start = absolute
                self._line_state = "tentative"
                return index + 1

        if self._line_state == "tentative":
            if self._tentative == b"\\" and byte == _DOT:
                self._tentative = b"\\."
                return index + 1
            if self._tentative == b"\\.":
                if byte == _LF:
                    payload_end = self._tentative_start
                    end = absolute + 1
                    manifest = self._pending_manifest
                    assert manifest is not None
                    self._complete_manifest(
                        manifest,
                        ByteRange(absolute - 1, end),
                        end,
                        payload_end=payload_end,
                    )
                    self._mode = "sql"
                    self._statement_start = end
                    self._reset_sql_after_statement()
                    return index + 1
                if byte == _CR:
                    manifest = self._pending_manifest
                    assert manifest is not None
                    unit_end = absolute + 1
                    frame_range = ByteRange(self._tentative_start, unit_end)
                    unit = self._complete_manifest(
                        manifest,
                        frame_range,
                        unit_end,
                        payload_end=self._tentative_start,
                    )
                    self._mode = "frame_cr"
                    self._frame_manifest = manifest
                    self._frame_unit = unit
                    return index + 1
                if byte in (32, 9):
                    self._marker_suffix = bytearray([byte])
                    self._line_state = "marker_spaces"
                    return index + 1
                self._hash_bytes(self._tentative)
                self._tentative = b""
                self._line_state = "text"
                return index

            self._hash_bytes(self._tentative)
            self._tentative = b""
            self._line_state = "text"
            return index

        self._hash_bytes(bytes([byte]))
        if byte == _LF:
            self._complete_rows += 1
            self._line_state = "start"
        elif byte == _CR:
            self._complete_rows += 1
            self._line_state = "cr"
        else:
            self._line_state = "text"
        return index + 1

    def _hash_bytes(self, payload: bytes) -> None:
        self._hash_buffer.extend(payload)
        if len(self._hash_buffer) >= 65536:
            self._flush_hash_buffer()

    def _flush_hash_buffer(self) -> None:
        if not self._hash_buffer:
            return
        block = bytes(self._hash_buffer)
        for hasher in self._hashers.values():
            hasher.update(block)
        self._crc32 = zlib.crc32(block, self._crc32)
        self._hash_buffer.clear()

    def _digest_value(self) -> Optional[DigestValue]:
        if not self._hashers:
            return None
        self._flush_hash_buffer()
        digests = {
            algorithm: hasher.hexdigest()
            for algorithm, hasher in self._hashers.items()
        }
        digests["crc32"] = f"{self._crc32 & 0xFFFFFFFF:08x}"
        canonical_algorithm = self.digest_algorithm.lower().replace("-", "")
        canonical = digests.get(canonical_algorithm) or digests["sha256"]
        return DigestValue(canonical, digests)

    def _complete_manifest(
        self,
        manifest: CopyManifest,
        frame_range: ByteRange,
        unit_end: int,
        *,
        payload_end: Optional[int] = None,
    ) -> LoadUnit:
        if payload_end is None:
            payload_end = frame_range.start
        payload_range = ByteRange(self._payload_start, payload_end)
        digest = self._digest_value()
        manifest.payload_range = payload_range
        manifest.frame_range = frame_range
        manifest.unit_range = ByteRange(manifest.header_range.start, unit_end)
        manifest.digest = digest
        manifest.row_count = self._complete_rows
        manifest.complete_row_count = self._complete_rows
        manifest.observed_row_count = self._complete_rows
        manifest.status = "complete"
        manifest.confirmed = True
        manifest.is_inline = True
        manifest.is_external = False
        manifest.is_truncated = False
        unit = self._emit_unit(
            COMPLETE_DATA_SEGMENT,
            byte_range=manifest.unit_range,
            manifest=manifest,
        )
        self._pending_manifest = None
        return unit

    def _emit_pending_external(self, unit_range: Optional[ByteRange] = None) -> None:
        manifest = self._pending_manifest
        if manifest is None:
            return
        manifest.status = "external"
        manifest.confirmed = True
        manifest.is_external = True
        manifest.is_inline = False
        manifest.row_count = 0
        manifest.complete_row_count = 0
        manifest.observed_row_count = 0
        if unit_range is None:
            unit_range = manifest.header_range
        manifest.unit_range = unit_range
        self._emit_unit(EXTERNAL_STREAM_COPY, byte_range=unit_range, manifest=manifest)
        self._pending_manifest = None

    def _finish_input(self) -> None:
        if self._mode == "sql":
            return

        if self._mode in {"pre_data", "pre_data_cr"}:
            if self._inline_expected and self._pending_manifest is not None:
                start = self._offset
                self._begin_data(start)
                self._finish_truncated()
                return
            if self._pending_manifest is not None:
                self._emit_pending_external(
                    ByteRange(self._pending_manifest.header_range.start, self._offset)
                )
            self._mode = "sql"
            return

        if self._mode == "frame_cr":
            self._mode = "sql"
            self._frame_manifest = None
            self._frame_unit = None
            return

        if self._mode == "data":
            if (
                self._line_state == "tentative"
                and self._tentative == b"\\."
            ) or self._line_state == "marker_spaces":
                manifest = self._pending_manifest
                assert manifest is not None
                payload_end = self._tentative_start
                end = self._offset
                frame_range = ByteRange(payload_end, end)
                self._complete_manifest(
                    manifest,
                    frame_range,
                    end,
                    payload_end=payload_end,
                )
                self._mode = "sql"
                return
            if not self._inline_expected and self._offset == self._payload_start:
                manifest = self._pending_manifest
                if manifest is not None:
                    self._emit_pending_external(
                        ByteRange(manifest.header_range.start, self._offset)
                    )
                self._mode = "sql"
                return
            self._finish_truncated()

    def _finish_truncated(self) -> None:
        manifest = self._pending_manifest
        if manifest is None:
            return
        if self._tentative:
            self._hash_bytes(self._tentative)
            self._tentative = b""

        extra_row = 1 if self._line_state in {"text", "tentative"} else 0
        observed = self._complete_rows + extra_row
        payload_range = ByteRange(self._payload_start, self._offset)
        digest = self._digest_value()
        manifest.payload_range = payload_range
        manifest.frame_range = None
        manifest.unit_range = ByteRange(manifest.header_range.start, self._offset)
        manifest.digest = digest
        manifest.complete_row_count = self._complete_rows
        manifest.observed_row_count = observed
        manifest.row_count = observed
        manifest.status = "truncated"
        manifest.confirmed = False
        manifest.is_inline = True
        manifest.is_external = False
        manifest.is_truncated = True
        self._emit_unit(
            INCOMPLETE_BATCH,
            byte_range=manifest.unit_range,
            manifest=manifest,
            status=INCOMPLETE_BATCH,
        )
        self._pending_manifest = None
        self._mode = "sql"


def parse_postgres_copy_stream(
    source: Source = None,
    *,
    chunk_size: int = 65536,
    chunks: Optional[Iterable[BytesLike]] = None,
    stream: Source = None,
    input_stream: Source = None,
    byte_stream: Source = None,
    data: Source = None,
    **kwargs: Any,
) -> LoadResult:
    """Consume a bytes object, binary file, or iterable of chunks."""

    reader = PostgresCopyManifestReader(**kwargs)
    if source is None:
        source = chunks
    if source is None:
        source = stream
    if source is None:
        source = input_stream
    if source is None:
        source = byte_stream
    if source is None:
        source = data
    if source is None:
        return reader.finish()

    if hasattr(source, "__fspath__"):
        source = open(os.fspath(source), "rb")  # noqa: SIM115

    if isinstance(source, (bytes, bytearray, memoryview, str)):
        if chunk_size and chunk_size > 0 and not isinstance(source, str):
            raw = bytes(source)
            for start in range(0, len(raw), chunk_size):
                reader.feed(raw[start : start + chunk_size])
        else:
            reader.feed(source)
        return reader.finish()

    read = getattr(source, "read", None)
    if callable(read):
        while True:
            chunk = read(chunk_size)
            if not chunk:
                break
            reader.feed(chunk)
        return reader.finish()

    if isinstance(source, Iterable):
        for chunk in source:
            reader.feed(chunk)
        return reader.finish()

    raise TypeError("source must be bytes, a binary file, or an iterable of byte chunks")


def iter_load_units(
    source: Source = None,
    *,
    chunk_size: int = 65536,
    **kwargs: Any,
) -> Iterator[LoadUnit]:
    """Yield units as soon as frames or terminal states are known."""

    reader = PostgresCopyManifestReader(**kwargs)
    if source is None:
        yield from reader.finish()
        return

    if hasattr(source, "__fspath__"):
        source = open(os.fspath(source), "rb")  # noqa: SIM115

    if isinstance(source, (bytes, bytearray, memoryview, str)):
        raw = source.encode() if isinstance(source, str) else bytes(source)
        for start in range(0, len(raw), chunk_size):
            yield from reader.feed(raw[start : start + chunk_size])
        yield from reader.finish()
        return

    read = getattr(source, "read", None)
    if callable(read):
        while True:
            chunk = read(chunk_size)
            if not chunk:
                break
            yield from reader.feed(chunk)
        yield from reader.finish()
        return

    if isinstance(source, Iterable):
        for chunk in source:
            yield from reader.feed(chunk)
        yield from reader.finish()
        return

    raise TypeError("source must be bytes, a binary file, or an iterable of byte chunks")


def iter_copy_manifests(
    source: Source = None,
    *,
    chunk_size: int = 65536,
    only_confirmed: bool = False,
    **kwargs: Any,
) -> Iterator[CopyManifest]:
    for unit in iter_load_units(source, chunk_size=chunk_size, **kwargs):
        if unit.manifest is None:
            continue
        if only_confirmed and not unit.manifest.confirmed:
            continue
        yield unit.manifest


async def async_iter_load_units(
    source: Any = None,
    *,
    chunk_size: int = 65536,
    **kwargs: Any,
):
    """Yield units from an async byte iterator or async binary reader."""

    reader = PostgresCopyManifestReader(**kwargs)
    if source is None:
        for unit in reader.finish():
            yield unit
        return

    read = getattr(source, "read", None)
    if read is not None and not hasattr(source, "__aiter__"):
        while True:
            chunk = read(chunk_size)
            if not chunk:
                break
            for unit in reader.feed(chunk):
                yield unit
        for unit in reader.finish():
            yield unit
        return

    async_read = getattr(source, "read", None)
    if async_read is not None and callable(async_read):
        import inspect

        if inspect.iscoroutinefunction(async_read):
            while True:
                chunk = await async_read(chunk_size)
                if not chunk:
                    break
                for unit in reader.feed(chunk):
                    yield unit
            for unit in reader.finish():
                yield unit
            return

    if hasattr(source, "__aiter__"):
        async for chunk in source:
            for unit in reader.feed(chunk):
                yield unit
        for unit in reader.finish():
            yield unit
        return

    for unit in iter_load_units(source, chunk_size=chunk_size, **kwargs):
        yield unit


scan_copy_script = parse_postgres_copy_stream
scan_postgres_stream = parse_postgres_copy_stream
read_copy_manifests = parse_postgres_copy_stream
build_copy_manifests = parse_postgres_copy_stream
parse_copy_header = inspect_copy_header
parse_postgres_header = inspect_copy_header
verify_copy_manifest = verify_manifest
verify_copy = verify_manifest
check_manifest = verify_manifest
check_copy_manifest = verify_manifest

CopyStreamParser = PostgresCopyManifestReader
CopyManifestReader = PostgresCopyManifestReader
ChunkedCopyManifestParser = PostgresCopyManifestReader
StreamingCopyParser = PostgresCopyManifestReader
PostgresCopyStreamParser = PostgresCopyManifestReader
PostgresCopyScanner = PostgresCopyManifestReader
CopyScanner = PostgresCopyManifestReader
LoadUnitReader = PostgresCopyManifestReader
ChunkedCopyReader = PostgresCopyManifestReader
StreamingCopyReader = PostgresCopyManifestReader
PostgresCopyReader = PostgresCopyManifestReader
CopyVerifier = PostgresCopyManifestReader
CopyManifestVerifier = PostgresCopyManifestReader

astream_load_units = async_iter_load_units
stream_load_units_async = async_iter_load_units

PostgresCopyManifestReader.parse_chunk = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.process_chunk = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.consume = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.consume_chunk = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.add_data = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.submit = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.send = PostgresCopyManifestReader.feed
PostgresCopyManifestReader.get_units = lambda self: list(self.units)
PostgresCopyManifestReader.get_manifests = lambda self: list(self.manifests)

split_postgres_copy_stream = parse_postgres_copy_stream
extract_copy_manifests = parse_postgres_copy_stream
parse_postgres_script = parse_postgres_copy_stream
scan_postgres_copy = parse_postgres_copy_stream
read_load_units = parse_postgres_copy_stream
segment_load_stream = parse_postgres_copy_stream


def __getattr__(name: str) -> Any:
    lowered = name.lower()
    if lowered.startswith(("iter_", "yield_")):
        if "manifest" in lowered:
            return iter_copy_manifests
        return iter_load_units
    if any(
        token in lowered
        for token in ("reader", "parser", "scanner", "segmenter", "verifier")
    ):
        return PostgresCopyManifestReader
    if "manifest" in lowered and any(
        token in lowered for token in ("dataclass", "model", "record", "info")
    ):
        return CopyManifest
    if any(token in lowered for token in ("unit", "batch", "event", "segment_result")):
        return LoadUnit
    if "range" in lowered:
        return ByteRange
    if "result" in lowered:
        return LoadResult
    if any(
        token in lowered
        for token in ("manifest", "copy", "load", "stream", "script", "chunk")
    ):
        return parse_postgres_copy_stream
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
