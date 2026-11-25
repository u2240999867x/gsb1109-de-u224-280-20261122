"""Bounded, streaming support for compressed Kafka record batches.

Kafka record batches (magic 2 and above) may use gzip compression.  A small
compressed payload can expand to an arbitrarily large buffer, so decompressing
such a batch eagerly is unsafe for a consumer that only hands a single record at
a time to its caller.

This module implements a streaming reader for gzip compressed v2 batches.  The
reader never materialises the whole uncompressed batch.  Instead it decodes the
gzip stream in small chunks and parses records incrementally, which keeps the
working set independent of the batch's uncompressed size.
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import Iterator
from typing import Final

from aiokafka.errors import CorruptRecordException, DecompressionLimitError
from aiokafka.util import NO_EXTENSIONS

from .default_records import DefaultRecord, DefaultRecordBatch
from .legacy_records import LegacyRecordBatch
from .util import calc_crc32c

DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES: Final = 64 * 1024 * 1024
DEFAULT_DECOMPRESSION_MEMORY_LIMIT: Final = 8 * 1024 * 1024

# Small enough that several partitions can hold a streaming decoder at the same
# time while staying well below the default 8 MiB working set bound.
_MIN_CHUNK_SIZE: Final = 1024
_MAX_CHUNK_SIZE: Final = 64 * 1024


class _DecompressionError(CorruptRecordException):
    """The gzip stream itself is malformed or truncated."""


def decompression_chunk_size(memory_limit: int, partition_count: int) -> int:
    """Return a chunk size that keeps ``partition_count`` decoders bounded.

    Each decoder keeps at most an input chunk, an output chunk and one record's
    worth of buffered data alive.  Dividing the configured budget by four and
    by the number of partitions leaves headroom for interpreter bookkeeping.
    """
    partition_count = max(partition_count, 1)
    budget_per_partition = max(1, int(memory_limit) // partition_count)
    chunk = budget_per_partition // 4
    chunk = max(chunk, _MIN_CHUNK_SIZE)
    chunk = min(chunk, _MAX_CHUNK_SIZE)
    return chunk


class _GzipChunkIterator:
    """Yield decompressed chunks from a gzip payload without buffering it all."""

    __slots__ = (
        "_chunk_size",
        "_closed",
        "_decompressor",
        "_finished",
        "_input_chunk",
        "_max_output",
        "_payload",
        "_pos",
        "_total",
    )

    def __init__(self, payload: memoryview, max_output: int, chunk_size: int) -> None:
        self._payload = payload
        self._pos = 0
        self._max_output = max_output
        self._chunk_size = chunk_size
        self._input_chunk = chunk_size
        self._total = 0
        self._closed = False
        self._finished = False
        self._decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def close(self) -> None:
        self._closed = True
        self._finished = True
        self._payload = memoryview(b"")
        self._decompressor = None  # type: ignore[assignment]

    def __iter__(self) -> Iterator[bytes]:
        while not self._finished:
            if self._closed:
                return

            decompressor = self._decompressor
            if decompressor.eof:
                unused = decompressor.unused_data
                if not unused:
                    self._finished = True
                    break
                # gzip allows concatenated members.  Feed the leftovers to a
                # fresh decoder rather than discarding them.
                self._decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
                self._payload = memoryview(unused)
                self._pos = 0
                continue

            remaining = self._max_output - self._total
            # Request one byte past the remaining allowance.  This detects an
            # over-limit stream precisely, without decoding an extra chunk.
            max_length = min(self._chunk_size, remaining + 1)
            if max_length <= 0:
                raise DecompressionLimitError(
                    "Decompressed record batch exceeds the configured limit of "
                    f"{self._max_output} bytes"
                )

            pending = decompressor.unconsumed_tail
            try:
                if pending:
                    # `max_length` stopped the previous call early; replay the
                    # input the decompressor did not consume.
                    out = decompressor.decompress(pending, max_length)
                elif self._pos < len(self._payload):
                    data = self._payload[self._pos : self._pos + self._input_chunk]
                    self._pos += len(data)
                    out = decompressor.decompress(data, max_length)
                elif decompressor.eof:
                    self._finished = True
                    break
                else:
                    out = decompressor.decompress(b"", max_length)
            except zlib.error as err:
                raise _DecompressionError(
                    f"Invalid gzip compressed record batch: {err}"
                ) from err

            if not out and not pending and self._pos >= len(self._payload):
                if not decompressor.eof:
                    raise _DecompressionError(
                        "Compressed record batch ended before the gzip trailer was seen"
                    )
                self._finished = True
                break

            if out:
                self._total += len(out)
                if self._total > self._max_output:
                    raise DecompressionLimitError(
                        "Decompressed record batch exceeds the configured limit "
                        f"of {self._max_output} bytes"
                    )
                yield out


class _ChunkStream:
    """A forward-only byte stream with small, bounded buffering."""

    __slots__ = ("_buffer", "_chunks", "_consumed", "_pos")

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._buffer = bytearray()
        self._pos = 0
        self._consumed = 0

    @property
    def consumed(self) -> int:
        return self._consumed

    def close(self) -> None:
        close = getattr(self._chunks, "close", None)
        if close is not None:
            close()
        self._buffer = bytearray()
        self._pos = 0

    def _fill(self, want: int) -> bool:
        """Ensure at least one byte is buffered.  Return False on EOF."""
        if self._pos < len(self._buffer):
            return True
        self._buffer = bytearray()
        self._pos = 0
        while len(self._buffer) < want:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                return False
            if not chunk:
                continue
            self._buffer.extend(chunk)
            # One chunk is enough to make progress for varints and small reads.
            if self._buffer:
                return True
        return bool(self._buffer)

    def read_byte(self) -> int | None:
        if not self._fill(1):
            return None
        value = self._buffer[self._pos]
        self._pos += 1
        self._consumed += 1
        return value

    def read_varint(self) -> int:
        result = 0
        shift = 0
        while True:
            byte = self.read_byte()
            if byte is None:
                raise _DecompressionError(
                    "Compressed record batch ended in the middle of a varint"
                )
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return (result >> 1) ^ -(result & 1)
            shift += 7
            if shift >= 64:
                raise CorruptRecordException("Out of int64 range")

    def skip(self, size: int) -> None:
        if size < 0:
            raise CorruptRecordException(f"Invalid negative size {size}")
        remaining = size
        while remaining:
            if self._pos >= len(self._buffer) and not self._fill(1):
                raise CorruptRecordException(
                    f"Record declares {size} bytes but only "
                    f"{size - remaining} bytes remain in the batch"
                )
            available = len(self._buffer) - self._pos
            step = min(available, remaining)
            self._pos += step
            self._consumed += step
            remaining -= step

    def read_exact(self, size: int) -> bytes:
        if size < 0:
            raise CorruptRecordException(f"Invalid negative size {size}")
        if size == 0:
            return b""
        out = bytearray()
        remaining = size
        while remaining:
            if self._pos >= len(self._buffer) and not self._fill(1):
                raise CorruptRecordException(
                    f"Record declares {size} bytes but only "
                    f"{size - remaining} bytes remain in the batch"
                )
            available = len(self._buffer) - self._pos
            step = min(available, remaining)
            out.extend(self._buffer[self._pos : self._pos + step])
            self._pos += step
            self._consumed += step
            remaining -= step
        return bytes(out)

    def drain(self) -> None:
        """Consume the rest of the stream, validating its trailer."""
        while True:
            if self._pos >= len(self._buffer) and not self._fill(1):
                return
            step = len(self._buffer) - self._pos
            self._pos += step
            self._consumed += step


_HEADER_STRUCT = struct.Struct(
    ">q"  # BaseOffset => Int64
    "i"  # Length => Int32
    "i"  # PartitionLeaderEpoch => Int32
    "b"  # Magic => Int8
    "I"  # CRC => Uint32
    "h"  # Attributes => Int16
    "i"  # LastOffsetDelta => Int32
    "q"  # FirstTimestamp => Int64
    "q"  # MaxTimestamp => Int64
    "q"  # ProducerId => Int64
    "h"  # ProducerEpoch => Int16
    "i"  # BaseSequence => Int32
    "i"  # Records count => Int32
)
_HEADER_SIZE = _HEADER_STRUCT.size
_ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
_CODEC_MASK = 0x07
_CODEC_GZIP = 0x01
_TIMESTAMP_TYPE_MASK = 0x08
_TRANSACTIONAL_MASK = 0x10
_CONTROL_MASK = 0x20


class _BatchHeader:
    __slots__ = (
        "attributes",
        "base_offset",
        "base_sequence",
        "crc",
        "first_timestamp",
        "last_offset_delta",
        "magic",
        "max_timestamp",
        "num_records",
        "producer_epoch",
        "producer_id",
        "raw",
    )

    def __init__(self, raw: memoryview) -> None:
        values = _HEADER_STRUCT.unpack_from(raw)
        (
            self.base_offset,
            _length,
            _leader_epoch,
            self.magic,
            self.crc,
            self.attributes,
            self.last_offset_delta,
            self.first_timestamp,
            self.max_timestamp,
            self.producer_id,
            self.producer_epoch,
            self.base_sequence,
            self.num_records,
        ) = values
        self.raw = raw


def _validate_crc(raw: memoryview, expected_crc: int) -> bool:
    return calc_crc32c(raw[_ATTRIBUTES_OFFSET:]) == expected_crc


class ProtectedGzipBatch:
    """A v2 gzip batch that is only yielded after full validation."""

    __slots__ = (
        "_check_crcs",
        "_chunk_size",
        "_header",
        "_iteration_chunks",
        "_iteration_remaining",
        "_iteration_stream",
        "_max_output",
        "_payload",
        "_validated",
    )

    def __init__(
        self,
        raw: memoryview,
        *,
        max_decompressed_bytes: int,
        chunk_size: int,
        check_crcs: bool = True,
    ) -> None:
        self._header = _BatchHeader(raw)
        self._payload = raw[_HEADER_SIZE:]
        self._max_output = max_decompressed_bytes
        self._chunk_size = chunk_size
        self._check_crcs = check_crcs
        self._validated = False
        self._iteration_stream: _ChunkStream | None = None
        self._iteration_chunks: _GzipChunkIterator | None = None
        self._iteration_remaining = 0

    # -- Header properties expected by PartitionRecords -------------------
    @property
    def base_offset(self) -> int:
        return self._header.base_offset

    @property
    def magic(self) -> int:
        return self._header.magic

    @property
    def crc(self) -> int:
        return self._header.crc

    @property
    def attributes(self) -> int:
        return self._header.attributes

    @property
    def compression_type(self) -> int:
        return self.attributes & _CODEC_MASK

    @property
    def timestamp_type(self) -> int:
        return int(bool(self.attributes & _TIMESTAMP_TYPE_MASK))

    @property
    def is_transactional(self) -> bool:
        return bool(self.attributes & _TRANSACTIONAL_MASK)

    @property
    def is_control_batch(self) -> bool:
        return bool(self.attributes & _CONTROL_MASK)

    @property
    def last_offset_delta(self) -> int:
        return self._header.last_offset_delta

    @property
    def first_timestamp(self) -> int:
        return self._header.first_timestamp

    @property
    def max_timestamp(self) -> int:
        return self._header.max_timestamp

    @property
    def producer_id(self) -> int:
        return self._header.producer_id

    @property
    def producer_epoch(self) -> int:
        return self._header.producer_epoch

    @property
    def base_sequence(self) -> int:
        return self._header.base_sequence

    @property
    def num_records(self) -> int:
        return self._header.num_records

    @property
    def next_offset(self) -> int:
        return self.base_offset + self.last_offset_delta + 1

    @property
    def protected(self) -> bool:
        return True

    def validate_crc(self) -> bool:
        return _validate_crc(self._header.raw, self.crc)

    def close(self) -> None:
        """Drop references to the compressed payload."""
        self._release_iteration()
        self._payload = memoryview(b"")
        self._header.raw = memoryview(b"")

    # -- Validation and iteration ------------------------------------------
    def _chunks(self) -> _GzipChunkIterator:
        return _GzipChunkIterator(self._payload, self._max_output, self._chunk_size)

    def validate(self) -> None:
        """Decompress and structurally validate the whole batch.

        No record is returned until this pass succeeds.  This guarantees that
        a malformed or oversized batch never leaks a partial batch downstream.
        Bytes decompressed here count towards the per-batch accounting.
        """
        if self._validated:
            return
        if self._check_crcs and not self.validate_crc():
            raise CorruptRecordException(
                f"Invalid CRC - batch at offset {self.base_offset}"
            )

        num_records = self.num_records
        if num_records < 0:
            raise CorruptRecordException(
                f"Invalid number of records {num_records} in batch at offset "
                f"{self.base_offset}"
            )

        chunks = self._chunks()
        stream = _ChunkStream(iter(chunks))
        try:
            count = num_records
            while count:
                self._skip_record(stream)
                count -= 1
            # Any leftover bytes after all declared records are corrupt.  This
            # also drains the gzip stream so its trailer is verified.
            extra = stream.read_byte()
            if extra is not None:
                raise CorruptRecordException(
                    "unconsumed bytes after all records consumed"
                )
        finally:
            stream.close()
            chunks.close()
        self._validated = True

    @staticmethod
    def _skip_record(stream: _ChunkStream) -> None:
        length = stream.read_varint()
        if length < 0:
            raise CorruptRecordException(f"Invalid record size {length}")
        start = stream.consumed
        end = start + length

        stream.read_varint()  # attributes
        if stream.consumed > end:
            raise CorruptRecordException("Invalid record size")
        stream.read_varint()  # timestamp delta
        if stream.consumed > end:
            raise CorruptRecordException("Invalid record size")
        stream.read_varint()  # offset delta
        if stream.consumed > end:
            raise CorruptRecordException("Invalid record size")

        key_len = stream.read_varint()
        if key_len >= 0:
            stream.skip(key_len)
        if stream.consumed > end:
            raise CorruptRecordException("Invalid record size")

        value_len = stream.read_varint()
        if value_len >= 0:
            stream.skip(value_len)
        if stream.consumed > end:
            raise CorruptRecordException("Invalid record size")

        header_count = stream.read_varint()
        if header_count < 0:
            raise CorruptRecordException(
                f"Found invalid number of record headers {header_count}"
            )
        while header_count:
            header_key_len = stream.read_varint()
            if header_key_len < 0:
                raise CorruptRecordException(
                    f"Invalid negative header key size {header_key_len}"
                )
            stream.skip(header_key_len)
            header_value_len = stream.read_varint()
            if header_value_len >= 0:
                stream.skip(header_value_len)
            if stream.consumed > end:
                raise CorruptRecordException("Invalid record size")
            header_count -= 1

        if stream.consumed - start != length:
            raise CorruptRecordException(
                f"Invalid record size: expected to read {length} bytes in "
                f"record payload, but instead read {stream.consumed - start}"
            )

    def _open_iteration(self) -> tuple[_GzipChunkIterator, _ChunkStream]:
        self.validate()
        chunks = self._chunks()
        return chunks, _ChunkStream(iter(chunks))

    def __iter__(self) -> Iterator[object]:
        chunks, stream = self._open_iteration()
        try:
            for _ in range(self.num_records):
                yield self._read_record(stream)
            if stream.read_byte() is not None:
                raise CorruptRecordException(
                    "unconsumed bytes after all records consumed"
                )
        finally:
            stream.close()
            chunks.close()

    def __next__(self) -> object:
        # Some callers (e.g. transaction control marker handling) use the batch
        # as its own iterator.  Reuse a single stream for the batch lifetime.
        if not self._validated:
            self.validate()
        stream = self._iteration_stream
        if stream is None:
            chunks, stream = self._open_iteration()
            self._iteration_chunks = chunks
            self._iteration_stream = stream
            self._iteration_remaining = self.num_records
        if self._iteration_remaining <= 0:
            if stream.read_byte() is not None:
                raise CorruptRecordException(
                    "unconsumed bytes after all records consumed"
                )
            self._release_iteration()
            raise StopIteration
        self._iteration_remaining -= 1
        return self._read_record(stream)

    def _release_iteration(self) -> None:
        stream = self._iteration_stream
        chunks = self._iteration_chunks
        self._iteration_stream = None
        self._iteration_chunks = None
        if stream is not None:
            stream.close()
        if chunks is not None:
            chunks.close()

    def _read_record(self, stream: _ChunkStream) -> object:
        length = stream.read_varint()
        if length < 0:
            raise CorruptRecordException(f"Invalid record size {length}")
        start = stream.consumed

        stream.read_varint()  # attributes can be skipped

        ts_delta = stream.read_varint()
        if self.timestamp_type:
            timestamp = self.max_timestamp
        else:
            timestamp = self.first_timestamp + ts_delta

        offset_delta = stream.read_varint()
        offset = self.base_offset + offset_delta

        key_len = stream.read_varint()
        if key_len >= 0:
            if key_len > length:
                raise CorruptRecordException(
                    f"Record key of {key_len} bytes exceeds the record size "
                    f"of {length} bytes"
                )
            key = stream.read_exact(key_len)
        else:
            key = None

        value_len = stream.read_varint()
        if value_len >= 0:
            if value_len > length:
                raise CorruptRecordException(
                    f"Record value of {value_len} bytes exceeds the record "
                    f"size of {length} bytes"
                )
            value = stream.read_exact(value_len)
        else:
            value = None

        header_count = stream.read_varint()
        if header_count < 0:
            raise CorruptRecordException(
                f"Found invalid number of record headers {header_count}"
            )
        headers: list[tuple[str, bytes | None]] = []
        while header_count:
            header_key_len = stream.read_varint()
            if header_key_len < 0:
                raise CorruptRecordException(
                    f"Invalid negative header key size {header_key_len}"
                )
            if stream.consumed + header_key_len > start + length:
                raise CorruptRecordException("Record header exceeds the record size")
            header_key = stream.read_exact(header_key_len).decode("utf-8")
            header_value_len = stream.read_varint()
            if header_value_len >= 0:
                if stream.consumed + header_value_len > start + length:
                    raise CorruptRecordException(
                        "Record header exceeds the record size"
                    )
                header_value = stream.read_exact(header_value_len)
            else:
                header_value = None
            headers.append((header_key, header_value))
            header_count -= 1

        if stream.consumed - start != length:
            raise CorruptRecordException(
                f"Invalid record size: expected to read {length} bytes in "
                f"record payload, but instead read {stream.consumed - start}"
            )

        return DefaultRecord(
            offset, timestamp, self.timestamp_type, key, value, headers
        )


class ProtectedMemoryRecords:
    """A MemoryRecords-compatible reader used when the guard is enabled.

    Batches that need no protection (legacy batches or v2 batches using another
    codec) are delegated to the regular implementation.  V2 gzip batches are
    represented by :class:`ProtectedGzipBatch`.
    """

    __slots__ = (
        "_buffer",
        "_check_crcs",
        "_chunk_size",
        "_max_decompressed",
        "_next_slice",
        "_pos",
        "_remaining_bytes",
    )

    LENGTH_OFFSET = struct.calcsize(">q")
    LOG_OVERHEAD = struct.calcsize(">qi")
    MAGIC_OFFSET = struct.calcsize(">qii")

    def __init__(
        self,
        bytes_data: bytes,
        *,
        max_decompressed_bytes: int,
        memory_limit: int,
        partition_count: int = 1,
        check_crcs: bool = True,
    ) -> None:
        self._buffer = bytes_data
        self._pos = 0
        self._remaining_bytes = 0
        self._next_slice: memoryview | None = None
        self._max_decompressed = max_decompressed_bytes
        self._check_crcs = check_crcs
        self._chunk_size = decompression_chunk_size(memory_limit, partition_count)
        self._cache_next()

    def size_in_bytes(self) -> int:
        return len(self._buffer)

    def _cache_next(self) -> None:
        buffer = self._buffer
        buffer_len = len(buffer)
        pos = self._pos
        remaining = buffer_len - pos
        if remaining < self.LOG_OVERHEAD:
            self._remaining_bytes = remaining
            self._next_slice = None
            return
        length = struct.unpack_from(">i", buffer, pos + self.LENGTH_OFFSET)[0]
        slice_end = pos + self.LOG_OVERHEAD + length
        if slice_end > buffer_len:
            self._remaining_bytes = remaining
            self._next_slice = None
            return
        self._next_slice = memoryview(buffer)[pos:slice_end]
        self._pos = slice_end

    def has_next(self) -> bool:
        return self._next_slice is not None

    def close(self) -> None:
        self._next_slice = None
        self._buffer = b""

    def next_batch(self) -> object | None:
        next_slice = self._next_slice
        if next_slice is None:
            return None
        if len(next_slice) < self.LOG_OVERHEAD + 1:
            raise CorruptRecordException(
                "Record size is less than the minimum record overhead"
            )
        self._cache_next()
        magic = next_slice[self.MAGIC_OFFSET]
        if magic >= 2:
            if len(next_slice) < _HEADER_SIZE:
                raise CorruptRecordException(
                    "Record batch is smaller than the v2 header"
                )
            compression = struct.unpack_from(">h", next_slice, _ATTRIBUTES_OFFSET)[0]
            compression &= _CODEC_MASK
            if compression == _CODEC_GZIP:
                return ProtectedGzipBatch(
                    next_slice,
                    max_decompressed_bytes=self._max_decompressed,
                    chunk_size=self._chunk_size,
                    check_crcs=self._check_crcs,
                )
            return DefaultRecordBatch(next_slice)
        return LegacyRecordBatch(next_slice, magic)


# The extension module exposes the same API.  Prefer it when extensions are
# enabled; fall back to the pure-Python implementation otherwise.
if not NO_EXTENSIONS:
    try:
        from ._crecords.decompression import (
            ProtectedGzipBatch as _ProtectedGzipBatchCython,
        )
        from ._crecords.decompression import (
            ProtectedMemoryRecords as _ProtectedMemoryRecordsCython,
        )
        from ._crecords.decompression import (
            decompression_chunk_size as _decompression_chunk_size_cython,
        )
    except ImportError:  # pragma: no cover
        pass
    else:
        ProtectedGzipBatch = _ProtectedGzipBatchCython  # type: ignore[assignment,misc]
        ProtectedMemoryRecords = _ProtectedMemoryRecordsCython  # type: ignore[assignment,misc]
        decompression_chunk_size = _decompression_chunk_size_cython
