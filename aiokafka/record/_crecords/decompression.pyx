#cython: language_level=3
# Bounded, streaming decompression of gzip-compressed v2 record batches.
#
# The Python implementation lives in ``aiokafka/record/decompression.py``.  The
# extension mirrors its behaviour so enabled native builds get the same guard
# without materialising the whole uncompressed batch.

import struct
import zlib

from libc.stdint cimport int32_t, int16_t, int64_t, uint32_t, uint64_t

from aiokafka.errors import CorruptRecordException, DecompressionLimitError

from .default_records cimport DefaultRecordBatch, DefaultRecord
from .legacy_records cimport LegacyRecordBatch
from aiokafka.record.default_records import DefaultRecordBatch as PyDefaultRecordBatch
from aiokafka.record.legacy_records import LegacyRecordBatch as PyLegacyRecordBatch

cdef extern from "Python.h":
    object PyMemoryView_FromObject(object obj)


DEF LOG_OVERHEAD = 12
DEF LENGTH_OFFSET = 8
DEF MAGIC_OFFSET = 16
DEF ATTRIBUTES_OFFSET = 21
DEF CODEC_MASK = 0x07
DEF CODEC_GZIP = 0x01
DEF TIMESTAMP_TYPE_MASK = 0x08
DEF TRANSACTIONAL_MASK = 0x10
DEF CONTROL_MASK = 0x20

DEF MIN_CHUNK_SIZE = 1024
DEF MAX_CHUNK_SIZE = 65536


def _validate_crc(bytes raw, unsigned int expected_crc):
    cdef:
        uint32_t crc
    from aiokafka.record.util import calc_crc32c
    return calc_crc32c(memoryview(raw)[ATTRIBUTES_OFFSET:]) == expected_crc


cdef class _GzipChunks:
    """Yield decompressed chunks from a gzip payload with a hard byte cap."""

    cdef:
        bytes _payload
        Py_ssize_t _pos
        Py_ssize_t _max_output
        Py_ssize_t _chunk_size
        Py_ssize_t _total
        object _decompressor
        bint _finished

    def __init__(self, bytes payload, Py_ssize_t max_output,
                 Py_ssize_t chunk_size):
        self._payload = payload
        self._pos = 0
        self._max_output = max_output
        self._chunk_size = chunk_size
        self._total = 0
        self._finished = False
        self._decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def __iter__(self):
        return self

    def __next__(self):
        cdef:
            Py_ssize_t remaining
            Py_ssize_t max_length
            bytes out
            bytes data
            object pending

        if self._finished:
            raise StopIteration
        decompressor = self._decompressor
        if decompressor.eof:
            unused = decompressor.unused_data
            if not unused:
                self._finished = True
                raise StopIteration
            self._decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            self._payload = unused
            self._pos = 0
            return self.__next__()

        remaining = self._max_output - self._total
        max_length = self._chunk_size
        if remaining + 1 < max_length:
            max_length = remaining + 1
        if max_length <= 0:
            raise DecompressionLimitError(
                "Decompressed record batch exceeds the configured limit of "
                "{} bytes".format(self._max_output))

        try:
            pending = decompressor.unconsumed_tail
            if pending:
                out = decompressor.decompress(pending, max_length)
            elif self._pos < len(self._payload):
                data = self._payload[self._pos:self._pos + self._chunk_size]
                self._pos = self._pos + len(data)
                out = decompressor.decompress(data, max_length)
            elif decompressor.eof:
                self._finished = True
                raise StopIteration
            else:
                out = decompressor.decompress(b"", max_length)
        except zlib.error as err:
            raise CorruptRecordException(
                "Invalid gzip compressed record batch: {}".format(err)) from err

        if not out and not pending and self._pos >= len(self._payload):
            if not decompressor.eof:
                raise CorruptRecordException(
                    "Compressed record batch ended before the gzip trailer "
                    "was seen")
            self._finished = True
            raise StopIteration

        if out:
            self._total = self._total + len(out)
            if self._total > self._max_output:
                raise DecompressionLimitError(
                    "Decompressed record batch exceeds the configured limit "
                    "of {} bytes".format(self._max_output))
            return out
        return self.__next__()


cdef class _ChunkStream:
    """Forward-only byte stream over decompressed chunks."""

    cdef:
        object _chunks
        bytearray _buffer
        Py_ssize_t _pos
        Py_ssize_t _consumed

    def __init__(self, object chunks):
        self._chunks = chunks
        self._buffer = bytearray()
        self._pos = 0
        self._consumed = 0

    def close(self):
        close = getattr(self._chunks, "close", None)
        self._chunks = None
        self._buffer = bytearray()
        self._pos = 0

    cdef inline int _fill(self) except -1:
        if self._pos < len(self._buffer):
            return 1
        self._buffer = bytearray()
        self._pos = 0
        while True:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                return 0
            if chunk:
                self._buffer = bytearray(chunk)
                return 1

    cdef int read_byte(self, int* value) except -1:
        if not self._fill():
            return 0
        value[0] = self._buffer[self._pos]
        self._pos = self._pos + 1
        self._consumed = self._consumed + 1
        return 1

    cdef int64_t read_varint(self) except? -1:
        cdef:
            int byte
            int shift = 0
            uint64_t result = 0
        while True:
            if not self.read_byte(&byte):
                raise CorruptRecordException(
                    "Compressed record batch ended in the middle of a varint")
            result |= <uint64_t>(byte & 0x7F) << shift
            if not (byte & 0x80):
                return (<int64_t>(result >> 1)) ^ -(<int64_t>(result & 1))
            shift = shift + 7
            if shift >= 64:
                raise CorruptRecordException("Out of int64 range")

    cdef skip(self, Py_ssize_t size):
        cdef:
            Py_ssize_t remaining = size
            Py_ssize_t available
            Py_ssize_t step
        if size < 0:
            raise CorruptRecordException(
                "Invalid negative size {}".format(size))
        while remaining:
            if self._pos >= len(self._buffer):
                if not self._fill():
                    raise CorruptRecordException(
                        "Record declares {} bytes but only {} bytes remain in "
                        "the batch".format(size, size - remaining))
            available = len(self._buffer) - self._pos
            step = available
            if remaining < step:
                step = remaining
            self._pos = self._pos + step
            self._consumed = self._consumed + step
            remaining = remaining - step

    cdef bytes read_exact(self, Py_ssize_t size):
        cdef:
            bytearray out
            Py_ssize_t remaining
            Py_ssize_t available
            Py_ssize_t step
        if size < 0:
            raise CorruptRecordException(
                "Invalid negative size {}".format(size))
        if size == 0:
            return b""
        out = bytearray()
        remaining = size
        while remaining:
            if self._pos >= len(self._buffer):
                if not self._fill():
                    raise CorruptRecordException(
                        "Record declares {} bytes but only {} bytes remain in "
                        "the batch".format(size, size - remaining))
            available = len(self._buffer) - self._pos
            step = available
            if remaining < step:
                step = remaining
            out.extend(self._buffer[self._pos:self._pos + step])
            self._pos = self._pos + step
            self._consumed = self._consumed + step
            remaining = remaining - step
        return bytes(out)


cdef class ProtectedGzipBatch:
    """A v2 gzip batch that is fully validated before any record is yielded."""

    cdef:
        bytes _raw
        bytes _payload
        int64_t _base_offset
        int _magic
        uint32_t _crc
        int16_t _attributes
        int32_t _last_offset_delta
        int64_t _first_timestamp
        int64_t _max_timestamp
        int64_t _producer_id
        int16_t _producer_epoch
        int32_t _base_sequence
        int32_t _num_records
        Py_ssize_t _max_output
        Py_ssize_t _chunk_size
        bint _check_crcs
        bint _validated
        char _timestamp_type
        _ChunkStream _iteration_stream
        int32_t _iteration_remaining

    def __init__(self, bytes raw, Py_ssize_t max_decompressed_bytes,
                 Py_ssize_t chunk_size, bint check_crcs=True):
        cdef:
            bytes header
        header = raw[:struct.calcsize(">qiibIhiqqqhii")]
        (
            self._base_offset,
            _length,
            _leader_epoch,
            self._magic,
            crc,
            attributes,
            self._last_offset_delta,
            self._first_timestamp,
            self._max_timestamp,
            self._producer_id,
            self._producer_epoch,
            self._base_sequence,
            self._num_records,
        ) = struct.unpack(">qiibIhiqqqhii", header)
        self._crc = crc
        self._attributes = attributes
        self._raw = raw
        self._payload = raw[len(header):]
        self._max_output = max_decompressed_bytes
        self._chunk_size = chunk_size
        self._check_crcs = check_crcs
        self._validated = False
        self._iteration_stream = None
        self._iteration_remaining = 0
        if attributes & TIMESTAMP_TYPE_MASK:
            self._timestamp_type = 1
        else:
            self._timestamp_type = 0

    @property
    def base_offset(self):
        return self._base_offset

    @property
    def magic(self):
        return self._magic

    @property
    def crc(self):
        return self._crc

    @property
    def attributes(self):
        return self._attributes

    @property
    def compression_type(self):
        return self._attributes & CODEC_MASK

    @property
    def timestamp_type(self):
        return self._timestamp_type

    @property
    def is_transactional(self):
        return bool(self._attributes & TRANSACTIONAL_MASK)

    @property
    def is_control_batch(self):
        return bool(self._attributes & CONTROL_MASK)

    @property
    def last_offset_delta(self):
        return self._last_offset_delta

    @property
    def first_timestamp(self):
        return self._first_timestamp

    @property
    def max_timestamp(self):
        return self._max_timestamp

    @property
    def producer_id(self):
        return self._producer_id

    @property
    def producer_epoch(self):
        return self._producer_epoch

    @property
    def base_sequence(self):
        return self._base_sequence

    @property
    def num_records(self):
        return self._num_records

    @property
    def next_offset(self):
        return self._base_offset + self._last_offset_delta + 1

    @property
    def protected(self):
        return True

    def validate_crc(self):
        return _validate_crc(self._raw, self._crc)

    def close(self):
        self._release_iteration()
        self._raw = b""
        self._payload = b""

    cdef _chunks(self):
        return _GzipChunks(self._payload, self._max_output, self._chunk_size)

    def validate(self):
        cdef:
            _GzipChunks chunks
            _ChunkStream stream
            int32_t count
            int byte
        if self._validated:
            return
        if self._check_crcs and not self.validate_crc():
            raise CorruptRecordException(
                "Invalid CRC - batch at offset {}".format(self._base_offset))
        if self._num_records < 0:
            raise CorruptRecordException(
                "Invalid number of records {} in batch at offset {}".format(
                    self._num_records, self._base_offset))
        chunks = self._chunks()
        stream = _ChunkStream(chunks)
        try:
            count = self._num_records
            while count:
                _skip_record(stream)
                count = count - 1
            if stream.read_byte(&byte):
                raise CorruptRecordException(
                    "unconsumed bytes after all records consumed")
        finally:
            stream.close()
        self._validated = True

    def __iter__(self):
        self.validate()
        self._release_iteration()
        self._iteration_stream = _ChunkStream(self._chunks())
        self._iteration_remaining = self._num_records
        return self

    def __next__(self):
        cdef:
            int byte
            object record
        if not self._validated:
            self.validate()
        if self._iteration_stream is None:
            self._iteration_stream = _ChunkStream(self._chunks())
            self._iteration_remaining = self._num_records
        if self._iteration_remaining <= 0:
            if self._iteration_stream.read_byte(&byte):
                raise CorruptRecordException(
                    "unconsumed bytes after all records consumed")
            self._release_iteration()
            raise StopIteration
        record = _read_record(self, self._iteration_stream)
        self._iteration_remaining = self._iteration_remaining - 1
        return record

    cdef _release_iteration(self):
        if self._iteration_stream is not None:
            self._iteration_stream.close()
            self._iteration_stream = None
        self._iteration_remaining = 0


cdef _skip_record(_ChunkStream stream):
    cdef:
        int64_t length
        Py_ssize_t start
        Py_ssize_t end
        int64_t key_len
        int64_t value_len
        int64_t header_count
        int64_t header_len
    length = stream.read_varint()
    if length < 0:
        raise CorruptRecordException("Invalid record size {}".format(length))
    start = stream._consumed
    end = start + <Py_ssize_t>length

    stream.read_varint()
    if stream._consumed > end:
        raise CorruptRecordException("Invalid record size")
    stream.read_varint()
    if stream._consumed > end:
        raise CorruptRecordException("Invalid record size")
    stream.read_varint()
    if stream._consumed > end:
        raise CorruptRecordException("Invalid record size")

    key_len = stream.read_varint()
    if key_len >= 0:
        stream.skip(<Py_ssize_t>key_len)
    if stream._consumed > end:
        raise CorruptRecordException("Invalid record size")

    value_len = stream.read_varint()
    if value_len >= 0:
        stream.skip(<Py_ssize_t>value_len)
    if stream._consumed > end:
        raise CorruptRecordException("Invalid record size")

    header_count = stream.read_varint()
    if header_count < 0:
        raise CorruptRecordException(
            "Found invalid number of record headers {}".format(header_count))
    while header_count:
        header_len = stream.read_varint()
        if header_len < 0:
            raise CorruptRecordException(
                "Invalid negative header key size {}".format(header_len))
        stream.skip(<Py_ssize_t>header_len)
        header_len = stream.read_varint()
        if header_len >= 0:
            stream.skip(<Py_ssize_t>header_len)
        if stream._consumed > end:
            raise CorruptRecordException("Invalid record size")
        header_count = header_count - 1

    if stream._consumed - start != <Py_ssize_t>length:
        raise CorruptRecordException(
            "Invalid record size: expected to read {} bytes in record "
            "payload, but instead read {}".format(
                length, stream._consumed - start))


cdef _read_record(ProtectedGzipBatch batch, _ChunkStream stream):
    cdef:
        int64_t length
        Py_ssize_t start
        Py_ssize_t end
        int64_t ts_delta
        int64_t offset_delta
        int64_t key_len
        int64_t value_len
        int64_t header_count
        int64_t header_len
        int64_t offset
        int64_t timestamp
        object key
        object value
        list headers
        object header_key
        object header_value

    length = stream.read_varint()
    if length < 0:
        raise CorruptRecordException("Invalid record size {}".format(length))
    start = stream._consumed
    end = start + <Py_ssize_t>length

    stream.read_varint()  # attributes
    ts_delta = stream.read_varint()
    if batch._timestamp_type:
        timestamp = batch._max_timestamp
    else:
        timestamp = batch._first_timestamp + ts_delta
    offset_delta = stream.read_varint()
    offset = batch._base_offset + offset_delta

    key_len = stream.read_varint()
    if key_len >= 0:
        if key_len > length:
            raise CorruptRecordException(
                "Record key of {} bytes exceeds the record size of {} "
                "bytes".format(key_len, length))
        key = stream.read_exact(<Py_ssize_t>key_len)
    else:
        key = None

    value_len = stream.read_varint()
    if value_len >= 0:
        if value_len > length:
            raise CorruptRecordException(
                "Record value of {} bytes exceeds the record size of {} "
                "bytes".format(value_len, length))
        value = stream.read_exact(<Py_ssize_t>value_len)
    else:
        value = None

    header_count = stream.read_varint()
    if header_count < 0:
        raise CorruptRecordException(
            "Found invalid number of record headers {}".format(header_count))
    headers = []
    while header_count:
        header_len = stream.read_varint()
        if header_len < 0:
            raise CorruptRecordException(
                "Invalid negative header key size {}".format(header_len))
        if stream._consumed + header_len > end:
            raise CorruptRecordException(
                "Record header exceeds the record size")
        header_key = stream.read_exact(<Py_ssize_t>header_len).decode("utf-8")
        header_len = stream.read_varint()
        if header_len >= 0:
            if stream._consumed + header_len > end:
                raise CorruptRecordException(
                    "Record header exceeds the record size")
            header_value = stream.read_exact(<Py_ssize_t>header_len)
        else:
            header_value = None
        headers.append((header_key, header_value))
        header_count = header_count - 1

    if stream._consumed - start != <Py_ssize_t>length:
        raise CorruptRecordException(
            "Invalid record size: expected to read {} bytes in record "
            "payload, but instead read {}".format(
                length, stream._consumed - start))
    return DefaultRecord.new(
        offset, timestamp, batch._timestamp_type, key, value, headers)


def decompression_chunk_size(int memory_limit, int partition_count):
    if partition_count < 1:
        partition_count = 1
    budget = memory_limit // partition_count
    chunk = budget // 4
    if chunk < MIN_CHUNK_SIZE:
        chunk = MIN_CHUNK_SIZE
    if chunk > MAX_CHUNK_SIZE:
        chunk = MAX_CHUNK_SIZE
    return chunk


cdef class ProtectedMemoryRecords:
    """MemoryRecords-compatible reader with the gzip guard enabled."""

    cdef:
        bytes _buffer
        Py_ssize_t _pos
        Py_ssize_t _max_decompressed
        Py_ssize_t _chunk_size
        bint _check_crcs

    def __init__(self, bytes bytes_data, Py_ssize_t max_decompressed_bytes,
                 Py_ssize_t memory_limit, int partition_count=1,
                 bint check_crcs=True):
        self._buffer = bytes_data
        self._pos = 0
        self._max_decompressed = max_decompressed_bytes
        self._check_crcs = check_crcs
        self._chunk_size = decompression_chunk_size(
            memory_limit, partition_count)

    def size_in_bytes(self):
        return len(self._buffer)

    def has_next(self):
        cdef:
            Py_ssize_t remaining
            Py_ssize_t length
        remaining = len(self._buffer) - self._pos
        if remaining < LOG_OVERHEAD:
            return False
        length = struct.unpack_from(">i", self._buffer, self._pos + 8)[0]
        if remaining < LOG_OVERHEAD + length:
            return False
        return True

    def next_batch(self):
        cdef:
            bytes raw
            Py_ssize_t pos
            Py_ssize_t slice_end
            Py_ssize_t length
            char magic
            int16_t compression
        if not self.has_next():
            return None
        pos = self._pos
        length = struct.unpack_from(">i", self._buffer, pos + LENGTH_OFFSET)[0]
        slice_end = pos + LOG_OVERHEAD + length
        raw = self._buffer[pos:slice_end]
        self._pos = slice_end
        magic = raw[MAGIC_OFFSET]
        if magic >= 2:
            compression = struct.unpack_from(">h", raw, ATTRIBUTES_OFFSET)[0]
            if compression & CODEC_MASK == CODEC_GZIP:
                return ProtectedGzipBatch(
                    raw, self._max_decompressed, self._chunk_size,
                    self._check_crcs)
            return PyDefaultRecordBatch(raw)
        return PyLegacyRecordBatch(raw, magic)

    def close(self):
        self._buffer = b""
        self._pos = 0


def select_protected_memory_records():
    return ProtectedMemoryRecords
