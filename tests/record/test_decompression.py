import gzip
import struct

import pytest

from aiokafka.errors import (
    CorruptRecordException,
    DecompressionLimitError,
    RecordTooLargeError,
)
from aiokafka.record.decompression import (
    DEFAULT_DECOMPRESSION_MEMORY_LIMIT,
    DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
    ProtectedMemoryRecords,
)
from aiokafka.record.util import calc_crc32c

KIB = 1024
MIB = 1024 * KIB

HEADER_SIZE = struct.calcsize(">qiibIhiqqqhii")
ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
CRC_OFFSET = struct.calcsize(">qiib")
CODEC_GZIP = 0x01


def encode_varint(value):
    value = (value << 1) ^ (value >> 63)
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def encode_record(offset, key, value, headers=(), timestamp_delta=0):
    body = bytearray()
    body.append(0)  # attributes
    body += encode_varint(timestamp_delta)
    body += encode_varint(offset)
    if key is None:
        body += encode_varint(-1)
    else:
        body += encode_varint(len(key))
        body += key
    if value is None:
        body += encode_varint(-1)
    else:
        body += encode_varint(len(value))
        body += value
    body += encode_varint(len(headers))
    for header_key, header_value in headers:
        key_bytes = header_key.encode()
        body += encode_varint(len(key_bytes)) + key_bytes
        if header_value is None:
            body += encode_varint(-1)
        else:
            body += encode_varint(len(header_value)) + header_value
    return encode_varint(len(body)) + bytes(body)


def build_batch(base_offset, records, *, compression=CODEC_GZIP, num_records=None):
    body = b"".join(records)
    if compression == CODEC_GZIP:
        payload = gzip.compress(body)
    else:
        payload = body
    if num_records is None:
        num_records = len(records)

    header = bytearray(HEADER_SIZE)
    struct.pack_into(">q", header, 0, base_offset)
    struct.pack_into(">i", header, 8, HEADER_SIZE - 12 + len(payload))
    struct.pack_into(">i", header, 12, -1)  # leader epoch
    header[16] = 2  # magic
    struct.pack_into(">h", header, 21, compression)
    struct.pack_into(">i", header, 23, max(num_records - 1, 0))  # last offset delta
    struct.pack_into(">q", header, 27, 1_000_000)
    struct.pack_into(">q", header, 35, 1_000_000 + max(num_records - 1, 0))
    struct.pack_into(">q", header, 43, -1)  # producer id
    struct.pack_into(">h", header, 51, -1)  # producer epoch
    struct.pack_into(">i", header, 53, -1)  # base sequence
    struct.pack_into(">i", header, 57, num_records)
    raw = bytes(header) + payload
    struct.pack_into(">I", header, CRC_OFFSET, calc_crc32c(raw[ATTRIBUTES_OFFSET:]))
    return bytes(header) + payload


def read_all(batch):
    return [
        (record.offset, record.timestamp, record.key, record.value, record.headers)
        for record in batch
    ]


def make_reader(data, *, limit=64 * MIB, memory=8 * MIB):
    return ProtectedMemoryRecords(
        data, max_decompressed_bytes=limit, memory_limit=memory
    )


def test_defaults_match_task_requirements():
    assert DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES == 64 * MIB
    assert DEFAULT_DECOMPRESSION_MEMORY_LIMIT == 8 * MIB


def test_preserves_record_fields():
    headers = [("trace", b"abc"), ("empty", None)]
    records = [
        encode_record(0, b"k0", b"v0"),
        encode_record(1, None, b"v1", timestamp_delta=1),
        encode_record(2, b"k2", None, headers, timestamp_delta=2),
    ]
    raw = build_batch(10, records)

    batch = make_reader(raw).next_batch()
    batch.validate()
    assert read_all(batch) == [
        (10, 1_000_000, b"k0", b"v0", []),
        (11, 1_000_001, None, b"v1", []),
        (12, 1_000_002, b"k2", None, headers),
    ]


def test_limits_are_configurable():
    records = [encode_record(i, None, b"x") for i in range(10)]
    raw = build_batch(0, records)

    with pytest.raises(DecompressionLimitError) as error:
        make_reader(raw, limit=1).next_batch().validate()
    assert isinstance(error.value, CorruptRecordException)
    assert isinstance(error.value, RecordTooLargeError)

    assert len(list(make_reader(raw, limit=64 * MIB).next_batch())) == 10


def test_corrupt_crc_is_rejected_before_any_record():
    raw = bytearray(build_batch(0, [encode_record(0, None, b"x")]))
    raw[-1] ^= 0xFF
    batch = make_reader(bytes(raw)).next_batch()
    with pytest.raises(CorruptRecordException):
        list(batch)


def test_corrupt_gzip_is_rejected():
    raw = bytearray(build_batch(0, [encode_record(0, None, b"x" * 100)]))
    raw[HEADER_SIZE + 12] ^= 0xFF  # break the deflate stream
    struct.pack_into(">I", raw, CRC_OFFSET, calc_crc32c(raw[ATTRIBUTES_OFFSET:]))
    batch = make_reader(bytes(raw)).next_batch()
    with pytest.raises(CorruptRecordException):
        list(batch)


def test_declared_length_larger_than_body_fails():
    # One record claims a payload far larger than the gzip body provides.
    oversized = bytearray(encode_record(0, None, b"x"))
    declared = b"\xfe\xff\xff\x03"  # zig-zag varint well above the body size
    oversized = declared + bytes(oversized[len(encode_varint(3)) :])
    raw = build_batch(0, [bytes(oversized)])
    batch = make_reader(raw).next_batch()
    with pytest.raises(CorruptRecordException):
        list(batch)


def test_memory_stays_bounded_for_many_batches():
    records = [encode_record(i, None, b"\x00" * (64 * KIB)) for i in range(16)]
    raw = build_batch(0, records)
    assert len(raw) < 64 * KIB  # highly compressible

    data = raw * 8
    reader = make_reader(data)
    batches = []
    while reader.has_next():
        batch = reader.next_batch()
        batch.validate()
        batches.append(batch)
    assert len(batches) == 8
    for batch in batches:
        assert batch.num_records == 16
        assert len(list(batch)) == 16


def test_gzip_payload_trailing_garbage_is_rejected():
    raw = build_batch(0, [encode_record(0, None, b"x")])
    # Append bytes after a complete gzip member; gzip treats them as another
    # member, which is not valid deflate here.
    payload = raw[HEADER_SIZE:] + b"garbage"
    header = bytearray(raw[:HEADER_SIZE])
    struct.pack_into(">i", header, 8, HEADER_SIZE - 12 + len(payload))
    raw = bytes(header) + payload
    header = bytearray(raw[:HEADER_SIZE])
    struct.pack_into(">I", header, CRC_OFFSET, calc_crc32c(raw[ATTRIBUTES_OFFSET:]))
    batch = make_reader(bytes(header) + payload).next_batch()
    with pytest.raises(CorruptRecordException):
        list(batch)
