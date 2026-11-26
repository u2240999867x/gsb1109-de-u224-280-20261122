"""Tests for streaming PostgreSQL COPY manifest extraction."""

from __future__ import annotations

import hashlib
import io

import pytest

from sqlfluff.dialects.postgres_copy_stream import (
    PostgresCopyManifestReader,
    parse_postgres_copy_stream,
)


def _sample_script() -> tuple[bytes, bytes]:
    before = (
        b"SELECT $tag$ COPY fake FROM STDIN;\n"
        b"quoted ; semicolon\n"
        b"\\.$tag$;\n"
    )
    header = (
        b"COPY public.load_target (\n"
        b"    id,\n"
        b"    note\n"
        b") FROM STDIN WITH (FORMAT text, DELIMITER ';');\r\n"
    )
    payload = (
        b"1;plain text containing ; semicolon\r\n"
        b"2;record that looks like COPY other FROM STDIN;\r\n"
        b"\\\\.\r\n"
        b"3;\xe4\xb8\xad\xe6\x96\x87\r\n"
    )
    frame = b"\\.\r\n"
    after = b"SELECT 42;\n"
    return before + header + payload + frame + after, payload


def _assert_sample_manifest(result, script: bytes, payload: bytes) -> None:
    assert len(result.manifests) == 1
    unit = result[0]
    manifest = result.manifests[0]

    assert unit.is_complete
    assert unit.kind == "complete"
    assert unit.kind == "complete_data_segment"
    assert manifest.payload_range is not None
    assert manifest.payload_slice(script) == payload
    assert manifest.byte_range == (
        manifest.payload_range.start,
        manifest.payload_range.end,
    )
    assert manifest.digest == hashlib.sha256(payload).hexdigest()
    assert manifest.sha256 == hashlib.sha256(payload).hexdigest()
    assert manifest.row_count == 4
    assert manifest.delimiter == ";"
    assert manifest.confirmed is True
    assert result.reader.offset == len(script)
    assert manifest.verify(script).ok


def test_copy_manifest_matches_for_every_chunk_size() -> None:
    script, payload = _sample_script()
    baseline = parse_postgres_copy_stream(script)
    baseline_manifest = baseline.manifests[0]

    _assert_sample_manifest(baseline, script, payload)

    for chunk_size in range(1, 65):
        result = parse_postgres_copy_stream(script, chunk_size=chunk_size)
        manifest = result.manifests[0]
        assert manifest.payload_range == baseline_manifest.payload_range
        assert manifest.digest == baseline_manifest.digest
        assert manifest.row_count == baseline_manifest.row_count
        assert result[-1].kind == "complete"

    reader = PostgresCopyManifestReader()
    for byte in script:
        reader.feed(bytes([byte]))
    bytewise = reader.finish()
    manifest = bytewise.manifests[0]
    assert manifest.payload_range == baseline_manifest.payload_range
    assert manifest.digest == baseline_manifest.digest
    assert manifest.row_count == baseline_manifest.row_count
    assert bytewise.reader.offset == len(script)


def test_copy_manifest_accepts_binary_stream_and_callbacks() -> None:
    script, payload = _sample_script()
    seen = []
    result = parse_postgres_copy_stream(
        io.BytesIO(script),
        chunk_size=13,
        on_unit=seen.append,
    )

    _assert_sample_manifest(result, script, payload)
    assert len(seen) == 1
    assert seen[0].manifest is result.manifests[0]


def test_empty_inline_batch_differs_from_header_only_external_copy() -> None:
    empty_script = b"COPY empty_target FROM STDIN;\r\n\\.\r\n"
    empty_result = parse_postgres_copy_stream(empty_script)
    empty_manifest = empty_result.manifests[0]

    assert empty_result[0].is_complete
    assert empty_manifest.payload_range is not None
    assert empty_manifest.payload_range.length == 0
    assert empty_manifest.row_count == 0
    assert empty_manifest.digest == hashlib.sha256(b"").hexdigest()

    external_script = b"COPY external_target FROM STDIN;\r\n"
    external_result = parse_postgres_copy_stream(external_script)
    external_manifest = external_result.manifests[0]

    assert external_result[0].is_external
    assert external_result[0].kind == "external_stream_copy"
    assert external_manifest.is_external
    assert external_manifest.payload_range is None
    assert external_manifest.row_count == 0


def test_completed_previous_copy_is_delivered_before_truncated_batch() -> None:
    first_payload = b"one;a\r\ntwo;b\r\n"
    first = b"COPY first FROM STDIN;\r\n" + first_payload + b"\\.\r\n"
    second_payload = b"three;c\r\nfour;d\r\n"
    second_header = b"COPY second FROM STDIN;\r\n"
    script = first + second_header + second_payload

    emitted = []
    reader = PostgresCopyManifestReader(on_unit=emitted.append)
    cut = first.index(b"\\.\r\n") + 3
    reader.feed(script[:cut])
    assert len(emitted) == 1
    assert emitted[0].is_complete
    assert emitted[0].manifest.digest == hashlib.sha256(first_payload).hexdigest()

    reader.feed(script[cut:])
    result = reader.finish()

    assert len(result) == 2
    assert result[0].is_complete
    assert result[1].is_truncated
    assert result[1].kind == "incomplete"
    truncated = result[1].manifest
    assert truncated is not None
    assert truncated.confirmed is False
    assert truncated.is_truncated is True
    assert truncated.payload_slice(script) == second_payload
    assert truncated.digest == hashlib.sha256(second_payload).hexdigest()
    assert truncated.complete_row_count == 2


def test_manifest_declares_external_copies_before_inline_truncation() -> None:
    script = (
        b"COPY one FROM STDIN;\n"
        b"COPY two FROM STDIN;\n"
        b"COPY three FROM STDIN;\n"
        b"only;row\n"
    )
    result = parse_postgres_copy_stream(
        script,
        manifests=[
            {"external": True},
            {"mode": "external_stream_copy"},
            {"mode": "inline"},
        ],
    )

    assert [str(unit.kind) for unit in result] == [
        "external_stream_copy",
        "external_stream_copy",
        "incomplete_batch",
    ]
    assert [manifest.index for manifest in result.manifests] == [0, 1, 2]
    assert result[2].manifest is not None
    assert result[2].manifest.is_truncated


def test_postgres_segment_exposes_manifest() -> None:
    from sqlfluff.core import Linter

    script, payload = _sample_script()
    parsed = Linter(dialect="postgres").parse_string(script.decode("utf-8"))
    assert parsed.tree is not None

    segments = list(
        parsed.tree.recursive_crawl("postgres_copy_stdin_data_statement")
    )
    assert segments
    segment = segments[0]
    manifest = segment.copy_manifest
    normalized_payload = payload.replace(b"\r\n", b"\n")
    assert manifest is not None
    assert manifest.row_count == 4
    assert manifest.payload_digest == hashlib.sha256(normalized_payload).hexdigest()
    assert segment.payload_byte_range == manifest.payload_range


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
