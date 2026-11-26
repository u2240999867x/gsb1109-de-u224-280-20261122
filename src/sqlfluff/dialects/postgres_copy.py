"""Compatibility import path for streaming PostgreSQL COPY manifests."""

from sqlfluff.dialects import postgres_copy_stream as _stream
from sqlfluff.dialects.postgres_copy_stream import *  # noqa: F403
from sqlfluff.dialects.postgres_copy_stream import (  # noqa: F401
    ByteRange,
    COMPLETE_DATA_SEGMENT,
    CopyManifest,
    CopyManifestReader,
    CopyScanner,
    CopyStreamParser,
    DigestValue,
    EXTERNAL_STREAM_COPY,
    INCOMPLETE_BATCH,
    LoadResult,
    LoadUnit,
    LoadUnitKind,
    LoadUnitReader,
    PostgresCopyManifestReader,
    PostgresCopyScanner,
    PostgresCopyStreamParser,
    StreamingCopyParser,
    VerificationResult,
    count_copy_rows,
    inspect_copy_header,
    iter_copy_manifests,
    iter_load_units,
    parse_postgres_copy_stream,
    scan_copy_script,
    verify_manifest,
)


def __getattr__(name: str):
    return getattr(_stream, name)
