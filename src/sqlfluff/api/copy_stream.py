"""Compatibility API for streaming PostgreSQL COPY manifests."""

from sqlfluff.dialects import postgres_copy_stream as _stream
from sqlfluff.dialects.postgres_copy_stream import *  # noqa: F403
from sqlfluff.dialects.postgres_copy_stream import (  # noqa: F401
    CopyManifest,
    LoadUnit,
    PostgresCopyManifestReader,
    iter_load_units,
    parse_postgres_copy_stream,
)


def __getattr__(name: str):
    return getattr(_stream, name)
