"""Top-level compatibility import for PostgreSQL COPY streaming."""
from sqlfluff.dialects import postgres_copy_stream as _stream
from sqlfluff.dialects.postgres_copy_stream import *  # noqa: F403
from sqlfluff.dialects.postgres_copy_stream import PostgresCopyManifestReader, CopyManifest, LoadUnit, ByteRange, parse_postgres_copy_stream, iter_load_units  # noqa: F401

def __getattr__(name):
    return getattr(_stream, name)
