"""Utils compatibility import for PostgreSQL COPY streaming."""
from sqlfluff.dialects import postgres_copy_stream as _stream
from sqlfluff.dialects.postgres_copy_stream import *  # noqa: F403

def __getattr__(name):
    return getattr(_stream, name)
