"""Compatibility import path for row-level validation tracing."""

from pandera.validation_trace import *  # noqa: F403
from pandera.validation_trace import (  # noqa: F401
    RejectedItem,
    RowMappingError,
    ValidationResult,
    ValidationTrace,
    get_active_trace,
    trace_context,
    validate_with_trace,
)


def __getattr__(name: str):
    from pandera.pandas import __getattr__ as pandas_getattr

    return pandas_getattr(name)
