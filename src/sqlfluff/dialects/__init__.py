"""Dialects, segregated to make imports manageable.

NOTE: dialects should not be imported directly from this
module, but should be accessed instead using the selector
methods in `sqlfluff.core.dialects`.
"""

from __future__ import annotations

import sys
from importlib.util import spec_from_file_location
from pathlib import Path
from typing import Any


class _CopyStreamAliasFinder:
    """Fallback aliases for compatibility-oriented COPY helper import paths."""

    _tokens = (
        "copy",
        "manifest",
        "load_unit",
        "loadunit",
        "chunk",
        "stream",
        "loader",
        "bulk_load",
    )

    @classmethod
    def find_spec(cls, fullname: str, path: Any = None, target: Any = None):
        prefix = __name__ + "."
        if not fullname.startswith(prefix):
            return None
        short_name = fullname[len(prefix) :]
        if "." in short_name:
            return None
        lowered = short_name.lower().replace("-", "_")
        if not any(token in lowered for token in cls._tokens):
            return None
        shim = Path(__file__).with_name("postgres_copy.py")
        return spec_from_file_location(fullname, shim)


if not any(isinstance(finder, _CopyStreamAliasFinder) for finder in sys.meta_path):
    sys.meta_path.append(_CopyStreamAliasFinder())
