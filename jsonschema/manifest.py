"""Progress-aware manifest (boxed schema) validation entry points."""

from __future__ import annotations

from jsonschema.progress import *  # noqa: F403
from jsonschema import progress as _progress


def __getattr__(name: str):
    return _progress.__getattr__(name)
