"""Singer IO reader and writer classes."""

from __future__ import annotations

__lazy_modules__ = {
    "singer_sdk.singerlib.encoding.base",
    "singer_sdk.singerlib.encoding.simple",
}

from singer_sdk.singerlib.encoding.base import (
    DryRunSingerWriter,
    GenericSingerReader,
    GenericSingerWriter,
    SingerMessageType,
)
from singer_sdk.singerlib.encoding.simple import SimpleSingerReader, SimpleSingerWriter

__all__ = [
    "DryRunSingerWriter",
    "GenericSingerReader",
    "GenericSingerWriter",
    "SimpleSingerReader",
    "SimpleSingerWriter",
    "SingerMessageType",
]
