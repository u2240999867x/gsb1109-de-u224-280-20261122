"""
fsspec user-defined exception classes
"""

import asyncio


class BlocksizeMismatchError(ValueError):
    """
    Raised when a cached file is opened with a different blocksize than it was
    written with
    """


class FSTimeoutError(asyncio.TimeoutError):
    """
    Raised when a fsspec function timed out occurs
    """


class UnconfirmedWriteError(OSError):
    """
    Raised when reading a file whose write was part of a bulk upload that
    failed before every target could be confirmed. The affected target may
    or may not have been published remotely, so the cache refuses to serve
    any local copy until a successful backend read resolves it.
    """
