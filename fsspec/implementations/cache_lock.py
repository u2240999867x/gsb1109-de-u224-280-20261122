from __future__ import annotations

import hashlib
import os
import threading
from contextlib import contextmanager

try:
    import fcntl
except ImportError:  # non-POSIX
    fcntl = None
try:
    import msvcrt
except ImportError:  # non-Windows
    msvcrt = None


class _CrossProcessLock:
    """A lock that serializes threads in this process and other processes.

    The in-process lock makes acquisition thread-safe; the OS file lock (POSIX
    ``flock`` or Windows byte-range lock) coordinates processes sharing one
    cache directory. The lock file itself is never deleted, so it can never be
    recreated underneath a waiting holder.
    """

    def __init__(self, path):
        self.path = path
        self._thread_lock = threading.Lock()
        self._fh = None

    def acquire(self):
        self._thread_lock.acquire()
        fh = open(self.path, "a+")
        try:
            if fcntl is not None:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            elif msvcrt is not None:
                # msvcrt locks a byte range, so the file needs one byte.
                fh.seek(0, os.SEEK_END)
                if fh.tell() == 0:
                    fh.write(b"\0")
                    fh.flush()
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)
            self._fh = fh
        except BaseException:
            fh.close()
            self._thread_lock.release()
            raise

    def release(self):
        fh = self._fh
        self._fh = None
        try:
            if fcntl is not None:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            fh.close()
            self._thread_lock.release()

    @contextmanager
    def __call__(self):
        self.acquire()
        try:
            yield
        finally:
            self.release()


class CrossProcessLockRegistry:
    """One :class:`_CrossProcessLock` per lock key inside a cache directory."""

    def __init__(self, directory):
        self.directory = directory
        self._locks = {}
        self._guard = threading.Lock()

    def __call__(self, key):
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                safe_key = hashlib.sha256(key.encode()).hexdigest()[:16]
                lock = _CrossProcessLock(
                    os.path.join(self.directory, f".{safe_key}.lock")
                )
                self._locks[key] = lock
        return lock()


_registries = {}
_registries_guard = threading.Lock()


def cache_locks(directory):
    """Return the process-wide lock registry for a cache storage directory."""
    with _registries_guard:
        registry = _registries.get(directory)
        if registry is None:
            registry = CrossProcessLockRegistry(directory)
            _registries[directory] = registry
        return registry
