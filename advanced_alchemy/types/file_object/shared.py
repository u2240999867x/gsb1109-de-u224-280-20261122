"""Shared retention registry for ``StoredObject`` fields.

Multiple rows (and multiple fields) may reference the same stored object. For
fields that opt in via ``StoredObject(..., shared=True)``, this module tracks
references per ``(backend key, path)`` pair and only hands a delete to the
storage backend once the last reference is released.

The registry is generation aware: every save of new bytes to a tracked path
starts a new generation, and cleanup requests are only honored for the
generation they were issued for. Replaying a stale cleanup therefore never
deletes newer bytes stored at the same path. Entries are keyed per backend,
so identical paths in different storage backends are cleaned up independently.
"""

import threading
import uuid
from typing import TYPE_CHECKING, Optional, Union

from advanced_alchemy.exceptions import SharedObjectUnavailableError
from advanced_alchemy.types.file_object.registry import storages

if TYPE_CHECKING:
    from advanced_alchemy.types.file_object.base import StorageBackend

__all__ = (
    "SharedObjectCleanup",
    "SharedObjectRegistry",
    "shared_objects",
)


def _new_generation() -> str:
    """Return a unique token identifying a content generation."""
    return uuid.uuid4().hex


class _SharedEntry:
    """Internal bookkeeping for one ``(backend key, path)`` pair."""

    __slots__ = ("delete_done", "deleting", "generation", "refcount")

    def __init__(self) -> None:
        self.refcount = 0
        self.generation = _new_generation()
        self.deleting = False
        self.delete_done: Optional[threading.Event] = None


class SharedObjectCleanup:
    """A pausable, replay-safe delete request for one shared object generation.

    A cleanup handle is created when the last reference to a shared object is
    released. The delete is only handed to the storage backend when
    :meth:`execute` (or :meth:`execute_async`) runs, and only if no new
    reference was acquired in the meantime and the tracked generation still
    matches. Handles are idempotent: executing the same handle twice, or
    executing a handle whose generation has been superseded, never deletes.
    """

    __slots__ = ("_executed", "_lock", "_registry", "backend_key", "generation", "path")

    def __init__(self, registry: "SharedObjectRegistry", backend_key: str, path: str, generation: str) -> None:
        self._registry = registry
        self.backend_key = backend_key
        self.path = path
        self.generation = generation
        self._executed = False
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return (
            f"SharedObjectCleanup(backend_key={self.backend_key!r}, path={self.path!r}, "
            f"generation={self.generation!r}, executed={self._executed})"
        )

    @property
    def executed(self) -> bool:
        """Whether this handle has already been executed."""
        return self._executed

    def _mark_executed(self) -> bool:
        with self._lock:
            if self._executed:
                return False
            self._executed = True
            return True

    def execute(self) -> bool:
        """Hand the delete to the storage backend if it is still valid.

        Returns:
            bool: True if the backend delete was performed, False if the
            cleanup was aborted (already executed, new references exist, or
            the generation was superseded).
        """
        if not self._mark_executed():
            return False
        return self._registry._execute_delete(self.backend_key, self.path, self.generation)  # noqa: SLF001

    async def execute_async(self) -> bool:
        """Async variant of :meth:`execute`."""
        if not self._mark_executed():
            return False
        return await self._registry._execute_delete_async(self.backend_key, self.path, self.generation)  # noqa: SLF001


class SharedObjectRegistry:
    """Tracks references to shared stored objects and guards their deletion.

    All methods are thread-safe. The registry is process-local; it coordinates
    cleanup between sessions in this process.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, str], _SharedEntry] = {}

    @staticmethod
    def _key(backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> "tuple[str, str]":
        backend_key = backend if isinstance(backend, str) else backend.key
        return backend_key, str(path)

    def register_save(self, backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> str:
        """Register that new bytes were stored at ``path``, starting a new generation.

        Any cleanup issued for a previous generation becomes a no-op.

        Args:
            backend: The storage backend or its registry key.
            path: The object path within the backend.

        Returns:
            str: The new generation token.
        """
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _SharedEntry()
                self._entries[key] = entry
            entry.generation = _new_generation()
            return entry.generation

    def acquire(
        self,
        backend: "Union[str, StorageBackend]",
        path: "Union[str, object]",
        *,
        timeout: "Optional[float]" = None,
    ) -> str:
        """Register a new reference to the object and return its generation.

        If a delete for the current generation is already being handed to the
        storage backend, wait for it to finish and then refuse the adoption:
        the previous bytes can no longer be guaranteed to exist.

        Args:
            backend: The storage backend or its registry key.
            path: The object path within the backend.
            timeout: Optional maximum seconds to wait for an in-flight delete.

        Raises:
            SharedObjectUnavailableError: If the object was deleted (or its
                generation superseded) while waiting for an in-flight delete,
                or when the wait times out.

        Returns:
            str: The generation the reference was acquired for.
        """
        key = self._key(backend, path)
        waited_generation: Optional[str] = None
        while True:
            done: Optional[threading.Event] = None
            with self._lock:
                entry = self._entries.get(key)
                if entry is None:
                    if waited_generation is not None:
                        msg = (
                            f"Shared object '{key[1]}' in backend '{key[0]}' was deleted while waiting; "
                            "it can no longer be adopted."
                        )
                        raise SharedObjectUnavailableError(msg)
                    entry = _SharedEntry()
                    self._entries[key] = entry
                if entry.deleting:
                    waited_generation = entry.generation
                    done = entry.delete_done
                elif waited_generation is not None and entry.generation != waited_generation:
                    msg = (
                        f"Shared object '{key[1]}' in backend '{key[0]}' was replaced while waiting; "
                        "the previous content can no longer be adopted."
                    )
                    raise SharedObjectUnavailableError(msg)
                else:
                    entry.refcount += 1
                    return entry.generation
            if done is not None and not done.wait(timeout):
                msg = f"Timed out waiting for the pending delete of shared object '{key[1]}' in backend '{key[0]}'."
                raise SharedObjectUnavailableError(msg)

    def release(
        self, backend: "Union[str, StorageBackend]", path: "Union[str, object]"
    ) -> "Optional[SharedObjectCleanup]":
        """Release one reference to the object.

        Args:
            backend: The storage backend or its registry key.
            path: The object path within the backend.

        Returns:
            A :class:`SharedObjectCleanup` handle if this was the last
            reference, otherwise None. The handle only deletes when executed.
        """
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.refcount > 0:
                entry.refcount -= 1
            if entry.refcount == 0 and not entry.deleting:
                return SharedObjectCleanup(self, key[0], key[1], entry.generation)
            return None

    def collect(self, backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> bool:
        """Delete the object immediately if it is tracked and unreferenced.

        Args:
            backend: The storage backend or its registry key.
            path: The object path within the backend.

        Returns:
            bool: True if the backend delete was performed.
        """
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            generation = entry.generation if entry is not None else None
        if generation is None:
            return False
        return self._execute_delete(key[0], key[1], generation)

    async def collect_async(self, backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> bool:
        """Async variant of :meth:`collect`."""
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            generation = entry.generation if entry is not None else None
        if generation is None:
            return False
        return await self._execute_delete_async(key[0], key[1], generation)

    def reference_count(self, backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> int:
        """Return the number of live references tracked for the object."""
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            return entry.refcount if entry is not None else 0

    def current_generation(self, backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> "Optional[str]":
        """Return the current generation token for the object, if tracked."""
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            return entry.generation if entry is not None else None

    def is_delete_in_progress(self, backend: "Union[str, StorageBackend]", path: "Union[str, object]") -> bool:
        """Return whether a backend delete for the object is currently in flight."""
        key = self._key(backend, path)
        with self._lock:
            entry = self._entries.get(key)
            return bool(entry is not None and entry.deleting)

    def clear(self) -> None:
        """Drop all tracked state. Intended for tests."""
        with self._lock:
            self._entries.clear()

    def _begin_delete(self, backend_key: str, path: str, generation: str) -> "Optional[_SharedEntry]":
        """Atomically validate and mark an entry as deleting.

        The check and the mark happen under the registry lock, so a reference
        acquired before this point cancels the delete, while a reference
        requested after this point waits for the delete outcome.
        """
        key = (backend_key, path)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.generation != generation or entry.refcount > 0 or entry.deleting:
                return None
            entry.deleting = True
            entry.delete_done = threading.Event()
            return entry

    def _finish_delete(self, backend_key: str, path: str, generation: str, entry: "_SharedEntry") -> None:
        key = (backend_key, path)
        with self._lock:
            done = entry.delete_done
            if self._entries.get(key) is entry:
                if entry.generation == generation:
                    del self._entries[key]
                else:
                    # Content was replaced while the delete was in flight; keep
                    # tracking the newer generation.
                    entry.deleting = False
                    entry.delete_done = None
        if done is not None:
            done.set()

    def _execute_delete(self, backend_key: str, path: str, generation: str) -> bool:
        entry = self._begin_delete(backend_key, path, generation)
        if entry is None:
            return False
        try:
            storages.get_backend(backend_key).delete_object(path)
        finally:
            self._finish_delete(backend_key, path, generation, entry)
        return True

    async def _execute_delete_async(self, backend_key: str, path: str, generation: str) -> bool:
        entry = self._begin_delete(backend_key, path, generation)
        if entry is None:
            return False
        try:
            await storages.get_backend(backend_key).delete_object_async(path)
        finally:
            self._finish_delete(backend_key, path, generation, entry)
        return True


shared_objects = SharedObjectRegistry()
"""Process-wide shared object retention registry."""
