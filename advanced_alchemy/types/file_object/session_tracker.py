# ruff: noqa: UP037
"""Application ORM configuration."""

import asyncio
import logging
import sys
from typing import TYPE_CHECKING, Any, Union

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup
else:
    from exceptiongroup import ExceptionGroup  # type: ignore[import-not-found,unused-ignore]

if TYPE_CHECKING:
    from pathlib import Path

    from advanced_alchemy.types.file_object import FileObject
    from advanced_alchemy.types.file_object.shared import SharedObjectCleanup

from advanced_alchemy.types.file_object.shared import shared_objects

logger = logging.getLogger("advanced_alchemy")


class FileObjectSessionTracker:
    """Tracks FileObject changes within a single session transaction."""

    def __init__(self, raise_on_error: bool = False) -> None:
        """Initialize empty tracking state.

        Args:
            raise_on_error: If True, raise exceptions on file operation failures.
                            If False, log warnings and continue.

        Internal structures:
        - ``pending_saves``: ``FileObject -> data`` to be saved on commit
        - ``pending_deletes``: ``FileObject`` instances to delete on commit
        - ``pending_releases``: shared ``FileObject`` instances whose reference
          is released on commit
        - ``_acquired_in_transaction``: shared references acquired at flush
          time, undone on rollback
        - ``_shared_saves``: objects whose successful save starts a new shared
          retention generation
        - ``_saved_in_transaction``: successfully saved objects used for
          selective cleanup on rollback
        """
        self.raise_on_error = raise_on_error
        # Stores objects that have pending data to be saved on commit.
        # Maps FileObject -> data source (bytes or Path)
        self.pending_saves: "dict[FileObject, Union[bytes, Path]]" = {}
        # Stores objects that should be deleted from storage on commit.
        self.pending_deletes: "set[FileObject]" = set()
        # Stores shared objects whose reference is released on commit.
        self.pending_releases: "list[FileObject]" = []
        # Stores shared objects whose reference was acquired during flush.
        self._acquired_in_transaction: "list[FileObject]" = []
        # Stores objects whose save registers a new shared retention generation.
        self._shared_saves: "set[FileObject]" = set()
        # Stores objects that were successfully saved within this transaction,
        # needed for rollback cleanup.
        self._saved_in_transaction: "set[FileObject]" = set()

    def add_pending_save(self, obj: "FileObject", data: "Union[bytes, Path]", *, shared: bool = False) -> None:
        """Mark a FileObject for saving.

        Args:
            obj: The FileObject to save on commit.
            data: The data source to persist.
            shared: Whether the object's field participates in shared retention.
        """
        self.pending_saves[obj] = data
        # If this object was previously marked for deletion, unmark it.
        self.pending_deletes.discard(obj)
        if shared:
            self._shared_saves.add(obj)
        else:
            self._shared_saves.discard(obj)

    def add_pending_delete(self, obj: "FileObject") -> None:
        """Mark a FileObject for deletion."""
        # If this object was pending save, unmark it.
        self.pending_saves.pop(obj, None)
        self._shared_saves.discard(obj)
        # Only add to pending deletes if it actually exists in storage (has a path)
        if obj.path:
            self.pending_deletes.add(obj)

    def add_pending_acquire(self, obj: "FileObject") -> None:
        """Acquire a shared retention reference for a FileObject.

        The reference is acquired immediately (during flush) so that an object
        whose bytes are already being deleted fails the transaction instead of
        committing an unreadable attachment. A matching pending release from
        the same transaction is cancelled instead.

        Args:
            obj: The shared FileObject being attached.

        Raises:
            SharedObjectUnavailableError: If the object can no longer be adopted.
        """
        for index, released in enumerate(self.pending_releases):
            if released is obj:
                del self.pending_releases[index]
                return
        shared_objects.acquire(obj.backend.key, obj.path)
        self._acquired_in_transaction.append(obj)

    def add_pending_release(self, obj: "FileObject") -> None:
        """Release a shared retention reference for a FileObject on commit.

        Args:
            obj: The shared FileObject being detached.
        """
        if not obj.path:
            return
        # If this very object was acquired within the transaction, undo the
        # acquire immediately instead of scheduling a release. Matching is
        # identity-based: a different FileObject for the same path is a
        # distinct reference (e.g. a replacement with new content).
        for index, acquired in enumerate(self._acquired_in_transaction):
            if acquired is obj:
                del self._acquired_in_transaction[index]
                shared_objects.release(obj.backend.key, obj.path)
                return
        # Attaching and detaching the same object within one transaction must
        # not save anything.
        for pending_obj in list(self.pending_saves):
            if pending_obj is obj:
                del self.pending_saves[pending_obj]
                self._shared_saves.discard(pending_obj)
                break
        self.pending_releases.append(obj)

    def commit(self) -> None:
        """Process pending saves and deletes after a successful commit."""
        for obj, data in self.pending_saves.items():
            try:
                obj.save(data)
                self._saved_in_transaction.add(obj)
                if obj in self._shared_saves:
                    shared_objects.register_save(obj.backend.key, obj.path)
            except Exception:
                if self.raise_on_error:
                    logger.exception("error saving file for object %s", obj)
                    raise
                logger.warning("error saving file for object %s", obj, exc_info=True)

        for cleanup in self._release_shared_references():
            try:
                cleanup.execute()
            except FileNotFoundError:
                pass
            except Exception:
                if self.raise_on_error:
                    logger.exception("error deleting shared file %s", cleanup.path)
                    raise
                logger.warning("error deleting shared file %s", cleanup.path, exc_info=True)

        for obj in self.pending_deletes:
            try:
                obj.delete()
            except FileNotFoundError:
                pass
            except Exception:
                if self.raise_on_error:
                    logger.exception("error deleting file for object %s", obj)
                    raise
                logger.warning("error deleting file for object %s", obj, exc_info=True)

        self.clear()

    def _release_shared_references(self) -> "list[SharedObjectCleanup]":
        """Release queued shared references and return the resulting cleanup handles."""
        cleanups: "list[SharedObjectCleanup]" = []
        for obj in self.pending_releases:
            try:
                cleanup = shared_objects.release(obj.backend.key, obj.path)
            except Exception:
                if self.raise_on_error:
                    logger.exception("error releasing shared reference for object %s", obj)
                    raise
                logger.warning("error releasing shared reference for object %s", obj, exc_info=True)
                continue
            if cleanup is not None:
                cleanups.append(cleanup)
        return cleanups

    async def commit_async(self) -> None:
        """Process pending saves and deletes after a successful commit."""

        save_items: "list[tuple[FileObject, Union[bytes, Path]]]" = list(self.pending_saves.items())
        delete_items: "list[FileObject]" = list(self.pending_deletes)

        save_results: "list[Any]" = await asyncio.gather(
            *(obj.save_async(data) for obj, data in save_items),
            return_exceptions=True,
        )
        delete_results: "list[Any]" = await asyncio.gather(
            *(obj.delete_async() for obj in delete_items),
            return_exceptions=True,
        )

        errors: list[Exception] = []

        self._handle_async_save_results(save_items, save_results, errors)

        try:
            cleanup_items = self._release_shared_references()
        except Exception as e:  # noqa: BLE001
            cleanup_items = []
            errors.append(e)

        await self._execute_shared_cleanups(cleanup_items, errors)

        for obj_to_delete, result in zip(delete_items, delete_results):
            if isinstance(result, FileNotFoundError):
                continue
            if isinstance(result, BaseException):
                if isinstance(result, Exception):
                    if self.raise_on_error:
                        logger.error(
                            "error deleting file %s",
                            obj_to_delete.path or obj_to_delete,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    else:
                        logger.warning(
                            "error deleting file %s",
                            obj_to_delete.path or obj_to_delete,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    errors.append(result)
                else:
                    raise result

        if errors and self.raise_on_error:
            if len(errors) == 1:
                raise errors[0]
            msg = "multiple FileObject operation failures"
            raise ExceptionGroup(msg, errors)
        if not errors:
            self.clear()

    def _handle_async_save_results(
        self,
        save_items: "list[tuple[FileObject, Union[bytes, Path]]]",
        save_results: "list[Any]",
        errors: "list[Exception]",
    ) -> None:
        """Collect async save failures into ``errors`` and register shared generations."""
        for (obj, _data), result in zip(save_items, save_results):
            if isinstance(result, BaseException):
                if isinstance(result, Exception):
                    if self.raise_on_error:
                        logger.error(
                            "error saving file for object %s",
                            obj,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    else:
                        # Legacy behavior: warning level
                        logger.warning(
                            "error saving file for object %s",
                            obj,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    errors.append(result)
                else:
                    # BaseException (e.g., CancelledError) - always raise
                    raise result
            else:
                self._saved_in_transaction.add(obj)
                if obj in self._shared_saves:
                    shared_objects.register_save(obj.backend.key, obj.path)

    async def _execute_shared_cleanups(
        self, cleanup_items: "list[SharedObjectCleanup]", errors: "list[Exception]"
    ) -> None:
        """Execute shared retention cleanups, collecting failures into ``errors``."""
        cleanup_results: "list[Any]" = await asyncio.gather(
            *(cleanup.execute_async() for cleanup in cleanup_items),
            return_exceptions=True,
        )
        for cleanup, result in zip(cleanup_items, cleanup_results):
            if isinstance(result, FileNotFoundError):
                continue
            if isinstance(result, BaseException):
                if isinstance(result, Exception):
                    if self.raise_on_error:
                        logger.error(
                            "error deleting shared file %s",
                            cleanup.path,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    else:
                        logger.warning(
                            "error deleting shared file %s",
                            cleanup.path,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    errors.append(result)
                else:
                    raise result

    def rollback(self) -> None:
        """Clean up files saved during a transaction that is being rolled back."""
        for obj in self._saved_in_transaction:
            if obj.path:
                try:
                    if obj in self._shared_saves:
                        # Only delete if no other shared reference keeps it alive.
                        shared_objects.collect(obj.backend.key, obj.path)
                    else:
                        obj.delete()
                except FileNotFoundError:
                    # Ignore if the file is already gone (shouldn't happen often here)
                    pass
                except Exception:
                    logger.exception("error deleting file during rollback %s", obj.path or obj)
                    raise
        self._undo_shared_acquires()
        self.clear()

    def _undo_shared_acquires(self) -> None:
        """Release shared references acquired during the rolled-back transaction."""
        for obj in self._acquired_in_transaction:
            try:
                # Discard any resulting cleanup handle: a rollback must never
                # delete bytes other transactions may still reference.
                shared_objects.release(obj.backend.key, obj.path)
            except Exception:  # noqa: BLE001
                logger.warning("error releasing shared reference during rollback %s", obj, exc_info=True)

    async def rollback_async(self) -> None:
        """Clean up files saved during a transaction that is being rolled back."""
        objects_to_delete = [obj for obj in self._saved_in_transaction if obj.path]
        shared_saved = [obj for obj in objects_to_delete if obj in self._shared_saves]
        regular_saved = [obj for obj in objects_to_delete if obj not in self._shared_saves]

        delete_results = await asyncio.gather(
            *(shared_objects.collect_async(obj.backend.key, obj.path) for obj in shared_saved),
            *(obj.delete_async() for obj in regular_saved),
            return_exceptions=True,
        )
        results_by_obj = list(zip([*shared_saved, *regular_saved], delete_results))
        self._undo_shared_acquires()
        self.clear()

        errors: list[Exception] = []
        for obj, result in results_by_obj:
            if isinstance(result, FileNotFoundError):
                continue
            if isinstance(result, BaseException):
                if isinstance(result, Exception):
                    logger.error(
                        "error deleting file during rollback %s",
                        obj.path or obj,
                        exc_info=(type(result), result, result.__traceback__),
                    )
                    errors.append(result)
                else:
                    # Propagate BaseExceptions like CancelledError
                    raise result

        if errors:
            if len(errors) == 1:
                raise errors[0]
            msg = "multiple FileObject rollback failures"
            raise ExceptionGroup(msg, errors)

    def clear(self) -> None:
        """Clear the tracker's state."""
        self.pending_saves.clear()
        self.pending_deletes.clear()
        self.pending_releases.clear()
        self._acquired_in_transaction.clear()
        self._shared_saves.clear()
        self._saved_in_transaction.clear()
