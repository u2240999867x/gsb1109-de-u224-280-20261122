# ruff: noqa: UP037
"""Application ORM configuration."""

import asyncio
import logging
import sys
from typing import TYPE_CHECKING, Any, Optional, Union

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup
else:
    from exceptiongroup import ExceptionGroup  # type: ignore[import-not-found,unused-ignore]

if TYPE_CHECKING:
    from pathlib import Path

    from advanced_alchemy.types.file_object import FileObject

logger = logging.getLogger("advanced_alchemy")


class _SavepointSnapshot:
    """Immutable tracker state used when rolling back to a savepoint."""

    __slots__ = ("pending_deletes", "pending_saves", "save_attempts", "saved_in_transaction")

    def __init__(
        self,
        *,
        pending_saves: "dict[FileObject, Union[bytes, Path]]",
        pending_deletes: "set[FileObject]",
        saved_in_transaction: "set[FileObject]",
        save_attempts: "set[FileObject]",
    ) -> None:
        self.pending_saves = pending_saves
        self.pending_deletes = pending_deletes
        self.saved_in_transaction = saved_in_transaction
        self.save_attempts = save_attempts


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
        - ``_saved_in_transaction``: successfully saved objects used for
          selective cleanup on rollback
        """
        self.raise_on_error = raise_on_error
        # Stores objects that have pending data to be saved on commit.
        # Maps FileObject -> data source (bytes or Path)
        self.pending_saves: "dict[FileObject, Union[bytes, Path]]" = {}
        # Stores objects that should be deleted from storage on commit.
        self.pending_deletes: "set[FileObject]" = set()
        # Stores objects that were successfully saved within this transaction,
        # needed for rollback cleanup.
        self._saved_in_transaction: "set[FileObject]" = set()
        # Stores objects whose save operation started. A backend may create
        # the remote object and then fail while reading its metadata, so these
        # must also be removed when the containing root transaction aborts.
        self._save_attempts: "set[FileObject]" = set()
        # Snapshots keyed by id() of SQLAlchemy's nested SessionTransaction.
        self._savepoints: "dict[int, _SavepointSnapshot]" = {}

    def _snapshot_state(self) -> _SavepointSnapshot:
        """Return a copy of the mutable tracker state."""
        return _SavepointSnapshot(
            pending_saves=dict(self.pending_saves),
            pending_deletes=set(self.pending_deletes),
            saved_in_transaction=set(self._saved_in_transaction),
            save_attempts=set(self._save_attempts),
        )

    def _restore_state(self, snapshot: _SavepointSnapshot) -> None:
        """Restore tracker state from a savepoint snapshot."""
        self.pending_saves.clear()
        self.pending_saves.update(snapshot.pending_saves)
        self.pending_deletes.clear()
        self.pending_deletes.update(snapshot.pending_deletes)
        self._saved_in_transaction.clear()
        self._saved_in_transaction.update(snapshot.saved_in_transaction)
        self._save_attempts.clear()
        self._save_attempts.update(snapshot.save_attempts)

    def capture_savepoint(self, transaction: Any) -> None:
        """Capture tracker state when a nested transaction is created."""
        if getattr(transaction, "nested", False) is not True:
            return
        transaction_id = id(transaction)
        if transaction_id in self._savepoints:
            return
        self._savepoints[transaction_id] = self._snapshot_state()

    def capture_transaction_stack(self, transaction: Any) -> None:
        """Capture any active savepoints that do not yet have snapshots.

        This supports callers that only register ``before_flush`` and do not
        listen for transaction creation events. Missing ancestor snapshots are
        taken before the current flush mutates tracker state.
        """
        current = transaction
        while current is not None:
            self.capture_savepoint(current)
            current = getattr(current, "_parent", None)

    def release_savepoint(self, transaction: Any) -> None:
        """Discard a savepoint snapshot after its changes are released."""
        if getattr(transaction, "nested", False) is True:
            self._savepoints.pop(id(transaction), None)

    def rollback_to_savepoint(self, transaction: Any) -> bool:
        """Restore tracker state captured for a rolled-back savepoint."""
        if getattr(transaction, "nested", False) is not True:
            return False

        transaction_id = id(transaction)
        keys = list(self._savepoints)
        try:
            index = keys.index(transaction_id)
        except ValueError:
            return False

        self._restore_state(self._savepoints[transaction_id])
        for key in keys[index:]:
            self._savepoints.pop(key, None)
        return True

    def add_pending_save(self, obj: "FileObject", data: "Union[bytes, Path]") -> None:
        """Mark a FileObject for saving."""
        self.pending_saves[obj] = data
        # If this object was previously marked for deletion, unmark it.
        self.pending_deletes.discard(obj)

    def add_pending_delete(self, obj: "FileObject") -> None:
        """Mark a FileObject for deletion."""
        # If this object was pending save, unmark it.
        self.pending_saves.pop(obj, None)
        # Only add to pending deletes if it actually exists in storage (has a path)
        if obj.path:
            self.pending_deletes.add(obj)

    def commit(self) -> None:
        """Process pending saves and deletes after a successful commit."""
        for obj, data in self.pending_saves.items():
            self._save_attempts.add(obj)
            try:
                obj.save(data)
                self._saved_in_transaction.add(obj)
            except Exception:
                if self.raise_on_error:
                    logger.exception("error saving file for object %s", obj)
                    raise
                logger.warning("error saving file for object %s", obj, exc_info=True)

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

    async def commit_async(self) -> None:
        """Process pending saves and deletes after a successful commit."""

        save_items: "list[tuple[FileObject, Union[bytes, Path]]]" = list(self.pending_saves.items())
        delete_items: "list[FileObject]" = list(self.pending_deletes)

        async def save_one(obj: "FileObject", data: "Union[bytes, Path]") -> Any:
            self._save_attempts.add(obj)
            return await obj.save_async(data)

        save_results: "list[Any]" = await asyncio.gather(
            *(save_one(obj, data) for obj, data in save_items),
            return_exceptions=True,
        )
        delete_results: "list[Any]" = await asyncio.gather(
            *(obj.delete_async() for obj in delete_items),
            return_exceptions=True,
        )

        errors: list[Exception] = []

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

    def rollback(self) -> None:
        """Clean up files saved during a transaction that is being rolled back."""
        objects_to_delete = set(self._saved_in_transaction)
        objects_to_delete.update(self._save_attempts)
        for obj in objects_to_delete:
            if obj.path:
                try:
                    obj.delete()
                except FileNotFoundError:
                    # Ignore if the file is already gone (shouldn't happen often here)
                    pass
                except Exception:
                    logger.exception("error deleting file during rollback %s", obj.path or obj)
                    self.clear()
                    raise
        self.clear()

    async def rollback_async(self) -> None:
        """Clean up files saved during a transaction that is being rolled back."""
        objects_to_delete_set = set(self._saved_in_transaction)
        objects_to_delete_set.update(self._save_attempts)
        objects_to_delete = [obj for obj in objects_to_delete_set if obj.path]
        if not objects_to_delete:
            self.clear()
            return

        delete_results = await asyncio.gather(
            *(obj.delete_async() for obj in objects_to_delete),
            return_exceptions=True,
        )

        errors: list[Exception] = []
        for obj, result in zip(objects_to_delete, delete_results):
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

        self.clear()
        if errors:
            if len(errors) == 1:
                raise errors[0]
            msg = "multiple FileObject rollback failures"
            raise ExceptionGroup(msg, errors)

    def clear(self) -> None:
        """Clear the tracker's state."""
        self.pending_saves.clear()
        self.pending_deletes.clear()
        self._saved_in_transaction.clear()
        self._save_attempts.clear()
        self._savepoints.clear()

    def prepare_commit(self) -> None:
        """Upload pending files before the database transaction is committed.

        Old files remain in ``pending_deletes`` until the database commit has
        succeeded. If an upload fails with ``raise_on_error`` enabled, files
        already attempted in this prepare phase are removed before the error is
        propagated.
        """
        for obj, data in list(self.pending_saves.items()):
            self._save_attempts.add(obj)
            try:
                obj.save(data)
            except Exception:
                if self.raise_on_error:
                    logger.exception("error saving file for object %s", obj)
                    self._cleanup_failed_prepare()
                    raise
                logger.warning("error saving file for object %s", obj, exc_info=True)
            else:
                self._saved_in_transaction.add(obj)

        self.pending_saves.clear()

    def _cleanup_failed_prepare(self) -> None:
        """Best-effort compensation after a synchronous prepare failure."""
        try:
            self.rollback()
        except Exception:
            logger.exception("error cleaning up files after failed commit prepare")

    async def prepare_commit_async(self) -> None:
        """Asynchronously upload pending files before database commit."""
        save_items: "list[tuple[FileObject, Union[bytes, Path]]]" = list(self.pending_saves.items())

        async def save_one(obj: "FileObject", data: "Union[bytes, Path]") -> Any:
            self._save_attempts.add(obj)
            return await obj.save_async(data)

        save_results: "list[Any]" = await asyncio.gather(
            *(save_one(obj, data) for obj, data in save_items),
            return_exceptions=True,
        )

        errors: list[Exception] = []
        base_error: Optional[BaseException] = None

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
                        logger.warning(
                            "error saving file for object %s",
                            obj,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    errors.append(result)
                else:
                    base_error = result
            else:
                self._saved_in_transaction.add(obj)

        if base_error is not None:
            await self._cleanup_failed_prepare_async()
            raise base_error

        if errors and self.raise_on_error:
            await self._cleanup_failed_prepare_async()
            if len(errors) == 1:
                raise errors[0]
            msg = "multiple FileObject operation failures"
            raise ExceptionGroup(msg, errors)

        # Prevent after_commit() from uploading the same objects again. When
        # raise_on_error is false, failed uploads are logged as before.
        self.pending_saves.clear()

    async def _cleanup_failed_prepare_async(self) -> None:
        """Best-effort compensation after an asynchronous prepare failure."""
        try:
            await self.rollback_async()
        except Exception:
            logger.exception("error cleaning up files after failed async commit prepare")
