"""Tests for shared retention of StoredObject fields."""

import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any, Optional, Union

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from advanced_alchemy._listeners import SyncFileObjectListener
from advanced_alchemy.exceptions import SharedObjectUnavailableError
from advanced_alchemy.types.file_object import (
    FileObject,
    FileObjectList,
    StoredObject,
    storages,
)
from advanced_alchemy.types.file_object.base import PathLike, StorageBackend
from advanced_alchemy.types.file_object.session_tracker import FileObjectSessionTracker
from advanced_alchemy.types.file_object.shared import (
    SharedObjectCleanup,
    SharedObjectRegistry,
    shared_objects,
)
from advanced_alchemy.types.mutables import MutableList


class InMemoryBackend(StorageBackend):
    """Minimal in-memory storage backend for shared retention tests."""

    def __init__(self, key: str) -> None:
        super().__init__(key=key, fs=None)
        self.store: dict[str, bytes] = {}
        # (path, bytes present when the delete executed)
        self.delete_calls: list[tuple[str, Optional[bytes]]] = []
        self.delete_hook: Optional[Any] = None

    def get_content(self, path: "PathLike", *, options: "Optional[dict[str, Any]]" = None) -> bytes:
        return self.store[str(path)]

    async def get_content_async(self, path: "PathLike", *, options: "Optional[dict[str, Any]]" = None) -> bytes:
        return self.get_content(path, options=options)

    def save_object(
        self,
        file_object: "FileObject",
        data: Any,
        *,
        use_multipart: "Optional[bool]" = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> "FileObject":
        if isinstance(data, (bytes, bytearray, memoryview)):
            content = bytes(data)
        elif isinstance(data, Path):
            content = data.read_bytes()
        else:
            content = b"".join(data)
        self.store[file_object.path] = content
        file_object.size = len(content)
        return file_object

    async def save_object_async(
        self,
        file_object: "FileObject",
        data: Any,
        *,
        use_multipart: "Optional[bool]" = None,
        chunk_size: int = 5 * 1024 * 1024,
        max_concurrency: int = 12,
    ) -> "FileObject":
        return self.save_object(
            file_object,
            data,
            use_multipart=use_multipart,
            chunk_size=chunk_size,
            max_concurrency=max_concurrency,
        )

    def delete_object(self, paths: "Union[PathLike, list[PathLike]]") -> None:
        for path in [paths] if isinstance(paths, (str, Path)) else paths:
            key = str(path)
            if self.delete_hook is not None:
                self.delete_hook(key)
            self.delete_calls.append((key, self.store.get(key)))
            if key not in self.store:
                msg = f"No such object: {key}"
                raise FileNotFoundError(msg)
            del self.store[key]

    async def delete_object_async(self, paths: "Union[PathLike, list[PathLike]]") -> None:
        self.delete_object(paths)

    def sign(
        self,
        paths: "Union[PathLike, list[PathLike]]",
        *,
        expires_in: "Optional[int]" = None,
        for_upload: bool = False,
    ) -> str:
        return f"signed://{self.key}/{paths}"

    async def sign_async(
        self,
        paths: "Union[PathLike, list[PathLike]]",
        *,
        expires_in: "Optional[int]" = None,
        for_upload: bool = False,
    ) -> str:
        return self.sign(paths, expires_in=expires_in, for_upload=for_upload)


@pytest.fixture
def primary_backend() -> "Generator[InMemoryBackend, None, None]":
    backend = InMemoryBackend("shared_primary")
    storages.register_backend(backend)
    try:
        yield backend
    finally:
        storages.unregister_backend("shared_primary")


@pytest.fixture
def backup_backend() -> "Generator[InMemoryBackend, None, None]":
    backend = InMemoryBackend("shared_backup")
    storages.register_backend(backend)
    try:
        yield backend
    finally:
        storages.unregister_backend("shared_backup")


@pytest.fixture(autouse=True)
def clean_shared_registry() -> "Generator[None, None, None]":
    shared_objects.clear()
    try:
        yield
    finally:
        shared_objects.clear()


# --- Registry level: the rescan scenario ---


def test_shared_retention_rescan_scenario(primary_backend: InMemoryBackend, backup_backend: InMemoryBackend) -> None:
    """Walk through the full rescan lifecycle described by the workflow."""
    # The original scan lands in the primary store; two old documents reference it.
    primary_backend.store["scan/42.pdf"] = b"original-bytes"
    backup_backend.store["scan/42.pdf"] = b"backup-bytes"
    generation_old = shared_objects.register_save("shared_primary", "scan/42.pdf")
    shared_objects.acquire("shared_primary", "scan/42.pdf")  # rework order attachment
    shared_objects.acquire("shared_primary", "scan/42.pdf")  # quality order attachment list

    # The rework order drops its attachment: the quality order still opens the file.
    assert shared_objects.release("shared_primary", "scan/42.pdf") is None
    assert primary_backend.get_content("scan/42.pdf") == b"original-bytes"

    # The quality order drops it too: cleanup sees zero references and pauses
    # before the delete is handed to the storage backend.
    cleanup_first = shared_objects.release("shared_primary", "scan/42.pdf")
    assert isinstance(cleanup_first, SharedObjectCleanup)

    # A supplementary record takes over the original before the delete proceeds.
    shared_objects.acquire("shared_primary", "scan/42.pdf")
    assert cleanup_first.execute() is False
    assert primary_backend.get_content("scan/42.pdf") == b"original-bytes"

    # The supplementary record drops it. The next cleanup enters the backend
    # delete call; while it is in flight another document tries to adopt.
    cleanup_second = shared_objects.release("shared_primary", "scan/42.pdf")
    assert isinstance(cleanup_second, SharedObjectCleanup)

    delete_entered = threading.Event()
    delete_may_finish = threading.Event()

    def delete_hook(path: str) -> None:
        delete_entered.set()
        assert delete_may_finish.wait(timeout=10)

    primary_backend.delete_hook = delete_hook

    adopt_errors: list[SharedObjectUnavailableError] = []

    def adopt() -> None:
        try:
            shared_objects.acquire("shared_primary", "scan/42.pdf")
        except SharedObjectUnavailableError as e:
            adopt_errors.append(e)

    delete_thread = threading.Thread(target=cleanup_second.execute)
    delete_thread.start()
    assert delete_entered.wait(timeout=10)

    adopt_thread = threading.Thread(target=adopt)
    adopt_thread.start()
    # The adoption attempt may wait for the in-flight delete, but must not succeed.
    time.sleep(0.2)
    assert shared_objects.is_delete_in_progress("shared_primary", "scan/42.pdf")
    delete_may_finish.set()
    delete_thread.join(timeout=10)
    adopt_thread.join(timeout=10)

    assert len(adopt_errors) == 1
    assert "scan/42.pdf" not in primary_backend.store

    # The collector re-ingests a new scan under the same path.
    primary_backend.store["scan/42.pdf"] = b"new-bytes"
    generation_new = shared_objects.register_save("shared_primary", "scan/42.pdf")
    assert generation_new != generation_old
    shared_objects.acquire("shared_primary", "scan/42.pdf")

    # The new scan is dropped as well and waits for its own cleanup.
    cleanup_third = shared_objects.release("shared_primary", "scan/42.pdf")
    assert isinstance(cleanup_third, SharedObjectCleanup)

    # Replaying the old scan's cleanup requests must not touch the new bytes.
    assert cleanup_second.execute() is False
    stale_replay = SharedObjectCleanup(shared_objects, "shared_primary", "scan/42.pdf", generation_old)
    assert stale_replay.execute() is False
    assert primary_backend.get_content("scan/42.pdf") == b"new-bytes"

    # The new scan's own cleanup is approved and deletes exactly its bytes.
    assert cleanup_third.execute() is True
    assert "scan/42.pdf" not in primary_backend.store

    # The backend received one effective delete per scan generation.
    assert primary_backend.delete_calls == [
        ("scan/42.pdf", b"original-bytes"),
        ("scan/42.pdf", b"new-bytes"),
    ]

    # The backup backend's copy stayed readable the whole time.
    assert backup_backend.get_content("scan/42.pdf") == b"backup-bytes"
    assert backup_backend.delete_calls == []


def test_registry_backends_are_independent(primary_backend: InMemoryBackend, backup_backend: InMemoryBackend) -> None:
    """The same path in two backends is tracked and cleaned independently."""
    primary_backend.store["scan/42.pdf"] = b"primary"
    backup_backend.store["scan/42.pdf"] = b"backup"
    shared_objects.register_save("shared_primary", "scan/42.pdf")
    shared_objects.register_save("shared_backup", "scan/42.pdf")
    shared_objects.acquire("shared_primary", "scan/42.pdf")
    shared_objects.acquire("shared_backup", "scan/42.pdf")

    cleanup_primary = shared_objects.release("shared_primary", "scan/42.pdf")
    assert cleanup_primary is not None
    assert cleanup_primary.execute() is True

    assert "scan/42.pdf" not in primary_backend.store
    assert backup_backend.get_content("scan/42.pdf") == b"backup"
    assert shared_objects.reference_count("shared_backup", "scan/42.pdf") == 1


def test_registry_release_untracked_path_returns_none(primary_backend: InMemoryBackend) -> None:
    assert shared_objects.release("shared_primary", "missing.txt") is None
    assert shared_objects.reference_count("shared_primary", "missing.txt") == 0
    assert shared_objects.current_generation("shared_primary", "missing.txt") is None


def test_registry_acquire_timeout(primary_backend: InMemoryBackend) -> None:
    """An acquire waiting on a stuck delete fails loudly instead of hanging."""
    primary_backend.store["stuck.txt"] = b"data"
    shared_objects.register_save("shared_primary", "stuck.txt")
    cleanup = shared_objects.release("shared_primary", "stuck.txt")
    assert cleanup is not None

    block = threading.Event()

    def delete_hook(path: str) -> None:
        block.wait(timeout=10)

    primary_backend.delete_hook = delete_hook
    delete_thread = threading.Thread(target=cleanup.execute)
    delete_thread.start()
    try:
        while not shared_objects.is_delete_in_progress("shared_primary", "stuck.txt"):
            time.sleep(0.01)
        with pytest.raises(SharedObjectUnavailableError, match="Timed out"):
            shared_objects.acquire("shared_primary", "stuck.txt", timeout=0.1)
    finally:
        block.set()
        delete_thread.join(timeout=10)


def test_registry_cleanup_repr_and_executed_flag(primary_backend: InMemoryBackend) -> None:
    primary_backend.store["repr.txt"] = b"data"
    shared_objects.register_save("shared_primary", "repr.txt")
    cleanup = shared_objects.release("shared_primary", "repr.txt")
    assert cleanup is not None
    assert not cleanup.executed
    assert "repr.txt" in repr(cleanup)
    assert cleanup.execute() is True
    assert cleanup.executed
    assert cleanup.execute() is False


async def test_registry_cleanup_execute_async(primary_backend: InMemoryBackend) -> None:
    primary_backend.store["async.txt"] = b"data"
    shared_objects.register_save("shared_primary", "async.txt")
    cleanup = shared_objects.release("shared_primary", "async.txt")
    assert cleanup is not None
    assert await cleanup.execute_async() is True
    assert "async.txt" not in primary_backend.store


async def test_registry_collect_async(primary_backend: InMemoryBackend) -> None:
    primary_backend.store["collect.txt"] = b"data"
    shared_objects.register_save("shared_primary", "collect.txt")
    assert await shared_objects.collect_async("shared_primary", "collect.txt") is True
    assert "collect.txt" not in primary_backend.store
    assert await shared_objects.collect_async("shared_primary", "collect.txt") is False


def test_registry_collect_respects_references(primary_backend: InMemoryBackend) -> None:
    primary_backend.store["kept.txt"] = b"data"
    shared_objects.register_save("shared_primary", "kept.txt")
    shared_objects.acquire("shared_primary", "kept.txt")
    assert shared_objects.collect("shared_primary", "kept.txt") is False
    assert primary_backend.get_content("kept.txt") == b"data"


def test_registry_isolated_instances() -> None:
    registry_a = SharedObjectRegistry()
    registry_b = SharedObjectRegistry()
    registry_a.acquire("backend", "path.txt")
    assert registry_a.reference_count("backend", "path.txt") == 1
    assert registry_b.reference_count("backend", "path.txt") == 0


# --- Session tracker level ---


def test_tracker_acquire_then_release_cancels(primary_backend: InMemoryBackend) -> None:
    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="cancel.txt")
    tracker.add_pending_acquire(obj)
    assert shared_objects.reference_count("shared_primary", "cancel.txt") == 1
    tracker.add_pending_release(obj)
    assert shared_objects.reference_count("shared_primary", "cancel.txt") == 0
    assert tracker.pending_releases == []


def test_tracker_release_then_acquire_cancels(primary_backend: InMemoryBackend) -> None:
    shared_objects.register_save("shared_primary", "swap.txt")
    shared_objects.acquire("shared_primary", "swap.txt")
    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="swap.txt")
    tracker.add_pending_release(obj)
    tracker.add_pending_acquire(obj)
    assert tracker.pending_releases == []
    assert shared_objects.reference_count("shared_primary", "swap.txt") == 1


def test_tracker_commit_shared_save_and_release(primary_backend: InMemoryBackend) -> None:
    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="stored.txt", content=b"payload")
    tracker.add_pending_save(obj, b"payload", shared=True)
    tracker.add_pending_acquire(obj)
    tracker.commit()

    assert primary_backend.store["stored.txt"] == b"payload"
    assert shared_objects.reference_count("shared_primary", "stored.txt") == 1
    assert shared_objects.current_generation("shared_primary", "stored.txt") is not None

    detach = FileObjectSessionTracker()
    detach.add_pending_release(obj)
    detach.commit()

    assert "stored.txt" not in primary_backend.store
    assert shared_objects.reference_count("shared_primary", "stored.txt") == 0


def test_tracker_commit_shared_save_registers_new_generation(primary_backend: InMemoryBackend) -> None:
    primary_backend.store["gen.txt"] = b"old"
    old_generation = shared_objects.register_save("shared_primary", "gen.txt")

    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="gen.txt", content=b"new")
    tracker.add_pending_save(obj, b"new", shared=True)
    tracker.commit()

    new_generation = shared_objects.current_generation("shared_primary", "gen.txt")
    assert new_generation is not None
    assert new_generation != old_generation


def test_tracker_rollback_undoes_shared_acquire(primary_backend: InMemoryBackend) -> None:
    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="rolled-back.txt")
    tracker.add_pending_acquire(obj)
    assert shared_objects.reference_count("shared_primary", "rolled-back.txt") == 1
    tracker.rollback()
    assert shared_objects.reference_count("shared_primary", "rolled-back.txt") == 0
    # The rollback undo must not schedule a delete for the path.
    assert "rolled-back.txt" not in primary_backend.store
    assert primary_backend.delete_calls == []


def test_tracker_rollback_shared_save_collects_only_when_unreferenced(
    primary_backend: InMemoryBackend,
) -> None:
    primary_backend.store["maybe-orphan.txt"] = b"data"
    shared_objects.register_save("shared_primary", "maybe-orphan.txt")

    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="maybe-orphan.txt")
    tracker._saved_in_transaction.add(obj)
    tracker._shared_saves.add(obj)
    tracker.rollback()
    assert "maybe-orphan.txt" not in primary_backend.store

    # A referenced object survives the rollback cleanup.
    primary_backend.store["referenced.txt"] = b"data"
    shared_objects.register_save("shared_primary", "referenced.txt")
    shared_objects.acquire("shared_primary", "referenced.txt")
    tracker = FileObjectSessionTracker()
    obj = FileObject(backend="shared_primary", filename="referenced.txt")
    tracker._saved_in_transaction.add(obj)
    tracker._shared_saves.add(obj)
    tracker.rollback()
    assert primary_backend.get_content("referenced.txt") == b"data"


def test_tracker_acquire_raises_when_object_unavailable(primary_backend: InMemoryBackend) -> None:
    """Adoption during an in-flight delete fails the flush instead of committing."""
    primary_backend.store["gone.txt"] = b"data"
    shared_objects.register_save("shared_primary", "gone.txt")
    cleanup = shared_objects.release("shared_primary", "gone.txt")
    assert cleanup is not None

    entered = threading.Event()
    finish = threading.Event()

    def delete_hook(path: str) -> None:
        entered.set()
        finish.wait(timeout=10)

    primary_backend.delete_hook = delete_hook
    delete_thread = threading.Thread(target=cleanup.execute)
    delete_thread.start()
    assert entered.wait(timeout=10)
    try:
        tracker = FileObjectSessionTracker()
        obj = FileObject(backend="shared_primary", filename="gone.txt")

        errors: list[Exception] = []

        def adopt() -> None:
            try:
                tracker.add_pending_acquire(obj)
            except SharedObjectUnavailableError as e:
                errors.append(e)

        adopt_thread = threading.Thread(target=adopt)
        adopt_thread.start()
        time.sleep(0.2)
        finish.set()
        adopt_thread.join(timeout=10)
        delete_thread.join(timeout=10)
        assert len(errors) == 1
        assert tracker._acquired_in_transaction == []
    finally:
        finish.set()


# --- ORM level: the two registered field kinds ---


class ORMBase(DeclarativeBase):
    pass


class ReworkOrder(ORMBase):
    """Single shared attachment field (``multiple=False``)."""

    __tablename__ = "rework_orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    attachment: Mapped[Optional[FileObject]] = mapped_column(
        StoredObject(backend="shared_primary", shared=True),
        nullable=True,
    )


class QualityOrder(ORMBase):
    """Shared attachment list field (``multiple=True``)."""

    __tablename__ = "quality_orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    attachments: Mapped[Optional[FileObjectList]] = mapped_column(
        StoredObject(backend="shared_primary", multiple=True, shared=True),
        nullable=True,
    )


class PrivateNote(ORMBase):
    """Non-shared field on the backup backend: cleanup stays independent."""

    __tablename__ = "private_notes"
    id: Mapped[int] = mapped_column(primary_key=True)
    attachment: Mapped[Optional[FileObject]] = mapped_column(
        StoredObject(backend="shared_backup"),
        nullable=True,
    )


@pytest.fixture
def db_session(primary_backend: InMemoryBackend, backup_backend: InMemoryBackend) -> "Generator[Session, None, None]":
    engine = create_engine("sqlite:///:memory:")
    ORMBase.metadata.create_all(engine)
    listeners: list[tuple[str, Any]] = [
        ("before_flush", SyncFileObjectListener.before_flush),
        ("after_commit", SyncFileObjectListener.after_commit),
        ("after_rollback", SyncFileObjectListener.after_rollback),
    ]
    for name, listener in listeners:
        event.listen(Session, name, listener)
    try:
        with Session(engine) as session:
            yield session
    finally:
        for name, listener in listeners:
            event.remove(Session, name, listener)
        engine.dispose()


def test_orm_shared_fields_retain_until_last_reference(db_session: Session, primary_backend: InMemoryBackend) -> None:
    rework = ReworkOrder(
        attachment=FileObject(backend="shared_primary", filename="scan/42.pdf", content=b"original-bytes")
    )
    db_session.add(rework)
    db_session.commit()
    assert primary_backend.store["scan/42.pdf"] == b"original-bytes"
    assert shared_objects.reference_count("shared_primary", "scan/42.pdf") == 1

    quality = QualityOrder(attachments=[FileObject(backend="shared_primary", filename="scan/42.pdf")])
    db_session.add(quality)
    db_session.commit()
    assert shared_objects.reference_count("shared_primary", "scan/42.pdf") == 2

    # Dropping the rework attachment keeps the quality order's original readable.
    assert rework.attachment is not None  # load the attribute before clearing it
    rework.attachment = None
    db_session.commit()
    assert primary_backend.delete_calls == []
    assert quality.attachments is not None
    assert quality.attachments[0].get_content() == b"original-bytes"

    # Dropping the last reference hands the delete to the backend.
    quality.attachments = MutableList([])
    db_session.commit()
    assert "scan/42.pdf" not in primary_backend.store
    assert primary_backend.delete_calls == [("scan/42.pdf", b"original-bytes")]


def test_orm_shared_field_row_delete_releases_reference(db_session: Session, primary_backend: InMemoryBackend) -> None:
    rework = ReworkOrder(attachment=FileObject(backend="shared_primary", filename="scan/43.pdf", content=b"row-delete"))
    db_session.add(rework)
    db_session.commit()
    quality = QualityOrder(attachments=[FileObject(backend="shared_primary", filename="scan/43.pdf")])
    db_session.add(quality)
    db_session.commit()

    db_session.delete(rework)
    db_session.commit()
    assert primary_backend.get_content("scan/43.pdf") == b"row-delete"

    db_session.delete(quality)
    db_session.commit()
    assert "scan/43.pdf" not in primary_backend.store


def test_orm_shared_field_replacement_moves_reference(db_session: Session, primary_backend: InMemoryBackend) -> None:
    rework = ReworkOrder(attachment=FileObject(backend="shared_primary", filename="scan/44.pdf", content=b"v1"))
    db_session.add(rework)
    db_session.commit()
    assert shared_objects.reference_count("shared_primary", "scan/44.pdf") == 1

    # Replacing the attachment releases the old object (last reference, so it
    # is deleted) and acquires the new one.
    assert rework.attachment is not None  # load the attribute before replacing it
    rework.attachment = FileObject(backend="shared_primary", filename="scan/45.pdf", content=b"v2")
    db_session.commit()
    assert "scan/44.pdf" not in primary_backend.store
    assert primary_backend.store["scan/45.pdf"] == b"v2"
    assert shared_objects.reference_count("shared_primary", "scan/45.pdf") == 1

    assert rework.attachment is not None
    rework.attachment = None
    db_session.commit()
    assert "scan/45.pdf" not in primary_backend.store


def test_orm_non_shared_field_deletes_immediately(db_session: Session, backup_backend: InMemoryBackend) -> None:
    """Fields that did not opt in keep the existing delete-on-detach behavior."""
    note = PrivateNote(attachment=FileObject(backend="shared_backup", filename="scan/42.pdf", content=b"private"))
    db_session.add(note)
    db_session.commit()
    assert backup_backend.store["scan/42.pdf"] == b"private"
    assert shared_objects.reference_count("shared_backup", "scan/42.pdf") == 0

    assert note.attachment is not None  # load the attribute before clearing it
    note.attachment = None
    db_session.commit()
    assert "scan/42.pdf" not in backup_backend.store


def test_stored_object_shared_flag(primary_backend: InMemoryBackend) -> None:
    shared_single = StoredObject(backend="shared_primary", shared=True)
    assert shared_single.shared is True
    assert shared_single.multiple is False
    assert "shared=True" in repr(shared_single)

    shared_multiple = StoredObject(backend="shared_primary", multiple=True, shared=True)
    assert shared_multiple.shared is True
    assert shared_multiple.multiple is True
    assert "multiple=True" in repr(shared_multiple)
    assert "shared=True" in repr(shared_multiple)

    default = StoredObject(backend="shared_primary")
    assert default.shared is False
    assert repr(default) == "StoredObject(backend='shared_primary')"
