from __future__ import annotations

import errno
import logging
from datetime import UTC, datetime
from errno import ENOTEMPTY
from io import BytesIO
from pathlib import PurePath, PureWindowsPath
from typing import Any

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.transaction import Transaction
from fsspec.utils import stringify_path, tokenize

logger = logging.getLogger("fsspec.memoryfs")


class MemoryFileConflictError(FileExistsError, RuntimeError, ValueError):
    """A stale handle or transaction tried to modify a replaced path."""


# Descriptive alias.
ConflictError = MemoryFileConflictError


class _AppendBatch:
    """Ordered append writes deferred by one filesystem transaction."""

    __slots__ = ("entry", "data", "committed")

    def __init__(self, entry):
        self.entry = entry
        self.data = bytearray()
        self.committed = False


class MemoryTransaction(Transaction):
    """Transaction which keeps append increments ordered by write call."""

    def __init__(self, fs, **kwargs):
        super().__init__(fs, **kwargs)
        self.append_batches: dict[str, _AppendBatch] = {}

    def start(self):
        super().start()
        self.append_batches = {}


class MemoryFileSystem(AbstractFileSystem):
    """A filesystem based on a dict of BytesIO objects

    By default, instances share a global in-memory filesystem. Pass
    ``global_store=False, skip_instance_cache=True`` to create a new instance
    with an independent store instead.
    """

    store: dict[str, Any] = {}  # shared by default
    pseudo_dirs = [""]  # shared by default
    protocol = "memory"
    root_marker = "/"
    transaction_type = MemoryTransaction

    def __init__(self, *args, global_store=True, **kwargs):
        """Create a memory filesystem.

        Parameters
        ----------
        global_store: bool
            Share files and directories with other default instances. If False,
            each instance starts with an empty, independent store. The normal
            instance cache still applies; pass ``skip_instance_cache=True``
            to create a new instance on every call. Pickling an
            independent filesystem copies its files and directories; pickling a
            global filesystem retains the reference to the global store.
        """
        super().__init__(*args, **kwargs)
        self.global_store = global_store
        if not global_store:
            self.store = {}
            self.pseudo_dirs = [""]

    @property
    def _fs_token(self):
        if self.global_store:
            return super()._fs_token
        return tokenize(super()._fs_token, id(self))

    def __reduce__(self):
        reduced = super().__reduce__()
        if self.global_store:
            return reduced
        factory, (cls, args, kwargs) = reduced
        # Restore into a new instance even when the source is cached.
        kwargs = {**kwargs, "skip_instance_cache": True}
        # Pickle's memo preserves the cycle through each MemoryFile.fs.
        return (
            factory,
            (cls, args, kwargs),
            {"store": self.store, "pseudo_dirs": self.pseudo_dirs},
        )

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, PurePath):
            if isinstance(path, PureWindowsPath):
                return LocalFileSystem._strip_protocol(path)
            else:
                path = stringify_path(path)

        path = path.removeprefix("memory://")
        if "::" in path or "://" in path:
            return path.rstrip("/")
        path = path.lstrip("/").rstrip("/")
        return "/" + path if path else ""

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        # The base implementation calls ls() once per directory, and each ls()
        # scans the whole store, giving O(n_dirs * n_entries) behaviour
        # for a tree. Since the store is a flat mapping of every path, the same
        # result can be produced with a single pass over it.
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")
        path = self._strip_protocol(path)
        if path in self.store:
            # path is itself a file
            if not detail:
                return [path]
            filelike = self.store[path]
            return {
                path: {
                    "name": path,
                    "size": filelike.size,
                    "type": "file",
                    "created": filelike.created.timestamp(),
                }
            }

        # Uniform prefix so that the search root "" (the filesystem root) and a
        # nested path are handled the same way; rel depth is rel.count("/") + 1.
        prefix = path + "/" if path else "/"
        out = {}
        dirs = {}

        def add_ancestor_dirs(name):
            # Register every directory implied between ``path`` and ``name`` that
            # is within maxdepth, mirroring how walk() surfaces implied dirs.
            idx = name.rfind("/")
            if maxdepth is not None:
                # Start at the deepest ancestor that can appear in the result.
                limit = name.find("/", len(prefix))
                depth = 1
                while limit >= 0 and depth + 1 <= maxdepth:
                    next_limit = name.find("/", limit + 1)
                    if next_limit < 0:
                        break
                    limit = next_limit
                    depth += 1
                if limit >= 0:
                    idx = min(idx, limit)
            while idx > len(path):
                parent = name[:idx]
                if parent in dirs:
                    break
                rel = parent[len(prefix) :]
                if maxdepth is None or rel.count("/") + 1 <= maxdepth:
                    dirs[parent] = {"name": parent, "size": 0, "type": "directory"}
                idx = parent.rfind("/")

        # `store` may be shared by multiple MemoryFileSystem instances, so iterate a
        # snapshot: a concurrent create/delete would otherwise raise
        # "dictionary changed size during iteration". ls() does the same.
        for name, filelike in tuple(self.store.items()):
            if not name.startswith(prefix):
                continue
            rel = name[len(prefix) :]
            if withdirs:
                add_ancestor_dirs(name)
            if maxdepth is not None and rel.count("/") + 1 > maxdepth:
                continue
            out[name] = {
                "name": name,
                "size": filelike.size,
                "type": "file",
                "created": filelike.created.timestamp(),
            }

        if withdirs:
            # Explicitly-created (possibly empty) directories live in pseudo_dirs.
            for pdir in self.pseudo_dirs:
                if pdir and pdir.startswith(prefix):
                    add_ancestor_dirs(pdir + "/")
            out.update(dirs)
            # Mirror the base find(): include the search root itself when it is
            # a directory (needed for posix glob compliance).
            if path != "" and self.isdir(path):
                out[path] = self.info(path)

        names = sorted(out)
        return {name: out[name] for name in names} if detail else names

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if path in self.store:
            # there is a key with this exact name
            if not detail:
                return [path]
            return [
                {
                    "name": path,
                    "size": self.store[path].size,
                    "type": "file",
                    "created": self.store[path].created.timestamp(),
                }
            ]
        paths = set()
        starter = path + "/"
        out = []
        for p2 in tuple(self.store):
            if p2.startswith(starter):
                if "/" not in p2[len(starter) :]:
                    # exact child
                    out.append(
                        {
                            "name": p2,
                            "size": self.store[p2].size,
                            "type": "file",
                            "created": self.store[p2].created.timestamp(),
                        }
                    )
                elif len(p2) > len(starter):
                    # implied child directory
                    ppath = starter + p2[len(starter) :].split("/", 1)[0]
                    if ppath not in paths:
                        out = out or []
                        out.append(
                            {
                                "name": ppath,
                                "size": 0,
                                "type": "directory",
                            }
                        )
                        paths.add(ppath)
        for p2 in self.pseudo_dirs:
            if p2.startswith(starter):
                if "/" not in p2[len(starter) :]:
                    # exact child pdir
                    if p2 not in paths:
                        out.append({"name": p2, "size": 0, "type": "directory"})
                        paths.add(p2)
                else:
                    # directory implied by deeper pdir
                    ppath = starter + p2[len(starter) :].split("/", 1)[0]
                    if ppath not in paths:
                        out.append({"name": ppath, "size": 0, "type": "directory"})
                        paths.add(ppath)
        if not out:
            if path in self.pseudo_dirs:
                # empty dir
                return []
            raise FileNotFoundError(path)
        if detail:
            return out
        return sorted([f["name"] for f in out])

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        if path in self.store or path in self.pseudo_dirs:
            raise FileExistsError(path)
        if self._parent(path).strip("/") and self.isfile(self._parent(path)):
            raise NotADirectoryError(self._parent(path))
        if not create_parents and not self.isdir(self._parent(path)):
            raise FileNotFoundError(self._parent(path))
        if create_parents and self._parent(path).strip("/"):
            try:
                self.mkdir(self._parent(path), create_parents, **kwargs)
            except FileExistsError:
                pass
        if path and path not in self.pseudo_dirs:
            self.pseudo_dirs.append(path)

    def makedirs(self, path, exist_ok=False):
        try:
            self.mkdir(path, create_parents=True)
        except FileExistsError:
            if not exist_ok or self.isfile(path):
                raise

    def pipe_file(self, path, value, mode="overwrite", **kwargs):
        """Set the bytes of given file

        Avoids copies of the data if possible
        """
        mode = "xb" if mode == "create" else "wb"
        self.open(path, mode=mode, data=value)

    def rmdir(self, path):
        path = self._strip_protocol(path)
        if path == "":
            # silently avoid deleting FS root
            return
        if path in self.pseudo_dirs:
            if not self.ls(path):
                self.pseudo_dirs.remove(path)
            else:
                raise OSError(ENOTEMPTY, "Directory not empty", path)
        else:
            raise FileNotFoundError(path)

    def info(self, path, **kwargs):
        logger.debug("info: %s", path)
        path = self._strip_protocol(path)
        if path in self.pseudo_dirs or any(
            p.startswith(path + "/") for p in list(self.store) + self.pseudo_dirs
        ):
            return {
                "name": path,
                "size": 0,
                "type": "directory",
            }
        elif path in self.store:
            filelike = self.store[path]
            return {
                "name": path,
                "size": filelike.size,
                "type": "file",
                "created": filelike.created.timestamp(),
            }
        else:
            raise FileNotFoundError(path)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        if mode in {"rb+", "wb+", "ab+", "xb+"}:
            mode = mode[0] + "+b"
        path = self._strip_protocol(path)
        if "x" in mode and self.exists(path):
            raise FileExistsError
        if path in self.pseudo_dirs or (
            path not in self.store and any(p.startswith(path + "/") for p in self.store)
        ):
            raise IsADirectoryError(path)
        parent = path
        while len(parent) > 1:
            parent = self._parent(parent)
            if self.isfile(parent):
                raise FileExistsError(parent)
        defer = bool(self._intrans) or not autocommit
        if mode == "rb":
            if path not in self.store:
                raise FileNotFoundError(path)
            return self._open_reader(path, self.store[path])
        elif mode in ["ab", "a+b"]:
            return self._open_append(path, kwargs.get("data"), defer)
        elif mode == "r+b":
            entry = self.store.get(path)
            if entry is None:
                raise FileNotFoundError(path)
            handle = self._open_reader(path, entry)
            handle._write_through = True
            return handle
        elif mode in {"wb", "w+b", "xb", "x+b"}:
            if "x" in mode and self.exists(path):
                raise FileExistsError
            m = MemoryFile(self, path, kwargs.get("data"))
            if not defer:
                m.commit()
            return m
        else:
            name = self.__class__.__name__
            raise ValueError(f"unsupported file mode for {name}: {mode!r}")

    def _open_reader(self, path, entry):
        # Each open reader gets its own cursor and a snapshot of the bytes that
        # were published when it was opened.
        reader = MemoryFile(self, path, entry.getvalue())
        reader.created = entry.created
        reader.modified = entry.modified
        reader._entry = entry
        reader.seek(0)
        return reader

    def _open_append(self, path, data, defer):
        entry = self.store.get(path)
        if entry is None:
            if defer:
                handle = MemoryFile(self, path, data)
                handle._append_mode = True
                batch = self._append_batch(path, None)
                handle._append_batch = batch
                if data:
                    batch.data.extend(data)
                handle.seek(0, 2)
                return handle

            # Append modes create missing paths, matching builtin open().
            entry = MemoryFile(self, path, data)
            entry.commit()

        handle = MemoryFile(self, path, entry.getvalue())
        handle.created = entry.created
        handle.modified = entry.modified
        handle._entry = entry
        handle._append_mode = True
        if defer:
            handle._append_batch = self._append_batch(path, entry)
        handle.seek(0, 2)
        return handle

    def _append_batch(self, path, entry):
        transaction = self.transaction
        if not isinstance(transaction, MemoryTransaction):
            raise TypeError("MemoryFileSystem expected a MemoryTransaction")
        batches = transaction.append_batches
        batch = batches.get(path)
        if batch is None:
            batch = _AppendBatch(entry)
            batches[path] = batch
        return batch

    def _commit_append_batch(self, path, batch):
        if batch.committed:
            return

        current = self.store.get(path)
        if batch.entry is None and current is not None:
            # The path appeared while the transaction was open. Append to the
            # current entity rather than overwriting its published bytes.
            batch.entry = current

        if batch.entry is None:
            entry = MemoryFile(self, path, bytes(batch.data))
            self.store[path] = entry
        else:
            if current is not batch.entry:
                raise MemoryFileConflictError(
                    errno.EEXIST,
                    f"Stale memory file conflict for {path}: the path was "
                    "deleted and recreated or replaced after this transaction "
                    "opened",
                    path,
                )
            if batch.data:
                current.seek(0, 2)
                current.write(batch.data)

        batch.committed = True

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        source_is_file = self.isfile(path1)
        if not source_is_file and not self.isdir(path1):
            raise FileNotFoundError(path1)
        parent = path2
        while len(parent) > 1:
            parent = self._parent(parent)
            if self.isfile(parent):
                raise FileExistsError(parent)
        if source_is_file:
            if self.isdir(path2):
                raise IsADirectoryError(path2)
            self.store[path2] = MemoryFile(
                self, path2, self.store[path1].getvalue()
            )  # implicit copy
        else:
            if self.isfile(path2):
                raise FileExistsError(path2)
            if path2 not in self.pseudo_dirs:
                self.pseudo_dirs.append(path2)

    def cat_file(self, path, start=None, end=None, **kwargs):
        logger.debug("cat: %s", path)
        path = self._strip_protocol(path)
        try:
            return bytes(self.store[path].getbuffer()[start:end])
        except KeyError as e:
            raise FileNotFoundError(path) from e

    def _rm(self, path):
        path = self._strip_protocol(path)
        try:
            del self.store[path]
        except KeyError as e:
            raise FileNotFoundError(path) from e

    def modified(self, path):
        path = self._strip_protocol(path)
        try:
            return self.store[path].modified
        except KeyError as e:
            raise FileNotFoundError(path) from e

    def created(self, path):
        path = self._strip_protocol(path)
        try:
            return self.store[path].created
        except KeyError as e:
            raise FileNotFoundError(path) from e

    def isfile(self, path):
        path = self._strip_protocol(path)
        return path in self.store

    def rm(self, path, recursive=False, maxdepth=None):
        if isinstance(path, str):
            path = self._strip_protocol(path)
        else:
            path = [self._strip_protocol(p) for p in path]
        paths = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        # A non-recursive expand keeps a literal path whether or not it exists,
        # so record what exists before anything is deleted.
        existed = {p for p in paths if self.exists(p)}
        for p in reversed(paths):
            if self.isfile(p):
                self.rm_file(p)
            elif not self.exists(p):
                # A directory that is not in self.pseudo_dirs only exists while it
                # has files under it, so deleting those files earlier in this loop
                # makes it vanish. That is expected; a path that never existed is
                # not, and raises like rm_file and the other filesystems do.
                if p not in existed:
                    raise FileNotFoundError(p)
                continue
            elif maxdepth is not None and self.ls(p, detail=False):
                # a directory still holding files deeper than maxdepth is kept
                continue
            else:
                self.rmdir(p)


class MemoryFile(BytesIO):
    """A BytesIO which can't close and works as a context manager

    Can initialise with data. Each path should only be active once at any moment.

    No need to provide fs, path if auto-committing (default)
    """

    def __init__(self, fs=None, path=None, data=None):
        logger.debug("open file %s", path)
        self.fs = fs
        self.path = path
        self.created = datetime.now(tz=UTC)
        self.modified = datetime.now(tz=UTC)
        self._entry = None
        self._write_through = False
        self._append_mode = False
        self._append_batch = None
        if data:
            super().__init__(data)
            self.seek(0)

    @property
    def size(self):
        return self.getbuffer().nbytes

    def write(self, data):
        if self._append_mode:
            return self._write_append(data)

        fs = self.fs if isinstance(self.fs, MemoryFileSystem) else None
        entry = self._entry if fs is not None and self._write_through else None
        if entry is not None and self._append_batch is None:
            self._check_current_entry(fs, entry)
            position = self.tell()
            written = super().write(data)
            if written:
                now = datetime.now(tz=UTC)
                part = data[:written]
                entry.seek(position)
                entry.write(part)
                self.modified = entry.modified = now
            return written

        written = super().write(data)
        if written:
            self.modified = datetime.now(tz=UTC)
        return written

    def _check_current_entry(self, fs=None, entry=None):
        fs = fs or self.fs
        entry = entry or self._entry
        if not isinstance(fs, MemoryFileSystem) or entry is None:
            return
        if fs.store.get(self.path) is not entry:
            raise MemoryFileConflictError(
                errno.EEXIST,
                f"Stale memory file conflict for {self.path}: the path was "
                "deleted and recreated or replaced after this handle opened",
                self.path,
            )

    def _write_append(self, data):
        fs = self.fs if isinstance(self.fs, MemoryFileSystem) else None
        if fs is not None and self._append_batch is None:
            self._check_current_entry(fs, self._entry)

        # BytesIO's position is per handle. Seeking to the end here ensures that
        # a prior seek() can only change reads, never the append location.
        super().seek(0, 2)
        written = super().write(data)
        if not written:
            return 0

        now = datetime.now(tz=UTC)
        self.modified = now
        part = data[:written]

        if self._append_batch is not None:
            # Keep one path-ordered batch shared by all handles in this
            # transaction. The store is intentionally untouched until commit.
            self._append_batch.data.extend(part)
        elif fs is not None:
            entry = self._entry
            entry.seek(0, 2)
            entry.write(part)
            self.modified = entry.modified = now

        return written

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def truncate(self, size=None):
        fs = self.fs if isinstance(self.fs, MemoryFileSystem) else None
        entry = self._entry if fs is not None and self._write_through else None
        if entry is not None and self._append_batch is None:
            self._check_current_entry(fs, entry)
            size = super().truncate(size)
            entry.truncate(size)
            self.modified = entry.modified = datetime.now(tz=UTC)
            return size

        size = super().truncate(size)
        self.modified = datetime.now(tz=UTC)
        return size

    def __enter__(self):
        return self

    def close(self):
        pass

    def discard(self):
        if self._append_batch is not None:
            self._append_batch.data.clear()

    def commit(self):
        if self._append_mode:
            if self._append_batch is not None:
                self.fs._commit_append_batch(self.path, self._append_batch)
            else:
                self._check_current_entry()
            # Autocommitted appends publish each write immediately; do not
            # replace the canonical entity with this handle's cursor snapshot.
            return
        if self._entry is not None:
            if self._write_through:
                self._check_current_entry()
            # Read snapshots and write-through handles are not store entities.
            return
        self.fs.store[self.path] = self
        self.modified = datetime.now(tz=UTC)
