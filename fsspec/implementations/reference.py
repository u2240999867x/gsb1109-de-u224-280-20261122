import base64
import collections
import io
import itertools
import logging
import math
import os
from functools import lru_cache
from itertools import chain
from typing import TYPE_CHECKING, Literal

import fsspec.core
from fsspec.spec import AbstractBufferedFile

try:
    import ujson as json
except ImportError:
    if not TYPE_CHECKING:
        import json

from fsspec.asyn import AsyncFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.core import filesystem, open, split_protocol
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.utils import (
    check_contained,
    isfilelike,
    merge_offset_ranges,
    other_paths,
)

logger = logging.getLogger("fsspec.reference")


class ReferenceNotReachable(RuntimeError):
    def __init__(self, reference, target, *args):
        super().__init__(*args)
        self.reference = reference
        self.target = target

    def __str__(self):
        return f'Reference "{self.reference}" failed to fetch target {self.target}'


def _first(d):
    return next(iter(d.values()))


def _inline_data(part):
    """The bytes an inline (in-memory) reference delivers to a reader.

    A ``base64:`` prefix marks base64-encoded content; other text is encoded
    as UTF-8. ``info`` and ``ls`` measure the result of this rather than the
    stored form, so that a size always matches what ``cat`` returns.
    """
    if isinstance(part, (str, bytes)):
        return _decode_inline(part)
    if hasattr(part, "to_bytes"):
        part = part.to_bytes()
    return _decode_inline(part)


@lru_cache(maxsize=1024)
def _decode_inline(part):
    """Decode a stored inline part once and stash the result.

    The same inline value is decoded for ``cat`` and again for the length
    check in ``info``/``ls``; caching avoids repeating the work for every
    listing. Inline references are small and few, so a bounded cache is
    enough.
    """
    if isinstance(part, str):
        part = part.encode()
    if part.startswith(b"base64:"):
        return base64.b64decode(part[7:])
    return part


def _prot_in_references(path, references):
    ref = references.get(path)
    if isinstance(ref, (list, tuple)) and isinstance(ref[0], str):
        return split_protocol(ref[0])[0] if ref[0] else ref[0]


def _protocol_groups(paths, references):
    if isinstance(paths, str):
        return {_prot_in_references(paths, references): [paths]}
    out = {}
    for path in paths:
        protocol = _prot_in_references(path, references)
        out.setdefault(protocol, []).append(path)
    return out


class RefsValuesView(collections.abc.ValuesView):
    def __iter__(self):
        # Follow the mapping's iteration so keys and values stay aligned even
        # when some chunk slots in a record have been deleted.
        for key in self._mapping:
            try:
                yield self._mapping[key]
            except KeyError:
                continue


class RefsItemsView(collections.abc.ItemsView):
    def __iter__(self):
        for key in self._mapping:
            try:
                yield key, self._mapping[key]
            except KeyError:
                continue


def ravel_multi_index(idx, sizes):
    val = 0
    mult = 1
    for i, s in zip(idx[::-1], sizes[::-1]):
        val += i * mult
        mult *= s
    return val


def unravel_multi_index(value, sizes):
    """Inverse of :func:`ravel_multi_index` for a full chunk index."""
    idx = []
    for size in reversed(sizes):
        idx.append(value % size)
        value //= size
    return tuple(reversed(idx))


def _is_null(value):
    """Whether a value read back from parquet represents a missing entry."""
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    # pandas' NA/NaT scalars are not equal to themselves, but comparing them
    # yields NA rather than a bool, so test the type name instead.
    return type(value).__name__ in ("NAType", "NaTType")


def _as_raw_bytes(value):
    """Store inline reference payloads in the on-disk byte representation."""
    if hasattr(value, "to_bytes"):
        value = value.to_bytes()
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return value


def _chunk_index(key):
    """Parse the dotted chunk coordinate at the end of a reference key."""
    _, chunk = key.rsplit("/", 1)
    try:
        return tuple(int(c) for c in chunk.split("."))
    except ValueError as exc:
        raise KeyError(key) from exc


def _within(index, grid):
    """Whether a full chunk coordinate lies inside a chunk grid."""
    return len(index) == len(grid) and all(i < g for i, g in zip(index, grid))


def _grid_product(grid):
    return math.prod(grid) if grid else 1


def _proc_raw(value):
    """Normalize an inline reference for the ``raw`` parquet column.

    Readers decode a ``base64:`` marker themselves, so the stored bytes must be
    the same representation an inline reference would have in a JSON index.
    """
    if hasattr(value, "to_bytes"):
        value = value.to_bytes()
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    return value


class LazyReferenceMapper(collections.abc.MutableMapping):
    """This interface can be used to read/write references from Parquet stores.
    It is not intended for other types of references.
    It can be used with Kerchunk's MultiZarrToZarr method to combine
    references into a parquet store.
    Examples of this use-case can be found here:
    https://fsspec.github.io/kerchunk/advanced.html?highlight=parquet#parquet-storage"""

    # import is class level to prevent numpy dep requirement for fsspec
    @property
    def np(self):
        import numpy as np

        return np

    @property
    def pd(self):
        import pandas as pd

        return pd

    def __init__(
        self,
        root,
        fs=None,
        out_root=None,
        cache_size=128,
        categorical_threshold=10,
        engine: Literal["fastparquet", "pyarrow"] = "fastparquet",
    ):
        """

        This instance will be writable, storing changes in memory until full partitions
        are accumulated or .flush() is called.

        To create an empty lazy store, use .create()

        Parameters
        ----------
        root : str
            Root of parquet store
        fs : fsspec.AbstractFileSystem
            fsspec filesystem object, default is local filesystem.
        cache_size : int, default=128
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
        categorical_threshold : int
            Encode urls as pandas.Categorical to reduce memory footprint if the ratio
            of the number of unique urls to total number of refs for each variable
            is greater than or equal to this number. (default 10)
        engine: Literal["fastparquet","pyarrow"]
            Engine choice for reading parquet files. (default is "fastparquet")
        """

        self.root = root
        self.chunk_sizes = {}
        self.cat_thresh = categorical_threshold
        self.engine = engine
        self.cache_size = cache_size
        self.url = self.root + "/{field}/refs.{record}.parq"
        # TODO: derive fs from `root`
        self.fs = fsspec.filesystem("file") if fs is None else fs
        self.out_root = self.fs.unstrip_protocol(out_root or self.root)

        from importlib.util import find_spec

        if self.engine == "pyarrow" and find_spec("pyarrow") is None:
            raise ImportError("engine choice `pyarrow` is not installed.")

        # Apply `lru_cache` decorator manually per instance.
        # This way `self` reference is not held on class level.
        # WARNING: However, this means that self and its members are not reflected
        # in the cache key, so we expect they won't be mutated once a value is cached.
        # The listing only depends on the metadata snapshot and is cleared whenever
        # metadata changes; chunk coordinates are intentionally *not* cached because
        # a shape edit changes every coordinate's physical location.
        if not hasattr(self.listdir, "cache_clear"):
            self.listdir = lru_cache()(self.listdir)

    _LAZY_ATTRS = (
        "_items",
        "record_size",
        "zmetadata",
        "_chunk_overlay",
        "_shape_transitions",
        "_base_grid",
        "_grid_cache",
    )

    def __getattr__(self, item):
        if item in self._LAZY_ATTRS:
            self.setup()
            # avoid possible recursion if setup fails somehow
            return self.__dict__[item]
        raise AttributeError(item)

    @staticmethod
    def _grid_from_zarray(zarray):
        """Logical chunk grid implied by an array's ``shape`` and ``chunks``."""
        shape = zarray.get("shape", [])
        chunks = zarray.get("chunks", [])
        grid = tuple(math.ceil(s / c) for s, c in zip(shape, chunks))
        # A scalar array has no dimensions but still has one reference (``field/0``).
        return grid or (1,)

    def setup(self):
        self._items = {}
        self._items[".zmetadata"] = self.fs.cat_file(
            "/".join([self.root, ".zmetadata"])
        )
        met = json.loads(self._items[".zmetadata"])
        self.record_size = met["record_size"]
        self.zmetadata = met["metadata"]

        # The base index is immutable for the lifetime of a revision: reads of
        # untouched keys keep resolving against the original root, while edits
        # live in the overlay below.
        self._base_grid = {}
        for key, value in self.zmetadata.items():
            if key.endswith("/.zarray"):
                self._base_grid[key[: -len("/.zarray")]] = self._grid_from_zarray(
                    value
                )
        # Overlay values are keyed by *logical* chunk coordinate, not by their
        # position in a physical parquet record. A shape change therefore cannot
        # move a value onto a different key.
        self._chunk_overlay = {}
        # Shape changes record which old coordinates were removed. This makes a
        # shrink destructive, matching zarr resize semantics: widening the array
        # again must not resurrect the truncated references.
        self._shape_transitions = {}
        self._grid_cache = {}
        self.chunk_sizes = self._grid_cache

        # Define function to open and decompress refs
        @lru_cache(maxsize=self.cache_size)
        def open_refs(field, record):
            """cached parquet file loader"""
            path = self.url.format(field=field, record=record)
            data = io.BytesIO(self.fs.cat_file(path))
            try:
                try:
                    df = self.pd.read_parquet(data, engine=self.engine)
                except ImportError:
                    # The metadata does not record which parquet engine wrote
                    # the store. If the configured engine is unavailable, the
                    # other supported engine can still read the same files.
                    if self.engine == "fastparquet":
                        df = self.pd.read_parquet(data, engine="pyarrow")
                    else:
                        raise
                refs = {c: df[c].to_numpy(copy=True) for c in df.columns}
            except OSError:
                refs = None
            return refs

        self.open_refs = open_refs

    @staticmethod
    def create(root, storage_options=None, fs=None, record_size=10000, **kwargs):
        """Make empty parquet reference set

        First deletes the contents of the given directory, if it exists.

        Parameters
        ----------
        root: str
            Directory to contain the output; will be created
        storage_options: dict | None
            For making the filesystem to use for writing is fs is None
        fs: FileSystem | None
            Filesystem for writing
        record_size: int
            Number of references per parquet file
        kwargs: passed to __init__

        Returns
        -------
        LazyReferenceMapper instance
        """
        met = {"metadata": {}, "record_size": record_size}
        if fs is None:
            fs, root = fsspec.core.url_to_fs(root, **(storage_options or {}))
        if fs.exists(root):
            fs.rm(root, recursive=True)
        fs.makedirs(root, exist_ok=True)
        fs.pipe("/".join([root, ".zmetadata"]), json.dumps(met).encode())
        return LazyReferenceMapper(root, fs, **kwargs)

    def listdir(self):
        """List top-level directories"""
        dirs = (p.rsplit("/", 1)[0] for p in self.zmetadata if not p.startswith(".z"))
        return set(dirs)

    def ls(self, path="", detail=True):
        """Shortcut file listings"""
        path = path.rstrip("/")
        pathdash = path + "/" if path else ""
        dirnames = self.listdir()
        dirs = [
            d
            for d in dirnames
            if d.startswith(pathdash) and "/" not in d.lstrip(pathdash)
        ]
        if dirs:
            others = {
                f
                for f in chain(
                    [".zmetadata"],
                    (name for name in self.zmetadata),
                    (name for name in self._items),
                )
                if f.startswith(pathdash) and "/" not in f.lstrip(pathdash)
            }
            if detail is False:
                others.update(dirs)
                return sorted(others)
            dirinfo = [{"name": name, "type": "directory", "size": 0} for name in dirs]
            fileinfo = [
                {
                    "name": name,
                    "type": "file",
                    "size": len(
                        json.dumps(self.zmetadata[name])
                        if name in self.zmetadata
                        else self._items[name]
                    ),
                }
                for name in others
            ]
            return sorted(dirinfo + fileinfo, key=lambda s: s["name"])
        field = path
        if not any(
            name.startswith(f"{path}/")
            for name in chain(self.zmetadata, self._items)
        ) and f"{path}/.zarray" not in self.zmetadata:
            # Not a known directory or array: preserve a KeyError so callers can
            # fall back (ReferenceFileSystem.ls raises FileNotFoundError).
            raise KeyError(path)
        others = set(
            [name for name in self.zmetadata if name.startswith(f"{path}/")]
            + [name for name in self._items if name.startswith(f"{path}/")]
        )
        fileinfo = [
            {
                "name": name,
                "type": "file",
                "size": len(
                    json.dumps(self.zmetadata[name])
                    if name in self.zmetadata
                    else self._items[name]
                ),
            }
            for name in others
        ]
        keys = self._keys_in_field(field)

        if detail is False:
            return list(others) + [k for k in keys if k in self]
        recinfo = []
        for name in keys:
            try:
                rec = self[name]
            except KeyError:
                # filters out path==None, deleted/missing or out-of-shape keys
                continue
            if isinstance(rec, bytes):
                size = len(_inline_data(rec))
            elif isinstance(rec, str):
                size = len(_inline_data(rec))
            elif len(rec) == 1:
                size = None
            else:
                size = rec[-1]
            recinfo.append({"name": name, "type": "file", "size": size})
        return fileinfo + recinfo

    def _load_one_key(self, key):
        """Get the reference for one key

        Returns bytes, one-element list or three-element list.
        """
        if key in self._items:
            value = self._items[key]
            if value is None:
                raise KeyError(key)
            return value
        elif key in self.zmetadata:
            return json.dumps(self.zmetadata[key]).encode()
        elif "/" not in key or self._is_meta(key):
            raise KeyError(key)
        field, _ = key.rsplit("/", 1)
        index = _chunk_index(key)
        grid = self._get_chunk_sizes(field, index)
        if not _within(index, grid):
            # Out of shape: this reference is missing, regardless of whether an
            # old physical record still happens to hold data for the slot.
            raise KeyError(key)
        return self._value_at(field, index)

    def _value_at(self, field, index):
        """Resolve one logical chunk coordinate against overlay then base index."""
        overlay = self._chunk_overlay.get(field)
        if overlay is not None and index in overlay:
            value = overlay[index]
            if value is None:
                raise KeyError(f"{field}/{'.'.join(map(str, index))}")
            return value
        if self._removed(field, index):
            raise KeyError(f"{field}/{'.'.join(map(str, index))}")
        base_grid = self._base_grid.get(field)
        if not base_grid or not _within(index, base_grid):
            raise KeyError(f"{field}/{'.'.join(map(str, index))}")
        return self._load_base_key(field, index, base_grid)

    def _load_base_key(self, field, index, grid):
        record, ri = self._locate(field, index, grid)
        # Chunk keys can be loaded from row group and cached in LRU cache
        try:
            refs = self.open_refs(field, record)
        except (ValueError, TypeError, FileNotFoundError) as exc:
            raise KeyError(f"{field}/{'.'.join(map(str, index))}") from exc
        if refs is None:
            raise KeyError(f"{field}/{'.'.join(map(str, index))}")
        columns = ["path", "offset", "size", "raw"]
        if ri >= len(next(iter(refs.values()), ())):
            # Padding row: a record covers ``record_size`` slots, but the array
            # may have fewer chunks than that at the tail.
            raise KeyError(f"{field}/{'.'.join(map(str, index))}")
        selection = [refs[c][ri] if c in refs else None for c in columns]
        raw = selection[-1]
        if not _is_null(raw):
            return _as_raw_bytes(raw)
        if _is_null(selection[0]):
            raise KeyError(
                f"{field}/{'.'.join(map(str, index))} does not exist or was deleted"
            )
        offset, size = selection[1], selection[2]
        if _is_null(offset) and _is_null(size):
            # URL only
            return [selection[0]]
        offset = 0 if _is_null(offset) else offset
        size = 0 if _is_null(size) else size
        if offset == 0 and size == 0:
            return [selection[0]]
        return [selection[0], offset, size]

    def _locate(self, field, index, grid):
        """Map a logical chunk coordinate to (record, row) in the base index."""
        chunk_number = ravel_multi_index(index, grid)
        return chunk_number // self.record_size, chunk_number % self.record_size

    def _get_chunk_sizes(self, field, index=None):
        """The number of chunks along each axis for a given field

        Lookups use the *current* grid derived from ``.zarray`` metadata. A key
        written with a different number of dimensions is treated as missing
        rather than indexing into an unrelated shape.
        """
        if field not in self._grid_cache:
            zarray = self.zmetadata.get(f"{field}/.zarray")
            if zarray is None:
                self._grid_cache[field] = ()
            else:
                self._grid_cache[field] = self._grid_from_zarray(zarray)
        grid = self._grid_cache[field]
        if index is not None and len(index) != len(grid):
            return ()
        return grid

    def _removed(self, field, index):
        """Whether a coordinate was cut away by an earlier shape shrink."""
        transitions = self._shape_transitions.get(field)
        if not transitions:
            return False
        for old_grid, new_grid in transitions:
            if _within(index, new_grid):
                continue
            if _within(index, old_grid):
                # It existed before the shrink and is outside the smaller shape.
                return True
        return False

    def _generate_record(self, field, record):
        """The references for a given parquet file of a given field"""
        refs = self.open_refs(field, record)
        if refs is None:
            return iter(())
        it = iter(zip(*refs.values()))
        if len(refs) == 3:
            # All urls
            return (list(t) for t in it)
        elif len(refs) == 1:
            # All raws
            return iter(refs["raw"])
        else:
            # Mix of urls and raws
            return (
                _as_raw_bytes(t[3]) if not _is_null(t[3]) else list(t[:3])
                for t in it
            )

    def _generate_all_records(self, field):
        """Load all the references within a field by iterating over the parquet files

        This reflects the original on-disk index, not pending edits.
        """
        nrec = 1
        for ch in self._get_chunk_sizes(field):
            nrec *= ch
        nrec = math.ceil(nrec / self.record_size)
        for record in range(nrec):
            yield from self._generate_record(field, record)

    def values(self):
        return RefsValuesView(self)

    def items(self):
        return RefsItemsView(self)

    def __hash__(self):
        return id(self)

    def __getitem__(self, key):
        return self._load_one_key(key)

    def __setitem__(self, key, value):
        if "/" in key and not self._is_meta(key):
            field, _ = key.rsplit("/", 1)
            index = _chunk_index(key)
            # Edits are indexed by their logical coordinate, so later shape
            # changes never shuffle them onto a different key.
            self._chunk_overlay.setdefault(field, {})[index] = value
        else:
            # metadata or top-level
            if hasattr(value, "to_bytes"):
                value = value.to_bytes()
            if isinstance(value, bytes):
                val = value.decode()
            elif isinstance(value, str):
                val = value
            else:
                # allow a dict (or other JSON-serializable object)
                val = json.dumps(value)
            is_zarray = key != ".zarray" and key.endswith("/.zarray")
            field = key[: -len("/.zarray")] if is_zarray else ""
            old_grid = self._get_chunk_sizes(field) if is_zarray else ()
            try:
                new_value = json.loads(val)
            except ValueError:
                # A plain inline value, not JSON metadata.
                new_value = None
            if isinstance(new_value, dict):
                merged = {**self.zmetadata.get(key, {}), **new_value}
                self.zmetadata[key] = merged
                # Keep the in-memory view canonical, so reading the metadata key
                # back shows the same merged object flush will write.
                self._items[key] = json.dumps(merged)
            else:
                self._items[key] = val
            if is_zarray:
                self._update_grid(field, old_grid)
            self.listdir.cache_clear()

    def _update_grid(self, field, old_grid):
        """Recompute a field's chunk grid and apply destructive resizes."""
        new_grid = self._grid_from_zarray(self.zmetadata[f"{field}/.zarray"])
        if (
            old_grid
            and len(old_grid) == len(new_grid)
            and old_grid != new_grid
        ):
            # A shrink is destructive: coordinates cut off now are gone for good
            # even if the array is later widened again.
            if any(n < o for n, o in zip(new_grid, old_grid)):
                self._shape_transitions.setdefault(field, []).append(
                    (old_grid, new_grid)
                )
                overlay = self._chunk_overlay.get(field)
                if overlay:
                    for index in list(overlay):
                        if not _within(index, new_grid):
                            del overlay[index]
                    if not overlay:
                        self._chunk_overlay.pop(field, None)
        self._grid_cache[field] = new_grid
        self.listdir.cache_clear()

    @staticmethod
    def _is_meta(key):
        return key.startswith(".z") or "/.z" in key

    def __delitem__(self, key):
        if key in self.zmetadata:
            del self.zmetadata[key]
            self._items.pop(key, None)
            if key != ".zarray" and key.endswith("/.zarray"):
                field = key[: -len("/.zarray")]
                self._grid_cache.pop(field, None)
            self.listdir.cache_clear()
        elif "/" in key and not self._is_meta(key):
            field, _ = key.rsplit("/", 1)
            index = _chunk_index(key)
            self._chunk_overlay.setdefault(field, {})[index] = None
        elif key in self._items:
            self._items[key] = None
        else:
            # metadata or top-level
            self._items[key] = None

    def _dirty_records(self, field, grid):
        """Records of an unchanged grid that hold pending edits."""
        return {
            ravel_multi_index(index, grid) // self.record_size
            for index in self._chunk_overlay.get(field, {})
            if _within(index, grid)
        }

    def _index_engine(self):
        """Parquet engine to use for writing, degrading to pyarrow if needed."""
        from importlib.util import find_spec

        if self.engine == "fastparquet" and find_spec("fastparquet") is None:
            if find_spec("pyarrow") is not None:
                return "pyarrow"
        return self.engine

    def _write_record(self, field, record, rows, base_url=None, storage_options=None):
        """Write one parquet record from already-resolved values."""
        import numpy as np
        import pandas as pd

        paths = np.full(self.record_size, np.nan, dtype="O")
        offsets = np.zeros(self.record_size, dtype="int64")
        sizes = np.zeros(self.record_size, dtype="int64")
        raws = np.full(self.record_size, np.nan, dtype="O")
        for j, data in enumerate(rows):
            if data is None:
                # delete / missing
                paths[j] = None
                raws[j] = None
            elif isinstance(data, (list, tuple)):
                paths[j] = data[0]
                if len(data) > 1:
                    offsets[j] = data[1]
                    sizes[j] = data[2]
            else:
                raws[j] = _proc_raw(data)
        df = pd.DataFrame(
            {"path": paths, "offset": offsets, "size": sizes, "raw": raws},
            copy=False,
        )
        if df.path.count() / (df.path.nunique() or 1) > self.cat_thresh:
            df["path"] = df["path"].astype("category")
        object_encoding = {"raw": "bytes", "path": "utf8"}
        has_nulls = ["path", "raw"]

        root = base_url or self.out_root
        fn = f"{root}/{field}/refs.{record}.parq"
        self.fs.makedirs(f"{root}/{field}", exist_ok=True)

        engine = self._index_engine()
        pyarrow_kwargs = {
            "write_statistics": False,
            "compression": "zstd",
            "index": False,
        }
        fastparquet_kwargs = {"compression": "zstd", "index": False}
        if engine == "fastparquet":
            fastparquet_kwargs.update(
                stats=False,
                object_encoding=object_encoding,
                has_nulls=has_nulls,
            )
        elif engine != "pyarrow":
            raise NotImplementedError(f"{engine} not supported")

        if engine == "pyarrow":
            # Writing through a file handle works for URL/path and non-file
            # backends alike, without pandas having to infer a filesystem.
            with self.fs.open(fn, "wb") as f:
                df.to_parquet(f, engine="pyarrow", **pyarrow_kwargs)
        else:
            # fastparquet requires a path and uses the fsspec URL machinery.
            full = self.fs.unstrip_protocol(fn)
            df.to_parquet(
                full,
                engine="fastparquet",
                storage_options=storage_options
                or getattr(self.fs, "storage_options", None),
                **fastparquet_kwargs,
            )

    def _resolve_record(self, field, record, grid):
        """Resolve one record's worth of logical coordinates from base+overlay."""
        total = _grid_product(grid)
        start = record * self.record_size
        rows = []
        for pos in range(start, start + self.record_size):
            if pos >= total:
                rows.append(None)
                continue
            try:
                rows.append(self._value_at(field, unravel_multi_index(pos, grid)))
            except KeyError:
                rows.append(None)
        return rows

    def write(self, field, record, base_url=None, storage_options=None):
        """Materialize a single record of a field into the output index.

        This is the low-level counterpart of ``flush``; ordinary callers only
        need ``flush``.
        """
        grid = self._get_chunk_sizes(field)
        if not grid:
            return
        rows = self._resolve_record(field, record, grid)
        self._write_record(
            field,
            record,
            rows,
            base_url=base_url,
            storage_options=storage_options,
        )

    def _copy_index_record(self, field, record, out_root):
        """Copy one parquet index record without touching payload data."""
        src = f"{self.root}/{field}/refs.{record}.parq"
        dst = f"{out_root}/{field}/refs.{record}.parq"
        if not self.fs.exists(src):
            return
        self.fs.makedirs(f"{out_root}/{field}", exist_ok=True)
        self.fs.cp_file(src, dst)

    def _copy_other_field(self, field, out_root):
        """Copy index files of a field that is not a shaped array."""
        try:
            entries = self.fs.ls(f"{self.root}/{field}", detail=True)
        except FileNotFoundError:
            return
        for entry in entries:
            if entry.get("type") != "file":
                continue
            name = entry["name"]
            if not name.endswith(".parq"):
                continue
            dst = f"{out_root}/{field}/{name.rsplit('/', 1)[-1]}"
            self.fs.makedirs(f"{out_root}/{field}", exist_ok=True)
            self.fs.cp_file(name, dst)

    def _current_fields(self):
        fields = {
            k[: -len("/.zarray")] for k in self.zmetadata if k.endswith("/.zarray")
        }
        fields.update(self._chunk_overlay)
        fields.update(self._base_grid)
        return fields

    def flush(self, base_url=None, storage_options=None):
        """Output any modified or deleted keys

        Parameters
        ----------
        base_url: str
            Location of the output
        """
        out_root = self.fs._strip_protocol(base_url or self.out_root)
        self.fs.makedirs(out_root, exist_ok=True)
        in_place = out_root == self.fs._strip_protocol(self.root)

        # Write every field that was touched, and copy the parquet index of
        # untouched fields, so the output directory is a complete index on its
        # own. No payload object is ever read or copied here.
        for field in sorted(self._current_fields()):
            grid = self._get_chunk_sizes(field)
            if not grid:
                if not in_place:
                    self._copy_other_field(field, out_root)
                continue
            total = _grid_product(grid)
            nrec = math.ceil(total / self.record_size)
            base_grid = self._base_grid.get(field)
            transitions = self._shape_transitions.get(field)
            if base_grid == grid and not transitions:
                # Coordinates did not move; only records holding an edit need
                # rewriting, the rest can be copied verbatim.
                dirty = self._dirty_records(field, grid)
                for record in range(nrec):
                    if record not in dirty:
                        if not in_place:
                            self._copy_index_record(field, record, out_root)
                        continue
                    rows = self._resolve_record(field, record, grid)
                    self._write_record(
                        field,
                        record,
                        rows,
                        base_url=out_root,
                        storage_options=storage_options,
                    )
                continue
            for record in range(nrec):
                rows = self._resolve_record(field, record, grid)
                self._write_record(
                    field,
                    record,
                    rows,
                    base_url=out_root,
                    storage_options=storage_options,
                )

        # gather .zmetadata from self._items and write that too
        for k in list(self._items):
            if k != ".zmetadata" and ".z" in k:
                value = self._items.pop(k)
                if value is None:
                    # deleted metadata
                    self.zmetadata.pop(k, None)
                    continue
                try:
                    parsed = json.loads(value)
                except ValueError:
                    continue
                if isinstance(parsed, dict):
                    # Merge, because a caller may have supplied only a subset of
                    # the metadata fields (e.g. just ``shape``).
                    self.zmetadata[k] = {**self.zmetadata.get(k, {}), **parsed}
                else:
                    self.zmetadata[k] = parsed
        met = {"metadata": self.zmetadata, "record_size": self.record_size}
        encoded = json.dumps(met).encode()
        self.fs.pipe(f"{out_root}/.zmetadata", encoded)

        if hasattr(self.open_refs, "cache_clear"):
            self.open_refs.cache_clear()
        if in_place:
            # Reads now resolve against the files we just wrote.
            self._base_grid = {
                f: self._get_chunk_sizes(f)
                for f in self._current_fields()
                if self._get_chunk_sizes(f)
            }
            self._chunk_overlay.clear()
            self._shape_transitions.clear()
            self._grid_cache.clear()
            self._items.clear()
            self._items[".zmetadata"] = encoded
        else:
            # The revision is now self-contained; read it back from the output
            # so this mapper reflects the flushed index (the source stays
            # untouched and no longer needs to be reachable).
            self.root = out_root
            self.url = self.root + "/{field}/refs.{record}.parq"
            self.listdir.cache_clear()
            self.setup()

    def __len__(self):
        # Caveat: This counts expected references, not actual - but is fast
        count = 0
        for field in self.listdir():
            if field.startswith("."):
                count += 1
            else:
                count += _grid_product(self._get_chunk_sizes(field))
        count += len(self.zmetadata)  # all metadata keys
        # any other files not in reference partitions
        count += sum(
            1
            for k, v in self._items.items()
            if not isinstance(k, tuple) and v is not None
        )
        return count

    def __iter__(self):
        # Caveat: returns only existing keys, so the number of these does not
        #  match len(self)
        metas = set(self.zmetadata)
        metas.update(
            k for k, v in self._items.items() if isinstance(k, str) and v is not None
        )
        for bit in metas:
            if isinstance(bit, str):
                yield bit
        for field in self.listdir():
            for k in self._keys_in_field(field):
                if k in self:
                    yield k

    def __contains__(self, item):
        try:
            self._load_one_key(item)
            return True
        except KeyError:
            return False

    def _keys_in_field(self, field):
        """List key names in given field

        Produces strings like "field/x.y" appropriate from the chunking of the array
        """
        if f"{field}/.zarray" not in self.zmetadata:
            chunk_sizes = ()
        else:
            chunk_sizes = self._get_chunk_sizes(field)
        if len(chunk_sizes) == 0:
            return
        inds = itertools.product(*(range(i) for i in chunk_sizes))
        for ind in inds:
            yield field + "/" + ".".join([str(c) for c in ind])


class ReferenceFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system
    Initial version: single file system target, which must support
    async, and must allow start and end args in _cat_file. Later versions
    may allow multiple arbitrary URLs for the targets.
    This FileSystem is read-only. It is designed to be used with async
    targets (for now). We do not get original file details from the target FS.
    Configuration is by passing a dict of references at init, or a URL to
    a JSON file containing the same; this dict
    can also contain concrete data for some set of paths.
    Reference dict format:
    {path0: bytes_data, path1: (target_url, offset, size)}
    https://github.com/fsspec/kerchunk/blob/main/README.md

    simple_references: if True (default), no jinja interpreting is done,
        which is the safe option.
    """

    protocol = "reference"
    cachable = False

    def __init__(
        self,
        fo,
        target=None,
        ref_storage_args=None,
        target_protocol=None,
        target_options=None,
        remote_protocol=None,
        remote_options=None,
        fs=None,
        template_overrides=None,
        simple_templates=True,
        max_gap=64_000,
        max_block=256_000_000,
        cache_size=128,
        **kwargs,
    ):
        """
        Parameters
        ----------
        fo : dict or str
            The set of references to use for this instance, with a structure as above.
            If str referencing a JSON file, will use fsspec.open, in conjunction
            with target_options and target_protocol to open and parse JSON at this
            location. If a directory, then assume references are a set of parquet
            files to be loaded lazily.
        target : str
            For any references having target_url as None, this is the default file
            target to use
        ref_storage_args : dict
            If references is a str, use these kwargs for loading the JSON file.
            Deprecated: use target_options instead.
        target_protocol : str
            Used for loading the reference file, if it is a path. If None, protocol
            will be derived from the given path
        target_options : dict
            Extra FS options for loading the reference file ``fo``, if given as a path
        remote_protocol : str
            The protocol of the filesystem on which the references will be evaluated
            (unless fs is provided). If not given, will be derived from the first
            URL that has a protocol in the templates or in the references, in that
            order.
        remote_options : dict
            kwargs to go with remote_protocol
        fs : AbstractFileSystem | dict(str, (AbstractFileSystem | dict))
            Directly provide a file system(s):
                - a single filesystem instance
                - a dict of protocol:filesystem, where each value is either a filesystem
                  instance, or a dict of kwargs that can be used to create in
                  instance for the given protocol

            If this is given, remote_options and remote_protocol are ignored.
        template_overrides : dict
            Swap out any templates in the references file with these - useful for
            testing.
        simple_templates: bool
            Whether templates can be processed with simple replace (True) or if
            jinja  is needed (False, much slower). All reference sets produced by
            ``kerchunk`` are simple in this sense, but the spec allows for complex.
        max_gap, max_block: int
            For merging multiple concurrent requests to the same remote file.
            Neighboring byte ranges will only be merged when their
            inter-range gap is <= ``max_gap``. Default is 64KB. Set to 0
            to only merge when it requires no extra bytes. Pass a negative
            number to disable merging, appropriate for local target files.
            Neighboring byte ranges will only be merged when the size of
            the aggregated range is <= ``max_block``. Default is 256MB.
        cache_size : int
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
            Only used for lazily loaded references.
        kwargs : passed to parent class
        """
        super().__init__(**kwargs)
        self.target = target
        self.template_overrides = template_overrides
        self.simple_templates = simple_templates
        self.templates = {}
        self.fss = {}
        self._dircache = {}
        self.max_gap = max_gap
        self.max_block = max_block
        if isinstance(fo, str):
            dic = dict(
                **(ref_storage_args or target_options or {}), protocol=target_protocol
            )
            ref_fs, fo2 = fsspec.core.url_to_fs(fo, **dic)
            if ".json" not in fo2 and (
                fo.endswith(("parq", "parquet", "/")) or ref_fs.isdir(fo2)
            ):
                # Lazy parquet refs
                logger.info("Open lazy reference dict from URL %s", fo)
                self.references = LazyReferenceMapper(
                    fo2,
                    fs=ref_fs,
                    cache_size=cache_size,
                )
            else:
                # text JSON
                with fsspec.open(fo, "rb", **dic) as f:
                    logger.info("Read reference from URL %s", fo)
                    text = json.load(f)
                self._process_references(text, template_overrides)
        else:
            # dictionaries
            self._process_references(fo, template_overrides)
        if isinstance(fs, dict):
            self.fss = {
                k: (
                    fsspec.filesystem(k.split(":", 1)[0], **opts)
                    if isinstance(opts, dict)
                    else opts
                )
                for k, opts in fs.items()
            }
            if None not in self.fss:
                self.fss[None] = filesystem("file")
            return
        if fs is not None:
            # single remote FS
            remote_protocol = (
                fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
            )
            self.fss[remote_protocol] = fs

        if remote_protocol is None:
            # get single protocol from any templates
            for ref in self.templates.values():
                if callable(ref):
                    ref = ref()
                protocol, _ = fsspec.core.split_protocol(ref)
                if protocol and protocol not in self.fss:
                    fs = filesystem(protocol, **(remote_options or {}))
                    self.fss[protocol] = fs
        if remote_protocol is None:
            # get single protocol from references
            # TODO: warning here, since this can be very expensive?
            for ref in self.references.values():
                if callable(ref):
                    ref = ref()
                if isinstance(ref, list) and ref[0]:
                    protocol, _ = fsspec.core.split_protocol(ref[0])
                    if protocol not in self.fss:
                        fs = filesystem(protocol, **(remote_options or {}))
                        self.fss[protocol] = fs
                        # only use first remote URL
                        break

        if remote_protocol and remote_protocol not in self.fss:
            fs = filesystem(remote_protocol, **(remote_options or {}))
            self.fss[remote_protocol] = fs

        self.fss[None] = fs or filesystem("file")  # default one
        # Wrap any non-async filesystems to ensure async methods are available below
        for k, f in self.fss.items():
            if not f.async_impl:
                self.fss[k] = AsyncFileSystemWrapper(f, asynchronous=self.asynchronous)
            elif self.asynchronous ^ f.asynchronous:
                raise ValueError(
                    "Reference-FS's target filesystem must have same value "
                    "of asynchronous"
                )

    def _cat_common(self, path, start=None, end=None):
        path = self._strip_protocol(path)
        logger.debug(f"cat: {path}")
        try:
            part = self.references[path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc
        if isinstance(part, str):
            part = part.encode()
        if hasattr(part, "to_bytes"):
            part = part.to_bytes()
        if isinstance(part, bytes):
            logger.debug(f"Reference: {path}, type bytes")
            return _inline_data(part), None, None

        if len(part) == 1:
            logger.debug(f"Reference: {path}, whole file => {part}")
            url = part[0]
            start1, end1 = start, end
        else:
            url, start0, size = part
            logger.debug(f"Reference: {path} => {url}, offset {start0}, size {size}")
            end0 = start0 + size

            # Resolve offsets like slicing the part, so they never reach target
            # bytes outside the referenced range.
            if start is not None:
                if start >= 0:
                    start1 = min(start0 + start, end0)
                else:
                    start1 = max(end0 + start, start0)
            else:
                start1 = start0
            if end is not None:
                if end >= 0:
                    end1 = min(start0 + end, end0)
                else:
                    end1 = max(end0 + end, start0)
            else:
                end1 = end0
        if url is None:
            url = self.target
        return url, start1, end1

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path, start=start, end=end)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        try:
            return await self.fss[protocol]._cat_file(
                part_or_url, start=start0, end=end0, **kwargs
            )
        except Exception as e:
            raise ReferenceNotReachable(path, part_or_url) from e

    def cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path, start=start, end=end)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        try:
            return self.fss[protocol].cat_file(
                part_or_url, start=start0, end=end0, **kwargs
            )
        except Exception as e:
            raise ReferenceNotReachable(path, part_or_url) from e

    def pipe_file(self, path, value, **_):
        """Temporarily add binary data or reference as a file"""
        self.references[path] = value

    async def _get_file(self, rpath, lpath, **kwargs):
        if self.isdir(rpath):
            return os.makedirs(lpath, exist_ok=True)
        data = await self._cat_file(rpath)
        with open(lpath, "wb") as f:
            f.write(data)

    def get_file(self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs):
        if self.isdir(rpath):
            return os.makedirs(lpath, exist_ok=True)
        data = self.cat_file(rpath, **kwargs)
        callback.set_size(len(data))
        if isfilelike(lpath):
            lpath.write(data)
        else:
            with open(lpath, "wb") as f:
                f.write(data)
        callback.absolute_update(len(data))

    def get(self, rpath, lpath, recursive=False, maxdepth=None, **kwargs):
        if recursive:
            # trigger directory build
            self.ls("")
        rpath = self.expand_path(rpath, recursive=recursive, maxdepth=maxdepth)
        fs = fsspec.filesystem("file", auto_mkdir=True)
        targets = other_paths(rpath, lpath)
        if isinstance(lpath, str):
            # The names came from the source listing; ".." in one of them
            # would otherwise place the copy above the destination. When
            # lpath is a list the caller named every destination itself.
            check_contained(lpath, targets)
        if recursive:
            data = self.cat([r for r in rpath if not self.isdir(r)])
        else:
            data = self.cat(rpath)
        for remote, local in zip(rpath, targets):
            if remote in data:
                fs.pipe_file(local, data[remote])

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        if isinstance(path, str) and recursive:
            raise NotImplementedError
        if isinstance(path, list) and (recursive or any("*" in p for p in path)):
            raise NotImplementedError
        # TODO: if references is lazy, pre-fetch all paths in batch before access
        proto_dict = _protocol_groups(path, self.references)
        out = {}
        for proto, paths in proto_dict.items():
            fs = self.fss[proto]
            urls, starts, ends, valid_paths = [], [], [], []
            for p in paths:
                # find references or label not-found. Early exit if any not
                # found and on_error is "raise"
                try:
                    u, s, e = self._cat_common(p)
                    if not isinstance(u, (bytes, str)):
                        # nan/None from parquet
                        continue
                except FileNotFoundError as err:
                    if on_error == "raise":
                        raise
                    if on_error != "omit":
                        out[p] = err
                else:
                    urls.append(u)
                    starts.append(s)
                    ends.append(e)
                    valid_paths.append(p)

            # process references into form for merging
            urls2 = []
            starts2 = []
            ends2 = []
            paths2 = []
            whole_files = set()
            for u, s, e, p in zip(urls, starts, ends, valid_paths):
                if isinstance(u, bytes):
                    # data
                    out[p] = u
                elif s is None:
                    # whole file - limits are None, None, but no further
                    # entries take for this file
                    whole_files.add(u)
                    urls2.append(u)
                    starts2.append(s)
                    ends2.append(e)
                    paths2.append(p)
            for u, s, e, p in zip(urls, starts, ends, valid_paths):
                # second run to account for files that are to be loaded whole
                if s is not None and u not in whole_files:
                    urls2.append(u)
                    starts2.append(s)
                    ends2.append(e)
                    paths2.append(p)

            # merge and fetch consolidated ranges
            new_paths, new_starts, new_ends = merge_offset_ranges(
                list(urls2),
                list(starts2),
                list(ends2),
                sort=True,
                max_gap=self.max_gap,
                max_block=self.max_block,
            )
            bytes_out = fs.cat_ranges(new_paths, new_starts, new_ends)

            # unbundle from merged bytes - simple approach
            for u, s, e, p in zip(urls, starts, ends, valid_paths):
                if p in out:
                    continue  # was bytes, already handled
                for np, ns, ne, b in zip(new_paths, new_starts, new_ends, bytes_out):
                    if np == u and (ns is None or ne is None):
                        if isinstance(b, Exception):
                            out[p] = b
                        else:
                            out[p] = b[s:e]
                    elif np == u and s >= ns and e <= ne:
                        if isinstance(b, Exception):
                            out[p] = b
                        else:
                            out[p] = b[s - ns : (e - ne) or None]

        for k, v in out.copy().items():
            # these were valid references, but fetch failed, so transform exc
            if isinstance(v, Exception) and k in self.references:
                ex = out[k]
                new_ex = ReferenceNotReachable(k, self.references[k])
                new_ex.__cause__ = ex
                if on_error == "raise":
                    raise new_ex
                elif on_error != "omit":
                    out[k] = new_ex
                else:
                    del out[k]

        if len(out) == 1 and isinstance(path, str) and "*" not in path:
            return _first(out)
        return out

    def _process_references(self, references, template_overrides=None):
        vers = references.get("version", None)
        if vers is None:
            self._process_references0(references)
        elif vers == 1:
            self._process_references1(references, template_overrides=template_overrides)
        else:
            raise ValueError(f"Unknown reference spec version: {vers}")
        # TODO: we make dircache by iterating over all entries, but for Spec >= 1,
        #  can replace with programmatic. Is it even needed for mapper interface?

    def _process_references0(self, references):
        """Make reference dict for Spec Version 0"""
        if isinstance(references, dict):
            # do not do this for lazy/parquet backend, which will not make dicts,
            # but must remain writable in the original object
            references = {
                key: json.dumps(val) if isinstance(val, dict) else val
                for key, val in references.items()
            }
        self.references = references

    def _process_references1(self, references, template_overrides=None):
        if not self.simple_templates or self.templates:
            import jinja2.sandbox
        self.references = {}
        self._process_templates(references.get("templates", {}))

        @lru_cache(1000)
        def _render_jinja(u):
            return (
                jinja2.sandbox.SandboxedEnvironment()
                .from_string(u)
                .render(**self.templates)
            )

        for k, v in references.get("refs", {}).items():
            if isinstance(v, str):
                if v.startswith("base64:"):
                    self.references[k] = base64.b64decode(v[7:])
                self.references[k] = v
            elif isinstance(v, dict):
                self.references[k] = json.dumps(v)
            elif self.templates:
                u = v[0]
                if "{{" in u:
                    if self.simple_templates:
                        u = (
                            u.replace("{{", "{")
                            .replace("}}", "}")
                            .format(**self.templates)
                        )
                    else:
                        u = _render_jinja(u)
                self.references[k] = [u] if len(v) == 1 else [u, v[1], v[2]]
            else:
                self.references[k] = v
        self.references.update(self._process_gen(references.get("gen", [])))

    def _process_templates(self, tmp):
        self.templates = {}
        if self.template_overrides is not None:
            tmp.update(self.template_overrides)
        for k, v in tmp.items():
            if "{{" in v:
                import jinja2.sandbox

                self.templates[k] = (
                    lambda temp=v, **kwargs: jinja2.sandbox.SandboxedEnvironment()
                    .from_string(temp)
                    .render(**kwargs)
                )
            else:
                self.templates[k] = v

    def _process_gen(self, gens):
        out = {}
        if self.simple_templates:
            return out
        for gen in gens:
            dimension = {
                k: (
                    v
                    if isinstance(v, list)
                    else range(v.get("start", 0), v["stop"], v.get("step", 1))
                )
                for k, v in gen["dimensions"].items()
            }
            products = (
                dict(zip(dimension.keys(), values))
                for values in itertools.product(*dimension.values())
            )
            for pr in products:
                import jinja2.sandbox

                key = (
                    jinja2.sandbox.SandboxedEnvironment()
                    .from_string(gen["key"])
                    .render(**pr, **self.templates)
                )
                url = (
                    jinja2.sandbox.SandboxedEnvironment()
                    .from_string(gen["url"])
                    .render(**pr, **self.templates)
                )
                if ("offset" in gen) and ("length" in gen):
                    offset = int(
                        jinja2.sandbox.SandboxedEnvironment()
                        .from_string(gen["offset"])
                        .render(**pr, **self.templates)
                    )
                    length = int(
                        jinja2.sandbox.SandboxedEnvironment()
                        .from_string(gen["length"])
                        .render(**pr, **self.templates)
                    )
                    out[key] = [url, offset, length]
                elif ("offset" in gen) ^ ("length" in gen):
                    raise ValueError(
                        "Both 'offset' and 'length' are required for a "
                        "reference generator entry if either is provided."
                    )
                else:
                    out[key] = [url]
        return out

    def _dircache_from_items(self):
        self.dircache = {"": []}
        it = self.references.items()
        for path, part in it:
            if isinstance(part, (bytes, str)) or hasattr(part, "to_bytes"):
                size = len(_inline_data(part))
            elif len(part) == 1:
                size = None
            else:
                _, _, size = part
            par = path.rsplit("/", 1)[0] if "/" in path else ""
            par0 = par
            subdirs = [par0]
            while par0 and par0 not in self.dircache:
                # collect parent directories
                par0 = self._parent(par0)
                subdirs.append(par0)

            subdirs.reverse()
            for parent, child in zip(subdirs, subdirs[1:]):
                # register newly discovered directories
                assert child not in self.dircache
                assert parent in self.dircache
                self.dircache[parent].append(
                    {"name": child, "type": "directory", "size": 0}
                )
                self.dircache[child] = []

            self.dircache[par].append({"name": path, "type": "file", "size": size})

    def _open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path)
        # This logic is kept outside `ReferenceFile` to avoid unnecessary redirection.
        # That does mean `_cat_common` gets called twice if it eventually reaches `ReferenceFile`.
        if isinstance(part_or_url, bytes):
            return io.BytesIO(part_or_url[start0:end0])

        protocol, _ = split_protocol(part_or_url)
        if start0 is None and end0 is None:
            return self.fss[protocol]._open(
                part_or_url,
                mode,
                block_size=block_size,
                cache_options=cache_options,
                **kwargs,
            )

        return ReferenceFile(
            self,
            path,
            mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs,
        )

    def ls(self, path, detail=True, **kwargs):
        logger.debug("list %s", path)
        path = self._strip_protocol(path)
        if isinstance(self.references, LazyReferenceMapper):
            try:
                return self.references.ls(path, detail)
            except KeyError:
                pass
            raise FileNotFoundError(f"'{path}' is not a known key")
        if not self.dircache:
            self._dircache_from_items()
        out = self._ls_from_cache(path)
        if out is None:
            raise FileNotFoundError(path)
        if detail:
            return out
        return [o["name"] for o in out]

    def exists(self, path, **kwargs):  # overwrite auto-sync version
        return self.isdir(path) or self.isfile(path)

    def isdir(self, path):  # overwrite auto-sync version
        if self.dircache:
            return path in self.dircache
        elif isinstance(self.references, LazyReferenceMapper):
            return path in self.references.listdir()
        else:
            # this may be faster than building dircache for single calls, but
            # by looping will be slow for many calls; could cache it?
            return any(_.startswith(f"{path}/") for _ in self.references)

    def isfile(self, path):  # overwrite auto-sync version
        return path in self.references

    async def _ls(self, path, detail=True, **kwargs):  # calls fast sync code
        return self.ls(path, detail, **kwargs)

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        if withdirs:
            return super().find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
            )
        if path:
            path = self._strip_protocol(path)
            r = sorted(k for k in self.references if k.startswith(path))
        else:
            r = sorted(self.references)
        if detail:
            if not self.dircache:
                self._dircache_from_items()
            return {k: self._ls_from_cache(k)[0] for k in r}
        else:
            return r

    def info(self, path, **kwargs):
        out = self.references.get(path)
        if out is not None:
            if isinstance(out, (str, bytes)):
                return {"name": path, "type": "file", "size": len(_inline_data(out))}
            elif len(out) > 1:
                return {"name": path, "type": "file", "size": out[2]}
            else:
                out0 = [{"name": path, "type": "file", "size": None}]
        else:
            out = self.ls(path, True)
            out0 = [o for o in out if o["name"] == path]
            if not out0:
                return {"name": path, "type": "directory", "size": 0}
        if out0[0]["size"] is None:
            # if this is a whole remote file, update size using remote FS
            prot, _ = split_protocol(self.references[path][0])
            out0[0]["size"] = self.fss[prot].size(self.references[path][0])
        return out0[0]

    async def _info(self, path, **kwargs):
        out = self.references.get(path)
        if isinstance(out, (list, tuple)) and len(out) == 1:
            # whole remote file: the size must come from the remote FS, which
            # cannot be called synchronously from inside the event loop
            prot, _ = split_protocol(out[0])
            fs = self.fss[prot]
            if fs.async_impl:
                size = await fs._size(out[0])
            else:
                size = fs.size(out[0])
            return {"name": path, "type": "file", "size": size}
        return self.info(path)  # calls fast sync code

    async def _rm_file(self, path, **kwargs):
        self.references.pop(
            path, None
        )  # ignores FileNotFound, just as well for directories
        self.dircache.clear()  # this is a bit heavy handed

    async def _pipe_file(self, path, data, mode="overwrite", **kwargs):
        if mode == "create" and self.exists(path):
            raise FileExistsError
        # can be str or bytes
        self.references[path] = data
        self.dircache.clear()  # this is a bit heavy handed

    async def _put_file(self, lpath, rpath, mode="overwrite", **kwargs):
        # puts binary
        if mode == "create" and self.exists(rpath):
            raise FileExistsError
        with open(lpath, "rb") as f:
            self.references[rpath] = f.read()
        self.dircache.clear()  # this is a bit heavy handed

    def save_json(self, url, **storage_options):
        """Write modified references into new location"""
        out = {}
        for k, v in self.references.items():
            if isinstance(v, bytes):
                try:
                    out[k] = v.decode("ascii")
                except UnicodeDecodeError:
                    out[k] = (b"base64:" + base64.b64encode(v)).decode()
            else:
                out[k] = v
        with fsspec.open(url, "wb", **storage_options) as f:
            f.write(json.dumps({"version": 1, "refs": out}).encode())


class ReferenceFile(AbstractBufferedFile):
    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            size=size,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )
        part_or_url, self.start, self.end = self.fs._cat_common(self.path)
        protocol, _ = split_protocol(part_or_url)
        self.src_fs = self.fs.fss[protocol]
        self.src_path = part_or_url
        self._f = None

    @property
    def f(self):
        if self._f is None or self._f.closed:
            self._f = self.src_fs._open(
                self.src_path,
                mode=self.mode,
                block_size=self.blocksize,
                autocommit=self.autocommit,
                cache_type="none",
                **self.kwargs,
            )
        return self._f

    def close(self):
        if self._f is not None:
            self._f.close()
        return super().close()

    def _fetch_range(self, start, end):
        start = start + self.start
        end = min(end + self.start, self.end)
        self.f.seek(start)
        return self.f.read(end - start)
