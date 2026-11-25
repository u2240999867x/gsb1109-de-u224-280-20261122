from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING

from fsspec.implementations.cached import SimpleCacheFileSystem
from fsspec.registry import get_filesystem_class
from fsspec.registry import register_implementation

from upath._chain import CACHE_PROTOCOLS as _CACHE_PROTOCOLS
from upath._chain import _CACHE_LAYER_NAMES
from upath.core import UPath
from upath.types import JoinablePathLike

if TYPE_CHECKING:
    from typing import Any
    from typing import Literal

    if sys.version_info >= (3, 11):
        from typing import Unpack
    else:
        from typing_extensions import Unpack

    from fsspec import AbstractFileSystem

    from upath._chain import FSSpecChainParser
    from upath.types.storage_options import SimpleCacheStorageOptions


__all__ = [
    "SimpleCachePath",
    "VerifiedSimpleCacheFileSystem",
    "CacheOptions",
    "OptionsDict",
    "ReleaseUnavailable",
    "ReleaseDigestMismatch",
    "ReleaseDigestError",
    "DigestMismatchError",
    "CacheDigestError",
]


class OptionsDict(dict):
    """plain JSON-compatible dict of filesystem options"""

    def as_json(self) -> dict[str, Any]:
        return json.loads(json.dumps(self))

    def to_dict(self) -> dict[str, Any]:
        return dict(self)


class CacheOptions(OptionsDict):
    """JSON-compatible options for each cache layer

    Behaves like a normal mapping with ``outer`` and ``inner`` keys, and can
    also be called to select one layer:

    >>> options("outer")          # doctest: +SKIP
    >>> options["outer"]          # doctest: +SKIP
    >>> options[0]                # doctest: +SKIP
    >>> options("inner", "cache_storage")  # doctest: +SKIP
    """

    _ALIASES = {
        "outer": "outer",
        "outermost": "outer",
        "0": "outer",
        "inner": "inner",
        "innermost": "inner",
        "1": "inner",
    }

    def _layer_name(self, key: Any) -> str:
        if isinstance(key, int):
            return "outer" if key == 0 else "inner"
        try:
            return self._ALIASES[str(key)]
        except KeyError:
            raise KeyError(f"unknown cache layer: {key!r}") from None

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return super().__getitem__(self._layer_name(key))
        if isinstance(key, str) and key in self._ALIASES:
            return super().__getitem__(self._ALIASES[key])
        return super().__getitem__(key)

    def __call__(
        self,
        layer: str | int | None = None,
        key: str | None = None,
    ) -> Any:
        if layer is None:
            return self
        options = self[layer]
        if key is None:
            return options
        return options.get(key)

    def __getattr__(self, name: str) -> Any:
        if name in ("outer", "inner"):
            try:
                return self[name]
            except KeyError:
                return {}
        raise AttributeError(name)

    def as_json(self) -> dict[str, Any]:
        return {key: dict(value) for key, value in self.items()}



_ARCHIVE_PROTOCOLS = frozenset({"zip", "tar"})
_DIGEST_KEYS = (
    "sha256",
    "sha256sum",
    "sha256_digest",
    "digest",
    "expected_sha256",
    "expected_digest",
    "release_sha256",
    "release_digest",
    "archive_sha256",
    "archive_digest",
    "content_sha256",
    "zip_sha256",
)
_CHUNK_SIZE = 1024 * 1024


class ReleaseUnavailable(FileNotFoundError):
    """raised when a requested release can not be obtained from any source"""


class ReleaseDigestMismatch(ReleaseUnavailable, ValueError):
    """raised when a downloaded ZIP does not match the expected release digest"""

    def __init__(
        self,
        message: str,
        *,
        expected_sha256: str | None = None,
        actual_sha256: str | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.expected_sha256 = expected_sha256
        self.actual_sha256 = actual_sha256
        self.path = path

    @property
    def expected(self) -> str | None:
        return self.expected_sha256

    @property
    def actual(self) -> str | None:
        return self.actual_sha256

    def __reduce__(self):
        return (
            type(self),
            (self.args[0],),
            {
                "expected_sha256": self.expected_sha256,
                "actual_sha256": self.actual_sha256,
                "path": self.path,
            },
        )

    def __setstate__(self, state):
        self.expected_sha256 = state.get("expected_sha256")
        self.actual_sha256 = state.get("actual_sha256")
        self.path = state.get("path")


DigestMismatchError = ReleaseDigestMismatch
CacheDigestError = ReleaseDigestMismatch
ReleaseDigestError = ReleaseDigestMismatch


def _normalize_sha256(value: Any) -> str:
    if isinstance(value, bytes):
        value = value.decode("ascii")
    normalized = str(value).strip().lower()
    if normalized.startswith("sha256:"):
        normalized = normalized.partition(":")[2].strip()
    if len(normalized) != 64 or any(
        c not in "0123456789abcdef" for c in normalized
    ):
        raise ValueError(f"invalid sha256 digest: {value!r}")
    return normalized


def _sha256_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(_CHUNK_SIZE)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def _find_digest(options: Any) -> Any:
    if not isinstance(options, Mapping):
        return None
    for key in _DIGEST_KEYS:
        value = options.get(key)
        if isinstance(value, (str, bytes)):
            return value
    for nested_key in ("target_options", "cache_options"):
        nested = options.get(nested_key)
        if isinstance(nested, Mapping):
            found = _find_digest(nested)
            if found is not None:
                return found
        elif isinstance(nested, (list, tuple)):
            for item in nested:
                found = _find_digest(item)
                if found is not None:
                    return found
    return None


def _pop_digest(options: dict[str, Any]) -> Any:
    found = None
    for key in _DIGEST_KEYS:
        if key in options:
            value = options.pop(key)
            if value is not None:
                found = value
    return found


class _DigestMapper:
    """maps names into a cache namespace selected by the release digest"""

    def __init__(self, digest: str, *, whole_file: bool) -> None:
        self.digest = digest
        self.whole_file = whole_file

    def __call__(self, path: str) -> str:
        if self.whole_file:
            return self.digest
        path_hash = hashlib.sha256(path.encode("utf-8")).hexdigest()
        return f"{self.digest}-{path_hash}"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, type(self))
            and self.digest == other.digest
            and self.whole_file == other.whole_file
        )

    def __hash__(self) -> int:
        return hash((type(self), self.digest, self.whole_file))


class LazyTargetFileSystem:
    """defer construction and connection of a chained target filesystem

    cache hits must work without any remote access, so the wrapped filesystem
    is only created on the first operation that actually needs it. a failed
    construction is not cached and may be retried.
    """

    _delegated = (
        "open",
        "_open",
        "get",
        "get_file",
        "put",
        "put_file",
        "cat",
        "cat_file",
        "cat_ranges",
        "exists",
        "info",
        "ls",
        "listdir",
        "_parent",
        "isdir",
        "isfile",
        "size",
        "sizes",
        "ukey",
        "glob",
        "find",
        "du",
        "walk",
        "expand_path",
        "mkdir",
        "makedirs",
        "rm",
        "rm_file",
        "_rm",
        "mv",
        "copy",
        "invalidate_cache",
        "checksum",
        "pipe_file",
        "pipe",
        "touch",
        "created",
        "modified",
        "read_block",
        "read_text",
        "write_text",
        "unstrip_protocol",
    )

    def __init__(
        self,
        protocol: str,
        options: Mapping[str, Any] | None = None,
        fs: AbstractFileSystem | None = None,
    ) -> None:
        self.protocol = protocol
        self.options = dict(options or {})
        self._provided = fs
        self._instance: AbstractFileSystem | None = fs

    async_impl = False
    mirror_sync_methods = False
    cachable = False
    local_file = False
    root_marker = "/"
    sep = "/"
    transaction_type = None

    @property
    def storage_options(self) -> dict[str, Any]:
        return dict(self.options)

    @property
    def storage_args(self) -> tuple[Any, ...]:
        return ()

    @property
    def _fs_token(self) -> str:
        from fsspec.utils import tokenize

        return tokenize(type(self).__name__, self.protocol, self.options)

    @property
    def fsid(self) -> str:
        return self._fs_token

    @property
    def _intrans(self) -> bool:
        return False

    @property
    def transaction(self) -> Any:
        return None

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return getattr(self._ensure(), name)

    def _ensure(self) -> AbstractFileSystem:
        if self._instance is None:
            fs_cls = get_filesystem_class(self.protocol)
            self._instance = fs_cls(**self.options)
        return self._instance

    def _strip_protocol(self, path: str) -> str:
        fs_cls = get_filesystem_class(self.protocol)
        return fs_cls._strip_protocol(path)

    def __repr__(self) -> str:
        state = "connected" if self._instance is not None else "lazy"
        return f"LazyTargetFileSystem({self.protocol!r}, {state})"


def _delegate(name: str):
    def method(self: LazyTargetFileSystem, *args: Any, **kwargs: Any) -> Any:
        return getattr(self._ensure(), name)(*args, **kwargs)

    method.__name__ = name
    return method


for _name in LazyTargetFileSystem._delegated:
    setattr(LazyTargetFileSystem, _name, _delegate(_name))


class VerifiedSimpleCacheFileSystem(SimpleCacheFileSystem):
    """simplecache with optional release-digest verification

    When a release digest is configured for a cache layer that stores a whole
    archive, the entry is stored under the digest itself. Downloads stream to
    a temporary file, are hashed only after the complete transfer, and are
    published atomically only on a digest match. A cached entry is verified
    before it is reused.

    For cache layers that store extracted archive members, the digest is used
    only as a cache namespace, so members from different releases of the same
    archive path never overwrite each other.
    """

    # Do not reuse cache filesystem instances: a reused instance may still hold
    # an opened archive from a previous upstream file, which would bypass digest
    # verification after the supplier replaces the release at the same URL.
    cachable = False

    def __init__(
        self,
        target_protocol: str | None = None,
        target_options: Mapping[str, Any] | None = None,
        fs: AbstractFileSystem | None = None,
        *,
        cache_storage: Any = "TMP",
        cache_check: Any = 10,
        check_files: Any = False,
        expiry_time: Any = 604800,
        same_names: bool | None = None,
        compression: Any = None,
        cache_mapper: Any = None,
        cache_storage_mode: Any = None,
        **kwargs: Any,
    ) -> None:
        digest_value = _pop_digest(kwargs)
        if digest_value is None:
            digest_value = _find_digest(target_options)
        self._release_sha256 = (
            None if digest_value is None else _normalize_sha256(digest_value)
        )

        if isinstance(fs, LazyTargetFileSystem):
            proxy = fs
            target_protocol = target_protocol or fs.protocol
        elif fs is not None:
            target_protocol = target_protocol or getattr(fs, "protocol", "file")
            proxy = LazyTargetFileSystem(target_protocol, target_options, fs)
        else:
            if target_protocol is None:
                raise ValueError(
                    "Please provide filesystem instance(fs) or target_protocol"
                )
            proxy = LazyTargetFileSystem(target_protocol, target_options)

        self._target_proxy = proxy
        self._target_protocol_name = str(target_protocol)
        self._verify_digest = (
            self._release_sha256 is not None
            and self._target_protocol_name not in _ARCHIVE_PROTOCOLS
        )

        super().__init__(
            fs=proxy,
            cache_storage=cache_storage,
            cache_check=cache_check,
            check_files=check_files,
            expiry_time=expiry_time,
            same_names=same_names,
            compression=compression,
            cache_mapper=cache_mapper,
            cache_storage_mode=cache_storage_mode,
            **kwargs,
        )
        if self._release_sha256 is not None:
            self._mapper = _DigestMapper(
                self._release_sha256,
                whole_file=self._verify_digest,
            )

    @property
    def release_sha256(self) -> str | None:
        return self._release_sha256

    @property
    def sha256(self) -> str | None:
        return self._release_sha256

    @property
    def expected_sha256(self) -> str | None:
        return self._release_sha256

    def _check_file(self, path: str) -> str | None:
        if self._release_sha256 is None:
            return super()._check_file(path)
        path = self._strip_protocol(path)
        self._check_cache()
        sha = self._mapper(path)
        for storage in self.storage:
            fn = os.path.join(storage, sha)
            if not os.path.exists(fn):
                continue
            if not self._verify_digest:
                return fn
            try:
                actual = _sha256_file(fn)
            except OSError:
                continue
            if actual == self._release_sha256:
                return fn
        return None

    def _get_cached_file_before_open(self, path: str, **kwargs: Any) -> None:
        path = self._strip_protocol(path)
        self._mkcache()
        fn = os.path.join(self.storage[-1], self._mapper(path))
        fd, tmp_name = tempfile.mkstemp(
            prefix=".upath-transfer-",
            suffix=".part",
            dir=self.storage[-1],
        )
        os.close(fd)
        published = False
        try:
            # one continuous whole-file transfer: avoids mixing bytes if the
            # upstream path is replaced while the transfer is in progress
            self.fs.get_file(path, tmp_name)
            if self._release_sha256 is not None and self._verify_digest:
                actual = _sha256_file(tmp_name)
                if actual != self._release_sha256:
                    raise ReleaseDigestMismatch(
                        "requested release is unavailable: the received ZIP"
                        " does not match the expected release digest"
                        f" (expected sha256 {self._release_sha256},"
                        f" received sha256 {actual})"
                        f" for {path!r}",
                        expected_sha256=self._release_sha256,
                        actual_sha256=actual,
                        path=path,
                    )
            os.replace(tmp_name, fn)
            published = True
        finally:
            if not published:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
        self._cache_size = None

    def _open(self, path: str, mode: str = "rb", **kwargs: Any):
        if self._release_sha256 is None or "r" not in mode:
            return super()._open(path, mode=mode, **kwargs)
        path = self._strip_protocol(path)
        fn = self._check_file(path)
        if fn is None:
            self._get_cached_file_before_open(path)
            fn = self._check_file(path)
        if fn is None:
            raise ReleaseUnavailable(
                f"requested release with sha256 {self._release_sha256}"
                f" is unavailable for {path!r}"
            )
        file = open(fn, mode)
        file.original = path
        return file

    def cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        if self._release_sha256 is None:
            return super().cat_file(path, start=start, end=end, **kwargs)
        with self.open(path, "rb") as f:
            if start is not None:
                if start >= 0:
                    f.seek(start)
                else:
                    f.seek(max(0, f.size + start))
            if end is not None:
                if end < 0:
                    end = f.size + end
                return f.read(end - f.tell())
            return f.read()

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        if self._release_sha256 is None:
            return super().cat(
                path, recursive=recursive, on_error=on_error, **kwargs
            )
        paths = self.expand_path(path, recursive=recursive)
        out = {}
        for p in paths:
            try:
                out[p] = self.cat_file(p, **kwargs)
            except Exception as e:
                if on_error == "raise":
                    raise
                if on_error == "return":
                    out[p] = e
        if isinstance(path, str) and len(paths) == 1 and not recursive:
            out = out[paths[0]]
        return out

    def get_file(
        self,
        rpath: str,
        lpath: Any = None,
        callback: Any = None,
        outfile: Any = None,
        **kwargs: Any,
    ) -> Any:
        if self._release_sha256 is None:
            return super().get_file(
                rpath, lpath, callback=callback, outfile=outfile, **kwargs
            )
        path = self._strip_protocol(rpath)
        fn = self._check_file(path)
        if fn is None:
            self._get_cached_file_before_open(path)
            fn = self._check_file(path)
        if fn is None:
            raise ReleaseUnavailable(
                f"requested release with sha256 {self._release_sha256}"
                f" is unavailable for {path!r}"
            )
        with open(fn, "rb") as src:
            if outfile is not None:
                shutil.copyfileobj(src, outfile)
                return None
            if lpath is None:
                raise ValueError("lpath or outfile is required")
            parent = os.path.dirname(os.fspath(lpath))
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(lpath, "wb") as dst:
                shutil.copyfileobj(src, dst)
        return None

    def exists(self, path: str, **kwargs: Any) -> bool:
        if self._release_sha256 is None:
            return super().exists(path, **kwargs)
        if self._check_file(path) is not None:
            return True
        try:
            return bool(self.fs.exists(path, **kwargs))
        except Exception:
            return False

    def isfile(self, path: str, **kwargs: Any) -> bool:
        if self._release_sha256 is None:
            return super().isfile(path, **kwargs)
        if self._check_file(path) is not None:
            return True
        try:
            return bool(self.fs.isfile(path, **kwargs))
        except Exception:
            return False

    def isdir(self, path: str, **kwargs: Any) -> bool:
        if self._release_sha256 is None:
            return super().isdir(path, **kwargs)
        try:
            return bool(self.fs.isdir(path, **kwargs))
        except Exception:
            return False

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        if self._release_sha256 is None:
            return super().info(path, **kwargs)
        fn = self._check_file(path)
        if fn is not None:
            return {
                "name": self._strip_protocol(path),
                "size": os.path.getsize(fn),
                "type": "file",
            }
        return super().info(path, **kwargs)


register_implementation("simplecache", VerifiedSimpleCacheFileSystem, clobber=True)


class SimpleCachePath(UPath):
    __slots__ = ()

    if TYPE_CHECKING:

        def __init__(
            self,
            *args: JoinablePathLike,
            protocol: Literal["simplecache"] | None = ...,
            chain_parser: FSSpecChainParser = ...,
            **storage_options: Unpack[SimpleCacheStorageOptions],
        ) -> None: ...

    @classmethod
    def _fs_factory(
        cls,
        urlpath: str,
        protocol: str,
        storage_options: Mapping[str, Any],
    ) -> AbstractFileSystem:
        so = dict(storage_options)
        so.pop("fo", None)
        return super()._fs_factory(
            urlpath,
            protocol,
            so,
        )

    @property
    def storage_options(self) -> Mapping[str, Any]:
        so = self._storage_options.copy()
        so.pop("fo", None)
        return MappingProxyType(so)

    def _cache_layer_segments(self):
        return [
            segment
            for segment in self._chain.to_list()
            if segment.protocol in _CACHE_PROTOCOLS
        ]

    @property
    def cache_options(self) -> Mapping[str, Any]:
        """Options for each simplecache layer in the chain.

        Returns a mapping with ``outer`` and ``inner`` keys, ordered from the
        outermost cache layer to the innermost one. The values are plain dicts
        suitable for JSON serialization.
        """
        layers = {}
        for name, segment in zip(("outer", "inner"), self._cache_layer_segments()):
            layers[name] = dict(segment.storage_options)
        return CacheOptions(layers)

    @cache_options.setter
    def cache_options(self, value: Mapping[str, Any]) -> None:
        from upath._chain import _normalize_cache_options_value

        normalized = _normalize_cache_options_value(value)
        segments = self._chain.to_list()
        cache_idx = [
            idx
            for idx, segment in enumerate(segments)
            if segment.protocol in _CACHE_PROTOCOLS
        ]
        if len(normalized) > len(cache_idx):
            raise ValueError(
                f"cache_options defines {len(normalized)} layers, but the chain"
                f" contains only {len(cache_idx)} cache layer(s)"
            )
        for idx, opts in zip(cache_idx, normalized):
            segments[idx] = segments[idx]._replace(storage_options=dict(opts))
        self._chain = type(self._chain).from_list(segments, index=self._chain._index)
        if hasattr(self, "_fs_cached"):
            del self._fs_cached

    def get_cache_options(self, layer: str | None = None) -> dict[str, Any]:
        """Return plain, JSON-compatible options for one or both cache layers.

        Parameters
        ----------
        layer:
            ``"outer"`` or ``"inner"`` selects a single layer. If omitted a
            mapping for both layers is returned.
        """
        layers = {name: dict(opts) for name, opts in self.cache_options.items()}
        if layer is None:
            return layers
        key = _CACHE_LAYER_NAMES[layer]
        name = "outer" if key == 0 else "inner"
        return layers.get(name, {})

    @property
    def outer_cache_options(self) -> Mapping[str, Any]:
        return self.cache_options.get("outer", MappingProxyType({}))

    @property
    def inner_cache_options(self) -> Mapping[str, Any]:
        return self.cache_options.get("inner", MappingProxyType({}))

    @property
    def release_sha256(self) -> str | None:
        """The release digest configured for this path, if any."""
        for segment in reversed(self._cache_layer_segments()):
            digest = segment.storage_options.get("sha256")
            if isinstance(digest, (str, bytes)):
                return (
                    digest.decode("ascii")
                    if isinstance(digest, bytes)
                    else digest
                )
        return None

    @property
    def sha256(self) -> str | None:
        return self.release_sha256

    @property
    def expected_sha256(self) -> str | None:
        return self.release_sha256
