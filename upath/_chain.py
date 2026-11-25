from __future__ import annotations

import copy
import sys
import warnings
from collections import defaultdict
from collections import deque
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import MutableMapping
from collections.abc import Sequence
from collections.abc import Set
from itertools import zip_longest
from typing import TYPE_CHECKING
from typing import Any
from typing import NamedTuple

from upath._flavour import WrappedFileSystemFlavour
from upath._protocol import get_upath_protocol
from upath.registry import available_implementations
from upath.types import UNSET_DEFAULT

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Never
        from typing import Self
    else:
        from typing_extensions import Never
        from typing_extensions import Self

__all__ = [
    "ChainSegment",
    "Chain",
    "FSSpecChainParser",
    "DEFAULT_CHAIN_PARSER",
]


CACHE_PROTOCOLS = frozenset({"blockcache", "filecache", "simplecache"})

_DIGEST_OPTION_NAMES = (
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

_LEAF_OPTION_NAMES = {
    "ftp": frozenset(
        {
            "host",
            "port",
            "username",
            "password",
            "acct",
            "tls",
            "timeout",
            "encoding",
        }
    ),
    "sftp": frozenset(
        {
            "host",
            "port",
            "username",
            "password",
            "passphrase",
            "timeout",
        }
    ),
    "ssh": frozenset({"host", "port", "username", "password", "timeout"}),
    "http": frozenset(
        {"simple_links", "same_scheme", "client_kwargs", "encoded"}
    ),
    "https": frozenset(
        {"simple_links", "same_scheme", "client_kwargs", "encoded"}
    ),
}

_CACHE_LAYER_NAMES = {
    "outer": 0,
    "outermost": 0,
    "top": 0,
    "first": 0,
    "member": 0,
    "members": 0,
    "layer0": 0,
    "l0": 0,
    "inner": -1,
    "innermost": -1,
    "bottom": -1,
    "second": -1,
    "last": -1,
    "archive": -1,
    "zip": -1,
    "layer1": -1,
    "l1": -1,
}

_CACHE_LAYER_KEY_NAMES = {
    "outer_cache_options": "outer",
    "inner_cache_options": "inner",
    "outer_cache": "outer",
    "inner_cache": "inner",
    "outer_options": "outer",
    "inner_options": "inner",
    "outer_simplecache": "outer",
    "inner_simplecache": "inner",
    "simplecache_outer": "outer",
    "simplecache_inner": "inner",
    "cache_outer": "outer",
    "cache_inner": "inner",
    "cache_options_outer": "outer",
    "cache_options_inner": "inner",
    "outer_cache_opts": "outer",
    "inner_cache_opts": "inner",
}

_CACHE_STORAGE_KEY_NAMES = {
    "outer_cache_storage": "outer",
    "inner_cache_storage": "inner",
    "cache_storage_outer": "outer",
    "cache_storage_inner": "inner",
    "outer_cache_dir": "outer",
    "inner_cache_dir": "inner",
    "outer_workdir": "outer",
    "inner_workdir": "inner",
    "outer_work_dir": "outer",
    "inner_work_dir": "inner",
}


def normalize_sha256(value: Any) -> str:
    """return a lowercase hex sha256 digest from common input formats"""
    if isinstance(value, bytes):
        value = value.decode("ascii")
    if not isinstance(value, str):
        raise TypeError(f"expected sha256 digest as str, not {type(value)!r}")
    digest = value.strip().lower()
    if digest.startswith("sha256:"):
        digest = digest.partition(":")[2].strip()
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"invalid sha256 digest: {value!r}")
    return digest


def _normalize_cache_options_value(value: Any) -> list[dict[str, Any]]:
    """accept a list of layer options or a mapping keyed by layer name"""
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [dict(v or {}) for v in value]
    if not isinstance(value, MutableMapping):
        raise TypeError(
            "cache_options must be a list of option mappings or a mapping"
            " with 'outer'/'inner' keys"
        )
    if not value:
        return []
    keys = list(value)
    if all(k in _CACHE_LAYER_NAMES or isinstance(k, int) for k in keys):
        layers: dict[int, dict[str, Any]] = {}
        for key, opts in value.items():
            idx = _CACHE_LAYER_NAMES.get(key, key) if isinstance(key, str) else key
            if idx == -1:
                idx = 1
            if idx not in (0, 1):
                raise ValueError(f"unsupported cache layer key: {key!r}")
            layers[idx] = dict(opts or {})
        if not layers:
            return []
        return [layers.get(i, {}) for i in range(max(layers) + 1)]
    return [dict(value)]


def _pop_digest_options(options: MutableMapping[str, Any]) -> str | None:
    digest = None
    for key in _DIGEST_OPTION_NAMES:
        if key in options:
            value = options.pop(key)
            if value is not None:
                digest = normalize_sha256(value)
    return digest


def _cache_layer_indices(segments: Sequence[ChainSegment]) -> list[int]:
    """indices of cache segments, ordered outermost to innermost"""
    return [
        idx
        for idx, segment in enumerate(segments)
        if segment.protocol in CACHE_PROTOCOLS
    ]


def _count_nested_cache_storage(options: Any) -> int:
    """count cache_storage entries in an already-nested target_options tree"""
    if not isinstance(options, Mapping):
        return 0
    count = 1 if "cache_storage" in options else 0
    nested = options.get("target_options")
    if isinstance(nested, Mapping):
        count += _count_nested_cache_storage(nested)
    return count


def _split_cache_options(
    segments: Sequence[ChainSegment],
    storage_options: MutableMapping[str, Any],
) -> tuple[list[dict[str, Any]], MutableMapping[str, Any]]:
    """extract and remove cache layer options from flat storage options

    the returned list is ordered from the outermost cache layer to the
    innermost cache layer. Unambiguous flat ``cache_storage`` options are
    assigned to the outer layer for backward compatibility.
    """
    cache_indices = _cache_layer_indices(segments)
    n_cache = len(cache_indices)
    layer_options: list[dict[str, Any]] = [{} for _ in cache_indices]

    raw = storage_options.pop("cache_options", None)
    if raw is not None:
        normalized = _normalize_cache_options_value(raw)
        if normalized and n_cache and len(normalized) > n_cache:
            raise ValueError(
                f"cache_options defines {len(normalized)} layers, but the chain"
                f" contains only {n_cache} cache layer(s)"
            )
        for idx, opts in enumerate(normalized):
            layer_options[idx].update(opts)

    for key, layer_name in _CACHE_LAYER_KEY_NAMES.items():
        value = storage_options.pop(key, None)
        if value is None:
            continue
        idx = 0 if layer_name == "outer" else 1
        if not n_cache:
            raise ValueError(f"{key} was provided but the chain has no cache layer")
        layer_options[idx].update(dict(value))

    for ref in list(storage_options):
        if not isinstance(ref, str) or ":" not in ref:
            continue
        proto, _, layer_name = ref.partition(":")
        if proto not in CACHE_PROTOCOLS:
            continue
        idx = _CACHE_LAYER_NAMES.get(layer_name)
        if idx is None:
            continue
        if idx == -1:
            idx = 1
        value = storage_options.pop(ref)
        if not n_cache:
            raise ValueError(f"{ref!r} was provided but the chain has no cache layer")
        if idx >= n_cache:
            raise ValueError(
                f"{ref!r} was provided but the chain has only {n_cache}"
                " cache layer(s)"
            )
        layer_options[idx].update(dict(value or {}))

    for proto in CACHE_PROTOCOLS:
        value = storage_options.get(proto)
        if not isinstance(value, MutableMapping):
            continue
        if n_cache == 1:
            layer_options[0].update(storage_options.pop(proto))
        elif "cache_storage" in value and n_cache > 1:
            if _count_nested_cache_storage(storage_options.get("target_options")):
                continue
            # a bare protocol mapping on a multi-cache chain is ambiguous
            raise ValueError(
                f"storage option {proto!r} with 'cache_storage' is ambiguous:"
                f" the chain has {n_cache} cache layers at chain positions"
                f" {cache_indices}. Use cache_options,"
                " outer_cache_options/inner_cache_options, or"
                f" {proto}:outer / {proto}:inner keys instead."
            )

    flat_cache_storage = storage_options.get("cache_storage", UNSET_DEFAULT)
    if flat_cache_storage is not UNSET_DEFAULT:
        if not n_cache:
            # keep fsspec behavior for non-cache chains
            pass
        elif n_cache == 1 and not (
            isinstance(flat_cache_storage, Mapping)
            or (
                isinstance(flat_cache_storage, Sequence)
                and not isinstance(flat_cache_storage, (str, bytes))
            )
        ):
            layer_options[0]["cache_storage"] = storage_options.pop("cache_storage")
        elif isinstance(flat_cache_storage, Mapping) and n_cache > 1:
            for layer_name, value in flat_cache_storage.items():
                idx = _CACHE_LAYER_NAMES.get(str(layer_name))
                if idx is None:
                    raise ValueError(
                        f"cache_storage key {layer_name!r} is not a cache layer;"
                        " use 'outer' or 'inner'"
                    )
                if idx == -1:
                    idx = 1
                layer_options[idx]["cache_storage"] = value
            storage_options.pop("cache_storage")
        elif (
            isinstance(flat_cache_storage, Sequence)
            and not isinstance(flat_cache_storage, (str, bytes))
            and n_cache > 1
            and len(flat_cache_storage) == n_cache
        ):
            for idx, value in enumerate(flat_cache_storage):
                layer_options[idx]["cache_storage"] = value
            storage_options.pop("cache_storage")
        elif n_cache == 1:
            layer_options[0]["cache_storage"] = storage_options.pop("cache_storage")
        elif _count_nested_cache_storage(storage_options.get("target_options")):
            # fully nested options (i.e. handed over as JSON) are unambiguous:
            # top-level options describe the outermost cache layer
            pass
        else:
            raise ValueError(
                "storage option 'cache_storage' is ambiguous:"
                f" the chain has {n_cache} cache layers at chain positions"
                f" {cache_indices}. Use cache_options,"
                " outer_cache_options/inner_cache_options, or"
                " simplecache:outer / simplecache:inner keys instead."
            )

    for key, layer_name in _CACHE_STORAGE_KEY_NAMES.items():
        value = storage_options.pop(key, None)
        if value is None:
            continue
        if not n_cache:
            raise ValueError(f"{key} was provided but the chain has no cache layer")
        idx = 0 if layer_name == "outer" else 1
        layer_options[idx]["cache_storage"] = value

    # a flat release digest pins the archive-holding cache layer
    flat_digest = _pop_digest_options(storage_options)
    if flat_digest is not None and n_cache:
        layer_options[-1]["sha256"] = flat_digest

    for opts in layer_options:
        digest = _pop_digest_options(opts)
        if digest is not None:
            opts["sha256"] = digest

    # route leaf-specific flat options to the final segment; explicit flat
    # options take precedence over values parsed from the urlpath
    leaf = segments[-1]
    leaf_names = _LEAF_OPTION_NAMES.get(leaf.protocol, frozenset())
    for name in list(storage_options):
        if name in leaf_names:
            leaf.storage_options[name] = storage_options.pop(name)

    for proto, leaf_names in _LEAF_OPTION_NAMES.items():
        value = storage_options.get(proto)
        if not isinstance(value, MutableMapping):
            continue
        if proto == segments[-1].protocol:
            leaf.storage_options.update(storage_options.pop(proto))
        elif set(value).issubset(leaf_names):
            storage_options.pop(proto)
        elif segments[-1].protocol in _LEAF_OPTION_NAMES:
            # the caller addressed a different source implementation; keep the
            # options but attach them to the actual leaf
            leaf.storage_options.update(storage_options.pop(proto))

    return layer_options, storage_options


class ChainSegment(NamedTuple):
    path: str | None  # support for path passthrough (i.e. simplecache)
    protocol: str
    storage_options: dict[str, Any]


class Chain:
    """holds current chain segments"""

    __slots__ = (
        "_segments",
        "_index",
    )

    def __init__(
        self,
        *segments: ChainSegment,
        index: int = 0,
    ) -> None:
        if not (0 <= index < len(segments)):
            raise ValueError("index must be between 0 and len(segments)")
        self._segments = segments
        self._index = index

    def __repr__(self) -> str:
        args = ", ".join(map(repr, self._segments))
        if self._index != 0:
            args += f", index={self._index}"
        return f"{type(self).__name__}({args})"

    @property
    def current(self) -> ChainSegment:
        return self._segments[self._index]

    @property
    def _path_index(self) -> int:
        for idx, segment in enumerate(self._segments[self._index :], start=self._index):
            if segment.path is not None:
                return idx
        raise IndexError("No target path found")

    @property
    def active_path(self) -> str:
        path = self._segments[self._path_index].path
        if path is None:
            raise RuntimeError
        return path

    @property
    def active_path_protocol(self) -> str:
        return self._segments[self._path_index].protocol

    def replace(
        self,
        *,
        path: str | None = None,
        protocol: str | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> Self:
        """replace the current chain segment keeping remaining chain segments"""
        segments = self.to_list()
        index = self._index

        replacements: MutableMapping[int, dict[str, Any]] = defaultdict(dict)
        if protocol is not None:
            replacements[index]["protocol"] = protocol
        if storage_options is not None:
            replacements[index]["storage_options"] = storage_options
        if path is not None:
            replacements[self._path_index]["path"] = path

        for idx, items in replacements.items():
            segments[idx] = segments[idx]._replace(**items)

        return type(self)(*segments, index=index)

    def to_list(self) -> list[ChainSegment]:
        """return a list of chain segments unnesting target_* segments"""
        queue = deque(self._segments)
        segments = []
        while queue:
            segment = queue.popleft()
            if (
                "target_protocol" in segment.storage_options
                and "fo" in segment.storage_options
            ):
                storage_options = segment.storage_options.copy()
                target_options = storage_options.pop("target_options", {})
                target_protocol = storage_options.pop("target_protocol")
                fo = storage_options.pop("fo")
                queue.appendleft(ChainSegment(fo, target_protocol, target_options))
                segments.append(
                    ChainSegment(segment.path, segment.protocol, storage_options)
                )
            elif not segments or segment != segments[-1]:
                segments.append(segment)
        return segments

    @classmethod
    def from_list(cls, segments: list[ChainSegment], index: int = 0) -> Self:
        return cls(*segments, index=index)

    def nest(self) -> ChainSegment:
        """return a nested target_* structure"""
        # see: fsspec.core.url_to_fs
        inkwargs: dict[str, Any] = {}
        # Reverse iterate the chain, creating a nested target_* structure
        chain = self._segments
        _prev = chain[-1].path
        for i, ch in enumerate(reversed(chain)):
            urls, protocol, kw = ch
            if urls is None:
                urls = _prev
            _prev = urls
            if i == len(chain) - 1:
                inkwargs = {**kw, **inkwargs}
                continue
            inkwargs["target_options"] = {**kw, **inkwargs}
            inkwargs["target_protocol"] = protocol
            inkwargs["fo"] = urls  # codespell:ignore fo
        urlpath, protocol, _ = chain[0]
        return ChainSegment(urlpath, protocol, inkwargs)


def _iter_fileobject_protocol_options(
    fileobject: str | None,
    protocol: str,
    storage_options: dict[str, Any],
    /,
) -> Iterator[tuple[str | None, str, dict[str, Any]]]:
    """yields fileobject, protocol and remaining storage options"""
    so = storage_options.copy()
    while "target_protocol" in so:
        t_protocol = so.pop("target_protocol", "")
        t_fileobject = so.pop("fo", None)  # codespell:ignore fo
        t_so = so.pop("target_options", {})
        yield fileobject, protocol, so
        fileobject, protocol, so = t_fileobject, t_protocol, t_so
    yield fileobject, protocol, so


def _merge_protocol_options(
    protocol: str,
    storage_options: dict[str, Any],
    /,
) -> dict[str, Any]:
    """merges storage options nested under protocol, flat options take precedence"""
    if protocol not in storage_options:
        return storage_options
    so = storage_options.copy()
    return {**so.pop(protocol), **so}


class FSSpecChainParser:
    """parse an fsspec chained urlpath"""

    def __init__(self) -> None:
        self.link: str = "::"
        self.known_protocols: Set[str] = set()

    def unchain(
        self,
        path: str,
        _deprecated_storage_options: Never = UNSET_DEFAULT,
        /,
        *,
        protocol: str | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> list[ChainSegment]:
        """implements same behavior as fsspec.core._un_chain

        two differences:
        1. it sets the urlpath to None for upstream filesystems that passthrough
        2. it checks against the known protocols for exact matches

        """
        if _deprecated_storage_options is not UNSET_DEFAULT:
            warnings.warn(
                "passing storage_options as positional argument is deprecated, "
                "pass as keyword argument instead",
                DeprecationWarning,
                stacklevel=2,
            )
            if storage_options is not None:
                raise ValueError(
                    "cannot pass storage_options both positionally and as keyword"
                )
            storage_options = _deprecated_storage_options
            protocol = protocol or storage_options.get("protocol")
        if storage_options is None:
            storage_options = {}
        else:
            try:
                storage_options = copy.deepcopy(storage_options)
            except Exception:
                storage_options = copy.copy(storage_options)

        segments: list[ChainSegment] = []
        path_bit: str | None
        next_path_overwrite: str | None = None
        for proto0, bit in zip_longest([protocol], path.split(self.link)):
            # get protocol and path_bit
            if (
                "://" in bit  # uri-like, fast-path (redundant)
                or "/" in bit  # path-like, fast-path
            ):
                proto = get_upath_protocol(bit, protocol=proto0)
                flavour = WrappedFileSystemFlavour.from_protocol(proto)
                path_bit = flavour.strip_protocol(bit)
                extra_so = flavour.get_kwargs_from_url(bit)
            elif bit in self.known_protocols and (
                proto0 is None or bit == proto0
            ):  # exact match a fsspec protocol
                proto = bit
                path_bit = None
                extra_so = {}
            elif bit in (m := set(available_implementations(fallback=True))) and (
                proto0 is None or bit == proto0
            ):
                self.known_protocols = m
                proto = bit
                path_bit = None
                extra_so = {}
            else:
                proto = get_upath_protocol(bit, protocol=proto0)
                flavour = WrappedFileSystemFlavour.from_protocol(proto)
                path_bit = flavour.strip_protocol(bit)
                extra_so = flavour.get_kwargs_from_url(bit)
            if proto in {"blockcache", "filecache", "simplecache"}:
                if path_bit is not None:
                    next_path_overwrite = path_bit or "/"
                path_bit = None
            elif next_path_overwrite is not None:
                path_bit = next_path_overwrite
                next_path_overwrite = None
            segments.append(ChainSegment(path_bit, proto, extra_so))

        # resolve explicit per-cache-layer options before any filesystem exists
        layer_options, storage_options = _split_cache_options(
            segments, storage_options
        )

        root_so = segments[0].storage_options
        for segment, proto_fo_so in zip_longest(
            segments,
            _iter_fileobject_protocol_options(
                path_bit if segments else None,
                protocol or "",
                _merge_protocol_options(segments[0].protocol, storage_options),
            ),
        ):
            t_fo, t_proto, t_so = proto_fo_so or (None, "", {})
            if segment is None:
                if next_path_overwrite is not None:
                    t_fo = next_path_overwrite
                    next_path_overwrite = None
                segments.append(ChainSegment(t_fo, t_proto, t_so))
            else:
                proto = segment.protocol
                # check if protocol is consistent with storage options
                if t_proto and t_proto != proto:
                    raise ValueError(
                        f"protocol {proto!r} collides with target_protocol {t_proto!r}"
                    )
                # update the storage_options
                segment.storage_options.update(root_so.pop(proto, {}))
                segment.storage_options.update(t_so)

        for idx, opts in zip(_cache_layer_indices(segments), layer_options):
            segment = segments[idx]
            segment.storage_options.update(
                {
                    key: value
                    for key, value in opts.items()
                    if key not in segment.storage_options
                }
            )

        return segments

    def chain(self, segments: Sequence[ChainSegment]) -> tuple[str, dict[str, Any]]:
        """returns a chained urlpath from the segments"""
        urlpaths = []
        kwargs = {}
        for segment in segments:
            if segment.protocol in CACHE_PROTOCOLS:
                # cache layers are passthrough links: their source is carried
                # by the next segment's inline ``fo`` option
                urlpath = segment.protocol
            elif segment.protocol and segment.path is not None:
                # FIXME: currently unstrip_protocol is only implemented by
                #   AbstractFileSystem, LocalFileSystem, and OSSFileSystem
                #   so to make this work we just implement it ourselves here.
                #   To do this properly we would need to instantiate the
                #   filesystem with its storage options and call
                #   fs.unstrip_protocol(segment.path)
                if segment.path.startswith(f"{segment.protocol}:/"):
                    urlpath = segment.path
                else:
                    urlpath = f"{segment.protocol}://{segment.path}"
            elif segment.protocol:
                urlpath = segment.protocol
            elif segment.path is not None:
                urlpath = segment.path
            else:
                warnings.warn(
                    f"skipping invalid segment {segment}",
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            urlpaths.append(urlpath)
            # TODO: ensure roundtrip with unchain behavior
            if segment.storage_options:
                kwargs[segment.protocol] = segment.storage_options
        return self.link.join(urlpaths), kwargs


DEFAULT_CHAIN_PARSER = FSSpecChainParser()


if __name__ == "__main__":
    from pprint import pp

    from fsspec.core import _un_chain

    chained_path = "simplecache::zip://haha.csv::gcs://bucket/file.zip"
    chained_kw = {"zip": {"allowZip64": False}}
    print(chained_path, chained_kw)
    out0 = _un_chain(chained_path, chained_kw)
    out1 = FSSpecChainParser().unchain(chained_path, storage_options=chained_kw)

    pp(out0)
    pp(out1)

    rechained_path, rechained_kw = FSSpecChainParser().chain(out1)
    print(rechained_path, rechained_kw)

    # UPath should store segments and access the path to operate on
    # through segments.current.path
    segments0 = Chain.from_list(segments=out1, index=1)
    assert segments0.current.protocol == "zip"

    # try to switch out zip path
    segments1 = segments0.replace(path="/newfile.csv")
    new_path, new_kw = FSSpecChainParser().chain(segments1.to_list())
    print(new_path, new_kw)
