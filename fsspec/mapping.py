import array
import json
import logging
import posixpath
import uuid
import warnings
from collections.abc import MutableMapping
from functools import cached_property
from urllib.parse import quote

from fsspec.core import url_to_fs

logger = logging.getLogger("fsspec.mapping")
_MISSING = object()

#: FSMap modes: "legacy" stores every key as a plain file below root;
#: "encoded" keeps an index of encoded keys so that keys may contain
#: any character (including "/" and trailing "/") without clashing with
#: pre-existing plain files.
FSMAP_MODES = ("legacy", "encoded")

#: Name of the directory below an encoded-mode root holding the key index
#: and the encoded values. It is reserved: it never appears as a key of the
#: mapping, and keys of the mapping never collide with it because encoded
#: values are only written below it.
FSMAP_META_DIR = "__fsmap_meta__"

_FSMAP_INDEX_VERSION = "urlquote-v1"


def encode_key(key):
    """Built-in encoding of a mapping key to a single safe file name"""
    return quote(key, safe="")


class FSMap(MutableMapping):
    """Wrap a FileSystem instance as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    root: string
        prefix for all the files
    fs: FileSystem instance
    check: bool (=True)
        performs a touch at the location, to check for write access.
    mode: "legacy" or "encoded"
        "legacy" (default) writes each key as a plain file below root, as
        previous versions of fsspec did. "encoded" keeps the plain files
        readable (no migration is required for existing directories) but
        writes new values to encoded paths recorded in an index file, so
        that keys may contain "/" (e.g. "site/a" and "site/a/minute/00"
        can both be keys), may end in "/", and may be named like the
        internal metadata (e.g. "__fsmap_keys__"). The metadata lives in
        the reserved ``__fsmap_meta__`` directory below root and never
        shows up as a key of the mapping.

    Examples
    --------
    >>> fs = FileSystem(**parameters)  # doctest: +SKIP
    >>> d = FSMap('my-data/path/', fs)  # doctest: +SKIP
    or, more likely
    >>> d = fs.get_mapper('my-data/path/')

    >>> d['loc1'] = b'Hello World'  # doctest: +SKIP
    >>> list(d.keys())  # doctest: +SKIP
    ['loc1']
    >>> d['loc1']  # doctest: +SKIP
    b'Hello World'
    """

    def __init__(
        self,
        root,
        fs,
        check=False,
        create=False,
        missing_exceptions=None,
        mode="legacy",
    ):
        if mode not in FSMAP_MODES:
            raise ValueError(f"FSMap mode must be one of {FSMAP_MODES}, got {mode!r}")
        self.mode = mode
        self._index = None
        self.fs = fs
        self.root = fs._strip_protocol(root)
        self._root_key_to_str = fs._strip_protocol(posixpath.join(root, "x"))[:-1]
        if missing_exceptions is None:
            missing_exceptions = (
                FileNotFoundError,
                IsADirectoryError,
                NotADirectoryError,
            )
        self.missing_exceptions = missing_exceptions
        self.check = check
        self.create = create
        if create:
            if not self.fs.exists(root):
                self.fs.mkdir(root)
        if check:
            if not self.fs.exists(root):
                raise ValueError(
                    f"Path {root} does not exist. Create "
                    f" with the ``create=True`` keyword"
                )
            check_path = f"{root}/{uuid.uuid4().hex}"
            self.fs.touch(check_path)
            self.fs.rm(check_path)

    @cached_property
    def dirfs(self):
        """dirfs instance that can be used with the same keys as the mapper"""
        from .implementations.dirfs import DirFileSystem

        return DirFileSystem(path=self._root_key_to_str, fs=self.fs)

    def clear(self):
        """Remove all keys below root - empties out mapping"""
        logger.info("Clear mapping at %s", self.root)
        try:
            self.fs.rm(self.root, True)
        except self.missing_exceptions:
            self._index = None
            return
        try:
            self.fs.mkdir(self.root)
        except FileExistsError:
            if not self.fs.isdir(self.root):
                raise
        self._index = None

    def getitems(self, keys, on_error="raise"):
        """Fetch multiple items from the store

        If the backend is async-able, this might proceed concurrently

        Parameters
        ----------
        keys: list(str)
            They keys to be fetched
        on_error : "raise", "omit", "return"
            If raise, an underlying exception will be raised (converted to KeyError
            if the type is in self.missing_exceptions); if omit, keys with exception
            will simply not be included in the output; if "return", all keys are
            included in the output, but the value will be bytes or an exception
            instance.

        Returns
        -------
        dict(key, bytes|exception)
        """
        if self.mode == "encoded":
            out = {}
            for key in keys:
                try:
                    out[key] = self[key]
                except KeyError as exc:
                    if on_error == "raise":
                        raise
                    if on_error == "return":
                        out[key] = exc
            return out
        keys2 = [self._key_to_str(k) for k in keys]
        oe = on_error if on_error == "raise" else "return"
        try:
            out = self.fs.cat(keys2, on_error=oe)
            if isinstance(out, bytes):
                out = {keys2[0]: out}
        except self.missing_exceptions as e:
            raise KeyError from e
        out = {
            k: (KeyError() if isinstance(v, self.missing_exceptions) else v)
            for k, v in out.items()
        }
        return {
            key: out[k2] if on_error == "raise" else out.get(k2, KeyError(k2))
            for key, k2 in zip(keys, keys2)
            if on_error == "return" or not isinstance(out[k2], BaseException)
        }

    def setitems(self, values_dict):
        """Set the values of multiple items in the store

        Parameters
        ----------
        values_dict: dict(str, bytes)
        """
        if self.mode == "encoded":
            self._encoded_setitems(values_dict)
            return
        values = {self._key_to_str(k): maybe_convert(v) for k, v in values_dict.items()}
        for parent in dict.fromkeys(self.fs._parent(k) for k in values):
            self.fs.mkdirs(parent, exist_ok=True)
        self.fs.pipe(values)

    def _encoded_setitems(self, values_dict):
        """Best-effort bulk write in encoded mode

        Every value is written to its own encoded file; only the keys whose
        physical write succeeded are recorded in the index, so a failed item
        keeps exposing its previous (e.g. plain legacy file) value while the
        successfully written items of the batch remain accurately readable.
        The first write error is re-raised after the batch.
        """
        index = self._load_index()
        written = False
        first_error = None
        for key, value in values_dict.items():
            key = self._normalize_key(key)
            path = self._key_to_encoded_str(key)
            try:
                self.fs.mkdirs(self.fs._parent(path), exist_ok=True)
                self.fs.pipe_file(path, maybe_convert(value))
            except Exception as exc:
                if first_error is None:
                    first_error = exc
                continue
            index[key] = encode_key(key)
            written = True
        if written:
            self._save_index()
        if first_error is not None:
            raise first_error

    def delitems(self, keys):
        """Remove multiple keys from the store"""
        if self.mode == "encoded":
            for key in keys:
                del self[key]
            return
        self.fs.rm([self._key_to_str(k) for k in keys])

    @staticmethod
    def _normalize_key(key):
        if not isinstance(key, str):
            # raise TypeError("key must be of type `str`, got `{type(key).__name__}`"
            warnings.warn(
                "from fsspec 2023.5 onward FSMap non-str keys will raise TypeError",
                DeprecationWarning,
            )
            if isinstance(key, list):
                key = tuple(key)
            key = str(key)
        return key

    def _key_to_str(self, key):
        """Generate full path for the key"""
        return f"{self._root_key_to_str}{self._normalize_key(key)}".rstrip("/")

    def _key_to_legacy_str(self, key):
        """Full path of the plain (legacy) file for the key, if there is one

        Unlike ``_key_to_str`` the key is used verbatim, so "a" and "a/"
        remain distinct keys. A key ending in "/" has no legacy
        representation at all (a legacy mapper strips trailing slashes), so
        None is returned for such keys.
        """
        key = self._normalize_key(key)
        if key.endswith("/"):
            return None
        return f"{self._root_key_to_str}{key}"

    @property
    def _meta_root(self):
        """Directory below root holding the encoded-mode metadata"""
        return f"{self._root_key_to_str}{FSMAP_META_DIR}"

    def _key_to_encoded_str(self, key):
        """Full path of the encoded file for the key"""
        return f"{self._meta_root}/data/{encode_key(key)}"

    def _load_index(self):
        """Read the encoded-key index, caching it on the instance"""
        if self._index is None:
            try:
                raw = self.fs.cat(f"{self._meta_root}/index.json")
            except self.missing_exceptions:
                self._index = {}
            else:
                self._index = dict(json.loads(raw).get("keys", {}))
        return self._index

    def _save_index(self):
        path = f"{self._meta_root}/index.json"
        self.fs.mkdirs(self.fs._parent(path), exist_ok=True)
        payload = json.dumps(
            {"encoding": _FSMAP_INDEX_VERSION, "keys": self._index}, sort_keys=True
        ).encode()
        self.fs.pipe_file(path, payload)

    def _str_to_key(self, s):
        """Strip path of to leave key name"""
        return s[len(self.root) :].lstrip("/")

    def __getitem__(self, key, default=None):
        """Retrieve data"""
        if self.mode == "encoded":
            key = self._normalize_key(key)
            index = self._load_index()
            if key in index:
                k = f"{self._meta_root}/data/{index[key]}"
            else:
                k = self._key_to_legacy_str(key)
                if k is None:
                    if default is not None:
                        return default
                    raise KeyError(key)
        else:
            k = self._key_to_str(key)
        try:
            result = self.fs.cat(k)
        except self.missing_exceptions as exc:
            if default is not None:
                return default
            raise KeyError(key) from exc
        return result

    def pop(self, key, default=_MISSING):
        """Pop data"""
        try:
            result = self[key]
        except KeyError:
            if default is _MISSING:
                raise
            return default
        try:
            del self[key]
        except KeyError:
            pass
        return result

    def __setitem__(self, key, value):
        """Store value in key"""
        if self.mode == "encoded":
            key = self._normalize_key(key)
            path = self._key_to_encoded_str(key)
            self.fs.mkdirs(self.fs._parent(path), exist_ok=True)
            self.fs.pipe_file(path, maybe_convert(value))
            index = self._load_index()
            index[key] = encode_key(key)
            self._save_index()
            return
        key = self._key_to_str(key)
        self.fs.mkdirs(self.fs._parent(key), exist_ok=True)
        self.fs.pipe_file(key, maybe_convert(value))

    def _encoded_keys(self):
        """All keys of an encoded-mode mapping: indexed keys plus plain files"""
        index = self._load_index()
        keys = set(index)
        try:
            found = self.fs.find(self.root)
        except self.missing_exceptions:
            found = []
        meta_prefix = f"{FSMAP_META_DIR}/"
        for path in found:
            key = self._str_to_key(path)
            if key == FSMAP_META_DIR or key.startswith(meta_prefix):
                # internal metadata, never a key of the mapping
                continue
            keys.add(key)
        return sorted(keys)

    def __iter__(self):
        if self.mode == "encoded":
            return iter(self._encoded_keys())
        return (self._str_to_key(x) for x in self.fs.find(self.root))

    def __len__(self):
        if self.mode == "encoded":
            return len(self._encoded_keys())
        return len(self.fs.find(self.root))

    def __delitem__(self, key):
        """Remove key"""
        if self.mode == "encoded":
            key = self._normalize_key(key)
            index = self._load_index()
            removed = False
            if key in index:
                try:
                    self.fs.rm(self._key_to_encoded_str(key))
                except self.missing_exceptions:
                    pass
                del index[key]
                self._save_index()
                removed = True
            try:
                # also drop any plain legacy file, so a deleted key cannot
                # reappear from it when the mapping is reopened
                legacy = self._key_to_legacy_str(key)
                if legacy is not None and self.fs.isfile(legacy):
                    self.fs.rm(legacy)
                    removed = True
            except self.missing_exceptions:
                pass
            if not removed:
                raise KeyError(key)
            return
        path = self._key_to_str(key)
        try:
            self.fs.rm(path)
        except self.missing_exceptions as exc:
            raise KeyError(key) from exc
        except Exception as exc:
            try:
                is_directory = self.fs.info(path)["type"] == "directory"
            except Exception:
                # Keep the removal error if classifying the path also fails.
                is_directory = False
            if is_directory:
                raise KeyError(key) from exc
            raise

    def __contains__(self, key):
        """Does key exist in mapping?"""
        if self.mode == "encoded":
            key = self._normalize_key(key)
            if key in self._load_index():
                return True
            path = self._key_to_legacy_str(key)
            return path is not None and self.fs.isfile(path)
        path = self._key_to_str(key)
        return self.fs.isfile(path)

    def __reduce__(self):
        return FSMap, (
            self.root,
            self.fs,
            False,
            False,
            self.missing_exceptions,
            self.mode,
        )


def maybe_convert(value):
    if isinstance(value, array.array) or hasattr(value, "__array__"):
        # bytes-like things
        if hasattr(value, "dtype") and value.dtype.kind in "Mm":
            # The buffer interface doesn't support datetime64/timdelta64 numpy
            # arrays
            value = value.view("int64")
        value = bytes(memoryview(value))
    return value


def get_mapper(
    url="",
    check=False,
    create=False,
    missing_exceptions=None,
    alternate_root=None,
    mode="legacy",
    **kwargs,
):
    """Create key-value interface for given URL and options

    The URL will be of the form "protocol://location" and point to the root
    of the mapper required. All keys will be file-names below this location,
    and their values the contents of each key.

    Also accepts compound URLs like zip::s3://bucket/file.zip , see ``fsspec.open``.

    Parameters
    ----------
    url: str
        Root URL of mapping
    check: bool
        Whether to attempt to read from the location before instantiation, to
        check that the mapping does exist
    create: bool
        Whether to make the directory corresponding to the root before
        instantiating
    missing_exceptions: None or tuple
        If given, these exception types will be regarded as missing keys and
        return KeyError when trying to read data. By default, you get
        (FileNotFoundError, IsADirectoryError, NotADirectoryError)
    alternate_root: None or str
        In cases of complex URLs, the parser may fail to pick the correct part
        for the mapper root, so this arg can override
    mode: "legacy" or "encoded"
        Storage layout of the mapping, see ``FSMap``. "encoded" must be
        enabled explicitly and supports arbitrary keys (containing "/",
        ending in "/", or named like internal metadata) while still reading
        plain files written by previous versions.

    Returns
    -------
    ``FSMap`` instance, the dict-like key-value store.
    """
    # Removing protocol here - could defer to each open() on the backend
    fs, urlpath = url_to_fs(url, **kwargs)
    root = alternate_root if alternate_root is not None else urlpath
    return FSMap(
        root, fs, check, create, missing_exceptions=missing_exceptions, mode=mode
    )
