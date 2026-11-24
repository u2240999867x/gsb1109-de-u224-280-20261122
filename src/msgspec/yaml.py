from __future__ import annotations

import collections.abc
import datetime as _datetime
from typing import TYPE_CHECKING, Any, TypeVar, overload

from . import (
    DecodeError as _DecodeError,
    ValidationError as _ValidationError,
    convert as _convert,
    to_builtins as _to_builtins,
)
from . import _yaml_provenance

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Literal

    from typing_extensions import Buffer


__all__ = ("encode", "decode")


def __dir__():
    return __all__


def _import_pyyaml(name):
    try:
        import yaml  # type: ignore
    except ImportError:
        raise ImportError(
            f"`msgspec.yaml.{name}` requires PyYAML be installed.\n\n"
            "Please either `pip` or `conda` install it as follows:\n\n"
            "  $ python -m pip install pyyaml  # using pip\n"
            "  $ conda install pyyaml          # or using conda"
        ) from None
    else:
        return yaml


def encode(
    obj: Any,
    *,
    enc_hook: Callable[[Any], Any] | None = None,
    order: Literal["deterministic", "sorted"] | None = None,
) -> bytes:
    """Serialize an object as YAML.

    Parameters
    ----------
    obj : Any
        The object to serialize.
    enc_hook : callable, optional
        A callable to call for objects that aren't supported msgspec types.
        Takes the unsupported object and should return a supported object, or
        raise a ``NotImplementedError`` if unsupported.
    order : {None, 'deterministic', 'sorted'}, optional
        The ordering to use when encoding unordered compound types.

        - ``None``: All objects are encoded in the most efficient manner
          matching their in-memory representations. The default.
        - `'deterministic'`: Dict keys and set elements are sorted so that
          values which compare equal produce identical encoded output,
          regardless of insertion or iteration order. Useful when
          comparison/hashing of the encoded binary output is necessary.
        - `'sorted'`: Like `'deterministic'`, but *all* object-like types
          (structs, dataclasses, ...) are also sorted by field name before
          encoding. This is slower than `'deterministic'`, but may produce more
          human-readable output.

    Returns
    -------
    data : bytes
        The serialized object.

    Notes
    -----
    This function requires that the third-party `PyYAML library
    <https://pyyaml.org/>`_ is installed.

    See Also
    --------
    decode
    """
    yaml = _import_pyyaml("encode")
    # Use the C extension if available
    Dumper = getattr(yaml, "CSafeDumper", yaml.SafeDumper)

    return yaml.dump_all(
        [
            _to_builtins(
                obj,
                builtin_types=(_datetime.datetime, _datetime.date),
                enc_hook=enc_hook,
                order=order,
            )
        ],
        encoding="utf-8",
        Dumper=Dumper,
        allow_unicode=True,
        sort_keys=False,
    )


_T = TypeVar("_T")


@overload
def decode(
    buf: Buffer | str,
    *,
    type: type[_T],
    strict: bool = True,
    dec_hook: Callable[[type[Any], Any], Any] | None = None,
) -> _T: ...


@overload
def decode(
    buf: Buffer | str,
    *,
    type: Any = ...,
    strict: bool = True,
    dec_hook: Callable[[type[Any], Any], Any] | None = None,
) -> Any: ...


def decode(
    buf: Buffer | str,
    *,
    type: Any = Any,
    strict: bool = True,
    dec_hook: Callable[[type[Any], Any], Any] | None = None,
) -> Any:
    """Deserialize an object from YAML.

    Parameters
    ----------
    buf : bytes-like or str
        The message to decode.
    type : type, optional
        A Python type (in type annotation form) to decode the object as. If
        provided, the message will be type checked and decoded as the specified
        type. Defaults to `Any`, in which case the message will be decoded
        using the default YAML types.
    strict : bool, optional
        Whether type coercion rules should be strict. Setting to False enables
        a wider set of coercion rules from string to non-string types for all
        values. Default is True.
    dec_hook : callable, optional
        An optional callback for handling decoding custom types. Should have
        the signature ``dec_hook(type: Type, obj: Any) -> Any``, where ``type``
        is the expected message type, and ``obj`` is the decoded representation
        composed of only basic YAML types. This hook should transform ``obj``
        into type ``type``, or raise a ``NotImplementedError`` if unsupported.

    Returns
    -------
    obj : Any
        The deserialized object.

    Notes
    -----
    This function requires that the third-party `PyYAML library
    <https://pyyaml.org/>`_ is installed.

    See Also
    --------
    encode
    """
    yaml = _import_pyyaml("decode")
    # Use the C extension if available
    Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    if not isinstance(buf, (str, bytes)):
        # call `memoryview` first, since `bytes(1)` is actually valid
        buf = bytes(memoryview(buf))
    try:
        obj = yaml.load(buf, Loader)
    except (yaml.YAMLError, ValueError, LookupError, AttributeError) as exc:
        raise _DecodeError(str(exc)) from None

    if type is Any:
        return obj
    try:
        return _convert(
            obj,
            type,
            builtin_types=(_datetime.datetime, _datetime.date),
            strict=strict,
            dec_hook=dec_hook,
        )
    except _ValidationError as exc:
        _annotate_error(yaml, buf, obj, type, strict, dec_hook, exc)
        raise


# ---------------------------------------------------------------------------
# Error enrichment for typed decodes
#
# This runs only after a typed decode has already failed, so the successful
# path stays free of extra bookkeeping.  When the failure is reported through a
# shared YAML template, the message is rewritten to name the logical table, the
# document line holding the value actually used, and the inheritance sites.
# ---------------------------------------------------------------------------

_MAX_PROBES = 20000


def _has_provenance_marker(exc):
    # Hooks may re-raise a `ValidationError` that already went through this
    # path; avoid enriching it twice.
    return getattr(exc, "__msgspec_yaml_annotated__", False)


def _split_message(exc):
    text = str(exc)
    marker = " - at `"
    index = text.rfind(marker)
    if index < 0:
        return text, None
    return text[:index], text[index + len(marker) :].rstrip("`")


def _probe(obj, type, strict):
    try:
        _convert(
            obj,
            type,
            builtin_types=(_datetime.datetime, _datetime.date),
            strict=strict,
        )
    except _ValidationError as exc:
        return exc
    except Exception:
        # Recursive aliases and other pathological inputs can fail in ways
        # unrelated to the original error; treat them as non-matches.
        return None
    return None


def _same_failure(exc, prefix):
    if exc is None:
        return False
    return _split_message(exc)[0] == prefix


_ELLIPSIS = object()
_NO_MATCH = object()


def _path_tokens(path_text):
    """Split a msgspec error path into field names, indices, and ``...``."""
    if not path_text or not path_text.startswith("$"):
        return None
    if path_text.startswith("key"):
        # A mapping-key error has no field path of its own.
        return None
    tokens = []
    index = 1
    length = len(path_text)
    while index < length:
        char = path_text[index]
        if char == ".":
            start = index + 1
            index = start
            while index < length and path_text[index] not in ".[":
                index += 1
            tokens.append(path_text[start:index])
        elif char == "[":
            end = path_text.find("]", index)
            if end < 0:
                return None
            token = path_text[index + 1 : end]
            index = end + 1
            if token == "...":
                tokens.append(_ELLIPSIS)
            else:
                try:
                    tokens.append(int(token))
                except ValueError:
                    tokens.append(token.strip("'\""))
        else:
            return None
    return tokens


class _MaskedMapping(collections.abc.Mapping):
    """A single-entry view of a mapping.

    Probing converts a document with one branch enabled at a time.  Copying a
    large document for every candidate would dominate the cost of reporting an
    error; this view only exposes the candidate key, while sharing the rest of
    the structure.
    """

    __slots__ = ("_base", "_key")

    def __init__(self, base, key):
        self._base = base
        self._key = key

    def __getitem__(self, key):
        if key == self._key:
            return self._base[key]
        raise KeyError(key)

    def __iter__(self):
        return iter((self._key,))

    def __len__(self):
        return 1


def _mask_at(obj, path, masked):
    """Return a shallow copy of ``obj`` with ``masked`` placed at ``path``."""
    if not path:
        return masked
    head, rest = path[0], path[1:]
    if isinstance(obj, dict):
        updated = dict(obj)
        updated[head] = _mask_at(obj[head], rest, masked)
        return updated
    if isinstance(obj, list):
        updated = list(obj)
        updated[head] = _mask_at(obj[head], rest, masked)
        return updated
    raise KeyError(head)


def _locate_ellipsis(obj, type, strict, prefix, parts, current, budget):
    """Choose the mapping key named by a ``[...]`` path segment.

    msgspec renders mapping keys as ``[...]`` because their names aren't known
    statically.  Re-convert the document with one candidate key enabled at a
    time and keep the branch that reproduces the same failure.  Candidates are
    tested in a stable order, so attribution doesn't depend on declaration
    order.  Returns ``_NO_MATCH`` when no candidate reproduces the failure.
    """
    if not isinstance(current, dict):
        return _NO_MATCH
    for key in sorted(current, key=_stable_key):
        budget[0] -= 1
        if budget[0] < 0:
            return _NO_MATCH
        candidate = _mask_at(obj, tuple(parts), _MaskedMapping(current, key))
        if _same_failure(_probe(candidate, type, strict), prefix):
            parts.append(key)
            return current[key]
    return _NO_MATCH


def _locate_path(obj, type, strict, prefix, path_text):
    """Resolve the logical path of the failing value or record.

    Walks the path msgspec reported.  Named fields and indices are taken
    directly; ``[...]`` segments are resolved by probing candidate keys.  For
    missing/unknown-field errors the failing record is reported, which is the
    mapping that owns the path.
    """
    tokens = _path_tokens(path_text)
    if tokens is None:
        return None
    budget = [_MAX_PROBES]
    parts = []
    current = obj

    for token in tokens:
        if token is _ELLIPSIS or (
            isinstance(token, str)
            and isinstance(current, dict)
            and token not in current
        ):
            current = _locate_ellipsis(
                obj, type, strict, prefix, parts, current, budget
            )
            if current is _NO_MATCH:
                return None
            continue

        if isinstance(current, dict):
            if token not in current:
                return None
            current = current[token]
        elif isinstance(current, list):
            if not isinstance(token, int) or not (0 <= token < len(current)):
                return None
            current = current[token]
        else:
            return None
        parts.append(token)

    return tuple(parts)


def _stable_key(key):
    # Group by type first so mixed key types (e.g. ints and strings) remain
    # totally ordered and comparisons can't raise.
    if isinstance(key, bool):
        return (0, int(key))
    if isinstance(key, int):
        return (1, key)
    if isinstance(key, str):
        return (2, key)
    return (3, type(key).__name__, repr(key))


def _fallback_path(path_text):
    """Best-effort conversion of a msgspec error path to a logical path.

    Mapping values print as ``[...]`` because msgspec can't know their keys.
    Such a segment makes the path unusable: truncating it would attribute the
    failure to a different member of the mapping.
    """
    if not path_text or not path_text.startswith("$"):
        return None
    parts = []
    index = 1
    length = len(path_text)
    while index < length:
        char = path_text[index]
        if char == ".":
            start = index + 1
            index = start
            while index < length and path_text[index] not in ".[":
                index += 1
            parts.append(path_text[start:index])
        elif char == "[":
            end = path_text.find("]", index)
            if end < 0:
                return None
            token = path_text[index + 1 : end]
            index = end + 1
            if token == "...":
                # Without probing we can't tell which mapping key was meant;
                # returning a partial path would misattribute the value.
                return None
            try:
                parts.append(int(token))
            except ValueError:
                parts.append(token.strip("'\""))
        else:
            return None
    return tuple(parts)


def _annotate_error(yaml, buf, obj, type, strict, dec_hook, exc):
    if _has_provenance_marker(exc):
        return

    try:
        prefix, path_text = _split_message(exc)
        if path_text is None:
            # msgspec reports no location for errors at the document root;
            # there is no logical field path to resolve.
            return
        # Probing re-runs conversion (but never a user `dec_hook`, which may
        # be expensive or side-effecting).  Without probing, only a path
        # msgspec named fully can be used.
        parts = _locate_path(obj, type, strict, prefix, path_text)
        if parts is None:
            parts = _fallback_path(path_text)
        if parts is None:
            return
        root = _yaml_provenance.snapshot(yaml, buf)
        origin = _yaml_provenance.resolve(yaml, root, obj, parts)
        if origin is None:
            return
    except RecursionError:
        # Pathological (e.g. deeply recursive) documents shouldn't replace the
        # validation error with an unrelated recursion failure.
        return
    except MemoryError:
        raise
    except Exception:
        return

    try:
        exc.__msgspec_yaml_annotated__ = True
    except Exception:
        pass
    _set_error_details(exc, origin)

    tokens = _path_tokens(path_text) or ()
    if _ELLIPSIS not in tokens and not origin.sites:
        # The message already names the value precisely and nothing was
        # inherited; leave it untouched.  Structured details are still set.
        return

    details = origin.describe()
    exc.args = (
        f"{prefix} - at `{origin.path}`" + (f"; {details}" if details else ""),
    )


def _set_error_details(exc, origin):
    sites = origin.sites
    inherited = tuple(site for site in sites if site.relation in ("merge", "alias"))
    recursive = tuple(site for site in sites if site.relation == "recursive")
    exc.path = origin.path
    exc.logical_path = origin.path
    exc.table = origin.table
    exc.line = origin.line
    exc.column = origin.column
    exc.anchor = origin.anchor
    exc.origin = origin
    exc.provenance = origin
    exc.sites = sites
    exc.inheritance = sites
    exc.merge_sites = inherited
    exc.inherited_from = inherited
    exc.recursive = recursive
    exc.value_line = origin.line if inherited else None
    exc.source_line = origin.line
    exc.document_line = origin.line
    exc.logical_table = origin.table
    exc.table_path = origin.table_path
    exc.anchors = origin.anchors
    exc.use_sites = sites
