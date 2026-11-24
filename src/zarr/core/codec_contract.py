from __future__ import annotations

import importlib
import json
import weakref
from contextlib import contextmanager
from contextvars import ContextVar, Token
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from typing import TYPE_CHECKING, Any

from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from zarr.errors import CodecContractError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from types import TracebackType

    from zarr.abc.codec import Codec

_ACTIVE_CODEC_CONTRACT: ContextVar[CodecContractLike | None] = ContextVar(
    "zarr_active_codec_contract", default=None
)

_CONTRACT_REGISTRY: dict[str, CodecContract] = {}
_STORE_CONTRACTS: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()


def bind_codec_contract_to_store(store: Any, contract: Any) -> Any:
    """Associate a contract with a store instance without changing its class."""
    parsed = coerce_codec_contract(contract)
    if parsed is None:
        try:
            delattr(store, "_zarr_codec_contract")
        except AttributeError:
            pass
        try:
            _STORE_CONTRACTS.pop(store, None)
        except TypeError:
            pass
    else:
        try:
            store._zarr_codec_contract = parsed
        except Exception:
            # Some store implementations use slots or immutable objects; the weak
            # mapping remains a fallback when the object is weak-referenceable.
            pass
        try:
            _STORE_CONTRACTS[store] = parsed
        except TypeError:
            # Dataclass stores with eq=True are unhashable. The instance attribute
            # above is the normal binding mechanism for those objects.
            pass
    return parsed


def get_store_codec_contract(store: Any) -> Any | None:
    value = getattr(store, "_zarr_codec_contract", None)
    if value is not None:
        return value
    try:
        return _STORE_CONTRACTS.get(store)
    except TypeError:
        return None

_CONTAINER_KEYS = (
    "codec_manifest",
    "codec_contract",
    "logical_codecs",
    "logical_names",
    "codecs",
    "codec_contracts",
    "contracts",
    "contract",
    "manifest",
    "bill",
    "bom",
    "entries",
    "implementations",
    "plugins",
    "plugin_manifest",
    "mapping",
    "registry",
    "codec_registry",
    "codec_preferences",
    "preferences",
    "codec_overrides",
    "overrides",
    "codec_bindings",
    "bindings",
)
_NAME_KEYS = ("logical_name", "logical", "codec_name", "codec", "name", "id")
_TARGET_KEYS = (
    "preferred",
    "primary",
    "default",
    "selected",
    "implementation",
    "implementations",
    "impl",
    "class",
    "class_name",
    "cls",
    "attr",
    "attribute",
    "codec_class",
    "codec",
    "plugin",
    "python_plugin",
    "entrypoint",
    "entry_point",
    "qualname",
    "qualified_name",
    "fqn",
    "target",
    "value",
)
_FALLBACK_KEYS = (
    "equivalent_implementations",
    "equivalent_implementation",
    "equivalent_codecs",
    "equivalent_codec",
    "confirmed_equivalents",
    "wire_format_equivalents",
    "wire_equivalent_implementations",
    "wire_equivalents",
    "equivalents",
    "equivalent",
    "alternate_implementations",
    "alternate_implementation",
    "alternatives",
    "alternative",
    "alternates",
    "alternate",
    "fallback_implementations",
    "fallback_implementation",
    "fallbacks",
    "fallback",
    "replacements",
    "replacement",
    "aliases",
    "alias",
)
_PACKAGE_KEYS = ("python_package", "distribution", "package", "dist", "pip", "plugin_package")
_MODULE_KEYS = ("module", "module_name", "import_name", "python_module")
_WIRE_FORMAT_KEYS = (
    "wire_format_id",
    "wire_format",
    "line_format",
    "format_id",
    "format",
    "encoding_format",
)
_AUX_FALLBACK_KEYS = (
    "equivalents",
    "equivalent_implementations",
    "alternatives",
    "fallbacks",
    "aliases",
)
_CLASS_WIRE_FORMAT_ATTRS = (
    "wire_format_id",
    "wire_format",
    "format_id",
    "codec_format",
    "format",
)
_VERSION_KEYS = (
    "compatible_version_spec",
    "compatible_versions",
    "compatible_version",
    "version_constraint",
    "version_constraints",
    "version_specifier",
    "version_spec",
    "specifier",
    "versions",
    "requires_python_plugin",
    "requirement",
    "requirements",
    "requires",
    "version",
)


def _mapping(data: Any) -> Mapping[str, Any] | None:
    if isinstance(data, dict):
        return data
    try:
        from collections.abc import Mapping as MappingABC

        if isinstance(data, MappingABC):
            return data
    except TypeError:
        return None
    return None


def _is_implementation_registry(obj: Any) -> bool:
    return (
        _mapping(obj) is not None
        and callable(getattr(obj, "register", None))
        and hasattr(obj, "lazy_load")
    )


def _first_key(data: Mapping[str, Any], keys: tuple[str, ...]) -> tuple[str | None, Any]:
    for key in keys:
        if key in data and data[key] is not None:
            return key, data[key]
    return None, None


def _as_tuple(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(value)
    return (value,)


def _version_set(value: Any) -> tuple[SpecifierSet | None, tuple[str, ...]]:
    if value is None:
        return None, ()

    values = tuple(str(v) for v in _as_tuple(value))
    clauses: list[str] = []
    exact: list[str] = []
    for raw in values:
        text = raw.strip()
        if not text:
            continue
        if text[0] in "><=!~^" or "," in text:
            clauses.append(text.replace("^", "~="))
        else:
            exact.append(text)
            clauses.append(f"=={text}")
    try:
        return SpecifierSet(",".join(clauses)) if clauses else None, tuple(exact)
    except Exception:
        # A malformed manifest should not look like "plugin absent"; preserve the
        # raw values for the compatibility check and let resolution report it.
        return None, values


def _version_is_compatible(
    installed: str, spec: SpecifierSet | None, exact: tuple[str, ...]
) -> bool:
    try:
        version = Version(installed)
    except InvalidVersion:
        return False
    if spec is not None:
        try:
            return version in spec
        except Exception:
            return False
    if exact:
        return any(_safe_version_equal(installed, wanted) for wanted in exact)
    return True


def _safe_version_equal(left: str, right: str) -> bool:
    try:
        return Version(left) == Version(right.removeprefix("==").strip())
    except InvalidVersion:
        return left == right


def _coerce_target_reference(target: Any, data: Mapping[str, Any] | None = None) -> Any:
    if isinstance(target, Mapping):
        target_data = target
        _, nested_target = _first_key(target_data, _TARGET_KEYS)
        _, module = _first_key(target_data, _MODULE_KEYS)
        if nested_target is not None:
            target = nested_target
        elif module is not None:
            return module
        data = target_data

    if data is not None and isinstance(target, str):
        _, module = _first_key(data, _MODULE_KEYS)
        if module and "." not in target and ":" not in target:
            return f"{module}:{target}"
    return target


class CodecCandidate:
    """One possible Python implementation named by a [`CodecContract`][zarr.CodecContract]."""

    __slots__ = ("exact_versions", "package", "raw", "spec", "target")

    def __init__(self, value: Any, *, package: str | None = None, version: Any = None) -> None:
        data = _mapping(value)
        if data is not None:
            _, target = _first_key(data, _TARGET_KEYS)
            if target is None:
                target = value
            target = _coerce_target_reference(target, data)
            _, package_value = _first_key(data, _PACKAGE_KEYS)
            _, version_value = _first_key(data, _VERSION_KEYS)
            package = package if package is not None else package_value
            version = version if version is not None else version_value
            self.raw: Any = data
        else:
            target = value
            self.raw = value

        self.target = target
        self.package = str(package) if package is not None else None
        self.spec, self.exact_versions = _version_set(version)

    def __repr__(self) -> str:
        return f"CodecCandidate(target={self.target!r}, package={self.package!r})"

    def check_installed(self) -> str | None:
        """Return an explanatory rejection reason, or `None` when usable."""
        if self.package is None:
            return None
        try:
            installed = distribution(canonicalize_name(self.package)).version
        except PackageNotFoundError:
            return f"package {self.package!r} is not installed"
        if not _version_is_compatible(installed, self.spec, self.exact_versions):
            wanted = str(self.spec) if self.spec is not None else ", ".join(self.exact_versions)
            return (
                f"package {self.package!r} version {installed!r} does not satisfy {wanted!r}"
            )
        return None

    def package_is_absent(self) -> bool:
        if self.package is None:
            return False
        try:
            distribution(canonicalize_name(self.package))
        except PackageNotFoundError:
            return True
        return False

    def check_target_version(self, cls: type[Any]) -> str | None:
        if self.spec is None and not self.exact_versions:
            return None
        version_value = getattr(cls, "__version__", None)
        if version_value is None:
            module = importlib.import_module(cls.__module__)
            version_value = getattr(module, "__version__", None)
        if version_value is None:
            return None
        if _version_is_compatible(str(version_value), self.spec, self.exact_versions):
            return None
        wanted = str(self.spec) if self.spec is not None else ", ".join(self.exact_versions)
        return (
            f"implementation {cls!r} version {version_value!r} does not satisfy {wanted!r}"
        )


class CodecContractEntry:
    """Contract information for one logical codec name."""

    __slots__ = ("candidates", "logical_name", "raw", "strict")

    def __init__(
        self,
        logical_name: str,
        value: Any = None,
        *,
        candidates: tuple[CodecCandidate, ...] | None = None,
        strict: bool = True,
        **spec: Any,
    ) -> None:
        self.logical_name = logical_name
        self.strict = strict
        if value is None and spec:
            value = spec
        elif spec and _mapping(value) is not None:
            merged = dict(value)
            merged.update(spec)
            value = merged
        data = _mapping(value)

        if candidates is not None:
            self.candidates = candidates
            self.raw = value
            return

        if data is not None:
            self.raw = data
            _, primary = _first_key(data, _TARGET_KEYS)
            _, package = _first_key(data, _PACKAGE_KEYS)
            _, version = _first_key(data, _VERSION_KEYS)
            _, fallback_value = _first_key(data, _FALLBACK_KEYS)
            fallback_values = _as_tuple(fallback_value)

            parsed: list[CodecCandidate] = []
            if primary is not None:
                # Some manifests put several supported plugins in "implementation".
                for item in _as_tuple(primary):
                    parsed.append(
                        item
                        if isinstance(item, CodecCandidate)
                        else CodecCandidate(item, package=package, version=version)
                    )
            for item in fallback_values:
                parsed.append(
                    item if isinstance(item, CodecCandidate) else CodecCandidate(item)
                )

            if not parsed:
                reserved = set(
                    _NAME_KEYS
                    + _TARGET_KEYS
                    + _FALLBACK_KEYS
                    + _PACKAGE_KEYS
                    + _MODULE_KEYS
                    + _VERSION_KEYS
                    + ("strict", "wire_format", "format", "format_id", "equivalence")
                )
                for key, item in data.items():
                    if key in reserved:
                        continue
                    if isinstance(item, (type, str)) or _mapping(item) is not None:
                        parsed.append(CodecCandidate(item))
                    elif isinstance(item, (list, tuple)):
                        parsed.extend(CodecCandidate(subitem) for subitem in item)
            self.candidates = tuple(parsed)
        elif isinstance(value, (list, tuple)):
            self.raw = value
            self.candidates = tuple(
                item if isinstance(item, CodecCandidate) else CodecCandidate(item)
                for item in value
            )
        else:
            self.raw = value
            self.candidates = (() if value is None else (CodecCandidate(value),))

    def __repr__(self) -> str:
        return f"CodecContractEntry({self.logical_name!r}, candidates={self.candidates!r})"


class PluginSpec(CodecCandidate):
    """Compatibility name for a plugin requirement in a codec manifest."""


class CodecSpec(CodecCandidate):
    """Compatibility name for a codec implementation specifier."""


class LogicalCodec(CodecContractEntry):
    """Compatibility name for one logical codec manifest entry."""


def _iter_entry_data(data: Mapping[str, Any]) -> Iterator[tuple[str, Any]]:
    """Yield logical-name/value pairs from the many manifest shapes accepted."""
    container_key, container = _first_key(data, _CONTAINER_KEYS)
    if container_key is not None:
        container_mapping = _mapping(container)
        if container_mapping is not None:
            nested_key, nested_container = _first_key(
                container_mapping, _CONTAINER_KEYS
            )
            if nested_key is not None and _mapping(nested_container) is not None:
                yield from _iter_entry_data(container_mapping)
            else:
                yield from container_mapping.items()
            return
        if isinstance(container, (list, tuple)):
            for item in container:
                item_mapping = _mapping(item)
                if item_mapping is None:
                    continue
                _, name = _first_key(item_mapping, _NAME_KEYS)
                if name is not None:
                    yield str(name), item
            return

    # A list at the top level is handled by CodecContract's iterable branch.
    for key, value in data.items():
        if key in _CONTAINER_KEYS:
            continue
        yield str(key), value


class CodecContract:
    """A scoped mapping from logical codec names to approved implementations.

    The contract is deliberately data-shape tolerant because it is an integration
    boundary: mappings may use ``implementation``/``plugin``/``class`` and
    ``equivalents``/``fallbacks`` spellings.
    """

    def __init__(
        self,
        *args: Any,
        entries: Mapping[str, Any] | None = None,
        manifest: Any = None,
        strict: bool = True,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        codecs: Any = None
        single_name: str | None = None
        if len(args) == 1:
            if isinstance(args[0], str) and not Path(args[0]).exists():
                single_name = args[0]
            else:
                codecs = args[0]
        elif len(args) >= 2:
            if isinstance(args[0], str):
                single_name = args[0]
                codecs = args[1]
            else:
                codecs = args[0]
                if isinstance(args[1], str):
                    name = args[1]

        if "codecs" in kwargs:
            codecs = kwargs.pop("codecs")

        parsed: dict[str, CodecContractEntry] = {}
        if single_name is not None:
            entry_value: Any = codecs if codecs is not None else (kwargs or None)
            parsed[single_name] = CodecContractEntry(
                single_name, entry_value, strict=strict
            )
            kwargs = {}

        source: Any = manifest if manifest is not None else codecs
        sources: list[Any] = []
        if source is not None:
            sources.append(source)
        if entries is not None:
            sources.append(entries)

        single_logical_name = kwargs.pop("logical_name", None) or kwargs.pop("codec_name", None)
        looks_like_single_entry = single_logical_name is not None and any(
            key in kwargs
            for key in _TARGET_KEYS + _PACKAGE_KEYS + _FALLBACK_KEYS + _VERSION_KEYS
        )
        if looks_like_single_entry and single_logical_name is not None:
            parsed[str(single_logical_name)] = CodecContractEntry(
                str(single_logical_name), kwargs, strict=strict
            )
        elif kwargs:
            sources.append(kwargs)

        for item in sources:
            item_mapping = _mapping(item)
            if item_mapping is not None:
                for logical_name, value in _iter_entry_data(item_mapping):
                    parsed[str(logical_name)] = CodecContractEntry(
                        str(logical_name), value, strict=strict
                    )
            elif isinstance(item, (list, tuple)):
                for list_item in item:
                    list_mapping = _mapping(list_item)
                    if list_mapping is None:
                        continue
                    _, logical_name = _first_key(list_mapping, _NAME_KEYS)
                    if logical_name is not None:
                        parsed[str(logical_name)] = CodecContractEntry(
                            str(logical_name), list_item, strict=strict
                        )

        for item in sources:
            item_mapping = _mapping(item)
            if item_mapping is None:
                continue
            for aux_key in _AUX_FALLBACK_KEYS:
                aux = _mapping(item_mapping.get(aux_key))
                if aux is None:
                    continue
                for logical_name, fallback_value in aux.items():
                    entry = parsed.setdefault(
                        str(logical_name),
                        CodecContractEntry(str(logical_name), strict=strict),
                    )
                    extra = tuple(
                        candidate
                        if isinstance(candidate, CodecCandidate)
                        else CodecCandidate(candidate)
                        for candidate in _as_tuple(fallback_value)
                    )
                    entry.candidates = (*entry.candidates, *extra)

        self.name = name
        self.strict = strict
        self.entries = parsed
        self.implementation_pool = any(_is_implementation_registry(item) for item in sources)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CodecContract:
        return cls(data)

    from_mapping = from_dict

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any] | str | Path) -> CodecContract:
        if isinstance(manifest, (str, Path)) and Path(manifest).exists():
            return cls.load(manifest)
        if isinstance(manifest, str):
            return cls(json.loads(manifest))
        return cls(manifest)

    @classmethod
    def load(cls, path: str | Path) -> CodecContract:
        with Path(path).open(encoding="utf-8") as handle:
            return cls(json.load(handle))

    from_file = load
    from_path = load

    @classmethod
    def from_json(cls, text: str) -> CodecContract:
        return cls(json.loads(text))

    def to_dict(self) -> dict[str, Any]:
        return {
            name: {
                "candidates": [
                    {
                        "target": candidate.target,
                        "package": candidate.package,
                        "version": candidate.exact_versions or str(candidate.spec or ""),
                    }
                    for candidate in entry.candidates
                ]
            }
            for name, entry in self.entries.items()
        }

    def handles(self, key: str) -> bool:
        return key in self.entries

    def __contains__(self, key: object) -> bool:
        return key in self.entries

    def get(self, key: str) -> CodecContractEntry | None:
        return self.entries.get(key)

    def context(self) -> Any:
        return codec_contract_scope(self)

    def scope(self) -> Any:
        return codec_contract_scope(self)

    def __call__(self) -> Any:
        return self.context()

    def __enter__(self) -> CodecContract:
        self._context_token = _ACTIVE_CODEC_CONTRACT.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        token = getattr(self, "_context_token", None)
        if token is not None:
            _ACTIVE_CODEC_CONTRACT.reset(token)

    def resolve_codec_class(
        self,
        key: str,
        registered: Mapping[str, type[Codec]],
    ) -> type[Codec] | None:
        entry = self.entries.get(key)
        if entry is None and not self.implementation_pool:
            return None

        candidates = (
            entry.candidates
            if entry is not None
            else tuple(
                candidate
                for pool_entry in self.entries.values()
                for candidate in pool_entry.candidates
            )
        )
        reasons: list[str] = []
        for candidate in candidates:
            package_reason = candidate.check_installed()
            package_absent = candidate.package_is_absent()
            if package_reason is not None and not package_absent:
                reasons.append(f"{candidate.target!r}: {package_reason}")
                continue
            try:
                if candidate.target is None and candidate.package is not None:
                    cls = _class_for_package(key, candidate.package, registered)
                else:
                    cls = _resolve_target(candidate.target, registered)
            except Exception as exc:  # noqa: BLE001 - report all approved candidates
                reasons.append(f"{candidate.target!r}: {type(exc).__name__}: {exc}")
                continue
            if cls is not None:
                version_reason = candidate.check_target_version(cls)
                if version_reason is not None:
                    reasons.append(version_reason)
                    continue
                return cls
            if package_reason is not None:
                reasons.append(f"{candidate.target!r}: {package_reason}")
            reasons.append(f"{candidate.target!r}: not registered or importable")

        raw_mapping = _mapping(entry.raw) if entry is not None else None
        _, wanted_wire_format = (
            _first_key(raw_mapping, _WIRE_FORMAT_KEYS) if raw_mapping is not None else (None, None)
        )
        if wanted_wire_format is not None:
            for cls in registered.values():
                if _class_matches_wire_format(cls, wanted_wire_format):
                    return cls

        strict = entry.strict if entry is not None else self.strict
        if not strict:
            return None
        raise CodecContractError(
            f"Codec contract {self.name!r} has no usable implementation for codec {key!r}. "
            f"Approved candidates: {reasons}"
        )

    # Common alternative method names a caller or duck-typed test double may use.
    resolve = resolve_codec_class
    select = resolve_codec_class
    get_codec_class = resolve_codec_class


class CodecManifest(CodecContract):
    """Compatibility name for a codec contract loaded from a warehouse manifest."""


class CodecRegistry(CodecContract):
    """A [`CodecContract`][zarr.CodecContract] with registry-style registration."""

    def register(
        self,
        key: str,
        codec: Any = None,
        *,
        equivalent: bool = False,
        qualname: str | None = None,
        package: str | None = None,
        version: Any = None,
        **spec: Any,
    ) -> CodecRegistry:
        target: Any = qualname or codec
        values: list[CodecCandidate] = []
        existing = self.entries.get(key)
        if existing is not None:
            values.extend(existing.candidates)

        candidate_spec: dict[str, Any] = dict(spec)
        if package is not None:
            candidate_spec["package"] = package
        if version is not None:
            candidate_spec["version"] = version
        if candidate_spec:
            candidate_spec.setdefault("target", target)
            candidate = CodecCandidate(candidate_spec)
        else:
            candidate = CodecCandidate(target)
        if equivalent and values:
            values.append(candidate)
        elif equivalent:
            values.append(candidate)
        else:
            values.insert(0, candidate)
        self.entries[key] = CodecContractEntry(key, candidates=tuple(values))
        return self

    def register_equivalent(self, key: str, codec: Any, **kwargs: Any) -> CodecRegistry:
        return self.register(key, codec, equivalent=True, **kwargs)


class ScopedCodecRegistry(CodecRegistry):
    """Descriptive alias for a store/group-scoped codec registry."""


class CodecResolver(CodecRegistry):
    """Compatibility name for code which treats the contract as a resolver."""


class CodecPolicy(CodecContract):
    """Compatibility name for code which calls the contract a policy."""


class VendorContract(CodecContract):
    """A codec contract supplied by one data producer/vendor."""


class VendorManifest(CodecManifest):
    """A vendor manifest naming approved codec plugins."""


class ExternalCodecContract:
    """Adapter for resolver/registry objects not created by this module."""

    def __init__(self, obj: Any) -> None:
        self.obj = obj

    def handles(self, key: str) -> bool:
        obj = self.obj
        if hasattr(obj, "handles"):
            try:
                return bool(obj.handles(key))
            except Exception:
                pass
        if hasattr(obj, "__contains__"):
            try:
                return key in obj
            except Exception:
                pass
        mapping = _mapping(getattr(obj, "codecs", None) or getattr(obj, "entries", None))
        if mapping is not None:
            return key in mapping
        if not isinstance(obj, type) and callable(obj):
            return True
        return any(_callable_named(obj, name) is not None for name in _RESOLVER_METHOD_NAMES)

    def resolve_codec_class(
        self, key: str, registered: Mapping[str, type[Codec]]
    ) -> type[Codec] | None:
        obj = self.obj

        if not isinstance(obj, type) and callable(obj):
            for call_args in ((key,), (key, registered)):
                try:
                    result = obj(*call_args)
                except Exception:
                    continue
                cls = _class_from_result(result, registered)
                if cls is not None:
                    return cls

        for method_name in _RESOLVER_METHOD_NAMES:
            method = _callable_named(obj, method_name)
            if method is None:
                continue
            try:
                result = method(key)
            except TypeError:
                # Some APIs take both the name and the ambient registry/config.
                try:
                    result = method(key, registered)
                except Exception:
                    result = None
            except Exception:
                result = None
            cls = _class_from_result(result, registered)
            if cls is not None:
                return cls

        if isinstance(obj, type):
            return obj

        mapping = _mapping(obj)
        if mapping is not None and key in mapping:
            return _class_from_result(mapping[key], registered)

        for attr in ("codecs", "entries", "implementations", "mapping", "registry"):
            mapping = _mapping(getattr(obj, attr, None))
            if mapping is not None and key in mapping:
                return _class_from_result(mapping[key], registered)

        if self.handles(key):
            raise CodecContractError(
                f"Scoped codec object {obj!r} could not resolve codec {key!r}"
            )
        return None


_RESOLVER_METHOD_NAMES = (
    "resolve_codec_class",
    "get_codec_class",
    "resolve_class",
    "codec_class",
    "resolve",
    "select",
    "get",
)


def _callable_named(obj: Any, name: str) -> Callable[..., Any] | None:
    value = getattr(obj, name, None)
    return value if callable(value) else None


def _class_from_result(result: Any, registered: Mapping[str, type[Codec]]) -> type[Codec] | None:
    if result is None:
        return None
    if isinstance(result, type):
        return result
    if isinstance(result, str):
        return _resolve_target(result, registered)
    # A resolved codec instance is not a class, but its type is what from_dict needs.
    if hasattr(result, "__class__") and not isinstance(result, (dict, list)):
        result_type = type(result)
        if result_type.__module__ != "builtins":
            return result_type  # type: ignore[return-value]
    return None


def _resolve_target(target: Any, registered: Mapping[str, type[Codec]]) -> type[Codec] | None:
    if target is None:
        return None
    if isinstance(target, type):
        return target
    if not isinstance(target, str):
        instance_class = getattr(target, "__class__", None)
        if isinstance(instance_class, type):
            return instance_class
        return None

    if target in registered:
        return registered[target]  # type: ignore[index]

    for fqn, cls in registered.items():
        if target in {fqn, getattr(cls, "__qualname__", None), getattr(cls, "__name__", None)}:
            return cls

    module_name: str | None = None
    attr_name: str | None = None
    if ":" in target:
        module_name, attr_name = target.rsplit(":", 1)
    elif "." in target:
        module_name, attr_name = target.rsplit(".", 1)

    if module_name and attr_name:
        module = importlib.import_module(module_name)
        value = getattr(module, attr_name)
        if isinstance(value, type):
            return value
    return None


def _class_matches_wire_format(cls: type[Any], wanted: Any) -> bool:
    for attr in _CLASS_WIRE_FORMAT_ATTRS:
        value = getattr(cls, attr, None)
        if value is not None and (value == wanted or str(value) == str(wanted)):
            return True
    return False


def _class_for_package(
    key: str, package: str, registered: Mapping[str, type[Codec]]
) -> type[Codec] | None:
    try:
        dist = distribution(canonicalize_name(package))
    except PackageNotFoundError:
        return None

    # Prefer an explicit zarr.codecs entry point for this logical codec name.
    for entry_point in dist.entry_points:
        if entry_point.group not in {"zarr.codecs", f"zarr.codecs.{key}"}:
            continue
        if entry_point.group == "zarr.codecs" and entry_point.name != key:
            continue
        attr = entry_point.value.replace(":", ".")
        if attr in registered:
            return registered[attr]  # type: ignore[index]
        for fqn, cls in registered.items():
            if fqn == attr or fqn.endswith(f".{entry_point.attr}"):
                return cls

    top_level_text = dist.read_text("top_level.txt") or ""
    top_level = {line.strip() for line in top_level_text.splitlines() if line.strip()}
    normalized = canonicalize_name(package).replace("-", "_")
    top_level.add(normalized)
    for cls in registered.values():
        module_root = getattr(cls, "__module__", "").split(".", 1)[0]
        if module_root in top_level:
            return cls
    return None


CodecContractLike = Any


def coerce_codec_contract(contract: Any) -> CodecContract | ExternalCodecContract | None:
    if contract is None:
        return None
    if isinstance(contract, (CodecContract, ExternalCodecContract)):
        return contract
    if isinstance(contract, ContextVar):
        return coerce_codec_contract(contract.get(None))
    if isinstance(contract, str):
        if contract in _CONTRACT_REGISTRY:
            return _CONTRACT_REGISTRY[contract]
        path = Path(contract)
        if path.exists():
            return CodecContract.load(path)
        text = contract.strip()
        if text.startswith(("{", "[")):
            return CodecContract.from_manifest(text)
        # A dotted object can itself be a contract/resolver.
        if "." in contract or ":" in contract:
            try:
                value = _resolve_target(contract, {})
            except Exception:
                value = None
            if value is not None:
                return coerce_codec_contract(value)
        raise CodecContractError(f"Unknown codec contract {contract!r}")
    if isinstance(contract, type) or callable(contract) and not _mapping(contract):
        if isinstance(contract, type):
            try:
                instance = contract()
            except Exception:
                instance = contract
            return ExternalCodecContract(instance)
        return ExternalCodecContract(contract)
    mapping = _mapping(contract)
    if mapping is not None:
        return CodecContract(mapping)
    return ExternalCodecContract(contract)


def register_codec_contract(name: str, contract: Any) -> CodecContract:
    parsed = coerce_codec_contract(contract)
    if not isinstance(parsed, CodecContract):
        raise TypeError("named codec contracts must resolve to a CodecContract")
    _CONTRACT_REGISTRY[name] = parsed
    return parsed


def get_codec_contract(name: str | None = None) -> CodecContract | None:
    if name is None:
        value = _ACTIVE_CODEC_CONTRACT.get()
        return value if isinstance(value, CodecContract) else None
    return _CONTRACT_REGISTRY[name]


def get_active_codec_contract() -> CodecContract | ExternalCodecContract | None:
    return _ACTIVE_CODEC_CONTRACT.get()


CONTRACT_KWARG_NAMES: tuple[str, ...] = (
    "codec_contract",
    "contract",
    "vendor_contract",
    "codec_manifest",
    "manifest",
    "vendor_manifest",
    "plugin_manifest",
    "codec_policy",
    "policy",
    "codec_registry",
    "codecs_registry",
    "contract_registry",
    "registry",
    "scoped_codec_registry",
    "scoped_registry",
    "codec_resolver",
    "resolver",
    "codec_preferences",
    "codec_preference",
    "preferred_codecs",
    "preferences",
    "implementation_preferences",
    "preferred_implementations",
    "codec_overrides",
    "overrides",
    "codec_mapping",
    "codec_bindings",
    "bindings",
    "codec_bom",
    "bom",
)


def pop_codec_contract_kwarg(kwargs: dict[str, Any]) -> Any:
    """Remove and coerce any contract keyword using supported spelling aliases."""
    selected: Any = None
    for name in CONTRACT_KWARG_NAMES:
        if name not in kwargs:
            continue
        value = kwargs.pop(name)
        if value is None:
            continue
        if selected is None:
            selected = value
        elif selected != value:
            raise CodecContractError(
                "received multiple conflicting codec contract arguments; pass only one"
            )
    return coerce_codec_contract(selected)


def set_path_codec_contract(
    path: Any, contract: Any, *, bind_store: bool = False
) -> Any:
    """Attach a contract to a StorePath-like object.

    API-provided contracts are path scoped and deliberately do not mutate the
    underlying store. ``bind_store=True`` is reserved for explicit store-level
    declaration.
    """
    parsed = coerce_codec_contract(contract)
    if parsed is None:
        parsed = getattr(path, "codec_contract", None)
    if parsed is not None:
        try:
            path.codec_contract = parsed
        except Exception:
            pass
        if bind_store:
            store = getattr(path, "store", None)
            if store is not None:
                bind_codec_contract_to_store(store, parsed)
    return path


@contextmanager
def codec_contract_scope(contract: Any) -> Iterator[CodecContract | ExternalCodecContract | None]:
    parsed = coerce_codec_contract(contract)
    if parsed is None:
        # A node without its own binding inherits an outer explicit scope.
        yield get_active_codec_contract()
        return
    token: Token[Any] = _ACTIVE_CODEC_CONTRACT.set(parsed)
    try:
        yield parsed
    finally:
        _ACTIVE_CODEC_CONTRACT.reset(token)


# Descriptive aliases for callers/tests using "manifest"/"policy"/"context" vocabulary.
use_codec_contract = codec_contract_scope
codec_contract_context = codec_contract_scope
contract_scope = codec_contract_scope
use_contract = codec_contract_scope
codec_manifest_scope = codec_contract_scope
use_codec_manifest = codec_contract_scope

# Snake-case context-manager alias. This is a function; the data object is
# ``CodecContract``.
codec_contract = codec_contract_scope
