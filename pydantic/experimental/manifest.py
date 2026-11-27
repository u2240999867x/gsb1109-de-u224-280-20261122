"""Experimental URL-based manifests.

This module lets a :class:`~pydantic.BaseModel` describe a *manifest*: a tree of
sub-manifests whose download references are resolved against a base URL that
belongs to the manifest tree.

The same declarations accept raw JSON and already-validated Python
sub-manifest instances. Reusing a Python instance preserves its unrelated
defaults, its field validation and the fields the caller originally supplied
explicitly; the validated instance itself is never mutated when the same
manifest is scheduled against another station.

The API is experimental and subject to change.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterator, Sequence
from typing import Any, ClassVar
from urllib.parse import urljoin, urlsplit, urlunsplit
from uuid import uuid4

from pydantic import BaseModel, PrivateAttr, model_validator
from pydantic_core import InitErrorDetails, PydanticCustomError, ValidationError

__all__ = ['ManifestBase', 'ManifestConfig', 'ManifestContext', 'resolve_reference']

_CONTEXT_KEY = 'pydantic_manifest_context'
_UNSET = object()


class ManifestContext(dict):
    """Validation context for :class:`ManifestBase` validation.

    Args:
        entry: Base URL explicitly supplied through an inbound alias for the
            root manifest. It is used only when the root declares neither a
            base column value nor a default factory.
        station: Optional, already-validated station identifier. It is passed
            to base factories and to :meth:`ManifestBase.manifest_verify_url`.
        extra: Additional context entries, available to ordinary validators
            through ``ValidationInfo.context``.
    """

    def __init__(self, entry: str | None = None, *, station: str | None = None, **extra: Any) -> None:
        super().__init__(**extra)
        self[_CONTEXT_KEY] = {
            'entry': entry,
            'station': station,
            'entry_explicit': entry is not None,
            'token': uuid4(),
            'depth': 0,
            'managed': True,
        }


class ManifestConfig:
    """Declares how a :class:`ManifestBase` subclass builds its request URLs.

    Args:
        base_field: Name of the column holding this manifest's base. The field
            itself may use any inbound (validation) alias.
        url_fields: Names of the columns (``str`` or ``list[str]``) holding
            download references that are resolved against the manifest base.
        children_field: Name of the column holding nested sub-manifests, as a
            single model or a list of them. When omitted, every nested
            :class:`ManifestBase` value is treated as a sub-manifest.
        default_factory: Callable returning a validated default base URL when
            the base column was not supplied explicitly. It is called with no
            arguments, or with a ``station`` keyword argument when it accepts
            one.
        relative: When true, the base column value is resolved against the
            parent manifest's base instead of standing on its own.
    """

    __slots__ = ('base_field', 'url_fields', 'children_field', 'default_factory', 'relative')

    def __init__(
        self,
        *,
        base_field: str,
        url_fields: Sequence[str] = (),
        children_field: str | None = None,
        default_factory: Callable[..., str] | None = None,
        relative: bool = False,
    ) -> None:
        self.base_field = base_field
        self.url_fields = tuple(url_fields)
        self.children_field = children_field
        self.default_factory = default_factory
        self.relative = relative


def resolve_reference(base: str, reference: str) -> str:
    """Resolve a download reference against a manifest base.

    Only literal ``.`` and ``..`` path segments are treated as directories;
    percent-encoded dots such as ``%2e`` are never decoded. An empty query
    marker (``?``) and the order of repeated query parameters are preserved
    exactly as they will be sent on the wire.

    Args:
        base: Absolute base URL, possibly carrying a query string.
        reference: Reference as stored on the manifest.

    Returns:
        The absolute request URL.
    """
    base_parts = urlsplit(base)
    if not base_parts.scheme or not base_parts.netloc:
        raise ValueError(f'expected an absolute base URL, got {base!r}')

    if reference == '':
        return base

    base_without_query = urlunsplit((base_parts.scheme, base_parts.netloc, base_parts.path, '', ''))

    if reference == '?':
        return urlunsplit(urlsplit(base_without_query)._replace(query='')) + '?'
    if reference.startswith('?'):
        joined = urlunsplit(urlsplit(base_without_query)._replace(query=reference[1:]))
        if reference.endswith('?') and not joined.endswith('?'):
            joined += '?'
        return joined
    if reference.startswith('#'):
        return urlunsplit(
            urlsplit(base_without_query)._replace(query=base_parts.query, fragment=reference[1:])
        )

    joined = urljoin(base_without_query, reference)
    # ``urljoin`` drops a trailing empty-query marker (``?``); restore it so the
    # queued URL keeps the exact text that will be sent on the wire.
    reference_parts = urlsplit(reference)
    joined_parts = urlsplit(joined)
    if '?' in reference and reference_parts.query == '' and joined_parts.query == '':
        if joined_parts.fragment:
            joined = joined.replace('#', '?#', 1)
        elif not joined.endswith('?'):
            joined += '?'
    return joined


class ManifestBase(BaseModel):
    """Base class for manifest models with a resolvable base URL.

    Subclasses declare a :class:`ManifestConfig` through the ``manifest``
    ClassVar. The download queue reads :meth:`request_urls` once validation has
    completed, so every address is the final URL that would actually be sent.
    """

    manifest: ClassVar[ManifestConfig]

    _manifest_base: str | None = PrivateAttr(default=None)
    _manifest_relative: bool = PrivateAttr(default=False)
    _manifest_owned: bool = PrivateAttr(default=False)
    _manifest_station: str | None = PrivateAttr(default=None)
    # Original download references, captured at the first validation so that
    # rebasing a reused instance never resolves an already resolved URL.
    _manifest_raw_urls: dict[str, list[str]] = PrivateAttr(default_factory=dict)
    _manifest_token: Any = PrivateAttr(default=None)

    @model_validator(mode='wrap')
    @classmethod
    def _manifest_wrap(cls, data: Any, handler: Any, info: Any) -> 'ManifestBase':
        payload = _manifest_payload(info.context)
        token = payload['token']
        station = payload.get('station')
        parent_base = payload.get('entry')
        entry_explicit = bool(payload.get('entry_explicit'))

        # A nested field may declare a base manifest class while the value is a
        # subclass instance; always drive resolution from the concrete class.
        model_cls = type(data) if isinstance(data, ManifestBase) else cls
        config = model_cls._manifest_config()
        reused = isinstance(data, ManifestBase)

        if reused:
            # Reuse of a validated sub-manifest. Copy it so the caller's
            # instance keeps the station (or original site) it was first
            # validated against; defaults and model_fields_set survive.
            model_copy = data.model_copy()
            base, owned = model_cls._base_from_instance(
                config, model_copy, parent_base, station, entry_explicit
            )
            model_cls._resolve_url_fields(config, model_copy, base, model_copy._manifest_raw_urls)
            handler_input: Any = model_copy
        else:
            raw_base, raw_urls = model_cls._read_raw_input(config, data)
            base, owned = model_cls._compute_base(
                config, raw_base, parent_base, station, entry_explicit
            )
            handler_input = model_cls._rewrite_input(config, data, base, raw_urls)

        previous_entry = payload.get('entry')
        previous_explicit = payload.get('entry_explicit')
        payload['entry'] = base
        # A base inherited from the enclosing manifest is not an inbound alias
        # for the children, so it must not outrank their default factory.
        payload['entry_explicit'] = False
        payload['depth'] = payload.get('depth', 0) + 1
        try:
            model = handler(handler_input)
        finally:
            payload['entry'] = previous_entry
            payload['entry_explicit'] = previous_explicit
            payload['depth'] = payload.get('depth', 1) - 1

        if not reused:
            object.__setattr__(model, '_manifest_raw_urls', raw_urls)
        object.__setattr__(model, '_manifest_base', base)
        object.__setattr__(model, '_manifest_relative', config.relative and owned)
        object.__setattr__(model, '_manifest_owned', owned)
        object.__setattr__(model, '_manifest_station', station)
        object.__setattr__(model, '_manifest_token', token)

        if payload['depth'] == 0 and payload.get('managed'):
            # Outermost manifest: rebase every nested sub-manifest whose wrap
            # was skipped through pydantic-core's same-type fast path, then run
            # the download address checks against the final request URLs.
            model._manifest_rebase_tree(payload)
        return model

    # ------------------------------------------------------------------ hooks

    def manifest_verify_url(self, url: str, *, field_name: str, index: int | None) -> None:
        """Validate a final request URL before it enters the download queue.

        Override this to reject addresses that must not be requested from the
        current station. Raise :class:`ValueError` to reject the address; the
        resulting validation error points at the owning sub-manifest and
        download item, while previously validated manifests stay usable.

        Args:
            url: Final absolute request URL.
            field_name: Model field that declared the download reference.
            index: Position inside a list field, or ``None`` for a scalar.
        """

    @property
    def manifest_base(self) -> str:
        """Absolute base URL used to resolve this manifest's references."""
        if self._manifest_base is None:
            raise AttributeError('manifest_base is only available after validation')
        return self._manifest_base

    @property
    def manifest_base_is_relative(self) -> bool:
        """Whether this manifest's base column was resolved against a parent."""
        return self._manifest_relative

    @property
    def manifest_base_owned(self) -> bool:
        """Whether this manifest carries its own base instead of inheriting."""
        return self._manifest_owned

    @property
    def manifest_station(self) -> str | None:
        """Station identifier carried by the validation context, if any."""
        return self._manifest_station

    def request_urls(self) -> list[str]:
        """All final request URLs declared by this manifest, in field order."""
        config = self._manifest_config()
        urls: list[str] = []
        for field_name in config.url_fields:
            _collect(getattr(self, field_name), urls)
        return urls

    def iter_manifests(self) -> Iterator['ManifestBase']:
        """Yield this manifest and every nested sub-manifest, depth first."""
        yield self
        for child, _loc in self._iter_children():
            yield from child.iter_manifests()

    # ------------------------------------------------------------- internals

    @classmethod
    def _manifest_config(cls) -> ManifestConfig:
        config = getattr(cls, 'manifest', None)
        if not isinstance(config, ManifestConfig):
            raise PydanticCustomError(
                'manifest_config_missing',
                '{name} must declare a ManifestConfig through the `manifest` ClassVar',
                {'name': cls.__name__},
            )
        return config

    @classmethod
    def _read_raw_input(cls, config: ManifestConfig, data: Any) -> tuple[Any, dict[str, list[str]]]:
        if not isinstance(data, dict):
            return _UNSET, {}
        raw_base = _UNSET
        base_key = _column_key(cls, config.base_field, data)
        if base_key is not None and data.get(base_key) is not None:
            raw_base = data[base_key]
        raw_urls: dict[str, list[str]] = {}
        for field_name in config.url_fields:
            key = _column_key(cls, field_name, data)
            value = data.get(key) if key is not None else None
            raw_urls[field_name] = _as_string_list(value)
        return raw_base, raw_urls

    @classmethod
    def _base_from_instance(
        cls,
        config: ManifestConfig,
        model: 'ManifestBase',
        parent_base: str | None,
        station: str | None,
        entry_explicit: bool,
    ) -> tuple[str, bool]:
        raw_base = _UNSET
        if config.base_field in model.model_fields_set:
            raw_base = getattr(model, config.base_field)
        return cls._compute_base(config, raw_base, parent_base, station, entry_explicit)

    @classmethod
    def _compute_base(
        cls,
        config: ManifestConfig,
        raw_base: Any,
        parent_base: str | None,
        station: str | None,
        entry_explicit: bool,
    ) -> tuple[str, bool]:
        if raw_base is not _UNSET:
            if not isinstance(raw_base, str):
                raise PydanticCustomError(
                    'manifest_base_invalid',
                    'Manifest base must be a string, got {type}',
                    {'type': type(raw_base).__name__},
                )
            if config.relative:
                if parent_base is None:
                    raise PydanticCustomError(
                        'manifest_relative_base_without_parent',
                        'Relative manifest base on {name} requires a parent base',
                        {'name': cls.__name__},
                    )
                return resolve_reference(parent_base, raw_base), True
            parts = urlsplit(raw_base)
            if parts.scheme and parts.netloc:
                return raw_base, True
            if parent_base is None:
                raise PydanticCustomError(
                    'manifest_base_invalid',
                    'Manifest base {base!r} is not an absolute URL and no parent base is available',
                    {'base': raw_base},
                )
            return resolve_reference(parent_base, raw_base), True

        # An inbound alias (root manifest only) outranks the default factory.
        if entry_explicit and parent_base is not None:
            return parent_base, True
        if config.default_factory is not None:
            return _call_factory(config.default_factory, station), True
        # A base inherited from the enclosing manifest is the final fallback.
        if parent_base is not None:
            return parent_base, False
        raise PydanticCustomError(
            'manifest_base_missing',
            'Manifest {name} has no base URL and no parent base to inherit',
            {'name': cls.__name__},
        )

    @classmethod
    def _rewrite_input(
        cls, config: ManifestConfig, data: Any, base: str, raw_urls: dict[str, list[str]]
    ) -> Any:
        if not isinstance(data, dict):
            return data
        rewritten = dict(data)
        for field_name, references in raw_urls.items():
            key = _column_key(cls, field_name, rewritten) or field_name
            current = rewritten.get(key)
            resolved = [resolve_reference(base, ref) for ref in references]
            if isinstance(current, list):
                rewritten[key] = resolved
            elif isinstance(current, str) and resolved:
                rewritten[key] = resolved[0]
        return rewritten

    @classmethod
    def _resolve_url_fields(
        cls,
        config: ManifestConfig,
        model: 'ManifestBase',
        base: str,
        raw_urls: dict[str, list[str]],
    ) -> None:
        for field_name, references in raw_urls.items():
            resolved = [resolve_reference(base, ref) for ref in references]
            current = model.__dict__.get(field_name)
            if isinstance(current, list):
                model.__dict__[field_name] = resolved
            elif isinstance(current, str) and resolved:
                model.__dict__[field_name] = resolved[0]

    def _manifest_rebase_tree(self, payload: dict[str, Any]) -> None:
        errors: list[InitErrorDetails] = []
        self._rebase_children(payload)
        self._verify_tree((), errors)
        if errors:
            raise ValidationError.from_exception_data(type(self).__name__, errors)

    def _rebase_children(self, payload: dict[str, Any]) -> None:
        token = payload['token']
        for child, loc in self._iter_children():
            if child._manifest_token != token:
                # Reused Python sub-manifest whose wrap was skipped: rebase a
                # copy so the caller's instance keeps its original station.
                config = child._manifest_config()
                rebased = child.model_copy()
                base, owned = child._base_from_instance(
                    config,
                    rebased,
                    self.manifest_base,
                    payload.get('station'),
                    False,
                )
                child._resolve_url_fields(config, rebased, base, rebased._manifest_raw_urls)
                object.__setattr__(rebased, '_manifest_base', base)
                object.__setattr__(rebased, '_manifest_relative', config.relative and owned)
                object.__setattr__(rebased, '_manifest_owned', owned)
                object.__setattr__(rebased, '_manifest_station', payload.get('station'))
                object.__setattr__(rebased, '_manifest_token', token)
                self._replace_child(loc, rebased)
                child = rebased
            child._rebase_children(payload)

    def _verify_tree(
        self, loc_prefix: tuple[Any, ...], errors: list[InitErrorDetails]
    ) -> None:
        config = self._manifest_config()
        for field_name in config.url_fields:
            value = getattr(self, field_name)
            values = value if isinstance(value, list) else [value]
            for index, url in enumerate(values):
                if not isinstance(url, str):
                    continue
                try:
                    self.manifest_verify_url(url, field_name=field_name, index=index)
                except ValueError as exc:
                    loc = (*loc_prefix, field_name, index) if isinstance(value, list) else (
                        *loc_prefix,
                        field_name,
                    )
                    errors.append(
                        InitErrorDetails(
                            type=PydanticCustomError(
                                'manifest_url_denied',
                                'Download address {url} rejected for {name}.{field}: {error}',
                                {
                                    'url': url,
                                    'name': type(self).__name__,
                                    'field': field_name,
                                    'error': str(exc),
                                },
                            ),
                            loc=loc,
                        )
                    )
        for child, loc in self._iter_children():
            # ``loc`` is relative to this model; prepend the accumulated path
            # so error locations stay absolute from the root manifest.
            child._verify_tree((*loc_prefix, *loc), errors)

    def _iter_children(self) -> Iterator[tuple['ManifestBase', tuple[Any, ...]]]:
        config = self._manifest_config()
        if config.children_field is not None:
            yield from _walk_manifests(
                getattr(self, config.children_field), (config.children_field,)
            )
            return
        for field_name in type(self).model_fields:
            yield from _walk_manifests(getattr(self, field_name), (field_name,))

    def _replace_child(self, loc: tuple[Any, ...], rebased: 'ManifestBase') -> None:
        # Walk the full location path, supporting nested lists and dicts.
        target: Any = self.__dict__[loc[0]]
        for key in loc[1:-1]:
            target = target[key]
        if isinstance(target, (list, dict)):
            target[loc[-1]] = rebased
        else:
            self.__dict__[loc[0]] = rebased


def _manifest_payload(context: Any) -> dict[str, Any]:
    if isinstance(context, dict):
        payload = context.get(_CONTEXT_KEY)
        if isinstance(payload, dict) and 'token' in payload:
            return payload
    token = uuid4()
    return {
        'entry': None,
        'station': None,
        'entry_explicit': False,
        'token': token,
        'depth': 0,
        'managed': False,
    }


def _column_key(model_cls: type[BaseModel], field_name: str, data: dict) -> str | None:
    if field_name in data:
        return field_name
    field = model_cls.model_fields.get(field_name)
    if field is None:
        return None
    for alias in (field.validation_alias, field.alias):
        if isinstance(alias, str) and alias in data:
            return alias
    return None


def _as_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    return []


def _call_factory(factory: Callable[..., str], station: str | None) -> str:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return factory()
    accepts_station = any(
        (
            parameter.kind
            in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            and parameter.name == 'station'
        )
        or parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if accepts_station:
        return factory(station=station)
    return factory()


def _walk_manifests(value: Any, loc: tuple[Any, ...]) -> Iterator[tuple[ManifestBase, tuple[Any, ...]]]:
    if isinstance(value, ManifestBase):
        yield value, loc
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_manifests(item, (*loc, index))
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _walk_manifests(item, (*loc, key))


def _collect(value: Any, urls: list[str]) -> None:
    if isinstance(value, str):
        urls.append(value)
    elif isinstance(value, list):
        for item in value:
            _collect(item, urls)
    elif isinstance(value, dict):
        for item in value.values():
            _collect(item, urls)
