"""Internal support for attribute read snapshots.

The snapshot is scoped to a single logical validation batch. Reads are cached by
the identity of the object and the attribute name, so an expensive descriptor is
only invoked once even when several models read it.

Each model (or *role*) in the batch is validated against its own view of the
snapshot. The view copies dictionaries and lists, keeping a memo so that a
container reached through different attributes is the same object within that
role. Because each role gets its own copies, an in-place transformation in one
role cannot affect another role, and the original ORM objects are never mutated.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic.main import BaseModel

_MISSING = object()
""":meta private: Sentinel stored for an attribute lookup that raised ``AttributeError``."""

_ERROR = object()
""":meta private: Sentinel stored for an attribute lookup that raised another exception."""

_IMMUTABLE_TYPES = (str, bytes, bytearray, int, float, complex, bool, type(None))
_CONTAINER_TYPES = (dict, list, tuple, set, frozenset)
_EXCLUDED_MODULE_ROOTS = frozenset(
    {'builtins', 'collections', 'datetime', 'decimal', 'enum', 'pathlib', 're', 'typing', 'types', 'uuid'}
)


class SnapshotState:
    """A cache of attribute reads for a single batch."""

    __slots__ = ('_reads', '_objects')

    def __init__(self) -> None:
        # (id(obj), attribute name) -> (value or exception, status)
        self._reads: dict[tuple[int, str], tuple[Any, Any]] = {}
        # Keep strong references so that ``id()`` values cannot be reused while the
        # batch is in progress:
        self._objects: dict[int, Any] = {}

    def read(self, obj: Any, name: str) -> Any:
        """Read ``name`` from ``obj``, returning the value cached for this batch."""
        key = (id(obj), name)
        try:
            value, status = self._reads[key]
        except KeyError:
            self._objects[id(obj)] = obj
            try:
                value = getattr(obj, name)
            except AttributeError as exc:
                self._reads[key] = (exc, _MISSING)
                raise
            except Exception as exc:
                self._reads[key] = (exc, _ERROR)
                raise
            else:
                self._reads[key] = (value, None)
                return value

        if status is _ERROR or status is _MISSING:
            raise value
        return value


def _is_pydantic_model(value: Any) -> bool:
    """Return whether ``value`` is a Pydantic model instance, without importing it eagerly."""
    pydantic = sys.modules.get('pydantic')
    main = sys.modules.get('pydantic.main')
    base_model = getattr(main, 'BaseModel', None) if main is not None else getattr(pydantic, 'BaseModel', None)
    return base_model is not None and isinstance(value, base_model)


def _is_proxyable(value: Any) -> bool:
    """Return whether ``value`` is an object whose attributes should be read through a proxy."""
    if isinstance(value, (*_IMMUTABLE_TYPES, *_CONTAINER_TYPES)):
        return False
    if isinstance(value, type) or callable(value) or _is_pydantic_model(value):
        return False
    module_root = type(value).__module__.split('.')[0]
    return module_root not in _EXCLUDED_MODULE_ROOTS


def _is_immutable_sequence(value: Any) -> bool:
    return isinstance(value, (str, bytes, bytearray))


class AttributeView:
    """A per-role view over a snapshot."""

    __slots__ = ('_state', '_containers', '_proxies')

    def __init__(self, state: SnapshotState) -> None:
        self._state = state
        self._containers: dict[int, Any] = {}
        self._proxies: dict[int, _SnapshotProxy] = {}

    def wrap(self, value: Any) -> Any:
        """Wrap a value read for this role."""
        if isinstance(value, _IMMUTABLE_TYPES) or _is_pydantic_model(value):
            return value
        if isinstance(value, _SnapshotProxy):
            return value
        if isinstance(value, dict):
            return self._copy_dict(value)
        if isinstance(value, list):
            return self._copy_list(value)
        if type(value) is tuple:
            return tuple(self.wrap(item) for item in value)
        if isinstance(value, (set, frozenset)):
            return type(value)(self.wrap(item) for item in value)
        if _is_proxyable(value):
            return self._proxy(value)
        return value

    def _copy_dict(self, value: dict[Any, Any]) -> dict[Any, Any]:
        key = id(value)
        try:
            return self._containers[key]
        except KeyError:
            copied: dict[Any, Any] = {}
            self._containers[key] = copied
            for k, v in value.items():
                copied[k] = self.wrap(v)
            return copied

    def _copy_list(self, value: list[Any]) -> list[Any]:
        key = id(value)
        try:
            return self._containers[key]
        except KeyError:
            copied: list[Any] = []
            self._containers[key] = copied
            copied.extend(self.wrap(item) for item in value)
            return copied

    def _proxy(self, obj: Any) -> _SnapshotProxy:
        key = id(obj)
        try:
            return self._proxies[key]
        except KeyError:
            proxy = _SnapshotProxy(obj, self)
            self._proxies[key] = proxy
            return proxy

    def read(self, obj: Any, name: str) -> Any:
        """Read and wrap an attribute from ``obj``."""
        return self.wrap(self._state.read(obj, name))


class _SnapshotProxy:
    """A proxy around an ORM object reading attributes through a snapshot."""

    __slots__ = ('_obj', '_view')

    def __init__(self, obj: Any, view: AttributeView) -> None:
        self._obj = obj
        self._view = view

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_'):
            raise AttributeError(name)
        return self._view.read(self._obj, name)

    def __dir__(self) -> list[str]:
        return dir(self._obj)

    def __repr__(self) -> str:
        return f'<_SnapshotProxy {self._obj!r}>'


_active_snapshot: ContextVar[SnapshotState | None] = ContextVar('_active_snapshot', default=None)


def active_snapshot() -> SnapshotState | None:
    """Return the snapshot currently active in this context, if any."""
    return _active_snapshot.get()


def wrap_input(value: Any, state: SnapshotState | None) -> Any:
    """Return a view of ``value`` suitable for validation with the provided snapshot state.

    A top-level list or tuple is treated as a batch of independent items. Each item gets
    its own view so that in-place changes made while validating one item cannot affect
    another item, while descriptor reads are still shared through ``state``.
    """
    if state is None:
        return value
    if isinstance(value, list):
        return [AttributeView(state).wrap(item) for item in value]
    if type(value) is tuple:
        return tuple(AttributeView(state).wrap(item) for item in value)
    return AttributeView(state).wrap(value)


def activate_snapshot(state: SnapshotState) -> Any:
    """Activate ``state`` for the current context, returning a reset token."""
    return _active_snapshot.set(state)


def deactivate_snapshot(token: Any) -> None:
    """Deactivate a snapshot previously activated with :func:`activate_snapshot`."""
    _active_snapshot.reset(token)


def resolve_snapshot_state(snapshot_reads: bool | None) -> SnapshotState | None:
    """Resolve the snapshot state for a validation call.

    ``None`` inherits an active snapshot (if any), ``True`` creates a fresh snapshot
    when none is active, and ``False`` disables snapshots for this call.
    """
    if snapshot_reads is False:
        return None
    active = _active_snapshot.get()
    if active is not None:
        return active
    if snapshot_reads is True:
        return SnapshotState()
    return None


def unwrap(value: Any) -> Any:
    """Replace snapshot proxies in a validation result with the original objects."""
    if isinstance(value, _SnapshotProxy):
        return value._obj
    if isinstance(value, dict):
        for key, item in value.items():
            value[key] = unwrap(item)
        return value
    if isinstance(value, list):
        for index, item in enumerate(value):
            value[index] = unwrap(item)
        return value
    if type(value) is tuple:
        return tuple(unwrap(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return type(value)(unwrap(item) for item in value)
    if _is_pydantic_model(value):
        _unwrap_model(value)
    return value


def _unwrap_model(model: BaseModel) -> None:
    for name, item in model.__dict__.items():
        model.__dict__[name] = unwrap(item)
    extra = getattr(model, '__pydantic_extra__', None)
    if isinstance(extra, dict):
        for name, item in extra.items():
            extra[name] = unwrap(item)


def prefix_validation_error(
    error: Any, index: int | None = None, title: str | None = None
) -> Any:  # Returns a pydantic_core.ValidationError
    """Return a new validation error, unwrapping snapshot proxies and optionally prefixing locations."""
    from pydantic_core import ValidationError

    errors: list[Any] = []
    for details in error.errors(include_url=False):
        loc = (index, *details['loc']) if index is not None else details['loc']
        error_data: dict[str, Any] = {'type': details['type'], 'loc': loc}
        if 'input' in details:
            error_data['input'] = unwrap(details['input'])
        if 'ctx' in details:
            error_data['ctx'] = details['ctx']
        errors.append(error_data)
    return ValidationError.from_exception_data(title or error.title, errors, input_type='python')


def rewrap_validation_error(error: Any, title: str | None = None) -> Any:
    """Unwrap snapshot proxies from an error's inputs without changing its locations."""
    return prefix_validation_error(error, None, title)


@contextmanager
def snapshot_reads(enabled: bool = True) -> Iterator[SnapshotState | None]:
    """Enable attribute read snapshots for the duration of the context.

    This is the explicit, call-scoped way of enabling read snapshots when multiple
    model validations need to share reads (for example, cleaning, reconciliation
    and audit roles for the same ORM row).
    """
    if not enabled:
        yield None
        return
    state = SnapshotState()
    token = _active_snapshot.set(state)
    try:
        yield state
    finally:
        _active_snapshot.reset(token)
