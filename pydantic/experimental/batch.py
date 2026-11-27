"""Experimental batch validation functionality. Be careful with this API, it's subject to change."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Generic, TypeVar, overload

from pydantic import TypeAdapter
from pydantic._internal import _attribute_cache
from pydantic.config import ExtraValues

__all__ = (
    'Batch',
    'BatchAdapter',
    'BatchValidator',
    'batch_validate',
    'snapshot_reads',
    'validate_batch',
    'validate_python',
)

T = TypeVar('T')

_UNSET: Any = object()


class _Entry:
    """A normalized batch entry."""

    __slots__ = ('adapter', 'obj')

    def __init__(self, adapter: TypeAdapter[Any], obj: Any) -> None:
        self.adapter = adapter
        self.obj = obj


def _coerce_adapter(model_or_adapter: Any) -> TypeAdapter[Any]:
    if isinstance(model_or_adapter, TypeAdapter):
        return model_or_adapter
    return TypeAdapter(model_or_adapter)


def _looks_like_model(value: Any) -> bool:
    if isinstance(value, TypeAdapter):
        return True
    return isinstance(value, type) and hasattr(value, '__pydantic_validator__')


def _coerce_entry(entry: Any) -> _Entry:
    if isinstance(entry, _Entry):
        return entry
    if isinstance(entry, tuple) and len(entry) == 2:
        first, second = entry
        if _looks_like_model(first):
            return _Entry(_coerce_adapter(first), second)
        if _looks_like_model(second):
            return _Entry(_coerce_adapter(second), first)
        return _Entry(_coerce_adapter(first), second)
    if isinstance(entry, Mapping) and 'model' in entry and 'object' in entry:
        return _Entry(_coerce_adapter(entry['model']), entry['object'])
    if isinstance(entry, Mapping) and 'object' in entry and 'model' in entry:
        return _Entry(_coerce_adapter(entry['model']), entry['object'])
    model = getattr(entry, 'model', None)
    if model is not None and hasattr(entry, 'object'):
        return _Entry(_coerce_adapter(model), entry.object)
    raise TypeError(
        'Batch entries must be `(model, object)` pairs (in either order), mappings with `model` and `object` keys, '
        'or objects exposing `model` and `object` attributes.'
    )


class BatchAdapter(Generic[T]):
    """Validate a homogeneous batch of inputs in a single logical validation run.

    This is a thin wrapper around :meth:`TypeAdapter.validate_batch`.

    Args:
        adapter: The model or adapter used to validate each item in the batch.
    """

    __slots__ = ('adapter',)

    def __init__(self, adapter: TypeAdapter[T] | Any) -> None:
        self.adapter = _coerce_adapter(adapter)

    def validate_python(
        self,
        inputs: Iterable[Any],
        /,
        *,
        snapshot_reads: bool = False,
        from_attributes: bool | None = None,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> list[T]:
        """Validate a batch of Python objects.

        Args:
            inputs: The objects to validate.
            snapshot_reads: Whether to cache attribute reads for the duration of this batch.
            from_attributes: Whether to extract data from object attributes.
            strict: Whether to strictly check types.
            extra: Whether to ignore, allow, or forbid extra data during model validation.
            context: Additional context to pass to the validator.
            by_alias: Whether to use the field's alias when validating against the provided input data.
            by_name: Whether to use the field's name when validating against the provided input data.

        Returns:
            The validated objects.
        """
        if not snapshot_reads and _attribute_cache.active_snapshot() is None:
            return [
                self.adapter.validate_python(
                    item,
                    from_attributes=from_attributes,
                    strict=strict,
                    extra=extra,
                    context=context,
                    by_alias=by_alias,
                    by_name=by_name,
                )
                for item in inputs
            ]
        return self.adapter.validate_batch(
            inputs,
            snapshot_reads=True,
            from_attributes=from_attributes,
            strict=strict,
            extra=extra,
            context=context,
            by_alias=by_alias,
            by_name=by_name,
        )


class Batch:
    """Validate a heterogeneous batch of ``(model, object)`` entries as one run.

    Each entry is validated against its own view of the snapshot, so in-place
    transformations in one entry cannot affect another entry or the original object.

    Args:
        snapshot_reads: Whether read snapshots are enabled when this batch is used as
            a context manager.
    """

    __slots__ = ('_snapshot_reads', '_state', '_token')

    def __init__(self, *, snapshot_reads: bool = False) -> None:
        self._snapshot_reads = snapshot_reads
        self._state: _attribute_cache.SnapshotState | None = None
        self._token: Any = None

    def __enter__(self) -> Batch:
        if self._snapshot_reads:
            self._state = _attribute_cache.SnapshotState()
            self._token = _attribute_cache.activate_snapshot(self._state)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._token is not None:
            _attribute_cache.deactivate_snapshot(self._token)
        self._state = None
        self._token = None

    @overload
    def validate(self, model: TypeAdapter[T] | Any, obj: Any, /) -> T: ...

    @overload
    def validate(self, entries: Iterable[Any], /) -> list[Any]: ...

    def validate(self, model_or_entries: Any, obj: Any = _UNSET, /) -> Any:
        """Validate one entry or an iterable of entries."""
        if obj is not _UNSET:
            entry = _coerce_entry((model_or_entries, obj))
            state = self._state
            if state is None and self._snapshot_reads:
                state = _attribute_cache.SnapshotState()
            input_value = _attribute_cache.wrap_input(entry.obj, state)
            try:
                result = entry.adapter.validate_python(input_value, from_attributes=True)
            except Exception as exc:
                if type(exc).__name__ == 'ValidationError':
                    raise _attribute_cache.prefix_validation_error(exc, 0, _title(entry.adapter)) from None
                raise
            return _attribute_cache.unwrap(result)
        return self.validate_python(model_or_entries)

    def validate_python(
        self,
        entries: Iterable[Any],
        /,
        *,
        snapshot_reads: bool | None = None,
        from_attributes: bool | None = True,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> list[Any]:
        """Validate a heterogeneous batch of entries.

        Args:
            entries: Entries as ``(model, object)`` pairs, mappings with ``model`` and
                ``object`` keys, or objects exposing ``model`` and ``object`` attributes.
            snapshot_reads: Whether to cache attribute reads for the duration of this
                batch. Defaults to the value this :class:`Batch` was created with, or to
                an already active snapshot if one was opened with :func:`snapshot_reads`.
            from_attributes: Whether to extract data from object attributes. Defaults to ``True``.
            strict: Whether to strictly check types.
            extra: Whether to ignore, allow, or forbid extra data during model validation.
            context: Additional context to pass to the validator.
            by_alias: Whether to use the field's alias when validating against the provided input data.
            by_name: Whether to use the field's name when validating against the provided input data.

        Returns:
            The validated models, in entry order.
        """
        if snapshot_reads is False:
            state = None
        elif self._state is not None:
            state = self._state
        elif snapshot_reads is True or self._snapshot_reads:
            state = _attribute_cache.SnapshotState()
        else:
            state = _attribute_cache.active_snapshot()

        values: list[Any] = []
        for index, raw_entry in enumerate(entries):
            entry = _coerce_entry(raw_entry)
            input_value = _attribute_cache.wrap_input(entry.obj, state)
            try:
                values.append(
                    entry.adapter.validate_python(
                        input_value,
                        from_attributes=from_attributes,
                        strict=strict,
                        extra=extra,
                        context=context,
                        by_alias=by_alias,
                        by_name=by_name,
                    )
                )
            except Exception as exc:
                if type(exc).__name__ == 'ValidationError':
                    raise _attribute_cache.prefix_validation_error(exc, index, _title(entry.adapter)) from None
                raise
        for index, value in enumerate(values):
            values[index] = _attribute_cache.unwrap(value)
        return values


def _title(adapter: TypeAdapter[Any]) -> str:
    type_ = adapter._type
    return getattr(type_, '__name__', None) or str(type_)


def validate_python(
    entries: Iterable[Any],
    /,
    *,
    snapshot_reads: bool = True,
    from_attributes: bool | None = True,
    strict: bool | None = None,
    extra: ExtraValues | None = None,
    context: Any | None = None,
    by_alias: bool | None = None,
    by_name: bool | None = None,
) -> list[Any]:
    """Validate a heterogeneous batch of ``(model, object)`` entries in one run.

    Args:
        entries: Entries as ``(model, object)`` pairs, mappings with ``model`` and
            ``object`` keys, or objects exposing ``model`` and ``object`` attributes.
        snapshot_reads: Whether to cache attribute reads for the duration of this batch.
            Defaults to ``True`` because that is the purpose of this entry point;
            pass ``False`` to opt out.
        from_attributes: Whether to extract data from object attributes. Defaults to ``True``.
        strict: Whether to strictly check types.
        extra: Whether to ignore, allow, or forbid extra data during model validation.
        context: Additional context to pass to the validator.
        by_alias: Whether to use the field's alias when validating against the provided input data.
        by_name: Whether to use the field's name when validating against the provided input data.

    Returns:
        The validated models, in entry order.
    """
    return Batch(snapshot_reads=snapshot_reads).validate_python(
        entries,
        from_attributes=from_attributes,
        strict=strict,
        extra=extra,
        context=context,
        by_alias=by_alias,
        by_name=by_name,
    )


validate_batch = validate_python
batch_validate = validate_python
BatchValidator = Batch


def snapshot_reads(enabled: bool = True) -> Any:
    """Enable attribute read snapshots for the duration of a ``with`` block.

    This is the explicit, call-scoped way of enabling read snapshots when several
    model validations need to share reads (for example, cleaning, reconciliation and
    audit roles for the same ORM row). The snapshot is discarded on exit.

    Args:
        enabled: Whether to enable snapshots. When ``False``, the context yields ``None``.

    Returns:
        A context manager yielding the snapshot state (or ``None``).
    """
    return _attribute_cache.snapshot_reads(enabled)
