"""Keep track of the items discarded by [`OnErrorOmit`][pydantic.types.OnErrorOmit] during validation.

When a collection is annotated with `OnErrorOmit`, invalid items are silently dropped from the
validated output. An [`OmitReceipt`][pydantic.omit_receipt.OmitReceipt] can be passed to
[`TypeAdapter.validate_python`][pydantic.TypeAdapter.validate_python] or
[`TypeAdapter.validate_json`][pydantic.TypeAdapter.validate_json] to record what was omitted,
where it was omitted (using the *original* indexes and keys of the input), and why:

```python
from pydantic import OmitReceipt, OnErrorOmit, TypeAdapter

adapter = TypeAdapter(list[OnErrorOmit[int]])
receipt = OmitReceipt()

assert adapter.validate_python(['1', 'a', '3'], omit_receipt=receipt) == [1, 3]
assert receipt.succeeded
assert [(r.locator, r.status) for r in receipt.omissions] == [(1, 'omitted')]
```
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from typing import Any, Literal

from pydantic_core import ValidationError, core_schema
from pydantic_core.core_schema import CoreSchema

__all__ = ('OmitRecord', 'OmitReceipt')

_RecordStatus = Literal['kept', 'omitted', 'overwritten', 'invalid_key']
"""The possible statuses of an [`OmitRecord`][pydantic.omit_receipt.OmitRecord]."""

_ValueStatus = Literal['validated', 'omitted', 'unvalidated']
"""The validation status of the value of a mapping entry."""

_Role = Literal['item', 'key', 'value']


class _Frame:
    """A node of the tree of omittable positions visited during a single validation call."""

    __slots__ = ('children', 'errors', 'outcome', 'raw_input', 'role', 'token', 'validated')

    def __init__(self, role: str, raw_input: Any, token: _OmitHook | None) -> None:
        self.role = role
        self.raw_input = raw_input
        self.token = token
        self.outcome: Literal['ok', 'omit', 'error'] = 'ok'
        self.errors: tuple[dict[str, Any], ...] = ()
        self.validated: Any = None
        self.children: list[_Frame] = []


class _Collector:
    """Per-call state: the frame tree being built while a receipt is collected."""

    def __init__(self) -> None:
        self.root = _Frame('root', None, None)
        self.stack = [self.root]


_active_collector: contextvars.ContextVar[_Collector | None] = contextvars.ContextVar(
    'pydantic_omit_receipt_collector', default=None
)


class _OmitHook:
    """Wrap validator installed around `OnErrorOmit` member schemas.

    When no receipt is being collected this is a transparent passthrough, so validation
    behaves (and performs the same validator calls) exactly as if the hook wasn't there.
    """

    __slots__ = ('role',)

    def __init__(self, role: _Role) -> None:
        self.role = role

    def __call__(self, value: Any, handler: Any) -> Any:
        collector = _active_collector.get()
        if collector is None:
            return handler(value)
        frame = _Frame(self.role, value, self)
        parent = collector.stack[-1]
        parent.children.append(frame)
        collector.stack.append(frame)
        try:
            result = handler(value)
        except ValidationError as exc:
            frame.outcome = 'omit'
            frame.errors = tuple(exc.errors())
            raise
        except BaseException:
            frame.outcome = 'error'
            raise
        else:
            frame.validated = result
            return result
        finally:
            collector.stack.pop()


def _wrap_omit_member_schema(schema: CoreSchema, role: _Role) -> CoreSchema:
    """Install an omit-recording hook around `schema` if it is an `OnErrorOmit` member schema.

    `OnErrorOmit` members are compiled to a `with_default` schema with `on_error='omit'`; the
    hook is placed inside it (so that errors are still converted to omissions) and around the
    actual member schema. Any other schema is returned unchanged.
    """
    if schema['type'] == 'default' and schema.get('on_error') == 'omit':
        inner = schema['schema']
        function = inner.get('function') if inner['type'] == 'function-wrap' else None
        existing = function.get('function') if isinstance(function, dict) else None
        if not isinstance(existing, _OmitHook):
            return {**schema, 'schema': core_schema.no_info_wrap_validator_function(_OmitHook(role), inner)}  # type: ignore[typeddict-item]
    return schema


class OmitRecord:
    """A single omittable position visited during validation.

    Records form a tree mirroring the structure of the validated data: each record corresponds
    to an item of a collection annotated with [`OnErrorOmit`][pydantic.types.OnErrorOmit] (or to
    an entry of a mapping whose keys and/or values are annotated), and its children are the
    omittable positions found inside that item or entry value.

    Records are located by the *original* coordinates of the input: the index of the item in its
    collection for sequences, or the raw (unvalidated) key of the entry for mappings.
    """

    __slots__ = (
        '_children',
        '_omissions_cache',
        '_parent',
        '_role',
        '_succeeded',
        '_validated_key',
        'errors',
        'locator',
        'path',
        'status',
        'value_status',
    )

    def __init__(
        self,
        *,
        role: Literal['item', 'entry'],
        locator: Any,
        path: tuple[Any, ...],
        status: _RecordStatus,
        errors: tuple[dict[str, Any], ...],
        value_status: _ValueStatus,
        validated_key: Any,
        succeeded: bool,
        parent: OmitRecord | None,
    ) -> None:
        self._role = role
        self.locator = locator
        """The original coordinates of this position: the item index or the raw mapping key."""
        self.path = path
        """The locators of this record and all its parents, starting from the top level."""
        self.status = status
        """What happened to this position: 'kept', 'omitted', 'overwritten' or 'invalid_key'."""
        self.errors = errors
        """The validation errors that caused the omission (empty unless omitted or invalid key)."""
        self.value_status = value_status
        """For mapping entries, what happened to the value: 'validated', 'omitted' or 'unvalidated'."""
        self._validated_key = validated_key
        self._succeeded = succeeded
        self._parent = parent
        self._children: tuple[OmitRecord, ...] = ()
        self._omissions_cache: tuple[OmitRecord, ...] | None = None

    @property
    def validated_key(self) -> Any:
        """The validated (normalized) key of a mapping entry, or `None` if its key was invalid."""
        return self._validated_key

    @property
    def children(self) -> tuple[OmitRecord, ...]:
        """The omittable positions found inside this item or entry value, in input order."""
        return self._children

    @property
    def items(self) -> tuple[OmitRecord, ...]:
        """The child records corresponding to sequence items, in input order."""
        return tuple(child for child in self._children if child._role == 'item')

    @property
    def entries(self) -> dict[Any, OmitRecord]:
        """The child records corresponding to mapping entries, keyed by the raw input key."""
        return {child.locator: child for child in self._children if child._role == 'entry'}

    @property
    def omissions(self) -> tuple[OmitRecord, ...]:
        """This record and all its descendants that were actually omitted, in input order."""
        if self._omissions_cache is None:
            result = [self] if self.status in ('omitted', 'invalid_key') else []
            for child in self._children:
                result.extend(child.omissions)
            self._omissions_cache = tuple(result)
        return self._omissions_cache

    @property
    def recoverable(self) -> bool:
        """Whether this omission can be re-collected individually.

        An omission is individually recoverable when it is a leaf omission (the item itself was
        discarded, not something inside it) and every position containing it was kept, i.e. the
        item was dropped from a collection that otherwise made it into the validated output.
        Always `False` if the validation call as a whole failed.
        """
        if self.status != 'omitted' or self._children:
            return False
        node = self._parent
        while node is not None:
            if node.status != 'kept':
                return False
            node = node._parent
        return self._succeeded

    def find(self, *locators: Any) -> OmitRecord | None:
        """Find a descendant record by its original coordinates, or `None` if there is no match."""
        records = self._children
        record: OmitRecord | None = None
        for locator in locators:
            record = next((r for r in records if r.locator == locator), None)
            if record is None:
                return None
            records = record._children
        return record

    def __repr__(self) -> str:
        return f'OmitRecord(path={self.path!r}, status={self.status!r})'


def _build_children(
    frame: _Frame, path: tuple[Any, ...], succeeded: bool, parent: OmitRecord | None
) -> tuple[OmitRecord, ...]:
    records: list[OmitRecord] = []
    entry_records: list[OmitRecord] = []
    item_ordinals: dict[int, int] = {}
    keyless_entry_ordinal = 0
    pending_key: _Frame | None = None

    def build_entry(key_frame: _Frame | None, value_frame: _Frame | None) -> None:
        nonlocal keyless_entry_ordinal
        if key_frame is not None:
            locator = key_frame.raw_input
            key_ok = key_frame.outcome == 'ok'
            key_errors = key_frame.errors
            validated_key = key_frame.validated if key_ok else None
        else:
            locator = keyless_entry_ordinal
            keyless_entry_ordinal += 1
            key_ok = True
            key_errors = ()
            validated_key = None
        child_frame: _Frame | None
        if not key_ok:
            status: _RecordStatus = 'invalid_key'
            errors = key_errors
            value_status: _ValueStatus = 'unvalidated'
            child_frame = None
        elif value_frame is None:
            # no hook was installed for the value (it is not omittable): as the key validated,
            # the entry was kept (a value error would have failed the whole validation call)
            status = 'kept'
            errors = ()
            value_status = 'validated'
            child_frame = None
        elif value_frame.outcome != 'ok':
            status = 'omitted'
            errors = value_frame.errors
            value_status = 'omitted'
            child_frame = value_frame
        else:
            status = 'kept'
            errors = ()
            value_status = 'validated'
            child_frame = value_frame
        record = OmitRecord(
            role='entry',
            locator=locator,
            path=path + (locator,),
            status=status,
            errors=errors,
            value_status=value_status,
            validated_key=validated_key,
            succeeded=succeeded,
            parent=parent,
        )
        if child_frame is not None:
            record._children = _build_children(child_frame, record.path, succeeded, record)
        entry_records.append(record)
        records.append(record)

    for child in frame.children:
        if child.role == 'key':
            if pending_key is not None:
                build_entry(pending_key, None)
            pending_key = child
        elif child.role == 'value':
            build_entry(pending_key, child)
            pending_key = None
        else:
            token = id(child.token)
            ordinal = item_ordinals.get(token, 0)
            item_ordinals[token] = ordinal + 1
            status = 'kept' if child.outcome == 'ok' else 'omitted'
            record = OmitRecord(
                role='item',
                locator=ordinal,
                path=path + (ordinal,),
                status=status,
                errors=child.errors,
                value_status='validated' if status == 'kept' else 'omitted',
                validated_key=None,
                succeeded=succeeded,
                parent=parent,
            )
            record._children = _build_children(child, record.path, succeeded, record)
            records.append(record)
    if pending_key is not None:
        build_entry(pending_key, None)

    # within a mapping, the last valid entry for a given validated key wins; earlier ones were
    # silently overwritten
    last_kept_by_key: dict[Any, OmitRecord] = {}
    for record in entry_records:
        if record.status != 'kept' or record._validated_key is None:
            continue
        try:
            previous = last_kept_by_key.get(record._validated_key)
        except TypeError:  # unhashable validated key, cannot resolve overwrites
            continue
        if previous is not None:
            previous.status = 'overwritten'
        last_kept_by_key[record._validated_key] = record

    return tuple(records)


def _find_in(records: tuple[OmitRecord, ...], locators: tuple[Any, ...]) -> OmitRecord | None:
    record: OmitRecord | None = None
    for locator in locators:
        record = next((r for r in records if r.locator == locator), None)
        if record is None:
            return None
        records = record.children
    return record


def _walk(record: OmitRecord) -> Iterator[OmitRecord]:
    yield record
    for child in record.children:
        yield from _walk(child)


class OmitReceipt:
    """An account of the items omitted by `OnErrorOmit` during a single validation call.

    Pass an instance to [`TypeAdapter.validate_python`][pydantic.TypeAdapter.validate_python] or
    [`TypeAdapter.validate_json`][pydantic.TypeAdapter.validate_json] via the `omit_receipt`
    argument; it is populated as validation proceeds, so the omissions that already happened can
    still be inspected when the call raises a [`ValidationError`][pydantic_core.ValidationError].

    The same receipt can be reused across calls: it is reset at the beginning of each call.
    """

    def __init__(self) -> None:
        self._collector: _Collector | None = None
        self._succeeded = False
        self._records: tuple[OmitRecord, ...] = ()
        self._omissions: tuple[OmitRecord, ...] = ()
        self._recoverable: tuple[OmitRecord, ...] = ()

    @property
    def succeeded(self) -> bool:
        """Whether the validation call succeeded (if `False`, no omission is recoverable)."""
        return self._succeeded

    @property
    def records(self) -> tuple[OmitRecord, ...]:
        """The top-level omittable positions visited during the call, in input order."""
        return self._records

    @property
    def items(self) -> tuple[OmitRecord, ...]:
        """The top-level records corresponding to sequence items, in input order."""
        return tuple(record for record in self._records if record._role == 'item')

    @property
    def entries(self) -> dict[Any, OmitRecord]:
        """The top-level records corresponding to mapping entries, keyed by the raw input key."""
        return {record.locator: record for record in self._records if record._role == 'entry'}

    @property
    def omissions(self) -> tuple[OmitRecord, ...]:
        """Every position that was actually omitted (including invalid mapping keys), in input order."""
        return self._omissions

    @property
    def recoverable(self) -> tuple[OmitRecord, ...]:
        """The omissions that can be re-collected individually.

        This is always empty if the validation call as a whole failed.
        """
        return self._recoverable

    def find(self, *locators: Any) -> OmitRecord | None:
        """Find a record by its original coordinates, or `None` if there is no match."""
        return _find_in(self._records, locators)

    def _begin(self) -> contextvars.Token[_Collector | None]:
        self._succeeded = False
        self._records = ()
        self._omissions = ()
        self._recoverable = ()
        self._collector = _Collector()
        return _active_collector.set(self._collector)

    def _end(self, token: contextvars.Token[_Collector | None], succeeded: bool) -> None:
        _active_collector.reset(token)
        collector = self._collector
        self._collector = None
        self._succeeded = succeeded
        self._records = () if collector is None else _build_children(collector.root, (), succeeded, None)
        self._omissions = tuple(omission for record in self._records for omission in record.omissions)
        self._recoverable = tuple(record for record in self._walk_records() if record.recoverable)

    def _walk_records(self) -> Iterator[OmitRecord]:
        for record in self._records:
            yield from _walk(record)

    def __repr__(self) -> str:
        return (
            f'OmitReceipt(succeeded={self._succeeded!r}, omissions={len(self._omissions)}, '
            f'recoverable={len(self._recoverable)})'
        )
