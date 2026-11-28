"""Experimental support for reconciling a validated manifest against warehouse zone capacity.

The reconciliation is opt-in and runs alongside a normal :class:`~pydantic.TypeAdapter`
validation. Callers describe how a fully initialized row type and the validated
`InitVar` values it was constructed with map to a storage zone and a number of cells,
together with the rows of the final manifest and the zone capacities.

Rows that enter the final manifest without being constructed by the manifest
validation (e.g. preview rows built directly inside a wrap validator, or rows left
over from a previous batch) have no construction basis and are reported at their
actual index in the final list, even when they compare equal to constructed rows.
"""

from __future__ import annotations

import contextvars
import copy
import dataclasses
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic_core import PydanticCustomError, ValidationError, core_schema

from .._internal import _config, _mock_val_ser
from ..errors import PydanticUserError
from ..plugin._schema_validator import create_schema_validator

if TYPE_CHECKING:
    from ..type_adapter import TypeAdapter

__all__ = (
    'ManifestReconciliation',
    'ManifestReconciliationEntry',
    'ManifestReconciliationResult',
)

RowT = TypeVar('RowT')


@dataclass(slots=True)
class _ReconciliationSession:
    """Per-validation bookkeeping.

    `anchors` keeps every constructed row alive for the whole validation so that
    `id()` cannot be reused by a later, unrelated construction.
    """

    ledger: dict[int, tuple[Any, ...]]
    anchors: list[Any]
    init_only_stack: list[tuple[Any, ...]]


_session: contextvars.ContextVar[_ReconciliationSession | None] = contextvars.ContextVar(
    'manifest_reconciliation_session', default=None
)


@dataclass(frozen=True, slots=True)
class ManifestReconciliationEntry:
    """The occupancy of a single row in the final manifest."""

    index: int
    zone: str
    cells: int


@dataclass(frozen=True, slots=True)
class ManifestReconciliationResult:
    """The outcome of a successful manifest reconciliation.

    Attributes:
        entries: One entry per row of the final manifest, ordered by final index.
        zone_usage: Total number of cells occupied in each zone.
    """

    entries: tuple[ManifestReconciliationEntry, ...]
    zone_usage: dict[str, int]

    @property
    def total_cells(self) -> int:
        return sum(self.zone_usage.values())


def _init_only_capture(value: Any, handler: core_schema.ValidatorFunctionWrapHandler) -> Any:
    output = handler(value)
    session = _session.get()
    if session is not None and isinstance(output, tuple):
        init_only = output[1]
        session.init_only_stack.append(() if init_only is None else tuple(init_only))
    return output


def _row_capture(value: Any, handler: core_schema.ValidatorFunctionWrapHandler) -> Any:
    instance = handler(value)
    session = _session.get()
    if session is not None and session.init_only_stack:
        session.anchors.append(instance)
        session.ledger[id(instance)] = session.init_only_stack.pop()
    return instance


def _matches_row_class(cls: Any, row_type: type[Any]) -> bool:
    if cls is row_type:
        return True
    return getattr(cls, '__origin__', None) is row_type


def _instrument_schema(node: Any, row_type: type[Any]) -> Any:
    if isinstance(node, dict):
        if node.get('type') == 'dataclass' and _matches_row_class(node.get('cls'), row_type):
            ref = node.get('ref')
            node = {key: value for key, value in node.items() if key != 'ref'}
            args_schema = dict(node['schema'])
            # Make sure the validated `InitVar` values are returned by the
            # dataclass-args validator even when the class has no `__post_init__`.
            args_schema['collect_init_only'] = True
            node['schema'] = core_schema.no_info_wrap_validator_function(_init_only_capture, args_schema)
            wrapped = core_schema.no_info_wrap_validator_function(_row_capture, node)
            # Keep the node resolvable through `definition-ref` when it is a definition.
            if ref is not None:
                wrapped['ref'] = ref
            return wrapped
        return {key: _instrument_schema(child, row_type) for key, child in node.items()}
    if isinstance(node, list):
        return [_instrument_schema(child, row_type) for child in node]
    return node


class ManifestReconciliation(Generic[RowT]):
    """Optional manifest reconciliation attached to a :class:`~pydantic.TypeAdapter` validation.

    Args:
        row_type: The Pydantic dataclass used for manifest rows. The class itself, its
            constructor hooks and its exported structure are left untouched; only the
            validator built for the adapter is instrumented.
        resolve: Called as ``resolve(row, init_only)`` with a fully initialized row and the
            tuple of validated `InitVar` values received during this construction. It returns
            the storage zone name and the number of cells occupied by the row.
        select_rows: Called with the validated manifest object. It returns the rows of the
            final manifest, in outbound order.
        capacity: Maximum number of cells available in each zone. Zones absent from this
            mapping are not capacity-checked.
    """

    def __init__(
        self,
        row_type: type[RowT],
        resolve: Callable[[RowT, tuple[Any, ...]], tuple[str, int]],
        select_rows: Callable[[Any], Sequence[RowT]],
        capacity: Mapping[str, int],
    ) -> None:
        if not dataclasses.is_dataclass(row_type):
            raise PydanticUserError(
                f'`row_type` must be a dataclass, got {row_type!r}.',
                code='manifest-reconciliation-row-type',
            )
        self.row_type = row_type
        self._resolve = resolve
        self._select_rows = select_rows
        self._capacity = dict(capacity)
        self._validators: dict[tuple[int, int], Any] = {}

    def _get_validator(self, adapter: TypeAdapter[Any]) -> Any:
        schema = adapter.core_schema
        if isinstance(schema, _mock_val_ser.MockCoreSchema):
            schema.rebuild()
            schema = adapter.core_schema
        cache_key = (id(adapter), id(schema))
        validator = self._validators.get(cache_key)
        if validator is None:
            config_wrapper = _config.ConfigWrapper(adapter._config)
            instrumented_schema = _instrument_schema(copy.deepcopy(schema), self.row_type)
            core_config = config_wrapper.core_config(None)
            validator = create_schema_validator(
                instrumented_schema,
                adapter._type,
                adapter._module_name,
                str(adapter._type),
                'TypeAdapter',
                config=core_config,
                plugin_settings=config_wrapper.plugin_settings,
                _use_prebuilt=False,
            )
            self._validators[cache_key] = validator
        return validator

    def validate(
        self,
        adapter: TypeAdapter[Any],
        method_name: str,
        object: Any,
        validation_kwargs: dict[str, Any],
    ) -> tuple[Any, ManifestReconciliationResult]:
        """Run one instrumented validation and reconcile the resulting manifest."""
        validator = self._get_validator(adapter)
        session = _ReconciliationSession(ledger={}, anchors=[], init_only_stack=[])
        token = _session.set(session)
        try:
            manifest = getattr(validator, method_name)(object, **validation_kwargs)
        finally:
            _session.reset(token)
        return manifest, self._reconcile(adapter, manifest, session.ledger)

    def _reconcile(
        self,
        adapter: TypeAdapter[Any],
        manifest: Any,
        ledger: dict[int, tuple[Any, ...]],
    ) -> ManifestReconciliationResult:
        rows = list(self._select_rows(manifest))
        entries: list[ManifestReconciliationEntry] = []
        zone_usage: dict[str, int] = {}
        missing_basis_errors: list[dict[str, Any]] = []

        for index, row in enumerate(rows):
            if id(row) not in ledger:
                missing_basis_errors.append(
                    {
                        'type': PydanticCustomError(
                            'manifest_missing_basis',
                            'Row at index {index} has no construction basis from this manifest',
                            {'index': index},
                        ),
                        'loc': (index,),
                        'input': row,
                    }
                )
                continue
            zone, cells = self._resolve(row, ledger[id(row)])
            entries.append(ManifestReconciliationEntry(index=index, zone=zone, cells=cells))
            zone_usage[zone] = zone_usage.get(zone, 0) + cells

        if missing_basis_errors:
            raise ValidationError.from_exception_data(str(adapter._type), missing_basis_errors)

        capacity_errors: list[dict[str, Any]] = []
        for zone, used in zone_usage.items():
            capacity = self._capacity.get(zone)
            if capacity is not None and used > capacity:
                capacity_errors.append(
                    {
                        'type': PydanticCustomError(
                            'manifest_capacity_exceeded',
                            "Zone '{zone}' occupies {used} cells but its capacity is {capacity}",
                            {'zone': zone, 'used': used, 'capacity': capacity},
                        ),
                        'loc': (zone,),
                        'input': used,
                    }
                )
        if capacity_errors:
            raise ValidationError.from_exception_data(str(adapter._type), capacity_errors)

        return ManifestReconciliationResult(entries=tuple(entries), zone_usage=zone_usage)
