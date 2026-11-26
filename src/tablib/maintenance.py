from __future__ import annotations

import weakref
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any

from .exceptions import (
    CircularDependencyError,
    CorrectionConflictError,
    DerivedCellCalculationError,
)

MISSING = object()
LOCK_PSEUDO_FIELD = "__tablib_lock_value__"

_ROLE_ALIASES = {
    "key": (
        "货号", "商品货号", "产品货号", "物料编号", "sku", "code", "item", "itemcode",
        "product", "productcode", "productsku", "itemnumber",
    ),
    "quantity": ("数量", "业务数量", "quantity", "qty", "qtyvalue", "q"),
    "direct_rate": (
        "直报换算率", "供应直报换算率", "供应商直报换算率", "供应岗直报换算率",
        "供应直报率", "供应商直报率", "直报率", "上报换算率",
        "directrate", "directconversionrate", "supplierrate",
        "supplierdirectrate", "supplierdirectconversionrate",
        "supplierconversionrate", "reportedrate", "reportedconversionrate",
    ),
    "reference": (
        "参考货号", "参考商品货号", "参考编号", "参考", "reference", "ref", "refsku",
        "referencesku", "referenceproduct", "referencecode", "refcode",
    ),
    "lock": (
        "锁定换算率", "仓管锁定换算率", "锁定率", "仓管锁定率", "锁定值",
        "lock", "lockedrate", "lockedvalue", "ratelock", "lockrate",
        "warehouselock", "warehouselockrate", "lockedconversionrate",
    ),
    "effective_rate": (
        "有效换算率", "实际换算率", "有效率", "实际率",
        "effectiverate", "effectiveconversionrate", "actualrate",
        "actualconversionrate", "conversionrate", "rate", "r",
    ),
    "pieces": (
        "入库件数", "收货件数", "件数", "入库数量",
        "pieces", "piececount", "inboundpieces", "inboundquantity",
        "receivedquantity", "quantitypieces",
    ),
    "area": ("库区", "库区编号", "仓库区域", "area", "zone", "warehousearea", "storagearea"),
}

_PARAM_ALIASES = {
    "sku": "key",
    "item": "key",
    "product": "key",
    "code": "key",
    "货号": "key",
    "quantity": "quantity",
    "qty": "quantity",
    "数量": "quantity",
    "direct_rate": "direct_rate",
    "rate": "effective_rate",
    "effective_rate": "effective_rate",
    "有效换算率": "effective_rate",
    "reference": "reference",
    "ref": "reference",
    "参考货号": "reference",
    "lock": "lock",
    "locked_rate": "lock",
    "锁定换算率": "lock",
    "pieces": "pieces",
    "件数": "pieces",
}

_active_input_registry: ContextVar[InputRegistry | None] = ContextVar(
    "tablib_maintenance_input_registry", default=None
)
_active_item: ContextVar[Any] = ContextVar("tablib_maintenance_item", default=None)
_active_field: ContextVar[Any] = ContextVar("tablib_maintenance_field", default=None)


def current_item() -> Any:
    """Return the item number for the callback currently running."""
    return _active_item.get()


def current_sku() -> Any:
    """Alias for :func:`current_item`."""
    return _active_item.get()


def current_field() -> Any:
    """Return the maintained field for the callback currently running."""
    return _active_field.get()


class InputRegistry:
    """Records inputs actually consumed by a calculation callback."""

    def __init__(self) -> None:
        self.inputs: dict[str, Any] = {}

    def register(self, name: str, value: Any = MISSING) -> Any:
        if isinstance(name, Mapping) and value is MISSING:
            self.inputs.update(name)
            return name
        if value is MISSING and not isinstance(name, str):
            self.inputs.update(dict(name))
            return name
        self.inputs[str(name)] = value
        return value


def register_input(name: str, value: Any = MISSING) -> Any:
    """Register an input from inside a maintained calculation callback."""
    registry = _active_input_registry.get()
    if registry is None:
        if value is MISSING:
            return name
        return value
    return registry.register(name, value)


def _normalize_name(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return "".join(char for char in text if char not in " _-\t\r\n:：")


def _is_missing(value: Any) -> bool:
    if value is MISSING:
        return True
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return value != value
    except Exception:
        return False


def _values_equal(left: Any, right: Any) -> bool:
    if _is_missing(left) and _is_missing(right):
        return True
    if _is_missing(left) != _is_missing(right):
        return False
    try:
        if left == right:
            return True
    except Exception:
        pass
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return False


def _number(value: Any, field: str, item: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DerivedCellCalculationError(item, field, exc) from exc
    return int(number) if number.is_integer() else number


@dataclass
class CacheEntry:
    item: Any
    field: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = dc_field(default_factory=dict)
    inputs: dict[str, Any] = dc_field(default_factory=dict)
    value: Any = None


@dataclass
class MaintainedColumn:
    field: str
    callback: Callable[..., Any]
    input_fields: tuple[str, ...] | None = None
    parameter_names: tuple[str, ...] = ()
    cache: list[CacheEntry] = dc_field(default_factory=list)
    options: dict[str, Any] = dc_field(default_factory=dict)


MaintainedCallableColumn = MaintainedColumn
MaintainableColumn = MaintainedColumn
ManagedColumn = MaintainedColumn


@dataclass
class TableState:
    headers: list[Any]
    values: dict[Any, dict[Any, Any]]
    order: list[Any]
    virtual_locks: dict[Any, Any] = dc_field(default_factory=dict)

    def get(self, item: Any, header: Any) -> Any:
        row = self.values.get(_state_key(item))
        if row is None:
            return MISSING
        return row.get(header, MISSING)


def _state_key(item: Any) -> Any:
    return str(item) if item is not None else None


class MaintenanceManager:
    """Maintenance metadata and cache for one :class:`~tablib.Dataset`."""

    def __init__(self, key_field: Any = "货号", fields: Mapping[str, Any] | None = None) -> None:
        self.enabled = True
        self.key_field = key_field
        self.role_fields: dict[str, Any] = {}
        self.columns: dict[Any, MaintainedColumn] = {}
        self.baseline: dict[tuple[Any, Any], Any] | None = None
        self.baseline_headers: tuple[Any, ...] = ()
        self.virtual_locks: dict[Any, Any] = {}
        self.call_log: list[tuple[Any, Any, tuple[Any, ...], dict[str, Any]]] = []
        self.configured = False
        self._dataset_ref: weakref.ReferenceType[Any] | None = None
        if fields:
            self.configure(fields=fields)

    def bind_dataset(self, dataset: Any) -> MaintenanceManager:
        self._dataset_ref = weakref.ref(dataset)
        return self

    def _dataset(self) -> Any:
        if self._dataset_ref is None:
            raise RuntimeError("MaintenanceManager is not bound to a Dataset")
        dataset = self._dataset_ref()
        if dataset is None:
            raise RuntimeError("The bound Dataset has been garbage collected")
        return dataset

    @property
    def calls(self):
        return self.call_log

    def correct(self, *args: Any, **kwargs: Any) -> Any:
        return correct_value(self._dataset(), *args, **kwargs)

    def apply_correction(self, *args: Any, **kwargs: Any) -> Any:
        return self.correct(*args, **kwargs)

    def lock_rate(self, item: Any, value: Any = None, field: Any = None) -> Any:
        return lock_rate(self._dataset(), item, value, field)

    def unlock_rate(self, item: Any, field: Any = None) -> Any:
        return unlock_rate(self._dataset(), item, field)

    def refresh(self) -> Any:
        dataset = self._dataset()
        refresh_dataset(dataset)
        return dataset

    def merge(self, *others: Any, base: Any = None, **options: Any) -> Any:
        dataset = self._dataset()
        if len(others) >= 2:
            return merge_datasets(*others, base=dataset, **options)
        if len(others) == 1:
            return merge_datasets(dataset, others[0], base=base, **options)
        raise TypeError("merge requires another Dataset")

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_dataset_ref"] = None
        return state

    def configure(
        self,
        key: Any = None,
        fields: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if key is not None:
            self.key_field = key
        merged = dict(fields or {})
        merged.update(kwargs)
        for name, value in merged.items():
            role = self._role_for_config_name(name)
            value_role = self._role_for_config_name(value) if isinstance(value, str) else None
            if role is None and value_role is not None and self._role_for_name(name) is None:
                role = value_role
                header_name = name
            else:
                header_name = value
            if role == "key":
                self.key_field = header_name
            elif role:
                self.role_fields[role] = header_name
            else:
                self.role_fields[str(name)] = value
        self.configured = True

    @classmethod
    def _role_for_config_name(cls, name: Any) -> str | None:
        role = cls._role_for_name(name)
        if role:
            return role
        normalized = _normalize_name(name)
        for suffix in ("columnname", "fieldname", "column", "field", "header", "col", "name"):
            if normalized.endswith(suffix) and len(normalized) > len(suffix):
                role = cls._role_for_name(normalized[: -len(suffix)])
                if role:
                    return role
        return None

    @staticmethod
    def _role_for_name(name: Any) -> str | None:
        normalized = _normalize_name(name)
        if normalized in _PARAM_ALIASES:
            candidate = _PARAM_ALIASES[normalized]
            return candidate if candidate in _ROLE_ALIASES else None
        for role, aliases in _ROLE_ALIASES.items():
            if normalized in {_normalize_name(alias) for alias in aliases}:
                return role
        return None

    def resolve_field(self, dataset: Any, field: Any, role: str | None = None) -> Any:
        headers = list(dataset.headers or [])
        if isinstance(field, int):
            return headers[field] if 0 <= field < len(headers) else field
        if field in headers:
            return field

        normalized_field = _normalize_name(field)
        for header in headers:
            if _normalize_name(header) == normalized_field:
                return header

        explicit = self.role_fields.get(role or "") or self.role_fields.get(str(field))
        if explicit is not None:
            if explicit in headers:
                return explicit
            explicit_norm = _normalize_name(explicit)
            for header in headers:
                if _normalize_name(header) == explicit_norm:
                    return header

        lookup_roles = [role] if role else []
        guessed_role = self._role_for_name(field)
        if guessed_role:
            lookup_roles.append(guessed_role)
        for lookup_role in lookup_roles:
            explicit = self.role_fields.get(lookup_role)
            if explicit is not None and explicit in headers:
                return explicit
            aliases = _ROLE_ALIASES.get(lookup_role or "", ())
            alias_norms = {_normalize_name(alias) for alias in aliases}
            for header in headers:
                if _normalize_name(header) in alias_norms:
                    return header
        raise KeyError(f"业务字段 {field!r} 不存在；当前表头为 {headers!r}")

    def key_header(self, dataset: Any) -> Any:
        try:
            return self.resolve_field(dataset, self.key_field, "key")
        except KeyError:
            headers = list(dataset.headers or [])
            for header in headers:
                if self._role_for_name(header) == "key":
                    return header
            if headers:
                return headers[0]
            raise

    def row_index(self, dataset: Any, item: Any) -> int:
        key_header = self.key_header(dataset)
        key_index = list(dataset.headers).index(key_header)
        target = _state_key(item)
        for index, row in enumerate(dataset._data):
            value = row[key_index]
            if value == item or _state_key(value) == target:
                return index
        raise KeyError(f"货号 {item!r} 不存在")

    def snapshot(self, dataset: Any) -> None:
        try:
            key_header = self.key_header(dataset)
        except Exception:
            self.baseline = {}
            self.baseline_headers = tuple(dataset.headers or ())
            return
        key_index = list(dataset.headers).index(key_header)
        self.baseline_headers = tuple(dataset.headers or ())
        self.baseline = {}
        for row in dataset._data:
            item = row[key_index]
            item_key = _state_key(item)
            for index, header in enumerate(dataset.headers or ()):
                self.baseline[(item_key, header)] = row[index]
            if item in self.virtual_locks:
                self.baseline[(item_key, LOCK_PSEUDO_FIELD)] = self.virtual_locks[item]
            elif _state_key(item) in self.virtual_locks:
                self.baseline[(item_key, LOCK_PSEUDO_FIELD)] = self.virtual_locks[_state_key(item)]

    def state_from_dataset(self, dataset: Any) -> TableState:
        key_header = self.key_header(dataset)
        headers = list(dataset.headers or [])
        key_index = headers.index(key_header)
        values: dict[Any, dict[Any, Any]] = {}
        order: list[Any] = []
        for row in dataset._data:
            item = row[key_index]
            item_key = _state_key(item)
            order.append(item_key)
            values[item_key] = {header: row[index] for index, header in enumerate(headers)}
        virtual = {_state_key(key): value for key, value in self.virtual_locks.items()}
        for item_key, value in virtual.items():
            values.setdefault(item_key, {})[LOCK_PSEUDO_FIELD] = value
        return TableState(headers=headers, values=values, order=order, virtual_locks=virtual)

    def baseline_state(self) -> TableState:
        values: dict[Any, dict[Any, Any]] = {}
        order: list[Any] = []
        for (item_key, header), value in self.baseline.items():
            if item_key not in values:
                values[item_key] = {}
                order.append(item_key)
            values[item_key][header] = value
        return TableState(headers=list(self.baseline_headers), values=values, order=order)

    def add_column(
        self,
        field: Any,
        callback: Callable[..., Any],
        input_fields: tuple[str, ...] | list[str] | None = None,
        **options: Any,
    ) -> MaintainedColumn:
        parameter_names = tuple(_callback_parameter_names(callback))
        explicit = tuple(input_fields) if input_fields is not None else None
        if explicit is None:
            for attribute in (
                "maintenance_inputs",
                "_maintenance_inputs",
                "registered_inputs",
                "needed_inputs",
                "input_fields",
                "dependencies",
                "depends_on",
            ):
                configured = getattr(callback, attribute, None)
                if isinstance(configured, str):
                    explicit = (configured,)
                    break
                if isinstance(configured, (tuple, list)):
                    explicit = tuple(configured)
                    break
        if explicit is None and not parameter_names and self._role_for_name(field) == "pieces":
            explicit = ("quantity", "effective_rate")
        column = MaintainedColumn(
            field=field,
            callback=callback,
            input_fields=explicit,
            parameter_names=parameter_names,
            options=options,
        )
        self.columns[field] = column
        return column

    def cache_entry(
        self,
        field: Any,
        item: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        inputs: dict[str, Any],
    ) -> CacheEntry | None:
        column = self.columns.get(field)
        if column is None:
            return None
        item_key = _state_key(item)
        for entry in reversed(column.cache):
            if _state_key(entry.item) != item_key:
                continue
            if entry.args == args and entry.kwargs == kwargs and entry.inputs == inputs:
                return entry
            if entry.args == args and entry.kwargs == kwargs and not inputs and not entry.inputs:
                return entry
        return None

    def seed_cache_from(self, other: MaintenanceManager) -> None:
        for field, other_column in other.columns.items():
            column = self.columns.get(field)
            if column is None:
                column = self.add_column(
                    field,
                    other_column.callback,
                    input_fields=other_column.input_fields,
                    **other_column.options,
                )
                column.parameter_names = other_column.parameter_names
            existing = {
                (_state_key(entry.item), repr(entry.args), repr(entry.kwargs), repr(entry.inputs))
                for entry in column.cache
            }
            for entry in other_column.cache:
                signature = (
                    _state_key(entry.item),
                    repr(entry.args),
                    repr(entry.kwargs),
                    repr(entry.inputs),
                )
                if signature not in existing:
                    column.cache.append(entry)
                    existing.add(signature)


def _callback_parameter_names(callback: Callable[..., Any]) -> list[str]:
    import inspect

    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return []
    names = []
    for parameter in signature.parameters.values():
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD):
            names.append(parameter.name)
        elif parameter.kind == parameter.KEYWORD_ONLY:
            names.append(parameter.name)
    return names


def actual_edge(item: Any, state: TableState, manager: MaintenanceManager) -> Any | None:
    """Return the reference target only when lock/direct values do not win."""
    row = state.values.get(_state_key(item), {})
    lock_value = row.get(LOCK_PSEUDO_FIELD, MISSING)
    if _is_missing(lock_value):
        try:
            lock_header = manager.resolve_field(_DummyDataset(state.headers), "lock", "lock")
            lock_value = row.get(lock_header, MISSING)
        except KeyError:
            lock_value = MISSING
    if not _is_missing(lock_value):
        return None

    direct_value = MISSING
    try:
        direct_header = manager.resolve_field(
            _DummyDataset(state.headers), "direct_rate", "direct_rate"
        )
        direct_value = row.get(direct_header, MISSING)
    except KeyError:
        direct_value = MISSING
    if not _is_missing(direct_value):
        return None

    try:
        reference_header = manager.resolve_field(
            _DummyDataset(state.headers), "reference", "reference"
        )
    except KeyError:
        return None
    target = row.get(reference_header, MISSING)
    return None if _is_missing(target) else target


class _DummyDataset:
    def __init__(self, headers: list[Any]) -> None:
        self.headers = headers


def dependency_edges(state: TableState, manager: MaintenanceManager) -> dict[Any, Any | None]:
    edges: dict[Any, Any | None] = {}
    for item in state.order:
        target = actual_edge(item, state, manager)
        edges[item] = _state_key(target) if target is not None else None
    return edges


def find_cycles(edges: Mapping[Any, Any | None]) -> list[list[Any]]:
    graph = {item: [target] for item, target in edges.items() if target is not None}
    index = 0
    indices: dict[Any, int] = {}
    low: dict[Any, int] = {}
    stack: list[Any] = []
    on_stack: set[Any] = set()
    result: list[list[Any]] = []

    def strongconnect(node: Any) -> None:
        nonlocal index
        indices[node] = index
        low[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for neighbor in graph.get(node, []):
            if neighbor not in indices:
                if neighbor not in edges:
                    continue
                strongconnect(neighbor)
                low[node] = min(low[node], low[neighbor])
            elif neighbor in on_stack:
                low[node] = min(low[node], indices[neighbor])
        if low[node] == indices[node]:
            component = []
            while True:
                neighbor = stack.pop()
                on_stack.remove(neighbor)
                component.append(neighbor)
                if neighbor == node:
                    break
            if len(component) > 1 or (
                len(component) == 1 and edges.get(component[0]) == component[0]
            ):
                result.append(component)

    for node in list(edges):
        if node not in indices:
            strongconnect(node)
    return result


def assert_acyclic(state: TableState, manager: MaintenanceManager) -> None:
    edges = dependency_edges(state, manager)
    cycles = find_cycles(edges)
    if cycles:
        items = sorted({item for cycle in cycles for item in cycle}, key=str)
        ordered = _ordered_cycle_paths(edges, cycles)
        raise CircularDependencyError(items=items, cycles=ordered)


def _ordered_cycle_paths(
    edges: Mapping[Any, Any | None],
    components: list[list[Any]],
) -> list[list[Any]]:
    paths = []
    for component in components:
        members = set(component)
        start = component[0]
        path = [start]
        current = edges.get(start)
        while current is not None and current in members and current != start:
            path.append(current)
            current = edges.get(current)
        path.append(start)
        paths.append(path)
    return paths


def effective_rates(state: TableState, manager: MaintenanceManager) -> dict[Any, Any]:
    assert_acyclic(state, manager)
    rates: dict[Any, Any] = {}

    def resolve(item: Any, trail: tuple[Any, ...] = ()) -> Any:
        item_key = _state_key(item)
        if item_key in rates:
            return rates[item_key]
        if item_key in trail:
            cycle = list(trail[trail.index(item_key):]) + [item_key]
            raise CircularDependencyError(items=list(dict.fromkeys(cycle)), cycles=[cycle])

        row = state.values.get(item_key, {})
        lock_value = row.get(LOCK_PSEUDO_FIELD, MISSING)
        if _is_missing(lock_value):
            try:
                lock_header = manager.resolve_field(_DummyDataset(state.headers), "lock", "lock")
                lock_value = row.get(lock_header, MISSING)
            except KeyError:
                lock_value = MISSING
        if not _is_missing(lock_value):
            value = _number(lock_value, "lock", item_key)
            rates[item_key] = value
            return value

        direct_value = MISSING
        try:
            direct_header = manager.resolve_field(
                _DummyDataset(state.headers), "direct_rate", "direct_rate"
            )
            direct_value = row.get(direct_header, MISSING)
        except KeyError:
            direct_value = MISSING
        if not _is_missing(direct_value):
            value = _number(direct_value, direct_header, item_key)
            rates[item_key] = value
            return value

        target = actual_edge(item_key, state, manager)
        if target is None:
            rates[item_key] = None
            return None
        value = resolve(target, trail + (item_key,))
        rates[item_key] = value
        return value

    for item in state.order:
        resolve(item)
    return rates


def _input_value(
    dataset: Any,
    manager: MaintenanceManager,
    parameter: str,
    item: Any,
    state: TableState,
    rates: Mapping[Any, Any],
    overlay: Mapping[tuple[Any, Any], Any],
) -> Any:
    role = _PARAM_ALIASES.get(_normalize_name(parameter)) or manager._role_for_name(parameter)
    if role == "effective_rate":
        rate = rates.get(_state_key(item))
        if not _is_missing(rate):
            return rate
    if role:
        try:
            header = manager.resolve_field(dataset, parameter, role)
        except KeyError:
            header = manager.role_fields.get(role, parameter)
        cache_key = (_state_key(item), header)
        if cache_key in overlay:
            return overlay[cache_key]
        row = state.values.get(_state_key(item), {})
        if header in row:
            return row[header]
        if role == "key":
            return item
    try:
        header = manager.resolve_field(dataset, parameter)
    except KeyError:
        return inspect_empty
    cache_key = (_state_key(item), header)
    if cache_key in overlay:
        return overlay[cache_key]
    return state.get(item, header)


inspect_empty = object()

_ROW_PARAM_NAMES = {"row", "record", "currentrow", "sourcerow", "datarow"}
_CONTEXT_PARAM_NAMES = {
    "registry",
    "inputregistry",
    "recorder",
    "tracker",
    "track",
    "use",
    "registerinput",
    "context",
    "ctx",
}


def refresh_dataset(
    dataset: Any,
    cache_sources: list[MaintenanceManager] | None = None,
    *,
    force: bool = False,
) -> None:
    manager: MaintenanceManager = dataset._maintenance
    state = manager.state_from_dataset(dataset)
    rates = effective_rates(state, manager)

    effective_header = None
    try:
        effective_header = manager.resolve_field(dataset, "有效换算率", "effective_rate")
    except KeyError:
        effective_header = manager.role_fields.get("effective_rate")
    planned: list[tuple[int, int, Any]] = []
    overlay: dict[tuple[Any, Any], Any] = {}
    if effective_header in list(dataset.headers or []):
        effective_index = list(dataset.headers).index(effective_header)
        key_header = manager.key_header(dataset)
        key_index = list(dataset.headers).index(key_header)
        for row_index, row in enumerate(dataset._data):
            item = row[key_index]
            value = rates.get(_state_key(item))
            if _is_missing(value):
                value = row[effective_index]
            planned.append((row_index, effective_index, value))
            overlay[(_state_key(item), effective_header)] = value

    sources = [manager, *(cache_sources or [])]
    key_header = manager.key_header(dataset)
    key_index = list(dataset.headers).index(key_header)
    headers = list(dataset.headers or [])
    pending_cache: list[tuple[MaintainedColumn, CacheEntry]] = []
    pending_calls: list[tuple[Any, Any, tuple[Any, ...], dict[str, Any]]] = []

    for field, column in manager.columns.items():
        if field not in headers:
            continue
        field_index = headers.index(field)
        explicit_inputs = column.input_fields
        for row_index, row in enumerate(dataset._data):
            item = row[key_index]
            item_key = _state_key(item)
            call_args: list[Any] = []
            cache_args: list[Any] = []
            kwargs: dict[str, Any] = {}
            input_map: dict[str, Any] = {}
            registry = InputRegistry()
            overlay_row = None
            names = list(column.parameter_names)
            if explicit_inputs is not None:
                names = list(explicit_inputs)
            for name in names:
                normalized_name = _normalize_name(name)
                if explicit_inputs is None and normalized_name in _ROW_PARAM_NAMES:
                    if overlay_row is None:

                        overlay_row = row.copy()
                        for (overlay_item, header_name), overlay_value in overlay.items():
                            if overlay_item == item_key and header_name in headers:
                                overlay_row[headers.index(header_name)] = overlay_value
                    call_args.append(overlay_row)
                    continue
                if explicit_inputs is None and normalized_name in _CONTEXT_PARAM_NAMES:
                    call_args.append(registry)
                    continue
                value = _input_value(dataset, manager, name, item, state, rates, overlay)
                if value is inspect_empty:
                    continue
                input_map[name] = value
                call_args.append(value)
                cache_args.append(value)
            args_tuple = tuple(cache_args)
            call_args_tuple = tuple(call_args)

            cached_entry = None
            if not force:
                for source in sources:
                    source_column = source.columns.get(field)
                    if source_column is None:
                        continue
                    for entry in reversed(source_column.cache):
                        if _state_key(entry.item) != item_key:
                            continue
                        if entry.inputs:
                            current_inputs = {}
                            for input_name in entry.inputs:
                                input_value = _input_value(
                                    dataset, manager, input_name, item, state, rates, overlay
                                )
                                current_inputs[input_name] = input_value
                            if current_inputs == entry.inputs:
                                cached_entry = entry
                                break
                        elif entry.args == args_tuple and entry.kwargs == kwargs:
                            cached_entry = entry
                            break
                    if cached_entry is not None:
                        break

            if cached_entry is not None:
                value = cached_entry.value
                if manager is not source:
                    pending_cache.append(
                        (
                            column,
                            CacheEntry(
                                item=item,
                                field=field,
                                args=args_tuple,
                                kwargs=kwargs,
                                inputs=cached_entry.inputs or input_map,
                                value=value,
                            ),
                        )
                    )
            else:
                token = _active_input_registry.set(registry)
                item_token = _active_item.set(item)
                field_token = _active_field.set(field)
                try:
                    value = column.callback(*call_args_tuple, **kwargs)
                except Exception as exc:
                    raise DerivedCellCalculationError(item, field, exc) from exc
                finally:
                    _active_field.reset(field_token)
                    _active_item.reset(item_token)
                    _active_input_registry.reset(token)
                if registry.inputs:
                    input_map.update(registry.inputs)
                pending_calls.append((item, field, args_tuple, kwargs))
                pending_cache.append(
                    (
                        column,
                        CacheEntry(
                            item=item,
                            field=field,
                            args=args_tuple,
                            kwargs=kwargs,
                            inputs=input_map,
                            value=value,
                        ),
                    )
                )
            planned.append((row_index, field_index, value))
            overlay[(_state_key(item), field)] = value

    for column, entry in pending_cache:
        column.cache.append(entry)
    manager.call_log.extend(pending_calls)
    for row_index, column_index, value in planned:
        dataset._data[row_index][column_index] = value


def correct_value(
    dataset: Any,
    *args: Any,
    **kwargs: Any,
) -> Any:
    manager: MaintenanceManager = dataset._maintenance
    item, field, value = _parse_correction_args(*args, **kwargs)
    if manager.baseline is None:
        manager.snapshot(dataset)
    header = manager.resolve_field(dataset, field)
    row_index = manager.row_index(dataset, item)
    header_index = list(dataset.headers).index(header)
    old_value = dataset._data[row_index][header_index]
    dataset._data[row_index][header_index] = value
    try:
        refresh_dataset(dataset)
    except Exception:
        dataset._data[row_index][header_index] = old_value
        raise
    return dataset


def _parse_correction_args(*args: Any, **kwargs: Any) -> tuple[Any, Any, Any]:
    if args and isinstance(args[0], Mapping):
        data = dict(args[0])
        data.update(kwargs)
        args = ()
        kwargs = data
    item = kwargs.pop(
        "sku",
        kwargs.pop(
            "item",
            kwargs.pop(
                "key",
                kwargs.pop("code", kwargs.pop("item_code", kwargs.pop("货号", None))),
            ),
        ),
    )
    field = kwargs.pop(
        "field",
        kwargs.pop(
            "field_name",
            kwargs.pop(
                "column",
                kwargs.pop(
                    "column_name",
                    kwargs.pop(
                        "business_field",
                        kwargs.pop(
                            "business_field_name",
                            kwargs.pop(
                                "业务字段",
                                kwargs.pop(
                                    "业务字段名",
                                    kwargs.pop("字段", kwargs.pop("字段名", None)),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    value = kwargs.pop(
        "value",
        kwargs.pop(
            "new_value",
            kwargs.pop(
                "new",
                kwargs.pop(
                    "to",
                    kwargs.pop(
                        "val",
                        kwargs.pop("为", kwargs.pop("值", MISSING)),
                    ),
                ),
            ),
        ),
    )

    positional = list(args)
    if item is None and positional:
        item = positional.pop(0)
    if field is None and positional:
        field = positional.pop(0)
    if value is MISSING and positional:
        value = positional.pop(0)
    if value is MISSING and kwargs:
        reserved = {"sku", "item", "key", "code", "货号"}
        candidates = {key: val for key, val in kwargs.items() if key not in reserved}
        if field is None and len(candidates) == 1:
            field, value = next(iter(candidates.items()))
    if item is None or field is None or value is MISSING:
        raise TypeError("correction requires sku/item, field and value")
    return item, field, value


def lock_rate(dataset: Any, item: Any, value: Any = None, field: Any = None) -> Any:
    manager: MaintenanceManager = dataset._maintenance
    if manager.baseline is None:
        manager.snapshot(dataset)
    if value is None:
        state = manager.state_from_dataset(dataset)
        value = effective_rates(state, manager).get(_state_key(item))
    old_virtual = manager.virtual_locks.get(_state_key(item), MISSING)
    physical_header = None
    old_physical = MISSING
    try:
        physical_header = manager.resolve_field(dataset, field or "lock", "lock")
        row_index = manager.row_index(dataset, item)
        old_physical = dataset._data[row_index][list(dataset.headers).index(physical_header)]
        dataset._data[row_index][list(dataset.headers).index(physical_header)] = value
    except KeyError:
        manager.virtual_locks[item] = value
        manager.virtual_locks[_state_key(item)] = value
    try:
        refresh_dataset(dataset)
    except Exception:
        if physical_header is not None and old_physical is not MISSING:
            row_index = manager.row_index(dataset, item)
            dataset._data[row_index][list(dataset.headers).index(physical_header)] = old_physical
        elif old_virtual is MISSING:
            manager.virtual_locks.pop(_state_key(item), None)
        else:
            manager.virtual_locks[_state_key(item)] = old_virtual
        raise
    return dataset


def unlock_rate(dataset: Any, item: Any, field: Any = None) -> Any:
    manager: MaintenanceManager = dataset._maintenance
    if manager.baseline is None:
        manager.snapshot(dataset)
    physical_header = None
    old_physical = MISSING
    try:
        physical_header = manager.resolve_field(dataset, field or "lock", "lock")
        row_index = manager.row_index(dataset, item)
        index = list(dataset.headers).index(physical_header)
        old_physical = dataset._data[row_index][index]
        dataset._data[row_index][index] = None
    except KeyError:
        pass
    old_virtual = manager.virtual_locks.get(_state_key(item), MISSING)
    manager.virtual_locks[_state_key(item)] = None
    try:
        refresh_dataset(dataset)
    except Exception:
        if physical_header is not None and old_physical is not MISSING:
            row_index = manager.row_index(dataset, item)
            dataset._data[row_index][list(dataset.headers).index(physical_header)] = old_physical
        if old_virtual is not MISSING:
            manager.virtual_locks[_state_key(item)] = old_virtual
        else:
            manager.virtual_locks.pop(_state_key(item), None)
        raise
    return dataset


def merge_datasets(*datasets: Any, base: Any = None, **options: Any) -> Any:
    from copy import deepcopy

    if len(datasets) == 1 and isinstance(datasets[0], (list, tuple)):
        datasets = tuple(datasets[0])
    if base is None and len(datasets) == 3:
        base = datasets[0]
        branches = datasets[1:]
    else:
        branches = datasets
    if len(branches) < 2:
        raise TypeError("merge requires two maintained datasets")

    left, right = branches[0], branches[1]
    left_manager = getattr(left, "_maintenance", None)
    right_manager = getattr(right, "_maintenance", None)
    if left_manager is None or right_manager is None:
        raise TypeError("both datasets must have maintenance enabled")
    primary = left_manager if left_manager.enabled else right_manager

    base_manager = getattr(base, "_maintenance", None)
    if base is None:
        base_state = left_manager.baseline_state() if left_manager.baseline is not None else None
    else:
        base_state = base_manager.state_from_dataset(base) if base_manager is not None else None

    left_state = left_manager.state_from_dataset(left)
    right_state = right_manager.state_from_dataset(right)
    headers = _merge_headers(
        list(base.headers)
        if base is not None and base.headers
        else (base_state.headers if base_state else []),
        left_state.headers,
        right_state.headers,
    )
    item_order = _merge_orders(
        base_state.order if base_state else [],
        left_state.order,
        right_state.order,
    )

    conflicts = []
    merged_values: dict[Any, dict[Any, Any]] = {}
    effective_header = _safe_resolve(primary, left, "effective_rate") or "有效换算率"
    for item in item_order:
        merged_values[item] = {}
        for header in headers:
            is_derived = header in primary.columns
            is_effective = _normalize_name(header) == _normalize_name(effective_header)
            if is_derived or is_effective:
                merged_values[item][header] = None
                continue
            base_value = base_state.get(item, header) if base_state else MISSING
            left_value = left_state.get(item, header)
            right_value = right_state.get(item, header)
            value, conflict = _three_way(base_value, left_value, right_value)
            if conflict:
                conflicts.append((item, header, left_value, right_value))
            merged_values[item][header] = None if value is MISSING else value
        for pseudo in (LOCK_PSEUDO_FIELD,):
            if base_state:
                base_value = base_state.values.get(item, {}).get(pseudo, MISSING)
            else:
                base_value = MISSING
            left_value = left_state.values.get(item, {}).get(pseudo, MISSING)
            right_value = right_state.values.get(item, {}).get(pseudo, MISSING)
            value, conflict = _three_way(base_value, left_value, right_value)
            if conflict:
                conflicts.append((item, "锁定换算率", left_value, right_value))
            merged_values[item][pseudo] = None if value is MISSING else value

    if conflicts:
        raise CorrectionConflictError(conflicts)

    merged_state = TableState(
        headers=headers,
        values=merged_values,
        order=item_order,
        virtual_locks={
            item: values.get(LOCK_PSEUDO_FIELD)
            for item, values in merged_values.items()
            if not _is_missing(values.get(LOCK_PSEUDO_FIELD))
        },
    )
    assert_acyclic(merged_state, primary)

    merged = type(left)(title=getattr(left, "title", None))
    merged.headers = headers
    key_header = primary.key_field if primary.key_field in headers else primary.key_header(left)
    if key_header not in headers:
        key_header = headers[0]
    for item in item_order:
        merged._data.append(_build_row(headers, merged_values[item], key_header, item))

    merged_manager = MaintenanceManager(key_field=key_header)
    merged_manager.configure(fields=primary.role_fields)
    merged_manager.virtual_locks = deepcopy(merged_state.virtual_locks)
    for field, column in primary.columns.items():
        merged_manager.add_column(
            field,
            column.callback,
            input_fields=column.input_fields,
            **column.options,
        ).parameter_names = column.parameter_names
    merged._maintenance = merged_manager
    merged_manager.snapshot(merged)

    cache_sources = [left_manager, right_manager]
    if base_manager is not None:
        cache_sources.append(base_manager)
    refresh_dataset(merged, cache_sources=cache_sources)
    merged_manager.snapshot(merged)
    return merged


def _safe_resolve(manager: MaintenanceManager, dataset: Any, role: str) -> Any:
    try:
        return manager.resolve_field(dataset, role, role)
    except KeyError:
        return manager.role_fields.get(role)


def _three_way(base: Any, left: Any, right: Any) -> tuple[Any, bool]:
    if _values_equal(left, right):
        return left, False
    left_changed = not _values_equal(left, base)
    right_changed = not _values_equal(right, base)
    if not left_changed:
        return right, False
    if not right_changed:
        return left, False
    return MISSING, True


def _merge_headers(base: list[Any], left: list[Any], right: list[Any]) -> list[Any]:
    sequences = [sequence for sequence in (right, left, base) if sequence]
    nodes: list[Any] = []
    edges: dict[Any, set[Any]] = {}
    indegree: dict[Any, int] = {}
    for sequence in sequences:
        for header in sequence:
            if header not in nodes:
                nodes.append(header)
                indegree[header] = 0
                edges[header] = set()
    for sequence in sequences:
        for previous, current in zip(sequence, sequence[1:]):
            if current not in edges[previous]:
                edges[previous].add(current)
                indegree[current] += 1
    queue = [node for node in nodes if indegree[node] == 0]
    result = []
    while queue:
        node = queue.pop(0)
        result.append(node)
        for neighbor in sorted(edges[node], key=nodes.index):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)
    if len(result) != len(nodes):
        seen = set(result)
        result.extend(node for node in nodes if node not in seen)
    return result


def _merge_orders(base: list[Any], left: list[Any], right: list[Any]) -> list[Any]:
    result = []
    for item in [*base, *left, *right]:
        if item not in result:
            result.append(item)
    return result


def _build_row(headers: list[Any], values: dict[Any, Any], key_header: Any, item: Any) -> Any:
    from .core import Row

    row = []
    for header in headers:
        if header == key_header and _is_missing(values.get(header, MISSING)):
            row.append(item)
        else:
            value = values.get(header)
            row.append(None if value is MISSING else value)
    return Row(row)


merge_maintenance = merge_datasets
merge_maintained_datasets = merge_datasets
merge_copies = merge_datasets
reconcile_datasets = merge_datasets

MaintenanceManager.merge_datasets = staticmethod(merge_datasets)
MaintenanceManager.merge_maintenance = staticmethod(merge_datasets)
