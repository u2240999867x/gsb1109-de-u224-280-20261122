"""Compile unit conversion tables from msgspec type metadata.

The device contract declares physical units via ``Annotated`` metadata, e.g.
``Annotated[Measurement, Meta(extra={"unit": "mV"})]``. The same type may be
referenced from several fields, each carrying its own unit. This module walks
the introspection tree produced by `msgspec.inspect.type_info`, collects every
occurrence that declares a unit, and emits a conversion table describing how to
normalize each occurrence.

Unit rules are scoped to an *occurrence*. An override supplied for a path only
applies while the compiler is visiting that exact occurrence; stepping back to
a parent field (or into a sibling) restores the unit declared by the contract.
The walk is depth-first with an on-stack cycle guard rather than a global
visited set, so the same type is compiled independently for every path through
which it is reached. Recursive references re-enter the same component, and
because rules are attached to the metadata nodes themselves, a rule takes
effect again on every re-entry while a sibling occurrence keeps its own scope.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

import msgspec

from . import inspect as mi

__all__ = (
    "UnitColumn",
    "UnitTable",
    "RuleSource",
    "CONTRACT",
    "OVERRIDE",
    "DEFAULT_UNIT_KEY",
    "compile_unit_table",
)

#: Rule source for units declared on the type via ``Annotated`` metadata.
CONTRACT: Final = "contract"
#: Rule source for units supplied by the caller at an occurrence path.
OVERRIDE: Final = "override"

RuleSource = str

#: The key read from ``Meta(extra={...})`` when no explicit ``unit_key`` is
#: given.
DEFAULT_UNIT_KEY: Final = "unit"


class UnitColumn(msgspec.Struct):
    """A single entry in a compiled unit conversion table.

    Each column describes one unit-carrying occurrence within the compiled
    type. Recursive references repeat the rule of the component they re-enter,
    so the finite set of columns fully describes an unbounded message.

    Parameters
    ----------
    path: str
        The occurrence path, rooted at ``"$"`` and using field encode names
        (e.g. ``"$.calibration"``).
    cls: type or None
        The concrete type attached to the occurrence, if any. Leaf scalar types
        like ``float`` report ``None``.
    field: str or None
        The innermost field name on the path, if the occurrence is a field.
    unit: str
        The unit actually applied to this occurrence.
    contract_unit: str or None
        The unit declared by the type contract, or ``None`` if the contract
        declares no unit there.
    rule: str
        Where the applied unit came from: ``"contract"`` or ``"override"``.
    factor: float
        Multiplicative factor converting a raw value into the target unit.
    """

    path: str
    cls: type | None
    field: str | None
    unit: str
    contract_unit: str | None
    rule: RuleSource
    factor: float


class UnitTable:
    """The output of compiling a type for unit conversion.

    Parameters
    ----------
    root: type
        The type the table was compiled from.
    columns: tuple[UnitColumn, ...]
        One entry per unit-carrying occurrence, ordered by traversal (parent
        occurrences before their children).
    overrides: dict[str, str]
        The normalized occurrence-path overrides used for this compilation.
    target_unit: str or None
        The common target unit values are normalized into, if any.
    """

    __slots__ = (
        "root",
        "columns",
        "overrides",
        "target_unit",
        "_node_rules",
        "_override_paths",
        "_root_info",
    )

    def __init__(
        self,
        root: Any,
        columns: tuple[UnitColumn, ...],
        overrides: dict[str, str],
        target_unit: str | None,
        node_rules: dict[int, UnitColumn],
        override_paths: dict[str, UnitColumn],
        root_info: mi.Type,
    ):
        self.root = root
        self.columns = columns
        self.overrides = overrides
        self.target_unit = target_unit
        # Rules keyed by metadata node identity, plus the introspected root:
        # they let `convert` guide a real row through the (possibly cyclic)
        # type tree without any path bookkeeping.
        self._node_rules = node_rules
        # Overrides targeting occurrences that have no `Metadata` wrapper can
        # only be resolved by occurrence path.
        self._override_paths = override_paths
        self._root_info = root_info

    def __repr__(self) -> str:
        root = getattr(self.root, "__name__", repr(self.root))
        return f"UnitTable(root={root}, columns={len(self.columns)})"

    def by_path(self) -> dict[str, UnitColumn]:
        """Return the columns keyed by their occurrence path."""
        return {column.path: column for column in self.columns}

    def convert(self, row: Any) -> Any:
        """Apply this table's factors to a decoded message ``row``.

        The row is walked alongside the compiled type tree. A column's factor
        scales every numeric leaf beneath its occurrence; when a nested
        occurrence declares its own unit, that rule takes over for its subtree
        and the outer rule is restored on exit. Recursive references re-enter
        the same component, so its rule applies at every depth. Non-numeric
        values are returned unchanged, and the container types of structs,
        dataclasses, named tuples, mappings, and sequences are preserved.
        """
        return _RowConverter(self._node_rules, self._override_paths).convert(
            row, self._root_info, "$", None
        )


class _UnitCompiler:
    def __init__(
        self,
        root: Any,
        overrides: Mapping[str, str],
        unit_key: str,
        target_unit: str | None,
        factors: Mapping[str, float],
    ):
        self.root = root
        self.unit_key = unit_key
        self.target_unit = target_unit
        self.factors = dict(factors)
        self.overrides = dict(overrides)
        self.columns: list[UnitColumn] = []
        # Override paths that actually resolved to an occurrence during the
        # walk; anything left over at the end is a caller mistake.
        self._consumed_override_paths: set[str] = set()
        # Rules keyed by the metadata node's identity. Distinct occurrences get
        # distinct Metadata nodes even when they wrap the same concrete type,
        # while recursive re-entry passes through the same node again.
        self.node_rules: dict[int, UnitColumn] = {}
        self.override_paths: dict[str, UnitColumn] = {}
        # On-stack guard: only breaks true recursion cycles. A type reached via
        # a different path is still visited afresh.
        self._on_stack: set[int] = set()

    def compile(self, info: mi.Type) -> UnitTable:
        self._visit(info, "$", None)
        unmatched = set(self.overrides) - self._consumed_override_paths
        if unmatched:
            raise KeyError(
                "override paths do not match any occurrence: "
                + ", ".join(sorted(unmatched))
            )
        return UnitTable(
            root=self.root,
            columns=tuple(self.columns),
            overrides=dict(self.overrides),
            target_unit=self.target_unit,
            node_rules=dict(self.node_rules),
            override_paths=dict(self.override_paths),
            root_info=info,
        )

    def _visit(self, node: mi.Type, path: str, field: str | None) -> None:
        node_id = id(node)
        if node_id in self._on_stack:
            # Recursive edge: re-entering this component repeats its own rules,
            # which are already recorded against its metadata nodes.
            return
        self._on_stack.add(node_id)
        try:
            self._visit_unchecked(node, path, field)
        finally:
            self._on_stack.discard(node_id)

    def _visit_unchecked(self, node: mi.Type, path: str, field: str | None) -> None:
        # Peel every `Metadata` wrapper: each wrapper may declare its own unit,
        # and an override targets the whole occurrence at this path.
        contract_unit = None
        current = node
        while isinstance(current, mi.Metadata):
            if current.extra is not None:
                unit = current.extra.get(self.unit_key)
                if unit is not None:
                    contract_unit = unit
            current = current.type

        override_unit = self.overrides.get(path)
        if override_unit is not None:
            self._consumed_override_paths.add(path)
            applied_unit = override_unit
            rule = OVERRIDE
        elif contract_unit is not None:
            applied_unit = contract_unit
            rule = CONTRACT
        else:
            applied_unit = None
            rule = None

        if applied_unit is not None:
            column = UnitColumn(
                path=path,
                cls=getattr(current, "cls", None),
                field=field,
                unit=applied_unit,
                contract_unit=contract_unit,
                rule=rule,
                factor=self._factor_for(contract_unit, applied_unit),
            )
            self.columns.append(column)
            if isinstance(node, mi.Metadata):
                self.node_rules[id(node)] = column
            else:
                # No wrapper to attach the rule to; resolve it by path when a
                # row is converted.
                self.override_paths[path] = column

        self._descend(current, path)

    def _factor_for(self, contract_unit: str | None, applied_unit: str) -> float:
        if applied_unit in self.factors:
            return self.factors[applied_unit]
        if contract_unit is not None and contract_unit in self.factors:
            # An override without its own factor entry: fall back to the factor
            # registered for the contract unit rather than silently producing
            # an identity conversion.
            return self.factors[contract_unit]
        return 1.0

    def _descend(self, node: mi.Type, path: str) -> None:
        if isinstance(
            node,
            (
                mi.StructType,
                mi.TypedDictType,
                mi.DataclassType,
                mi.NamedTupleType,
            ),
        ):
            for subfield in node.fields:
                self._visit(
                    subfield.type,
                    f"{path}.{subfield.encode_name}",
                    subfield.encode_name,
                )
        elif isinstance(node, mi.CollectionType):
            self._visit(node.item_type, f"{path}[*]", None)
        elif isinstance(node, mi.TupleType):
            for index, item_type in enumerate(node.item_types):
                self._visit(item_type, f"{path}[{index}]", None)
        elif isinstance(node, (mi.DictType, mi.FrozenDictType)):
            self._visit(node.key_type, f"{path}{{key}}", None)
            self._visit(node.value_type, f"{path}{{value}}", None)
        elif isinstance(node, mi.UnionType):
            for subtype in node.types:
                self._visit(subtype, path, None)
        # Leaf and custom types have nothing to descend into.


class _RowConverter:
    """Walks a decoded row together with the compiled type tree.

    Rules attached to ``Metadata`` nodes resolve by node identity, so recursive
    re-entry reactivates the component's own rules and the walk terminates
    simply because rows are finite. Overrides on occurrences lacking a
    ``Metadata`` wrapper resolve through the parallel occurrence path.
    """

    def __init__(
        self,
        node_rules: dict[int, UnitColumn],
        override_paths: dict[str, UnitColumn],
    ):
        self.node_rules = node_rules
        self.override_paths = override_paths

    def convert(
        self, value: Any, node: mi.Type, path: str, factor: float | None
    ) -> Any:
        while isinstance(node, mi.Metadata):
            column = self.node_rules.get(id(node))
            if column is not None:
                factor = column.factor
            node = node.type

        path_column = self.override_paths.get(path)
        if path_column is not None:
            factor = path_column.factor

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value * factor if factor is not None else value
        if value is None:
            return None

        if isinstance(node, mi.UnionType):
            selected = self._select_union_member(node, value)
            if selected is None:
                return value
            return self.convert(value, selected, path, factor)

        if isinstance(
            node,
            (
                mi.StructType,
                mi.TypedDictType,
                mi.DataclassType,
                mi.NamedTupleType,
            ),
        ):
            return self._convert_object(value, node, path, factor)
        if isinstance(node, mi.CollectionType):
            return self._convert_sequence(value, node.item_type, f"{path}[*]", factor)
        if isinstance(node, mi.TupleType):
            if not isinstance(value, (list, tuple)):
                return value
            return tuple(
                self.convert(item, item_node, f"{path}[{index}]", factor)
                for index, (item, item_node) in enumerate(zip(value, node.item_types))
            )
        if isinstance(node, (mi.DictType, mi.FrozenDictType)):
            return self._convert_mapping(value, node, path, factor)
        return value

    def _convert_object(
        self, value: Any, node: mi.Type, path: str, factor: float | None
    ) -> Any:
        if isinstance(value, Mapping):
            converted = {}
            for subfield in node.fields:
                if subfield.encode_name in value:
                    converted[subfield.encode_name] = self.convert(
                        value[subfield.encode_name],
                        subfield.type,
                        f"{path}.{subfield.encode_name}",
                        factor,
                    )
            return converted

        if not hasattr(value, "__class__"):
            return value

        field_types = {subfield.name: subfield.type for subfield in node.fields}
        field_paths = {
            subfield.name: f"{path}.{subfield.encode_name}" for subfield in node.fields
        }
        if hasattr(value, "__struct_fields__"):
            names = value.__struct_fields__
        elif hasattr(value, "__dataclass_fields__"):
            names = tuple(value.__dataclass_fields__)
        elif hasattr(value, "__attrs_attrs__"):
            names = tuple(attribute.name for attribute in value.__attrs_attrs__)
        elif isinstance(value, tuple) and hasattr(value, "_fields"):
            names = value._fields
        else:
            return value

        return type(value)(
            **{
                name: self.convert(
                    getattr(value, name), field_types[name], field_paths[name], factor
                )
                for name in names
                if name in field_types
            }
        )

    def _convert_sequence(
        self, value: Any, item_node: mi.Type, item_path: str, factor: float | None
    ) -> Any:
        if not isinstance(value, (list, tuple, set, frozenset)):
            return value
        converted = [self.convert(item, item_node, item_path, factor) for item in value]
        if isinstance(value, tuple):
            return tuple(converted)
        if isinstance(value, frozenset):
            return frozenset(converted)
        if isinstance(value, set):
            return set(converted)
        return converted

    def _convert_mapping(
        self, value: Any, node: mi.Type, path: str, factor: float | None
    ) -> Any:
        if not isinstance(value, Mapping):
            return value
        return {
            self.convert(key, node.key_type, f"{path}{{key}}", None): self.convert(
                item, node.value_type, f"{path}{{value}}", factor
            )
            for key, item in value.items()
        }

    def _select_union_member(self, union: mi.UnionType, value: Any) -> mi.Type | None:
        for member in union.types:
            node = member
            while isinstance(node, mi.Metadata):
                node = node.type
            if self._matches(node, value):
                return member
        return None

    @staticmethod
    def _matches(node: mi.Type, value: Any) -> bool:
        if value is None:
            return isinstance(node, mi.NoneType)
        if isinstance(node, (mi.StructType, mi.EnumType)):
            return isinstance(value, node.cls)
        if isinstance(node, mi.IntType):
            return isinstance(value, int) and not isinstance(value, bool)
        if isinstance(node, mi.FloatType):
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        checks = (
            (mi.BoolType, bool),
            (mi.StrType, str),
            (mi.BytesType, bytes),
            (mi.ByteArrayType, bytearray),
            ((mi.ListType, mi.SetType, mi.FrozenSetType), (list, set, frozenset)),
            (mi.VarTupleType, tuple),
            ((mi.DictType, mi.FrozenDictType), Mapping),
        )
        return any(
            isinstance(node, node_type) and isinstance(value, value_type)
            for node_type, value_type in checks
        )


def _normalize_path(path: str) -> str:
    if not path:
        return "$"
    normalized = path if path.startswith("$") else f"$.{path.lstrip('.')}"
    return normalized.rstrip(".")


def _normalize_overrides(
    overrides: Mapping[str, Any] | None,
) -> dict[str, str]:
    if not overrides:
        return {}
    normalized: dict[str, str] = {}
    for path, value in overrides.items():
        if isinstance(value, Mapping):
            unit = value.get("unit")
            if unit is None:
                raise ValueError(
                    f"override at {path!r} is a mapping without a 'unit' entry"
                )
        else:
            unit = value
        if not isinstance(unit, str):
            raise ValueError(
                f"override at {path!r} must declare a unit string, got {unit!r}"
            )
        normalized[_normalize_path(path)] = unit
    return normalized


def compile_unit_table(
    type: Any,
    *,
    overrides: Mapping[str, Any] | None = None,
    unit_key: str = DEFAULT_UNIT_KEY,
    target_unit: str | None = None,
    factors: Mapping[str, float] | None = None,
) -> UnitTable:
    """Compile a unit conversion table for a msgspec-compatible type.

    The type is introspected with `msgspec.inspect.type_info`. Every
    occurrence whose ``Annotated`` metadata carries ``extra[unit_key]`` becomes
    a column in the returned table. The same concrete type reached through
    different paths yields one column per path, each recording the rule that
    was actually applied.

    Parameters
    ----------
    type: type
        The type to compile.
    overrides: mapping, optional
        Occurrence-path overrides. Keys are paths rooted at ``"$"`` (a leading
        ``"$"`` or ``"$."`` may be omitted). Values are either a unit string or
        a mapping containing a ``"unit"`` entry. An override only takes effect
        while visiting its exact path; it neither leaks into the occurrence's
        children nor changes sibling occurrences. The path must match an
        occurrence emitted by a compile without overrides; recursive
        occurrences are addressed by their first-occurrence path. An unmatched
        path raises `KeyError`.
    unit_key: str, optional
        The metadata key to read from ``Meta(extra=...)``. Defaults to
        ``"unit"``.
    target_unit: str, optional
        The unit callers intend to normalize raw rows into. Stored on the
        returned table for documentation and downstream tooling.
    factors: mapping, optional
        Multiplicative factors per unit, recorded on each matching column and
        used by `UnitTable.convert`. Defaults to an empty mapping, meaning
        identity factors.

    Returns
    -------
    UnitTable
        The compiled conversion table. Compiling the same class twice with
        different override configurations produces independent tables, and
        every column identifies the rule it adopted via its ``rule`` and
        ``contract_unit`` fields.
    """
    info = mi.type_info(type)
    compiler = _UnitCompiler(
        root=type,
        overrides=_normalize_overrides(overrides),
        unit_key=unit_key,
        target_unit=target_unit,
        factors=factors or {},
    )
    return compiler.compile(info)
