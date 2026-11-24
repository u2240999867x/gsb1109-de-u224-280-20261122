"""Build per-channel, executable field contracts from msgspec types.

A *field contract* is a generated `msgspec.Struct` (or union of structs) that
describes exactly the columns a downstream channel is allowed to see.  The
generated type is independent from the source type: removing a field for one
channel never changes another channel's contract or the original type.

The module also provides a projection function for real records.  Projection is
type-aware, so tagged unions (including tagged values nested in arrays) select
the contract for the tag that is actually present.

Examples
--------
>>> import msgspec
>>> from msgspec import contracts
>>>
>>> class Waybill(msgspec.Struct, tag="waybill", tag_field="kind"):
...     id: str
...     container: str
...     address: str
>>>
>>> policy = contracts.AccessPolicy(exclude={"address"}, primary_keys={"id"})
>>> analytics = contracts.contract(Waybill, name="analytics", policy=policy)
>>> analytics.export(Waybill("w1", "C1", "secret"))
{'kind': 'waybill', 'id': 'w1', 'container': 'C1'}
"""

from __future__ import annotations

import datetime as _datetime
import decimal as _decimal
import itertools as _itertools
import re as _re
import sys as _sys
import types as _types
import typing as _typing
import uuid as _uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import msgspec

from . import inspect as _inspect
from ._core import NODEFAULT, UNSET  # type: ignore

__all__ = (
    "AccessPolicy",
    "ColumnPolicy",
    "ColumnRule",
    "Conflict",
    "ContractConflict",
    "ContractError",
    "FieldContract",
    "FieldRule",
    "build_contract",
    "build_contracts",
    "build_channels",
    "check_contract",
    "contract",
    "field_contract",
    "export",
    "project",
)


class FieldRule:
    """A rule controlling which fields are exposed.

    Parameters
    ----------
    include: iterable of str, optional
        If provided, only these field names are exposed.  Names may be either
        Python attribute names or encoded/wire names.
    exclude: iterable of str, optional
        Field names to hide.  Exclusion wins over inclusion for ordinary
        fields, but a declared primary key is always retained.
    allow, visible: iterable of str, optional
        Aliases for ``include``.
    deny, hidden: iterable of str, optional
        Aliases for ``exclude``.
    """

    __slots__ = ("include", "exclude")

    def __init__(
        self,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] = (),
        *,
        allow: Iterable[str] | None = None,
        deny: Iterable[str] | None = None,
        visible: Iterable[str] | None = None,
        hidden: Iterable[str] | None = None,
    ):
        for extra in (allow, visible):
            if extra is not None:
                include = extra if include is None else (*include, *extra)
        deny = (*deny, *hidden) if deny and hidden else (deny or hidden or ())
        self.include = None if include is None else frozenset(include)
        self.exclude = frozenset((*exclude, *deny))

    def __repr__(self) -> str:
        return f"FieldRule(include={self.include!r}, exclude={self.exclude!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, FieldRule)
            and self.include == other.include
            and self.exclude == other.exclude
        )

    def __hash__(self) -> int:
        return hash((self.include, self.exclude))

    def allows(self, name: str, encode_name: str) -> bool:
        """Return whether an ordinary (non-primary-key) field is exposed."""
        if name in self.exclude or encode_name in self.exclude:
            return False
        if self.include is not None:
            return name in self.include or encode_name in self.include
        return True

    def allows_tag_field(self, tag_field: str) -> bool:
        """Return whether a discriminator field is exposed.

        A discriminator is not an ordinary column: every tagged row must carry
        it.  An explicit allow-list therefore has to mention it, otherwise the
        channel is asking for a tagged contract with no usable discriminator.
        """
        if tag_field in self.exclude:
            return False
        if self.include is not None and tag_field not in self.include:
            return False
        return True


class AccessPolicy:
    """Per-channel field visibility rules.

    Parameters
    ----------
    include: iterable of str, optional
        Default allow-list for ordinary fields.
    exclude: iterable of str, optional
        Default deny-list for ordinary fields.
    by_type: mapping, optional
        Rules for specific struct types.  Keys may be the class itself, a
        class name, a qualified name, or a ``module.qualname`` string.  Values
        may be a `FieldRule`, an iterable of allowed names, or a mapping with
        ``include`` / ``exclude`` (and the aliases accepted by `FieldRule`).
    by_tag: mapping, optional
        Rules for tagged structs, keyed by tag value.  A type-specific rule
        takes precedence over a tag rule.
    default: FieldRule, optional
        Rule used when no type or tag rule matches.  If omitted, the default
        is built from ``include`` and ``exclude``.
    primary_keys: iterable of str, optional
        Field names that are primary keys.  They are retained at every level,
        even when an include/exclude rule would hide them.  Primary keys may
        also be marked with ``Annotated[T, msgspec.Meta(extra={"primary_key":
        True})]``.
    allow, visible: iterable of str, optional
        Aliases for ``include``.
    deny, hidden: iterable of str, optional
        Aliases for ``exclude``.
    keys, identifiers, key_fields: iterable of str, optional
        Aliases for ``primary_keys``.
    """

    __slots__ = ("default", "_by_type", "_by_tag", "primary_keys")

    def __init__(
        self,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] = (),
        *,
        by_type: Mapping[Any, Any] | None = None,
        by_tag: Mapping[Any, Any] | None = None,
        default: FieldRule | None = None,
        primary_keys: Iterable[str] = (),
        allow: Iterable[str] | None = None,
        deny: Iterable[str] | None = None,
        visible: Iterable[str] | None = None,
        hidden: Iterable[str] | None = None,
        keys: Iterable[str] = (),
        identifiers: Iterable[str] = (),
        key_fields: Iterable[str] = (),
    ):
        if default is None:
            default = FieldRule(
                include=include,
                exclude=exclude,
                allow=allow,
                deny=deny,
                visible=visible,
                hidden=hidden,
            )
        elif not isinstance(default, FieldRule):
            default = _coerce_rule(default)
        self.default = default
        self._by_type = tuple(
            (key, _coerce_rule(value)) for key, value in (by_type or {}).items()
        )
        self._by_tag = tuple(
            (key, _coerce_rule(value)) for key, value in (by_tag or {}).items()
        )
        self.primary_keys = frozenset((*primary_keys, *keys, *identifiers, *key_fields))

    def __repr__(self) -> str:
        return (
            "AccessPolicy("
            f"default={self.default!r}, "
            f"by_type={dict(self._by_type)!r}, "
            f"by_tag={dict(self._by_tag)!r}, "
            f"primary_keys={self.primary_keys!r})"
        )

    def with_primary_keys(self, primary_keys: Iterable[str]) -> AccessPolicy:
        """Return a copy of this policy with additional primary keys."""
        out = AccessPolicy.__new__(AccessPolicy)
        out.default = self.default
        out._by_type = self._by_type
        out._by_tag = self._by_tag
        out.primary_keys = self.primary_keys | frozenset(primary_keys)
        return out

    def rule_for(self, cls: Any, tag: Any = None) -> FieldRule:
        """Return the rule applying to a struct class/tag."""
        for key, rule in self._by_type:
            if _type_key_matches(key, cls):
                return rule
        if tag is not None:
            for key, rule in self._by_tag:
                if key == tag:
                    return rule
        return self.default

    def field_is_primary(self, field: _inspect.Field) -> bool:
        """Return whether a field is a declared primary key."""
        names = self.primary_keys
        if field.name in names or field.encode_name in names:
            return True
        return _metadata_marks_primary(field.type)


ColumnRule = FieldRule
ColumnPolicy = AccessPolicy


@dataclass(frozen=True)
class Conflict:
    """A single reason a channel contract cannot be built safely."""

    channel: str
    kind: str
    path: str
    message: str
    struct: type | None = None
    tag_field: str | None = None
    tag: Any = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible description of this conflict."""
        return {
            "channel": self.channel,
            "kind": self.kind,
            "path": self.path,
            "message": self.message,
            "struct": None if self.struct is None else _type_name(self.struct),
            "tag_field": self.tag_field,
            "tag": self.tag,
        }


class ContractConflict(ValueError):
    """Raised when a field contract would be un-decodable.

    ``conflicts`` contains one or more `Conflict` records.  The most common
    case is a tagged struct whose discriminator field is hidden by the channel
    rule: the msgspec metadata still names the tag, so emitting a contract
    without the discriminator would produce rows no normal decoder can accept.
    """

    def __init__(self, channel: str, conflicts: Iterable[Conflict]):
        self.channel = channel
        self.conflicts = tuple(conflicts)
        lines = [f"cannot build field contract for channel {channel!r}:"]
        lines.extend(f"- {c.message}" for c in self.conflicts)
        super().__init__("\n".join(lines))


ContractError = ContractConflict


class FieldContract:
    """An executable field contract for one channel.

    Attributes
    ----------
    name: str
        Channel/contract name.
    type: type
        A generated msgspec-compatible type.  It can be passed directly to
        `msgspec.json.decode`, `msgspec.convert`, or `msgspec.json.schema`.
    policy: AccessPolicy
        The policy used to build this contract.
    source_types: tuple
        The original type annotations supplied by the caller.
    conflicts: tuple[Conflict]
        Always empty for a successfully built contract.  It is included so
        callers can inspect both successful and diagnostic results uniformly.
    """

    __slots__ = (
        "name",
        "type",
        "policy",
        "source_types",
        "conflicts",
        "_roots",
        "_plans",
    )

    def __init__(
        self,
        name: str,
        type: Any,
        policy: AccessPolicy,
        source_types: tuple[Any, ...],
        roots: tuple[_inspect.Type, ...],
        plans: Mapping[int, _StructPlan],
    ):
        self.name = name
        self.type = type
        self.policy = policy
        self.source_types = source_types
        self.conflicts = ()
        self._roots = roots
        self._plans = dict(plans)

    def __repr__(self) -> str:
        return (
            f"FieldContract(name={self.name!r}, type={self.type!r}, "
            f"source_types={self.source_types!r})"
        )

    @property
    def schema(self) -> dict[str, Any]:
        """Generate a JSON Schema for this contract's executable type."""
        return msgspec.json.schema(self.type)

    def to_schema(self) -> dict[str, Any]:
        """Alias for `schema`."""
        return self.schema

    def describe(self) -> dict[str, Any]:
        """Return a JSON-compatible description of the contract.

        This describes the executable type (fields, wire names, tags,
        required/default flags) rather than a JSON Schema.  It is useful as a
        machine-readable field contract when a JSON Schema is not wanted.
        """
        return _describe_type(self.type_info)

    def columns(self) -> tuple[str, ...]:
        """Return the top-level encoded column names of the contract."""
        return tuple(_collect_columns(self.type_info))

    @property
    def type_info(self) -> _inspect.Type:
        """Inspect the generated contract type."""
        return _inspect.type_info(self.type)

    def export(self, obj: Any) -> Any:
        """Project a record onto this channel's visible fields.

        The result is made of JSON-compatible builtins and is ready for
        `msgspec.json.encode` or for handing to another contract.
        """
        return _export_value(
            obj,
            self._roots[0] if len(self._roots) == 1 else _UnionRoot(self._roots),
            self.policy,
        )

    def export_many(self, objs: Iterable[Any]) -> list[Any]:
        """Project an iterable of records."""
        return [self.export(obj) for obj in objs]

    def encode(self, obj: Any) -> bytes:
        """Project ``obj`` and encode the result as JSON."""
        return msgspec.json.encode(self.export(obj))

    def decode(self, data: bytes | str) -> Any:
        """Decode a JSON row using this contract's executable type."""
        return msgspec.json.decode(data, type=self.type)

    def validate(self, data: bytes | str) -> Any:
        """Decode and validate a JSON row."""
        return self.decode(data)

    def convert(self, obj: Any) -> Any:
        """Project ``obj`` and convert it to this contract's generated type."""
        return msgspec.convert(self.export(obj), type=self.type)


def contract(
    source: Any,
    *,
    name: str = "contract",
    policy: AccessPolicy | Mapping[Any, Any] | None = None,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] = (),
    primary_keys: Iterable[str] = (),
    forbid_unknown_fields: bool = True,
) -> FieldContract:
    """Build an executable field contract for one channel.

    Parameters
    ----------
    source: type or iterable of types
        The source msgspec-compatible type(s).  A tuple/list of types is
        treated as a contract root union.
    name: str
        Channel name, used for diagnostics and generated class names.
    policy: AccessPolicy or mapping, optional
        Field visibility rules.  A mapping is treated as ``by_type`` rules.
    include: iterable of str, optional
        Shorthand default allow-list, used when ``policy`` is not given.
    exclude: iterable of str, optional
        Shorthand default deny-list, used when ``policy`` is not given.
    primary_keys: iterable of str, optional
        Extra primary-key field names.  These are added to the policy's keys.
        May be used both with and without an explicit ``policy``.
    forbid_unknown_fields: bool, default True
        Whether the generated contract rejects fields that are not part of the
        channel.  Exported rows are already projected, so strict contracts
        remain usable while catching accidental leakage.

    Raises
    ------
    ContractConflict
        If a tagged type's discriminator field is hidden, or if union metadata
        references tags in a way that cannot be represented by a valid
        contract.  The exception's ``conflicts`` attribute explains each case.
    """
    policy = _resolve_policy(policy, include, exclude, primary_keys)

    types, roots = _coerce_sources(source)
    builder = _ContractBuilder(
        name=name,
        roots=roots,
        policy=policy,
        forbid_unknown_fields=forbid_unknown_fields,
    )
    dynamic_type, plans = builder.build()
    return FieldContract(name, dynamic_type, policy, types, roots, plans)


def check_contract(
    source: Any,
    *,
    name: str = "contract",
    policy: AccessPolicy | Mapping[Any, Any] | None = None,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] = (),
    primary_keys: Iterable[str] = (),
    forbid_unknown_fields: bool = True,
) -> tuple[Conflict, ...]:
    """Return conflicts without building or raising.

    This is the diagnostic counterpart to `contract`.  An empty tuple means a
    contract can be built with the supplied rules.
    """
    policy = _resolve_policy(policy, include, exclude, primary_keys)

    _, roots = _coerce_sources(source)
    builder = _ContractBuilder(
        name=name,
        roots=roots,
        policy=policy,
        forbid_unknown_fields=forbid_unknown_fields,
    )
    builder.collect()
    return tuple(builder.conflicts)


# `build_contract`/`field_contract` are aliases so that both spellings refer to
# the same function object, matching the rest of the public API.
build_contract = contract
field_contract = contract


def build_contracts(
    sources: Iterable[Any],
    *,
    names: Iterable[str] | None = None,
    policies: Iterable[AccessPolicy | Mapping[Any, Any] | None] | None = None,
    primary_keys: Iterable[str] = (),
    forbid_unknown_fields: bool = True,
) -> tuple[FieldContract, ...]:
    """Build one contract per source, in order."""
    sources = tuple(sources)
    if names is None:
        names = tuple(f"contract{i}" for i in range(len(sources)))
    else:
        names = tuple(names)
    if len(names) != len(sources):
        raise ValueError("names must have the same length as sources")
    if policies is None:
        policies = (None,) * len(sources)
    else:
        policies = tuple(policies)
    if len(policies) != len(sources):
        raise ValueError("policies must have the same length as sources")
    return tuple(
        contract(
            source,
            name=name,
            policy=policy,
            primary_keys=primary_keys,
            forbid_unknown_fields=forbid_unknown_fields,
        )
        for source, name, policy in zip(sources, names, policies)
    )


def build_channels(
    source: Any,
    rules: Mapping[str, AccessPolicy | Mapping[Any, Any]],
    *,
    primary_keys: Iterable[str] = (),
    forbid_unknown_fields: bool = True,
) -> dict[str, FieldContract]:
    """Build one independently named contract for each channel rule."""
    return {
        name: contract(
            source,
            name=name,
            policy=policy,
            primary_keys=primary_keys,
            forbid_unknown_fields=forbid_unknown_fields,
        )
        for name, policy in rules.items()
    }


def export(obj: Any, contract: FieldContract) -> Any:
    """Project ``obj`` through ``contract`` (convenience function)."""
    return contract.export(obj)


project = export


# ---------------------------------------------------------------------------
# Internal policy helpers


def _resolve_policy(
    policy: AccessPolicy | Mapping[Any, Any] | None,
    include: Iterable[str] | None,
    exclude: Iterable[str],
    primary_keys: Iterable[str],
) -> AccessPolicy:
    if policy is None:
        return AccessPolicy(include=include, exclude=exclude, primary_keys=primary_keys)
    if isinstance(policy, Mapping):
        policy = AccessPolicy(by_type=policy)
    elif not isinstance(policy, AccessPolicy):
        raise TypeError("policy must be an AccessPolicy, mapping, or None")
    if include is not None or exclude:
        # Shorthand rules augment the policy's default rule without changing
        # its per-type/tag rules.
        policy = AccessPolicy(
            default=FieldRule(
                include=policy.default.include if include is None else include,
                exclude=(*policy.default.exclude, *exclude),
            ),
            by_type=dict(policy._by_type),
            by_tag=dict(policy._by_tag),
            primary_keys=policy.primary_keys,
        )
    if primary_keys:
        policy = policy.with_primary_keys(primary_keys)
    return policy


def _coerce_rule(value: Any) -> FieldRule:
    if isinstance(value, FieldRule):
        return value
    if value is None:
        return FieldRule()
    if isinstance(value, Mapping):
        return FieldRule(
            include=value.get("include", value.get("allow", value.get("visible"))),
            exclude=value.get("exclude", value.get("deny", value.get("hidden", ()))),
        )
    if isinstance(value, str):
        return FieldRule(include=(value,))
    if isinstance(value, Iterable):
        return FieldRule(include=tuple(value))
    raise TypeError(f"cannot interpret {value!r} as a FieldRule")


def _type_key_matches(key: Any, cls: Any) -> bool:
    if key is cls:
        return True
    if not isinstance(key, str):
        return False
    name = getattr(cls, "__name__", None)
    qualname = getattr(cls, "__qualname__", None)
    module = getattr(cls, "__module__", None)
    return key in {
        name,
        qualname,
        f"{module}.{qualname}" if module and qualname else None,
    }


def _unwrap_metadata(t: _inspect.Type) -> _inspect.Type:
    while isinstance(t, _inspect.Metadata):
        t = t.type
    return t


def _metadata_marks_primary(t: _inspect.Type) -> bool:
    while isinstance(t, _inspect.Metadata):
        extra = t.extra or {}
        if extra.get("primary_key") is True or extra.get("primary-key") is True:
            return True
        if extra.get("key") == "primary":
            return True
        schema = t.extra_json_schema or {}
        if schema.get("primaryKey") is True or schema.get("x-primary-key") is True:
            return True
        t = t.type
    return False


def _is_kw_only(cls: Any) -> bool:
    """Return whether a struct's generated ``__init__`` is keyword-only.

    ``StructConfig`` does not expose ``kw_only`` in this snapshot, so inspect
    the constructor signature instead.  This matters when a removed field
    leaves a required field after an optional one: the generated contract must
    use the same keyword-only setting as the source.
    """
    import inspect as _pyinspect

    try:
        params = _pyinspect.signature(cls).parameters
    except (TypeError, ValueError):
        return False
    return any(p.kind is _pyinspect.Parameter.KEYWORD_ONLY for p in params.values())


def _coerce_sources(source: Any) -> tuple[tuple[Any, ...], tuple[_inspect.Type, ...]]:
    if isinstance(source, (tuple, list)):
        types = tuple(source)
    else:
        types = (source,)
    if not types:
        raise ValueError("at least one source type is required")
    roots = _inspect.multi_type_info(types)
    return types, roots


# ---------------------------------------------------------------------------
# Dynamic contract type generation


class _StructPlan:
    __slots__ = (
        "original",
        "name",
        "path",
        "fields",
        "cls",
        "rename",
        "omit_defaults",
        "kw_only",
    )

    def __init__(self, original: _inspect.StructType, name: str, path: str):
        self.original = original
        self.name = name
        self.path = path
        self.fields: list[Any] = []
        self.cls: type | None = None
        self.rename: dict[str, str] = {}
        self.omit_defaults = False
        self.kw_only = False


class _ContractBuilder:
    def __init__(
        self,
        *,
        name: str,
        roots: tuple[_inspect.Type, ...],
        policy: AccessPolicy,
        forbid_unknown_fields: bool,
    ):
        self.name = name
        self.roots = roots
        self.policy = policy
        self.forbid_unknown_fields = forbid_unknown_fields
        self.conflicts: list[Conflict] = []
        self._conflict_keys: set[Any] = set()

        self._module_name = f"__msgspec_contract_{next(_MODULE_COUNTER)}__"
        self._module = _types.ModuleType(self._module_name)
        self._module.__dict__.update(
            {
                "msgspec": msgspec,
                "typing": _typing,
                "datetime": _datetime,
                "decimal": _decimal,
                "uuid": _uuid,
                "Annotated": _typing.Annotated,
                "Literal": _typing.Literal,
                "Union": _typing.Union,
                "Any": _typing.Any,
                "Optional": _typing.Optional,
            }
        )
        _sys.modules[self._module_name] = self._module

        self._plans: dict[int, _StructPlan] = {}
        self._plan_order: list[_StructPlan] = []
        self._used_names: set[str] = set()
        self._external_names: dict[int, str] = {}

    # -- diagnostics -----------------------------------------------------

    def _add_conflict(
        self,
        *,
        kind: str,
        path: str,
        message: str,
        struct: type | None = None,
        tag_field: str | None = None,
        tag: Any = None,
    ) -> None:
        key = (kind, id(struct), path, tag_field, tag)
        if key in self._conflict_keys:
            return
        self._conflict_keys.add(key)
        self.conflicts.append(
            Conflict(
                channel=self.name,
                kind=kind,
                path=path,
                message=message,
                struct=struct,
                tag_field=tag_field,
                tag=tag,
            )
        )

    def _check_tag_visible(self, t: _inspect.StructType, path: str) -> None:
        if t.tag_field is None:
            return
        if t.tag_field in self.policy.primary_keys:
            return
        rule = self.policy.rule_for(t.cls, t.tag)
        if rule.allows_tag_field(t.tag_field):
            return
        clsname = _type_name(t.cls)
        self._add_conflict(
            kind="hidden-discriminator",
            path=path,
            struct=t.cls,
            tag_field=t.tag_field,
            tag=t.tag,
            message=(
                f"{path}: {clsname} is tagged with {t.tag!r} via "
                f"discriminator field {t.tag_field!r}, but that field is not "
                f"exposed to channel {self.name!r}; the msgspec metadata still "
                f"references this discriminator/tag, so a generated contract "
                f"would require a column the channel forbids. Expose "
                f"{t.tag_field!r} or stop using this tagged type on this "
                f"channel."
            ),
        )

    # -- type expression building ----------------------------------------

    def _ensure_struct(self, t: _inspect.StructType, path: str) -> str:
        key = id(t.cls)
        plan = self._plans.get(key)
        if plan is not None:
            return plan.name
        name = self._make_name(t.cls)
        plan = _StructPlan(t, name, path)
        self._plans[key] = plan
        self._plan_order.append(plan)
        return name

    def _make_name(self, cls: Any) -> str:
        base = _type_name(cls)
        base = _re.sub(r"[^0-9A-Za-z_]", "_", base) or "Struct"
        if base[0].isdigit():
            base = f"T_{base}"
        base = f"{_re.sub(r'[^0-9A-Za-z_]', '_', self.name)}_{base}"
        candidate = base
        i = 2
        while candidate in self._used_names:
            candidate = f"{base}_{i}"
            i += 1
        self._used_names.add(candidate)
        return candidate

    def _external(self, obj: Any) -> str:
        key = id(obj)
        if key in self._external_names:
            return self._external_names[key]
        name = f"_external_{len(self._external_names)}"
        self._external_names[key] = name
        setattr(self._module, name, obj)
        return name

    def _expr(self, t: _inspect.Type, path: str) -> str:
        if isinstance(t, _inspect.Metadata):
            inner = self._expr(t.type, path)
            return f"Annotated[{inner}, {_metadata_expr(t)}]"

        if isinstance(t, _inspect.StructType):
            name = self._ensure_struct(t, path)
            self._check_tag_visible(t, path)
            return name

        if isinstance(
            t,
            (
                _inspect.TypedDictType,
                _inspect.DataclassType,
                _inspect.NamedTupleType,
            ),
        ):
            raise TypeError(
                "field contracts currently support msgspec Struct types; "
                f"got {_type_name(t.cls)!r}. Convert it to a Struct first."
            )

        if isinstance(t, _inspect.UnionType):
            self._check_union(t, path)
            parts = [self._expr(item, f"{path}|") for item in t.types]
            if len(parts) == 1:
                return parts[0]
            return f"typing.Union[{', '.join(parts)}]"

        if isinstance(t, _inspect.ListType):
            return _with_meta(
                f"list[{self._expr(t.item_type, f'{path}[]')}]",
                min_length=t.min_length,
                max_length=t.max_length,
            )
        if isinstance(t, _inspect.SetType):
            return _with_meta(
                f"set[{self._expr(t.item_type, f'{path}[]')}]",
                min_length=t.min_length,
                max_length=t.max_length,
            )
        if isinstance(t, _inspect.FrozenSetType):
            return _with_meta(
                f"frozenset[{self._expr(t.item_type, f'{path}[]')}]",
                min_length=t.min_length,
                max_length=t.max_length,
            )
        if isinstance(t, _inspect.VarTupleType):
            return _with_meta(
                f"tuple[{self._expr(t.item_type, f'{path}[]')}, ...]",
                min_length=t.min_length,
                max_length=t.max_length,
            )
        if isinstance(t, _inspect.TupleType):
            if not t.item_types:
                return "tuple[()]"
            parts = [
                self._expr(item, f"{path}[{i}]") for i, item in enumerate(t.item_types)
            ]
            return f"tuple[{', '.join(parts)}]"
        if isinstance(t, _inspect.DictType):
            key = self._expr(t.key_type, f"{path}{{key}}")
            value = self._expr(t.value_type, f"{path}{{value}}")
            return _with_meta(
                f"dict[{key}, {value}]",
                min_length=t.min_length,
                max_length=t.max_length,
            )
        if isinstance(t, _inspect.FrozenDictType):
            key = self._expr(t.key_type, f"{path}{{key}}")
            value = self._expr(t.value_type, f"{path}{{value}}")
            return _with_meta(
                f"dict[{key}, {value}]",
                min_length=t.min_length,
                max_length=t.max_length,
            )

        if isinstance(t, _inspect.AnyType):
            return "typing.Any"
        if isinstance(t, _inspect.NoneType):
            return "None"
        if isinstance(t, _inspect.BoolType):
            return "bool"
        if isinstance(t, _inspect.IntType):
            return _with_meta(
                "int",
                gt=t.gt,
                ge=t.ge,
                lt=t.lt,
                le=t.le,
                multiple_of=t.multiple_of,
            )
        if isinstance(t, _inspect.FloatType):
            return _with_meta(
                "float",
                gt=t.gt,
                ge=t.ge,
                lt=t.lt,
                le=t.le,
                multiple_of=t.multiple_of,
            )
        if isinstance(t, _inspect.StrType):
            return _with_meta(
                "str",
                pattern=t.pattern,
                min_length=t.min_length,
                max_length=t.max_length,
            )
        if isinstance(t, _inspect.BytesType):
            return _with_meta("bytes", min_length=t.min_length, max_length=t.max_length)
        if isinstance(t, _inspect.ByteArrayType):
            return _with_meta(
                "bytearray", min_length=t.min_length, max_length=t.max_length
            )
        if isinstance(t, _inspect.MemoryViewType):
            return _with_meta(
                "memoryview", min_length=t.min_length, max_length=t.max_length
            )
        if isinstance(t, _inspect.DateTimeType):
            return _with_meta("datetime.datetime", tz=t.tz)
        if isinstance(t, _inspect.TimeType):
            return _with_meta("datetime.time", tz=t.tz)
        if isinstance(t, _inspect.DateType):
            return "datetime.date"
        if isinstance(t, _inspect.TimeDeltaType):
            return "datetime.timedelta"
        if isinstance(t, _inspect.UUIDType):
            return "uuid.UUID"
        if isinstance(t, _inspect.DecimalType):
            return "decimal.Decimal"
        if isinstance(t, _inspect.RawType):
            return "msgspec.Raw"
        if isinstance(t, _inspect.ExtType):
            return "msgspec.msgpack.Ext"
        if isinstance(t, _inspect.LiteralType):
            return f"Literal[{', '.join(repr(v) for v in t.values)}]"
        if isinstance(t, _inspect.EnumType):
            return self._external(t.cls)
        if isinstance(t, _inspect.CustomType):
            return self._external(t.cls)

        raise TypeError(f"unsupported type for field contract: {t!r}")

    def _check_union(self, t: _inspect.UnionType, path: str) -> None:
        structs = []
        for item in t.types:
            inner = _unwrap_metadata(item)
            if isinstance(inner, _inspect.StructType):
                structs.append(inner)

        tagged = [s for s in structs if s.tag_field is not None]
        if not tagged:
            return

        if len(tagged) != len(structs):
            self._add_conflict(
                kind="mixed-tagged-union",
                path=path,
                struct=None,
                tag_field=tagged[0].tag_field,
                tag=None,
                message=(
                    f"{path}: union mixes tagged and untagged structs; the "
                    f"tag metadata cannot be represented consistently in "
                    f"channel {self.name!r}."
                ),
            )

        tag_fields = {s.tag_field for s in tagged}
        if len(tag_fields) > 1:
            self._add_conflict(
                kind="inconsistent-tag-field",
                path=path,
                struct=None,
                tag_field=None,
                tag=None,
                message=(
                    f"{path}: tagged union members use different discriminator "
                    f"fields {sorted(tag_fields)!r}; channel {self.name!r} "
                    f"cannot expose one consistent discriminator."
                ),
            )
        else:
            for s in tagged:
                self._check_tag_visible(s, path)

        tags = [s.tag for s in tagged]
        if len(set(tags)) != len(tags):
            self._add_conflict(
                kind="duplicate-tag",
                path=path,
                struct=None,
                tag_field=tagged[0].tag_field,
                tag=None,
                message=(
                    f"{path}: tagged union has duplicate tag values {tags!r}; "
                    f"channel {self.name!r} cannot select a single struct "
                    f"definition for a row."
                ),
            )

    def _fill_plan(self, plan: _StructPlan) -> None:
        t = plan.original
        rule = self.policy.rule_for(t.cls, t.tag)
        self._check_tag_visible(t, plan.path)

        fields: list[Any] = []
        rename: dict[str, str] = {}
        for field in t.fields:
            primary = self.policy.field_is_primary(field)
            visible = primary or rule.allows(field.name, field.encode_name)
            if not visible:
                continue
            expr = self._expr(field.type, f"{plan.path}.{field.encode_name}")
            fields.append((field, expr))
            if field.encode_name != field.name:
                rename[field.name] = field.encode_name

        config = getattr(t.cls, "__struct_config__", None)
        omit_defaults = bool(getattr(config, "omit_defaults", False))
        kw_only = _is_kw_only(t.cls)

        dynamic_fields = []
        for field, expr in fields:
            if field.required:
                dynamic_fields.append((field.name, expr))
            elif field.default_factory is not NODEFAULT:
                dynamic_fields.append(
                    (
                        field.name,
                        expr,
                        msgspec.field(default_factory=field.default_factory),
                    )
                )
            elif field.default is not NODEFAULT:
                dynamic_fields.append((field.name, expr, field.default))
            else:
                # Original optional-without-default (typically `UnsetType`).
                dynamic_fields.append((field.name, expr, UNSET))

        plan.fields = dynamic_fields
        plan.rename = rename
        plan.omit_defaults = omit_defaults
        plan.kw_only = kw_only

    def collect(self) -> None:
        """Build the full type graph without creating Python classes."""
        for root in self.roots:
            self._expr(root, "$")
        i = 0
        while i < len(self._plan_order):
            self._fill_plan(self._plan_order[i])
            i += 1

    def build(self) -> tuple[Any, dict[int, _StructPlan]]:
        self.collect()
        if self.conflicts:
            raise ContractConflict(self.name, self.conflicts)

        # `defstruct` stores string annotations; every generated class is in
        # this module before any annotation is evaluated by a decoder.
        for plan in self._plan_order:
            t = plan.original
            cls = msgspec.defstruct(
                plan.name,
                plan.fields,
                module=self._module_name,
                tag=t.tag,
                tag_field=t.tag_field,
                rename=getattr(plan, "rename", None),
                omit_defaults=getattr(plan, "omit_defaults", False),
                kw_only=getattr(plan, "kw_only", False),
                array_like=t.array_like,
                forbid_unknown_fields=self.forbid_unknown_fields,
            )
            plan.cls = cls
            setattr(self._module, plan.name, cls)

        if len(self.roots) == 1:
            expr = self._expr(self.roots[0], "$")
        else:
            parts = [self._expr(root, "$") for root in self.roots]
            expr = f"typing.Union[{', '.join(parts)}]"
        dynamic_type = eval(expr, self._module.__dict__)  # noqa: S307
        return dynamic_type, self._plans


# A tiny marker used only by the export path for a multi-root contract.
class _UnionRoot:
    __slots__ = ("types",)

    def __init__(self, types: tuple[_inspect.Type, ...]):
        self.types = types


# ---------------------------------------------------------------------------
# Value projection


_MISSING = object()
_OMIT = object()
_MODULE_COUNTER = _itertools.count()


def _export_value(value: Any, t: Any, policy: AccessPolicy, path: str = "$") -> Any:
    if value is UNSET:
        return _OMIT

    if isinstance(t, _UnionRoot):
        return _export_union(value, t.types, policy, path)

    if isinstance(t, _inspect.Metadata):
        return _export_value(value, t.type, policy, path)

    if isinstance(t, _inspect.StructType):
        return _export_struct(value, t, policy, path)

    if isinstance(t, _inspect.UnionType):
        return _export_union(value, t.types, policy, path)

    if isinstance(t, _inspect.ListType):
        return [
            _clean_omit(_export_value(item, t.item_type, policy, f"{path}[]"))
            for item in value
        ]
    if isinstance(t, _inspect.SetType):
        return [
            _clean_omit(_export_value(item, t.item_type, policy, f"{path}[]"))
            for item in value
        ]
    if isinstance(t, _inspect.FrozenSetType):
        return [
            _clean_omit(_export_value(item, t.item_type, policy, f"{path}[]"))
            for item in value
        ]
    if isinstance(t, _inspect.VarTupleType):
        return [
            _clean_omit(_export_value(item, t.item_type, policy, f"{path}[]"))
            for item in value
        ]
    if isinstance(t, _inspect.TupleType):
        return [
            _clean_omit(_export_value(item, item_t, policy, f"{path}[{i}]"))
            for i, (item, item_t) in enumerate(zip(value, t.item_types))
        ]
    if isinstance(t, (_inspect.DictType, _inspect.FrozenDictType)):
        return {
            _clean_omit(
                _export_value(k, t.key_type, policy, f"{path}{{key}}")
            ): _clean_omit(_export_value(v, t.value_type, policy, f"{path}{{value}}"))
            for k, v in value.items()
        }

    return _to_builtins_leaf(value)


def _export_union(
    value: Any, types: tuple[_inspect.Type, ...], policy: AccessPolicy, path: str
) -> Any:
    branches = [(_unwrap_metadata(t), t) for t in types]

    if value is None:
        for inner, original in branches:
            if isinstance(inner, _inspect.NoneType):
                return None

    if isinstance(value, Mapping):
        # Prefer a tag match; this is what makes heterogeneous arrays stable.
        for inner, original in branches:
            if isinstance(inner, _inspect.StructType) and inner.tag_field is not None:
                if value.get(inner.tag_field, _MISSING) == inner.tag:
                    return _export_value(value, original, policy, path)
        for inner, original in branches:
            if isinstance(inner, _inspect.StructType) and inner.tag_field is None:
                return _export_value(value, original, policy, path)
        for inner, original in branches:
            if isinstance(inner, (_inspect.DictType, _inspect.FrozenDictType)):
                return _export_value(value, original, policy, path)
        tagged = [
            inner
            for inner, _ in branches
            if isinstance(inner, _inspect.StructType) and inner.tag_field is not None
        ]
        if tagged:
            field = tagged[0].tag_field
            seen = value.get(field, _MISSING)
            raise ValueError(
                f"{path}: no tagged union member matches {field}="
                f"{None if seen is _MISSING else seen!r}"
            )

    if not isinstance(value, (str, bytes, bytearray, int, float, bool)):
        for inner, original in branches:
            if isinstance(inner, _inspect.StructType):
                cls = inner.cls
                try:
                    if isinstance(value, cls):
                        return _export_value(value, original, policy, path)
                except TypeError:
                    pass

    for inner, original in branches:
        if _leaf_matches(value, inner):
            return _export_value(value, original, policy, path)

    # No precise match: prefer the first branch that can consume the value.
    last_error = None
    for inner, original in branches:
        try:
            return _export_value(value, original, policy, path)
        except Exception as exc:  # pragma: no cover - defensive fallback
            last_error = exc
    if last_error is not None:
        raise last_error
    return _to_builtins_leaf(value)


def _leaf_matches(value: Any, t: _inspect.Type) -> bool:
    if isinstance(t, _inspect.AnyType):
        return True
    if isinstance(t, _inspect.NoneType):
        return value is None
    if isinstance(t, _inspect.BoolType):
        return isinstance(value, bool)
    if isinstance(t, _inspect.IntType):
        return isinstance(value, int) and not isinstance(value, bool)
    if isinstance(t, _inspect.FloatType):
        return isinstance(value, float)
    if isinstance(t, _inspect.StrType):
        return isinstance(value, str)
    if isinstance(
        t, (_inspect.BytesType, _inspect.ByteArrayType, _inspect.MemoryViewType)
    ):
        return isinstance(value, (bytes, bytearray, memoryview))
    if isinstance(t, _inspect.DateTimeType):
        return isinstance(value, _datetime.datetime)
    if isinstance(t, _inspect.TimeType):
        return isinstance(value, _datetime.time)
    if isinstance(t, _inspect.DateType):
        return isinstance(value, _datetime.date) and not isinstance(
            value, _datetime.datetime
        )
    if isinstance(t, _inspect.TimeDeltaType):
        return isinstance(value, _datetime.timedelta)
    if isinstance(t, _inspect.UUIDType):
        return isinstance(value, _uuid.UUID)
    if isinstance(t, _inspect.DecimalType):
        return isinstance(value, _decimal.Decimal)
    if isinstance(t, _inspect.LiteralType):
        return value in t.values
    if isinstance(t, _inspect.EnumType):
        try:
            return isinstance(value, t.cls)
        except TypeError:
            return False
    if isinstance(t, _inspect.CustomType):
        try:
            return isinstance(value, t.cls)
        except TypeError:
            return False
    if isinstance(t, _inspect.ListType):
        return isinstance(value, list)
    if isinstance(t, _inspect.SetType):
        return isinstance(value, set)
    if isinstance(t, _inspect.FrozenSetType):
        return isinstance(value, frozenset)
    if isinstance(t, (_inspect.TupleType, _inspect.VarTupleType)):
        return isinstance(value, tuple)
    if isinstance(t, (_inspect.DictType, _inspect.FrozenDictType)):
        return isinstance(value, Mapping)
    return True


def _export_struct(
    value: Any, t: _inspect.StructType, policy: AccessPolicy, path: str
) -> Any:
    rule = policy.rule_for(t.cls, t.tag)
    tag_field = t.tag_field
    tag = t.tag

    out: dict[str, Any] = {}
    positional: list[Any] = []

    if tag_field is not None and tag is not None:
        if t.array_like:
            positional.append(tag)
        else:
            out[tag_field] = tag

    for field in t.fields:
        primary = policy.field_is_primary(field)
        if not (primary or rule.allows(field.name, field.encode_name)):
            continue

        if isinstance(value, Mapping):
            raw = value.get(field.encode_name, _MISSING)
            if raw is _MISSING:
                raw = value.get(field.name, _MISSING)
        else:
            raw = getattr(value, field.name, _MISSING)

        if raw is _MISSING or raw is UNSET:
            continue

        projected = _export_value(
            raw, field.type, policy, f"{path}.{field.encode_name}"
        )
        if projected is _OMIT:
            continue
        if t.array_like:
            positional.append(projected)
        else:
            out[field.encode_name] = projected

    if t.array_like:
        return positional
    return out


def _clean_omit(value: Any) -> Any:
    return None if value is _OMIT else value


def _to_builtins_leaf(value: Any) -> Any:
    try:
        return msgspec.to_builtins(value)
    except Exception:
        return value


# ---------------------------------------------------------------------------
# Small expression helpers


def _type_name(cls: Any) -> str:
    origin = getattr(cls, "__origin__", None)
    if origin is not None:
        cls = origin
    return (
        getattr(cls, "__name__", None) or getattr(cls, "__qualname__", None) or str(cls)
    )


def _metadata_expr(t: _inspect.Metadata) -> str:
    """Build the `Meta(...)` expression for a metadata wrapper.

    Constraints are carried by the inner leaf type in `msgspec.inspect` and
    are emitted by `_with_meta` there.  This wrapper only adds the metadata
    that the leaf cannot represent: JSON-schema extras and user extras.
    """
    args = []
    if t.extra_json_schema:
        args.append(f"extra_json_schema={_value_expr(t.extra_json_schema)}")
    if t.extra:
        args.append(f"extra={_value_expr(t.extra)}")
    if not args:
        return "msgspec.Meta()"
    return f"msgspec.Meta({', '.join(args)})"


_META_ATTRS = (
    "gt",
    "ge",
    "lt",
    "le",
    "multiple_of",
    "pattern",
    "min_length",
    "max_length",
    "tz",
)


def _with_meta(expr: str, **constraints: Any) -> str:
    """Wrap a type expression in `Annotated` when it carries constraints.

    `msgspec.inspect` places constraints directly on leaf types (e.g.
    ``IntType(ge=0)``) rather than in a separate `Metadata` wrapper.  Such
    constraints must be carried over explicitly, otherwise the generated
    contract would silently accept values the source type rejects.
    """
    args = [f"{key}={val!r}" for key, val in constraints.items() if val is not None]
    if not args:
        return expr
    return f"Annotated[{expr}, msgspec.Meta({', '.join(args)})]"


def _value_expr(value: Any) -> str:
    if isinstance(value, dict):
        if not value:
            return "{}"
        return (
            "{"
            + ", ".join(f"{_value_expr(k)}: {_value_expr(v)}" for k, v in value.items())
            + "}"
        )
    if isinstance(value, list):
        return "[" + ", ".join(_value_expr(v) for v in value) + "]"
    if isinstance(value, tuple):
        if len(value) == 1:
            return f"({_value_expr(value[0])},)"
        return "(" + ", ".join(_value_expr(v) for v in value) + ")"
    if isinstance(value, type):
        return repr(value)
    return repr(value)


# ---------------------------------------------------------------------------
# Contract description


def _describe_type(t: _inspect.Type) -> dict[str, Any]:
    """A JSON-compatible description of an inspected type."""
    if isinstance(t, _inspect.Metadata):
        out = _describe_type(t.type)
        if t.extra_json_schema:
            out["extra_json_schema"] = t.extra_json_schema
        if t.extra:
            out["extra"] = t.extra
        return out

    if isinstance(t, _inspect.StructType):
        fields = []
        for field in t.fields:
            item = {
                "name": field.name,
                "wire_name": field.encode_name,
                "type": _describe_type(field.type),
                "required": field.required,
            }
            if field.default is not NODEFAULT:
                item["default"] = msgspec.to_builtins(field.default)
            elif field.default_factory is not NODEFAULT:
                factory = getattr(field.default_factory, "__name__", None)
                item["default_factory"] = factory or repr(field.default_factory)
            fields.append(item)
        out = {
            "kind": "struct",
            "type": _type_name(t.cls),
            "fields": fields,
            "array_like": t.array_like,
            "forbid_unknown_fields": t.forbid_unknown_fields,
        }
        if t.tag_field is not None:
            out["tag_field"] = t.tag_field
            out["tag"] = t.tag
        return out

    if isinstance(t, _inspect.UnionType):
        return {"kind": "union", "types": [_describe_type(x) for x in t.types]}

    if isinstance(t, _inspect.ListType):
        return {
            "kind": "list",
            "items": _describe_type(t.item_type),
            "min_length": t.min_length,
            "max_length": t.max_length,
        }
    if isinstance(t, _inspect.SetType):
        return {
            "kind": "set",
            "items": _describe_type(t.item_type),
            "min_length": t.min_length,
            "max_length": t.max_length,
        }
    if isinstance(t, _inspect.FrozenSetType):
        return {
            "kind": "frozenset",
            "items": _describe_type(t.item_type),
            "min_length": t.min_length,
            "max_length": t.max_length,
        }
    if isinstance(t, _inspect.VarTupleType):
        return {
            "kind": "var_tuple",
            "items": _describe_type(t.item_type),
            "min_length": t.min_length,
            "max_length": t.max_length,
        }
    if isinstance(t, _inspect.TupleType):
        return {
            "kind": "tuple",
            "items": [_describe_type(x) for x in t.item_types],
        }
    if isinstance(t, (_inspect.DictType, _inspect.FrozenDictType)):
        return {
            "kind": "dict",
            "keys": _describe_type(t.key_type),
            "values": _describe_type(t.value_type),
            "min_length": t.min_length,
            "max_length": t.max_length,
        }
    if isinstance(t, _inspect.LiteralType):
        return {"kind": "literal", "values": list(t.values)}
    if isinstance(t, _inspect.EnumType):
        return {
            "kind": "enum",
            "type": _type_name(t.cls),
            "values": [member.value for member in t.cls],
        }
    if isinstance(t, _inspect.CustomType):
        return {"kind": "custom", "type": _type_name(t.cls)}

    mapping = {
        _inspect.AnyType: "any",
        _inspect.NoneType: "none",
        _inspect.BoolType: "bool",
        _inspect.IntType: "int",
        _inspect.FloatType: "float",
        _inspect.StrType: "str",
        _inspect.BytesType: "bytes",
        _inspect.ByteArrayType: "bytearray",
        _inspect.MemoryViewType: "memoryview",
        _inspect.DateTimeType: "datetime",
        _inspect.TimeType: "time",
        _inspect.DateType: "date",
        _inspect.TimeDeltaType: "timedelta",
        _inspect.UUIDType: "uuid",
        _inspect.DecimalType: "decimal",
        _inspect.RawType: "raw",
        _inspect.ExtType: "ext",
    }
    for cls, kind in mapping.items():
        if isinstance(t, cls):
            out = {"kind": kind}
            for attr in _META_ATTRS:
                val = getattr(t, attr, None)
                if val is not None:
                    out[attr] = val
            return out
    raise TypeError(f"unsupported type for contract description: {t!r}")


def _collect_columns(t: _inspect.Type) -> list[str]:
    """Collect encoded column names, descending into containers/unions."""
    if isinstance(t, _inspect.Metadata):
        return _collect_columns(t.type)
    if isinstance(t, _inspect.StructType):
        cols = []
        if t.tag_field is not None:
            cols.append(t.tag_field)
        cols.extend(field.encode_name for field in t.fields)
        return cols
    if isinstance(t, _inspect.UnionType):
        cols: list[str] = []
        for member in t.types:
            for name in _collect_columns(member):
                if name not in cols:
                    cols.append(name)
        return cols
    if isinstance(t, _inspect.CollectionType):
        return _collect_columns(t.item_type)
    if isinstance(t, _inspect.TupleType):
        cols = []
        for member in t.item_types:
            for name in _collect_columns(member):
                if name not in cols:
                    cols.append(name)
        return cols
    if isinstance(t, (_inspect.DictType, _inspect.FrozenDictType)):
        return _collect_columns(t.value_type)
    return []
