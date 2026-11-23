"""Static read planning for columnar extraction from nested msgspec events.

Given only a msgspec type (the declared contract) and zero rows of data,
decide for each result column which source paths may need to be read:

- A read whose guard condition cannot hold under the contract is pruned,
  and the plan explains *why* it is unreachable (e.g. a field the guard
  requires to be absent is declared required, so it is always present --
  though it may still be explicitly null).
- A read whose guard may hold is kept, and the plan returns a concrete
  legal example input (in decoded form) that satisfies the guard and
  exercises the source path.

The analysis is purely contract-driven: union variants that simply never
occurred in an early sampling batch are still considered live, never
treated as dead branches.

All paths -- in rules, reasons, witnesses, and reports -- use the
*encoded* field names (``Field.encode_name``), never Python attribute
names.

Guard predicates
----------------
- ``Present("a.b")``: field ``b`` of object ``a`` was actually sent
  (distinguishes "absent" from "present but null" for required-nullable
  fields, which are always present).
- ``TagEq("a", "tag")``: the tagged object at ``a`` has the given tag.
- ``And(...)`` / ``Or(...)`` / ``Not(...)``: boolean combinations.
- ``Exists("items", pred)``: some element of the list ``items`` satisfies
  ``pred`` (paths inside ``pred`` are relative to the element; ``""`` is
  the element itself).
"""

from __future__ import annotations

import datetime
import decimal
import itertools
import uuid
from dataclasses import dataclass, field as dc_field
from typing import Any, Iterable, Optional, Union

import msgspec
import msgspec.inspect as mi

__all__ = [
    "Pred",
    "Present",
    "TagEq",
    "And",
    "Or",
    "Not",
    "Exists",
    "ReadRule",
    "PlannedRead",
    "ReadPlan",
    "plan_reads",
    "evaluate",
]


# ---------------------------------------------------------------------------
# Guard predicates
# ---------------------------------------------------------------------------


class Pred:
    """Base class for guard predicates."""


def _segments(path: Union[str, Iterable[str]]) -> tuple[str, ...]:
    if isinstance(path, str):
        if path == "":
            return ()
        return tuple(path.split("."))
    return tuple(path)


@dataclass(frozen=True)
class Present(Pred):
    """True if the field at ``path`` was present in the input."""

    path: Union[str, tuple[str, ...]]

    def __post_init__(self):
        if not _segments(self.path):
            raise ValueError("Present requires a non-empty path")


@dataclass(frozen=True)
class TagEq(Pred):
    """True if the tagged object at ``path`` has tag ``tag``."""

    path: Union[str, tuple[str, ...]]
    tag: Any


@dataclass(frozen=True)
class And(Pred):
    parts: tuple[Pred, ...]

    def __init__(self, *parts: Pred):
        object.__setattr__(self, "parts", tuple(parts))


@dataclass(frozen=True)
class Or(Pred):
    parts: tuple[Pred, ...]

    def __init__(self, *parts: Pred):
        object.__setattr__(self, "parts", tuple(parts))


@dataclass(frozen=True)
class Not(Pred):
    part: Pred


@dataclass(frozen=True)
class Exists(Pred):
    """True if some element of the list at ``path`` satisfies ``pred``."""

    path: Union[str, tuple[str, ...]]
    pred: Pred


# ---------------------------------------------------------------------------
# Constraint pins (a "world" is a consistent tuple of pins)
#
# Pins are kept in tuples rather than sets because msgspec.inspect types
# are unhashable; deduplication relies on value equality.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _VariantPin:
    path: tuple[str, ...]
    struct: mi.StructType


@dataclass(frozen=True)
class _PresencePin:
    path: tuple[str, ...]
    present: bool


@dataclass(frozen=True)
class _EmptyPin:
    path: tuple[str, ...]


@dataclass(frozen=True)
class _ElemPin:
    path: tuple[str, ...]
    elem_id: int
    sub: tuple  # tuple of pins, paths absolute (rooted at the list path + "[]")


def _fmt_path(path: tuple[str, ...]) -> str:
    if not path:
        return "<root>"
    return ".".join(path)


def _extends(prefix: tuple[str, ...], path: tuple[str, ...]) -> bool:
    return len(path) > len(prefix) and path[: len(prefix)] == prefix


def _pin_conflict(p, q) -> Optional[str]:
    """Return a human-readable conflict reason between two pins, or None."""
    if (
        isinstance(p, _VariantPin)
        and isinstance(q, _VariantPin)
        and p.path == q.path
        and p.struct != q.struct
    ):
        return (
            f"conflicting variants required at '{_fmt_path(p.path)}': "
            f"tag {p.struct.tag!r} vs tag {q.struct.tag!r}"
        )
    if (
        isinstance(p, _PresencePin)
        and isinstance(q, _PresencePin)
        and p.path == q.path
        and p.present != q.present
    ):
        return f"field '{_fmt_path(p.path)}' cannot be both present and absent"
    if (
        isinstance(p, _EmptyPin)
        and isinstance(q, _ElemPin)
        or isinstance(p, _ElemPin)
        and isinstance(q, _EmptyPin)
    ) and p.path == q.path:
        return f"list '{_fmt_path(p.path)}' cannot be both empty and non-empty"
    # An absent field conflicts with any pin at or beneath it.
    for pres, other in ((p, q), (q, p)):
        if isinstance(pres, _PresencePin) and not pres.present:
            at_or_beneath = other.path == pres.path or _extends(pres.path, other.path)
            if at_or_beneath and not (isinstance(other, _PresencePin) and not other.present):
                return (
                    f"field '{_fmt_path(pres.path)}' is absent but another "
                    f"condition requires data at '{_fmt_path(other.path)}'"
                )
    return None


def _merge_pins(c1: tuple, c2: tuple) -> tuple[Optional[tuple], Optional[str]]:
    out = list(c1)
    for pin in c2:
        for existing in out:
            reason = _pin_conflict(pin, existing)
            if reason is not None:
                return None, reason
        if isinstance(pin, _ElemPin):
            hit = next(
                (e for e in out if isinstance(e, _ElemPin) and (e.path, e.elem_id) == (pin.path, pin.elem_id)),
                None,
            )
            if hit is not None:
                sub = list(hit.sub)
                for s in pin.sub:
                    if s not in sub:
                        sub.append(s)
                out[out.index(hit)] = _ElemPin(pin.path, pin.elem_id, tuple(sub))
                continue
        if pin not in out:
            out.append(pin)
    return tuple(out), None


def _merge_worlds(a: list, b: list) -> tuple[list, list]:
    merged, conflicts = [], []
    for x in a:
        for y in b:
            m, err = _merge_pins(x, y)
            if m is not None:
                merged.append(m)
            else:
                conflicts.append(err)
    return merged, conflicts


# ---------------------------------------------------------------------------
# Contract navigation over msgspec.inspect type info
# ---------------------------------------------------------------------------


def _field_of(struct: mi.StructType, encode_name: str):
    for f in struct.fields:
        if f.encode_name == encode_name:
            return f
    return None


def _expand_value(typ: mi.Type, path: tuple[str, ...]):
    """Expand a value type into ``(variant, pins)`` alternatives.

    Struct members of a union carry a pin recording the chosen variant.
    """
    if isinstance(typ, mi.Metadata):
        typ = typ.type
    if isinstance(typ, mi.UnionType):
        out = []
        for member in typ.types:
            if isinstance(member, mi.StructType):
                out.append((member, (_VariantPin(path, member),)))
            else:
                out.append((member, ()))
        return out
    return [(typ, ())]


def _resolve(variants, segments, base_path, assume=()):
    """Walk encoded field names from ``variants``.

    ``variants`` is a tuple of ``(type, pins)`` alternatives. Returns
    ``(terms, failures)`` where each term is ``(type, pins, path)`` for one
    alternative of the value at the end of the path. Union members that
    contradict a variant pin in ``assume`` are skipped.
    """
    failures = []
    terms = [(t, pins, base_path) for t, pins in variants]
    for seg in segments:
        new_terms = []
        for t, pins, path in terms:
            if isinstance(t, mi.Metadata):
                t = t.type
            if not isinstance(t, mi.StructType):
                failures.append(
                    f"'{_fmt_path(path)}' is not an object in the contract, "
                    f"so it has no field '{seg}'"
                )
                continue
            if t.array_like:
                failures.append(
                    f"'{_fmt_path(path)}' is an array-like struct, which is "
                    f"not supported by this planner"
                )
                continue
            fld = _field_of(t, seg)
            if fld is None:
                choices = ", ".join(repr(f.encode_name) for f in t.fields)
                failures.append(
                    f"no encoded field '{seg}' under '{_fmt_path(path)}' "
                    f"(declared fields: {choices})"
                )
                continue
            fpath = path + (seg,)
            assumed = _pin_at(assume, fpath, _VariantPin)
            for t2, pins2 in _expand_value(fld.type, fpath):
                if assumed is not None and t2 is not assumed.struct:
                    continue
                new_terms.append((t2, pins + pins2, fpath))
        terms = new_terms
    return terms, failures


# ---------------------------------------------------------------------------
# Solver: which worlds (pin tuples) make a predicate true / false, and why not
# ---------------------------------------------------------------------------


@dataclass
class _Worlds:
    true: list = dc_field(default_factory=list)
    false: list = dc_field(default_factory=list)
    true_reason: Optional[str] = None  # why no world makes it true
    false_reason: Optional[str] = None  # why no world makes it false


_elem_ids = itertools.count(1)


def _solve(pred: Pred, variants, path: tuple[str, ...], assume: tuple = ()) -> _Worlds:
    if isinstance(pred, Present):
        return _solve_present(pred, variants, path, assume)
    if isinstance(pred, TagEq):
        return _solve_tageq(pred, variants, path, assume)
    if isinstance(pred, And):
        return _solve_and(pred.parts, variants, path, assume)
    if isinstance(pred, Or):
        return _solve_or(pred.parts, variants, path, assume)
    if isinstance(pred, Not):
        w = _solve(pred.part, variants, path, assume)
        return _Worlds(w.false, w.true, w.false_reason, w.true_reason)
    if isinstance(pred, Exists):
        return _solve_exists(pred, variants, path, assume)
    raise TypeError(f"unsupported predicate: {pred!r}")


def _solve_present(pred: Present, variants, path, assume) -> _Worlds:
    segments = _segments(pred.path)
    parent, last = segments[:-1], segments[-1]
    terms, failures = _resolve(variants, parent, path, assume)
    out = _Worlds()
    missing_in = []
    declared: list[str] = []
    for t, pins, ppath in terms:
        if not isinstance(t, mi.StructType):
            failures.append(f"'{_fmt_path(ppath)}' is not an object in the contract")
            continue
        declared.extend(repr(f.encode_name) for f in t.fields)
        fld = _field_of(t, last)
        if fld is None:
            missing_in.append(t.tag or t.cls.__name__)
            # In this variant the field is never present.
            out.false.append(pins)
        elif fld.required:
            out.true.append(pins)
        else:
            fpath = ppath + (last,)
            out.true.append(pins + (_PresencePin(fpath, True),))
            out.false.append(pins + (_PresencePin(fpath, False),))
    full = _fmt_path(path + segments)
    if not out.true:
        if failures and not terms:
            out.true_reason = "; ".join(failures)
        elif missing_in:
            choices = ", ".join(dict.fromkeys(declared)) or "none"
            out.true_reason = (
                f"field '{full}' is not declared in "
                f"{', '.join(repr(m) for m in missing_in)} "
                f"(declared encoded fields: {choices})"
            )
        else:
            out.true_reason = (
                f"field '{full}' is required by the contract, so it is always "
                f"present (though it may still be explicitly null)"
            )
        if failures and terms:
            out.true_reason += "; " + "; ".join(failures)
    if not out.false:
        if failures and not terms:
            out.false_reason = "; ".join(failures)
        else:
            out.false_reason = (
                f"field '{full}' is required by the contract, so it can never "
                f"be absent (note: required-nullable fields may still be null, "
                f"but null is present)"
            )
    return out


def _solve_tageq(pred: TagEq, variants, path, assume) -> _Worlds:
    segments = _segments(pred.path)
    terms, failures = _resolve(variants, segments, path, assume)
    out = _Worlds()
    tags = []
    non_object = False
    for t, pins, ppath in terms:
        if isinstance(t, mi.StructType):
            tags.append(t.tag)
            (out.true if t.tag is not None and t.tag == pred.tag else out.false).append(pins)
        else:
            non_object = True
            out.false.append(pins)
    where = _fmt_path(path + segments)
    if not out.true:
        if failures and not terms:
            out.true_reason = "; ".join(failures)
        elif non_object and not tags:
            out.true_reason = f"value at '{where}' is not a tagged object"
        else:
            decl = ", ".join(repr(t) for t in tags) or "none"
            out.true_reason = (
                f"no variant at '{where}' is tagged {pred.tag!r} "
                f"(declared tags: {decl})"
            )
    if not out.false:
        out.false_reason = (
            f"every variant at '{where}' is tagged {pred.tag!r}, "
            f"so the test is always true"
        )
    return out


def _solve_and(parts, variants, path, assume) -> _Worlds:
    out = _Worlds(true=[()])
    child_worlds = [_solve(p, variants, path, assume) for p in parts]
    conflicts: list[str] = []
    for i, w in enumerate(child_worlds):
        out.true, errs = _merge_worlds(out.true, w.true)
        conflicts.extend(errs)
        if not out.true:
            if not w.true:
                out.true_reason = f"conjunct {i + 1} can never hold: {w.true_reason}"
            else:
                # Try to explain the conflict in contract terms: re-solve
                # each conjunct assuming the others, and report every one
                # that becomes impossible.
                explanations = []
                for j, part in enumerate(parts):
                    others: list = [()]
                    for k, w2 in enumerate(child_worlds):
                        if j != k:
                            others, _ = _merge_worlds(others, w2.true)
                    for pins in others:
                        merged, _ = _merge_pins(pins, assume)
                        if merged is None:
                            continue
                        re = _solve(part, variants, path, merged)
                        if not re.true and re.true_reason:
                            explanations.append(
                                f"under the other conjuncts, conjunct {j + 1} "
                                f"can never hold: {re.true_reason}"
                            )
                            break
                if explanations:
                    out.true_reason = "; ".join(dict.fromkeys(explanations))
                else:
                    out.true_reason = "conjuncts conflict: " + "; ".join(
                        dict.fromkeys(conflicts)
                    )
            break
    # false = at least one conjunct false
    for i, w in enumerate(child_worlds):
        others: list = [()]
        ok = True
        for j, w2 in enumerate(child_worlds):
            if i == j:
                continue
            others, _ = _merge_worlds(others, w2.true + w2.false)
            if not others:
                ok = False
                break
        if not ok:
            continue
        combos, _ = _merge_worlds(others, w.false)
        out.false.extend(combos)
    if not out.false:
        always = [
            f"conjunct {i + 1} is always true ({w.false_reason})"
            for i, w in enumerate(child_worlds)
            if not w.false
        ]
        out.false_reason = (
            "; ".join(always)
            if always
            else "the conjuncts cannot be falsified together"
        )
    return out


def _solve_or(parts, variants, path, assume) -> _Worlds:
    out = _Worlds(false=[()])
    reasons = []
    conflicts: list[str] = []
    child_worlds = [_solve(p, variants, path, assume) for p in parts]
    for w in child_worlds:
        out.true.extend(w.true)
        if w.true_reason:
            reasons.append(w.true_reason)
    for w in child_worlds:
        out.false, errs = _merge_worlds(out.false, w.false)
        conflicts.extend(errs)
        if not out.false:
            break
    if not out.true:
        out.true_reason = "; ".join(r for r in reasons if r) or None
    if not out.false:
        always = [w.false_reason for w in child_worlds if not w.false and w.false_reason]
        out.false_reason = (
            "; ".join(always)
            if always
            else "disjuncts conflict: " + "; ".join(dict.fromkeys(conflicts))
        )
    return out


def _solve_exists(pred: Exists, variants, path, assume) -> _Worlds:
    segments = _segments(pred.path)
    terms, failures = _resolve(variants, segments, path, assume)
    out = _Worlds()
    sub_reasons = []
    for t, pins, ppath in terms:
        if isinstance(t, mi.Metadata):
            t = t.type
        if not isinstance(t, (mi.ListType, mi.SetType, mi.VarTupleType)):
            failures.append(f"'{_fmt_path(ppath)}' is not a list in the contract")
            continue
        elem_path = ppath + ("[]",)
        elem_id = next(_elem_ids)
        for etype, epins in _expand_value(t.item_type, elem_path):
            w = _solve(pred.pred, ((etype, epins),), elem_path, assume)
            for wt in w.true:
                out.true.append(pins + (_ElemPin(ppath, elem_id, wt),))
            if t.min_length in (None, 0):
                out.false.append(pins + (_EmptyPin(ppath),))
            for wf in w.false:
                out.false.append(pins + (_ElemPin(ppath, elem_id, wf),))
            if not w.true and w.true_reason:
                sub_reasons.append(w.true_reason)
    where = _fmt_path(path + segments)
    if not out.true:
        if sub_reasons:
            out.true_reason = (
                f"no element of '{where}' can satisfy the condition: "
                + "; ".join(dict.fromkeys(sub_reasons))
            )
        elif failures:
            out.true_reason = "; ".join(failures)
        else:
            out.true_reason = f"no element of '{where}' can satisfy the condition"
    if not out.false:
        out.false_reason = (
            f"every element of '{where}' satisfies the condition and the "
            f"contract requires at least one element"
        )
    return out


# ---------------------------------------------------------------------------
# Witness construction
# ---------------------------------------------------------------------------


def _pin_at(pins, path, kind):
    for p in pins:
        if isinstance(p, kind) and p.path == path:
            return p
    return None


def _has_pins_at_or_beneath(pins, path) -> bool:
    return any(p.path == path or _extends(path, p.path) for p in pins)


def _visible_pins(pins, path, elem_sub):
    """Pins visible while building a list element: global pins not rooted at
    the element prefix, plus this element's own sub-pins."""
    prefix = path + ("[]",)
    out = [p for p in pins if not (p.path == prefix or _extends(prefix, p.path))]
    for s in elem_sub:
        if s not in out:
            out.append(s)
    return out


def _build(typ: mi.Type, pins, path: tuple[str, ...], in_union: bool):
    if isinstance(typ, mi.Metadata):
        typ = typ.type
    if isinstance(typ, mi.UnionType):
        pin = _pin_at(pins, path, _VariantPin)
        member = pin.struct if pin is not None else typ.types[0]
        return _build(member, pins, path, True)
    if isinstance(typ, mi.StructType):
        if typ.array_like:
            raise NotImplementedError("array-like structs are not supported")
        obj = {}
        if in_union and typ.tag is not None:
            obj[typ.tag_field] = typ.tag
        for f in typ.fields:
            fpath = path + (f.encode_name,)
            pres = _pin_at(pins, fpath, _PresencePin)
            if pres is not None and not pres.present:
                continue
            if f.required or _has_pins_at_or_beneath(pins, fpath):
                obj[f.encode_name] = _build(f.type, pins, fpath, False)
        return obj
    if isinstance(typ, (mi.ListType, mi.SetType, mi.VarTupleType)):
        if _pin_at(pins, path, _EmptyPin) is not None:
            items: list = []
        else:
            items = [
                _build(
                    typ.item_type,
                    _visible_pins(pins, path, p.sub),
                    path + ("[]",),
                    False,
                )
                for p in pins
                if isinstance(p, _ElemPin) and p.path == path
            ]
        while len(items) < (typ.min_length or 0):
            items.append(
                _build(typ.item_type, _visible_pins(pins, path, ()), path + ("[]",), False)
            )
        return items
    if isinstance(typ, mi.NoneType):
        return None
    if isinstance(typ, mi.BoolType):
        return False
    if isinstance(typ, mi.IntType):
        v = 0
        if typ.ge is not None:
            v = max(v, typ.ge)
        if typ.gt is not None:
            v = max(v, typ.gt + 1)
        if typ.multiple_of:
            v = -(-v // typ.multiple_of) * typ.multiple_of
        return v
    if isinstance(typ, mi.FloatType):
        v = 0.0
        if typ.ge is not None:
            v = max(v, float(typ.ge))
        if typ.gt is not None:
            v = max(v, float(typ.gt) + 1.0)
        return v
    if isinstance(typ, mi.StrType):
        return "x" * (typ.min_length or 0)
    if isinstance(typ, (mi.BytesType, mi.ByteArrayType, mi.MemoryViewType)):
        return b"x" * (typ.min_length or 0)
    if isinstance(typ, mi.EnumType):
        return next(iter(typ.cls)).value
    if isinstance(typ, mi.LiteralType):
        return typ.values[0]
    if isinstance(typ, mi.DateTimeType):
        tz = datetime.timezone.utc if typ.tz else None
        return datetime.datetime(2000, 1, 1, tzinfo=tz)
    if isinstance(typ, mi.DateType):
        return datetime.date(2000, 1, 1)
    if isinstance(typ, mi.TimeType):
        return datetime.time(0, 0)
    if isinstance(typ, mi.TimeDeltaType):
        return datetime.timedelta(0)
    if isinstance(typ, mi.UUIDType):
        return uuid.UUID(int=0)
    if isinstance(typ, mi.DecimalType):
        return decimal.Decimal(0)
    if isinstance(typ, (mi.DictType, mi.FrozenDictType)):
        return {}
    if isinstance(typ, mi.TupleType):
        return [
            _build(t, pins, path + (str(i),), False)
            for i, t in enumerate(typ.item_types)
        ]
    if isinstance(typ, mi.AnyType):
        return None
    raise NotImplementedError(f"cannot build a witness value for {typ!r}")


# ---------------------------------------------------------------------------
# Concrete predicate evaluation (used to validate witnesses)
# ---------------------------------------------------------------------------


def _variant_of(obj, typ: mi.UnionType):
    for m in typ.types:
        if isinstance(m, mi.StructType) and isinstance(obj, dict):
            if m.tag is not None and obj.get(m.tag_field) == m.tag:
                return m
    for m in typ.types:
        if isinstance(m, mi.StructType) and isinstance(obj, dict):
            return m
    return typ.types[0]


def _navigate(obj, typ, segments):
    for seg in segments:
        if isinstance(typ, mi.Metadata):
            typ = typ.type
        if isinstance(typ, mi.UnionType):
            typ = _variant_of(obj, typ)
        if not isinstance(typ, mi.StructType) or not isinstance(obj, dict):
            raise KeyError(seg)
        fld = _field_of(typ, seg)
        if fld is None or seg not in obj:
            raise KeyError(seg)
        obj = obj[seg]
        typ = fld.type
    return obj, typ


def evaluate(pred: Pred, obj, typ) -> bool:
    """Evaluate a predicate on a decoded-form object ``obj`` of type ``typ``.

    ``typ`` may be a msgspec type or an already-computed
    ``msgspec.inspect.Type``.
    """
    if not isinstance(typ, mi.Type):
        typ = mi.type_info(typ)
    if isinstance(typ, mi.Metadata):
        typ = typ.type
    if isinstance(pred, Present):
        segments = _segments(pred.path)
        try:
            parent, _ = _navigate(obj, typ, segments[:-1])
        except KeyError:
            return False
        return isinstance(parent, dict) and segments[-1] in parent
    if isinstance(pred, TagEq):
        segments = _segments(pred.path)
        try:
            value, vtyp = _navigate(obj, typ, segments)
        except KeyError:
            return False
        if isinstance(vtyp, mi.Metadata):
            vtyp = vtyp.type
        if isinstance(vtyp, mi.UnionType):
            vtyp = _variant_of(value, vtyp)
        return isinstance(vtyp, mi.StructType) and vtyp.tag == pred.tag
    if isinstance(pred, And):
        return all(evaluate(p, obj, typ) for p in pred.parts)
    if isinstance(pred, Or):
        return any(evaluate(p, obj, typ) for p in pred.parts)
    if isinstance(pred, Not):
        return not evaluate(pred.part, obj, typ)
    if isinstance(pred, Exists):
        segments = _segments(pred.path)
        try:
            value, vtyp = _navigate(obj, typ, segments)
        except KeyError:
            return False
        if isinstance(vtyp, mi.Metadata):
            vtyp = vtyp.type
        if not isinstance(vtyp, (mi.ListType, mi.SetType, mi.VarTupleType)):
            return False
        return any(evaluate(pred.pred, item, vtyp.item_type) for item in value)
    raise TypeError(f"unsupported predicate: {pred!r}")


# ---------------------------------------------------------------------------
# Planner API
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReadRule:
    """One candidate read: ``source`` feeds result ``column`` when ``guard``
    holds. Paths are dotted encoded field names; a ``None`` guard means the
    read is unconditional."""

    column: str
    source: str
    guard: Optional[Pred] = None


@dataclass
class PlannedRead:
    column: str
    source: str
    reachable: bool
    reason: Optional[str] = None
    witness: Optional[dict] = None
    note: Optional[str] = None


@dataclass
class ReadPlan:
    reads: list[PlannedRead]

    def sources_for(self, column: str) -> list[str]:
        """Reachable source paths (encoded names) for a result column."""
        return [r.source for r in self.reads if r.column == column and r.reachable]

    def pruned(self) -> list[PlannedRead]:
        return [r for r in self.reads if not r.reachable]

    def report(self) -> str:
        lines = []
        for r in self.reads:
            head = f"column {r.column!r} <- {r.source}"
            if r.reachable:
                lines.append(f"{head}\n  REACHABLE  witness: {r.witness!r}")
                if r.note:
                    lines.append(f"             note: {r.note}")
            else:
                lines.append(f"{head}\n  PRUNED     {r.reason}")
        return "\n".join(lines)


def _root_variants(info: mi.Type):
    if isinstance(info, mi.Metadata):
        info = info.type
    return tuple(_expand_value(info, ()))


def plan_reads(root_type, rules: Iterable[ReadRule]) -> ReadPlan:
    """Plan source reads for columnar extraction from ``root_type``.

    For each rule, decide statically (from the type contract alone) whether
    the guarded read can ever happen. Reachable reads carry a witness: a
    legal decoded-form input, validated with ``msgspec.convert``, that
    satisfies the guard and contains the source path. Pruned reads carry a
    contract-based reason.
    """
    info = mi.type_info(root_type)
    variants = _root_variants(info)
    reads: list[PlannedRead] = []
    for rule in rules:
        if not _segments(rule.source):
            raise ValueError(
                f"rule for column {rule.column!r} has an empty source path"
            )
        guard = rule.guard if rule.guard is not None else And()
        guard_worlds = _solve(guard, variants, ())
        if not guard_worlds.true:
            reads.append(
                PlannedRead(
                    rule.column,
                    rule.source,
                    reachable=False,
                    reason=f"guard can never hold: {guard_worlds.true_reason}",
                )
            )
            continue
        src = _solve(Present(rule.source), variants, ())
        if not src.true:
            reads.append(
                PlannedRead(
                    rule.column,
                    rule.source,
                    reachable=False,
                    reason=(
                        f"source path '{rule.source}' is not readable: "
                        f"{src.true_reason}"
                    ),
                )
            )
            continue
        worlds, conflicts = _merge_worlds(guard_worlds.true, src.true)
        if not worlds:
            detail = "; ".join(dict.fromkeys(conflicts)) or "unknown conflict"
            reads.append(
                PlannedRead(
                    rule.column,
                    rule.source,
                    reachable=False,
                    reason=(
                        f"guard conflicts with source path "
                        f"'{rule.source}': {detail}"
                    ),
                )
            )
            continue
        witness, note = None, None
        try:
            witness = _build(info, worlds[0], (), False)
            msgspec.convert(witness, type=root_type)
            if not evaluate(guard, witness, info):
                raise AssertionError("witness does not satisfy the guard")
            if not evaluate(Present(rule.source), witness, info):
                raise AssertionError("witness does not contain the source path")
        except Exception as exc:  # keep the read, but be honest about it
            witness, note = None, f"witness generation failed: {exc}"
        reads.append(
            PlannedRead(
                rule.column,
                rule.source,
                reachable=True,
                witness=witness,
                note=note,
            )
        )
    return ReadPlan(reads)
