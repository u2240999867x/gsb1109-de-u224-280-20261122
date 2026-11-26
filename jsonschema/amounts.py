"""
Exact, decimal-safe auditing of monetary amount contracts.

A regular JSON Schema validation only answers whether a single JSON number
satisfies a schema.  An amount contract additionally needs to know:

* which amounts inside a finite audit window are genuinely contractually
  admissible, presented as queryable segments rather than enumerated points,
* whether every admissible amount can be registered losslessly in a warehouse
  that only accepts multiples of a fixed smallest unit,
* and why a particular amount is rejected: out of bounds, off the contract's
  step, or simultaneously matching two branches of a ``oneOf`` (mutually
  exclusive tiers that overlap on some cent amounts).

All arithmetic is done with `decimal.Decimal`.  Schemas are expected to be
loaded with ``json.loads(..., parse_float=decimal.Decimal)`` so that the
contract's original decimal literals are preserved; equivalent spellings such
as ``0.03``, ``0.030`` and ``3E-2`` then produce identical results.

The supported schema dialect is deliberately narrow: a single numeric value
constrained by ``minimum`` / ``maximum`` / ``exclusiveMinimum`` /
``exclusiveMaximum``, ``multipleOf``, ``type`` and the applicators
``allOf`` / ``anyOf`` / ``oneOf``.  Any other keyword raises
`jsonschema.exceptions.SchemaError`.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from math import gcd
from typing import Any

from attrs import define, field

from jsonschema.exceptions import SchemaError

_AMOUNT_KEYWORDS = frozenset(
    {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "type",
        "allOf",
        "anyOf",
        "oneOf",
    },
)
_SUPPORTED_TYPES = frozenset({"number", "integer"})
_APPLICATORS = ("allOf", "anyOf", "oneOf")
_BOUND_KEYWORDS = (
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
)


class AmountStatus(str, Enum):
    """
    Verdict returned for an amount or an amount segment.
    """

    ADMISSIBLE = "admissible"
    OUT_OF_RANGE = "out_of_range"
    STEP_MISMATCH = "step_mismatch"
    EXCLUSIVE_OVERLAP = "exclusive_overlap"


@define(frozen=True)
class AmountAdmission:
    """
    The verdict for a single queried amount.

    ``registry_compatible`` is ``True`` or ``False`` for admissible amounts
    depending on whether the amount is an exact multiple of the warehouse
    unit, and ``None`` when the contract itself rejects the amount.
    """

    value: Decimal
    status: AmountStatus
    accepted: bool
    registry_compatible: bool | None
    reason: str
    matched_branches: tuple[str, ...] = field(default=())
    conflicting_branches: tuple[str, ...] = field(default=())


@define(frozen=True)
class AmountSegment:
    """
    One lattice of the partitioned audit window.

    A segment covers the lattice points ``start + k * step`` for
    ``start <= ... <= end``; all of them share one status and one warehouse
    verdict.  The segments partition the window's grid points exactly once.
    """

    start: Decimal
    end: Decimal
    step: Decimal
    status: AmountStatus
    reason: str
    registry_compatible: bool | None = None
    registry_counterexample: Decimal | None = None


@define(frozen=True)
class _Constraint:
    """
    A conjunctive numeric constraint, kept in exact decimals.
    """

    lower: Decimal | None = None
    lower_inclusive: bool = True
    upper: Decimal | None = None
    upper_inclusive: bool = True
    step: Decimal | None = None
    integer_only: bool = False

    def combined_with(self, other: _Constraint) -> _Constraint:
        """
        Return the conjunction of this constraint with another one.
        """
        lower = self.lower
        lower_inclusive = self.lower_inclusive
        if other.lower is not None:
            if lower is None or other.lower > lower:
                lower = other.lower
                lower_inclusive = other.lower_inclusive
            elif other.lower == lower:
                lower_inclusive = (
                    self.lower_inclusive and other.lower_inclusive
                )

        upper = self.upper
        upper_inclusive = self.upper_inclusive
        if other.upper is not None:
            if upper is None or other.upper < upper:
                upper = other.upper
                upper_inclusive = other.upper_inclusive
            elif other.upper == upper:
                upper_inclusive = (
                    self.upper_inclusive and other.upper_inclusive
                )

        step = self.step
        if other.step is not None:
            step = (
                other.step
                if step is None
                else _lcm_decimal(step, other.step)
            )

        return _Constraint(
            lower=lower,
            lower_inclusive=lower_inclusive,
            upper=upper,
            upper_inclusive=upper_inclusive,
            step=step,
            integer_only=self.integer_only or other.integer_only,
        )


@define(frozen=True)
class _Record:
    """
    One disjunctive leaf (a tier) of the schema, mapped onto the integer grid.
    """

    lower: int
    upper: int
    step: int | None


def _as_decimal(value: Any, *, what: str) -> Decimal:
    """
    Convert one JSON numeric value to an exact finite Decimal.
    """
    if isinstance(value, bool) or not isinstance(
        value,
        (int, float, Decimal),
    ):
        raise SchemaError(f"The {what} must be a JSON number, got {value!r}.")
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite():
        raise SchemaError(f"The {what} must be finite, got {value!r}.")
    return decimal


def _lcm_decimal(left: Decimal, right: Decimal) -> Decimal:
    """
    Return the least common multiple of two positive finite decimals.

    For fractions ``a / b`` and ``c / d`` the least common multiple is
    ``lcm(a, c) / gcd(b, d)``; the result stays an exact decimal whenever both
    operands are decimals.
    """
    left_fraction = Fraction(left)
    right_fraction = Fraction(right)
    result = (
        left_fraction.numerator
        * right_fraction.numerator
        // gcd(left_fraction.numerator, right_fraction.numerator)
    )
    denominator = gcd(
        left_fraction.denominator,
        right_fraction.denominator,
    )
    return Decimal(result) / Decimal(denominator)


def _grid_unit(decimals: list[Decimal]) -> Decimal:
    """
    Find the finest decimal place used by any literal.

    The grid is the place value ``10**-p`` of the most precise literal rather
    than the greatest common divisor of the literal coefficients: a tier with
    no ``multipleOf`` accepts every grid point (including e.g. ``0.01`` next
    to a ``0.02`` warehouse unit), so coarsening the grid via a common divisor
    would hide genuine warehouse counterexamples.
    """
    finest = min(each.as_tuple().exponent for each in decimals)
    return Decimal(1).scaleb(finest)


def _local_constraint(schema: Mapping[str, Any]) -> _Constraint:
    """
    Extract the conjunctive constraint carried by one schema object.
    """
    type_name = schema.get("type")
    if type_name is not None and type_name not in _SUPPORTED_TYPES:
        raise SchemaError(
            "Amount contracts only support type "
            f"{sorted(_SUPPORTED_TYPES)!r}, got {type_name!r}.",
        )
    if isinstance(
        schema.get("exclusiveMinimum"),
        bool,
    ) or isinstance(schema.get("exclusiveMaximum"), bool):
        raise SchemaError(
            "Boolean exclusiveMinimum/exclusiveMaximum (draft 4 style) are "
            "not supported; use numeric bounds instead.",
        )

    constraint = _Constraint(integer_only=type_name == "integer")
    if "minimum" in schema:
        constraint = constraint.combined_with(
            _Constraint(
                lower=_as_decimal(schema["minimum"], what="minimum"),
            ),
        )
    if "maximum" in schema:
        constraint = constraint.combined_with(
            _Constraint(
                upper=_as_decimal(schema["maximum"], what="maximum"),
            ),
        )
    if "exclusiveMinimum" in schema:
        constraint = constraint.combined_with(
            _Constraint(
                lower=_as_decimal(
                    schema["exclusiveMinimum"],
                    what="exclusiveMinimum",
                ),
                lower_inclusive=False,
            ),
        )
    if "exclusiveMaximum" in schema:
        constraint = constraint.combined_with(
            _Constraint(
                upper=_as_decimal(
                    schema["exclusiveMaximum"],
                    what="exclusiveMaximum",
                ),
                upper_inclusive=False,
            ),
        )
    if "multipleOf" in schema:
        step = _as_decimal(schema["multipleOf"], what="multipleOf")
        if step <= 0:
            raise SchemaError(
                "multipleOf must be strictly positive, got "
                f"{schema['multipleOf']!r}.",
            )
        constraint = constraint.combined_with(_Constraint(step=step))
    return constraint


def _validate_keywords(schema: Any) -> None:
    """
    Reject any keyword outside the supported amount-contract dialect.
    """
    if isinstance(schema, bool):
        return
    if not isinstance(schema, Mapping):
        raise SchemaError(
            f"Schemas must be objects or booleans, got {schema!r}.",
        )
    unknown = set(schema) - _AMOUNT_KEYWORDS
    if unknown:
        raise SchemaError(
            "Amount contracts only support the keywords "
            f"{sorted(_AMOUNT_KEYWORDS)!r}; unsupported: {sorted(unknown)!r}.",
        )
    if isinstance(schema.get("type"), list):
        raise SchemaError(
            "Arrays of types are not supported; use a single numeric type.",
        )
    for keyword in _APPLICATORS:
        if keyword in schema:
            subschemas = schema[keyword]
            if not isinstance(subschemas, list) or not subschemas:
                raise SchemaError(
                    f"{keyword} must be a non-empty array of schemas.",
                )
            for subschema in subschemas:
                _validate_keywords(subschema)


def _collect_decimals(schema: Any, sink: list[Decimal]) -> None:
    """
    Gather every numeric literal in the schema into ``sink``.
    """
    if isinstance(schema, bool):
        return
    for keyword in _BOUND_KEYWORDS:
        if keyword in schema:
            sink.append(_as_decimal(schema[keyword], what=keyword))
    for keyword in _APPLICATORS:
        for subschema in schema.get(keyword, ()):
            _collect_decimals(subschema, sink)


def _disjuncts(
    schema: Any,
    base: _Constraint,
) -> list[_Constraint]:
    """
    Expand the schema into its disjunctive conjunctive leaves.

    ``allOf`` distributes over the surrounding alternatives while ``anyOf``
    and ``oneOf`` create new branches.  Only the numeric constraints matter
    here: the exclusive-overlap semantics are resolved separately by
    `_diagnose`.
    """
    if schema is True:
        return [base]
    if schema is False:
        return []

    current = base.combined_with(_local_constraint(schema))
    results = [current]
    for subschema in schema.get("allOf", ()):
        results = [
            leaf
            for constraint in results
            for leaf in _disjuncts(subschema, constraint)
        ]
    for keyword in ("anyOf", "oneOf"):
        if keyword not in schema:
            continue
        branched: list[_Constraint] = []
        for subschema in schema[keyword]:
            for constraint in results:
                branched.extend(_disjuncts(subschema, constraint))
        results = branched
    return results


_FAILURE_RANK = {
    AmountStatus.OUT_OF_RANGE: 0,
    AmountStatus.STEP_MISMATCH: 1,
    AmountStatus.EXCLUSIVE_OVERLAP: 2,
}


@define(frozen=True)
class _Diagnosis:
    """
    The exact diagnostic verdict for one value against one schema node.
    """

    accepted: bool
    status: AmountStatus | None
    matched: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()


def _diagnose(
    schema: Any,
    value: Decimal,
    path: tuple[str, ...],
) -> _Diagnosis:
    """
    Explain exactly why a value is accepted or rejected at a schema node.

    The node's conjunctive parts (its own bounds/step and every ``allOf``
    child) are all inspected, and the weakest failure wins: an out-of-range
    rejection must not be masked by a deeper step or exclusive-overlap
    failure.  Only once every conjunctive part passes are the disjunctive
    ``anyOf`` / ``oneOf`` parts evaluated; there a zero-match reports the most
    specific child failure, while a multi-match ``oneOf`` is itself the
    exclusive overlap.
    """
    if schema is True:
        return _Diagnosis(
            accepted=True,
            status=None,
            matched=(".".join(path),) if path else ("$",),
        )
    if schema is False:
        return _Diagnosis(
            accepted=False,
            status=AmountStatus.OUT_OF_RANGE,
        )

    conjunctive_failures = []
    local_status = _local_failure(_local_constraint(schema), value)
    if local_status is not None:
        conjunctive_failures.append(
            _Diagnosis(accepted=False, status=local_status),
        )
    for index, each in enumerate(schema.get("allOf", ())):
        child = _diagnose(
            each,
            value,
            path + (f"allOf[{index}]",),
        )
        if not child.accepted:
            conjunctive_failures.append(child)
    if conjunctive_failures:
        return _weakest_failure(conjunctive_failures)

    if "anyOf" in schema:
        children = [
            _diagnose(
                each,
                value,
                path + (f"anyOf[{index}]",),
            )
            for index, each in enumerate(schema["anyOf"])
        ]
        accepted = [each for each in children if each.accepted]
        if not accepted:
            return _strongest_failure(children)
        matched = tuple(
            branch for each in accepted for branch in each.matched
        )
    else:
        matched = (".".join(path),) if path else ("$",)

    if "oneOf" in schema:
        children = [
            _diagnose(
                each,
                value,
                path + (f"oneOf[{index}]",),
            )
            for index, each in enumerate(schema["oneOf"])
        ]
        accepted = [each for each in children if each.accepted]
        if len(accepted) > 1:
            conflicts = tuple(
                branch
                for each in accepted
                for branch in each.matched
            )
            return _Diagnosis(
                accepted=False,
                status=AmountStatus.EXCLUSIVE_OVERLAP,
                conflicts=conflicts,
            )
        if len(accepted) == 1:
            matched = accepted[0].matched
        else:
            return _strongest_failure(children)

    return _Diagnosis(accepted=True, status=None, matched=matched)


def _local_failure(
    constraint: _Constraint,
    value: Decimal,
) -> AmountStatus | None:
    """
    Return the status of a local constraint failure, or ``None`` on success.
    """
    if constraint.lower is not None:
        if constraint.lower_inclusive:
            if value < constraint.lower:
                return AmountStatus.OUT_OF_RANGE
        elif value <= constraint.lower:
            return AmountStatus.OUT_OF_RANGE
    if constraint.upper is not None:
        if constraint.upper_inclusive:
            if value > constraint.upper:
                return AmountStatus.OUT_OF_RANGE
        elif value >= constraint.upper:
            return AmountStatus.OUT_OF_RANGE
    if constraint.step is not None and value % constraint.step != 0:
        return AmountStatus.STEP_MISMATCH
    if constraint.integer_only and value != value.to_integral_value():
        return AmountStatus.STEP_MISMATCH
    return None


def _weakest_failure(children: list[_Diagnosis]) -> _Diagnosis:
    """
    Merge conjunctive failures by keeping the weakest (lowest-rank) reason.
    """
    weakest = min(children, key=lambda each: _FAILURE_RANK[each.status])
    return _Diagnosis(
        accepted=False,
        status=weakest.status,
        conflicts=weakest.conflicts,
    )


def _strongest_failure(children: list[_Diagnosis]) -> _Diagnosis:
    """
    Merge disjunctive failures by keeping the strongest (highest-rank) reason.
    """
    failures = [each for each in children if not each.accepted]
    strongest = max(
        failures,
        key=lambda each: _FAILURE_RANK[each.status],
    )
    return _Diagnosis(
        accepted=False,
        status=strongest.status,
        conflicts=strongest.conflicts,
    )


def _lcm_int(left: int, right: int) -> int:
    """
    Return the least common multiple of two positive integers.
    """
    return left * right // gcd(left, right)


def _to_grid(value: Decimal, grid: Decimal) -> int:
    """
    Map an exact grid-aligned decimal to its integer grid coordinate.
    """
    quotient = value / grid
    if quotient != quotient.to_integral_value():
        raise SchemaError(
            f"{value} is not aligned to the contract's grid unit {grid}.",
        )
    return int(quotient)


@define(frozen=True)
class AmountContract:
    """
    An audited monetary amount contract.

    Instances are produced by `audit_amount_contract`; they bind one schema to
    a finite audit window and a warehouse unit, answer point queries via
    `classify`, segment queries via `segments` and warehouse registration
    counterexamples via `warehouse_counterexample`.
    """

    schema: Mapping[str, Any] | bool
    unit: Decimal
    grid_unit: Decimal
    window_lower: Decimal
    window_upper: Decimal
    window_lower_inclusive: bool
    window_upper_inclusive: bool
    records: tuple[_Record, ...]
    window_lower_int: int
    window_upper_int: int
    unit_int: int
    integer_stride: int
    segments_: tuple[AmountSegment, ...] = field(default=())

    def classify(self, amount: Decimal | int | float) -> AmountAdmission:
        """
        Return the exact admission verdict for one amount.
        """
        value = _as_decimal(amount, what="amount")
        if not self._in_window(value):
            return AmountAdmission(
                value=value,
                status=AmountStatus.OUT_OF_RANGE,
                accepted=False,
                registry_compatible=None,
                reason=(
                    f"{value} is outside the audited window "
                    f"{self._window_repr()}."
                ),
            )
        if self.schema is False:
            return AmountAdmission(
                value=value,
                status=AmountStatus.OUT_OF_RANGE,
                accepted=False,
                registry_compatible=None,
                reason="The contract schema is false and accepts no amounts.",
            )

        diagnosis = _diagnose(self.schema, value, ())

        if diagnosis.accepted:
            registry = value % self.unit == 0
            return AmountAdmission(
                value=value,
                status=AmountStatus.ADMISSIBLE,
                accepted=True,
                registry_compatible=registry,
                reason=(
                    f"{value} is contractually admissible and "
                    + (
                        "can be registered losslessly in the warehouse "
                        f"unit {self.unit}."
                        if registry
                        else "is not an exact multiple of the warehouse "
                        f"unit {self.unit}, so it cannot be registered "
                        "losslessly."
                    )
                ),
                matched_branches=diagnosis.matched,
            )

        if diagnosis.status is AmountStatus.EXCLUSIVE_OVERLAP:
            return AmountAdmission(
                value=value,
                status=AmountStatus.EXCLUSIVE_OVERLAP,
                accepted=False,
                registry_compatible=None,
                reason=(
                    f"{value} simultaneously matches mutually exclusive "
                    f"tiers: {', '.join(diagnosis.conflicts)}."
                ),
                conflicting_branches=diagnosis.conflicts,
            )
        if diagnosis.status is AmountStatus.STEP_MISMATCH:
            return AmountAdmission(
                value=value,
                status=AmountStatus.STEP_MISMATCH,
                accepted=False,
                registry_compatible=None,
                reason=(
                    f"{value} is inside a tier's bounds but is not an exact "
                    "multiple of that tier's required step."
                ),
            )
        return AmountAdmission(
            value=value,
            status=AmountStatus.OUT_OF_RANGE,
            accepted=False,
            registry_compatible=None,
            reason=(
                f"{value} lies outside every tier's open/closed bounds "
                f"within the audited window {self._window_repr()}."
            ),
        )

    def segments(self) -> tuple[AmountSegment, ...]:
        """
        Return the queryable, point-enumeration-free admission lattices.

        The segments partition the audit window's exact decimal grid: every
        grid point belongs to exactly one lattice, and each lattice carries a
        single status and a single warehouse verdict.
        """
        return self.segments_

    def segment_status_at(
        self,
        amount: Decimal | int | float,
    ) -> AmountStatus:
        """
        Return the status of one amount from the partitioned lattice segments.

        The segments partition the audit window by grid position modulo their
        period, so exactly one segment contains each grid point.  This is the
        same verdict as `classify`, resolved through the segments rather than
        the recursive schema walk.
        """
        value = _as_decimal(amount, what="amount")
        if not self._in_window(value):
            return AmountStatus.OUT_OF_RANGE
        for segment in self.segments_:
            if value < segment.start or value > segment.end:
                continue
            if (value - segment.start) % segment.step == 0:
                return segment.status
        return AmountStatus.OUT_OF_RANGE

    def warehouse_counterexample(self) -> Decimal | None:
        """
        Return the smallest admissible amount the warehouse cannot register.

        Returns ``None`` when every contractually admissible amount is an
        exact multiple of the warehouse unit.
        """
        candidates = [
            segment.registry_counterexample
            for segment in self.segments_
            if segment.status is AmountStatus.ADMISSIBLE
            and segment.registry_counterexample is not None
        ]
        return min(candidates) if candidates else None

    def _in_window(self, value: Decimal) -> bool:
        """
        Check the open/closed audit window membership of one value.
        """
        if self.window_lower_inclusive:
            if value < self.window_lower:
                return False
        elif value <= self.window_lower:
            return False
        if self.window_upper_inclusive:
            return value <= self.window_upper
        return value < self.window_upper

    def _window_repr(self) -> str:
        """
        Render the audit window with its open/closed bracket notation.
        """
        left = "[" if self.window_lower_inclusive else "("
        right = "]" if self.window_upper_inclusive else ")"
        return f"{left}{self.window_lower}, {self.window_upper}{right}"


def audit_amount_contract(
    schema: Mapping[str, Any] | bool,
    *,
    lower: Decimal | int | float,
    upper: Decimal | int | float,
    unit: Decimal | int | float,
    lower_inclusive: bool = True,
    upper_inclusive: bool = True,
) -> AmountContract:
    """
    Audit one amount contract over a finite window and a warehouse unit.

    ``schema`` is a JSON Schema restricted to numeric bounds, ``multipleOf``
    and the ``allOf`` / ``anyOf`` / ``oneOf`` applicators.  Load it with
    ``json.loads(..., parse_float=decimal.Decimal)`` to retain the contract's
    original decimal literals.

    ``lower`` / ``upper`` define the finite audit window (with explicit
    inclusivity) and ``unit`` is the warehouse's smallest registrable unit.
    The returned `AmountContract` exposes exact point verdicts, segmented
    admission lattices and a precise warehouse counterexample.  The window may
    be far too large to enumerate cent points: all results are computed from
    bounds and lattice steps only.
    """
    _validate_keywords(schema)
    lower_decimal = _as_decimal(lower, what="audit lower bound")
    upper_decimal = _as_decimal(upper, what="audit upper bound")
    unit_decimal = _as_decimal(unit, what="warehouse unit")
    if lower_decimal >= upper_decimal:
        raise SchemaError(
            "The audit lower bound must be below the upper bound.",
        )
    if unit_decimal <= 0:
        raise SchemaError("The warehouse unit must be strictly positive.")

    literals: list[Decimal] = [lower_decimal, upper_decimal, unit_decimal]
    if not isinstance(schema, bool):
        _collect_decimals(schema, literals)
        if _has_integer_type(schema):
            literals.append(Decimal(1))
    grid = _grid_unit(literals)

    window_lower_int = _to_grid(lower_decimal, grid)
    window_upper_int = _to_grid(upper_decimal, grid)
    if not lower_inclusive:
        window_lower_int += 1
    if not upper_inclusive:
        window_upper_int -= 1
    if window_lower_int > window_upper_int:
        raise SchemaError("The audit window contains no grid points.")
    unit_int = _to_grid(unit_decimal, grid)
    integer_stride = _to_grid(Decimal(1), grid)

    records = []
    seen: set[tuple[int, int, int | None]] = set()
    if schema is not False:
        for constraint in _disjuncts(schema, _Constraint()):
            record = _build_record(
                constraint,
                grid,
                window_lower_int,
                window_upper_int,
                integer_stride,
            )
            if record is None:
                continue
            key = (record.lower, record.upper, record.step)
            if key not in seen:
                seen.add(key)
                records.append(record)

    contract = AmountContract(
        schema=schema,
        unit=unit_decimal,
        grid_unit=grid,
        window_lower=lower_decimal,
        window_upper=upper_decimal,
        window_lower_inclusive=lower_inclusive,
        window_upper_inclusive=upper_inclusive,
        records=tuple(records),
        window_lower_int=window_lower_int,
        window_upper_int=window_upper_int,
        unit_int=unit_int,
        integer_stride=integer_stride,
    )
    object.__setattr__(
        contract,
        "segments_",
        _build_segments(contract),
    )
    return contract


def _has_integer_type(schema: Any) -> bool:
    """
    Check whether any schema object constrains the value to integers.
    """
    if isinstance(schema, bool):
        return False
    if schema.get("type") == "integer":
        return True
    return any(
        _has_integer_type(subschema)
        for keyword in _APPLICATORS
        for subschema in schema.get(keyword, ())
    )


def _build_record(
    constraint: _Constraint,
    grid: Decimal,
    window_lower: int,
    window_upper: int,
    integer_stride: int,
) -> _Record | None:
    """
    Convert one conjunctive leaf into a clipped integer-grid record.
    """
    lower = window_lower
    if constraint.lower is not None:
        bound = _to_grid(constraint.lower, grid)
        if not constraint.lower_inclusive:
            bound += 1
        lower = max(lower, bound)
    upper = window_upper
    if constraint.upper is not None:
        bound = _to_grid(constraint.upper, grid)
        if not constraint.upper_inclusive:
            bound -= 1
        upper = min(upper, bound)
    if lower > upper:
        return None

    step = (
        None
        if constraint.step is None
        else _to_grid(constraint.step, grid)
    )
    if constraint.integer_only:
        step = integer_stride if step is None else _lcm_int(
            step,
            integer_stride,
        )
    return _Record(lower=lower, upper=upper, step=step)


def _build_segments(contract: AmountContract) -> tuple[AmountSegment, ...]:
    """
    Partition the window into exact, non-overlapping admission lattices.

    Slab boundaries are the tier bounds, so which records bound a point is
    constant inside a slab.  Within a slab every relevant condition (tier
    steps and the warehouse unit) is periodic; their joint pattern therefore
    repeats with a period equal to the least common multiple of those steps.
    Only one period is probed point by point -- the probe cost depends on the
    step sizes, never on the audit window width -- and each period offset is
    emitted directly as a width-independent lattice segment.
    """
    records = contract.records
    grid = contract.grid_unit
    boundaries = {
        contract.window_lower_int,
        contract.window_upper_int + 1,
    }
    for record in records:
        boundaries.add(record.lower)
        boundaries.add(record.upper + 1)
    ordered = sorted(boundaries)

    segments: list[AmountSegment] = []
    for index, slab_lower in enumerate(ordered[:-1]):
        slab_upper = ordered[index + 1] - 1
        active = [
            record
            for record in records
            if record.lower <= slab_lower and record.upper >= slab_upper
        ]
        if not active:
            segments.append(
                AmountSegment(
                    start=Decimal(slab_lower) * grid,
                    end=Decimal(slab_upper) * grid,
                    step=grid,
                    status=AmountStatus.OUT_OF_RANGE,
                    reason=_segment_reason(AmountStatus.OUT_OF_RANGE),
                ),
            )
            continue
        segments.extend(
            _slab_segments(
                contract,
                active,
                slab_lower,
                slab_upper,
            ),
        )
    return tuple(segments)


def _slab_segments(
    contract: AmountContract,
    active: list[_Record],
    slab_lower: int,
    slab_upper: int,
) -> list[AmountSegment]:
    """
    Probe one bounded period and express it as width-independent lattices.

    For each offset inside one period, every grid point carrying that offset
    repeats every ``period`` grid units with the same status and warehouse
    verdict.  Each offset is therefore emitted directly as a lattice segment
    whose step is the period itself and whose end is clipped to the slab; no
    grid point of the (potentially enormous) slab is ever enumerated.
    """
    grid = contract.grid_unit
    strides = {
        record.step if record.step is not None else 1 for record in active
    }
    period = 1
    for stride in strides:
        period = _lcm_int(period, stride)
    period = _lcm_int(period, contract.unit_int)

    length = slab_upper - slab_lower + 1
    pattern = [
        _point_key(
            contract,
            Decimal(slab_lower + offset) * grid,
        )
        for offset in range(min(period, length))
    ]

    segments: list[AmountSegment] = []
    for offset, (status, registry) in enumerate(pattern):
        first = slab_lower + offset
        if first > slab_upper:
            continue
        last = first + (slab_upper - first) // period * period
        segments.append(
            AmountSegment(
                start=Decimal(first) * grid,
                end=Decimal(last) * grid,
                step=Decimal(period) * grid,
                status=status,
                reason=_segment_reason(status),
                registry_compatible=registry,
                registry_counterexample=(
                    Decimal(first) * grid
                    if status is AmountStatus.ADMISSIBLE
                    and registry is False
                    else None
                ),
            ),
        )
    return segments


def _segment_reason(status: AmountStatus) -> str:
    """
    Return the value-independent explanation shared by a whole lattice.
    """
    if status is AmountStatus.ADMISSIBLE:
        return (
            "Every lattice point of this segment matches exactly one "
            "contract branch and is contractually admissible."
        )
    if status is AmountStatus.EXCLUSIVE_OVERLAP:
        return (
            "Every lattice point of this segment matches two mutually "
            "exclusive oneOf tiers at once and is rejected."
        )
    if status is AmountStatus.STEP_MISMATCH:
        return (
            "Every lattice point of this segment lies inside tier bounds "
            "but is off the required step."
        )
    return (
        "Every lattice point of this segment lies outside the contract's "
        "open/closed bounds."
    )


def _point_key(
    contract: AmountContract,
    value: Decimal,
) -> tuple[AmountStatus, bool | None]:
    """
    Compute the canonical (status, registry) key for one grid point.
    """
    admission = contract.classify(value)
    return (
        admission.status,
        admission.registry_compatible,
    )
