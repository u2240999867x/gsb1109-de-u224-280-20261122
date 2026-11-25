"""Exact decimal support for the SQLite backend.

SQLite has no exact decimal type. Tortoise stores :class:`~tortoise.fields.DecimalField`
values as text, but the historical query path cast them to ``NUMERIC`` for comparisons,
ordering and aggregation.  SQLite then converts those values to IEEE-754 doubles, which
silently rounds amounts larger than the 53-bit mantissa (for example a value of
``92233720368547758.07``).

This module keeps the text storage (so existing batches stay valid and the regular ORM
can keep writing/correcting rows) while making comparison, ordering and ``SUM`` exact:

* a collation parses the stored text as :class:`~decimal.Decimal` and compares exactly;
* a user defined aggregate accumulates integer coefficients so the total can grow past
  the range of a signed 64 bit integer without losing a cent.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from pypika_tortoise import functions
from pypika_tortoise.context import SqlContext
from pypika_tortoise.terms import Case, Field, NullValue, Term
from pypika_tortoise.utils import format_alias_sql

DECIMAL_COLLATION = "tortoise_decimal"
DECIMAL_SUM_FUNCTION = "TORTOISE_DECIMAL_SUM"


def _as_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def decimal_collation(left: object, right: object) -> int:
    """Compare two text encoded decimal values exactly."""
    try:
        left_decimal = _as_decimal(left)
        right_decimal = _as_decimal(right)
    except (InvalidOperation, ValueError, TypeError):
        # Not a decimal value; fall back to a stable textual ordering.
        left_text, right_text = str(left), str(right)
        return (left_text > right_text) - (left_text < right_text)
    return (left_decimal > right_decimal) - (left_decimal < right_decimal)


def _integer_and_exponent(value: object) -> tuple[int, int]:
    """Return ``(coefficient, exponent)`` so that ``value == coefficient * 10**exponent``."""
    decimal_value = _as_decimal(value)
    sign, digits, exponent = decimal_value.as_tuple()
    if not isinstance(exponent, int):  # NaN / Infinity
        return 0, 0
    coefficient = int("".join(map(str, digits))) if digits else 0
    return (-coefficient if sign else coefficient), exponent


def _format_decimal(coefficient: int, exponent: int) -> str:
    sign = "-" if coefficient < 0 else ""
    digits = str(abs(coefficient))
    if exponent == 0:
        return f"{sign}{digits}"
    if exponent > 0:
        return f"{sign}{digits}{'0' * exponent}"

    places = -exponent
    if len(digits) <= places:
        digits = "0" * (places - len(digits) + 1) + digits
    split = len(digits) - places
    return f"{sign}{digits[:split]}.{digits[split:]}"


class DecimalSumAggregate:
    """Exact sum of text encoded decimals using integer arithmetic."""

    def __init__(self) -> None:
        self.coefficient = 0
        self.exponent = 0
        self.seen = False

    def step(self, value: object) -> None:
        if value is None:
            return
        coefficient, exponent = _integer_and_exponent(value)
        if not self.seen:
            self.coefficient, self.exponent = coefficient, exponent
            self.seen = True
        elif exponent < self.exponent:
            self.coefficient = self.coefficient * 10 ** (self.exponent - exponent) + coefficient
            self.exponent = exponent
        else:
            self.coefficient += coefficient * 10 ** (exponent - self.exponent)

    def finalize(self) -> str | None:
        if not self.seen:
            return None
        return _format_decimal(self.coefficient, self.exponent)


class DecimalCollate(Term):
    """``term COLLATE tortoise_decimal`` so SQLite compares/orders numerically."""

    def __init__(self, term: Term, alias: str | None = None) -> None:
        super().__init__(alias)
        self.term = term
        # ``is_aggregate`` drives HAVING vs WHERE resolution, so propagate it.
        self.is_aggregate = getattr(term, "is_aggregate", False)

    def get_sql(self, ctx: SqlContext) -> str:
        inner = self.term.get_sql(ctx.copy(with_alias=False))
        sql = f"{inner} COLLATE {DECIMAL_COLLATION}"
        if ctx.with_alias:
            return format_alias_sql(sql, self.alias, ctx)
        return sql

    def nodes_(self):
        yield from self.term.nodes_()
        yield self


class DecimalSumFunction(functions.DistinctOptionFunction):
    """A ``SUM`` that keeps decimal precision on SQLite."""

    def __init__(self, term: Term, alias: str | None = None) -> None:
        super().__init__(DECIMAL_SUM_FUNCTION, term, alias=alias)


def collate_decimal(term: Term) -> Term:
    """Wrap *term* in the exact decimal collation, without stacking duplicates."""
    if isinstance(term, DecimalCollate):
        return term
    return DecimalCollate(term)


def _is_plain_decimal_term(term: Term) -> bool:
    """Whether *term* only maps a decimal column through null/identity transforms."""
    if isinstance(term, DecimalCollate):
        return _is_plain_decimal_term(term.term)
    if isinstance(term, NullValue):
        return True
    if isinstance(term, Case):
        terms = [value for _, value in term._cases]
        if term._else is not None:
            terms.append(term._else)
        return bool(terms) and all(_is_plain_decimal_term(value) for value in terms)
    return isinstance(term, Field)


def _strip_collation(term: Term) -> Term:
    """Drop the decimal collation, which is meaningless inside ``SUM``."""
    if isinstance(term, DecimalCollate):
        return _strip_collation(term.term)
    if isinstance(term, Case):
        stripped = Case(alias=term.alias)
        for criterion, value in term._cases:
            stripped.when(criterion, _strip_collation(value))
        if term._else is not None:
            stripped.else_(_strip_collation(term._else))
        return stripped
    return term


def exact_sum(term: Term) -> Term:
    """Replace a plain ``SUM`` over a decimal column with the exact aggregate.

    Only plain (possibly collated) column references, optionally wrapped in a ``CASE``
    by an aggregate ``_filter``, are routed through the exact aggregate.  Arithmetic
    expressions keep the backend's native aggregate because their intermediate results
    are already floating point.
    """
    argument = getattr(term, "args", (None,))[0]
    if not _is_plain_decimal_term(argument):
        return term
    exact = DecimalSumFunction(_strip_collation(argument))
    if getattr(term, "_distinct", False):
        exact = exact.distinct()
    return DecimalCollate(exact)


async def install_decimal_functions(connection) -> None:
    """Register the collation/aggregate on an open ``aiosqlite`` connection."""
    raw_connection = getattr(connection, "_conn", None)
    if raw_connection is None:  # pragma: nocoverage
        return
    await connection._execute(raw_connection.create_collation, DECIMAL_COLLATION, decimal_collation)
    await connection._execute(
        raw_connection.create_aggregate,
        DECIMAL_SUM_FUNCTION,
        1,
        DecimalSumAggregate,
    )
