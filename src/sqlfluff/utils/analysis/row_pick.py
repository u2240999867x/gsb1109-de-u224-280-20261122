"""Static determinism analysis for ``ROW_NUMBER()`` row picks.

A common warehouse pattern picks one row per group with::

    SELECT ...
    FROM (
        SELECT ...,
               ROW_NUMBER() OVER (
                   PARTITION BY site_id
                   ORDER BY event_time DESC, record_no
               ) AS rn
        FROM event
        JOIN site_catalog USING (site_id)
    ) picked
    WHERE rn = 1

That pattern is only deterministic when, within each partition, the ``ORDER
BY`` expressions uniquely identify every row that reaches the window.  A
unique natural key, a one-row relationship and a filter which pins part of the
key can each contribute to that proof.  ``DISTINCT`` on an outer query does
not: the window has already assigned the same rank to both rows before the
outer query sees them.

This module is deliberately conservative.  It reports:

* :attr:`RowPickStatus.DETERMINISTIC` when catalog evidence proves that every
  partition has a tie-breaker,
* :attr:`RowPickStatus.INDETERMINATE` when the query or catalog shows a
  concrete way for two rows to share a rank, and
* :attr:`RowPickStatus.INSUFFICIENT_EVIDENCE` when the catalog does not declare
  enough (for example a missing natural key) to make either claim.

The analysis never writes to the catalog.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Iterator, Optional, Sequence

from sqlfluff.core.dialects.common import iter_raw_references
from sqlfluff.core.parser import BaseSegment
from sqlfluff.utils.analysis.catalog import (
    Catalog,
    Relationship,
    UniqueKey,
    _clean_name,
)

_DIRECTION_KEYWORDS = frozenset({"ASC", "DESC"})
_NULLS_KEYWORDS = frozenset({"NULLS", "FIRST", "LAST"})


class RowPickStatus(str, Enum):
    """The outcome of a row-pick determinism analysis."""

    DETERMINISTIC = "deterministic"
    INDETERMINATE = "indeterminate"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    # Readable aliases for callers which use the negative/status wording.
    NOT_DETERMINISTIC = "indeterminate"
    INSUFFICIENT = "insufficient_evidence"


class DuplicateKind(str, Enum):
    """Why two rows can contend for the same rank."""

    MISSING_KEY = "missing_key"
    INCOMPLETE_ORDER = "incomplete_order"
    JOIN_AMPLIFICATION = "join_amplification"
    NULLABLE_KEY = "nullable_key"
    AMBIGUOUS_COLUMN = "ambiguous_column"
    NO_ORDER = "no_order"
    MISSING_CATALOG = "missing_catalog"
    UNSUPPORTED_EXPRESSION = "unsupported_expression"


@dataclass(frozen=True)
class DuplicateCause:
    """A concrete reason two output rows can tie at the same rank."""

    kind: DuplicateKind
    message: str
    table: Optional[str] = None
    columns: tuple[str, ...] = ()
    relationship: Optional[str] = None


@dataclass(frozen=True)
class ColumnRef:
    """A resolved column reference."""

    alias: str
    table: Optional[str]
    column: str

    @property
    def key(self) -> tuple[str, str]:
        """Identity used in key coverage checks."""
        return (self.alias, self.column)


@dataclass(frozen=True)
class OrderTerm:
    """One ``ORDER BY`` term of the window."""

    raw: str
    direction: Optional[str] = None
    nulls: Optional[str] = None
    column: Optional[ColumnRef] = None
    anchor: Optional[BaseSegment] = None
    unresolved_name: Optional[str] = None
    ambiguous: bool = False

    @property
    def is_plain_column(self) -> bool:
        """Whether the term is a bare column reference."""
        return self.column is not None


@dataclass(frozen=True)
class PartitionTerm:
    """One ``PARTITION BY`` term of the window."""

    raw: str
    column: Optional[ColumnRef] = None
    anchor: Optional[BaseSegment] = None
    unresolved_name: Optional[str] = None
    ambiguous: bool = False


@dataclass
class JoinEdge:
    """A resolved join between the accumulated scope and a new table."""

    left_alias: Optional[str]
    left_table: Optional[str]
    right_alias: str
    right_table: Optional[str]
    pairs: tuple[tuple[str, str], ...] = ()
    right_max_one: Optional[bool] = None
    right_key: Optional[UniqueKey] = None
    relationship: Optional[Relationship] = None
    join_clause: Optional[BaseSegment] = None


@dataclass
class RowPickResult:
    """The determinism assessment for one ``ROW_NUMBER()`` row pick."""

    status: RowPickStatus
    select_statement: BaseSegment
    rank_expression: BaseSegment
    rank_alias: Optional[str] = None
    partition_by: tuple[str, ...] = ()
    order_by: tuple[str, ...] = ()
    partition_terms: tuple[PartitionTerm, ...] = ()
    order_terms: tuple[OrderTerm, ...] = ()
    keys: tuple[tuple[ColumnRef, ...], ...] = ()
    duplicate_causes: tuple[DuplicateCause, ...] = ()
    reasons: tuple[str, ...] = ()
    tables: dict[str, str] = field(default_factory=dict)
    outer_distinct: bool = False
    filter_anchor: Optional[BaseSegment] = None
    configured_catalog: bool = False

    @property
    def is_deterministic(self) -> bool:
        """Whether the catalog proves a unique tie-breaker."""
        return self.status is RowPickStatus.DETERMINISTIC

    @property
    def is_indeterminate(self) -> bool:
        """Whether there is concrete evidence of tied rows."""
        return self.status is RowPickStatus.INDETERMINATE

    @property
    def is_insufficient_evidence(self) -> bool:
        """Whether the catalog did not declare enough to decide."""
        return self.status is RowPickStatus.INSUFFICIENT_EVIDENCE

    @property
    def ordering_expressions(self) -> tuple[str, ...]:
        """Alias for :attr:`order_by`, with the ordering terms verbatim."""
        return self.order_by

    @property
    def partition_expressions(self) -> tuple[str, ...]:
        """Alias for :attr:`partition_by`, with the partition terms verbatim."""
        return self.partition_by

    @property
    def duplicate_kinds(self) -> tuple[DuplicateKind, ...]:
        """The distinct kinds of duplication which can break the pick."""
        ordered: list[DuplicateKind] = []
        for cause in self.duplicate_causes:
            if cause.kind not in ordered:
                ordered.append(cause.kind)
        return tuple(ordered)

    @property
    def anchor(self) -> BaseSegment:
        """A useful segment to attach a lint violation to."""
        if self.filter_anchor is not None:
            return self.filter_anchor
        if self.order_terms and self.order_terms[0].anchor is not None:
            assert self.order_terms[0].anchor is not None
            return self.order_terms[0].anchor
        return self.rank_expression

    @property
    def message(self) -> str:
        """A human readable explanation linking order terms to the tie."""
        prefix = {
            RowPickStatus.DETERMINISTIC: "Row pick is deterministic",
            RowPickStatus.INDETERMINATE: "Row pick is not deterministic",
            RowPickStatus.INSUFFICIENT_EVIDENCE: (
                "Insufficient evidence to prove the row pick is deterministic"
            ),
        }[self.status]
        pieces = [prefix + "."]
        if self.partition_by:
            pieces.append("Window partition: " + ", ".join(self.partition_by) + ".")
        else:
            pieces.append("Window partition: (none).")
        if self.order_by:
            pieces.append("Window order: " + ", ".join(self.order_by) + ".")
        else:
            pieces.append("Window order: (none).")
        pieces.extend(reason for reason in self.reasons if reason)
        if self.outer_distinct and self.status is not RowPickStatus.DETERMINISTIC:
            pieces.append(
                "Outer DISTINCT cannot repair the window: it removes duplicate "
                "output rows after the window has already given tied rows the "
                "same rank."
            )
        return " ".join(pieces)

    def to_dict(self) -> dict[str, Any]:
        """Serialisable summary, useful for the public API and tests."""
        return {
            "status": self.status.value,
            "deterministic": self.is_deterministic,
            "rank_alias": self.rank_alias,
            "partition_by": list(self.partition_by),
            "order_by": list(self.order_by),
            "keys": [
                [f"{column.alias}.{column.column}" for column in key]
                for key in self.keys
            ],
            "duplicate_causes": [
                {
                    "kind": cause.kind.value,
                    "message": cause.message,
                    "table": cause.table,
                    "columns": list(cause.columns),
                    "relationship": cause.relationship,
                }
                for cause in self.duplicate_causes
            ],
            "reasons": list(self.reasons),
            "tables": dict(self.tables),
            "outer_distinct": self.outer_distinct,
            "message": self.message,
        }


# ---------------------------------------------------------------------------
# Segment helpers
# ---------------------------------------------------------------------------


def _code_children(segment: BaseSegment) -> list[BaseSegment]:
    """Return meaningful children (drop whitespace, comments and meta)."""
    return [child for child in segment.segments if child.is_code]


def _identifier_name(segment: Optional[BaseSegment]) -> Optional[str]:
    """Return the final identifier of a reference-like segment."""
    if segment is None:
        return None
    identifiers = list(segment.recursive_crawl("identifier"))
    if identifiers:
        return _clean_name(identifiers[-1].raw)
    return _clean_name(segment.raw)


def _leading_identifier(segment: Optional[BaseSegment]) -> Optional[str]:
    """Return the first direct identifier (e.g. a CTE name)."""
    if segment is None:
        return None
    for child in segment.segments:
        if child.is_type("naked_identifier", "quoted_identifier"):
            return _clean_name(child.raw)
    identifiers = list(segment.recursive_crawl("identifier"))
    return _clean_name(identifiers[0].raw) if identifiers else None


def _column_parts(
    segment: BaseSegment, dialect_name: Optional[str]
) -> tuple[Optional[str], str]:
    """Return ``(qualifier, column)`` for a column reference."""
    parts = list(iter_raw_references(segment, dialect_name or "ansi"))
    if not parts:
        return None, _clean_name(segment.raw)
    if len(parts) == 1:
        return None, _clean_name(parts[0].part)
    return _clean_name(parts[-2].part), _clean_name(parts[-1].part)


def _is_literal(segment: BaseSegment) -> bool:
    """Whether a segment is a literal/parameter which fixes a value."""
    if segment.is_type(
        "literal",
        "numeric_literal",
        "quoted_literal",
        "string_literal",
        "null_literal",
        "boolean_literal",
        "date_constructor_literal",
    ):
        return True
    # Bind parameters are values at execution time.  They still fix the
    # column to a single value for the duration of a query, which is enough
    # to remove it from a uniqueness key.
    raw = segment.raw.strip()
    return bool(re.fullmatch(r"(:\w+|\$\d+|\?|%s|%\(\w+\)s|\{\{.*\}\})", raw))


def _literal_text(segment: BaseSegment) -> str:
    """Return a normalised literal value for comparing bounds."""
    return segment.raw.strip().strip("'\"").upper()


def _is_one(segment: BaseSegment) -> bool:
    """Whether a literal is exactly ``1``."""
    return _is_literal(segment) and _literal_text(segment) == "1"


def _is_two(segment: BaseSegment) -> bool:
    """Whether a literal is exactly ``2``."""
    return _is_literal(segment) and _literal_text(segment) == "2"


def _conjuncts(segment: BaseSegment) -> list[list[BaseSegment]]:
    """Split a predicate into AND-ed groups.

    OR branches are not split: a constant inside an OR does not fix the column
    for every row, so the whole OR expression is returned as one unusable
    group.
    """
    if segment.is_type("bracketed"):
        expressions = segment.get_children("expression")
        if len(expressions) == 1:
            return _conjuncts(expressions[0])
        return [[segment]]
    if not segment.is_type("expression"):
        expressions = segment.get_children("expression")
        if expressions:
            groups: list[list[BaseSegment]] = []
            for expression in expressions:
                groups.extend(_conjuncts(expression))
            return groups
        return [[segment]]

    code = _code_children(segment)
    if any(
        child.is_type("binary_operator") and child.raw_upper == "OR" for child in code
    ):
        return [code]
    split_groups: list[list[BaseSegment]] = [[]]
    for child in code:
        if child.is_type("binary_operator") and child.raw_upper == "AND":
            split_groups.append([])
        else:
            split_groups[-1].append(child)
    flattened: list[list[BaseSegment]] = []
    for group in split_groups:
        if len(group) == 1 and group[0].is_type("bracketed"):
            nested = _conjuncts(group[0])
            if nested and nested != [[group[0]]]:
                flattened.extend(nested)
                continue
        flattened.append(group)
    return flattened


def _comparison(
    group: Sequence[BaseSegment],
) -> Optional[tuple[BaseSegment, BaseSegment, BaseSegment]]:
    """Return ``(left, operator, right)`` for a simple comparison group."""
    code = [segment for segment in group if segment.is_code]
    operator_index: Optional[int] = None
    for index, segment in enumerate(code):
        if segment.is_type("comparison_operator"):
            operator_index = index
            break
    if operator_index is None or operator_index == 0 or operator_index >= len(code) - 1:
        return None
    return code[operator_index - 1], code[operator_index], code[operator_index + 1]


def _operator_text(operator: BaseSegment) -> str:
    """Normalised comparison operator text."""
    return operator.raw.strip().upper()


def _contains_rank_reference(
    segments: Sequence[BaseSegment],
    rank_alias: Optional[str],
    rank_function: BaseSegment,
) -> bool:
    """Whether any segment refers to the rank column/function."""
    alias = _clean_name(rank_alias) if rank_alias else None
    for segment in segments:
        if alias:
            for reference in segment.recursive_crawl("column_reference"):
                if _column_parts(reference, "ansi")[1] == alias:
                    return True
        for function in segment.recursive_crawl("function"):
            name = function.get_child("function_name")
            if (
                name
                and name.raw_upper == "ROW_NUMBER"
                and function.get_child("over_clause")
                and " ".join(function.raw.split())
                == " ".join(rank_function.raw.split())
            ):
                return True
    return False


def _rank_predicate_kind(
    group: Sequence[BaseSegment],
    rank_alias: Optional[str],
    rank_function: BaseSegment,
) -> bool:
    """Whether a conjunct is of the form ``rank = 1`` (or equivalent)."""
    code = [segment for segment in group if segment.is_code]
    if not _contains_rank_reference(code, rank_alias, rank_function):
        return False
    comparison = _comparison(code)
    if comparison:
        _, operator, right = comparison
        op = _operator_text(operator)
        if op == "=" and _is_one(right):
            return True
        if op == "<=" and _is_one(right):
            return True
        if op == "<" and _is_two(right):
            return True
        return False
    for index, segment in enumerate(code):
        if segment.is_type("keyword") and segment.raw_upper == "IN":
            bracket = next(
                (
                    candidate
                    for candidate in code[index + 1 :]
                    if candidate.is_type("bracketed")
                ),
                None,
            )
            if bracket is None:
                continue
            values = [
                nested
                for nested in bracket.recursive_crawl(
                    "literal", "numeric_literal", "quoted_literal"
                )
            ]
            return len(values) == 1 and _is_one(values[0])
        if segment.is_type("keyword") and segment.raw_upper == "BETWEEN":
            literals = [
                nested
                for remainder in code[index + 1 :]
                for nested in remainder.recursive_crawl(
                    "literal", "numeric_literal", "quoted_literal"
                )
            ]
            return len(literals) == 2 and all(_is_one(item) for item in literals)
    return False


def _predicate_has_rank_one(
    expression: BaseSegment,
    rank_alias: Optional[str],
    rank_function: BaseSegment,
    qualifier: Optional[str] = None,
) -> bool:
    """Whether the predicate keeps only rank one."""
    for group in _conjuncts(expression):
        if qualifier:
            references = [
                reference
                for segment in group
                for reference in segment.recursive_crawl("column_reference")
                if _column_parts(reference, "ansi")[1] == _clean_name(rank_alias or "")
            ]
            if references and all(
                (_column_parts(reference, "ansi")[0] or _clean_name(qualifier))
                != _clean_name(qualifier)
                for reference in references
            ):
                continue
        if _rank_predicate_kind(group, rank_alias, rank_function):
            return True
    if "OR" in expression.raw_upper:
        return _rank_predicate_kind(
            _code_children(expression), rank_alias, rank_function
        )
    return False


def _find_rank_filter(
    select: BaseSegment,
    rank_alias: Optional[str],
    rank_function: BaseSegment,
) -> Optional[BaseSegment]:
    """Find the predicate which keeps only rank 1.

    The filter can be a ``QUALIFY`` in the same select or a ``WHERE`` in the
    enclosing query.  Returns the expression which contains the rank
    comparison, or ``None`` if this ``ROW_NUMBER()`` is not a row pick.
    """
    for clause_type in ("qualify_clause", "where_clause", "having_clause"):
        clause = select.get_child(clause_type)
        if not clause:
            continue
        expression = clause.get_child("expression")
        if expression and _predicate_has_rank_one(
            expression, rank_alias, rank_function
        ):
            return expression

    subquery_alias, outer = _enclosing_select(select)
    for _ in range(2):
        if outer is None:
            break
        for clause_type in ("where_clause", "qualify_clause", "having_clause"):
            clause = outer.get_child(clause_type)
            if not clause:
                continue
            expression = clause.get_child("expression")
            if expression and _predicate_has_rank_one(
                expression, rank_alias, rank_function, subquery_alias
            ):
                return expression
        subquery_alias, outer = _enclosing_select(outer)
    return None


def _enclosing_select(
    select: BaseSegment,
) -> tuple[Optional[str], Optional[BaseSegment]]:
    """Find the select which consumes this select's output.

    Returns ``(alias, select)``.  ``alias`` is the subquery alias where one
    exists.  CTEs are resolved through their name when a single consuming
    select can be found.
    """
    parent_info = select.get_parent()
    while parent_info is not None:
        parent, _ = parent_info
        if parent.is_type("from_expression_element"):
            alias_expression = parent.get_child("alias_expression")
            alias = (
                _identifier_name(
                    alias_expression.get_child("naked_identifier", "quoted_identifier")
                )
                if alias_expression
                else None
            )
            outer = _parent_select(parent)
            if outer is not None:
                return alias, outer
        if parent.is_type("common_table_expression"):
            cte_name = _leading_identifier(parent)
            with_compound = _parent_of_type(parent, "with_compound_statement")
            if with_compound is not None:
                for candidate in with_compound.get_children("select_statement"):
                    if _select_references(candidate, cte_name):
                        return cte_name, candidate
            return cte_name, None
        parent_info = parent.get_parent()
    return None, None


def _parent_select(segment: BaseSegment) -> Optional[BaseSegment]:
    """Return the nearest ancestor select statement."""
    info = segment.get_parent()
    while info is not None:
        parent, _ = info
        if parent.is_type("select_statement"):
            return parent
        info = parent.get_parent()
    return None


def _parent_of_type(segment: BaseSegment, seg_type: str) -> Optional[BaseSegment]:
    """Return the nearest ancestor of a given type."""
    info = segment.get_parent()
    while info is not None:
        parent, _ = info
        if parent.is_type(seg_type):
            return parent
        info = parent.get_parent()
    return None


def _select_references(select: BaseSegment, name: Optional[str]) -> bool:
    """Whether a select references a table/CTE name."""
    if not name:
        return False
    for table_reference in select.recursive_crawl(
        "table_reference", no_recursive_seg_type="select_statement"
    ):
        parts = list(iter_raw_references(table_reference, "ansi"))
        if parts and _clean_name(parts[-1].part) == _clean_name(name):
            return True
    return False


# ---------------------------------------------------------------------------
# Window extraction
# ---------------------------------------------------------------------------


@dataclass
class _WindowSpec:
    partition_items: list[list[BaseSegment]] = field(default_factory=list)
    order_items: list[list[BaseSegment]] = field(default_factory=list)


def _split_list_items(
    segment: BaseSegment, leading_keywords: frozenset[str]
) -> list[list[BaseSegment]]:
    """Split a comma-separated clause into code-segment groups.

    Leading clause keywords (``ORDER BY`` / ``PARTITION BY``) are removed so
    each group starts at its expression.
    """
    groups: list[list[BaseSegment]] = [[]]
    for child in segment.segments:
        if child.is_type("comma"):
            groups.append([])
        elif child.is_code:
            groups[-1].append(child)
    cleaned: list[list[BaseSegment]] = []
    for group in groups:
        while (
            group
            and group[0].is_type("keyword")
            and group[0].raw_upper in (leading_keywords)
        ):
            group = group[1:]
        if group:
            cleaned.append(group)
    return cleaned


def _named_windows(select: BaseSegment) -> dict[str, BaseSegment]:
    """Collect named window definitions in a SELECT."""
    windows: dict[str, BaseSegment] = {}
    for named_window in select.get_children("named_window"):
        for expression in named_window.get_children("named_window_expression"):
            name_segment = expression.get_child("naked_identifier", "quoted_identifier")
            bracket = expression.get_child("bracketed")
            spec = bracket.get_child("window_specification") if bracket else None
            if name_segment and spec:
                windows[_clean_name(name_segment.raw)] = spec
    return windows


def _resolve_named_window(
    spec: Optional[BaseSegment],
    named: dict[str, BaseSegment],
    seen: Optional[set[str]] = None,
) -> _WindowSpec:
    """Resolve a window specification, following ``WINDOW w AS (...)``."""
    seen = seen or set()
    if spec is None:
        return _WindowSpec()
    result = _WindowSpec()
    partition = spec.get_child("partitionby_clause")
    if partition:
        result.partition_items = _split_list_items(
            partition, frozenset({"PARTITION", "BY"})
        )
    order = spec.get_child("orderby_clause")
    if order:
        result.order_items = _split_list_items(order, frozenset({"ORDER", "BY"}))

    # An inline ``OVER (w ORDER BY ...)`` inherits from ``w``.
    inherited_name: Optional[str] = None
    for child in spec.segments:
        if child.is_type("naked_identifier", "quoted_identifier"):
            inherited_name = _clean_name(child.raw)
            break
    if inherited_name and inherited_name in named and inherited_name not in seen:
        base = _resolve_named_window(
            named[inherited_name], named, seen | {inherited_name}
        )
        if not result.partition_items:
            result.partition_items = base.partition_items
        if not result.order_items:
            result.order_items = base.order_items
    return result


def _over_clause_spec(function: BaseSegment) -> Optional[BaseSegment]:
    """Return the window specification used by a window function."""
    over = function.get_child("over_clause")
    if not over:
        return None
    direct_spec = over.get_child("window_specification")
    if direct_spec:
        return direct_spec
    bracket = over.get_child("bracketed")
    if bracket:
        return bracket.get_child("window_specification")
    return None


def _over_clause_name(function: BaseSegment) -> Optional[str]:
    """Return a bare named window used as ``OVER w``."""
    over = function.get_child("over_clause")
    if not over:
        return None
    for child in over.segments:
        if child.is_type("naked_identifier", "quoted_identifier"):
            return _clean_name(child.raw)
    return None


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------


@dataclass
class _ScopeTable:
    alias: str
    table: Optional[str]
    element: BaseSegment


@dataclass
class _Scope:
    catalog: Catalog
    tables: list[_ScopeTable] = field(default_factory=list)
    joins: list[JoinEdge] = field(default_factory=list)
    fixed: set[tuple[str, str]] = field(default_factory=set)
    ambiguous_columns: set[str] = field(default_factory=set)
    join_clauses: list[BaseSegment] = field(default_factory=list)

    def by_alias(self, alias: Optional[str]) -> Optional[_ScopeTable]:
        """Look up a scope table by alias."""
        if not alias:
            return None
        clean = _clean_name(alias)
        for table in self.tables:
            if table.alias == clean:
                return table
        return None

    def table_has_column(self, table: Optional[str], column: str) -> bool:
        """Whether catalog metadata mentions a column for a table."""
        if not table:
            return False
        spec = self.catalog.table(table)
        if not spec:
            return False
        if column in spec.columns:
            return True
        return any(column in key.columns for key in spec.keys)

    def resolve_column(
        self, qualifier: Optional[str], column: str
    ) -> tuple[Optional[ColumnRef], bool]:
        """Resolve a column reference.

        Returns ``(column_ref, ambiguous)``.  ``column_ref`` is ``None`` when
        the reference cannot be attributed confidently.
        """
        column = _clean_name(column)
        if qualifier:
            table = self.by_alias(qualifier)
            if table:
                return ColumnRef(table.alias, table.table, column), False
            return None, False
        matches = [
            table for table in self.tables if self.table_has_column(table.table, column)
        ]
        if len(matches) == 1:
            return ColumnRef(matches[0].alias, matches[0].table, column), False
        if len(matches) > 1:
            return None, True
        if len(self.tables) == 1:
            table = self.tables[0]
            return ColumnRef(table.alias, table.table, column), False
        return None, False


def _table_from_element(element: BaseSegment) -> _ScopeTable:
    """Resolve a ``from_expression_element`` to a scope table."""
    alias_expression = element.get_child("alias_expression")
    alias = (
        _identifier_name(
            alias_expression.get_child("naked_identifier", "quoted_identifier")
        )
        if alias_expression
        else None
    )
    table_reference = None
    table_expression = element.get_child("table_expression")
    if table_expression:
        table_reference = table_expression.get_child("table_reference")
        if table_reference is None:
            # Bracketed subqueries and table functions are not catalog tables.
            return _ScopeTable(alias or "", None, element)
    if table_reference is not None:
        parts = list(iter_raw_references(table_reference, "ansi"))
        table_name = _clean_name(parts[-1].part) if parts else None
    else:
        table_name = None
    return _ScopeTable(alias or table_name or "", table_name, element)


def _collect_scope(select: BaseSegment, catalog: Catalog) -> _Scope:
    """Build the table/alias/join scope for a SELECT."""
    scope = _Scope(catalog=catalog)
    from_clause = select.get_child("from_clause")
    if not from_clause:
        return scope

    for from_expression in from_clause.get_children("from_expression"):
        elements = from_expression.get_children("from_expression_element")
        join_clauses = from_expression.get_children("join_clause")
        for element in elements:
            table = _table_from_element(element)
            if table.alias:
                scope.tables.append(table)
        for join_clause in join_clauses:
            right_element = join_clause.get_child("from_expression_element")
            if right_element is None:
                continue
            right = _table_from_element(right_element)
            if right.alias:
                scope.tables.append(right)
            scope.join_clauses.append(join_clause)
    return scope


def _rebuild_joins(scope: _Scope) -> None:
    """Resolve joins after constraints have been collected."""
    scope.joins = []
    for join_clause in scope.join_clauses:
        right_element = join_clause.get_child("from_expression_element")
        if right_element is None:
            continue
        right = _table_from_element(right_element)
        if not right.alias:
            continue
        scope.joins.append(_build_join_edge(join_clause, right, scope))
    # Remove the last-added copies of joined tables which may appear after
    # their first addition.  ``_collect_scope`` appends each joined table once,
    # so this method only replaces the edge objects.


def _build_join_edge(
    join_clause: BaseSegment,
    right: _ScopeTable,
    scope: _Scope,
) -> JoinEdge:
    """Resolve the equated columns and cardinality of one join."""
    edge = JoinEdge(
        left_alias=None,
        left_table=None,
        right_alias=right.alias,
        right_table=right.table,
        join_clause=join_clause,
    )
    pairs: list[tuple[str, str]] = []
    left_groups: dict[str, list[str]] = {}
    join_on = join_clause.get_child("join_on_condition")
    if join_on:
        for expression in join_on.get_children("expression"):
            for group in _conjuncts(expression):
                comparison = _comparison(group)
                if not comparison:
                    continue
                left, operator, right_operand = comparison
                if _operator_text(operator) != "=":
                    continue
                if not (
                    left.is_type("column_reference")
                    and right_operand.is_type("column_reference")
                ):
                    continue
                left_q, left_c = _column_parts(left, "ansi")
                right_q, right_c = _column_parts(right_operand, "ansi")
                left_ref, _ = scope.resolve_column(left_q, left_c)
                right_ref, _ = scope.resolve_column(right_q, right_c)
                if right_ref and right_ref.alias == right.alias and left_ref:
                    pairs.append((left_ref.column, right_ref.column))
                    left_groups.setdefault(left_ref.alias, []).append(left_ref.column)
                elif left_ref and left_ref.alias == right.alias and right_ref:
                    pairs.append((right_ref.column, left_ref.column))
                    left_groups.setdefault(right_ref.alias, []).append(right_ref.column)
    # USING (a, b) is an equi-join on same-named columns.
    if not pairs:
        seen_using = False
        for child in join_clause.segments:
            if child.is_type("keyword") and child.raw_upper == "USING":
                seen_using = True
            elif seen_using and child.is_type("bracketed"):
                for identifier in child.recursive_crawl("identifier"):
                    column = _clean_name(identifier.raw)
                    for table in scope.tables:
                        if table.alias != right.alias and scope.table_has_column(
                            table.table, column
                        ):
                            pairs.append((column, column))
                            left_groups.setdefault(table.alias, []).append(column)
                            break
                seen_using = False

    edge.pairs = tuple(pairs)
    if left_groups:
        edge.left_alias = next(iter(left_groups))
        left_table = scope.by_alias(edge.left_alias)
        edge.left_table = left_table.table if left_table else None

    # 1. A declared unique key of the right table covered by the join columns.
    right_columns = {right_col for _, right_col in pairs}
    for key in _usable_keys(right.table, scope.catalog, scope.fixed):
        if set(key.columns) <= right_columns:
            edge.right_key = key
            edge.right_max_one = True
            break

    # 2. A catalog relationship may declare the cardinality explicitly.
    if edge.right_max_one is None and edge.left_alias:
        left_columns = left_groups[edge.left_alias]
        left_table = scope.by_alias(edge.left_alias)
        left_name = (left_table.table if left_table else None) or edge.left_alias
        right_name = right.table or right.alias
        relationship = scope.catalog.relationship_for(
            left_name,
            left_columns,
            right_name,
            [right_col for _, right_col in pairs],
        )
        if relationship:
            edge.relationship = relationship
            _, right_max = scope.catalog.max_one_per_key(
                left_name,
                left_columns,
                right_name,
                [right_col for _, right_col in pairs],
            )
            edge.right_max_one = right_max
    return edge


def _usable_keys(
    table: Optional[str], catalog: Catalog, fixed: Iterable[tuple[str, str]]
) -> list[UniqueKey]:
    """Return keys which are usable as identity evidence.

    A key is usable when every explicitly nullable column in it is pinned by
    an equality filter (or is part of a primary key).
    """
    if not table:
        return []
    fixed_columns = {column for _, column in fixed}
    usable: list[UniqueKey] = []
    for key in catalog.unique_keys(table):
        unsafe = {
            column for column in key.nullable_columns if column not in fixed_columns
        }
        if not unsafe:
            usable.append(key)
    return usable


def _base_candidates(
    scope: _Scope,
) -> tuple[list[frozenset[tuple[str, str]]], list[DuplicateCause]]:
    """Build identity key candidates for the FROM/join scope."""
    causes: list[DuplicateCause] = []
    catalog = scope.catalog
    if not scope.tables:
        # A SELECT without a table source produces at most one row.
        return [frozenset()], causes

    base = scope.tables[0]
    if base.table is None or not catalog.has_table(base.table):
        causes.append(
            DuplicateCause(
                DuplicateKind.MISSING_CATALOG,
                f"The source `{base.alias}` is not declared in the catalog.",
                table=base.alias,
            )
        )
        return [], causes
    keys = _usable_keys(base.table, catalog, scope.fixed)
    if not keys:
        causes.append(
            DuplicateCause(
                DuplicateKind.MISSING_KEY,
                "The catalog does not declare a usable unique/natural key "
                f"for `{base.table}`.",
                table=base.table,
            )
        )
        return [], causes
    candidates = [
        frozenset((base.alias, column) for column in key.columns) for key in keys
    ]

    for edge in scope.joins:
        right = scope.by_alias(edge.right_alias)
        if right is None:
            continue
        right_keys = _usable_keys(right.table, catalog, scope.fixed)
        right_candidates = [
            frozenset((right.alias, column) for column in key.columns)
            for key in right_keys
        ]
        new_candidates: list[frozenset[tuple[str, str]]] = []
        if edge.right_max_one is True:
            new_candidates.extend(candidates)
        for left_candidate in candidates:
            for right_candidate in right_candidates:
                new_candidates.append(left_candidate | right_candidate)
        if not new_candidates:
            if right.table is None:
                causes.append(
                    DuplicateCause(
                        DuplicateKind.MISSING_CATALOG,
                        f"`{right.alias}` is joined without a catalog declaration, "
                        "so the number of rows it contributes cannot be proven.",
                        table=right.alias,
                    )
                )
            elif not right_keys:
                causes.append(
                    DuplicateCause(
                        DuplicateKind.MISSING_KEY,
                        "The catalog does not declare a usable unique key for "
                        f"`{right.table}`, so rows added by the join cannot be "
                        "distinguished.",
                        table=right.table,
                    )
                )
            else:  # pragma: no cover - defensive
                causes.append(
                    DuplicateCause(
                        DuplicateKind.UNSUPPORTED_EXPRESSION,
                        f"The join to `{right.alias}` could not be resolved to "
                        "a uniqueness key.",
                        table=right.table,
                    )
                )
            return [], causes
        if edge.right_max_one is not True:
            columns = tuple(column for _, column in edge.pairs)
            message = (
                f"Joining `{right.table or right.alias}` can return more than "
                "one row for the same "
                + ("`" + "`, `".join(columns) + "`" if columns else "left-side key")
                + "."
            )
            if right_keys:
                message += (
                    " The ordering must also include the right-side key ("
                    + ", ".join(
                        f"`{right.table or right.alias}.{column}`"
                        for key in right_keys
                        for column in key.columns
                    )
                    + ") to distinguish those copies."
                )
            causes.append(
                DuplicateCause(
                    DuplicateKind.JOIN_AMPLIFICATION,
                    message,
                    table=right.table,
                    columns=columns,
                    relationship=(
                        edge.relationship.name if edge.relationship else None
                    ),
                )
            )
        candidates = new_candidates
    return candidates, causes


# ---------------------------------------------------------------------------
# Constraints and projections
# ---------------------------------------------------------------------------


def _resolve_column_ref(segment: BaseSegment, scope: _Scope) -> Optional[ColumnRef]:
    """Resolve a column_reference segment against a scope."""
    qualifier, column = _column_parts(segment, "ansi")
    ref, ambiguous = scope.resolve_column(qualifier, column)
    if ambiguous:
        scope.ambiguous_columns.add(column)
    return ref


def _collect_constraints_from_expression(
    expression: BaseSegment, scope: _Scope
) -> None:
    """Record columns fixed to a single non-null value."""
    for group in _conjuncts(expression):
        code = [segment for segment in group if segment.is_code]
        if not code:
            continue
        # col IS NOT NULL
        if (
            len(code) >= 4
            and code[1].is_type("keyword")
            and code[1].raw_upper == "IS"
            and code[2].is_type("keyword")
            and code[2].raw_upper == "NOT"
        ):
            if code[0].is_type("column_reference"):
                ref = _resolve_column_ref(code[0], scope)
                if ref:
                    scope.fixed.add(ref.key)
            continue
        # col IS NULL does not prove uniqueness; ignore it.
        if (
            len(code) >= 3
            and code[1].is_type("keyword")
            and code[1].raw_upper == "IS"
            and code[2].is_type("keyword")
            and code[2].raw_upper == "NULL"
        ):
            continue
        comparison = _comparison(code)
        if comparison:
            left, operator, right = comparison
            op = _operator_text(operator)
            if op != "=":
                continue
            if left.is_type("column_reference") and _is_literal(right):
                ref = _resolve_column_ref(left, scope)
                if ref:
                    scope.fixed.add(ref.key)
            elif right.is_type("column_reference") and _is_literal(left):
                ref = _resolve_column_ref(right, scope)
                if ref:
                    scope.fixed.add(ref.key)
            continue
        # IN (single value)
        for index, segment in enumerate(code):
            if segment.is_type("keyword") and segment.raw_upper == "IN":
                bracket = next(
                    (
                        candidate
                        for candidate in code[index + 1 :]
                        if candidate.is_type("bracketed")
                    ),
                    None,
                )
                if not bracket:
                    continue
                literals = [
                    nested
                    for nested in bracket.recursive_crawl(
                        "literal", "numeric_literal", "quoted_literal"
                    )
                ]
                if len(literals) == 1 and code[0].is_type("column_reference"):
                    ref = _resolve_column_ref(code[0], scope)
                    if ref:
                        scope.fixed.add(ref.key)


def _collect_inner_constraints(select: BaseSegment, scope: _Scope) -> None:
    """Collect fixed columns from the select which contains the window."""
    for clause_type in ("where_clause", "qualify_clause"):
        clause = select.get_child(clause_type)
        if not clause:
            continue
        for expression in clause.get_children("expression"):
            _collect_constraints_from_expression(expression, scope)
    from_clause = select.get_child("from_clause")
    if from_clause:
        for join_on in from_clause.recursive_crawl(
            "join_on_condition", no_recursive_seg_type="select_statement"
        ):
            for expression in join_on.get_children("expression"):
                _collect_constraints_from_expression(expression, scope)


def _projection_map(select: BaseSegment, scope: _Scope) -> dict[str, ColumnRef]:
    """Map output column names to source columns for simple projections."""
    mapping: dict[str, ColumnRef] = {}
    select_clause = select.get_child("select_clause")
    if not select_clause:
        return mapping
    for target in select_clause.get_children("select_clause_element"):
        alias_info = getattr(target, "get_alias", lambda: None)()
        output_name = (
            _clean_name(alias_info.alias_identifier_name) if alias_info else None
        )
        column_reference = target.get_child("column_reference")
        if column_reference is None:
            continue
        ref = _resolve_column_ref(column_reference, scope)
        if ref is None:
            continue
        mapping[output_name or ref.column] = ref
    return mapping


def _collect_outer_constraints(
    outer: BaseSegment,
    inner: BaseSegment,
    scope: _Scope,
) -> None:
    """Propagate simple outer filters into the inner source scope."""
    mapping = _projection_map(inner, scope)
    for clause_type in ("where_clause", "qualify_clause"):
        clause = outer.get_child(clause_type)
        if not clause:
            continue
        for expression in clause.get_children("expression"):
            for group in _conjuncts(expression):
                code = [segment for segment in group if segment.is_code]
                if not code:
                    continue
                # col IS NOT NULL
                if (
                    len(code) >= 4
                    and code[1].is_type("keyword")
                    and code[1].raw_upper == "IS"
                    and code[2].is_type("keyword")
                    and code[2].raw_upper == "NOT"
                ):
                    if code[0].is_type("column_reference"):
                        _, column = _column_parts(code[0], "ansi")
                        source = mapping.get(column)
                        if source:
                            scope.fixed.add(source.key)
                    continue
                comparison = _comparison(code)
                if not comparison:
                    continue
                left, operator, right = comparison
                if _operator_text(operator) != "=":
                    continue
                if not (left.is_type("column_reference") and _is_literal(right)):
                    continue
                _, column = _column_parts(left, "ansi")
                source = mapping.get(column)
                if source:
                    scope.fixed.add(source.key)


# ---------------------------------------------------------------------------
# Window term extraction
# ---------------------------------------------------------------------------


def _order_term_from_group(
    group: Sequence[BaseSegment],
    scope: _Scope,
) -> OrderTerm:
    """Build an :class:`OrderTerm` from a comma-separated group."""
    code = [segment for segment in group if segment.is_code]
    direction = None
    nulls = None
    expression_segments: list[BaseSegment] = []
    for segment in code:
        if segment.is_type("keyword") and segment.raw_upper in _DIRECTION_KEYWORDS:
            direction = segment.raw_upper
            continue
        if segment.is_type("keyword") and segment.raw_upper in _NULLS_KEYWORDS:
            if segment.raw_upper in ("FIRST", "LAST"):
                nulls = segment.raw_upper
            continue
        expression_segments.append(segment)
    raw = " ".join(segment.raw.strip() for segment in expression_segments)
    if direction:
        raw = f"{raw} {direction}"
    if nulls:
        raw = f"{raw} NULLS {nulls}"
    column: Optional[ColumnRef] = None
    unresolved_name: Optional[str] = None
    ambiguous = False
    reference = _single_column_reference(expression_segments)
    if reference is not None:
        qualifier, name = _column_parts(reference, "ansi")
        column, ambiguous = scope.resolve_column(qualifier, name)
        if column is None:
            unresolved_name = f"{qualifier}.{name}" if qualifier else name
    anchor = expression_segments[0] if expression_segments else None
    return OrderTerm(
        raw=raw,
        direction=direction,
        nulls=nulls,
        column=column,
        anchor=anchor,
        unresolved_name=unresolved_name,
        ambiguous=ambiguous,
    )


def _single_column_reference(
    expression_segments: Sequence[BaseSegment],
) -> Optional[BaseSegment]:
    """Return the column reference if the group is exactly one reference."""
    if len(expression_segments) == 1:
        segment = expression_segments[0]
        if segment.is_type("column_reference"):
            return segment
        if segment.is_type("expression"):
            children = _code_children(segment)
            if len(children) == 1 and children[0].is_type("column_reference"):
                return children[0]
    return None


def _partition_term_from_group(
    group: Sequence[BaseSegment], scope: _Scope
) -> PartitionTerm:
    """Build a :class:`PartitionTerm` from a comma-separated group."""
    code = [segment for segment in group if segment.is_code]
    raw = " ".join(segment.raw.strip() for segment in code)
    column: Optional[ColumnRef] = None
    unresolved_name: Optional[str] = None
    ambiguous = False
    reference = _single_column_reference(code)
    if reference is not None:
        qualifier, name = _column_parts(reference, "ansi")
        column, ambiguous = scope.resolve_column(qualifier, name)
        if column is None:
            unresolved_name = f"{qualifier}.{name}" if qualifier else name
    anchor = code[0] if code else None
    return PartitionTerm(
        raw=raw,
        column=column,
        anchor=anchor,
        unresolved_name=unresolved_name,
        ambiguous=ambiguous,
    )


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


def _iter_row_number_functions(select: BaseSegment) -> Iterator[BaseSegment]:
    """Yield ``ROW_NUMBER()`` calls with an OVER clause in a select."""
    for function in select.recursive_crawl(
        "function", no_recursive_seg_type="select_statement", allow_self=False
    ):
        name = function.get_child("function_name")
        if (
            name
            and name.raw_upper == "ROW_NUMBER"
            and function.get_child("over_clause")
        ):
            yield function


def _rank_alias(function: BaseSegment) -> Optional[str]:
    """Return the output alias of the select target containing a function."""
    parent_info = function.get_parent()
    while parent_info is not None:
        parent, _ = parent_info
        if parent.is_type("select_clause_element"):
            alias_info = getattr(parent, "get_alias", lambda: None)()
            return alias_info.alias_identifier_name if alias_info else None
        if parent.is_type("expression", "function"):
            parent_info = parent.get_parent()
            continue
        break
    return None


def _table_for_alias(scope: _Scope, alias: str) -> Optional[str]:
    """Return the table name for a scope alias."""
    table = scope.by_alias(alias)
    return table.table if table else None


def _select_has_distinct(select: BaseSegment) -> bool:
    """Whether this select's projection uses DISTINCT."""
    select_clause = select.get_child("select_clause")
    if not select_clause:
        return False
    modifier = select_clause.get_child("select_clause_modifier")
    return bool(
        modifier
        and any(
            keyword.raw_upper == "DISTINCT"
            for keyword in modifier.recursive_crawl("keyword")
        )
    )


def _outer_has_distinct(select: BaseSegment) -> bool:
    """Whether this select or any enclosing select uses DISTINCT."""
    if _select_has_distinct(select):
        return True
    _, outer = _enclosing_select(select)
    seen = 0
    while outer is not None and seen < 3:
        select_clause = outer.get_child("select_clause")
        if select_clause:
            modifier = select_clause.get_child("select_clause_modifier")
            if modifier and any(
                keyword.raw_upper == "DISTINCT"
                for keyword in modifier.recursive_crawl("keyword")
            ):
                return True
        _, outer = _enclosing_select(outer)
        seen += 1
    return False


def _determinism_check(
    candidates: list[frozenset[tuple[str, str]]],
    partition_keys: set[tuple[str, str]],
    order_keys: set[tuple[str, str]],
    fixed: set[tuple[str, str]],
) -> tuple[bool, list[set[tuple[str, str]]], list[frozenset[tuple[str, str]]]]:
    """Check whether any identity key is covered by partition/order/filters.

    Returns ``(covered, missing_by_candidate, covered_candidates)``.
    """
    missing_by_candidate: list[set[tuple[str, str]]] = []
    covered_candidates: list[frozenset[tuple[str, str]]] = []
    for candidate in candidates:
        required: set[tuple[str, str]] = set(candidate - fixed - partition_keys)
        missing: set[tuple[str, str]] = set(required - order_keys)
        if not missing:
            covered_candidates.append(candidate)
        else:
            missing_by_candidate.append(missing)
    return bool(covered_candidates), missing_by_candidate, covered_candidates


def analyze_select(
    select: BaseSegment,
    catalog: Optional[Catalog] = None,
    *,
    dialect_name: Optional[str] = None,
    configured_catalog: bool = False,
) -> list[RowPickResult]:
    """Analyse every ``ROW_NUMBER()`` row pick in a select statement.

    Args:
        select: A ``select_statement`` segment.
        catalog: Read-only table metadata.  ``None`` is treated as an empty
            catalog, which yields ``INSUFFICIENT_EVIDENCE`` rather than a
            claim of determinism.
        dialect_name: Dialect name, used to normalise object references.
        configured_catalog: Whether the user supplied a catalog.  This only
            affects messages.

    Returns:
        One :class:`RowPickResult` per ``ROW_NUMBER()`` function which is
        filtered to ``rank = 1``.
    """
    del dialect_name  # reserved for dialect-specific reference handling
    assert select.is_type("select_statement")
    catalog = catalog or Catalog()
    results: list[RowPickResult] = []
    named = _named_windows(select)
    seen: set[tuple[str, str]] = set()

    for function in _iter_row_number_functions(select):
        rank_alias = _rank_alias(function)
        filter_expression = _find_rank_filter(select, rank_alias, function)
        if filter_expression is None:
            continue
        # A QUALIFY clause may repeat the same window function that the
        # SELECT projects.  That is one row pick, not two.
        dedupe_key = (function.raw, filter_expression.raw)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        window_spec = _over_clause_spec(function)
        named_name = _over_clause_name(function)
        if window_spec is None and named_name:
            window_spec = named.get(named_name)
        spec = _resolve_named_window(window_spec, named)

        scope = _collect_scope(select, catalog)
        _collect_inner_constraints(select, scope)
        _rebuild_joins(scope)
        subquery_alias, outer = _enclosing_select(select)
        inner_for_projection: BaseSegment = select
        for _ in range(2):
            if outer is None:
                break
            _collect_outer_constraints(outer, inner_for_projection, scope)
            # ``DISTINCT`` on an outer select cannot undo the ranking, and the
            # next projection level may expose the key columns.
            inner_for_projection = outer
            _, outer = _enclosing_select(outer)
        if subquery_alias:
            # The added constraint may make a nullable join key usable.
            _rebuild_joins(scope)

        partition_terms = tuple(
            _partition_term_from_group(group, scope) for group in spec.partition_items
        )
        order_terms = tuple(
            _order_term_from_group(group, scope) for group in spec.order_items
        )
        partition_by = tuple(term.raw for term in partition_terms)
        order_by = tuple(term.raw for term in order_terms)

        candidates, causes = _base_candidates(scope)
        fixed = set(scope.fixed)
        partition_keys = {
            term.column.key for term in partition_terms if term.column is not None
        }
        order_keys = {
            term.column.key for term in order_terms if term.column is not None
        }

        nullable_order_terms = [
            term
            for term in order_terms
            if term.column
            and term.column.table
            and catalog.column_nullable(term.column.table, term.column.column) is True
            and term.column.key not in fixed
        ]
        if nullable_order_terms:
            expressions = ", ".join(f"`{term.raw}`" for term in nullable_order_terms)
            causes.append(
                DuplicateCause(
                    DuplicateKind.NULLABLE_KEY,
                    f"The ordering expression(s) {expressions} come from a "
                    "column the catalog declares nullable; multiple NULL values "
                    "share the same sort position and contend for the same rank.",
                    columns=tuple(
                        term.column.column
                        for term in nullable_order_terms
                        if term.column
                    ),
                )
            )

        for term in order_terms:
            if term.ambiguous:
                causes.append(
                    DuplicateCause(
                        DuplicateKind.AMBIGUOUS_COLUMN,
                        f"The ORDER BY expression `{term.raw}` is ambiguous: "
                        "the same column name is declared on more than one "
                        "source, so it cannot be tied to a single unique key.",
                        columns=(term.unresolved_name or "",),
                    )
                )

        covered, missing_by_candidate, covered_candidates = _determinism_check(
            candidates, partition_keys, order_keys, fixed
        )

        if covered:
            status = RowPickStatus.DETERMINISTIC
            reasons: list[str] = []
            winning = covered_candidates[0]
            if winning:
                reasons.append(
                    "The partition, filters and ordering cover the identity key "
                    + ", ".join(
                        f"`{alias}.{column}`" for alias, column in sorted(winning)
                    )
                    + "."
                )
            else:
                reasons.append(
                    "The query has no table source and returns at most one row."
                )
            for edge in scope.joins:
                if edge.right_max_one is True and edge.right_table:
                    reasons.append(
                        f"The join to `{edge.right_table}` is at most one row "
                        "per key, so it cannot duplicate the ordering key."
                    )
            # A nullable term or an amplifying join which the winning key
            # already accounts for does not break uniqueness.
            causes = [
                cause
                for cause in causes
                if cause.kind
                not in (DuplicateKind.NULLABLE_KEY, DuplicateKind.JOIN_AMPLIFICATION)
            ]
            results.append(
                RowPickResult(
                    status=status,
                    select_statement=select,
                    rank_expression=function,
                    rank_alias=rank_alias,
                    partition_by=partition_by,
                    order_by=order_by,
                    partition_terms=partition_terms,
                    order_terms=order_terms,
                    keys=tuple(
                        tuple(
                            ColumnRef(alias, _table_for_alias(scope, alias), column)
                            for alias, column in sorted(key)
                        )
                        for key in covered_candidates
                    ),
                    duplicate_causes=tuple(causes),
                    reasons=tuple(reasons),
                    tables={
                        table.alias: table.table or table.alias
                        for table in scope.tables
                    },
                    outer_distinct=_outer_has_distinct(select),
                    filter_anchor=filter_expression,
                    configured_catalog=configured_catalog,
                )
            )
            continue

        if not candidates:
            status = RowPickStatus.INSUFFICIENT_EVIDENCE
            reasons = [cause.message for cause in causes if cause.message]
            if not reasons:
                reasons = [
                    "The catalog does not declare enough table/key metadata to "
                    "prove a tie-breaker."
                ]
            # A declared relationship which can return more than one row per
            # key is concrete amplification evidence, even if the right-side
            # table does not declare its own key.
            amplified = [
                edge
                for edge in scope.joins
                if edge.right_max_one is False and edge.right_table
            ]
            if amplified:
                status = RowPickStatus.INDETERMINATE
                for edge in amplified:
                    relationship = edge.relationship
                    columns = ", ".join(f"`{column}`" for _, column in edge.pairs)
                    reasons.append(
                        f"The catalog declares that `{edge.right_table}` can "
                        "return more than one row for each "
                        + (columns or "left-side key")
                        + (
                            f" (relationship `{relationship.name}`)."
                            if relationship and relationship.name
                            else "."
                        )
                        + " The output therefore has multiple rows per rank."
                    )
            # If a usable key exists but an ordering term is nullable, that is
            # concrete tie evidence rather than mere missing metadata.
            if nullable_order_terms:
                status = RowPickStatus.INDETERMINATE
        elif not order_terms:
            status = RowPickStatus.INDETERMINATE
            reasons = [
                "The window has no ORDER BY, so rows within each partition can "
                "be numbered arbitrarily."
            ]
        else:
            best_missing = (
                min(missing_by_candidate, key=len) if missing_by_candidate else set()
            )
            missing_text = ", ".join(
                f"`{alias}.{column}`" for alias, column in sorted(best_missing)
            )
            status = RowPickStatus.INDETERMINATE
            reasons = []
            if best_missing:
                order_text = ", ".join(f"`{term.raw}`" for term in order_terms)
                reasons.append(
                    "The ordering does not cover the unique key; rows which "
                    f"agree on {order_text} but differ in {missing_text} "
                    "contend for the same rank."
                )
            reasons.extend(
                cause.message
                for cause in causes
                if cause.kind
                in (
                    DuplicateKind.JOIN_AMPLIFICATION,
                    DuplicateKind.NULLABLE_KEY,
                    DuplicateKind.AMBIGUOUS_COLUMN,
                )
            )
            if not reasons:
                reasons.append(
                    "The ordering does not prove a unique row per partition."
                )

        results.append(
            RowPickResult(
                status=status,
                select_statement=select,
                rank_expression=function,
                rank_alias=rank_alias,
                partition_by=partition_by,
                order_by=order_by,
                partition_terms=partition_terms,
                order_terms=order_terms,
                keys=tuple(
                    tuple(
                        ColumnRef(alias, _table_for_alias(scope, alias), column)
                        for alias, column in sorted(key)
                    )
                    for key in candidates
                ),
                duplicate_causes=tuple(causes),
                reasons=tuple(reasons),
                tables={
                    table.alias: table.table or table.alias for table in scope.tables
                },
                outer_distinct=_outer_has_distinct(select),
                filter_anchor=filter_expression,
                configured_catalog=configured_catalog,
            )
        )
    return results


def analyze_row_pick(
    segment: BaseSegment,
    catalog: Optional[Catalog] = None,
    *,
    dialect_name: Optional[str] = None,
) -> list[RowPickResult]:
    """Analyse a select statement or an entire parsed file.

    Args:
        segment: A ``select_statement`` or any segment/file containing one.
        catalog: Read-only catalog metadata.
        dialect_name: Optional dialect name.

    Returns:
        A list of assessments, one per ``rank = 1`` row pick.
    """
    results: list[RowPickResult] = []
    if segment.is_type("select_statement"):
        selects = [segment]
    else:
        selects = [
            candidate
            for candidate in segment.recursive_crawl_all()
            if candidate.is_type("select_statement")
        ]
    for select in selects:
        results.extend(analyze_select(select, catalog, dialect_name=dialect_name))
    return results


__all__ = (
    "DuplicateCause",
    "DuplicateKind",
    "OrderTerm",
    "PartitionTerm",
    "RowPickAnalysis",
    "RowPickFinding",
    "RowPickResult",
    "RowPickStatus",
    "RowPickVerdict",
    "DeterminismResult",
    "DeterminismStatus",
    "analyze",
    "analyze_row_pick",
    "analyze_row_pick_determinism",
    "analyze_row_picks",
    "analyze_select",
    "check_row_pick",
)


# ---------------------------------------------------------------------------
# Compatibility aliases
# ---------------------------------------------------------------------------
# These descriptive names make the result consumable without needing to know
# which historical term a caller prefers.  ``RowPickStatus`` and
# ``RowPickResult`` remain the canonical names used by the rules.
RowPickAnalysis = RowPickResult
RowPickFinding = RowPickResult
RowPickVerdict = RowPickStatus
DeterminismStatus = RowPickStatus
DeterminismResult = RowPickResult

analyze = analyze_row_pick
analyze_row_picks = analyze_row_pick
check_row_pick = analyze_row_pick
analyze_row_pick_determinism = analyze_row_pick
