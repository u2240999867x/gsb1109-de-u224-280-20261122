"""Static delete-scope analysis for ``MERGE`` statements.

This module answers one narrow question without a database connection:

    Can a ``WHEN NOT MATCHED BY SOURCE THEN DELETE`` branch in a ``MERGE``
    script delete a target row outside the caller's allowed scope?

The default scope policy is "only rows whose ``warehouse`` column equals
``'north'`` may be deleted"; rows with a ``NULL`` warehouse are therefore
protected automatically.

The analysis is fully symbolic.  It combines three static inputs:

* the parsed ``MERGE`` script (``USING`` filter, alias projections,
  ``CASE`` expressions, equality ``ON`` matches and the ordered
  ``WHEN NOT MATCHED BY SOURCE`` branches);
* the target table DDL (``NOT NULL`` columns and ``CHECK`` constraints,
  including column ``IN (...)`` lists and boolean AND/OR/NOT predicates);
* a feed contract describing the row shapes that are guaranteed to be
  delivered for every target key, expressed as concrete alternative rows.

Every delivery allowed by the contract (each guaranteed alternative on its
own plus their combinations) is explored.  For every target row that is
statically possible under the DDL, the checker reports whether it is kept
by a source match, kept by an earlier protective branch, or reaches a
``DELETE`` branch.

Only a deliberately small SQL vocabulary is interpreted: equality and
``IN``/``IS NULL`` style boolean conditions, boolean AND/OR/NOT with SQL
null semantics, simple alias projections, ``CASE`` expressions and
equality ``ON`` matches.  Anything outside that vocabulary (unknown
functions, arithmetic, ``LIKE``, ...) is reported as *missing evidence*
with a source position instead of being guessed at.

CHECK constraints use three-valued logic conservatively: a candidate row is
excluded only when some ``CHECK`` evaluates to ``FALSE``.  Rows for which a
``CHECK`` evaluates to ``NULL`` remain statically possible and the
un-decidable constraint is reported as evidence, so a reviewer can tighten
the DDL or the analysis.  This keeps the check safe-sided and is what makes
a ``NULL`` warehouse / ``H`` route row visible to the protective branch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from itertools import combinations, product
from typing import Any, NamedTuple, Optional, Sequence

from sqlfluff.core import FluffConfig, Linter
from sqlfluff.core.parser import BaseSegment

# Segment types that carry no semantic content for this analysis.
_META_TYPES = frozenset(
    {
        "whitespace",
        "newline",
        "indent",
        "dedent",
        "implicit_indent",
        "implicit_dedent",
        "comma",
        "statement_terminator",
        "end_of_file",
        "start_bracket",
        "end_bracket",
    }
)

# Sentinel for "the SQL shape cannot be evaluated with the current vocabulary".
MISSING = object()

__all__ = [
    "Counterexample",
    "DeleteBranchResult",
    "Evidence",
    "FeedContract",
    "MergeCheckResult",
    "SourcePosition",
    "Verdict",
    "check_merge_delete_scope",
]


@dataclass(frozen=True)
class SourcePosition:
    """A 1-based line/column position inside a parsed SQL script."""

    line: int
    col: int

    def __str__(self) -> str:
        return f"L{self.line}:P{self.col}"


@dataclass(frozen=True)
class Evidence:
    """A piece of evidence (or a gap in the evidence) used in a verdict."""

    code: str
    message: str
    position: Optional[SourcePosition] = None
    snippet: str = ""

    def render(self) -> str:
        """Render the evidence note with its code, position and snippet."""
        location = f" {self.position}" if self.position else ""
        text = f"[{self.code}]{location} {self.message}"
        if self.snippet:
            text += f" -- {self.snippet}"
        return text


class Verdict(str, Enum):
    """Verdict for a single DELETE branch."""

    IN_SCOPE = "in_scope"
    OUT_OF_SCOPE = "out_of_scope"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FeedContract:
    """The source data delivery contract.

    Args:
        columns: Column order of the feed, e.g.
            ``("sku", "route", "backup_route", "kind", "approved")``.
        alternatives: Alternative row shapes guaranteed to be delivered for
            every target key.  Each tuple follows ``columns`` order.  At
            least one alternative is delivered per key; receiving only one
            of them still satisfies the contract.
        key_column: Column whose values identify the target key (e.g.
            ``"sku"``).  Every target key is guaranteed at least one
            alternative row carrying that key.
        extra_rows_allowed: Whether additional, non-guaranteed rows may be
            delivered.  Extra rows can only add source matches, never remove
            them, so they cannot widen a delete.
    """

    columns: Sequence[str]
    alternatives: Sequence[Sequence[Any]]
    key_column: str
    extra_rows_allowed: bool = True


@dataclass(frozen=True)
class Counterexample:
    """A concrete, contract-valid scenario that reaches an out-of-scope delete."""

    target_row: dict[str, Any]
    delivery_name: str
    delivery: list[dict[str, Any]]
    retained_rows: list[dict[str, Any]]
    on_evaluation: list[str]
    branch_path: list[str]
    delete_branch_position: SourcePosition


class _Outcome(NamedTuple):
    kind: str  # "match" | "delete" | "do_nothing" | "update" | "no_action"
    branch_index: Optional[int]


@dataclass
class RowDisposition:
    """What happens to one statically possible target row."""

    row: dict[str, Any]
    outcomes: dict[str, _Outcome] = field(default_factory=dict)

    def reaches(self, branch_index: int) -> list[str]:
        return [
            name
            for name, outcome in self.outcomes.items()
            if outcome.kind == "delete" and outcome.branch_index == branch_index
        ]

    def match_deliveries(self) -> list[str]:
        return [
            name for name, outcome in self.outcomes.items() if outcome.kind == "match"
        ]

    def protective_deliveries(self) -> dict[str, int]:
        return {
            name: outcome.branch_index
            for name, outcome in self.outcomes.items()
            if outcome.kind in ("do_nothing", "update")
            and outcome.branch_index is not None
        }

    def no_action_deliveries(self) -> list[str]:
        return [
            name
            for name, outcome in self.outcomes.items()
            if outcome.kind == "no_action"
        ]


@dataclass
class DeleteBranchResult:
    """Conclusion for one ``WHEN NOT MATCHED BY SOURCE THEN DELETE`` branch."""

    index: int
    position: SourcePosition
    verdict: Verdict
    scope_policy: str
    reachable_rows: list[dict[str, Any]] = field(default_factory=list)
    dispositions: list[RowDisposition] = field(default_factory=list)
    counterexample: Optional[Counterexample] = None
    evidence: list[Evidence] = field(default_factory=list)

    @property
    def decidable(self) -> bool:
        """Whether enough evidence exists to give a definitive verdict."""
        return self.verdict != Verdict.UNKNOWN


@dataclass
class MergeCheckResult:
    """Result of a delete-scope check, covering every DELETE branch found."""

    compliant: bool
    decidable: bool
    policy: str
    branches: list[DeleteBranchResult] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)

    def render_text(self) -> str:
        """Render a human-readable report that points back into the script."""
        lines = [
            f"Delete-scope check (policy: {self.policy})",
            f"  overall: {'COMPLIANT' if self.compliant else 'NON-COMPLIANT'}"
            f" (decidable={self.decidable})",
        ]
        for branch in self.branches:
            lines.append(
                f"  DELETE branch #{branch.index} at {branch.position}: "
                f"{branch.verdict.value}"
            )
            for disposition in branch.dispositions:
                row_text = _render_row(disposition.row)
                matched = disposition.match_deliveries()
                protected = disposition.protective_deliveries()
                deleted = disposition.reaches(branch.index)
                if deleted:
                    lines.append(f"    - {row_text} -> DELETE in: {', '.join(deleted)}")
                elif protected:
                    protected_text = ", ".join(
                        f"{name} via branch #{idx}" for name, idx in protected.items()
                    )
                    lines.append(
                        f"    - {row_text} -> kept by earlier branch: {protected_text}"
                    )
                elif matched:
                    lines.append(
                        f"    - {row_text} -> kept by source match in: "
                        f"{', '.join(matched)}"
                    )
                elif disposition.no_action_deliveries():
                    lines.append(
                        f"    - {row_text} -> no WHEN clause matches, row left "
                        f"unchanged in: {', '.join(disposition.no_action_deliveries())}"
                    )
                else:
                    lines.append(f"    - {row_text} -> no delivery reaches this row")
            if branch.counterexample:
                counterexample = branch.counterexample
                lines.append("    counterexample:")
                lines.append(
                    f"      target row: {_render_row(counterexample.target_row)}"
                )
                lines.append(
                    f"      contract delivery ({counterexample.delivery_name}):"
                )
                retained_keys = {
                    tuple(item["feed_row"].items())
                    for item in counterexample.retained_rows
                }
                for feed_row in counterexample.delivery:
                    marker = (
                        "retained by USING"
                        if tuple(feed_row.items()) in retained_keys
                        else "filtered out by USING WHERE"
                    )
                    lines.append(f"        {_render_row(feed_row)}  [{marker}]")
                lines.append("      rows retained by USING, with projections:")
                if not counterexample.retained_rows:
                    lines.append("        (none)")
                for retained in counterexample.retained_rows:
                    lines.append(
                        "        feed "
                        f"{_render_row(retained['feed_row'])} -> projections "
                        f"{_render_row(retained['projections'])}"
                    )
                lines.append("      ON evaluation:")
                lines.extend(f"        {item}" for item in counterexample.on_evaluation)
                lines.append("      branch path:")
                lines.extend(f"        {item}" for item in counterexample.branch_path)
                lines.append(
                    f"      lands in DELETE branch #{branch.index} at "
                    f"{counterexample.delete_branch_position}"
                )
        for note in self.evidence:
            lines.append(f"  evidence: {note.render()}")
        for branch in self.branches:
            for note in branch.evidence:
                lines.append(f"  evidence: {note.render()}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _position(segment: BaseSegment) -> SourcePosition:
    assert segment.pos_marker is not None, (
        f"segment {segment.type!r} has no position marker"
    )
    point = segment.pos_marker.start_point_marker().source_position()
    return SourcePosition(line=point[0], col=point[1])


def _children(segment: BaseSegment) -> list[BaseSegment]:
    return [child for child in segment.segments if child.type not in _META_TYPES]


def _snippet(segment: BaseSegment, limit: int = 80) -> str:
    raw = " ".join(segment.raw.split())
    return raw if len(raw) <= limit else raw[: limit - 3] + "..."


def _identifier_name(segment: BaseSegment) -> str:
    raw = segment.raw
    if (
        segment.is_type("quoted_identifier")
        and raw.startswith('"')
        and raw.endswith('"')
    ):
        return raw[1:-1].replace('""', '"')
    return raw


def _column_parts(segment: BaseSegment) -> Optional[tuple[str, ...]]:
    if not segment.is_type("column_reference"):
        return None
    names = [
        _identifier_name(child)
        for child in segment.recursive_crawl(
            "naked_identifier", "quoted_identifier", "identifier"
        )
    ]
    return tuple(names) if names else None


def _literal_value(segment: BaseSegment) -> Any:
    """Convert a literal segment to a Python value, or :data:`MISSING`."""
    if segment.is_type("null_literal"):
        return None
    if segment.is_type("boolean_literal"):
        return segment.raw.upper() == "TRUE"
    if segment.is_type("quoted_literal"):
        return segment.raw[1:-1].replace("''", "'")
    if segment.is_type("numeric_literal"):
        return float(segment.raw) if "." in segment.raw else int(segment.raw)
    if segment.is_type("literal"):
        kids = [child for child in segment.segments if child.type not in _META_TYPES]
        if len(kids) == 1:
            return _literal_value(kids[0])
        raw = segment.raw
        upper = raw.upper()
        if upper == "NULL":
            return None
        if upper in ("TRUE", "FALSE"):
            return upper == "TRUE"
        if raw.startswith("'") and raw.endswith("'"):
            return raw[1:-1].replace("''", "'")
    return MISSING


# ---------------------------------------------------------------------------
# Small boolean/condition parser producing plain tuple nodes
#
# Node shapes:
#   ("and", [nodes]) | ("or", [nodes]) | ("not", node)
#   ("is_null", parts) | ("is_not_null", parts)
#   ("in", parts, [values], negated)
#   ("cmp", parts, op, ("lit", value) | ("col", parts))
#   ("bool", ("lit", value) | ("col", parts))
# ---------------------------------------------------------------------------


def _split_on_keyword(
    tokens: Sequence[BaseSegment], word: str
) -> list[list[BaseSegment]]:
    groups: list[list[BaseSegment]] = [[]]
    for token in tokens:
        if token.is_type("binary_operator") and token.raw.upper() == word:
            groups.append([])
        else:
            groups[-1].append(token)
    return groups


def _parse_atom(segment: BaseSegment) -> Any:
    if segment.is_type("bracketed"):
        inner = segment.get_child("expression")
        return _parse_expression(inner) if inner else MISSING
    parts = _column_parts(segment)
    if parts is not None:
        return ("col", parts)
    value = _literal_value(segment)
    if value is not MISSING:
        return ("lit", value)
    return MISSING


def _parse_not(tokens: Sequence[BaseSegment]) -> Any:
    tokens = list(tokens)
    if not tokens:
        return MISSING
    if (
        len(tokens) >= 2
        and tokens[0].is_type("keyword")
        and tokens[0].raw.upper() == "NOT"
    ):
        rest = tokens[1:]
        if len(rest) == 1:
            atom = _parse_atom(rest[0])
            if atom is MISSING:
                return MISSING
            return ("not", atom)
        parsed = _parse_predicate_tokens(rest)
        if parsed is MISSING:
            return MISSING
        return ("not", parsed)
    return _parse_predicate_tokens(tokens)


def _parse_and(tokens: Sequence[BaseSegment]) -> Any:
    groups = _split_on_keyword(list(tokens), "AND")
    nodes = [_parse_not(group) for group in groups]
    if any(node is MISSING for node in nodes):
        return MISSING
    return nodes[0] if len(nodes) == 1 else ("and", nodes)


def _parse_expression(segment: Optional[BaseSegment]) -> Any:
    if segment is None:
        return MISSING
    if segment.is_type("bracketed"):
        inner = segment.get_child("expression")
        return _parse_expression(inner) if inner else MISSING
    if not segment.is_type("expression"):
        return _parse_atom(segment)
    tokens = _children(segment)
    groups = _split_on_keyword(tokens, "OR")
    nodes = [_parse_and(group) for group in groups]
    if any(node is MISSING for node in nodes):
        return MISSING
    return nodes[0] if len(nodes) == 1 else ("or", nodes)


def _parse_predicate_tokens(tokens: list[BaseSegment]) -> Any:
    tokens = [token for token in tokens if token.type not in _META_TYPES]
    if len(tokens) == 1:
        atom = _parse_atom(tokens[0])
        if atom is MISSING:
            return MISSING
        if atom[0] in ("col", "lit"):
            return ("bool", atom)
        return atom

    # col IS [NOT] NULL
    if (
        len(tokens) in (3, 4)
        and (parts := _column_parts(tokens[0])) is not None
        and tokens[1].is_type("keyword")
        and tokens[1].raw.upper() == "IS"
    ):
        negated = len(tokens) == 4
        if negated and not (
            tokens[2].is_type("keyword") and tokens[2].raw.upper() == "NOT"
        ):
            return MISSING
        null_token = tokens[-1]
        if not null_token.is_type("null_literal", "literal") or (
            null_token.is_type("literal") and _literal_value(null_token) is not None
        ):
            return MISSING
        return ("is_not_null" if negated else "is_null", parts)

    # col [NOT] IN (literals...)
    in_index = next(
        (
            idx
            for idx, token in enumerate(tokens)
            if token.is_type("keyword") and token.raw.upper() == "IN"
        ),
        None,
    )
    if in_index is not None and (parts := _column_parts(tokens[0])) is not None:
        negated = in_index == 2 and tokens[1].raw.upper() == "NOT"
        bracket = tokens[-1]
        if not bracket.is_type("bracketed"):
            return MISSING
        values = []
        for literal in bracket.recursive_crawl(
            "quoted_literal",
            "numeric_literal",
            "boolean_literal",
            "null_literal",
            "literal",
        ):
            value = _literal_value(literal)
            if value is MISSING:
                return MISSING
            values.append(value)
        return ("in", parts, values, negated)

    # binary comparison: atom <op> atom
    operator_index = next(
        (
            idx
            for idx, token in enumerate(tokens)
            if token.is_type("comparison_operator")
        ),
        None,
    )
    if operator_index is not None:
        left_tokens = tokens[:operator_index]
        right_tokens = tokens[operator_index + 1 :]
        if len(left_tokens) != 1 or len(right_tokens) != 1:
            return MISSING
        left = _parse_atom(left_tokens[0])
        right = _parse_atom(right_tokens[0])
        if left is MISSING or right is MISSING or left[0] != "col":
            return MISSING
        return ("cmp", left[1], tokens[operator_index].raw.upper(), right)

    return MISSING


def _resolve_column(parts: tuple[str, ...], row: dict[str, Any]) -> Any:
    name = parts[-1]
    if name not in row:
        return MISSING
    return row[name]


def _eval_node(node: Any, row: dict[str, Any]) -> Any:
    """Evaluate a parsed node against a row.

    Returns ``True``/``False``/``None`` (SQL unknown) or :data:`MISSING`.
    """
    if node is MISSING:
        return MISSING
    # A bare truth value stands in for a missing WHERE/guard (always applies).
    if node is True or node is False or node is None:
        return node
    kind = node[0]
    if kind == "and":
        result: Any = True
        for child in node[1]:
            value = _eval_node(child, row)
            if value is MISSING:
                return MISSING
            if value is False:
                return False
            if value is None:
                result = None
        return result
    if kind == "or":
        result = False
        for child in node[1]:
            value = _eval_node(child, row)
            if value is MISSING:
                return MISSING
            if value is True:
                return True
            if value is None:
                result = None
        return result
    if kind == "not":
        value = _eval_node(node[1], row)
        if value is MISSING or value is None:
            return value
        return not value
    if kind in ("is_null", "is_not_null"):
        value = _resolve_column(node[1], row)
        if value is MISSING:
            return MISSING
        is_null = value is None
        return is_null if kind == "is_null" else not is_null
    if kind == "in":
        value = _resolve_column(node[1], row)
        if value is MISSING:
            return MISSING
        if value is None:
            return None
        membership = value in node[2]
        return (not membership) if node[3] else membership
    if kind == "cmp":
        left = _resolve_column(node[1], row)
        right_descriptor = node[3]
        if right_descriptor[0] == "lit":
            right = right_descriptor[1]
        else:
            right = _resolve_column(right_descriptor[1], row)
        if left is MISSING or right is MISSING:
            return MISSING
        if left is None or right is None:
            return None
        operator = node[2]
        if operator in ("=", "=="):
            return left == right
        if operator in ("<>", "!="):
            return left != right
        return MISSING
    if kind == "bool":
        descriptor = node[1]
        if descriptor[0] == "lit":
            value = descriptor[1]
        else:
            value = _resolve_column(descriptor[1], row)
        if value is MISSING:
            return MISSING
        if value is None:
            return None
        return bool(value)
    return MISSING


def _unsupported_evidence(
    segment: BaseSegment, code: str, message: str
) -> list[Evidence]:
    """Find concrete unsupported constructs inside an unevaluable segment."""
    notes: list[Evidence] = []
    for function in segment.recursive_crawl("function"):
        name = function.get_child("function_name")
        notes.append(
            Evidence(
                code="unknown_function",
                message=(
                    f"function {name.raw if name else function.raw!r} is not in the "
                    "supported vocabulary; its result cannot be evaluated statically"
                ),
                position=_position(function),
                snippet=_snippet(function),
            )
        )
    if not notes:
        notes.append(
            Evidence(
                code=code,
                message=message,
                position=_position(segment),
                snippet=_snippet(segment),
            )
        )
    return notes


# ---------------------------------------------------------------------------
# DDL model
# ---------------------------------------------------------------------------


@dataclass
class _ColumnDomain:
    name: str
    nullable: bool
    enum_values: Optional[tuple[Any, ...]] = None


@dataclass
class _TableModel:
    name: str
    domains: dict[str, _ColumnDomain]
    checks: list[Any]
    check_segments: list[BaseSegment]
    evidence: list[Evidence]


def _parse_string(linter: Linter, sql: str) -> Optional[Any]:
    parsed = linter.parse_string(sql)
    if parsed.tree is None or parsed.violations:
        return None
    if "unparsable" in parsed.tree.descendant_type_set:
        return None
    return parsed


def _parse_ddl(
    ddl_sql: str, linter: Linter
) -> tuple[Optional[_TableModel], list[Evidence]]:
    parsed = _parse_string(linter, ddl_sql)
    if parsed is None:
        return None, [
            Evidence(
                code="unparsable_ddl",
                message="target DDL could not be parsed; no constraints are available",
            )
        ]
    statement = next(parsed.tree.recursive_crawl("create_table_statement"), None)
    if statement is None:
        return None, [
            Evidence(
                code="missing_create_table",
                message="no CREATE TABLE statement found in the target DDL input",
            )
        ]

    table_ref = statement.get_child("table_reference")
    table_name = table_ref.raw if table_ref else "<unknown>"
    domains: dict[str, _ColumnDomain] = {}
    check_nodes: list[Any] = []
    check_segments: list[BaseSegment] = []
    evidence: list[Evidence] = []

    for definition in statement.recursive_crawl("column_definition"):
        name_segment = next(
            definition.recursive_crawl("naked_identifier", "quoted_identifier"),
            None,
        )
        if name_segment is None:
            continue
        column = _identifier_name(name_segment)
        not_null = any(
            constraint.raw.upper().replace(" ", "").startswith("NOTNULL")
            for constraint in definition.recursive_crawl("column_constraint_segment")
        )
        domains[column] = _ColumnDomain(name=column, nullable=not not_null)

    for constraint in statement.recursive_crawl(
        "column_constraint_segment", "table_constraint"
    ):
        if not any(
            keyword.raw.upper() == "CHECK"
            for keyword in constraint.recursive_crawl("keyword")
        ):
            continue
        bracketed = next(constraint.recursive_crawl("bracketed"), None)
        expression = bracketed.get_child("expression") if bracketed else None
        if expression is None:
            evidence.append(
                Evidence(
                    code="unparsable_check",
                    message="CHECK constraint has no evaluable expression",
                    position=_position(constraint),
                    snippet=_snippet(constraint),
                )
            )
            continue
        node = _parse_expression(expression)
        if node is MISSING:
            evidence.extend(
                _unsupported_evidence(
                    expression,
                    "unsupported_check",
                    "CHECK constraint uses unsupported syntax and is not enforced "
                    "by the static row enumeration",
                )
            )
            continue
        check_nodes.append(node)
        check_segments.append(expression)
        _absorb_enum(node, domains, evidence, expression)

    return (
        _TableModel(
            name=table_name,
            domains=domains,
            checks=check_nodes,
            check_segments=check_segments,
            evidence=evidence,
        ),
        evidence,
    )


def _absorb_enum(
    node: Any,
    domains: dict[str, _ColumnDomain],
    evidence: list[Evidence],
    segment: BaseSegment,
) -> None:
    """Turn ``col IN (...)`` and ``col = 'a' OR col = 'b'`` into enum domains."""
    candidates: list[tuple[tuple[str, ...], list[Any]]] = []
    if node[0] == "in" and not node[3]:
        candidates.append((node[1], list(node[2])))
    elif node[0] == "or":
        equalities: list[tuple[tuple[str, ...], Any]] = []
        for child in node[1]:
            if child[0] == "cmp" and child[2] in ("=", "==") and child[3][0] == "lit":
                equalities.append((child[1], child[3][1]))
            else:
                equalities = []
                break
        if equalities:
            first_parts = equalities[0][0]
            if all(parts == first_parts for parts, _ in equalities):
                candidates.append((first_parts, [value for _, value in equalities]))
    for parts, values in candidates:
        column = parts[-1]
        domain = domains.get(column)
        if domain is None:
            evidence.append(
                Evidence(
                    code="unknown_constraint_column",
                    message=f"CHECK references column {column!r} absent from the DDL",
                    position=_position(segment),
                    snippet=_snippet(segment),
                )
            )
            continue
        enum_values = tuple(values)
        if domain.enum_values is not None:
            enum_values = tuple(
                value for value in domain.enum_values if value in values
            )
        domain.enum_values = enum_values


# ---------------------------------------------------------------------------
# MERGE model
# ---------------------------------------------------------------------------


@dataclass
class _Projection:
    alias: str
    segment: BaseSegment
    scalar: BaseSegment


@dataclass
class _SourceBranch:
    index: int
    segment: BaseSegment
    guard: Any
    guard_segment: Optional[BaseSegment]
    action: str  # "delete" | "do_nothing" | "update"


@dataclass
class _MergeModel:
    segment: BaseSegment
    target_alias: str
    source_alias: str
    projections: list[_Projection]
    where_node: Any
    where_segment: Optional[BaseSegment]
    on_pairs: list[tuple[str, str, BaseSegment]]
    source_branches: list[_SourceBranch]
    delete_branches: list[_SourceBranch]
    evidence: list[Evidence]


def _parse_merge(
    merge_sql: str, linter: Linter
) -> tuple[list[_MergeModel], list[Evidence]]:
    parsed = _parse_string(linter, merge_sql)
    if parsed is None:
        return [], [
            Evidence(
                code="unparsable_merge",
                message="MERGE script could not be parsed cleanly",
            )
        ]
    models: list[_MergeModel] = []
    evidence: list[Evidence] = []
    for statement in parsed.tree.recursive_crawl("merge_statement"):
        model, model_evidence = _parse_one_merge(statement)
        evidence.extend(model_evidence)
        if model is not None:
            models.append(model)
    if not models and not evidence:
        evidence.append(
            Evidence(
                code="missing_merge",
                message="no MERGE statement found in the script input",
            )
        )
    return models, evidence


def _parse_one_merge(
    statement: BaseSegment,
) -> tuple[Optional[_MergeModel], list[Evidence]]:
    evidence: list[Evidence] = []
    direct = _children(statement)

    alias_segments = [child for child in direct if child.is_type("alias_expression")]
    target_alias = _alias_name(alias_segments[0]) if alias_segments else ""

    using_bracket = next(
        (
            child
            for child in direct
            if child.is_type("bracketed")
            and next(child.recursive_crawl("select_statement"), None) is not None
        ),
        None,
    )
    if using_bracket is None:
        evidence.append(
            Evidence(
                code="unsupported_using",
                message=(
                    "USING is not a bracketed subquery; feed rows cannot be "
                    "evaluated against the contract"
                ),
                position=_position(statement),
                snippet=_snippet(statement),
            )
        )
        return None, evidence

    using_index = direct.index(using_bracket)
    source_alias = ""
    for later in direct[using_index + 1 :]:
        if later.is_type("alias_expression"):
            source_alias = _alias_name(later)
            break
        if later.is_type("join_on_condition", "merge_match"):
            break

    select_statement = next(using_bracket.recursive_crawl("select_statement"))
    select_clause = select_statement.get_child("select_clause")
    projections: list[_Projection] = []
    if select_clause is not None:
        for element in select_clause.recursive_crawl("select_clause_element"):
            alias_expression = element.get_child("alias_expression")
            if alias_expression is None:
                evidence.append(
                    Evidence(
                        code="unnamed_projection",
                        message=(
                            "USING projection has no alias; it cannot be tied to an "
                            "ON comparison"
                        ),
                        position=_position(element),
                        snippet=_snippet(element),
                    )
                )
                continue
            scalar = (
                element.get_child("expression")
                or element.get_child("column_reference")
                or element.get_child("function")
            )
            if scalar is None:
                evidence.extend(
                    _unsupported_evidence(
                        element,
                        "unsupported_projection",
                        "USING projection has no evaluable scalar expression",
                    )
                )
                continue
            projections.append(
                _Projection(
                    alias=_alias_name(alias_expression),
                    segment=element,
                    scalar=scalar,
                )
            )

    where_clause = select_statement.get_child("where_clause")
    where_segment = where_clause.get_child("expression") if where_clause else None
    where_node = _parse_expression(where_segment) if where_segment else True
    if where_node is MISSING and where_segment is not None:
        evidence.extend(
            _unsupported_evidence(
                where_segment,
                "unsupported_filter",
                "USING filter cannot be evaluated; retained feed rows are unknown",
            )
        )

    on_condition = next(statement.recursive_crawl("join_on_condition"), None)
    on_pairs: list[tuple[str, str, BaseSegment]] = []
    if on_condition is not None:
        on_expression = on_condition.get_child("expression")
        if on_expression is not None:
            for condition in _split_on_keyword(_children(on_expression), "AND"):
                pair = _parse_on_equality(condition, target_alias, source_alias)
                if pair is None:
                    evidence.extend(
                        _unsupported_evidence(
                            on_expression,
                            "unsupported_on",
                            "ON contains a condition that is not a simple equality "
                            "of target and source columns",
                        )
                    )
                    on_pairs = []
                    break
                on_pairs.append(pair)

    source_branches: list[_SourceBranch] = []
    delete_branches: list[_SourceBranch] = []
    merge_match = next(statement.recursive_crawl("merge_match"), None)
    if merge_match is not None:
        index = 0
        for clause in _children(merge_match):
            if not clause.is_type("merge_when_not_matched_by_source_clause"):
                continue
            guard_segment = clause.get_child("expression")
            guard = _parse_expression(guard_segment) if guard_segment else True
            if guard is MISSING:
                assert guard_segment is not None
                evidence.extend(
                    _unsupported_evidence(
                        guard_segment,
                        "unsupported_branch_guard",
                        "WHEN NOT MATCHED BY SOURCE guard cannot be evaluated",
                    )
                )
            action = "do_nothing"
            if next(clause.recursive_crawl("merge_delete_clause"), None) is not None:
                action = "delete"
            elif next(clause.recursive_crawl("merge_update_clause"), None) is not None:
                action = "update"
            branch = _SourceBranch(
                index=index,
                segment=clause,
                guard=guard,
                guard_segment=guard_segment,
                action=action,
            )
            source_branches.append(branch)
            if action == "delete":
                delete_branches.append(branch)
            index += 1

    return (
        _MergeModel(
            segment=statement,
            target_alias=target_alias,
            source_alias=source_alias,
            projections=projections,
            where_node=where_node,
            where_segment=where_segment,
            on_pairs=on_pairs,
            source_branches=source_branches,
            delete_branches=delete_branches,
            evidence=evidence,
        ),
        evidence,
    )


def _alias_name(alias_expression: BaseSegment) -> str:
    identifier = next(
        alias_expression.recursive_crawl(
            "naked_identifier", "quoted_identifier", "identifier"
        ),
        None,
    )
    return _identifier_name(identifier) if identifier else alias_expression.raw


def _parse_on_equality(
    condition: list[BaseSegment], target_alias: str, source_alias: str
) -> Optional[tuple[str, str, BaseSegment]]:
    tokens = [token for token in condition if token.type not in _META_TYPES]
    # Equality predicates may be individually parenthesised.
    if len(tokens) == 1 and tokens[0].is_type("bracketed"):
        inner = tokens[0].get_child("expression")
        if inner is not None:
            tokens = _children(inner)
    operator = next(
        (token for token in tokens if token.is_type("comparison_operator")),
        None,
    )
    if operator is None or operator.raw.upper() not in ("=", "=="):
        return None
    operator_index = tokens.index(operator)
    left = tokens[:operator_index]
    right = tokens[operator_index + 1 :]
    if len(left) != 1 or len(right) != 1:
        return None
    left_parts = _column_parts(left[0])
    right_parts = _column_parts(right[0])
    if left_parts is None or right_parts is None:
        return None
    if _matches_alias(left_parts, target_alias) and _matches_alias(
        right_parts, source_alias
    ):
        return left_parts[-1], right_parts[-1], operator
    if _matches_alias(right_parts, target_alias) and _matches_alias(
        left_parts, source_alias
    ):
        return right_parts[-1], left_parts[-1], operator
    if target_alias == "" and source_alias == "":
        return left_parts[-1], right_parts[-1], operator
    return None


def _matches_alias(parts: tuple[str, ...], alias: str) -> bool:
    if len(parts) == 1:
        return True
    return parts[0] == alias


# ---------------------------------------------------------------------------
# Feed row / projection evaluation
# ---------------------------------------------------------------------------


def _evaluate_scalar(segment: BaseSegment, row: dict[str, Any]) -> Any:
    case_expression = (
        segment
        if segment.is_type("case_expression")
        else next(segment.recursive_crawl("case_expression"), None)
    )
    if case_expression is not None:
        for when_clause in case_expression.recursive_crawl("when_clause"):
            condition = when_clause.get_child("expression")
            node = _parse_expression(condition)
            if node is MISSING:
                return MISSING
            if _eval_node(node, row) is True:
                result_expression = _then_expression(when_clause)
                if result_expression is None:
                    return MISSING
                return _evaluate_scalar(result_expression, row)
        else_clause = next(case_expression.recursive_crawl("else_clause"), None)
        if else_clause is None:
            return None
        result_expression = _then_expression(else_clause)
        if result_expression is None:
            return MISSING
        return _evaluate_scalar(result_expression, row)
    parts = _column_parts(segment)
    if parts is not None:
        return _resolve_column(parts, row)
    value = _literal_value(segment)
    if value is not MISSING:
        return value
    if segment.is_type("expression", "bracketed"):
        inner = segment.get_child("expression") or segment.get_child("column_reference")
        if inner is not None:
            return _evaluate_scalar(inner, row)
    return MISSING


def _then_expression(segment: BaseSegment) -> Optional[BaseSegment]:
    expressions = [
        child
        for child in segment.segments
        if child.is_type(
            "expression",
            "column_reference",
            "quoted_literal",
            "literal",
        )
    ]
    if not expressions:
        return None
    return expressions[-1]


def _contract_rows(contract: FeedContract) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for alternative in contract.alternatives:
        if len(alternative) != len(contract.columns):
            raise ValueError(
                "feed contract alternative has "
                f"{len(alternative)} values but columns has {len(contract.columns)}"
            )
        rows.append(dict(zip(contract.columns, alternative)))
    return rows


def _delivery_subsets(
    rows: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    deliveries: list[tuple[str, list[dict[str, Any]]]] = []
    for size in range(1, len(rows) + 1):
        for indexes in combinations(range(len(rows)), size):
            name = "+".join(f"alt{idx + 1}" for idx in indexes)
            deliveries.append((name, [rows[idx] for idx in indexes]))
    return deliveries


# ---------------------------------------------------------------------------
# Target row enumeration
# ---------------------------------------------------------------------------


def _enumerate_target_rows(
    table: _TableModel,
    needed_columns: set[str],
    key_column: str,
    key_values: Sequence[Any],
    evidence: list[Evidence],
) -> tuple[list[dict[str, Any]], bool]:
    domains: dict[str, list[Any]] = {}
    complete = True
    for column in sorted(needed_columns):
        if column == key_column:
            domains[column] = list(dict.fromkeys(key_values))
            continue
        domain = table.domains.get(column)
        if domain is None or domain.enum_values is None:
            evidence.append(
                Evidence(
                    code="missing_column_domain",
                    message=(
                        "no finite value domain can be derived for column "
                        f"{column!r}; add an IN/equality CHECK to make the delete "
                        "scope decidable"
                    ),
                )
            )
            complete = False
            continue
        values = list(domain.enum_values)
        if domain.nullable:
            values.append(None)
        domains[column] = values
    if not complete:
        return [], False

    rows: list[dict[str, Any]] = []
    column_names = list(domains)
    for combo in product(*(domains[name] for name in column_names)):
        row = dict(zip(column_names, combo))
        if _row_is_possible(row, table, evidence):
            rows.append(row)
    return rows, True


def _row_is_possible(
    row: dict[str, Any], table: _TableModel, evidence: list[Evidence]
) -> bool:
    for column, value in row.items():
        domain = table.domains.get(column)
        if domain is not None and value is None and not domain.nullable:
            return False
    for node, segment in zip(table.checks, table.check_segments):
        result = _eval_node(node, row)
        if result is MISSING:
            evidence.append(
                Evidence(
                    code="unsupported_check",
                    message="CHECK cannot be evaluated for an enumerated row",
                    position=_position(segment),
                    snippet=_snippet(segment),
                )
            )
            continue
        if result is False:
            return False
    return True


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def check_merge_delete_scope(
    merge_sql: str,
    ddl_sql: str,
    contract: FeedContract,
    *,
    dialect: str = "postgres",
    policy_column: str = "warehouse",
    policy_value: Any = "north",
) -> MergeCheckResult:
    """Check whether every DELETE branch of a MERGE script stays in scope.

    Args:
        merge_sql: The MERGE script to analyse.
        ddl_sql: The CREATE TABLE DDL of the MERGE target.
        contract: The guaranteed feed delivery contract.
        dialect: SQLFluff dialect name used to parse all three inputs.
        policy_column: Target column that defines the delete scope.
        policy_value: Only rows where ``policy_column`` equals this value may
            be deleted. A ``NULL`` policy column is always out of scope.

    Returns:
        A :class:`MergeCheckResult` with one verdict per DELETE branch,
        per-delivery row dispositions and concrete counterexamples when a
        branch is out of scope or evidence is insufficient.
    """
    config = FluffConfig.from_kwargs(dialect=dialect)
    linter = Linter(config=config)
    evidence: list[Evidence] = []

    table, ddl_evidence = _parse_ddl(ddl_sql, linter)
    evidence.extend(ddl_evidence)
    models, merge_evidence = _parse_merge(merge_sql, linter)
    evidence.extend(merge_evidence)

    if table is None or not models:
        return MergeCheckResult(
            compliant=False,
            decidable=False,
            policy=f"{policy_column} = {policy_value!r} (NULL protected)",
            evidence=evidence,
        )

    if contract.key_column not in contract.columns:
        evidence.append(
            Evidence(
                code="missing_contract_key",
                message=(
                    f"contract key column {contract.key_column!r} is not part of "
                    "the feed column list"
                ),
            )
        )
        return MergeCheckResult(
            compliant=False,
            decidable=False,
            policy=f"{policy_column} = {policy_value!r} (NULL protected)",
            evidence=evidence,
        )

    if not contract.alternatives:
        evidence.append(
            Evidence(
                code="empty_contract",
                message=(
                    "the feed contract guarantees no delivery rows, so it cannot "
                    "establish which target keys are matched"
                ),
            )
        )
        return MergeCheckResult(
            compliant=False,
            decidable=False,
            policy=f"{policy_column} = {policy_value!r} (NULL protected)",
            evidence=evidence,
        )

    feed_rows = _contract_rows(contract)
    key_values = [row[contract.key_column] for row in feed_rows]
    branch_results: list[DeleteBranchResult] = []
    decidable = True

    for model in models:
        evidence.extend(model.evidence)
        results, model_decidable = _analyse_model(
            model=model,
            table=table,
            contract=contract,
            feed_rows=feed_rows,
            key_values=key_values,
            policy_column=policy_column,
            policy_value=policy_value,
        )
        branch_results.extend(results)
        for branch in results:
            evidence.extend(branch.evidence)
        if not model_decidable:
            decidable = False

    compliant = all(branch.verdict == Verdict.IN_SCOPE for branch in branch_results)
    return MergeCheckResult(
        compliant=compliant,
        decidable=decidable,
        policy=f"{policy_column} = {policy_value!r} (NULL protected)",
        branches=branch_results,
        evidence=evidence,
    )


def _analyse_model(
    *,
    model: _MergeModel,
    table: _TableModel,
    contract: FeedContract,
    feed_rows: list[dict[str, Any]],
    key_values: Sequence[Any],
    policy_column: str,
    policy_value: Any,
) -> tuple[list[DeleteBranchResult], bool]:
    scope_policy = f"{policy_column} = {policy_value!r} (NULL protected)"
    if not model.delete_branches:
        return [], True

    needed_columns = {policy_column, contract.key_column}
    for target_column, _, _ in model.on_pairs:
        needed_columns.add(target_column)
    for branch in model.source_branches:
        for ref in branch.segment.recursive_crawl("column_reference"):
            parts = _column_parts(ref)
            if parts:
                needed_columns.add(parts[-1])

    local_evidence: list[Evidence] = []
    target_rows, rows_complete = _enumerate_target_rows(
        table, needed_columns, contract.key_column, key_values, local_evidence
    )

    unsupported_guard = any(branch.guard is MISSING for branch in model.source_branches)
    if (
        not rows_complete
        or unsupported_guard
        or model.where_node is MISSING
        or not model.on_pairs
        or not model.projections
    ):
        return (
            [
                DeleteBranchResult(
                    index=branch.index,
                    position=_position(branch.segment),
                    verdict=Verdict.UNKNOWN,
                    scope_policy=scope_policy,
                    evidence=list(local_evidence),
                )
                for branch in model.delete_branches
            ],
            False,
        )

    deliveries = _delivery_subsets(feed_rows)
    evaluated_deliveries: list[
        tuple[
            str,
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[tuple[dict[str, Any], dict[str, Any]]],
        ]
    ] = []
    for delivery_name, delivery_rows in deliveries:
        retained: list[dict[str, Any]] = []
        projected_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for feed_row in delivery_rows:
            if _eval_node(model.where_node, feed_row) is not True:
                continue
            projections: dict[str, Any] = {}
            for projection in model.projections:
                value = _evaluate_scalar(projection.scalar, feed_row)
                if value is MISSING:
                    local_evidence.extend(
                        _unsupported_evidence(
                            projection.segment,
                            "unsupported_projection",
                            f"projection {projection.alias!r} cannot be evaluated",
                        )
                    )
                    return (
                        [
                            DeleteBranchResult(
                                index=branch.index,
                                position=_position(branch.segment),
                                verdict=Verdict.UNKNOWN,
                                scope_policy=scope_policy,
                                evidence=list(local_evidence),
                            )
                            for branch in model.delete_branches
                        ],
                        False,
                    )
                projections[projection.alias] = value
            enriched = dict(feed_row)
            enriched["__projections__"] = projections
            retained.append(enriched)
            projected_rows.append((dict(feed_row), dict(projections)))
        evaluated_deliveries.append(
            (
                delivery_name,
                [dict(row) for row in delivery_rows],
                retained,
                projected_rows,
            )
        )

    dispositions: list[RowDisposition] = []
    for target_row in target_rows:
        disposition = RowDisposition(row=target_row)
        for delivery_name, _, retained, _ in evaluated_deliveries:
            outcome = _classify_row(model, target_row, retained)
            disposition.outcomes[delivery_name] = outcome
        dispositions.append(disposition)

    results: list[DeleteBranchResult] = []
    for delete_branch in model.delete_branches:
        reachable = [
            disposition.row
            for disposition in dispositions
            if disposition.reaches(delete_branch.index)
        ]
        out_of_scope_rows = [
            row for row in reachable if row.get(policy_column) != policy_value
        ]
        if out_of_scope_rows:
            counterexample = _build_counterexample(
                model=model,
                target_row=_preferred_counterexample(out_of_scope_rows, policy_column),
                deliveries=evaluated_deliveries,
                delete_branch=delete_branch,
            )
            verdict = Verdict.OUT_OF_SCOPE
            counterexample_obj: Optional[Counterexample] = counterexample
        else:
            verdict = Verdict.IN_SCOPE
            counterexample_obj = None
        results.append(
            DeleteBranchResult(
                index=delete_branch.index,
                position=_position(delete_branch.segment),
                verdict=verdict,
                scope_policy=scope_policy,
                reachable_rows=reachable,
                dispositions=dispositions,
                counterexample=counterexample_obj,
                evidence=list(local_evidence),
            )
        )
    return results, True


def _classify_row(
    model: _MergeModel,
    target_row: dict[str, Any],
    retained: list[dict[str, Any]],
) -> _Outcome:
    for source_row in retained:
        projections = source_row["__projections__"]
        if all(
            _values_match(target_row.get(target_column), projections.get(source_alias))
            for target_column, source_alias, _ in model.on_pairs
        ):
            return _Outcome("match", None)
    for branch in model.source_branches:
        if branch.guard is True:
            guard_value: Any = True
        elif branch.guard is MISSING:
            guard_value = MISSING
        else:
            guard_value = _eval_node(branch.guard, target_row)
        if guard_value is True:
            return _Outcome(branch.action, branch.index)
    # No WHEN clause matched: PostgreSQL leaves the target row unchanged.
    return _Outcome("no_action", None)


def _values_match(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    return left == right


def _preferred_counterexample(
    rows: list[dict[str, Any]], policy_column: str
) -> dict[str, Any]:
    def rank(row: dict[str, Any]) -> tuple[int, ...]:
        value = row.get(policy_column)
        # Prefer non-null, in-domain violations first: they are the clearest.
        return (1 if value is None else 0,)

    return sorted(rows, key=rank)[0]


def _build_counterexample(
    *,
    model: _MergeModel,
    target_row: dict[str, Any],
    deliveries: list[
        tuple[
            str,
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[tuple[dict[str, Any], dict[str, Any]]],
        ]
    ],
    delete_branch: _SourceBranch,
) -> Counterexample:
    chosen_delivery = None
    for delivery_name, delivery_rows, retained, projected_rows in deliveries:
        outcome = _classify_row(model, target_row, retained)
        if outcome.kind == "delete" and outcome.branch_index == delete_branch.index:
            chosen_delivery = (
                delivery_name,
                delivery_rows,
                retained,
                projected_rows,
            )
            break
    assert chosen_delivery is not None
    delivery_name, delivery_rows, retained, projected_rows = chosen_delivery

    on_evaluation: list[str] = []
    if not retained:
        on_evaluation.append(
            "USING retained 0 feed rows for this delivery, so no source row can "
            "match; NOT MATCHED BY SOURCE holds for every target row"
        )
    for feed_row, projections in projected_rows:
        for target_column, source_alias, operator in model.on_pairs:
            target_value = target_row.get(target_column)
            source_value = projections.get(source_alias)
            on_evaluation.append(
                f"feed {_render_row(feed_row)} projected "
                f"{source_alias}={source_value!r}: target "
                f"{target_column}={target_value!r} {operator.raw} "
                f"{source_value!r} -> {_values_match(target_value, source_value)}"
            )

    branch_path: list[str] = []
    for branch in model.source_branches:
        if branch is delete_branch:
            break
        if branch.guard is True:
            guard_value: Any = True
        else:
            guard_value = _eval_node(branch.guard, target_row)
        guard_text = (
            _snippet(branch.guard_segment) if branch.guard_segment is not None else ""
        )
        branch_path.append(
            f"branch #{branch.index} at {_position(branch.segment)} guard "
            f"{guard_text!r} -> {guard_value}; action {branch.action}"
        )
    guard_text = (
        _snippet(delete_branch.guard_segment)
        if delete_branch.guard_segment is not None
        else "(no guard)"
    )
    branch_path.append(
        f"branch #{delete_branch.index} at {_position(delete_branch.segment)} "
        f"guard {guard_text!r} -> True; action delete"
    )

    return Counterexample(
        target_row=dict(target_row),
        delivery_name=delivery_name,
        delivery=[dict(feed_row) for feed_row in delivery_rows],
        retained_rows=[
            {"feed_row": dict(feed_row), "projections": dict(projections)}
            for feed_row, projections in projected_rows
        ],
        on_evaluation=on_evaluation,
        branch_path=branch_path,
        delete_branch_position=_position(delete_branch.segment),
    )


def _render_row(row: dict[str, Any]) -> str:
    parts = [
        f"{key}={value!r}" for key, value in row.items() if key != "__projections__"
    ]
    return "(" + ", ".join(parts) + ")"
