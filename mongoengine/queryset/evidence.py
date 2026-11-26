"""Support for returning the matching evidence of a query.

This module is deliberately independent from the queryset execution code.  A
rule is evaluated against the raw SON that was returned by the same cursor
which selected the document.  In particular, evidence is never reconstructed
with a second query (for example by querying the document again by its primary
key): a document may be changed while a cursor is being consumed and the
selection and its explanation must describe the same original document.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping

try:  # pragma: no cover - bson is a mandatory dependency of mongoengine
    from bson import Regex as BSONRegex
except Exception:  # pragma: no cover
    BSONRegex = ()

from mongoengine.queryset import transform
from mongoengine.queryset.visitor import Q, QCombination, QNode

__all__ = (
    "EvidenceCondition",
    "EvidenceRule",
    "EvidenceRuleGroup",
    "EvidenceResult",
    "Rule",
    "RuleGroup",
    "RuleCondition",
    "Evidence",
    "RuleEvidence",
    "RuleEvidenceCollection",
    "ConditionEvidence",
    "EvidenceHit",
    "compile_evidence_program",
)


# ---------------------------------------------------------------------------
# Rule definitions (the public, small DSL)
# ---------------------------------------------------------------------------


class EvidenceCondition:
    """A single condition used by an :class:`EvidenceRule`.

    A condition wraps a :class:`~mongoengine.queryset.Q`.  ``scope`` may be
    ``"cross_row"`` (the default) or ``"same_row"``.  ``same_row`` means that
    all field conditions inside the wrapped Q must be true on the same array
    element (and, when the Q contains nested ``$elemMatch`` expressions, on the
    same group/reading pair).
    """

    def __init__(
        self,
        query=None,
        scope="cross_row",
        same_row=False,
        cross_row=False,
        **conditions,
    ):
        if query is None:
            query = Q(**conditions)
        elif conditions:
            query = query & Q(**conditions)
        if same_row:
            scope = "same_row"
        elif cross_row:
            scope = "cross_row"
        if scope not in ("cross_row", "same_row"):
            raise ValueError("scope must be 'cross_row' or 'same_row'")
        self.mapping = dict(query) if isinstance(query, Mapping) else None
        if isinstance(query, Mapping):
            query = Q(__raw__=dict(query))
        if isinstance(query, (list, tuple)):
            combined = None
            for item in query:
                condition = (
                    item
                    if isinstance(item, EvidenceCondition)
                    else EvidenceCondition(item, scope=scope)
                )
                combined = condition if combined is None else (combined & condition)
            query = combined
        if not isinstance(query, (QNode, EvidenceCondition, _ConditionCombination)):
            raise TypeError("An EvidenceCondition expects a Q object or a mapping")
        self.query = query
        self.scope = scope

    @classmethod
    def cross_row(cls, query):
        return cls(query, scope="cross_row")

    @classmethod
    def same_row(cls, query):
        return cls(query, scope="same_row")

    def _coerce(self, other):
        if isinstance(other, (EvidenceCondition, QNode, _ConditionCombination)):
            return other
        return EvidenceCondition(other, scope=self.scope)

    def __and__(self, other):
        return _ConditionCombination("and", [self, self._coerce(other)])

    def __or__(self, other):
        return _ConditionCombination("or", [self, self._coerce(other)])

    def __repr__(self):
        return "EvidenceCondition(%r, scope=%r)" % (self.query, self.scope)


class _ConditionCombination:
    def __init__(self, operation, children):
        self.operation = operation
        self.children = children

    def _coerce(self, other):
        if isinstance(other, (EvidenceCondition, QNode, _ConditionCombination)):
            return other
        return EvidenceCondition(other)

    def __and__(self, other):
        return _ConditionCombination("and", [self, self._coerce(other)])

    def __or__(self, other):
        return _ConditionCombination("or", [self, self._coerce(other)])


class EvidenceRule:
    """A named review rule.

    A rule carries either a :class:`Q` tree or one or more
    :class:`EvidenceCondition` objects.  Rules combine with ``&`` and ``|``
    to form the same boolean expression used for selection.  The rule name is
    what the downstream report uses as the selection reason, so it should be
    stable and human-readable.
    """

    def __init__(
        self, name=None, query=None, scope=None, same_row=False, **conditions
    ):
        if name is None:
            name = conditions.pop("rule_name", None) or conditions.pop("name", None)
        self.mapping = None
        if same_row:
            scope = "same_row"
        if scope is not None and scope not in ("cross_row", "same_row"):
            raise ValueError("scope must be 'cross_row' or 'same_row'")
        if query is None:
            query = Q(**conditions)
        elif conditions:
            query = query & Q(**conditions)
        elif isinstance(query, Mapping):
            self.mapping = dict(query)
            if scope is not None:
                query = EvidenceCondition(query, scope=scope)
            else:
                query = Q(__raw__=dict(query))
        if isinstance(query, (list, tuple)):
            combined = None
            for item in query:
                condition = (
                    item
                    if isinstance(item, EvidenceCondition)
                    else EvidenceCondition(item)
                )
                combined = condition if combined is None else (combined & condition)
            query = combined
        if scope is not None and isinstance(query, QNode):
            query = EvidenceCondition(query, scope=scope)
        if not isinstance(query, (QNode, EvidenceCondition, _ConditionCombination)):
            raise TypeError(
                "EvidenceRule expects a Q object (or an EvidenceCondition as rule query)"
            )
        self.name = name
        self.query = query
        self.scope = scope

    @classmethod
    def same_row(cls, name, query):
        return cls(name, EvidenceCondition.same_row(query))

    @classmethod
    def cross_row(cls, name, query):
        return cls(name, EvidenceCondition.cross_row(query))

    @property
    def rule_name(self):
        return self.name

    @property
    def rule(self):
        return self.name

    def _coerce(self, other):
        if isinstance(other, (EvidenceRule, EvidenceRuleGroup)):
            return other
        return EvidenceRule("rule", other)

    def __and__(self, other):
        return EvidenceRuleGroup("and", [self, self._coerce(other)])

    def __or__(self, other):
        return EvidenceRuleGroup("or", [self, self._coerce(other)])

    def to_query(self):
        """Return the rule's underlying Q object (when it has one)."""
        if isinstance(self.query, EvidenceCondition):
            if self.query.mapping is not None:
                return Q(__raw__=dict(self.query.mapping))
            return self.query.query
        if isinstance(self.query, _ConditionCombination):
            return self.query
        return self.query

    def __repr__(self):
        return "EvidenceRule(%r, %r)" % (self.name, self.query)


class EvidenceRuleGroup:
    """An AND/OR combination of rules."""

    AND = "and"
    OR = "or"

    def __init__(self, operation, children):
        if operation not in (self.AND, self.OR):
            raise ValueError("operation must be 'and' or 'or'")
        self.operation = operation
        self.children = list(children)

    def _coerce(self, other):
        if isinstance(other, (EvidenceRule, EvidenceRuleGroup)):
            return other
        return EvidenceRule("rule", other)

    def __and__(self, other):
        return EvidenceRuleGroup("and", [self, self._coerce(other)])

    def __or__(self, other):
        return EvidenceRuleGroup("or", [self, self._coerce(other)])

    def __repr__(self):
        return "EvidenceRuleGroup(%r, %r)" % (self.operation, self.children)


# Short names for callers who do not need the Evidence prefix.
Rule = EvidenceRule
RuleGroup = EvidenceRuleGroup
RuleCondition = EvidenceCondition


# ---------------------------------------------------------------------------
# Results (the public output objects)
# ---------------------------------------------------------------------------


class EvidenceHit:
    """One position at which a condition was satisfied.

    ``kind`` describes the value that produced a match for conditions that
    look for ``null``.  It is one of ``"value"``, ``"null"`` or ``"missing"``.
    The actual value is intentionally not exposed; the hit only carries the
    position and, for null-like matches, whether the key was missing or
    explicitly null.
    """

    __slots__ = ("position", "kind")

    def __init__(self, position, kind="value"):
        self.position = tuple(position)
        self.kind = kind

    @property
    def group(self):
        return self.position[0] if len(self.position) > 0 else None

    @property
    def reading(self):
        return self.position[1] if len(self.position) > 1 else None

    @property
    def is_null(self):
        return self.kind == "null"

    @property
    def is_missing(self):
        return self.kind == "missing"

    @property
    def null_or_missing(self):
        return self.kind if self.kind in ("null", "missing") else None

    @property
    def value_category(self):
        return self.kind

    def to_dict(self):
        result = {
            "position": list(self.position),
            "kind": self.kind,
            "null_or_missing": self.null_or_missing,
        }
        if len(self.position) > 0:
            result["group"] = self.position[0]
        if len(self.position) > 1:
            result["reading"] = self.position[1]
        return result

    def __getitem__(self, key):
        if key in ("position", "pos"):
            return self.position
        if key in ("kind", "null_or_missing"):
            return self.kind if key == "kind" else self.null_or_missing
        if key in ("is_null", "null"):
            return self.is_null
        if key in ("is_missing", "missing"):
            return self.is_missing
        if key == "group":
            return self.group
        if key == "reading":
            return self.reading
        raise KeyError(key)

    def __iter__(self):
        return iter(self.position)

    def __eq__(self, other):
        if isinstance(other, EvidenceHit):
            return (self.position, self.kind) == (other.position, other.kind)
        if isinstance(other, tuple):
            return self.position == other
        return NotImplemented

    def __hash__(self):
        return hash((self.position, self.kind))

    def keys(self):
        return ("position", "group", "reading", "kind", "null_or_missing")

    def values(self):
        return (
            self.position,
            self.group,
            self.reading,
            self.kind,
            self.null_or_missing,
        )

    def items(self):
        return tuple(zip(self.keys(), self.values()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __repr__(self):
        return "EvidenceHit(%r, kind=%r)" % (self.position, self.kind)


class ConditionEvidence:
    """Evidence for one condition of a rule.

    ``scope`` is ``"cross_row"`` for report-wide conditions and
    ``"same_row"`` for conditions which had to match one group/reading.
    ``hits`` holds zero-based positions; ``field`` is the primary matched
    database path and ``fields`` lists the distinct leaf paths.
    """

    __slots__ = ("scope", "field", "hits", "matched", "_db_fields")

    def __init__(self, scope, field, hits, db_fields=()):
        self.scope = scope
        self.field = field
        self.hits = tuple(hits)
        self.matched = bool(self.hits)
        self._db_fields = tuple(db_fields)

    @property
    def is_same_row(self):
        return self.scope == "same_row"

    @property
    def is_cross_row(self):
        return self.scope == "cross_row"

    @property
    def matches(self):
        return self.matched

    @property
    def positions(self):
        return tuple(hit.position for hit in self.hits)

    @property
    def kinds(self):
        return tuple(hit.kind for hit in self.hits)

    @property
    def hit_positions(self):
        return self.positions

    @property
    def null_or_missing(self):
        return tuple(
            hit.to_dict() for hit in self.hits if hit.kind in ("null", "missing")
        )

    @property
    def null_or_missing_kinds(self):
        return tuple(
            hit.kind for hit in self.hits if hit.kind in ("null", "missing")
        )

    @property
    def value_positions(self):
        return tuple(hit.position for hit in self.hits if hit.kind == "value")

    @property
    def db_fields(self):
        return getattr(self, "_db_fields", ())

    @property
    def fields(self):
        return tuple(
            field
            for field in self.db_fields
            if not any(
                other != field and other.startswith(field + ".")
                for other in self.db_fields
            )
        )

    def to_dict(self):
        return {
            "scope": self.scope,
            "field": self.field,
            "db_fields": list(self.db_fields),
            "fields": list(self.fields),
            "matched": self.matched,
            "positions": [list(position) for position in self.positions],
            "hits": [hit.to_dict() for hit in self.hits],
        }

    def __getitem__(self, key):
        if key in ("scope", "kind"):
            return self.scope
        if key in ("field", "name"):
            return self.field
        if key in ("db_fields", "db_field"):
            return self.db_fields
        if key == "fields":
            return self.fields
        if key in ("hits", "positions", "hit_positions"):
            return self.hits if key == "hits" else self.positions
        if key in ("null_or_missing",):
            return self.null_or_missing
        if key in ("null_or_missing_kinds", "kinds"):
            return self.null_or_missing_kinds if key != "kinds" else self.kinds
        if key == "value_positions":
            return self.value_positions
        if key == "matched":
            return self.matched
        raise KeyError(key)

    def keys(self):
        return ("scope", "field", "matched", "positions", "hits")

    def values(self):
        return (
            self.scope,
            self.field,
            self.matched,
            self.positions,
            self.hits,
        )

    def items(self):
        return tuple(zip(self.keys(), self.values()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other):
        if not isinstance(other, ConditionEvidence):
            return NotImplemented
        return (
            self.scope == other.scope
            and self.field == other.field
            and self.hits == other.hits
        )

    def __hash__(self):
        return hash((self.scope, self.field, self.hits))

    def __repr__(self):
        return "ConditionEvidence(scope=%r, field=%r, hits=%r)" % (
            self.scope,
            self.field,
            self.hits,
        )


class RuleEvidence:
    """Evidence for one named rule."""

    __slots__ = ("name", "matched", "conditions")

    def __init__(self, name, matched, conditions):
        self.name = name
        self.matched = bool(matched)
        self.conditions = tuple(conditions)

    @property
    def rule_name(self):
        return self.name

    @property
    def rule(self):
        return self.name

    @property
    def is_match(self):
        return self.matched

    @property
    def truth(self):
        return self.matched

    @property
    def is_true(self):
        return self.matched

    @property
    def true(self):
        return self.matched

    @property
    def false(self):
        return not self.matched

    @property
    def matches(self):
        return self.matched

    @property
    def result(self):
        return self.matched

    @property
    def hits(self):
        hits = []
        for condition in self.conditions:
            hits.extend(condition.hits)
        return tuple(hits)

    @property
    def positions(self):
        positions = []
        seen = set()
        for condition in self.conditions:
            for hit in condition.hits:
                if hit.position not in seen:
                    seen.add(hit.position)
                    positions.append(hit.position)
        return tuple(positions)

    @property
    def hit_positions(self):
        return self.positions

    @property
    def cross_row(self):
        return tuple(c for c in self.conditions if c.scope == "cross_row")

    @property
    def same_row(self):
        return tuple(c for c in self.conditions if c.scope == "same_row")

    @property
    def cross_row_positions(self):
        result = []
        for condition in self.cross_row:
            result.extend(condition.positions)
        return tuple(result)

    @property
    def same_row_positions(self):
        result = []
        for condition in self.same_row:
            result.extend(condition.positions)
        return tuple(result)

    @property
    def positions_by_field(self):
        return {
            condition.field: condition.positions for condition in self.conditions
        }

    @property
    def hits_by_field(self):
        return {condition.field: condition.hits for condition in self.conditions}

    def to_dict(self):
        return {
            "name": self.name,
            "matched": self.matched,
            "positions": [list(position) for position in self.positions],
            "cross_row_positions": [
                list(position) for position in self.cross_row_positions
            ],
            "same_row_positions": [
                list(position) for position in self.same_row_positions
            ],
            "conditions": [condition.to_dict() for condition in self.conditions],
        }

    def __getitem__(self, key):
        if key in ("name", "rule_name"):
            return self.name
        if key in ("matched", "truth", "is_match"):
            return self.matched
        if key in ("conditions", "evidence"):
            return self.conditions
        if key in ("hits", "positions", "hit_positions"):
            return self.hits if key == "hits" else self.positions
        raise KeyError(key)

    def keys(self):
        return ("name", "matched", "conditions")

    def values(self):
        return (self.name, self.matched, self.conditions)

    def items(self):
        return tuple(zip(self.keys(), self.values()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other):
        if not isinstance(other, RuleEvidence):
            return NotImplemented
        return (
            self.name == other.name
            and self.matched == other.matched
            and self.conditions == other.conditions
        )

    def __hash__(self):
        return hash((self.name, self.matched, self.conditions))

    def __repr__(self):
        return "RuleEvidence(%r, matched=%r, conditions=%r)" % (
            self.name,
            self.matched,
            self.conditions,
        )


class RuleEvidenceCollection(tuple):
    """A tuple of rule evidence which also supports lookup by rule name."""

    def __new__(cls, values=()):
        return super().__new__(cls, tuple(values))

    def __getitem__(self, key):
        if isinstance(key, str):
            for item in self:
                if getattr(item, "name", None) == key:
                    return item
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError):
            return default

    @property
    def by_name(self):
        return {item.name: item for item in self}

    def names(self):
        return tuple(item.name for item in self)


class EvidenceResult:
    """A selected document plus the evidence for all review rules.

    ``document`` is materialized from the same raw SON that produced the
    selection and the evidence.  It is lazy so that callers that only transmit
    the report number and the reasons do not pay the deserialization cost.
    """

    __slots__ = ("_document_cls", "_raw_doc", "_document", "rules", "matched")

    def __init__(self, document_cls, raw_doc, rules, matched):
        self._document_cls = document_cls
        self._raw_doc = raw_doc
        self._document = None
        self.rules = RuleEvidenceCollection(rules)
        self.matched = bool(matched)

    @property
    def report_number(self):
        id_field = self._document_cls._meta["id_field"]
        db_field = self._document_cls._fields[id_field].db_field
        return self._raw_doc.get(db_field, self._raw_doc.get("_id"))

    @property
    def report_no(self):
        return self.report_number

    @property
    def pk(self):
        return self.report_number

    @property
    def number(self):
        return self.report_number

    @property
    def report_id(self):
        return self.report_number

    @property
    def selected(self):
        return self.matched

    @property
    def is_selected(self):
        return self.matched

    def as_tuple(self):
        return self.document, self.rules

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.document, name)

    @property
    def evidence(self):
        return self.rules

    @property
    def reasons(self):
        return self.rules

    @property
    def reason(self):
        return self.rules

    @property
    def rule_evidence(self):
        return self.rules

    @property
    def reason_text(self):
        return ", ".join(
            rule.name for rule in self.rules if rule.matched
        )

    @property
    def selection_reason(self):
        return self.rules

    def rule(self, name):
        for evidence in self.rules:
            if evidence.name == name:
                return evidence
        return None

    @property
    def true_rules(self):
        return tuple(rule for rule in self.rules if rule.matched)

    @property
    def false_rules(self):
        return tuple(rule for rule in self.rules if not rule.matched)

    @property
    def rule_results(self):
        return {rule.name: rule.matched for rule in self.rules}

    @property
    def rule_positions(self):
        return {rule.name: rule.positions for rule in self.rules}

    @property
    def evidence_positions(self):
        return {
            rule.name: {condition.field: condition.positions for condition in rule.conditions}
            for rule in self.rules
        }

    def to_mongo(self):
        return self._raw_doc

    @property
    def raw(self):
        """The raw SON used for both selection and evidence (in-process only)."""
        return self._raw_doc

    def get_rule(self, name, default=None):
        return self.rule(name) or default

    @property
    def document(self):
        if self._document is None:
            self._document = self._document_cls._from_son(self._raw_doc)
        return self._document

    def to_dict(self):
        return {
            "report_number": self.report_number,
            "matched": self.matched,
            "rules": [rule.to_dict() for rule in self.rules],
        }

    def transmit(self):
        """Return the minimal payload safe to send with the report number.

        Only rule names, truth values, positions and null/missing categories
        are included.  Raw reading values, tags and supplier identifiers stay
        in the database.
        """
        return {
            "report_number": self.report_number,
            "rules": [
                {
                    "name": rule.name,
                    "matched": rule.matched,
                    "conditions": [
                        {
                            "scope": condition.scope,
                            "positions": [
                                list(position)
                                for position in condition.positions
                            ],
                            "hits": [
                                {
                                    "position": list(hit.position),
                                    "kind": hit.kind,
                                }
                                for hit in condition.hits
                                if hit.kind in ("null", "missing")
                            ],
                        }
                        for condition in rule.conditions
                    ],
                }
                for rule in self.rules
            ],
        }

    def to_payload(self):
        return self.transmit()

    def __getitem__(self, key):
        if key in ("report_number", "number", "pk", "id"):
            return self.report_number
        if key in ("matched", "is_match", "truth"):
            return self.matched
        if key in ("rules", "evidence", "reasons"):
            return self.rules
        if key == "document":
            return self.document
        if isinstance(key, int):
            return self.rules[key]
        if isinstance(key, str):
            return self.rules[key]
        raise KeyError(key)

    def __iter__(self):
        return iter(self.rules)

    def __len__(self):
        return len(self.rules)

    def keys(self):
        return ("report_number", "matched", "rules")

    def values(self):
        return (self.report_number, self.matched, self.rules)

    def items(self):
        return tuple(zip(self.keys(), self.values()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other):
        if not isinstance(other, EvidenceResult):
            return NotImplemented
        return (
            self.report_number == other.report_number
            and self.matched == other.matched
            and self.rules == other.rules
        )

    def __repr__(self):
        return "EvidenceResult(%r, matched=%r, rules=%r)" % (
            self.report_number,
            self.matched,
            self.rules,
        )


# ---------------------------------------------------------------------------
# Query evaluation against a raw SON
# ---------------------------------------------------------------------------


# Short alias for a returned evidence record.
Evidence = EvidenceResult


class _Missing:
    __slots__ = ()

    def __repr__(self):  # pragma: no cover - debugging aid
        return "<missing>"


MISSING = _Missing()


def _collation_options(collation):
    """Return (case_insensitive, document) for a PyMongo/MongoEngine collation."""
    if not collation:
        return False, None
    document = getattr(collation, "document", collation)
    if not isinstance(document, Mapping):
        return False, None
    strength = document.get("strength", 3)
    case_level = bool(document.get("caseLevel", False))
    case_insensitive = strength in (1, 2) and not case_level
    return case_insensitive, dict(document)


def _equal(left, right, collation):
    if left is MISSING:
        return right is None
    if right is MISSING:
        return left is None
    if left is None or right is None:
        return left is None and right is None
    case_insensitive, _ = _collation_options(collation)
    if case_insensitive and isinstance(left, str) and isinstance(right, str):
        return left.casefold() == right.casefold()
    return left == right


def _compare(left, right, collation):
    """Compare two values, honoring a case-insensitive English collation."""
    if left is MISSING or right is MISSING:
        return None
    if left is None or right is None:
        return None
    case_insensitive, _ = _collation_options(collation)
    if case_insensitive and isinstance(left, str) and isinstance(right, str):
        left = left.casefold()
        right = right.casefold()
    try:
        if left < right:
            return -1
        if left > right:
            return 1
        return 0
    except TypeError:
        return None


def _regex_match(value, pattern, options, collation):
    if not isinstance(value, str):
        return False
    if BSONRegex and isinstance(pattern, BSONRegex):
        regex = pattern.try_compile()
        return regex.search(value) is not None
    flags = 0
    if isinstance(options, str):
        for option in options:
            if option == "i":
                flags |= re.IGNORECASE
            elif option == "m":
                flags |= re.MULTILINE
            elif option == "s":
                flags |= re.DOTALL
            elif option == "x":
                flags |= re.VERBOSE
    case_insensitive, _ = _collation_options(collation)
    if case_insensitive:
        flags |= re.IGNORECASE
    if hasattr(pattern, "pattern"):
        flags |= getattr(pattern, "flags", 0)
        pattern = pattern.pattern
    try:
        return re.search(pattern, value, flags) is not None
    except re.error:
        return False


def _values_equal_for_query(left, right, collation):
    if isinstance(left, list) and not isinstance(right, list):
        return any(_values_equal_for_query(item, right, collation) for item in left)
    return _equal(left, right, collation)


def _match_value(value, condition, collation):
    if isinstance(condition, Mapping) and any(
        isinstance(key, str) and key.startswith("$") for key in condition
    ):
        return _match_operator(value, condition, collation)

    if isinstance(condition, Mapping) and condition:
        # An embedded document condition such as {shape: "square"}.
        if isinstance(value, list):
            return any(
                _match_value(item, condition, collation) for item in value
            )
        if isinstance(value, Mapping):
            return _match_query(value, condition, collation)
        return False

    return _values_equal_for_query(value, condition, collation)


def _match_operator(value, condition, collation):  # noqa: C901
    for operator, operand in condition.items():
        if operator == "$eq":
            if not _values_equal_for_query(value, operand, collation):
                return False
        elif operator == "$ne":
            if _values_equal_for_query(value, operand, collation):
                return False
        elif operator in ("$gt", "$gte", "$lt", "$lte"):
            values = value if isinstance(value, list) else [value]
            matched = False
            for item in values:
                comparison = _compare(item, operand, collation)
                if comparison is None:
                    continue
                if operator == "$gt" and comparison > 0:
                    matched = True
                elif operator == "$gte" and comparison >= 0:
                    matched = True
                elif operator == "$lt" and comparison < 0:
                    matched = True
                elif operator == "$lte" and comparison <= 0:
                    matched = True
                if matched:
                    break
            if not matched:
                return False
        elif operator == "$in":
            values = value if isinstance(value, list) else [value]
            if not any(
                _match_value(item, candidate, collation)
                for item in values
                for candidate in operand
            ):
                return False
        elif operator == "$nin":
            values = value if isinstance(value, list) else [value]
            if any(
                _match_value(item, candidate, collation)
                for item in values
                for candidate in operand
            ):
                return False
        elif operator == "$exists":
            exists = value is not MISSING
            if bool(operand) != exists:
                return False
        elif operator == "$regex":
            options = condition.get("$options", "")
            if isinstance(value, list):
                if not any(_regex_match(item, operand, options, collation) for item in value):
                    return False
            elif not _regex_match(value, operand, options, collation):
                return False
        elif operator == "$options":
            continue
        elif operator == "$not":
            if _match_operator(value, operand, collation):
                return False
        elif operator == "$elemMatch":
            if not isinstance(value, list):
                return False
            if not any(_match_value(item, operand, collation) for item in value):
                return False
        elif operator == "$all":
            if not isinstance(value, list):
                return False
            for candidate in operand:
                if not any(
                    _match_value(item, candidate, collation) for item in value
                ):
                    return False
        elif operator == "$size":
            if not isinstance(value, list) or len(value) != operand:
                return False
        elif operator == "$type":
            # Rarely useful for evidence queries; accept the common aliases.
            names = {
                "string": str,
                "int": int,
                "long": int,
                "double": float,
                "bool": bool,
                "null": type(None),
                "array": list,
                "object": dict,
            }
            expected = names.get(operand, operand)
            if not isinstance(value, expected):
                return False
        elif operator == "$mod":
            if not isinstance(value, (int, float)):
                return False
            divisor, remainder = operand
            if value % divisor != remainder:
                return False
        elif operator == "$and":
            if not all(_match_value(value, sub, collation) for sub in operand):
                return False
        elif operator == "$or":
            if not any(_match_value(value, sub, collation) for sub in operand):
                return False
        elif operator == "$nor":
            if any(_match_value(value, sub, collation) for sub in operand):
                return False
        else:
            # Unknown operator: be conservative.  The selection itself is
            # performed by MongoDB, so this only affects the explanation.
            return False
    return True


def _resolve(node, parts):
    """Resolve a dotted path against a raw SON, returning all values found.

    MongoDB implicitly traverses arrays.  The pruned document used by the
    evidence evaluator normally contains one element per array, but the
    resolver still handles arrays so that group-level conditions work.
    """
    if not parts:
        return [node]
    head = parts[0]
    if isinstance(node, list):
        values = []
        for item in node:
            values.extend(_resolve(item, parts))
        return values or [MISSING]
    if isinstance(node, Mapping):
        if head not in node:
            return [MISSING]
        return _resolve(node[head], parts[1:])
    return [MISSING]


def _match_query(document, query, collation):
    if not isinstance(query, Mapping):
        return False
    for key, condition in query.items():
        if key == "$and":
            if not all(_match_query(document, sub, collation) for sub in condition):
                return False
        elif key == "$or":
            if not any(_match_query(document, sub, collation) for sub in condition):
                return False
        elif key == "$nor":
            if any(_match_query(document, sub, collation) for sub in condition):
                return False
        elif key == "$where":
            return False
        elif key == "$comment":
            continue
        elif isinstance(key, str) and key.startswith("$"):
            return False
        else:
            values = _resolve(document, key.split("."))
            if not any(_match_value(value, condition, collation) for value in values):
                return False
    return True


# ---------------------------------------------------------------------------
# Structural query inspection
# ---------------------------------------------------------------------------


def _collect_query_paths(query, prefix=""):
    """Collect field paths referenced by a MongoDB query document."""
    paths = set()
    if not isinstance(query, Mapping):
        return paths
    for key, value in query.items():
        if key in ("$and", "$or", "$nor", "$where", "$comment"):
            if isinstance(value, list):
                for item in value:
                    paths.update(_collect_query_paths(item, prefix))
            continue
        if key == "$not":
            paths.update(_collect_query_paths(value, prefix))
            continue
        if key == "$elemMatch":
            paths.update(_collect_query_paths(value, prefix))
            continue
        if isinstance(key, str) and key.startswith("$"):
            continue
        path = "%s.%s" % (prefix, key) if prefix else key
        paths.add(path)
        if isinstance(value, Mapping):
            paths.update(_collect_query_paths(value, path))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    paths.update(_collect_query_paths(item, path))
    return paths


def _array_paths_for_document(document, query_paths):
    """Return the list-valued prefixes traversed by ``query_paths``."""

    array_paths = set()

    def visit(node, parts, built):
        if not parts:
            return
        if isinstance(node, list):
            array_paths.add(built)
            for item in node:
                visit(item, parts, built)
            return
        if not isinstance(node, Mapping):
            return
        head = parts[0]
        if head not in node:
            return
        current = "%s.%s" % (built, head) if built else head
        value = node[head]
        if len(parts) == 1:
            if isinstance(value, list):
                array_paths.add(current)
            return
        visit(value, parts[1:], current)

    for path in query_paths:
        segments = [segment for segment in path.split(".") if segment and not segment.isdigit()]
        visit(document, segments, "")
    return sorted(array_paths, key=lambda path: (path.count("."), path))


def _parent_path(path):
    if "." not in path:
        return ""
    return path.rsplit(".", 1)[0]


def _iter_coordinates(document, array_paths):
    """Yield coordinate tuples for the arrays in ``array_paths``.

    The tuples contain one zero-based index per array path, in order.  For
    ``["gs", "gs.rs"]`` this yields ``(group_index, reading_index)`` pairs.
    """

    if not array_paths:
        yield ()
        return

    roots = [path for path in array_paths if not _parent_path(path)]

    def walk(node, path, prefix):
        children = [p for p in array_paths if _parent_path(p) == path]
        if not children:
            yield tuple(prefix)
            return
        for child in children:
            if path:
                relative = child[len(path) + 1:]
            else:
                relative = child
            value = _resolve(node, relative.split("."))
            # ``_resolve`` may return several values when an intermediate
            # array exists.  Normally the pruned document means one value.
            for item in value:
                if isinstance(item, list):
                    for index, element in enumerate(item):
                        yield from walk(element, child, prefix + [index])
                elif item is MISSING:
                    yield from walk(node, child, prefix)
                else:
                    yield from walk(item, child, prefix)

    for root in roots:
        value = _resolve(document, root.split("."))
        for item in value:
            if isinstance(item, list):
                for index, element in enumerate(item):
                    yield from walk(element, root, [index])
            elif item is not MISSING:
                yield from walk(item, root, [])


def _prune_document(document, array_paths, coordinate):
    indices = dict(zip(array_paths, coordinate))

    def clone(node, path):
        if not isinstance(node, Mapping):
            return copy.deepcopy(node)
        result = {}
        for key, value in node.items():
            child_path = "%s.%s" % (path, key) if path else key
            if child_path in indices and isinstance(value, list):
                index = indices[child_path]
                if 0 <= index < len(value):
                    result[key] = [clone(value[index], child_path)]
                else:
                    result[key] = []
            elif isinstance(value, Mapping):
                result[key] = clone(value, child_path)
            elif isinstance(value, list):
                # Preserve lists which do not have a selected index.
                result[key] = [clone(item, child_path) for item in value]
            else:
                result[key] = copy.deepcopy(value)
        return result

    return clone(document, "")


def _mask_query(query):
    """Split a query into parts which are evaluated as one condition.

    MongoEngine's Q tree is already a good unit for a condition; however a
    single Q may contain several independent keyword conditions.  Splitting
    them is useful because each one gets its own hit list.
    """

    if not isinstance(query, Mapping) or not query:
        return [dict(query or {})]
    if any(
        key in ("__raw__",)
        or "__match" in key
        or key.startswith("$")
        for key in query
    ):
        return [dict(query)]
    return [{key: value} for key, value in query.items()]


def _deepest_field_path(query):
    paths = sorted(
        _collect_query_paths(query),
        key=lambda path: (path.count("."), path),
    )
    return paths[-1] if paths else None


class _CompiledCondition:
    __slots__ = (
        "query",
        "scope",
        "field",
        "array_paths",
        "null_path",
        "auto",
        "force_same_row",
    )

    def __init__(
        self,
        query,
        scope,
        field,
        array_paths,
        null_path,
        auto=False,
        force_same_row=False,
    ):
        self.query = query
        self.scope = scope
        self.field = field
        self.array_paths = tuple(array_paths)
        self.null_path = null_path
        self.auto = auto
        self.force_same_row = force_same_row

    def _field_name(self, query, prefix=""):
        for key, value in query.items():
            if isinstance(key, str) and key.startswith("$"):
                continue
            path = "%s.%s" % (prefix, key) if prefix else key
            if isinstance(value, Mapping):
                nested = self._field_name(value, path)
                if nested:
                    return nested
            return path
        return prefix or ""

    def _null_path(self, query, prefix=""):
        for key, value in query.items():
            if isinstance(key, str) and key.startswith("$"):
                if key == "$and":
                    for item in value:
                        found = self._null_path(item, prefix)
                        if found:
                            return found
                continue
            path = "%s.%s" % (prefix, key) if prefix else key
            if isinstance(value, Mapping):
                if value.get("$eq", object()) is None:
                    return path
                if "$in" in value and any(item is None for item in value["$in"]):
                    return path
                if "$elemMatch" in value:
                    found = self._null_path(value["$elemMatch"], path)
                    if found:
                        return found
                else:
                    found = self._null_path(value, path)
                    if found:
                        return found
            elif value is None:
                return path
        return None

    @classmethod
    def compile(
        cls, document, mongo_query, scope, auto=False, force_same_row=False
    ):
        condition = cls(
            mongo_query,
            scope,
            "",
            (),
            None,
            auto=auto,
            force_same_row=force_same_row,
        )
        condition.null_path = condition._null_path(mongo_query)
        condition.field = condition.null_path or _deepest_field_path(mongo_query)
        return condition

    def db_fields(self):
        """Return the database field paths referenced by this condition."""
        return tuple(sorted(_collect_query_paths(self.query)))

    def _query_for(self, document):
        if self.force_same_row:
            return _same_row_query(document, self.query)
        return self.query

    def paths_for(self, document):
        """Return the array prefixes this condition traverses in ``document``."""
        return tuple(
            _array_paths_for_document(
                document, _collect_query_paths(self._query_for(document))
            )
        )

    def hits(self, document, collation):
        query = self._query_for(document)
        array_paths = self.paths_for(document)
        if not array_paths:
            matched = _match_query(document, query, collation)
            return [EvidenceHit((), self._kind(document, ()))] if matched else []

        result = []
        for coordinate in _iter_coordinates(document, array_paths):
            pruned = _prune_document(document, array_paths, coordinate)
            if _match_query(pruned, query, collation):
                result.append(EvidenceHit(coordinate, self._kind(pruned, coordinate)))
        return result

    def _kind(self, pruned, coordinate):
        if not self.null_path:
            return "value"
        values = _resolve(pruned, self.null_path.split("."))
        if any(value is MISSING for value in values):
            return "missing"
        if any(value is None for value in values):
            return "null"
        return "value"


# ---------------------------------------------------------------------------
# Rule compilation/evaluation
# ---------------------------------------------------------------------------


class _Leaf:
    __slots__ = ("condition",)

    def __init__(self, condition):
        self.condition = condition


class _Bool:
    __slots__ = ("operation", "children")

    def __init__(self, operation, children):
        self.operation = operation
        self.children = children


def _as_q_node(node):
    if isinstance(node, EvidenceRule):
        return node.query if isinstance(node.query, QNode) else Q()
    return node


def _bool_from_q(document, node, default_scope="cross_row", force_scope=None):
    node = _as_q_node(node)
    if isinstance(node, QCombination):
        operation = "and" if node.operation == QNode.AND else "or"
        children = [_as_q_node(child) for child in node.children]
        if (
            operation == "and"
            and force_scope is None
            and children
            and all(isinstance(child, Q) for child in children)
        ):
            # A natural MongoEngine spelling for a same-row condition is a
            # conjunction of dot-notation siblings, e.g.
            # Q(groups__kind="cold-chain") & Q(groups__readings__tag="hold").
            # MongoDB would evaluate those independently.  When one field
            # path is a prefix of another (a group path and a reading path),
            # collapse the conjunction into a single same-row condition.
            mongo_queries = [
                transform.query(document, **child.query)
                for child in children
            ]
            paths = set()
            for mongo_query in mongo_queries:
                paths.update(_collect_query_paths(mongo_query))
            containers = sorted(
                {
                    path.rsplit(".", 1)[0]
                    for path in paths
                    if "." in path
                },
                key=len,
            )
            same_row = any(
                right.startswith(left + ".")
                for index, left in enumerate(containers)
                for right in containers[index + 1:]
            )
            if same_row:
                combined = _collapsed_query(document, node)
                return _Leaf(
                    _CompiledCondition.compile(
                        document,
                        combined,
                        "same_row",
                        auto=True,
                        force_same_row=True,
                    )
                )
        return _Bool(
            operation,
            [
                _bool_from_q(
                    document, child, default_scope, force_scope=force_scope
                )
                for child in node.children
            ],
        )
    if isinstance(node, Q):
        if not node.query:
            return _Bool("and", [])
        mongo_query = transform.query(document, **node.query)
        scope = force_scope or default_scope
        if _contains_elem_match(mongo_query):
            scope = "same_row"
        if scope == "same_row":
            # A same-row condition is evaluated as a whole against one
            # pruned group/reading.  Splitting it would allow the individual
            # fields to match different rows.
            return _Leaf(
                _CompiledCondition.compile(
                    document, mongo_query, scope, auto=force_scope is None
                )
            )
        parts = _mask_query(mongo_query)
        leaves = [
            _Leaf(
                _CompiledCondition.compile(
                    document, part, scope, auto=force_scope is None
                )
            )
            for part in parts
        ]
        if len(leaves) == 1:
            return leaves[0]
        return _Bool("and", leaves)
    raise TypeError("Unsupported rule query: %r" % (node,))


def _contains_elem_match(query):
    if not isinstance(query, Mapping):
        return False
    for key, value in query.items():
        if key == "$elemMatch":
            return True
        if isinstance(value, Mapping) and _contains_elem_match(value):
            return True
        if isinstance(value, list):
            if any(
                isinstance(item, Mapping) and _contains_elem_match(item)
                for item in value
            ):
                return True
    return False


def _merge_query_documents(documents):
    """Merge MongoDB query documents with AND semantics."""
    result = {}
    pending_and = []
    for document in documents:
        if not document:
            continue
        for key, value in document.items():
            if key in result:
                pending_and.append({key: value})
            else:
                result[key] = value
    if pending_and:
        existing = result.get("$and")
        if existing is None:
            result["$and"] = pending_and
        else:
            result["$and"] = list(existing) + pending_and
    return result


def _collapsed_query(document, node):
    """Collapse a Q expression to a single MongoDB query document."""
    node = _as_q_node(node)
    if isinstance(node, Q):
        return transform.query(document, **node.query)
    if isinstance(node, QCombination):
        children = [_collapsed_query(document, child) for child in node.children]
        if node.operation == QNode.AND:
            return _merge_query_documents(children)
        return {"$or": children}
    raise TypeError("Unsupported same-row query: %r" % (node,))


def _grouped(elem_key, inner):
    return {elem_key: {"$elemMatch": inner}}


def _same_row_query(document, mongo_query):
    """Force a same-row query into nested ``$elemMatch`` expressions.

    A same-row condition may be expressed either explicitly with
    ``__match``/``__raw__`` or as several dot-notation siblings.  The latter
    is the natural MongoEngine spelling but MongoDB would evaluate those
    siblings independently.  This converts both spellings to the equivalent
    nested ``$elemMatch`` form used for evaluation.
    """

    if _contains_elem_match(mongo_query):
        return mongo_query

    array_paths = _array_paths_for_document(
        document, _collect_query_paths(mongo_query)
    )
    if not array_paths:
        return mongo_query

    scalar_terms = {}
    nested_terms = {}
    for key, value in mongo_query.items():
        if isinstance(key, str) and key.startswith("$"):
            return mongo_query
        applicable = [path for path in array_paths if key.startswith(path + ".")]
        if not applicable:
            scalar_terms[key] = value
            continue
        deepest = max(applicable, key=len)
        nested_terms.setdefault(deepest, {})[key[len(deepest) + 1:]] = value

    def build(path):
        inner = dict(nested_terms.get(path, {}))
        for child in array_paths:
            if _parent_path(child) != path:
                continue
            inner[child[len(path) + 1:]] = {"$elemMatch": build(child)}
        return inner

    result = dict(scalar_terms)
    for path in array_paths:
        if not _parent_path(path):
            result[path] = {"$elemMatch": build(path)}
    return result


def _same_row_expr(document, mongo_query):
    """Build a condition that keeps a same-row query as one evaluation unit."""
    return _Leaf(
        _CompiledCondition.compile(
            document,
            mongo_query,
            "same_row",
            auto=True,
            force_same_row=True,
        )
    )


def _expr_from_condition(document, node):
    if isinstance(node, EvidenceCondition):
        if node.mapping is not None:
            query = _mapping_to_q(document, node.mapping)
        else:
            query = node.query
        if node.scope == "same_row":
            # A same-row condition is one unit.  Evaluating its individual
            # fields separately would let them match different rows.
            if isinstance(query, QNode):
                mongo_query = _collapsed_query(document, query)
            else:
                mongo_query = {}
            if not mongo_query:
                return _Bool("and", [])
            return _same_row_expr(document, mongo_query)
        return _bool_from_q(
            document, query, default_scope=node.scope, force_scope=node.scope
        )
    if isinstance(node, _ConditionCombination):
        return _Bool(
            node.operation,
            [
                _expr_from_condition(
                    document,
                    child
                    if isinstance(child, (EvidenceCondition, _ConditionCombination))
                    else EvidenceCondition(child),
                )
                for child in node.children
            ],
        )
    return _bool_from_q(document, node)


def _leaves(expr):
    if isinstance(expr, _Leaf):
        return [expr]
    result = []
    for child in expr.children:
        result.extend(_leaves(child))
    return result


def _leaf_paths(document, leaf):
    return leaf.condition.paths_for(document)


def _filter_hits(descendant_paths, ancestor_paths, hits, ancestor_hits):
    prefixes = {hit.position for hit in ancestor_hits}
    width = len(ancestor_paths)
    result = []
    for hit in hits:
        if hit.position[:width] in prefixes:
            result.append(hit)
    return result


def _is_pure_conjunction(expr):
    if isinstance(expr, _Leaf):
        return True
    if expr.operation == "or":
        return False
    return all(_is_pure_conjunction(child) for child in expr.children)


def _mandatory_leaves(expr):
    """Leaves which are unconditionally required by a conjunction subtree."""
    if isinstance(expr, _Leaf):
        return [expr]
    if expr.operation == "or":
        return []
    result = []
    for child in expr.children:
        result.extend(_mandatory_leaves(child))
    return result


def _evaluate_expr(expr, document, collation):
    """Evaluate an expression and return (matched, {leaf: hits}).

    For a pure conjunction, a condition on a parent array (for example a
    cold-chain group) restricts every condition on its descendant array (the
    readings in that group).  When such a group-level condition is present we
    also intersect sibling reading conditions: this is what makes

        Q(groups__kind="cold-chain")
        & Q(groups__readings__tag="hold")
        & Q(groups__readings__value=None)

    mean "the same reading" instead of "some tag and some value somewhere in
    the report".  A plain conjunction without a parent-array condition keeps
    the cross-row meaning, so ``Q(tag="hold") & Q(value=0)`` may use different
    readings.
    """

    leaves = _leaves(expr)
    raw_hits = {
        leaf: leaf.condition.hits(document, collation) for leaf in leaves
    }
    paths = {leaf: _leaf_paths(document, leaf) for leaf in leaves}

    if _is_pure_conjunction(expr):
        filtered = dict(raw_hits)
        ordered = sorted(leaves, key=lambda leaf: len(paths[leaf]))
        for leaf in ordered:
            if not (leaf.condition.auto or leaf.condition.scope == "same_row"):
                continue
            for ancestor in ordered:
                if ancestor is leaf:
                    continue
                ancestor_paths = paths[ancestor]
                leaf_paths = paths[leaf]
                if not ancestor_paths:
                    continue
                if len(ancestor_paths) >= len(leaf_paths):
                    continue
                if leaf_paths[: len(ancestor_paths)] != ancestor_paths:
                    continue
                filtered[leaf] = _filter_hits(
                    leaf_paths,
                    ancestor_paths,
                    filtered[leaf],
                    filtered[ancestor],
                )

        auto_leaves = [leaf for leaf in leaves if leaf.condition.auto]
        auto_parent = any(
            paths[a]
            and any(
                paths[d][: len(paths[a])] == paths[a]
                and len(paths[d]) > len(paths[a])
                for d in auto_leaves
            )
            for a in auto_leaves
        )

        if auto_parent:
            siblings = {}
            for leaf in leaves:
                if leaf.condition.auto:
                    siblings.setdefault(paths[leaf], []).append(leaf)
            for group in siblings.values():
                if len(group) < 2:
                    continue
                common = None
                for leaf in group:
                    positions = {hit.position for hit in filtered[leaf]}
                    common = positions if common is None else common & positions
                for leaf in group:
                    filtered[leaf] = [
                        hit for hit in filtered[leaf] if hit.position in common
                    ]

        def evaluate_flat(node):
            if isinstance(node, _Leaf):
                return bool(filtered[node])
            values = [evaluate_flat(child) for child in node.children]
            if node.operation == "and":
                return all(values)
            return any(values)

        return evaluate_flat(expr), filtered

    filtered_hits = {}

    def walk(node, context):
        if isinstance(node, _Leaf):
            hits = raw_hits[node]
            if node.condition.auto or node.condition.scope == "same_row":
                for ancestor, ancestor_hits in context.items():
                    ancestor_paths = paths[ancestor]
                    if not ancestor_paths:
                        continue
                    leaf_paths = paths[node]
                    if len(ancestor_paths) >= len(leaf_paths):
                        continue
                    if leaf_paths[: len(ancestor_paths)] != ancestor_paths:
                        continue
                    hits = _filter_hits(
                        leaf_paths, ancestor_paths, hits, ancestor_hits
                    )
            filtered_hits[node] = hits
            return bool(hits)

        if node.operation == "or":
            return any(walk(child, context) for child in node.children)

        mandatory = []
        for child in node.children:
            if _is_pure_conjunction(child):
                mandatory.extend(_mandatory_leaves(child))
        matched_all = True
        for child in node.children:
            child_context = dict(context)
            for sibling in mandatory:
                if sibling in _leaves(child):
                    continue
                child_context[sibling] = filtered_hits.get(
                    sibling, raw_hits[sibling]
                )
            if not walk(child, child_context):
                matched_all = False
        return matched_all

    matched = walk(expr, {})
    for leaf in leaves:
        filtered_hits.setdefault(leaf, raw_hits[leaf])
    return matched, filtered_hits


class _CompiledRule:
    __slots__ = ("name", "expr")

    def __init__(self, name, expr):
        self.name = name
        self.expr = expr

    def evaluate(self, document, collation):
        matched, hits_by_leaf = _evaluate_expr(self.expr, document, collation)
        conditions = []
        for leaf in _leaves(self.expr):
            condition = leaf.condition
            conditions.append(
                ConditionEvidence(
                    condition.scope,
                    condition.field,
                    hits_by_leaf.get(leaf, ()),
                    condition.db_fields(),
                )
            )
        return RuleEvidence(self.name, matched, conditions)


class EvidenceProgram:
    """A compiled set of rules, reusable across querysets and documents."""

    __slots__ = ("document", "rules", "root")

    def __init__(self, document, rules, root):
        self.document = document
        self.rules = rules
        self.root = root

    def evaluate(self, raw_doc, collation):
        # Keep a private copy: the caller must be able to describe this exact
        # document even if a writer mutates the collection while the result
        # is being consumed.
        raw_doc = copy.deepcopy(raw_doc)
        by_rule = {id(rule): rule.evaluate(raw_doc, collation) for rule in self.rules}

        def evaluate_node(node):
            if isinstance(node, _CompiledRule):
                return by_rule[id(node)].matched
            if isinstance(node, EvidenceRuleGroup):
                values = [evaluate_node(child) for child in node.children]
                if node.operation == EvidenceRuleGroup.AND:
                    return all(values)
                return any(values)
            return False

        matched = evaluate_node(self.root)
        return EvidenceResult(
            self.document,
            raw_doc,
            [by_rule[id(rule)] for rule in self.rules],
            matched,
        )


def _mapping_to_q(document, mapping):
    """Prefer MongoEngine keyword syntax and fall back to a raw query."""
    mapping = dict(mapping)
    if "__raw__" in mapping:
        return mapping["__raw__"]
    try:
        transform.query(document, **mapping)
    except Exception:
        return Q(__raw__=mapping)
    return Q(**mapping)


def _wrap_query(document, query, scope="cross_row"):
    if isinstance(query, EvidenceCondition):
        return _expr_from_condition(document, query)
    if isinstance(query, _ConditionCombination):
        return _expr_from_condition(document, query)
    if isinstance(query, Mapping):
        query = _mapping_to_q(document, query)
    if not isinstance(query, QNode):
        raise TypeError("A rule query must be a Q object or a mapping")
    return _bool_from_q(document, query, scope)


def _rule_from_descriptor(document, name, descriptor):
    """Build a rule from a mapping descriptor.

    Accepted forms include ``{"name": ..., "query"|"rules": ...}`` together
    with an optional ``"scope"`` or ``"same_row"`` option.
    """
    if isinstance(descriptor, EvidenceRule):
        return descriptor
    if isinstance(descriptor, Mapping) and (
        "query" in descriptor or "rules" in descriptor
    ):
        query = descriptor.get("query", descriptor.get("rules"))
        return EvidenceRule(
            name,
            query,
            scope=descriptor.get("scope"),
            same_row=descriptor.get("same_row", False),
        )
    return EvidenceRule(name, descriptor)


def _compile_single_rule(document, rule):
    if isinstance(rule, EvidenceRule):
        query = rule.query
        if rule.mapping is not None and isinstance(query, QNode):
            query = _mapping_to_q(document, rule.mapping)
        return _CompiledRule(rule.name, _wrap_query(document, query))
    raise TypeError("Expected an EvidenceRule, got %r" % (rule,))


def _coerce_rules(document, rules, combine="or"):
    """Normalize the many accepted forms into (leaf rules, root node)."""

    if rules is None:
        query = Q()
        # A QuerySet always has a _query_obj; use it as a single unnamed rule.
        raise TypeError("rules are required")

    if isinstance(rules, EvidenceRuleGroup):
        return _compile_group(document, rules)

    if isinstance(rules, EvidenceRule):
        compiled = _compile_single_rule(document, rules)
        return [compiled], compiled

    if isinstance(rules, tuple) and len(rules) in (2, 3) and isinstance(rules[0], str):
        name, query = rules[0], rules[1]
        options = rules[2] if len(rules) == 3 else {}
        if isinstance(options, Mapping):
            compiled = _compile_single_rule(
                document,
                EvidenceRule(
                    name,
                    query,
                    scope=options.get("scope"),
                    same_row=options.get("same_row", False),
                ),
            )
        else:
            compiled = _compile_single_rule(
                document, EvidenceRule(name, query, same_row=bool(options))
            )
        return [compiled], compiled

    if isinstance(rules, Mapping):
        leaves = []
        children = []
        for name, rule in rules.items():
            if isinstance(rule, EvidenceRule):
                compiled = _compile_single_rule(document, rule)
                leaves.append(compiled)
                children.append(compiled)
            elif isinstance(rule, EvidenceRuleGroup):
                nested_leaves, nested = _compile_group(document, rule)
                leaves.extend(nested_leaves)
                children.append(nested)
            elif isinstance(rule, (QNode, EvidenceCondition, _ConditionCombination, Mapping)):
                compiled = _compile_single_rule(
                    document, _rule_from_descriptor(document, name, rule)
                )
                leaves.append(compiled)
                children.append(compiled)
            else:
                raise TypeError("Unsupported rule specification: %r" % (rule,))
        return leaves, _combine_compiled(children, combine)

    if isinstance(rules, (list, tuple)):
        leaves = []
        children = []
        for item in rules:
            if isinstance(item, EvidenceRule):
                compiled = _compile_single_rule(document, item)
                leaves.append(compiled)
                children.append(compiled)
            elif isinstance(item, EvidenceRuleGroup):
                nested_leaves, nested = _compile_group(document, item)
                leaves.extend(nested_leaves)
                children.append(nested)
            elif (
                isinstance(item, tuple)
                and len(item) in (2, 3)
                and isinstance(item[0], str)
            ):
                name, query = item[0], item[1]
                options = item[2] if len(item) == 3 else {}
                if isinstance(options, Mapping):
                    entry = EvidenceRule(
                        name,
                        query,
                        scope=options.get("scope"),
                        same_row=options.get("same_row", False),
                    )
                else:
                    entry = EvidenceRule(name, query, same_row=bool(options))
                compiled = _compile_single_rule(document, entry)
                leaves.append(compiled)
                children.append(compiled)
            elif isinstance(item, Mapping) and "name" in item:
                name = item["name"]
                compiled = _compile_single_rule(
                    document, _rule_from_descriptor(document, name, item)
                )
                leaves.append(compiled)
                children.append(compiled)
            elif isinstance(item, (QNode, EvidenceCondition, _ConditionCombination, Mapping)):
                compiled = _CompiledRule(
                    "rule_%d" % len(leaves), _wrap_query(document, item)
                )
                leaves.append(compiled)
                children.append(compiled)
            else:
                raise TypeError("Unsupported rule specification: %r" % (item,))
        return leaves, _combine_compiled(children, combine)

    if isinstance(rules, QNode):
        # Derive named rules from top-level OR branches when possible.
        return _rules_from_q(document, rules)

    raise TypeError("Unsupported rules specification: %r" % (rules,))


def _rules_from_q(document, query):
    if isinstance(query, QCombination) and query.operation == QNode.OR:
        leaves = []
        for index, child in enumerate(query.children):
            if isinstance(child, QCombination) and child.operation == QNode.AND:
                # A branch is one rule; keep its internal AND.
                expr = _bool_from_q(document, child)
            else:
                expr = _bool_from_q(document, child)
            leaves.append(_CompiledRule("rule_%d" % index, expr))
        return leaves, _combine_compiled(leaves, "or")
    compiled = _compile_single_rule(document, EvidenceRule("rule_0", query))
    return [compiled], compiled


def _compile_group(document, group):
    leaves = []
    compiled_children = []
    for index, child in enumerate(group.children):
        if isinstance(child, EvidenceRule):
            compiled = _compile_single_rule(document, child)
            leaves.append(compiled)
            compiled_children.append(compiled)
        elif isinstance(child, EvidenceRuleGroup):
            nested_leaves, nested = _compile_group(document, child)
            leaves.extend(nested_leaves)
            compiled_children.append(nested)
        elif isinstance(
            child, (QNode, EvidenceCondition, _ConditionCombination, Mapping)
        ):
            compiled = _CompiledRule(
                "rule_%d" % index, _wrap_query(document, child)
            )
            leaves.append(compiled)
            compiled_children.append(compiled)
        else:
            raise TypeError("Unsupported rule group child: %r" % (child,))
    if len(compiled_children) == 1:
        return leaves, compiled_children[0]
    return leaves, EvidenceRuleGroup(group.operation, compiled_children)


def _combine_compiled(compiled, combine):
    if not compiled:
        return _CompiledRule("__always_true__", _Bool("and", []))
    if len(compiled) == 1:
        return compiled[0]
    operation = "and" if str(combine).lower() == "and" else "or"
    # Build a tiny synthetic group node; EvidenceProgram.evaluate understands it.
    return _CombinedCompiled(operation, compiled)


class _CombinedCompiled(EvidenceRuleGroup):
    """Internal helper that combines already-compiled rules."""

    def __init__(self, operation, children):
        super().__init__(operation, children)


def _selection_query_from_rules(rules, combine="or", document=None):
    """Best-effort conversion of the rule DSL to a regular Q object.

    This is only used when ``evidence()`` is called on an unfiltered
    queryset.  A pre-filtered queryset keeps its original selection
    untouched, which is the documented behaviour of the evidence read mode.
    """

    if isinstance(rules, EvidenceRule):
        if rules.mapping is not None:
            if document is not None:
                return _mapping_to_q(document, rules.mapping)
            return Q(__raw__=dict(rules.mapping))
        return _selection_query_from_rules(rules.query, combine, document)
    if isinstance(rules, EvidenceCondition):
        if rules.mapping is not None:
            if document is not None:
                return _mapping_to_q(document, rules.mapping)
            return Q(__raw__=dict(rules.mapping))
        return _selection_query_from_rules(rules.query, combine, document)
    if isinstance(rules, _ConditionCombination):
        query = None
        for child in rules.children:
            child_query = _selection_query_from_rules(child, combine, document)
            if query is None:
                query = child_query
            elif rules.operation == "and":
                query = query & child_query
            else:
                query = query | child_query
        return query if query is not None else Q()
    if isinstance(rules, EvidenceRuleGroup):
        query = None
        for child in rules.children:
            child_query = _selection_query_from_rules(child, combine, document)
            if query is None:
                query = child_query
            elif rules.operation == EvidenceRuleGroup.AND:
                query = query & child_query
            else:
                query = query | child_query
        return query if query is not None else Q()
    if isinstance(rules, Mapping) and "name" in rules and (
        "query" in rules or "rules" in rules
    ):
        return _selection_query_from_rules(
            rules.get("query", rules.get("rules", Q())), combine, document
        )
    if isinstance(rules, Mapping):
        query = None
        for item in rules.values():
            if isinstance(item, Mapping):
                item = item.get("query", item.get("rules", item))
            child_query = _selection_query_from_rules(item, combine, document)
            if query is None:
                query = child_query
            elif str(combine).lower() == "and":
                query = query & child_query
            else:
                query = query | child_query
        return query if query is not None else Q()
    if isinstance(rules, (list, tuple)):
        query = None
        for item in rules:
            if isinstance(item, tuple) and len(item) in (2, 3):
                item = item[1]
            elif isinstance(item, Mapping) and "name" in item:
                item = item.get("query", item.get("rules", Q()))
            child_query = _selection_query_from_rules(item, combine, document)
            if query is None:
                query = child_query
            elif str(combine).lower() == "and":
                query = query & child_query
            else:
                query = query | child_query
        return query if query is not None else Q()
    if isinstance(rules, QCombination):
        query = None
        for child in rules.children:
            child_query = _selection_query_from_rules(child, combine, document)
            if query is None:
                query = child_query
            elif rules.operation == QNode.AND:
                query = query & child_query
            else:
                query = query | child_query
        return query if query is not None else Q()
    if isinstance(rules, (QNode, EvidenceRule)):
        return _as_q_node(rules)
    return Q()


def compile_evidence_program(document, rules, combine="or"):
    leaves, root = _coerce_rules(document, rules, combine)
    return EvidenceProgram(document, leaves, root)
