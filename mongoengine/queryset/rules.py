"""Batch cleaning/update rules for MongoEngine querysets.

This module implements the "submit several rules together" workflow. Every
rule is evaluated against the *pre-image* of the documents involved (the state
loaded before any rule is written), the resulting corrections are merged, and
each document is written with a single update only when no two rules fight
over the same field of the same element.

Public API
----------

``UpdateRule`` (alias ``Rule``)
    A single rule, written with model field names.  It selects elements of a
    list field (``path``) whose fields satisfy ``match`` (and whose enclosing
    element satisfies ``scope``) and describes the changes to apply.

``QuerySet.update_rules(rules)`` / ``QuerySet.apply_rules(rules)``
    Evaluate and commit all rules together.  Returns a
    :class:`RuleApplicationResult`.

``Document.apply_rules(rules)``
    Convenience wrapper for a single document.

``RuleConflictError``
    Raised on demand (``raise_on_conflict=True`` or
    ``result.raise_for_conflicts()``) when rules contend for the same field.

Example
-------

.. code-block:: python

    from mongoengine import Q
    from mongoengine.queryset.rules import UpdateRule

    rules = [
        UpdateRule(
            name="A",
            path="devices.readings",
            match=Q(no="07", status="待核"),
            update={"set__status": "已核", "set__remark": "复核"},
        ),
        UpdateRule(
            name="B",
            path="devices.readings",
            scope=Q(device="east"),
            match=Q(no__in=["07", "08"]) & (Q(score__gte=80) | Q(status="已核")),
            update={"unset__remark": True},
        ),
    ]
    result = Submission.objects.update_rules(rules)
    result.applied    # corrections that made it to the database
    result.conflicts  # corrections that fight over the same field
"""

import re

from mongoengine.common import _import_class
from mongoengine.connection import _get_session
from mongoengine.errors import InvalidQueryError, RuleConflictError
from mongoengine.queryset import transform
from mongoengine.queryset.visitor import QNode

__all__ = (
    "UpdateRule",
    "Rule",
    "CleaningRule",
    "CorrectionRule",
    "BatchRule",
    "RuleSet",
    "RuleBatch",
    "RuleApplicationResult",
    "RuleResult",
    "RuleCorrection",
    "Correction",
    "RuleConflict",
    "RuleConflictError",
    "apply_rules",
    "submit_rules",
    "DELETE",
    "REMOVE",
)


class _DeleteValue:
    """Sentinel meaning "unset this field" inside a rule update."""

    __slots__ = ()

    def __repr__(self):
        return "DELETE"

    def __bool__(self):
        return False

    def __reduce__(self):
        return (_get_delete_value, ())


def _get_delete_value():
    return DELETE


DELETE = _DeleteValue()
REMOVE = DELETE


# ---------------------------------------------------------------------------
# Path and field helpers
# ---------------------------------------------------------------------------


def _split_field_path(path):
    """Split ``"a.b"`` / ``"a__b"`` / ``["a", "b"]`` into a list of parts."""
    if isinstance(path, (list, tuple)):
        parts = [str(part) for part in path if str(part) != ""]
    elif isinstance(path, str):
        parts = [part for part in re.split(r"\.|__", path) if part]
    else:
        raise InvalidQueryError(
            "A field path must be a string or a sequence of strings, got %r" % (path,)
        )
    if not parts:
        raise InvalidQueryError("Empty field path is not allowed")
    return parts


def _resolve_field_path(document_cls, parts):
    """Translate model field names into ``(db_parts, fields)``.

    ``fields`` mirrors MongoEngine's ``_lookup_field`` result; entries that are
    plain strings are list indices or parts that could not be resolved.
    """
    if document_cls is None:
        return list(parts), list(parts)

    try:
        fields = document_cls._lookup_field(parts)
    except Exception as exc:
        raise InvalidQueryError(exc)

    db_parts = []
    for field in fields:
        if isinstance(field, str):
            db_parts.append(field)
        else:
            db_parts.append(field.db_field)
    return db_parts, fields


def _field_by_name(document_cls, name):
    if document_cls is None:
        return None
    try:
        fields = document_cls._lookup_field([name])
    except Exception:
        return None
    return fields[0] if fields and not isinstance(fields[0], str) else None


def _element_document_type(field):
    """Return the embedded document class wrapped by ``field``, if any."""
    EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
    ListField = _import_class("ListField")
    DictField = _import_class("DictField")

    while isinstance(field, (ListField, DictField)) or hasattr(field, "field"):
        if isinstance(field, ListField) or isinstance(field, DictField):
            field = field.field
        else:
            break
        if field is None:
            return None
    if isinstance(field, EmbeddedDocumentField):
        try:
            return field.document_type
        except Exception:
            return None
    return None


def _to_query(document_cls, spec):
    """Build a MongoDB query from a ``Q``/``QNode`` object or a mapping."""
    if spec is None:
        return {}
    if isinstance(spec, QNode):
        if not bool(spec):
            return {}
        return spec.to_query(document_cls)
    if isinstance(spec, dict):
        return transform.query(document_cls, **spec)
    raise InvalidQueryError(
        "A rule match/scope must be a Q object or a dict, got %r" % (type(spec),)
    )


# ---------------------------------------------------------------------------
# Raw document matching (pre-image evaluation)
# ---------------------------------------------------------------------------


def _values_at(document, path):
    """Return all values reached by a dotted MongoDB path.

    Arrays encountered along the way are traversed implicitly, following
    MongoDB's dotted-field semantics.
    """
    return [value for _found, value in _resolve_path(document, path)]


def _resolve_path(document, path):
    """Return ``(found, value)`` pairs for a dotted path.

    ``found`` distinguishes an existing field whose value is ``[]`` from an
    entirely missing field.  Numeric parts index lists (and dict keys).
    """
    candidates = [(True, document)]
    for part in path.split("."):
        next_candidates = []
        for found, candidate in candidates:
            if not found:
                continue
            if isinstance(candidate, list):
                if part.isdigit():
                    index = int(part)
                    if index < len(candidate):
                        next_candidates.append((True, candidate[index]))
                    continue
                for item in candidate:
                    if isinstance(item, dict) and part in item:
                        next_candidates.append((True, item[part]))
            elif isinstance(candidate, dict):
                if part in candidate:
                    next_candidates.append((True, candidate[part]))
        candidates = next_candidates
    return candidates


def _path_exists(document, path):
    return bool(_resolve_path(document, path))


def _regex_matches(value, pattern, options=""):
    if not isinstance(value, str):
        return False
    flags = 0
    if "i" in options:
        flags |= re.IGNORECASE
    if "m" in options:
        flags |= re.MULTILINE
    if "s" in options:
        flags |= re.DOTALL
    if "x" in options:
        flags |= re.VERBOSE
    if hasattr(pattern, "search"):
        return bool(pattern.search(value))
    return bool(re.search(pattern, value, flags))


_BSON_TYPE_NAMES = {
    "double": (float,),
    "string": (str,),
    "object": (dict,),
    "array": (list, tuple),
    "bool": (bool,),
    "date": (),
    "int": (int,),
    "long": (int,),
    "decimal": (),
    "null": (type(None),),
}


def _iter_comparable(value):
    """Expand a stored array so operators compare against its elements."""
    if isinstance(value, list):
        return value
    return [value]


def _equal(value, expected):
    if isinstance(value, list) and not isinstance(expected, list):
        return any(item == expected for item in value)
    return value == expected


def _match_operator(value, operator, operand, options=""):
    if operator == "$eq":
        return _equal(value, operand)
    if operator == "$ne":
        return not _equal(value, operand)
    if operator == "$in":
        return any(_equal(value, option) for option in operand)
    if operator == "$nin":
        return not any(_equal(value, option) for option in operand)
    if operator in ("$gt", "$gte", "$lt", "$lte"):
        for item in _iter_comparable(value):
            try:
                if operator == "$gt":
                    matches = item > operand
                elif operator == "$gte":
                    matches = item >= operand
                elif operator == "$lt":
                    matches = item < operand
                else:
                    matches = item <= operand
            except TypeError:
                continue
            if matches:
                return True
        return False
    if operator == "$regex":
        return _regex_matches(value, operand, options)
    if operator == "$options":
        return True
    if operator == "$all":
        if not isinstance(value, list):
            return False
        return all(
            any(_equal(item, expected) for item in value) for expected in operand
        )
    if operator == "$size":
        return isinstance(value, list) and len(value) == operand
    if operator == "$elemMatch":
        if not isinstance(value, list):
            return False
        return any(_match_condition(item, operand) for item in value)
    if operator == "$not":
        return not _match_condition(value, operand)
    if operator == "$type":
        return isinstance(value, _BSON_TYPE_NAMES.get(operand, ()))
    if operator == "$mod":
        if not isinstance(operand, (list, tuple)) or len(operand) != 2:
            return False
        divisor, remainder = operand
        return isinstance(value, (int, float)) and value % divisor == remainder
    raise InvalidQueryError("Unsupported query operator: %s" % operator)


def _match_condition(value, condition):
    if (
        isinstance(condition, dict)
        and condition
        and all(key.startswith("$") for key in condition)
    ):
        options = condition.get("$options", "")
        for operator, operand in condition.items():
            if operator == "$options":
                continue
            if not _match_operator(value, operator, operand, options):
                return False
        return True
    return _equal(value, condition)


def _match_values(values, condition):
    """Apply a (possibly operator based) condition to fetched field values."""
    if (
        isinstance(condition, dict)
        and condition
        and all(key.startswith("$") for key in condition)
    ):
        if "$not" in condition:
            stripped = {k: v for k, v in condition.items() if k != "$not"}
            if stripped and _match_values(values, stripped):
                return False
            return not _match_values(values, condition["$not"])

        all_operators = {
            operator: operand
            for operator, operand in condition.items()
            if operator in ("$ne", "$nin")
        }
        any_operators = {
            operator: operand
            for operator, operand in condition.items()
            if operator not in ("$ne", "$nin", "$not", "$options")
        }
        options = condition.get("$options", "")

        for operator, operand in all_operators.items():
            if values and not all(
                _match_operator(item, operator, operand, options) for item in values
            ):
                return False
        if any_operators:
            if not values:
                return False
            for item in values:
                if all(
                    _match_operator(item, operator, operand, options)
                    for operator, operand in any_operators.items()
                ):
                    return True
            return False
        return True

    if not values:
        return False
    return any(_equal(item, condition) for item in values)


def _raw_matches(document, query):
    """Evaluate a MongoDB query document against a raw document dict."""
    for key, condition in query.items():
        if key == "$and":
            if not all(_raw_matches(document, part) for part in condition):
                return False
            continue
        if key == "$or":
            if not any(_raw_matches(document, part) for part in condition):
                return False
            continue
        if key == "$nor":
            if any(_raw_matches(document, part) for part in condition):
                return False
            continue
        if key == "$not":
            if _raw_matches(document, condition):
                return False
            continue
        if key.startswith("$"):
            raise InvalidQueryError("Unsupported top-level query operator: %s" % key)

        if isinstance(condition, dict) and "$exists" in condition:
            if _path_exists(document, key) != bool(condition["$exists"]):
                return False
            condition = {
                operator: operand
                for operator, operand in condition.items()
                if operator != "$exists"
            }
            if not condition:
                continue

        if not _match_values(_values_at(document, key), condition):
            return False
    return True


# ---------------------------------------------------------------------------
# Rule objects
# ---------------------------------------------------------------------------


class UpdateRule:
    """A single cleaning rule, written with model field names.

    :param path: dotted model path to a list field whose elements are the
        rule's targets (for instance ``"devices.readings"`` or
        ``"adjustments"``).  When omitted the rule targets the document itself.
    :param match: ``Q`` object or dict of *element-relative* conditions.
    :param scope: ``Q`` object or dict of conditions that the element enclosing
        the target list must satisfy (for instance the device of a reading).
    :param update: mapping of element-relative changes.  Keys may use
        MongoEngine update syntax (``"set__status"``, ``"unset__remark"``) or
        plain model field names (defaulting to ``$set``).
    :param filter: optional document-level ``Q`` object or dict.  It is
        combined with the rule's scope/match when selecting documents.
    :param name: human readable rule name used in conflict reports.
    """

    def __init__(
        self,
        path=None,
        match=None,
        scope=None,
        update=None,
        changes=None,
        filter=None,
        name=None,
        **kwargs,
    ):
        # Accept the common keyword aliases a rule may be written with.
        extra_updates = {}
        for argument, value in kwargs.items():
            if argument in ("field", "collection", "list_field", "target"):
                if path is None:
                    path = value
                continue
            if argument in ("where", "query", "condition", "conditions", "criteria"):
                if match is None:
                    match = value
                continue
            if argument in ("device", "owner", "within", "enclosing"):
                if scope is None:
                    scope = value
                continue
            if argument in ("document_filter", "document_query", "filter_by"):
                if filter is None:
                    filter = value
                continue
            if argument in ("updates", "assignments", "set_fields", "operations"):
                if update is None:
                    update = value
                continue
            extra_updates[argument] = value

        if extra_updates:
            if update is None:
                update = {}
            update = {**update, **extra_updates}

        if update is not None and changes is not None:
            raise InvalidQueryError("Provide either 'update' or 'changes', not both")
        if update is None:
            update = changes
        if update is None:
            raise InvalidQueryError("A rule needs an update/changes mapping")
        if not isinstance(update, dict):
            raise InvalidQueryError("A rule update must be a mapping")
        self.path = path
        self.match = match
        self.scope = scope
        self.filter = filter
        self.update = update
        self.name = name

    def __repr__(self):
        return "UpdateRule(name=%r, path=%r)" % (self.name, self.path)

    @property
    def _display_name(self):
        return self.name if self.name is not None else "rule"

    def _normalized_updates(self):
        """Yield ``(operator, model_parts, value)`` for this rule."""
        raw_style = any(
            key in ("$set", "$unset") and isinstance(value, dict)
            for key, value in self.update.items()
        )
        if raw_style:
            for operator, fields in self.update.items():
                if operator not in ("$set", "$unset"):
                    raise InvalidQueryError(
                        "A raw rule update may only use $set and $unset, got %r"
                        % (operator,)
                    )
                for field_path, value in fields.items():
                    yield (
                        "unset" if operator == "$unset" else "set",
                        _split_field_path(field_path),
                        value,
                    )
            return

        for key, value in self.update.items():
            if key in ("set", "unset", "delete", "remove") and isinstance(value, dict):
                operator = "unset" if key in ("unset", "delete", "remove") else "set"
                for field_path, item in value.items():
                    yield operator, _split_field_path(field_path), item
                continue
            if key.startswith("set__"):
                yield "set", _split_field_path(key[len("set__") :]), value
                continue
            if key.startswith("unset__"):
                yield "unset", _split_field_path(key[len("unset__") :]), value
                continue
            if key.startswith("delete__"):
                yield "unset", _split_field_path(key[len("delete__") :]), value
                continue
            if key.startswith("remove__"):
                yield "unset", _split_field_path(key[len("remove__") :]), value
                continue
            if key in ("set", "unset", "delete", "remove"):
                raise InvalidQueryError(
                    "The '%s' operator needs a mapping of field paths" % key
                )
            if isinstance(value, _DeleteValue):
                yield "unset", _split_field_path(key), value
                continue
            yield "set", _split_field_path(key), value


Rule = UpdateRule
CleaningRule = UpdateRule
CorrectionRule = UpdateRule
BatchRule = UpdateRule


class RuleCorrection:
    """A single correction produced by a rule for a concrete element/field."""

    __slots__ = (
        "rule",
        "document_id",
        "path",
        "model_path",
        "field_name",
        "operation",
        "value",
    )

    def __init__(
        self, rule, document_id, path, model_path, field_name, operation, value
    ):
        self.rule = rule
        self.document_id = document_id
        self.path = path
        self.model_path = model_path
        self.field_name = field_name
        self.operation = operation
        self.value = value

    def __repr__(self):
        return "RuleCorrection(rule=%r, path=%r, operation=%r, value=%r)" % (
            self.rule,
            self.path,
            self.operation,
            self.value,
        )

    def as_dict(self):
        return {
            "rule": self.rule,
            "document_id": self.document_id,
            "path": self.path,
            "model_path": self.model_path,
            "operation": self.operation,
            "value": self.value,
        }


class RuleConflict:
    """Two or more rules want to write the same field differently."""

    __slots__ = ("document_id", "path", "model_path", "rules", "corrections")

    def __init__(self, document_id, path, model_path, corrections):
        self.document_id = document_id
        self.path = path
        self.model_path = model_path
        self.corrections = list(corrections)
        names = {correction.rule for correction in self.corrections}
        self.rules = tuple(sorted(names))

    def __repr__(self):
        return "RuleConflict(document_id=%r, path=%r, rules=%r)" % (
            self.document_id,
            self.path,
            self.rules,
        )

    def as_dict(self):
        return {
            "document_id": self.document_id,
            "path": self.path,
            "model_path": self.model_path,
            "rules": self.rules,
            "corrections": [correction.as_dict() for correction in self.corrections],
        }


class RuleApplicationResult:
    """Outcome of submitting a batch of rules together."""

    __slots__ = (
        "rules",
        "documents",
        "corrections",
        "conflicts",
        "applied",
        "dry_run",
        "writes",
    )

    def __init__(
        self,
        rules,
        documents=None,
        corrections=None,
        conflicts=None,
        applied=None,
        dry_run=False,
        writes=None,
    ):
        self.rules = list(rules)
        self.documents = list(documents or [])
        self.corrections = list(corrections or [])
        self.conflicts = list(conflicts or [])
        self.applied = list(applied or [])
        self.dry_run = dry_run
        self.writes = dict(writes or {})

    @property
    def ok(self):
        return not self.conflicts

    @property
    def has_conflicts(self):
        return bool(self.conflicts)

    @property
    def is_conflict(self):
        return bool(self.conflicts)

    @property
    def matched(self):
        return self.corrections

    @property
    def planned(self):
        return self.corrections if self.dry_run else []

    @property
    def conflicted_corrections(self):
        corrections = []
        for conflict in self.conflicts:
            corrections.extend(conflict.corrections)
        return corrections

    @property
    def applied_count(self):
        return len(self.applied)

    @property
    def applied_documents(self):
        return list(self.writes)

    @property
    def conflicting_documents(self):
        return sorted({conflict.document_id for conflict in self.conflicts})

    def applied_by_rule(self):
        mapping = {}
        for correction in self.applied:
            mapping.setdefault(correction.rule, []).append(correction.as_dict())
        return mapping

    @property
    def error(self):
        if not self.conflicts:
            return None
        return RuleConflictError(result=self)

    def raise_for_conflicts(self):
        if self.conflicts:
            raise RuleConflictError(result=self)
        return self

    raise_if_conflicts = raise_for_conflicts
    raise_on_conflict = raise_for_conflicts

    def as_dict(self):
        return {
            "ok": self.ok,
            "documents": self.documents,
            "corrections": [correction.as_dict() for correction in self.corrections],
            "conflicts": [conflict.as_dict() for conflict in self.conflicts],
            "applied": [correction.as_dict() for correction in self.applied],
        }

    def __bool__(self):
        return self.ok

    def __repr__(self):
        return (
            "RuleApplicationResult(ok=%r, documents=%r, corrections=%d, "
            "conflicts=%d, applied=%d)"
            % (
                self.ok,
                self.documents,
                len(self.corrections),
                len(self.conflicts),
                len(self.applied),
            )
        )


class RuleSet(list):
    """A list of :class:`UpdateRule` objects that can be applied as a batch."""

    def __init__(self, rules=(), name=None):
        super().__init__(rules)
        self.name = name

    def apply(self, queryset, **kwargs):
        return queryset.update_rules(self, **kwargs)

    apply_rules = apply
    update_rules = apply
    submit = apply


RuleBatch = RuleSet
RuleResult = RuleApplicationResult
Correction = RuleCorrection


# ---------------------------------------------------------------------------
# Rule normalization
# ---------------------------------------------------------------------------

_RULE_KEY_ALIASES = {
    "path": ("path", "field", "collection", "list_field", "target", "path_to"),
    "match": (
        "match",
        "where",
        "query",
        "condition",
        "conditions",
        "criteria",
        "matches",
        "match_on",
    ),
    "scope": ("scope", "device", "owner", "within", "enclosing", "parent", "scope_on"),
    "filter": ("filter", "document_filter", "document_query", "documents", "filters"),
    "update": (
        "update",
        "updates",
        "changes",
        "assignments",
        "set_fields",
        "operations",
    ),
    "name": ("name", "rule", "id", "label"),
}


def _coerce_rule(rule, index):
    if isinstance(rule, UpdateRule):
        if rule.name is None:
            rule.name = "rule-%d" % (index + 1)
        return rule
    if isinstance(rule, dict):
        mapping = dict(rule)
    elif hasattr(rule, "__dict__") or hasattr(type(rule), "__dict__"):
        mapping = {}
        for source in (vars(type(rule)), vars(rule)):
            for key, value in source.items():
                if key.startswith("_") or callable(value):
                    continue
                mapping[key] = value
    else:
        raise InvalidQueryError(
            "Rules must be UpdateRule instances, objects or dicts, got %r"
            % (type(rule),)
        )

    data = {}
    for argument, aliases in _RULE_KEY_ALIASES.items():
        for alias in aliases:
            if alias in mapping and argument not in data:
                data[argument] = mapping[alias]
                break

    known_keys = set()
    for aliases in _RULE_KEY_ALIASES.values():
        known_keys.update(aliases)
    known_keys.update({"set", "unset", "delete", "remove", "$set", "$unset"})

    if "update" not in data:
        direct_update = {}
        for operator in ("$set", "set"):
            if operator in mapping and isinstance(mapping[operator], dict):
                direct_update["set"] = {
                    **direct_update.get("set", {}),
                    **mapping[operator],
                }
        for operator in ("$unset", "unset", "delete", "remove"):
            if operator in mapping and isinstance(mapping[operator], dict):
                direct_update["unset"] = {
                    **direct_update.get("unset", {}),
                    **mapping[operator],
                }
        if direct_update:
            data["update"] = direct_update
        else:
            leftover = {
                key: value
                for key, value in mapping.items()
                if key not in known_keys and not key.startswith("$")
            }
            if leftover:
                data["update"] = leftover

    data.setdefault("name", "rule-%d" % (index + 1))
    return UpdateRule(**data)


def _flatten_rules(rules):
    for rule in rules:
        if isinstance(rule, (list, tuple, RuleSet)):
            yield from _flatten_rules(rule)
        else:
            yield rule


def _looks_like_rule_dict(rule):
    """Tell a single rule dict from a mapping of rule-name -> rule body."""
    known = set()
    for aliases in _RULE_KEY_ALIASES.values():
        known.update(aliases)
    return any(
        key in known or key.startswith(("set__", "unset__", "$")) for key in rule
    )


def _named_rule(name, body):
    if isinstance(body, UpdateRule):
        if body.name is None:
            body.name = name
        return body
    if isinstance(body, dict):
        body = dict(body)
        body.setdefault("name", name)
        return body
    raise InvalidQueryError(
        "Rule %r must be an UpdateRule or a mapping, got %r" % (name, type(body))
    )


# ---------------------------------------------------------------------------
# Target discovery
# ---------------------------------------------------------------------------


def _child_containers(container, container_cls, segment):
    """Yield the containers nested one level below ``container``."""
    if not isinstance(container, dict):
        return

    field = _field_by_name(container_cls, segment)
    db_field = field.db_field if field is not None else segment

    if db_field not in container:
        return
    value = container[db_field]

    if field is None:
        # Dynamic/untyped container: just descend into embedded dicts/lists.
        if isinstance(value, dict):
            yield value, None, [db_field], [segment]
            return
        elif isinstance(value, list):
            for position, element in enumerate(value):
                if isinstance(element, dict):
                    yield (
                        element,
                        None,
                        [db_field, str(position)],
                        [
                            segment,
                            str(position),
                        ],
                    )
            return
        else:
            return

    ListField = _import_class("ListField")
    DictField = _import_class("DictField")
    EmbeddedDocumentField = _import_class("EmbeddedDocumentField")

    if isinstance(field, ListField):
        if not isinstance(value, list):
            return
        element_cls = _element_document_type(field)
        for position, element in enumerate(value):
            yield (
                element,
                element_cls,
                [db_field, str(position)],
                [segment, str(position)],
            )
    elif isinstance(field, DictField):
        if not isinstance(value, dict):
            return
        element_cls = _element_document_type(field)
        for key, element in value.items():
            if isinstance(element, dict):
                yield element, element_cls, [db_field, str(key)], [segment, str(key)]
            elif isinstance(element, list):
                for position, item in enumerate(element):
                    if isinstance(item, dict):
                        yield (
                            item,
                            element_cls,
                            [db_field, str(key), str(position)],
                            [segment, str(key), str(position)],
                        )
    elif isinstance(field, EmbeddedDocumentField) and isinstance(value, dict):
        yield value, _element_document_type(field), [db_field], [segment]


def _walk_containers(document, document_cls, segments):
    """Yield ``(container, container_cls, db_prefix, model_prefix)`` tuples."""
    containers = [(document, document_cls, [], [])]
    for segment in segments:
        if segment.isdigit():
            # Explicit index into the list expanded by the previous segment.
            containers = [
                entry for entry in containers if entry[3] and entry[3][-1] == segment
            ]
            continue
        next_containers = []
        for container, container_cls, db_prefix, model_prefix in containers:
            for child in _child_containers(container, container_cls, segment):
                child_container, child_cls, child_db, child_model = child
                next_containers.append(
                    (
                        child_container,
                        child_cls,
                        db_prefix + child_db,
                        model_prefix + child_model,
                    )
                )
        containers = next_containers
    return containers


def _iter_targets(document, document_cls, rule):
    """Yield ``(element, element_cls, db_prefix, model_prefix)`` for matches."""
    if rule.path is None:
        if rule.scope is not None:
            scope_query = _to_query(document_cls, rule.scope)
            if not _raw_matches(document, scope_query):
                return
        yield document, document_cls, [], []
        return

    segments = _split_field_path(rule.path)
    final_segment = segments[-1]
    containers = _walk_containers(document, document_cls, segments[:-1])

    for container, container_cls, db_prefix, model_prefix in containers:
        if not isinstance(container, dict):
            continue

        if rule.scope is not None:
            scope_query = _to_query(container_cls, rule.scope)
            if not _raw_matches(container, scope_query):
                continue

        target_field = _field_by_name(container_cls, final_segment)
        db_field = target_field.db_field if target_field is not None else final_segment
        if db_field not in container:
            continue
        values = container[db_field]
        if not isinstance(values, list):
            continue

        target_cls = _element_document_type(target_field)
        match_query = _to_query(target_cls, rule.match)

        for position, element in enumerate(values):
            if match_query and not _raw_matches(element, match_query):
                continue
            yield (
                element,
                target_cls,
                db_prefix + [db_field, str(position)],
                model_prefix + [final_segment, str(position)],
            )


def _build_corrections(document, document_cls, document_id, rule):
    corrections = []
    for element, element_cls, db_prefix, model_prefix in _iter_targets(
        document, document_cls, rule
    ):
        for operator, model_parts, value in rule._normalized_updates():
            db_parts, fields = _resolve_field_path(element_cls, model_parts)
            field = fields[-1] if fields and not isinstance(fields[-1], str) else None
            if operator == "unset" or isinstance(value, _DeleteValue):
                converted = None
                operator = "unset"
            else:
                converted = _convert_value(field, value)
            corrections.append(
                RuleCorrection(
                    rule=rule._display_name,
                    document_id=document_id,
                    path=".".join(db_prefix + db_parts),
                    model_path=".".join(model_prefix + model_parts),
                    field_name=model_parts[-1],
                    operation=operator,
                    value=converted,
                )
            )
    return corrections


def _convert_value(field, value):
    if field is None:
        return value
    prepare = getattr(field, "prepare_query_value", None)
    if prepare is None:
        return value
    try:
        return prepare("set", value)
    except Exception as exc:
        raise InvalidQueryError(exc)


# ---------------------------------------------------------------------------
# Conflict detection and writes
# ---------------------------------------------------------------------------


def _same_value(left, right):
    try:
        return left == right
    except Exception:
        return False


def _paths_overlap(parent, child):
    return child.startswith(parent + ".")


def _find_conflicts(document_id, corrections):
    conflicts = []
    by_path = {}
    for correction in corrections:
        by_path.setdefault(correction.path, []).append(correction)

    for path, entries in by_path.items():
        first = entries[0]
        compatible = all(
            entry.operation == first.operation
            and (first.operation != "set" or _same_value(first.value, entry.value))
            for entry in entries
        )
        if not compatible:
            conflicts.append(RuleConflict(document_id, path, first.model_path, entries))

    paths = list(by_path)
    for index, parent in enumerate(paths):
        for child in paths[index + 1 :]:
            if _paths_overlap(parent, child) or _paths_overlap(child, parent):
                conflicts.append(
                    RuleConflict(
                        document_id,
                        child if _paths_overlap(parent, child) else parent,
                        by_path[parent][0].model_path,
                        by_path[parent] + by_path[child],
                    )
                )
    return conflicts


def _merged_write_document(corrections):
    set_values = {}
    unset_values = {}
    for correction in corrections:
        if correction.operation == "unset":
            unset_values[correction.path] = ""
        else:
            set_values[correction.path] = correction.value
    update = {}
    if set_values:
        update["$set"] = set_values
    if unset_values:
        update["$unset"] = unset_values
    return update


def submit_rules(queryset, rules=None, **kwargs):
    """Convenience alias for :func:`apply_rules`."""
    return apply_rules(queryset, rules, **kwargs)


def apply_rules(
    queryset,
    rules=None,
    dry_run=False,
    raise_on_conflict=False,
    write_concern=None,
    **kwargs,
):
    """Evaluate ``rules`` against ``queryset``'s pre-image and apply them."""
    for alias in ("raise_if_conflicts", "raise_conflict", "fail_on_conflict"):
        if alias in kwargs:
            raise_on_conflict = kwargs.pop(alias)
    for alias in ("plan_only", "plan", "preview"):
        if alias in kwargs:
            dry_run = kwargs.pop(alias)
    for alias in ("multi", "atomic", "session"):
        kwargs.pop(alias, None)

    if rules is None:
        rules = []
    if isinstance(rules, dict):
        if _looks_like_rule_dict(rules):
            rules = [rules]
        else:
            # A mapping of rule name -> rule body.
            rules = [_named_rule(name, body) for name, body in rules.items()]
    elif isinstance(rules, UpdateRule):
        rules = [rules]
    normalized_rules = [
        _coerce_rule(rule, index) for index, rule in enumerate(_flatten_rules(rules))
    ]

    document_cls = queryset._document
    raw_documents = list(queryset.clone().as_pymongo())

    corrections = []
    conflicts = []
    conflicts_by_document = {}
    documents = []

    for document in raw_documents:
        document_id = document.get("_id")
        if document_id is None:
            continue

        rule_corrections = []
        document_matched = False
        for rule in normalized_rules:
            filter_query = _to_query(document_cls, rule.filter)
            if filter_query and not _raw_matches(document, filter_query):
                continue
            document_corrections = _build_corrections(
                document, document_cls, document_id, rule
            )
            if document_corrections:
                document_matched = True
                rule_corrections.extend(document_corrections)

        if not document_matched:
            continue

        documents.append(document_id)
        corrections.extend(rule_corrections)
        document_conflicts = _find_conflicts(document_id, rule_corrections)
        if document_conflicts:
            conflicts_by_document[document_id] = document_conflicts
            conflicts.extend(document_conflicts)

    result = RuleApplicationResult(
        rules=normalized_rules,
        documents=documents,
        corrections=corrections,
        conflicts=conflicts,
        applied=[],
        dry_run=dry_run,
    )

    if dry_run:
        if raise_on_conflict and conflicts:
            raise RuleConflictError(result=result)
        return result

    writes = {}
    written_corrections = []
    collection = queryset._collection
    if write_concern:
        collection = collection.with_options(**write_concern)
    for document_id in documents:
        if document_id in conflicts_by_document:
            continue
        document_corrections = [
            correction
            for correction in corrections
            if correction.document_id == document_id
        ]
        update = _merged_write_document(document_corrections)
        if not update:
            continue
        writes[document_id] = update
        collection.update_one({"_id": document_id}, update, session=_get_session())
        written_corrections.extend(document_corrections)

    result.writes = writes
    result.applied = written_corrections

    if raise_on_conflict and conflicts:
        raise RuleConflictError(result=result)
    return result
