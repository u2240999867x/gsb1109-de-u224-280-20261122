"""Read-only recycling approval preview for querysets.

This module provides a dry-run counterpart to the reverse delete rules
(``CASCADE`` / ``DENY`` / ``NULLIFY`` / ``PULL``) applied by
:meth:`~mongoengine.queryset.QuerySet.delete`.  Given the page of documents
actually returned by a queryset, a :class:`RecyclePreview` computes the
largest group of documents that can be recycled together for any selection of
candidates, explains why the other selected documents have to stay, and lists
the references that surviving documents would have to clear.

The preview only reads the database: nothing is ever written and no delete
signals are triggered.  A preview is a point-in-time snapshot.  Re-evaluating
it with a different selection reuses the documents and references captured at
opening time; calling :meth:`~mongoengine.queryset.QuerySet.recycle_preview`
again is required to pick up changes in the database.
"""

from collections import deque

from mongoengine.errors import OperationError
from mongoengine.queryset.base import CASCADE, DENY, NULLIFY, PULL

__all__ = (
    "RecyclePreview",
    "RecycleResult",
    "BlockingPath",
    "PathEdge",
    "FieldClear",
)


def _reference_ids(value):
    """Extract the referenced document id(s) from a stored field value."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        ids = []
        for item in value:
            ids.extend(_reference_ids(item))
        return ids
    if isinstance(value, dict) and "_ref" in value:
        # GenericReferenceField stores {"_cls": ..., "_ref": DBRef(...)}
        return [value["_ref"].id]
    if hasattr(value, "id"):
        return [value.id]
    if hasattr(value, "pk"):
        return [value.pk]
    return [value]


class _RuleSnapshot:
    """An evaluated reverse delete rule captured at preview opening.

    Each entry describes one referring document and the target ids it
    references through the rule's field (restricted to the preview universe).
    """

    def __init__(self, rule, document_cls, field_name, db_field, entries):
        self.rule = rule
        self.document_cls = document_cls
        self.field_name = field_name
        self.db_field = db_field
        self.entries = entries


class RecyclePreview:
    """A read-only recycling approval preview.

    Instances are created through
    :meth:`~mongoengine.queryset.QuerySet.recycle_preview` and are bound to the
    page of documents returned by that queryset and to the reverse rules
    observed in the database at opening time.

    :meth:`evaluate` (or calling the preview directly) may be invoked
    repeatedly with different selections without touching the database again.
    Reopen the preview to obtain an up-to-date view.
    """

    def __init__(self, document, candidates, rule_snapshots):
        self._document = document
        self._candidates = list(candidates)
        self._candidate_ids = [candidate.id for candidate in self._candidates]
        self._rules = rule_snapshots
        self._documents = {candidate.id: candidate for candidate in self._candidates}
        for rule in self._rules:
            for entry in rule.entries:
                self._documents.setdefault(entry["document"].id, entry["document"])

    @property
    def document_class(self):
        """The document class the originating queryset returns."""
        return self._document

    @property
    def candidates(self):
        """The page of documents captured from the queryset at opening."""
        return list(self._candidates)

    def evaluate(self, selected=None):
        """Compute the recycling result for a selection of candidates.

        :param selected: candidate documents (or their ids) currently ticked
            by the approver.  ``None`` means every candidate.  Ids and
            documents that do not belong to the captured page are ignored.
        :returns: a :class:`RecycleResult` computed from the snapshot.
        """
        selected_ids = self._resolve_selection(selected)
        selected_id_set = set(selected_ids)

        blocked = set()
        while True:
            removable, parents = self._cascade_closure(selected_id_set - blocked)
            # A blocked candidate could be pulled back in through another
            # CASCADE edge; it must never be considered removable.
            removable.difference_update(blocked)

            violation = self._first_deny_violation(removable)
            if violation is None:
                break

            target_id, _, _ = violation
            blocked_candidates = (
                self._cascade_upstream(target_id, parents) | {target_id}
            ) & selected_id_set
            if blocked_candidates <= blocked:
                break
            blocked.update(blocked_candidates)

        blocking_paths = {}
        for candidate_id in blocked:
            edges = self._explain(candidate_id, removable)
            if edges:
                blocking_paths[candidate_id] = [
                    BlockingPath(
                        edges=edges,
                        blocked_document=self._documents.get(candidate_id),
                    )
                ]

        recycled = [
            self._documents[doc_id]
            for doc_id in self._removable_order(removable)
        ]
        retained = [
            self._documents[doc_id]
            for doc_id in selected_ids
            if doc_id not in removable
        ]

        return RecycleResult(
            preview=self,
            recycled_documents=recycled,
            retained_documents=retained,
            blocking_paths={
                self._documents[doc_id]: paths
                for doc_id, paths in blocking_paths.items()
            },
            field_clears=self._field_clears(removable),
        )

    def __call__(self, selected=None):
        return self.evaluate(selected)

    def _resolve_selection(self, selected):
        if selected is None:
            return list(self._candidate_ids)

        candidate_id_set = set(self._candidate_ids)
        resolved = []
        for item in selected:
            item_id = item.id if hasattr(item, "id") else item
            if item_id in candidate_id_set and item_id not in resolved:
                resolved.append(item_id)
        return resolved

    def _cascade_closure(self, seeds):
        """Expand ticked documents with every CASCADE victim.

        Returns the removable id set and a map recording, for every document
        pulled in by CASCADE, the target documents that pull it in.
        """
        removable = set(seeds)
        parents = {}
        while True:
            added = False
            for rule in self._rules:
                if rule.rule != CASCADE:
                    continue
                for entry in rule.entries:
                    if entry["id"] in removable:
                        continue
                    pull_targets = entry["target_ids"] & removable
                    if pull_targets:
                        removable.add(entry["id"])
                        parents.setdefault(entry["id"], set()).update(pull_targets)
                        added = True
            if not added:
                return removable, parents

    def _first_deny_violation(self, removable):
        """Return the first ``(target_id, rule, entry)`` DENY obstruction."""
        violations = []
        for rule in self._rules:
            if rule.rule != DENY:
                continue
            for entry in rule.entries:
                if entry["id"] in removable:
                    continue
                for target_id in entry["target_ids"] & removable:
                    violations.append((target_id, rule, entry))
        if not violations:
            return None
        violations.sort(
            key=lambda item: (
                str(item[0]),
                item[1].document_cls.__name__,
                item[1].db_field,
                str(item[2]["id"]),
            )
        )
        return violations[0]

    def _incoming_edges(self, target_id, rule_kind=None):
        """All rule entries referencing ``target_id``, as (rule, entry)."""
        edges = []
        for rule in self._rules:
            if rule_kind is not None and rule.rule != rule_kind:
                continue
            for entry in rule.entries:
                if target_id in entry["target_ids"]:
                    edges.append((rule, entry))
        return edges

    def _cascade_upstream(self, start_id, parents):
        """Documents on which ``start_id``'s removal depends via CASCADE.

        ``parents`` maps a cascade-removed referrer to the targets that pull
        it in.  Following those targets back yields the selected seeds that
        ultimately cause ``start_id`` to be removable: the audit DENY lands
        on cascade victim Z, whose removal traces back to selected task B.
        """
        upstream = set()
        pending = deque([start_id])
        while pending:
            current = pending.popleft()
            for target_id in parents.get(current, set()):
                if target_id not in upstream:
                    upstream.add(target_id)
                    pending.append(target_id)
        return upstream

    def _explain(self, blocked_id, removable):
        """Find the shortest reference chain explaining a retained candidate.

        The walk starts at the blocked document and moves between documents
        that all survive this preview:

        * across an incoming reference to its (retained) referrer, for both
          CASCADE and DENY rules;
        * across an outgoing CASCADE reference of the current document to a
          retained target.  This lets the explanation continue past a
          retained CASCADE victim such as Y, which blocks A through DENY but
          itself stays because it follows B through CASCADE.

        It ends at a document with no further surviving reference to follow,
        and the last edge of the chain is always the DENY reference that
        anchors the block.  For B the chain is, for example,
        ``B --(CASCADE via Z.field)--> Z <--(DENY via AuditForm.field)--``.
        """
        queue = deque([(blocked_id, [])])
        seen = {blocked_id}
        while queue:
            current_id, edges = queue.popleft()
            transitions = [
                (next_id, edge)
                for next_id, edge in self._explanation_transitions(
                    current_id, removable
                )
                if next_id not in seen
            ]

            if not transitions and edges and edges[-1].rule == DENY:
                return edges

            for next_id, edge in sorted(transitions, key=lambda item: str(item[0])):
                seen.add(next_id)
                queue.append((next_id, edges + [edge]))

        return []

    def _explanation_transitions(self, node_id, removable):
        """``(next_node_id, PathEdge)`` transitions for :meth:`_explain`.

        Only CASCADE and DENY edges participate: NULLIFY/PULL references are
        soft cleanup and never block recycling, so they cannot explain a
        retained candidate.
        """
        for rule, entry in self._incoming_edges(node_id):
            if rule.rule not in (CASCADE, DENY):
                continue
            if entry["id"] not in removable:
                yield entry["id"], self._make_edge(rule, entry)

        for rule in self._rules:
            if rule.rule != CASCADE:
                continue
            for entry in rule.entries:
                if entry["id"] != node_id or entry["id"] in removable:
                    continue
                for target_id in entry["target_ids"]:
                    if target_id not in removable:
                        yield target_id, self._make_edge(rule, entry)

    @staticmethod
    def _make_edge(rule, entry):
        return PathEdge(
            document=entry["document"],
            document_cls=rule.document_cls,
            field_name=rule.field_name,
            db_field=rule.db_field,
            rule=rule.rule,
        )

    def _field_clears(self, removable):
        clears = []
        for rule in self._rules:
            if rule.rule not in (NULLIFY, PULL):
                continue
            for entry in rule.entries:
                if entry["id"] in removable:
                    continue
                if rule.rule == NULLIFY:
                    if not entry["target_ids"] & removable:
                        continue
                    clears.append(
                        FieldClear(
                            document=entry["document"],
                            document_cls=rule.document_cls,
                            field_name=rule.field_name,
                            db_field=rule.db_field,
                            rule=NULLIFY,
                            removed_values=[],
                        )
                    )
                else:
                    removed = []
                    for value in entry["raw_value"]:
                        ids = _reference_ids(value)
                        if ids and ids[0] in removable:
                            removed.append(ids[0])
                    if not removed:
                        continue
                    clears.append(
                        FieldClear(
                            document=entry["document"],
                            document_cls=rule.document_cls,
                            field_name=rule.field_name,
                            db_field=rule.db_field,
                            rule=PULL,
                            removed_values=removed,
                        )
                    )

        unique = {}
        for clear in clears:
            unique.setdefault((clear.document.id, clear.db_field), clear)
        return list(unique.values())

    def _removable_order(self, removable):
        order = {doc_id: index for index, doc_id in enumerate(self._candidate_ids)}
        return sorted(
            removable,
            key=lambda doc_id: (
                doc_id not in order,
                order.get(doc_id, len(order)),
                str(doc_id),
            ),
        )


class PathEdge:
    """One edge of a :class:`BlockingPath`."""

    def __init__(self, document, document_cls, field_name, db_field, rule):
        self.document = document
        self.document_cls = document_cls
        self.field_name = field_name
        self.db_field = db_field
        self.rule = rule

    def __repr__(self):
        rule_name = {CASCADE: "CASCADE", DENY: "DENY"}.get(self.rule, self.rule)
        return (
            "<PathEdge %s.%s (%s) %s %s>"
            % (
                self.document_cls.__name__,
                self.field_name,
                self.db_field,
                rule_name,
                getattr(self.document, "id", None),
            )
        )


class BlockingPath:
    """The chain of references that keeps a candidate from being recycled."""

    def __init__(self, edges, blocked_document):
        self.edges = edges
        self.blocked_document = blocked_document

    @property
    def path(self):
        return list(self.edges)

    @property
    def documents(self):
        return [edge.document for edge in self.path]

    @property
    def rule(self):
        return self.edges[-1].rule if self.edges else None

    def __repr__(self):
        chain = " -> ".join(
            "%s.%s" % (edge.document_cls.__name__, edge.field_name)
            for edge in self.edges
        )
        return "<BlockingPath %s>" % chain


class FieldClear:
    """A reference on a surviving document that would have to be cleared."""

    def __init__(
        self, document, document_cls, field_name, db_field, rule, removed_values
    ):
        self.document = document
        self.document_cls = document_cls
        self.field_name = field_name
        self.db_field = db_field
        self.rule = rule
        self.removed_values = removed_values

    @property
    def operation(self):
        return "unset" if self.rule == NULLIFY else "pull"

    def __repr__(self):
        return (
            "<FieldClear %s(%s).%s (%s) %s>"
            % (
                self.document_cls.__name__,
                getattr(self.document, "id", None),
                self.field_name,
                self.db_field,
                self.operation,
            )
        )


class RecycleResult:
    """The outcome of evaluating a :class:`RecyclePreview` for one selection."""

    def __init__(
        self,
        preview,
        recycled_documents,
        retained_documents,
        blocking_paths,
        field_clears,
    ):
        self.preview = preview
        self.recycled_documents = recycled_documents
        self.retained_documents = retained_documents
        self.blocking_paths = blocking_paths
        self.field_clears = field_clears

    @property
    def recycled_ids(self):
        return [document.id for document in self.recycled_documents]

    @property
    def retained_ids(self):
        return [document.id for document in self.retained_documents]

    def blocking_paths_for(self, document):
        """Return the blocking paths recorded for a retained candidate."""
        return self.blocking_paths.get(document, [])

    def __repr__(self):
        return (
            "<RecycleResult recycled=%d retained=%d field_clears=%d>"
            % (
                len(self.recycled_documents),
                len(self.retained_documents),
                len(self.field_clears),
            )
        )


def build_recycle_preview(queryset):
    """Capture the read-only snapshot backing a :class:`RecyclePreview`.

    The queryset is materialised once (the page the approver gets to tick)
    and the reverse delete rules reachable from that page through CASCADE
    edges are evaluated against the database.  All later selections are
    answered from this snapshot; the database is not touched again.
    """
    candidates = list(queryset)
    root_cls = queryset._document
    if queryset._scalar or queryset._as_pymongo:
        raise OperationError(
            "recycle_preview requires a queryset returning %s documents"
            % root_cls.__name__
        )
    universe = {candidate.id for candidate in candidates}

    # Candidates can belong to subclasses whose reverse rules are registered
    # on the subclass rather than the queried document class.
    seen_classes = {root_cls}
    seen_classes.update(candidate.__class__ for candidate in candidates)
    fetched = {}
    changed = True
    while changed:
        changed = False
        for cls in list(seen_classes):
            delete_rules = cls._meta.get("delete_rules") or {}
            for (document_cls, field_name), rule in delete_rules.items():
                if rule != CASCADE or document_cls._meta.get("abstract"):
                    continue
                key = (document_cls, field_name)
                known = {entry.id for entry in fetched.get(key, [])}
                for ref in _referencing_documents(document_cls, field_name, universe):
                    if ref.id in known:
                        continue
                    fetched.setdefault(key, []).append(ref)
                    known.add(ref.id)
                    if ref.id not in universe:
                        universe.add(ref.id)
                        changed = True
                    if ref.__class__ not in seen_classes:
                        seen_classes.add(ref.__class__)
                        changed = True

    rule_snapshots = []
    snapshot_keys = set()
    for cls in seen_classes:
        delete_rules = cls._meta.get("delete_rules") or {}
        for (document_cls, field_name), rule in delete_rules.items():
            if document_cls._meta.get("abstract"):
                continue
            key = (document_cls, field_name)
            if key in snapshot_keys or not universe:
                continue
            snapshot_keys.add(key)

            refs = fetched.get(key)
            if refs is None:
                refs = _referencing_documents(document_cls, field_name, universe)
            if not refs:
                continue

            db_field = document_cls._db_field_map.get(field_name, field_name)
            entries = []
            for ref in refs:
                value = getattr(ref, field_name)
                target_ids = set(_reference_ids(value)) & universe
                if not target_ids:
                    continue
                entry = {
                    "id": ref.id,
                    "document": ref,
                    "target_ids": target_ids,
                }
                if rule == PULL:
                    if value is None:
                        entry["raw_value"] = []
                    elif isinstance(value, (list, tuple)):
                        entry["raw_value"] = list(value)
                    else:
                        entry["raw_value"] = [value]
                entries.append(entry)

            if entries:
                rule_snapshots.append(
                    _RuleSnapshot(
                        rule=rule,
                        document_cls=document_cls,
                        field_name=field_name,
                        db_field=db_field,
                        entries=entries,
                    )
                )

    return RecyclePreview(
        document=root_cls,
        candidates=candidates,
        rule_snapshots=rule_snapshots,
    )


def _referencing_documents(document_cls, field_name, universe):
    """Fetch documents referencing any id in ``universe`` through ``field``."""
    if not universe:
        return []
    return list(
        document_cls.objects.no_dereference().filter(
            **{"%s__in" % field_name: list(universe)}
        )
    )
