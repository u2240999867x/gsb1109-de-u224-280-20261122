"""
Object graph queries and corrections.

The regular JSONPath semantics treat data as a tree: every container is
visited in isolation.  Python programs often build *object graphs* instead,
where the same ``dict`` or ``list`` is referenced from several places and
references may eventually loop back to a container that is already on the
current walk.  The tools in this module are opt-in; plain
:meth:`JSONPath.find` keeps its tree semantics and ordering.

Use :class:`GraphQuery` (or :func:`find_graph`) to traverse a graph:

* every walk that reaches a value is reported, so several paths to the same
  object produce several matches;
* when a reference loops back to a container already visited on the current
  walk, a :class:`GraphCycle` is reported at the reference's location and the
  walk does not continue around the loop.

Use :func:`correct_graph` to produce corrected copies of selected subgraphs
("mounts").  Each mount is copied independently, so corrections scoped to one
mount never affect another mount or the original input.  Shared objects that
do not need to change stay shared, across mounts and between the copies and
the original graph.
"""

from __future__ import annotations

import typing

from jsonpath_ng.exceptions import JSONPathError
from jsonpath_ng.jsonpath import AutoIdForDatum
from jsonpath_ng.jsonpath import Child
from jsonpath_ng.jsonpath import DatumInContext
from jsonpath_ng.jsonpath import Descendants
from jsonpath_ng.jsonpath import Fields
from jsonpath_ng.jsonpath import Index
from jsonpath_ng.jsonpath import JSONPath
from jsonpath_ng.jsonpath import Parent
from jsonpath_ng.jsonpath import Root
from jsonpath_ng.jsonpath import This
from jsonpath_ng.jsonpath import Where
from jsonpath_ng.jsonpath import WhereNot
from jsonpath_ng.jsonpath import _Union


class Edge(typing.NamedTuple):
    """
    One reference in an object graph.

    The root of a query is represented by an edge whose parent and selector
    are ``None``.
    """

    parent: typing.Any
    selector: str | int | None
    child: typing.Any


class GraphCycle:
    """
    A reference that loops back to a container on the current walk.

    Cycles are reported, never expanded, and they cannot be correction
    targets.

    Attributes:
        link_path: The full JSONPath of the reference that closes the loop,
            such as ``jobs.east.review.links[0]``.
        target_path: The full JSONPath of the earlier container that the
            reference points back to, such as ``jobs.east``.
    """

    def __init__(self, link_path: JSONPath, target_path: JSONPath) -> None:
        self.link_path = link_path
        self.target_path = target_path

    @property
    def link(self) -> str:
        """The rendered path of the reference that closes the loop."""

        return str(self.link_path)

    @property
    def target(self) -> str:
        """The rendered path of the container pointed back to."""

        return str(self.target_path)

    def __repr__(self) -> str:
        return (
            f"GraphCycle(link_path={self.link_path!r}, "
            f"target_path={self.target_path!r})"
        )

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, GraphCycle)
            and other.link_path == self.link_path
            and other.target_path == self.target_path
        )


class GraphMatch:
    """
    A single value reached while traversing an object graph.

    Because the same object may be reachable through several distinct walks,
    every match records the chain of references followed to reach it.

    Attributes:
        value: The matched value.
        path: The full JSONPath of this occurrence.
        chain: The :class:`Edge` objects walked from the query root to this
            value, in order.  Two matches with the same chain describe the
            same occurrence.
        datum: The underlying :class:`DatumInContext`, whose context chain
            reaches the parent of the value.
        cycle: A :class:`GraphCycle` when this occurrence is a back reference
            rather than an ordinary value match.  The walk stops at the back
            reference.
    """

    def __init__(
        self,
        datum: DatumInContext,
        chain: tuple[Edge, ...],
        cycle: GraphCycle | None = None,
    ) -> None:
        self.value = datum.value
        self.path = datum.full_path
        self.chain = chain
        self.datum = datum
        self.cycle = cycle

    @property
    def is_cycle(self) -> bool:
        """Whether this match reports a back reference."""

        return self.cycle is not None

    def __repr__(self) -> str:
        return (
            f"GraphMatch(value={self.value!r}, path={self.path!r}, "
            f"cycle={self.cycle!r})"
        )

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, GraphMatch)
            and other.value is self.value
            and other.path == self.path
            and _same_chain(other.chain, self.chain)
            and other.cycle == self.cycle
        )


class CorrectionConflict(JSONPathError):
    """
    Raised when one correction mount receives inconsistent new values.

    A mount must settle on a single new value for each field it owns.  The
    whole correction is rejected; no partial results are produced.
    """

    def __init__(
        self,
        mount_path: str,
        field_path: str,
        conflicting_paths: typing.Sequence[str],
        values: typing.Sequence[typing.Any],
    ) -> None:
        self.mount_path = mount_path
        self.field_path = field_path
        self.conflicting_paths = list(conflicting_paths)
        self.values = list(values)
        super().__init__(
            f"Conflicting corrections for {field_path!r} within mount "
            f"{mount_path!r}: paths {self.conflicting_paths!r} request "
            f"values {self.values!r}"
        )


class GraphQuery:
    """
    An opt-in object-graph query bound to a JSONPath expression.

    Construction never touches data, so one query can be run against several
    graphs.
    """

    def __init__(self, expression: JSONPath | str) -> None:
        if isinstance(expression, str):
            from jsonpath_ng.parser import parse

            expression = parse(expression)
        self.expression = expression

    def find(self, data: typing.Any) -> list[GraphMatch]:
        """Return every occurrence of the expression in the graph ``data``."""

        return find_graph(self.expression, data)

    def __repr__(self) -> str:
        return f"GraphQuery({self.expression!r})"


class _Step(typing.NamedTuple):
    """One internal evaluation step."""

    datum: DatumInContext
    chain: tuple[Edge, ...]
    cycle: GraphCycle | None


def _same_chain(
    left: tuple[Edge, ...], right: tuple[Edge, ...]
) -> bool:
    """Identity-based chain comparison (never deep value equality)."""

    if len(left) != len(right):
        return False
    return all(
        left_edge.parent is right_edge.parent
        and left_edge.selector == right_edge.selector
        and left_edge.child is right_edge.child
        for left_edge, right_edge in zip(left, right)
    )


def _chain_is_prefix(
    prefix: tuple[Edge, ...], chain: tuple[Edge, ...]
) -> bool:
    """Whether ``prefix`` reaches a value on the walk described by ``chain``."""

    if len(prefix) > len(chain):
        return False
    return _same_chain(prefix, chain[: len(prefix)])


def _chain_container_ids(chain: tuple[Edge, ...]) -> set[int]:
    return {
        id(edge.child)
        for edge in chain
        if isinstance(edge.child, (dict, list))
    }


def _selector(path: JSONPath) -> str | int | None:
    if isinstance(path, Index) and len(path.indices) == 1:
        return path.indices[0]
    if isinstance(path, Fields) and len(path.fields) == 1:
        return path.fields[0]
    return None


def _ancestor_datum(datum: DatumInContext, value: typing.Any) -> DatumInContext:
    """Find the datum on the context chain whose value is ``value``."""

    current: DatumInContext | None = datum
    while current is not None:
        if current.value is value:
            return current
        current = current.context
    raise AssertionError("Cycle target was not found on the context chain")


def _classify_children(
    node: JSONPath,
    datum: DatumInContext,
    chain: tuple[Edge, ...],
) -> list[_Step]:
    """
    Apply a single-step selector and classify its results.

    Ordinary children extend the walk.  A container that is already on the
    walk becomes a cycle step and is never expanded.  Scalars are never
    treated as cycles, even when Python interns their values.
    """

    seen_container_ids = _chain_container_ids(chain)
    steps: list[_Step] = []
    for child in node.find(datum):
        if isinstance(child, AutoIdForDatum):
            continue
        parent = child.context.value if child.context is not None else None
        child_chain = chain + (
            Edge(parent, _selector(child.path), child.value),
        )
        if (
            isinstance(child.value, (dict, list))
            and id(child.value) in seen_container_ids
        ):
            target = _ancestor_datum(datum, child.value)
            steps.append(
                _Step(
                    child,
                    child_chain,
                    GraphCycle(child.full_path, target.full_path),
                )
            )
        else:
            steps.append(_Step(child, child_chain, None))
    return steps


def _evaluate(
    node: JSONPath,
    datum: DatumInContext,
    chain: tuple[Edge, ...],
) -> list[_Step]:
    """Graph-aware evaluation of one AST node on one datum."""

    if isinstance(node, Root):
        if datum.context is None:
            return [_Step(datum, chain, None)]
        return _evaluate(node, datum.context, chain[:-1])

    if isinstance(node, This):
        return [_Step(datum, chain, None)]

    if isinstance(node, Parent):
        if datum.context is None:
            return []
        return [_Step(datum.context, chain[:-1], None)]

    if isinstance(node, Child):
        steps: list[_Step] = []
        for left_step in _evaluate(node.left, datum, chain):
            if left_step.cycle is not None:
                steps.append(left_step)
            else:
                steps.extend(
                    _evaluate(node.right, left_step.datum, left_step.chain)
                )
        return steps

    if isinstance(node, Descendants):
        return _evaluate_descendants(node, datum, chain)

    if isinstance(node, (Where, WhereNot)):
        return _evaluate_where(node, datum, chain)

    if isinstance(node, _Union):
        return _evaluate(node.left, datum, chain) + _evaluate(
            node.right, datum, chain
        )

    # Fields, Index, Wildcard, Slice and any node without dedicated graph
    # semantics are single-step selectors.  Extended-parser nodes such as
    # filters keep their tree semantics here; their outputs are still
    # classified so back references are detected below them.
    return _classify_children(node, datum, chain)


def _evaluate_descendants(
    node: Descendants,
    datum: DatumInContext,
    chain: tuple[Edge, ...],
) -> list[_Step]:
    matches: list[_Step] = []
    stack: list[_Step] = []
    for left_step in _evaluate(node.left, datum, chain):
        if left_step.cycle is not None:
            matches.append(left_step)
        else:
            stack.append(left_step)

    while stack:
        current = stack.pop()

        matches.extend(_evaluate(node.right, current.datum, current.chain))

        value = current.datum.value
        if not isinstance(value, (dict, list)):
            continue

        child_steps: list[_Step] = []
        if isinstance(value, list):
            for index, child in enumerate(value):
                child_datum = DatumInContext(
                    child, path=Index(index), context=current.datum
                )
                child_steps.extend(
                    _classify_children(This(), child_datum, current.chain)
                )
        else:
            for field, child in value.items():
                child_datum = DatumInContext(
                    child,
                    path=Fields._compliant_constructor(field),
                    context=current.datum,
                )
                child_steps.extend(
                    _classify_children(This(), child_datum, current.chain)
                )

        # Back edges reached while descending are intermediate wildcard
        # steps, not expression results: do not report them here and do not
        # walk around the loop.  Cycles are reported when the expression's
        # own selector reaches a back edge, via ``_evaluate(node.right)``.
        child_steps = [step for step in child_steps if step.cycle is None]

        # Push in reverse so the natural dict/list order is walked first.
        stack.extend(reversed(child_steps))

    return matches


def _evaluate_where(
    node: Where | WhereNot,
    datum: DatumInContext,
    chain: tuple[Edge, ...],
) -> list[_Step]:
    # ``WhereNot`` subclasses ``Where``, so compare exact types.
    keep_on_match = type(node) is Where
    results: list[_Step] = []
    for left_step in _evaluate(node.left, datum, chain):
        if left_step.cycle is not None:
            results.append(left_step)
            continue
        has_match = bool(
            _evaluate(node.right, left_step.datum, left_step.chain)
        )
        if has_match is keep_on_match:
            results.append(left_step)
    return results


def find_graph(
    expression: JSONPath | str, data: typing.Any
) -> list[GraphMatch]:
    """
    Query an object graph with ``expression``.

    Every reachable occurrence is returned, including several walks to the
    same object.  Back references are returned as cycle matches and are not
    expanded.  On plain tree data the values, paths, and order match
    :meth:`JSONPath.find`; only the wrapper type differs.
    """

    if isinstance(expression, str):
        from jsonpath_ng.parser import parse

        expression = parse(expression)

    root = DatumInContext(data, path=Root(), context=None)
    chain = (Edge(None, None, data),)
    return [
        GraphMatch(step.datum, step.chain, step.cycle)
        for step in _evaluate(expression, root, chain)
    ]


class Correction(typing.NamedTuple):
    """One corrected mount produced by :func:`correct_graph`."""

    mount: GraphMatch
    value: typing.Any


# A field overwrite addressed by its actual storage location.
_FieldKey = tuple[int, str | int]


class _MountPlan:
    """
    The validated correction of one mounted subgraph.

    Overwrites are keyed by the parent object's identity and its selector, so
    several paths to the same field necessarily settle on one value.  Nothing
    is copied until every mount has been validated.
    """

    def __init__(self, mount: GraphMatch) -> None:
        self.mount = mount
        self.fields: dict[_FieldKey, tuple[str, typing.Any]] = {}

    def assign(self, target: GraphMatch, new_value: typing.Any) -> None:
        if not target.chain:
            raise JSONPathError(
                f"Correction target {target.path!r} is not a field of a container"
            )
        leaf_edge = target.chain[-1]
        if leaf_edge.parent is None or leaf_edge.selector is None:
            raise JSONPathError(
                f"Correction target {target.path!r} is not a field of a container"
            )

        key = (id(leaf_edge.parent), leaf_edge.selector)
        existing = self.fields.get(key)
        if existing is not None and existing[1] != new_value:
            raise CorrectionConflict(
                str(self.mount.path),
                str(target.path),
                [existing[0], str(target.path)],
                [existing[1], new_value],
            )
        self.fields[key] = (str(target.path), new_value)

    def build(self) -> typing.Any:
        dirty_parent_ids = {parent_id for parent_id, _ in self.fields}
        recreate_ids = _recreate_closure(self.mount.value, dirty_parent_ids)
        return _copy_mount(self.mount.value, recreate_ids, self.fields, {})


def _recreate_closure(
    root: typing.Any, dirty_ids: set[int]
) -> set[int]:
    """
    Every container from which an overwritten field is reachable.

    A container directly holding an overwritten field is dirty.  Every
    container that can reach one must itself be recreated, including
    containers that only reach the dirty subgraph through a back edge.
    Reachability is solved to a fixed point so loops do not hide a container
    that leads back into a recreated one.
    """

    # Collect every reachable container and its container children,
    # stopping at back edges during this discovery pass.
    children_by_id: dict[int, list[int]] = {}
    stack: list[typing.Any] = [root]
    visited: set[int] = set()
    while stack:
        value = stack.pop()
        if not isinstance(value, (dict, list)) or id(value) in visited:
            continue
        visited.add(id(value))
        child_ids: list[int] = []
        children = value if isinstance(value, list) else value.values()
        for child in children:
            if isinstance(child, (dict, list)):
                # Record back edges too; they still make this container a
                # parent of a possibly-recreated container.
                child_ids.append(id(child))
                if id(child) not in visited:
                    stack.append(child)
        children_by_id[id(value)] = child_ids

    recreate = {value_id for value_id in visited if value_id in dirty_ids}
    while True:
        added: set[int] = set()
        for value_id, child_ids in children_by_id.items():
            if value_id in recreate:
                continue
            if any(child_id in recreate for child_id in child_ids):
                added.add(value_id)
        if not added:
            return recreate
        recreate.update(added)


def _copy_mount(
    value: typing.Any,
    recreate_ids: set[int],
    fields: dict[_FieldKey, tuple[str, typing.Any]],
    copies: dict[int, typing.Any],
) -> typing.Any:
    """
    Recreate the mounted subgraph with the planned overwrites applied.

    Containers leading to an overwritten field are recreated once, and a back
    reference onto the walk returns the new copy so internal loops are
    rewired onto the corrected branch.  Every other value is returned as-is
    and stays shared with the original graph and the other mounts.
    """

    if not isinstance(value, (dict, list)):
        return value

    if id(value) in copies:
        return copies[id(value)]

    if id(value) not in recreate_ids:
        return value

    if isinstance(value, list):
        new_container: list[typing.Any] = []
        copies[id(value)] = new_container
        for index, child in enumerate(value):
            overwrite = fields.get((id(value), index))
            if overwrite is not None:
                new_container.append(overwrite[1])
            else:
                new_container.append(
                    _copy_mount(child, recreate_ids, fields, copies)
                )
        return new_container

    new_map: dict[typing.Any, typing.Any] = {}
    copies[id(value)] = new_map
    for field, child in value.items():
        overwrite = fields.get((id(value), field))
        if overwrite is not None:
            new_map[field] = overwrite[1]
        else:
            new_map[field] = _copy_mount(child, recreate_ids, fields, copies)
    return new_map


def correct_graph(
    data: typing.Any,
    corrections: typing.Mapping[str | GraphMatch, typing.Any]
    | typing.Iterable[tuple[GraphMatch | str, typing.Any]],
    *,
    mounts: GraphQuery | JSONPath | str | typing.Sequence[GraphMatch | str],
    query: GraphQuery | JSONPath | str | None = None,
) -> list[Correction]:
    """
    Produce corrected copies of mounted subgraphs.

    Arguments:
        data: The object graph.  It is never modified.
        corrections: Either a mapping of targets to new values, or an iterable
            of ``(target, value)`` pairs.  A target is a :class:`GraphMatch`
            produced by ``query`` or the rendered string of its full path.
        mounts: The task mounts that bound the corrections.  Give a graph
            query (or JSONPath expression/string), or a sequence of mount
            matches or full paths.  Every target must lie within exactly one
            mount, and a correction never reaches past its mount.
        query: The graph query that produced the correction targets.  It
            defaults to a recursive search over the whole graph and is used
            to validate string targets and reject cycle references.

    Returns:
        One :class:`Correction` per mount that receives a correction, in the
        order of the selected mounts.

    Raises:
        CorrectionConflict: Two paths within one mount request different new
            values for the same field.  No partial results are produced.
        JSONPathError: A target or mount is unknown, a target is a cycle, or
            a target is outside every mount.
    """

    if query is None:
        query = GraphQuery("$..*")
    elif not isinstance(query, GraphQuery):
        query = GraphQuery(query)
    matches = query.find(data)
    match_by_path = {str(match.path): match for match in matches}

    if isinstance(mounts, GraphQuery):
        mount_matches = [
            match for match in mounts.find(data) if not match.is_cycle
        ]
    elif isinstance(mounts, (JSONPath, str)):
        mount_matches = [
            match
            for match in GraphQuery(mounts).find(data)
            if not match.is_cycle
        ]
    else:
        mount_matches = _resolve_mounts(list(mounts), data)

    specs = _normalise_corrections(corrections, match_by_path)
    if not specs:
        return []

    plans = _plan_mounts(mount_matches, specs)
    return [Correction(plan.mount, plan.build()) for plan in plans]


def _resolve_mounts(
    mounts: typing.Sequence[GraphMatch | str], data: typing.Any
) -> list[GraphMatch]:
    all_matches = find_graph("$..*", data)
    resolved: list[GraphMatch] = []
    for mount in mounts:
        if isinstance(mount, GraphMatch):
            resolved.append(mount)
            continue
        found = [
            match
            for match in all_matches
            if str(match.path) == mount and not match.is_cycle
        ]
        if not found:
            raise JSONPathError(
                f"Mount {mount!r} did not match the object graph"
            )
        resolved.append(found[0])
    return resolved


def _normalise_corrections(
    corrections: typing.Mapping[str | GraphMatch, typing.Any]
    | typing.Iterable[tuple[GraphMatch | str, typing.Any]],
    match_by_path: dict[str, GraphMatch],
) -> list[tuple[GraphMatch, typing.Any]]:
    if isinstance(corrections, typing.Mapping):
        pairs: typing.Iterable[tuple[GraphMatch | str, typing.Any]] = (
            corrections.items()
        )
    else:
        pairs = corrections

    specs: list[tuple[GraphMatch, typing.Any]] = []
    for target, new_value in pairs:
        if isinstance(target, GraphMatch):
            match: GraphMatch = target
            if str(match.path) not in match_by_path:
                raise JSONPathError(
                    f"Correction target {match.path!r} was not produced by "
                    "the graph query"
                )
        else:
            found = match_by_path.get(target)
            if found is None:
                raise JSONPathError(
                    f"Correction target {target!r} did not match the graph query"
                )
            match = found
        if match.is_cycle:
            raise JSONPathError(
                f"Correction target {match.path!r} is a cycle reference and "
                "cannot be corrected"
            )
        specs.append((match, new_value))
    return specs


def _plan_mounts(
    mount_matches: list[GraphMatch],
    specs: list[tuple[GraphMatch, typing.Any]],
) -> list[_MountPlan]:
    plans: list[_MountPlan | None] = [None] * len(mount_matches)
    for target, new_value in specs:
        mount_index = _find_mount_index(mount_matches, target)
        plan = plans[mount_index]
        if plan is None:
            plan = _MountPlan(mount_matches[mount_index])
            plans[mount_index] = plan
        plan.assign(target, new_value)
    return [plan for plan in plans if plan is not None]


def _find_mount_index(
    mount_matches: list[GraphMatch], target: GraphMatch
) -> int:
    for index, mount in enumerate(mount_matches):
        # The target must be strictly inside the mount; the mount root
        # itself belongs to its parent and cannot be overwritten here.
        if len(target.chain) > len(mount.chain) and _chain_is_prefix(
            mount.chain, target.chain
        ):
            return index
    raise JSONPathError(
        f"Correction target {target.path!r} is outside every selected mount"
    )
