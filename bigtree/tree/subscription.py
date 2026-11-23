"""Path subscriptions for trees.

A :class:`PathSubscription` resolves one or more wildcard path rules against a tree, keeps the last observed
result for every rule, and reports the pages that are genuinely new or genuinely gone. Results are matched by a
stable page identity (a node attribute such as a page id) rather than by path or node name, so a page that is
moved by a directory reorganisation is not reported as an exit plus an entry.

Rules are resolved relative to an anchor node, which keeps them valid when the anchor itself is moved. A rule can
also declare excluded subtrees (for example, a whole province-level directory) so those pages are never reported.

A batch of directory changes should be committed as one unit. Use :meth:`PathSubscription.batch` (or
:meth:`PathSubscription.begin_batch` / :meth:`PathSubscription.end_batch`) so paths that only exist while the
batch is in progress are never reported. A batch that raises leaves the previous snapshot untouched.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, TypeVar

from bigtree.node import basenode, node
from bigtree.tree import search
from bigtree.utils import exceptions

__all__ = [
    "path_subscription",
    "PathRule",
    "PathSubscriptionChange",
    "PathSubscriptionBatch",
    "PathSubscriptionTransaction",
    "PathSubscription",
]

NodeT = TypeVar("NodeT", bound=node.Node)


def path_subscription(
    tree: NodeT,
    rules: Iterable[PathRule] | None = None,
    id_attr: str = "page_id",
    identity: str = "attr",
) -> PathSubscription:
    """Create a subscription for a tree.

    Args:
        tree: root of the tree to observe, accepts a node or a `Tree`
        rules: rules to register immediately
        id_attr: default node attribute holding the stable page identity
        identity: how to identify a page, ``"attr"`` uses `id_attr`, ``"path"`` uses the node path name,
            ``"node_name"`` uses the node name

    Returns:
        Subscription
    """
    return PathSubscription(
        tree,
        rules=rules,
        id_attr=id_attr,
        identity=identity,
    )


def _contains(values: Iterable[Any], value: Any) -> bool:
    """Membership check that also works for unhashable identities.

    Args:
        values: values to search
        value: value to look for

    Returns:
        Indicator whether `value` is present in `values`
    """
    return any(item == value for item in values)


def _difference(values: Iterable[Any], other: Iterable[Any]) -> tuple[Any, ...]:
    """Order-preserving difference, supporting unhashable identities.

    Args:
        values: values to filter
        other: values that must not be present in the result

    Returns:
        Values from `values` that are not in `other`
    """
    other = tuple(other)
    return tuple(value for value in values if not _contains(other, value))


class PathRule:
    """A wildcard path rule anchored at a node.

    The rule is resolved by :func:`bigtree.tree.search.find_relative_paths` starting from `anchor`, so moving the
    anchor keeps the rule pointing at the same place in the tree. Any node inside an excluded subtree is skipped,
    including the excluded subtree root itself.

    Examples:
        >>> from bigtree import Node, PathRule
        >>> root = Node("catalog")
        >>> regions = Node("regions", parent=root)
        >>> north = Node("north", parent=regions)
        >>> rule = PathRule("north-siblings", "../*/pages/*", anchor=north, exclude="../south")
        >>> rule.path_name
        '../*/pages/*'
    """

    def __init__(
        self,
        name: str,
        path_name: str,
        anchor: NodeT | None = None,
        exclude: (
            str | basenode.BaseNode | Iterable[str | basenode.BaseNode] | None
        ) = None,
        id_attr: str | None = None,
    ):
        """Initialize a path rule.

        Args:
            name: rule name, used to key subscription results
            path_name: wildcard path, relative to `anchor`, e.g. ``../*/pages/*``
            anchor: node the path is resolved from, defaults to the tree root
            exclude: subtree path(s) relative to `anchor`, or subtree node(s), whose nodes must not be reported
            id_attr: node attribute holding the stable page identity, overrides the subscription default
        """
        if not name:
            raise ValueError("`name` must be a non-empty string")
        if not path_name:
            raise ValueError("`path_name` must be a non-empty string")
        if isinstance(exclude, (str, basenode.BaseNode)):
            exclude = [exclude]
        self.name = name
        self.path_name = path_name
        self.anchor = anchor
        self.exclude = tuple(exclude or ())
        self.id_attr = id_attr

    @property
    def key(self) -> tuple[Any, ...]:
        """Identity of the rule, used to detect re-registration."""
        return (self.name, self.path_name, self.anchor, self.exclude, self.id_attr)

    def resolve(self, root: NodeT) -> tuple[NodeT, ...]:
        """Resolve the rule against `root` and return the matching nodes in tree order.

        Args:
            root: tree root used when the rule has no explicit anchor

        Returns:
            Matching nodes
        """
        anchor = self.anchor if self.anchor is not None else root
        try:
            nodes = search.find_relative_paths(anchor, self.path_name)
        except (exceptions.SearchError, ValueError):
            # A fixed path component disappeared (or the anchor detached); nothing matches this round.
            return ()
        excluded = self._excluded_nodes(anchor)
        if not excluded:
            return nodes
        return tuple(
            _node
            for _node in nodes
            if not any(
                _node is excluded_node or _node in excluded_node.descendants
                for excluded_node in excluded
            )
        )

    def _excluded_nodes(self, anchor: NodeT) -> tuple[NodeT, ...]:
        """Resolve excluded subtree roots, ignoring paths that no longer exist.

        Args:
            anchor: node the exclusion paths are resolved from

        Returns:
            Excluded subtree roots
        """
        excluded_nodes: list[NodeT] = []
        for exclusion in self.exclude:
            if isinstance(exclusion, basenode.BaseNode):
                excluded_nodes.append(exclusion)  # type: ignore[arg-type]
                continue
            try:
                matches = search.find_relative_paths(anchor, exclusion)
            except (exceptions.SearchError, ValueError):
                continue
            excluded_nodes.extend(matches)
        return tuple(excluded_nodes)

    def __repr__(self) -> str:
        return (
            f"PathRule(name={self.name!r}, path_name={self.path_name!r}, "
            f"exclude={self.exclude!r}, id_attr={self.id_attr!r})"
        )


class PathSubscriptionChange:
    """Changes observed for one rule between two snapshots."""

    def __init__(
        self,
        name: str,
        added: tuple[Any, ...],
        exited: tuple[Any, ...],
        current: tuple[Any, ...],
        snapshot: tuple[Any, ...] | None = None,
    ):
        """Initialize the change record.

        Args:
            name: rule name
            added: page identities that entered the rule since the previous snapshot
            exited: page identities that left the rule since the previous snapshot
            current: page identities in the rule after this snapshot
            snapshot: page identities before this snapshot, ``None`` if the rule was not seen before
        """
        self.name = name
        self.added = added
        self.exited = exited
        self.current = current
        self.snapshot = snapshot

    @property
    def entered(self) -> tuple[Any, ...]:
        """Alias for :attr:`added`."""
        return self.added

    @property
    def removed(self) -> tuple[Any, ...]:
        """Alias for :attr:`exited`."""
        return self.exited

    @property
    def changed(self) -> bool:
        """Indicator whether this rule reported added or exited pages."""
        return bool(self.added or self.exited)

    @property
    def retained(self) -> tuple[Any, ...]:
        """Existing result carried forward by an unaffected rule."""
        return () if self.changed else self.current

    @property
    def unaffected(self) -> bool:
        """Indicator whether the rule kept its previous result without changes."""
        return not self.changed

    def __repr__(self) -> str:
        return (
            f"PathSubscriptionChange(name={self.name!r}, added={self.added!r}, "
            f"exited={self.exited!r}, current={self.current!r})"
        )


class PathSubscriptionBatch:
    """Result of a single :meth:`PathSubscription.commit` call."""

    def __init__(self, changes: Mapping[str, PathSubscriptionChange]):
        """Initialize the batch result.

        Args:
            changes: rule name to change record
        """
        self.changes = dict(changes)

    def __getitem__(self, name: str) -> PathSubscriptionChange:
        return self.changes[name]

    def __iter__(self) -> Any:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    def __contains__(self, name: object) -> bool:
        return name in self.changes

    def added(self, name: str) -> tuple[Any, ...]:
        """Pages added for a rule.

        Args:
            name: rule name

        Returns:
            Added page identities
        """
        return self.changes[name].added

    def exited(self, name: str) -> tuple[Any, ...]:
        """Pages that exited a rule.

        Args:
            name: rule name

        Returns:
            Exited page identities
        """
        return self.changes[name].exited

    def removed(self, name: str) -> tuple[Any, ...]:
        """Alias for :meth:`exited`.

        Args:
            name: rule name

        Returns:
            Exited page identities
        """
        return self.changes[name].exited

    @property
    def has_changes(self) -> bool:
        """Indicator whether any rule reported added or exited pages."""
        return any(change.changed for change in self.changes.values())

    def __repr__(self) -> str:
        return f"PathSubscriptionBatch(changes={self.changes!r})"


class PathSubscriptionTransaction:
    """A batch of directory changes committed as one unit.

    While a transaction is open, the baseline is the snapshot from before the batch. Intermediate states are never
    stored, so a path that is only passed through while the batch is in progress cannot trigger a page event.
    Leaving the context manager commits the batch; if the context body raises, the previous snapshot is restored.

    Examples:
        >>> from bigtree import Node, PathRule, PathSubscription
        >>> root = Node("catalog")
        >>> regions = Node("regions", parent=root)
        >>> north = Node("north", parent=regions)
        >>> pages = Node("pages", parent=north)
        >>> _ = Node("list", parent=pages, page_id="N-1")
        >>> sub = PathSubscription(root)
        >>> _ = sub.add_rule(PathRule("north", "../*/pages/*", anchor=north))
        >>> _ = sub.snapshot()
        >>> with sub.batch() as transaction:
        ...     _ = Node("detail", parent=pages, page_id="N-2")
        >>> transaction.result.added("north")
        ('N-2',)
    """

    def __init__(self, subscription: PathSubscription):
        """Initialize the transaction.

        Args:
            subscription: subscription this transaction belongs to
        """
        self._subscription = subscription
        self._baseline: dict[str, tuple[Any, ...]] | None = None
        self._result: PathSubscriptionBatch | None = None
        self._affected: list[str] = []

    @property
    def result(self) -> PathSubscriptionBatch:
        """Batch result, available after :meth:`commit` or after exiting the context."""
        if self._result is None:
            raise RuntimeError("Transaction has not been committed")
        return self._result

    def __enter__(self) -> PathSubscriptionTransaction:
        if self._baseline is not None:
            raise RuntimeError("Transaction is already open")
        self._baseline = dict(self._subscription._snapshots)
        self._subscription._transaction = self
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        try:
            if exc_type is not None:
                self._baseline = None
                return False
            if self._result is None:
                self.commit()
            return False
        finally:
            if self._subscription._transaction is self:
                self._subscription._transaction = None

    def mark_affected(self, *names: str) -> None:
        """Mark rules as affected by this batch.

        Args:
            names: rule names affected by the directory changes
        """
        self._affected.extend(names)

    def commit(self, affected: Iterable[str] | None = None) -> PathSubscriptionBatch:
        """Commit the final state of the batch.

        Calling this more than once returns the result of the first commit.

        Args:
            affected: rule names affected by the batch; other rules carry their previous result forward.
                Defaults to the rules marked with :meth:`mark_affected`, or all rules when none are marked.

        Returns:
            Batch of per-rule changes
        """
        if self._result is not None:
            return self._result
        if self._baseline is None:
            raise RuntimeError("Transaction is not open")
        affected_names = (
            list(affected) if affected is not None else list(self._affected)
        )
        if not affected_names:
            affected_names = None
        self._result = self._subscription._diff(
            rules=self._subscription._rules,
            changes=None,
            affected=affected_names,
            baseline=self._baseline,
        )
        self._subscription._snapshots = {
            name: change.current for name, change in self._result.changes.items()
        }
        self._baseline = None
        return self._result


class PathSubscription:
    """Track wildcard path rules against a tree and report added and exited pages.

    The subscription resolves each :class:`PathRule` on demand. Between snapshots it remembers the page identities
    each rule produced. A page is reported as exited only when its identity is genuinely absent afterwards, so
    moving a node or shifting a sibling subtree into place does not produce spurious exits or entries.

    Examples:
        >>> from bigtree import Node, PathRule, PathSubscription
        >>> root = Node("catalog")
        >>> regions = Node("regions", parent=root)
        >>> north = Node("north", parent=regions)
        >>> pages = Node("pages", parent=north)
        >>> _ = Node("list", parent=pages, page_id="N-1")
        >>> _ = Node("detail", parent=pages, page_id="N-2")
        >>> sub = PathSubscription(root)
        >>> _ = sub.add_rule(PathRule("north", "../*/pages/*", anchor=north))
        >>> batch = sub.snapshot()
        >>> batch["north"].current
        ('N-1', 'N-2')
    """

    def __init__(
        self,
        tree: NodeT,
        rules: Iterable[PathRule] | None = None,
        id_attr: str = "page_id",
        identity: str = "attr",
    ):
        """Initialize the subscription.

        Args:
            tree: root of the tree to observe, accepts a node or a `Tree`
            rules: rules to register immediately
            id_attr: default node attribute holding the stable page identity
            identity: how to identify a page, ``"attr"`` uses `id_attr`, ``"path"`` uses the node path name,
                ``"node_name"`` uses the node name
        """
        if identity not in ("attr", "path", "node_name"):
            raise ValueError("`identity` must be one of 'attr', 'path', or 'node_name'")
        if not isinstance(tree, basenode.BaseNode) and hasattr(tree, "node"):
            tree = tree.node  # type: ignore[assignment]
        if not isinstance(tree, basenode.BaseNode):
            raise TypeError(
                "Tree should be of type `BaseNode`, or a `Tree` wrapping a `BaseNode`"
            )
        self._root = tree
        self._id_attr = id_attr
        self._identity = identity
        self._rules: dict[str, PathRule] = {}
        self._snapshots: dict[str, tuple[Any, ...]] = {}
        self._transaction: PathSubscriptionTransaction | None = None
        for rule in rules or ():
            self.add_rule(rule)

    @property
    def rules(self) -> dict[str, PathRule]:
        """Registered rules, keyed by rule name."""
        return dict(self._rules)

    @property
    def root(self) -> NodeT:
        """Root of the observed tree."""
        return self._root

    def add_rule(self, rule: PathRule) -> PathRule:
        """Register a rule.

        Registering a rule under an existing name with different settings replaces it and clears the stored
        snapshot for that name, so the next snapshot reports the rule's full result as added rather than diffing
        against an unrelated rule.

        Args:
            rule: rule to register

        Returns:
            Registered rule
        """
        existing = self._rules.get(rule.name)
        if existing is None or existing.key != rule.key:
            self._snapshots.pop(rule.name, None)
        self._rules[rule.name] = rule
        return rule

    def subscribe(
        self,
        name: str,
        path_name: str,
        anchor: NodeT | None = None,
        exclude: (
            str | basenode.BaseNode | Iterable[str | basenode.BaseNode] | None
        ) = None,
        id_attr: str | None = None,
    ) -> PathRule:
        """Create and register a rule.

        Args:
            name: rule name
            path_name: wildcard path, relative to `anchor`
            anchor: node the path is resolved from, defaults to the tree root
            exclude: subtree path(s) relative to `anchor`, or subtree node(s), whose nodes must not be reported
            id_attr: node attribute holding the stable page identity

        Returns:
            Registered rule
        """
        rule = PathRule(
            name,
            path_name,
            anchor=anchor,
            exclude=exclude,
            id_attr=id_attr,
        )
        return self.add_rule(rule)

    def remove_rule(self, name: str) -> None:
        """Remove a rule and its stored snapshot.

        Args:
            name: rule name
        """
        self._rules.pop(name, None)
        self._snapshots.pop(name, None)

    def clear(self) -> None:
        """Forget all stored snapshots, keeping registered rules."""
        self._snapshots.clear()

    def resolve(self, name: str) -> tuple[NodeT, ...]:
        """Resolve one rule to its matching nodes.

        Args:
            name: rule name

        Returns:
            Matching nodes
        """
        return self._rules[name].resolve(self._root)

    def identities(self, name: str) -> tuple[Any, ...]:
        """Resolve one rule to the page identities of its matching nodes.

        Args:
            name: rule name

        Returns:
            Page identities
        """
        rule = self._rules[name]
        return self._identities(rule, rule.resolve(self._root))

    def snapshot(self) -> PathSubscriptionBatch:
        """Resolve every rule, store the result, and report changes since the last snapshot.

        This is a convenience wrapper around :meth:`commit` for callers that do not need to compute several rules
        from one consistent view.

        Returns:
            Batch of per-rule changes
        """
        return self.commit()

    def begin_batch(self) -> PathSubscriptionTransaction:
        """Start a batch of directory changes.

        Returns:
            Open transaction
        """
        if self._transaction is not None:
            raise RuntimeError("A batch is already in progress")
        return PathSubscriptionTransaction(self).__enter__()

    def end_batch(self, affected: Iterable[str] | None = None) -> PathSubscriptionBatch:
        """Finish the current batch and return its changes.

        Args:
            affected: rule names affected by the batch; other rules carry their previous result forward

        Returns:
            Batch of per-rule changes
        """
        if self._transaction is None:
            raise RuntimeError("No batch is in progress")
        return self._transaction.commit(affected=affected)

    def batch(self) -> PathSubscriptionTransaction:
        """Open a batch of directory changes, usable as a context manager.

        Returns:
            Transaction
        """
        return PathSubscriptionTransaction(self)

    def commit(
        self,
        rules: Iterable[PathRule] | None = None,
        changes: Mapping[str, Iterable[NodeT]] | None = None,
        affected: Iterable[str] | None = None,
    ) -> PathSubscriptionBatch:
        """Resolve rules and report changes relative to the stored snapshot.

        The batch is computed before any snapshot is stored. If resolving a rule or reading an identity raises, the
        previous snapshots are left untouched and the error propagates.

        Args:
            rules: additional rules to register as part of this batch
            changes: optional per-rule node lists resolved by the caller; rules not present are resolved from the tree
            affected: rule names affected by the batch; other rules carry their previous result forward

        Returns:
            Batch of per-rule changes
        """
        if self._transaction is not None:
            self._transaction.mark_affected(*(affected or ()))
            for rule in rules or ():
                self.add_rule(rule)
            return self._transaction.commit()

        staged_rules = dict(self._rules)
        staged_baseline = dict(self._snapshots)
        for rule in rules or ():
            existing = staged_rules.get(rule.name)
            if existing is None or existing.key != rule.key:
                staged_baseline.pop(rule.name, None)
            staged_rules[rule.name] = rule
        affected_names = list(affected) if affected is not None else None
        result = self._diff(
            rules=staged_rules,
            changes=changes,
            affected=affected_names,
            baseline=staged_baseline,
        )
        self._rules = staged_rules
        self._snapshots = {
            name: change.current for name, change in result.changes.items()
        }
        return result

    def peek(
        self,
        rules: Iterable[PathRule] | None = None,
        changes: Mapping[str, Iterable[NodeT]] | None = None,
        affected: Iterable[str] | None = None,
    ) -> PathSubscriptionBatch:
        """Compute the batch that :meth:`commit` would produce without storing it.

        Args:
            rules: additional rules to consider
            changes: optional per-rule node lists resolved by the caller
            affected: rule names affected by the batch

        Returns:
            Batch of per-rule changes that would be committed
        """
        staged_rules = dict(self._rules)
        staged_baseline = dict(self._snapshots)
        for rule in rules or ():
            existing = staged_rules.get(rule.name)
            if existing is None or existing.key != rule.key:
                staged_baseline.pop(rule.name, None)
            staged_rules[rule.name] = rule
        affected_names = list(affected) if affected is not None else None
        return self._diff(
            rules=staged_rules,
            changes=changes,
            affected=affected_names,
            baseline=staged_baseline,
        )

    def _diff(
        self,
        rules: Mapping[str, PathRule],
        changes: Mapping[str, Iterable[NodeT]] | None,
        affected: Iterable[str] | None,
        baseline: Mapping[str, tuple[Any, ...]],
    ) -> PathSubscriptionBatch:
        """Compute changes for a staged set of rules.

        Args:
            rules: rules to evaluate
            changes: optional per-rule node lists resolved by the caller
            affected: rule names affected by the batch, ``None`` means all rules
            baseline: snapshot to diff against

        Returns:
            Batch of per-rule changes
        """
        affected_set = set(affected) if affected is not None else None
        change_map: dict[str, PathSubscriptionChange] = {}
        for name, rule in rules.items():
            previous = baseline.get(name)
            if (
                affected_set is not None
                and name not in affected_set
                and previous is not None
            ):
                change_map[name] = PathSubscriptionChange(
                    name, (), (), previous, snapshot=previous
                )
                continue
            if changes is not None and name in changes:
                nodes = tuple(changes[name])
            else:
                nodes = rule.resolve(self._root)
            current = self._identities(rule, nodes)
            added = _difference(current, previous or ())
            exited = _difference(previous or (), current)
            change_map[name] = PathSubscriptionChange(
                name, added, exited, current, snapshot=previous
            )
        return PathSubscriptionBatch(change_map)

    def _identities(self, rule: PathRule, nodes: Iterable[NodeT]) -> tuple[Any, ...]:
        """Map nodes to stable page identities, preserving order.

        Args:
            rule: rule that produced the nodes
            nodes: matching nodes

        Returns:
            Page identities
        """
        if self._identity == "path":
            return tuple(_node.path_name for _node in nodes)
        if self._identity == "node_name":
            return tuple(_node.node_name for _node in nodes)
        id_attr = rule.id_attr or self._id_attr
        identities = []
        for _node in nodes:
            identity = _node.get_attr(id_attr)
            if identity is None:
                raise ValueError(
                    f"Missing page identity attribute {id_attr!r} on node {_node.path_name}"
                )
            identities.append(identity)
        return tuple(identities)
