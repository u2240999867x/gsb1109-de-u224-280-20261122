"""Batch tree modification with snapshot reads and read-only validation.

A :class:`TreeBatch` resolves every path against the layout captured when the
batch was created.  Operations are then validated against a candidate layout
before any node is moved, so rejected batches never expose partially exchanged
subtrees or partial manifests to readers.

The module also provides :class:`TreeManifest`, an immutable layout captured at
a point in time.  A manifest keeps the sibling order that existed when it was
opened, while subsequent batches and direct node edits continue to work on the
live tree.
"""

from __future__ import annotations

import threading
import weakref
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Sequence,
)

from bigtree.node import basenode, binarynode, node
from bigtree.utils import common, exceptions

__all__ = [
    "BatchOperation",
    "LayoutNode",
    "LayoutView",
    "TreeBatch",
    "TreeManifest",
    "apply_batch",
    "batch",
    "clear_validators",
    "get_tree_lock",
    "move_subtree",
    "open_manifest",
    "register_validator",
    "swap_subtrees",
    "unregister_validator",
]

Validator = Callable[["LayoutView"], Any]

_TREE_LOCKS: "weakref.WeakKeyDictionary[Any, threading.RLock]" = (
    weakref.WeakKeyDictionary()
)
_TREE_VALIDATORS: "weakref.WeakKeyDictionary[Any, list[Validator]]" = (
    weakref.WeakKeyDictionary()
)
_REGISTRY_LOCK = threading.RLock()


def _root_of(tree: Any) -> basenode.BaseNode:
    """Return the root of a tree, unwrapping a ``Tree`` wrapper if needed."""
    if hasattr(tree, "node") and not isinstance(tree, basenode.BaseNode):
        tree = tree.node
    if not isinstance(tree, basenode.BaseNode):
        raise TypeError(f"Expected a BaseNode or Tree, received {type(tree).__name__}")
    root = tree
    seen: set[int] = set()
    while root.parent is not None:
        if id(root) in seen:  # pragma: no cover - corrupted tree guard
            raise exceptions.CorruptedTreeError("Cycle detected while finding root")
        seen.add(id(root))
        root = root.parent
    return root


def get_tree_lock(tree: Any) -> threading.RLock:
    """Get the re-entrant lock associated with a tree.

    All manifests and batch commits of the same tree share this lock.  Direct
    node edits remain unlocked for backwards compatibility.

    Args:
        tree: any node belonging to the tree

    Returns:
        Lock associated with the tree root
    """
    root = _root_of(tree)
    with _REGISTRY_LOCK:
        lock = _TREE_LOCKS.get(root)
        if lock is None:
            lock = threading.RLock()
            _TREE_LOCKS[root] = lock
        return lock


def register_validator(tree: Any, callback: Validator) -> Validator:
    """Register a read-only batch validator for a tree.

    The callback receives a :class:`LayoutView` describing the complete
    candidate layout.  It may return ``False`` (or raise) to reject the batch.

    Args:
        tree: any node belonging to the tree
        callback: validator accepting a candidate layout

    Returns:
        The registered callback
    """
    if not callable(callback):
        raise TypeError("Batch validator must be callable")
    root = _root_of(tree)
    with _REGISTRY_LOCK:
        validators = _TREE_VALIDATORS.setdefault(root, [])
        if all(existing is not callback for existing in validators):
            validators.append(callback)
    return callback


def unregister_validator(tree: Any, callback: Validator) -> None:
    """Remove a previously registered batch validator.

    Args:
        tree: any node belonging to the tree
        callback: validator to remove
    """
    root = _root_of(tree)
    with _REGISTRY_LOCK:
        validators = _TREE_VALIDATORS.get(root)
        if validators:
            validators[:] = [
                existing for existing in validators if existing is not callback
            ]


def clear_validators(tree: Any) -> None:
    """Remove every registered batch validator for a tree.

    Args:
        tree: any node belonging to the tree
    """
    root = _root_of(tree)
    with _REGISTRY_LOCK:
        _TREE_VALIDATORS.pop(root, None)


def _normalise_sep(sep: str | None, root: basenode.BaseNode) -> str:
    if sep:
        return sep
    return getattr(root, "sep", "/")


def _name_of(n: basenode.BaseNode) -> str:
    name = getattr(n, "node_name", None)
    if name is None:
        name = getattr(n, "name", None)
    return str(name)


def _capture_layout(
    root: basenode.BaseNode, sep: str | None = None
) -> tuple[
    basenode.BaseNode,
    dict[basenode.BaseNode, basenode.BaseNode | None],
    dict[basenode.BaseNode, tuple[basenode.BaseNode, ...]],
    dict[basenode.BaseNode, str],
    str,
]:
    """Capture the current parent, ordered-children, and name mapping."""
    tree_sep = _normalise_sep(sep, root)
    parents: dict[basenode.BaseNode, basenode.BaseNode | None] = {}
    children: dict[basenode.BaseNode, tuple[basenode.BaseNode, ...]] = {}
    names: dict[basenode.BaseNode, str] = {}
    stack: list[basenode.BaseNode] = [root]
    while stack:
        current = stack.pop()
        if current in parents:
            continue
        child_list = tuple(child for child in current.children if child is not None)
        parents[current] = current.parent
        children[current] = child_list
        names[current] = _name_of(current)
        stack.extend(reversed(child_list))
    parents[root] = None
    names[root] = _name_of(root)
    return root, parents, children, names, tree_sep


class LayoutView:
    """Immutable read-only view of a complete tree layout.

    The view stores the parent and ordered children of every node.  Nodes are
    the original node objects, so a callback can map a saved reference into the
    candidate layout with :meth:`view`.
    """

    __slots__ = ("_root", "_parents", "_children", "_names", "_sep")

    def __init__(
        self,
        root: basenode.BaseNode,
        parents: dict[basenode.BaseNode, basenode.BaseNode | None],
        children: Mapping[basenode.BaseNode, Sequence[basenode.BaseNode]],
        sep: str,
        names: dict[basenode.BaseNode, str] | None = None,
    ):
        self._root = root
        self._parents = dict(parents)
        self._children = {key: tuple(value) for key, value in dict(children).items()}
        self._sep = sep
        self._names = dict(names) if names else {}
        for key, value in self._children.items():
            self._parents.setdefault(key, None)
            self._names.setdefault(key, _name_of(key))
            for child in value:
                self._parents.setdefault(child, key)
                self._names.setdefault(child, _name_of(child))
        self._names.setdefault(self._root, _name_of(self._root))

    @property
    def root(self) -> basenode.BaseNode:
        """Root node of the captured layout."""
        return self._root

    @property
    def node_name(self) -> str:
        """Captured name of the root node."""
        return self._names[self._root]

    @property
    def name(self) -> str:
        """Captured name of the root node."""
        return self._names[self._root]

    @property
    def parent(self) -> None:
        """The root has no parent in the captured layout."""
        return None

    @property
    def children(self) -> tuple[basenode.BaseNode, ...]:
        """Captured ordered children of the root node."""
        return self.children_of(self._root)

    @property
    def root_path_name(self) -> str:
        """Captured path of the root node."""
        return self.path_of(self._root)

    @property
    def sep(self) -> str:
        """Path separator used by the captured layout."""
        return self._sep

    @property
    def layout(self) -> "LayoutView":
        """This complete read-only layout."""
        return self

    def view_layout(self) -> "LayoutView":
        """Return this complete read-only layout."""
        return self

    @property
    def nodes(self) -> tuple[basenode.BaseNode, ...]:
        """All nodes in the layout in pre-order."""
        return tuple(self.walk())

    def __iter__(self) -> Iterator[basenode.BaseNode]:
        return self.walk()

    def __len__(self) -> int:
        return sum(1 for _ in self.walk())

    def __contains__(self, target: object) -> bool:
        return target in self._parents

    def walk(self) -> Iterator[basenode.BaseNode]:
        """Yield nodes in the captured pre-order layout."""
        stack: list[basenode.BaseNode] = [self._root]
        seen: set[basenode.BaseNode] = set()
        while stack:
            current = stack.pop()
            if current in seen:
                raise exceptions.CorruptedTreeError("Candidate layout contains a cycle")
            seen.add(current)
            yield current
            stack.extend(reversed(self._children.get(current, ())))

    def parent_of(self, target: basenode.BaseNode) -> basenode.BaseNode | None:
        """Get the captured parent of a node."""
        if target not in self._parents:
            raise exceptions.NotFoundError(f"Node {target!r} is not in the layout")
        return self._parents[target]

    def children_of(self, target: basenode.BaseNode) -> tuple[basenode.BaseNode, ...]:
        """Get the captured ordered children of a node."""
        if target not in self._parents:
            raise exceptions.NotFoundError(f"Node {target!r} is not in the layout")
        return self._children.get(target, ())

    def siblings_of(self, target: basenode.BaseNode) -> tuple[basenode.BaseNode, ...]:
        """Get the captured siblings of a node, excluding itself."""
        parent = self.parent_of(target)
        if parent is None:
            return ()
        return tuple(child for child in self.children_of(parent) if child is not target)

    def left_sibling_of(self, target: basenode.BaseNode) -> basenode.BaseNode | None:
        """Get the captured left sibling of a node."""
        parent = self.parent_of(target)
        if parent is None:
            return None
        siblings = self.children_of(parent)
        index = next(
            (idx for idx, child in enumerate(siblings) if child is target), None
        )
        if index is None:
            raise exceptions.CorruptedTreeError(
                f"Node {target!r} is not a child of its captured parent"
            )
        return siblings[index - 1] if index else None

    def right_sibling_of(self, target: basenode.BaseNode) -> basenode.BaseNode | None:
        """Get the captured right sibling of a node."""
        parent = self.parent_of(target)
        if parent is None:
            return None
        siblings = self.children_of(parent)
        index = next(
            (idx for idx, child in enumerate(siblings) if child is target), None
        )
        if index is None:
            raise exceptions.CorruptedTreeError(
                f"Node {target!r} is not a child of its captured parent"
            )
        return siblings[index + 1] if index + 1 < len(siblings) else None

    def index_of(self, target: basenode.BaseNode) -> int:
        """Get the captured zero-based sibling position of a node."""
        parent = self.parent_of(target)
        if parent is None:
            return 0
        return next(
            idx for idx, child in enumerate(self.children_of(parent)) if child is target
        )

    def path_of(self, target: basenode.BaseNode) -> str:
        """Get the captured full path of a node."""
        return self.path_name(target)

    def path_name(self, target: basenode.BaseNode) -> str:
        """Get the captured full path of a node."""
        if target not in self._parents:
            raise exceptions.NotFoundError(f"Node {target!r} is not in the layout")
        names: list[str] = []
        current: basenode.BaseNode | None = target
        seen: set[basenode.BaseNode] = set()
        while current is not None:
            if current in seen:
                raise exceptions.CorruptedTreeError("Candidate layout contains a cycle")
            seen.add(current)
            names.append(self._names[current])
            current = self._parents.get(current)
        return self._sep + self._sep.join(reversed(names))

    def path_of_node(self, target: basenode.BaseNode) -> str:
        """Get the captured full path of a node."""
        return self.path_name(target)

    def node_path(self, target: basenode.BaseNode) -> tuple[basenode.BaseNode, ...]:
        """Get the captured root-to-node path."""
        if target not in self._parents:
            raise exceptions.NotFoundError(f"Node {target!r} is not in the layout")
        result: list[basenode.BaseNode] = []
        current: basenode.BaseNode | None = target
        while current is not None:
            result.append(current)
            current = self._parents.get(current)
        return tuple(reversed(result))

    def paths(self) -> tuple[tuple[str, basenode.BaseNode], ...]:
        """Get every captured path and node in pre-order."""
        return tuple((self.path_of(n), n) for n in self.walk())

    def full_layout(self) -> dict[str, basenode.BaseNode]:
        """Get a complete mapping of captured path to original node."""
        return dict(self.paths())

    def render(self) -> str:
        """Render the captured layout as a string tree."""

        def _render_tree(current: basenode.BaseNode) -> list[str]:
            lines = [self._names[current]]
            child_lines = self._children.get(current, ())
            for index, child in enumerate(child_lines):
                last = index == len(child_lines) - 1
                connector = "└── " if last else "├── "
                continuation = "    " if last else "│   "
                child_body = _render_tree(child)
                lines.append(connector + child_body[0])
                lines.extend(continuation + line for line in child_body[1:])
            return lines

        return "\n".join(_render_tree(self._root))

    def __str__(self) -> str:
        return self.render()

    def view(self, target: basenode.BaseNode) -> "LayoutNode":
        """Wrap a saved node reference in this captured layout."""
        if target not in self._parents:
            raise exceptions.NotFoundError(f"Node {target!r} is not in the layout")
        return LayoutNode(self, target)

    def node(self) -> "LayoutNode":
        """Wrap the root in this captured layout."""
        return self.view(self._root)

    node_at = node

    def resolve(self, path: str) -> basenode.BaseNode:
        """Resolve a path against this captured layout.

        The path may be a full path starting at the root, or a unique trailing
        partial path.  Ambiguous partial paths are rejected instead of silently
        selecting the first match.

        Args:
            path: path to resolve

        Returns:
            Resolved node
        """
        if not isinstance(path, str):
            raise TypeError("Path must be a string")
        sep = self._sep
        cleaned = path.strip()
        if not cleaned:
            raise exceptions.NotFoundError("Path is empty")
        cleaned = cleaned.rstrip(sep)
        while cleaned.startswith(sep):
            cleaned = cleaned[len(sep) :]
        if not cleaned:
            return self._root
        parts = cleaned.split(sep)
        root_name = self._names[self._root]
        if parts[0] == root_name:
            current: basenode.BaseNode = self._root
            for part in parts[1:]:
                child = next(
                    (
                        item
                        for item in self._children.get(current, ())
                        if self._names[item] == part
                    ),
                    None,
                )
                if child is None:
                    raise exceptions.NotFoundError(
                        f"Unable to find path {path!r} in the layout"
                    )
                current = child
            return current

        suffix = sep + cleaned
        matches = [
            n
            for n in self.walk()
            if self.path_of(n) == suffix or self.path_of(n).endswith(suffix)
        ]
        if not matches:
            raise exceptions.NotFoundError(
                f"Unable to find path {path!r} in the layout"
            )
        if len(matches) > 1:
            raise exceptions.BatchConflictError(
                f"Path {path!r} is ambiguous in the layout, found "
                f"{[self.path_of(m) for m in matches]}"
            )
        return matches[0]

    def __getitem__(self, path: str) -> basenode.BaseNode:
        return self.resolve(path)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(root={self._names[self._root]!r}, "
            f"nodes={len(self)})"
        )


class LayoutNode:
    """A node proxy fixed to one immutable layout view."""

    __slots__ = ("_layout", "_node")

    def __init__(self, layout: LayoutView, target: basenode.BaseNode):
        self._layout = layout
        self._node = target

    @property
    def node(self) -> basenode.BaseNode:
        """Original node behind this proxy."""
        return self._node

    @property
    def node_name(self) -> str:
        return self._layout._names[self._node]

    @property
    def name(self) -> str:
        return self._layout._names[self._node]

    @property
    def parent(self) -> "LayoutNode | None":
        parent = self._layout.parent_of(self._node)
        return None if parent is None else LayoutNode(self._layout, parent)

    @property
    def children(self) -> tuple["LayoutNode", ...]:
        return tuple(
            LayoutNode(self._layout, child)
            for child in self._layout.children_of(self._node)
        )

    @property
    def siblings(self) -> tuple["LayoutNode", ...]:
        return tuple(
            LayoutNode(self._layout, sibling)
            for sibling in self._layout.siblings_of(self._node)
        )

    @property
    def left_sibling(self) -> "LayoutNode | None":
        sibling = self._layout.left_sibling_of(self._node)
        return None if sibling is None else LayoutNode(self._layout, sibling)

    @property
    def right_sibling(self) -> "LayoutNode | None":
        sibling = self._layout.right_sibling_of(self._node)
        return None if sibling is None else LayoutNode(self._layout, sibling)

    @property
    def index(self) -> int:
        return self._layout.index_of(self._node)

    @property
    def siblings_index(self) -> int:
        return self._layout.index_of(self._node)

    @property
    def depth(self) -> int:
        return len(self._layout.path_of(self._node).split(self._layout.sep)) - 2

    @property
    def path_name(self) -> str:
        return self._layout.path_of(self._node)

    @property
    def root(self) -> "LayoutNode":
        return LayoutNode(self._layout, self._layout.root)

    @property
    def ancestors(self) -> tuple["LayoutNode", ...]:
        result: list[LayoutNode] = []
        current = self._layout.parent_of(self._node)
        while current is not None:
            result.append(LayoutNode(self._layout, current))
            current = self._layout.parent_of(current)
        return tuple(result)

    @property
    def is_root(self) -> bool:
        return self._layout.parent_of(self._node) is None

    @property
    def is_leaf(self) -> bool:
        return not self._layout.children_of(self._node)

    @property
    def descendants(self) -> tuple["LayoutNode", ...]:
        result: list[LayoutNode] = []
        stack = list(reversed(self._layout.children_of(self._node)))
        while stack:
            current = stack.pop()
            result.append(LayoutNode(self._layout, current))
            stack.extend(reversed(self._layout.children_of(current)))
        return tuple(result)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._node, item)

    def get_attr(self, attr_name: str, default: Any = None) -> Any:
        if isinstance(attr_name, str):
            if attr_name in {
                "parent",
                "children",
                "siblings",
                "left_sibling",
                "right_sibling",
                "index",
                "path_name",
                "node_name",
                "name",
                "root",
                "ancestors",
                "descendants",
                "is_root",
                "is_leaf",
            }:
                return getattr(self, attr_name)
            head, separator, tail = attr_name.partition(".")
            if separator:
                value = self.get_attr(head)
                if isinstance(value, LayoutNode):
                    return value.get_attr(tail, default)
                return default
        return common.get_attr(self._node, attr_name, default)

    def __getitem__(self, child_name: str) -> "LayoutNode":
        child = next(
            (
                item
                for item in self._layout.children_of(self._node)
                if self._layout._names[item] == child_name
            ),
            None,
        )
        if child is None:
            raise KeyError(child_name)
        return LayoutNode(self._layout, child)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LayoutNode):
            return self._node is other._node
        if isinstance(other, basenode.BaseNode):
            return self._node is other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._node)

    def __repr__(self) -> str:
        return f"LayoutNode({self.path_name!r}, node={self._node!r})"


class TreeManifest(LayoutView):
    """Immutable snapshot of a tree layout.

    A manifest opened before a batch keeps its original sibling order and paths
    for as long as it is held.  Opening a new manifest after commit observes the
    complete new layout.
    """

    __slots__ = ("_captured_at_revision",)

    _REVISION_LOCK = threading.RLock()
    _REVISIONS: "weakref.WeakKeyDictionary[basenode.BaseNode, int]" = (
        weakref.WeakKeyDictionary()
    )

    def __init__(
        self,
        root: basenode.BaseNode,
        parents: dict[basenode.BaseNode, basenode.BaseNode | None],
        children: Mapping[basenode.BaseNode, Sequence[basenode.BaseNode]],
        sep: str,
        revision: int = 0,
        names: dict[basenode.BaseNode, str] | None = None,
    ):
        super().__init__(root, parents, children, sep, names)
        self._captured_at_revision = revision

    @classmethod
    def capture(cls, tree: Any, sep: str | None = None) -> "TreeManifest":
        """Capture the current complete layout of a tree.

        Args:
            tree: any node belonging to the tree
            sep: path separator, defaults to the tree separator

        Returns:
            Immutable manifest
        """
        root, parents, children, names, tree_sep = _capture_layout(tree, sep)
        with cls._REVISION_LOCK:
            revision = cls._REVISIONS.get(root, 0)
        return cls(root, parents, children, tree_sep, revision, names)

    @property
    def revision(self) -> int:
        """Revision number observed when the manifest was opened."""
        return self._captured_at_revision

    @classmethod
    def _bump_revision(cls, root: basenode.BaseNode) -> int:
        with cls._REVISION_LOCK:
            revision = cls._REVISIONS.get(root, 0) + 1
            cls._REVISIONS[root] = revision
            return revision


def open_manifest(tree: Any, sep: str | None = None) -> TreeManifest:
    """Open an immutable manifest of the tree's current layout.

    Args:
        tree: any node belonging to the tree
        sep: path separator, defaults to the tree separator

    Returns:
        Immutable manifest
    """
    root = _root_of(tree)
    with get_tree_lock(root):
        return TreeManifest.capture(root, sep)


@dataclass(frozen=True)
class BatchOperation:
    """One path-addressable operation in a :class:`TreeBatch`."""

    kind: Literal["swap", "move"]
    first: str
    second: str | None = None
    index: int | None = None

    @classmethod
    def swap(cls, first: str, second: str) -> "BatchOperation":
        """Create a subtree swap operation."""
        return cls("swap", first, second)

    @classmethod
    def move(
        cls, from_path: str, to_parent_path: str, index: int | None = None
    ) -> "BatchOperation":
        """Create a subtree move operation."""
        return cls("move", from_path, to_parent_path, index)


def _is_ancestor(
    ancestor: basenode.BaseNode,
    target: basenode.BaseNode,
    parents: dict[basenode.BaseNode, basenode.BaseNode | None],
) -> bool:
    current = parents.get(target)
    seen: set[basenode.BaseNode] = set()
    while current is not None:
        if current is ancestor:
            return True
        if current in seen:  # pragma: no cover - cycle guard
            raise exceptions.BatchCycleError("Cycle detected in candidate layout")
        seen.add(current)
        current = parents.get(current)
    return False


class TreeBatch:
    """A batch of subtree moves resolved against one starting layout.

    Paths passed to :meth:`swap`, :meth:`move`, and :meth:`add_operation` are
    resolved when the batch is built.  Reordering the tree therefore never
    reinterprets a later path such as ``站点/旧版/归档/index`` against a layout
    that already changed.

    Example:
        >>> from bigtree import Tree
        >>> tree = Tree.from_list(
        ...     ["站点/旧版/归档/index", "站点/新版/归档/index"]
        ... )
        >>> old_index = tree.node["旧版"]["归档"]["index"]
        >>> new_index = tree.node["新版"]["归档"]["index"]
        >>> batch = tree.batch()
        >>> _ = batch.swap("站点/旧版/归档", "站点/新版/归档")
        >>> manifest = batch.commit()
        >>> manifest["站点/旧版/归档/index"] is new_index
        True
        >>> manifest["站点/新版/归档/index"] is old_index
        True

    Args:
        tree: tree root, or any node belonging to the tree
        sep: path separator, defaults to the tree separator
        validators: additional read-only validators for this batch
    """

    def __init__(
        self,
        tree: Any,
        sep: str | None = None,
        validators: Iterable[Validator] | None = None,
    ):
        root = _root_of(tree)
        if not isinstance(root, node.Node):
            raise TypeError(
                "TreeBatch requires a Node or Node subclass, received "
                f"{type(root).__name__}"
            )
        if isinstance(root, binarynode.BinaryNode):
            raise TypeError("TreeBatch does not support BinaryNode trees")
        self._root = root
        self._sep = _normalise_sep(sep, root)
        self._start = TreeManifest.capture(root, self._sep)
        self._operations: list[BatchOperation] = []
        self._validators: list[Validator] = []
        for callback in validators or ():
            self.add_validator(callback)
        self._committed = False
        self._committed_manifest: TreeManifest | None = None

    @property
    def root(self) -> basenode.BaseNode:
        """Root of the batch scope."""
        return self._root

    @property
    def sep(self) -> str:
        """Path separator used by the batch."""
        return self._sep

    @property
    def start_layout(self) -> TreeManifest:
        """Layout captured when the batch was created."""
        return self._start

    @property
    def operations(self) -> tuple[BatchOperation, ...]:
        """Queued operations in insertion order."""
        return tuple(self._operations)

    @property
    def committed(self) -> bool:
        """Whether this batch has been committed."""
        return self._committed

    @property
    def committed_manifest(self) -> TreeManifest | None:
        """Manifest produced by a successful commit."""
        return self._committed_manifest

    def add_validator(self, callback: Validator) -> "TreeBatch":
        """Register a read-only validator for this batch.

        Args:
            callback: function accepting a candidate :class:`LayoutView`

        Returns:
            Self, to allow chaining
        """
        if not callable(callback):
            raise TypeError("Batch validator must be callable")
        if all(existing is not callback for existing in self._validators):
            self._validators.append(callback)
        return self

    register_validator = add_validator

    def swap(self, first: str, second: str) -> "TreeBatch":
        """Swap two entire subtrees, preserving their sibling positions.

        Args:
            first: path of the first subtree
            second: path of the second subtree

        Returns:
            Self, to allow chaining
        """
        self._ensure_pending()
        self._operations.append(BatchOperation.swap(first, second))
        return self

    swap_subtrees = swap

    def move(
        self, from_path: str, to_parent_path: str, index: int | None = None
    ) -> "TreeBatch":
        """Move a subtree under a destination parent.

        Args:
            from_path: path of the subtree to move
            to_parent_path: path of the destination parent
            index: optional zero-based insertion position, defaults to append

        Returns:
            Self, to allow chaining
        """
        self._ensure_pending()
        if index is not None and index < 0:
            raise ValueError("`index` must be non-negative")
        self._operations.append(BatchOperation.move(from_path, to_parent_path, index))
        return self

    move_subtree = move

    def add_operation(self, operation: BatchOperation) -> "TreeBatch":
        """Append a pre-built operation.

        Args:
            operation: operation to append

        Returns:
            Self, to allow chaining
        """
        self._ensure_pending()
        if not isinstance(operation, BatchOperation):
            raise TypeError("Expected a BatchOperation")
        if operation.kind == "move" and operation.index is not None:
            if operation.index < 0:
                raise ValueError("`index` must be non-negative")
        if operation.kind not in ("swap", "move"):
            raise ValueError(f"Unknown batch operation {operation.kind!r}")
        self._operations.append(operation)
        return self

    def extend(self, operations: Iterable[BatchOperation]) -> "TreeBatch":
        """Append multiple operations in order."""
        for operation in operations:
            self.add_operation(operation)
        return self

    def _ensure_pending(self) -> None:
        if self._committed:
            raise exceptions.BatchError("Batch has already been committed")

    def _resolve(self, layout: LayoutView, path: str) -> basenode.BaseNode:
        try:
            return layout.resolve(path)
        except exceptions.NotFoundError:
            raise
        except exceptions.BatchConflictError:
            raise

    def _resolved_operations(
        self,
    ) -> list[tuple[BatchOperation, basenode.BaseNode, basenode.BaseNode | None]]:
        resolved: list[
            tuple[BatchOperation, basenode.BaseNode, basenode.BaseNode | None]
        ] = []
        for operation in self._operations:
            if operation.kind == "swap":
                if operation.second is None:
                    raise exceptions.BatchConflictError(
                        "Swap operation requires two paths"
                    )
                first = self._resolve(self._start, operation.first)
                second = self._resolve(self._start, operation.second)
                resolved.append((operation, first, second))
            elif operation.kind == "move":
                if operation.second is None:
                    raise exceptions.BatchConflictError(
                        "Move operation requires a destination parent path"
                    )
                source = self._resolve(self._start, operation.first)
                destination = self._resolve(self._start, operation.second)
                resolved.append((operation, source, destination))
            else:  # pragma: no cover - guarded by add_operation
                raise ValueError(f"Unknown batch operation {operation.kind!r}")
        return resolved

    def _check_current_matches_start(
        self,
        current: LayoutView,
        resolved: Sequence[
            tuple[BatchOperation, basenode.BaseNode, basenode.BaseNode | None]
        ],
    ) -> None:
        """Reject a commit if an involved node's structural position changed."""
        involved: set[basenode.BaseNode] = set()
        for operation, first, second in resolved:
            targets = (first, second) if operation.kind == "swap" else (first, second)
            for target in targets:
                if target is None:
                    continue
                involved.add(target)
                parent = self._start.parent_of(target)
                if parent is not None:
                    involved.add(parent)
        for target in involved:
            if target not in current:
                raise exceptions.BatchConflictError(
                    f"Node {self._start.path_name(target)!r} is no longer in the tree"
                )
            if current.parent_of(target) is not self._start.parent_of(target):
                raise exceptions.BatchConflictError(
                    f"Node {self._start.path_name(target)!r} moved outside the batch"
                )
            if current.children_of(target) != self._start.children_of(target):
                raise exceptions.BatchConflictError(
                    f"Sibling order under {self._start.path_name(target)!r} "
                    "changed outside the batch"
                )

    def _candidate_from(
        self,
        base: LayoutView,
        resolved: Sequence[
            tuple[BatchOperation, basenode.BaseNode, basenode.BaseNode | None]
        ],
    ) -> LayoutView:
        parents = dict(base._parents)
        children = {key: list(value) for key, value in base._children.items()}

        def ensure_node(target: basenode.BaseNode, path: str) -> None:
            if target not in parents:
                raise exceptions.NotFoundError(
                    f"Unable to resolve path {path!r} in the candidate layout"
                )

        for operation, first, second in resolved:
            ensure_node(first, operation.first)
            if operation.kind == "swap":
                assert second is not None
                ensure_node(second, operation.second or "")
                if first is second:
                    continue
                first_parent = parents[first]
                second_parent = parents[second]
                if first_parent is None or second_parent is None:
                    raise exceptions.BatchConflictError(
                        "Cannot swap the root node of a tree"
                    )
                if _is_ancestor(first, second, parents) or _is_ancestor(
                    second, first, parents
                ):
                    raise exceptions.BatchCycleError(
                        "Cannot swap a subtree with one of its own descendants"
                    )
                first_index = next(
                    idx
                    for idx, child in enumerate(children[first_parent])
                    if child is first
                )
                second_index = next(
                    idx
                    for idx, child in enumerate(children[second_parent])
                    if child is second
                )
                if first_parent is second_parent:
                    siblings = children[first_parent]
                    siblings[first_index], siblings[second_index] = (
                        siblings[second_index],
                        siblings[first_index],
                    )
                else:
                    children[first_parent][first_index] = second
                    children[second_parent][second_index] = first
                    parents[first] = second_parent
                    parents[second] = first_parent
            else:
                assert second is not None
                source = first
                destination = second
                if source is destination or _is_ancestor(source, destination, parents):
                    raise exceptions.BatchCycleError(
                        "Cannot move a subtree below one of its own descendants"
                    )
                source_parent = parents[source]
                if source_parent is None:
                    raise exceptions.BatchConflictError(
                        "Cannot move the root node of a tree"
                    )
                children[source_parent] = [
                    child for child in children[source_parent] if child is not source
                ]
                destination_children = children[destination]
                position = (
                    len(destination_children)
                    if operation.index is None
                    else operation.index
                )
                if position > len(destination_children):
                    raise IndexError(
                        f"Insertion index {position} exceeds the destination "
                        f"child count {len(destination_children)}"
                    )
                destination_children.insert(position, source)
                parents[source] = destination

        candidate = LayoutView(base.root, parents, children, base.sep, base._names)
        self._validate_structure(candidate)
        return candidate

    @staticmethod
    def _validate_structure(candidate: LayoutView) -> None:
        reachable: set[basenode.BaseNode] = set()
        stack: list[basenode.BaseNode] = [candidate.root]
        while stack:
            current = stack.pop()
            if current in reachable:
                raise exceptions.BatchCycleError("Candidate layout visits a node twice")
            reachable.add(current)
            for child in candidate.children_of(current):
                stack.append(child)
        if reachable != set(candidate._parents):  # pragma: no cover - internal
            raise exceptions.BatchCycleError(
                "Candidate layout contains a cycle or an unreachable node"
            )

    def candidate(self) -> LayoutView:
        """Build the complete candidate layout without touching the live tree.

        Returns:
            Read-only candidate layout
        """
        self._ensure_pending()
        resolved = self._resolved_operations()
        current = TreeManifest.capture(self._root, self._sep)
        self._check_current_matches_start(current, resolved)
        return self._candidate_from(current, resolved)

    preview = candidate

    def _run_validators(self, candidate: LayoutView) -> None:
        callbacks: list[Validator] = []
        with _REGISTRY_LOCK:
            callbacks.extend(_TREE_VALIDATORS.get(self._root, ()))
        callbacks.extend(self._validators)
        seen: set[int] = set()
        for callback in callbacks:
            if id(callback) in seen:
                continue
            seen.add(id(callback))
            try:
                result = callback(candidate)
            except exceptions.BatchError:
                raise
            except exceptions.LoopError:
                raise
            except Exception as exc:
                raise exceptions.BatchValidationError(
                    f"Batch validator {callback!r} raised: {exc}"
                ) from exc
            if result is False:
                raise exceptions.BatchValidationError(
                    f"Batch rejected by validator {callback!r}"
                )

    def _apply_layout(self, layout: LayoutView) -> None:
        """Apply a complete layout to the live nodes, preserving identity."""
        desired_parent = {
            current: layout.parent_of(current) for current in layout.nodes
        }
        desired_children = {
            current: layout.children_of(current) for current in layout.nodes
        }

        for current, parent in desired_parent.items():
            if current.parent is not parent and parent is not None:
                current.parent = None
        for current, parent in desired_parent.items():
            if parent is None and current.parent is not None:
                current.parent = None

        for current in layout.nodes:
            current_children = tuple(
                child for child in current.children if child is not None
            )
            if current_children != desired_children[current]:
                current.children = list(desired_children[current])

    def _matches_layout(self, layout: LayoutView) -> bool:
        current = TreeManifest.capture(self._root, self._sep)
        if set(current.nodes) != set(layout.nodes):
            return False
        for target in current.nodes:
            if current.parent_of(target) is not layout.parent_of(target):
                return False
            if current.children_of(target) != layout.children_of(target):
                return False
        return True

    def commit(self) -> TreeManifest:
        """Validate and atomically apply the batch.

        The candidate layout is applied to the live nodes under the tree lock
        before read-only validators run, so a validator holding a saved node
        reference observes that node in its new parent and sibling order.  If a
        validator rejects the batch, the original layout is restored before the
        lock is released.  Manifests opened before the commit keep the old
        layout; manifests opened afterwards see the complete new layout.

        Returns:
            Manifest of the committed layout
        """
        self._ensure_pending()
        with get_tree_lock(self._root):
            resolved = self._resolved_operations()
            current = TreeManifest.capture(self._root, self._sep)
            self._check_current_matches_start(current, resolved)
            candidate = self._candidate_from(current, resolved)

            before = TreeManifest.capture(self._root, self._sep)
            try:
                self._apply_layout(candidate)
                applied = TreeManifest.capture(self._root, self._sep)
                if not self._matches_layout(candidate):
                    raise exceptions.BatchError(
                        "Applied layout does not match the candidate layout"
                    )
                self._run_validators(applied)
            except Exception:
                try:
                    self._apply_layout(before)
                    if not self._matches_layout(before):  # pragma: no cover
                        raise exceptions.BatchError(
                            "Rollback did not restore the original layout"
                        )
                except Exception:  # pragma: no cover - defensive rollback
                    pass
                raise

            TreeManifest._bump_revision(self._root)
            manifest = TreeManifest.capture(self._root, self._sep)
            self._committed = True
            self._committed_manifest = manifest
            return manifest

    def open(self) -> TreeManifest:
        """Open a manifest of the current live layout."""
        return open_manifest(self._root, self._sep)

    open_manifest = open

    def __enter__(self) -> "TreeBatch":
        self._ensure_pending()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> Literal[False]:
        if exc_type is None:
            self.commit()
        return False

    def __repr__(self) -> str:
        return (
            f"TreeBatch(root={self._root.node_name!r}, "
            f"operations={len(self._operations)}, committed={self._committed})"
        )


def apply_batch(
    tree: Any,
    swaps: Iterable[tuple[str, str]] = (),
    moves: Iterable[tuple[str, str] | tuple[str, str, int]] = (),
    validators: Iterable[Validator] = (),
    sep: str | None = None,
) -> TreeManifest:
    """Apply a group of subtree swaps and moves atomically.

    Args:
        tree: tree root, or any node belonging to the tree
        swaps: pairs of subtree paths to swap
        moves: ``(from_path, to_parent_path)`` or ``(from_path, to_parent_path,
            index)`` triples
        validators: read-only validators run against the candidate layout
        sep: path separator, defaults to the tree separator

    Returns:
        Manifest of the committed layout
    """
    batch = TreeBatch(tree, sep=sep, validators=validators)
    for first, second in swaps:
        batch.swap(first, second)
    for move in moves:
        if len(move) == 2:
            batch.move(move[0], move[1])
        elif len(move) == 3:
            batch.move(move[0], move[1], move[2])
        else:  # pragma: no cover - defensive
            raise ValueError("Move must contain two or three items")
    return batch.commit()


def batch(
    tree: Any,
    sep: str | None = None,
    validators: Iterable[Validator] | None = None,
) -> TreeBatch:
    """Create a batch bound to the tree's current layout.

    Args:
        tree: tree root, or any node belonging to the tree
        sep: path separator, defaults to the tree separator
        validators: additional read-only validators for this batch

    Returns:
        New batch
    """
    return TreeBatch(tree, sep=sep, validators=validators)


def swap_subtrees(
    tree: Any,
    first: str,
    second: str,
) -> TreeManifest:
    """Atomically swap two entire subtrees.

    Args:
        tree: tree root, or any node belonging to the tree
        first: path of the first subtree
        second: path of the second subtree

    Returns:
        Manifest of the committed layout
    """
    return TreeBatch(tree).swap(first, second).commit()


def move_subtree(
    tree: Any,
    from_path: str,
    to_parent_path: str,
    index: int | None = None,
) -> TreeManifest:
    """Atomically move a subtree under a new parent.

    Args:
        tree: tree root, or any node belonging to the tree
        from_path: path of the subtree to move
        to_parent_path: path of the destination parent
        index: optional zero-based insertion position, defaults to append

    Returns:
        Manifest of the committed layout
    """
    return TreeBatch(tree).move(from_path, to_parent_path, index).commit()
