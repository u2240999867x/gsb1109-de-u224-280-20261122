"""Provenance tracking for typed YAML decodes.

YAML anchors and merge keys are commonly used to share "template" mappings
between records.  When validation of such a document fails, the logical key
path alone isn't enough: the offending value may live in a template far away
from the record that inherited it, and a template may be shared by several
records.  This module re-parses the document into an immutable snapshot of its
node graph and resolves, for a logical path, the document position of the value
that was actually used together with the merge/alias sites needed to reach it.

The snapshot is taken before PyYAML constructs Python objects, since its
``flatten_mapping`` mutates mapping nodes in place and merge sequences move
pairs between nodes.  Resolution is purely structural: it follows the logical
path through the *effective* mapping produced by merge keys, mirroring
PyYAML's precedence rules.  Because every resolution starts at the root and
walks the logical path, the declaration order of sibling records can't change
which value is attributed to a path.
"""

from __future__ import annotations

import re as _re

_MERGE_TAG = "tag:yaml.org,2002:merge"
_IDENTIFIER_RE = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MISSING = object()


class UseSite:
    """A place where an anchored node is reached from another node.

    Attributes
    ----------
    relation : {"merge", "alias", "recursive"}
        How the target was reached.
    anchor : str or None
        The anchor naming the target, if it had one.
    line, column : int
        1-based position of the merge key or alias that caused the use.
    target_line : int or None
        1-based line where the target node is defined.
    """

    __slots__ = ("relation", "anchor", "line", "column", "target_line")

    def __init__(self, relation, anchor, line, column, target_line):
        self.relation = relation
        self.anchor = anchor
        self.line = line
        self.column = column
        self.target_line = target_line

    def describe(self):
        label = f"&{self.anchor}" if self.anchor else "mapping"
        if self.relation == "recursive":
            return f"recursive reference to {label} at line {self.line}"
        if self.relation == "alias":
            return f"aliased from {label} at line {self.line}"
        return f"inherited from {label} at line {self.line}"

    @property
    def source_line(self):
        return self.line

    @property
    def use_line(self):
        return self.line

    def as_dict(self):
        return {
            "relation": self.relation,
            "anchor": self.anchor,
            "line": self.line,
            "column": self.column,
            "target_line": self.target_line,
        }

    def __repr__(self):
        return (
            f"UseSite(relation={self.relation!r}, anchor={self.anchor!r}, "
            f"line={self.line!r}, column={self.column!r})"
        )


class Origin:
    """Where a value in a typed YAML document came from.

    Attributes
    ----------
    path : str
        The logical path of the value, in the same ``$.field[0]`` form used by
        validation errors.
    line, column : int or None
        1-based position of the value that was actually used, or `None` if the
        value didn't come from the document (e.g. a default).
    table : str or None
        The logical table (top-level key) that owns the value.
    anchor : str or None
        The anchor naming the template that provides the value, if any.
    sites : tuple[UseSite, ...]
        The merge/alias sites traversed, ordered from the outermost template to
        the value itself.
    """

    __slots__ = ("path", "line", "column", "table", "anchor", "sites", "path_parts")

    def __init__(
        self,
        path,
        line=None,
        column=None,
        table=None,
        anchor=None,
        sites=(),
        path_parts=(),
    ):
        self.path = path
        self.line = line
        self.column = column
        self.table = table
        self.anchor = anchor
        self.sites = tuple(sites)
        self.path_parts = tuple(path_parts)

    @property
    def source_line(self):
        return self.line

    @property
    def document_line(self):
        return self.line

    @property
    def logical_table(self):
        return self.table

    @property
    def table_path(self):
        """Dotted path to the record that owns the value, if resolvable."""
        parts = list(self.path_parts)
        if parts and isinstance(parts[-1], str):
            parts = parts[:-1]
        out = []
        for part in parts:
            if isinstance(part, str):
                out.append(part)
            else:
                out.append(f"[{part}]")
        return ".".join(out)

    @property
    def anchors(self):
        return tuple(
            dict.fromkeys(
                site.anchor for site in self.sites if site.anchor is not None
            )
        )

    @property
    def use_sites(self):
        return self.sites

    def as_dict(self):
        """Return the details as plain builtin values."""
        return {
            "path": self.path,
            "path_parts": self.path_parts,
            "line": self.line,
            "column": self.column,
            "table": self.table,
            "table_path": self.table_path,
            "anchor": self.anchor,
            "anchors": self.anchors,
            "sites": [site.as_dict() for site in self.sites],
        }

    def to_dict(self):
        return self.as_dict()

    def keys(self):
        return self.as_dict().keys()

    def __getitem__(self, key):
        return self.as_dict()[key]

    def describe(self):
        parts = []
        if self.table is not None:
            parts.append(f"table `{self.table}`")
        if self.line is not None:
            parts.append(f"value at line {self.line}")
        for site in self.sites:
            parts.append(site.describe())
        return ", ".join(parts)

    def __repr__(self):
        return (
            f"Origin(path={self.path!r}, line={self.line!r}, "
            f"column={self.column!r}, table={self.table!r}, "
            f"anchor={self.anchor!r}, sites={self.sites!r})"
        )


class _MapItem:
    __slots__ = ("key", "value", "alias")

    def __init__(self, key, value, alias=None):
        self.key = key
        self.value = value
        self.alias = alias


class _Snapshot:
    __slots__ = (
        "node_id",
        "kind",
        "tag",
        "value",
        "line",
        "column",
        "end_line",
        "anchor",
        "anchor_line",
        "anchor_column",
        "map_items",
        "seq_items",
        "seq_aliases",
    )

    def __init__(self, node_id, kind, tag, line, column, end_line):
        self.node_id = node_id
        self.kind = kind
        self.tag = tag
        self.value = None
        self.line = line
        self.column = column
        self.end_line = end_line
        self.anchor = None
        self.anchor_line = 0
        self.anchor_column = 0
        self.map_items = ()
        self.seq_items = ()
        self.seq_aliases = ()


class _Pair:
    __slots__ = ("key", "value", "chain", "alias")

    def __init__(self, key, value, chain=(), alias=None):
        self.key = key
        self.value = value
        self.chain = chain
        self.alias = alias


class _OpaqueKey:
    """A placeholder for keys that aren't simple hashable scalars."""

    __slots__ = ("token",)

    def __init__(self, token):
        self.token = token

    def __hash__(self):
        return hash(self.token)

    def __eq__(self, other):
        return isinstance(other, _OpaqueKey) and self.token == other.token


def _index_token(index):
    return index if isinstance(index, int) else id(index)


def _make_loader(yaml):
    class _TrackingSafeLoader(yaml.SafeLoader):
        """A SafeLoader that records anchors and alias use sites."""

        def __init__(self, stream):
            super().__init__(stream)
            self._anchors = {}
            self._alias_sites = {}

        def compose_node(self, parent, index):
            if self.check_event(yaml.events.AliasEvent):
                event = self.get_event()
                anchor = event.anchor
                if anchor not in self.anchors:
                    raise yaml.composer.ComposerError(
                        None,
                        None,
                        f"found undefined alias {anchor!r}",
                        event.start_mark,
                    )
                node = self.anchors[anchor]
                self._alias_sites[(id(parent), _index_token(index))] = UseSite(
                    "alias",
                    anchor,
                    event.start_mark.line + 1,
                    event.start_mark.column + 1,
                    node.start_mark.line + 1,
                )
                return node

            event = self.peek_event()
            anchor = event.anchor
            self.descend_resolver(parent, index)
            if self.check_event(yaml.events.ScalarEvent):
                node = self.compose_scalar_node(anchor)
            elif self.check_event(yaml.events.SequenceStartEvent):
                node = self.compose_sequence_node(anchor)
            else:
                node = self.compose_mapping_node(anchor)
            self.ascend_resolver()
            if anchor is not None:
                self._anchors[id(node)] = (anchor, event.start_mark)
            return node

    return _TrackingSafeLoader


def _snapshot_node(yaml, loader, node, memo):
    node_id = id(node)
    existing = memo.get(node_id)
    if existing is not None:
        return existing

    start = node.start_mark
    if isinstance(node, yaml.nodes.MappingNode):
        kind = "map"
    elif isinstance(node, yaml.nodes.SequenceNode):
        kind = "seq"
    else:
        kind = "scalar"
    out = _Snapshot(
        node_id,
        kind,
        node.tag,
        start.line + 1,
        start.column + 1,
        node.end_mark.line + 1,
    )
    anchor_info = loader._anchors.get(node_id)
    if anchor_info is not None:
        out.anchor, mark = anchor_info
        out.anchor_line = mark.line + 1
        out.anchor_column = mark.column + 1

    memo[node_id] = out
    if kind == "map":
        items = []
        for key_node, value_node in node.value:
            key = _snapshot_node(yaml, loader, key_node, memo)
            value = _snapshot_node(yaml, loader, value_node, memo)
            alias = loader._alias_sites.get((node_id, id(key_node)))
            items.append(_MapItem(key, value, alias))
        out.map_items = tuple(items)
    elif kind == "seq":
        items = []
        aliases = []
        for index, value_node in enumerate(node.value):
            items.append(_snapshot_node(yaml, loader, value_node, memo))
            aliases.append(loader._alias_sites.get((node_id, index)))
        out.seq_items = tuple(items)
        out.seq_aliases = tuple(aliases)
    else:
        out.value = node.value
    return out


def snapshot(yaml, stream):
    """Parse ``stream`` into an immutable snapshot of its node graph."""
    Loader = _make_loader(yaml)
    loader = Loader(stream)
    try:
        node = loader.get_single_node()
        if node is None:
            return None
        return _snapshot_node(yaml, loader, node, {})
    finally:
        loader.dispose()


def _scalar_key(yaml, node):
    if node.kind != "scalar":
        return _OpaqueKey(node.node_id)
    try:
        constructor = yaml.constructor.SafeConstructor()
        return constructor.construct_object(
            yaml.nodes.ScalarNode(node.tag, node.value), deep=True
        )
    except Exception:
        return _OpaqueKey(node.node_id)


def _merge_targets(yaml, value_node):
    """Return ``(mapping, alias)`` pairs merged for a ``<<`` value node."""
    if value_node.kind == "map":
        return ((value_node, None),)
    if value_node.kind == "seq":
        return tuple(
            (child, alias)
            for child, alias in zip(value_node.seq_items, value_node.seq_aliases)
            if child.kind == "map"
        )
    return ()


def _flatten(yaml, node, stack=()):
    """Flatten merge keys, mirroring PyYAML's ``flatten_mapping``.

    Pairs are returned in construction order, so later pairs override earlier
    ones.  Merge entries therefore come first and explicit entries last.
    """
    if node.kind != "map":
        return ()

    merge_pairs = []
    explicit = []
    recursive = None

    for item in node.map_items:
        if item.key.tag != _MERGE_TAG:
            explicit.append(
                _Pair(_scalar_key(yaml, item.key), item.value, alias=item.alias)
            )
            continue
        for target, alias in _merge_targets(yaml, item.value):
            if alias is not None:
                line, column = alias.line, alias.column
            else:
                line, column = item.key.line, item.key.column
            if target is node or target.node_id in stack:
                # A node may legitimately be merged more than once (e.g. two
                # sibling records sharing a template).  Only a cycle *within
                # the current ancestry* is recursive.
                recursive = UseSite(
                    "recursive",
                    target.anchor or (alias.anchor if alias is not None else None),
                    line,
                    column,
                    target.line,
                )
                continue
            step = UseSite(
                "merge",
                target.anchor or (alias.anchor if alias is not None else None),
                line,
                column,
                target.line,
            )
            for pair in _flatten(yaml, target, stack + (node.node_id,)):
                # Later merges override earlier ones, matching PyYAML's
                # construction order.
                merge_pairs.append(_Pair(pair.key, pair.value, (step,) + pair.chain))

    if recursive is not None:
        # A self-referential merge contributes no keys of its own, but its use
        # site is still relevant context for the keys this mapping provides.
        merge_pairs = [
            _Pair(pair.key, pair.value, pair.chain + (recursive,))
            for pair in merge_pairs
        ]
        explicit = [
            _Pair(pair.key, pair.value, (recursive,), pair.alias) for pair in explicit
        ]

    return tuple(merge_pairs) + tuple(explicit)


def _effective_entries(yaml, node):
    entries = {}
    for pair in _flatten(yaml, node):
        try:
            entries[pair.key] = pair
        except TypeError:
            continue
    return entries


def format_path(path):
    """Format a sequence of keys/indices as ``$.field[0]``."""
    out = "$"
    for part in path:
        if isinstance(part, bool):
            out += f"[{part}]"
        elif isinstance(part, int):
            out += f"[{part}]"
        elif isinstance(part, str) and _IDENTIFIER_RE.match(part):
            out += f".{part}"
        else:
            out += f"[{part!r}]"
    return out


def resolve(yaml, root, decoded, path):
    """Resolve ``path`` against ``decoded``, returning an `Origin` or `None`.

    ``path`` follows the decoded structure: mapping keys and sequence indices.
    """
    if root is None:
        return None

    node = root
    chain = ()
    stack = (root.node_id,)
    used = root
    table = None
    resolved = []

    for part in path:
        if node.kind == "map":
            item = _effective_entries(yaml, node).get(part, _MISSING)
            if item is _MISSING:
                return None
            chain = chain + item.chain
            used = item.value
            alias = item.alias
        elif node.kind == "seq":
            if not isinstance(part, int) or not (0 <= part < len(node.seq_items)):
                return None
            used = node.seq_items[part]
            alias = node.seq_aliases[part]
        else:
            return None

        recursive = used.node_id in stack
        if alias is not None and not recursive:
            chain = chain + (
                UseSite("alias", alias.anchor, alias.line, alias.column, used.line),
            )
        elif alias is not None and recursive:
            # A recursive alias: report the use site and stop descending, since
            # further path segments just revisit the same nodes.
            chain = chain + (
                UseSite(
                    "recursive",
                    alias.anchor,
                    alias.line,
                    alias.column,
                    used.line,
                ),
            )
            resolved.append(part)
            used = node
            break

        node = used
        resolved.append(part)
        if node.node_id in stack:
            # A cycle that doesn't go through an alias node (e.g. a mapping
            # that merges itself); stop rather than loop forever.
            chain = chain + (
                UseSite(
                    "recursive",
                    node.anchor,
                    node.line,
                    node.column,
                    node.line,
                ),
            )
            break
        stack = stack + (node.node_id,)
        if isinstance(part, str) and node.kind == "map":
            table = part

    if table is None:
        for part in resolved:
            if isinstance(part, str):
                table = part
                break

    # The same site can be recorded by both a merge entry and the nested
    # mapping it flattens; report each place once.
    deduped = []
    for site in chain:
        if deduped and deduped[-1] is site:
            continue
        if any(
            other.relation == site.relation
            and other.anchor == site.anchor
            and other.line == site.line
            for other in deduped
        ):
            continue
        deduped.append(site)

    resolved = tuple(resolved)
    return Origin(
        format_path(resolved),
        used.line,
        used.column,
        table,
        used.anchor,
        deduped,
        resolved,
    )
