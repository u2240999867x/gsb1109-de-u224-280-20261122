from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable, Sequence, TypeVar

from bigtree.node import node
from bigtree.utils import exceptions

__all__ = [
    "clean_newick",
    "clean_newick_file",
    "clean_newick_files",
    "newick_file_to_trees",
    "newick_files_to_trees",
    "newick_to_trees",
    "read_newick",
    "read_newick_file",
    "read_newick_files",
    "validate_newick_codes",
]

T = TypeVar("T", bound=node.Node)

_DELIMITERS = set("(),:;[]")


@dataclass
class _ParsedNode:
    """Intermediate Newick node, before converting to a BigTree node."""

    name: str | None = None
    length: int | float | None = None
    attrs: dict[str, object] = field(default_factory=dict)
    children: list["_ParsedNode"] = field(default_factory=list)
    offset: int = 0


class _NewickParser:
    """Small recursive-descent Newick parser.

    It deliberately keeps parsed labels, lengths and annotations separate from the
    tree materialisation step. That allows duplicate sibling labels to be kept
    when the caller opts into it, and it gives every failure a source position.
    """

    def __init__(
        self,
        tree_string: str,
        *,
        attr_prefix: str = "&&NHX:",
        attr_sep: str = ":",
        source: str | None = None,
        start_tree_index: int = 0,
    ):
        self.tree_string = tree_string
        self.attr_prefix = attr_prefix
        self.attr_sep = attr_sep
        self.source = source
        self.start_tree_index = start_tree_index
        self.pos = 0
        self.tree_index = start_tree_index + 1
        self.unlabelled_node_counter = 0

    # ------------------------------------------------------------------
    # Position and error helpers
    # ------------------------------------------------------------------
    def _line_column(self, index: int) -> tuple[int, int]:
        line = self.tree_string.count("\n", 0, index) + 1
        last_newline = self.tree_string.rfind("\n", 0, index)
        column = index - last_newline
        return line, column

    def _error(
        self,
        message: str,
        index: int,
        *,
        label: str | None = None,
        code: object | None = None,
    ) -> exceptions.NewickError:
        line, column = self._line_column(min(index, len(self.tree_string)))
        return exceptions.NewickError(
            message,
            index=min(index, len(self.tree_string)),
            line=line,
            column=column,
            tree_index=self.tree_index,
            label=label,
            code=code,
            source=self.source,
            context=True,
        )

    def _skip_whitespace(self) -> None:
        while self.pos < len(self.tree_string) and self.tree_string[self.pos].isspace():
            self.pos += 1

    def _peek(self) -> str | None:
        if self.pos >= len(self.tree_string):
            return None
        return self.tree_string[self.pos]

    def _next_node_name(self) -> str:
        name = f"node{self.unlabelled_node_counter}"
        self.unlabelled_node_counter += 1
        return name

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------
    def parse_trees(self) -> list[_ParsedNode]:
        trees: list[_ParsedNode] = []
        while True:
            self._skip_whitespace()
            while self._peek() == ";":
                self.pos += 1
                self._skip_whitespace()
            if self._peek() is None:
                break

            self.tree_index = self.start_tree_index + len(trees) + 1
            self.unlabelled_node_counter = 0
            tree = self._parse_subtree()
            trees.append(tree)

            self._skip_whitespace()
            if self._peek() == ";":
                self.pos += 1
            elif self._peek() is not None:
                raise self._error(
                    "Expect ';' between Newick trees",
                    self.pos,
                    label=tree.name,
                )
        return trees

    def _parse_subtree(self) -> _ParsedNode:
        start = self.pos
        children: list[_ParsedNode] = []

        if self._peek() == "(":
            self.pos += 1
            while True:
                self._skip_whitespace()
                if self._peek() in (None, ")", ",", ";"):
                    raise self._error("Expect a child node in Newick tree", self.pos)
                children.append(self._parse_subtree())
                self._skip_whitespace()
                character = self._peek()
                if character == ",":
                    self.pos += 1
                    continue
                if character == ")":
                    self.pos += 1
                    break
                raise self._error("Expect ',' or ')' after Newick child", self.pos)

        parsed = _ParsedNode(children=children, offset=start)
        self._parse_label_and_decorations(parsed)
        if parsed.name is None:
            parsed.name = self._next_node_name()
        return parsed

    def _parse_label_and_decorations(self, parsed: _ParsedNode) -> None:
        self._skip_whitespace()
        if self._peek() not in (None, *tuple(_DELIMITERS)):
            label, label_offset = self._parse_label()
            parsed.name = label
            parsed.offset = label_offset

        while True:
            self._skip_whitespace()
            character = self._peek()
            if character == ":":
                self.pos += 1
                parsed.length = self._parse_length(parsed.name)
            elif character == "[":
                attrs = self._parse_annotation(parsed.name)
                parsed.attrs.update(attrs)
            else:
                break

    def _parse_label(self) -> tuple[str, int]:
        offset = self.pos
        character = self._peek()
        if character == "'":
            return self._parse_quoted(offset), offset
        if character == '"':
            raise self._error(
                "Newick labels must use single quotes, not double quotes", offset
            )

        start = self.pos
        while self.pos < len(self.tree_string):
            character = self.tree_string[self.pos]
            if character in _DELIMITERS or character.isspace():
                break
            self.pos += 1
        label = self.tree_string[start : self.pos].strip()  # noqa: E203
        if not label:
            raise self._error("Expect a Newick label", offset)
        if "'" in label:
            raise self._error("Newick label contains an unmatched single quote", offset)
        return label, offset

    def _parse_quoted(self, offset: int) -> str:
        self.pos += 1
        characters: list[str] = []
        while self.pos < len(self.tree_string):
            character = self.tree_string[self.pos]
            if character == "'":
                if (
                    self.pos + 1 < len(self.tree_string)
                    and self.tree_string[self.pos + 1] == "'"
                ):
                    characters.append("'")
                    self.pos += 2
                    continue
                self.pos += 1
                return "".join(characters)
            characters.append(character)
            self.pos += 1
        raise self._error(
            "String not properly closed, check `tree_string`",
            offset,
            label="".join(characters).rstrip(),
        )

    def _parse_length(self, label: str | None) -> int | float:
        offset = self.pos
        self._skip_whitespace()
        start = self.pos
        while self.pos < len(self.tree_string):
            character = self.tree_string[self.pos]
            if character in "(),;[]" or character.isspace():
                break
            self.pos += 1
        value = self.tree_string[start : self.pos].strip()  # noqa: E203
        if not value:
            raise self._error("Expect a branch length after ':'", offset, label=label)
        try:
            number = float(value)
        except ValueError:
            raise self._error(
                f"Invalid branch length {value!r}", offset, label=label
            ) from None
        if number.is_integer() and "." not in value and "e" not in value.lower():
            return int(number)
        return number

    def _parse_annotation(self, label: str | None) -> dict[str, object]:
        offset = self.pos
        self.pos += 1
        start = self.pos
        characters: list[str] = []
        quote = False
        while self.pos < len(self.tree_string):
            character = self.tree_string[self.pos]
            if character == "'":
                quote = not quote
            elif character == "]" and not quote:
                break
            characters.append(character)
            self.pos += 1
        if self.pos >= len(self.tree_string):
            raise self._error(
                "String not properly closed, check `tree_string`", offset, label=label
            )
        self.pos += 1  # closing ]

        content = "".join(characters)
        if self.attr_prefix and content.startswith(self.attr_prefix):
            content = content[len(self.attr_prefix) :]  # noqa: E203
        if not content.strip():
            return {}

        attrs: dict[str, object] = {}
        separator = self.attr_sep
        for item in _split_outside_quotes(content, separator):
            item = item.strip()
            if not item:
                continue
            if "=" in item:
                key, value = item.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key.startswith("'"):
                    key = _unquote(key)
                elif "'" in key:
                    raise self._error(
                        "Attribute name contains an unmatched single quote",
                        start,
                        label=label,
                    )
                if value.startswith("'"):
                    value = _unquote(value)
                elif '"' in value:
                    raise self._error(
                        "Attribute values must use single quotes, not double quotes",
                        start,
                        label=label,
                    )
                elif "'" in value:
                    raise self._error(
                        "Attribute value contains an unmatched single quote",
                        start,
                        label=label,
                    )
            else:
                key, value = "note", _unquote(item)
            if not key:
                raise self._error("Expect an attribute name", start, label=label)
            attrs[key] = value
        return attrs


def _split_outside_quotes(value: str, separator: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote = False
    index = 0
    while index < len(value):
        character = value[index]
        if character == "'":
            quote = not quote
            current.append(character)
        elif character == separator and not quote:
            parts.append("".join(current))
            current = []
        else:
            current.append(character)
        index += 1
    parts.append("".join(current))
    return parts


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _has_duplicate_siblings(parsed: _ParsedNode) -> tuple[bool, _ParsedNode | None]:
    stack = [parsed]
    while stack:
        current = stack.pop()
        names = [child.name for child in current.children]
        duplicate = {name for name, count in Counter(names).items() if count > 1}
        if duplicate:
            seen: set[str] = set()
            for child in current.children:
                assert child.name is not None
                if child.name in duplicate:
                    if child.name in seen:
                        return True, child
                    seen.add(child.name)
        stack.extend(reversed(current.children))
    return False, None


def _materialise(
    parsed: _ParsedNode,
    *,
    node_type: type[T],
    length_attr: str,
    allow_duplicate_names: bool,
    tree_index: int,
    source: str | None,
    tree_string: str,
) -> T:
    has_duplicates, duplicate_node = _has_duplicate_siblings(parsed)
    if has_duplicates and not allow_duplicate_names:
        assert duplicate_node is not None
        line = tree_string.count("\n", 0, duplicate_node.offset) + 1
        last_newline = tree_string.rfind("\n", 0, duplicate_node.offset)
        raise exceptions.NewickError(
            f"Duplicate label {duplicate_node.name!r}",
            index=duplicate_node.offset,
            line=line,
            column=duplicate_node.offset - last_newline,
            tree_index=tree_index,
            tree_label=parsed.name,
            label=duplicate_node.name,
            source=source,
            context=True,
        )

    def create(parsed_node: _ParsedNode, parent: T | None = None) -> T:
        new_node = node_type(parsed_node.name)
        attrs = dict(parsed_node.attrs)
        if parsed_node.length is not None:
            attrs[length_attr] = parsed_node.length
        if attrs:
            new_node.set_attrs(attrs)
        if parent is not None:
            from bigtree._globals import Globals

            # Duplicate labels are a deliberate feature here; bypass the
            # same-path guard only while linking, then restore the global flag.
            previous_assertions = Globals.ASSERTIONS
            Globals.ASSERTIONS = False
            try:
                new_node.parent = parent
            finally:
                Globals.ASSERTIONS = previous_assertions
        for child in parsed_node.children:
            create(child, new_node)
        return new_node

    return create(parsed)


def _validate_codes(
    parsed: _ParsedNode,
    *,
    code_attr: str | None,
    tree_index: int,
    source: str | None,
    tree_string: str,
) -> None:
    if not code_attr:
        return

    seen: dict[str, _ParsedNode] = {}
    stack = [parsed]
    while stack:
        current = stack.pop()
        if not current.children:
            code = current.attrs.get(code_attr)
            if code is not None and str(code) in seen:
                first = seen[str(code)]
                line = tree_string.count("\n", 0, current.offset) + 1
                last_newline = tree_string.rfind("\n", 0, current.offset)
                raise exceptions.NewickError(
                    f"Duplicate sample code {code!r} for label {current.name!r}; "
                    f"first seen for label {first.name!r}",
                    index=current.offset,
                    line=line,
                    column=current.offset - last_newline,
                    tree_index=tree_index,
                    tree_label=parsed.name,
                    label=current.name,
                    code=code,
                    source=source,
                    context=True,
                )
            if code is not None:
                seen[str(code)] = current
        stack.extend(reversed(current.children))


def validate_newick_codes(
    tree_string: str,
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    attr_prefix: str = "&&NHX:",
    attr_sep: str = ":",
    source: str | None = None,
    _start_tree_index: int = 0,
) -> None:
    """Validate that leaf sample codes are present and unique.

    This parses without materialising BigTree nodes so it can be used by both
    the single-tree compatibility API and the file cleaner.

    Args:
        tree_string: Newick notation
        code_attr: leaf attribute identifying a sample
        allow_duplicate_codes: allow repeated values of ``code_attr``
        attr_prefix: prefix before attributes inside square brackets
        attr_sep: separator between attributes inside square brackets
        source: source name (usually a filename) for error messages
        _start_tree_index: starting tree number for concatenated inputs
    """
    if not code_attr or allow_duplicate_codes:
        return

    parser = _NewickParser(
        tree_string,
        attr_prefix=attr_prefix,
        attr_sep=attr_sep,
        source=source,
        start_tree_index=_start_tree_index,
    )
    for tree_index, parsed in enumerate(
        parser.parse_trees(), start=_start_tree_index + 1
    ):
        _validate_codes(
            parsed,
            code_attr=code_attr,
            tree_index=tree_index,
            source=source,
            tree_string=tree_string,
        )


def newick_to_trees(
    tree_string: str,
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
    source: str | None = None,
    _start_tree_index: int = 0,
) -> list[T]:
    """Construct one or more trees from Newick text.

    This is the file/multi-tree counterpart of :func:`newick_to_tree`. It accepts
    standard Newick input (including a trailing ``;``), quoted labels containing
    punctuation such as parentheses and semicolons, internal-node annotations and
    branch lengths. Parent/child order is preserved.

    When ``code_attr`` is set, duplicate leaf values for that attribute are
    rejected with a :class:`~bigtree.utils.exceptions.NewickError` carrying the
    tree number, label, line and column. Duplicate labels are allowed by default
    when their sample codes are distinct.

    Args:
        tree_string: Newick notation, possibly containing multiple trees
        length_attr: attribute name to store branch length
        attr_prefix: prefix before attributes inside square brackets
        node_type: node type of tree to be created
        code_attr: leaf attribute identifying a sample, used for duplicate checks
        allow_duplicate_codes: allow duplicate values of ``code_attr``
        allow_duplicate_names: allow sibling nodes with the same label
        attr_sep: separator between attributes inside square brackets
        source: source name (usually a filename) for error messages
        _start_tree_index: internal starting tree number for concatenated inputs

    Returns:
        List of root nodes, one per Newick tree
    """
    if not tree_string or not tree_string.strip():
        raise ValueError("Tree string does not contain any data, check `tree_string`")

    parser = _NewickParser(
        tree_string,
        attr_prefix=attr_prefix,
        attr_sep=attr_sep,
        source=source,
        start_tree_index=_start_tree_index,
    )
    parsed_trees = parser.parse_trees()
    if not parsed_trees:
        raise ValueError("Tree string does not contain any data, check `tree_string`")

    trees: list[T] = []
    for tree_index, parsed in enumerate(parsed_trees, start=_start_tree_index + 1):
        if not allow_duplicate_codes:
            _validate_codes(
                parsed,
                code_attr=code_attr,
                tree_index=tree_index,
                source=source,
                tree_string=tree_string,
            )
        trees.append(
            _materialise(
                parsed,
                node_type=node_type,
                length_attr=length_attr,
                allow_duplicate_names=allow_duplicate_names,
                tree_index=tree_index,
                source=source,
                tree_string=tree_string,
            )
        )
    return trees


def _read_file(filepath: str | os.PathLike[str]) -> tuple[str, str]:
    path = os.fspath(filepath)
    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            return path, file_handle.read()
    except UnicodeDecodeError:
        with open(path, "rb") as file_handle:
            return path, file_handle.read().decode("utf-8-sig")


def newick_files_to_trees(
    filepaths: Iterable[str | os.PathLike[str]],
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
    _start_tree_index: int = 0,
) -> list[T]:
    """Construct trees from one or more Newick files through one interface.

    File order is preserved. Tree numbers in errors are global across the input
    files, and errors also report the originating filename.

    Args:
        filepaths: Newick file paths
        length_attr: attribute name to store branch length
        attr_prefix: prefix before attributes inside square brackets
        node_type: node type of tree to be created
        code_attr: leaf attribute identifying a sample
        allow_duplicate_codes: allow duplicate ``code_attr`` values
        allow_duplicate_names: allow sibling nodes with the same label
        attr_sep: separator between attributes inside square brackets

    Returns:
        List of root nodes, one per Newick tree
    """
    if isinstance(filepaths, (str, os.PathLike)):
        filepaths = [filepaths]
    filepaths = list(filepaths)
    if not filepaths:
        raise ValueError("Path list does not contain any data, check `filepaths`")

    trees: list[T] = []
    tree_index = _start_tree_index
    for filepath in filepaths:
        source, content = _read_file(filepath)
        if not content.strip():
            raise ValueError(
                f"Tree string does not contain any data, check source {source}"
            )
        file_trees = newick_to_trees(
            content,
            length_attr=length_attr,
            attr_prefix=attr_prefix,
            node_type=node_type,
            code_attr=code_attr,
            allow_duplicate_codes=allow_duplicate_codes,
            allow_duplicate_names=allow_duplicate_names,
            attr_sep=attr_sep,
            source=source,
            _start_tree_index=tree_index,
        )
        tree_index += len(file_trees)
        trees.extend(file_trees)
    return trees


def newick_file_to_trees(
    filepath: str | os.PathLike[str],
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
) -> list[T]:
    """Construct trees from a single Newick file, returning a list of roots."""
    return newick_files_to_trees(
        [filepath],
        length_attr=length_attr,
        attr_prefix=attr_prefix,
        node_type=node_type,
        code_attr=code_attr,
        allow_duplicate_codes=allow_duplicate_codes,
        allow_duplicate_names=allow_duplicate_names,
        attr_sep=attr_sep,
    )


def read_newick(
    source: str | os.PathLike[str] | Iterable[str | os.PathLike[str]],
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
) -> list[T]:
    """Read Newick from filenames, file contents, or a mixture of both.

    This is a convenience dispatcher over :func:`newick_files_to_trees` and
    :func:`newick_to_trees`. A string is treated as a filename when it names an
    existing file; otherwise it is treated as Newick text.
    """
    if isinstance(source, os.PathLike):
        return newick_files_to_trees(
            [source],
            length_attr=length_attr,
            attr_prefix=attr_prefix,
            node_type=node_type,
            code_attr=code_attr,
            allow_duplicate_codes=allow_duplicate_codes,
            allow_duplicate_names=allow_duplicate_names,
            attr_sep=attr_sep,
        )

    if isinstance(source, str):
        if os.path.isfile(source):
            return newick_files_to_trees(
                [source],
                length_attr=length_attr,
                attr_prefix=attr_prefix,
                node_type=node_type,
                code_attr=code_attr,
                allow_duplicate_codes=allow_duplicate_codes,
                allow_duplicate_names=allow_duplicate_names,
                attr_sep=attr_sep,
            )
        return newick_to_trees(
            source,
            length_attr=length_attr,
            attr_prefix=attr_prefix,
            node_type=node_type,
            code_attr=code_attr,
            allow_duplicate_codes=allow_duplicate_codes,
            allow_duplicate_names=allow_duplicate_names,
            attr_sep=attr_sep,
        )

    items = list(source)
    if not items:
        raise ValueError("Path list does not contain any data, check `source`")
    if all(
        isinstance(item, os.PathLike) or os.path.isfile(str(item)) for item in items
    ):
        return newick_files_to_trees(
            items,
            length_attr=length_attr,
            attr_prefix=attr_prefix,
            node_type=node_type,
            code_attr=code_attr,
            allow_duplicate_codes=allow_duplicate_codes,
            allow_duplicate_names=allow_duplicate_names,
            attr_sep=attr_sep,
        )

    trees: list[T] = []
    tree_index = 0
    for item in items:
        if isinstance(item, os.PathLike) or os.path.isfile(str(item)):
            item_trees = newick_files_to_trees(
                [item],
                length_attr=length_attr,
                attr_prefix=attr_prefix,
                node_type=node_type,
                code_attr=code_attr,
                allow_duplicate_codes=allow_duplicate_codes,
                allow_duplicate_names=allow_duplicate_names,
                attr_sep=attr_sep,
                _start_tree_index=tree_index,
            )
        else:
            item_trees = newick_to_trees(
                str(item),
                length_attr=length_attr,
                attr_prefix=attr_prefix,
                node_type=node_type,
                code_attr=code_attr,
                allow_duplicate_codes=allow_duplicate_codes,
                allow_duplicate_names=allow_duplicate_names,
                attr_sep=attr_sep,
                _start_tree_index=tree_index,
            )
        tree_index += len(item_trees)
        trees.extend(item_trees)
    return trees


def clean_newick(
    source: str | os.PathLike[str] | Iterable[str | os.PathLike[str]],
    *additional_sources: str | os.PathLike[str],
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
) -> list[T]:
    """Clean one or more Newick inputs into BigTree nodes.

    This is the single entry point intended for files coming from different
    producers. It accepts filenames, raw Newick text, or a list mixing both, and
    returns roots in input order. Multiple filenames can be passed either as a
    list or as positional arguments, e.g. ``clean_newick("a.nwk", "b.nwk")``.

    Examples:
        >>> from bigtree import clean_newick
        >>> roots = clean_newick(["lab_a.nwk", "lab_b.nwk"])
        >>> quoted = clean_newick(
        ...     "(('A(1);x':1.2[&&NHX:code=A7],'B;2':0)inner[&&NHX:note=n]:3.4,C)r;"
        ... )[0]
        >>> [(node.node_name, node.get_attr("code")) for node in quoted.leaves]
        [('A(1);x', 'A7'), ('B;2', None), ('C', None)]

    Args:
        source: filename, raw Newick text, or a list mixing both
        additional_sources: additional filenames or raw Newick text
        length_attr: attribute name to store branch length
        attr_prefix: prefix before attributes inside square brackets
        node_type: node type of tree to be created
        code_attr: leaf attribute identifying a sample; duplicate values raise
            `NewickError` unless `allow_duplicate_codes=True`
        allow_duplicate_codes: allow duplicate ``code_attr`` values
        allow_duplicate_names: allow sibling nodes with the same label
        attr_sep: separator between attributes inside square brackets

    Returns:
        List of root nodes, one per Newick tree
    """
    if additional_sources:
        if isinstance(source, (str, os.PathLike)):
            source = [source, *additional_sources]
        else:
            source = [*source, *additional_sources]
    return read_newick(
        source,
        length_attr=length_attr,
        attr_prefix=attr_prefix,
        node_type=node_type,
        code_attr=code_attr,
        allow_duplicate_codes=allow_duplicate_codes,
        allow_duplicate_names=allow_duplicate_names,
        attr_sep=attr_sep,
    )


def clean_newick_file(
    filepath: str | os.PathLike[str],
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
) -> list[T]:
    """Clean a single Newick file, returning a list of root nodes."""
    return newick_file_to_trees(
        filepath,
        length_attr=length_attr,
        attr_prefix=attr_prefix,
        node_type=node_type,
        code_attr=code_attr,
        allow_duplicate_codes=allow_duplicate_codes,
        allow_duplicate_names=allow_duplicate_names,
        attr_sep=attr_sep,
    )


def clean_newick_files(
    filepaths: Sequence[str | os.PathLike[str]],
    length_attr: str = "length",
    attr_prefix: str = "&&NHX:",
    node_type: type[T] = node.Node,  # type: ignore[assignment]
    *,
    code_attr: str | None = "code",
    allow_duplicate_codes: bool = False,
    allow_duplicate_names: bool = True,
    attr_sep: str = ":",
) -> list[T]:
    """Clean multiple Newick files with shared settings and global error numbers."""
    return newick_files_to_trees(
        filepaths,
        length_attr=length_attr,
        attr_prefix=attr_prefix,
        node_type=node_type,
        code_attr=code_attr,
        allow_duplicate_codes=allow_duplicate_codes,
        allow_duplicate_names=allow_duplicate_names,
        attr_sep=attr_sep,
    )


read_newick_file = clean_newick_file
read_newick_files = clean_newick_files
