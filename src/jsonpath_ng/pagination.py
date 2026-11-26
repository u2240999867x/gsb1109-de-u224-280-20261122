"""
Resumable, paginated JSONPath queries.

The regular ``JSONPath.find()`` API materializes every match at once.  The
:class:`PagedFind` class in this module instead produces matches a page at a
time, in exactly the order and with the same multiplicity as a complete
``find()`` call, while keeping a serializable *receipt* that lets a different
process continue the query.

Only ordinary field, array (index/slice/wildcard), and descendant traversal is
supported.  Paths that rely on filters, unions, parents, sorting, or other
context-sensitive constructs raise :class:`UnsupportedPathError`.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import typing

from jsonpath_ng import jsonpath as _jsonpath
from jsonpath_ng.exceptions import JSONPathError
from jsonpath_ng.jsonpath import Child
from jsonpath_ng.jsonpath import DatumInContext
from jsonpath_ng.jsonpath import Descendants
from jsonpath_ng.jsonpath import Fields
from jsonpath_ng.jsonpath import Index
from jsonpath_ng.jsonpath import JSONPath
from jsonpath_ng.jsonpath import Root
from jsonpath_ng.jsonpath import Slice
from jsonpath_ng.jsonpath import This
from jsonpath_ng.jsonpath import Wildcard

if typing.TYPE_CHECKING:
    from pathlib import Path

# A reference to a node in the queried document:
# strings name object members, integers name array elements.
PathPart: typing.TypeAlias = str | int
NodePath: typing.TypeAlias = tuple[PathPart, ...]
# A reference to a node in the JSONPath expression tree:
# 0 is ``.left``, 1 is ``.right``.
ExprPath: typing.TypeAlias = tuple[int, ...]

_RECEIPT_VERSION = 1
_FINGERPRINT_ALGORITHM = "sha256-ordered-json"

# Expressions whose traversal can be reduced to structural DFS and selectors.
_SUPPORTED_EXPRESSIONS = (
    Root,
    This,
    Child,
    Descendants,
    Fields,
    Wildcard,
    Index,
    Slice,
)
# Expressions that match the children of a single reached node.
_SELECTOR_EXPRESSIONS = (Fields, Wildcard, Index, Slice)


class PaginationError(JSONPathError):
    """Base class for errors raised by paginated queries."""


class UnsupportedPathError(PaginationError):
    """Raised when an expression cannot be paginated deterministically."""


class InvalidCursorError(PaginationError):
    """Raised when a receipt is malformed."""


class SourceUnavailableError(PaginationError):
    """Raised when the original JSON document cannot be opened or parsed."""


class CursorMismatchError(PaginationError):
    """Base class for refusals to continue a query with mismatched inputs."""


class DataChangedError(CursorMismatchError):
    """Raised when the reopened document is not the version a receipt belongs to."""


class CursorQueryMismatchError(CursorMismatchError):
    """Raised when a receipt is continued with a different expression."""


def fingerprint_data(data: typing.Any) -> str:
    """
    Return a stable fingerprint of a JSON document.

    Object key order and array order are both part of the fingerprint,
    because descendant traversal follows that order.  The fingerprint only
    covers values representable in JSON.
    """

    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(payload.encode("utf-8"))
    return digest.hexdigest()


def _validate_expression(expression: JSONPath) -> None:
    """Reject expressions whose semantics cannot be resumed from a frontier."""

    pending = [expression]
    while pending:
        node = pending.pop()
        if not isinstance(node, _SUPPORTED_EXPRESSIONS):
            msg = (
                f"Paginated queries do not support {node!r}. "
                "Use only fields, indices, slices, wildcards, and descendant traversal."
            )
            raise UnsupportedPathError(msg)
        # Deprecated wildcard modes depend on runtime data shapes and warnings,
        # which makes a persisted frontier ambiguous.
        if isinstance(node, (Fields, Slice)) and node._deprecated_wildcard_use:
            msg = (
                "Paginated queries do not support the deprecated wildcard "
                f"use in {node!r}."
            )
            raise UnsupportedPathError(msg)
        if isinstance(node, (Child, Descendants)):
            pending.append(node.right)
            pending.append(node.left)


def _resolve_node(expression: JSONPath, expr_path: ExprPath) -> JSONPath:
    """Walk ``0``/``1`` choices to locate a sub-expression."""

    node = expression
    for choice in expr_path:
        if not isinstance(node, (Child, Descendants)):
            raise InvalidCursorError("Receipt references a node that is not composite.")
        node = node.left if choice == 0 else node.right
    return node


def _datum_at(data: typing.Any, node_path: NodePath) -> DatumInContext:
    """Build a ``DatumInContext`` with the real parent chain for a node."""

    datum = DatumInContext(data, path=Root(), context=None)
    value = data
    for part in node_path:
        if isinstance(part, int):
            value = value[part]
            datum = DatumInContext(value, path=Index(part), context=datum)
        else:
            value = value[part]
            datum = DatumInContext(
                value, path=Fields._compliant_constructor(part), context=datum
            )
    return datum


def _path_of(datum: DatumInContext) -> NodePath:
    """Extract the document path from a datum's context chain."""

    parts: list[PathPart] = []
    current: DatumInContext | None = datum
    while current is not None and not isinstance(current.path, (Root, This)):
        path = current.path
        if isinstance(path, Index):
            parts.append(path.indices[0])
        elif isinstance(path, Fields):
            parts.append(path.fields[0])
        else:
            raise UnsupportedPathError(
                f"Cannot persist a traversal position with path {path!r}."
            )
        current = current.context
    parts.reverse()
    return tuple(parts)


def _child_paths(value: typing.Any) -> list[NodePath]:
    """Paths of the direct children of a node, in natural traversal order."""

    if isinstance(value, list):
        return [(index,) for index in range(len(value))]
    if isinstance(value, dict):
        return [(key,) for key in value]
    return []


def _path_resolves(data: typing.Any, node_path: NodePath) -> bool:
    """Whether every part of ``node_path`` exists in ``data``."""

    value = data
    for part in node_path:
        try:
            if isinstance(part, int):
                if not isinstance(value, list) or not -len(value) <= part < len(value):
                    return False
            elif not isinstance(value, dict) or part not in value:
                return False
            value = value[part]
        except (TypeError, KeyError, IndexError):
            return False
    return True


@dataclasses.dataclass
class _Frame:
    """One suspended traversal step.

    An ``eval`` frame evaluates ``node`` at ``node_path`` and delivers the
    matched paths to ``sink`` (``-1`` means the query output, otherwise the
    stack index of a continuation frame).  Continuation frames drive the
    ``Child``/``Descendants`` loops: ``queue`` holds input paths or the DFS
    frontier, and ``incoming`` collects the result of the latest child eval.

    Only not-yet-visited nodes ever sit on the frontier, so a subtree that was
    fully visited on an earlier page is never walked again.
    """

    node: JSONPath
    expr_path: ExprPath
    node_path: NodePath
    mode: str  # "eval", "child", or "desc"
    phase: int = 1
    sink: int = -1
    queue: list[NodePath] = dataclasses.field(default_factory=list)
    incoming: list[NodePath] = dataclasses.field(default_factory=list)

    def to_json(self) -> list[typing.Any]:
        return [
            list(self.expr_path),
            list(self.node_path),
            self.mode,
            self.phase,
            self.sink,
            [list(path) for path in self.queue],
            [list(path) for path in self.incoming],
        ]

    @classmethod
    def from_json(cls, expression: JSONPath, raw: typing.Any) -> _Frame:
        if not isinstance(raw, list) or len(raw) != 7:
            raise InvalidCursorError("Each stack frame must be a 7-element list.")
        (
            raw_expr_path,
            raw_node_path,
            raw_mode,
            raw_phase,
            raw_sink,
            raw_queue,
            raw_incoming,
        ) = raw
        if not isinstance(raw_expr_path, list) or not all(
            isinstance(choice, int) and choice in (0, 1) for choice in raw_expr_path
        ):
            raise InvalidCursorError("Invalid expression reference in receipt.")
        if not isinstance(raw_node_path, list) or not all(
            isinstance(part, (str, int)) for part in raw_node_path
        ):
            raise InvalidCursorError("Invalid document path in receipt.")
        if raw_mode not in ("eval", "child", "desc"):
            raise InvalidCursorError("Invalid frame mode in receipt.")
        if not isinstance(raw_phase, int) or raw_phase < 1:
            raise InvalidCursorError("Invalid frame phase in receipt.")
        if not isinstance(raw_sink, int) or raw_sink < -1:
            raise InvalidCursorError("Invalid frame sink in receipt.")
        for label, value in (("queue", raw_queue), ("incoming", raw_incoming)):
            if not isinstance(value, list) or not all(
                isinstance(path, list)
                and all(isinstance(part, (str, int)) for part in path)
                for path in value
            ):
                raise InvalidCursorError(f"Invalid frame {label} in receipt.")
        expr_path = tuple(raw_expr_path)
        node = _resolve_node(expression, expr_path)
        if raw_mode == "child" and not isinstance(node, Child):
            raise InvalidCursorError("Child frame does not reference a Child node.")
        if raw_mode == "desc" and not isinstance(node, Descendants):
            raise InvalidCursorError(
                "Descendant frame does not reference a Descendants node."
            )
        return cls(
            node=node,
            expr_path=expr_path,
            node_path=tuple(raw_node_path),
            mode=raw_mode,
            phase=raw_phase,
            sink=raw_sink,
            queue=[tuple(path) for path in raw_queue],
            incoming=[tuple(path) for path in raw_incoming],
        )


class _Engine:
    """
    Lazy depth-first traversal matching ``JSONPath.find()`` ordering.

    Leaf selectors deliver their matches straight to the query output as soon
    as they run, so :meth:`next_match` visits only the nodes needed to produce
    the next match.  The frame stack and its queues are the entire state, which
    makes the traversal serializable and resumable without re-walking any
    subtree that was already fully visited.
    """

    def __init__(
        self,
        expression: JSONPath,
        data: typing.Any,
        frames: list[_Frame] | None = None,
        root_results: list[NodePath] | None = None,
    ) -> None:
        self._expression = expression
        self._data = data
        self._stack: list[_Frame] = frames if frames is not None else [
            _Frame(expression, (), (), mode="eval")
        ]
        self._root_results: list[NodePath] = (
            root_results if root_results is not None else []
        )
        # A standalone ``@`` keeps the ``this`` path; nested ``This()`` nodes
        # are identities and reconstruct the reached node's ordinary path.
        self._bare_this = isinstance(expression, This)

    @property
    def exhausted(self) -> bool:
        return not self._stack and not self._root_results

    def snapshot(self) -> dict[str, typing.Any]:
        return {
            "stack": [frame.to_json() for frame in self._stack],
            "results": [list(path) for path in self._root_results],
        }

    def next_match(self) -> DatumInContext | None:
        while self._stack and not self._root_results:
            self._step()
        if not self._root_results:
            return None
        datum = _datum_at(self._data, self._root_results.pop(0))
        if self._bare_this:
            datum = DatumInContext(datum.value, path=This(), context=None)
        return datum

    def _deliver(self, sink: int, paths: list[NodePath]) -> None:
        if sink == -1:
            self._root_results.extend(paths)
        else:
            self._stack[sink].incoming.extend(paths)

    def _step(self) -> None:
        frame = self._stack[-1]
        if frame.mode == "eval":
            self._eval(frame)
        elif frame.mode == "child":
            self._continue_child(typing.cast(Child, frame.node), frame)
        else:
            self._continue_descendants(typing.cast(Descendants, frame.node), frame)

    def _eval(self, frame: _Frame) -> None:
        node = frame.node
        if isinstance(node, (Child, Descendants)):
            # Replace the eval frame with a continuation driven by the left
            # sub-expression, then evaluate that left side into it.
            self._stack.pop()
            mode = "child" if isinstance(node, Child) else "desc"
            continuation = _Frame(
                node=node,
                expr_path=frame.expr_path,
                node_path=frame.node_path,
                mode=mode,
                phase=1,
                sink=frame.sink,
            )
            self._stack.append(continuation)
            target = len(self._stack) - 1
            self._stack.append(
                _Frame(
                    node.left,
                    frame.expr_path + (0,),
                    frame.node_path,
                    mode="eval",
                    sink=target,
                )
            )
            return

        if isinstance(node, (Root, This)):
            matched = [frame.node_path]
        elif isinstance(node, _SELECTOR_EXPRESSIONS):
            datum = _datum_at(self._data, frame.node_path)
            matched = [_path_of(match) for match in node.find(datum)]
        else:  # pragma: no cover - validated before traversal
            raise UnsupportedPathError(f"Paginated queries do not support {node!r}.")
        self._stack.pop()
        self._deliver(frame.sink, matched)

    def _continue_child(self, node: Child, frame: _Frame) -> None:
        incoming = frame.incoming
        frame.incoming = []
        if frame.phase == 1:
            if not incoming:
                self._stack.pop()
                return
            frame.queue = list(incoming)
            frame.phase = 2
        else:
            # The right side finished for one left-hand input.
            self._deliver(frame.sink, incoming)

        if not frame.queue:
            self._stack.pop()
            return
        next_path = frame.queue.pop(0)
        self._stack.append(
            _Frame(
                node.right,
                frame.expr_path + (1,),
                next_path,
                mode="eval",
                sink=self._stack.index(frame),
            )
        )

    def _continue_descendants(self, node: Descendants, frame: _Frame) -> None:
        if frame.phase == 1:
            # The left side finished; seed the DFS frontier and enter the
            # scheduling stage.
            incoming = frame.incoming
            frame.incoming = []
            if not incoming:
                self._stack.pop()
                return
            frame.queue = list(reversed(incoming))
            frame.phase = 2

        if frame.phase == 2:
            # Take the next DFS node and evaluate the right side on it.
            if not frame.queue:
                self._stack.pop()
                return
            frame.node_path = frame.queue.pop()
            frame.phase = 3
            self._stack.append(
                _Frame(
                    node.right,
                    frame.expr_path + (1,),
                    frame.node_path,
                    mode="eval",
                    sink=self._stack.index(frame),
                )
            )
            return

        # phase == 3: the right side just finished at ``frame.node_path``.
        incoming = frame.incoming
        frame.incoming = []
        self._deliver(frame.sink, incoming)
        datum = _datum_at(self._data, frame.node_path)
        children = _child_paths(datum.value)
        # Extend in reverse so the children leave the queue in natural order.
        frame.queue.extend(
            frame.node_path + child_path for child_path in reversed(children)
        )
        frame.phase = 2


def _load_json(source: str | Path) -> typing.Any:
    try:
        with open(source, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
        raise SourceUnavailableError(
            f"Could not open the original JSON document at {source!r}."
        ) from exc


class PagedFind:
    """
    A resumable, paginated JSONPath query.

    The order and multiplicity of the returned :class:`DatumInContext` objects
    match a complete ``expression.find(data)`` call for the same document.
    Each returned datum keeps its real parent context.

    A :attr:`receipt` is available after every page and can be persisted.  Pass
    it to :meth:`resume` in another process together with the same document and
    the same expression to continue the query.
    """

    def __init__(
        self,
        data: typing.Any,
        expression: JSONPath,
        *,
        limit: int | None = None,
        receipt: dict[str, typing.Any] | None = None,
    ) -> None:
        _validate_expression(expression)
        if _jsonpath.auto_id_field is not None:
            msg = (
                "Paginated queries cannot be resumed while "
                f"auto_id_field={_jsonpath.auto_id_field!r} is set, because "
                "auto-id datum values have no document path to persist."
            )
            raise UnsupportedPathError(msg)
        self._expression = expression
        self._data = data
        self._fingerprint = fingerprint_data(data)

        if receipt is None:
            self._emitted = 0
            self._engine = _Engine(expression, data)
        else:
            self._emitted = self._resume_state(expression, receipt)

        self._limit = limit

    @classmethod
    def from_file(
        cls,
        source: str | Path,
        expression: JSONPath,
        *,
        limit: int | None = None,
    ) -> PagedFind:
        """Open a JSON document and start a fresh paginated query."""

        return cls(_load_json(source), expression, limit=limit)

    @classmethod
    def resume(
        cls,
        data: typing.Any,
        expression: JSONPath,
        receipt: dict[str, typing.Any],
        *,
        limit: int | None = None,
    ) -> PagedFind:
        """Continue a query using a receipt produced by an earlier process."""

        return cls(data, expression, limit=limit, receipt=receipt)

    @classmethod
    def resume_from_file(
        cls,
        source: str | Path,
        expression: JSONPath,
        receipt: dict[str, typing.Any],
        *,
        limit: int | None = None,
    ) -> PagedFind:
        """Reopen a JSON document and continue a query from a receipt."""

        return cls.resume(_load_json(source), expression, receipt, limit=limit)

    @property
    def expression(self) -> JSONPath:
        return self._expression

    @property
    def emitted(self) -> int:
        """Number of matches already returned."""

        return self._emitted

    @property
    def exhausted(self) -> bool:
        """Whether every match has been returned."""

        return self._engine.exhausted

    @property
    def receipt(self) -> dict[str, typing.Any]:
        """A JSON-serializable receipt that captures the current position."""

        return {
            "version": _RECEIPT_VERSION,
            "fingerprint_algorithm": _FINGERPRINT_ALGORITHM,
            "fingerprint": self._fingerprint,
            "expression": str(self._expression),
            "emitted": self._emitted,
            "state": self._engine.snapshot(),
        }

    def save_receipt(self, target: str | Path) -> None:
        """Write the receipt to ``target`` as JSON."""

        with open(target, "w", encoding="utf-8") as handle:
            json.dump(self.receipt, handle)

    @staticmethod
    def load_receipt(target: str | Path) -> dict[str, typing.Any]:
        """Load a receipt previously written with :meth:`save_receipt`."""

        try:
            with open(target, encoding="utf-8") as handle:
                raw = json.load(handle)
        except (OSError, json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise InvalidCursorError(
                f"Could not read the receipt at {target!r}."
            ) from exc
        if not isinstance(raw, dict):
            raise InvalidCursorError("The receipt must be a JSON object.")
        return raw

    def fetch(self, limit: int | None = None) -> list[DatumInContext]:
        """
        Return up to ``limit`` matches and advance the position.

        ``limit`` defaults to the page size given when the query was created.
        Duplicate matches are counted individually, just like ``find()``.
        """

        page_size = limit if limit is not None else self._limit
        if page_size is None:
            raise ValueError("A positive page limit is required.")
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or page_size <= 0
        ):
            raise ValueError("The page limit must be a positive integer.")

        matches: list[DatumInContext] = []
        for _ in range(page_size):
            match = self._engine.next_match()
            if match is None:
                break
            matches.append(match)
        self._emitted += len(matches)
        return matches

    def _resume_state(
        self, expression: JSONPath, receipt: dict[str, typing.Any]
    ) -> int:
        if not isinstance(receipt, dict):
            raise InvalidCursorError("The receipt must be a JSON object.")
        if receipt.get("version") != _RECEIPT_VERSION:
            raise InvalidCursorError("Unsupported receipt version.")
        if receipt.get("fingerprint_algorithm") != _FINGERPRINT_ALGORITHM:
            raise InvalidCursorError("Unsupported fingerprint algorithm in receipt.")

        stored_expression = receipt.get("expression")
        if (
            not isinstance(stored_expression, str)
            or stored_expression != str(expression)
        ):
            raise CursorQueryMismatchError(
                "The receipt was created for expression "
                f"{stored_expression!r}, not {str(expression)!r}."
            )

        stored_fingerprint = receipt.get("fingerprint")
        if not isinstance(stored_fingerprint, str):
            raise InvalidCursorError("Invalid fingerprint in receipt.")
        current_fingerprint = fingerprint_data(self._data)
        if stored_fingerprint != current_fingerprint:
            raise DataChangedError(
                "The reopened document does not match the version "
                "the receipt was created from."
            )

        emitted = receipt.get("emitted")
        if not isinstance(emitted, int) or emitted < 0:
            raise InvalidCursorError("Invalid emitted count in receipt.")

        state = receipt.get("state")
        if not isinstance(state, dict) or not isinstance(state.get("stack"), list):
            raise InvalidCursorError("Invalid traversal state in receipt.")
        if not isinstance(state.get("results"), list) or not all(
            isinstance(path, list)
            and all(isinstance(part, (str, int)) for part in path)
            for path in state["results"]
        ):
            raise InvalidCursorError("Invalid buffered results in receipt.")
        frames = [_Frame.from_json(expression, raw) for raw in state["stack"]]
        root_results = [tuple(path) for path in state["results"]]
        stored_paths: typing.Iterable[NodePath] = [
            *root_results,
            *(
                path
                for frame in frames
                for path in (frame.node_path, *frame.queue, *frame.incoming)
            ),
        ]
        if not all(_path_resolves(self._data, path) for path in stored_paths):
            raise InvalidCursorError(
                "The receipt references a document path that no longer exists."
            )
        self._engine = _Engine(expression, self._data, frames, root_results)
        return emitted
