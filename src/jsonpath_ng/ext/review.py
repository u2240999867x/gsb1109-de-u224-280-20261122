#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Review sessions that keep sorted-list hits stable across concurrent edits.

A :class:`ReviewSession` gives every occurrence in a JSON document a stable
identity. A reviewer finds occurrences with the existing JSONPath expressions
(including the ``[/field]`` sort extension) and later submits replacements or
deletions through the same session. Because hits identify occurrences rather
than re-running a stale query, edits made in the meantime cannot cause a hit to
be written back to the wrong row or to the wrong duplicate value.

Only edits submitted through a review session are tracked. Plain queries and
direct edits outside a session keep their existing behavior.
"""

from __future__ import annotations

import dataclasses
import functools
import json
import re
import typing

from jsonpath_ng import Child
from jsonpath_ng import DatumInContext
from jsonpath_ng import Fields
from jsonpath_ng import Index
from jsonpath_ng import JSONPath
from jsonpath_ng import Root
from jsonpath_ng import Slice
from jsonpath_ng import This
from jsonpath_ng import Wildcard
from jsonpath_ng.ext.filter import Filter
from jsonpath_ng.ext.iterable import SortedThis
from jsonpath_ng.ext.parser import parse
from jsonpath_ng.jsonpath import Descendants
from jsonpath_ng.jsonpath import Where
from jsonpath_ng.jsonpath import WhereNot
from jsonpath_ng.jsonpath import _Union

_Key: typing.TypeAlias = str | int
_Children: typing.TypeAlias = list[int] | dict[str, int]


class ReviewError(Exception):
    """Base class for errors raised by review sessions."""


class StaleHitError(ReviewError):
    """Raised when a submitted hit no longer identifies an occurrence."""

    def __init__(self, hit: ReviewHit, message: str | None = None) -> None:
        self.hit = hit
        super().__init__(
            message
            or (
                "the reviewed occurrence at "
                f"{hit.original_path} no longer exists in the document"
            )
        )


@dataclasses.dataclass(frozen=True)
class ReviewHit:
    """A single occurrence captured by :meth:`ReviewSession.find`.

    The hit remembers the expression that matched it, the rank it held inside
    any sorted list, the path at which it originally appeared, and the value it
    had when captured. Its ``token_id`` is the stable occurrence identity used
    when the ticket is submitted later.
    """

    expression: str
    token_id: int
    original_path: str
    rank: int | None
    value: typing.Any


@dataclasses.dataclass(frozen=True)
class ReviewAction:
    """A pending replacement or deletion of a hit."""

    hit: ReviewHit
    value: typing.Any
    delete: bool

    @classmethod
    def replace(cls, hit: ReviewHit, value: typing.Any) -> ReviewAction:
        return cls(hit, value, False)

    @classmethod
    def remove(cls, hit: ReviewHit) -> ReviewAction:
        return cls(hit, None, True)


@dataclasses.dataclass(frozen=True)
class ReviewHitStatus:
    """Whether a captured hit still points at the same occurrence."""

    found: bool
    path: str | None
    value: typing.Any


@dataclasses.dataclass
class _Token:
    """One stable occurrence, including slots that hold scalar values."""

    id: int
    parent_id: int | None
    key: _Key | None
    children: _Children | None
    value: typing.Any
    # Synthetic tokens back a sorted-list view. They point at the real list
    # token they order, and are never the target of a submitted action.
    origin_id: int | None = None


class _IdentityStore:
    """Maps every document occurrence to a stable token id.

    Each document slot is a token owned by its parent container. Deleting a
    sibling removes one token and leaves the surviving tokens pointing at the
    same occurrences even though their indices shift. Re-tokenization on
    replacement keeps the token of the replaced slot itself stable.
    """

    def __init__(self, document: typing.Any) -> None:
        self._tokens: dict[int, _Token] = {}
        self._next_id = 1
        self.root_id = self._tokenize(document, None, None)

    def _tokenize(
        self, value: typing.Any, parent_id: int | None, key: _Key | None
    ) -> int:
        token_id = self._next_id
        self._next_id += 1

        children: _Children | None = None
        if isinstance(value, list):
            children = []
            for index, child in enumerate(value):
                children.append(self._tokenize(child, token_id, index))
        elif isinstance(value, dict):
            children = {}
            for child_key, child in value.items():
                children[child_key] = self._tokenize(child, token_id, child_key)

        self._tokens[token_id] = _Token(
            token_id, parent_id, key, children, value
        )
        return token_id

    def make_sorted_view(
        self, token_id: int, ordered_ids: list[int]
    ) -> int:
        """Create an ephemeral token whose children are in sorted order."""

        view_id = self._next_id
        self._next_id += 1
        ordered_value = [
            self._tokens[child_id].value for child_id in ordered_ids
        ]
        self._tokens[view_id] = _Token(
            id=view_id,
            parent_id=token_id,
            key=None,
            children=list(ordered_ids),
            value=ordered_value,
            origin_id=token_id,
        )
        return view_id

    def is_synthetic(self, token_id: int) -> bool:
        return self._tokens[token_id].origin_id is not None

    def clear_views(self) -> None:
        """Discard every sorted-view token created during one query."""

        for view_id in [
            token_id
            for token_id, token in self._tokens.items()
            if token.origin_id is not None
        ]:
            self._tokens.pop(view_id, None)

    def origin(self, token_id: int) -> int:
        token = self._tokens[token_id]
        return token.origin_id if token.origin_id is not None else token_id

    def is_alive(self, token_id: int) -> bool:
        return token_id in self._tokens

    def value(self, token_id: int) -> typing.Any:
        return self._tokens[token_id].value

    def child_token(self, token_id: int, key: _Key) -> int | None:
        children = self._tokens[token_id].children
        if isinstance(children, list):
            if isinstance(key, int) and -len(children) <= key < len(children):
                return children[key]
            return None
        if isinstance(children, dict):
            return children.get(key) if isinstance(key, str) else None
        return None

    def child_ids(self, token_id: int) -> list[int]:
        children = self._tokens[token_id].children
        if isinstance(children, list):
            return list(children)
        if isinstance(children, dict):
            return list(children.values())
        return []

    def child_pairs(self, token_id: int) -> list[tuple[_Key, int]]:
        children = self._tokens[token_id].children
        if isinstance(children, list):
            return list(enumerate(children))
        if isinstance(children, dict):
            return list(children.items())
        return []

    def path_of(self, token_id: int) -> str:
        keys: list[_Key] = []
        current = token_id
        while self._tokens[current].parent_id is not None:
            token = self._tokens[current]
            keys.append(typing.cast(_Key, token.key))
            current = typing.cast(int, token.parent_id)
        keys.reverse()
        return _render_chain(tuple(keys))

    def depth(self, token_id: int) -> int:
        depth = 0
        current = token_id
        while self._tokens[current].parent_id is not None:
            current = typing.cast(int, self._tokens[current].parent_id)
            depth += 1
        return depth

    def native_datum(self, token_id: int) -> DatumInContext:
        """Build a context-linked datum for filters evaluated on a token."""

        keys: list[_Key] = []
        current = token_id
        while self._tokens[current].parent_id is not None:
            keys.append(typing.cast(_Key, self._tokens[current].key))
            current = typing.cast(int, self._tokens[current].parent_id)
        keys.reverse()

        datum = DatumInContext(
            self._tokens[self.root_id].value, path=Root(), context=None
        )
        current_value: typing.Any = datum.value
        for key in keys:
            current_value = current_value[key]
            datum = DatumInContext(
                current_value, path=_key_path(key), context=datum
            )
        return datum

    def replace(self, token_id: int, value: typing.Any) -> None:
        token = self._tokens[token_id]
        if token.parent_id is None:
            msg = "the document root cannot be replaced through a review"
            raise ReviewError(msg)
        parent = self._tokens[token.parent_id]
        self._drop_subtree_children(token)
        self._write_parent(parent, typing.cast(_Key, token.key), value)
        self._adopt(token, value)

    def delete(self, token_id: int) -> None:
        token = self._tokens[token_id]
        if token.parent_id is None:
            msg = "the document root cannot be deleted through a review"
            raise ReviewError(msg)
        parent = self._tokens[token.parent_id]
        key = typing.cast(_Key, token.key)
        if isinstance(parent.children, list):
            index = typing.cast(int, key)
            del parent.value[index]
            del parent.children[index]
            for shifted_index in range(index, len(parent.children)):
                self._tokens[parent.children[shifted_index]].key = shifted_index
        else:
            del parent.value[typing.cast(str, key)]
            del typing.cast(dict[str, int], parent.children)[key]
        self._drop_subtree(token_id)

    def _write_parent(
        self, parent: _Token, key: _Key, value: typing.Any
    ) -> None:
        if isinstance(parent.children, list):
            parent.value[typing.cast(int, key)] = value
        else:
            parent.value[typing.cast(str, key)] = value

    def _adopt(self, token: _Token, value: typing.Any) -> None:
        token.value = value
        token.children = None

        if isinstance(value, list):
            token.children = []
            for index, child in enumerate(value):
                token.children.append(self._tokenize(child, token.id, index))
        elif isinstance(value, dict):
            token.children = {}
            for child_key, child in value.items():
                token.children[child_key] = self._tokenize(
                    child, token.id, child_key
                )

    def _drop_subtree_children(self, token: _Token) -> None:
        if isinstance(token.children, list):
            for child_id in token.children:
                self._drop_subtree(child_id)
        elif isinstance(token.children, dict):
            for child_id in token.children.values():
                self._drop_subtree(child_id)
        token.children = None

    def _drop_subtree(self, token_id: int) -> None:
        stack = [token_id]
        while stack:
            current_id = stack.pop()
            current = self._tokens.pop(current_id)
            if isinstance(current.children, list):
                stack.extend(current.children)
            elif isinstance(current.children, dict):
                stack.extend(current.children.values())


def _key_path(key: _Key) -> JSONPath:
    if isinstance(key, int):
        return Index(key)
    return Fields._compliant_constructor(key)


def _render_chain(chain: typing.Sequence[_Key]) -> str:
    rendered = "$"
    for key in chain:
        if isinstance(key, int):
            rendered += f"[{key}]"
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_@-]*", key):
            rendered += f".{key}"
        else:
            rendered += f"[{json.dumps(key, ensure_ascii=False)}]"
    return rendered


def _track(
    expression: JSONPath,
    store: _IdentityStore,
    token_id: int,
    rank_by_token: dict[int, int],
) -> list[int]:
    """Evaluate an expression against tokens, returning matched token ids."""

    value = store.value(token_id)

    # `SortedThis` is a subclass of `This`, so it must be checked first.
    if isinstance(expression, SortedThis):
        if isinstance(value, dict) and expression.expressions:
            return [token_id]
        if isinstance(value, (list, dict)):
            pairs = [
                (child_id, store.value(child_id))
                for child_id in store.child_ids(token_id)
            ]
            if expression.expressions:
                comparator = functools.cmp_to_key(
                    lambda left, right: expression._compare(
                        DatumInContext.wrap(left[1]),
                        DatumInContext.wrap(right[1]),
                    )
                )
                ordered = sorted(pairs, key=comparator)
            elif isinstance(value, dict):
                # Native ``sorted`` on an object orders its keys.
                ordered = sorted(pairs, key=lambda pair: pair[0])
            else:
                ordered = sorted(pairs, key=lambda pair: pair[1])
            ordered_ids = [child_id for child_id, _ in ordered]
            for rank, child_id in enumerate(ordered_ids):
                rank_by_token[child_id] = rank
            return [store.make_sorted_view(token_id, ordered_ids)]
        return [token_id]
    if isinstance(expression, (Root, This)):
        return [token_id]
    if isinstance(expression, Child):
        matches: list[int] = []
        for left_id in _track(expression.left, store, token_id, rank_by_token):
            left_rank = rank_by_token.get(left_id)
            for right_id in _track(
                expression.right, store, left_id, rank_by_token
            ):
                if left_rank is not None:
                    # A sorted row's rank applies to every occurrence below it.
                    rank_by_token.setdefault(right_id, left_rank)
                matches.append(right_id)
        return matches
    if isinstance(expression, Descendants):
        results: list[int] = []
        for left_id in _track(expression.left, store, token_id, rank_by_token):
            left_rank = rank_by_token.get(left_id)
            stack = [left_id]
            while stack:
                current_id = stack.pop()
                for right_id in _track(
                    expression.right,
                    store,
                    current_id,
                    rank_by_token,
                ):
                    if left_rank is not None:
                        rank_by_token.setdefault(right_id, left_rank)
                    results.append(right_id)
                pairs = store.child_pairs(current_id)
                for child_key, child_id in reversed(pairs):
                    stack.append(child_id)
        return results
    if isinstance(expression, (Where, WhereNot)):
        matches = []
        negate = isinstance(expression, WhereNot)
        for left_id in _track(expression.left, store, token_id, rank_by_token):
            has_match = bool(
                _track(expression.right, store, left_id, rank_by_token)
            )
            if has_match is not negate:
                matches.append(left_id)
        return matches
    if isinstance(expression, _Union):
        return _track(expression.left, store, token_id, rank_by_token) + _track(
            expression.right, store, token_id, rank_by_token
        )
    if isinstance(expression, Fields):
        if expression._deprecated_wildcard_use and isinstance(value, list):
            return store.child_ids(token_id)
        if not isinstance(value, dict):
            return []
        matches = []
        for field in expression.fields:
            child_id = store.child_token(token_id, field)
            if child_id is not None:
                matches.append(child_id)
        return matches
    if isinstance(expression, Wildcard):
        if isinstance(value, (list, dict)):
            return store.child_ids(token_id)
        return []
    if isinstance(expression, Index):
        if not isinstance(value, (list, tuple)):
            return []
        matches = []
        length = len(value)
        for index in expression.indices:
            if length and -length <= index < length:
                child_id = store.child_token(token_id, index)
                if child_id is not None:
                    matches.append(child_id)
        return matches
    if isinstance(expression, Slice):
        if expression._deprecated_wildcard_use and isinstance(value, dict):
            return store.child_ids(token_id)
        if not isinstance(value, (list, tuple)):
            return []
        if expression.step == 0:
            return []
        length = len(value)
        if (
            expression.start is None
            and expression.end is None
            and expression.step is None
        ):
            indices = range(length)
        else:
            indices = range(length)[
                expression.start : expression.end : expression.step
            ]
        return [
            child_id
            for index in indices
            if (child_id := store.child_token(token_id, index)) is not None
        ]
    if isinstance(expression, Filter):
        if not isinstance(value, (list, dict)):
            return []
        parent_token = store.origin(token_id)
        parent_datum = store.native_datum(parent_token)
        matches = []
        for key, child_id in store.child_pairs(token_id):
            candidate = DatumInContext(
                store.value(child_id),
                path=_key_path(key),
                context=parent_datum,
            )
            if all(
                subexpression.find(candidate)
                for subexpression in expression.expressions
            ):
                matches.append(child_id)
        return matches

    # Computed nodes (arithmetic, ``len``, ``keys``, ``path``, ...) produce
    # derived values without a stable occurrence, so they cannot be reviewed.
    return []


class ReviewSession:
    """Tracks occurrences in one document and applies reviewed edits to it."""

    def __init__(self, document: typing.Any) -> None:
        self._store = _IdentityStore(document)

    def find(self, expression: str) -> list[ReviewHit]:
        """Find occurrences with an existing JSONPath expression."""

        parsed = parse(expression)
        rank_by_token: dict[int, int] = {}
        try:
            token_ids = _track(
                parsed, self._store, self._store.root_id, rank_by_token
            )
            hits = []
            for token_id in token_ids:
                sorted_value: typing.Any = None
                is_view = self._store.is_synthetic(token_id)
                if is_view:
                    # Keep the ordered value readable as a list while the
                    # hit still identifies the real list token.
                    sorted_value = self._store.value(token_id)
                token_id = self._store.origin(token_id)
                hits.append(
                    ReviewHit(
                        expression=expression,
                        token_id=token_id,
                        original_path=self._store.path_of(token_id),
                        rank=rank_by_token.get(token_id),
                        value=(
                            sorted_value
                            if is_view
                            else self._store.value(token_id)
                        ),
                    )
                )
            return hits
        finally:
            self._store.clear_views()

    def refresh(self, hit: ReviewHit) -> ReviewHitStatus:
        """Report whether a captured hit still identifies one occurrence."""

        if not self._store.is_alive(hit.token_id):
            return ReviewHitStatus(False, None, None)
        return ReviewHitStatus(
            True,
            self._store.path_of(hit.token_id),
            self._store.value(hit.token_id),
        )

    def replace(self, hit: ReviewHit, value: typing.Any) -> ReviewAction:
        """Prepare an action that overwrites a hit."""

        return ReviewAction.replace(hit, value)

    def delete(self, hit: ReviewHit) -> ReviewAction:
        """Prepare an action that deletes a hit."""

        return ReviewAction.remove(hit)

    def submit(self, *actions: ReviewAction) -> None:
        """Apply actions, rejecting the whole batch if any hit went stale."""

        for action in actions:
            if not self._store.is_alive(action.hit.token_id):
                raise StaleHitError(action.hit)

        for action in actions:
            if not action.delete:
                self._store.replace(action.hit.token_id, action.value)

        deletions = [action for action in actions if action.delete]
        # Delete descendants before their ancestors so every target is still
        # mounted when its turn comes.
        deletions.sort(
            key=lambda action: self._store.depth(action.hit.token_id),
            reverse=True,
        )
        for action in deletions:
            self._store.delete(action.hit.token_id)


class ReviewTicket:
    """A reviewer's collected hits and the changes planned for them."""

    def __init__(self, session: ReviewSession) -> None:
        self._session = session
        self._actions: list[ReviewAction] = []

    @property
    def actions(self) -> tuple[ReviewAction, ...]:
        return tuple(self._actions)

    @property
    def hits(self) -> tuple[ReviewHit, ...]:
        return tuple(action.hit for action in self._actions)

    def add_replace(self, hit: ReviewHit, value: typing.Any) -> ReviewAction:
        action = self._session.replace(hit, value)
        self._actions.append(action)
        return action

    def add_delete(self, hit: ReviewHit) -> ReviewAction:
        action = self._session.delete(hit)
        self._actions.append(action)
        return action

    def submit(self) -> None:
        self._session.submit(*self._actions)
        self._actions.clear()


__all__ = [
    "ReviewAction",
    "ReviewError",
    "ReviewHit",
    "ReviewHitStatus",
    "ReviewSession",
    "ReviewTicket",
    "StaleHitError",
]
