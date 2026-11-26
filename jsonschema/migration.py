"""
Checking column-renaming plans between flat JSON object contracts.

A supplier renames report columns before a data warehouse accepts them
under canonical names.  Both sides are described by *flat object
contracts*: exact columns in ``properties``, pattern-declared columns in
``patternProperties`` (a column may be governed by several patterns at
once), and rejection of any additional column.  This module checks,
before a migration plan is published, whether the plan keeps every
previously legal report legal, whether two old columns can collapse onto
a single new name, and whether the new contract accepts names or values
that no old report could have produced.

Names range over an explicitly declared domain: a finite alphabet and a
maximum length.  Patterns are limited to a decidable subset (anchors,
literal characters, ASCII character ranges, alternation and repetition);
anything outside the subset is reported as ``unknown`` rather than
guessed.  Value rules are limited to ``type`` and finite ``enum``.  All
name legality is checked under the caller supplied case policy, and the
verdict -- when one exists -- covers the whole declared domain via
finite-automata reasoning, never by sampling a batch of reports or by
enumerating candidate column names.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
import re

__all__ = [
    "Contract",
    "MergeWitness",
    "MigrationReport",
    "NameDomain",
    "RenamePlan",
    "RowWitness",
    "UnsupportedPattern",
    "ValueRule",
    "accepts_row",
    "check_migration",
]

_JSON_TYPES = (
    "string",
    "integer",
    "number",
    "boolean",
    "null",
    "array",
    "object",
)


class UnsupportedPattern(Exception):
    """A pattern outside the decidable supported subset."""


def _ascii_lower(char):
    if "A" <= char <= "Z":
        return chr(ord(char) + 32)
    return char


def _ascii_upper(char):
    if "a" <= char <= "z":
        return chr(ord(char) - 32)
    return char


def _ascii_swap(char):
    lowered = _ascii_lower(char)
    if lowered != char:
        return lowered
    return _ascii_upper(char)


def _ascii_fold(text):
    return "".join(_ascii_lower(char) for char in text)


def _require(condition, message):
    if not condition:
        raise RuntimeError(f"internal consistency check failed: {message}")


_NO_DIFFERENCE = object()


_FROZEN_TAGS = frozenset({"null", "bool", "num", "str", "arr", "obj"})


def _freeze(value):
    if isinstance(value, tuple) and value[:1] and value[0] in _FROZEN_TAGS:
        return value
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return ("num", value)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, (list, tuple)):
        return ("arr", tuple(_freeze(item) for item in value))
    if isinstance(value, Mapping):
        return (
            "obj",
            frozenset((key, _freeze(item)) for key, item in value.items()),
        )
    raise ValueError(f"not a JSON value: {value!r}")


def _unfreeze(frozen):
    tag = frozen[0]
    if tag == "null":
        return None
    if tag in ("bool", "num", "str"):
        return frozen[1]
    if tag == "arr":
        return [_unfreeze(item) for item in frozen[1]]
    if tag == "obj":
        return {key: _unfreeze(item) for key, item in frozen[1]}
    raise ValueError(f"not a frozen JSON value: {frozen!r}")


def _json_types(value):
    if value is None:
        return frozenset({"null"})
    if isinstance(value, bool):
        return frozenset({"boolean"})
    if isinstance(value, int):
        return frozenset({"integer", "number"})
    if isinstance(value, float):
        if value.is_integer():
            return frozenset({"integer", "number"})
        return frozenset({"number"})
    if isinstance(value, str):
        return frozenset({"string"})
    if isinstance(value, list):
        return frozenset({"array"})
    if isinstance(value, Mapping):
        return frozenset({"object"})
    return frozenset()


def _fabricate(type_name):
    if type_name == "string":
        return ""
    if type_name == "integer":
        return 0
    if type_name == "number":
        return 0.5
    if type_name == "boolean":
        return False
    if type_name == "null":
        return None
    if type_name == "array":
        return []
    if type_name == "object":
        return {}
    raise ValueError(f"unknown JSON type: {type_name!r}")


def _candidates(type_name, limit):
    if type_name == "string":
        return [""] + ["x" * i for i in range(1, limit)]
    if type_name == "integer":
        return list(range(limit))
    if type_name == "number":
        return [i + 0.5 for i in range(limit)]
    if type_name == "boolean":
        return [False, True][:limit]
    if type_name == "null":
        return [None]
    if type_name == "array":
        return [[0] * i for i in range(limit)]
    if type_name == "object":
        return [{"a": i} for i in range(limit)]
    raise ValueError(f"unknown JSON type: {type_name!r}")


@dataclass(frozen=True)
class ValueRule:
    """
    A value rule restricted to ``type`` and a finite ``enum``.

    ``types=None`` means any type is allowed and ``enum=None`` means any
    value is allowed.  ``enum`` stores frozen JSON values internally.
    """

    types: frozenset[str] | None = None
    enum: frozenset | None = None

    def __post_init__(self):
        if self.types is not None:
            types = frozenset(self.types)
            unknown = types - frozenset(_JSON_TYPES)
            if unknown:
                raise ValueError(f"unknown JSON types: {sorted(unknown)}")
            object.__setattr__(self, "types", types)
        if self.enum is not None:
            object.__setattr__(
                self,
                "enum",
                frozenset(_freeze(v) for v in self.enum),
            )

    def meet(self, other):
        """The rule accepting exactly the values both rules accept."""

        def combine(mine, theirs):
            if mine is None:
                return theirs
            if theirs is None:
                return mine
            return mine & theirs

        return ValueRule(
            types=combine(self.types, other.types),
            enum=combine(self.enum, other.enum),
        )

    def _type_ok(self, value):
        return self.types is None or bool(_json_types(value) & self.types)

    def satisfies(self, value):
        """Whether ``value`` is allowed by this rule."""
        if not self._type_ok(value):
            return False
        return self.enum is None or _freeze(value) in self.enum

    def is_satisfiable(self):
        """Whether any value at all is allowed by this rule."""
        if self.types is not None and not self.types:
            return False
        if self.enum is None:
            return True
        return any(self._type_ok(_unfreeze(v)) for v in self.enum)

    def witness(self):
        """Some value allowed by this rule."""
        if self.enum is not None:
            for frozen in sorted(self.enum, key=repr):
                value = _unfreeze(frozen)
                if self._type_ok(value):
                    return value
            raise ValueError("unsatisfiable value rule")
        for type_name in _JSON_TYPES:
            if self.types is None or type_name in self.types:
                return _fabricate(type_name)
        raise ValueError("unsatisfiable value rule")

    def difference_witness(self, other, default=None):
        """
        A value allowed by ``self`` but rejected by ``other``, if any.

        Returns ``default`` when no such value exists; pass a unique
        sentinel as ``default`` when the JSON ``null`` value must be
        distinguishable from "no difference".
        """
        if self.enum is not None:
            for frozen in sorted(self.enum, key=repr):
                value = _unfreeze(frozen)
                if self.satisfies(value) and not other.satisfies(value):
                    return value
            return default
        types = self.types if self.types is not None else _JSON_TYPES
        limit = len(other.enum) + 1 if other.enum is not None else 3
        for type_name in sorted(types):
            for value in _candidates(type_name, limit):
                if self.satisfies(value) and not other.satisfies(value):
                    return value
        return default


@dataclass(frozen=True)
class NameDomain:
    """The declared universe of column names: an alphabet and a length cap."""

    alphabet: frozenset[str]
    max_length: int

    def __init__(self, alphabet, max_length):
        alphabet = frozenset(alphabet)
        if not alphabet:
            raise ValueError("the alphabet must not be empty")
        for char in alphabet:
            if not isinstance(char, str) or len(char) != 1:
                raise ValueError("alphabet entries must be single characters")
        if not isinstance(max_length, int) or max_length < 0:
            raise ValueError("max_length must be a non-negative integer")
        object.__setattr__(self, "alphabet", alphabet)
        object.__setattr__(self, "max_length", max_length)

    @classmethod
    def from_spec(cls, spec):
        """Build a domain from ``{"alphabet": ..., "max_length": ...}``."""
        if isinstance(spec, cls):
            return spec
        return cls(spec["alphabet"], spec["max_length"])

    def contains(self, name):
        """Whether ``name`` lies inside the declared domain."""
        return len(name) <= self.max_length and all(
            char in self.alphabet for char in name
        )

    def size(self):
        """The number of names in the domain."""
        width = len(self.alphabet)
        return sum(width**i for i in range(self.max_length + 1))

    def length_dfa(self):
        """An automaton accepting exactly the names in the domain."""
        return DFA.length_at_most(self.alphabet, self.max_length)


class DFA:
    """A complete deterministic finite automaton over a fixed alphabet."""

    __slots__ = ("alphabet", "finals", "start", "transitions")

    def __init__(self, alphabet, start, finals, transitions):
        self.alphabet = frozenset(alphabet)
        self.start = start
        self.finals = frozenset(finals)
        self.transitions = tuple(dict(row) for row in transitions)

    @classmethod
    def trivial(cls, alphabet, accept):
        row = dict.fromkeys(alphabet, 0)
        return cls(alphabet, 0, {0} if accept else set(), [row])

    @classmethod
    def for_literal(cls, text, alphabet, charsets=None):
        sink = len(text) + 1
        rows = []
        for index, char in enumerate(text):
            chars = charsets[index] if charsets is not None else {char}
            row = dict.fromkeys(alphabet, sink)
            for c in chars:
                if c in alphabet:
                    row[c] = index + 1
            rows.append(row)
        rows.append(dict.fromkeys(alphabet, sink))
        rows.append(dict.fromkeys(alphabet, sink))
        return cls(alphabet, 0, {len(text)}, rows)

    @classmethod
    def length_at_most(cls, alphabet, bound):
        rows = [
            {c: min(i + 1, bound + 1) for c in alphabet}
            for i in range(bound + 2)
        ]
        return cls(alphabet, 0, set(range(bound + 1)), rows)

    @classmethod
    def length_at_least(cls, alphabet, bound):
        rows = [
            {c: min(i + 1, bound) for c in alphabet} for i in range(bound + 1)
        ]
        return cls(alphabet, 0, {bound}, rows)

    @classmethod
    def containing_any(cls, alphabet, chars):
        rows = [
            {c: 1 if c in chars else 0 for c in alphabet},
            dict.fromkeys(alphabet, 1),
        ]
        return cls(alphabet, 0, {1}, rows)

    @classmethod
    def with_prefix(cls, prefix, alphabet):
        sink = len(prefix) + 1
        rows = []
        for index, char in enumerate(prefix):
            row = dict.fromkeys(alphabet, sink)
            if char in alphabet:
                row[char] = index + 1
            rows.append(row)
        rows.append({c: len(prefix) for c in alphabet})
        rows.append(dict.fromkeys(alphabet, sink))
        return cls(alphabet, 0, {len(prefix)}, rows)

    def negate(self):
        return DFA(
            self.alphabet,
            self.start,
            set(range(len(self.transitions))) - self.finals,
            self.transitions,
        )

    def combine(self, other, operator):
        if self.alphabet != other.alphabet:
            raise ValueError("automata over different alphabets")
        start = (self.start, other.start)
        ids = {start: 0}
        queue = deque([start])
        rows = []
        finals = set()
        while queue:
            left, right = pair = queue.popleft()
            index = ids[pair]
            if operator(left in self.finals, right in other.finals):
                finals.add(index)
            row = {}
            for char in self.alphabet:
                nxt = (
                    self.transitions[left][char],
                    other.transitions[right][char],
                )
                if nxt not in ids:
                    ids[nxt] = len(ids)
                    queue.append(nxt)
                row[char] = ids[nxt]
            rows.append(row)
        return DFA(self.alphabet, 0, finals, rows)

    def union(self, other):
        return self.combine(other, lambda a, b: a or b)

    def intersection(self, other):
        return self.combine(other, lambda a, b: a and b)

    def difference(self, other):
        return self.combine(other, lambda a, b: a and not b)

    def is_empty(self):
        seen = {self.start}
        queue = deque([self.start])
        while queue:
            state = queue.popleft()
            if state in self.finals:
                return False
            for nxt in set(self.transitions[state].values()):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return True

    def witness(self):
        """A shortest accepted name, or ``None`` if the language is empty."""
        if self.start in self.finals:
            return ""
        parents = {self.start: None}
        queue = deque([self.start])
        while queue:
            state = queue.popleft()
            for char in sorted(self.alphabet):
                nxt = self.transitions[state][char]
                if nxt in parents:
                    continue
                parents[nxt] = (state, char)
                if nxt in self.finals:
                    chars = []
                    node = nxt
                    while parents[node] is not None:
                        prev, ch = parents[node]
                        chars.append(ch)
                        node = prev
                    return "".join(reversed(chars))
                queue.append(nxt)
        return None

    def derivative(self, text):
        if any(char not in self.alphabet for char in text):
            return DFA.trivial(self.alphabet, False)
        state = self.start
        for char in text:
            state = self.transitions[state][char]
        return DFA(self.alphabet, state, self.finals, self.transitions)

    def prepend(self, text):
        if not text:
            return self
        if any(char not in self.alphabet for char in text):
            return DFA.trivial(self.alphabet, False)
        size = len(self.transitions)
        dead = size + len(text)
        rows = [dict(row) for row in self.transitions]
        for index, char in enumerate(text):
            row = dict.fromkeys(self.alphabet, dead)
            row[char] = (
                self.start if index == len(text) - 1 else size + index + 1
            )
            rows.append(row)
        rows.append(dict.fromkeys(self.alphabet, dead))
        return DFA(self.alphabet, size, self.finals, rows)

    def map_chars(self, mapping):
        nfa = {}
        for state, row in enumerate(self.transitions):
            for char, target in row.items():
                image = mapping(char)
                if image in self.alphabet:
                    nfa.setdefault(state, {}).setdefault(image, set()).add(
                        target,
                    )
        return _determinize(self.alphabet, {self.start}, self.finals, nfa, {})

    def preimage_chars(self, mapping):
        size = len(self.transitions)
        dead = size
        rows = []
        for row in self.transitions:
            new_row = {}
            for char in self.alphabet:
                image = mapping(char)
                new_row[char] = row[image] if image in self.alphabet else dead
            rows.append(new_row)
        rows.append(dict.fromkeys(self.alphabet, dead))
        return DFA(self.alphabet, self.start, self.finals, rows)

    def lift_alphabet(self, alphabet):
        """The same language viewed over a superset alphabet."""
        alphabet = frozenset(alphabet)
        if alphabet == self.alphabet:
            return self
        if not alphabet >= self.alphabet:
            raise ValueError("can only lift to a superset alphabet")
        dead = len(self.transitions)
        rows = []
        for row in self.transitions:
            new_row = dict(row)
            for char in alphabet - self.alphabet:
                new_row[char] = dead
            rows.append(new_row)
        rows.append(dict.fromkeys(alphabet, dead))
        return DFA(alphabet, self.start, self.finals, rows)


def _determinize(alphabet, starts, finals, transitions, epsilon):
    def closure(states):
        result = set(states)
        stack = list(result)
        while stack:
            state = stack.pop()
            for nxt in epsilon.get(state, ()):
                if nxt not in result:
                    result.add(nxt)
                    stack.append(nxt)
        return result

    start = frozenset(closure(starts))
    ids = {start: 0}
    queue = deque([start])
    rows = []
    finals_out = set()
    while queue:
        states = queue.popleft()
        index = ids[states]
        if states & finals:
            finals_out.add(index)
        row = {}
        for char in alphabet:
            reached = set()
            for state in states:
                reached |= transitions.get(state, {}).get(char, set())
            if reached:
                reached = frozenset(closure(reached))
                if reached not in ids:
                    ids[reached] = len(ids)
                    queue.append(reached)
                row[char] = ids[reached]
            else:
                row[char] = -1
        rows.append(row)
    sink = len(rows)
    for row in rows:
        for char in alphabet:
            if row[char] == -1:
                row[char] = sink
    rows.append(dict.fromkeys(alphabet, sink))
    return DFA(alphabet, 0, finals_out, rows)


class _NFA:
    def __init__(self):
        self.trans = {}
        self.eps = {}
        self.count = 0

    def new(self):
        self.count += 1
        return self.count - 1

    def add_edge(self, source, char, target):
        self.trans.setdefault(source, {}).setdefault(char, set()).add(target)

    def add_eps(self, source, target):
        self.eps.setdefault(source, set()).add(target)


def _build_fragment(nfa, node):
    kind = node[0]
    if kind == "eps":
        start, end = nfa.new(), nfa.new()
        nfa.add_eps(start, end)
        return start, end
    if kind == "lit":
        start, end = nfa.new(), nfa.new()
        for char in node[1]:
            nfa.add_edge(start, char, end)
        return start, end
    if kind == "concat":
        start, end = nfa.new(), nfa.new()
        prev = start
        for part in node[1]:
            part_start, part_end = _build_fragment(nfa, part)
            nfa.add_eps(prev, part_start)
            prev = part_end
        nfa.add_eps(prev, end)
        return start, end
    if kind == "alt":
        start, end = nfa.new(), nfa.new()
        for branch in node[1]:
            branch_start, branch_end = _build_fragment(nfa, branch)
            nfa.add_eps(start, branch_start)
            nfa.add_eps(branch_end, end)
        return start, end
    if kind == "star":
        start, end = nfa.new(), nfa.new()
        body_start, body_end = _build_fragment(nfa, node[1])
        nfa.add_eps(start, body_start)
        nfa.add_eps(start, end)
        nfa.add_eps(body_end, body_start)
        nfa.add_eps(body_end, end)
        return start, end
    if kind == "plus":
        start, end = nfa.new(), nfa.new()
        body_start, body_end = _build_fragment(nfa, node[1])
        nfa.add_eps(start, body_start)
        nfa.add_eps(body_end, body_start)
        nfa.add_eps(body_end, end)
        return start, end
    if kind == "opt":
        start, end = nfa.new(), nfa.new()
        body_start, body_end = _build_fragment(nfa, node[1])
        nfa.add_eps(start, body_start)
        nfa.add_eps(start, end)
        nfa.add_eps(body_end, end)
        return start, end
    raise ValueError(f"unknown pattern node: {kind!r}")


def _strip_anchors(pattern):
    anchored_start = pattern.startswith("^")
    text = pattern[1:] if anchored_start else pattern
    anchored_end = False
    if text.endswith("$"):
        backslashes = 0
        index = len(text) - 2
        while index >= 0 and text[index] == "\\":
            backslashes += 1
            index -= 1
        if backslashes % 2 == 0:
            anchored_end = True
            text = text[:-1]
    return text, anchored_start, anchored_end


def _scan_pattern(text):
    in_class = False
    depth = 0
    anchor = False
    top_level_alt = False
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if in_class:
            if char == "]":
                in_class = False
        elif char == "[":
            in_class = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "|" and depth == 0:
            top_level_alt = True
        elif char in "^$":
            anchor = True
        index += 1
    return anchor, top_level_alt


class _PatternParser:
    def __init__(self, text, alphabet, insensitive, repeat_cap):
        self.text = text
        self.alphabet = alphabet
        self.insensitive = insensitive
        self.repeat_cap = repeat_cap
        self.pos = 0

    def error(self, message):
        raise UnsupportedPattern(f"{self.text!r}: {message}")

    def peek(self):
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def parse_full(self):
        node = self.parse_alt()
        if self.pos != len(self.text):
            self.error("unbalanced ')'")
        return node

    def parse_alt(self):
        branches = [self.parse_concat()]
        while self.peek() == "|":
            self.pos += 1
            branches.append(self.parse_concat())
        if len(branches) == 1:
            return branches[0]
        return ("alt", branches)

    def parse_concat(self):
        parts = []
        while self.pos < len(self.text) and self.text[self.pos] not in "|)":
            parts.append(self.parse_repeat())
        if not parts:
            return ("eps",)
        if len(parts) == 1:
            return parts[0]
        return ("concat", parts)

    def parse_repeat(self):
        node = self.parse_atom()
        while True:
            char = self.peek()
            if char == "*":
                self.pos += 1
                node = ("star", node)
            elif char == "+":
                self.pos += 1
                node = ("plus", node)
            elif char == "?":
                self.pos += 1
                node = ("opt", node)
            elif char == "{":
                repeat = self.try_parse_bounded()
                if repeat is None:
                    return node
                node = self.expand(node, *repeat)
            else:
                return node

    def try_parse_bounded(self):
        match = re.match(r"\{(\d+)(,(\d*))?\}", self.text[self.pos :])
        if match is None:
            return None
        self.pos += match.end()
        low = int(match.group(1))
        if match.group(2) is None:
            return low, low
        if match.group(3):
            return low, int(match.group(3))
        return low, None

    def expand(self, node, low, high):
        if high is not None and high < low:
            self.error("decreasing bounded repetition")
        cap = self.repeat_cap
        low = min(low, cap)
        parts = [node] * low
        if high is None:
            parts.append(("star", node))
        else:
            high = min(high, cap)
            parts.extend(("opt", node) for _ in range(high - low))
        if not parts:
            return ("eps",)
        if len(parts) == 1:
            return parts[0]
        return ("concat", parts)

    def parse_atom(self):
        char = self.text[self.pos]
        if char == "(":
            self.pos += 1
            node = self.parse_alt()
            if self.peek() != ")":
                self.error("unbalanced '('")
            self.pos += 1
            return node
        if char == "[":
            return self.parse_class()
        if char == "\\":
            return self.parse_escape()
        if char == ".":
            self.error("'.' is not supported")
        if char in "^$":
            self.error("anchors are only supported at pattern edges")
        if char in "*+?":
            self.error(f"dangling repeat operator {char!r}")
        self.pos += 1
        return self.literal(char)

    def parse_escape(self):
        self.pos += 1
        if self.pos >= len(self.text):
            self.error("trailing backslash")
        char = self.text[self.pos]
        self.pos += 1
        if char.isalnum():
            self.error(f"escape '\\{char}' is not supported")
        return self.literal(char)

    def literal(self, char):
        chars = {char}
        if self.insensitive:
            chars.add(_ascii_swap(char))
        return ("lit", frozenset(chars & self.alphabet))

    def parse_class(self):
        self.pos += 1
        if self.peek() == "^":
            self.error("negated character classes are not supported")
        chars = set()
        first = True
        while True:
            if self.pos >= len(self.text):
                self.error("unterminated character class")
            char = self.text[self.pos]
            if char == "]" and not first:
                self.pos += 1
                break
            first = False
            if char == "\\":
                self.pos += 1
                if self.pos >= len(self.text):
                    self.error("trailing backslash")
                low = self.text[self.pos]
                if low.isalnum():
                    self.error(f"escape '\\{low}' is not supported")
                self.pos += 1
            else:
                low = char
                self.pos += 1
            if (
                self.peek() == "-"
                and self.pos + 1 < len(self.text)
                and self.text[self.pos + 1] != "]"
            ):
                self.pos += 1
                high = self.text[self.pos]
                if high == "\\":
                    self.pos += 1
                    if self.pos >= len(self.text):
                        self.error("trailing backslash")
                    high = self.text[self.pos]
                    if high.isalnum():
                        self.error(f"escape '\\{high}' is not supported")
                self.pos += 1
                if not (low.isascii() and high.isascii()):
                    self.error("non-ASCII character range")
                if ord(low) > ord(high):
                    self.error("decreasing character range")
                chars.update(
                    chr(code) for code in range(ord(low), ord(high) + 1)
                )
            else:
                chars.add(low)
        if self.insensitive:
            chars = chars | {_ascii_swap(c) for c in chars}
        return ("lit", frozenset(chars & self.alphabet))


def _compile_pattern(pattern, domain, insensitive):
    text, anchored_start, anchored_end = _strip_anchors(pattern)
    anchor, top_level_alt = _scan_pattern(text)
    if anchor:
        raise UnsupportedPattern(
            f"{pattern!r}: anchors are only supported at pattern edges",
        )
    if (anchored_start or anchored_end) and top_level_alt:
        raise UnsupportedPattern(
            f"{pattern!r}: anchors combined with top-level alternation "
            "are not supported",
        )
    parser = _PatternParser(
        text,
        domain.alphabet,
        insensitive,
        domain.max_length + 1,
    )
    node = parser.parse_full()
    nfa = _NFA()
    start, end = _build_fragment(nfa, node)
    if not anchored_start:
        new_start = nfa.new()
        for char in domain.alphabet:
            nfa.add_edge(new_start, char, new_start)
        nfa.add_eps(new_start, start)
        start = new_start
    if not anchored_end:
        for char in domain.alphabet:
            nfa.add_edge(end, char, end)
    dfa = _determinize(domain.alphabet, {start}, {end}, nfa.trans, nfa.eps)
    return dfa.intersection(domain.length_dfa())


def _parse_value_rule(subschema):
    if subschema is True:
        return ValueRule()
    if subschema is False:
        return ValueRule(types=frozenset())
    if not isinstance(subschema, Mapping):
        raise TypeError(f"unsupported value rule: {subschema!r}")
    unsupported = set(subschema) - {"type", "enum"}
    if unsupported:
        raise ValueError(
            f"unsupported value rule keywords: {sorted(unsupported)}",
        )
    types = subschema.get("type")
    if isinstance(types, str):
        types = [types]
    return ValueRule(types=types, enum=subschema.get("enum"))


@dataclass
class Contract:
    """
    A flat object contract: exact columns, pattern columns, no extras.

    ``additional_properties`` defaults to ``False``: any column not
    declared in ``properties`` or ``pattern_properties`` is rejected.
    Value rules are restricted to ``type`` and finite ``enum`` (a bare
    boolean is also accepted, where ``False`` rejects every value).
    """

    properties: Mapping[str, Any] = field(default_factory=dict)
    pattern_properties: Mapping[str, Any] = field(default_factory=dict)
    additional_properties: bool = False

    @classmethod
    def from_schema(cls, schema):
        """Build a contract from a JSON-Schema-shaped mapping."""
        if isinstance(schema, cls):
            return schema
        return cls(
            properties=schema.get("properties", {}),
            pattern_properties=schema.get("patternProperties", {}),
            additional_properties=schema.get("additionalProperties", False),
        )


@dataclass(frozen=True)
class RenamePlan:
    """
    A renaming plan: fixed prefix rewrites plus ASCII case normalization.

    The longest matching prefix wins; names matching no prefix keep
    their spelling.  ``case_normalization`` is ``None``, ``"lower"`` or
    ``"upper"`` and folds ASCII letters only.
    """

    prefix_rewrites: tuple = ()
    case_normalization: str | None = None

    def __init__(self, prefix_rewrites=(), case_normalization=None):
        if isinstance(prefix_rewrites, Mapping):
            pairs = tuple(prefix_rewrites.items())
        else:
            pairs = tuple(tuple(pair) for pair in prefix_rewrites)
        prefixes = [prefix for prefix, _ in pairs]
        if any(not prefix for prefix in prefixes):
            raise ValueError("rewrite prefixes must be non-empty")
        if len(set(prefixes)) != len(prefixes):
            raise ValueError("duplicate rewrite prefix")
        if case_normalization not in (None, "lower", "upper"):
            raise ValueError(
                "case_normalization must be None, 'lower' or 'upper'",
            )
        object.__setattr__(self, "prefix_rewrites", pairs)
        object.__setattr__(self, "case_normalization", case_normalization)

    @classmethod
    def from_spec(cls, spec):
        """Build a plan from a mapping or return an existing plan."""
        if isinstance(spec, cls):
            return spec
        return cls(
            prefix_rewrites=spec.get("prefix_rewrites", ()),
            case_normalization=spec.get("case_normalization"),
        )

    def fold_char(self, char):
        """Apply the plan's ASCII case normalization to one character."""
        if self.case_normalization == "lower":
            return _ascii_lower(char)
        if self.case_normalization == "upper":
            return _ascii_upper(char)
        return char

    def fold(self, text):
        """Apply the plan's ASCII case normalization to a whole name."""
        return "".join(self.fold_char(char) for char in text)

    def rename(self, name):
        """The canonical name that ``name`` is mapped to."""
        best = None
        for prefix, replacement in self.prefix_rewrites:
            if name.startswith(prefix) and (
                best is None or len(prefix) > len(best[0])
            ):
                best = (prefix, replacement)
        if best is not None:
            name = best[1] + name[len(best[0]) :]
        return self.fold(name)


@dataclass
class _Matcher:
    label: str
    dfa: DFA
    rule: ValueRule
    literal: str | None = None
    pattern: str | None = None
    insensitive: bool = False

    @classmethod
    def for_literal(cls, name, subschema, domain, insensitive):
        charsets = []
        for char in name:
            chars = {char}
            if insensitive:
                chars.add(_ascii_swap(char))
            charsets.append(chars)
        dfa = DFA.for_literal(name, domain.alphabet, charsets)
        dfa = dfa.intersection(domain.length_dfa())
        return cls(
            f"properties[{name!r}]",
            dfa,
            _parse_value_rule(subschema),
            literal=name,
            insensitive=insensitive,
        )

    @classmethod
    def for_pattern(cls, pattern, subschema, domain, insensitive):
        dfa = _compile_pattern(pattern, domain, insensitive)
        return cls(
            f"patternProperties[{pattern!r}]",
            dfa,
            _parse_value_rule(subschema),
            pattern=pattern,
            insensitive=insensitive,
        )

    def matches(self, name):
        if self.literal is not None:
            if self.insensitive:
                return _ascii_fold(name) == _ascii_fold(self.literal)
            return name == self.literal
        if self.insensitive:
            return re.search(self.pattern, name, re.IGNORECASE) is not None
        return re.search(self.pattern, name) is not None


class _Region:
    def __init__(self, labels, dfa, rule):
        self.labels = labels
        self.dfa = dfa
        self.rule = rule


class _CompiledContract:
    def __init__(self, contract, domain, case_policy):
        if case_policy not in ("sensitive", "insensitive"):
            raise ValueError(f"unknown case policy: {case_policy!r}")
        if not isinstance(contract.additional_properties, bool):
            raise TypeError("additional_properties must be a boolean")
        self.contract = contract
        self.domain = domain
        self.case_policy = case_policy
        insensitive = case_policy == "insensitive"
        self.matchers = []
        self.unsupported = []
        for name, subschema in sorted(contract.properties.items()):
            self.matchers.append(
                _Matcher.for_literal(name, subschema, domain, insensitive),
            )
        for pattern, subschema in sorted(contract.pattern_properties.items()):
            try:
                matcher = _Matcher.for_pattern(
                    pattern,
                    subschema,
                    domain,
                    insensitive,
                )
            except UnsupportedPattern:
                self.unsupported.append(pattern)
            else:
                self.matchers.append(matcher)
        base = domain.length_dfa()
        if contract.additional_properties:
            self.allowed = base
        else:
            self.allowed = DFA.trivial(domain.alphabet, False)
            for matcher in self.matchers:
                self.allowed = self.allowed.union(matcher.dfa)
        self.regions = self._compute_regions()

    def _compute_regions(self):
        base = self.domain.length_dfa()
        complements = [base.difference(m.dfa) for m in self.matchers]
        regions = []
        for mask in range(1 << len(self.matchers)):
            if mask == 0 and not self.contract.additional_properties:
                continue
            dfa = base
            rule = ValueRule()
            labels = []
            empty = False
            for index, matcher in enumerate(self.matchers):
                if mask & (1 << index):
                    dfa = dfa.intersection(matcher.dfa)
                    rule = rule.meet(matcher.rule)
                    labels.append(matcher.label)
                else:
                    dfa = dfa.intersection(complements[index])
                if dfa.is_empty():
                    empty = True
                    break
            if not empty:
                regions.append(_Region(tuple(labels), dfa, rule))
        return regions

    def usable_regions(self):
        return [r for r in self.regions if r.rule.is_satisfiable()]

    def usable_names(self):
        names = DFA.trivial(self.domain.alphabet, False)
        for region in self.usable_regions():
            names = names.union(region.dfa)
        return names

    def rule_for(self, name):
        if not self.domain.contains(name):
            return None
        rule = ValueRule()
        matched = False
        for matcher in self.matchers:
            if matcher.matches(name):
                matched = True
                rule = rule.meet(matcher.rule)
        if not matched and not self.contract.additional_properties:
            return None
        return rule

    def accepts_name(self, name):
        return self.rule_for(name) is not None

    def accepts_row(self, row):
        for name, value in row.items():
            rule = self.rule_for(name)
            if rule is None or not rule.satisfies(value):
                return False
        return True


class _Rename:
    """A symbolic view of a renaming plan over a name domain."""

    def __init__(self, plan, domain):
        self.plan = plan
        self.domain = domain
        base = domain.length_dfa()
        alphabet = domain.alphabet
        self.classes = []
        any_prefix = DFA.trivial(alphabet, False)
        prefix_langs = []
        for prefix, replacement in plan.prefix_rewrites:
            lang = DFA.with_prefix(prefix, alphabet).intersection(base)
            prefix_langs.append((prefix, replacement, lang))
            any_prefix = any_prefix.union(lang)
        for prefix, replacement, lang in prefix_langs:
            longer = DFA.trivial(alphabet, False)
            for other_prefix, _, other_lang in prefix_langs:
                if len(other_prefix) > len(prefix) and (
                    other_prefix.startswith(prefix)
                ):
                    longer = longer.union(other_lang)
            self.classes.append((prefix, replacement, lang.difference(longer)))
        self.no_prefix = base.difference(any_prefix)
        self.bad_chars = frozenset(
            char for char in alphabet if plan.fold_char(char) not in alphabet
        )

    def image(self, dfa):
        base = self.domain.length_dfa()
        image = DFA.trivial(self.domain.alphabet, False)
        for prefix, replacement, longest in self.classes:
            part = dfa.intersection(longest).derivative(prefix)
            image = image.union(part.prepend(replacement))
        image = image.union(dfa.intersection(self.no_prefix))
        image = image.intersection(base)
        return image.map_chars(self.plan.fold_char).intersection(base)

    def preimage(self, dfa):
        base = self.domain.length_dfa()
        pulled = dfa.preimage_chars(self.plan.fold_char)
        preimage = DFA.trivial(self.domain.alphabet, False)
        for prefix, replacement, longest in self.classes:
            part = pulled.derivative(replacement).prepend(prefix)
            preimage = preimage.union(part.intersection(longest))
        preimage = preimage.union(self.no_prefix.intersection(pulled))
        return preimage.intersection(base)

    def out_of_domain(self):
        base = self.domain.length_dfa()
        alphabet = self.domain.alphabet
        limit = self.domain.max_length
        out = DFA.trivial(alphabet, False)
        for prefix, replacement, longest in self.classes:
            if any(
                self.plan.fold_char(c) not in alphabet for c in replacement
            ):
                out = out.union(longest)
                continue
            threshold = limit + len(prefix) - len(replacement)
            if threshold < limit:
                too_long = DFA.length_at_least(alphabet, threshold + 1)
                out = out.union(longest.intersection(too_long))
            if self.bad_chars:
                tail = DFA.containing_any(alphabet, self.bad_chars)
                out = out.union(longest.intersection(tail.prepend(prefix)))
        if self.bad_chars:
            bad = DFA.containing_any(alphabet, self.bad_chars)
            out = out.union(self.no_prefix.intersection(bad))
        return out.intersection(base)


def _find_fold_pairs(dfa, mapping, limit=8):
    groups = {}
    for char in sorted(dfa.alphabet):
        groups.setdefault(mapping(char), []).append(char)
    start = (dfa.start, dfa.start, False)
    seen = {start}
    queue = deque([(start, "", "")])
    found = []
    while queue and len(found) < limit:
        (left, right, differed), left_text, right_text = queue.popleft()
        if differed and left in dfa.finals and right in dfa.finals:
            found.append((left_text, right_text))
            continue
        for chars in groups.values():
            for left_char in chars:
                next_left = dfa.transitions[left][left_char]
                for right_char in chars:
                    state = (
                        next_left,
                        dfa.transitions[right][right_char],
                        differed or left_char != right_char,
                    )
                    if state not in seen:
                        seen.add(state)
                        queue.append(
                            (
                                state,
                                left_text + left_char,
                                right_text + right_char,
                            ),
                        )
    return found


@dataclass
class MergeWitness:
    """
    Evidence that two old columns collapse onto one new name.

    ``source_row`` is accepted by the old contract whenever
    ``cooccurs`` is true, which makes the merge reviewable: both source
    names may appear together in the same original row.
    """

    target: str
    sources: tuple
    cooccurs: bool
    source_row: dict
    renamed_row: dict

    def to_dict(self):
        """A JSON-friendly view of the witness."""
        return {
            "target": self.target,
            "sources": list(self.sources),
            "cooccurs": self.cooccurs,
            "source_row": self.source_row,
            "renamed_row": self.renamed_row,
        }


@dataclass
class RowWitness:
    """A single counterexample (or reverse) row plus the failing name."""

    kind: str
    target_name: str
    value: Any = None
    source_row: dict | None = None
    target_row: dict | None = None
    detail: str = ""

    def to_dict(self):
        """A JSON-friendly view of the witness."""
        return {
            "kind": self.kind,
            "target_name": self.target_name,
            "value": self.value,
            "source_row": self.source_row,
            "target_row": self.target_row,
            "detail": self.detail,
        }


@dataclass
class MigrationReport:
    """The outcome of checking a renaming plan between two contracts."""

    verdict: str
    case_policy: str
    domain: NameDomain
    collisions: list = field(default_factory=list)
    forward_witnesses: list = field(default_factory=list)
    reverse_witnesses: list = field(default_factory=list)
    unknown_patterns: list = field(default_factory=list)
    coverage: str = ""

    @property
    def faithful(self):
        """Whether every old-legal report stays legal under the plan."""
        return self.verdict == "faithful"

    def to_dict(self):
        """A JSON-friendly view of the whole report."""
        return {
            "verdict": self.verdict,
            "case_policy": self.case_policy,
            "domain": {
                "alphabet": sorted(self.domain.alphabet),
                "max_length": self.domain.max_length,
            },
            "collisions": [c.to_dict() for c in self.collisions],
            "forward_witnesses": [w.to_dict() for w in self.forward_witnesses],
            "reverse_witnesses": [w.to_dict() for w in self.reverse_witnesses],
            "unknown_patterns": list(self.unknown_patterns),
            "coverage": self.coverage,
        }

    def summary(self):
        """A short human-readable summary of the verdict."""
        lines = [f"verdict: {self.verdict}"]
        if self.unknown_patterns:
            lines.append(f"unsupported patterns: {self.unknown_patterns}")
        if self.collisions:
            lines.append(f"collisions: {len(self.collisions)} merge(s)")
        lines.append(f"forward witnesses: {len(self.forward_witnesses)}")
        lines.append(f"reverse witnesses: {len(self.reverse_witnesses)}")
        lines.append(self.coverage)
        return "\n".join(lines)


def _coverage(domain, prefix):
    return (
        f"{prefix} over the whole declared name domain "
        f"(alphabet of {len(domain.alphabet)} characters, "
        f"length at most {domain.max_length}, "
        f"{domain.size()} candidate names), decided symbolically with "
        f"finite automata rather than by sampling a report batch or "
        f"pre-enumerating candidate column names"
    )


def _find_collisions(old_contract, rename, plan, domain, usable_old):
    extra = set(domain.alphabet)
    for _, replacement in plan.prefix_rewrites:
        extra.update(replacement)
    extra |= {plan.fold_char(char) for char in extra}
    growth = max(
        [
            len(replacement) - len(prefix)
            for prefix, replacement in plan.prefix_rewrites
        ]
        + [0],
    )
    out_alphabet = frozenset(extra)
    out_base = DFA.length_at_most(
        out_alphabet,
        domain.max_length + max(0, growth),
    )

    classes = [
        (prefix, replacement, usable_old.intersection(longest))
        for prefix, replacement, longest in rename.classes
    ]
    classes.append((None, None, usable_old.intersection(rename.no_prefix)))

    tails_list = []
    outputs = []
    for prefix, replacement, cls in classes:
        tails = cls if prefix is None else cls.derivative(prefix)
        tails_list.append(tails)
        out = tails.lift_alphabet(out_alphabet).prepend(replacement or "")
        outputs.append(out.intersection(out_base))
    folded = [out.map_chars(plan.fold_char) for out in outputs]

    def source_of(index, output):
        prefix, replacement, _ = classes[index]
        if prefix is None:
            return output
        return prefix + output[len(replacement) :]

    pairs = []
    for i in range(len(classes)):
        for j in range(i + 1, len(classes)):
            both = folded[i].intersection(folded[j])
            if both.is_empty():
                continue
            target = both.witness()
            variants = DFA.for_literal(
                target,
                out_alphabet,
            ).preimage_chars(plan.fold_char)
            first = outputs[i].intersection(variants).witness()
            second = outputs[j].intersection(variants).witness()
            pairs.append((source_of(i, first), source_of(j, second), target))
    for i, tails in enumerate(tails_list):
        prefix = classes[i][0] or ""
        for first, second in _find_fold_pairs(tails, plan.fold_char):
            first_name = prefix + first
            pairs.append(
                (
                    first_name,
                    prefix + second,
                    plan.rename(first_name),
                ),
            )

    witnesses = []
    seen = set()
    for first_name, second_name, target in pairs:
        if first_name == second_name:
            continue
        key = (target, frozenset((first_name, second_name)))
        if key in seen:
            continue
        seen.add(key)
        _require(
            plan.rename(first_name) == target,
            f"{first_name!r} should rename to {target!r}",
        )
        _require(
            plan.rename(second_name) == target,
            f"{second_name!r} should rename to {target!r}",
        )
        first_rule = old_contract.rule_for(first_name)
        second_rule = old_contract.rule_for(second_name)
        row = {
            first_name: first_rule.witness(),
            second_name: second_rule.witness(),
        }
        renamed = {}
        for name, value in row.items():
            renamed[plan.rename(name)] = value
        witnesses.append(
            MergeWitness(
                target=target,
                sources=(first_name, second_name),
                cooccurs=old_contract.accepts_row(row),
                source_row=row,
                renamed_row=renamed,
            ),
        )
    return witnesses


def _forward_witnesses(old_contract, new_contract, rename, plan, domain):
    base = domain.length_dfa()
    name_failures = rename.out_of_domain().union(
        rename.preimage(base.difference(new_contract.allowed)),
    )
    preimages = {
        id(region): rename.preimage(region.dfa)
        for region in new_contract.regions
    }
    witnesses = []
    for region in old_contract.usable_regions():
        bad = region.dfa.intersection(name_failures)
        if not bad.is_empty():
            name = bad.witness()
            target = plan.rename(name)
            value = region.rule.witness()
            row = {name: value}
            _require(
                old_contract.accepts_row(row),
                f"{row!r} should be accepted by the old contract",
            )
            _require(
                not new_contract.accepts_row({target: value}),
                f"{target!r} should be rejected by the new contract",
            )
            witnesses.append(
                RowWitness(
                    kind="name_not_accepted",
                    target_name=target,
                    value=value,
                    source_row=row,
                    detail=(
                        f"{name!r} is old-legal but renames to {target!r}, "
                        "which the new contract does not accept"
                    ),
                ),
            )
        for target_region in new_contract.regions:
            overlap = region.dfa.intersection(preimages[id(target_region)])
            if overlap.is_empty():
                continue
            value = region.rule.difference_witness(
                target_region.rule,
                _NO_DIFFERENCE,
            )
            if value is _NO_DIFFERENCE:
                continue
            name = overlap.witness()
            target = plan.rename(name)
            row = {name: value}
            _require(
                old_contract.accepts_row(row),
                f"{row!r} should be accepted by the old contract",
            )
            _require(
                not new_contract.accepts_row({target: value}),
                f"{target!r} should reject {value!r} under the new contract",
            )
            witnesses.append(
                RowWitness(
                    kind="value_not_accepted",
                    target_name=target,
                    value=value,
                    source_row=row,
                    detail=(
                        f"value {value!r} is old-legal at {name!r} but the "
                        f"new contract rejects it at {target!r}"
                    ),
                ),
            )
    return witnesses


def _reverse_witnesses(old_contract, new_contract, rename, domain, usable_old):
    base = domain.length_dfa()
    image = rename.image(usable_old)
    witnesses = []
    uncovered = new_contract.usable_names().intersection(
        base.difference(image),
    )
    if not uncovered.is_empty():
        name = uncovered.witness()
        value = new_contract.rule_for(name).witness()
        row = {name: value}
        _require(
            new_contract.accepts_row(row),
            f"{row!r} should be accepted by the new contract",
        )
        literal = DFA.for_literal(name, domain.alphabet)
        _require(
            rename.preimage(literal).intersection(usable_old).is_empty(),
            f"{name!r} should have no old-legal preimage",
        )
        witnesses.append(
            RowWitness(
                kind="target_only_name",
                target_name=name,
                value=value,
                target_row=row,
                detail=(
                    f"the new contract accepts {name!r} but no old-legal "
                    "name renames to it"
                ),
            ),
        )
    region_images = [
        (region, rename.image(region.dfa))
        for region in old_contract.usable_regions()
    ]
    seen = set()
    for target_region in new_contract.usable_regions():
        for region, region_image in region_images:
            if target_region.dfa.intersection(region_image).is_empty():
                continue
            value = target_region.rule.difference_witness(
                region.rule,
                _NO_DIFFERENCE,
            )
            if value is _NO_DIFFERENCE:
                continue
            covered = target_region.dfa.intersection(image)
            for other, other_image in region_images:
                if other.rule.satisfies(value):
                    covered = covered.difference(other_image)
            if covered.is_empty():
                continue
            name = covered.witness()
            key = ("target_only_value", name, _freeze(value))
            if key in seen:
                continue
            seen.add(key)
            row = {name: value}
            _require(
                new_contract.accepts_row(row),
                f"{row!r} should be accepted by the new contract",
            )
            literal = DFA.for_literal(name, domain.alphabet)
            pulled = rename.preimage(literal)
            for other, _ in region_images:
                if other.rule.satisfies(value):
                    _require(
                        pulled.intersection(other.dfa).is_empty(),
                        f"{name!r} should not be reachable with {value!r}",
                    )
            witnesses.append(
                RowWitness(
                    kind="target_only_value",
                    target_name=name,
                    value=value,
                    target_row=row,
                    detail=(
                        f"the new contract accepts value {value!r} at "
                        f"{name!r} but no old-legal row produces it"
                    ),
                ),
            )
    return witnesses


def check_migration(old, new, plan, domain, case_policy="sensitive"):
    """
    Check whether a renaming plan preserves old-legal reports.

    ``old`` and ``new`` are contracts (or JSON-Schema-shaped mappings),
    ``plan`` a :class:`RenamePlan` (or a mapping with ``prefix_rewrites``
    and ``case_normalization``), and ``domain`` a :class:`NameDomain`
    (or a mapping with ``alphabet`` and ``max_length``).  All name
    legality is checked under ``case_policy`` (``"sensitive"`` or
    ``"insensitive"``).

    The verdict is one of:

    - ``"unknown"``: a pattern outside the supported subset was found.
    - ``"collision"``: two old columns can merge into one new name; the
      plan is rejected and ``collisions`` carries merge witnesses.
    - ``"unfaithful"``: an old-legal row fails after renaming;
      ``forward_witnesses`` carries counterexample rows.
    - ``"faithful"``: every old-legal row stays legal, verified over the
      whole declared domain.

    Whenever the new contract accepts names or values no old-legal row
    can produce, ``reverse_witnesses`` carries examples.
    """
    domain = NameDomain.from_spec(domain)
    plan = RenamePlan.from_spec(plan)
    old_contract = _CompiledContract(
        Contract.from_schema(old),
        domain,
        case_policy,
    )
    new_contract = _CompiledContract(
        Contract.from_schema(new),
        domain,
        case_policy,
    )
    unsupported = old_contract.unsupported + new_contract.unsupported
    if unsupported:
        return MigrationReport(
            verdict="unknown",
            case_policy=case_policy,
            domain=domain,
            unknown_patterns=unsupported,
            coverage=(
                "no conclusion: unsupported pattern(s) "
                + ", ".join(sorted(set(unsupported)))
            ),
        )
    rename = _Rename(plan, domain)
    usable_old = old_contract.usable_names()
    collisions = _find_collisions(
        old_contract,
        rename,
        plan,
        domain,
        usable_old,
    )
    if collisions:
        return MigrationReport(
            verdict="collision",
            case_policy=case_policy,
            domain=domain,
            collisions=collisions,
            coverage=_coverage(domain, "plan rejected: name collision"),
        )
    forward = _forward_witnesses(
        old_contract,
        new_contract,
        rename,
        plan,
        domain,
    )
    reverse = _reverse_witnesses(
        old_contract,
        new_contract,
        rename,
        domain,
        usable_old,
    )
    verdict = "unfaithful" if forward else "faithful"
    return MigrationReport(
        verdict=verdict,
        case_policy=case_policy,
        domain=domain,
        forward_witnesses=forward,
        reverse_witnesses=reverse,
        coverage=_coverage(domain, "checked exhaustively"),
    )


def accepts_row(contract, row, domain, case_policy="sensitive"):
    """
    Independently check whether ``row`` is accepted by ``contract``.

    Useful to review witnesses produced by :func:`check_migration`, for
    instance whether two merged source names may appear together in one
    original row.
    """
    compiled = _CompiledContract(
        Contract.from_schema(contract),
        NameDomain.from_spec(domain),
        case_policy,
    )
    if compiled.unsupported:
        raise UnsupportedPattern(
            f"unsupported pattern(s): {compiled.unsupported}",
        )
    return compiled.accepts_row(row)
