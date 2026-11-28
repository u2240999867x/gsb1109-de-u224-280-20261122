"""
Position-aware TOML parsing for strict handoff mode.

``tomllib`` produces non-finite floats for ``nan`` and ``inf`` literals,
which cannot be delivered as JSON numbers. Handoff mode locates those literals
in the original text (ignoring comments and string contents) and pairs them,
in source order, with the non-finite floats found by a depth-first walk of the
parsed data. Python's insertion-ordered mappings make that walk follow the
order of the source literals.

Syntactic TOML problems, including a value tomllib cannot parse, remain parse
errors. A real ``null`` does not exist in TOML; JSON ``null`` values are left
for the schema contract to judge.
"""

from __future__ import annotations

import math
import re
import tomllib
import typing as t

from . import (
    NON_FINITE_NUMBER,
    HandoffDocument,
    HandoffIssue,
    HandoffLocation,
    build_pointer,
)

_NON_FINITE_RE = re.compile(r"[+-]?(?:nan|inf)\b")


def _iter_non_finite_paths(
    node: t.Any, parts: tuple[str, ...] = ()
) -> t.Iterator[tuple[str, ...]]:
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _iter_non_finite_paths(value, (*parts, str(key)))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _iter_non_finite_paths(value, (*parts, str(index)))
    elif isinstance(node, float) and not math.isfinite(node):
        yield parts


def _scan_non_finite_locations(text: str) -> list[HandoffLocation]:
    locations: list[HandoffLocation] = []
    index = 0
    line = 1
    column = 1
    length = len(text)

    def advance() -> None:
        nonlocal index, line, column
        if text[index] == "\n":
            line += 1
            column = 1
        else:
            column += 1
        index += 1

    def skip_string(quote: str) -> None:
        triple = text.startswith(quote * 3, index)
        if triple:
            for _ in range(3):
                advance()
            while index < length and not text.startswith(quote * 3, index):
                advance()
            for _ in range(3):
                if index < length:
                    advance()
        else:
            advance()
            while index < length and text[index] != quote:
                if quote == '"' and text[index] == "\\":
                    advance()
                advance()
            if index < length:
                advance()

    while index < length:
        char = text[index]

        # a table header occupies a whole line and only contains keys, so
        # nothing inside it can be a non-finite value; honor quoted keys which
        # may themselves contain ']'
        if column == 1 and char == "[":
            while index < length and text[index] != "\n":
                if text[index] in "\"'":
                    skip_string(text[index])
                else:
                    advance()
            continue

        if char == "#":
            while index < length and text[index] != "\n":
                advance()
            continue
        if char in "\"'":
            skip_string(char)
            continue
        match = _NON_FINITE_RE.match(text, index)
        if match is not None and (
            index == 0 or not (text[index - 1].isalnum() or text[index - 1] in "_.")
        ):
            lookahead = match.end()
            while lookahead < length and text[lookahead] in " \t":
                lookahead += 1
            following = text[lookahead] if lookahead < length else "\n"
            # a key (including a dotted key or an inline-table key) is followed
            # by '=' or '.'; a value is followed by ',', ']', '}', newline ...
            if following not in "=.":
                locations.append(HandoffLocation(line=line, column=column))
            for _ in match.group(0):
                advance()
            continue
        advance()
    return locations


def load(stream: t.IO[bytes] | bytes) -> HandoffDocument:
    if isinstance(stream, bytes):
        raw_bytes = stream
    else:
        raw_bytes = stream.read()
    data = tomllib.loads(raw_bytes.decode("utf-8"))
    paths = list(_iter_non_finite_paths(data))
    locations = _scan_non_finite_locations(raw_bytes.decode("utf-8"))
    issues = [
        HandoffIssue(
            category=NON_FINITE_NUMBER,
            pointer=build_pointer(parts),
            locations=(location,),
        )
        for parts, location in zip(paths, locations)
    ]
    return HandoffDocument(data=data, issues=issues)
