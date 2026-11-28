"""
Position-aware JSON parsing for strict handoff mode.

A small recursive parser built on top of the standard library's JSON scanner
primitives. Unlike :func:`json.loads`, it

- keeps every key of an object, so two keys which decode to the same field
  name can both be located and reported,
- records the 1-based line/column of every key and scalar,
- detects non-finite numbers (including overflow such as ``1e999`` which the
  stdlib parser silently turns into ``Infinity``) without relying on any
  optional third-party parser.

``null`` is a perfectly valid JSON value and is left for the schema contract
to judge.
"""

from __future__ import annotations

import json.decoder
import json.scanner
import math
import typing as t

from . import (
    DUPLICATE_KEY,
    NON_FINITE_NUMBER,
    HandoffDocument,
    HandoffIssue,
    HandoffLocation,
    build_pointer,
)

_json_decoder = json.decoder.JSONDecoder()
_scanstring = json.decoder.scanstring
_NUMBER_RE = json.scanner.NUMBER_RE


class JsonParseError(ValueError):
    pass


def _location(text: str, index: int) -> HandoffLocation:
    line = text.count("\n", 0, index) + 1
    if line == 1:
        column = index + 1
    else:
        column = index - text.rfind("\n", 0, index)
    return HandoffLocation(line=line, column=column)


class _Parser:
    def __init__(self, text: str) -> None:
        self._text = text
        self._length = len(text)
        self._index = 0
        self.issues: list[HandoffIssue] = []

    def parse(self) -> t.Any:
        self._skip_whitespace()
        data = self._parse_value(())
        self._skip_whitespace()
        if self._index != self._length:
            raise self._error("Extra data")
        return data

    def _error(self, message: str) -> JsonParseError:
        loc = _location(self._text, self._index)
        return JsonParseError(
            f"{message} (at line {loc.line}, column {loc.column})"
        )

    def _skip_whitespace(self) -> None:
        text = self._text
        while self._index < self._length and text[self._index] in " \t\n\r":
            self._index += 1

    def _parse_string(self) -> str:
        value, end = _scanstring(
            self._text, self._index + 1, _json_decoder.strict
        )
        self._index = end
        return value

    def _parse_value(self, pointer_parts: tuple[str, ...]) -> t.Any:
        self._skip_whitespace()
        if self._index >= self._length:
            raise self._error("Expecting value")
        char = self._text[self._index]
        if char == "{":
            return self._parse_object(pointer_parts)
        if char == "[":
            return self._parse_array(pointer_parts)
        if char == '"':
            return self._parse_string()
        if char == "t":
            return self._parse_literal("true", True)
        if char == "f":
            return self._parse_literal("false", False)
        if char == "n":
            return self._parse_literal("null", None)
        if char == "-" or char.isdigit():
            return self._parse_number(pointer_parts)
        raise self._error("Expecting value")

    def _parse_literal(self, literal: str, value: t.Any) -> t.Any:
        if not self._text.startswith(literal, self._index):
            raise self._error("Expecting value")
        self._index += len(literal)
        return value

    def _parse_number(self, pointer_parts: tuple[str, ...]) -> t.Any:
        match = _NUMBER_RE.match(self._text, self._index)
        if match is None:
            raise self._error("Expecting value")
        number_text = match.group(0)
        start_index = self._index
        self._index = match.end()
        if "." in number_text or "e" in number_text or "E" in number_text:
            value = float(number_text)
            if not math.isfinite(value):
                self.issues.append(
                    HandoffIssue(
                        category=NON_FINITE_NUMBER,
                        pointer=build_pointer(pointer_parts),
                        locations=(_location(self._text, start_index),),
                    )
                )
            return value
        return int(number_text)

    def _parse_object(
        self, pointer_parts: tuple[str, ...]
    ) -> dict[str, t.Any]:
        self._index += 1
        result: dict[str, t.Any] = {}
        seen: dict[str, list[HandoffLocation]] = {}
        self._skip_whitespace()
        if self._text.startswith("}", self._index):
            self._index += 1
            return result
        while True:
            self._skip_whitespace()
            if not self._text.startswith('"', self._index):
                raise self._error(
                    "Expecting property name enclosed in double quotes"
                )
            key_start = self._index
            key = self._parse_string()
            key_location = _location(self._text, key_start)
            self._skip_whitespace()
            if not self._text.startswith(":", self._index):
                raise self._error("Expecting ':' delimiter")
            self._index += 1
            child_parts = (*pointer_parts, key)
            result[key] = self._parse_value(child_parts)
            seen.setdefault(key, []).append(key_location)
            self._skip_whitespace()
            if self._text.startswith(",", self._index):
                self._index += 1
                continue
            if self._text.startswith("}", self._index):
                self._index += 1
                break
            raise self._error("Expecting ',' delimiter")
        for field_name, locations in seen.items():
            if len(locations) > 1:
                self.issues.append(
                    HandoffIssue(
                        category=DUPLICATE_KEY,
                        pointer=build_pointer((*pointer_parts, field_name)),
                        locations=tuple(locations),
                    )
                )
        return result

    def _parse_array(self, pointer_parts: tuple[str, ...]) -> list[t.Any]:
        self._index += 1
        result: list[t.Any] = []
        self._skip_whitespace()
        if self._text.startswith("]", self._index):
            self._index += 1
            return result
        index = 0
        while True:
            result.append(
                self._parse_value((*pointer_parts, str(index)))
            )
            index += 1
            self._skip_whitespace()
            if self._text.startswith(",", self._index):
                self._index += 1
                continue
            if self._text.startswith("]", self._index):
                self._index += 1
                break
            raise self._error("Expecting ',' delimiter")
        return result


def load(text: str | bytes | t.IO[bytes]) -> HandoffDocument:
    if not isinstance(text, (str, bytes)):
        text = text.read()
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    parser = _Parser(text)
    data = parser.parse()
    return HandoffDocument(data=data, issues=parser.issues)
