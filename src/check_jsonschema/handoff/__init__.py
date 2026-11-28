"""
Strict "handoff" parsing.

When handoff mode is enabled explicitly, instance files are parsed as they will
be handed off to a JSON-based contract:

- mapping field names are the JSON field names of the decoded data
  (YAML integer key ``1`` becomes ``"1"``, boolean key ``true`` becomes
  ``"True"``, string keys keep their decoded spelling)
- two source keys which collapse onto the same field name are reported as
  duplicate-key handoff errors
- non-finite numbers (``NaN``, ``Infinity``, ``.inf``, ...) are reported as
  non-finite handoff errors because they cannot be delivered as JSON numbers

Each handoff error carries the final field's JSON Pointer together with the
1-based line/column position of the original source text that the author must
fix. For YAML, alias usage positions on the path from the current document
location back to the original text are recorded as well.

Without handoff mode, the regular parsers in :mod:`check_jsonschema.parsers`
continue to be used and none of this machinery runs.
"""

from __future__ import annotations

import dataclasses
import typing as t

# handoff error categories
DUPLICATE_KEY = "duplicate_key"
NON_FINITE_NUMBER = "non_finite_number"


@dataclasses.dataclass(frozen=True)
class HandoffLocation:
    """A 1-based position in the original source text."""

    line: int
    column: int


@dataclasses.dataclass(frozen=True)
class HandoffAliasUse:
    """A YAML alias usage on the path back to the original source text."""

    anchor: str
    line: int
    column: int


@dataclasses.dataclass(frozen=True)
class HandoffIssue:
    category: str
    pointer: str
    locations: tuple[HandoffLocation, ...]
    alias_uses: tuple[HandoffAliasUse, ...] = ()


@dataclasses.dataclass
class HandoffDocument:
    """
    The parsed, JSON-compatible ``data`` together with the handoff issues
    found while parsing.
    """

    data: t.Any
    issues: list[HandoffIssue]


def escape_pointer_token(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def build_pointer(parts: t.Sequence[str]) -> str:
    if not parts:
        return ""
    return "".join("/" + escape_pointer_token(part) for part in parts)
