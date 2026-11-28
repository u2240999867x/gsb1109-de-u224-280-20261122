from __future__ import annotations

import pathlib

import jsonschema

from .handoff import HandoffIssue
from .parsers import ParseError


class CheckResult:
    def __init__(self) -> None:
        self.validation_errors: dict[str, list[jsonschema.ValidationError]] = {}
        self.parse_errors: dict[str, list[ParseError]] = {}
        self.handoff_errors: dict[str, list[HandoffIssue]] = {}
        self.successes: list[str] = []

    @property
    def success(self) -> bool:
        return not (
            bool(self.parse_errors)
            or bool(self.validation_errors)
            or bool(self.handoff_errors)
        )

    def record_validation_success(self, path: pathlib.Path | str) -> None:
        self.successes.append(str(path))

    def record_validation_error(
        self, path: pathlib.Path | str, err: jsonschema.ValidationError
    ) -> None:
        filename = str(path)
        if filename not in self.validation_errors:
            self.validation_errors[filename] = []
        self.validation_errors[filename].append(err)

    def record_parse_error(self, path: pathlib.Path | str, err: ParseError) -> None:
        filename = str(path)
        if filename not in self.parse_errors:
            self.parse_errors[filename] = []
        self.parse_errors[filename].append(err)

    def record_handoff_errors(
        self, path: pathlib.Path | str, issues: list[HandoffIssue]
    ) -> None:
        filename = str(path)
        if filename not in self.handoff_errors:
            self.handoff_errors[filename] = []
        self.handoff_errors[filename].extend(issues)
