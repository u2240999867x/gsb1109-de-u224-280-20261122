"""Shared implementation for the row-pick determinism rules.

The mixin is deliberately not a :class:`BaseRule` subclass: rule classes must
be declaratively named ``Rule_XXXX`` for the rule metaclass, so the shared
implementation is composed into each rule instead of being a rule itself.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from sqlfluff.core.errors import SQLFluffUserError
from sqlfluff.core.rules import BaseRule, LintResult, RuleContext
from sqlfluff.utils.analysis.catalog import Catalog, load_catalog
from sqlfluff.utils.analysis.row_pick import (
    RowPickResult,
    RowPickStatus,
    analyze_select,
)

# Config keys accepted as a catalog source.  ``catalog_path`` is resolved
# relative to the config file by the config loader; the others are accepted
# for convenience and resolved relative to the file being linted.
CATALOG_CONFIG_KEYS = (
    "catalog",
    "catalog_path",
    "catalog_file",
    "table_catalog",
    "row_pick_catalog",
    "row_pick_catalog_path",
    "row_pick_catalog_file",
    "schema_catalog",
)


class RowPickDeterminismMixin:
    """Mixin implementing ``ROW_NUMBER()`` row-pick determinism checks.

    The rule inspects the common "latest row per group" idiom::

        SELECT ...
        FROM (
            SELECT ...,
                   ROW_NUMBER() OVER (
                       PARTITION BY ...
                       ORDER BY ...
                   ) AS rn
            FROM ...
        ) picked
        WHERE rn = 1

    A row pick is only deterministic if, within each partition, the ordering
    (plus any filters) uniquely identifies every row.  The catalog declares
    natural keys, nullability and relationship cardinality; without that
    evidence the rule reports *insufficient evidence* rather than claiming a
    problem or a proof.

    The rule never proposes a fix: adding a tie-breaker changes the meaning of
    a query, so the decision must stay with the author.
    """

    # These annotations satisfy mypy; the real values come from rule config.
    catalog: Optional[Any]
    catalog_path: Optional[Any]
    report_insufficient_evidence: bool

    # ------------------------------------------------------------------
    # Catalog access
    # ------------------------------------------------------------------
    def _catalog(self, context: RuleContext) -> tuple[Optional[Catalog], bool]:
        """Return ``(catalog, configured)`` for this rule evaluation."""
        assert isinstance(self, BaseRule)
        section = context.config.get_section(("rules", self.get_config_ref())) or {}
        value: Any = None
        for key in CATALOG_CONFIG_KEYS:
            candidate = section.get(key)
            if candidate not in (None, "", "None"):
                value = candidate
                break
        # Also allow the catalog to live in the generic rules section.
        if value is None:
            generic = context.config.get_section("rules") or {}
            for key in CATALOG_CONFIG_KEYS:
                candidate = generic.get(key)
                if candidate not in (None, "", "None"):
                    value = candidate
                    break
        if value is None:
            return None, False
        base_paths: list[str] = []
        if context.path is not None:
            base_paths.append(str(context.path.parent))
        # ``catalog_path`` may have been resolved relative to the config file
        # already, but inline configs (and config strings) can leave it
        # relative.  Try both the SQL file and the working directory.
        base_paths.append(os.getcwd())
        if isinstance(value, str):
            expanded = os.path.expanduser(value)
            if not os.path.isabs(expanded):
                for base_path in base_paths:
                    candidate = os.path.join(base_path, expanded)
                    if os.path.exists(candidate):
                        value = candidate
                        break
        last_error: Optional[Exception] = None
        for base_path in base_paths:
            try:
                return load_catalog(value, base_path=base_path), True
            except (OSError, SQLFluffUserError) as err:
                last_error = err
                continue
        if last_error is not None:
            # A configured catalog which cannot be read is a user error, not a
            # silent pass: report it rather than pretending there is no
            # evidence.
            raise SQLFluffUserError(
                f"Could not load the row-pick catalog: {last_error}"
            ) from last_error
        return None, False

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------
    def _eval(self, context: RuleContext) -> Optional[list[LintResult]]:
        """Assess every row pick in this SELECT."""
        if not context.segment.is_type("select_statement"):
            return None
        catalog, configured = self._catalog(context)
        if catalog is None:
            # Running the rule without a catalog cannot prove anything.  Do
            # not manufacture a result: "the rule is enabled" is not evidence.
            return None
        results = analyze_select(
            context.segment,
            catalog,
            dialect_name=context.dialect.name,
            configured_catalog=configured,
        )
        lint_results: list[LintResult] = []
        for result in results:
            if result.status is RowPickStatus.DETERMINISTIC:
                continue
            if (
                result.status is RowPickStatus.INSUFFICIENT_EVIDENCE
                and not self.report_insufficient_evidence
            ):
                continue
            lint_results.append(self._lint_result(result))
        return lint_results or None

    @staticmethod
    def _lint_result(result: RowPickResult) -> LintResult:
        """Convert an analysis result into a lint violation."""
        return LintResult(
            anchor=result.anchor,
            description=result.message,
        )
