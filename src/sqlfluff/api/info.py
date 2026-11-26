"""Information API."""

from typing import Optional

from sqlfluff.core import Linter, dialect_readout
from sqlfluff.core.dialects import DialectTuple
from sqlfluff.core.linter import RuleTuple
from sqlfluff.utils.analysis.catalog import Catalog, load_catalog
from sqlfluff.utils.analysis.row_pick import RowPickResult, analyze_row_pick


def list_rules() -> list[RuleTuple]:
    """Return a list of available rule tuples."""
    linter = Linter()
    return linter.rule_tuples()


def list_dialects() -> list[DialectTuple]:
    """Return a list of available dialect info."""
    return list(dialect_readout())


def analyze_row_picks(
    sql: str,
    catalog: Optional[Catalog | dict | str] = None,
    dialect: str = "ansi",
) -> list[RowPickResult]:
    """Analyse ``ROW_NUMBER()`` row picks for determinism.

    Args:
        sql: The SQL text to analyse.
        catalog: A :class:`Catalog`, a mapping (inline catalog), or a path to
            a YAML/JSON catalog.
        dialect: The SQL dialect to parse with.

    Returns:
        One :class:`RowPickResult` per row pick filtered to rank one.
    """
    linter = Linter(dialect=dialect)
    parsed = linter.parse_string(sql)
    if parsed.tree is None:
        return []
    loaded_catalog = catalog if isinstance(catalog, Catalog) else load_catalog(catalog)
    return analyze_row_pick(parsed.tree, loaded_catalog, dialect_name=dialect)
