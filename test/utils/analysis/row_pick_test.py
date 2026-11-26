"""Tests for the deterministic row-pick analysis."""

from __future__ import annotations

import textwrap

import pytest

from sqlfluff.core import FluffConfig, Linter
from sqlfluff.utils.analysis.catalog import Catalog, _clean_name, load_catalog
from sqlfluff.utils.analysis.row_pick import (
    DuplicateKind,
    RowPickStatus,
    analyze_row_pick,
)

CATALOG = {
    "tables": {
        "site_catalog": {"primary_key": "site_id"},
        "event": {
            "primary_key": ["site_id", "event_time", "record_no"],
        },
        "receipt": {
            "primary_key": "receipt_no",
            "columns": {
                "record_no": {"nullable": False},
                "receipt_no": {"nullable": False},
            },
        },
        "event_detail": {
            "primary_key": ["site_id", "event_time", "record_no"],
            "columns": {
                "note": {"nullable": True},
            },
        },
    },
    "relationships": [
        {
            "name": "catalog_per_site",
            "left": "site_catalog",
            "right": "event",
            "left_columns": ["site_id"],
            "right_columns": ["site_id"],
            "cardinality": "one_to_many",
        },
        {
            "name": "receipts_per_record",
            "left": "receipt",
            "right": "event",
            "left_columns": ["record_no"],
            "right_columns": ["record_no"],
            "cardinality": "many_to_one",
        },
    ],
}

ORDERING = "PARTITION BY e.site_id ORDER BY e.event_time DESC, e.record_no"


def _catalog() -> Catalog:
    return Catalog.from_dict(CATALOG)


def _analyze(sql: str, dialect: str = "ansi", catalog: Catalog | None = None):
    linter = Linter(config=FluffConfig(overrides={"dialect": dialect}))
    parsed = linter.parse_string(textwrap.dedent(sql))
    assert not parsed.violations, parsed.violations
    assert parsed.tree
    return analyze_row_pick(parsed.tree, catalog if catalog is not None else _catalog())


def _single(sql: str, dialect: str = "ansi", catalog: Catalog | None = None):
    results = _analyze(sql, dialect, catalog)
    assert len(results) == 1, [result.message for result in results]
    return results[0]


# ---------------------------------------------------------------------------
# Catalog parsing
# ---------------------------------------------------------------------------


def test_catalog_declares_keys_and_nullability():
    """Table, key and nullability declarations survive normalisation."""
    catalog = _catalog()
    event = catalog.table("event")
    assert event is not None
    assert event.keys[0].columns == ("SITE_ID", "EVENT_TIME", "RECORD_NO")
    assert catalog.column_nullable("event", "event_time") is False
    assert catalog.column_nullable("event_detail", "note") is True
    assert catalog.unique_keys("receipt")[0].columns == ("RECEIPT_NO",)


def test_catalog_accepts_common_aliases():
    """Natural-key, relationship and cardinality aliases are understood."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {
                    "natural_key": "site_id, event_time, record_no",
                    "nullable_keys": ["event_time"],
                },
                "site_catalog": {"keys": [{"columns": ["site_id"], "name": "site"}]},
            },
            "relations": [
                {
                    "parent": "site_catalog",
                    "child": "event",
                    "on": {"site_id": "site_id"},
                    "cardinality": "one_to_many",
                }
            ],
        }
    )
    event = catalog.table("event")
    assert event is not None
    assert event.keys[0].columns == ("SITE_ID", "EVENT_TIME", "RECORD_NO")
    relationship = catalog.relationships[0]
    assert (relationship.left_max_one, relationship.right_max_one) == (True, False)


def test_catalog_from_inline_yaml(tmp_path):
    """An inline YAML catalog and a file produce the same metadata."""
    inline = "tables:\n  event:\n    primary_key: [site_id, record_no]\n"
    from_text = load_catalog(inline)
    path = tmp_path / "catalog.yml"
    path.write_text(inline, encoding="utf-8")
    from_file = load_catalog(str(path))
    assert from_text.unique_keys("event")[0].columns == ("SITE_ID", "RECORD_NO")
    assert from_file.unique_keys("event")[0].columns == ("SITE_ID", "RECORD_NO")


def test_catalog_is_read_only_snapshot():
    """Callers get a plain snapshot; the source mapping is unchanged."""
    raw = {"tables": {"event": {"primary_key": "id"}}}
    catalog = Catalog.from_dict(raw)
    assert raw == {"tables": {"event": {"primary_key": "id"}}}
    catalog.table("event").keys.clear()  # type: ignore[union-attr]
    assert Catalog.from_dict(raw).unique_keys("event")


# ---------------------------------------------------------------------------
# The two main queries
# ---------------------------------------------------------------------------


def test_one_row_catalog_join_is_deterministic():
    """A catalog with one row per site cannot amplify the event row."""
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
            JOIN site_catalog AS c ON c.site_id = e.site_id
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.status is RowPickStatus.DETERMINISTIC
    assert result.order_by == ("e.event_time DESC", "e.record_no")
    assert "SITE_CATALOG" not in " ".join(
        cause.table or "" for cause in result.duplicate_causes
    )
    assert any("at most one row" in reason for reason in result.reasons)


def test_two_receipts_per_record_are_indeterminate():
    """The receipt join duplicates each event; record_no no longer decides."""
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
            JOIN receipt AS r ON r.record_no = e.record_no
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert any(
        cause.kind is DuplicateKind.JOIN_AMPLIFICATION
        for cause in result.duplicate_causes
    )
    assert "RECEIPT_NO" in result.message
    assert "r.event_no" not in result.order_by


def test_outer_distinct_one_column_cannot_repair_window():
    """DISTINCT removes duplicates after the window has ranked both rows."""
    result = _single(
        f"""
        SELECT DISTINCT picked.site_id
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
            JOIN receipt AS r ON r.record_no = e.record_no
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert result.outer_distinct is True
    assert "Outer DISTINCT cannot repair the window" in result.message


def test_named_window_reuse():
    """A named WINDOW definition is resolved and exposed to the message."""
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER latest AS rn
            FROM event AS e
            WINDOW latest AS ({ORDERING})
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.status is RowPickStatus.DETERMINISTIC
    assert result.partition_by == ("e.site_id",)
    assert result.order_by == ("e.event_time DESC", "e.record_no")


def test_named_window_partition_with_inline_order():
    """An inline ORDER BY on a named window extends its partition."""
    result = _single(
        """
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (latest ORDER BY e.record_no) AS rn
            FROM event AS e
            WINDOW latest AS (PARTITION BY e.site_id ORDER BY e.event_time DESC)
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.partition_by == ("e.site_id",)
    assert result.order_by == ("e.record_no",)
    assert result.status is RowPickStatus.INDETERMINATE


# ---------------------------------------------------------------------------
# Key tails and nullability
# ---------------------------------------------------------------------------


def test_inner_fixed_key_tail_is_deterministic():
    """An inner equality on the extra key column completes the key."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {
                    "primary_key": [
                        "site_id",
                        "event_time",
                        "record_no",
                        "event_seq",
                    ],
                }
            }
        }
    )
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no, e.event_seq,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
            WHERE e.event_seq = 7
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=catalog,
    )
    assert result.status is RowPickStatus.DETERMINISTIC


def test_outer_fixed_key_tail_is_deterministic():
    """An outer equality on a projected key tail is propagated inward."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {
                    "primary_key": [
                        "site_id",
                        "event_time",
                        "record_no",
                        "event_seq",
                    ],
                }
            }
        }
    )
    result = _single(
        f"""
        SELECT picked.site_id, picked.event_seq
        FROM (
            SELECT e.site_id, e.event_time, e.record_no, e.event_seq,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
          AND picked.event_seq = 7
        """,
        catalog=catalog,
    )
    assert result.status is RowPickStatus.DETERMINISTIC


def test_unfiltered_key_tail_is_indeterminate():
    """Without the key-tail filter the same query is not proven."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {
                    "primary_key": [
                        "site_id",
                        "event_time",
                        "record_no",
                        "event_seq",
                    ],
                }
            }
        }
    )
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no, e.event_seq,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=catalog,
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert "EVENT_SEQ" in result.message


def test_nullable_receipt_column_in_order_is_indeterminate():
    """A same-named nullable column cannot be used as a tie-breaker."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {"primary_key": ["site_id", "event_time", "record_no"]},
                "receipt": {
                    "primary_key": "receipt_no",
                    "columns": {"note": {"nullable": True}},
                },
            }
        }
    )
    result = _single(
        """
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.site_id
                       ORDER BY e.event_time DESC, e.record_no, r.note
                   ) AS rn
            FROM event AS e
            JOIN receipt AS r ON r.record_no = e.record_no
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=catalog,
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert any(
        cause.kind is DuplicateKind.NULLABLE_KEY for cause in result.duplicate_causes
    )


def test_nullable_key_is_usable_after_is_not_null_filter():
    """Pinning a nullable key column with IS NOT NULL makes the key usable."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {
                    "primary_key": ["site_id", "event_time", "record_no"],
                    "nullable": ["event_time"],
                }
            }
        }
    )
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
            WHERE e.event_time IS NOT NULL
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=catalog,
    )
    assert result.status is RowPickStatus.DETERMINISTIC


def test_nullable_key_without_filter_is_indeterminate():
    """A nullable key column allows multiple NULLs to tie."""
    catalog = Catalog.from_dict(
        {
            "tables": {
                "event": {
                    "primary_key": ["site_id", "event_time", "record_no"],
                    "nullable": ["event_time"],
                }
            }
        }
    )
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=catalog,
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert any(
        cause.kind is DuplicateKind.NULLABLE_KEY for cause in result.duplicate_causes
    )


# ---------------------------------------------------------------------------
# Evidence boundaries
# ---------------------------------------------------------------------------


def test_missing_catalog_business_key_is_insufficient():
    """A table absent from the catalog yields insufficient evidence, not a lie."""
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM unknown_event AS e
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=Catalog.from_dict(
            {
                "tables": {
                    "event": {"primary_key": ["site_id", "event_time", "record_no"]}
                }
            }
        ),
    )
    assert result.status is RowPickStatus.INSUFFICIENT_EVIDENCE
    assert any(
        cause.kind is DuplicateKind.MISSING_CATALOG for cause in result.duplicate_causes
    )


def test_undeclared_business_key_is_insufficient():
    """A declared table without a natural key is still insufficient."""
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
        """,
        catalog=Catalog.from_dict({"tables": {"event": {"columns": ["site_id"]}}}),
    )
    assert result.status is RowPickStatus.INSUFFICIENT_EVIDENCE


def test_missing_order_is_indeterminate():
    """Without ORDER BY the window has no deterministic choice."""
    result = _single(
        """
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time,
                   ROW_NUMBER() OVER (PARTITION BY e.site_id) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert "no ORDER BY" in result.message


def test_plain_row_number_without_rank_filter_is_ignored():
    """A window function which is not used to pick one row is out of scope."""
    results = _analyze(
        """
        SELECT e.site_id,
               ROW_NUMBER() OVER (PARTITION BY e.site_id ORDER BY e.event_time) AS rn
        FROM event AS e
        """
    )
    assert results == []


def test_public_api_analyze_row_picks():
    """The public simple API returns the same assessment."""
    import sqlfluff

    results = sqlfluff.analyze_row_picks(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
        """,
        CATALOG,
    )
    assert len(results) == 1
    assert results[0].status is RowPickStatus.DETERMINISTIC
    assert results[0].to_dict()["deterministic"] is True


def test_cte_row_pick_is_analysed():
    """A CTE row pick consumed by an outer WHERE rn = 1 is in scope."""
    result = _single(
        """
        WITH ranked AS (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.site_id
                       ORDER BY e.event_time DESC, e.record_no
                   ) AS rn
            FROM event AS e
        )
        SELECT ranked.site_id, ranked.event_time
        FROM ranked
        WHERE ranked.rn = 1
        """
    )
    assert result.status is RowPickStatus.DETERMINISTIC
    assert result.rank_alias == "rn"


def test_cte_outer_distinct_one_column_cannot_repair_window():
    """The outer CTE projection cannot rescue a tied ordering."""
    result = _single(
        """
        WITH ranked AS (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.site_id
                       ORDER BY e.event_time DESC
                   ) AS rn
            FROM event AS e
        )
        SELECT DISTINCT ranked.site_id
        FROM ranked
        WHERE ranked.rn = 1
        """
    )
    assert result.status is RowPickStatus.INDETERMINATE
    assert result.outer_distinct is True


def test_rank_alias_and_status_aliases():
    """The public result exposes readable aliases for consumers/tests."""
    result = _single(
        f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
        """
    )
    assert result.ordering_expressions == result.order_by
    assert result.partition_expressions == result.partition_by
    assert result.rank_alias == "rn"
    assert RowPickStatus.NOT_DETERMINISTIC is RowPickStatus.INDETERMINATE
    assert RowPickStatus.INSUFFICIENT is RowPickStatus.INSUFFICIENT_EVIDENCE


def test_qualify_duplicate_window_is_analysed_once():
    """QUALIFY repeating the projected window function is one row pick."""
    results = _analyze(
        f"""
        SELECT e.site_id, e.event_time, e.record_no,
               ROW_NUMBER() OVER ({ORDERING}) AS rn
        FROM event AS e
        QUALIFY ROW_NUMBER() OVER ({ORDERING}) = 1
        """,
        dialect="snowflake",
    )
    assert len(results) == 1
    assert results[0].status is RowPickStatus.DETERMINISTIC


# ---------------------------------------------------------------------------
# Rule integration
# ---------------------------------------------------------------------------


def test_st13_reports_only_with_catalog():
    """No catalog means no result at all; a catalog makes the check run."""
    sql = """
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (PARTITION BY e.site_id
                                      ORDER BY e.event_time DESC) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
    """
    configs = {
        "rules": {
            "structure.deterministic_row_pick": {"catalog": CATALOG},
        }
    }
    with_catalog = Linter(
        config=FluffConfig(
            configs=configs,
            overrides={"dialect": "ansi", "rules": "ST13"},
        )
    ).lint_string(sql)
    assert any(v.rule_code() == "ST13" for v in with_catalog.violations)

    without_catalog = Linter(
        config=FluffConfig(overrides={"dialect": "ansi", "rules": "ST13"})
    ).lint_string(sql)
    assert not any(v.rule_code() == "ST13" for v in without_catalog.violations)


def test_st13_does_not_flag_proven_query():
    """A deterministic pick produces no violation."""
    sql = f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
            JOIN site_catalog AS c ON c.site_id = e.site_id
        ) AS picked
        WHERE picked.rn = 1
    """
    linted = Linter(
        config=FluffConfig(
            configs={
                "rules": {
                    "structure.deterministic_row_pick": {"catalog": CATALOG},
                }
            },
            overrides={"dialect": "ansi", "rules": "ST13"},
        )
    ).lint_string(sql)
    assert not any(v.rule_code() == "ST13" for v in linted.violations)


def test_am10_alias_behaves_like_st13():
    """The ambiguity-grouped rule uses the same analysis."""
    sql = """
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (PARTITION BY e.site_id
                                      ORDER BY e.event_time DESC) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
    """
    linted = Linter(
        config=FluffConfig(
            configs={
                "rules": {
                    "ambiguous.row_pick_determinism": {"catalog": CATALOG},
                }
            },
            overrides={"dialect": "ansi", "rules": "AM10"},
        )
    ).lint_string(sql)
    assert any(v.rule_code() == "AM10" for v in linted.violations)


def test_catalog_path_empty_falls_back_to_inline_catalog():
    """An empty ``catalog_path`` does not shadow an inline ``catalog``."""
    sql = f"""
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER ({ORDERING}) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
    """
    linted = Linter(
        config=FluffConfig(
            configs={
                "rules": {
                    "structure.deterministic_row_pick": {
                        "catalog_path": "",
                        "catalog": CATALOG,
                    }
                }
            },
            overrides={"dialect": "ansi", "rules": "ST13"},
        )
    ).lint_string(sql)
    assert not any(v.rule_code() == "ST13" for v in linted.violations)


def test_catalog_path_config_is_relative_to_source_file(tmp_path):
    """A catalog path in config is resolved next to the SQL/config tree."""
    catalog_path = tmp_path / "catalog.yml"
    catalog_path.write_text(
        "tables:\n  event:\n    primary_key: [site_id, event_time, record_no]\n",
        encoding="utf-8",
    )
    sql = """
        SELECT *
        FROM (
            SELECT e.site_id, e.event_time, e.record_no,
                   ROW_NUMBER() OVER (PARTITION BY e.site_id
                                      ORDER BY e.event_time DESC) AS rn
            FROM event AS e
        ) AS picked
        WHERE picked.rn = 1
    """
    linter = Linter(
        config=FluffConfig(
            configs={
                "rules": {
                    "structure.deterministic_row_pick": {"catalog_path": "catalog.yml"}
                }
            },
            overrides={"dialect": "ansi", "rules": "ST13"},
        )
    )
    # ``lint_string`` carries the source path through to the rule context,
    # which is what resolves a relative catalog path.
    linted = linter.lint_string(sql, fname=str(tmp_path / "query.sql"))
    assert any(v.rule_code() == "ST13" for v in linted.violations)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Site_Catalog", "SITE_CATALOG"),
        ("`site_catalog`", "SITE_CATALOG"),
        ("schema.site_catalog", "SITE_CATALOG"),
        ('"site_catalog"', "SITE_CATALOG"),
    ],
)
def test_clean_name_variants(name, expected):
    """Quoted and qualified names normalise to a simple uppercase name."""
    assert _clean_name(name) == expected
