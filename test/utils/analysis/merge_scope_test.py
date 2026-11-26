"""Tests for static MERGE delete-scope analysis.

The fixtures mirror the inventory ETL example: a ``stock`` target whose
rows may only be deleted when ``warehouse = 'north'`` (NULL warehouse is
protected), fed by a contract with one guaranteed normal row and one
guaranteed override row per SKU.
"""

import pytest

from sqlfluff.utils.analysis.merge_scope import (
    FeedContract,
    MergeCheckResult,
    Verdict,
    check_merge_delete_scope,
)

DDL = """
CREATE TABLE stock (
    sku VARCHAR NOT NULL,
    route VARCHAR NOT NULL CHECK (route IN ('S', 'H')),
    warehouse VARCHAR CHECK (warehouse IN ('north', 'south')),
    CHECK (warehouse = 'north' OR route = 'S')
);
"""

MERGE = """
MERGE INTO stock AS t
USING (
    SELECT
        sku AS stock_key,
        CASE WHEN kind = 'override' THEN backup_route ELSE route END AS lane
    FROM feed
    WHERE (kind = 'normal' AND approved IS NULL)
       OR (kind = 'override' AND approved = TRUE)
) AS s
ON t.sku = s.stock_key AND t.route = s.lane
WHEN NOT MATCHED BY TARGET THEN DO NOTHING
WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NULL AND t.route = 'H' THEN DO NOTHING
WHEN NOT MATCHED BY SOURCE THEN DELETE;
"""

CONTRACT = FeedContract(
    columns=("sku", "route", "backup_route", "kind", "approved"),
    alternatives=[
        ("SKU7", "S", None, "normal", None),
        ("SKU7", "H", "S", "override", True),
    ],
    key_column="sku",
)


def _result(**overrides):
    merge_sql = overrides.pop("merge_sql", MERGE)
    contract = overrides.pop("contract", CONTRACT)
    ddl_sql = overrides.pop("ddl_sql", DDL)
    return check_merge_delete_scope(merge_sql, ddl_sql, contract, **overrides)


def test_canonical_script_is_compliant():
    """The reference script keeps every in-scope row protected."""
    result = _result()
    assert isinstance(result, MergeCheckResult)
    assert result.compliant is True
    assert result.decidable is True
    assert len(result.branches) == 1
    branch = result.branches[0]
    assert branch.verdict == Verdict.IN_SCOPE
    assert branch.counterexample is None
    # Only the north/H row (in scope) can ever reach DELETE.
    assert branch.reachable_rows == [
        {"route": "H", "sku": "SKU7", "warehouse": "north"}
    ]


def test_dispositions_cover_every_contract_delivery():
    """Each possible target row is explained across all allowed deliveries."""
    branch = _result().branches[0]
    deliveries = {"alt1", "alt2", "alt1+alt2"}
    for disposition in branch.dispositions:
        assert set(disposition.outcomes) == deliveries

    by_row = {
        tuple(sorted(disposition.row.items())): disposition
        for disposition in branch.dispositions
    }
    assert len(by_row) == len(branch.dispositions)
    # south/S rows survive by source match in every delivery.
    south_s = next(
        disposition
        for disposition in branch.dispositions
        if disposition.row["warehouse"] == "south" and disposition.row["route"] == "S"
    )
    assert set(south_s.match_deliveries()) == deliveries
    # NULL/H rows survive via the protective branch.
    null_h = next(
        d
        for d in branch.dispositions
        if d.row["warehouse"] is None and d.row["route"] == "H"
    )
    assert set(null_h.protective_deliveries()) == deliveries
    assert null_h.protective_deliveries()["alt1"] == 0


def test_report_points_back_into_the_script():
    """The text report carries line/column evidence for every branch."""
    report = _result().render_text()
    assert "DELETE branch #1 at L14:P1" in report
    assert "kept by earlier branch" in report
    assert "kept by source match" in report


def test_weakened_normal_filter_gives_south_s_counterexample():
    """Approved IS NULL -> approved = TRUE exposes south/S stock."""
    merge_sql = MERGE.replace("approved IS NULL", "approved = TRUE")
    result = _result(merge_sql=merge_sql)
    assert result.compliant is False
    assert result.decidable is True
    counterexample = result.branches[0].counterexample
    assert counterexample is not None
    assert counterexample.target_row == {
        "route": "S",
        "sku": "SKU7",
        "warehouse": "south",
    }
    # The normal-only delivery is retained by nothing: the WHERE rejects it.
    assert counterexample.delivery_name == "alt1"
    assert counterexample.retained_rows == []
    assert counterexample.delivery[0]["kind"] == "normal"
    assert any("retained 0 feed rows" in line for line in counterexample.on_evaluation)
    report = result.render_text()
    assert "filtered out by USING WHERE" in report


def test_removed_protective_branch_gives_null_h_counterexample():
    """Dropping the NULL-warehouse guard exposes the NULL/H stock row."""
    merge_sql = MERGE.replace(
        "WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NULL "
        "AND t.route = 'H' THEN DO NOTHING\n",
        "",
    )
    result = _result(merge_sql=merge_sql)
    assert result.compliant is False
    counterexample = result.branches[0].counterexample
    assert counterexample is not None
    assert counterexample.target_row["warehouse"] is None
    assert counterexample.target_row["route"] == "H"
    # The normal feed survives the filter but does not match the H row.
    assert counterexample.retained_rows[0]["projections"] == {
        "stock_key": "SKU7",
        "lane": "S",
    }
    on_text = "\n".join(counterexample.on_evaluation)
    assert "target route='H' = 'S' -> False" in on_text


def test_changed_backup_route_gives_override_only_counterexample():
    """backup_route S -> H makes the override-only delivery delete south/S."""
    contract = FeedContract(
        columns=CONTRACT.columns,
        alternatives=[
            ("SKU7", "S", None, "normal", None),
            ("SKU7", "H", "H", "override", True),
        ],
        key_column="sku",
    )
    result = _result(contract=contract)
    assert result.compliant is False
    counterexample = result.branches[0].counterexample
    assert counterexample is not None
    assert counterexample.target_row["warehouse"] == "south"
    assert counterexample.target_row["route"] == "S"
    assert counterexample.delivery_name == "alt2"
    assert counterexample.delivery[0]["kind"] == "override"
    assert counterexample.retained_rows[0]["projections"]["lane"] == "H"


def test_renamed_aliases_columns_and_extra_parentheses():
    """Different names, values and bracket styles must analyse identically."""
    ddl_sql = """
    CREATE TABLE inventory (
        item_id VARCHAR NOT NULL,
        lane VARCHAR NOT NULL CHECK (lane IN ('S', 'H')),
        wh VARCHAR CHECK (wh IN ('north', 'south')),
        CHECK ((wh = 'north') OR (lane = 'S'))
    );
    """
    merge_sql = """
    MERGE INTO inventory AS tg
    USING (
        SELECT
            item_id AS item_key,
            CASE WHEN (kind = 'override') THEN backup_lane ELSE lane END AS item_lane
        FROM inbound
        WHERE ((kind = 'normal') AND (approved IS NULL))
           OR ((kind = 'override') AND (approved = TRUE))
    ) AS src
    ON (tg.item_id = src.item_key) AND (tg.lane = src.item_lane)
    WHEN NOT MATCHED BY TARGET THEN DO NOTHING
    WHEN NOT MATCHED BY SOURCE AND tg.wh IS NULL AND tg.lane = 'H' THEN DO NOTHING
    WHEN NOT MATCHED BY SOURCE THEN DELETE;
    """
    contract = FeedContract(
        columns=("item_id", "lane", "backup_lane", "kind", "approved"),
        alternatives=[
            ("SKU7", "S", None, "normal", None),
            ("SKU7", "H", "S", "override", True),
        ],
        key_column="item_id",
    )
    result = check_merge_delete_scope(
        merge_sql,
        ddl_sql,
        contract,
        policy_column="wh",
        policy_value="north",
    )
    assert result.compliant is True
    assert result.decidable is True
    assert result.branches[0].reachable_rows == [
        {"item_id": "SKU7", "lane": "H", "wh": "north"}
    ]


def test_contract_alternatives_may_be_given_in_any_order():
    """Ordering of the guaranteed alternatives must not change the verdict."""
    contract = FeedContract(
        columns=CONTRACT.columns,
        alternatives=[
            ("SKU7", "H", "S", "override", True),
            ("SKU7", "S", None, "normal", None),
        ],
        key_column="sku",
    )
    result = _result(contract=contract)
    assert result.compliant is True
    disposition = next(
        d
        for d in result.branches[0].dispositions
        if d.row["warehouse"] == "south" and d.row["route"] == "S"
    )
    # alt1 now names the override row, but every delivery still matches.
    assert set(disposition.match_deliveries()) == {"alt1", "alt2", "alt1+alt2"}


def test_unknown_function_is_reported_as_missing_evidence():
    """An unsupported function yields UNKNOWN plus a positioned note."""
    merge_sql = MERGE.replace(
        "CASE WHEN kind = 'override' THEN backup_route ELSE route END AS lane",
        "COALESCE(backup_route, route) AS lane",
    )
    result = _result(merge_sql=merge_sql)
    assert result.decidable is False
    assert result.branches[0].verdict == Verdict.UNKNOWN
    codes = {note.code for note in result.evidence}
    assert "unknown_function" in codes
    note = next(note for note in result.evidence if note.code == "unknown_function")
    assert note.position is not None
    assert "COALESCE" in note.snippet


def test_missing_column_domain_is_not_decided():
    """Without an IN/equality CHECK on warehouse, no finite row set exists."""
    ddl_sql = """
    CREATE TABLE stock (
        sku VARCHAR NOT NULL,
        route VARCHAR NOT NULL CHECK (route IN ('S', 'H')),
        warehouse VARCHAR
    );
    """
    result = _result(ddl_sql=ddl_sql)
    assert result.decidable is False
    assert result.branches[0].verdict == Verdict.UNKNOWN
    note = next(
        note
        for note in result.branches[0].evidence
        if note.code == "missing_column_domain"
    )
    assert "warehouse" in note.message


def test_unsupported_on_condition_is_unknown():
    """Non-equality ON predicates cannot be decided with the vocabulary."""
    merge_sql = MERGE.replace(
        "ON t.sku = s.stock_key AND t.route = s.lane",
        "ON t.sku = s.stock_key AND t.route LIKE s.lane",
    )
    result = _result(merge_sql=merge_sql)
    assert result.decidable is False
    assert result.branches[0].verdict == Verdict.UNKNOWN


def test_unparsable_script_is_undecidable_not_guessed():
    """Broken SQL produces an undecidable result, not an exception."""
    result = check_merge_delete_scope("MERGE INTO stock AS t USING (", DDL, CONTRACT)
    assert result.decidable is False
    assert result.compliant is False
    assert any(note.code == "unparsable_merge" for note in result.evidence)


def test_script_without_delete_branch_is_compliant():
    """No DELETE branch means no out-of-scope delete is possible."""
    merge_sql = MERGE.replace(
        "WHEN NOT MATCHED BY SOURCE THEN DELETE;",
        "WHEN NOT MATCHED BY SOURCE THEN DO NOTHING;",
    )
    result = _result(merge_sql=merge_sql)
    assert result.compliant is True
    assert result.decidable is True
    assert result.branches == []


def test_multiple_delete_branches_each_get_a_verdict():
    """Every DELETE branch is analysed independently with branch ordering."""
    merge_sql = MERGE.replace(
        "WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NULL "
        "AND t.route = 'H' THEN DO NOTHING\n",
        "WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NULL THEN DELETE\n"
        "WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NULL "
        "AND t.route = 'H' THEN DO NOTHING\n",
    )
    result = _result(merge_sql=merge_sql)
    assert len(result.branches) == 2
    # The earlier NULL-warehouse DELETE fires before the protective branch.
    assert result.branches[0].verdict == Verdict.OUT_OF_SCOPE
    assert result.branches[0].counterexample is not None
    assert result.branches[0].counterexample.target_row["warehouse"] is None
    # The final catch-all still only deletes the in-scope north/H row.
    assert result.branches[1].verdict == Verdict.IN_SCOPE
    assert result.branches[1].reachable_rows == [
        {"route": "H", "sku": "SKU7", "warehouse": "north"}
    ]
    assert result.compliant is False


def test_check_constraint_three_valued_logic_keeps_null_rows():
    """A NULL warehouse must not be treated as violating the OR check."""
    branch = _result().branches[0]
    enumerated_warehouses = {
        disposition.row["warehouse"] for disposition in branch.dispositions
    }
    assert None in enumerated_warehouses
    null_h = next(
        disposition
        for disposition in branch.dispositions
        if disposition.row["warehouse"] is None and disposition.row["route"] == "H"
    )
    # The NULL/H row exists and never reaches DELETE thanks to the guard.
    assert null_h.reaches(branch.index) == []


def test_invalid_contract_alternative_width_raises():
    """Malformed contracts fail loudly rather than silently."""
    contract = FeedContract(
        columns=CONTRACT.columns,
        alternatives=[("SKU7", "S", None, "normal")],
        key_column="sku",
    )
    with pytest.raises(ValueError, match="alternative has"):
        _result(contract=contract)


def test_extra_feed_rows_cannot_widen_a_delete():
    """The contract flag is accepted; guarantees alone drive the verdict."""
    contract = FeedContract(
        columns=CONTRACT.columns,
        alternatives=CONTRACT.alternatives,
        key_column="sku",
        extra_rows_allowed=True,
    )
    assert _result(contract=contract).compliant is True


def test_unsupported_branch_guard_is_unknown():
    """A LIKE guard cannot be evaluated, so the verdict stays undecidable."""
    merge_sql = MERGE.replace(
        "AND t.route = 'H' THEN DO NOTHING",
        "AND t.route LIKE 'H%' THEN DO NOTHING",
    )
    result = _result(merge_sql=merge_sql)
    assert result.decidable is False
    assert result.branches[0].verdict == Verdict.UNKNOWN
    assert any(note.code == "unsupported_branch_guard" for note in result.evidence)


def test_unsupported_filter_is_unknown():
    """A function inside the USING filter blocks retention analysis."""
    merge_sql = MERGE.replace(
        "(kind = 'normal' AND approved IS NULL)",
        "(COALESCE(kind, 'x') = 'normal' AND approved IS NULL)",
    )
    result = _result(merge_sql=merge_sql)
    assert result.decidable is False
    assert any(note.code == "unknown_function" for note in result.evidence)
    assert result.branches[0].verdict == Verdict.UNKNOWN


def test_unsupported_check_reports_missing_evidence():
    """A function-based CHECK cannot derive a finite warehouse domain."""
    ddl_sql = """
    CREATE TABLE stock (
        sku VARCHAR NOT NULL,
        route VARCHAR NOT NULL CHECK (route IN ('S', 'H')),
        warehouse VARCHAR CHECK (UPPER(warehouse) IN ('NORTH', 'SOUTH'))
    );
    """
    result = _result(ddl_sql=ddl_sql)
    assert result.decidable is False
    assert any(note.code == "unknown_function" for note in result.evidence)
    assert any(
        note.code == "missing_column_domain" and "warehouse" in note.message
        for note in result.branches[0].evidence
    )


def test_unparsable_ddl_is_undecidable():
    """Broken DDL produces evidence instead of a guessed verdict."""
    result = check_merge_delete_scope(MERGE, "CREATE TABLE stock (", CONTRACT)
    assert result.decidable is False
    assert any(note.code == "unparsable_ddl" for note in result.evidence)


def test_contract_key_outside_columns_is_undecidable():
    """A key column absent from the feed columns cannot be projected."""
    contract = FeedContract(
        columns=CONTRACT.columns,
        alternatives=CONTRACT.alternatives,
        key_column="unknown_key",
    )
    result = _result(contract=contract)
    assert result.decidable is False
    assert any(note.code == "missing_contract_key" for note in result.evidence)


def test_not_null_and_not_in_vocabulary_in_delete_guard():
    """IS NOT NULL and NOT IN guards evaluate with SQL null semantics."""
    merge_sql = MERGE.replace(
        "WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NULL "
        "AND t.route = 'H' THEN DO NOTHING\n"
        "WHEN NOT MATCHED BY SOURCE THEN DELETE;",
        "WHEN NOT MATCHED BY SOURCE AND t.warehouse IS NOT NULL "
        "AND t.route NOT IN ('S') THEN DELETE;",
    )
    result = _result(merge_sql=merge_sql)
    assert result.compliant is True
    # Only the non-null north/H row satisfies the NOT NULL / NOT IN guard.
    assert result.branches[0].reachable_rows == [
        {"route": "H", "sku": "SKU7", "warehouse": "north"}
    ]


def test_counterexample_carries_script_positions():
    """Every branch and piece of evidence links back to a source position."""
    result = _result(merge_sql=MERGE.replace("approved IS NULL", "approved = TRUE"))
    counterexample = result.branches[0].counterexample
    assert counterexample is not None
    assert counterexample.delete_branch_position.line == 14
    branch_path = "\n".join(counterexample.branch_path)
    assert "L13:P1" in branch_path
    assert "L14:P1" in branch_path


def test_empty_contract_is_undecidable():
    """A contract with no guarantees cannot prove any row is protected."""
    contract = FeedContract(columns=CONTRACT.columns, alternatives=[], key_column="sku")
    result = _result(contract=contract)
    assert result.decidable is False
    assert any(note.code == "empty_contract" for note in result.evidence)


def test_using_without_where_retains_every_feed_row():
    """A USING subquery with no WHERE treats all contract rows as retained."""
    merge_sql = MERGE.replace(
        "    FROM feed\n"
        "    WHERE (kind = 'normal' AND approved IS NULL)\n"
        "       OR (kind = 'override' AND approved = TRUE)\n",
        "    FROM feed\n",
    )
    result = _result(merge_sql=merge_sql)
    assert result.decidable is True
    assert result.compliant is True
    south_s = next(
        disposition
        for disposition in result.branches[0].dispositions
        if disposition.row["warehouse"] == "south" and disposition.row["route"] == "S"
    )
    assert set(south_s.match_deliveries()) == {"alt1", "alt2", "alt1+alt2"}


def test_guarded_delete_without_match_leaves_row_unchanged():
    """A guarded-only DELETE that never matches is not a delete at all."""
    merge_sql = MERGE.replace(
        "WHEN NOT MATCHED BY SOURCE THEN DELETE;",
        "WHEN NOT MATCHED BY SOURCE AND t.route = 'S' THEN DELETE;",
    )
    result = _result(merge_sql=merge_sql)
    branch = result.branches[0]
    # Unmatched rows are H rows, so an S-only guard never fires: in scope but
    # empty, and every unmatched row is left untouched rather than deleted.
    assert branch.verdict == Verdict.IN_SCOPE
    assert branch.reachable_rows == []
    north_h = next(
        disposition
        for disposition in branch.dispositions
        if disposition.row["warehouse"] == "north" and disposition.row["route"] == "H"
    )
    assert north_h.reaches(branch.index) == []
    assert set(north_h.no_action_deliveries()) == {"alt1", "alt2", "alt1+alt2"}
    # The NULL/H row is still caught by the earlier protective branch.
    null_h = next(
        disposition
        for disposition in branch.dispositions
        if disposition.row["warehouse"] is None and disposition.row["route"] == "H"
    )
    assert set(null_h.protective_deliveries()) == {"alt1", "alt2", "alt1+alt2"}
