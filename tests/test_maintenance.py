import copy

import pytest

import tablib
from tablib.exceptions import (
    CircularDependencyError,
    CorrectionConflictError,
    DerivedCellCalculationError,
)

HEADERS = ("货号", "数量", "直报换算率", "参考货号", "锁定换算率")
ROWS = (
    ("A", 1, 6, None, None),
    ("C", 1, 6, None, None),
    ("B", 2, None, "A", None),
    ("D", 3, None, "B", 6),
    ("E", 4, None, "B", None),
)


def make_receipt(calls=None, bad_item=None):
    dataset = tablib.Dataset(headers=HEADERS)
    dataset.extend(ROWS)
    dataset.enable_maintenance(key="货号")

    def pieces(quantity, effective_rate):
        if bad_item is not None and calls and calls[-1][0] == bad_item:
            raise RuntimeError("boom")
        if calls is not None:
            calls.append((quantity, effective_rate))
        return quantity * effective_rate

    dataset.append_col(pieces, header="件数", maintained=True)
    if calls is not None:
        calls.clear()
    return dataset, pieces


def test_full_reconciliation_scenario():
    calls = []
    base, _ = make_receipt(calls)
    supplier = base.copy()
    warehouse = copy.deepcopy(base)

    supplier.correct("B", "参考货号", "D")
    supplier.correct("C", "直报换算率", 8)
    warehouse.lpush_col(["A区", "B区", "C区", "D区", "E区"], header="库区")
    warehouse.unlock("D")

    assert supplier["件数"] == [6, 8, 12, 18, 24]
    assert warehouse["件数"] == [6, 6, 12, 18, 24]
    assert warehouse.effective_rate("D") == 6

    with pytest.raises(CircularDependencyError) as exc_info:
        supplier.merge(warehouse)
    assert set(exc_info.value.items) == {"B", "D"}
    assert calls == [(1, 8)]

    warehouse.correct("D", "参考货号", "C")
    assert warehouse.effective_rate("D") == 6

    calls.clear()
    merged = supplier.merge(warehouse)

    assert merged.headers[0] == "库区"
    assert merged["库区"] == ["A区", "B区", "C区", "D区", "E区"]
    assert merged["件数"] == [6, 8, 16, 24, 32]
    assert calls == [(2, 8), (3, 8), (4, 8)]
    assert [entry[0] for entry in merged.maintenance.call_log] == ["B", "D", "E"]

    merged.correct("C", "直报换算率", 9)
    assert merged["件数"] == [6, 9, 18, 27, 36]
    assert base["件数"] == [6, 6, 12, 18, 24]
    assert supplier["件数"] == [6, 8, 12, 18, 24]
    assert warehouse["件数"] == [6, 6, 12, 18, 24]


def test_same_source_field_conflict_is_reported():
    base, _ = make_receipt()
    left = base.copy()
    right = base.copy()
    left.correct("C", "直报换算率", 8)
    right.correct("C", "直报换算率", 9)

    with pytest.raises(CorrectionConflictError) as exc_info:
        left.merge(right)
    assert exc_info.value.conflicts
    assert "C" in str(exc_info.value)
    assert "直报换算率" in str(exc_info.value)


def test_callback_error_keeps_tables_readable():
    fail = {"active": False}

    def pieces(quantity, effective_rate):
        if fail["active"] and effective_rate == 9:
            raise RuntimeError("bad D")
        return quantity * effective_rate

    dataset = tablib.Dataset(headers=HEADERS)
    dataset.extend(ROWS)
    dataset.enable_maintenance(key="货号")
    dataset.append_col(pieces, header="件数", maintained=True)
    old_values = dataset["件数"]
    fail["active"] = True

    with pytest.raises(DerivedCellCalculationError) as exc_info:
        dataset.correct("D", "锁定换算率", 9)
    assert exc_info.value.item == "D"
    assert exc_info.value.field == "件数"
    assert "D" in str(exc_info.value)
    assert "件数" in str(exc_info.value)
    assert dataset["件数"] == old_values
    assert dataset.effective_rate("D") == 6


def test_lock_equal_to_current_value_is_explicit():
    dataset, _ = make_receipt()
    dataset.lock("B", 6)
    assert dataset.effective_rate("B") == 6
    dataset.unlock("B")
    assert dataset.effective_rate("B") == 6


def test_normal_callable_column_still_uses_row():
    dataset = tablib.Dataset(headers=("first", "second"))
    dataset.append(("John", "Doe"))
    dataset.append_col(lambda row: row[0].upper(), header="shout")
    dataset.append(("jane", "roe"))
    assert dataset["shout"] == ["JOHN", "JANE"]
