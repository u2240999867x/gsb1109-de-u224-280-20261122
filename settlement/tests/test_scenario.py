"""场景验证：供应商重传结算明细的去重迁移（线上 + 离线 SQL）。"""

from __future__ import annotations

import sqlite3

import pytest
import sqlalchemy.exc

from settlement import scenario
from settlement.offline_runner import apply_offline_migration


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "settlement.db"
    scenario.create_baseline(path)
    scenario.seed(path)
    return path


def assert_migrated_state(path):
    connection = scenario.connect(path)
    try:
        details = connection.execute(
            "SELECT id, supplier_code, order_no, amount_text,"
            " amount_cents, quantity, total_cents"
            " FROM settlement_detail ORDER BY id"
        ).fetchall()
        assert details == [
            (10, "SUP1", "AB-7", "01.25", 125, 2, 250),
            (20, "SUP2", "AB-7", "3.00", 300, 1, 300),
        ]
        receipts = connection.execute(
            "SELECT id, detail_id, product_code, received_qty"
            " FROM receipt_line ORDER BY id"
        ).fetchall()
        assert receipts == [
            (101, 10, "P-1", 2),
            (102, 10, "P-1", 2),
            (201, 20, "P-9", 1),
        ]
    finally:
        connection.close()
    assert scenario.current_version(path) == "0002"


def assert_conflict_state(path, quantity_text="3"):
    """冲突失败后：两张旧表和版本都保持原样、仍可使用。"""
    connection = scenario.connect(path)
    try:
        details = connection.execute(
            "SELECT id, supplier_code, order_no, amount_text, quantity_text"
            " FROM settlement_detail ORDER BY id"
        ).fetchall()
        assert details == [
            (10, "SUP1", " ab-7 ", "01.25", "2"),
            (17, "SUP1", "AB-7", "1.25", quantity_text),
            (20, "SUP2", "AB-7", "3.00", "1"),
        ]
        receipts = connection.execute(
            "SELECT id, detail_id FROM receipt_line ORDER BY id"
        ).fetchall()
        assert receipts == [(101, 10), (102, 17), (201, 20)]
    finally:
        connection.close()
    assert scenario.current_version(path) == "0001"


def assert_conflict_report(path):
    connection = scenario.connect(path)
    try:
        conflicts = scenario.load_plan().find_conflicts(connection)
    finally:
        connection.close()
    assert conflicts == [
        {"supplier_code": "SUP1", "order_no": "AB-7", "detail_ids": [10, 17]}
    ]


def test_online_conflict_then_retry(db_path):
    # 明细 17 的数量改成 3：同组数量不一致，整次迁移必须失败
    scenario.set_quantity_text(db_path, 17, "3")
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        scenario.run_upgrade(db_path)
    # 报出冲突组里的原明细 ID，两张旧表及版本仍可使用
    assert_conflict_report(db_path)
    assert_conflict_state(db_path, quantity_text="3")

    # 数量改回 2 后重试：迁移成功
    scenario.set_quantity_text(db_path, 17, "2")
    scenario.run_upgrade(db_path)
    assert_migrated_state(db_path)


def test_offline_executor_conflict_then_retry(db_path):
    scenario.set_quantity_text(db_path, 17, "3")
    sql_text = scenario.generate_offline_sql(db_path)
    assert "CREATE TEMP TABLE _dedupe_guard_settlement_detail" in sql_text
    assert "_alembic_tmp_settlement_detail" in sql_text
    assert "alembic_version" in sql_text

    connection = scenario.connect(db_path)
    try:
        # 外层事务先记下接收批号，再逐句执行本次迁移
        connection.execute("BEGIN")
        connection.execute(
            "INSERT INTO receive_batch (batch_no, note) VALUES (?, ?)",
            ("RCV-20261126-01", "含冲突的重传批次"),
        )
        with pytest.raises(sqlite3.IntegrityError):
            apply_offline_migration(connection, sql_text)
        # 冲突明细另查；换表和版本登记都没有发生
        conflicts = scenario.load_plan().find_conflicts(connection)
        assert conflicts == [
            {
                "supplier_code": "SUP1",
                "order_no": "AB-7",
                "detail_ids": [10, 17],
            }
        ]
        # 迁移的改动被撤销，但外层事务记下的接收批号仍能提交
        connection.commit()
    finally:
        connection.close()
    assert_conflict_state(db_path, quantity_text="3")

    connection = scenario.connect(db_path)
    try:
        batches = connection.execute(
            "SELECT batch_no FROM receive_batch"
        ).fetchall()
        assert batches == [("RCV-20261126-01",)]
    finally:
        connection.close()

    # 数量改回 2 后重试同一份离线 SQL：迁移成功
    scenario.set_quantity_text(db_path, 17, "2")
    connection = scenario.connect(db_path)
    try:
        connection.execute("BEGIN")
        connection.execute(
            "INSERT INTO receive_batch (batch_no, note) VALUES (?, ?)",
            ("RCV-20261126-02", "修正后的重传批次"),
        )
        apply_offline_migration(connection, sql_text)
        connection.commit()
    finally:
        connection.close()
    assert_migrated_state(db_path)

    connection = scenario.connect(db_path)
    try:
        batches = connection.execute(
            "SELECT batch_no FROM receive_batch ORDER BY id"
        ).fetchall()
        assert batches == [("RCV-20261126-01",), ("RCV-20261126-02",)]
    finally:
        connection.close()


def test_constraints_after_migration(db_path):
    scenario.run_upgrade(db_path)
    connection = scenario.connect(db_path)
    try:
        # 规范单号在供应商内唯一
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO settlement_detail"
                " (id, supplier_code, order_no, amount_text, quantity)"
                " VALUES (30, 'SUP1', 'AB-7', '9.99', 1)"
            )
        # 另一个供应商可以有同名单号
        connection.execute(
            "INSERT INTO settlement_detail"
            " (id, supplier_code, order_no, amount_text, quantity)"
            " VALUES (31, 'SUP3', 'AB-7', '9.99', 1)"
        )
        # 外键检查全程开启
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO receipt_line"
                " (id, detail_id, product_code, received_qty)"
                " VALUES (999, 12345, 'X', 1)"
            )
        # 删除级联保留：删掉明细 10，收货行 101、102 被级联删除
        connection.execute("DELETE FROM settlement_detail WHERE id = 10")
        receipts = connection.execute(
            "SELECT id FROM receipt_line ORDER BY id"
        ).fetchall()
        assert receipts == [(201,)]
    finally:
        connection.close()


def test_downgrade_restores_old_schema(db_path):
    scenario.run_upgrade(db_path)
    from alembic import command

    command.downgrade(scenario.make_config(db_path), "0001")
    connection = scenario.connect(db_path)
    try:
        columns = [
            row[1] for row in connection.execute(
                "PRAGMA table_info(settlement_detail)"
            )
        ]
        assert columns == [
            "id",
            "supplier_code",
            "order_no",
            "amount_text",
            "quantity_text",
        ]
        fks = connection.execute(
            "PRAGMA foreign_key_list(receipt_line)"
        ).fetchall()
        assert fks[0][2] == "settlement_detail"
        assert fks[0][6] == "CASCADE"
    finally:
        connection.close()
    assert scenario.current_version(db_path) == "0001"
