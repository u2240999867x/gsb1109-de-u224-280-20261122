"""场景辅助：建库、播种、线上迁移、离线 SQL 生成与冲突另查。"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config

PACKAGE_DIR = Path(__file__).resolve().parent
INI_PATH = PACKAGE_DIR / "alembic.ini"
REVISION_0002 = (
    PACKAGE_DIR / "migrations" / "versions" / "0002_dedupe_settlement_detail.py"
)

BASELINE = "0001"
HEAD = "head"


def load_plan():
    """从迁移脚本里取出声明的去重计划（迁移即唯一事实来源）。"""
    spec = importlib.util.spec_from_file_location(
        "settlement_revision_0002", REVISION_0002
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.PLAN


def make_config(db_path: Path | str) -> Config:
    config = Config(str(INI_PATH))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    return config


def connect(db_path: Path | str) -> sqlite3.Connection:
    """手动事务模式（autocommit）的连接，外键检查全程开启。"""
    connection = sqlite3.connect(str(db_path), isolation_level=None)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def create_baseline(db_path: Path | str) -> None:
    command.upgrade(make_config(db_path), BASELINE)


def run_upgrade(db_path: Path | str) -> None:
    command.upgrade(make_config(db_path), HEAD)


def generate_offline_sql(db_path: Path | str, revision_range: str = f"{BASELINE}:{HEAD}") -> str:
    """生成离线 SQL（batch 用两张旧表定义 copy_from，无需连接反射）。"""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        command.upgrade(make_config(db_path), revision_range, sql=True)
    return buffer.getvalue()


def seed(db_path: Path | str) -> None:
    """播种场景数据：同一供应商同一笔的两次重传 + 另一个供应商的同名单。"""
    connection = connect(db_path)
    try:
        connection.execute("BEGIN")
        connection.executemany(
            "INSERT INTO settlement_detail"
            " (id, supplier_code, order_no, amount_text, quantity_text)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (10, "SUP1", " ab-7 ", "01.25", "2"),
                (17, "SUP1", "AB-7", "1.25", "2"),
                (20, "SUP2", "AB-7", "3.00", "1"),
            ],
        )
        connection.executemany(
            "INSERT INTO receipt_line"
            " (id, detail_id, product_code, received_qty)"
            " VALUES (?, ?, ?, ?)",
            [
                (101, 10, "P-1", 2),
                (102, 17, "P-1", 2),
                (201, 20, "P-9", 1),
            ],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def set_quantity_text(db_path: Path | str, detail_id: int, quantity_text: str) -> None:
    connection = connect(db_path)
    try:
        connection.execute(
            "UPDATE settlement_detail SET quantity_text = ? WHERE id = ?",
            (quantity_text, detail_id),
        )
        connection.commit()
    finally:
        connection.close()


def current_version(db_path: Path | str) -> str | None:
    connection = connect(db_path)
    try:
        row = connection.execute(
            "SELECT version_num FROM alembic_version"
        ).fetchone()
        return row[0] if row else None
    finally:
        connection.close()
