"""离线 SQL 执行器。

把 ``alembic upgrade --sql`` 生成的脚本在外层事务里逐句执行：
用 SAVEPOINT 圈住本次迁移，遇到 SQL 错误就停止并撤销本次迁移的
改动（回滚到保存点），但外层事务先前记下的内容（例如接收批号）
不受影响，仍可由调用方提交。
"""

from __future__ import annotations

import sqlite3
from typing import Iterable

SAVEPOINT_NAME = "alembic_offline_migration"


def split_statements(sql_text: str) -> list[str]:
    """把脚本拆成逐条语句，跳过注释和空行。"""
    statements: list[str] = []
    buffer = ""
    for line in sql_text.splitlines(keepends=True):
        if not buffer.strip() and line.lstrip().startswith("--"):
            continue
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                statements.append(statement)
            buffer = ""
    if buffer.strip():
        statements.append(buffer.strip())
    return statements


def apply_offline_migration(
    connection: sqlite3.Connection,
    sql_text: str,
    savepoint: str = SAVEPOINT_NAME,
) -> None:
    """在外层事务内逐句执行迁移脚本。

    调用方负责外层事务（BEGIN/COMMIT）以及 ``PRAGMA foreign_keys=ON``。
    任何一句报 SQL 错误都会停止执行，并把本次迁移的改动回滚到
    保存点；错误继续抛给调用方，冲突明细可另行查询。
    """
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        for statement in split_statements(sql_text):
            connection.execute(statement)
    except Exception:
        connection.execute(f"ROLLBACK TO {savepoint}")
        connection.execute(f"RELEASE {savepoint}")
        raise
    connection.execute(f"RELEASE {savepoint}")
