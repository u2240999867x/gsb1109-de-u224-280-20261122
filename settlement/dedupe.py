"""声明式的历史结算明细去重计划。

迁移脚本通过 :class:`DedupePlan` 声明三件事：

* ``group_columns`` —— 怎样认定同一笔（供应商 + 规范单号）；
* ``value_columns`` —— 怎样从旧表取值（金额文本精确转整数分、
  整数字符串转整数），同组内这些值不一致的原始行不能合并；
* ``writeback`` —— 合并后写回旧表的规范化值。

计划本身只生成 SQL，因此线上迁移和 ``--sql`` 离线脚本走同一套规则。
SQL 表达式里的 ``{t}`` 会被替换成目标表名或别名。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable


def decimal_text_to_cents(column: str) -> str:
    """把十进制金额文本精确转成整数分的 SQL 表达式。

    只做字符串切分和整数运算，不经过浮点，因此 ``01.25`` 与 ``1.25``
    都得到 125。支持没有小数点的整数文本和一到两位小数。
    """
    col = f"trim({column})"
    return (
        f"CAST(CASE WHEN instr({col}, '.') = 0 THEN {col} "
        f"ELSE substr({col}, 1, instr({col}, '.') - 1) END AS INTEGER) * 100 "
        f"+ CAST(CASE WHEN instr({col}, '.') = 0 THEN '00' "
        f"ELSE substr(substr({col}, instr({col}, '.') + 1) || '00', 1, 2) "
        f"END AS INTEGER)"
    )


@dataclass(frozen=True)
class DedupePlan:
    """一张表的去重计划。"""

    table: str
    id_column: str
    #: 逻辑名 -> 认定同一笔的 SQL 表达式（含 ``{t}`` 占位符）
    group_columns: dict[str, str]
    #: 逻辑名 -> 从旧表取值的 SQL 表达式；同组不一致则拒绝合并
    value_columns: dict[str, str]
    #: 列名 -> 合并后写回的规范化表达式
    writeback: dict[str, str]
    #: 通过外键引用本表、需要改挂到保留 ID 的子表: (表名, 外键列)
    children: tuple[tuple[str, str], ...] = ()

    @property
    def guard_table(self) -> str:
        return f"_dedupe_guard_{self.table}"

    def _render(self, expr: str, alias: str) -> str:
        return expr.format(t=alias)

    def _group_exprs(self, alias: str) -> list[str]:
        return [self._render(expr, alias) for expr in self.group_columns.values()]

    def _value_exprs(self, alias: str) -> list[str]:
        return [self._render(expr, alias) for expr in self.value_columns.values()]

    def guard_statements(self) -> Iterable[str]:
        """冲突守卫：同组出现不同的取值组合时，INSERT 触发主键冲突，
        整个迁移以 SQL 错误中止。"""
        group_names = ", ".join(self.group_columns)
        value_names = ", ".join(self.value_columns)
        group_exprs = ", ".join(self._group_exprs(self.table))
        value_exprs = ", ".join(self._value_exprs(self.table))
        yield f"DROP TABLE IF EXISTS temp.{self.guard_table}"
        yield (
            f"CREATE TEMP TABLE {self.guard_table} ("
            f"{group_names}, {value_names}, "
            f"PRIMARY KEY ({group_names}))"
        )
        yield (
            f"INSERT INTO {self.guard_table} ({group_names}, {value_names}) "
            f"SELECT DISTINCT {group_exprs}, {value_exprs} FROM {self.table}"
        )

    def drop_guard_statement(self) -> str:
        return f"DROP TABLE IF EXISTS temp.{self.guard_table}"

    def conflict_query(self) -> str:
        """另查冲突明细：返回每个不能合并的组及其原始明细 ID。"""
        group_select = ", ".join(
            f"{self._render(expr, self.table)} AS {name}"
            for name, expr in self.group_columns.items()
        )
        fingerprint = " || char(31) || ".join(
            f"({expr})" for expr in self._value_exprs(self.table)
        )
        return (
            f"SELECT {group_select}, "
            f"group_concat({self.id_column}) AS detail_ids "
            f"FROM {self.table} "
            f"GROUP BY {', '.join(self._group_exprs(self.table))} "
            f"HAVING count(DISTINCT {fingerprint}) > 1 "
            f"ORDER BY {', '.join(self._group_exprs(self.table))}"
        )

    def find_conflicts(
        self, connection: sqlite3.Connection
    ) -> list[dict[str, object]]:
        """在（可能已回滚的）库上另查冲突组，报出组里的原明细 ID。"""
        rows = connection.execute(self.conflict_query()).fetchall()
        names = list(self.group_columns)
        return [
            {
                **dict(zip(names, row[: len(names)])),
                "detail_ids": sorted(int(v) for v in row[len(names)].split(",")),
            }
            for row in rows
        ]

    def repoint_update(self, child_table: str, fk_column: str) -> str:
        """把子表行改挂到组内保留的最小明细 ID，子表主键和内容照旧。"""
        join_conditions = " AND ".join(
            f"{self._render(expr, 'd2')} = {self._render(expr, 'd1')}"
            for expr in self.group_columns.values()
        )
        return (
            f"UPDATE {child_table} SET {fk_column} = ("
            f"SELECT min(d2.{self.id_column}) "
            f"FROM {self.table} d1 JOIN {self.table} d2 ON {join_conditions} "
            f"WHERE d1.{self.id_column} = {child_table}.{fk_column})"
        )

    def repoint_statements(self) -> Iterable[str]:
        for child_table, fk_column in self.children:
            yield self.repoint_update(child_table, fk_column)

    def delete_duplicates_statement(self) -> str:
        """删除组内未保留的明细行（收货行已改挂，不会触发级联）。"""
        return (
            f"DELETE FROM {self.table} WHERE {self.id_column} NOT IN ("
            f"SELECT min({self.id_column}) FROM {self.table} "
            f"GROUP BY {', '.join(self._group_exprs(self.table))})"
        )

    def writeback_statements(self) -> Iterable[str]:
        assignments = ", ".join(
            f"{name} = {self._render(expr, self.table)}"
            for name, expr in self.writeback.items()
        )
        if assignments:
            yield f"UPDATE {self.table} SET {assignments}"

    def data_statements(self) -> Iterable[str]:
        """去重数据语句的完整顺序：守卫 -> 改挂 -> 删除 -> 规范化。"""
        yield from self.guard_statements()
        yield from self.repoint_statements()
        yield self.delete_duplicates_statement()
        yield from self.writeback_statements()
