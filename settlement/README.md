# 结算明细去重迁移

供应商重传结算明细时，同一笔会用不同大小写的单号，收货行却已经引用了
各次入库拿到的明细 ID。本项目用 Alembic 的 SQLite batch 迁移把历史数据
清成**每个供应商、每个规范单号只有一条**，线上迁移和离线 SQL
（`alembic upgrade --sql`）走同一套规则。

## 布局

- `migrations/versions/0001_baseline.py` —— 旧表：`settlement_detail`
  （单号/金额/数量都是供应商上送的原始文本）、`receipt_line`
  （带 `ON DELETE CASCADE` 的外键引用明细 ID）、`receive_batch`（接收批号）。
- `migrations/versions/0002_dedupe_settlement_detail.py` —— 去重迁移。
  文件顶部用 `DedupePlan` 声明：
  - `group_columns`：怎样认定同一笔（供应商 + `upper(trim(单号))`）；
  - `value_columns`：怎样从旧表取值（金额按十进制精确转成整数分、
    数量从整数字符串转成整数）；同组这些值不一致的原始行**不能合并**；
  - `writeback`：合并后写回的规范化值；
  - `children`：需要改挂到保留 ID 的子表。
- `settlement/dedupe.py` —— 把声明编译成 SQL（守卫、改挂、删除、规范化、
  冲突另查），线上与离线共用。
- `settlement/offline_runner.py` —— 离线执行器：在外层事务里用
  `SAVEPOINT` 圈住本次迁移并逐句执行，遇到 SQL 错误就停止并撤销本次
  迁移的改动；外层事务先前记下的接收批号仍可由调用方提交。
- `settlement/scenario.py` —— 建库、播种、线上迁移、离线 SQL 生成等辅助。
- `settlement/tests/test_scenario.py` —— 场景验证。

## 迁移规则（0002）

1. **冲突守卫**：先把每组（供应商、规范单号）出现的每个
   （金额分、数量）组合写进守卫临时表，主键是（供应商、规范单号）。
   同组金额或数量不一致时主键冲突，整次迁移以 SQL 错误中止——
   换表和 `alembic_version` 版本登记都不会发生，两张旧表保持可用。
   冲突明细可用 `PLAN.conflict_query()` / `PLAN.find_conflicts(conn)` 另查，
   会报出组里的原明细 ID。
2. **改挂**：收货行的 `detail_id` 改为组内最小明细 ID，收货行自己的
   主键和内容照旧。
3. **删除与规范化**：删掉组内未保留的明细行（收货行已改挂，不会触发
   级联），把保留行的单号写成规范形式。
4. **batch 重建**（全程 `PRAGMA foreign_keys=ON`，`copy_from` 用两张旧表
   定义，离线 `--sql` 无需反射）：
   - 先重建 `receipt_line` 并暂时去掉外键——否则 `DROP` 旧明细表时
     开启的外键动作会把收货行级联删掉；
   - 重建 `settlement_detail`：`quantity_text` 改名并 CAST 成 INTEGER
     的 `quantity`，新增数据库计算列 `amount_cents`（金额文本精确转分）
     和 `total_cents`（= `amount_cents * quantity`），并加
     `UNIQUE(supplier_code, order_no)` 保证规范单号在供应商内唯一；
   - 再重建 `receipt_line`，把带 `ON DELETE CASCADE` 的外键建回来。

## 用法

```bash
# 线上迁移（连接全程开启外键检查，见 migrations/env.py）
python3 -m alembic -c settlement/alembic.ini upgrade head

# 生成离线 SQL（batch 用 copy_from，无需连接反射）
python3 -m alembic -c settlement/alembic.ini upgrade 0001:head --sql
```

离线执行器用法：

```python
import sqlite3
from settlement.offline_runner import apply_offline_migration

conn = sqlite3.connect("settlement.db", isolation_level=None)
conn.execute("PRAGMA foreign_keys=ON")
conn.execute("BEGIN")
conn.execute("INSERT INTO receive_batch (batch_no) VALUES ('RCV-001')")
try:
    apply_offline_migration(conn, sql_text)   # 逐句执行，出错即停并回滚本次迁移
except sqlite3.Error:
    conflicts = PLAN.find_conflicts(conn)      # 冲突明细另查
conn.commit()                                  # 接收批号仍能提交
```

## 验证

```bash
python3 -m pytest settlement/tests -p no:rerunfailures
```

覆盖：数量改成 `3` 时首次迁移报 SQL 错误并另查出冲突明细 10、17，
旧表与版本仍可用；改回 `2` 重试后收货行 101/102 指向明细 10，
明细 10 的原始金额仍是 `01.25`、`total_cents` 为 250；唯一约束、
外键级联、降级还原旧表结构；离线执行器冲突时撤销迁移改动但
接收批号照常提交。
