# Warehouse API 滚动切换

这份方案在不修改旧 v1 进程代码和 `add_job()` 调用的前提下，让旧 worker 只领取 v1，新 worker 领取已确认的 v1/v2，并把无法判定协议的 `threadpool` 历史工作隔离人工核对。

**运行验收**
- `python3 warehouse_ingest.py`：运行完整确定性场景，并生成 `warehouse_rollout_report.json`。
- `python3 -m pytest tests/test_warehouse_rollout.py -q`：锁定顺序、金额、租约、隔离和回执。
- `psql "$WAREHOUSE_ADMIN_DSN" -f sql/warehouse_rolling_upgrade.sql`：在 PostgreSQL/APScheduler 表上启用生产路由策略。

**协议与金额**
- v1：`1250` 是欧分，回执金额为 `12.50`，展示 `12.50 欧元`。
- v2：`1250` 是欧元，回执金额为 `1250.00`，展示 `1250.00 欧元`。
- 回执保留原 job ID、批次号、实际 `protocol_used`、输入金额和格式化金额，默认保留 30 天。

**滚动升级机制**
- 新提交端把解析后的协议写入 APScheduler job metadata：`warehouse_protocol=v1|v2`。省略版本时读取当前默认值；回退默认值与提交通过 `RolloutConfig`/`EnqueueGate` 保证先提交 v1，再放行等待中的入队。
- 旧进程继续使用原数据库角色和原 `Scheduler.add_job(...)`。PostgreSQL 触发器按旧角色、`legacy_ingest` 标签/任务名和显式协议归类；RLS 对旧角色隐藏 v2 与 `manual_review` 行，因此 APScheduler 原生 `acquire_jobs()` 的 `created_at` 排序会跳过最早的 v2，继续领取后面的 v1。
- `threadpool` 是唯一线索且没有显式协议、旧角色或 `legacy_ingest` 证据时，归类为 `manual_review`。原 APScheduler `jobs` 行、ID 和序列化参数保持不动，同时在 `warehouse_manual_review` 中记录 `awaiting_manual_verification`；两个 worker 角色的 RLS 都看不到该行。
- `warehouse_ingest` 任务配置 `max_running_jobs=1`，沿用 APScheduler 的 PostgreSQL `tasks.running_jobs` 计数和行锁；场景中另有写入锁检测，最大并发写入断言为 1。甲手中的工作未完成时，乙无法领取同一任务的 v2。
- 租约由 APScheduler 的 `acquired_by`、`acquired_until` 和租约续期循环维护；确定性场景用未过期租约复现“甲卡在写入前”。

**PostgreSQL 对象**
- `jobs.protocol`、`jobs.routing_status`、`jobs.routing_labels`：协议路由状态。
- `trg_warehouse_classify_job`：对新旧插入透明归类。
- `warehouse_old_job_isolation`：旧角色只见 ready 的 v1。
- `warehouse_new_job_isolation`：新角色可见 ready 的 v1/v2，但不见人工核对行。
- `warehouse_receipts`：按原 job ID 查询保留期内回执。
- `warehouse_manual_review`：保留人工核对状态和原始参数引用。

生产部署时把 SQL 中的 `warehouse_old`、`warehouse_new` 和密码替换为实际角色；旧进程 DSN 使用旧角色，新进程 DSN 使用新角色。`warehouse_apscheduler.py` 提供同步 APScheduler 工厂和新提交端 metadata 助手。
