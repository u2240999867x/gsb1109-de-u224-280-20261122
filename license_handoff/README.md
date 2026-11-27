# license_handoff

影像档案从 STAC 0.9.0 过渡到 1.1.0 期间的本地许可迁移与三方交接工具。

每次成功交接都产出**一对目录**：

- `legacy/`：给旧交接流程，保持 STAC 0.9.0 + `commons` 写法；
- `current/`：给入仓程序，用 `pystac.read_file` 打开的 STAC 1.1.0 目录。

两个目录都使用 `collections/delta.json` 与 `items/<ID>.json` 的平铺布局。

## 同步范围

只同步许可相关的两类信息，其余字段原样保留：

- Item 的 `properties.license`；
- 旧 Collection 在 `properties.license` 提供的 `commons` 共享默认值。

“显式填写”和“省略”是两种不同业务状态，分别用 `LicenseState.written(...)` 与
`LicenseState.omitted()` 表示，不会被归一化合并掉。旧字面值
（`various` / `proprietary`）与“是否改成明细自己填写”都作为业务信息保留；
归一化到 1.1.0 的规则与 `pystac.serialization.migrate._migrate_license` 一致：
`various`、`proprietary` → `other`，SPDX 标识原样透传。

## 来源追溯

新包每个 Item 都带 `license_handoff:provenance` 属性，记录许可来源：

```json
{"source": "item", "literal": "various", "resolved": "other"}
```

- `source=item`：来自 Item 自己写的明细；
- `source=collection`：Item 省略许可，来自 Collection 共享默认值。

该属性随文件落盘，因此换进程后仍可追溯“读到的 `other` 来自哪里”。

## 链接约定

- 新包全部使用相对链接、不写 `self` 链接，整个目录搬走后包内链接仍解析到
  随包的同一份 Collection。
- 旧包保留每个 Item 原始 collection 链接的字面值：A 的包外绝对地址照旧，
  其余 Item 的 `../collections/delta.json` 相对地址照旧。

## 工作流

```python
from license_handoff import (
    HandoffPaths,
    LicenseConflictError,
    Side,
    initial_migration,
    merge_handoff,
)

# 首轮：从供应商 0.9.0 目录迁移出第一对交接目录
base = HandoffPaths("handoff/legacy", "handoff/current")
initial_migration("vendor_090", base)

# 后续轮：以“上一对成功交付”为基准，合并旧方与新方的独立改动
try:
    merge_handoff(base, sides, output)
except LicenseConflictError as error:
    # 未解决冲突时不写任何目录，保留上一对成功交付
    choices = {"A": Side.CURRENT, "B": Side.LEGACY}
    merge_handoff(base, sides, output, choices)
```

冲突按 Item 判定：同一 Item 在旧方与新方相对基准都发生了许可相关改动即为冲突，
冲突说明会区分 Item 自身改动与 Collection 默认值变更（影响省略许可的 Item）。
只有一方改动的 Item 自动合入，无需人工选择。

## 测试

从仓库根目录运行：

```bash
.venv/bin/python -m pytest license_handoff/tests/
```

测试覆盖三轮交接故事、目录搬迁可移植性、显式/省略区别保持、非许可字段保留，
以及在独立子进程中仅凭落盘目录延续来源链。
