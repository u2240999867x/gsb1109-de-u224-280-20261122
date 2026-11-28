# LALR-305 分号竞争归约与 `left.1` 优先级调整排查报告

## 0. 复现方式（部署入口与词法约定）

- 入口（与工单一致的真实装配）：
  `Lark.open(entry, parser="lalr", lexer="contextual", start=["batch","checkpoint"], import_paths=[<导入目录>])`
- 普通 contextual lexer；含可空规则 `pad`、可空 `["?"]`；无自定义词法回调、无 postlex。
- 主文法 `%ignore /\s+/`，空格在每个 contextual 状态里以 `__IGNORE_0` 出现但被过滤，绝不交给解析器。
- 环境：仓库自带 Lark 1.3.1（`.venv/bin/python`）。本机无法联网安装 `interegular`，
  因此词法正则碰撞的 interegular 严格子项跑不了；但 LALR 严格门禁（R/R、S/R 裁决）在
  `LALR_Analyzer(strict=True)` 内部，与 interegular 无关，本报告的严格结论均直接来自它。
- 重放命令（在 workspace 根目录）：
  - `.venv/bin/python probe/evidence.py`  ：严格门禁 + 部署入口失败 + 分号/词法状态 + 最短清单矩阵
  - `.venv/bin/python probe/replay.py left1` / `right1` ：逐 token 全量重放（含坐标、尾部）
  - `.venv/bin/python probe/boundary.py`  ：单字母结构下限探针
  - `.venv/bin/python probe/lex_collision_check.py` ：离线版同优先级正则碰撞枚举（interegular 替身）

## 1. 规则声明位置（`nl -ba` 实测行号）

导入目录（竞争的两条规则体完全相同：`NAME pad`，`pad` 可空）：
- `probe/headers.lark:7` `left: NAME pad`
- `probe/headers.lark:9` `right: NAME pad`
- `probe/headers.lark:11` `NAME: /[a-z]+/`
- `probe/headers.lark:13` `pad:`（空规则）

主文法：
- `probe/entry.lark:10-11` 导入 `left` / `right`
- `probe/entry.lark:13-14` `batch: ... left ";" lt | ... right ";" rt`
- `probe/entry.lark:16-17` `checkpoint: ... left ";" rt | ... right ";" lt`
- `probe/entry.lark:19` `lt: LOW | HIGH "?"`
- `probe/entry.lark:20` `rt: LOW ["?"]`
- `probe/entry.lark:22` `HIGH.2: /[a-z]+/`（终结符优先级 2）
- `probe/entry.lark:23` `LOW: /[a-z]+/`（终结符优先级 0）
- `probe/entry.lark:25` `%ignore /\s+/`

优先级改动只在导入目录落地（主文法不动）：
- 提议：`probe/headers_left1.lark:8` `left.1: NAME pad`（规则优先级 1），`:10` `right` 保持默认（None→0）。
- 对照（仅用于证明另一种竞争解释，不是工单改动）：`probe/headers_right1.lark:11` `right.1`。

## 2. 分号前的竞争归约（根因）

严格门禁与真实 `Lark.open` 都在同一点失败（`evidence.py` STEP 1/2）：

```
Reduce/Reduce collision in Terminal('SEMICOLON') between:
  - <right : headers__NAME headers__pad>
  - <left  : headers__NAME headers__pad>
collision state: { <left : NAME pad *>, <right : NAME pad *> }
```

- 触发竞争的实读符号是 `SEMICOLON`（原文里的 `;`）。读完头部单词（`NAME`，如 `asset`）后，
  可空的 `pad` 先归约，进入同时含 `left: NAME pad *` 与 `right: NAME pad *` 的状态；
  前瞻 `;` 时两条规则都可归约，规则体一模一样，默认优先级都是 0，无法裁决。
- 代码依据：R/R 仅按规则优先级裁决，并列即抛 `GrammarError`——
  `lark/parsers/lalr_analysis.py:273-283`（排序取唯一最优，否则进 `reduce_reduce`）、
  `:301-308`（R/R 无论是否 strict 都抛错）。S/R 才是 strict 专属（`:287-290`），本文法没有 S/R。
- 工程事实：调整前这份文法在 LALR 下**根本成不了表**，部署入口对任何清单都读不进去；
  所谓“两种归档解释”是该 R/R 里对撞的两条规则，本报告用两个强制裁决变体在**同一份原文**上
  各自读到 EOF 来分别取证（分析性证人），而不是声称基线文法本身能产出两棵树。

## 3. 为什么分号后同一个 `ok` 读法不同（contextual lexer）

contextual lexer 每个解析器状态只装载该状态 accept 的终结符（`lark/parser_frontends.py:276`、
`lark/lexer.py:734-745`），同状态内按 `(-priority, -max_width, -len, name)` 排序
（`lark/lexer.py:630`），按解析器位置切换词法器（`lark/lexer.py:751-755`）。
分号前谁赢得 R/R，决定分号后进入哪个尾部非终结符的状态，从而决定 `ok` 被切成哪个 token：

- 进入 `lt` 状态：accept 同时含 `HIGH`(优先级2) 与 `LOW`(优先级0)，正则同为 `[a-z]+`，
  于是 `ok` 被判给 `HIGH`；`lt: HIGH "?"` 强制要求后续 `?`。
- 进入 `rt` 状态：accept 只有 `LOW`，`ok` 切成 `LOW`；`rt: LOW ["?"]` 的 `?` 可空，EOF 即可收尾。

`HIGH` 与 `LOW` 正则完全相同，差异只来自优先级与当前 contextual 状态——这正是“相同 `ok`、不同读法”。

## 4. 实测矩阵（原文坐标与交给解析器的尾部 token）

记号：解释 = 头部竞争规则；尾部非终结符(实读终结符)。下表均为同一原文在指定入口读到结束。

### 提议 `left.1`（`entry_left1.lark`，分号后 left 必胜）

| 入口 | 原文（字符数） | 分号后实读符号 / span / 尾部 | EOF | 结果与解释 |
|---|---|---|---|---|
| batch | `sync delta asset;ok` (19) | `HIGH 'ok'` span(17,19) tail `'ok'` | 读到 EOF，`$END` 不被接受（只 accept `QMARK`） | 拒绝 |
| batch | `sync delta asset;ok?` (20) | `HIGH 'ok'` span(17,19) tail `'ok?'` | 接受 | `left;lt(HIGH,QMARK)` |
| checkpoint | `resume delta asset;ok` (21) | `LOW 'ok'` span(19,21) tail `'ok'` | 接受 | `left;rt(LOW)` |
| checkpoint | `resume delta asset;ok?` (22) | `LOW 'ok'` span(19,21) tail `'ok?'` | 接受 | `left;rt(LOW,QMARK)` |

### 对照 `right.1`（`entry_right1.lark`，证明被抹掉的另一种解释）

| 入口 | 原文（字符数） | 分号后实读符号 / span / 尾部 | EOF | 结果与解释 |
|---|---|---|---|---|
| batch | `sync delta asset;ok` (19) | `LOW 'ok'` span(17,19) tail `'ok'` | 接受 | `right;rt(LOW)` |
| batch | `sync delta asset;ok?` (20) | `LOW 'ok'` span(17,19) tail `'ok?'` | 接受 | `right;rt(LOW,QMARK)` |
| checkpoint | `resume delta asset;ok` (21) | `HIGH 'ok'` span(19,21) tail `'ok'` | 读到 EOF，`$END` 不被接受（只 accept `QMARK`） | 拒绝 |
| checkpoint | `resume delta asset;ok?` (22) | `HIGH 'ok'` span(19,21) tail `'ok?'` | 接受 | `right;lt(HIGH,QMARK)` |

同一份带问号清单的两种解释都能从同一入口读到 EOF：
- `sync delta asset;ok?`：left 侧 `left;lt(HIGH,QMARK)` 与 right 侧 `right;rt(LOW,QMARK)`——
  连 `ok` 的 token 类型都不同（HIGH vs LOW）。
- `resume delta asset;ok?`：`left;rt(LOW,QMARK)` 与 `right;lt(HIGH,QMARK)`。

## 5. 每个受影响入口的最短清单（按原文字符数）

### 入口 `batch`
- 最短被拒清单：`sync delta asset;ok`，**19 字符**。
  提议下 left 胜 → 分号后 `lt` → `ok` 被切成 `HIGH` → 缺 `?`，EOF 拒绝（expected `QMARK`）。
  结构下限探针 `sync delta a;b`（14 字符）同样被拒，说明这是结构性最短形态，不是巧合。
- 最短“仍接受但丢失一种归档解释”：`sync delta asset;ok?`，**20 字符**。
  保留 `left;lt(HIGH,QMARK)`；丢失的 `right;rt(LOW,QMARK)` 由对照变体对同一原文读到 EOF 证明。

### 入口 `checkpoint`
- 最短被拒清单：**在本有限范围内未发现（范围已查完，非预算用尽）**。
  提议下 checkpoint 的 left 分支后接 `rt`（只含 `LOW`，`?` 可空），无问号/带问号都接受；
  单字母下限 `resume delta a;b`（16）也接受。checkpoint 的被拒形态只存在于 right 侧（对照变体），
  而 right 侧已被 `left.1` 永久压制，故本入口在提议下没有可触发的拒绝。
- 最短“仍接受但丢失一种归档解释”：`resume delta asset;ok?`，**22 字符**。
  保留 `left;rt(LOW,QMARK)`；丢失 `right;lt(HIGH,QMARK)`（对照变体同原文读到 EOF 证明）。
  注：无问号的 `resume delta asset;ok`（21）在对照变体下本就被拒，故它从来只有一种可行解释，
  不能算作“丢失一种解释”的证据。

## 6. 结论与边界

- 只把导入规则 `left` 提到规则优先级 1：解开 SEMICOLON 上的 R/R，使文法能过严格 LALR 门禁并成表；
  代价是 left 在两个入口都必胜，right 侧解释被整体抹掉，并把 batch 的无问号清单逼成 HIGH+缺问号拒绝。
- 受影响入口分别成立，互不替代：batch 同时存在“被拒”和“接受但丢解释”两类最短证据；
  checkpoint 仅有“接受但丢解释”，被拒类在本有限范围查无证据（已查完）。
- 未跑项：interegular 的词法正则碰撞严格校验（环境离线，装不上）。已用 `lex_collision_check.py`
  按 interegular 的同优先级分组规则静态枚举全部 contextual 状态：同优先级正则碰撞数为 **0**——
  HIGH 在优先级 2 单独成组，LOW 与 NAME 虽同为优先级 0 但从不在同一 BasicLexer 共存；
  实际裁决另由逐 token 词法顺序直接证实。正式工单替换整份文法后建议在联网环境再跑一次 interegular 复核。
