# 工单 B314 — 交接故障调查报告

清洗器(`sh` 快照,`src/sh/__init__.py`)经普通管道运行,stdout 按行交出
`A,7`、`B,4`,stderr 交出一行回执
`receipt batch=B314 lines=1,2 ck1=aa45b10c26ba ck2=b54957644383`。
两路各由一个无文件描述符的文本接收器(有 `encoding`/`write`/`flush`,无
`fileno`)处理,两路 tee 均开启(`_tee=("out","err")`;注意 `_tee=True`
只 tee stdout,因为 `TEE_STDERR = {"err", 2}`,见 `src/sh/__init__.py:80`)。
接收器只在"有待写内容的 flush 成功"后才把内容追加到各自文件;两路最后都会
再遇到一次无待写内容的 flush(收尾 flush)。

复跑方式:`../.venv/bin/python harness.py`(全部场景)或
`../.venv/bin/python harness.py S3-out-park-then-ok`(单场景)。
每个场景 × 每种观测法一份证据:`evidence/<场景>__<观测>.json`,
内含批号、故障注入计划、每次 flush 的流水、观测时刻的文件内容、退出码、
线程异常、以及 sh 内部 `_output_thread_exc_queue` 的残留。

## 机制(为什么这三种现象会同时出现)

- 接收器走 `get_file_chunk_consumer`(`src/sh/__init__.py:3086`):每个
  chunk 先 `write()` 再 `flush()`;流关闭时 `StreamReader.close()`
  (`src/sh/__init__.py:3199`)再调一次 `finish()` → 即那次无内容的收尾
  flush(`src/sh/__init__.py:3111`)。
- 迭代结束哨兵 `None` 在 `close()` 里、收尾 flush **之后**才放进管道队列
  (`src/sh/__init__.py:3207`)。收尾 flush 阻塞或抛错 → 哨兵永远不来 →
  `_iter_noblock` 的 `next()` 永远只返回 `errno.EWOULDBLOCK`
  (`src/sh/__init__.py:872`)——"一直只读到暂时无数据"。
- `output_thread` 先 `stdout.close()` 再 `stderr.close()`,最后才
  `output_complete()`(`src/sh/__init__.py:2775-2781`);后者设置的
  `aio_output_complete` 正是 `await cmd` 等待的事件
  (`src/sh/__init__.py:888`)。out 路收尾抛错 → err 路的 close 被跳过、
  `output_complete` 不执行 → err 路哨兵也没有、`await` 永远等——"另一个
  协程还在等"。
- 输出线程的异常被 `_start_daemon_thread` 收进
  `OProc._output_thread_exc_queue`(`src/sh/__init__.py:1706`),**没有任何
  代码路径消费它**。`wait()`、迭代、`await` 都不会重新抛出它;它只经
  `threading.excepthook` 以线程名 `STDOUT/ERR thread for pid N` 露面——
  一条未标流向的 `OSError`。
- 普通 `wait()` 只是 join 输出线程(`src/sh/__init__.py:2646`):flush
  停住时它跟着堵;flush 抛错、线程已死时它照常返回退出码 0。
- `wait(timeout=...)` 只轮询子进程是否退出(`src/sh/__init__.py:746-780`),
  成功即置 `_waited_until_completion`,之后的普通 `wait()` 直接短路
  (`src/sh/__init__.py:744`)——它从不等交接。

## 场景矩阵(证据见 evidence/*.json)

| 场景 | 文件 | 退出码 | iter(out) | iter(err) | await | wait() | wait(timeout)后再wait() | 线程异常 |
|---|---|---|---|---|---|---|---|---|
| S0 无故障 | 全 | 0 | 结束 | 结束 | 返回 | 返回0 | 0;no-op | 无 |
| S1 out 收尾flush抛错 | 全 | 0 | 永远EWOULDBLOCK | 永远EWOULDBLOCK | 挂起 | 返回0 | 0;no-op | OSError |
| S2 err 收尾flush抛错 | 全 | 0 | **正常结束** | 永远EWOULDBLOCK | 挂起 | 返回0 | 0;no-op | OSError |
| S3 out 收尾flush停住→放行 | 全 | 0 | 阻塞后结束 | 结束 | 阻塞后返回 | 阻塞后返回0 | 停住期间已得0;no-op | 无 |
| S4 out 收尾flush停住→抛错 | 全 | 0 | 永远EWOULDBLOCK | 永远EWOULDBLOCK | 挂起 | 阻塞后返回0 | 停住期间已得0;no-op | OSError |
| S5 out 末行flush抛错 | 明细缺`B,4` | 0 | 永远EWOULDBLOCK | 永远EWOULDBLOCK | 挂起 | 返回0 | 0;no-op | OSError |
| S6 err 回执flush抛错 | 全空 | 0 | 永远EWOULDBLOCK | 永远EWOULDBLOCK | 挂起 | 返回0 | 0;no-op | OSError |

要点:

- S2 里 iter(out) **正常结束**(哨兵在 err 的 close 之前已送出),但 err 路
  收尾失败——"有人已经读到迭代结束"与"交接故障"并存。
- S1 里 err 路本身无故障,却因 out 路先抛错、err 的 close 被跳过而同样永远
  等不到哨兵。
- S3/S4 在放行之前**任何观测都区分不开**:同样的 EWOULDBLOCK、同样的
  wait() 阻塞、同样的 await 挂起、wait(timeout) 同样已拿到退出码 0
  (证据 `S3-...__wait-timeout-first.json` 显示 wait(timeout) 返回时最后一
  条 flush 流水仍是 `parked`)。
- S5 里 `A,7` 在失败前已落盘且仍可读;`B,4` 随那次 flush 一起丢失。
- S6 里回执那次 flush 失败直接杀死整个输出线程,其后所有数据无人再读,
  两个文件全空,退出码仍是 0。

## 值班员判定规则

**哪些明细可入仓**:只看落盘且对得上本批回执的行。
S0–S4:`A,7`、`B,4` 两行均可入仓;S5:仅 `A,7`;S6:一行都不行(回执本身
没落盘,无从核对)。

**哪些批次还有交接故障(不可关账)**:S1、S2、S4(放行后)、S5、S6。
文件齐全不等于交接完成——S1/S2/S4 文件全、退出码 0,但收尾确认(哨兵 /
`aio_output_complete`)永远缺失。S3 放行成功后两路收尾齐全,可关账。

**什么时候应继续等**:只要还有一路的收尾 flush 未出结果(停住未放行),
EWOULDBLOCK / wait() 阻塞 / await 挂起都只是"尚未完成",应继续等(S3 证明
放行后一切自然收尾)。一旦该次 flush 已抛出 OSError(输出线程已死),哨兵和
`aio_output_complete` 都不会再来,迭代和 await 不可能自然结束,值班员应停止
等待,按交接故障处理;此时唯一还会"正常返回"的是 wait()/wait(timeout)——
它返回退出码 0,恰恰掩盖故障。

**为什么换等待顺序只改变先看到的现象**:先用 `wait(timeout=...)` 会在交接
未完成时就"成功",并把随后的普通 wait() 变成 no-op,最先看到的是"一切正常";
在 S2 里先观察 iter(out),最先看到的是干净的 StopIteration。但缺失的交接证据
(哨兵、close 完成、收尾 flush 的结果)不会因为观测顺序而补齐——顺序只决定
谁先撞上缺失,不产生证据。

## 只剩退出码 + 文件内容 + 一条未标流向的 OSError 时

还能判断:

- 退出码 0 → 子进程一侧已完成;故障在父进程一侧的交接(flush/close),不在
  清洗器本身。
- 文件内容 + 回执 → 哪些行已落盘且可核对 → 这些行可入仓;文件是否齐全还能
  区分"数据没落盘"(S5/S6,文件缺内容)与"文件齐全但交接失败"(S1/S2/S4)。
- 有一条 OSError → 至少一路收尾失败 → 批次不可关账。

不能判断:

- 故障在哪一路;另一路的收尾是成功了(S2 的 out)还是根本没被尝试(S1 的
  err,close 被跳过)。
- 因此也无法知道该等哪一路、还是两路都不必再等。

**现场必须再留的一项证据**:错误的流向标注——即按流(out/err)分别记录
收尾结果(哪次 flush 抛了错,或等价地:每路 close 是否完成 / 哨兵是否送出)。
有了"退出码 + 文件内容 + 带流向的收尾结果",上表七种状态全部可区分;没有
流向标注,S1 与 S2(以及"另一路是否确认过")永远分不开。
