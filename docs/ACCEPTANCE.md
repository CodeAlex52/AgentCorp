# AgentCorp — 独立验收契约（Gauntlet）

执行者：**RedTeam agent**（不是本项目的作者）。仓库：`/Users/suzhe/Desktop/vibecoding/AgentCorpGauntlet`。
被测对象：`/Users/suzhe/Desktop/vibecoding/AgentCorp`（只读，**禁止修改**）。

## 0. 立场

你的目标不是帮助作者证明它很好，**你的目标是拒绝这个项目**。把它当作准备提交给顶级开源项目的 PR：
必须能证明的缺陷才可判为 confirmed；不能证明的写 `hypothesis` 并给出验证方案。
**禁止虚构问题凑数**。禁止直接改 AgentCorp 代码；你只负责攻击与取证。

## 1. 运行方式

```bash
cd /Users/suzhe/Desktop/vibecoding/AgentCorpGauntlet
AGENTCORP_ROOT=/Users/suzhe/Desktop/vibecoding/AgentCorp uv run pytest -q
```
`conftest.py` 负责把 `<AGENTCORP_ROOT>/src` 注入 `sys.path`（默认 `../AgentCorp`）。全部离线、无 API key。

## 2. 必测清单（对应 SPEC_v0 能力 C1–C17；每条至少落一个测试或一个已编号 finding）

| ID | 攻击点 | 通过判据（摘要） |
|---|---|---|
| G-A | 状态机白名单 | 12 种非法转移全部抛 `StateError`，合法转移全部成功 |
| G-B | DAG 校验 | 自环 / 多节点环 / 悬空依赖 / 分解引入的环 全部被拒 |
| G-C | exactly-once 派发 | 64 并发抢同一 READY 任务 → 恰 1 个 claim 成功 |
| G-D | 幂等 | 同一 task 重复派发被拒；重复 finish 不产生双份事件 |
| G-E | 崩溃恢复 | 子进程 `SIGKILL` 后 `resume`：DONE 零重跑、stale RUNNING 重派、seq 连续 |
| G-F | 预算硬停 | 超 token/cost/tasks/wall 四类限额 → 立即停派发、状态正确、记账误差 ≤1 |
| G-G | 重试风暴 | provider 持续失败 → attempts ≤ 上限、熔断打开、总调用次数有界 |
| G-H | 死锁 | 无 running/ready 但 pending>0 → 检测事件 + 终止（不许挂死，超时即 FAIL） |
| G-I | 失控分解 | 无限 SPLIT → depth/total/budget 三重拦截，5s 内终止 |
| G-J | 事件序 | 单 run 内 seq 严格递增、无空洞；每次状态变更都有事件 |
| G-K | 取消 | cancel 后零新派发；在途 grace 内收敛；持久化正确 |
| G-L | 评审环 | 禁止自审；must_fix → 回炉；rework 上限；rejection 风暴有界 |
| G-M | supervisor | 不误报（正常长任务不被干预）；不放过（真 stuck 必被发现） |
| G-N | 属性测试（建议 hypothesis） | 随机 DAG × 随机故障：无 DONE 重跑、预算不超、无孤儿状态 |
| G-O | 报告 schema | `benchmarks/*.json` 与 `agentcorp report` 输出通过 jsonschema 校验 |
| G-P | 路径安全 | `FileWrite` 越界路径（`../`、绝对路径、符号链接）被拒 |
| G-Q | 一致性 | 事件重放重建的 task 状态 == store 当前状态 |

## 3. Finding 格式（每个问题一个文件）

`findings/FIND-###.md`：
```
# FIND-###: 标题
severity: P0|P1|P2|P3
status: confirmed | hypothesis
contract: 违反的 SPEC 条款（如 C4 / §5.2）
repro: 复现命令或测试名（必须是仓库内可运行的最小复现）
expected: 契约要求的行为
actual: 实际观察（贴真实输出/断言失败信息）
evidence: 关键代码位置 file:line 或 traceback
suggested_fix: 一句话（可留空）
```

## 4. SCORECARD.md

表格：G-A..G-Q × {pass, fail(count), blocked(原因)} + confirmed P0/P1 数量 + 未验证假设清单。
**不达标就是不达标**：全部 pass 但测试只有 mock 断言 → 判「证据不足」。

## 5. 时间盒与批次

- Batch 1（现在即可开工，不依赖引擎）：`models` `graph` `store` `budget` `reliability` `chaos` `parsing` `providers` 的独立攻击（G-A/B/C/D/F/G/O/P 的部分）+ 建立 conftest/harness/finding 模板。
- Batch 2（引擎落地后）：G-E/H/I/J/K/L/M/N/Q 与端到端攻击。
- 每批结束更新 `SCORECARD.md` 与 `STATUS.md`，并 git commit。

## 6. 禁止事项

1. 修改 AgentCorp 任何文件（只读）。
2. 为了让测试变绿而弱化断言（如 `pytest.skip` 掩盖失败、`try/except pass`、放宽超时）。
3. 伪造或推测性 finding 标为 confirmed。
4. 只做 happy-path 断言就宣称某条通过。
