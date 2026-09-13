# AgentCorp v0 — 完工合同（Completion Contract）

- 状态：baseline 已于 2026-09-14 01:15 从原型快照导入，**未经任何验证**（无测试、CLI 入口缺失、无 git 历史）。
- 本文档定义「v0 完工」的判定标准。实现者必须逐条满足；验收者（RedTeam/Gauntlet）按 `ACCEPTANCE.md` 独立取证。

## 1. 定位（一句话，用于简历与面试）

AgentCorp 是一个 **local-first、event-sourced、可崩溃恢复、预算硬约束的 multi-agent 交付编排器**：
输入 `(git repo, PRD)`，输出 `reviewed + tested 的 delivery + 完整事件账本 + 可复现 benchmark 报告`。

差异化（面试故事线）：
1. **Recursive Delivery**：任务可在运行时递归分解为子 DAG，受 depth/total/budget 三重上界约束。
2. **Governance**：Supervisor 检测 stuck/空转/失败风暴并留痕干预（Finding + Intervention），且被测试证明「不误报」。
3. **可审计**：append-only 事件日志，任何状态变化可重放；崩溃后 `resume` 不重复执行已完成任务。
4. **离线可验证**：mock/chaos provider 让全部可靠性测试无需网络与 API key，可 CI。

## 2. Baseline 现状（快照 2026-09-14 01:15）

已存在（**不重写**，除非测试证明有缺陷；缺陷修复须记入 `docs/DESIGN_DECISIONS.md`）：

| 模块 | 职责 |
|---|---|
| `models.py` | Task/TaskStatus/TaskKind/Usage/BudgetLimits/WorkerOutcome/Review/Artifact/Project/SupervisorFinding/Intervention 等领域模型 |
| `graph.py` | TaskGraph：add/update/remove/rewire、ready()/blocked_by_failure()/stuck_parents()、find_cycle()/validate()/problems()/stats()、critical_path()、to_ascii()/to_mermaid() |
| `store.py` | 事件源存储（run/task/event 持久化，约 37KB，需审计其事务与并发语义） |
| `events.py` | EventType/Event、NullEmitter、RecordingEmitter |
| `runtime.py` | AgentRuntime.call（含重试钩子、context 压缩 _shrink_largest、gather_limited） |
| `reliability.py` | RetryPolicy/call_with_retry、CircuitBreaker、TokenBucket、AsyncLimiter |
| `budget.py` | BudgetManager（record/check/status/remaining/pressure/snapshot/summary） |
| `chaos.py` | FaultKind/ChaosConfig/ChaosController/ChaosProvider（故障注入） |
| `providers/` | base(AgentProvider/Message/CompletionRequest/Response/estimate_tokens)、mock(确定性)、openai_compat、cli_provider、registry(build_provider) |
| `prompts.py` | worker/reviewer/planner/decomposer/prd 的 message 构造 + Contract marker + review_to_repair_hint |
| `parsing.py` | extract_json/extract_json_object/coerce_bool/normalise_keys/as_list_of_str |
| `errors.py` | TransientError/RateLimitError/TimeoutError_/ContextOverflowError/PermanentError/SchemaError/BudgetExceededError/CircuitOpenError/DecompositionError/GraphError/StateError + is_retryable |
| `util.py` | 杂项工具 |

缺失（本合同的实现范围）：`prd.py` `repo.py` `planner.py` `decomposer.py` `scheduler.py` `worker.py` `reviewer.py` `supervisor.py` `engine.py` `cli.py`、公开 API 导出、`tests/`、`examples/`、`benchmarks/`、README/ARCHITECTURE/DESIGN_DECISIONS、`STATUS.md`。

## 3. 必须实现的能力（P0 / C1–C17）

- **C1 规划**：PRD 文本 → `Requirement[]` → 初始 `Task DAG`（planner+prd+repo 模块；repo 提供 RepositoryContext 采集，离线可测）。
- **C2 状态机**：合法转移白名单，非法转移抛 `StateError`，且该约束在 scheduler 内部强制执行（不只是文档）。
- **C3 DAG 校验**：engine 在 run 开始与每次子图插入前调用 `graph.validate()`；环/悬空依赖/孤儿必须被拒绝并产生事件。
- **C4 exactly-once 派发**：同一 task 的并发 claim 只能有一个成功（原子 claim；建议 store 内 `UPDATE ... WHERE status='READY'` 语义）。N=64 并发抢同一任务只能 1 胜。
- **C5 崩溃恢复**：进程被 SIGKILL 后 `agentcorp resume <run_id>` 必须：DONE 任务零重跑；RUNNING 且 lease 过期的任务重派；事件 seq 连续；任务状态收敛。恢复后新增事件必须接续原 seq。
- **C6 重试与风暴防护**：指数退避 + 抖动、attempts 上限、poison task 隔离（终态 FAILED/QUARANTINED）；连续失败触发 CircuitBreaker 打开；重试总调用次数有界。
- **C7 预算硬停**：四类预算（token、cost、任务数、wall-clock）；派发前 check；超限即停止派发、pending 任务置 CANCELLED(budget)、run 状态 BUDGET_EXHAUSTED、报告与账本落盘。记账误差 ≤ 1 token。
- **C8 有界递归分解**：`max_depth`（默认 3）、`max_total_tasks`（默认 200）、每层预算检查；SPLIT 结果携带子 DAG，校验后**原子插入**；子任务全部终态后父任务聚合为 DONE/FAILED。
- **C9 并发正确性**：有界并发（默认 4，可配）；无就绪集竞态；死锁检测：`running==0 && ready==0 && pending>0` → `DEADLOCK_DETECTED` 事件 + 终止（不得挂死）。
- **C10 取消与优雅退出**：`cancel` API/CLI 后不再派发新任务；在途任务在 grace 期内结束或标记；SIGINT/SIGTERM → 持久化后退出，退出码语义化。
- **C11 事件日志**：append-only、run 内 seq 严格递增；每个状态变化必须有对应事件；可用事件流重放出等价任务状态（提供 `store.rebuild_from_events()` 或等价物 + 测试）。
- **C12 独立评审环**：reviewer 与 worker 必须是不同的实例/角色（禁止自审）；`Review.must_fix()` 为真 → 任务回炉（rework），rework 次数有上限；rejection 计数进入报告。
- **C13 Supervisor 治理**：检测 stuck/超时/重复失败/空转，产生 `SupervisorFinding` + `Intervention`；**必须有不误报测试**：正常长任务（超过阈值但仍在进展）不得被干预。
- **C14 Worker 执行**：prompt 构造（复用 prompts.py）→ provider 调用（复用 runtime/reliability）→ `parsing.py` 解析 → schema 校验（非法输出 → SchemaError → 重试）→ `FileWrite` 路径白名单校验（禁止越界写入）→ Artifact 记录。
- **C15 可观测性**：结构化 JSON 日志（可开关）；`agentcorp status` 输出 run/task/budget 快照；run report 见 §6。
- **C16 CLI**（typer，复用现有 `[project.scripts] agentcorp`）：`run` / `resume` / `status` / `graph`（ascii+mermaid）/ `report` / `providers`。退出码：0 成功，1 业务失败（run FAILED），2 用法错误，3 预算耗尽。
- **C17 Benchmark**：`examples/` 中的端到端 demo（mock provider，确定性）生成 `benchmarks/self_hosting_sim.json`，字段见 §6；`scripts/` 提供一键复现命令。

## 4. 非目标（v0 明确不做）

分布式/多机调度、Web UI（`web/` 可继续留空）、真实 LLM 效果优化、多租户、k8s、数据库选型替换。

## 5. 关键契约细节

### 5.1 状态机（白名单）
```
PENDING  -> READY | CANCELLED
READY    -> RUNNING | CANCELLED | BLOCKED
RUNNING  -> DONE | FAILED | REVIEW | SPLIT | BLOCKED | CANCELLED
REVIEW   -> DONE | READY(rework) | FAILED | CANCELLED
SPLIT    -> DONE(聚合后) | FAILED | CANCELLED
BLOCKED  -> READY | FAILED | CANCELLED
FAILED   -> READY(retry, 仅当 attempts<max) | QUARANTINED
终态：DONE, QUARANTINED, CANCELLED
```
除白名单外一切转移抛 `StateError`；白名单表必须可被测试直接引用（例如 `models.ALLOWED_TRANSITIONS`）。

### 5.2 exactly-once 与 lease
- claim 原子性由 store 事务保证；claim 成功即写入 `claimed_at/lease_expires_at`。
- 崩溃判定：`RUNNING && now > lease_expires_at` 视为孤儿，`resume` 时重派（at-least-once；副作用由幂等键约束）。
- 幂等键：`(run_id, task_id, attempt_group)` 语义须在 DESIGN_DECISIONS.md 写清。

### 5.3 预算语义
- check-before-dispatch；估算器可用 `providers.base.estimate_tokens`。
- 超限：pending → CANCELLED(budget)；RUNNING 允许跑完（默认）或按 `cancel_inflight` 配置取消。

### 5.4 事件类型（至少覆盖）
`RUN_STARTED / RUN_FINISHED / RUN_RESUMED`、`TASK_READY / TASK_CLAIMED / TASK_STARTED / TASK_FINISHED / TASK_FAILED / TASK_RETRIED / TASK_SPLIT / TASK_AGGREGATED / TASK_CANCELLED / TASK_REWORK`、`REVIEW_STARTED / REVIEW_REJECTED / REVIEW_APPROVED`、`BUDGET_WARNING / BUDGET_EXHAUSTED`、`INTERVENTION_RAISED / INTERVENTION_APPLIED`、`CYCLE_DETECTED / DEADLOCK_DETECTED / VALIDATION_FAILED`。

## 6. Run Report JSON schema（`benchmarks/*.json` 与 `agentcorp report`）

```json
{
  "schema_version": "1",
  "run_id": "str",
  "status": "DONE|FAILED|BUDGET_EXHAUSTED|CANCELLED|DEADLOCK",
  "started_at": "ISO8601", "finished_at": "ISO8601", "wall_ms": 0,
  "tasks": {"total": 0, "done": 0, "failed": 0, "cancelled": 0, "quarantined": 0, "split_parents": 0, "max_depth": 0},
  "success_rate": 0.0,
  "agent_calls": 0,
  "tokens": {"input": 0, "output": 0, "total": 0, "budget_limit": null, "pressure": 0.0},
  "cost_usd": 0.0,
  "retries": {"total": 0, "by_task": {}},
  "reviews": {"total": 0, "rejected": 0, "rework_rounds": 0},
  "interventions": {"total": 0, "false_positive_guarded": true},
  "files_changed": 0,
  "events_count": 0,
  "critical_path": ["task_id"],
  "per_task": [{"task_id": "str", "kind": "str", "status": "str", "attempts": 0, "tokens": 0, "duration_ms": 0, "review_rejected": false}]
}
```

## 7. 测试门槛（硬性）

1. `uv run pytest -q` 全绿；用例数 **≥ 150**（含参数化）。
2. 17 条能力每条至少 1 个正向 + 1 个反向（失败路径）用例。
3. 全部离线：无网络、无真实 API key；真实 provider 仅可作为 opt-in smoke（`-m smoke`，默认跳过）。
4. 覆盖 C4/C5/C6/C8/C9/C10 的测试必须使用真实并发/子进程（kill -9）/故障注入，不允许只做 mock 断言。
5. 单测总时长 < 120s（可用 `-m slow` 分离长测试，但 CI 默认跑全量不超 300s）。
6. `mypy`（strict, 已有配置）在 `src/agentcorp` 通过；`ruff check` 无 error。

## 8. 交付物（DoD 清单）

- [ ] §2 中「缺失」列出的全部模块实现并在 `agentcorp/__init__.py` 导出公共 API
- [ ] `tests/` ≥150 用例全绿（含 concurrency / recovery / budget / chaos 场景）
- [ ] `examples/` 端到端可跑（mock provider，确定性可复现）
- [ ] `benchmarks/self_hosting_sim.json` 生成且通过 §6 schema 校验
- [ ] `README.md`（含 30 秒 demo、架构图、quickstart、面试导向的亮点说明）
- [ ] `docs/ARCHITECTURE.md`、`docs/DESIGN_DECISIONS.md`（含 5 个关键设计决策 + 备选方案 + tradeoff）
- [ ] `docs/BASELINE_AUDIT.md`（对 §2 已存在模块的审计结论：可信/存疑/已修复）
- [ ] `STATUS.md`（滚动更新的状态与阻塞）
- [ ] 全程 git 提交，粒度清晰（每个里程碑 1+ commit）
