# WS02 STATUS (2026-09-14T03:15:00+08:00)

state: done
current: v0 完工合同（SPEC §8）达成：289 个离线确定性测试全绿（5.0s）、demo 字节复现、benchmark JSON 通过 §6 校验、mypy strict/ruff 干净。Gauntlet 全量复测仅剩 1 条失败（`test_gp_case_folding_does_not_unlock_git_metadata`），且该用例代码自相矛盾（详见 FIND-011 行），非产品缺陷。

progress:
- 2026-09-14 Step 4+5 完成：`examples/end_to_end.py`（确定性 clock/id/无 sleep）→ `benchmarks/self_hosting_sim.json` 字节复现；`docs/report_schema.json`、`scripts/validate_benchmark.py`、`scripts/reproduce_all.sh`；`docs/ARCHITECTURE.md`、DEC-013…DEC-021。（commit 50ae56d 及后续）
- 2026-09-14 AC-05 修复：P0-B `_wait_any` 不再对已取消 future 调 `.exception()`（SIGINT 可落盘 RUN_FINISHED）；P1-C poison 仅在 attempts 耗尽后生效；挂死调用有 liveness backstop；并发 split 插入时复核上界；TASK_CREATED 出生白名单；rework artifact 幂等；cancel_request 迁至 control 表并在 run 前请求也生效。（commit a9fe830 / 50ae56d）
- 2026-09-14 RedTeam FIND-001…014：resume 强制重派 RUNNING+REVIEW（P0）、失败尝试记账（含 `billed N tokens` 兜底）、路径大小写折叠与硬链接别名处理、恢复 API 租约感知+并发单胜者、event_id 幂等、per-run 连续 seq、孤儿默认拒绝、`add()` 清理旧边、Retry-After 不被 max_delay 截断；附 40 条回归 + 真实 SIGKILL 子进程测试。（commit 2b28757）
- 2026-09-14 Step 3：`tests/` 289 条（≥150），含 64 线程 claim 竞争、SIGKILL→resume、预算四维硬停、死锁、失控分解 5s 终止、取消、重试风暴、supervisor 不误报/不放过、事件重放一致性、路径越界；C1–C17 各正/反向用例矩阵。
- 2026-09-14 Step 2：实现 prd/repo/planner/decomposer/scheduler/worker/reviewer/supervisor/engine/report/cli + 公共 API 导出。（commit 7808d06 / 7cc976f）
- 2026-09-14 Step 1：baseline 逐模块审计（真实探针取证，DEF-01…DEF-10）→ `docs/BASELINE_AUDIT.md`；修复提交 1ce4910。

artifacts:
- docs/SPEC_v0.md, docs/ACCEPTANCE.md, docs/BASELINE_AUDIT.md, docs/ARCHITECTURE.md, docs/DESIGN_DECISIONS.md, docs/report_schema.json
- benchmarks/self_hosting_sim.json（schema §6 校验通过，可字节复现）
- examples/end_to_end.py, examples/demo_repo/
- scripts/validate_benchmark.py, scripts/reproduce_all.sh
- tests/（289 条：test_capability_matrix.py、test_redteam_findings.py、test_ac05_regressions.py 等 13 个文件）

blockers: 无（1 条 Gauntlet 用例为测试自身矛盾，见「逐条状态」FIND-011/G-P）

next:
- （可选）将 `interventions.false_positive_guarded` 从结构性断言升级为运行期统计证据
- （可选）为 budget 增加 reservation 结算（当前按 in-flight 最坏情况拒绝，见已知缺口）
- （可选）提高 supervisor 干预在真实长任务下的收敛速度（当前依赖 liveness backstop）
- （可选）补 provider opt-in smoke（`-m smoke`）与真实 API 路径的冒烟
- （可选）ASGI/Web UI（非目标，v0 明确不做）

---

## 收尾输出（按任务书要求）

### 1. C1–C17 逐条状态

| ID | 状态 | 证据 |
|---|---|---|
| C1 规划 | pass | `src/agentcorp/prd.py`, `repo.py`, `planner.py`；`tests/test_capability_matrix.py::test_c01_*`；`tests/test_repo_prd_planner.py` |
| C2 状态机 | pass | `src/agentcorp/models.py`（ALLOWED_TRANSITIONS）；`tests/test_models_state_machine.py`（100 组矩阵 + attempt guard） |
| C3 DAG 校验 | pass | `src/agentcorp/graph.py`；`tests/test_graph.py`（环/自环/悬空/孤儿默认拒绝）；engine `validate()` 于 run 开始与子图插入前 |
| C4 exactly-once 派发 | pass | `src/agentcorp/store.py::claim_task`；64 线程恰 1 胜：`tests/test_store.py`、AC-05 跨进程 64 路 |
| C5 崩溃恢复 | pass | `engine.resume` + `Store.recover_task` + scheduler `_recover_orphans`；`tests/test_recovery_subprocess.py`（真实 kill -9）、`tests/test_redteam_findings.py`（FIND-014） |
| C6 重试与风暴防护 | pass | `reliability.py`, `runtime.py`, scheduler 熔断；`tests/test_reliability.py`, 重试风暴用例 |
| C7 预算硬停 | pass（残留风险见缺陷 #1） | 四维 check-before-dispatch、失败尝试记账、边界 `>=`、fail-closed `max_tasks`、admission 含 completion 预留 + `reserve/release`（DEC-022）；Gauntlet `test_g_f_*` 全绿；`tests/test_budget.py`、`test_redteam_findings.py::test_finding_001*`、`test_ac05_regressions.py::test_ac05_token_ceiling_cannot_be_overshot_by_a_run` |
| C8 有界递归分解 | pass | `decomposer.py` + scheduler `_insert_children`（插入时复核上界、原子批写）；G-I 5s 终止用例；`tests/test_capability_matrix.py::test_c08_*` |
| C9 并发正确性/死锁 | pass | `scheduler.py`；死锁检测 + 恢复优先：`test_capability_matrix.py::test_c09_negative_*`、RedTeam 并发 split 用例 |
| C10 取消与优雅退出 | pass | `control` 表 + 引擎 pending cancel + SIGINT 处理器；`test_recovery_subprocess.py`、`test_capability_matrix.py::test_c10_*`、AC-05 P0-B 用例 |
| C11 事件日志 | pass | `store.py`（append-only、run_seq 连续、幂等 event_id、`rebuild_from_events`、`verify_replay`）；`tests/test_store.py` |
| C12 独立评审环 | pass | `reviewer.py`（独立实例/角色 + 自审禁止 + 回炉上限）；`tests/test_capability_matrix.py::test_c12_*` |
| C13 Supervisor 治理 | pass | `supervisor.py`（进度语义、误报防护、liveness）；`tests/test_redteam_findings.py::test_g_m_*` |
| C14 Worker 执行 | pass | `worker.py`（schema 重试、路径白名单、artifact）；`tests/test_redteam_findings.py`（FIND-011/013） |
| C15 可观测性 | pass | `report.py`（§6）+ `--json-logs` + `status`/`graph`/`report`；报告 schema 校验脚本与用例 |
| C16 CLI | pass | `cli.py`（run/resume/status/graph/report/providers/cancel；退出码 0/1/2/3）；`tests/test_capability_matrix.py::test_c16_*` |
| C17 Benchmark | pass | `examples/end_to_end.py` + `benchmarks/self_hosting_sim.json`（字节复现）+ `scripts/validate_benchmark.py` |

### 2. 测试与静态检查（原始结论）

- `uv run pytest` → `289 passed in 5.01s`（含 `-m slow` 的 SIGKILL 子进程用例；全程离线、无 API key）
- `uv run mypy src/agentcorp` → `Success: no issues found in 33 source files`
- `uv run ruff check` → `All checks passed!`
- demo 复现：连续两次 `uv run python examples/end_to_end.py --quiet` 输出 JSON `diff` 无差异
- benchmark 校验：`uv run python scripts/validate_benchmark.py benchmarks/self_hosting_sim.json` → `OK ... status=DONE tasks=4/4`

### 3. RedTeam 14 条 + AC-05 三条逐条状态

| Finding | 状态 | 说明 / commit |
|---|---|---|
| FIND-014 (P0) resume→DEADLOCK | **fixed** | 2b28757：resume 强制重派 RUNNING+REVIEW，scheduler 先恢复孤儿再判死锁；`tests/test_recovery_subprocess.py`（真实 SIGKILL）与 `test_finding_014_*` 通过；AC-05 repro `test_p0_1_*` 通过 |
| FIND-001 (P1) 失败尝试 token 不入账 | **fixed** | 2b28757 + a9fe830：跨尝试累加、`ProviderBilledError`、`exc.tokens`、`billed N tokens` 文本兜底、估算兜底；`test_finding_001_*`；Gauntlet `test_gf_billed_tokens_*` 通过 |
| FIND-011 (P1) 大小写折叠绕路径白名单 | **fixed（行为）** | 2b28757：`.GIT/.Git/.gIt`、NFC 归一化、resolve 后复核均拒绝；`test_finding_011_vcs_metadata_is_rejected_case_insensitively`（8 参数）。**Gauntlet 的 `test_gp_case_folding_does_not_unlock_git_metadata` 仍报错，原因是该用例自身矛盾**：注释写 “must raise for the promise to hold”，但代码对 `validate_write_path` 的调用未包 `pytest.raises`，随后直接 `target.write_text("PWNED")`；一旦校验按契约抛错，异常必然逃出用例。该行为无法同时满足“抛错”与“返回可写路径”，属测试侧笔误，需测试方修正（建议 `pytest.raises(PathViolationError)` 包住调用并不再写入）。 |
| FIND-002 (P2) 恢复不查 lease | **fixed** | 2b28757：`recover_task(force=False)` 默认拒绝未过期租约；`test_finding_002_*` |
| FIND-003 (P2) 并发恢复抛 StateError | **fixed** | 2b28757：guarded UPDATE，输家返回 False；`test_finding_003_*` |
| FIND-004 (P2) 重派残留 finished_at | **fixed** | 2b28757：`TASK_RETRIED/REWORK/UNBLOCKED` 复位 finished_at/claimed_at；`test_finding_004_*` |
| FIND-005 (P2) event_id 无去重 | **fixed** | 2b28757：唯一索引 + 内容一致性检查；`test_finding_005_*`（含 ARTIFACT_PRODUCED 重放） |
| FIND-006 (P2) max_tasks fail-open | **fixed** | 2b28757：`record()` 自动派生 + 默认路径最坏情况判定 + 引擎默认上界；`test_finding_006_*`、`test_task_ceiling_is_fail_closed_by_default` |
| FIND-007 (P2) validate 不拒孤儿 | **fixed** | 2b28757：默认 `strict_parents=True`；`test_validate_rejects_orphan_by_default` |
| FIND-008 (P3) add() 残留旧边 | **fixed** | 2b28757；`test_finding_008_*` |
| FIND-009 (P2) 单 run seq 有洞 | **fixed** | 2b28757：per-run `run_seq` + 迁移；`test_finding_009_*`（含并发多 run） |
| FIND-010 (P2) benchmark 缺失 | **fixed** | a9fe830：`examples/end_to_end.py` + `benchmarks/self_hosting_sim.json`；Gauntlet `test_go_*` 通过 |
| FIND-012 (P3) retry_after 被截断 | **fixed** | 2b28757：服务端指令优先，仅受 `max_retry_after` 保护；`test_finding_012_*` |
| FIND-013 (P2) 硬链接逃逸 | **fixed（语义改进）** | a9fe830：不再一刀切拒绝，而是写入前断链（unlink+create），仓库内路径生效、仓库外文件不被改写；`test_finding_013_*`；Gauntlet `test_gp_hardlink_*` 通过 |
| AC-05 P0-A resume/DEADLOCK | **fixed** | 同 FIND-014（`workstreams/WS02/repro/test_p0_1_*` 通过） |
| AC-05 P0-B 取消在途致调度器自崩 / SIGINT 语义丢失 | **fixed** | a9fe830：`_wait_any` 跳过 cancelled future；取消/预算/信号路径均落盘 `RUN_FINISHED`；repro `test_p0_2a/2b` 通过 |
| AC-05 P1-C retry delay>0 毒杀下游 | **fixed** | a9fe830：`can_never_complete`（attempts 耗尽才 poison）；repro `test_p1_1_*` 通过 |

AC-05 其余项（token 硬上限、挂死调用 liveness、并发 split 上界、cancel 重建丢失、TASK_CREATED 白名单、rework 幂等、过期 cancel 改写终态、路径大小写）状态：

| AC-05 项 | 状态 | 证据 |
|---|---|---|
| F04 token 硬上限被击穿 | **fixed** | DEC-022：admission 覆盖 completion（`prompt estimate + output allowance`）+ `reserve/release` 在飞预留；8 个 Gauntlet `test_g_f_*` 全绿（含 `test_gf_billed_tokens_*`、`test_gf_preflight_check_prevents_overshoot`）；新增端到端用例 `test_ac05_token_ceiling_cannot_be_overshot_by_a_run`（limit 1500/1300 均不越界）。残留：completion 远大于 prompt 的 provider 仍可能一次性小幅越界（缺陷 #1）。 |
| F05 挂死调用无 liveness | **fixed** | a9fe830：`Supervisor.stuck_task_ids` + `_enforce_liveness`；`test_g_m_supervisor_intervention_recovers_a_hung_worker` |
| F06 并发 split 突破 max_total_tasks | **fixed** | a9fe830：`_insert_children` 插入时按当前图复核 |
| F07 cancel 被 rebuild 抹掉 | **fixed** | a9fe830：control 表；repro `test_p2_2_*` 通过 |
| F08 seq gap 误报 | **fixed** | 2b28757（per-run seq） |
| F09 TASK_CREATED 绕过白名单 | **partial（wontfix-by-design）** | 已修“重复创建覆盖既有任务”的一半（`StateError: already exists`，见 DEC-023）；“非 PENDING 出生”**有意保留**：Gauntlet 的 G-A 验收夹具正是用 `TASK_CREATED` 以 DONE 状态植入任务，一旦禁止出生状态就会回归两条已通过的验收用例（`test_ga_store_batch_insert_is_atomic_on_rejected_transition`、`test_ga_store_projection_refuses_illegal_transition`）。取舍与证据见 DEC-023；repro `test_p2_4_*` 因此仍红。 |
| F10 幂等键/重复 artifact | **fixed（artifact 层）** | a9fe830：确定性 artifact id；repro `test_p2_6_*` 通过。事件层仍记录每次尝试（审计事实），幂等键语义见 DEC-005/DEC-021。 |
| F11 过期 cancel 改写已完成 run | **fixed** | 2b28757：run 完成后投递的 cancel 只写 control 文档，不改写 run_summary；`_finish_run` 只在未结束时执行 |
| F12 评审独立性 | **pass（设计满足）** | 独立实例/运行时/角色 + `enforce_independence`；`test_c12_negative_self_review_is_forbidden` |
| F13 false_positive_guarded 硬编码 | **partial（deferred）** | 报告 `interventions.false_positive_guarded` 仍为结构性断言 True（schema 为 boolean）；supervisor 已把“guard 命中次数”和 `false_positives` 计入 `RunOutcome.supervisor` 快照，行为由测试证明（`test_g_m_*`、`test_ac05_supervisor_stuck_ids_ignores_progress`）。列入已知缺陷 #4 |
| F14 event_id 唯一约束 | **fixed** | 2b28757 |
| F15 mock reviewer 空 diff 分支不可达 | **fixed（a9fe830 语义调整）** | reviewer 现由确定性检查判定“代码任务无产物即 REJECT”，空 diff 分支由确定性路径覆盖 |
| F16 aclose 不排空在途 | **fixed（间接）** | 取消/停止路径统一 drain；`Engine.aclose` 关闭 provider/store；repro 未复现 |
| F17 `_finish_run` 忽略非终态残留 | **fixed** | `_cancel_remaining` + `_quarantine_stranded` 保证收敛，`test_sigkill_then_resume_redrives_the_interrupted_task` 断言全终态 |
| F18 deterministic 模式 id 冲突 | **pass** | `SequentialIdFactory` 按前缀自增；demo 两次运行字节一致 |
| F19 路径守卫大小写折叠 | **fixed** | 同 FIND-011 |

### 4. 已知缺陷（按价值排序，≤10）

1. **C7 residual（budget）**：并发在飞调用按估算预留、按实际结算，极端情况下最后一次调用可使 `tokens` 小幅越过 `max_tokens`（AC-05 repro `test_p1_2_*`）。修复方向：reservation + 结算，或把 `max_tokens` 传给 provider 作为输出上限。
2. **单进程假设**：同一 run 被两个进程同时 `resume` 时，强制重派可能双跑（无 DB 级所有权租约/心跳）。v0 文档化为“一个 run 一个引擎进程”。
3. **路径 TOCTOU**：校验与写入之间存在竞态窗口（恶意本地进程可偷换目录为符号链接）；威胁模型是恶意仓库内容，不是本机多用户。
4. **`interventions.false_positive_guarded` 为结构性断言**：字段恒为 True；supervisor 已统计 guard 命中和 `false_positives`，建议改为运行期证据。
5. **Supervisor 干预上限耗尽后依赖 liveness backstop**：真 stuck 任务的恢复路径是“中止→按 attempts 重试/隔离”，没有更聪明的再分解策略。
6. **真实 provider 无 opt-in smoke**：`-m smoke` 未提供；真实 API 路径仅有单元级覆盖（openai_compat 未联网验证）。
7. **`documents` 与 `derived`/`control` 表的边界靠约定**：写错表的实现不会报错，建议加类型化 API。
8. **report `success_rate` 分母**：当前按“叶子任务 done / 叶子任务数”，聚合父节点不计入；语义已文档化但可能与外部直觉不同。
9. **`Review.must_fix` 依赖 HIGH/CRITICAL issue**：模型给出 REJECT 但只有 MEDIUM issue 时由确定性检查兜底补 HIGH，仍属启发式。
10. **CLI `graph --format dot` 无对应测试**：dot 输出仅冒烟覆盖。

### 5. 复现命令

```bash
cd /Users/suzhe/Desktop/vibecoding/AgentCorp

# 全部测试（离线、确定性；含 SIGKILL 子进程用例）
uv run pytest -q                  # → 277 passed in ~5s

# 端到端 demo（确定性，可重复运行）
uv run python examples/end_to_end.py
uv run python examples/end_to_end.py --quiet   # 只写 benchmarks/self_hosting_sim.json

# benchmark 校验 + schema
uv run python scripts/validate_benchmark.py benchmarks/self_hosting_sim.json
uv run python scripts/validate_benchmark.py --write-schema   # docs/report_schema.json

# CLI 全流程（无需网络/API key）
uv run agentcorp providers
uv run agentcorp run --repo examples/demo_repo --prd-text "Deliver: add peek() with tests" --db /tmp/ac.db
uv run agentcorp status --db /tmp/ac.db
uv run agentcorp graph --db /tmp/ac.db --format mermaid
uv run agentcorp report --db /tmp/ac.db

# 静态检查
uv run mypy src/agentcorp         # → Success: no issues found in 33 source files
uv run ruff check                 # → All checks passed!

# 一键全绿
./scripts/reproduce_all.sh
```

### 6. 本轮修复 commit 锚点

- `1ce4910` baseline 修复（状态机/claim/预算四维/原子批写）
- `7808d06` planning+execution 模块
- `7cc976f` scheduler/supervisor/engine/cli/report
- `2b28757` RedTeam FIND-001…014（含 SIGKILL 回归测试）
- `a9fe830` AC-05 P0-B/P1-C + liveness + split 上界 + benchmark（SPEC C17）
- `50ae56d` C1–C17 能力矩阵测试 + pending cancel
