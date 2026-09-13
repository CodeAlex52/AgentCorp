# AgentCorp baseline audit (Step 1)

Scope: the unverified prototype snapshot imported 2026-09-14 (commit `4379ef5`).
Method: every verdict below comes from a real execution — imports, minimal calls and
throw-away probe scripts run against the editable install
(`.venv/bin/python`, Python 3.13.5) — not from reading alone. Raw probe transcripts
are summarised inline; commands are reproducible as described.

Verdict key: **trusted** (behaviour verified, used as-is) · **suspect** (behaviour
verified but incomplete for SPEC v0) · **defective** (verified wrong behaviour,
fixed, see `docs/DESIGN_DECISIONS.md`).

---

## Summary table

| Module | Verdict | Evidence |
|---|---|---|
| `util.py` | trusted | `FakeClock`/`SystemClock` advance both wall+monotonic (`probe2` C1/C2 breaker time travel works) |
| `errors.py` | suspect | taxonomy works, but `ContextOverflowError.retryable=False` breaks the runtime shrink path (DEF-03) |
| `models.py` | defective | missing `READY/REVIEW/QUARANTINED` statuses and `ALLOWED_TRANSITIONS` (DEF-01); `BudgetLimits` lacks task-count/wall-clock caps (DEF-02); no lease fields |
| `events.py` | defective | 17 SPEC §5.4 event names missing (DEF-05) |
| `graph.py` | suspect | validation/ready/critical path verified; `rewire()` leaves stale dependents (DEF-04); glyphs/poison set incomplete |
| `store.py` | defective | `append_many` not atomic (verified partial batch, DEF-06); no atomic claim/lease API (C4/C5 gap); `TASK_READY` projects to `PENDING` (DEF-01 side-effect); no `busy_timeout` |
| `runtime.py` | defective | context-overflow shrink never retried (DEF-03, traceback); `chaos_injections` never populated (DEF-07) |
| `reliability.py` | defective | duplicate `on_attempt` callback (verified `[(1,0.0),(1,1.0),(2,0.0)]`, DEF-08); latency/token-budget tests (probe2/3b) otherwise trusted |
| `budget.py` | suspect | accounting exact (`B2` refusal at 100+1>100); boundary at exactly-limit allows a zero-estimate call (`B1` `exhausted=False`); missing two SPEC dimensions (DEF-02) |
| `chaos.py` | trusted | `RateLimitError` injected and reported (`CH1/CH2`); seeded, deterministic |
| `parsing.py` | trusted | fences/prose/bare-list/`coerce_bool`/`as_list_of_str` all verified |
| `prompts.py` | trusted | all five `CONTRACT:` markers present (`P1–P3`); repair hint correct (`P4`) |
| `providers/base.py` | trusted | request/response models, `estimate_tokens`, `cost_of` used successfully in `probe2` R1–R2 |
| `providers/mock.py` | suspect | worker/planner/prd/reviewer contracts verified; `decomposer` contract falls through to the worker handler (DEF-09, probe `R3`) |
| `providers/openai_compat.py` | trusted (unused offline) | imports clean; not exercised (no network allowed) |
| `providers/cli_provider.py` | trusted (unused offline) | imports clean; not exercised |
| `providers/registry.py` | trusted | `build_provider("mock")` import path works; unknown spec raises `PermanentError` |

Baseline test suite: **0 tests existed** (`tests/` empty). `uv run pytest -q` exited
5 ("no tests ran"). That is the largest single gap against SPEC §7.

---

## Detailed findings

### DEF-01 — Task state machine does not match SPEC §5.1 (fixed)

Probe: `python -c "from agentcorp import models; print([s.value for s in models.TaskStatus])"`
→ `['pending','running','blocked','failed','done','split','cancelled']`;
`models.Task(status="ready")` → `ValidationError`; `hasattr(models, "ALLOWED_TRANSITIONS")`
→ `False`.

SPEC §5.1 requires `READY`, `REVIEW`, `QUARANTINED`, an explicit whitelist table
(`models.ALLOWED_TRANSITIONS`), and terminal statuses `{DONE, QUARANTINED, CANCELLED}`.
Baseline `models.py:67-70` additionally classified `SPLIT` as terminal, which
contradicts `SPLIT -> DONE(聚合后)`.

Impact: C2 (state machine), C4 (claim of READY tasks), C8 (aggregation), C12 (review
loop) are unimplementable without these statuses.
Resolution: statuses + whitelist added (`models.py`), transitions validated in the
store projection (`store.py`), documented as DEC-001/DEC-003.

### DEF-02 — Budget has only 3 of the 4 SPEC dimensions (fixed)

Probe: `BudgetLimits.model_fields` → `max_tokens, max_cost_usd, max_agent_calls,
max_tokens_per_task`. `BudgetLimits(max_tasks=5, max_wall_seconds=10)` is accepted
**and silently ignored** (pydantic drops extras).

SPEC C7/§5.3 require four hard-stop classes: token, cost, **task count**,
**wall-clock**. Resolution: added `max_tasks` / `max_wall_seconds` fields and
enforcement (`budget.py`), documented as DEC-004.

### DEF-03 — context-overflow shrink never retries (fixed)

Probe (`probe3`): a provider raising `ContextOverflowError` on the first call makes
`runtime.call` fail on attempt 1 even though `_shrink_largest` ran. Root cause:
`runtime.py:146-154` shrinks and re-raises, expecting the retry loop to try again,
but `errors.py:60-67` declares `retryable = False`, so `call_with_retry` propagates
immediately. Traceback captured at `runtime.py:145`.
Resolution: after a successful shrink the runtime re-raises a retryable
`TransientError` carrying the original message; when shrink budget is exhausted the
original `ContextOverflowError` surfaces. New tests cover both branches.

### DEF-04 — `TaskGraph.rewire()` leaves the parent in `_dependents` (fixed)

Probe (`probe4`): after `rewire("P", ["C1","C2"])` the dependent `D.dependencies`
is correctly `['C1','C2']`, but `graph.stuck_parents()` still returns `['P']` and the
networkx edge `P -> D` survives. Root cause: `graph.py:112-121` mutates
`dependent.dependencies` in place *before* calling `self.update(dependent)`, so
`update()` (`graph.py:74-82`) reads the *new* dependency list as the "old" one and
never removes the stale edge.
Impact: supervisor `stuck_parents` signal would fire on healthy splits (C8/C13);
`descendants()`/cycle checks see phantom edges.
Resolution: `update()` now removes edges by inspecting the existing graph edges
rather than the (possibly mutated) task object's dependency list.

### DEF-05 — SPEC event names absent (fixed)

Probe: `EventType` has 32 members; missing: `RUN_STARTED RUN_FINISHED RUN_RESUMED
TASK_CLAIMED TASK_FINISHED TASK_AGGREGATED TASK_REWORK REVIEW_STARTED REVIEW_REJECTED
REVIEW_APPROVED BUDGET_WARNING BUDGET_EXHAUSTED INTERVENTION_RAISED
INTERVENTION_APPLIED CYCLE_DETECTED DEADLOCK_DETECTED VALIDATION_FAILED`.
Resolution: all added; old names kept as aliases so no generated data breaks.

### DEF-06 — `Store.append_many` is not atomic (fixed)

Probe (`probe2` S6/S7): appending `[NOTE, TASK_STARTED(unknown task)]` raises
`StateError` on the second event but the first event is already committed
(`event_count == 2` — project + note). SPEC C8 requires the child-DAG insertion to be
**atomic**. Resolution: `append_many` writes every event in one transaction; a
failure rolls the whole batch back (regression test added).

### DEF-07 — `AgentRun.chaos_injections` never populated (fixed)

Probe (`probe3b` CH3): a `DUPLICATE_RESPONSE` injection is served but
`run.chaos_injections == []`. Resolution: the runtime copies `response.raw["chaos"]`
into the run record.

### DEF-08 — duplicate `on_attempt` callback (fixed)

Probe (`probe3` RB1): `[(1,'RateLimitError',0.0), (1,'RateLimitError',1.0),
(2,'RateLimitError',0.0)]` — the failure callback fires twice per attempt (once
before the delay is computed, once after), producing two `NOTE` retry events per
attempt in the ledger. Resolution: delay computed first, callback invoked once.

### DEF-09 — mock provider has no decomposer contract (fixed)

Probe (`probe2` R3): `prompts.decomposer_messages(...)` routed through
`MockProvider._render` falls into `handler.get(contract, self._worker)`
(`providers/mock.py:141-147`) and returns a **worker** reply; parsing yields
`should_split: None`. Resolution: added a deterministic `_decompose` handler that
derives subtasks from the task's recommended subtasks/title.

### DEF-10 — budget boundary permits a zero-estimate call at exactly the cap (fixed)

Probe (`probe2` B1/B2): with `max_tokens=100` and exactly 100 tokens used,
`BudgetManager.status().allowed is True` but `check(estimated_tokens=1)` refuses.
`BudgetSnapshot.exhausted` (models.py:318-324) disagrees with `BudgetManager.status`
(budget.py:102). Resolution: `status()` treats `used >= limit` as refusal (matching
the snapshot property and SPEC "超限即停止派发").

---

## Non-defects (verified, left alone per DEC-002)

* `graph.validate()` catches multi-node cycles **and** self-loops (`G3`,`G4`);
  dangling-dependency detection covered by `graph.py:274-287`.
* `store.rebuild_projections()` is idempotent — digests match before/after
  (`S5`), which is the replay property C11/Q depends on.
* `Store` appends assign strictly increasing `seq` (`S1`).
* Usage projection is exact after run completion (`U2` 15 tokens / 1 call from
  `AGENT_RUN_STARTED`+`AGENT_RUN_FINISHED`).
* `MockProvider` worker/planner/reviewer contracts and `ScriptedProvider` are
  deterministic (`R1`,`R2`, `probe3b CH1`).
* Retry/backoff, circuit breaker state machine, token bucket and `is_retryable`
  behave as documented (probe2 C1/C2, probe3 RB1).

## Gaps carried into DESIGN_DECISIONS

See `docs/DESIGN_DECISIONS.md`: DEC-001 (statuses), DEC-002 (baseline treatment),
DEC-003 (transition enforcement point), DEC-004 (budget dimensions), DEC-005
(claim/lease), DEC-006 (crash recovery path), DEC-007 (review/rework events),
DEC-008 (benchmark determinism).
