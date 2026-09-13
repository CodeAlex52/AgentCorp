# AgentCorp design decisions

Each decision records the chosen approach, the alternatives considered and the
trade-off accepted. Entries are numbered `DEC-xxx` and are referenced from code
comments and `docs/BASELINE_AUDIT.md`.

---

## DEC-001 — Extend `TaskStatus` to the SPEC whitelist instead of modelling READY as derived state

**Context.** The baseline deliberately stored only `PENDING` and *derived*
readiness from the graph (`graph.ready()` = "pending with all deps done"), with the
comment "readiness is derived, never stored, so it cannot drift".

**Options.**
1. Keep READY derived; implement the SPEC state machine around it.
2. Store READY as a real status (SPEC §5.1) and keep derivation as the *promotion
   rule* (`PENDING -> READY` when deps are DONE).

**Chosen: 2.** SPEC §5.1 lists `PENDING -> READY | CANCELLED` as transitions and the
acceptance contract (G-A) tests the table directly; a stored READY is also required
for atomic claim (`UPDATE ... WHERE status='READY'`), which is the only way C4 can be
exactly-once without a second lock.

**Trade-off.** Two writes per task before execution (promote, then claim) and a
possible transient window where a task is READY but not yet claimed. Mitigation:
promotion and claim happen in the same scheduler tick, and READY is never persisted
without the graph still agreeing (promotion re-checks dependencies).

---

## DEC-002 — Treat the baseline as product code: fix only proven defects

The snapshot is unverified but not disposable. Every module was exercised
(see `BASELINE_AUDIT.md`); defects DEN-x were fixed with a regression test, verified
behaviour was left untouched even where a rewrite would have been convenient.
Rationale: rewriting working persistence/graph code increases risk without adding
capability, and the brief explicitly forbids "rewrite because it looks nicer".

---

## DEC-003 — Transition enforcement lives in the store projection **and** the scheduler

SPEC §5.1 says illegal transitions raise `StateError` and C2 says the constraint is
enforced inside the scheduler. Enforcing only in the scheduler would let any event
hand-written into the log (or replayed) bypass the whitelist; enforcing only in the
store would make `StateError` a persistence concern.

**Chosen:** both. `models.ALLOWED_TRANSITIONS` is the single table;
`models.assert_transition()` is called by the scheduler before it emits, and by the
store's `_mutate_task()` while projecting (which also guards `rebuild_projections`).

**Trade-off.** Every projection must produce legal sequences — more discipline in the
event design (e.g. rework is `REVIEW -> READY`, recovery is a two-step
`RUNNING -> FAILED -> READY`). This is the point: an illegal log fails loudly at
replay time instead of silently corrupting the projection.

---

## DEC-004 — Four budget dimensions with refusal semantics at `used >= limit`

SPEC C7 requires token / cost / task-count / wall-clock ceilings. Baseline had
token, cost, calls and per-task tokens.

**Chosen:** `BudgetLimits` gains `max_tasks` and `max_wall_seconds`;
`BudgetManager.check()` treats *reaching* a ceiling as exhausted (`used >= limit`)
so the boundary case cannot dispatch work with a zero estimate
(`BASELINE_AUDIT.md` DEF-10). `max_agent_calls` remains as the call-level ceiling
(a stricter sibling of `max_tasks`), and the run report exposes all of them.

**Trade-off.** A limit of exactly N tokens now refuses the N-th token rather than
the N+1-th; documented in the report as `pressure == 1.0`.

**Wall clock** is measured from `BudgetManager.start()` using the injected clock;
on `resume` the manager is re-seeded with the original run start (persisted in the
`RUN_STARTED` payload) so a crash loop cannot extend the budget.

---

## DEC-005 — exactly-once claim = guarded UPDATE + events in one transaction

**Chosen.** `Store.claim_task()` executes, inside a single `RLock` + transaction:
`UPDATE tasks SET status='running' WHERE id=? AND status='ready'`; on `rowcount == 1`
it appends `TASK_CLAIMED` (owner, `claimed_at`, `lease_expires_at`) and
`TASK_STARTED` (attempt+1, `started_at`), otherwise returns `None` without writing.
Rejected alternative: `SELECT` then `UPDATE` (two processes can interleave between
the statements under SQLite's default deferred transactions).

**Trade-off.** Claim is "paid for" with a write even when it loses the race
(rowcount 0) — negligible at this scale. `PRAGMA busy_timeout=5000` is set so a
competing writer retries instead of raising `database is locked`.

**Idempotency key.** `(run_id, task_id, attempt_group)` where `attempt_group` is the
1-based `Task.attempts` value at claim time: a provider retry inside one claim keeps
the same group (runtime retries are invisible to the ledger), while a re-claim after
failure/lease expiry gets a new group, so side effects can be keyed
`run:task:attempt_group` without double-applying.

---

## DEC-006 — Crash recovery goes through FAILED before re-dispatch

SPEC's whitelist has no `RUNNING -> READY` edge, but C5 requires stale RUNNING tasks
to be re-dispatched on resume.

**Chosen:** recovery is the two-step legal path `RUNNING -> FAILED`
(`TASK_FAILED`, reason `lease_expired`) then `FAILED -> READY` (`TASK_RETRIED`),
subject to the same `attempts < max_attempts` guard as any retry. A crash therefore
consumes one attempt — deliberate: repeatedly crashing on the same task *should*
eventually quarantine it rather than loop forever.

**Rejected:** adding `RUNNING -> READY` to the whitelist (weakens G-A), or a special
"recovery" bypass (makes `ALLOWED_TRANSITIONS` a lie).

---

## DEC-007 — Review/rework event granularity

`REVIEW_STARTED` moves `RUNNING -> REVIEW`; `REVIEW_APPROVED` moves `REVIEW -> DONE`;
`REVIEW_REJECTED` records the verdict (and the `Review` row) *without* a status
change, then either `TASK_REWORK` (`REVIEW -> READY`, `rework_count += 1`) or
`TASK_FAILED` fires. This keeps every status change paired with exactly one event
(C11) and keeps the rework cap (`max_reworks`) visible in the ledger.

**Trade-off.** A rejected-and-not-reworable task produces two events
(`REVIEW_REJECTED`, `TASK_FAILED`) instead of one. Accepted for auditability:
"why did it die" is answered by the rejection event.

---

## DEC-008 — Benchmark determinism: injected clock, no wall-clock dependency

`examples/end_to_end.py` runs the real engine against a checked-in toy repository
with a mock provider, a seeded RNG and a `FakeClock`. Wall-clock durations in the
report come from the injected monotonic clock, so re-running the demo produces a
byte-identical `benchmarks/self_hosting_sim.json` (asserted by a test). No network,
no API key, no `time.sleep`.

**Trade-off.** `wall_ms` is a *simulated* duration, not wall time; the report header
documents this and real runs simply inject `SystemClock`.

---

## DEC-009 — Run identity: one project == one orchestration run

`run_id` in the CLI/report/events is the `Project.id`; `AgentRun` remains the
per-provider-call record. Avoids introducing a third identity concept for v0.
`agentcorp resume <run_id>` resolves ids *or* names via `Store.resolve_project`.

---

## DEC-010 — Report schema is validated structurally by a stdlib script

No `jsonschema` dependency is available offline, so
`scripts/validate_benchmark.py` implements the SPEC §6 checks (required keys, types,
enums, `per_task` item shape) with the standard library only, and
`benchmarks/report_schema.json` is emitted for consumers that do have `jsonschema`.
The engine's own tests assert the same properties so a malformed report cannot pass
CI even if the script is not run.

---

## DEC-011 — Supervisor progress signal instead of a stuck timer alone

A "cancel anything running longer than T" supervisor is the classic false-positive
generator (a legitimate long agent call looks identical to a hang). The supervisor
therefore tracks `last_progress_at` per task: claim/start, every finished agent run,
every artifact, every heartbeat `NOTE` resets it. Only "no progress for
`stuck_after_s`" raises STUCK. A long task whose agent calls keep completing is
never touched — that property is directly tested (C13 false-positive guard).

**Trade-off.** A task whose single provider call hangs produces no progress and is
detected; a task that makes progress forever within its budget is intentionally not
interrupted (budget wall-clock is the backstop for that).

---

## DEC-012 — Cancel is durable and cooperative

`agentcorp cancel <run_id>` writes a `cancel_request` document; a running engine
polls it once per tick, stops dispatching, gives in-flight tasks `cancel_grace_s`
to finish, then cancels the remainder (`TASK_CANCELLED`) and finishes the run as
`CANCELLED`. In-process callers use `engine.request_cancel()`.

**Trade-off.** Cancellation latency is one tick (< `tick_interval_s` + grace) rather
than immediate; it works across processes and does not require signal plumbing into
worker tasks.
