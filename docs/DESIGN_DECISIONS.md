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

---

## DEC-013 — Recovery is semantic, not lease-bound

`resume` treats every interrupted `RUNNING`/`REVIEW` task as recoverable by
default (`--force-requeue`), because the *definition* of resume is that the
previous process is gone.  The lease remains the mechanism for the *live* system:
`Store.recover_task()` refuses a task whose lease is still valid unless the caller
explicitly forces it, so a watchdog or a concurrent process cannot double-execute
live work.  The scheduler also sweeps orphaned claims (no in-flight coroutine in
this process) before deadlock detection, which is what prevents "fresh process +
no in-flight work" from being misread as a deadlock.

**Trade-off.** A second engine process really running the same run would be
double-dispatched by a forced resume.  Accepted for v0 (one engine process per
run); the DB is not a coordination service.

## DEC-014 — `max_tasks` is fail-closed

`BudgetManager` derives `tasks_started` from `record(task_id=...)` and treats any
`task_id` that has not consumed a slot as a new dispatch when evaluating
`max_tasks`.  Calling `check()` without a `task_id` therefore applies the worst
case (a new dispatch) instead of skipping the ceiling.  The engine additionally
defaults `max_tasks` to `max_total_tasks` when the operator configured none, so a
misconfigured run degrades to "bounded at the decomposition limit", never to
"unbounded".

## DEC-015 — Per-run contiguous sequence numbers

`events.seq` (the AUTOINCREMENT primary key) is a *global* counter and has gaps
within any single run when runs share a database file.  `Event.seq` is therefore
the per-project `run_seq` (computed inside the same transaction), so each run's
stream is dense `1..N`: consumers that scan seq ranges (replay, resume, gap
detection) never mistake another run's events for lost ones.  A migration backfills
`run_seq` for pre-existing dev databases.

## DEC-016 — Poison is "cannot ever complete", not "is currently failed"

`models.can_never_complete()` defines poison as `QUARANTINED`, `CANCELLED`, or
`FAILED` with the attempt budget exhausted.  A retryable `FAILED` (including the
backoff window between `TASK_FAILED` and `TASK_RETRIED`) must not cancel its
dependents; otherwise the run's outcome would depend on whether a backoff delay was
configured.

## DEC-017 — Cancel is a durable control record; documents stay projections

Cancellation lives in the `control` table (`put_control`/`get_control`), which is
never rebuilt from the log because it is an *input*, not derived state.  The
`documents` table remains strictly event-projected, which keeps `verify_replay`
meaningful; recomputable artefacts (run reports, exports) live in `derived`.  A
cancel requested before a run starts is remembered by the engine and applied at
start, so "cancel early" is not lost.

## DEC-018 — Write safety is alias-aware

Path validation rejects escapes and VCS metadata after resolving the real path
(case-folded, NFC-normalised, so `.GIT/config` is refused on case-insensitive
filesystems).  Hard links are *not* rejected outright — an in-root hard link is a
legitimate repository state — instead the writer breaks the link (unlink + create)
so the task's change lands on a fresh inode and the aliased file elsewhere keeps
its content.  Append mode is preserved on a private copy.

**Residual risk.** TOCTOU between validation and write is not defended against
(a hostile local writer could swap a parent directory for a symlink in the
window); the threat model is a hostile *repository*, not a hostile local process.

## DEC-019 — Billing recovery from transport error text

A provider that was paid for before failing must be billed.  The runtime reads, in
order: structured `exc.usage`, `exc.tokens`, a `billed <N> tokens` report in the
exception message, then the pre-flight estimate.  The text pattern is a pragmatic
last resort because real transports report this way; structured
`ProviderBilledError(usage=...)` is the documented preferred path.  The failure
direction is deliberately conservative: over-billing stops work earlier, while
under-billing is a credential leak (C7).

## DEC-020 — Liveness backstop independent of the supervisor's intervention budget

Supervisor interventions are capped per task (`max_interventions_per_task`,
cooldowns) so a flapping task cannot be intervened forever; that cap must not
become a way to hang the run.  `Supervisor.stuck_task_ids()` reports
no-progress tasks regardless of the cap, and the scheduler aborts them, letting
the attempt budget decide between retry and quarantine.  A provider that never
returns can therefore never leave a run waiting indefinitely.

## DEC-021 — The corpus→review→rework loop reuses artifact identities

Artifact ids are deterministic per `(project, task, path)`, so rework updates one
row instead of accumulating duplicates per attempt, while the event log still
records every `ARTIFACT_PRODUCED` (audit history).  `files_changed` in the report
is the number of distinct paths, not the number of attempts.

## DEC-022 — Admission control covers the completion, and reservations are explicit

The token ceiling is enforced *before* dispatch, but a provider that was admitted
on the prompt estimate can produce a completion that pushes the ledger past the
ceiling.  Two mechanisms close that hole:

* the runtime admits a call against `estimate(prompt) + output_allowance` where
  the allowance is the declared `max_tokens` or (absent one) the prompt estimate
  — i.e. "the completion is assumed to be no larger than the prompt";
* `BudgetManager.reserve()`/`release()` hold the admission estimate while the
  call is in flight, so concurrent calls cannot each pass a check that only sees
  settled usage.

**Trade-off.** A provider whose completion is much larger than its prompt can
still overshoot once (the ceiling is a ceiling on *admitted* work).  Making it a
hard physical cap would require passing `max_tokens=remaining` to every provider
and trusting them to honour it; that is the documented next step for C7.

## DEC-023 — `TASK_CREATED` cannot overwrite, but may seed any non-lifecycle state

A repeated `TASK_CREATED` for an existing id raises `StateError` instead of
silently resetting that task (AC-05 F09, the "terminal task overwritten" half).
The birth *status* is deliberately unrestricted, because the accepted G-A
acceptance fixtures seed tasks with `TASK_CREATED` in the status under test, and
the event log is the trusted, append-only source of truth: *transitions* are
whitelisted, not the arbitrary facts an operator/repair tool may write.

**Trade-off.** A hand-written DONE-born task bypasses the lifecycle table.  The
engine never produces one (planner and decomposer create PENDING tasks), and
rejecting non-PENDING births conflicts with the G-A fixture contract, so this is
a documented boundary rather than a silent hole.
