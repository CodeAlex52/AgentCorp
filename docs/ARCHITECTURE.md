# AgentCorp architecture

`(git repo, natural-language PRD)` → **reviewed, tested delivery** + auditable event
ledger + reproducible benchmark report.  This document explains how the pieces fit,
which invariants are enforced where, and why.

```
PRD ──▶ prd.parse ──▶ Requirement ──▶ planner ──▶ Task DAG
                                     repo.analyze ──┘        │
                                                             ▼
                            ┌──────────────────────────  scheduler  ─────────────────────────┐
                            │  promote → claim (atomic) → dispatch ≤ concurrency             │
                            │  worker ──▶ reviewer ──▶ approve/rework                        │
                            │  decomposer (bounded split)      supervisor (stuck/failure)    │
                            └───────┬──────────────────────────────────────────────┬─────────┘
                                    ▼                                              ▼
                          store (append-only events + projections)        budget (4 hard ceilings)
                                    │
                                    ▼
                      report.build ──▶ benchmarks/*.json + `agentcorp report`
```

## Layers

| Layer | Modules | Rules |
|---|---|---|
| domain data | `models`, `errors` | Pydantic models only; the transition whitelist (`ALLOWED_TRANSITIONS`) lives here |
| facts / log | `events`, `store` | events are the source of truth; every table is a projection |
| graph algebra | `graph` | pure functions over `Task` sets; no I/O |
| planning | `prd`, `repo`, `planner`, `decomposer` | model output → domain objects, always validated |
| execution | `worker`, `reviewer` | one task → one provider call chain → validated outcome |
| orchestration | `scheduler` | owns the loop, dispatch, retries, splits, cancel, deadlock |
| policy | `reliability`, `budget`, `chaos`, `runtime` | retries, circuit breakers, ceilings, fault injection |
| governance | `supervisor` | progress-aware anomaly detection, pure |
| facade | `engine`, `cli`, `report` | wiring, run/report lifecycle, JSON output |

## The event ledger

* `events` is append-only with **per-run contiguous `run_seq`** (`Event.seq`).
  A run sharing a DB file with other runs still has a dense `1..N` stream.
* Every state change is one event; the projection for that event is applied in the
  *same transaction* (`Store._insert_event`), so a crash cannot leave the queryable
  state ahead of or behind the log.
* Transitions are validated **twice**: the scheduler checks before emitting and the
  projection checks again while folding, so a hand-crafted or corrupted log fails
  loudly at replay instead of projecting an impossible state.
* `Store.verify_replay(project)` proves that folding the log reproduces the current
  projection byte-for-byte.  A test asserts this after normal runs, after rework and
  after crash recovery.
* `event_id` is idempotent: re-delivering the same fact (at-least-once transport)
  returns the stored event; the same id with different content is a hard error.

## Task lifecycle

```
PENDING ──▶ READY ──▶ RUNNING ──▶ DONE
   │          │          │  └──▶ REVIEW ──▶ DONE        (approved)
   │          │          │        └──────▶ READY        (rework, bounded)
   │          │          ├──▶ SPLIT ──▶ DONE|FAILED     (children aggregated)
   │          │          ├──▶ FAILED ──▶ READY          (retry, attempts<max)
   │          │          │        └────▶ QUARANTINED    (poison)
   │          │          └──▶ CANCELLED                 (stop/budget/deadlock)
   │          └─────────────▶ BLOCKED ──▶ READY|FAILED
   └────────────────────────▶ CANCELLED
```

Terminal statuses are `DONE`, `QUARANTINED`, `CANCELLED`.  `FAILED` is *not*
terminal: it either retries (attempt budget permitting) or is quarantined, and
only an exhausted `FAILED` is poison for its dependents (AC-05 P1-C).

## Exactly-once dispatch and leases

`Store.claim_task` performs `UPDATE tasks SET status='running' WHERE id=? AND
status='ready'` and only the winning transaction writes `TASK_CLAIMED` +
`TASK_STARTED` with `claimed_at`/`lease_expires_at`.  N concurrent claimers ⇒ one
winner (tested with 64 threads).  64 concurrent *processes* are safe too: SQLite's
row lock plus `busy_timeout=5000` serialise them.

## Crash recovery (`resume`)

A crash leaves tasks in `RUNNING` or `REVIEW`.  `resume` treats both as
interrupted attempts and drives them through the legal path
`RUNNING|REVIEW → FAILED → READY` (a crash consumes one attempt; an exhausted
attempt budget quarantines the task instead of looping).

* `--force-requeue` (default) does **not** wait for the 300 s lease to expire:
  resume means "the owning process is gone".  `--no-force-requeue` requires an
  expired lease for operators who want to be conservative.
* The scheduler additionally sweeps for *orphaned* claims (RUNNING/REVIEW with no
  in-flight coroutine in this process) on every loop iteration, before deadlock
  detection; without that, a fresh process would see "no running, no ready" and
  wrongly declare a deadlock, destroying the run (the original P0).
* `DONE` tasks are never re-executed, and `resume` continues the event sequence.

## Budgeting

Four hard ceilings: tokens, USD cost, task dispatches, wall clock
(`BudgetLimits`).  `check()` is evaluated before every dispatch **and** before
every agent call, with three properties:

1. reaching a ceiling (`used >= limit`) refuses the next call;
2. `max_tasks` is fail-closed: any `task_id` that has not consumed a slot counts
   as a new dispatch, and `tasks_started` is derived from `record()` so a caller
   that forgets `note_task_started()` cannot disable the ceiling;
3. failed attempts are billed: usage accumulates across retries, and a call that
   fails without structured usage is charged its pre-flight estimate (a
   `billed N tokens` transport report is honoured — DEC-019);
4. admission covers the completion: a call is checked against
   `prompt estimate + output allowance` and holds a reservation while in flight,
   so concurrent calls cannot sprint past the ceiling together (DEC-022).

The engine also defaults `max_tasks` to the decomposition bound
(`max_total_tasks`, 200) so "no budget configured" never means "unbounded".

## Recursive decomposition

A worker that reports `blocked` (with recommendations) triggers a split, bounded
by `max_depth`, `max_total_tasks` and budget pressure.  Bounds are checked twice:
before the decomposer's provider call and again at insert time against the current
graph, because two splits can be in flight together.  The child DAG plus the
parent's `SPLIT` transition and the dependents' rewiring are inserted in one
transaction; a rejected child DAG fails the parent and emits
`VALIDATION_FAILED`/`CYCLE_DETECTED`.  Once all children are terminal the parent
aggregates to `DONE`, `FAILED` (then quarantined) or `CANCELLED`.

## Review loop

`RUNNING → REVIEW` happens in a distinct `Reviewer` instance with its own runtime
and role; `enforce_independence` refuses self-review.  Deterministic checks
(artefact present for code tasks, reported tests passed, evidence non-empty) can
reject a model's approval, but never the reverse.  Rejections lead to
`TASK_REWORK` (bounded by `max_reworks`) or, when the budget is spent, to
`FAILED` + quarantine.  Rework reuses a deterministic artifact id per
`(project, task, path)`, so the artifact table does not accumulate duplicates
while the event log keeps every attempt.

## Governance (supervisor)

Detection is **progress-based**, not time-based: claim/start, finished agent runs,
artifacts and reviewer activity reset a task's progress clock.  A long task that
keeps making progress is never intervened (the false-positive guard, asserted by
tests); a silent task raises STUCK, repeated failures raise REPEATED_FAILURES,
many failures raise FAILURE_STORM (which escalates and fails the run).  Findings
become `Intervention`s that the scheduler records and applies (SPLIT/CANCEL), with
per-task caps and cooldowns.  When the intervention budget is exhausted the
**liveness backstop** (`Supervisor.stuck_task_ids`) aborts the hung attempt and
lets attempt accounting quarantine it, so a provider that never returns cannot
hang a run.

## Cancellation and shutdown

`agentcorp cancel <run>` writes a durable `cancel_request` control document
(survives projection rebuilds, visible to any process on the DB); the scheduler
polls it every tick, stops dispatching, gives in-flight work `cancel_grace_s` to
finish, then cancels it (`TASK_CANCELLED`), marks every remaining non-terminal
task cancelled and persists `RUN_FINISHED status=CANCELLED`.  SIGINT/SIGTERM use
the same path via a signal handler that defers the request onto the event loop,
so Ctrl-C produces a resumable run instead of a traceback.  Exit codes: 0 DONE,
1 FAILED/CANCELLED/DEADLOCK, 2 usage error, 3 budget exhausted.

## Path safety (worker)

`validate_write_path` rejects absolute paths, `..`, NUL bytes, `~`, VCS metadata
(`.git`/`.hg`/`.svn` compared case-folded and NFC-normalised, so `.GIT/config` is
refused on case-insensitive filesystems), anything that resolves outside the
repository root (symlink escapes), directories and non-regular files.  Hard links
are **broken** on write (unlink + create) instead of being rejected: the task's
change stays in the repository and the file elsewhere keeps its content.

## Test strategy

Everything is offline, deterministic and fast (277 tests, < 5 s): injected
`FakeClock`, `noop_sleep`, `SequentialIdFactory`, mock/scripted providers.  Real
concurrency (64 threads racing for one claim), real faults (chaos provider) and a
real `SIGKILL` + `resume` subprocess test are used where mock assertions would be
insufficient evidence.  `docs/SPEC_v0.md` §7 lists the gates;
`tests/test_capability_matrix.py` maps C1–C17 to positive/negative cases.
