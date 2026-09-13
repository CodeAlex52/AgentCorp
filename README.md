# AgentCorp — Recursive Delivery OS

> `(git repo, natural-language PRD)` → **reviewed, tested delivery** + auditable event ledger + reproducible benchmark report.

AgentCorp is a local-first, event-sourced, crash-recoverable, budget-bounded multi-agent
delivery orchestrator. It is deliberately **offline-verifiable**: deterministic mock and
fault-injection providers let the entire reliability test-suite run without network access
or API keys.

**Status: v0 complete against the `docs/SPEC_v0.md` contract** — 289 offline
deterministic tests, a byte-reproducible benchmark, mypy strict + ruff clean.
`STATUS.md` lists the per-capability evidence and the remaining (documented)
gaps; `docs/ARCHITECTURE.md` explains how the pieces fit.

## Why this exists (design thesis)

Most multi-agent demos show a happy path. AgentCorp is built around the failure paths:

| Concern | Mechanism |
|---|---|
| Duplicate execution | atomic task claim in the store (exactly-once dispatch) |
| Crash recovery | SIGKILL-safe `resume`: DONE tasks never re-run, stale RUNNING tasks re-leased |
| Retry storms | exponential backoff + jitter, attempt caps, circuit breaker, poison-task quarantine |
| Runaway cost | four-dimensional hard budget (tokens / cost / tasks / wall-clock), check-before-dispatch |
| Runaway decomposition | bounded recursive task splitting (depth / total-tasks / budget) |
| Deadlock | explicit detector: no running + no ready + pending > 0 → terminate with event |
| Silent drift | append-only event log with monotonic seq; state is replayable from events |
| Unsupervised agents | independent reviewer (never self-review) + supervisor with false-positive guard |

## Architecture

```
PRD ──▶ planner ──▶ Task DAG ──▶ scheduler ──▶ worker ──▶ reviewer ──▶ integration
                                    │              ▲            │
                                    │              └── rework ──┘
                                    ├── store (events, tasks, leases)   ← resume/crash-safe
                                    ├── budget (hard stop)              ← cost control
                                    ├── reliability (retry, breaker)    ← fault tolerance
                                    ├── chaos (fault injection)         ← adversarial tests
                                    └── supervisor (governance)         ← stuck detection
```

Layering (see `src/agentcorp/__init__.py` for the authoritative map):
domain `models` · facts `events`/`store` · graph algebra `graph` · planning
`prd`/`repo`/`planner`/`decomposer` · execution `scheduler`/`worker`/`reviewer` ·
policy `reliability`/`budget`/`chaos`/`runtime` · governance `supervisor` · facade
`engine`/`cli`.

## 30-second demo

```bash
uv venv .venv && uv pip install -e ".[dev]"

# full test suite: offline, deterministic, ~5s
uv run pytest -q

# deterministic end-to-end run (mock provider, no network, no API key)
#   -> writes benchmarks/self_hosting_sim.json; re-running gives identical bytes
uv run python examples/end_to_end.py
uv run python scripts/validate_benchmark.py benchmarks/self_hosting_sim.json

# the same run through the CLI
uv run agentcorp run --repo examples/demo_repo \
    --prd-text "Deliver: add peek() to taskqueue.py with tests"
uv run agentcorp status
uv run agentcorp graph --format mermaid
uv run agentcorp report
```

Everything below runs without credentials; real providers (`openai`, `deepseek`,
`ollama`, `cli:claude -p`, …) are opt-in via `--provider`.

```bash
# crash a run with kill -9, then resume it: DONE tasks are never re-executed
uv run agentcorp run  ... --provider mock:latency=0.5 &
kill -9 %1
uv run agentcorp resume <run_id>          # recovers RUNNING/REVIEW work

# one command for the whole contract (tests + demo + benchmark + mypy + ruff)
./scripts/reproduce_all.sh
```

## Evidence (what makes this more than a demo)

| Claim | How it is proven |
|---|---|
| exactly-once dispatch | 64 threads race one READY task; exactly one claim wins (`tests/test_store.py`) |
| crash recovery | real `SIGKILL` mid-run, then `resume`; no re-run of DONE tasks, sequence continues (`tests/test_recovery_subprocess.py`) |
| budget hard stops | four ceilings, check-before-dispatch, failed attempts billed, admission reservations (`tests/test_budget.py`, `test_ac05_regressions.py`) |
| retry storms | exponential backoff + jitter, attempt caps, circuit breaker, quarantine (`tests/test_reliability.py`) |
| deadlock | no running/ready but unfinished ⇒ event + terminal status, never a hang |
| runaway decomposition | depth/total/budget bounds, re-checked at insert time; terminates < 5s under a split storm |
| replayability | `verify_replay()` reproduces the projection byte-for-byte after runs, rework and crash recovery |
| no false-positive governance | progress-aware supervisor: a long task that keeps working is never intervened (`test_g_m_*`) |
| path safety | `../`, absolute paths, `.GIT/config` case folding, symlink and hard-link escapes all handled (`test_redteam_findings.py`) |

## Roadmap

- [x] domain models, task graph, event-sourced store, budget, reliability, chaos, providers
- [x] planner / decomposer (PRD → requirements → DAG; bounded recursive split)
- [x] scheduler / worker / reviewer / supervisor / engine facade
- [x] CLI (`run` / `resume` / `status` / `graph` / `report` / `providers` / `cancel`)
- [x] 289 offline tests incl. concurrency, SIGKILL recovery, budget hard-stop, chaos runs
- [x] self-hosting benchmark report (`benchmarks/self_hosting_sim.json`)
- [x] reserve-based token admission (see `docs/DESIGN_DECISIONS.md` DEC-022)
- [ ] provider output cap wired from the remaining budget (C7 residual risk)
- [ ] optional `-m smoke` against a real provider

## Docs

- `docs/SPEC_v0.md` — completion contract (capabilities C1–C17, invariants, schemas)
- `docs/ACCEPTANCE.md` — independent verification contract (how v0 is falsified)
- `docs/ARCHITECTURE.md` — layers, invariants, recovery/cancel semantics
- `docs/BASELINE_AUDIT.md` — per-module audit of the imported prototype (DEF-01…10)
- `docs/DESIGN_DECISIONS.md` — DEC-001…DEC-023 with alternatives and trade-offs
- `docs/report_schema.json` — JSON Schema for the §6 report (for external validators)
- `STATUS.md` — per-capability evidence, RedTeam/AC-05 finding dispositions, known gaps

MIT
