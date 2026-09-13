# AgentCorp — Recursive Delivery OS

> `(git repo, natural-language PRD)` → **reviewed, tested delivery** + auditable event ledger + reproducible benchmark report.

AgentCorp is a local-first, event-sourced, crash-recoverable, budget-bounded multi-agent
delivery orchestrator. It is deliberately **offline-verifiable**: deterministic mock and
fault-injection providers let the entire reliability test-suite run without network access
or API keys.

**Status: v0 in active development** (see `STATUS.md` and `docs/SPEC_v0.md` for the
completion contract and current gaps). Baseline imported from an unverified prototype on
2026-09-14; engine/CLI/tests are being completed in this repo.

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

## Quickstart

```bash
uv venv .venv && uv pip install -e ".[dev]"

# deterministic end-to-end demo (no network, no API key)
uv run python examples/end_to_end.py

# full test suite (offline)
uv run pytest -q
```

## Roadmap

- [x] domain models, task graph, event-sourced store, budget, reliability, chaos, providers
- [ ] planner / decomposer (PRD → requirements → DAG; bounded recursive split)
- [ ] scheduler / worker / reviewer / supervisor / engine facade
- [ ] CLI (`run` / `resume` / `status` / `graph` / `report`)
- [ ] ≥150 offline tests incl. concurrency, SIGKILL recovery, budget hard-stop, chaos runs
- [ ] self-hosting benchmark report (`benchmarks/self_hosting_sim.json`)

## Docs

- `docs/SPEC_v0.md` — completion contract (capabilities C1–C17, invariants, schemas)
- `docs/ACCEPTANCE.md` — independent verification contract (how v0 is falsified)
- `docs/ARCHITECTURE.md`, `docs/DESIGN_DECISIONS.md` — (added with v0 completion)

MIT
