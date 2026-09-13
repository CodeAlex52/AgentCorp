#!/usr/bin/env python
"""Deterministic end-to-end demo + benchmark generator (SPEC C17).

Runs the real engine (not a stub) against ``examples/demo_repo`` with the mock
provider, a deterministic clock, no-op sleeps and a sequential id factory, then
writes ``benchmarks/self_hosting_sim.json``.  Re-running this script produces a
byte-identical report (asserted by ``tests/test_examples_demo.py``), which is
what makes the benchmark usable as a regression signal.

Usage::

    uv run python examples/end_to_end.py
    uv run python examples/end_to_end.py --out benchmarks/self_hosting_sim.json
    uv run python examples/end_to_end.py --chaos 0.2 --out benchmarks/chaos_sim.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:  # allow running from a plain checkout
    sys.path.insert(0, str(REPO_ROOT / "src"))

from agentcorp import (  # noqa: E402
    BudgetLimits,
    DecompositionBounds,
    Engine,
    EngineConfig,
    MockProvider,
    RunSummary,
    SchedulerConfig,
    SequentialIdFactory,
)
from agentcorp.report import validate_report  # noqa: E402
from agentcorp.util import Clock, noop_sleep  # noqa: E402

DEMO_REPO = REPO_ROOT / "examples" / "demo_repo"
DEFAULT_OUT = REPO_ROOT / "benchmarks" / "self_hosting_sim.json"

PRD = """\
Deliver a reliability improvement to the task queue demo repository.

- add a `peek()` accessor to `src/taskqueue.py` with unit tests
- document the queue's failure modes in `docs/guide.md`
- keep the existing FIFO semantics and the bounded capacity contract intact
"""


class DeterministicClock(Clock):
    """A clock that advances a fixed step on every read.

    The demo must not depend on wall time (otherwise the benchmark cannot be
    reproduced byte-for-byte), but a report full of zeros would hide timing
    regressions.  A fixed increment keeps both properties.
    """

    def __init__(self, *, start: datetime | None = None, step_s: float = 0.01) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=UTC)
        self._monotonic = 1_000_000.0
        self._step = step_s

    def now(self) -> datetime:
        self._now = self._now + timedelta(seconds=self._step)
        return self._now

    def monotonic(self) -> float:
        self._monotonic += self._step
        return self._monotonic


def build_engine(
    *,
    db_path: Path,
    chaos: float = 0.0,
    seed: int = 7,
    max_concurrency: int = 3,
) -> Engine:
    from agentcorp import ChaosConfig

    config = EngineConfig(
        repo_path=str(DEMO_REPO),
        provider_spec="mock",
        db_path=str(db_path),
        prd_mode="provider",
        budget=BudgetLimits(max_tokens=200_000, max_tasks=40),
        scheduler=SchedulerConfig(
            max_concurrency=max_concurrency,
            tick_interval_s=0.01,
            review=True,
            max_reworks=2,
        ),
        bounds=DecompositionBounds(max_depth=2, max_total_tasks=40),
        chaos=ChaosConfig(enabled=chaos > 0, probability=chaos, seed=seed) if chaos > 0 else None,
        deterministic=True,
    )
    return Engine(
        config,
        provider=MockProvider(seed=seed, skill=1.0),
        clock=DeterministicClock(),
        sleep=noop_sleep,
        id_factory=SequentialIdFactory(),
    )


async def run_demo(
    *,
    db_path: Path,
    chaos: float = 0.0,
    seed: int = 7,
    run_id: str = "RUN-SELFHOST",
) -> RunSummary:
    engine = build_engine(db_path=db_path, chaos=chaos, seed=seed)
    try:
        summary = await engine.run_prd(PRD, run_id=run_id, run_name="self_hosting_sim")
    finally:
        await engine.aclose()
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="report path")
    parser.add_argument("--db", type=Path, default=None, help="workspace DB (temp by default)")
    parser.add_argument("--chaos", type=float, default=0.0, help="fault-injection probability")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    db_path = args.db or (REPO_ROOT / "benchmarks" / ".self_hosting_sim.db")
    if db_path.exists():
        db_path.unlink()
    summary = asyncio.run(run_demo(db_path=db_path, chaos=args.chaos, seed=args.seed))

    problems = validate_report(summary.report)
    if problems:
        for problem in problems:
            print(f"INVALID REPORT: {problem}", file=sys.stderr)
        return 1

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(summary.report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if not args.quiet:
        report = summary.report
        print(f"run {summary.run_id}: {summary.status} ({summary.reason or 'ok'})")
        print(
            f"  tasks: {report['tasks']['done']}/{report['tasks']['total']} done, "
            f"success_rate={report['success_rate']}, depth={report['tasks']['max_depth']}"
        )
        print(
            f"  agent_calls={report['agent_calls']} tokens={report['tokens']['total']} "
            f"pressure={report['tokens']['pressure']} cost=${report['cost_usd']}"
        )
        print(
            f"  reviews={report['reviews']} retries={report['retries']['total']} "
            f"interventions={report['interventions']['total']} events={report['events_count']}"
        )
        print(f"  report written to {args.out}")
    if db_path.exists() and args.db is None:
        # Keep the workspace tidy; the report is the artefact.
        db_path.unlink()
        for suffix in ("-wal", "-shm"):
            stale = db_path.with_name(db_path.name + suffix)
            if stale.exists():
                stale.unlink()
    return 0 if summary.status == "DONE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
