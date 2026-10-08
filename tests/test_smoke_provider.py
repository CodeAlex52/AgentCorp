"""Opt-in real-provider smoke: one tiny delivery through a live HTTP provider.

Skipped unless ``AGENTCORP_SMOKE_PROVIDER`` names a provider (and its
credentials are present), e.g.::

    AGENTCORP_SMOKE_PROVIDER=openai uv run pytest -m smoke -s

The test mirrors the ``agentcorp run`` path (same ``Engine`` / ``EngineConfig``
construction), runs it against a throwaway copy of ``examples/demo_repo``, then
asserts the run finished ``DONE`` with a replayable ledger and prints the run
metrics (status, tasks, tokens, cost, wall time) for the record.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

import pytest

from agentcorp import Engine, EngineConfig, PermanentError
from agentcorp.providers.registry import build_provider

REPO_ROOT = Path(__file__).resolve().parents[1]

PROVIDER = os.environ.get("AGENTCORP_SMOKE_PROVIDER", "").strip()

PRD = """\
Deliver a small improvement to the task queue demo repository.

- add a `peek()` accessor with unit tests
"""

pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(
        not PROVIDER,
        reason="opt-in real-provider smoke: set AGENTCORP_SMOKE_PROVIDER=openai|deepseek|...",
    ),
]


def test_real_provider_end_to_end(tmp_path: Path) -> None:
    try:
        provider = build_provider(PROVIDER)
    except PermanentError as exc:  # credentials missing / unknown spec
        pytest.skip(f"provider not ready: {exc}")

    repo = tmp_path / "repo"
    shutil.copytree(REPO_ROOT / "examples" / "demo_repo", repo)
    engine = Engine(
        EngineConfig(
            repo_path=str(repo),
            provider_spec=PROVIDER,
            db_path=str(tmp_path / "smoke.db"),
        ),
        provider=provider,
    )
    try:
        summary = asyncio.run(engine.run_prd(PRD))
        replay_ok = engine.store.verify_replay(summary.run_id)
    finally:
        asyncio.run(engine.aclose())

    assert summary.status == "DONE", f"smoke run finished {summary.status}: {summary.reason}"
    assert replay_ok, "ledger replay diverged after a real-provider run"
    tasks = summary.report.get("tasks", [])
    metrics = {
        "provider": PROVIDER,
        "status": summary.status,
        "tasks": len(tasks) if isinstance(tasks, list) else tasks,
        "tokens": summary.report.get("tokens"),
        "cost_usd": summary.report.get("cost_usd"),
        "wall_ms": summary.report.get("wall_ms"),
    }
    print(f"\nsmoke metrics: {metrics}")
