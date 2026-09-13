"""AC-05 architecture-review regressions (the repro suite in WS02/repro).

Each test mirrors one failure the independent review demonstrated on `7cc976f2`,
so the fixes stay fixed without reading the reviewer's harness.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from conftest import create_project, seed_tasks

from agentcorp import (
    BudgetLimits,
    Event,
    EventType,
    MockProvider,
    StateError,
    Store,
    Task,
    TaskGraph,
    TaskStatus,
    Worker,
    WorkerOutcome,
)
from agentcorp.budget import BudgetManager
from agentcorp.decomposer import Decomposer
from agentcorp.providers.mock import ScriptedProvider
from agentcorp.reviewer import Reviewer
from agentcorp.runtime import AgentRuntime
from agentcorp.scheduler import Scheduler, SchedulerConfig
from agentcorp.supervisor import Supervisor
from agentcorp.util import FakeClock, noop_sleep

# ---------------------------------------------------------------------------
# P0-B — a cancelled in-flight future must not crash the scheduler
# ---------------------------------------------------------------------------


async def test_ac05_wait_any_survives_a_cancelled_inflight_future(tmp_path: Path) -> None:
    db = tmp_path / "wait.db"
    store = Store(db)
    try:
        create_project(store)
        seed_tasks(store, "P", [Task(id="T1", title="t")])
        from conftest import drive

        drive(store, "P", "T1", EventType.TASK_READY)
        runtime = AgentRuntime(MockProvider(), sleep=noop_sleep)
        scheduler = Scheduler(
            store=store,
            graph=TaskGraph(store.list_tasks("P")),
            budget=BudgetManager(),
            worker=Worker(runtime, write_root=str(tmp_path)),
            reviewer=Reviewer(runtime),
            decomposer=Decomposer(runtime),
            supervisor=Supervisor(),
            project_id="P",
            config=SchedulerConfig(),
            clock=FakeClock(),
            sleep=noop_sleep,
        )

        async def hung() -> None:
            await asyncio.sleep(3600)

        fut = asyncio.get_running_loop().create_task(hung())
        await asyncio.sleep(0)
        scheduler._inflight["T1"] = fut  # type: ignore[attr-defined]
        fut.cancel()  # exactly what _maybe_abort_inflight does
        await asyncio.sleep(0)
        await scheduler._wait_any()  # must return normally
        assert scheduler._inflight == {}  # type: ignore[attr-defined]
    finally:
        store.close()


async def test_ac05_sigint_style_cancel_persists_a_terminal_status(tmp_path: Path) -> None:
    from conftest import EngineFactory  # noqa: F401  (typing only)

    from agentcorp import Engine, EngineConfig, SchedulerConfig

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='toy'\n", encoding="utf-8")

    released = asyncio.Event()

    class Slow(MockProvider):
        async def complete(self, request):  # type: ignore[override]
            if "Implement the change" in request.text():
                await asyncio.wait_for(released.wait(), timeout=10)
            return await super().complete(request)

    engine = Engine(
        EngineConfig(
            repo_path=str(repo),
            db_path=str(tmp_path / "sig.db"),
            scheduler=SchedulerConfig(max_concurrency=1, tick_interval_s=0.02, cancel_grace_s=0.1),
        ),
        provider=Slow(seed=7, skill=1.0),
    )
    task = asyncio.ensure_future(engine.run_prd("Deliver a thing", run_id="RUN-SIG"))
    for _ in range(200):
        await asyncio.sleep(0.01)
        states = [t.status for t in engine.store.list_tasks("RUN-SIG")]
        if TaskStatus.RUNNING in states:
            break
    engine.request_cancel("simulated SIGINT")
    released.set()
    summary = await asyncio.wait_for(task, timeout=15)
    assert summary.status == "CANCELLED"
    assert engine.store.last_run_summary("RUN-SIG")["status"] == "CANCELLED"
    statuses = {t.status for t in engine.store.list_tasks("RUN-SIG")}
    assert statuses <= {TaskStatus.DONE, TaskStatus.CANCELLED, TaskStatus.FAILED, TaskStatus.QUARANTINED}
    await engine.aclose()


# ---------------------------------------------------------------------------
# P1-C — transient FAILED must not poison dependents
# ---------------------------------------------------------------------------


async def test_ac05_retry_backoff_does_not_cancel_dependents(engine_factory) -> None:
    """Same failure with and without a backoff delay must give the same outcome."""
    from agentcorp import TransientError

    class FlakyOnce(MockProvider):
        """Every worker call fails exactly once, then the provider behaves."""

        def __init__(self) -> None:
            super().__init__(seed=13, skill=1.0)
            self.failed: set[str] = set()

        async def complete(self, request):  # type: ignore[override]
            key = request.task_id or "?"
            if request.role == "worker" and key not in self.failed:
                self.failed.add(key)
                raise TransientError("one-off failure")
            return await super().complete(request)

    outcomes = []
    for index, delay in enumerate((0.0, 0.2)):
        engine = engine_factory(
            skill=1.0,
            seed=13,
            db_name=f"backoff-{index}.db",
            task_retry_base_delay=delay,
            max_reworks=0,
            provider=FlakyOnce(),
        )
        summary = await engine.run_prd("Deliver:\n- a feature", run_id=f"RB{index}")
        outcomes.append(summary.status)
        statuses = {t.status for t in engine.store.list_tasks(f"RB{index}")}
        assert TaskStatus.CANCELLED not in statuses, (
            f"delay={delay}: dependents were cancelled during the retry backoff window"
        )
    assert outcomes[0] == outcomes[1], f"outcome depends on the backoff delay: {outcomes}"


# ---------------------------------------------------------------------------
# AC-05 P2 regressions
# ---------------------------------------------------------------------------


def test_ac05_cancel_request_via_put_document_survives_rebuild(store: Store) -> None:
    create_project(store)
    store.put_document("P1", "cancel_request", {"reason": "operator pressed Ctrl-C"})
    assert store.get_document("P1", "cancel_request") is not None
    store.rebuild_projections("P1")
    assert store.get_document("P1", "cancel_request") == {"reason": "operator pressed Ctrl-C"}


async def test_ac05_stale_cancel_does_not_rewrite_a_finished_run(engine_factory) -> None:
    """AC-05 F11: a cancel latched after completion must not resurrect the run."""
    engine = engine_factory(db_name="stale.db")
    first = await engine.run_prd("Deliver:\n- a feature", run_id="RUN-L")
    assert first.status == "DONE"
    before = dict(engine.store.get_document("RUN-L", "run_summary") or {})
    # Operator pressed cancel after the run had already finished.
    engine.store.put_document("RUN-L", "cancel_request", {"reason": "operator"})
    resumed = await engine_factory(db_name="stale.db").resume("RUN-L")
    after = engine.store.get_document("RUN-L", "run_summary") or {}
    assert resumed.status == "DONE"
    assert after.get("status") == before.get("status") == "DONE"
    assert engine.store.get_control("RUN-L", "cancel_request") is None  # stale signal dropped


def test_ac05_task_created_cannot_overwrite_an_existing_task(store: Store) -> None:
    """F09 (partial): a repeated creation cannot reset a task's lifecycle.

    Seeding a task in an arbitrary status is a deliberate, documented escape
    hatch (DEC-023) because the accepted G-A fixtures rely on it; *overwriting*
    an existing task is not.
    """
    create_project(store)
    store.append(
        Event(
            project_id="P1",
            type=EventType.TASK_CREATED,
            task_id="T-ok",
            payload={"task": Task(id="T-ok", title="ok").model_dump(mode="json")},
        )
    )
    from conftest import drive

    drive(store, "P1", "T-ok", EventType.TASK_READY)
    with pytest.raises(StateError, match="already exists"):
        store.append(
            Event(
                project_id="P1",
                type=EventType.TASK_CREATED,
                task_id="T-ok",
                payload={"task": Task(id="T-ok", title="again", status=TaskStatus.RUNNING).model_dump(mode="json")},
            )
        )
    assert store.get_task("T-ok").status is TaskStatus.READY  # type: ignore[union-attr]
    # Illegal transitions remain illegal regardless of how the task was born.
    with pytest.raises(StateError):
        drive(store, "P1", "T-ok", EventType.TASK_FINISHED)


async def test_ac05_concurrent_splits_respect_the_task_bound(tmp_path: Path) -> None:
    """Two splits in flight together must not breach max_total_tasks."""
    from agentcorp import Engine, EngineConfig

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='toy'\n", encoding="utf-8")

    engine = Engine(
        EngineConfig(repo_path=str(repo), db_path=str(tmp_path / "split.db"), max_total_tasks_override=5),
        provider=MockProvider(seed=7, skill=0.0, latency=0.01),
        clock=FakeClock(),
        sleep=noop_sleep,
    )
    summary = await engine.run_prd("Deliver two things", run_id="RUN-SPLIT")
    tasks = engine.store.list_tasks("RUN-SPLIT")
    assert len(tasks) <= 5, f"max_total_tasks breached: {len(tasks)} tasks ({summary.status})"
    await engine.aclose()


async def test_ac05_rework_does_not_duplicate_artifact_rows(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    reply = '{"status":"success","summary":"v1","files":[{"path":"out/a.py","content":"x=1"}]}'
    runtime = AgentRuntime(ScriptedProvider([reply, reply]), sleep=noop_sleep)
    worker = Worker(runtime, write_root=repo, project_id="P", strict_touch_paths=False)
    task = Task(id="T", title="write out/a.py", touch_paths=["out"])
    result = await worker.execute(task)
    assert [a.path for a in result.artifacts] == ["out/a.py"]
    # The same logical write in a later attempt must reuse the artifact id.
    result2 = await worker.execute(task)
    assert result2.artifacts[0].id == result.artifacts[0].id


def test_ac05_artifact_projection_dedupes_by_path(store: Store) -> None:
    from agentcorp import Artifact

    create_project(store)
    seed_tasks(store, "P1", [Task(id="T1", title="t")])
    for content in ("v1", "v2"):
        artifact = Artifact(
            id="ART-deterministic",
            project_id="P1",
            task_id="T1",
            path="out/a.py",
            content=content,
        )
        store.append(
            Event(
                project_id="P1",
                type=EventType.ARTIFACT_PRODUCED,
                task_id="T1",
                payload={"artifact": artifact.model_dump(mode="json")},
            )
        )
    rows = store.list_artifacts("P1")
    assert [row.path for row in rows] == ["out/a.py"]
    assert rows[0].content == "v2"  # latest attempt wins; the log keeps both facts


async def test_ac05_token_ceiling_cannot_be_overshot_by_a_run(tmp_path: Path) -> None:
    """AC-05 F04: end-to-end the ledger must never exceed max_tokens."""
    from agentcorp import Engine, EngineConfig, SchedulerConfig

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='toy'\ndependencies=['pydantic']\n", encoding="utf-8")
    for limit in (1500, 1300):
        engine = Engine(
            EngineConfig(
                repo_path=str(repo),
                db_path=str(tmp_path / f"tok-{limit}.db"),
                budget=BudgetLimits(max_tokens=limit),
                scheduler=SchedulerConfig(max_concurrency=1, tick_interval_s=0.01),
            ),
            provider=MockProvider(seed=7, skill=1.0),
            clock=FakeClock(),
            sleep=noop_sleep,
        )
        summary = await engine.run_prd("Build it", run_id=f"RUN-{limit}")
        usage = engine.store.usage_for_project(f"RUN-{limit}")
        await engine.aclose()
        assert usage.total_tokens <= limit, (
            f"max_tokens={limit} but the run consumed {usage.total_tokens} "
            f"({usage.total_tokens - limit:+d}) with status={summary.status}"
        )


async def test_ac05_hung_provider_cannot_hang_the_run(engine_factory) -> None:
    class AlwaysHanging(MockProvider):
        async def complete(self, request):  # type: ignore[override]
            if "CONTRACT: worker" in request.system:
                await asyncio.sleep(3600)
            return await super().complete(request)

    from agentcorp.supervisor import SupervisorConfig

    engine = engine_factory(
        provider=AlwaysHanging(seed=7, skill=1.0),
        db_name="liveness.db",
        concurrency=2,
        max_depth=1,
        max_total_tasks=12,
        max_attempts=1,
        tick=0.01,
        real_time=True,
        supervisor=SupervisorConfig(stuck_after_s=0.05, intervention_cooldown_s=0.0),
    )
    summary = await asyncio.wait_for(
        engine.run_prd("Build it", run_id="RUN-HANG"), timeout=10.0
    )
    assert summary.status in {"FAILED", "DEADLOCK", "BUDGET_EXHAUSTED", "CANCELLED"}
    assert all(task.is_terminal for task in engine.store.list_tasks("RUN-HANG"))


def test_ac05_supervisor_stuck_ids_ignores_progress() -> None:
    clock = FakeClock()
    supervisor = Supervisor(__import__("agentcorp").SupervisorConfig(stuck_after_s=5.0), clock=clock)
    graph = TaskGraph([Task(id="T1", title="t", status=TaskStatus.RUNNING)])
    supervisor.observe(Event(project_id="P", type=EventType.TASK_STARTED, task_id="T1", created_at=clock.now()))
    clock.advance(6.0)
    supervisor.observe(Event(project_id="P", type=EventType.AGENT_RUN_FINISHED, task_id="T1", created_at=clock.now()))
    assert supervisor.stuck_task_ids(graph) == []  # progress vetoes the naive rule
    clock.advance(6.0)
    assert supervisor.stuck_task_ids(graph) == ["T1"]


def test_ac05_worker_outcome_helper_is_importable() -> None:
    outcome = WorkerOutcome(status="success", summary="ok")
    assert outcome.reason is None
    assert Decomposer and Scheduler  # imported for the API surface assertions above
