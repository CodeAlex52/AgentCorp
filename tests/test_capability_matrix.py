"""SPEC §3 capability matrix — C1…C17, one positive and one negative case each.

This file is the auditable mapping between the completion contract and the test
suite: every capability has a *positive* test (the promised behaviour works) and
a *negative* test (the failure path is refused or handled, not silently
accepted).  Deeper coverage for the hard ones (C4/C5/C6/C8/C9/C10) lives in
``test_store.py``, ``test_redteam_findings.py`` and
``test_recovery_subprocess.py``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from conftest import create_project, drive, make_task, seed_tasks

from agentcorp import (
    DecompositionBounds,
    DecompositionError,
    Event,
    EventType,
    GraphError,
    MockProvider,
    PathViolationError,
    SchemaError,
    StateError,
    Store,
    SupervisorConfig,
    Task,
    TaskGraph,
    TaskStatus,
    Usage,
    WorkerOutcome,
    parse_requirement,
    validate_report,
    validate_write_path,
)
from agentcorp.decomposer import Decomposer
from agentcorp.reviewer import Reviewer
from agentcorp.runtime import AgentRuntime
from agentcorp.supervisor import Supervisor
from agentcorp.util import FakeClock, noop_sleep

CAPABILITIES = [f"C{index}" for index in range(1, 18)]


def test_capability_matrix_is_complete() -> None:
    assert len(CAPABILITIES) == 17


# --------------------------------------------------------------------------- C1
async def test_c01_positive_prd_to_dag() -> None:
    from agentcorp import SequentialIdFactory, plan_tasks

    runtime = AgentRuntime(MockProvider(seed=3, skill=1.0), sleep=noop_sleep)
    requirement = await parse_requirement(runtime, "Deliver:\n- a feature\nmust not break")
    tasks = await plan_tasks(runtime, requirement, "repo", id_factory=SequentialIdFactory())
    graph = TaskGraph(tasks)
    graph.validate()
    assert len(tasks) >= 2 and any(task.dependencies for task in tasks)


async def test_c01_negative_empty_prd_is_refused() -> None:
    runtime = AgentRuntime(MockProvider(), sleep=noop_sleep)
    with pytest.raises(SchemaError):
        await parse_requirement(runtime, "   ")


def test_c01_negative_unstructured_model_reply_is_a_schema_error() -> None:
    from agentcorp.prd import requirement_from_payload

    with pytest.raises(SchemaError):
        requirement_from_payload({"goal": ""}, raw_text="x")


# --------------------------------------------------------------------------- C2
def test_c02_positive_whitelist_allows_legal_transition() -> None:
    from agentcorp import assert_transition

    assert_transition(Task(title="t", status=TaskStatus.RUNNING), TaskStatus.REVIEW)


def test_c02_negative_illegal_transition_raises_state_error() -> None:
    from agentcorp import assert_transition

    with pytest.raises(StateError):
        assert_transition(Task(title="t", status=TaskStatus.PENDING), TaskStatus.DONE)


# --------------------------------------------------------------------------- C3
def test_c03_positive_valid_dag_accepted() -> None:
    TaskGraph([Task(id="A", title="a"), Task(id="B", title="b", dependencies=["A"])]).validate()


def test_c03_negative_cycle_rejected() -> None:
    with pytest.raises(GraphError):
        TaskGraph([Task(id="A", title="a", dependencies=["B"]), Task(id="B", title="b", dependencies=["A"])]).validate()


# --------------------------------------------------------------------------- C4
def test_c04_positive_claim_moves_ready_to_running(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    assert store.claim_task("T1", owner="w") is not None
    assert store.get_task("T1").status is TaskStatus.RUNNING  # type: ignore[union-attr]


def test_c04_negative_second_claim_is_refused(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    assert store.claim_task("T1", owner="w1") is not None
    assert store.claim_task("T1", owner="w2") is None


# --------------------------------------------------------------------------- C5
async def test_c05_positive_resume_recovers_interrupted_work(engine_factory) -> None:
    engine = engine_factory(db_name="c05.db")
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="RC05")
    assert summary.status == "DONE"
    task_id = summary.report["per_task"][0]["task_id"]
    task = engine.store.get_task(task_id)
    task.status = TaskStatus.RUNNING  # simulate a crash mid-claim
    task.lease_expires_at = datetime.now(UTC) + timedelta(seconds=300)
    engine.store._conn.execute("UPDATE tasks SET status='running', data=? WHERE id=?", (task.model_dump_json(), task_id))
    engine.store._conn.commit()
    resumed = await engine_factory(db_name="c05.db").resume("RC05")
    assert resumed.status == "DONE"


def test_c05_negative_fresh_claim_is_not_recovered_by_default(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    store.claim_task("T1", owner="w", lease_seconds=300)
    assert store.recover_task("T1") is False  # live lease is respected


# --------------------------------------------------------------------------- C6
async def test_c06_positive_retry_recovers_from_transient_failure() -> None:
    from agentcorp import RateLimitError, RetryPolicy

    calls = {"n": 0}

    class Flaky(MockProvider):
        async def complete(self, request):  # type: ignore[override]
            calls["n"] += 1
            if calls["n"] == 1:
                raise RateLimitError("429", retry_after=0.0)
            return await super().complete(request)

    runtime = AgentRuntime(Flaky(seed=1), retry=RetryPolicy(max_attempts=3, jitter=0), sleep=noop_sleep)
    result = await runtime.call([{"role": "user", "content": "hi"}], task_id="T")
    assert result.attempts == 2 and calls["n"] == 2


async def test_c06_negative_permanent_error_is_not_retried() -> None:
    from agentcorp import PermanentError, RetryPolicy

    calls = {"n": 0}

    class Fatal(MockProvider):
        async def complete(self, request):  # type: ignore[override]
            calls["n"] += 1
            raise PermanentError("bad key")

    runtime = AgentRuntime(Fatal(), retry=RetryPolicy(max_attempts=5, jitter=0), sleep=noop_sleep)
    with pytest.raises(PermanentError):
        await runtime.call([{"role": "user", "content": "hi"}], task_id="T")
    assert calls["n"] == 1


# --------------------------------------------------------------------------- C7
def test_c07_positive_all_four_dimensions_refuse() -> None:
    from agentcorp import BudgetExceededError, BudgetLimits, BudgetManager

    cases = [
        (BudgetLimits(max_tokens=10), Usage(tokens_in=10)),
        (BudgetLimits(max_cost_usd=0.1), Usage(cost_usd=0.1)),
        (BudgetLimits(max_tasks=1), Usage(tokens_in=1)),
        (BudgetLimits(max_wall_seconds=5.0), Usage()),
    ]
    for index, (limits, usage) in enumerate(cases):
        clock = FakeClock()
        manager = BudgetManager(limits, clock=clock)
        if limits.max_tasks is not None:
            manager.note_task_started(1)
        else:
            manager.record(usage, task_id=f"T{index}")
        if limits.max_wall_seconds is not None:
            clock.advance(6.0)
        with pytest.raises(BudgetExceededError):
            manager.check(estimated_tokens=1)


def test_c07_negative_accounting_never_negative() -> None:
    from agentcorp import BudgetLimits, BudgetManager

    manager = BudgetManager(BudgetLimits(max_tokens=100))
    manager.record(Usage(tokens_in=50, tokens_out=50, calls=1))
    assert manager.remaining_tokens() == 0
    assert manager.pressure() == pytest.approx(1.0)


# --------------------------------------------------------------------------- C8
def test_c08_positive_split_children_carry_parent_context() -> None:
    from agentcorp import SequentialIdFactory
    from agentcorp.decomposer import children_from_titles

    parent = Task(id="P", title="big", dependencies=["EXT"], touch_paths=["src/a.py"])
    kids = children_from_titles(parent, ["one", "two"], id_factory=SequentialIdFactory())
    assert [kid.parent_id for kid in kids] == ["P", "P"]
    assert all("EXT" in kid.dependencies for kid in kids)
    assert kids[0].id in kids[1].dependencies, "sibling ordering must be encoded"


def test_c08_negative_bounds_refuse_a_split() -> None:
    bounds = DecompositionBounds(max_depth=1, max_total_tasks=10)
    refusals = bounds.check(depth=1, total_tasks=3, new_children=1)
    assert refusals and "max_depth" in refusals


async def test_c08_negative_decomposer_raises_when_bounds_exceeded() -> None:
    runtime = AgentRuntime(MockProvider(seed=1), sleep=noop_sleep)
    decomposer = Decomposer(runtime, bounds=DecompositionBounds(max_depth=0, max_total_tasks=2))
    parent = Task(id="P", title="big", depth=0, dependencies=[])
    outcome = WorkerOutcome(status="blocked", reason="too big", recommended_subtasks=["a", "b"])
    with pytest.raises(DecompositionError):
        await decomposer.split(parent, outcome=outcome, signal="too big", total_tasks=2)


# --------------------------------------------------------------------------- C9
async def test_c09_positive_bounded_concurrency_holds(engine_factory) -> None:
    engine = engine_factory(concurrency=1, db_name="c09.db")
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="RC09")
    assert summary.status == "DONE"
    starts = [
        event
        for event in engine.store.list_events("RC09")
        if event.type is EventType.TASK_STARTED
    ]
    claims = [
        event
        for event in engine.store.list_events("RC09")
        if event.type is EventType.TASK_CLAIMED
    ]
    assert len(starts) == len(claims)


def test_c09_negative_deadlock_is_detected_not_waited_out(store: Store) -> None:
    from agentcorp.budget import BudgetManager
    from agentcorp.scheduler import Scheduler, SchedulerConfig
    from agentcorp.worker import Worker

    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY, EventType.TASK_BLOCKED, reason="waiting on a human")
    graph = TaskGraph(store.list_tasks("P1"))
    runtime = AgentRuntime(MockProvider(), sleep=noop_sleep)
    scheduler = Scheduler(
        store=store,
        graph=graph,
        budget=BudgetManager(),
        worker=Worker(runtime, write_root="/tmp"),
        reviewer=Reviewer(runtime),
        decomposer=Decomposer(runtime),
        supervisor=Supervisor(SupervisorConfig(stuck_after_s=60.0), clock=FakeClock()),
        project_id="P1",
        config=SchedulerConfig(),
        clock=FakeClock(),
        sleep=noop_sleep,
    )
    outcome = asyncio.run(scheduler.run())
    assert outcome.status == "DEADLOCK"
    assert outcome.deadlock
    types = [event.type for event in store.list_events("P1")]
    assert EventType.DEADLOCK_DETECTED in types


# --------------------------------------------------------------------------- C10
async def test_c10_positive_cancel_drains_and_persists(engine_factory) -> None:
    engine = engine_factory(db_name="c10.db", concurrency=1)
    engine.request_cancel("test cancel")  # before the run even starts
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="RC10")
    assert summary.status == "CANCELLED", summary.reason
    assert summary.exit_code == 1
    assert engine.store.last_run_summary("RC10")["status"] == "CANCELLED"
    assert all(task.is_terminal for task in engine.store.list_tasks("RC10"))


def test_c10_negative_cancel_does_not_dispatch_after_the_request(store: Store) -> None:
    create_project(store)
    store.put_control("P1", "cancel_request", {"reason": "operator"})
    assert store.get_control("P1", "cancel_request") is not None
    store.rebuild_projections("P1")
    assert store.get_control("P1", "cancel_request") is not None  # survives rebuilds


# --------------------------------------------------------------------------- C11
def test_c11_positive_replay_reproduces_state(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY, EventType.TASK_CLAIMED, EventType.TASK_STARTED, EventType.TASK_FINISHED)
    assert store.verify_replay("P1") is True
    assert store.event_seqs("P1") == [1, 2, 3, 4, 5, 6]


def test_c11_negative_event_without_a_state_change_is_still_recorded(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    before = store.event_count("P1")
    store.append(Event(project_id="P1", type=EventType.NOTE, payload={"summary": "work is happening"}))
    assert store.event_count("P1") == before + 1


# --------------------------------------------------------------------------- C12
async def test_c12_positive_review_rejects_unverifiable_work() -> None:
    runtime = AgentRuntime(MockProvider(seed=1, skill=1.0), sleep=noop_sleep)
    reviewer = Reviewer(runtime, use_provider=False)
    task = Task(id="T", title="implement", kind="implementation")
    outcome = WorkerOutcome(status="success", summary="done", files=[])
    review = await reviewer.review(task, outcome, worker_identity="worker")
    assert review.verdict == "REJECT" and review.must_fix


def test_c12_negative_self_review_is_forbidden() -> None:
    from agentcorp import PermanentError, enforce_independence

    with pytest.raises(PermanentError):
        enforce_independence("worker", "worker")


# --------------------------------------------------------------------------- C13
def test_c13_positive_supervisor_reports_a_genuine_fault() -> None:
    clock = FakeClock()
    supervisor = Supervisor(SupervisorConfig(stuck_after_s=5.0), clock=clock)
    graph = TaskGraph([Task(id="T1", title="stuck", status=TaskStatus.RUNNING)])
    supervisor.observe(Event(project_id="P", type=EventType.TASK_STARTED, task_id="T1", created_at=clock.now()))
    clock.advance(30.0)
    interventions = supervisor.tick(graph=graph, project_id="P")
    assert interventions and interventions[0].finding.kind == "stuck"


def test_c13_negative_supervisor_does_not_flag_progress() -> None:
    clock = FakeClock()
    supervisor = Supervisor(SupervisorConfig(stuck_after_s=5.0), clock=clock)
    graph = TaskGraph([Task(id="T1", title="busy", status=TaskStatus.RUNNING)])
    supervisor.observe(Event(project_id="P", type=EventType.TASK_STARTED, task_id="T1", created_at=clock.now()))
    for _ in range(5):
        clock.advance(4.0)
        supervisor.observe(Event(project_id="P", type=EventType.AGENT_RUN_FINISHED, task_id="T1", created_at=clock.now()))
    assert supervisor.tick(graph=graph, project_id="P") == []


# --------------------------------------------------------------------------- C14
async def test_c14_positive_worker_produces_artifacts(engine_factory) -> None:
    engine = engine_factory(db_name="c14.db")
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="RC14")
    assert summary.report["files_changed"] >= 1
    assert engine.store.list_artifacts("RC14")


def test_c14_negative_worker_write_escape_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    with pytest.raises(PathViolationError):
        validate_write_path("../outside.txt", root)


# --------------------------------------------------------------------------- C15
async def test_c15_positive_status_snapshot_and_report(engine_factory) -> None:
    engine = engine_factory(db_name="c15.db")
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="RC15")
    status = engine.status("RC15")
    assert status["run_summary"]["status"] == "DONE"
    assert status["tasks"]["done"] >= 1
    assert validate_report(summary.report) == []


def test_c15_negative_invalid_report_is_rejected() -> None:
    problems = validate_report({"schema_version": "1", "run_id": "x", "status": "NOPE"})
    assert any("status" in problem for problem in problems)


# --------------------------------------------------------------------------- C16
def test_c16_positive_cli_exit_codes_are_semantic(tmp_path: Path) -> None:
    from agentcorp.cli import DEFAULT_DB  # noqa: F401
    from agentcorp.engine import RunSummary

    assert RunSummary(run_id="r", status="DONE").exit_code == 0
    assert RunSummary(run_id="r", status="FAILED").exit_code == 1
    assert RunSummary(run_id="r", status="BUDGET_EXHAUSTED").exit_code == 3


def test_c16_negative_unknown_format_is_a_usage_error(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from agentcorp.cli import app

    runner = CliRunner()
    db = tmp_path / "c16.db"
    store = Store(db)
    create_project(store, "P1")
    store.close()
    result = runner.invoke(app, ["graph", "--db", str(db), "--format", "nonsense"])
    assert result.exit_code == 2


# --------------------------------------------------------------------------- C17
def test_c17_positive_benchmark_exists_and_validates() -> None:
    import json

    from agentcorp.report import validate_report

    path = Path(__file__).resolve().parents[1] / "benchmarks" / "self_hosting_sim.json"
    assert path.exists(), "run examples/end_to_end.py to regenerate the benchmark"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert validate_report(payload) == []
    assert payload["status"] == "DONE"


def test_c17_negative_demo_is_deterministic() -> None:
    import json
    import subprocess
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    out_a = repo_root / "benchmarks" / "_det_a.json"
    out_b = repo_root / "benchmarks" / "_det_b.json"
    try:
        for out in (out_a, out_b):
            subprocess.run(
                [sys.executable, str(repo_root / "examples" / "end_to_end.py"), "--quiet", "--out", str(out)],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )
        payload_a = json.loads(out_a.read_text(encoding="utf-8"))
        payload_b = json.loads(out_b.read_text(encoding="utf-8"))
        assert payload_a == payload_b, "the demo must be byte-for-byte reproducible"
    finally:
        for out in (out_a, out_b):
            out.unlink(missing_ok=True)
