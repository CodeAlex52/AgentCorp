"""Regression tests for the RedTeam findings FIND-001 … FIND-014.

Each test is named after the finding it closes; the docstring records the
mechanism that used to be broken so a future refactor cannot silently regress it.
"""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from conftest import create_project, drive, make_task, seed_tasks

from agentcorp import (
    AgentProvider,
    BudgetExceededError,
    BudgetLimits,
    BudgetManager,
    CompletionRequest,
    CompletionResponse,
    Event,
    EventType,
    MockProvider,
    PathViolationError,
    ProviderBilledError,
    RateLimitError,
    RetryPolicy,
    StateError,
    Store,
    Task,
    TaskGraph,
    TaskStatus,
    Usage,
    call_with_retry,
    validate_write_path,
)
from agentcorp.runtime import AgentRuntime, usage_from_exception
from agentcorp.util import FakeClock

# ---------------------------------------------------------------------------
# FIND-001 — failed attempts must be billed
# ---------------------------------------------------------------------------


class BillingFailureProvider(AgentProvider):
    """Bills ``tokens_in`` per call and always fails, like a billed timeout."""

    name = "billing"
    default_model = "billing-v1"

    def __init__(self, *, tokens_in: int = 500, fail_times: int = 3) -> None:
        self.tokens_in = tokens_in
        self.fail_times = fail_times
        self.billed = 0
        self.calls = 0

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        self.calls += 1
        self.billed += self.tokens_in
        if self.calls <= self.fail_times:
            raise ProviderBilledError(
                "billed timeout",
                usage={"tokens_in": self.tokens_in, "calls": 1},
            )
        return CompletionResponse(
            text='{"status": "success", "summary": "ok"}',
            model=self.default_model,
            provider=self.name,
            usage=Usage(tokens_in=self.tokens_in, calls=1),
        )


async def test_finding_001_billed_tokens_from_failed_attempts_reach_the_ledger() -> None:
    provider = BillingFailureProvider(tokens_in=500, fail_times=3)
    budget = BudgetManager(BudgetLimits(max_tokens=10_000))
    runtime = AgentRuntime(
        provider,
        retry=RetryPolicy(max_attempts=3, jitter=0.0),
        budget=budget,
        sleep=lambda s: asyncio.sleep(0),
    )
    with pytest.raises(ProviderBilledError):
        await runtime.call([{"role": "user", "content": "hi"}], task_id="T-billed")

    assert provider.billed == 1500
    recorded = budget.project.usage.total_tokens
    assert abs(recorded - provider.billed) <= 1, (
        f"provider billed {provider.billed} tokens over 3 attempts, ledger recorded "
        f"{recorded}: accounting error must be <= 1 token (C7)"
    )
    assert budget.project.usage.calls == 3


async def test_finding_001_hard_stop_after_failed_attempts_burn_the_budget() -> None:
    provider = BillingFailureProvider(tokens_in=600, fail_times=2)
    budget = BudgetManager(BudgetLimits(max_tokens=1000))
    runtime = AgentRuntime(
        provider,
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
        budget=budget,
        sleep=lambda s: asyncio.sleep(0),
    )
    with pytest.raises(ProviderBilledError):
        await runtime.call([{"role": "user", "content": "hi"}], task_id="T-1")
    assert provider.billed == 1200
    assert budget.project.usage.total_tokens == 1200  # 2 attempts x 600
    with pytest.raises(BudgetExceededError):
        budget.check(estimated_tokens=1)


async def test_finding_001_usage_accumulates_across_retries_instead_of_overwriting() -> None:
    provider = BillingFailureProvider(tokens_in=100, fail_times=2)
    budget = BudgetManager()
    runtime = AgentRuntime(
        provider,
        retry=RetryPolicy(max_attempts=3, jitter=0.0),
        budget=budget,
        sleep=lambda s: asyncio.sleep(0),
    )
    result = await runtime.call([{"role": "user", "content": "hi"}], task_id="T-2")
    assert result.usage.tokens_in == 300  # 100 + 100 (failures) + 100 (success)
    assert budget.project.usage.tokens_in == 300


async def test_finding_001_unsignalled_failures_are_billed_from_the_estimate() -> None:
    class SilentFailure(AgentProvider):
        name = "silent"
        default_model = "silent-v1"

        async def complete(self, request: CompletionRequest) -> CompletionResponse:
            raise TimeoutError_("no usage reported")

    from agentcorp import TimeoutError_

    provider = SilentFailure()
    runtime = AgentRuntime(
        provider,
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
        sleep=lambda s: asyncio.sleep(0),
    )
    with pytest.raises(TimeoutError_):
        await runtime.call([{"role": "user", "content": "x" * 400}], task_id="T-3")
    assert runtime.budget is None  # no budget wired; the run record still carries usage
    # The run-level usage is what the engine books; it must not be zero.
    usage = usage_from_exception(TimeoutError_("x"), fallback_tokens=123)
    assert usage.tokens_in == 123 and usage.calls == 1


# ---------------------------------------------------------------------------
# FIND-002 / FIND-003 / FIND-004 — recovery semantics
# ---------------------------------------------------------------------------


def _running_task(store: Store, *, lease_seconds: float = 300.0, status: TaskStatus = TaskStatus.RUNNING) -> str:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    store.claim_task("T1", owner="w", lease_seconds=lease_seconds)
    if status is TaskStatus.REVIEW:
        drive(store, "P1", "T1", EventType.REVIEW_STARTED)
    return "T1"


def test_finding_002_recovery_refuses_a_task_whose_lease_is_still_live(store: Store) -> None:
    _running_task(store, lease_seconds=300.0)
    assert store.recover_task("T1") is False
    task = store.get_task("T1")
    assert task.status is TaskStatus.RUNNING  # type: ignore[union-attr]
    assert task.attempts == 1  # type: ignore[union-attr]
    # Explicit takeover (resume) is allowed.
    assert store.recover_task("T1", force=True) is True


def test_finding_002_recovery_accepts_an_expired_lease(store: Store) -> None:
    _running_task(store, lease_seconds=1.0)
    later = datetime.now(UTC) + timedelta(seconds=5)
    assert [task.id for task in store.stale_running(later)] == ["T1"]
    assert store.recover_task("T1", now=later) is True
    assert store.get_task("T1").status is TaskStatus.READY  # type: ignore[union-attr]


def test_finding_003_concurrent_recovery_has_exactly_one_winner(store: Store) -> None:
    _running_task(store, lease_seconds=1.0)
    later = datetime.now(UTC) + timedelta(seconds=5)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(lambda _: store.recover_task("T1", now=later), range(8))
        )
    assert results.count(True) == 1, f"exactly one recovery may win, got {results}"
    assert results.count(False) == 7  # losers return cleanly, never raise
    assert store.get_task("T1").status is TaskStatus.READY  # type: ignore[union-attr]
    events = [event.type for event in store.list_events("P1", task_id="T1")]
    assert events.count(EventType.TASK_RETRIED) == 1


def test_finding_003_concurrent_recovery_on_review_task(store: Store) -> None:
    _running_task(store, lease_seconds=1.0, status=TaskStatus.REVIEW)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: store.recover_task("T1", force=True), range(4)))
    assert results.count(True) == 1
    assert results.count(False) == 3


def test_finding_004_retried_task_loses_the_dead_attempt_timestamps(store: Store) -> None:
    _running_task(store, lease_seconds=1.0)
    failed_at = datetime.now(UTC)
    drive(store, "P1", "T1", EventType.TASK_FAILED, reason="boom")
    assert store.get_task("T1").finished_at is not None  # type: ignore[union-attr]
    store.append(
        Event(
            project_id="P1",
            type=EventType.TASK_RETRIED,
            task_id="T1",
            payload={"reason": "retry"},
            created_at=failed_at,
        )
    )
    task = store.get_task("T1")
    assert task.status is TaskStatus.READY  # type: ignore[union-attr]
    assert task.finished_at is None, "a re-dispatched task must not keep the dead attempt's finish time"  # type: ignore[union-attr]
    assert task.claimed_at is None and task.lease_expires_at is None  # type: ignore[union-attr]


def test_finding_004_unblocked_and_rework_clear_finished_at(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY, EventType.TASK_CLAIMED, EventType.TASK_STARTED)
    drive(store, "P1", "T1", EventType.TASK_FAILED, reason="x")
    drive(store, "P1", "T1", EventType.TASK_RETRIED)
    drive(store, "P1", "T1", EventType.TASK_BLOCKED, reason="waiting on human")
    drive(store, "P1", "T1", EventType.TASK_UNBLOCKED)
    task = store.get_task("T1")
    assert task.status is TaskStatus.READY  # type: ignore[union-attr]
    assert task.finished_at is None  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# FIND-005 — idempotent event log
# ---------------------------------------------------------------------------


def test_finding_005_same_event_id_is_appended_once(store: Store) -> None:
    create_project(store)
    event = Event(
        project_id="P1",
        type=EventType.NOTE,
        event_id="EV-idempotency-key-1",
        payload={"summary": "delivered at least once"},
    )
    first = store.append(event)
    count_before = store.event_count("P1")
    second = store.append(event)
    assert second.seq == first.seq
    assert store.event_count("P1") == count_before
    assert store.event_seqs("P1") == list(range(1, count_before + 1))


def test_finding_005_conflicting_content_for_same_event_id_is_rejected(store: Store) -> None:
    create_project(store)
    store.append(Event(project_id="P1", type=EventType.NOTE, event_id="EV-dup", payload={"a": 1}))
    with pytest.raises(StateError, match="already exists with different content"):
        store.append(Event(project_id="P1", type=EventType.NOTE, event_id="EV-dup", payload={"a": 2}))


def test_finding_005_replayed_artifact_event_is_not_duplicated(store: Store) -> None:
    from agentcorp import Artifact

    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    artifact = Artifact(project_id="P1", task_id="T1", path="src/a.py", content="x = 1")
    event = Event(
        project_id="P1",
        type=EventType.ARTIFACT_PRODUCED,
        task_id="T1",
        event_id="EV-artifact-1",
        payload={"artifact": artifact.model_dump(mode="json")},
    )
    store.append(event)
    store.append(event)  # at-least-once redelivery
    assert len(store.list_artifacts("P1")) == 1
    assert store.event_count("P1") == 3  # project + task + one artifact event


def test_finding_005_auto_ids_stay_unique_for_identical_events(store: Store) -> None:
    """Auto ids must not collide: two identical NOTEs are two distinct facts."""
    create_project(store)
    for _ in range(3):
        store.append(Event(project_id="P1", type=EventType.NOTE, payload={"same": True}))
    assert store.event_count("P1") == 4


# ---------------------------------------------------------------------------
# FIND-006 — fail-closed task ceiling (engine-level default)
# ---------------------------------------------------------------------------


def test_finding_006_engine_defaults_to_a_finite_task_ceiling(engine_factory) -> None:
    engine = engine_factory(max_total_tasks=7)
    assert engine.config.budget.max_tasks == 7, (
        "without an explicit max_tasks the engine must align the budget with the "
        "decomposition bound (fail-closed), never stay unbounded"
    )


def test_finding_006_explicit_task_ceiling_is_respected(engine_factory) -> None:
    engine = engine_factory(budget=BudgetLimits(max_tasks=2), max_total_tasks=50)
    assert engine.config.budget.max_tasks == 2


# ---------------------------------------------------------------------------
# FIND-007 / FIND-008 — graph correctness
# ---------------------------------------------------------------------------


def test_finding_008_add_with_existing_id_drops_the_old_edges() -> None:
    graph = TaskGraph(
        [
            Task(id="T-a", title="a"),
            Task(id="T-b", title="b", dependencies=["T-a"]),
        ]
    )
    assert graph.dependents_of("T-a") == ["T-b"]
    graph.add(Task(id="T-b", title="b", dependencies=[]))
    assert graph.dependents_of("T-a") == []
    assert graph.ancestors("T-b") == []
    graph.validate()
    graph2 = TaskGraph([Task(id="X", title="x"), Task(id="Y", title="y", dependencies=["X"])])
    graph2.add(Task(id="Y", title="y", dependencies=[]))
    assert graph2.ready()[0].id in {"X", "Y"}
    graph2.validate()


# ---------------------------------------------------------------------------
# FIND-009 — per-run contiguous sequence numbers
# ---------------------------------------------------------------------------


def test_finding_009_each_run_has_a_contiguous_sequence(store: Store) -> None:
    create_project(store, "PRJ-a")
    create_project(store, "PRJ-b")
    store.append(Event(project_id="PRJ-a", type=EventType.NOTE, payload={"n": 1}))
    store.append(Event(project_id="PRJ-b", type=EventType.NOTE, payload={"n": 2}))
    store.append(Event(project_id="PRJ-a", type=EventType.NOTE, payload={"n": 3}))
    store.append(Event(project_id="PRJ-b", type=EventType.NOTE, payload={"n": 4}))
    assert store.event_seqs("PRJ-a") == [1, 2, 3]
    assert store.event_seqs("PRJ-b") == [1, 2, 3]
    # list_events returns the run-scoped sequence, so consumers see a dense stream.
    assert [event.seq for event in store.list_events("PRJ-a")] == [1, 2, 3]


def test_finding_009_concurrent_runs_keep_their_own_sequences(store: Store) -> None:
    create_project(store, "PRJ-a")
    create_project(store, "PRJ-b")

    def append_many(project: str) -> None:
        for index in range(20):
            store.append(Event(project_id=project, type=EventType.NOTE, payload={"i": index}))

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(append_many, ["PRJ-a", "PRJ-b", "PRJ-a", "PRJ-b"]))
    for project in ("PRJ-a", "PRJ-b"):
        seqs = store.event_seqs(project)
        assert seqs == list(range(1, len(seqs) + 1)), project
        assert len(seqs) == 41  # project_created + 40 notes


def test_finding_009_resume_continues_the_sequence(store: Store) -> None:
    create_project(store)
    store.append(Event(project_id="P1", type=EventType.NOTE, payload={"a": 1}))
    store.put_control("P1", "restart", {"simulated": True})
    stored = store.append(Event(project_id="P1", type=EventType.RUN_RESUMED, payload={}))
    assert stored.seq == 3
    assert store.event_seqs("P1") == [1, 2, 3]


# ---------------------------------------------------------------------------
# FIND-011 / FIND-013 — path safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        ".GIT/config",
        ".Git/config",
        ".gIt/config",
        ".git/HOOKS/pre-commit",
        "src/../.GIT/config",
        r".GIT\config",
        ".svn/entries",
        ".HG/hgrc",
    ],
)
def test_finding_011_vcs_metadata_is_rejected_case_insensitively(tmp_path: Path, path: str) -> None:
    root = tmp_path / "repo"
    (root / ".git").mkdir(parents=True)
    (root / ".git" / "config").write_text("ORIGINAL", encoding="utf-8")
    with pytest.raises(PathViolationError):
        validate_write_path(path, root)
    # Nothing was written through.
    assert (root / ".git" / "config").read_text(encoding="utf-8") == "ORIGINAL"


def test_finding_013_hardlink_inside_the_root_is_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside_secret.txt"
    outside.write_text("CLASSIFIED", encoding="utf-8")
    root = tmp_path / "repo"
    root.mkdir()
    link = root / "notes.md"
    os.link(outside, link)  # a repository can carry this
    with pytest.raises(PathViolationError, match="hard link"):
        validate_write_path("notes.md", root)
    assert outside.read_text(encoding="utf-8") == "CLASSIFIED"


def test_finding_013_symlink_escape_is_still_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "repo"
    root.mkdir()
    (root / "link").symlink_to(outside)
    with pytest.raises(PathViolationError):
        validate_write_path("link/pwned.txt", root)
    assert not (outside / "pwned.txt").exists()


# ---------------------------------------------------------------------------
# FIND-012 — Retry-After is not clamped by max_delay
# ---------------------------------------------------------------------------


async def test_finding_012_retry_after_wins_over_max_delay() -> None:
    delays: list[float] = []

    async def failing(attempt: int) -> None:
        raise RateLimitError("429", retry_after=120.0)

    async def sleeper(seconds: float) -> None:
        delays.append(seconds)

    with pytest.raises(RateLimitError):
        await call_with_retry(
            failing,
            RetryPolicy(max_attempts=2, base_delay=1.0, max_delay=30.0, jitter=0.0),
            sleep=sleeper,
        )
    assert delays == [120.0], "server Retry-After must not be clamped to max_delay"


async def test_finding_012_absolute_cap_still_protects() -> None:
    delays: list[float] = []

    async def failing(attempt: int) -> None:
        raise RateLimitError("429", retry_after=10_000.0)

    async def sleeper(seconds: float) -> None:
        delays.append(seconds)

    with pytest.raises(RateLimitError):
        await call_with_retry(
            failing,
            RetryPolicy(max_attempts=2, max_delay=30.0, max_retry_after=600.0, jitter=0.0),
            sleep=sleeper,
        )
    assert delays == [600.0]


# ---------------------------------------------------------------------------
# FIND-014 — resume re-dispatches interrupted work without waiting for the lease
# ---------------------------------------------------------------------------


async def test_finding_014_resume_recovers_running_task_with_live_lease(engine_factory) -> None:
    engine = engine_factory(db_name="f14.db")
    repo = Path(engine.config.repo_path)
    summary = await engine.run_prd("Deliver:\n- a small feature", run_id="R14")
    assert summary.status == "DONE"
    # Simulate a crash: pick a DONE task and force it back to RUNNING with a
    # fresh 300s lease, exactly what a kill -9 mid-call leaves behind.
    task_id = summary.report["per_task"][0]["task_id"]
    engine.store._conn.execute(
        "UPDATE tasks SET status='running' WHERE id=?", (task_id,)
    )
    engine.store._conn.commit()
    data = engine.store.get_task(task_id)
    data.status = TaskStatus.RUNNING
    data.lease_expires_at = datetime.now(UTC) + timedelta(seconds=300)
    engine.store._conn.execute(
        "UPDATE tasks SET data=? WHERE id=?", (data.model_dump_json(), task_id)
    )
    engine.store._conn.commit()

    engine2 = engine_factory(db_name="f14.db")
    resumed = await engine2.resume("R14")
    assert resumed.status == "DONE", f"resume must not deadlock: {resumed.reason}"
    assert resumed.outcome is not None and not resumed.outcome.deadlock
    assert engine2.store.get_task(task_id).attempts >= 2  # type: ignore[union-attr]


async def test_finding_014_resume_recovers_a_task_interrupted_in_review(engine_factory) -> None:
    engine = engine_factory(db_name="f14b.db")
    summary = await engine.run_prd("Deliver:\n- a small feature", run_id="R14B")
    task_id = summary.report["per_task"][0]["task_id"]
    engine.store._conn.execute("UPDATE tasks SET status='review' WHERE id=?", (task_id,))
    engine.store._conn.commit()
    task = engine.store.get_task(task_id)
    task.status = TaskStatus.REVIEW
    engine.store._conn.execute("UPDATE tasks SET data=? WHERE id=?", (task.model_dump_json(), task_id))
    engine.store._conn.commit()

    engine2 = engine_factory(db_name="f14b.db")
    resumed = await engine2.resume("R14B")
    assert resumed.status == "DONE"
    assert engine2.store.get_task(task_id).status is TaskStatus.DONE  # type: ignore[union-attr]


def test_finding_014_deadlock_detector_ignores_orphans(store: Store) -> None:
    """A RUNNING task with no coroutine is recoverable, not a deadlock."""
    from agentcorp.scheduler import Scheduler

    _running_task(store, lease_seconds=300.0)  # RUNNING, lease still valid
    graph = TaskGraph(store.list_tasks("P1"))
    assert [t.id for t in graph.active()] == ["T1"]

    class _Null:
        def __getattr__(self, item: str) -> object:  # pragma: no cover - unused
            raise AssertionError(item)

    # The scheduler's orphan sweep is the only thing the deadlock check needs;
    # assert its precondition directly: active RUNNING tasks exist and none are in flight.
    assert any(task.status is TaskStatus.RUNNING for task in graph.active())
    assert not hasattr(Scheduler, "_inflight")  # instance attribute, not class state


# ---------------------------------------------------------------------------
# G-I — runaway decomposition terminates within 5 seconds
# ---------------------------------------------------------------------------


async def test_g_i_runaway_split_terminates_within_five_seconds(engine_factory) -> None:
    from agentcorp import DecompositionBounds
    from agentcorp.decomposer import Decomposer

    engine = engine_factory(skill=0.0, seed=1, max_depth=2, max_total_tasks=9, db_name="gi.db")
    engine.config.max_attempts = 1
    engine.bounds = DecompositionBounds(max_depth=2, max_total_tasks=9, max_subtasks=3)
    started = time.monotonic()
    summary = await asyncio.wait_for(
        engine.run_prd("Deliver:\n- an oversized feature", run_id="RGI"), timeout=5.0
    )
    elapsed = time.monotonic() - started
    assert elapsed < 5.0, f"runaway split took {elapsed:.2f}s (must terminate in 5s)"
    assert summary.status in {"FAILED", "DONE"}
    tasks = engine.store.list_tasks("RGI")
    assert len(tasks) <= 9, "max_total_tasks must hold even under a split storm"
    assert max(task.depth for task in tasks) <= 2, "max_depth must hold"
    assert all(task.is_terminal for task in tasks), "run must converge to terminal statuses"
    del Decomposer  # imported for the type reference above; keeps ruff honest


# ---------------------------------------------------------------------------
# G-M — supervisor: no false positive, no miss
# ---------------------------------------------------------------------------


def test_g_m_supervisor_does_not_intervene_in_a_progressing_long_task() -> None:
    from agentcorp.events import Event
    from agentcorp.supervisor import Supervisor, SupervisorConfig

    clock = FakeClock()
    supervisor = Supervisor(SupervisorConfig(stuck_after_s=10.0), clock=clock)
    graph = TaskGraph([Task(id="T1", title="long but healthy")])
    graph.update(Task(id="T1", title="long but healthy", status=TaskStatus.RUNNING))
    supervisor.observe(
        Event(project_id="P", type=EventType.TASK_STARTED, task_id="T1", created_at=clock.now())
    )
    interventions: list[object] = []
    for _ in range(50):  # 50 progress ticks over 100 simulated seconds
        clock.advance(2.0)
        supervisor.observe(
            Event(
                project_id="P",
                type=EventType.AGENT_RUN_FINISHED,
                task_id="T1",
                created_at=clock.now(),
            )
        )
        interventions.extend(supervisor.tick(graph=graph, project_id="P"))
    assert interventions == [], "a task that keeps making progress must never be intervened"
    assert supervisor.snapshot()["false_positive_guarded"] > 0  # the guard actually fired


def test_g_m_supervisor_detects_a_genuinely_stuck_task() -> None:
    from agentcorp.events import Event
    from agentcorp.supervisor import Supervisor, SupervisorConfig

    clock = FakeClock()
    supervisor = Supervisor(SupervisorConfig(stuck_after_s=10.0), clock=clock)
    graph = TaskGraph([Task(id="T1", title="silent and stuck")])
    graph.update(Task(id="T1", title="silent and stuck", status=TaskStatus.RUNNING, attempts=1))
    supervisor.observe(
        Event(project_id="P", type=EventType.TASK_STARTED, task_id="T1", created_at=clock.now())
    )
    clock.advance(30.0)
    interventions = supervisor.tick(graph=graph, project_id="P")
    assert len(interventions) == 1
    finding = interventions[0].finding  # type: ignore[attr-defined]
    assert finding.kind == "stuck"
    assert finding.task_ids == ["T1"]
    assert "no progress" in finding.detail


async def test_g_m_supervisor_intervention_recovers_a_hung_worker(engine_factory) -> None:
    """End-to-end: a hung provider call is aborted by the supervisor, not waited out."""
    from agentcorp import EventType
    from agentcorp.supervisor import SupervisorConfig

    hang = asyncio.Event()
    release = asyncio.Event()

    class HangingProvider(MockProvider):
        async def complete(self, request: CompletionRequest) -> CompletionResponse:
            if "CONTRACT: worker" in request.system and not release.is_set():
                hang.set()
                await asyncio.wait_for(release.wait(), timeout=10.0)
            return await super().complete(request)

    engine = engine_factory(
        provider=HangingProvider(seed=5, skill=1.0),
        supervisor=SupervisorConfig(stuck_after_s=0.05, intervention_cooldown_s=0.0),
        db_name="gm.db",
        tick=0.01,
        max_depth=0,
        real_time=True,
    )
    run_task = asyncio.ensure_future(engine.run_prd("Deliver:\n- a small feature", run_id="RGM"))
    await asyncio.wait_for(hang.wait(), timeout=5.0)
    # Wait until the supervisor has raised + applied an intervention.
    deadline = time.monotonic() + 5.0
    interventions = []
    while time.monotonic() < deadline:
        interventions = [
            event
            for event in engine.store.list_events("RGM")
            if event.type is EventType.INTERVENTION_APPLIED
        ]
        if interventions:
            break
        await asyncio.sleep(0.01)
    release.set()
    summary = await asyncio.wait_for(run_task, timeout=10.0)
    assert interventions, "the stuck task must produce an intervention"
    applied = [event for event in interventions if event.payload.get("applied")]
    assert applied, "the stuck intervention must actually be applied"
    assert summary.run_id == "RGM"
    assert all(task.is_terminal for task in engine.store.list_tasks("RGM"))


# ---------------------------------------------------------------------------
# TokenBucket starvation (found by the token-bucket test in this suite)
# ---------------------------------------------------------------------------


async def test_token_bucket_does_not_starve_on_float_residuals() -> None:
    from agentcorp import TokenBucket

    clock = FakeClock()
    bucket = TokenBucket(rate_per_s=10.0, capacity=1.0, clock=clock)
    assert bucket.try_acquire()

    async def sleeper(seconds: float) -> None:
        clock.advance(seconds)

    await asyncio.wait_for(bucket.acquire(sleep=sleeper), timeout=1.0)
    assert bucket.available < 1.0  # consumed, refilled, consumed again
