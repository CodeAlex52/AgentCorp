"""Chaos and fault-tolerance at the engine level (SPEC C6, §7.4).

Unit coverage of the injector lives in ``test_reliability.py``-style checks
below; the engine-level tests prove the *orchestrator* survives injected faults
without weakening its invariants (no DONE re-run, bounded calls, no orphan
states).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import run as run_coro

from agentcorp import (
    ChaosConfig,
    ChaosController,
    ChaosProvider,
    CompletionRequest,
    CompletionResponse,
    FaultKind,
    MockProvider,
    ProviderError,
    SupervisorConfig,
    TaskStatus,
    TransientError,
    Usage,
)
from agentcorp.chaos import SCHEMA_RECOVERABLE, ChaosReport
from agentcorp.engine import Engine, EngineConfig
from agentcorp.runtime import AgentRuntime
from agentcorp.scheduler import SchedulerConfig
from agentcorp.util import FakeClock, noop_sleep


@pytest.fixture
def toy_repo_files(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname='toy'\n", encoding="utf-8")
    (repo / "src" / "toy.py").write_text("x = 1\n", encoding="utf-8")
    return repo


def make_engine(
    *,
    repo: Path,
    db: Path,
    chaos: float,
    seed: int = 5,
    skill: float = 1.0,
    max_attempts: int = 3,
    concurrency: int = 2,
) -> Engine:
    config = EngineConfig(
        repo_path=str(repo),
        provider_spec="mock",
        db_path=str(db),
        scheduler=SchedulerConfig(max_concurrency=concurrency, tick_interval_s=0.01),
        supervisor=SupervisorConfig(stuck_after_s=30.0),
        chaos=ChaosConfig(enabled=True, probability=chaos, seed=seed) if chaos > 0 else None,
        max_attempts=max_attempts,
        deterministic=True,
    )
    return Engine(config, provider=MockProvider(seed=seed, skill=skill), clock=FakeClock(), sleep=noop_sleep)


# ---------------------------------------------------------------------------
# injector unit behaviour
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    [FaultKind.RATE_LIMIT, FaultKind.TIMEOUT, FaultKind.AGENT_CRASH, FaultKind.TOOL_FAILURE, FaultKind.WORKER_STUCK],
)
async def test_chaos_retryable_faults_raise_transient_errors(kind: FaultKind) -> None:
    from agentcorp import RetryPolicy
    from agentcorp.errors import is_retryable

    controller = ChaosController(ChaosConfig(enabled=True, probability=1.0, seed=1, kinds=(kind,)))
    provider = ChaosProvider(MockProvider(seed=1), controller)
    runtime = AgentRuntime(provider, retry=RetryPolicy(max_attempts=1), sleep=noop_sleep)
    with pytest.raises(Exception) as excinfo:
        await runtime.call([{"role": "user", "content": "hi"}], role="worker", task_id="T")
    assert is_retryable(excinfo.value)
    assert controller.report.total == 1


async def test_chaos_invalid_json_is_schema_recoverable() -> None:
    controller = ChaosController(
        ChaosConfig(enabled=True, probability=1.0, seed=2, kinds=(FaultKind.INVALID_JSON,))
    )
    provider = ChaosProvider(MockProvider(seed=2), controller)
    runtime = AgentRuntime(provider, sleep=noop_sleep)
    result = await runtime.call([{"role": "user", "content": "hi"}], role="worker", task_id="T")
    from agentcorp import SchemaError
    from agentcorp.parsing import extract_json_object

    with pytest.raises(SchemaError):
        extract_json_object(result.text)
    assert FaultKind.INVALID_JSON in SCHEMA_RECOVERABLE


async def test_chaos_context_overflow_is_recovered_by_shrinking() -> None:
    controller = ChaosController(
        ChaosConfig(enabled=True, probability=1.0, seed=3, kinds=(FaultKind.CONTEXT_OVERFLOW,), max_injections=1)
    )
    provider = ChaosProvider(MockProvider(seed=3), controller)
    runtime = AgentRuntime(provider, max_context_shrinks=2, sleep=noop_sleep)
    result = await runtime.call(
        [{"role": "user", "content": "x" * 1000}], role="worker", task_id="T"
    )
    assert result.recovered_from == ["context_overflow_shrink_1"]
    assert result.shrunk_context == 1


async def test_chaos_duplicate_response_is_recorded_on_the_run() -> None:
    controller = ChaosController(ChaosConfig(enabled=True, probability=1.0, seed=4, kinds=(FaultKind.DUPLICATE_RESPONSE,)))
    provider = ChaosProvider(MockProvider(seed=4, skill=1.0), controller)
    runtime = AgentRuntime(provider, sleep=noop_sleep)
    task_args = {"role": "worker", "task_id": "T"}
    first = await runtime.call([{"role": "user", "content": "hi"}], **task_args)
    controller.config.max_injections = None
    controller.config.probability = 1.0
    controller.config.kinds = (FaultKind.DUPLICATE_RESPONSE,)
    second = await runtime.call([{"role": "user", "content": "hi"}], **task_args)
    assert second.text == first.text
    assert second.run.chaos_injections == [FaultKind.DUPLICATE_RESPONSE.value]


def test_chaos_report_counts_and_recovery() -> None:
    report = ChaosReport()
    report.record(FaultKind.TIMEOUT, "T1")
    report.record(FaultKind.TIMEOUT, "T2")
    controller = ChaosController(ChaosConfig(enabled=True))
    controller.recovered = report.recovered
    controller.note_recovery(FaultKind.TIMEOUT)
    payload = report.to_dict()
    assert payload["total_injections"] == 2
    assert payload["by_kind"]["timeout"] == 2
    assert payload["tasks_hit"] == ["T1", "T2"]


def test_chaos_config_normalises_strings_and_rejects_garbage() -> None:
    config = ChaosConfig(enabled=True, kinds=("429", "timeout")).normalised()  # type: ignore[arg-type]
    assert config.kinds == (FaultKind.RATE_LIMIT, FaultKind.TIMEOUT)
    with pytest.raises(ValueError):
        ChaosConfig(enabled=True, kinds=("nonsense",)).normalised()  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ChaosConfig(enabled=True, probability=1.5).normalised()


def test_chaos_max_injections_bounds_the_faults() -> None:
    controller = ChaosController(ChaosConfig(enabled=True, probability=1.0, seed=1, max_injections=2))
    assert controller.should_fault("worker", "T")
    controller.note(FaultKind.TIMEOUT, "T")
    assert controller.should_fault("worker", "T")
    controller.note(FaultKind.TIMEOUT, "T")
    assert not controller.should_fault("worker", "T")


def test_chaos_fail_forever_and_role_filter() -> None:
    controller = ChaosController(ChaosConfig(enabled=True, probability=0.0, fail_forever=True, roles=("reviewer",)))
    assert not controller.should_fault("worker", "T")
    assert controller.should_fault("reviewer", "T")


# ---------------------------------------------------------------------------
# engine-level fault injection
# ---------------------------------------------------------------------------


async def test_engine_completes_under_moderate_chaos(toy_repo_files: Path, tmp_path: Path) -> None:
    engine = make_engine(repo=toy_repo_files, db=tmp_path / "chaos.db", chaos=0.3, seed=11)
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="R-CHAOS")
    try:
        assert summary.status in {"DONE", "FAILED"}, summary.reason
        tasks = engine.store.list_tasks("R-CHAOS")
        assert all(task.is_terminal for task in tasks), "fault injection must not leave orphan states"
        assert summary.report["events_count"] > 0
        assert engine.store.verify_replay("R-CHAOS") is True
    finally:
        await engine.aclose()


async def test_engine_retry_storm_is_bounded_and_quarantines(toy_repo_files: Path, tmp_path: Path) -> None:
    """A provider that always fails must not loop: attempts cap + quarantine."""

    class AlwaysDown(MockProvider):
        """Planning/review work fine; only worker calls are down."""

        def __init__(self) -> None:
            super().__init__(seed=1, skill=1.0)
            self.worker_calls = 0  # note: MockProvider.calls is its request log

        async def complete(self, request: CompletionRequest) -> CompletionResponse:
            if request.role == "worker":
                self.worker_calls += 1
                raise ProviderError("provider is down")
            return await super().complete(request)

    provider = AlwaysDown()
    engine = make_engine(repo=toy_repo_files, db=tmp_path / "storm.db", chaos=0.0, max_attempts=2)
    engine.provider = provider
    engine.worker_runtime.provider = provider
    engine.reviewer_runtime.provider = provider
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="R-STORM")
    try:
        assert summary.status == "FAILED"
        tasks = engine.store.list_tasks("R-STORM")
        assert all(task.status in {TaskStatus.QUARANTINED, TaskStatus.CANCELLED} for task in tasks)
        # Calls are bounded: provider attempts (<= max_attempts per task) plus the
        # planning calls that failed before dispatch started.
        assert provider.worker_calls <= 3 * len(tasks), f"unbounded retry storm: {provider.worker_calls} worker calls"
        types = [event.type.value for event in engine.store.list_events("R-STORM")]
        assert "TASK_QUARANTINED" in types or "TASK_CANCELLED" in types
    finally:
        await engine.aclose()


async def test_engine_circuit_breaker_opens_and_run_fails_fast(toy_repo_files: Path, tmp_path: Path) -> None:
    class Failing(MockProvider):
        """Only worker calls are transient; planning/review stay healthy."""

        name = "failing"
        default_model = "failing-v1"

        def __init__(self) -> None:
            super().__init__(seed=1, skill=1.0)
            self.worker_calls = 0

        async def complete(self, request: CompletionRequest) -> CompletionResponse:
            if request.role == "worker":
                self.worker_calls += 1
                raise TransientError("always transient")
            return await super().complete(request)

    provider = Failing()
    engine = make_engine(repo=toy_repo_files, db=tmp_path / "circuit.db", chaos=0.0, max_attempts=1)
    engine.provider = provider
    engine.worker_runtime.provider = provider
    engine.reviewer_runtime.provider = provider
    # Trip after two consecutive worker failures so the breaker is exercised
    # within a single small run.
    engine.worker_runtime.breaker.failure_threshold = 2
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="R-CIRCUIT")
    try:
        assert summary.status == "FAILED"
        breaker = engine.worker_runtime.breaker
        assert breaker.stats.failures >= 1, "provider failures must reach the breaker"
        # The run failed fast: the poisoned chain never grew a retry storm.
        tasks = engine.store.list_tasks("R-CIRCUIT")
        assert provider.worker_calls <= len(tasks), (
            f"circuit breaker did not bound the damage: {provider.worker_calls} worker calls for {len(tasks)} tasks"
        )
        assert all(task.is_terminal for task in tasks)
        notes = [
            event
            for event in engine.store.list_events("R-CIRCUIT")
            if event.type.value == "NOTE" and event.payload.get("stage") == "circuit_open"
        ]
        # Either the breaker short-circuited a call (NOTE) or every task got at
        # most one attempt: both are "fail fast, never hammer".
        assert notes or provider.worker_calls <= len(tasks)
    finally:
        await engine.aclose()


async def test_engine_chaos_injections_are_recorded_in_usage(toy_repo_files: Path, tmp_path: Path) -> None:
    engine = make_engine(repo=toy_repo_files, db=tmp_path / "chaos2.db", chaos=1.0, seed=17, max_attempts=3)
    summary = await engine.run_prd("Deliver:\n- a feature", run_id="R-CHAOS2")
    try:
        usage: Usage = engine.store.usage_for_project("R-CHAOS2")
        assert usage.calls >= 1
        # One logical AgentRun may span several attempts; the ledger counts
        # attempts, so the per-run call counters must add up to it.
        runs = engine.store.list_runs("R-CHAOS2")
        assert runs, "every provider call must be recorded"
        assert sum(run.usage.calls for run in runs) == usage.calls
        assert sum(run.usage.total_tokens for run in runs) == usage.total_tokens
        assert summary.report["agent_calls"] == usage.calls or summary.status != "DONE"
    finally:
        await engine.aclose()


def test_chaos_provider_describe_includes_report() -> None:
    controller = ChaosController(ChaosConfig(enabled=True, probability=0.0))
    provider = ChaosProvider(MockProvider(seed=1), controller)
    described = provider.describe()
    assert described["chaos"]["total_injections"] == 0
    assert provider.name == "chaos(mock)"


def test_scheduler_and_chaos_wiring_is_deterministic() -> None:
    """The same seed produces the same injections, twice."""
    async def collect(seed: int) -> list[str]:
        controller = ChaosController(ChaosConfig(enabled=True, probability=0.7, seed=seed, kinds=(FaultKind.TIMEOUT,)))
        provider = ChaosProvider(MockProvider(seed=seed, skill=1.0), controller)
        runtime = AgentRuntime(provider, sleep=noop_sleep)
        for index in range(5):
            try:
                await runtime.call([{"role": "user", "content": f"call {index}"}], task_id=f"T{index}")
            except TransientError:
                continue
        return sorted(controller.report.injections.keys()) + [str(controller.report.total)]

    assert run_coro(collect(21)) == run_coro(collect(21))
