"""C7 budget: four hard-stop dimensions, refusal semantics, accounting."""

from __future__ import annotations

import pytest

from agentcorp import BudgetExceededError, BudgetLimits, BudgetManager, EventType, Usage
from agentcorp.util import FakeClock


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, str | None, dict[str, object]]] = []

    def __call__(self, type_: EventType, task_id: str | None, payload: dict[str, object]) -> None:
        self.events.append((type_.value, task_id, payload))

    def types(self) -> list[str]:
        return [entry[0] for entry in self.events]


def test_unlimited_budget_allows_everything() -> None:
    manager = BudgetManager()
    manager.check(estimated_tokens=10**9)
    assert manager.status().allowed
    assert manager.remaining_tokens() is None
    assert not manager.exhausted
    assert manager.pressure() == 0.0


def test_token_ceiling_refuses_before_dispatch() -> None:
    manager = BudgetManager(BudgetLimits(max_tokens=100))
    manager.check(estimated_tokens=100)  # exactly fits
    with pytest.raises(BudgetExceededError) as excinfo:
        manager.check(estimated_tokens=101)
    assert excinfo.value.limit == "max_tokens"


def test_token_boundary_at_exact_ceiling_refuses_zero_estimate_call() -> None:
    manager = BudgetManager(BudgetLimits(max_tokens=100))
    manager.record(Usage(tokens_in=60, tokens_out=40, calls=1))
    assert not manager.status().allowed  # used >= limit
    assert manager.exhausted
    with pytest.raises(BudgetExceededError):
        manager.check()


def test_cost_ceiling() -> None:
    manager = BudgetManager(BudgetLimits(max_cost_usd=0.5))
    manager.record(Usage(cost_usd=0.5, calls=1))
    with pytest.raises(BudgetExceededError) as excinfo:
        manager.check()
    assert excinfo.value.limit == "max_cost_usd"


def test_task_count_ceiling_only_applies_to_new_dispatch() -> None:
    manager = BudgetManager(BudgetLimits(max_tasks=2))
    manager.note_task_started(2)
    assert not manager.status(new_task=True).allowed
    assert manager.status(new_task=False).allowed  # an in-flight call may finish
    with pytest.raises(BudgetExceededError) as excinfo:
        manager.check(new_task=True)
    assert excinfo.value.limit == "max_tasks"
    assert manager.status().remaining_tasks == 0


def test_task_ceiling_is_fail_closed_by_default() -> None:
    """FIND-006: the default check path must not be fail-open."""
    manager = BudgetManager(BudgetLimits(max_tasks=1))
    manager.note_task_started(1)
    # Explicit "this is a new dispatch".
    assert not manager.status(new_task=True).allowed
    # No task_id and no new_task flag: worst case (a new dispatch) ⇒ refuse.
    assert not manager.status().allowed
    with pytest.raises(BudgetExceededError):
        manager.check()
    # A call that declares itself in-flight is still allowed to finish.
    assert manager.status(new_task=False).allowed


def test_tasks_started_is_derived_from_record() -> None:
    """FIND-006: a caller that forgets note_task_started() is still counted."""
    manager = BudgetManager(BudgetLimits(max_tasks=1))
    manager.record(Usage(tokens_in=1, calls=1), task_id="T1")
    assert manager.tasks_started == 1
    assert not manager.status(task_id="T2").allowed  # T2 is a fresh dispatch
    assert manager.status(task_id="T1").allowed  # T1 already owns its slot
    manager.record(Usage(tokens_in=1, calls=1), task_id="T1")  # idempotent
    assert manager.tasks_started == 1


def test_wall_clock_ceiling_uses_injected_clock() -> None:
    clock = FakeClock()
    manager = BudgetManager(BudgetLimits(max_wall_seconds=10.0), clock=clock)
    assert manager.status().allowed
    clock.advance(9.9)
    manager.refusals.clear()
    assert manager.status().allowed
    clock.advance(0.2)
    with pytest.raises(BudgetExceededError) as excinfo:
        manager.check()
    assert excinfo.value.limit == "max_wall_seconds"
    assert manager.elapsed_seconds() == pytest.approx(10.1, abs=0.01)


def test_agent_call_ceiling() -> None:
    manager = BudgetManager(BudgetLimits(max_agent_calls=1))
    manager.record(Usage(calls=1))
    with pytest.raises(BudgetExceededError) as excinfo:
        manager.check()
    assert excinfo.value.limit == "max_agent_calls"


def test_per_task_token_ceiling() -> None:
    manager = BudgetManager(BudgetLimits(max_tokens_per_task=50))
    manager.record(Usage(tokens_in=20, tokens_out=20, calls=1), task_id="T1")
    manager.check(task_id="T1", estimated_tokens=10)  # 40 + 10 == 50 fits
    with pytest.raises(BudgetExceededError) as excinfo:
        manager.check(task_id="T1", estimated_tokens=11)
    assert excinfo.value.scope == "task"
    assert manager.task_usage("T1").total_tokens == 40
    assert manager.task_usage("T-other").total_tokens == 0


def test_accounting_error_within_one_token() -> None:
    manager = BudgetManager(BudgetLimits(max_tokens=10**6))
    total_in = 0
    for index in range(50):
        manager.record(Usage(tokens_in=index, tokens_out=1, calls=1))
        total_in += index
    assert manager.project.usage.tokens_in == total_in
    assert manager.project.usage.calls == 50


def test_refusal_emits_budget_exhausted_event() -> None:
    recorder = Recorder()
    manager = BudgetManager(BudgetLimits(max_agent_calls=0), emit=recorder)
    with pytest.raises(BudgetExceededError):
        manager.check(task_id="T1")
    assert recorder.types() == [EventType.BUDGET_EXHAUSTED.value]
    assert manager.refusals and manager.refusals[0].limit == "max_agent_calls"


def test_record_emits_budget_updated() -> None:
    recorder = Recorder()
    manager = BudgetManager(BudgetLimits(max_tokens=1000), emit=recorder)
    manager.record(Usage(tokens_in=10, calls=1), task_id="T1")
    assert EventType.BUDGET_UPDATED.value in recorder.types()


def test_warning_emitted_once_at_pressure_threshold() -> None:
    recorder = Recorder()
    manager = BudgetManager(BudgetLimits(max_tokens=100), emit=recorder)
    manager.record(Usage(tokens_in=50, calls=1))
    assert EventType.BUDGET_WARNING.value not in recorder.types()
    manager.record(Usage(tokens_in=35, calls=1))
    manager.record(Usage(tokens_in=5, calls=1))
    assert recorder.types().count(EventType.BUDGET_WARNING.value) == 1


def test_pressure_takes_tightest_dimension() -> None:
    manager = BudgetManager(BudgetLimits(max_tokens=1000, max_tasks=2))
    manager.note_task_started(1)
    assert manager.pressure() == pytest.approx(0.5)
    manager.record(Usage(tokens_in=900, calls=1))
    assert manager.pressure() == pytest.approx(0.9)


def test_snapshot_and_summary_report_limits() -> None:
    manager = BudgetManager(BudgetLimits(max_tokens=100, max_cost_usd=1.0))
    manager.record(Usage(tokens_in=10, tokens_out=5, calls=1, cost_usd=0.25))
    snapshot = manager.snapshot()
    assert snapshot.limits.max_tokens == 100
    assert snapshot.used.total_tokens == 15
    assert "15/100" in manager.summary()


def test_reseed_rehydrates_after_restart() -> None:
    clock = FakeClock()
    manager = BudgetManager(BudgetLimits(max_tokens=1000, max_tasks=10), clock=clock)
    manager.reseed(
        usage=Usage(tokens_in=500, tokens_out=100, calls=5, cost_usd=0.5),
        by_task={"T1": Usage(tokens_in=10, calls=1)},
        started_monotonic=clock.monotonic() - 30.0,
        tasks_started=3,
    )
    assert manager.project.usage.total_tokens == 600
    assert manager.tasks_started == 3
    assert manager.elapsed_seconds() == pytest.approx(30.0, abs=0.01)
    assert manager.task_usage("T1").tokens_in == 10
    status = manager.status(new_task=True)
    assert status.allowed
    assert status.remaining_tokens == 400
    # The crash loop cannot reset the budget: 7 more tasks are allowed, not 10.
    assert status.remaining_tasks == 7
