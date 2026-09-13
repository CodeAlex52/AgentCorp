"""C2 — the task state machine whitelist and the domain models around it."""

from __future__ import annotations

import pytest

from agentcorp import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    AcceptanceCriterion,
    BudgetLimits,
    BudgetSnapshot,
    Review,
    ReviewIssue,
    Severity,
    StateError,
    Task,
    TaskStatus,
    Usage,
    WorkerOutcome,
    assert_transition,
    transition_allowed,
)

ALL_STATUSES = list(TaskStatus)


def _task(status: TaskStatus, *, attempts: int = 0, max_attempts: int = 3) -> Task:
    return Task(title="t", status=status, attempts=attempts, max_attempts=max_attempts)


# ---------------------------------------------------------------------------
# Exhaustive whitelist matrix: 10 statuses x 10 targets = 100 cases.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("current", ALL_STATUSES, ids=[s.value for s in ALL_STATUSES])
@pytest.mark.parametrize("target", ALL_STATUSES, ids=[s.value for s in ALL_STATUSES])
def test_transition_matrix_matches_whitelist(current: TaskStatus, target: TaskStatus) -> None:
    allowed = target in ALLOWED_TRANSITIONS[current]
    task = _task(current, attempts=0)
    if allowed:
        assert transition_allowed(current, target)
        assert_transition(task, target)  # must not raise
    else:
        assert not transition_allowed(current, target)
        with pytest.raises(StateError):
            assert_transition(task, target)


def test_terminal_statuses_exact() -> None:
    assert {
        TaskStatus.DONE,
        TaskStatus.QUARANTINED,
        TaskStatus.CANCELLED,
    } == TERMINAL_STATUSES
    # SPLIT is a waiting state, not terminal (SPEC 5.1).
    assert TaskStatus.SPLIT not in TERMINAL_STATUSES
    assert TaskStatus.FAILED not in TERMINAL_STATUSES


def test_failed_retry_guard_respects_attempts() -> None:
    assert_transition(_task(TaskStatus.FAILED, attempts=1, max_attempts=3), TaskStatus.READY)
    with pytest.raises(StateError, match="attempts"):
        assert_transition(_task(TaskStatus.FAILED, attempts=3, max_attempts=3), TaskStatus.READY)
    # The guard only applies to the retry edge.
    assert_transition(_task(TaskStatus.FAILED, attempts=3, max_attempts=3), TaskStatus.QUARANTINED)


def test_terminal_statuses_have_no_outgoing_edges() -> None:
    for status in TERMINAL_STATUSES:
        assert ALLOWED_TRANSITIONS[status] == frozenset()


def test_every_non_terminal_status_can_reach_a_terminal_status() -> None:
    reachable: set[TaskStatus] = set(TERMINAL_STATUSES)
    changed = True
    while changed:
        changed = False
        for source, targets in ALLOWED_TRANSITIONS.items():
            if source in reachable:
                continue
            if targets & reachable:
                reachable.add(source)
                changed = True
    assert reachable == set(ALL_STATUSES), f"unreachable terminal: {set(ALL_STATUSES) - reachable}"


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


def test_task_default_objective_falls_back_to_title() -> None:
    task = Task(title="Implement caching")
    assert task.objective == "Implement caching"
    assert task.status is TaskStatus.PENDING
    assert task.attempts == 0 and task.rework_count == 0
    assert task.claimed_at is None and task.lease_expires_at is None


def test_task_is_terminal_and_is_leaf() -> None:
    assert Task(title="t", status=TaskStatus.DONE).is_terminal
    assert Task(title="t", status=TaskStatus.QUARANTINED).is_terminal
    assert not Task(title="t", status=TaskStatus.SPLIT).is_terminal
    assert not Task(title="t", status=TaskStatus.SPLIT).is_leaf


def test_task_prompt_block_mentions_contract_fields() -> None:
    task = Task(
        id="T-1",
        title="Ship it",
        acceptance_criteria=[AcceptanceCriterion(statement="works", verification="test", command="pytest -q")],
        touch_paths=["src/a.py"],
    )
    block = task.to_prompt_block()
    for expected in ("TASK: T-1", "TITLE: Ship it", "pytest -q", "src/a.py", "ACCEPTANCE CRITERIA"):
        assert expected in block


def test_acceptance_criterion_machine_verifiability() -> None:
    assert AcceptanceCriterion(statement="s", verification="test", command="pytest").is_machine_verifiable
    assert AcceptanceCriterion(statement="s", verification="static").is_machine_verifiable
    assert not AcceptanceCriterion(statement="s", verification="review").is_machine_verifiable
    assert not AcceptanceCriterion(statement="s", verification="test").is_machine_verifiable  # no command


def test_requirement_covers_uses_keywords() -> None:
    from agentcorp import Requirement

    requirement = Requirement(raw_text="r", goal="g", keywords=["cache", "redis"])
    assert requirement.covers("Implement the CACHE layer")
    assert not requirement.covers("Rewrite the billing system")
    empty = Requirement(raw_text="r", goal="g")
    assert empty.covers("anything")  # no vocabulary ⇒ cannot drift


def test_usage_addition_and_rounding() -> None:
    total = Usage(tokens_in=10, tokens_out=5, calls=1, cost_usd=0.1) + Usage(
        tokens_in=1, tokens_out=1, calls=2, cost_usd=0.2
    )
    assert total.total_tokens == 17
    assert total.calls == 3
    assert total.cost_usd == pytest.approx(0.3)
    usage = Usage()
    usage.add_in_place(Usage(tokens_in=3, tokens_out=4))
    assert usage.total_tokens == 7


def test_budget_limits_has_four_spec_dimensions() -> None:
    fields = set(BudgetLimits.model_fields)
    for dimension in ("max_tokens", "max_cost_usd", "max_tasks", "max_wall_seconds"):
        assert dimension in fields, dimension
    limits = BudgetLimits(max_tokens=100, max_cost_usd=1.0, max_tasks=5, max_wall_seconds=60.0)
    assert limits.max_tokens == 100 and limits.max_wall_seconds == 60.0


def test_budget_snapshot_remaining_and_exhausted() -> None:
    snap = BudgetSnapshot(
        limits=BudgetLimits(max_tokens=100, max_cost_usd=1.0),
        used=Usage(tokens_in=60, tokens_out=40, cost_usd=0.5, calls=1),
    )
    assert snap.remaining_tokens == 0
    assert snap.exhausted  # reached the ceiling exactly
    assert snap.remaining_cost_usd == 0.5


def test_worker_outcome_blocked_requires_reason() -> None:
    outcome = WorkerOutcome(status="blocked")
    assert outcome.reason  # defaulted, never silently blank
    assert WorkerOutcome(status="success").files == []


def test_review_must_fix_requires_blocking_issue() -> None:
    blocking = Review(
        task_id="T",
        verdict="REJECT",
        issues=[ReviewIssue(severity=Severity.HIGH, message="broken")],
    )
    assert blocking.must_fix
    advisory = Review(
        task_id="T",
        verdict="REJECT",
        issues=[ReviewIssue(severity=Severity.LOW, message="style")],
    )
    assert not advisory.must_fix
    assert not Review(task_id="T", verdict="PASS").must_fix


def test_intervention_kinds_are_stable() -> None:
    from agentcorp import InterventionKind

    assert {kind.value for kind in InterventionKind} >= {
        "retry",
        "split",
        "cancel",
        "escalate",
        "noop",
    }
