"""C4/C5/C11 store: events, projections, exactly-once claim, leases, replay."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from conftest import create_project, drive, make_task, seed_tasks

from agentcorp import Event, EventType, StateError, Store, TaskStatus
from agentcorp.models import Usage


def test_append_assigns_monotonic_sequence(store: Store) -> None:
    create_project(store)
    first = store.append(Event(project_id="P1", type=EventType.NOTE, payload={"i": 1}))
    second = store.append(Event(project_id="P1", type=EventType.NOTE, payload={"i": 2}))
    assert second.seq == first.seq + 1
    assert store.event_seqs("P1") == [1, 2, 3]
    assert store.event_count("P1") == 3


def test_append_many_is_atomic_on_failure(store: Store) -> None:
    create_project(store)
    before = store.event_count("P1")
    good = Event(project_id="P1", type=EventType.NOTE, payload={"i": 1})
    bad = Event(project_id="P1", type=EventType.TASK_STARTED, task_id="ghost", payload={})
    with pytest.raises(StateError):
        store.append_many([good, bad])
    assert store.event_count("P1") == before  # nothing from the failed batch persisted


def test_append_many_batch_commits_together(store: Store) -> None:
    create_project(store)
    tasks = [make_task("T1"), make_task("T2")]
    stored = store.append_many(
        [
            Event(
                project_id="P1",
                type=EventType.TASK_CREATED,
                task_id=task.id,
                payload={"task": task.model_dump(mode="json")},
            )
            for task in tasks
        ]
    )
    assert [event.seq for event in stored] == [2, 3]
    assert {task.id for task in store.list_tasks("P1")} == {"T1", "T2"}


def test_task_ready_projects_to_ready_not_pending(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    assert store.get_task("T1").status is TaskStatus.READY  # type: ignore[union-attr]


def test_illegal_transition_rejected_by_projection(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    # PENDING -> DONE is not on the whitelist.
    with pytest.raises(StateError, match="illegal task transition"):
        drive(store, "P1", "T1", EventType.TASK_COMPLETED)


def test_failed_to_ready_requires_attempt_budget(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1", max_attempts=1)])
    drive(store, "P1", "T1", EventType.TASK_READY, EventType.TASK_CLAIMED, EventType.TASK_STARTED)
    drive(store, "P1", "T1", EventType.TASK_FAILED, reason="boom")
    with pytest.raises(StateError, match="attempts"):
        drive(store, "P1", "T1", EventType.TASK_RETRIED)
    # Quarantine is always allowed from FAILED.
    drive(store, "P1", "T1", EventType.TASK_QUARANTINED, reason="poison")
    assert store.get_task("T1").status is TaskStatus.QUARANTINED  # type: ignore[union-attr]


def test_review_lifecycle_projection(store: Store) -> None:
    from agentcorp import Review, ReviewIssue, Severity

    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY, EventType.TASK_CLAIMED, EventType.TASK_STARTED)
    drive(store, "P1", "T1", EventType.REVIEW_STARTED)
    assert store.get_task("T1").status is TaskStatus.REVIEW  # type: ignore[union-attr]
    review = Review(
        task_id="T1",
        verdict="REJECT",
        issues=[ReviewIssue(severity=Severity.HIGH, message="broken")],
    )
    store.append(
        Event(
            project_id="P1",
            type=EventType.REVIEW_REJECTED,
            task_id="T1",
            payload={"review": review.model_dump(mode="json")},
        )
    )
    # REJECTED records the verdict; the status move is TASK_REWORK.
    assert store.get_task("T1").status is TaskStatus.REVIEW  # type: ignore[union-attr]
    drive(store, "P1", "T1", EventType.TASK_REWORK)
    task = store.get_task("T1")
    assert task.status is TaskStatus.READY  # type: ignore[union-attr]
    assert task.rework_count == 1  # type: ignore[union-attr]
    assert len(store.reviews_for_task("T1")) == 1


def test_claim_task_is_exactly_once_under_64_threads(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)

    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(lambda i: store.claim_task("T1", owner=f"w{i}"), range(64)))

    winners = [events for events in results if events is not None]
    assert len(winners) == 1, "exactly one claim may win"
    assert [event.type for event in winners[0]] == [EventType.TASK_CLAIMED, EventType.TASK_STARTED]
    task = store.get_task("T1")
    assert task.status is TaskStatus.RUNNING  # type: ignore[union-attr]
    assert task.attempts == 1  # type: ignore[union-attr]
    assert task.lease_expires_at is not None  # type: ignore[union-attr]


def test_claim_task_refuses_non_ready_status(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    assert store.claim_task("T1", owner="w") is None  # still PENDING
    drive(store, "P1", "T1", EventType.TASK_READY)
    assert store.claim_task("T1", owner="w") is not None
    # Second claim on RUNNING fails.
    assert store.claim_task("T1", owner="w2") is None


def test_claim_unknown_task_returns_none(store: Store) -> None:
    create_project(store)
    assert store.claim_task("nope", owner="w") is None


def test_stale_running_and_recovery_path(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert store.claim_task("T1", owner="w", lease_seconds=10, now=now) is not None
    assert store.stale_running(now) == []
    assert [task.id for task in store.stale_running(now + timedelta(seconds=11))] == ["T1"]

    requeued = store.recover_stale_task("T1", now=now + timedelta(seconds=11))
    assert requeued is True
    task = store.get_task("T1")
    assert task.status is TaskStatus.READY  # type: ignore[union-attr]
    types = [event.type for event in store.list_events("P1", task_id="T1")]
    assert EventType.TASK_FAILED in types and EventType.TASK_RETRIED in types
    # The crash consumed exactly one attempt.
    assert task.attempts == 1  # type: ignore[union-attr]


def test_recovery_quarantines_when_attempts_exhausted(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1", max_attempts=1)])
    drive(store, "P1", "T1", EventType.TASK_READY)
    store.claim_task("T1", owner="w", lease_seconds=1, now=datetime(2026, 1, 1, tzinfo=UTC))
    requeued = store.recover_stale_task("T1", now=datetime(2026, 1, 1, 0, 0, 5, tzinfo=UTC))
    assert requeued is False
    assert store.get_task("T1").status is TaskStatus.QUARANTINED  # type: ignore[union-attr]


def test_recover_non_running_is_noop(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    assert store.recover_stale_task("T1") is False


def test_renew_lease_emits_event_and_keeps_replay_pure(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    store.claim_task("T1", owner="w", lease_seconds=10)
    event = store.renew_lease("T1", lease_seconds=100)
    assert event.type is EventType.TASK_UPDATED
    assert store.get_task("T1").lease_expires_at is not None  # type: ignore[union-attr]
    assert store.verify_replay("P1")


def test_renew_lease_on_non_running_raises(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    with pytest.raises(StateError, match="cannot renew"):
        store.renew_lease("T1")


def test_run_usage_projection_tracks_calls_and_tokens(store: Store) -> None:
    from agentcorp import AgentRun

    create_project(store)
    run = AgentRun(
        project_id="P1",
        task_id="T1",
        role="worker",
        provider="mock",
        model="m",
        usage=Usage(tokens_in=10, tokens_out=5, calls=1),
    )
    store.append(
        Event(project_id="P1", type=EventType.AGENT_RUN_STARTED, task_id="T1", payload={"run": run.model_dump(mode="json")})
    )
    assert len(store.unfinished_runs("P1")) == 1
    run.finished_at = run.started_at
    store.append(
        Event(project_id="P1", type=EventType.AGENT_RUN_FINISHED, task_id="T1", payload={"run": run.model_dump(mode="json")})
    )
    assert store.unfinished_runs("P1") == []
    usage = store.usage_for_project("P1")
    assert usage.total_tokens == 15 and usage.calls == 1
    assert store.usage_by_task("P1")["T1"].total_tokens == 15


def test_run_finished_projection_updates_progress(store: Store) -> None:
    from agentcorp import AgentRun

    create_project(store)
    seed_tasks(store, "P1", [make_task("T1")])
    drive(store, "P1", "T1", EventType.TASK_READY)
    store.claim_task("T1", owner="w")
    run = AgentRun(
        project_id="P1",
        task_id="T1",
        role="worker",
        provider="mock",
        model="m",
        finished_at=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
    )
    store.append(
        Event(
            project_id="P1",
            type=EventType.AGENT_RUN_FINISHED,
            task_id="T1",
            payload={"run": run.model_dump(mode="json")},
            created_at=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
        )
    )
    assert store.get_task("T1").progress_at is not None  # type: ignore[union-attr]


def test_rebuild_reproduces_projection_and_is_idempotent(store: Store) -> None:
    from agentcorp import AgentRun, Review

    create_project(store)
    seed_tasks(store, "P1", [make_task("T1"), make_task("T2", dependencies=["T1"])])
    drive(store, "P1", "T1", EventType.TASK_READY)
    store.claim_task("T1", owner="w")
    run = AgentRun(project_id="P1", task_id="T1", role="worker", provider="mock", model="m")
    store.append(Event(project_id="P1", type=EventType.AGENT_RUN_STARTED, task_id="T1", payload={"run": run.model_dump(mode="json")}))
    run.finished_at = run.started_at
    store.append(Event(project_id="P1", type=EventType.AGENT_RUN_FINISHED, task_id="T1", payload={"run": run.model_dump(mode="json")}))
    drive(store, "P1", "T1", EventType.TASK_FINISHED)
    drive(store, "P1", "T2", EventType.TASK_READY, EventType.TASK_CLAIMED, EventType.TASK_STARTED, EventType.REVIEW_STARTED)
    review = Review(task_id="T2", verdict="PASS")
    store.append(
        Event(
            project_id="P1",
            type=EventType.REVIEW_APPROVED,
            task_id="T2",
            payload={"review": review.model_dump(mode="json")},
        )
    )

    digest = store.projection_digest("P1")
    assert store.rebuild_from_events("P1") > 0
    assert store.projection_digest("P1") == digest
    store.rebuild_projections("P1")
    assert store.projection_digest("P1") == digest


def test_documents_derived_and_control_are_separate(store: Store) -> None:
    create_project(store)
    store.put_control("P1", "cancel_request", {"reason": "x"})
    store.put_derived("P1", "run_report", {"cached": True})
    store.put_document("P1", "ad_hoc", {"note": 1})
    assert store.get_control("P1", "cancel_request") == {"reason": "x"}
    assert store.get_derived("P1", "run_report") == {"cached": True}
    store.rebuild_from_events("P1")
    # A cancel request is an input: it must survive a projection rebuild.
    assert store.get_control("P1", "cancel_request") == {"reason": "x"}
    assert store.get_derived("P1", "run_report") is not None
    assert store.get_document("P1", "ad_hoc") is None  # documents are projections


def test_verify_integrity_reports_seq_gap(store: Store) -> None:
    create_project(store)
    store.append(Event(project_id="P1", type=EventType.NOTE, payload={"i": 1}))
    store.append(Event(project_id="P1", type=EventType.NOTE, payload={"i": 2}))
    # Delete a middle event directly to simulate log corruption.
    store._conn.execute("DELETE FROM events WHERE seq=2")
    store._conn.commit()
    problems = store.verify_integrity("P1")
    assert any("seq gap" in problem for problem in problems)


def test_run_summary_document_from_event(store: Store) -> None:
    create_project(store)
    store.append(
        Event(
            project_id="P1",
            type=EventType.RUN_FINISHED,
            payload={"status": "DONE", "run_id": "P1", "wall_ms": 12},
        )
    )
    summary = store.last_run_summary("P1")
    assert summary is not None and summary["status"] == "DONE" and summary["wall_ms"] == 12
    project = store.get_project("P1")
    assert project.status == "completed"  # type: ignore[union-attr]


def test_run_finished_failed_and_cancelled_statuses(store: Store) -> None:
    create_project(store)
    store.append(Event(project_id="P1", type=EventType.RUN_FINISHED, payload={"status": "BUDGET_EXHAUSTED"}))
    assert store.get_project("P1").status == "failed"  # type: ignore[union-attr]


def test_budget_events_project_documents(store: Store) -> None:
    from agentcorp import BudgetSnapshot

    create_project(store)
    snapshot = BudgetSnapshot()
    store.append(
        Event(
            project_id="P1",
            type=EventType.BUDGET_UPDATED,
            payload={"budget": snapshot.model_dump(mode="json")},
        )
    )
    assert store.get_document("P1", "budget") is not None
    store.append(Event(project_id="P1", type=EventType.BUDGET_EXHAUSTED, payload={"reason": "tokens"}))
    assert store.get_document("P1", "budget_exhausted") is not None
    store.append(Event(project_id="P1", type=EventType.BUDGET_WARNING, payload={"reason": "80%"}))
    assert store.get_document("P1", "budget_warning") is not None


def test_status_counts_covers_all_statuses(store: Store) -> None:
    create_project(store)
    seed_tasks(store, "P1", [make_task("T1"), make_task("T2")])
    counts = store.status_counts("P1")
    assert set(TaskStatus) == set(counts)
    assert counts[TaskStatus.PENDING] == 2


def test_concurrent_appends_keep_sequence_contiguous(tmp_path: Path) -> None:
    store = Store(tmp_path / "conc.db")
    try:
        create_project(store)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: store.append(Event(project_id="P1", type=EventType.NOTE, payload={"i": i})), range(64)))
        seqs = store.event_seqs("P1")
        assert seqs == list(range(1, len(seqs) + 1))
    finally:
        store.close()


def test_store_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "reopen.db"
    first = Store(path)
    create_project(first)
    first.close()
    second = Store(path)
    try:
        assert second.get_project("P1") is not None
        second.append(Event(project_id="P1", type=EventType.NOTE, payload={}))
        assert second.event_count("P1") == 2
    finally:
        second.close()
