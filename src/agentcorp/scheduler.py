"""The scheduler: bounded-concurrency dispatch over the task DAG.

Invariants enforced here (each has a dedicated test):

* **C2**  every status change goes through the whitelist — the store projection
  re-checks `models.assert_transition`, so a bug here cannot corrupt the log;
* **C4**  dispatch is ``Store.claim_task`` (atomic ``UPDATE ... WHERE READY``);
* **C7**  budget is checked *before* every dispatch and inside every agent call;
* **C8**  splits are bounded and inserted atomically with their child DAG;
* **C9**  deadlock is detected explicitly, never waited out;
* **C10** cancellation is durable (store document) and drains within a grace period;
* **C13** supervisor findings become interventions here;
* **C15** every emitted event can be logged as one JSON line.

The loop is event-driven: it only awaits when work is in flight, and every wait
is bounded by ``tick_interval_s`` so governance runs even while a provider call
hangs.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from .budget import BudgetManager
from .decomposer import Decomposer, decide_split
from .errors import (
    BudgetExceededError,
    CircuitOpenError,
    DecompositionError,
    GraphError,
    StateError,
    is_retryable,
)
from .events import Event, EventType
from .graph import TaskGraph
from .models import (
    AgentRun,
    Intervention,
    InterventionKind,
    Task,
    TaskStatus,
    WorkerOutcome,
    can_never_complete,
)
from .prompts import review_to_repair_hint
from .reviewer import Reviewer, validate_review
from .store import Store
from .supervisor import Supervisor
from .util import Clock, Sleeper, SystemClock, system_sleep
from .worker import PathViolationError, Worker, WorkerResult

__all__ = ["Scheduler", "SchedulerConfig", "RunOutcome"]

log = logging.getLogger("agentcorp.scheduler")

#: Statuses that can still be cancelled when a run is stopping.
_CANCELLABLE: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.PENDING,
        TaskStatus.READY,
        TaskStatus.BLOCKED,
        TaskStatus.REVIEW,
        TaskStatus.SPLIT,
    }
)


@dataclass
class SchedulerConfig:
    max_concurrency: int = 4
    lease_seconds: float = 300.0
    tick_interval_s: float = 0.05
    cancel_grace_s: float = 5.0
    review: bool = True
    max_reworks: int = 2
    task_retry_base_delay: float = 0.0
    task_retry_max_delay: float = 30.0
    #: Consecutive quarantined tasks that abort the whole run.
    circuit_failure_threshold: int = 5
    stop_on_task_failure: bool = False
    cancel_inflight_on_budget: bool = False
    #: When true every emitted event is also logged as one JSON line.
    log_events: bool = False


@dataclass
class RunOutcome:
    status: str  # DONE | FAILED | BUDGET_EXHAUSTED | CANCELLED | DEADLOCK
    reason: str = ""
    deadlock: bool = False
    tasks_total: int = 0
    tasks_done: int = 0
    tasks_failed: int = 0
    tasks_quarantined: int = 0
    tasks_cancelled: int = 0
    tasks_split: int = 0
    agent_calls: int = 0
    interventions: int = 0
    supervisor: dict[str, Any] = field(default_factory=dict)


class Scheduler:
    """Runs one project's DAG to a terminal state."""

    def __init__(
        self,
        *,
        store: Store,
        graph: TaskGraph,
        budget: BudgetManager,
        worker: Worker,
        reviewer: Reviewer,
        decomposer: Decomposer,
        supervisor: Supervisor,
        project_id: str,
        config: SchedulerConfig | None = None,
        clock: Clock | None = None,
        sleep: Sleeper = system_sleep,
        id_factory: Callable[[str], str] | None = None,
        repo_context: str = "",
    ) -> None:
        self.store = store
        self.graph = graph
        self.budget = budget
        self.worker = worker
        self.reviewer = reviewer
        self.decomposer = decomposer
        self.supervisor = supervisor
        self.project_id = project_id
        self.config = config or SchedulerConfig()
        self.clock = clock or SystemClock()
        self.sleep = sleep
        self.id_factory = id_factory
        self.repo_context = repo_context

        self._inflight: dict[str, asyncio.Task[None]] = {}
        self._abort_silently: set[str] = set()
        self._repair_hints: dict[str, str] = {}
        self._stop_kind: str | None = None
        self._stop_reason = ""
        self._stop_at: float | None = None
        self._consecutive_failures = 0
        self._interventions_applied = 0
        self._suppress_cancel_events = False

    # ------------------------------------------------------------------ public
    @property
    def stopping(self) -> bool:
        return self._stop_kind is not None

    def request_cancel(self, reason: str = "cancel requested") -> None:
        """In-process cancellation; the CLI writes the durable document instead."""
        self._begin_stop("cancel", reason)

    def observe_agent_run(self, run: AgentRun) -> None:
        """``AgentRuntime.record_run`` sink: persist the run + feed the supervisor."""
        if run.finished_at is None:
            event_type = EventType.AGENT_RUN_STARTED
        else:
            event_type = EventType.AGENT_RUN_FINISHED if run.ok else EventType.AGENT_RUN_FAILED
        stored = self.store.append(
            Event(
                project_id=self.project_id,
                type=event_type,
                task_id=run.task_id,
                actor=run.role,
                payload={"run": run.model_dump(mode="json")},
            )
        )
        self.supervisor.observe(stored)

    async def run(self) -> RunOutcome:
        """Dispatch until every task is terminal or the run is stopped."""
        if not self.graph.tasks:
            return self._outcome("DONE", "no tasks to run")
        while True:
            self._poll_stop_documents()
            if not self.stopping:
                if self._recover_orphans():
                    continue
                self._promote()
                self._aggregate()
                self._propagate_poison()
                await self._apply_supervisor()
            self._renew_leases()
            if not self.stopping:
                self._dispatch()
            if self._inflight:
                await self._wait_any()
                continue
            if self.stopping:
                self._cancel_remaining(self._stop_reason)
                return self._finish_run()
            if self.graph.all_terminal():
                return self._finish_run()
            if self._recover_orphans():
                continue
            progressed = self._promote() + self._aggregate() + self._propagate_poison()
            if progressed:
                continue
            if self._quarantine_stranded():
                continue
            return self._declare_deadlock()

    # --------------------------------------------------------------- emission
    def _emit(
        self,
        type_: EventType,
        task_id: str | None,
        payload: dict[str, Any] | None = None,
        *,
        actor: str = "scheduler",
    ) -> Event:
        event = Event(
            project_id=self.project_id,
            type=type_,
            task_id=task_id,
            actor=actor,
            payload=payload or {},
            created_at=self.clock.now(),
        )
        stored = self.store.append(event)
        self.supervisor.observe(stored)
        if task_id is not None:
            self._refresh(task_id)
        if self.config.log_events:
            log.info(
                json.dumps(
                    {
                        "ev": stored.type.value,
                        "seq": stored.seq,
                        "task": stored.task_id,
                        "at": stored.created_at.isoformat(),
                        "payload_keys": sorted(stored.payload),
                    },
                    sort_keys=True,
                )
            )
        return stored

    def _refresh(self, task_id: str) -> Task | None:
        task = self.store.get_task(task_id)
        if task is not None:
            self.graph.update(task)
        return task

    def _task(self, task_id: str) -> Task | None:
        return self._refresh(task_id)

    # ----------------------------------------------------------------- phase 1
    def _promote(self) -> int:
        """``PENDING -> READY`` for tasks whose dependencies are all DONE."""
        promoted = 0
        for task in self.graph.ready():
            self._emit(EventType.TASK_READY, task.id, {"dependencies": list(task.dependencies)})
            promoted += 1
        return promoted

    def _aggregate(self) -> int:
        """``SPLIT -> DONE|FAILED|CANCELLED`` once every child is terminal."""
        aggregated = 0
        for task in sorted(self.graph.tasks, key=lambda t: t.id):
            if task.status is not TaskStatus.SPLIT:
                continue
            children = self.graph.children_of(task.id)
            if not children or not all(c.is_terminal for c in children):
                continue
            statuses = {c.status for c in children}
            if statuses == {TaskStatus.DONE}:
                target, reason = TaskStatus.DONE, ""
            elif TaskStatus.CANCELLED in statuses and not statuses & {
                TaskStatus.FAILED,
                TaskStatus.QUARANTINED,
            }:
                target, reason = TaskStatus.CANCELLED, "all unfinished children were cancelled"
            else:
                bad = sorted(
                    c.id for c in children if c.status in {TaskStatus.FAILED, TaskStatus.QUARANTINED}
                )
                target, reason = TaskStatus.FAILED, f"child tasks failed: {', '.join(bad)}"
            self._emit(
                EventType.TASK_AGGREGATED,
                task.id,
                {"status": target.value, "reason": reason, "children": [c.id for c in children]},
            )
            if target is TaskStatus.FAILED:
                # Re-running an aggregate is meaningless: its children are dead.
                # Quarantining keeps the poison terminal instead of leaving a
                # FAILED node that can never be retried (and would look like a
                # deadlock afterwards).
                self._emit(
                    EventType.TASK_QUARANTINED,
                    task.id,
                    {"reason": f"aggregate failed: {reason}", "children": [c.id for c in children]},
                )
            aggregated += 1
        return aggregated

    def _propagate_poison(self) -> int:
        """Cancel everything that can never run because a dependency is dead."""
        cancelled = 0
        for task in self.graph.blocked_by_failure():
            current = self.graph.get(task.id)
            if current is None or current.status is not task.status:
                continue
            dead: str | None = None
            for dep_id in task.dependencies:
                dep = self.graph.get(dep_id)
                if dep is None:
                    dead = f"{dep_id}:missing"
                    break
                if can_never_complete(dep):
                    dead = f"{dep_id}:{dep.status.value}"
                    break
            self._emit(
                EventType.TASK_CANCELLED,
                task.id,
                {"reason": f"dependency cannot complete ({dead})", "poison": True},
            )
            cancelled += 1
        return cancelled

    # ----------------------------------------------------------------- phase 2
    def _dispatch(self) -> int:
        dispatched = 0
        while len(self._inflight) < self.config.max_concurrency:
            claimable = self.graph.claimable()
            if not claimable:
                break
            task = claimable[0]
            try:
                self.budget.check(task_id=task.id, estimated_tokens=task.est_tokens, new_task=True)
            except BudgetExceededError as exc:
                self._begin_stop("budget", str(exc))
                break
            claimed = self.store.claim_task(
                task.id,
                owner=self.worker.identity,
                lease_seconds=self.config.lease_seconds,
                now=self.clock.now(),
            )
            if claimed is None:
                self._task(task.id)  # lost the race: refresh and retry
                continue
            self.budget.note_task_started(task_id=task.id)
            for event in claimed:
                self.supervisor.observe(event)
            self._refresh(task.id)
            self._inflight[task.id] = asyncio.get_running_loop().create_task(
                self._guarded_execute(task.id)
            )
            dispatched += 1
        return dispatched

    async def _wait_any(self) -> None:
        pending = [fut for fut in self._inflight.values() if not fut.done()]
        if pending:
            await asyncio.wait(
                pending,
                timeout=self.config.tick_interval_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
        for task_id, fut in list(self._inflight.items()):
            if not fut.done():
                continue
            self._inflight.pop(task_id, None)
            if fut.cancelled():
                # `Task.exception()` re-raises CancelledError; a cancelled
                # in-flight call is expected during stop/cancel and must not
                # take the scheduler down with it (AC-05 P0-B).
                continue
            exc = fut.exception()
            if exc is not None:  # pragma: no cover - defensive
                log.error("task %s coroutine crashed: %r", task_id, exc)
        self._maybe_abort_inflight()

    async def _guarded_execute(self, task_id: str) -> None:
        try:
            await self._execute(task_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive net
            log.exception("unexpected failure executing %s", task_id)
            task = self._task(task_id)
            if task is not None and not task.is_terminal:
                await self._fail_task(
                    task_id,
                    f"internal scheduler error: {type(exc).__name__}: {exc}",
                    retryable=False,
                )

    async def _execute(self, task_id: str) -> None:
        task = self._task(task_id)
        if task is None or task.status is not TaskStatus.RUNNING:
            return
        try:
            result = await self.worker.execute(
                task,
                repo_context=self.repo_context,
                dependency_outputs=self._dependency_outputs(task),
                repair_hint=self._repair_hints.pop(task_id, ""),
            )
        except asyncio.CancelledError:
            if task_id not in self._abort_silently:
                current = self._task(task_id)
                if current is not None and current.status is TaskStatus.RUNNING:
                    self._emit(
                        EventType.TASK_CANCELLED,
                        task_id,
                        {"reason": self._stop_reason or "cancelled in flight"},
                    )
            raise
        except BudgetExceededError as exc:
            self._emit(
                EventType.TASK_CANCELLED,
                task_id,
                {"reason": f"budget exhausted: {exc}", "limit": exc.limit, "stop": "budget"},
            )
            self._begin_stop("budget", str(exc))
            return
        except PathViolationError as exc:
            await self._fail_task(task_id, f"path violation: {exc}", retryable=False)
            return
        except CircuitOpenError as exc:
            self._emit(
                EventType.NOTE,
                task_id,
                {"stage": "circuit_open", "error": str(exc), "summary": "circuit open; failing fast"},
            )
            await self._fail_task(task_id, f"circuit open: {exc}", retryable=True)
            return
        except Exception as exc:
            await self._fail_task(
                task_id, f"{type(exc).__name__}: {exc}", retryable=is_retryable(exc)
            )
            return

        for artifact in result.artifacts:
            self._emit(
                EventType.ARTIFACT_PRODUCED,
                task_id,
                {"artifact": artifact.model_dump(mode="json")},
            )

        if result.status == "success":
            await self._after_success(task_id, result)
        elif result.status == "blocked":
            await self._after_blocked(task_id, result)
        else:
            await self._fail_task(
                task_id,
                result.outcome.reason or result.summary or "worker reported failure",
                retryable=True,
            )

    async def _after_success(self, task_id: str, result: WorkerResult) -> None:
        task = self._task(task_id)
        if task is None or task.status is not TaskStatus.RUNNING:
            return
        if not self.config.review:
            self._emit(
                EventType.TASK_FINISHED,
                task_id,
                {"summary": result.summary[:500], "artifacts": [a.path for a in result.artifacts]},
            )
            self._consecutive_failures = 0
            return
        self._emit(EventType.REVIEW_STARTED, task_id, {"attempt": task.attempts})
        review = await self.reviewer.review(
            task,
            result.outcome,
            worker_identity=self.worker.identity,
            repair_round=task.rework_count,
        )
        validate_review(review)
        if review.verdict == "PASS":
            self._emit(
                EventType.REVIEW_APPROVED,
                task_id,
                {
                    "review": review.model_dump(mode="json"),
                    "artifacts": [a.path for a in result.artifacts],
                    "summary": result.summary[:500],
                },
            )
            self._consecutive_failures = 0
            return
        self._emit(EventType.REVIEW_REJECTED, task_id, {"review": review.model_dump(mode="json")})
        if task.rework_count < self.config.max_reworks:
            self._repair_hints[task_id] = review_to_repair_hint(review)
            self._emit(
                EventType.TASK_REWORK,
                task_id,
                {
                    "reason": (review.rationale or "rejected by reviewer")[:500],
                    "rework_count": task.rework_count + 1,
                    "review_id": review.id,
                },
            )
            return
        await self._fail_task(
            task_id,
            f"rework limit of {self.config.max_reworks} reached; last review: "
            f"{(review.rationale or 'rejected')[:200]}",
            retryable=False,
        )

    async def _after_blocked(self, task_id: str, result: WorkerResult) -> None:
        task = self._task(task_id)
        if task is None or task.status is not TaskStatus.RUNNING:
            return
        allowed, reason = decide_split(
            result.outcome,
            depth=task.depth,
            total_tasks=len(self.graph),
            bounds=self.decomposer.bounds,
            budget_pressure=self.budget.pressure(),
            max_reworks=self.config.max_reworks,
            rework_count=task.rework_count,
        )
        if not allowed:
            retryable = not reason.startswith(
                ("max_depth", "max_total_tasks", "budget exhausted", "rework budget")
            )
            await self._fail_task(
                task_id,
                f"worker blocked ({result.outcome.reason or 'no reason'}); split refused: {reason}",
                retryable=retryable,
            )
            return
        signal = result.outcome.reason or "worker reported the task is too large"
        try:
            plan = await self.decomposer.split(
                task,
                outcome=result.outcome,
                signal=signal,
                repo_context=self.repo_context,
                total_tasks=len(self.graph),
                id_factory=self.id_factory,
            )
        except DecompositionError as exc:
            await self._fail_task(task_id, f"decomposition failed: {exc}", retryable=False)
            return
        await self._insert_children(task, plan.children, plan.reason)

    async def _insert_children(self, parent: Task, children: list[Task], reason: str) -> None:
        """Validate the candidate DAG and insert parent+children atomically (C8).

        Bounds are re-checked here, against the *current* graph, because the
        decomposer's own check used a snapshot taken before its (awaited)
        provider call: two splits that were in flight together would otherwise
        both land and breach ``max_total_tasks`` (AC-05 P2-1).
        """
        total_now = len(self.graph.tasks)
        if total_now + len(children) > self.decomposer.bounds.max_total_tasks:
            await self._fail_task(
                parent.id,
                (
                    f"split refused at insert time: {total_now} existing + "
                    f"{len(children)} children exceeds max_total_tasks "
                    f"{self.decomposer.bounds.max_total_tasks}"
                ),
                retryable=False,
            )
            return
        if parent.depth + 1 > self.decomposer.bounds.max_depth:
            await self._fail_task(
                parent.id,
                f"split refused at insert time: depth {parent.depth + 1} exceeds max_depth",
                retryable=False,
            )
            return
        copies = {t.id: t.model_copy(deep=True) for t in self.graph.tasks}
        scratch = TaskGraph(list(copies.values()))
        for child in children:
            scratch.add(child.model_copy(deep=True))
        affected = scratch.rewire(parent.id, [c.id for c in children])
        try:
            scratch.validate(strict_parents=True)
        except GraphError as exc:
            self._emit(EventType.CYCLE_DETECTED, parent.id, {"stage": "split", "error": str(exc)})
            self._emit(EventType.VALIDATION_FAILED, parent.id, {"stage": "split", "error": str(exc)})
            await self._fail_task(
                parent.id, f"child DAG rejected: {exc}", retryable=False
            )
            return

        now = self.clock.now()
        events = [
            Event(
                project_id=self.project_id,
                type=EventType.TASK_CREATED,
                task_id=child.id,
                actor="scheduler",
                payload={"task": child.model_dump(mode="json")},
                created_at=now,
            )
            for child in children
        ]
        events.append(
            Event(
                project_id=self.project_id,
                type=EventType.TASK_SPLIT,
                task_id=parent.id,
                actor="scheduler",
                payload={
                    "children": [c.id for c in children],
                    "reason": reason[:500],
                    "source": "decomposer",
                },
                created_at=now,
            )
        )
        for dependent_id in affected:
            events.append(
                Event(
                    project_id=self.project_id,
                    type=EventType.TASK_UPDATED,
                    task_id=dependent_id,
                    actor="scheduler",
                    payload={"patch": {"dependencies": copies[dependent_id].dependencies}},
                    created_at=now,
                )
            )
        stored = self.store.append_many(events)
        for event in stored:
            self.supervisor.observe(event)
        for child in children:
            self.graph.add(child)
        for dependent_id in affected:
            self._refresh(dependent_id)
        self._refresh(parent.id)

    async def _fail_task(self, task_id: str, reason: str, *, retryable: bool) -> None:
        task = self._task(task_id)
        if task is None or task.is_terminal:
            return
        if task.status not in {TaskStatus.RUNNING, TaskStatus.REVIEW}:
            return
        self._emit(EventType.TASK_FAILED, task_id, {"reason": reason[:2000], "retryable": retryable})
        task = self._refresh(task_id)
        if task is None or task.status is not TaskStatus.FAILED:
            return
        if retryable and task.attempts < task.max_attempts and not self.stopping:
            delay = self._retry_delay(task.attempts)
            if delay > 0:
                await self.sleep(delay)
            self._emit(
                EventType.TASK_RETRIED,
                task_id,
                {"reason": reason[:500], "attempt": task.attempts},
            )
            return
        self._emit(
            EventType.TASK_QUARANTINED,
            task_id,
            {"reason": reason[:2000], "attempts": task.attempts},
        )
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.config.circuit_failure_threshold:
            self._begin_stop(
                "circuit",
                f"{self._consecutive_failures} consecutive task failures (circuit open)",
            )
        elif self.config.stop_on_task_failure:
            self._begin_stop("failure", f"task {task_id} quarantined")

    def _retry_delay(self, attempts: int) -> float:
        base = self.config.task_retry_base_delay
        if base <= 0:
            return 0.0
        delay = base * float(2 ** max(attempts - 1, 0))
        return float(min(delay, self.config.task_retry_max_delay))

    # ------------------------------------------------------------- supervision
    async def _apply_supervisor(self) -> None:
        interventions = self.supervisor.tick(
            graph=self.graph,
            project_id=self.project_id,
            budget_pressure=self.budget.pressure(),
        )
        handled = {
            intervention.finding.task_ids[0]
            for intervention in interventions
            if intervention.finding.task_ids
            and intervention.kind in {InterventionKind.SPLIT, InterventionKind.CANCEL}
        }
        for intervention in interventions:
            await self._apply_intervention(intervention)
        await self._enforce_liveness(handled)

    async def _apply_intervention(self, intervention: Intervention) -> None:
        task_id = intervention.finding.task_ids[0] if intervention.finding.task_ids else None
        self._emit(
            EventType.INTERVENTION_RAISED,
            task_id,
            {
                "id": intervention.id,
                "kind": intervention.kind.value,
                "finding": intervention.finding.model_dump(mode="json"),
                "detail": intervention.detail,
            },
            actor="supervisor",
        )
        applied = False
        if intervention.kind in {InterventionKind.SPLIT, InterventionKind.CANCEL} and task_id:
            applied = await self._apply_stuck_intervention(intervention, task_id)
        elif intervention.kind is InterventionKind.ESCALATE and intervention.finding.kind == "failure_storm":
            self._begin_stop("storm", intervention.finding.detail)
            applied = True
        # NOOP and RETRY are recorded only: RETRY retries are already governed
        # by the attempt policy, so claiming "applied" here would be dishonest.
        if applied:
            self._interventions_applied += 1
            self.supervisor.stats.applied += 1
        self._emit(
            EventType.INTERVENTION_APPLIED,
            task_id,
            {
                "id": intervention.id,
                "kind": intervention.kind.value,
                "applied": applied,
                "detail": intervention.detail,
            },
            actor="supervisor",
        )

    async def _apply_stuck_intervention(self, intervention: Intervention, task_id: str) -> bool:
        task = self._task(task_id)
        if task is None or task.status is not TaskStatus.RUNNING:
            return False
        self._abort_silently.add(task_id)
        await self._abort(task_id)
        self._abort_silently.discard(task_id)
        task = self._task(task_id)
        if task is None or task.status is not TaskStatus.RUNNING:
            return False
        if intervention.kind is InterventionKind.SPLIT:
            outcome = WorkerOutcome(
                status="blocked",
                summary="supervisor: no progress",
                reason=intervention.finding.detail,
                recommended_subtasks=[],
            )
            try:
                plan = await self.decomposer.split(
                    task,
                    outcome=outcome,
                    signal=intervention.finding.detail,
                    repo_context=self.repo_context,
                    total_tasks=len(self.graph),
                    id_factory=self.id_factory,
                )
            except DecompositionError as exc:
                await self._fail_task(task_id, f"supervisor split failed: {exc}", retryable=False)
                return True
            await self._insert_children(task, plan.children, f"supervisor: {plan.reason}")
            return True
        await self._fail_task(
            task_id,
            f"supervisor aborted the attempt: {intervention.finding.detail}",
            retryable=True,
        )
        return True

    async def _enforce_liveness(self, handled: set[str]) -> None:
        """Hard liveness backstop (AC-05 P1-3).

        The supervisor stops intervening once ``max_interventions_per_task`` is
        reached, but a provider that hangs forever would leave the run waiting
        forever.  Any task that is provably making no progress and was not
        handled this tick is therefore aborted and failed — attempt accounting
        (not an unbounded retry loop) decides whether it retries or is
        quarantined, so the run always converges.
        """
        for task_id in self.supervisor.stuck_task_ids(self.graph):
            if task_id in handled:
                continue
            task = self._task(task_id)
            if task is None or task.status is not TaskStatus.RUNNING:
                continue
            self._emit(
                EventType.NOTE,
                task_id,
                {
                    "stage": "liveness",
                    "summary": (
                        f"liveness backstop: {task_id} made no progress and no "
                        "supervisor intervention is available; aborting the attempt"
                    ),
                    "attempts": task.attempts,
                },
            )
            self._abort_silently.add(task_id)
            await self._abort(task_id)
            self._abort_silently.discard(task_id)
            refreshed = self._task(task_id)
            if refreshed is None or refreshed.status is not TaskStatus.RUNNING:
                continue
            await self._fail_task(
                task_id,
                "liveness backstop: provider call produced no progress",
                retryable=True,
            )

    async def _abort(self, task_id: str) -> None:
        fut = self._inflight.pop(task_id, None)
        if fut is None or fut.done():
            return
        fut.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await fut

    # ------------------------------------------------------------ cancellation
    def _poll_stop_documents(self) -> None:
        if self.stopping:
            return
        document = self.store.get_control(self.project_id, "cancel_request")
        if document:
            self._begin_stop("cancel", str(document.get("reason", "cancel requested")))

    def _begin_stop(self, kind: str, reason: str) -> None:
        if self._stop_kind is not None:
            return
        self._stop_kind = kind
        self._stop_reason = reason
        self._stop_at = self.clock.monotonic()
        self._emit(
            EventType.NOTE,
            None,
            {
                "stage": "stop",
                "kind": kind,
                "reason": reason[:500],
                "summary": f"run stopping: {kind} ({reason[:120]})",
            },
        )

    def _maybe_abort_inflight(self) -> None:
        if not self._inflight or self._stop_kind is None:
            return
        now = self.clock.monotonic()
        if self._stop_kind == "cancel":
            deadline = (self._stop_at or now) + self.config.cancel_grace_s
            if now < deadline:
                return
        elif self._stop_kind == "budget":
            if not self.config.cancel_inflight_on_budget:
                return
        else:
            return
        for fut in self._inflight.values():
            if not fut.done():
                fut.cancel()

    def _cancel_remaining(self, reason: str) -> int:
        """Drive every non-running task to a terminal status."""
        cancelled = 0
        for task in sorted(self.graph.active(), key=lambda t: t.id):
            if task.status is TaskStatus.RUNNING:
                continue
            if task.status is TaskStatus.FAILED:
                # FAILED is not terminal (it can retry); with dispatch stopped it
                # must be isolated so the run converges.
                self._emit(
                    EventType.TASK_QUARANTINED,
                    task.id,
                    {"reason": f"attempt abandoned: {reason}"[:2000]},
                )
                cancelled += 1
                continue
            if task.status not in _CANCELLABLE:
                continue
            self._emit(
                EventType.TASK_CANCELLED,
                task.id,
                {"reason": reason[:500], "stop": self._stop_kind or "deadlock"},
            )
            cancelled += 1
        return cancelled

    def _renew_leases(self) -> None:
        lease = self.config.lease_seconds
        if lease <= 0:
            return
        threshold = self.clock.now() + timedelta(seconds=lease / 2.0)
        for task in self.graph.active():
            if task.status is not TaskStatus.RUNNING:
                continue
            if task.lease_expires_at is None or task.lease_expires_at <= threshold:
                try:
                    self.store.renew_lease(task.id, lease_seconds=lease, now=self.clock.now())
                except StateError:  # pragma: no cover - raced with a transition
                    continue
                self._refresh(task.id)

    def _recover_orphans(self) -> bool:
        """Re-dispatch attempts that no coroutine in this process owns (FIND-014).

        A RUNNING/REVIEW task with no in-flight coroutine is proof that the
        previous process died mid-claim (or that a claim was never executed).
        Without this, the deadlock detector would see "no running, no ready" and
        destroy the whole run instead of resuming it.

        v0 runs one engine process per run; `agentcorp resume` is the documented
        takeover, so the recovery is forced rather than waiting for the lease.
        """
        changed = False
        for task in sorted(self.graph.tasks, key=lambda t: t.id):
            if task.status not in {TaskStatus.RUNNING, TaskStatus.REVIEW}:
                continue
            if task.id in self._inflight or task.id in self._abort_silently:
                continue
            self._emit(
                EventType.NOTE,
                task.id,
                {
                    "stage": "recovery",
                    "summary": (
                        f"orphaned {task.status.value} task {task.id} has no owner; "
                        "re-dispatching"
                    ),
                    "attempts": task.attempts,
                },
            )
            recovered = self.store.recover_task(
                task.id,
                reason="orphaned_attempt",
                now=self.clock.now(),
                force=True,
            )
            self._refresh(task.id)
            changed = True
            if not recovered:
                self._emit(
                    EventType.NOTE,
                    task.id,
                    {
                        "stage": "recovery",
                        "summary": f"orphaned task {task.id} could not be requeued (attempts exhausted)",
                    },
                )
        return changed

    def _quarantine_stranded(self) -> bool:
        """A FAILED task with no attempts left can never move; isolate it.

        Returns True when something changed, so the caller re-evaluates
        terminality before declaring a deadlock.
        """
        changed = False
        for task in sorted(self.graph.active(), key=lambda t: t.id):
            if task.status is not TaskStatus.FAILED:
                continue
            if task.attempts < task.max_attempts and not self.stopping:
                continue
            self._emit(
                EventType.TASK_QUARANTINED,
                task.id,
                {"reason": task.failure_reason or "attempts exhausted", "attempts": task.attempts},
            )
            changed = True
        return changed

    # --------------------------------------------------------------- finishing
    def _declare_deadlock(self) -> RunOutcome:
        unfinished = sorted(t.id for t in self.graph.active())
        reason = (
            f"deadlock: no running and no ready tasks, but {len(unfinished)} unfinished "
            f"({', '.join(unfinished[:8])})"
        )
        self._emit(
            EventType.DEADLOCK_DETECTED,
            None,
            {
                "tasks": unfinished,
                "running": 0,
                "ready": 0,
                "reason": reason,
                "summary": reason,
            },
        )
        intervention = self.supervisor.report_idle(
            graph=self.graph, project_id=self.project_id, reason=reason
        )
        task_id = intervention.finding.task_ids[0] if intervention.finding.task_ids else None
        self._emit(
            EventType.INTERVENTION_RAISED,
            task_id,
            {
                "id": intervention.id,
                "kind": intervention.kind.value,
                "finding": intervention.finding.model_dump(mode="json"),
                "detail": intervention.detail,
            },
            actor="supervisor",
        )
        self._emit(
            EventType.INTERVENTION_APPLIED,
            task_id,
            {
                "id": intervention.id,
                "kind": intervention.kind.value,
                "applied": True,
                "detail": intervention.detail,
            },
            actor="supervisor",
        )
        self._interventions_applied += 1
        self.supervisor.stats.applied += 1
        self._cancel_remaining("run deadlocked: no dispatchable work")
        return self._outcome("DEADLOCK", reason)

    def _finish_run(self) -> RunOutcome:
        counts = self.store.status_counts(self.project_id)
        any_poison = counts.get(TaskStatus.FAILED, 0) + counts.get(TaskStatus.QUARANTINED, 0)
        if self._stop_kind == "cancel":
            status, reason = "CANCELLED", self._stop_reason
        elif self._stop_kind == "budget":
            status, reason = "BUDGET_EXHAUSTED", self._stop_reason
        elif self._stop_kind in {"circuit", "failure", "storm"}:
            status, reason = "FAILED", self._stop_reason
        elif any_poison or counts.get(TaskStatus.CANCELLED, 0):
            status, reason = "FAILED", "one or more tasks did not complete"
        else:
            status, reason = "DONE", ""
        return self._outcome(status, reason)

    def _outcome(self, status: str, reason: str) -> RunOutcome:
        counts = self.store.status_counts(self.project_id)
        return RunOutcome(
            status=status,
            reason=reason,
            deadlock=status == "DEADLOCK",
            tasks_total=sum(counts.values()),
            tasks_done=counts.get(TaskStatus.DONE, 0),
            tasks_failed=counts.get(TaskStatus.FAILED, 0),
            tasks_quarantined=counts.get(TaskStatus.QUARANTINED, 0),
            tasks_cancelled=counts.get(TaskStatus.CANCELLED, 0),
            tasks_split=counts.get(TaskStatus.SPLIT, 0),
            agent_calls=self.budget.project.usage.calls,
            interventions=self._interventions_applied,
            supervisor=self.supervisor.snapshot(),
        )

    # ---------------------------------------------------------------- helpers
    def _dependency_outputs(self, task: Task) -> str:
        parts: list[str] = []
        for dep_id in task.dependencies:
            dep = self.store.get_task(dep_id)
            if dep is None:
                continue
            runs = [
                run
                for run in self.store.list_runs(self.project_id, task_id=dep_id)
                if run.role == "worker" and run.ok and run.finished_at is not None
            ]
            detail = runs[-1].response[:800] if runs else "(no agent output recorded)"
            parts.append(f"## {dep.id} — {dep.title} (status: {dep.status.value})\n{detail}")
        return "\n\n".join(parts)
