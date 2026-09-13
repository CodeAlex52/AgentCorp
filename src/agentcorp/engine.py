"""Engine facade: wire the pieces and run one PRD end to end.

``run_prd``            plan a fresh run from text; ``resume``            continue a
crashed run; ``status``/``graph``/``report`` expose the derived views the CLI
prints.  The engine owns run-level events (``RUN_STARTED``/``RUN_RESUMED``/
``RUN_FINISHED``) and the scheduler owns task-level events.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .budget import BudgetManager
from .chaos import ChaosConfig, ChaosProvider, chaos_provider
from .decomposer import Decomposer, DecompositionBounds
from .errors import GraphError, PermanentError
from .events import Event, EventType
from .graph import TaskGraph
from .models import AgentRun, BudgetLimits, Project, Requirement, Task
from .planner import plan_tasks
from .prd import heuristic_requirement, parse_requirement
from .providers.base import AgentProvider
from .providers.registry import build_provider
from .reliability import RetryPolicy
from .repo import analyze_repository
from .report import build_run_report
from .reviewer import Reviewer
from .runtime import AgentRuntime
from .scheduler import RunOutcome, Scheduler, SchedulerConfig
from .store import Store
from .supervisor import Supervisor, SupervisorConfig
from .util import Clock, SequentialIdFactory, Sleeper, SystemClock, new_id, system_sleep
from .worker import Worker

__all__ = ["Engine", "AgentCorpEngine", "EngineConfig", "RunSummary"]

log = logging.getLogger("agentcorp.engine")

_DEFAULT_RUN_STATUS = "FAILED"


@dataclass
class EngineConfig:
    """Everything the CLI can override, in one place."""

    repo_path: str = "."
    provider_spec: str = "mock"
    db_path: str = ".agentcorp/agentcorp.db"
    prd_mode: str = "provider"  # provider | heuristic
    budget: BudgetLimits = field(default_factory=BudgetLimits)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    supervisor: SupervisorConfig = field(default_factory=SupervisorConfig)
    bounds: DecompositionBounds = field(default_factory=DecompositionBounds)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    apply_writes: bool = True
    strict_touch_paths: bool = False
    review_use_provider: bool = True
    max_plan_tasks: int = 12
    #: Applied to every planned task (attempt budget per task).
    max_attempts: int = 3
    #: CLI overrides for the decomposition bounds.
    max_depth_override: int | None = None
    max_total_tasks_override: int | None = None
    #: ``resume`` re-dispatches interrupted tasks even when their lease has not
    #: expired.  That is the definition of a resume: the owning process is gone
    #: (FIND-014).  Turn it off to require an expired lease instead.
    resume_force_requeue: bool = True
    chaos: ChaosConfig | None = None
    deterministic: bool = False
    seed: int = 0

    def validated(self) -> EngineConfig:
        if self.prd_mode not in {"provider", "heuristic"}:
            msg = f"unknown prd_mode {self.prd_mode!r}; use 'provider' or 'heuristic'"
            raise PermanentError(msg)
        if self.scheduler.max_concurrency < 1:
            msg = "max_concurrency must be >= 1"
            raise PermanentError(msg)
        return self


@dataclass
class RunSummary:
    run_id: str
    status: str
    reason: str = ""
    report: dict[str, Any] = field(default_factory=dict)
    outcome: RunOutcome | None = None

    @property
    def exit_code(self) -> int:
        """SPEC C16: 0 ok, 1 business failure, 3 budget exhausted."""
        if self.status == "DONE":
            return 0
        if self.status == "BUDGET_EXHAUSTED":
            return 3
        return 1


class Engine:
    """One AgentCorp workspace: store + runtimes + scheduler wiring."""

    def __init__(
        self,
        config: EngineConfig | None = None,
        *,
        provider: AgentProvider | None = None,
        store: Store | None = None,
        clock: Clock | None = None,
        sleep: Sleeper = system_sleep,
        id_factory: Callable[[str], str] | None = None,
    ) -> None:
        self.config = (config or EngineConfig()).validated()
        self.clock = clock or SystemClock()
        self.sleep = sleep
        self.id_factory = id_factory or (SequentialIdFactory() if self.config.deterministic else new_id)
        self.store = store or Store(self.config.db_path)
        self._project_id: str | None = None
        self._scheduler: Scheduler | None = None
        self._repo_context: str = ""
        self.bounds = DecompositionBounds(
            max_depth=(
                self.config.max_depth_override
                if self.config.max_depth_override is not None
                else self.config.bounds.max_depth
            ),
            max_total_tasks=(
                self.config.max_total_tasks_override
                if self.config.max_total_tasks_override is not None
                else self.config.bounds.max_total_tasks
            ),
            max_subtasks=self.config.bounds.max_subtasks,
        )
        # Fail-closed default (FIND-006): if the operator set no task ceiling,
        # align the budget with the decomposition bound so a run can never grow
        # past `max_total_tasks` dispatches, and the ceiling is reported as a
        # real budget dimension rather than an absent one.
        if self.config.budget.max_tasks is None:
            self.config.budget = self.config.budget.model_copy(
                update={"max_tasks": self.bounds.max_total_tasks}
            )

        self.provider = provider or build_provider(self.config.provider_spec)
        if self.config.chaos is not None and not isinstance(self.provider, ChaosProvider):
            self.provider = chaos_provider(self.provider, self.config.chaos)
        self.budget = BudgetManager(
            self.config.budget,
            emit=self._emit_from_budget,
            clock=self.clock,
        )
        self.worker_runtime = self._build_runtime(role="worker")
        # C12: the reviewer gets its own runtime instance (separate circuit
        # breaker and bookkeeping) even though it shares the provider transport
        # and the project budget.
        self.reviewer_runtime = self._build_runtime(role="reviewer")

    # ------------------------------------------------------------------ wiring
    def _build_runtime(self, *, role: str) -> AgentRuntime:  # noqa: ARG002 - role documents intent at call sites
        return AgentRuntime(
            self.provider,
            retry=self.config.retry,
            budget=self.budget,
            emit=self._emit_runtime_note,
            record_run=self._record_run,
            clock=self.clock,
            sleep=self.sleep,
        )

    def _emit_from_budget(self, type_: EventType, task_id: str | None, payload: dict[str, Any]) -> None:
        if self._project_id is None:
            return
        self.store.append(
            Event(
                project_id=self._project_id,
                type=type_,
                task_id=task_id,
                actor="budget",
                payload=payload,
                created_at=self.clock.now(),
            )
        )

    def _emit_runtime_note(self, type_: EventType, task_id: str | None, payload: dict[str, Any]) -> None:
        if self._project_id is None:
            return
        self.store.append(
            Event(
                project_id=self._project_id,
                type=type_,
                task_id=task_id,
                actor="runtime",
                payload=payload,
                created_at=self.clock.now(),
            )
        )

    def _record_run(self, run: AgentRun) -> None:
        """``AgentRuntime.record_run``: persist the run, feed the supervisor."""
        if self._scheduler is not None:
            self._scheduler.observe_agent_run(run)
            return
        if self._project_id is None:
            return
        event_type = (
            EventType.AGENT_RUN_STARTED if run.finished_at is None
            else (EventType.AGENT_RUN_FINISHED if run.ok else EventType.AGENT_RUN_FAILED)
        )
        self.store.append(
            Event(
                project_id=self._project_id,
                type=event_type,
                task_id=run.task_id,
                actor=run.role,
                payload={"run": run.model_dump(mode="json")},
                created_at=self.clock.now(),
            )
        )

    def _emit(
        self,
        project_id: str,
        type_: EventType,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
        *,
        actor: str = "engine",
    ) -> Event:
        return self.store.append(
            Event(
                project_id=project_id,
                type=type_,
                task_id=task_id,
                actor=actor,
                payload=payload or {},
                created_at=self.clock.now(),
            )
        )

    # ------------------------------------------------------------------ public
    def request_cancel(self, reason: str = "cancelled by operator") -> str | None:
        """Durable cancellation (DEC-012): visible to every process on this DB."""
        project_id = self._project_id or self._latest_project_id()
        if project_id is None:
            return None
        self.store.put_control(project_id, "cancel_request", {"reason": reason})
        if self._scheduler is not None:
            self._scheduler.request_cancel(reason)
        return project_id

    async def run_prd(
        self,
        raw_text: str,
        *,
        repo_path: str | Path | None = None,
        run_name: str | None = None,
        run_id: str | None = None,
    ) -> RunSummary:
        """Plan and execute a fresh run."""
        repo = Path(repo_path or self.config.repo_path).expanduser().resolve()
        project_id = run_id or self.id_factory("RUN")
        self._project_id = project_id
        project = Project(
            id=project_id,
            name=run_name or repo.name or project_id,
            repo_path=str(repo),
        )
        self._emit(project_id, EventType.PROJECT_CREATED, payload={"project": project.model_dump(mode="json")})

        try:
            requirement = await self._obtain_requirement(raw_text, project_id)
        except Exception as exc:
            return self._fail_run(project_id, f"PRD parsing failed: {type(exc).__name__}: {exc}")
        self._emit(
            project_id,
            EventType.REQUIREMENT_PARSED,
            payload={"requirement": requirement.model_dump(mode="json")},
        )

        try:
            context = analyze_repository(repo, keywords=requirement.keywords)
        except PermanentError as exc:
            return self._fail_run(project_id, f"repository analysis failed: {exc}")
        self._emit(project_id, EventType.REPO_ANALYZED, payload={"context": context.model_dump(mode="json")})
        self._repo_context = context.summary()

        try:
            tasks = await plan_tasks(
                self.worker_runtime,
                requirement,
                self._repo_context,
                project_id=project_id,
                max_tasks=self.config.max_plan_tasks,
                id_factory=self.id_factory,
            )
        except Exception as exc:
            return self._fail_run(project_id, f"planning failed: {type(exc).__name__}: {exc}")

        for task in tasks:
            task.max_attempts = max(self.config.max_attempts, 1)
        self._emit(
            project_id,
            EventType.PLAN_CREATED,
            payload={
                "tasks": len(tasks),
                "requirement_id": requirement.id,
                "max_depth": max((t.depth for t in tasks), default=0),
            },
        )
        self.store.append_many(
            [
                Event(
                    project_id=project_id,
                    type=EventType.TASK_CREATED,
                    task_id=task.id,
                    actor="planner",
                    payload={"task": task.model_dump(mode="json")},
                    created_at=self.clock.now(),
                )
                for task in tasks
            ]
        )

        graph = TaskGraph(tasks)
        try:
            graph.validate(strict_parents=True)
        except GraphError as exc:
            self._emit(project_id, EventType.VALIDATION_FAILED, payload={"stage": "plan", "error": str(exc)})
            return self._fail_run(project_id, f"plan is not a valid DAG: {exc}")

        if self.config.deterministic:
            self.budget.reseed(started_monotonic=self.clock.monotonic(), tasks_started=0)
        self._emit(
            project_id,
            EventType.RUN_STARTED,
            payload={
                "run_id": project_id,
                "started_at": self.clock.now().isoformat(),
                "repo": str(repo),
                "tasks": len(tasks),
                "budget": self.config.budget.model_dump(mode="json"),
                "config": {
                    "max_concurrency": self.config.scheduler.max_concurrency,
                    "review": self.config.scheduler.review,
                    "apply_writes": self.config.apply_writes,
                },
            },
        )
        return await self._execute_run(project_id, graph, requirement, resumed=False)

    async def resume(self, run_id: str) -> RunSummary:
        """Continue a run whose process died (C5): DONE tasks are never re-run."""
        project = self.store.resolve_project(run_id)
        if project is None:
            msg = f"unknown run {run_id!r}; use `agentcorp status` to list runs"
            raise PermanentError(msg)
        project_id = project.id
        self._project_id = project_id
        tasks = self.store.list_tasks(project_id)
        if not tasks:
            msg = f"run {project_id!r} has no tasks recorded"
            raise PermanentError(msg)
        graph = TaskGraph(tasks)
        try:
            graph.validate(strict_parents=True)
        except GraphError as exc:
            self._emit(project_id, EventType.VALIDATION_FAILED, payload={"stage": "resume", "error": str(exc)})
            return self._fail_run(project_id, f"stored graph is invalid: {exc}")

        # Rehydrate the budget from the projection and the run's original start.
        usage = self.store.usage_for_project(project_id)
        by_task = self.store.usage_by_task(project_id)
        started_at = self._run_started_at(project_id)
        elapsed = max((self.clock.now() - started_at).total_seconds(), 0.0) if started_at else 0.0
        started_monotonic = self.clock.monotonic() - elapsed
        tasks_started = sum(
            1
            for event in self.store.list_events(project_id)
            if event.type is EventType.TASK_STARTED
        )
        self.budget.reseed(
            usage=usage,
            by_task=by_task,
            started_monotonic=started_monotonic,
            tasks_started=tasks_started,
        )

        # Crash recovery for interrupted attempts (DEC-006 / FIND-014):
        # RUNNING *and* REVIEW tasks are recovered.  By default the lease is not
        # consulted — `resume` means the previous process is gone — but operators
        # can demand an expired lease with `--no-force-requeue`.
        now = self.clock.now()
        requeued: list[str] = []
        quarantined: list[str] = []
        for task in self.store.stale_running(now, force=self.config.resume_force_requeue):
            self._emit(
                project_id,
                EventType.NOTE,
                task_id=task.id,
                payload={
                    "stage": "recovery",
                    "summary": (
                        f"interrupted task {task.id} (status {task.status.value}, "
                        f"attempts {task.attempts}) is being recovered"
                    ),
                    "attempts": task.attempts,
                },
            )
            recovered = self.store.recover_task(
                task.id,
                reason="interrupted_by_crash",
                now=self.clock.now(),
                force=self.config.resume_force_requeue,
            )
            if recovered:
                requeued.append(task.id)
            else:
                quarantined.append(task.id)
        # Stale unfinished agent runs are evidence, not state.
        for run in self.store.unfinished_runs(project_id):
            self._emit(
                project_id,
                EventType.NOTE,
                task_id=run.task_id,
                payload={
                    "stage": "recovery",
                    "summary": f"agent run {run.id} never reported; treating as crashed",
                    "run_id": run.id,
                },
            )

        requirement = self.store.get_requirement(project_id)
        self._emit(
            project_id,
            EventType.RUN_RESUMED,
            payload={
                "run_id": project_id,
                "started_at": (started_at.isoformat() if started_at else self.clock.now().isoformat()),
                "requeued": requeued,
                "quarantined": quarantined,
                "tasks": len(tasks),
            },
        )
        # Refresh the graph after recovery transitions.
        graph = TaskGraph(self.store.list_tasks(project_id))
        return await self._execute_run(project_id, graph, requirement, resumed=True)

    def status(self, run_id: str | None = None, *, events: int = 10) -> dict[str, Any]:
        project = self.store.resolve_project(run_id)
        if project is None:
            return {"error": f"unknown run {run_id!r}"}
        tasks = self.store.list_tasks(project.id)
        counts = self.store.status_counts(project.id)
        recent = self.store.list_events(project.id)[-max(events, 0) :]
        return {
            "run_id": project.id,
            "name": project.name,
            "status": project.status,
            "run_summary": self.store.last_run_summary(project.id),
            "repo_path": project.repo_path,
            "tasks": {status.value: count for status, count in counts.items() if count},
            "tasks_total": len(tasks),
            "budget": self.store.get_document(project.id, "budget"),
            "usage": self.store.usage_for_project(project.id).model_dump(mode="json"),
            "recent_events": [event.compact() for event in recent],
            "unfinished_runs": [run.id for run in self.store.unfinished_runs(project.id)],
        }

    def graph_view(self, run_id: str | None = None) -> dict[str, Any]:
        project = self.store.resolve_project(run_id)
        if project is None:
            return {"error": f"unknown run {run_id!r}"}
        graph = TaskGraph(self.store.list_tasks(project.id))
        return {
            "run_id": project.id,
            "ascii": graph.to_ascii(),
            "mermaid": graph.to_mermaid(),
            "dot": graph.to_dot(),
            "stats": graph.stats().__dict__,
            "critical_path": graph.critical_path(include_done=True),
        }

    def report(self, run_id: str | None = None) -> dict[str, Any]:
        project = self.store.resolve_project(run_id)
        if project is None:
            return {"error": f"unknown run {run_id!r}"}
        return build_run_report(self.store, project.id)

    async def aclose(self) -> None:
        await self.worker_runtime.aclose()
        self.store.close()

    # ----------------------------------------------------------------- private
    async def _obtain_requirement(self, raw_text: str, project_id: str) -> Requirement:
        if self.config.prd_mode == "heuristic":
            return heuristic_requirement(raw_text)
        return await parse_requirement(self.worker_runtime, raw_text, project_id=project_id)

    async def _execute_run(
        self,
        project_id: str,
        graph: TaskGraph,
        requirement: Requirement | None,
        *,
        resumed: bool,
    ) -> RunSummary:
        supervisor = Supervisor(
            self.config.supervisor,
            clock=self.clock,
            requirement=requirement,
            max_depth=self.bounds.max_depth,
        )
        if resumed:
            for task in graph.active():
                supervisor.seed_from_task(task)
        project = self.store.get_project(project_id)
        repo_root = project.repo_path if project else self.config.repo_path
        worker = Worker(
            self.worker_runtime,
            write_root=repo_root,
            project_id=project_id,
            apply_writes=self.config.apply_writes,
            strict_touch_paths=self.config.strict_touch_paths,
        )
        reviewer = Reviewer(
            self.reviewer_runtime,
            project_id=project_id,
            use_provider=self.config.review_use_provider,
        )
        decomposer = Decomposer(
            self.worker_runtime,
            bounds=self.bounds,
            project_id=project_id,
        )
        scheduler = Scheduler(
            store=self.store,
            graph=graph,
            budget=self.budget,
            worker=worker,
            reviewer=reviewer,
            decomposer=decomposer,
            supervisor=supervisor,
            project_id=project_id,
            config=self.config.scheduler,
            clock=self.clock,
            sleep=self.sleep,
            id_factory=self.id_factory,
            repo_context=self._repo_context,
        )
        self._scheduler = scheduler
        try:
            outcome = await scheduler.run()
        finally:
            self._scheduler = None
        started_at = self._run_started_at(project_id) or self.clock.now()
        wall_ms = max(int((self.clock.now() - started_at).total_seconds() * 1000), 0)
        self._emit(
            project_id,
            EventType.RUN_FINISHED,
            payload={
                "run_id": project_id,
                "status": outcome.status,
                "reason": outcome.reason[:1000],
                "wall_ms": wall_ms,
                "tasks": {
                    "total": outcome.tasks_total,
                    "done": outcome.tasks_done,
                    "failed": outcome.tasks_failed,
                    "quarantined": outcome.tasks_quarantined,
                    "cancelled": outcome.tasks_cancelled,
                    "split": outcome.tasks_split,
                },
                "agent_calls": outcome.agent_calls,
                "interventions": outcome.interventions,
                "supervisor": outcome.supervisor,
            },
        )
        report = build_run_report(self.store, project_id, graph=graph)
        self.store.put_derived(project_id, "run_report", report)
        log.info(
            "run %s finished status=%s tasks=%d/%d calls=%d",
            project_id,
            outcome.status,
            outcome.tasks_done,
            outcome.tasks_total,
            outcome.agent_calls,
        )
        return RunSummary(
            run_id=project_id,
            status=outcome.status,
            reason=outcome.reason,
            report=report,
            outcome=outcome,
        )

    def _fail_run(self, project_id: str, reason: str) -> RunSummary:
        self._emit(
            project_id,
            EventType.RUN_FINISHED,
            payload={"run_id": project_id, "status": _DEFAULT_RUN_STATUS, "reason": reason[:1000]},
        )
        report = build_run_report(self.store, project_id)
        self.store.put_derived(project_id, "run_report", report)
        return RunSummary(run_id=project_id, status=_DEFAULT_RUN_STATUS, reason=reason, report=report)

    def _run_started_at(self, project_id: str) -> Any:
        """Wall-clock start of the run (RUN_STARTED / RUN_RESUMED payload)."""
        from datetime import datetime

        for event in self.store.list_events(project_id):
            if event.type in {EventType.RUN_STARTED, EventType.RUN_RESUMED}:
                payload_start = event.payload.get("started_at")
                if payload_start:
                    return datetime.fromisoformat(str(payload_start))
                return event.created_at
        return None

    def _latest_project_id(self) -> str | None:
        project = self.store.latest_project()
        return project.id if project else None


#: Alias kept for readers who prefer an explicit product-style name.
AgentCorpEngine = Engine


def task_counts(tasks: list[Task]) -> dict[str, int]:
    """Small helper shared by the CLI and tests."""
    counts: dict[str, int] = {}
    for task in tasks:
        counts[task.status.value] = counts.get(task.status.value, 0) + 1
    return counts
