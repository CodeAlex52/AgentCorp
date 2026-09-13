"""Governance: detect stuck / repeated-failure / spinning tasks and intervene.

The supervisor is deliberately **pure**: it observes events and returns
:class:`~agentcorp.models.Intervention` objects; the scheduler emits the events
and performs the state changes.  That split is what makes "does not
false-positive" a unit test instead of an integration hope.

False-positive guard (C13, DEC-011): stuck detection keys off *progress*, not
elapsed time.  A task that has been running for an hour but whose agent calls
keep finishing is making progress and is never touched; the supervisor counts
those as `false_positive_guarded` so the protection is visible in the report.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

from .events import Event, EventType
from .graph import TaskGraph
from .models import (
    Intervention,
    InterventionKind,
    Requirement,
    Severity,
    SupervisorFinding,
    TaskStatus,
)
from .util import Clock, SystemClock

__all__ = ["Supervisor", "SupervisorConfig", "SupervisorStats"]


@dataclass(frozen=True)
class SupervisorConfig:
    #: No progress for this many seconds on a RUNNING task ⇒ STUCK.
    stuck_after_s: float = 120.0
    #: Rolling window used for failure counting.
    failure_window_s: float = 60.0
    #: Same task failing this often inside the window ⇒ REPEATED_FAILURES.
    repeated_failure_threshold: int = 3
    #: This many task failures across the run inside the window ⇒ FAILURE_STORM.
    storm_threshold: int = 4
    #: Same finding for the same task is suppressed for this long.
    intervention_cooldown_s: float = 30.0
    #: Hard cap of interventions per task per run.
    max_interventions_per_task: int = 2
    #: Report low-severity findings when a task shares no vocabulary with the PRD.
    drift_check: bool = False


@dataclass
class SupervisorStats:
    raised: int = 0
    applied: int = 0
    findings_by_kind: dict[str, int] = field(default_factory=dict)
    #: Times the naive elapsed-time heuristic would have fired but the task was
    #: provably making progress (the guard doing its job).
    false_positive_guarded: int = 0
    #: Interventions raised against a task that *was* progressing.  Structurally
    #: 0; the report asserts it rather than hardcoding a happy answer (F13).
    false_positives: int = 0


@dataclass
class _TaskTrace:
    last_progress_monotonic: float
    started_monotonic: float
    failures: deque[tuple[float, str]] = field(default_factory=lambda: deque(maxlen=32))
    interventions: int = 0
    last_intervention: dict[str, float] = field(default_factory=dict)


class Supervisor:
    """Progress-aware anomaly detection. No I/O, no timers, no side effects."""

    def __init__(
        self,
        config: SupervisorConfig | None = None,
        *,
        clock: Clock | None = None,
        requirement: Requirement | None = None,
        max_depth: int = 3,
    ) -> None:
        self.config = config or SupervisorConfig()
        self.clock = clock or SystemClock()
        self.requirement = requirement
        self.max_depth = max_depth
        self.stats = SupervisorStats()
        self._traces: dict[str, _TaskTrace] = {}
        self._failures: deque[tuple[float, str]] = deque(maxlen=256)
        self._storm_raised_at: float | None = None
        #: Tasks where the naive elapsed-time rule fired but progress vetoed it.
        self._guarded: set[str] = set()

    # --------------------------------------------------------------- observation
    def observe(self, event: Event) -> None:
        """Feed every persisted event through here (the scheduler calls this)."""
        now = self.clock.monotonic()
        task_id = event.task_id
        if task_id is None:
            return
        trace = self._traces.get(task_id)
        if event.type in {EventType.TASK_CREATED, EventType.TASK_READY}:
            self._trace(task_id, now)
            return
        if event.type in {EventType.TASK_CLAIMED, EventType.TASK_STARTED}:
            trace = self._trace(task_id, now)
            trace.started_monotonic = now
            trace.last_progress_monotonic = now
            return
        if event.type in {
            EventType.AGENT_RUN_STARTED,
            EventType.AGENT_RUN_FINISHED,
            EventType.AGENT_RUN_FAILED,
            EventType.ARTIFACT_PRODUCED,
            EventType.TASK_REWORK,
            EventType.REVIEW_STARTED,
        }:
            if trace is None:
                trace = self._trace(task_id, now)
            trace.last_progress_monotonic = now
            return
        if event.type is EventType.TASK_FAILED:
            if trace is None:
                trace = self._trace(task_id, now)
            reason = str(event.payload.get("reason", ""))[:160]
            trace.failures.append((now, reason))
            self._failures.append((now, task_id))
            trace.last_progress_monotonic = now
            return

    def seed_from_task(self, task: object) -> None:
        """Seed tracking for a task loaded from a previous process (resume)."""
        from .models import Task  # local import: avoid a cycle at module import

        if not isinstance(task, Task):
            return
        now = self.clock.monotonic()
        started_wall = task.started_at or task.claimed_at or task.updated_at
        age = max((self.clock.now() - started_wall).total_seconds(), 0.0)
        started_mono = now - age
        progress_wall = task.progress_at or task.started_at or task.updated_at
        progress_age = max((self.clock.now() - progress_wall).total_seconds(), 0.0)
        trace = self._trace(task.id, now)
        trace.started_monotonic = started_mono
        trace.last_progress_monotonic = now - progress_age

    # ------------------------------------------------------------------ ticking
    def tick(
        self,
        *,
        graph: TaskGraph,
        project_id: str,
        budget_pressure: float = 0.0,
    ) -> list[Intervention]:
        """Evaluate the run and return the interventions to apply."""
        now = self.clock.monotonic()
        out: list[Intervention] = []
        for task in sorted(graph.tasks, key=lambda t: t.id):
            if task.status is not TaskStatus.RUNNING:
                continue
            trace = self._trace(task.id, now)
            # Seed a task that was already running when we started (resume).
            if trace.started_monotonic == 0.0:
                self.seed_from_task(task)
            idle_for = now - trace.last_progress_monotonic
            if idle_for < self.config.stuck_after_s:
                # The guard: a long-running task that *is* making progress is
                # never intervened, even though the naive elapsed-time rule
                # would have fired.  Counted so tests/reports can prove it.
                if now - trace.started_monotonic >= self.config.stuck_after_s:
                    self._guarded.add(task.id)
                continue
            finding = SupervisorFinding(
                kind="stuck",
                severity=Severity.HIGH,
                detail=(
                    f"task {task.id} ({task.title[:60]}) made no progress for "
                    f"{idle_for:.1f}s (threshold {self.config.stuck_after_s:.1f}s, "
                    f"attempt {task.attempts}/{task.max_attempts})"
                ),
                task_ids=[task.id],
                evidence={
                    "project": project_id,
                    "idle_s": round(idle_for, 3),
                    "attempts": task.attempts,
                    "depth": task.depth,
                    "budget_pressure": round(budget_pressure, 3),
                },
            )
            kind = (
                InterventionKind.SPLIT
                if task.depth < self.max_depth and budget_pressure < 1.0
                else InterventionKind.CANCEL
            )
            intervention = self._intervention(kind, finding, now, task.id)
            if intervention is not None:
                out.append(intervention)

        out.extend(self._failure_interventions(graph, project_id, now))
        out.extend(self._drift_interventions(graph, now))
        self.stats.raised += len(out)
        return out

    def stuck_task_ids(self, graph: TaskGraph) -> list[str]:
        """RUNNING tasks that have made no progress for ``stuck_after_s``.

        Unlike :meth:`tick` this ignores the intervention budget/cooldown: it is
        the scheduler's liveness backstop input (AC-05 P1-3), so a hung provider
        can never leave the run waiting after the supervisor stops intervening.
        """
        now = self.clock.monotonic()
        out: list[str] = []
        for task in sorted(graph.tasks, key=lambda t: t.id):
            if task.status is not TaskStatus.RUNNING:
                continue
            trace = self._traces.get(task.id)
            if trace is None:
                self.seed_from_task(task)
                trace = self._traces.get(task.id)
            if trace is None:  # pragma: no cover - defensive
                continue
            if now - trace.last_progress_monotonic >= self.config.stuck_after_s:
                out.append(task.id)
        return out

    def report_idle(
        self,
        *,
        graph: TaskGraph,
        project_id: str,
        reason: str,
    ) -> Intervention:
        """Called by the scheduler when it detects a deadlock: record + escalate."""
        unfinished = [t.id for t in graph.active()]
        finding = SupervisorFinding(
            kind="idle",
            severity=Severity.CRITICAL,
            detail=f"no running or ready tasks but {len(unfinished)} unfinished: {reason}",
            task_ids=sorted(unfinished),
            evidence={
                "project": project_id,
                "unfinished": sorted(unfinished),
                "reason": reason,
                "at_monotonic": round(self.clock.monotonic(), 3),
            },
        )
        intervention = Intervention(
            kind=InterventionKind.ESCALATE,
            finding=finding,
            detail="escalate deadlocked run to the operator",
        )
        self.stats.raised += 1
        self._record_kind(finding.kind)
        return intervention

    # ------------------------------------------------------------------ helpers
    def _trace(self, task_id: str, now: float) -> _TaskTrace:
        trace = self._traces.get(task_id)
        if trace is None:
            trace = _TaskTrace(last_progress_monotonic=now, started_monotonic=now)
            self._traces[task_id] = trace
        return trace

    def _intervention(
        self,
        kind: InterventionKind,
        finding: SupervisorFinding,
        now: float,
        task_id: str,
    ) -> Intervention | None:
        trace = self._traces.setdefault(task_id, _TaskTrace(now, now))
        if trace.interventions >= self.config.max_interventions_per_task:
            return None
        last = trace.last_intervention.get(kind.value)
        if last is not None and now - last < self.config.intervention_cooldown_s:
            return None
        trace.interventions += 1
        trace.last_intervention[kind.value] = now
        self._record_kind(finding.kind)
        detail = {
            InterventionKind.SPLIT: "abort the attempt and decompose the task",
            InterventionKind.CANCEL: "abort the stalled attempt so the retry policy can act",
            InterventionKind.RETRY: "retry the task (governed by the attempt policy)",
        }.get(kind, "")
        return Intervention(kind=kind, finding=finding, detail=detail)

    def _failure_interventions(
        self, graph: TaskGraph, project_id: str, now: float
    ) -> list[Intervention]:
        out: list[Intervention] = []
        window = self.config.failure_window_s
        # Same task failing repeatedly.
        by_task: dict[str, list[float]] = defaultdict(list)
        for at, task_id in self._failures:
            if now - at <= window:
                by_task[task_id].append(at)
        for task_id, times in sorted(by_task.items()):
            if len(times) < self.config.repeated_failure_threshold:
                continue
            task = graph.get(task_id)
            finding = SupervisorFinding(
                kind="repeated_failures",
                severity=Severity.MEDIUM,
                detail=(
                    f"task {task_id} failed {len(times)} times in the last {window:.0f}s"
                    + (f" (status {task.status.value})" if task else "")
                ),
                task_ids=[task_id],
                evidence={
                    "project": project_id,
                    "failures": len(times),
                    "window_s": window,
                },
            )
            intervention = self._intervention(
                InterventionKind.RETRY, finding, now, task_id
            )
            if intervention is not None:
                out.append(intervention)
        # Many different tasks failing.
        recent = [t for at, t in self._failures if now - at <= window]
        storm_cooled_down = self._storm_raised_at is None or now - self._storm_raised_at >= window
        if len(recent) >= self.config.storm_threshold and storm_cooled_down:
                self._storm_raised_at = now
                finding = SupervisorFinding(
                    kind="failure_storm",
                    severity=Severity.HIGH,
                    detail=(
                        f"{len(recent)} task failures in the last {window:.0f}s — "
                        "the provider or the plan is unhealthy"
                    ),
                    task_ids=sorted(set(recent)),
                    evidence={
                        "project": project_id,
                        "failures": len(recent),
                        "window_s": window,
                    },
                )
                intervention = Intervention(
                    kind=InterventionKind.ESCALATE,
                    finding=finding,
                    detail="stop dispatching and fail the run with the storm evidence",
                )
                self._record_kind(finding.kind)
                out.append(intervention)
        return out

    def _drift_interventions(self, graph: TaskGraph, now: float) -> list[Intervention]:
        if not self.config.drift_check or self.requirement is None:
            return []
        out: list[Intervention] = []
        for task in sorted(graph.tasks, key=lambda t: t.id):
            if task.status is not TaskStatus.RUNNING:
                continue
            if self.requirement.covers(f"{task.title} {task.objective}"):
                continue
            finding = SupervisorFinding(
                kind="scope_drift",
                severity=Severity.LOW,
                detail=(
                    f"task {task.id} shares no vocabulary with the requirement "
                    f"(goal: {self.requirement.goal[:60]})"
                ),
                task_ids=[task.id],
                evidence={"keywords": self.requirement.keywords[:10]},
            )
            intervention = self._intervention(InterventionKind.NOOP, finding, now, task.id)
            if intervention is not None:
                out.append(intervention)
        return out

    def _record_kind(self, kind: str) -> None:
        self.stats.findings_by_kind[kind] = self.stats.findings_by_kind.get(kind, 0) + 1

    def snapshot(self) -> dict[str, object]:
        return {
            "raised": self.stats.raised,
            "applied": self.stats.applied,
            "by_kind": dict(self.stats.findings_by_kind),
            "false_positive_guarded": len(self._guarded),
            "false_positives": self.stats.false_positives,
            "guarded_tasks": sorted(self._guarded),
            "tracked_tasks": len(self._traces),
        }
