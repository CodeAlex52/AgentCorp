"""Domain model.

Everything the orchestrator moves around is defined here as a Pydantic model so
that it can be (a) validated at the boundary, (b) serialised into the event log,
and (c) reconstructed after a restart.  These objects are *data*: behaviour lives
in the engine modules (planner, scheduler, worker, reviewer, supervisor).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .util import new_id, utcnow

__all__ = [
    "TaskStatus",
    "TaskKind",
    "RiskLevel",
    "TERMINAL_STATUSES",
    "ACTIVE_STATUSES",
    "AcceptanceCriterion",
    "Task",
    "Requirement",
    "RepositoryContext",
    "Usage",
    "BudgetLimits",
    "BudgetSnapshot",
    "WorkerOutcome",
    "FileWrite",
    "ReviewIssue",
    "Review",
    "AgentRun",
    "Artifact",
    "Project",
    "SupervisorFinding",
    "Intervention",
    "InterventionKind",
    "Severity",
]


def _new(prefix: str) -> str:
    return new_id(prefix)


class TaskStatus(StrEnum):
    """Lifecycle of a task node.

    ``PENDING`` is the only non-terminal status a task can be *created* in.
    ``READY`` is deliberately absent: readiness is derived from the graph
    (all dependencies ``DONE``), never stored, so it cannot drift.
    """

    PENDING = "pending"
    RUNNING = "running"
    BLOCKED = "blocked"
    FAILED = "failed"
    DONE = "done"
    SPLIT = "split"  # decomposed into children; terminal for the parent
    CANCELLED = "cancelled"  # cancelled by the supervisor


TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.SPLIT, TaskStatus.CANCELLED}
)
ACTIVE_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.RUNNING, TaskStatus.PENDING})


class TaskKind(StrEnum):
    RESEARCH = "research"
    IMPLEMENTATION = "implementation"
    TEST = "test"
    REFACTOR = "refactor"
    DOCS = "docs"
    REVIEW = "review"
    INTEGRATION = "integration"
    VERIFICATION = "verification"
    FIX = "fix"


class RiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Severity(StrEnum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AcceptanceCriterion(BaseModel):
    """A single, checkable statement about what 'done' means.

    ``verification`` tells the reviewer *how* to check it, which is what makes
    'acceptance criteria are not verifiable' a machine-detectable condition
    (and therefore a decomposition trigger).
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: _new("AC"))
    statement: str
    verification: Literal["test", "command", "static", "manual", "review"] = "review"
    command: str | None = None
    expected: str | None = None

    @property
    def is_machine_verifiable(self) -> bool:
        return self.verification in {"test", "command", "static"} and bool(
            self.command or self.verification == "static"
        )


class Task(BaseModel):
    """One node of the delivery DAG."""

    model_config = ConfigDict(validate_assignment=True)

    id: str = Field(default_factory=lambda: _new("T"))
    title: str
    objective: str = ""
    kind: TaskKind = TaskKind.IMPLEMENTATION
    status: TaskStatus = TaskStatus.PENDING
    dependencies: list[str] = Field(default_factory=list)
    owner: str | None = None  # agent role that owns this task
    inputs: list[str] = Field(default_factory=list)
    expected_output: str = ""
    acceptance_criteria: list[AcceptanceCriterion] = Field(default_factory=list)
    risk: RiskLevel = RiskLevel.MEDIUM
    parent_id: str | None = None
    depth: int = 0
    attempts: int = 0
    max_attempts: int = 3
    #: Repo paths this task is expected to touch.  The scheduler refuses to run
    #: two tasks with overlapping ``touch_paths`` concurrently.
    touch_paths: list[str] = Field(default_factory=list)
    artifacts: list[str] = Field(default_factory=list)
    blocked_reason: str | None = None
    failure_reason: str | None = None
    priority: int = 50  # higher runs first when several tasks are ready
    est_tokens: int = 0
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @model_validator(mode="after")
    def _default_objective(self) -> Task:
        """An empty objective is almost always a bug; fall back to the title so
        the prompt the worker receives is never blank."""
        if not self.objective.strip():
            self.objective = self.title
        return self

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def is_leaf(self) -> bool:
        return self.status is not TaskStatus.SPLIT

    def to_prompt_block(self) -> str:
        """Compact, model-facing rendering of this task."""
        acs = (
            "\n".join(
                f"  - [{ac.verification}] {ac.statement}"
                + (f" (cmd: {ac.command})" if ac.command else "")
                for ac in self.acceptance_criteria
            )
            or "  - (none declared)"
        )
        return (
            f"TASK: {self.id} | kind={self.kind.value} | risk={self.risk.value}\n"
            f"TITLE: {self.title}\n"
            f"OBJECTIVE: {self.objective}\n"
            f"EXPECTED OUTPUT: {self.expected_output}\n"
            f"ACCEPTANCE CRITERIA:\n{acs}\n"
            f"TOUCH PATHS: {', '.join(self.touch_paths) or '(unspecified)'}"
        )


class Requirement(BaseModel):
    """Structured form of the natural-language PRD."""

    id: str = Field(default_factory=lambda: _new("REQ"))
    raw_text: str
    goal: str
    deliverables: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    acceptance_criteria: list[AcceptanceCriterion] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    risk: RiskLevel = RiskLevel.MEDIUM
    created_at: datetime = Field(default_factory=utcnow)

    def covers(self, text: str) -> bool:
        """Does ``text`` mention any of this requirement's vocabulary?

        Used by the supervisor for PRD-drift detection: a task that shares no
        vocabulary with the requirement is a scope-creep suspect.
        """
        if not self.keywords:
            return True
        lowered = text.lower()
        return any(k in lowered for k in self.keywords)


class RepositoryContext(BaseModel):
    """What the analyzer learned about the target repository."""

    root: str
    languages: dict[str, int] = Field(default_factory=dict)  # language -> file count
    manifests: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    entry_points: list[str] = Field(default_factory=list)
    test_paths: list[str] = Field(default_factory=list)
    docs_paths: list[str] = Field(default_factory=list)
    relevant_files: list[str] = Field(default_factory=list)
    conventions: list[str] = Field(default_factory=list)
    tree: str = ""
    file_count: int = 0
    total_bytes: int = 0
    is_git_repo: bool = False
    notes: list[str] = Field(default_factory=list)

    def summary(self, max_chars: int = 4000) -> str:
        """Token-bounded digest handed to the agents."""
        parts = [
            f"REPOSITORY: {self.root}",
            f"languages: {', '.join(f'{k}({v})' for k, v in self.languages.items()) or 'unknown'}",
            f"manifests: {', '.join(self.manifests) or 'none'}",
            f"entry_points: {', '.join(self.entry_points) or 'none'}",
            f"tests: {', '.join(self.test_paths) or 'none'}",
            f"conventions: {'; '.join(self.conventions) or 'none observed'}",
        ]
        if self.dependencies:
            parts.append(f"key dependencies: {', '.join(self.dependencies[:25])}")
        if self.relevant_files:
            parts.append("likely relevant files:\n" + "\n".join(f"  - {p}" for p in self.relevant_files[:20]))
        if self.tree:
            parts.append("tree:\n" + self.tree)
        text = "\n".join(parts)
        if len(text) > max_chars:
            text = text[: max_chars - 40] + "\n... [repository context truncated]"
        return text


class Usage(BaseModel):
    """Resource accounting for one or more agent calls."""

    model_config = ConfigDict(validate_assignment=True)

    tokens_in: int = 0
    tokens_out: int = 0
    calls: int = 0
    cost_usd: float = 0.0
    duration_s: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.tokens_in + self.tokens_out

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            tokens_in=self.tokens_in + other.tokens_in,
            tokens_out=self.tokens_out + other.tokens_out,
            calls=self.calls + other.calls,
            cost_usd=round(self.cost_usd + other.cost_usd, 6),
            duration_s=round(self.duration_s + other.duration_s, 4),
        )

    def add_in_place(self, other: Usage) -> None:
        self.tokens_in += other.tokens_in
        self.tokens_out += other.tokens_out
        self.calls += other.calls
        self.cost_usd = round(self.cost_usd + other.cost_usd, 6)
        self.duration_s = round(self.duration_s + other.duration_s, 4)


class BudgetLimits(BaseModel):
    """Ceilings.  ``None`` means unbounded for that dimension."""

    model_config = ConfigDict(frozen=True)

    max_tokens: int | None = None
    max_cost_usd: float | None = None
    max_agent_calls: int | None = None
    max_tokens_per_task: int | None = None


class BudgetSnapshot(BaseModel):
    limits: BudgetLimits = Field(default_factory=BudgetLimits)
    used: Usage = Field(default_factory=Usage)

    @property
    def remaining_tokens(self) -> int | None:
        if self.limits.max_tokens is None:
            return None
        return max(self.limits.max_tokens - self.used.total_tokens, 0)

    @property
    def remaining_cost_usd(self) -> float | None:
        if self.limits.max_cost_usd is None:
            return None
        return max(self.limits.max_cost_usd - self.used.cost_usd, 0.0)

    @property
    def exhausted(self) -> bool:
        lim, used = self.limits, self.used
        if lim.max_tokens is not None and used.total_tokens >= lim.max_tokens:
            return True
        if lim.max_cost_usd is not None and used.cost_usd >= lim.max_cost_usd:
            return True
        return lim.max_agent_calls is not None and used.calls >= lim.max_agent_calls


class FileWrite(BaseModel):
    """A concrete file mutation proposed by a worker."""

    model_config = ConfigDict(frozen=True)

    path: str
    content: str
    mode: Literal["write", "append"] = "write"


class WorkerOutcome(BaseModel):
    """The worker's structured reply. This *is* the agent protocol."""

    status: Literal["success", "blocked", "failed"]
    summary: str = ""
    files: list[FileWrite] = Field(default_factory=list)
    diff: str | None = None
    tests_run: list[str] = Field(default_factory=list)
    tests_passed: bool | None = None
    evidence: list[str] = Field(default_factory=list)
    #: Populated when status == "blocked": what the worker learned and what it
    #: thinks the task should be split into.
    reason: str | None = None
    new_information: list[str] = Field(default_factory=list)
    recommended_subtasks: list[str] = Field(default_factory=list)
    followups: list[str] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    raw: str = ""

    @model_validator(mode="after")
    def _check(self) -> WorkerOutcome:
        if self.status == "blocked" and not self.reason:
            self.reason = "worker reported blocked without a reason"
        return self


class ReviewIssue(BaseModel):
    model_config = ConfigDict(frozen=True)

    severity: Severity
    message: str
    file: str | None = None
    line: int | None = None
    criterion_id: str | None = None


class Review(BaseModel):
    """Reviewer verdict for one worker attempt."""

    id: str = Field(default_factory=lambda: _new("RV"))
    task_id: str
    run_id: str | None = None
    verdict: Literal["PASS", "REJECT"]
    score: float = 1.0  # 0..1
    issues: list[ReviewIssue] = Field(default_factory=list)
    checks: dict[str, bool] = Field(default_factory=dict)
    rationale: str = ""
    reviewer: str = "deterministic"
    created_at: datetime = Field(default_factory=utcnow)

    @property
    def blocking_issues(self) -> list[ReviewIssue]:
        return [i for i in self.issues if i.severity in {Severity.HIGH, Severity.CRITICAL}]

    @property
    def must_fix(self) -> bool:
        return self.verdict == "REJECT" and bool(self.blocking_issues)


class AgentRun(BaseModel):
    """One provider call, recorded for replay/budget/benchmark."""

    id: str = Field(default_factory=lambda: _new("RUN"))
    project_id: str
    task_id: str | None = None
    role: str = "worker"
    provider: str
    model: str
    attempt: int = 1
    request_hash: str = ""
    response_hash: str = ""
    prompt: str = ""
    response: str = ""
    usage: Usage = Field(default_factory=Usage)
    error: str | None = None
    error_type: str | None = None
    chaos_injections: list[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: datetime | None = None
    ok: bool = True


class Artifact(BaseModel):
    """A durable output produced by a task (file written, patch, report, log)."""

    id: str = Field(default_factory=lambda: _new("ART"))
    project_id: str
    task_id: str | None = None
    path: str
    kind: Literal["file", "patch", "report", "log", "note"] = "file"
    content: str = ""
    created_at: datetime = Field(default_factory=utcnow)


class Project(BaseModel):
    """Top-level aggregate."""

    id: str = Field(default_factory=lambda: _new("PRJ"))
    name: str
    repo_path: str
    requirement_id: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    status: Literal["active", "paused", "completed", "failed"] = "active"
    meta: dict[str, Any] = Field(default_factory=dict)


class InterventionKind(StrEnum):
    RETRY = "retry"
    SPLIT = "split"
    CANCEL = "cancel"
    PAUSE_PROJECT = "pause_project"
    ESCALATE = "escalate"
    DEDUPE = "dedupe"
    UNBLOCK = "unblock"
    NOOP = "noop"


class SupervisorFinding(BaseModel):
    """One anomaly detected by the supervisor during a tick."""

    model_config = ConfigDict(frozen=True)

    kind: str
    severity: Severity
    detail: str
    task_ids: list[str] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)


class Intervention(BaseModel):
    id: str = Field(default_factory=lambda: _new("INT"))
    kind: InterventionKind
    finding: SupervisorFinding
    applied: bool = False
    detail: str = ""
    created_at: datetime = Field(default_factory=utcnow)


# Re-exported for convenience in type hints elsewhere.
Numeric = int | float
