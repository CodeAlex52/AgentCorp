"""AgentCorp — Recursive Delivery OS.

A local, event-sourced multi-agent orchestrator that turns

    (git repo, natural-language PRD)

into a reviewed, tested delivery:

    PRD -> Requirement -> Repository Context -> Task DAG -> Workers
        -> Reviewer -> Integration -> Supervisor -> Report

The package is deliberately layered so each concern is swappable and testable
in isolation:

======================  =====================================================
layer                   module
======================  =====================================================
domain data             :mod:`agentcorp.models`
facts / log             :mod:`agentcorp.events`, :mod:`agentcorp.store`
graph algebra           :mod:`agentcorp.graph`
planning                :mod:`agentcorp.prd`, :mod:`agentcorp.repo`,
                        :mod:`agentcorp.planner`, :mod:`agentcorp.decomposer`
execution               :mod:`agentcorp.scheduler`, :mod:`agentcorp.worker`,
                        :mod:`agentcorp.reviewer`
policy / resilience     :mod:`agentcorp.reliability`, :mod:`agentcorp.budget`,
                        :mod:`agentcorp.chaos`, :mod:`agentcorp.runtime`
governance              :mod:`agentcorp.supervisor`
facade + interfaces     :mod:`agentcorp.engine`, :mod:`agentcorp.cli`,
                        :mod:`agentcorp.report`
======================  =====================================================

The public API below is what tests and embedders should import; internal
modules may change shape between minor versions.
"""

from __future__ import annotations

from .budget import BudgetManager
from .chaos import ChaosConfig, ChaosController, ChaosProvider, FaultKind
from .decomposer import Decomposer, DecompositionBounds, decide_split
from .engine import AgentCorpEngine, Engine, EngineConfig, RunSummary
from .errors import (
    AgentCorpError,
    BudgetExceededError,
    CircuitOpenError,
    ContextOverflowError,
    DecompositionError,
    GraphError,
    PermanentError,
    ProviderBilledError,
    ProviderError,
    RateLimitError,
    SchemaError,
    StateError,
    TimeoutError_,
    TransientError,
    is_retryable,
)
from .events import Event, EventType
from .graph import TaskGraph
from .models import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    AcceptanceCriterion,
    AgentRun,
    Artifact,
    BudgetLimits,
    BudgetSnapshot,
    FileWrite,
    Intervention,
    InterventionKind,
    Project,
    RepositoryContext,
    Requirement,
    Review,
    ReviewIssue,
    Severity,
    SupervisorFinding,
    Task,
    TaskKind,
    TaskStatus,
    Usage,
    WorkerOutcome,
    assert_transition,
    transition_allowed,
)
from .planner import plan_tasks
from .prd import heuristic_requirement, parse_requirement
from .providers.base import AgentProvider, CompletionRequest, CompletionResponse
from .providers.mock import FlakyProvider, MockProvider, ScriptedProvider
from .providers.registry import build_provider, list_providers
from .reliability import CircuitBreaker, RetryPolicy, TokenBucket, call_with_retry
from .repo import analyze_repository
from .report import REPORT_SCHEMA, build_run_report, validate_report, write_report
from .reviewer import Reviewer, enforce_independence
from .runtime import AgentRuntime
from .scheduler import RunOutcome, Scheduler, SchedulerConfig
from .store import Store
from .supervisor import Supervisor, SupervisorConfig
from .util import FakeClock, SequentialIdFactory, SystemClock, noop_sleep, system_sleep
from .worker import PathViolationError, Worker, validate_write_path

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # domain
    "Task",
    "TaskStatus",
    "TaskKind",
    "TaskGraph",
    "Requirement",
    "RepositoryContext",
    "AcceptanceCriterion",
    "Usage",
    "BudgetLimits",
    "BudgetSnapshot",
    "WorkerOutcome",
    "FileWrite",
    "Review",
    "ReviewIssue",
    "Artifact",
    "AgentRun",
    "Project",
    "SupervisorFinding",
    "Intervention",
    "InterventionKind",
    "Severity",
    "ALLOWED_TRANSITIONS",
    "TERMINAL_STATUSES",
    "assert_transition",
    "transition_allowed",
    # facts
    "Event",
    "EventType",
    "Store",
    # planning
    "parse_requirement",
    "heuristic_requirement",
    "analyze_repository",
    "plan_tasks",
    "Decomposer",
    "DecompositionBounds",
    "decide_split",
    # execution
    "Scheduler",
    "SchedulerConfig",
    "RunOutcome",
    "Worker",
    "PathViolationError",
    "validate_write_path",
    "Reviewer",
    "enforce_independence",
    "Supervisor",
    "SupervisorConfig",
    # policy
    "BudgetManager",
    "AgentRuntime",
    "RetryPolicy",
    "CircuitBreaker",
    "TokenBucket",
    "call_with_retry",
    "ChaosConfig",
    "ChaosController",
    "ChaosProvider",
    "FaultKind",
    "AgentProvider",
    "CompletionRequest",
    "CompletionResponse",
    "MockProvider",
    "FlakyProvider",
    "ScriptedProvider",
    "build_provider",
    "list_providers",
    # facade
    "Engine",
    "AgentCorpEngine",
    "EngineConfig",
    "RunSummary",
    "build_run_report",
    "validate_report",
    "write_report",
    "REPORT_SCHEMA",
    # errors
    "AgentCorpError",
    "TransientError",
    "RateLimitError",
    "TimeoutError_",
    "ProviderBilledError",
    "ProviderError",
    "ContextOverflowError",
    "SchemaError",
    "PermanentError",
    "BudgetExceededError",
    "CircuitOpenError",
    "DecompositionError",
    "GraphError",
    "StateError",
    "is_retryable",
    # utils
    "FakeClock",
    "SystemClock",
    "SequentialIdFactory",
    "noop_sleep",
    "system_sleep",
]
