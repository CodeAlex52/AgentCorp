"""Event taxonomy.

AgentCorp is event-sourced: every state change in the system is a persisted
:class:`Event`.  Queryable tables (tasks, projects, runs, reviews) are
*projections* derived from the log, which is what makes ``resume``, ``replay``
and ``rebuild`` possible without bespoke migration code.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .util import utcnow

__all__ = ["EventType", "Event", "PROJECTION_EVENTS", "EventEmitter", "NullEmitter"]


class EventType(StrEnum):
    # --- project lifecycle -------------------------------------------------
    PROJECT_CREATED = "PROJECT_CREATED"
    PROJECT_PAUSED = "PROJECT_PAUSED"
    PROJECT_RESUMED = "PROJECT_RESUMED"
    PROJECT_COMPLETED = "PROJECT_COMPLETED"
    PROJECT_FAILED = "PROJECT_FAILED"
    REQUIREMENT_PARSED = "REQUIREMENT_PARSED"
    REPO_ANALYZED = "REPO_ANALYZED"

    # --- planning ----------------------------------------------------------
    PLAN_CREATED = "PLAN_CREATED"
    TASK_CREATED = "TASK_CREATED"
    TASK_UPDATED = "TASK_UPDATED"

    # --- execution ---------------------------------------------------------
    TASK_READY = "TASK_READY"
    TASK_STARTED = "TASK_STARTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    TASK_BLOCKED = "TASK_BLOCKED"
    TASK_UNBLOCKED = "TASK_UNBLOCKED"
    TASK_SPLIT = "TASK_SPLIT"
    TASK_RETRIED = "TASK_RETRIED"
    TASK_CANCELLED = "TASK_CANCELLED"

    # --- agents ------------------------------------------------------------
    AGENT_RUN_STARTED = "AGENT_RUN_STARTED"
    AGENT_RUN_FINISHED = "AGENT_RUN_FINISHED"
    AGENT_RUN_FAILED = "AGENT_RUN_FAILED"
    CHAOS_INJECTED = "CHAOS_INJECTED"

    # --- review ------------------------------------------------------------
    REVIEW_PASSED = "REVIEW_PASSED"
    REVIEW_FAILED = "REVIEW_FAILED"

    # --- budget ------------------------------------------------------------
    BUDGET_UPDATED = "BUDGET_UPDATED"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"

    # --- supervision -------------------------------------------------------
    SUPERVISOR_TICK = "SUPERVISOR_TICK"
    SUPERVISOR_INTERVENTION = "SUPERVISOR_INTERVENTION"

    # --- misc --------------------------------------------------------------
    ARTIFACT_PRODUCED = "ARTIFACT_PRODUCED"
    NOTE = "NOTE"
    RESUME = "RESUME"


#: Events that mutate the task projection.  Kept explicit so that adding an
#: event that *should* change a task forces a deliberate decision here.
PROJECTION_EVENTS: frozenset[EventType] = frozenset(
    {
        EventType.TASK_CREATED,
        EventType.TASK_UPDATED,
        EventType.TASK_READY,
        EventType.TASK_STARTED,
        EventType.TASK_COMPLETED,
        EventType.TASK_FAILED,
        EventType.TASK_BLOCKED,
        EventType.TASK_UNBLOCKED,
        EventType.TASK_SPLIT,
        EventType.TASK_RETRIED,
        EventType.TASK_CANCELLED,
    }
)


class Event(BaseModel):
    """An immutable fact.  ``seq`` is assigned by the store, monotonically."""

    model_config = ConfigDict(frozen=True)

    seq: int = 0  # assigned on append; 0 means "not yet persisted"
    event_id: str = ""
    project_id: str
    type: EventType
    task_id: str | None = None
    actor: str = "system"  # scheduler | worker:<role> | reviewer | supervisor | cli
    payload: dict[str, Any] = Field(default_factory=dict)
    schema_version: int = 1
    created_at: datetime = Field(default_factory=utcnow)

    def with_seq(self, seq: int) -> Event:
        return self.model_copy(update={"seq": seq})

    @property
    def is_projection_event(self) -> bool:
        """Whether this event mutates the task projection."""
        return self.type in PROJECTION_EVENTS

    def compact(self) -> str:
        """One-line human rendering for ``agentcorp events`` and replay."""
        target = f" {self.task_id}" if self.task_id else ""
        detail = self.payload.get("summary") or self.payload.get("reason") or ""
        if not detail:
            for key in ("title", "status", "verdict", "kind", "detail"):
                if key in self.payload:
                    detail = str(self.payload[key])
                    break
        detail = str(detail)[:110].replace("\n", " ")
        return f"[{self.seq:>4}] {self.created_at:%H:%M:%S} {self.type.value:<24}{target} {detail}".rstrip()


# ``emit(type, task_id, payload)``.  Components that mutate state take an
# emitter rather than a :class:`~agentcorp.store.Store`, so they stay unit
# testable without a database.  Defined after :class:`EventType` because the
# subscript is evaluated at import time.
EventEmitter = Callable[[EventType, str | None, dict[str, Any]], None]


def NullEmitter(_type: EventType, _task_id: str | None, _payload: dict[str, Any]) -> None:
    """Emitter for unit tests and for projecting a replay without writing."""
    return None


class RecordingEmitter:
    """In-memory emitter that captures events — the test double of choice."""

    def __init__(self) -> None:
        self.events: list[tuple[EventType, str | None, dict[str, Any]]] = []

    def __call__(self, type_: EventType, task_id: str | None, payload: dict[str, Any]) -> None:
        self.events.append((type_, task_id, payload))

    def types(self) -> list[EventType]:
        return [e[0] for e in self.events]

    def count(self, type_: EventType) -> int:
        return sum(1 for e in self.events if e[0] == type_)

    def find(self, type_: EventType) -> list[tuple[EventType, str | None, dict[str, Any]]]:
        return [e for e in self.events if e[0] == type_]
