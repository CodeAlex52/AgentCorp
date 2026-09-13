"""Requirement + repository → initial task DAG.

The planner is the boundary where model output becomes domain objects: keys are
mapped to real task ids, dependencies are rewritten, and the resulting graph is
validated (cycles, dangling keys, self-dependencies) *before* anything is
persisted.  An invalid plan is a contract violation, not something to "fix up".
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .errors import GraphError, SchemaError
from .graph import TaskGraph
from .models import (
    AcceptanceCriterion,
    Requirement,
    RiskLevel,
    Task,
    TaskKind,
)
from .parsing import as_list_of_str, extract_json_object
from .prompts import planner_messages
from .runtime import AgentRuntime
from .util import new_id

__all__ = ["plan_tasks", "tasks_from_plan", "PlanError"]

_MAX_PLAN_ATTEMPTS = 2
_VALID_KINDS = {kind.value for kind in TaskKind}
_VALID_RISKS = {risk.value for risk in RiskLevel}

#: Signature of the id source; defaults to uuid-based :func:`new_id` and is
#: replaced by a deterministic counter in tests/benchmarks (DEC-008).
IdFactory = Callable[[str], str]


class PlanError(GraphError):
    """The planner produced a DAG that cannot be executed."""


async def plan_tasks(
    runtime: AgentRuntime,
    requirement: Requirement,
    repo_context: str = "",
    *,
    project_id: str = "unknown",
    max_tasks: int = 12,
    max_attempts: int = _MAX_PLAN_ATTEMPTS,
    id_factory: IdFactory = new_id,
) -> list[Task]:
    """Ask the planner model for a task list and validate it into a DAG."""
    repair = ""
    last_error: Exception | None = None
    for attempt in range(1, max(1, max_attempts) + 1):
        messages = planner_messages(requirement, repo_context, max_tasks=max_tasks)
        if repair:
            messages = [
                *messages,
                {
                    "role": "user",
                    "content": (
                        "# Contract violation\n"
                        f"Your previous reply was rejected: {repair}\n"
                        "Reply with a single JSON object matching the schema exactly."
                    ),
                },
            ]
        result = await runtime.call(
            messages,
            role="planner",
            task_id=None,
            metadata={"project_id": project_id, "attempt": attempt},
        )
        try:
            payload = extract_json_object(result.text)
            tasks = tasks_from_plan(payload, id_factory=id_factory)
            graph = TaskGraph(tasks)
            graph.validate(strict_parents=True)
            return tasks
        except (SchemaError, GraphError) as exc:
            last_error = exc
            repair = str(exc)
    assert last_error is not None
    raise last_error


def tasks_from_plan(
    payload: dict[str, Any],
    *,
    parent_id: str | None = None,
    depth: int = 0,
    base_dependencies: Sequence[str] = (),
    base_tags: Sequence[str] = (),
    id_factory: IdFactory = new_id,
    max_tasks: int = 50,
) -> list[Task]:
    """Convert a planner payload into :class:`Task` objects (pure, testable).

    ``payload["tasks"]`` entries use local ``key`` strings; ``depends_on``
    refers to those keys.  Unknown keys, duplicates and self-dependencies are
    contract violations.
    """
    raw = payload.get("tasks")
    if not isinstance(raw, list) or not raw:
        msg = "planner payload must contain a non-empty 'tasks' list"
        raise SchemaError(msg)
    if len(raw) > max_tasks:
        msg = f"planner returned {len(raw)} tasks, above the limit of {max_tasks}"
        raise SchemaError(msg)

    keys: list[str] = []
    specs: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            msg = f"task entry must be an object, got {type(item).__name__}"
            raise SchemaError(msg)
        key = str(item.get("key") or "").strip()
        title = str(item.get("title") or "").strip()
        if not key:
            msg = f"task entry has no 'key': {item!r}"
            raise SchemaError(msg)
        if not title:
            msg = f"task {key!r} has no 'title'"
            raise SchemaError(msg)
        if key in keys:
            msg = f"duplicate task key {key!r}"
            raise SchemaError(msg)
        keys.append(key)
        specs.append(item)

    id_by_key: dict[str, str] = {key: id_factory("T") for key in keys}
    tasks: list[Task] = []
    for key, item in zip(keys, specs, strict=True):
        title = str(item.get("title") or "").strip()
        deps: list[str] = []
        for dep_key in as_list_of_str(item.get("depends_on")) or []:
            if dep_key not in id_by_key:
                msg = f"task {key!r} depends on unknown key {dep_key!r}"
                raise SchemaError(msg)
            if dep_key == key:
                msg = f"task {key!r} depends on itself"
                raise SchemaError(msg)
            dep_id = id_by_key[dep_key]
            if dep_id not in deps:
                deps.append(dep_id)
        tasks.append(
            Task(
                id=id_by_key[key],
                title=title,
                objective=str(item.get("objective") or "").strip() or title,
                kind=_coerce_kind(item.get("kind")),
                dependencies=[*base_dependencies, *deps],
                parent_id=parent_id,
                depth=depth,
                expected_output=str(item.get("expected_output") or "").strip(),
                acceptance_criteria=_criteria_from(item.get("acceptance_criteria")),
                risk=_coerce_risk(item.get("risk")),
                touch_paths=[p for p in as_list_of_str(item.get("touch_paths")) if p],
                tags=list(dict.fromkeys([*base_tags, *as_list_of_str(item.get("tags"))])),
                est_tokens=int(item.get("est_tokens") or 0),
            )
        )
    return tasks


def _coerce_kind(value: Any) -> TaskKind:
    text = str(value or TaskKind.IMPLEMENTATION.value).strip().lower()
    if text in _VALID_KINDS:
        return TaskKind(text)
    return TaskKind.IMPLEMENTATION


def _coerce_risk(value: Any) -> RiskLevel:
    text = str(value or RiskLevel.MEDIUM.value).strip().lower()
    if text in _VALID_RISKS:
        return RiskLevel(text)
    return RiskLevel.MEDIUM


def _criteria_from(value: Any) -> list[AcceptanceCriterion]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: list[AcceptanceCriterion] = []
    for item in items:
        if isinstance(item, str):
            if item.strip():
                out.append(AcceptanceCriterion(statement=item.strip(), verification="review"))
            continue
        if isinstance(item, dict):
            statement = str(item.get("statement") or "").strip()
            if not statement:
                continue
            verification = str(item.get("verification") or "review").lower()
            if verification not in {"test", "command", "static", "manual", "review"}:
                verification = "review"
            command = item.get("command")
            out.append(
                AcceptanceCriterion(
                    statement=statement,
                    verification=verification,  # type: ignore[arg-type]
                    command=str(command) if command else None,
                )
            )
    return out
