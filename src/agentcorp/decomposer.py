"""Bounded recursive decomposition (SPEC C8).

A task splits when the worker reports it as blocked-with-recommendations (or
when the supervisor decides a stuck task should be broken up).  Every split is
checked against three bounds *before* the provider is asked to produce children:

* ``max_depth``        — recursion depth per branch
* ``max_total_tasks``  — global task budget for the run
* budget pressure      — a run already at its ceiling must not grow

The child DAG is built with the same validation as the initial plan and is
inserted atomically by the scheduler (one store transaction), so a rejected
child DAG never leaves partial state behind.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .errors import DecompositionError, GraphError, SchemaError
from .graph import TaskGraph
from .models import Task, TaskStatus, WorkerOutcome
from .parsing import as_list_of_str, extract_json_object
from .planner import tasks_from_plan
from .prompts import decomposer_messages
from .runtime import AgentRuntime

__all__ = [
    "DecompositionBounds",
    "DecompositionPlan",
    "Decomposer",
    "decide_split",
    "children_from_titles",
]


@dataclass(frozen=True)
class DecompositionBounds:
    max_depth: int = 3
    max_total_tasks: int = 200
    max_subtasks: int = 5

    def check(self, *, depth: int, total_tasks: int, new_children: int) -> str | None:
        """Return a refusal reason, or ``None`` when the split is allowed."""
        if depth + 1 > self.max_depth:
            return f"max_depth {self.max_depth} reached (task depth {depth})"
        if total_tasks + new_children > self.max_total_tasks:
            return (
                f"max_total_tasks {self.max_total_tasks} would be exceeded "
                f"({total_tasks} existing + {new_children} new)"
            )
        return None


@dataclass
class DecompositionPlan:
    children: list[Task]
    reason: str
    source: str  # "provider" | "recommendations"


def decide_split(
    outcome: WorkerOutcome,
    *,
    depth: int,
    total_tasks: int,
    bounds: DecompositionBounds,
    budget_pressure: float = 0.0,
    max_reworks: int = 2,
    rework_count: int = 0,
) -> tuple[bool, str]:
    """Decide whether a blocked worker outcome should trigger a split.

    Pure and synchronous so the rules are unit-testable without a provider.
    """
    if outcome.status != "blocked":
        return False, "worker did not report blocked"
    if not outcome.recommended_subtasks and not outcome.reason:
        return False, "no decomposition signal from the worker"
    if budget_pressure >= 1.0:
        return False, "budget exhausted"
    if rework_count >= max_reworks:
        return False, "rework budget exhausted"
    refusal = bounds.check(depth=depth, total_tasks=total_tasks, new_children=1)
    if refusal is not None:
        return False, refusal
    return True, "worker reported blocked with a decomposition signal"


class Decomposer:
    """Turns a blocked task into validated child tasks."""

    role = "decomposer"

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        bounds: DecompositionBounds | None = None,
        project_id: str = "unknown",
        allow_recommendation_fallback: bool = True,
    ) -> None:
        self.runtime = runtime
        self.bounds = bounds or DecompositionBounds()
        self.project_id = project_id
        self.allow_recommendation_fallback = allow_recommendation_fallback

    async def split(
        self,
        task: Task,
        *,
        outcome: WorkerOutcome,
        signal: str,
        repo_context: str = "",
        total_tasks: int = 0,
        id_factory: Any = None,
    ) -> DecompositionPlan:
        """Produce validated children for ``task`` or raise DecompositionError."""
        pending_children = max(len(outcome.recommended_subtasks), 1)
        refusal = self.bounds.check(
            depth=task.depth,
            total_tasks=total_tasks,
            new_children=pending_children,
        )
        if refusal is not None:
            raise DecompositionError(f"cannot split {task.id}: {refusal}")

        last_error: Exception | None = None
        if signal.strip():
            try:
                return await self._split_via_provider(
                    task,
                    signal=signal,
                    repo_context=repo_context,
                    total_tasks=total_tasks,
                    id_factory=id_factory,
                )
            except (SchemaError, GraphError, DecompositionError) as exc:
                last_error = exc
                if not self.allow_recommendation_fallback:
                    raise

        titles = as_list_of_str(outcome.recommended_subtasks)
        if not titles:
            msg = f"cannot split {task.id}: no recommended subtasks from the worker"
            raise DecompositionError(msg)
        try:
            children = children_from_titles(
                task,
                titles,
                max_subtasks=self.bounds.max_subtasks,
                id_factory=id_factory,
            )
            _validate_children(task, children, total_tasks=total_tasks, bounds=self.bounds)
        except GraphError as exc:
            msg = f"cannot split {task.id}: child DAG rejected ({exc})"
            raise DecompositionError(msg) from exc
        reason = (
            f"provider split unusable ({last_error}); used worker recommendations"
            if last_error is not None
            else "worker recommendations"
        )
        return DecompositionPlan(children=children, reason=reason, source="recommendations")

    async def _split_via_provider(
        self,
        task: Task,
        *,
        signal: str,
        repo_context: str,
        total_tasks: int,
        id_factory: Any,
    ) -> DecompositionPlan:
        messages = decomposer_messages(
            task,
            signal=signal,
            repo_context=repo_context,
            max_subtasks=self.bounds.max_subtasks,
        )
        result = await self.runtime.call(
            messages,
            role=self.role,
            task_id=task.id,
            metadata={"project_id": self.project_id, "stage": "decompose"},
        )
        payload = extract_json_object(result.text)
        if not bool(payload.get("should_split", False)):
            msg = f"decomposer refused to split {task.id}: {payload.get('reason') or 'no reason given'}"
            raise DecompositionError(msg)
        children = tasks_from_plan(
            {"tasks": _subtasks_as_plan(payload)},
            parent_id=task.id,
            depth=task.depth + 1,
            base_dependencies=list(task.dependencies),
            base_tags=[*task.tags, "split-child"],
            id_factory=id_factory if id_factory is not None else _default_id_factory,
            max_tasks=self.bounds.max_subtasks,
        )
        _validate_children(task, children, total_tasks=total_tasks, bounds=self.bounds)
        return DecompositionPlan(
            children=children,
            reason=str(payload.get("reason") or "provider split"),
            source="provider",
        )


def _default_id_factory(prefix: str) -> str:
    from .util import new_id

    return new_id(prefix)


def _subtasks_as_plan(payload: dict[str, Any]) -> list[dict[str, Any]]:
    subtasks = payload.get("subtasks")
    if not isinstance(subtasks, list) or not subtasks:
        msg = "decomposer payload has no 'subtasks'"
        raise SchemaError(msg)
    out: list[dict[str, Any]] = []
    for index, item in enumerate(subtasks):
        if not isinstance(item, dict):
            msg = f"subtask entry must be an object, got {type(item).__name__}"
            raise SchemaError(msg)
        title = str(item.get("title") or "").strip()
        if not title:
            msg = f"subtask {index} has no title"
            raise SchemaError(msg)
        siblings = [
            int(i)
            for i in as_list_of_str(item.get("depends_on_siblings"))
            if str(i).lstrip("-").isdigit()
        ]
        out.append(
            {
                "key": f"S{index}",
                "title": title,
                "objective": str(item.get("objective") or title),
                "kind": item.get("kind", "implementation"),
                "depends_on": [f"S{i}" for i in siblings if 0 <= i < index],
                "acceptance_criteria": item.get("acceptance_criteria", []),
                "touch_paths": item.get("touch_paths", []),
                "risk": item.get("risk", "medium"),
            }
        )
    return out


def children_from_titles(
    parent: Task,
    titles: Sequence[str],
    *,
    max_subtasks: int = 5,
    id_factory: Any = None,
) -> list[Task]:
    """Deterministic children from the worker's recommended subtask titles."""
    factory = id_factory if id_factory is not None else _default_id_factory
    plan = {
        "tasks": [
            {
                "key": f"S{index}",
                "title": title.strip(),
                "objective": title.strip(),
                "kind": "implementation" if index == 1 else ("test" if index >= 2 else "research"),
                "depends_on": [f"S{index - 1}"] if index > 0 else [],
                "acceptance_criteria": [
                    {
                        "statement": f"{title.strip()} is complete and checkable",
                        "verification": "review",
                    }
                ],
                "touch_paths": list(parent.touch_paths) if index == 1 else [],
            }
            for index, title in enumerate(list(titles)[:max_subtasks])
            if title.strip()
        ]
    }
    if not plan["tasks"]:
        msg = f"no usable subtask titles for {parent.id}"
        raise DecompositionError(msg)
    return tasks_from_plan(
        plan,
        parent_id=parent.id,
        depth=parent.depth + 1,
        base_dependencies=list(parent.dependencies),
        base_tags=[*parent.tags, "split-child"],
        id_factory=factory,
        max_tasks=max_subtasks,
    )


def _validate_children(
    parent: Task,
    children: list[Task],
    *,
    total_tasks: int,
    bounds: DecompositionBounds,
) -> None:
    if not children:
        msg = f"split of {parent.id} produced no children"
        raise DecompositionError(msg)
    refusal = bounds.check(depth=parent.depth, total_tasks=total_tasks, new_children=len(children))
    if refusal is not None:
        raise DecompositionError(f"cannot split {parent.id}: {refusal}")
    # Children inherit the parent's dependencies (external to this split), so
    # the child graph is validated with those dependencies stubbed in — the
    # scheduler re-validates the full candidate graph before insertion anyway.
    child_ids = {c.id for c in children}
    stubs = [
        Task(id=dep, title="(external dependency)", status=TaskStatus.DONE)
        for dep in parent.dependencies
        if dep not in child_ids
    ]
    graph = TaskGraph([*stubs, *children])
    graph.validate(strict_parents=False)
    for child in children:
        if child.id == parent.id:
            msg = f"child {child.id} reuses the parent id"
            raise DecompositionError(msg)
        if parent.id in child.dependencies:
            msg = f"child {child.id} depends on its own parent {parent.id}"
            raise DecompositionError(msg)
