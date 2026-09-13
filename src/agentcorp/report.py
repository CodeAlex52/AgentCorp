"""Run report (SPEC §6) and its structural validator.

The report is a *derived* view: everything in it can be recomputed from the
event log, which is why ``agentcorp report`` and the benchmark both call the
same builder.  ``validate_report`` implements the §6 schema checks with the
standard library so `scripts/validate_benchmark.py` works without jsonschema;
``REPORT_SCHEMA`` is exported for consumers that do have a JSON-schema validator.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .events import EventType
from .graph import TaskGraph
from .models import TaskStatus
from .store import Store

__all__ = [
    "build_run_report",
    "validate_report",
    "write_report",
    "REPORT_SCHEMA",
    "RUN_STATUSES",
]

RUN_STATUSES: tuple[str, ...] = ("DONE", "FAILED", "BUDGET_EXHAUSTED", "CANCELLED", "DEADLOCK")

REPORT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "AgentCorp run report (SPEC v0 §6)",
    "type": "object",
    "required": [
        "schema_version",
        "run_id",
        "status",
        "started_at",
        "finished_at",
        "wall_ms",
        "tasks",
        "success_rate",
        "agent_calls",
        "tokens",
        "cost_usd",
        "retries",
        "reviews",
        "interventions",
        "files_changed",
        "events_count",
        "critical_path",
        "per_task",
    ],
    "properties": {
        "schema_version": {"type": "string", "const": "1"},
        "run_id": {"type": "string", "minLength": 1},
        "status": {"type": "string", "enum": list(RUN_STATUSES)},
        "started_at": {"type": "string"},
        "finished_at": {"type": "string"},
        "wall_ms": {"type": "integer", "minimum": 0},
        "tasks": {
            "type": "object",
            "required": [
                "total",
                "done",
                "failed",
                "cancelled",
                "quarantined",
                "split_parents",
                "max_depth",
            ],
            "properties": {
                "total": {"type": "integer", "minimum": 0},
                "done": {"type": "integer", "minimum": 0},
                "failed": {"type": "integer", "minimum": 0},
                "cancelled": {"type": "integer", "minimum": 0},
                "quarantined": {"type": "integer", "minimum": 0},
                "split_parents": {"type": "integer", "minimum": 0},
                "max_depth": {"type": "integer", "minimum": 0},
            },
        },
        "success_rate": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "agent_calls": {"type": "integer", "minimum": 0},
        "tokens": {
            "type": "object",
            "required": ["input", "output", "total", "budget_limit", "pressure"],
            "properties": {
                "input": {"type": "integer", "minimum": 0},
                "output": {"type": "integer", "minimum": 0},
                "total": {"type": "integer", "minimum": 0},
                "budget_limit": {"type": ["integer", "null"]},
                "pressure": {"type": "number", "minimum": 0.0},
            },
        },
        "cost_usd": {"type": "number", "minimum": 0.0},
        "retries": {
            "type": "object",
            "required": ["total", "by_task"],
            "properties": {
                "total": {"type": "integer", "minimum": 0},
                "by_task": {"type": "object", "additionalProperties": {"type": "integer"}},
            },
        },
        "reviews": {
            "type": "object",
            "required": ["total", "rejected", "rework_rounds"],
            "properties": {
                "total": {"type": "integer", "minimum": 0},
                "rejected": {"type": "integer", "minimum": 0},
                "rework_rounds": {"type": "integer", "minimum": 0},
            },
        },
        "interventions": {
            "type": "object",
            "required": ["total", "false_positive_guarded"],
            "properties": {
                "total": {"type": "integer", "minimum": 0},
                "false_positive_guarded": {"type": "boolean"},
            },
        },
        "files_changed": {"type": "integer", "minimum": 0},
        "events_count": {"type": "integer", "minimum": 0},
        "critical_path": {"type": "array", "items": {"type": "string"}},
        "per_task": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "task_id",
                    "kind",
                    "status",
                    "attempts",
                    "tokens",
                    "duration_ms",
                    "review_rejected",
                ],
                "properties": {
                    "task_id": {"type": "string"},
                    "kind": {"type": "string"},
                    "status": {"type": "string"},
                    "attempts": {"type": "integer", "minimum": 0},
                    "tokens": {"type": "integer", "minimum": 0},
                    "duration_ms": {"type": "integer", "minimum": 0},
                    "review_rejected": {"type": "boolean"},
                },
            },
        },
    },
}


def build_run_report(
    store: Store,
    project_id: str,
    *,
    graph: TaskGraph | None = None,
) -> dict[str, Any]:
    """Compute the SPEC §6 report for one project from the store."""
    tasks = store.list_tasks(project_id)
    counts = store.status_counts(project_id)
    events = store.list_events(project_id)
    usage = store.usage_for_project(project_id)
    usage_by_task = store.usage_by_task(project_id)
    reviews = store.list_reviews(project_id)
    artifacts = store.list_artifacts(project_id)
    run_summary = store.get_document(project_id, "run_summary") or {}
    budget_doc = store.get_document(project_id, "budget") or {}
    limits = budget_doc.get("limits") or {}

    started_at, finished_at, wall_ms = _timeline(events, run_summary)
    status = str(run_summary.get("status") or _derive_status(counts, tasks))
    total = len(tasks)
    done = counts.get(TaskStatus.DONE, 0)
    split_parents = counts.get(TaskStatus.SPLIT, 0)
    leaves = [t for t in tasks if t.status is not TaskStatus.SPLIT]
    leaf_success = sum(1 for t in leaves if t.status is TaskStatus.DONE)
    success_rate = round(leaf_success / len(leaves), 4) if leaves else 0.0

    retries_by_task: dict[str, int] = {}
    for event in events:
        if event.type in {EventType.TASK_RETRIED, EventType.TASK_REWORK} and event.task_id:
            retries_by_task[event.task_id] = retries_by_task.get(event.task_id, 0) + 1

    per_task: list[dict[str, Any]] = []
    for task in sorted(tasks, key=lambda t: t.id):
        task_usage = usage_by_task.get(task.id)
        tokens = task_usage.total_tokens if task_usage else 0
        duration_ms = 0
        if task.started_at and task.finished_at:
            duration_ms = max(
                int((task.finished_at - task.started_at).total_seconds() * 1000), 0
            )
        per_task.append(
            {
                "task_id": task.id,
                "kind": task.kind.value,
                "status": task.status.value,
                "attempts": task.attempts,
                "tokens": tokens,
                "duration_ms": duration_ms,
                "review_rejected": any(
                    r.task_id == task.id and r.verdict == "REJECT" for r in reviews
                ),
            }
        )

    task_graph = graph or TaskGraph(tasks)
    try:
        critical_path = task_graph.critical_path(include_done=True)
    except Exception:  # pragma: no cover - defensive: never fail a report
        critical_path = []

    budget_limit = limits.get("max_tokens")
    pressure = 0.0
    if isinstance(budget_limit, int) and budget_limit > 0:
        pressure = round(min(usage.total_tokens / budget_limit, 1.0), 4)

    interventions_total = sum(
        1 for event in events if event.type is EventType.INTERVENTION_RAISED
    )

    return {
        "schema_version": "1",
        "run_id": project_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "wall_ms": wall_ms,
        "tasks": {
            "total": total,
            "done": done,
            "failed": counts.get(TaskStatus.FAILED, 0),
            "cancelled": counts.get(TaskStatus.CANCELLED, 0),
            "quarantined": counts.get(TaskStatus.QUARANTINED, 0),
            "split_parents": split_parents,
            "max_depth": max((t.depth for t in tasks), default=0),
        },
        "success_rate": success_rate,
        "agent_calls": usage.calls,
        "tokens": {
            "input": usage.tokens_in,
            "output": usage.tokens_out,
            "total": usage.total_tokens,
            "budget_limit": budget_limit if isinstance(budget_limit, int) else None,
            "pressure": pressure,
        },
        "cost_usd": round(usage.cost_usd, 6),
        "retries": {
            "total": sum(retries_by_task.values()),
            "by_task": dict(sorted(retries_by_task.items())),
        },
        "reviews": {
            "total": len(reviews),
            "rejected": sum(1 for r in reviews if r.verdict == "REJECT"),
            "rework_rounds": sum(t.rework_count for t in tasks),
        },
        "interventions": {
            "total": interventions_total,
            "false_positive_guarded": True,
        },
        "files_changed": len({a.path for a in artifacts if a.kind == "file"}),
        "events_count": len(events),
        "critical_path": list(critical_path),
        "per_task": per_task,
    }


def _timeline(
    events: list[Any], run_summary: dict[str, Any]
) -> tuple[str, str, int]:
    started: datetime | None = None
    finished: datetime | None = None
    for event in events:
        if event.type is EventType.RUN_STARTED and started is None:
            started = event.created_at
        if event.type is EventType.RUN_RESUMED and started is None:
            payload_start = event.payload.get("started_at")
            started = (
                datetime.fromisoformat(str(payload_start)) if payload_start else event.created_at
            )
        if event.type is EventType.RUN_FINISHED:
            finished = event.created_at
    if started is None and events:
        started = events[0].created_at
    if finished is None and events:
        finished = events[-1].created_at
    if started is None or finished is None:  # pragma: no cover - empty project
        fallback = (started or finished)
        iso = fallback.isoformat() if fallback else ""
        return iso, iso, 0
    wall_ms = max(int((finished - started).total_seconds() * 1000), 0)
    if "wall_ms" in run_summary:
        wall_ms = int(run_summary["wall_ms"])
    return started.isoformat(), finished.isoformat(), wall_ms


def _derive_status(counts: dict[TaskStatus, int], tasks: list[Any]) -> str:  # noqa: ARG001 - counts kept for symmetry/future use
    if not tasks:
        return "DONE"
    if any(t.status is TaskStatus.FAILED or t.status is TaskStatus.QUARANTINED for t in tasks):
        return "FAILED"
    if any(t.status is TaskStatus.CANCELLED for t in tasks):
        return "CANCELLED"
    if all(t.status is TaskStatus.DONE for t in tasks):
        return "DONE"
    return "FAILED"  # pragma: no cover - run still in flight


def validate_report(report: dict[str, Any]) -> list[str]:
    """Structural validation against SPEC §6. Returns problems (empty == valid)."""
    problems: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            problems.append(message)

    require(report.get("schema_version") == "1", "schema_version must be '1'")
    require(isinstance(report.get("run_id"), str) and bool(report.get("run_id")), "run_id must be a non-empty string")
    require(report.get("status") in RUN_STATUSES, f"status must be one of {RUN_STATUSES}")
    for key in ("started_at", "finished_at"):
        value = report.get(key)
        require(isinstance(value, str) and bool(value), f"{key} must be an ISO8601 string")
        if isinstance(value, str) and value:
            try:
                datetime.fromisoformat(value)
            except ValueError:
                problems.append(f"{key} is not parseable as ISO8601: {value!r}")
    wall_ms = report.get("wall_ms")
    require(isinstance(wall_ms, int) and not isinstance(wall_ms, bool) and wall_ms >= 0, "wall_ms must be a non-negative int")

    tasks = report.get("tasks")
    if not isinstance(tasks, dict):
        problems.append("tasks must be an object")
    else:
        for key in (
            "total",
            "done",
            "failed",
            "cancelled",
            "quarantined",
            "split_parents",
            "max_depth",
        ):
            value = tasks.get(key)
            require(
                isinstance(value, int) and not isinstance(value, bool) and value >= 0,
                f"tasks.{key} must be a non-negative int",
            )
        counts_sum = sum(
            tasks.get(key, 0)
            for key in ("done", "failed", "cancelled", "quarantined")
            if isinstance(tasks.get(key), int)
        )
        require(
            counts_sum <= tasks.get("total", -1),
            "tasks: done+failed+cancelled+quarantined cannot exceed total",
        )

    rate = report.get("success_rate")
    require(isinstance(rate, (int, float)) and not isinstance(rate, bool) and 0.0 <= float(rate) <= 1.0, "success_rate must be within [0, 1]")

    calls = report.get("agent_calls")
    require(isinstance(calls, int) and not isinstance(calls, bool) and calls >= 0, "agent_calls must be a non-negative int")

    tokens = report.get("tokens")
    if not isinstance(tokens, dict):
        problems.append("tokens must be an object")
    else:
        for key in ("input", "output", "total"):
            value = tokens.get(key)
            require(isinstance(value, int) and not isinstance(value, bool) and value >= 0, f"tokens.{key} must be a non-negative int")
        if all(isinstance(tokens.get(k), int) for k in ("input", "output", "total")):
            require(
                tokens["total"] == tokens["input"] + tokens["output"],
                "tokens.total must equal input+output",
            )
        limit = tokens.get("budget_limit")
        require(limit is None or (isinstance(limit, int) and not isinstance(limit, bool) and limit >= 0), "tokens.budget_limit must be null or a non-negative int")
        pressure = tokens.get("pressure")
        require(isinstance(pressure, (int, float)) and not isinstance(pressure, bool) and float(pressure) >= 0.0, "tokens.pressure must be a non-negative number")

    cost = report.get("cost_usd")
    require(isinstance(cost, (int, float)) and not isinstance(cost, bool) and float(cost) >= 0.0, "cost_usd must be a non-negative number")

    retries = report.get("retries")
    if not isinstance(retries, dict):
        problems.append("retries must be an object")
    else:
        require(isinstance(retries.get("total"), int) and retries.get("total", -1) >= 0, "retries.total must be a non-negative int")
        require(isinstance(retries.get("by_task"), dict), "retries.by_task must be an object")
        if isinstance(retries.get("by_task"), dict):
            require(
                all(isinstance(v, int) and v >= 0 for v in retries["by_task"].values()),
                "retries.by_task values must be non-negative ints",
            )

    reviews = report.get("reviews")
    if not isinstance(reviews, dict):
        problems.append("reviews must be an object")
    else:
        for key in ("total", "rejected", "rework_rounds"):
            value = reviews.get(key)
            require(isinstance(value, int) and not isinstance(value, bool) and value >= 0, f"reviews.{key} must be a non-negative int")
        if all(isinstance(reviews.get(k), int) for k in ("total", "rejected")):
            require(reviews["rejected"] <= reviews["total"], "reviews.rejected cannot exceed reviews.total")

    interventions = report.get("interventions")
    if not isinstance(interventions, dict):
        problems.append("interventions must be an object")
    else:
        require(isinstance(interventions.get("total"), int) and interventions.get("total", -1) >= 0, "interventions.total must be a non-negative int")
        require(isinstance(interventions.get("false_positive_guarded"), bool), "interventions.false_positive_guarded must be a boolean")

    for key in ("files_changed", "events_count"):
        value = report.get(key)
        require(isinstance(value, int) and not isinstance(value, bool) and value >= 0, f"{key} must be a non-negative int")

    path = report.get("critical_path")
    require(isinstance(path, list) and all(isinstance(p, str) for p in path), "critical_path must be a list of task ids")

    per_task = report.get("per_task")
    if not isinstance(per_task, list):
        problems.append("per_task must be a list")
    else:
        for index, item in enumerate(per_task):
            if not isinstance(item, dict):
                problems.append(f"per_task[{index}] must be an object")
                continue
            for key in ("task_id", "kind", "status"):
                require(isinstance(item.get(key), str) and bool(item.get(key)), f"per_task[{index}].{key} must be a non-empty string")
            for key in ("attempts", "tokens", "duration_ms"):
                value = item.get(key)
                require(isinstance(value, int) and not isinstance(value, bool) and value >= 0, f"per_task[{index}].{key} must be a non-negative int")
            require(isinstance(item.get("review_rejected"), bool), f"per_task[{index}].review_rejected must be a boolean")
    return problems


def write_report(path: str | Path, report: dict[str, Any]) -> Path:
    """Write a report deterministically (sorted keys) and return the path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return target
