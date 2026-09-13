"""Shared fixtures for the AgentCorp test-suite.

Everything here is offline and deterministic: no network, no API keys, no real
sleeps (``noop_sleep``), no wall-clock dependence (``FakeClock``), and id
sequences pinned by ``SequentialIdFactory``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from agentcorp import (
    DecompositionBounds,
    Engine,
    EngineConfig,
    Event,
    EventType,
    MockProvider,
    Project,
    SchedulerConfig,
    SequentialIdFactory,
    Store,
    SupervisorConfig,
    Task,
    TaskGraph,
    noop_sleep,
)
from agentcorp.models import BudgetLimits
from agentcorp.util import FakeClock

# --------------------------------------------------------------------- clocks


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


# ---------------------------------------------------------------------- store


@pytest.fixture
def store(tmp_path: Path) -> Store:
    s = Store(tmp_path / "agentcorp.db")
    yield s
    s.close()


def create_project(store: Store, project_id: str = "P1", **overrides: Any) -> Project:
    project = Project(
        id=project_id,
        name=overrides.pop("name", project_id),
        repo_path=overrides.pop("repo_path", "/tmp/repo"),
        **overrides,
    )
    store.append(
        Event(
            project_id=project_id,
            type=EventType.PROJECT_CREATED,
            payload={"project": project.model_dump(mode="json")},
        )
    )
    return project


def seed_tasks(store: Store, project_id: str, tasks: list[Task]) -> TaskGraph:
    for task in tasks:
        store.append(
            Event(
                project_id=project_id,
                type=EventType.TASK_CREATED,
                task_id=task.id,
                payload={"task": task.model_dump(mode="json")},
            )
        )
    return TaskGraph(store.list_tasks(project_id))


def make_task(task_id: str, **overrides: Any) -> Task:
    data: dict[str, Any] = {"id": task_id, "title": f"task {task_id}"}
    target = overrides.pop("status", None)
    if target is not None:
        data["status"] = target
    data.update(overrides)
    return Task(**data)


def drive(store: Store, project_id: str, task_id: str, *types: EventType, **payload: Any) -> None:
    """Append lifecycle events for one task (test convenience)."""
    for type_ in types:
        store.append(
            Event(
                project_id=project_id,
                type=type_,
                task_id=task_id,
                payload=dict(payload),
            )
        )


# --------------------------------------------------------------------- engine


@pytest.fixture
def toy_repo(tmp_path: Path) -> Path:
    """A tiny, deterministic repository used by engine-level tests."""
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "tests").mkdir()
    (repo / "docs").mkdir()
    (repo / "pyproject.toml").write_text(
        "[project]\nname = 'toy'\nversion = '0.1.0'\ndependencies = ['pydantic>=2']\n"
        "[project.optional-dependencies]\ndev = ['pytest>=8']\n",
        encoding="utf-8",
    )
    (repo / "src" / "toy.py").write_text("def add(a: int, b: int) -> int:\n    return a + b\n", encoding="utf-8")
    (repo / "tests" / "test_toy.py").write_text(
        "from src.toy import add\n\ndef test_add():\n    assert add(1, 2) == 3\n", encoding="utf-8"
    )
    (repo / "docs" / "README.md").write_text("# toy\n", encoding="utf-8")
    return repo


EngineFactory = Callable[..., Engine]


@pytest.fixture
def engine_factory(tmp_path: Path, toy_repo: Path) -> EngineFactory:
    """Build a deterministic Engine; every knob that matters is a kwarg."""

    def build(
        *,
        skill: float = 1.0,
        seed: int = 7,
        review: bool = True,
        concurrency: int = 3,
        db_name: str = "engine.db",
        budget: BudgetLimits | None = None,
        max_depth: int = 3,
        max_total_tasks: int = 200,
        max_reworks: int = 2,
        max_attempts: int = 3,
        chaos: Any = None,
        provider: Any = None,
        repo: Path | None = None,
        tick: float = 0.01,
        task_retry_base_delay: float = 0.0,
        supervisor: SupervisorConfig | None = None,
        review_use_provider: bool = True,
        strict_touch_paths: bool = False,
        apply_writes: bool = True,
        prd_mode: str = "provider",
        real_time: bool = False,
    ) -> Engine:
        config = EngineConfig(
            repo_path=str(repo or toy_repo),
            provider_spec="mock",
            db_path=str(tmp_path / db_name),
            prd_mode=prd_mode,
            budget=budget or BudgetLimits(),
            scheduler=SchedulerConfig(
                max_concurrency=concurrency,
                review=review,
                max_reworks=max_reworks,
                tick_interval_s=tick,
                task_retry_base_delay=task_retry_base_delay,
            ),
            supervisor=supervisor or SupervisorConfig(),
            bounds=DecompositionBounds(max_depth=max_depth, max_total_tasks=max_total_tasks),
            max_attempts=max_attempts,
            chaos=chaos,
            deterministic=True,
            review_use_provider=review_use_provider,
            apply_writes=apply_writes,
            strict_touch_paths=strict_touch_paths,
        )
        from agentcorp import SystemClock, system_sleep

        return Engine(
            config,
            provider=provider or MockProvider(seed=seed, skill=skill),
            clock=SystemClock() if real_time else FakeClock(),
            sleep=system_sleep if real_time else noop_sleep,
            id_factory=SequentialIdFactory(),
        )

    return build


def run(coro: Any) -> Any:
    """Run a coroutine in a fresh loop (helper for non-async tests)."""
    return asyncio.run(coro)


@pytest.fixture
def no_sleep() -> Callable[[float], Any]:
    return noop_sleep


class RecordingSleep:
    """Records requested delays without sleeping — used to assert backoff."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


@pytest.fixture
def recording_sleep() -> RecordingSleep:
    return RecordingSleep()
