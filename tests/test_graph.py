"""C3/C8 graph algebra: validation, readiness, rewiring, rendering."""

from __future__ import annotations

import pytest

from agentcorp import GraphError, Task, TaskGraph, TaskStatus


def t(task_id: str, deps: list[str] | None = None, **overrides: object) -> Task:
    return Task(id=task_id, title=f"task {task_id}", dependencies=deps or [], **overrides)


@pytest.fixture
def linear() -> TaskGraph:
    return TaskGraph([t("A"), t("B", ["A"]), t("C", ["B"])])


def test_validate_accepts_valid_dag(linear: TaskGraph) -> None:
    linear.validate()  # no exception


def test_validate_rejects_multi_node_cycle() -> None:
    graph = TaskGraph([t("X", ["Y"]), t("Y", ["X"])])
    with pytest.raises(GraphError, match="cycle"):
        graph.validate()


def test_validate_rejects_self_loop() -> None:
    graph = TaskGraph([t("S", ["S"])])
    with pytest.raises(GraphError):
        graph.validate()


def test_validate_rejects_dangling_dependency() -> None:
    graph = TaskGraph([t("A", ["ghost"])])
    with pytest.raises(GraphError, match="dangling"):
        graph.validate()


def test_validate_rejects_orphan_by_default() -> None:
    """FIND-007: an orphan must be rejected by the default validate()."""
    graph = TaskGraph([t("A", parent_id="ghost")])
    with pytest.raises(GraphError, match="missing parent"):
        graph.validate()
    graph.validate(strict_parents=False)  # partial subgraphs can opt out


def test_ready_returns_pending_tasks_with_done_deps(linear: TaskGraph) -> None:
    assert [task.id for task in linear.ready()] == ["A"]
    linear.get("A").status = TaskStatus.DONE  # type: ignore[union-attr]
    assert [task.id for task in linear.ready()] == ["B"]
    linear.get("B").status = TaskStatus.RUNNING  # type: ignore[union-attr]
    assert linear.ready() == []
    assert [task.id for task in linear.claimable()] == []


def test_claimable_returns_only_ready(linear: TaskGraph) -> None:
    linear.get("A").status = TaskStatus.READY  # type: ignore[union-attr]
    linear.get("B").status = TaskStatus.READY  # type: ignore[union-attr]
    assert [task.id for task in linear.claimable()] == ["A", "B"]


def test_ready_order_is_priority_then_depth_then_id() -> None:
    graph = TaskGraph(
        [
            t("low", priority=10),
            t("high", priority=90),
            t("mid", priority=50),
        ]
    )
    assert [task.id for task in graph.ready()] == ["high", "mid", "low"]


def test_blocked_by_failure_includes_ready_and_blocked() -> None:
    graph = TaskGraph(
        [
            t("dead", status=TaskStatus.QUARANTINED),
            t("p", ["dead"]),
            t("r", ["dead"], status=TaskStatus.READY),
            t("b", ["dead"], status=TaskStatus.BLOCKED),
            t("fine"),
        ]
    )
    assert {task.id for task in graph.blocked_by_failure()} == {"p", "r", "b"}


def test_rewire_moves_dependents_to_children_and_clears_stuck_parents() -> None:
    graph = TaskGraph(
        [
            t("P", status=TaskStatus.SPLIT),
            t("D", ["P"]),
            t("C1", parent_id="P"),
            t("C2", ["C1"], parent_id="P"),
        ]
    )
    affected = graph.rewire("P", ["C1", "C2"])
    assert affected == ["D"]
    assert graph.get("D").dependencies == ["C1", "C2"]  # type: ignore[union-attr]
    assert graph.stuck_parents() == []
    assert "P" not in graph.ancestors("D")


def test_rewire_ignores_dependents_that_are_children() -> None:
    graph = TaskGraph([t("P", status=TaskStatus.SPLIT), t("C1", ["P"], parent_id="P")])
    assert graph.rewire("P", ["C1"]) == []
    assert graph.get("C1").dependencies == ["P"]  # type: ignore[union-attr]


def test_stuck_parent_detected_when_rewire_skipped() -> None:
    graph = TaskGraph([t("P", status=TaskStatus.SPLIT), t("D", ["P"])])
    assert [task.id for task in graph.stuck_parents()] == ["P"]


def test_critical_path_excludes_terminal_and_split() -> None:
    graph = TaskGraph(
        [
            t("A", status=TaskStatus.DONE),
            t("B", ["A"]),
            t("C", ["B"]),
            t("S", ["A"], status=TaskStatus.SPLIT),
            t("D", ["S"]),
        ]
    )
    path = graph.critical_path()
    assert path == ["B", "C"]
    full = graph.critical_path(include_done=True)
    assert full.index("A") < full.index("B") < full.index("C")


def test_all_terminal_and_active(linear: TaskGraph) -> None:
    assert not linear.all_terminal()
    for task in linear.tasks:
        task.status = TaskStatus.DONE
    assert linear.all_terminal()
    assert linear.active() == []
    assert linear.unfinished() == []


def test_critical_path_empty_graph() -> None:
    assert TaskGraph().critical_path() == []


def test_depth_of_uses_longest_path() -> None:
    graph = TaskGraph([t("A"), t("B", ["A"]), t("C", ["B"]), t("D", ["A"])])
    assert graph.depth_of("A") == 0
    assert graph.depth_of("C") == 2
    assert graph.depth_of("D") == 1
    assert graph.depth_of("missing") == 0


def test_descendants_ancestors_and_removal() -> None:
    graph = TaskGraph([t("A"), t("B", ["A"]), t("C", ["B"])])
    assert graph.descendants("A") == ["B", "C"]
    assert graph.ancestors("C") == ["A", "B"]
    graph.remove("B")
    # Removing B rewires C to drop the dangling dependency.
    assert graph.get("C").dependencies == []  # type: ignore[union-attr]
    graph.validate()


def test_stats_counts_status_and_kind() -> None:
    graph = TaskGraph(
        [
            t("A", status=TaskStatus.DONE, kind="implementation"),
            t("B", ["A"], status=TaskStatus.RUNNING, kind="test"),
        ]
    )
    stats = graph.stats()
    assert stats.total == 2
    assert stats.by_status["done"] == 1
    assert stats.by_kind["test"] == 1
    assert stats.roots == 1 and stats.leaves == 1
    assert stats.is_complete is False
    assert stats.has_failures is False


def test_problems_reports_missing_acceptance_criteria() -> None:
    graph = TaskGraph([t("A", kind="implementation")])
    problems = graph.problems()
    assert any("acceptance criteria" in problem for problem in problems)
    graph2 = TaskGraph([Task(id="B", title="b", acceptance_criteria=[])])
    assert graph2.problems()


def test_topological_order_raises_on_cycle() -> None:
    graph = TaskGraph([t("X", ["Y"]), t("Y", ["X"])])
    with pytest.raises(GraphError):
        graph.topological_order()


def test_ascii_and_mermaid_renderings(linear: TaskGraph) -> None:
    ascii_view = linear.to_ascii()
    assert "A" in ascii_view and "C" in ascii_view
    mermaid = linear.to_mermaid()
    assert mermaid.startswith("graph TD")
    assert "A --> B" in mermaid
    assert "classDef done" in mermaid
    assert linear.to_dot().startswith("digraph agentcorp")


def test_subgraph_for_subtree(linear: TaskGraph) -> None:
    sub = linear.subgraph_for_subtree("A")
    assert {task.id for task in sub.tasks} == {"A", "B", "C"}
    sub_b = linear.subgraph_for_subtree("B")
    assert {task.id for task in sub_b.tasks} == {"B", "C"}


def test_update_removes_stale_edges() -> None:
    graph = TaskGraph([t("A"), t("B"), t("C", ["A"])])
    graph.update(Task(id="C", title="c", dependencies=["B"]))
    assert graph.dependents_of("A") == []
    assert graph.dependents_of("B") == ["C"]
    graph.validate()
