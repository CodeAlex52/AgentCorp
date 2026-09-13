"""Task DAG algebra.

Pure functions over :class:`~agentcorp.models.Task` sets: validation, readiness,
reachability, critical path, and rendering.  No I/O, no clock, no store — which
is why the graph behaviour is cheap to test exhaustively.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

import networkx as nx

from .errors import GraphError
from .models import TERMINAL_STATUSES, Task, TaskKind, TaskStatus

__all__ = ["TaskGraph", "GraphStats", "STATUS_GLYPH"]

STATUS_GLYPH: dict[TaskStatus, str] = {
    TaskStatus.PENDING: "○",
    TaskStatus.READY: "◔",
    TaskStatus.RUNNING: "◐",
    TaskStatus.REVIEW: "◎",
    TaskStatus.BLOCKED: "⊘",
    TaskStatus.FAILED: "✗",
    TaskStatus.DONE: "●",
    TaskStatus.SPLIT: "⑂",
    TaskStatus.QUARANTINED: "⚿",
    TaskStatus.CANCELLED: "×",
}

#: Statuses that make a dependent permanently unrunnable.
POISON_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.QUARANTINED}
)


@dataclass
class GraphStats:
    total: int = 0
    by_status: dict[str, int] = field(default_factory=dict)
    by_kind: dict[str, int] = field(default_factory=dict)
    max_depth: int = 0
    roots: int = 0
    leaves: int = 0
    edges: int = 0

    @property
    def is_complete(self) -> bool:
        return self.by_status.get(TaskStatus.DONE.value, 0) == self.total and self.total > 0

    @property
    def has_failures(self) -> bool:
        return bool(self.by_status.get(TaskStatus.FAILED.value, 0))


class TaskGraph:
    """A validated DAG of tasks."""

    def __init__(self, tasks: Iterable[Task] = ()) -> None:
        self._tasks: dict[str, Task] = {}
        self._dependents: dict[str, set[str]] = defaultdict(set)
        self._nx = nx.DiGraph()
        for task in tasks:
            self.add(task)

    # ------------------------------------------------------------- mutation
    def add(self, task: Task) -> None:
        self._tasks[task.id] = task
        self._nx.add_node(task.id)
        for dep in task.dependencies:
            self._dependents[dep].add(task.id)
            self._nx.add_edge(dep, task.id)

    def update(self, task: Task) -> None:
        """Replace a task, re-reading its dependencies.

        Edges are removed by consulting the *graph*, not the (possibly already
        mutated) task object — ``rewire()`` used to mutate the dependent before
        calling this, which left a stale ``parent -> dependent`` edge and made
        ``stuck_parents()`` fire on healthy splits (BASELINE_AUDIT DEF-04).
        """
        if task.id in self._nx:
            for dep in list(self._nx.predecessors(task.id)):
                self._dependents[dep].discard(task.id)
                self._nx.remove_edge(dep, task.id)
        self.add(task)

    def replace_all(self, tasks: Iterable[Task]) -> None:
        self._tasks.clear()
        self._dependents.clear()
        self._nx.clear()
        for task in tasks:
            self.add(task)

    def remove(self, task_id: str) -> None:
        task = self._tasks.pop(task_id, None)
        if task is None:
            return
        for dep in task.dependencies:
            self._dependents[dep].discard(task_id)
        for dependent in list(self._dependents.pop(task_id, ())):
            if dependent in self._tasks:
                self._tasks[dependent].dependencies = [
                    d for d in self._tasks[dependent].dependencies if d != task_id
                ]
        self._nx.remove_node(task_id)

    def rewire(self, parent_id: str, child_ids: list[str]) -> list[str]:
        """After splitting ``parent_id``, make its dependents depend on the children.

        Without this the parent's dependents would either wait forever on a
        ``SPLIT`` node or silently lose their ordering constraint — the classic
        bug in recursive task decomposition.
        """
        affected: list[str] = []
        for dependent_id in list(self._dependents.get(parent_id, ())):
            dependent = self._tasks.get(dependent_id)
            if dependent is None or dependent_id in child_ids:
                continue
            new_deps = [d for d in dependent.dependencies if d != parent_id]
            new_deps.extend(c for c in child_ids if c not in new_deps)
            dependent.dependencies = new_deps
            self.update(dependent)
            affected.append(dependent_id)
        return affected

    def set_dependencies(self, task_id: str, dependencies: list[str]) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            msg = f"unknown task {task_id!r}"
            raise GraphError(msg)
        task.dependencies = list(dependencies)
        self.update(task)

    # -------------------------------------------------------------- queries
    def __len__(self) -> int:
        return len(self._tasks)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._tasks

    def __iter__(self) -> Iterator[Task]:
        return iter(self._tasks.values())

    def get(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    @property
    def tasks(self) -> list[Task]:
        return list(self._tasks.values())

    def dependents_of(self, task_id: str) -> list[str]:
        return sorted(self._dependents.get(task_id, ()))

    def dependencies_of(self, task_id: str) -> list[str]:
        task = self._tasks.get(task_id)
        return list(task.dependencies) if task else []

    def by_status(self, *statuses: TaskStatus) -> list[Task]:
        wanted = set(statuses)
        return [t for t in self._tasks.values() if t.status in wanted]

    def children_of(self, task_id: str) -> list[Task]:
        return sorted((t for t in self._tasks.values() if t.parent_id == task_id), key=lambda t: t.id)

    def roots(self) -> list[Task]:
        return [t for t in self._tasks.values() if not self.dependencies_of(t.id)]

    def leaves(self) -> list[Task]:
        return [t for t in self._tasks.values() if not self._dependents.get(t.id)]

    def topological_order(self) -> list[Task]:
        self.validate()
        return [self._tasks[i] for i in nx.topological_sort(self._nx)]

    def descendants(self, task_id: str) -> list[str]:
        if task_id not in self._nx:
            return []
        return sorted(nx.descendants(self._nx, task_id))

    def ancestors(self, task_id: str) -> list[str]:
        if task_id not in self._nx:
            return []
        return sorted(nx.ancestors(self._nx, task_id))

    def depth_of(self, task_id: str) -> int:
        """Longest path length from any root to ``task_id``."""
        ancestors = self.ancestors(task_id)
        if not ancestors:
            return 0
        sub = self._nx.subgraph([*ancestors, task_id])
        for node in sub.nodes:
            sub.nodes[node]["w"] = 1
        return len(nx.dag_longest_path(sub, weight="w")) - 1

    # ------------------------------------------------------------- readiness
    def ready(self) -> list[Task]:
        """``PENDING`` tasks whose dependencies are all ``DONE``.

        These are *promotable* to ``READY`` (SPEC §5.1 ``PENDING -> READY``).
        Sorted by (priority desc, depth asc, id) so the scheduler's choice is
        deterministic — important for reproducible benchmarks.
        """
        out = [
            t
            for t in self._tasks.values()
            if t.status is TaskStatus.PENDING
            and all((self._tasks.get(d) is not None and self._tasks[d].status is TaskStatus.DONE) for d in t.dependencies)
        ]
        return sorted(out, key=lambda t: (-t.priority, t.depth, t.id))

    def claimable(self) -> list[Task]:
        """``READY`` tasks, in dispatch order (same deterministic ordering)."""
        out = [t for t in self._tasks.values() if t.status is TaskStatus.READY]
        return sorted(out, key=lambda t: (-t.priority, t.depth, t.id))

    def active(self) -> list[Task]:
        """Every non-terminal task."""
        return [t for t in self._tasks.values() if t.status not in TERMINAL_STATUSES]

    def unfinished(self) -> list[Task]:
        """Alias of :meth:`active`; naming used by the scheduler/report."""
        return self.active()

    def all_terminal(self) -> bool:
        return bool(self._tasks) and all(t.status in TERMINAL_STATUSES for t in self._tasks.values())

    def blocked_by_failure(self) -> list[Task]:
        """Pending tasks that can never run because an ancestor is dead."""
        out = []
        for task in self._tasks.values():
            if task.status is not TaskStatus.PENDING:
                continue
            dead = [
                d
                for d in task.dependencies
                if (self._tasks.get(d) is None) or self._tasks[d].status in POISON_STATUSES
            ]
            missing = [d for d in task.dependencies if d not in self._tasks]
            if dead or missing:
                out.append(task)
        return out

    def stuck_parents(self) -> list[Task]:
        """SPLIT nodes that still have a dependent pointing at them.

        After a healthy split, :meth:`rewire` removes those edges.  Anything
        left here is a rewiring bug and the supervisor reports it.
        """
        return [
            t
            for t in self._tasks.values()
            if t.status is TaskStatus.SPLIT and self._dependents.get(t.id)
        ]

    def critical_path(self, *, include_done: bool = False) -> list[str]:
        """Longest remaining dependency chain.

        Node-count weighted: the number of serial hand-offs still ahead is what
        determines wall-clock time when agents run in parallel.
        """
        if not self._nx.nodes:
            return []
        eligible = [
            n
            for n, t in self._tasks.items()
            if include_done or (t.status not in TERMINAL_STATUSES and t.status is not TaskStatus.SPLIT)
        ]
        if not eligible:
            return []
        sub = self._nx.subgraph(eligible).copy()
        if not sub.nodes:
            return []
        for node in sub.nodes:
            sub.nodes[node]["w"] = 1
        try:
            return list(nx.dag_longest_path(sub, weight="w"))
        except nx.NetworkXError:  # pragma: no cover - defensive
            return []

    # ------------------------------------------------------------ validation
    def find_cycle(self) -> list[str]:
        try:
            return list(nx.find_cycle(self._nx))
        except nx.NetworkXNoCycle:
            return []

    def validate(self, *, strict_parents: bool = False) -> None:
        """Raise :class:`GraphError` if the structure is not a usable DAG."""
        cycle = self.find_cycle()
        if cycle:
            chain = " -> ".join(str(edge[0]) for edge in cycle) + f" -> {cycle[0][0]}"
            msg = f"dependency cycle detected: {chain}"
            raise GraphError(msg)
        dangling: list[str] = []
        self_loops: list[str] = []
        for task in self._tasks.values():
            for dep in task.dependencies:
                if dep not in self._tasks:
                    dangling.append(f"{task.id} -> {dep}")
                if dep == task.id:
                    self_loops.append(task.id)
        if self_loops:
            msg = f"task depends on itself: {', '.join(self_loops)}"
            raise GraphError(msg)
        if dangling:
            msg = f"dangling dependencies: {'; '.join(dangling)}"
            raise GraphError(msg)
        if strict_parents:
            for task in self._tasks.values():
                if task.parent_id and task.parent_id not in self._tasks:
                    msg = f"task {task.id} references missing parent {task.parent_id}"
                    raise GraphError(msg)

    def problems(self) -> list[str]:
        """Non-fatal structural warnings."""
        issues: list[str] = []
        for task in self._tasks.values():
            if task.status is TaskStatus.PENDING and not task.acceptance_criteria:
                issues.append(f"{task.id} has no acceptance criteria")
            if task.kind is TaskKind.IMPLEMENTATION and not task.acceptance_criteria:
                issues.append(f"{task.id} is an implementation task with no acceptance criteria")
        return issues

    def stats(self) -> GraphStats:
        by_status: dict[str, int] = defaultdict(int)
        by_kind: dict[str, int] = defaultdict(int)
        for task in self._tasks.values():
            by_status[task.status.value] += 1
            by_kind[task.kind.value] += 1
        return GraphStats(
            total=len(self._tasks),
            by_status=dict(by_status),
            by_kind=dict(by_kind),
            max_depth=max((t.depth for t in self._tasks.values()), default=0),
            roots=len(self.roots()),
            leaves=len(self.leaves()),
            edges=self._nx.number_of_edges(),
        )

    # ----------------------------------------------------------- rendering
    def to_ascii(self, *, show_kind: bool = True) -> str:
        """Human tree view: parent/child nesting plus cross-dependency notes."""
        lines: list[str] = []
        roots = sorted(
            (t for t in self._tasks.values() if not t.parent_id),
            key=lambda t: (t.depth, t.id),
        )
        if not roots:  # all tasks are children of something missing; fall back
            roots = sorted(self.roots(), key=lambda t: (t.depth, t.id))

        def render(task: Task, prefix: str, is_last: bool, seen: set[str]) -> None:
            if task.id in seen:
                lines.append(f"{prefix}{'└─ ' if is_last else '├─ '}{task.id} (already shown)")
                return
            seen.add(task.id)
            glyph = STATUS_GLYPH.get(task.status, "?")
            kind = f"[{task.kind.value[:4]}] " if show_kind else ""
            suffix = ""
            if task.status is TaskStatus.BLOCKED and task.blocked_reason:
                suffix = f"  <- {task.blocked_reason[:60]}"
            elif task.status is TaskStatus.FAILED and task.failure_reason:
                suffix = f"  <- {task.failure_reason[:60]}"
            lines.append(f"{prefix}{'└─ ' if is_last else '├─ '}{glyph} {task.id} {kind}{task.title}{suffix}")
            child_prefix = prefix + ("   " if is_last else "│  ")
            own_children = self.children_of(task.id)
            # An edge is "internal" only when both ends are siblings under a
            # real parent; every other edge is a cross-dependency worth showing.
            notes = [
                d
                for d in task.dependencies
                if (dep := self._tasks.get(d)) is not None
                and not (task.parent_id is not None and dep.parent_id == task.parent_id)
            ]
            child_lines = list(own_children)
            for i, child in enumerate(child_lines):
                render(child, child_prefix, i == len(child_lines) - 1 and not notes, seen)
            if notes:
                lines.append(f"{child_prefix}└─ depends: {', '.join(notes)}")

        seen: set[str] = set()
        for i, root in enumerate(roots):
            render(root, "", i == len(roots) - 1, seen)
        orphaned = [t for t in self._tasks.values() if t.id not in seen]
        for task in sorted(orphaned, key=lambda t: t.id):
            render(task, "", False, seen)
        return "\n".join(lines)

    def to_mermaid(self, *, direction: str = "TD") -> str:
        lines = [f"graph {direction}"]
        for task in sorted(self._tasks.values(), key=lambda t: t.id):
            label = f"{task.id}<br/>{task.title[:48]}"
            lines.append(f'  {task.id}["{label}"]:::{task.status.value}')
        for task in sorted(self._tasks.values(), key=lambda t: t.id):
            for dep in task.dependencies:
                if dep in self._tasks:
                    lines.append(f"  {dep} --> {task.id}")
        lines.append("  classDef pending fill:#f5f5f5,stroke:#9e9e9e,color:#212121;")
        lines.append("  classDef ready fill:#fffde7,stroke:#fbc02d,color:#f57f17;")
        lines.append("  classDef running fill:#e3f2fd,stroke:#1976d2,color:#0d47a1;")
        lines.append("  classDef review fill:#e0f7fa,stroke:#00838f,color:#006064;")
        lines.append("  classDef done fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;")
        lines.append("  classDef failed fill:#ffebee,stroke:#c62828,color:#b71c1c;")
        lines.append("  classDef blocked fill:#fff8e1,stroke:#f9a825,color:#e65100;")
        lines.append("  classDef split fill:#ede7f6,stroke:#6a1b9a,color:#4a148c;")
        lines.append("  classDef quarantined fill:#fce4ec,stroke:#ad1457,color:#880e4f;")
        lines.append("  classDef cancelled fill:#eceff1,stroke:#607d8b,color:#37474f;")
        return "\n".join(lines)

    def to_dot(self) -> str:
        lines = ["digraph agentcorp {", "  rankdir=TB;", '  node [shape=box, fontname="Helvetica"];']
        for task in sorted(self._tasks.values(), key=lambda t: t.id):
            label = f"{task.id}\\n{task.title[:40]}"
            lines.append(f'  "{task.id}" [label="{label}", class="{task.status.value}"];')
        for task in sorted(self._tasks.values(), key=lambda t: t.id):
            for dep in task.dependencies:
                if dep in self._tasks:
                    lines.append(f'  "{dep}" -> "{task.id}";')
        lines.append("}")
        return "\n".join(lines)

    def subgraph_for_subtree(self, task_id: str) -> TaskGraph:
        ids = {task_id, *self.descendants(task_id)}
        return TaskGraph([t for t in self._tasks.values() if t.id in ids])
