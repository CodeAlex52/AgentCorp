"""SQLite event store with rebuildable projections.

Design
------
* ``events`` is the source of truth: append-only, monotonically sequenced.
* Everything else (tasks, runs, reviews, artifacts, documents) is a *projection*
  produced by folding events through :meth:`Store._project`.
* Appending an event and updating its projection happen in **one transaction**,
  so a crash can never leave the queryable state disagreeing with the log.
* :meth:`Store.rebuild_projections` wipes every projection and replays the log.
  The test suite asserts that a rebuild is a no-op on a healthy database —
  that property is what makes ``resume`` trustworthy.

Concurrency
-----------
One connection guarded by an ``RLock`` (``check_same_thread=False``).  The
scheduler runs agent calls concurrently but funnels all state writes through
this lock; SQLite is in WAL mode so readers never block on the writer.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .errors import StateError
from .events import PROJECTION_EVENTS, Event, EventType
from .models import (
    AgentRun,
    Artifact,
    BudgetSnapshot,
    Project,
    Requirement,
    RepositoryContext,
    Review,
    Task,
    TaskStatus,
    Usage,
)
from .util import short_hash, utcnow

__all__ = ["Store", "DEFAULT_DB_NAME"]

DEFAULT_DB_NAME = "agentcorp.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    type           TEXT    NOT NULL,
    task_id        TEXT,
    actor          TEXT    NOT NULL,
    payload        TEXT    NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_project ON events(project_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_task    ON events(task_id, seq);

CREATE TABLE IF NOT EXISTS projects (
    id         TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requirements (
    id         TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    data       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_requirements_project ON requirements(project_id);

CREATE TABLE IF NOT EXISTS tasks (
    id         TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    parent_id  TEXT,
    status     TEXT NOT NULL,
    kind       TEXT NOT NULL,
    depth      INTEGER NOT NULL DEFAULT 0,
    priority   INTEGER NOT NULL DEFAULT 50,
    data       TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_parent  ON tasks(parent_id);

CREATE TABLE IF NOT EXISTS runs (
    id           TEXT PRIMARY KEY,
    project_id   TEXT NOT NULL,
    task_id      TEXT,
    role         TEXT NOT NULL,
    provider     TEXT NOT NULL,
    model        TEXT NOT NULL,
    attempt      INTEGER NOT NULL DEFAULT 1,
    ok           INTEGER NOT NULL DEFAULT 1,
    error_type   TEXT,
    tokens_in    INTEGER NOT NULL DEFAULT 0,
    tokens_out   INTEGER NOT NULL DEFAULT 0,
    calls        INTEGER NOT NULL DEFAULT 0,
    cost_usd     REAL    NOT NULL DEFAULT 0.0,
    duration_s   REAL    NOT NULL DEFAULT 0.0,
    started_at   TEXT    NOT NULL,
    finished_at  TEXT,
    data         TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_id, started_at);
CREATE INDEX IF NOT EXISTS idx_runs_task    ON runs(task_id, attempt);

CREATE TABLE IF NOT EXISTS reviews (
    id         TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    task_id    TEXT NOT NULL,
    run_id     TEXT,
    verdict    TEXT NOT NULL,
    score      REAL NOT NULL DEFAULT 0.0,
    created_at TEXT NOT NULL,
    data       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reviews_task ON reviews(task_id, created_at);

CREATE TABLE IF NOT EXISTS artifacts (
    id         TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    task_id    TEXT,
    path       TEXT NOT NULL,
    kind       TEXT NOT NULL,
    content    TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifacts_project ON artifacts(project_id, created_at);

CREATE TABLE IF NOT EXISTS documents (
    project_id TEXT NOT NULL,
    key        TEXT NOT NULL,
    data       TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, key)
);
"""

#: Tables wiped by rebuild.  ``events`` is deliberately not here.
_PROJECTION_TABLES = (
    "projects",
    "requirements",
    "tasks",
    "runs",
    "reviews",
    "artifacts",
    "documents",
)

#: Which column scopes each projection table to a single project.  ``projects``
#: is keyed by ``id``; everything else carries a ``project_id``.
_PROJECTION_SCOPE: dict[str, str] = {
    "projects": "id",
    "requirements": "project_id",
    "tasks": "project_id",
    "runs": "project_id",
    "reviews": "project_id",
    "artifacts": "project_id",
    "documents": "project_id",
}


class Store:
    """Event-sourced persistence for one AgentCorp workspace."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if self.path.parent and str(self.path.parent) != "":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------ basics
    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock, self._conn:
            yield self._conn

    # ------------------------------------------------------------------ events
    def append(self, event: Event) -> Event:
        """Persist ``event`` and fold it into the projections atomically."""
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT INTO events (event_id, project_id, type, task_id, actor, payload,"
                " schema_version, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    event.event_id or short_hash([event.project_id, event.type.value, utcnow().isoformat()], 16),
                    event.project_id,
                    event.type.value,
                    event.task_id,
                    event.actor,
                    json.dumps(event.payload, default=str),
                    event.schema_version,
                    event.created_at.isoformat(),
                ),
            )
            stored = event.with_seq(int(cur.lastrowid or 0))
            self._project(conn, stored)
        return stored

    def append_many(self, events: Iterable[Event]) -> list[Event]:
        return [self.append(e) for e in events]

    def list_events(
        self,
        project_id: str,
        *,
        since_seq: int = 0,
        task_id: str | None = None,
        types: Sequence[EventType] | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        sql = "SELECT * FROM events WHERE project_id=? AND seq>?"
        args: list[Any] = [project_id, since_seq]
        if task_id is not None:
            sql += " AND task_id=?"
            args.append(task_id)
        if types:
            sql += f" AND type IN ({','.join('?' * len(types))})"
            args.extend(t.value for t in types)
        sql += " ORDER BY seq"
        if limit is not None:
            sql += " LIMIT ?"
            args.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_event(r) for r in rows]

    def event_count(self, project_id: str | None = None) -> int:
        with self._lock:
            if project_id is None:
                row = self._conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE project_id=?", (project_id,)
                ).fetchone()
        return int(row["c"])

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        return Event(
            seq=int(row["seq"]),
            event_id=row["event_id"],
            project_id=row["project_id"],
            type=EventType(row["type"]),
            task_id=row["task_id"],
            actor=row["actor"],
            payload=json.loads(row["payload"]),
            schema_version=int(row["schema_version"]),
            created_at=row["created_at"],
        )

    # -------------------------------------------------------------- projection
    def _project(self, conn: sqlite3.Connection, event: Event) -> None:
        """Fold one event into the queryable tables. Must be a pure function of
        (current projection state, event) — rebuilding depends on this."""
        handler = getattr(self, f"_proj_{event.type.value.lower()}", None)
        if handler is not None:
            handler(conn, event)

    def _upsert_task(self, conn: sqlite3.Connection, task: Task, project_id: str) -> None:
        conn.execute(
            "INSERT INTO tasks (id, project_id, parent_id, status, kind, depth, priority, data, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET project_id=excluded.project_id, parent_id=excluded.parent_id,"
            " status=excluded.status, kind=excluded.kind, depth=excluded.depth,"
            " priority=excluded.priority, data=excluded.data, updated_at=excluded.updated_at",
            (
                task.id,
                project_id,
                task.parent_id,
                task.status.value,
                task.kind.value,
                task.depth,
                task.priority,
                json.dumps(task.model_dump(mode="json"), default=str),
                task.updated_at.isoformat(),
            ),
        )

    def _load_task(self, conn: sqlite3.Connection, task_id: str) -> Task:
        row = conn.execute("SELECT data FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise StateError(f"event references unknown task {task_id!r}")
        return Task.model_validate(json.loads(row["data"]))

    def _mutate_task(self, conn: sqlite3.Connection, task_id: str, event: Event, **changes: Any) -> Task:
        """Apply field changes to a task.

        ``updated_at`` comes from ``event.created_at``, never from the wall
        clock: the projection must be a pure function of the log, otherwise
        :meth:`rebuild_projections` produces different bytes than the original
        write and the digest check fails.
        """
        task = self._load_task(conn, task_id).model_copy(update=changes)
        task.updated_at = event.created_at
        self._upsert_task(conn, task, self._project_of(conn, task_id))
        return task

    @staticmethod
    def _project_of(conn: sqlite3.Connection, task_id: str) -> str:
        row = conn.execute("SELECT project_id FROM tasks WHERE id=?", (task_id,)).fetchone()
        return str(row["project_id"]) if row else ""

    # --- individual handlers (name matters: _proj_<event_type_lower>) --------
    def _write_project(self, conn: sqlite3.Connection, project: Project, at: Any = None) -> None:
        if at is not None:
            project = project.model_copy(update={"updated_at": at})
        conn.execute(
            "INSERT INTO projects (id, data, updated_at) VALUES (?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
            (
                project.id,
                json.dumps(project.model_dump(mode="json"), default=str),
                project.updated_at.isoformat(),
            ),
        )

    def _proj_project_created(self, conn: sqlite3.Connection, event: Event) -> None:
        self._write_project(conn, Project.model_validate(event.payload["project"]))

    def _proj_project_paused(self, conn: sqlite3.Connection, event: Event) -> None:
        self._set_project_status(conn, event.project_id, "paused", event.created_at)

    def _proj_project_resumed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._set_project_status(conn, event.project_id, "active", event.created_at)

    def _proj_project_completed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._set_project_status(conn, event.project_id, "completed", event.created_at)

    def _proj_project_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._set_project_status(conn, event.project_id, "failed", event.created_at)

    def _set_project_status(
        self, conn: sqlite3.Connection, project_id: str, status: str, at: Any
    ) -> None:
        row = conn.execute("SELECT data FROM projects WHERE id=?", (project_id,)).fetchone()
        if row is None:
            raise StateError(f"cannot set status {status!r} on unknown project {project_id!r}")
        project = Project.model_validate(json.loads(row["data"]))
        self._write_project(conn, project.model_copy(update={"status": status, "updated_at": at}))

    def _proj_requirement_parsed(self, conn: sqlite3.Connection, event: Event) -> None:
        req = Requirement.model_validate(event.payload["requirement"])
        conn.execute(
            "INSERT INTO requirements (id, project_id, data) VALUES (?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET data=excluded.data",
            (req.id, event.project_id, json.dumps(req.model_dump(mode="json"), default=str)),
        )

    def _proj_repo_analyzed(self, conn: sqlite3.Connection, event: Event) -> None:
        ctx = RepositoryContext.model_validate(event.payload["context"])
        self._put_document(conn, event.project_id, "repo_context", ctx.model_dump(mode="json"), event.created_at)

    def _proj_task_created(self, conn: sqlite3.Connection, event: Event) -> None:
        task = Task.model_validate(event.payload["task"])
        self._upsert_task(conn, task, event.project_id)

    def _proj_task_updated(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        patch = dict(event.payload.get("patch", {}))
        patch.pop("id", None)
        self._mutate_task(conn, event.task_id, event, **patch)

    def _proj_task_ready(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event, status=TaskStatus.PENDING)

    def _proj_task_started(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        task = self._load_task(conn, event.task_id)
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.RUNNING,
            owner=event.payload.get("owner", task.owner),
            attempts=task.attempts + 1,
            started_at=event.created_at,
            blocked_reason=None,
        )

    def _proj_task_completed(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        task = self._load_task(conn, event.task_id)
        artifacts = list(dict.fromkeys([*task.artifacts, *event.payload.get("artifacts", [])]))
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.DONE,
            finished_at=event.created_at,
            failure_reason=None,
            blocked_reason=None,
            artifacts=artifacts,
        )

    def _proj_task_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.FAILED,
            finished_at=event.created_at,
            failure_reason=str(event.payload.get("reason", ""))[:2000],
        )

    def _proj_task_blocked(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.BLOCKED,
            blocked_reason=str(event.payload.get("reason", ""))[:2000],
        )

    def _proj_task_unblocked(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event, status=TaskStatus.PENDING, blocked_reason=None)

    def _proj_task_retried(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.PENDING,
            blocked_reason=None,
            failure_reason=None,
            owner=None,
        )

    def _proj_task_split(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.SPLIT,
            finished_at=event.created_at,
            blocked_reason=None,
        )

    def _proj_task_cancelled(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event,
            status=TaskStatus.CANCELLED,
            finished_at=event.created_at,
            failure_reason=str(event.payload.get("reason", "")),
        )

    def _proj_agent_run_started(self, conn: sqlite3.Connection, event: Event) -> None:
        run = AgentRun.model_validate(event.payload["run"])
        conn.execute(
            "INSERT INTO runs (id, project_id, task_id, role, provider, model, attempt, ok, error_type,"
            " tokens_in, tokens_out, calls, cost_usd, duration_s, started_at, finished_at, data)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET data=excluded.data",
            (
                run.id,
                run.project_id,
                run.task_id,
                run.role,
                run.provider,
                run.model,
                run.attempt,
                int(run.ok),
                run.error_type,
                run.usage.tokens_in,
                run.usage.tokens_out,
                run.usage.calls,
                run.usage.cost_usd,
                run.usage.duration_s,
                run.started_at.isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                json.dumps(run.model_dump(mode="json"), default=str),
            ),
        )

    def _proj_agent_run_finished(self, conn: sqlite3.Connection, event: Event) -> None:
        run = AgentRun.model_validate(event.payload["run"])
        conn.execute(
            "UPDATE runs SET ok=?, error_type=?, tokens_in=?, tokens_out=?, calls=?, cost_usd=?,"
            " duration_s=?, finished_at=?, data=? WHERE id=?",
            (
                int(run.ok),
                run.error_type,
                run.usage.tokens_in,
                run.usage.tokens_out,
                run.usage.calls,
                run.usage.cost_usd,
                run.usage.duration_s,
                run.finished_at.isoformat() if run.finished_at else run.started_at.isoformat(),
                json.dumps(run.model_dump(mode="json"), default=str),
                run.id,
            ),
        )

    def _proj_agent_run_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._proj_agent_run_finished(conn, event)

    def _proj_budget_updated(self, conn: sqlite3.Connection, event: Event) -> None:
        snap = BudgetSnapshot.model_validate(event.payload["budget"])
        self._put_document(conn, event.project_id, "budget", snap.model_dump(mode="json"), event.created_at)

    def _proj_budget_exceeded(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "budget_exceeded", dict(event.payload), event.created_at)

    def _proj_review_passed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._insert_review(conn, event)

    def _proj_review_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._insert_review(conn, event)

    def _insert_review(self, conn: sqlite3.Connection, event: Event) -> None:
        review = Review.model_validate(event.payload["review"])
        conn.execute(
            "INSERT INTO reviews (id, project_id, task_id, run_id, verdict, score, created_at, data)"
            " VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
            (
                review.id,
                event.project_id,
                review.task_id,
                review.run_id,
                review.verdict,
                review.score,
                review.created_at.isoformat(),
                json.dumps(review.model_dump(mode="json"), default=str),
            ),
        )

    def _proj_artifact_produced(self, conn: sqlite3.Connection, event: Event) -> None:
        art = Artifact.model_validate(event.payload["artifact"])
        conn.execute(
            "INSERT INTO artifacts (id, project_id, task_id, path, kind, content, created_at)"
            " VALUES (?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET content=excluded.content",
            (
                art.id,
                event.project_id,
                art.task_id,
                art.path,
                art.kind,
                art.content,
                art.created_at.isoformat(),
            ),
        )

    def _proj_supervisor_intervention(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, f"intervention:{event.payload.get('id', event.seq)}", dict(event.payload), event.created_at)

    def _proj_supervisor_tick(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "last_supervisor_tick", dict(event.payload), event.created_at)

    def _proj_note(self, conn: sqlite3.Connection, event: Event) -> None:
        return None

    def _proj_plan_created(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "plan", dict(event.payload), event.created_at)

    def _proj_chaos_injected(self, conn: sqlite3.Connection, event: Event) -> None:
        return None

    def _proj_resume(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "last_resume", dict(event.payload), event.created_at)

    # ------------------------------------------------------------- projections
    def _put_document(
        self, conn: sqlite3.Connection, project_id: str, key: str, data: dict[str, Any], at: Any
    ) -> None:
        conn.execute(
            "INSERT INTO documents (project_id, key, data, updated_at) VALUES (?,?,?,?)"
            " ON CONFLICT(project_id, key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
            (project_id, key, json.dumps(data, default=str), at.isoformat()),
        )

    def put_document(self, project_id: str, key: str, data: dict[str, Any]) -> None:
        """Write a non-event's worth of data (reports, caches).

        Prefer an event when the write is part of the delivery story; this is
        for derived artefacts that can always be recomputed.
        """
        with self._tx() as conn:
            self._put_document(conn, project_id, key, data, utcnow())

    def get_document(self, project_id: str, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM documents WHERE project_id=? AND key=?", (project_id, key)
            ).fetchone()
        return json.loads(row["data"]) if row else None

    def list_document_keys(self, project_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM documents WHERE project_id=? ORDER BY key", (project_id,)
            ).fetchall()
        return [str(r["key"]) for r in rows]

    # ---------------------------------------------------------------- projects
    def get_project(self, project_id: str) -> Project | None:
        with self._lock:
            row = self._conn.execute("SELECT data FROM projects WHERE id=?", (project_id,)).fetchone()
        return Project.model_validate(json.loads(row["data"])) if row else None

    def list_projects(self) -> list[Project]:
        with self._lock:
            rows = self._conn.execute("SELECT data FROM projects ORDER BY updated_at DESC").fetchall()
        return [Project.model_validate(json.loads(r["data"])) for r in rows]

    def latest_project(self) -> Project | None:
        projects = self.list_projects()
        return projects[0] if projects else None

    def resolve_project(self, name_or_id: str | None = None) -> Project | None:
        """Accept a project id, a project name, or ``None`` (⇒ most recent)."""
        if name_or_id is None:
            return self.latest_project()
        project = self.get_project(name_or_id)
        if project is not None:
            return project
        for candidate in self.list_projects():
            if candidate.name == name_or_id:
                return candidate
        return None

    # ------------------------------------------------------------------- tasks
    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._conn.execute("SELECT data FROM tasks WHERE id=?", (task_id,)).fetchone()
        return Task.model_validate(json.loads(row["data"])) if row else None

    def list_tasks(self, project_id: str) -> list[Task]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM tasks WHERE project_id=? ORDER BY depth, priority DESC, id",
                (project_id,),
            ).fetchall()
        return [Task.model_validate(json.loads(r["data"])) for r in rows]

    def children_of(self, task_id: str) -> list[Task]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM tasks WHERE parent_id=? ORDER BY id", (task_id,)
            ).fetchall()
        return [Task.model_validate(json.loads(r["data"])) for r in rows]

    def status_counts(self, project_id: str) -> dict[TaskStatus, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS c FROM tasks WHERE project_id=? GROUP BY status", (project_id,)
            ).fetchall()
        counts = {s: 0 for s in TaskStatus}
        for row in rows:
            counts[TaskStatus(row["status"])] = int(row["c"])
        return counts

    def get_requirement(self, project_id: str) -> Requirement | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM requirements WHERE project_id=? ORDER BY rowid DESC LIMIT 1",
                (project_id,),
            ).fetchone()
        return Requirement.model_validate(json.loads(row["data"])) if row else None

    # -------------------------------------------------------------------- runs
    def get_run(self, run_id: str) -> AgentRun | None:
        with self._lock:
            row = self._conn.execute("SELECT data FROM runs WHERE id=?", (run_id,)).fetchone()
        return AgentRun.model_validate(json.loads(row["data"])) if row else None

    def list_runs(
        self, project_id: str, *, task_id: str | None = None, role: str | None = None, limit: int | None = None
    ) -> list[AgentRun]:
        sql = "SELECT data FROM runs WHERE project_id=?"
        args: list[Any] = [project_id]
        if task_id:
            sql += " AND task_id=?"
            args.append(task_id)
        if role:
            sql += " AND role=?"
            args.append(role)
        sql += " ORDER BY started_at, rowid"
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [AgentRun.model_validate(json.loads(r["data"])) for r in rows]

    def unfinished_runs(self, project_id: str) -> list[AgentRun]:
        """Runs that started but never reported — evidence of a crash.

        ``resume`` uses this to detect tasks that were mid-flight when the
        process died, instead of trusting the (stale) ``running`` status.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM runs WHERE project_id=? AND finished_at IS NULL ORDER BY started_at",
                (project_id,),
            ).fetchall()
        return [AgentRun.model_validate(json.loads(r["data"])) for r in rows]

    # ----------------------------------------------------------------- reviews
    def list_reviews(self, project_id: str, *, task_id: str | None = None) -> list[Review]:
        sql = "SELECT data FROM reviews WHERE project_id=?"
        args: list[Any] = [project_id]
        if task_id:
            sql += " AND task_id=?"
            args.append(task_id)
        sql += " ORDER BY created_at, rowid"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [Review.model_validate(json.loads(r["data"])) for r in rows]

    def reviews_for_task(self, task_id: str) -> list[Review]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT data FROM reviews WHERE task_id=? ORDER BY created_at, rowid", (task_id,)
            ).fetchall()
        return [Review.model_validate(json.loads(r["data"])) for r in rows]

    # --------------------------------------------------------------- artifacts
    def list_artifacts(self, project_id: str, *, task_id: str | None = None) -> list[Artifact]:
        sql = "SELECT * FROM artifacts WHERE project_id=?"
        args: list[Any] = [project_id]
        if task_id:
            sql += " AND task_id=?"
            args.append(task_id)
        sql += " ORDER BY created_at, rowid"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [
            Artifact(
                id=r["id"],
                project_id=r["project_id"],
                task_id=r["task_id"],
                path=r["path"],
                kind=r["kind"],
                content=r["content"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ usage
    _USAGE_COLUMNS = (
        "COALESCE(SUM(tokens_in),0) AS ti, COALESCE(SUM(tokens_out),0) AS to_,"
        " COALESCE(SUM(calls),0) AS c, COALESCE(SUM(cost_usd),0) AS cost,"
        " COALESCE(SUM(duration_s),0) AS dur"
    )

    def usage_for_project(self, project_id: str) -> Usage:
        return self._usage("project_id=?", (project_id,))

    def usage_for_task(self, task_id: str) -> Usage:
        return self._usage("task_id=?", (task_id,))

    def usage_by_role(self, project_id: str) -> dict[str, Usage]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT role, COALESCE(SUM(tokens_in),0) ti, COALESCE(SUM(tokens_out),0) to_,"
                " COALESCE(SUM(calls),0) c, COALESCE(SUM(cost_usd),0) cost, COALESCE(SUM(duration_s),0) dur"
                " FROM runs WHERE project_id=? GROUP BY role",
                (project_id,),
            ).fetchall()
        return {
            str(r["role"]): Usage(
                tokens_in=int(r["ti"]),
                tokens_out=int(r["to_"]),
                calls=int(r["c"]),
                cost_usd=float(r["cost"]),
                duration_s=float(r["dur"]),
            )
            for r in rows
        }

    def usage_by_task(self, project_id: str) -> dict[str, Usage]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_id, COALESCE(SUM(tokens_in),0) ti, COALESCE(SUM(tokens_out),0) to_,"
                " COALESCE(SUM(calls),0) c, COALESCE(SUM(cost_usd),0) cost, COALESCE(SUM(duration_s),0) dur"
                " FROM runs WHERE project_id=? AND task_id IS NOT NULL GROUP BY task_id",
                (project_id,),
            ).fetchall()
        return {
            str(r["task_id"]): Usage(
                tokens_in=int(r["ti"]),
                tokens_out=int(r["to_"]),
                calls=int(r["c"]),
                cost_usd=float(r["cost"]),
                duration_s=float(r["dur"]),
            )
            for r in rows
        }

    def _usage(self, where: str, args: Sequence[Any]) -> Usage:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {self._USAGE_COLUMNS} FROM runs WHERE {where}",  # noqa: S608 - `where` is a literal from callers
                args,
            ).fetchone()
        if row is None:
            return Usage()
        return Usage(
            tokens_in=int(row["ti"]),
            tokens_out=int(row["to_"]),
            calls=int(row["c"]),
            cost_usd=float(row["cost"]),
            duration_s=float(row["dur"]),
        )

    # ---------------------------------------------------------------- rebuild
    def rebuild_projections(self, project_id: str | None = None) -> int:
        """Replay the event log into the projection tables.

        Returns the number of events applied.  Safe to run at any time; the
        test suite asserts it is idempotent.
        """
        with self._tx() as conn:
            for table in _PROJECTION_TABLES:
                if project_id is None:
                    conn.execute(f"DELETE FROM {table}")  # noqa: S608 - table names are a fixed tuple
                else:
                    scope = _PROJECTION_SCOPE[table]
                    conn.execute(f"DELETE FROM {table} WHERE {scope}=?", (project_id,))  # noqa: S608
            if project_id is None:
                rows = conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM events WHERE project_id=? ORDER BY seq", (project_id,)
                ).fetchall()
            for row in rows:
                self._project(conn, self._row_to_event(row))
        return len(rows)

    def projection_digest(self, project_id: str | None = None) -> str:
        """Stable fingerprint of all queryable state (for consistency tests)."""
        payload: dict[str, Any] = {}
        with self._lock:
            for table in _PROJECTION_TABLES:
                if project_id is None:
                    rows = self._conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()  # noqa: S608
                else:
                    scope = _PROJECTION_SCOPE[table]
                    rows = self._conn.execute(
                        f"SELECT * FROM {table} WHERE {scope}=? ORDER BY 1, 2", (project_id,)  # noqa: S608
                    ).fetchall()
                payload[table] = [dict(r) for r in rows]
        return short_hash(payload, 24)

    def verify_integrity(self, project_id: str) -> list[str]:
        """Cheap invariants.  Returns a list of human-readable problems."""
        problems: list[str] = []
        tasks = {t.id: t for t in self.list_tasks(project_id)}
        for task in tasks.values():
            for dep in task.dependencies:
                if dep not in tasks:
                    problems.append(f"task {task.id} depends on missing task {dep}")
            if task.parent_id and task.parent_id not in tasks:
                problems.append(f"task {task.id} references missing parent {task.parent_id}")
            if task.status is TaskStatus.DONE and not task.acceptance_criteria:
                problems.append(f"task {task.id} is done but declares no acceptance criteria")
        for run in self.unfinished_runs(project_id):
            problems.append(f"run {run.id} (task {run.task_id}) started but never finished")
        budget_doc = self.get_document(project_id, "budget")
        if budget_doc is None:
            problems.append("no budget snapshot recorded")
        return problems

    # ------------------------------------------------------------------ export
    def export_state(self, project_id: str) -> dict[str, Any]:
        """JSON-serialisable snapshot — used by the web UI and the report."""
        return {
            "project": (self.get_project(project_id).model_dump(mode="json") if self.get_project(project_id) else None),
            "requirement": (
                self.get_requirement(project_id).model_dump(mode="json")
                if self.get_requirement(project_id)
                else None
            ),
            "tasks": [t.model_dump(mode="json") for t in self.list_tasks(project_id)],
            "reviews": [r.model_dump(mode="json") for r in self.list_reviews(project_id)],
            "runs": [r.model_dump(mode="json") for r in self.list_runs(project_id)],
            "artifacts": [a.model_dump(mode="json") for a in self.list_artifacts(project_id)],
            "usage": self.usage_for_project(project_id).model_dump(mode="json"),
            "usage_by_role": {k: v.model_dump(mode="json") for k, v in self.usage_by_role(project_id).items()},
            "usage_by_task": {k: v.model_dump(mode="json") for k, v in self.usage_by_task(project_id).items()},
            "status_counts": {k.value: v for k, v in self.status_counts(project_id).items()},
            "event_count": self.event_count(project_id),
        }
