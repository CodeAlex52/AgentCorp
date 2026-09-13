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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .errors import StateError
from .events import Event, EventType
from .models import (
    AgentRun,
    Artifact,
    BudgetSnapshot,
    Project,
    RepositoryContext,
    Requirement,
    Review,
    Task,
    TaskStatus,
    Usage,
    assert_transition,
)
from .util import new_id, short_hash, utcnow

__all__ = ["Store", "DEFAULT_DB_NAME"]

DEFAULT_DB_NAME = "agentcorp.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_seq        INTEGER NOT NULL DEFAULT 0,
    event_id       TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    type           TEXT    NOT NULL,
    task_id        TEXT,
    actor          TEXT    NOT NULL,
    payload        TEXT    NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_project ON events(project_id, run_seq);
-- FIND-005: at-least-once redelivery must not duplicate the audit log.
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);
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

-- Derived caches (reports, exports).  Deliberately *not* a projection: their
-- content can always be recomputed from the log, and including them would break
-- the "replay reproduces the projection byte-for-byte" invariant.
CREATE TABLE IF NOT EXISTS derived (
    project_id TEXT NOT NULL,
    key        TEXT NOT NULL,
    data       TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, key)
);

-- Cross-process control signals (cancel requests).  Inputs, not outputs: they
-- must survive projection rebuilds and are never derived from the log.
CREATE TABLE IF NOT EXISTS control (
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
        # A competing writer (another process sharing the DB, e.g. a SIGKILL
        # recovery test) must be retried, not surfaced as `database is locked`.
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """Bring a pre-existing dev database up to the current schema.

        v0 has no data in the wild, but silently reading an older file as if it
        had ``run_seq`` would corrupt ordering guarantees, so backfill it once.
        """
        columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if "run_seq" not in columns:  # pragma: no cover - only on legacy files
            self._conn.execute("ALTER TABLE events ADD COLUMN run_seq INTEGER NOT NULL DEFAULT 0")
            projects = [
                str(row["project_id"])
                for row in self._conn.execute("SELECT DISTINCT project_id FROM events").fetchall()
            ]
            for project_id in projects:
                rows = self._conn.execute(
                    "SELECT seq FROM events WHERE project_id=? ORDER BY seq", (project_id,)
                ).fetchall()
                for index, row in enumerate(rows, start=1):
                    self._conn.execute(
                        "UPDATE events SET run_seq=? WHERE seq=?", (index, int(row["seq"]))
                    )
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)"
        )

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
    def _insert_event(self, conn: sqlite3.Connection, event: Event) -> Event:
        """Insert + project one event on an open transaction. Never commits.

        * ``run_seq`` is a per-project counter, so a run's stream is contiguous
          even when several runs share one SQLite file (FIND-009); ``Event.seq``
          is that run-scoped value.
        * An explicit ``event_id`` makes the append idempotent (FIND-005):
          re-delivering the same fact returns the stored event and projects
          nothing twice; the same id with different content is a hard error.
        """
        payload_json = json.dumps(event.payload, default=str)
        if event.event_id:
            existing = conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event.event_id,)
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["project_id"]) != event.project_id
                    or str(existing["type"]) != event.type.value
                    or existing["task_id"] != event.task_id
                    or json.loads(existing["payload"]) != event.payload
                ):
                    msg = (
                        f"event_id {event.event_id!r} already exists with different content"
                    )
                    raise StateError(msg)
                return self._row_to_event(existing)
            event_id = event.event_id
        else:
            event_id = new_id("EV")
        run_seq_row = conn.execute(
            "SELECT COALESCE(MAX(run_seq), 0) AS m FROM events WHERE project_id=?",
            (event.project_id,),
        ).fetchone()
        run_seq = int(run_seq_row["m"]) + 1 if run_seq_row is not None else 1
        conn.execute(
            "INSERT INTO events (run_seq, event_id, project_id, type, task_id, actor, payload,"
            " schema_version, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                run_seq,
                event_id,
                event.project_id,
                event.type.value,
                event.task_id,
                event.actor,
                payload_json,
                event.schema_version,
                event.created_at.isoformat(),
            ),
        )
        stored = event.with_seq(run_seq)
        self._project(conn, stored)
        return stored

    def append(self, event: Event) -> Event:
        """Persist ``event`` and fold it into the projections atomically."""
        with self._tx() as conn:
            return self._insert_event(conn, event)

    def append_many(self, events: Iterable[Event]) -> list[Event]:
        """Append a batch in **one** transaction — all of it or none of it.

        SPEC C8 needs the child-DAG insertion to be atomic: a validation error
        halfway through must not leave orphan children in the projection.
        """
        batch = list(events)
        if not batch:
            return []
        with self._tx() as conn:
            return [self._insert_event(conn, event) for event in batch]

    def list_events(
        self,
        project_id: str,
        *,
        since_seq: int = 0,
        task_id: str | None = None,
        types: Sequence[EventType] | None = None,
        limit: int | None = None,
    ) -> list[Event]:
        sql = "SELECT * FROM events WHERE project_id=? AND run_seq>?"
        args: list[Any] = [project_id, since_seq]
        if task_id is not None:
            sql += " AND task_id=?"
            args.append(task_id)
        if types:
            sql += f" AND type IN ({','.join('?' * len(types))})"
            args.extend(t.value for t in types)
        sql += " ORDER BY run_seq"
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

    def event_seqs(self, project_id: str) -> list[int]:
        """All seq values for one project, ascending (gap + monotonicity tests)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_seq FROM events WHERE project_id=? ORDER BY run_seq", (project_id,)
            ).fetchall()
        return [int(r["run_seq"]) for r in rows]

    def event_type_counts(self, project_id: str) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT type, COUNT(*) AS c FROM events WHERE project_id=? GROUP BY type",
                (project_id,),
            ).fetchall()
        return {str(r["type"]): int(r["c"]) for r in rows}

    def last_run_summary(self, project_id: str) -> dict[str, Any] | None:
        """The ``RUN_FINISHED`` payload, if the run has finished."""
        return self.get_document(project_id, "run_summary")

    # ------------------------------------------------------- claim and lease
    def claim_task(
        self,
        task_id: str,
        *,
        owner: str,
        lease_seconds: float = 300.0,
        now: datetime | None = None,
        actor: str = "scheduler",
    ) -> list[Event] | None:
        """Atomically move a READY task to RUNNING and lease it (SPEC §5.2, C4).

        Returns ``[TASK_CLAIMED, TASK_STARTED]`` on success, ``None`` if another
        claimer got there first.  The guard is a single
        ``UPDATE ... WHERE status='ready'`` so the winner is decided by SQLite's
        row lock, never by a read-then-write race.
        """
        at = now or utcnow()
        expires = at.timestamp() + lease_seconds
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE tasks SET status=? WHERE id=? AND status=?",
                (TaskStatus.RUNNING.value, task_id, TaskStatus.READY.value),
            )
            if cur.rowcount != 1:
                return None
            lease_iso = datetime.fromtimestamp(expires, tz=UTC).isoformat()
            claimed = self._insert_event(
                conn,
                Event(
                    project_id=self._project_of(conn, task_id),
                    type=EventType.TASK_CLAIMED,
                    task_id=task_id,
                    actor=actor,
                    created_at=at,
                    payload={
                        "owner": owner,
                        "lease_seconds": lease_seconds,
                        "lease_expires_at": lease_iso,
                    },
                ),
            )
            started = self._insert_event(
                conn,
                Event(
                    project_id=self._project_of(conn, task_id),
                    type=EventType.TASK_STARTED,
                    task_id=task_id,
                    actor=actor,
                    created_at=at,
                    payload={"owner": owner, "claim_seq": claimed.seq},
                ),
            )
            return [claimed, started]

    def renew_lease(
        self,
        task_id: str,
        *,
        lease_seconds: float = 300.0,
        now: datetime | None = None,
    ) -> Event:
        """Extend a RUNNING task's lease through a ``TASK_UPDATED`` event.

        The baseline renewed leases and touched progress harness-side, which
        silently broke replay equivalence (the projection contained writes that
        were not derivable from the log).  Every mutation is an event (DEC-003).
        """
        at = now or utcnow()
        with self._tx() as conn:
            task = self._load_task(conn, task_id)
            if task.status is not TaskStatus.RUNNING:
                msg = f"cannot renew lease of task {task_id!r} in status {task.status.value!r}"
                raise StateError(msg)
            lease_iso = datetime.fromtimestamp(
                at.timestamp() + lease_seconds, tz=UTC
            ).isoformat()
            return self._insert_event(
                conn,
                Event(
                    project_id=self._project_of(conn, task_id),
                    type=EventType.TASK_UPDATED,
                    task_id=task_id,
                    actor="scheduler",
                    created_at=at,
                    payload={"patch": {"lease_expires_at": lease_iso}, "reason": "lease_renewal"},
                ),
            )

    #: Statuses an interrupted attempt can be left in (C5).  REVIEW is included
    #: because a crash between REVIEW_STARTED and the verdict is just as
    #: orphaned as one mid-RUNNING (FIND-014).
    RECOVERABLE_STATUSES: frozenset[TaskStatus] = frozenset(
        {TaskStatus.RUNNING, TaskStatus.REVIEW}
    )

    def stale_running(self, now: datetime | None = None, *, force: bool = False) -> list[Task]:
        """Interrupted tasks whose attempt cannot be alive any more (C5).

        A task is stale when ``now > lease_expires_at`` (or it never had a
        lease, which is the safe direction for recovery).  ``force=True`` is the
        explicit "the owning process is gone" override used by ``resume``.
        """
        at = now or utcnow()
        out: list[Task] = []
        for task in self.list_tasks_all():
            if task.status not in self.RECOVERABLE_STATUSES:
                continue
            if force:
                out.append(task)
                continue
            deadline = task.lease_expires_at or task.started_at or task.updated_at
            if deadline <= at:
                out.append(task)
        return out

    def lease_alive(self, task: Task, now: datetime | None = None) -> bool:
        """True when the task's lease has not expired yet."""
        if task.status not in self.RECOVERABLE_STATUSES:
            return False
        at = now or utcnow()
        deadline = task.lease_expires_at or task.started_at or task.updated_at
        return deadline > at

    def list_tasks_all(self) -> list[Task]:
        """Every task in the store, across projects (recovery sweeps)."""
        with self._lock:
            rows = self._conn.execute("SELECT data FROM tasks ORDER BY id").fetchall()
        return [Task.model_validate(json.loads(r["data"])) for r in rows]

    def recover_task(
        self,
        task_id: str,
        *,
        reason: str = "lease_expired",
        now: datetime | None = None,
        force: bool = False,
    ) -> bool:
        """Re-dispatch an interrupted RUNNING/REVIEW task through a legal path.

        ``RUNNING|REVIEW -> FAILED -> READY`` (DEC-006): the interrupted attempt
        counts as a failure; when the attempt budget is spent the task is
        quarantined instead.  Returns ``True`` when the task is READY again.

        * ``force=False`` (default) refuses to touch a task whose lease is still
          valid, so a live worker can never be double-dispatched (FIND-002).
        * The first step is a guarded ``UPDATE ... WHERE status IN (...)'', so
          concurrent resumers lose cleanly with ``False`` instead of raising
          ``StateError`` (FIND-003), exactly like :meth:`claim_task`.
        """
        at = now or utcnow()
        with self._tx() as conn:
            task = self._load_task(conn, task_id)
            if task.status not in self.RECOVERABLE_STATUSES:
                return False
            if not force and self.lease_alive(task, at):
                return False
            project_id = self._project_of(conn, task_id)
            cur = conn.execute(
                "UPDATE tasks SET status=? WHERE id=? AND status IN (?,?)",
                (
                    TaskStatus.FAILED.value,
                    task_id,
                    TaskStatus.RUNNING.value,
                    TaskStatus.REVIEW.value,
                ),
            )
            if cur.rowcount != 1:
                return False
            self._insert_event(
                conn,
                Event(
                    project_id=project_id,
                    type=EventType.TASK_FAILED,
                    task_id=task_id,
                    actor="recovery",
                    created_at=at,
                    payload={"reason": reason, "recoverable": True},
                ),
            )
            refreshed = self._load_task(conn, task_id)
            if refreshed.attempts >= refreshed.max_attempts:
                self._insert_event(
                    conn,
                    Event(
                        project_id=project_id,
                        type=EventType.TASK_QUARANTINED,
                        task_id=task_id,
                        actor="recovery",
                        created_at=at,
                        payload={"reason": f"{reason}: attempts exhausted"},
                    ),
                )
                return False
            self._insert_event(
                conn,
                Event(
                    project_id=project_id,
                    type=EventType.TASK_RETRIED,
                    task_id=task_id,
                    actor="recovery",
                    created_at=at,
                    payload={"reason": reason, "recoverable": True},
                ),
            )
        return True

    #: Backwards-compatible name; prefer :meth:`recover_task`.
    def recover_stale_task(
        self,
        task_id: str,
        *,
        reason: str = "lease_expired",
        now: datetime | None = None,
        max_attempts: int | None = None,
        force: bool = False,
    ) -> bool:
        del max_attempts  # attempt budget is always read from the task itself
        return self.recover_task(task_id, reason=reason, now=now, force=force)


    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        return Event(
            seq=int(row["run_seq"]),
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
        """Apply field changes to a task, enforcing the transition whitelist.

        ``updated_at`` comes from ``event.created_at``, never from the wall
        clock: the projection must be a pure function of the log, otherwise
        :meth:`rebuild_projections` produces different bytes than the original
        write and the digest check fails.
        """
        task = self._load_task(conn, task_id)
        target = changes.get("status")
        if isinstance(target, TaskStatus):
            # Enforced here too (not just in the scheduler) so that replaying a
            # hand-crafted or corrupted log fails loudly instead of projecting
            # an impossible state (DEC-003).
            assert_transition(task, target, reason=f"event {event.type.value}")
        task = task.model_copy(update=changes)
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
        if "status" in patch:
            patch["status"] = TaskStatus(patch["status"])
        # Validate the merged document so string datetimes from the payload are
        # coerced before they reach the JSON column.
        current = self._load_task(conn, event.task_id)
        merged = {**current.model_dump(), **patch}
        updated = Task.model_validate(merged)
        target = patch.get("status")
        if isinstance(target, TaskStatus):
            assert_transition(current, target, reason="TASK_UPDATED")
        updated.updated_at = event.created_at
        self._upsert_task(conn, updated, self._project_of(conn, event.task_id))

    def _proj_task_ready(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(conn, event.task_id, event, status=TaskStatus.READY)

    def _proj_task_claimed(self, conn: sqlite3.Connection, event: Event) -> None:
        """Record the lease. Status is flipped by the guarded UPDATE in claim_task."""
        assert event.task_id is not None
        task = self._load_task(conn, event.task_id)
        lease_iso = event.payload.get("lease_expires_at")
        lease = (
            datetime.fromisoformat(str(lease_iso)) if lease_iso else task.lease_expires_at
        )
        self._mutate_task(
            conn,
            event.task_id,
            event,
            owner=event.payload.get("owner", task.owner),
            claimed_at=event.created_at,
            lease_expires_at=lease,
            progress_at=event.created_at,
        )

    def _proj_task_started(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        task = self._load_task(conn, event.task_id)
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.RUNNING,
            owner=event.payload.get("owner", task.owner),
            attempts=task.attempts + 1,
            started_at=event.created_at,
            progress_at=event.created_at,
            blocked_reason=None,
        )

    def _proj_task_finished(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        task = self._load_task(conn, event.task_id)
        artifacts = list(dict.fromkeys([*task.artifacts, *event.payload.get("artifacts", [])]))
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.DONE,
            finished_at=event.created_at,
            failure_reason=None,
            blocked_reason=None,
            lease_expires_at=None,
            artifacts=artifacts,
        )

    # Baseline alias.
    _proj_task_completed = _proj_task_finished

    def _proj_task_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.FAILED,
            finished_at=event.created_at,
            lease_expires_at=None,
            failure_reason=str(event.payload.get("reason", ""))[:2000],
        )

    def _proj_task_blocked(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.BLOCKED,
            blocked_reason=str(event.payload.get("reason", ""))[:2000],
        )

    def _proj_task_unblocked(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.READY,
            blocked_reason=None,
            failure_reason=None,
            finished_at=None,
        )

    def _proj_task_retried(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.READY,
            blocked_reason=None,
            failure_reason=None,
            owner=None,
            claimed_at=None,
            lease_expires_at=None,
            # FIND-004: a re-dispatched task is in flight again; started/finished
            # must bracket the *current* attempt, never the dead one.
            finished_at=None,
        )

    def _proj_task_rework(self, conn: sqlite3.Connection, event: Event) -> None:
        """``REVIEW -> READY`` after a rejection (DEC-007)."""
        assert event.task_id is not None
        task = self._load_task(conn, event.task_id)
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.READY,
            blocked_reason=None,
            failure_reason=None,
            rework_count=task.rework_count + 1,
            owner=None,
            lease_expires_at=None,
            finished_at=None,
        )

    def _proj_task_split(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.SPLIT,
            blocked_reason=None,
            lease_expires_at=None,
        )

    def _proj_task_aggregated(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        raw = str(event.payload.get("status", TaskStatus.DONE.value))
        target = TaskStatus(raw)
        if target not in {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            msg = f"TASK_AGGREGATED cannot aggregate to {raw!r}"
            raise StateError(msg)
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=target,
            finished_at=event.created_at,
            failure_reason=(
                str(event.payload.get("reason", ""))[:2000] if target is not TaskStatus.DONE else None
            ),
        )

    def _proj_task_quarantined(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.QUARANTINED,
            finished_at=event.created_at,
            failure_reason=str(event.payload.get("reason", ""))[:2000],
        )

    def _proj_task_cancelled(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.CANCELLED,
            finished_at=event.created_at,
            lease_expires_at=None,
            failure_reason=str(event.payload.get("reason", ""))[:2000],
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
        if run.task_id and run.finished_at is not None:
            row = conn.execute("SELECT data FROM tasks WHERE id=?", (run.task_id,)).fetchone()
            if row is not None:
                task = Task.model_validate(json.loads(row["data"]))
                if task.status is TaskStatus.RUNNING:
                    # A finished agent call is the canonical progress signal.
                    self._mutate_task(conn, run.task_id, event, progress_at=event.created_at)
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
        self._put_document(conn, event.project_id, "budget_exhausted", dict(event.payload), event.created_at)

    #: SPEC §5.4 name.
    _proj_budget_exhausted = _proj_budget_exceeded

    def _proj_budget_warning(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "budget_warning", dict(event.payload), event.created_at)

    # ------------------------------------------------------------------- runs
    def _proj_run_started(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(
            conn,
            event.project_id,
            "run_state",
            {"status": "RUNNING", **dict(event.payload)},
            event.created_at,
        )
        self._set_project_status(conn, event.project_id, "active", event.created_at)

    def _proj_run_resumed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(
            conn,
            event.project_id,
            "run_state",
            {"status": "RESUMED", **dict(event.payload)},
            event.created_at,
        )
        self._put_document(conn, event.project_id, "last_resume", dict(event.payload), event.created_at)
        self._set_project_status(conn, event.project_id, "active", event.created_at)

    def _proj_run_finished(self, conn: sqlite3.Connection, event: Event) -> None:
        status = str(event.payload.get("status", "FAILED"))
        self._put_document(
            conn,
            event.project_id,
            "run_summary",
            {
                "status": status,
                "run_id": event.payload.get("run_id", event.project_id),
                "finished_at": event.created_at.isoformat(),
                **dict(event.payload),
            },
            event.created_at,
        )
        project_status = {
            "DONE": "completed",
            "FAILED": "failed",
            "BUDGET_EXHAUSTED": "failed",
            "DEADLOCK": "failed",
            "CANCELLED": "cancelled",
        }.get(status, "failed")
        self._set_project_status(conn, event.project_id, project_status, event.created_at)

    def _proj_review_passed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._insert_review(conn, event)

    def _proj_review_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._insert_review(conn, event)

    def _proj_review_started(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.REVIEW,
            progress_at=event.created_at,
        )

    def _proj_review_approved(self, conn: sqlite3.Connection, event: Event) -> None:
        assert event.task_id is not None
        self._insert_review(conn, event)
        task = self._load_task(conn, event.task_id)
        artifacts = list(
            dict.fromkeys([*task.artifacts, *event.payload.get("artifacts", [])])
        )
        self._mutate_task(
            conn,
            event.task_id,
            event,
            status=TaskStatus.DONE,
            finished_at=event.created_at,
            failure_reason=None,
            blocked_reason=None,
            lease_expires_at=None,
            artifacts=artifacts,
        )

    def _proj_review_rejected(self, conn: sqlite3.Connection, event: Event) -> None:
        """Record the verdict; the status move is TASK_REWORK / TASK_FAILED (DEC-007)."""
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
        if art.task_id:
            task = self._load_task(conn, art.task_id)
            if art.path not in task.artifacts:
                self._mutate_task(
                    conn,
                    art.task_id,
                    event,
                    artifacts=[*task.artifacts, art.path],
                    progress_at=event.created_at,
                )

    def _proj_supervisor_intervention(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, f"intervention:{event.payload.get('id', event.seq)}", dict(event.payload), event.created_at)

    def _proj_supervisor_tick(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "last_supervisor_tick", dict(event.payload), event.created_at)

    def _proj_intervention_raised(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(
            conn,
            event.project_id,
            f"intervention:{event.payload.get('id', event.seq)}",
            dict(event.payload),
            event.created_at,
        )

    def _proj_intervention_applied(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(
            conn,
            event.project_id,
            f"intervention_applied:{event.payload.get('id', event.seq)}",
            dict(event.payload),
            event.created_at,
        )

    def _proj_note(self, _conn: sqlite3.Connection, _event: Event) -> None:
        return None

    def _proj_plan_created(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "plan", dict(event.payload), event.created_at)

    def _proj_chaos_injected(self, _conn: sqlite3.Connection, _event: Event) -> None:
        return None

    def _proj_resume(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "last_resume", dict(event.payload), event.created_at)

    def _proj_cycle_detected(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "cycle_detected", dict(event.payload), event.created_at)

    def _proj_deadlock_detected(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "deadlock", dict(event.payload), event.created_at)

    def _proj_validation_failed(self, conn: sqlite3.Connection, event: Event) -> None:
        self._put_document(conn, event.project_id, "validation_failed", dict(event.payload), event.created_at)

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
        """Write a document that is *not* event-derived (e.g. cancel requests).

        Anything that is part of the delivery story should be an event; anything
        that can be recomputed belongs in :meth:`put_derived`.
        """
        with self._tx() as conn:
            self._put_document(conn, project_id, key, data, utcnow())

    def put_derived(self, project_id: str, key: str, data: dict[str, Any]) -> None:
        """Cache a recomputable artefact (run report, export) outside projections."""
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO derived (project_id, key, data, updated_at) VALUES (?,?,?,?)"
                " ON CONFLICT(project_id, key) DO UPDATE SET data=excluded.data,"
                " updated_at=excluded.updated_at",
                (project_id, key, json.dumps(data, default=str), utcnow().isoformat()),
            )

    def get_derived(self, project_id: str, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM derived WHERE project_id=? AND key=?",
                (project_id, key),
            ).fetchone()
        return json.loads(row["data"]) if row else None

    def put_control(self, project_id: str, key: str, data: dict[str, Any]) -> None:
        """Record an out-of-band control signal (e.g. a cancel request)."""
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO control (project_id, key, data, updated_at) VALUES (?,?,?,?)"
                " ON CONFLICT(project_id, key) DO UPDATE SET data=excluded.data,"
                " updated_at=excluded.updated_at",
                (project_id, key, json.dumps(data, default=str), utcnow().isoformat()),
            )

    def get_control(self, project_id: str, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM control WHERE project_id=? AND key=?",
                (project_id, key),
            ).fetchone()
        return json.loads(row["data"]) if row else None

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
        counts = dict.fromkeys(TaskStatus, 0)
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

    #: SPEC C11 wording; identical semantics to :meth:`rebuild_projections`.
    def rebuild_from_events(self, project_id: str | None = None) -> int:
        return self.rebuild_projections(project_id)

    def verify_replay(self, project_id: str) -> bool:
        """True when replaying the log reproduces the current projection byte-for-byte."""
        before = self.projection_digest(project_id)
        self.rebuild_projections(project_id)
        return before == self.projection_digest(project_id)

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
        seqs = self.event_seqs(project_id)
        if seqs and seqs != list(range(seqs[0], seqs[0] + len(seqs))):
            problems.append(
                f"event seq gap: {len(seqs)} events from {seqs[0]} to {seqs[-1]} are not contiguous"
            )
        budget_doc = self.get_document(project_id, "budget")
        if budget_doc is None:
            problems.append("no budget snapshot recorded")
        return problems

    # ------------------------------------------------------------------ export
    def export_state(self, project_id: str) -> dict[str, Any]:
        """JSON-serialisable snapshot — used by the web UI and the report."""
        project = self.get_project(project_id)
        requirement = self.get_requirement(project_id)
        return {
            "project": project.model_dump(mode="json") if project else None,
            "requirement": requirement.model_dump(mode="json") if requirement else None,
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
