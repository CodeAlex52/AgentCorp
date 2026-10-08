"""STATUS.md gap #2 — a second concurrent ``resume`` must not double-run a run.

Two processes force-requeueing the same RUNNING tasks would execute them
twice.  ``Engine.resume`` now holds a per-run OS advisory lock for the whole
resume, so the second process fails fast instead of racing; the kernel
releases the lock when the holder dies, so crash recovery keeps working.
"""

from __future__ import annotations

import os

import pytest
from conftest import create_project, make_task, seed_tasks
from conftest import run as run_coro

from agentcorp import PermanentError
from agentcorp.engine import resume_lock_path

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX advisory locks")


def _seed(engine) -> None:
    create_project(engine.store, "RUN-LOCK")
    seed_tasks(engine.store, "RUN-LOCK", [make_task("T1")])


def test_resume_is_refused_while_another_process_holds_the_lock(engine_factory) -> None:
    import fcntl

    engine = engine_factory(db_name="resume-lock.db")
    _seed(engine)
    lock_path = resume_lock_path(engine.store.path, "RUN-LOCK")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(PermanentError, match="already being resumed"):
            run_coro(engine.resume("RUN-LOCK"))
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def test_resume_proceeds_once_the_lock_is_free(engine_factory) -> None:
    engine = engine_factory(db_name="resume-free.db")
    _seed(engine)
    summary = run_coro(engine.resume("RUN-LOCK"))
    assert summary.run_id == "RUN-LOCK"
