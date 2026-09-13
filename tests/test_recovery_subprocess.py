"""C5 — real-process crash recovery: SIGKILL mid-run, then ``resume``.

This is the only test that uses a real subprocess, a real ``kill -9`` and real
time; it is marked ``slow`` but runs (not skipped) in the default suite, because
"crash recovery works" is the project's headline claim (FIND-014).
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"

PRD = (
    "Deliver:\n"
    "- add a greeting helper with unit tests\n"
    "- document it in the README\n"
)


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _run_cli(args: list[str], *, timeout: float = 60.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "agentcorp", *args],
        capture_output=True,
        text=True,
        env=_env(),
        timeout=timeout,
    )


def _db_state(db: Path) -> tuple[dict[str, str], dict[str, int]]:
    """(task_id -> status, status -> count) read straight from SQLite."""
    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute("SELECT id, status FROM tasks ORDER BY id").fetchall()
    finally:
        conn.close()
    statuses = {str(task_id): str(status) for task_id, status in rows}
    counts: dict[str, int] = {}
    for status in statuses.values():
        counts[status] = counts.get(status, 0) + 1
    return statuses, counts


def _event_rows(db: Path, *, project_id: str | None = None) -> list[tuple[int, str, str | None]]:
    conn = sqlite3.connect(str(db))
    try:
        if project_id is None:
            rows = conn.execute("SELECT run_seq, type, task_id FROM events ORDER BY seq").fetchall()
        else:
            rows = conn.execute(
                "SELECT run_seq, type, task_id FROM events WHERE project_id=? ORDER BY run_seq",
                (project_id,),
            ).fetchall()
    finally:
        conn.close()
    return [(int(seq), str(type_), task_id) for seq, type_, task_id in rows]


def _run_id_from_output(stdout: str, stderr: str, db: Path) -> str:
    """The CLI prints the run id inside the report line; fall back to the DB."""
    for blob in (stdout, stderr):
        for token in blob.replace(",", " ").split():
            cleaned = token.strip("'\"")
            if cleaned.startswith("RUN-"):
                return cleaned
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute("SELECT id FROM projects ORDER BY created_at LIMIT 1").fetchone()
    finally:
        conn.close()
    assert row is not None, f"no project in {db}\nstdout={stdout}\nstderr={stderr}"
    return str(row[0])


def test_sigkill_then_resume_redrives_the_interrupted_task(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "README.md").write_text("# demo\n", encoding="utf-8")
    db = tmp_path / "crash.db"
    prd = tmp_path / "prd.md"
    prd.write_text(PRD, encoding="utf-8")

    # Concurrency 1 + latency 0.5s makes it easy to catch the process mid-call.
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "agentcorp",
            "run",
            "--repo",
            str(repo),
            "--prd",
            str(prd),
            "--db",
            str(db),
            "--provider",
            "mock:latency=0.5,seed=11",
            "--concurrency",
            "1",
            "--report-out",
            str(tmp_path / "report.json"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_env(),
    )
    assert process.poll() is None
    # Wait until one task is RUNNING and another is DONE, i.e. a real interruption window.
    deadline = time.monotonic() + 40.0
    statuses: dict[str, str] = {}
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            statuses, counts = _db_state(db)
        except sqlite3.Error:
            time.sleep(0.05)
            continue
        if counts.get("done", 0) >= 1 and counts.get("running", 0) >= 1:
            break
        time.sleep(0.02)
    assert process.poll() is None, "the run finished before we could interrupt it"
    assert counts.get("running", 0) >= 1, f"never caught a RUNNING task: {counts}"
    done_before = {task_id for task_id, status in statuses.items() if status == "done"}
    running_before = {task_id for task_id, status in statuses.items() if status == "running"}
    assert done_before and running_before

    os.kill(process.pid, signal.SIGKILL)
    process.wait(timeout=10)
    assert process.returncode == -signal.SIGKILL

    # The DB must still be readable and the interrupted task still RUNNING.
    statuses, counts = _db_state(db)
    assert set(running_before) <= {t for t, s in statuses.items() if s == "running"}

    # Find the run id and resume.
    started = _run_cli(["status", "--db", str(db)])
    assert started.returncode == 0, started.stderr
    run_id = json.loads(started.stdout)["run_id"]

    resumed = _run_cli(
        ["resume", run_id, "--db", str(db), "--provider", "mock:seed=11", "--report-out", str(tmp_path / "report2.json")]
    )
    assert resumed.returncode == 0, f"resume failed:\n{resumed.stdout}\n{resumed.stderr}"
    assert "DONE" in resumed.stdout

    # DONE tasks were never re-run: no TASK_STARTED for them after their completion.
    events = _event_rows(db, project_id=run_id)
    for task_id in done_before:
        task_events = [type_ for _, type_, tid in events if tid == task_id]
        assert task_events.count("TASK_COMPLETED") + task_events.count("TASK_FINISHED") <= 1
        first_finish = next(
            index
            for index, (_, type_, tid) in enumerate(events)
            if tid == task_id and type_ in {"TASK_COMPLETED", "TASK_FINISHED", "REVIEW_APPROVED"}
        )
        later_starts = [
            index
            for index, (_, type_, tid) in enumerate(events)
            if tid == task_id and type_ in {"TASK_STARTED", "TASK_CLAIMED"} and index > first_finish
        ]
        assert not later_starts, f"{task_id} was re-executed after finishing"

    # The interrupted task was re-dispatched (attempts grew) and converged.
    statuses, counts = _db_state(db)
    assert counts.get("running", 0) == 0, f"tasks left RUNNING after resume: {statuses}"
    assert counts.get("review", 0) == 0
    assert counts.get("done", 0) == len(statuses), f"not all tasks converged: {statuses}"
    for task_id in running_before:
        task_events = [type_ for _, type_, tid in events if tid == task_id]
        assert "TASK_RETRIED" in task_events or "TASK_FAILED" in task_events, (
            f"the interrupted task {task_id} was never recovered"
        )

    # seq stays contiguous per run.
    seqs = [seq for seq, _, _ in events]
    assert seqs == list(range(1, len(seqs) + 1)), "event sequence must not have gaps"


def test_resume_of_an_unknown_run_fails_cleanly(tmp_path: Path) -> None:
    db = tmp_path / "empty.db"
    result = _run_cli(["resume", "RUN-doesnotexist", "--db", str(db)])
    assert result.returncode == 1
    assert "unknown run" in (result.stdout + result.stderr)
