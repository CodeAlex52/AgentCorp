"""Worker execution: one task → provider call → validated outcome → artifacts.

The worker is the only place in the system that turns model output into file
mutations, so it is also the only place that needs a path policy.  Every
proposed write is checked against the repository root (no absolute paths, no
``..`` traversal, no symlink escapes, no ``.git`` internals) **before** anything
touches the disk.  A violation is a :class:`PathViolationError` — a permanent
error: retrying a task that tried to escape the sandbox is not a recovery
strategy.
"""

from __future__ import annotations

import json
import os
import stat
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from pydantic import ValidationError

from .errors import PermanentError, SchemaError
from .models import Artifact, FileWrite, Task, WorkerOutcome
from .parsing import extract_json_object, normalise_keys
from .prompts import worker_messages
from .runtime import AgentRuntime

__all__ = ["Worker", "WorkerResult", "PathViolationError", "validate_write_path"]

_MAX_SCHEMA_RETRIES = 2
_MAX_FILE_BYTES = 2_000_000  # 2 MB per file: a model reply cannot be larger anyway

#: Directories that must never be written through, compared case-folded and
#: unicode-normalised: on a case-insensitive filesystem (macOS/APFS, Windows)
#: ``.GIT/config`` *is* ``.git/config`` (FIND-011).
_VCS_DIR_NAMES: frozenset[str] = frozenset({".git", ".hg", ".svn", ".bzr"})
_VCS_DIR_KEYS: frozenset[str] = frozenset(
    unicodedata.normalize("NFC", name).casefold() for name in _VCS_DIR_NAMES
)


def _is_vcs_component(part: str) -> bool:
    return unicodedata.normalize("NFC", part).casefold() in _VCS_DIR_KEYS


#: Keys models spell differently -> canonical WorkerOutcome field.
_ALIASES: dict[str, str] = {
    "result": "status",
    "outcome": "status",
    "file_writes": "files",
    "writes": "files",
    "details": "summary",
    "message": "summary",
    "tests": "tests_run",
    "passed": "tests_passed",
    "test_passed": "tests_passed",
    "evidence_notes": "evidence",
    "subtasks": "recommended_subtasks",
    "recommended_tasks": "recommended_subtasks",
}


class PathViolationError(PermanentError):
    """A worker proposed a file write outside the sandbox."""


@dataclass
class WorkerResult:
    task_id: str
    status: str  # success | blocked | failed
    outcome: WorkerOutcome
    artifacts: list[Artifact] = field(default_factory=list)
    written_paths: list[str] = field(default_factory=list)
    schema_retries: int = 0
    error: str | None = None

    @property
    def summary(self) -> str:
        return self.outcome.summary or self.outcome.reason or ""


class Worker:
    """Executes one task against the shared :class:`AgentRuntime`."""

    role = "worker"
    identity = "worker"

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        write_root: str | Path,
        project_id: str = "unknown",
        apply_writes: bool = True,
        strict_touch_paths: bool = False,
        max_schema_retries: int = _MAX_SCHEMA_RETRIES,
        allow_writes: bool = True,
    ) -> None:
        self.runtime = runtime
        self.write_root = Path(write_root).expanduser().resolve()
        self.project_id = project_id
        self.apply_writes = apply_writes
        self.strict_touch_paths = strict_touch_paths
        self.max_schema_retries = max(max_schema_retries, 0)
        self.allow_writes = allow_writes

    async def execute(
        self,
        task: Task,
        *,
        repo_context: str = "",
        dependency_outputs: str = "",
        repair_hint: str = "",
    ) -> WorkerResult:
        """Run the worker loop, re-sampling on schema violations."""
        schema_retries = 0
        hint = repair_hint
        last_error: SchemaError | None = None
        for attempt in range(1, self.max_schema_retries + 2):
            messages = worker_messages(
                task,
                repo_context=repo_context,
                dependency_outputs=dependency_outputs,
                repair_hint=hint,
                allow_writes=self.allow_writes,
            )
            result = await self.runtime.call(
                messages,
                role=self.role,
                task_id=task.id,
                metadata={
                    "project_id": self.project_id,
                    "stage": "execute",
                    "attempt": attempt,
                },
            )
            try:
                outcome = outcome_from_text(result.text)
            except SchemaError as exc:
                last_error = exc
                if attempt > self.max_schema_retries:
                    raise
                schema_retries += 1
                hint = (
                    "Your previous reply was not valid according to the worker contract "
                    f"({exc}). Reply with a single JSON object with keys status/summary/"
                    "files/evidence; no prose before or after the JSON."
                )
                continue
            artifacts, written = self._materialise(task, outcome)
            return WorkerResult(
                task_id=task.id,
                status=outcome.status,
                outcome=outcome,
                artifacts=artifacts,
                written_paths=written,
                schema_retries=schema_retries,
            )
        assert last_error is not None  # loop returns or raises
        raise last_error

    # ---------------------------------------------------------------- helpers
    def _materialise(self, task: Task, outcome: WorkerOutcome) -> tuple[list[Artifact], list[str]]:
        """Validate + apply file writes; produce artifact records."""
        if outcome.status != "success":
            return [], []
        artifacts: list[Artifact] = []
        written: list[str] = []
        seen: set[str] = set()
        for write in outcome.files:
            if write.path in seen:
                msg = f"task {task.id} proposed the same path twice: {write.path!r}"
                raise PathViolationError(msg)
            seen.add(write.path)
            target = validate_write_path(
                write.path,
                self.write_root,
                touch_paths=task.touch_paths,
                strict_touch=self.strict_touch_paths,
            )
            rel = target.relative_to(self.write_root).as_posix()
            if len(write.content) > _MAX_FILE_BYTES:
                msg = f"file {rel!r} exceeds the {_MAX_FILE_BYTES} byte write limit"
                raise PathViolationError(msg)
            if self.apply_writes:
                target.parent.mkdir(parents=True, exist_ok=True)
                _write_file(target, write.content, mode=write.mode)
            written.append(rel)
            artifacts.append(
                Artifact(
                    project_id=self.project_id,
                    task_id=task.id,
                    path=rel,
                    kind="file",
                    content=write.content,
                )
            )
        return artifacts, written


def outcome_from_text(text: str) -> WorkerOutcome:
    """Parse + validate a worker reply (raises SchemaError on any violation)."""
    try:
        payload = extract_json_object(text)
    except SchemaError:
        raise
    payload = normalise_keys(payload, _ALIASES)
    if "status" not in payload:
        msg = "worker reply has no 'status' field"
        raise SchemaError(msg)
    status = str(payload["status"]).strip().lower()
    if status not in {"success", "blocked", "failed"}:
        msg = f"worker reply has unknown status {status!r}"
        raise SchemaError(msg)
    files_raw = payload.get("files") or []
    if isinstance(files_raw, dict):
        files_raw = [files_raw]
    if not isinstance(files_raw, list):
        msg = "worker 'files' must be a list"
        raise SchemaError(msg)
    files: list[FileWrite] = []
    for item in files_raw:
        if not isinstance(item, dict):
            msg = f"file entry must be an object, got {type(item).__name__}"
            raise SchemaError(msg)
        path = str(item.get("path") or "").strip()
        content = item.get("content")
        if not path:
            msg = "file entry has no path"
            raise SchemaError(msg)
        if content is None and item.get("content_b64") is None:
            msg = f"file entry {path!r} has no content"
            raise SchemaError(msg)
        mode = str(item.get("mode") or "write").lower()
        if mode not in {"write", "append"}:
            raise SchemaError(f"file entry {path!r} has unknown mode {mode!r}")
        files.append(
            FileWrite(path=path, content=str(content or ""), mode=mode)  # type: ignore[arg-type]
        )
    payload_files_clean = {k: v for k, v in payload.items() if k != "files"}
    try:
        return WorkerOutcome.model_validate({**payload_files_clean, "files": files, "raw": text})
    except ValidationError as exc:
        msg = f"worker reply failed schema validation: {exc.errors()[:3]}"
        raise SchemaError(msg) from exc


def validate_write_path(
    path: str,
    root: str | Path,
    *,
    touch_paths: tuple[str, ...] | list[str] = (),
    strict_touch: bool = False,
) -> Path:
    """Return the absolute destination or raise :class:`PathViolationError`.

    Rejections: empty/NUL/absolute paths, ``..`` traversal, symlink escapes,
    writes into ``.git``, and (when ``strict_touch``) paths outside the task's
    declared ``touch_paths``.
    """
    if not path or not path.strip():
        raise PathViolationError("empty path")
    if "\x00" in path:
        raise PathViolationError(f"path contains a NUL byte: {path!r}")
    raw = path.strip().replace("\\", "/")
    if raw.startswith("~"):
        raise PathViolationError(f"home-relative path is not allowed: {path!r}")
    pure = PurePosixPath(raw)
    if pure.is_absolute() or (len(raw) > 1 and raw[1] == ":"):
        raise PathViolationError(f"absolute paths are not allowed: {path!r}")
    if any(part == ".." for part in pure.parts):
        raise PathViolationError(f"path traversal is not allowed: {path!r}")
    if any(_is_vcs_component(part) for part in pure.parts):
        raise PathViolationError(f"writes into version-control metadata are not allowed: {path!r}")
    if not pure.parts:
        raise PathViolationError(f"path resolves to nothing: {path!r}")

    base = Path(root).expanduser().resolve()
    candidate = base / pure
    try:
        resolved = candidate.resolve()
    except OSError as exc:  # pragma: no cover - exotic filesystem states
        raise PathViolationError(f"cannot resolve {path!r}: {exc}") from exc
    if resolved != base and base not in resolved.parents:
        raise PathViolationError(
            f"path escapes the repository root: {path!r} -> {resolved}"
        )
    # Re-check the *real* path: a symlinked or case-folded component may only become
    # visible after resolution.
    try:
        relative_parts = resolved.relative_to(base).parts
    except ValueError:  # pragma: no cover - covered by the root check above
        relative_parts = resolved.parts
    if any(_is_vcs_component(part) for part in relative_parts):
        raise PathViolationError(
            f"writes into version-control metadata are not allowed: {path!r}"
        )
    if resolved.exists() and resolved.is_dir():
        raise PathViolationError(f"path is a directory: {path!r}")
    if resolved.exists():
        try:
            info = os.lstat(resolved)
        except OSError as exc:  # pragma: no cover - raced deletion
            raise PathViolationError(f"cannot stat {path!r}: {exc}") from exc
        if not stat.S_ISREG(info.st_mode):
            raise PathViolationError(f"refusing to write through {path!r}: not a regular file")

    if strict_touch and touch_paths:
        allowed = [_normalise_touch(t) for t in touch_paths]
        if not any(_is_within(raw, entry) for entry in allowed):
            raise PathViolationError(
                f"path {path!r} is outside the task's declared touch_paths {sorted(allowed)}"
            )
    return resolved


def _write_file(target: Path, content: str, *, mode: str) -> None:
    """Write ``content`` at ``target`` without ever writing *through* an alias.

    A hard link inside the repository can share its inode with a file outside
    it; writing in place would then modify that outside file (FIND-013).  The
    link is therefore broken first (unlink + create), which keeps the task's
    change inside the sandbox and leaves every other name of the old inode
    untouched.  Append semantics are preserved on a private copy of the file.
    """
    existing: str | None = None
    if target.exists():
        info = os.lstat(target)
        if info.st_nlink > 1:
            if mode == "append":
                existing = target.read_text(encoding="utf-8", errors="replace")
            target.unlink()
    if mode == "append" and target.exists():
        with target.open("a", encoding="utf-8") as handle:
            handle.write(content)
        return
    target.write_text((existing or "") + content, encoding="utf-8")


def _normalise_touch(path: str) -> str:
    raw = path.strip().replace("\\", "/").strip("/")
    try:
        pure = PurePosixPath(raw)
    except ValueError:  # pragma: no cover - malformed
        return raw
    if any(part == ".." for part in pure.parts):
        return "__invalid__"
    return pure.as_posix()


def _is_within(path: str, entry: str) -> bool:
    if entry in {"", ".", "__invalid__"}:
        return False
    return path == entry or path.startswith(entry.rstrip("/") + "/")


def outcome_to_json(outcome: WorkerOutcome, *, indent: int | None = None) -> str:
    """Serialise an outcome for events/logs (stable key order)."""
    return json.dumps(outcome.model_dump(mode="json"), sort_keys=True, indent=indent)
