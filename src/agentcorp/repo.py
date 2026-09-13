"""Repository analysis: turn a checkout on disk into a bounded context document.

Deliberately offline and pure: no git binary, no network, no provider.  The
result is a :class:`~agentcorp.models.RepositoryContext` that is small enough to
embed in every planner/worker prompt and stable enough to hash for benchmarks.
"""

from __future__ import annotations

import json
import os
import tomllib
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from .errors import PermanentError
from .models import RepositoryContext

__all__ = ["analyze_repository", "LANGUAGE_BY_SUFFIX", "MANIFEST_NAMES"]

#: suffix -> language name (file counts only; no parsing).
LANGUAGE_BY_SUFFIX: dict[str, str] = {
    ".py": "python",
    ".pyi": "python",
    ".js": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".jsx": "javascript",
    ".go": "go",
    ".rs": "rust",
    ".java": "java",
    ".kt": "kotlin",
    ".rb": "ruby",
    ".php": "php",
    ".cs": "csharp",
    ".c": "c",
    ".h": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".hpp": "cpp",
    ".swift": "swift",
    ".scala": "scala",
    ".sh": "shell",
    ".sql": "sql",
    ".md": "markdown",
    ".rst": "markdown",
    ".toml": "config",
    ".yaml": "config",
    ".yml": "config",
    ".json": "config",
}

MANIFEST_NAMES: tuple[str, ...] = (
    "pyproject.toml",
    "setup.py",
    "setup.cfg",
    "requirements.txt",
    "requirements-dev.txt",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
    "build.gradle",
    "Gemfile",
    "Makefile",
    "Dockerfile",
    "docker-compose.yml",
    "tox.ini",
    "noxfile.py",
)

_ENTRY_POINT_NAMES: tuple[str, ...] = (
    "main.py",
    "__main__.py",
    "cli.py",
    "app.py",
    "server.py",
    "manage.py",
    "index.js",
    "index.ts",
    "main.go",
    "main.rs",
    "Main.java",
)

_SKIP_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        ".venv",
        "venv",
        "env",
        "node_modules",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".idea",
        ".vscode",
        "dist",
        "build",
        "target",
        ".next",
        "coverage",
        "htmlcov",
        ".eggs",
    }
)

_TEXT_SUFFIXES: frozenset[str] = frozenset(
    {".py", ".pyi", ".toml", ".md", ".rst", ".txt", ".cfg", ".ini", ".json", ".yaml", ".yml"}
)


def analyze_repository(
    root: str | Path,
    *,
    keywords: Sequence[str] = (),
    max_files: int = 20_000,
    max_file_bytes: int = 2_000_000,
    tree_max_depth: int = 3,
    tree_max_entries: int = 80,
    tree_max_chars: int = 2500,
    relevant_limit: int = 20,
) -> RepositoryContext:
    """Scan ``root`` and return a bounded, deterministic context document."""
    base = Path(root).expanduser()
    if not base.exists():
        msg = f"repository path does not exist: {base}"
        raise PermanentError(msg)
    if not base.is_dir():
        msg = f"repository path is not a directory: {base}"
        raise PermanentError(msg)
    base = base.resolve()

    languages: Counter[str] = Counter()
    manifests: list[str] = []
    entry_points: list[str] = []
    test_paths: list[str] = []
    docs_paths: list[str] = []
    all_files: list[str] = []
    file_count = 0
    total_bytes = 0
    truncated = False

    for current_root, dirs, files in os.walk(base):
        dirs[:] = sorted(d for d in dirs if d not in _SKIP_DIRS and not d.startswith("."))
        files.sort()
        for name in files:
            if file_count >= max_files:
                truncated = True
                break
            path = Path(current_root) / name
            if path.is_symlink():
                continue
            try:
                size = path.stat().st_size
            except OSError:  # pragma: no cover - racing deletion
                continue
            file_count += 1
            total_bytes += size
            rel = path.relative_to(base).as_posix()
            all_files.append(rel)
            languages[LANGUAGE_BY_SUFFIX.get(path.suffix.lower(), "other")] += 1
            if name in MANIFEST_NAMES:
                manifests.append(rel)
            if name in _ENTRY_POINT_NAMES:
                entry_points.append(rel)
            if _is_test_path(rel):
                test_paths.append(rel)
            if path.suffix.lower() in {".md", ".rst"} or rel.startswith(("docs/", "doc/")):
                docs_paths.append(rel)

    dependencies = _collect_dependencies(base, manifests, max_file_bytes=max_file_bytes)
    conventions = _detect_conventions(base, manifests, dependencies, all_files)
    tree = _render_tree(all_files, max_depth=tree_max_depth, max_entries=tree_max_entries, max_chars=tree_max_chars)
    relevant = _rank_relevant(all_files, keywords, entry_points, test_paths, limit=relevant_limit)

    notes: list[str] = []
    if truncated:
        notes.append(f"file scan truncated at {max_files} files")

    return RepositoryContext(
        root=str(base),
        languages=dict(languages.most_common()),
        manifests=sorted(manifests),
        dependencies=dependencies[:60],
        entry_points=sorted(entry_points)[:20],
        test_paths=sorted(test_paths)[:50],
        docs_paths=sorted(docs_paths)[:30],
        relevant_files=relevant,
        conventions=conventions,
        tree=tree,
        file_count=file_count,
        total_bytes=total_bytes,
        is_git_repo=(base / ".git").exists(),
        notes=notes,
    )


def _is_test_path(rel: str) -> bool:
    parts = rel.split("/")
    name = parts[-1]
    if any(part in {"tests", "test", "spec", "__tests__"} for part in parts[:-1]):
        return True
    return (
        name.startswith("test_")
        or name.endswith(("_test.py", "_test.go", ".test.ts", ".test.js", ".spec.ts"))
        or name in {"conftest.py"}
    )


def _collect_dependencies(base: Path, manifests: Sequence[str], *, max_file_bytes: int) -> list[str]:
    out: list[str] = []
    for rel in manifests:
        path = base / rel
        try:
            if path.stat().st_size > max_file_bytes:
                continue
        except OSError:  # pragma: no cover
            continue
        if rel == "pyproject.toml":
            out.extend(_deps_from_pyproject(path))
        elif rel.startswith("requirements") and rel.endswith(".txt"):
            out.extend(_deps_from_requirements(path))
        elif rel == "package.json":
            out.extend(_deps_from_package_json(path))
    # de-duplicate, keep order
    seen: set[str] = set()
    ordered: list[str] = []
    for dep in out:
        if dep and dep not in seen:
            seen.add(dep)
            ordered.append(dep)
    return ordered


def _deps_from_pyproject(path: Path) -> list[str]:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError, UnicodeDecodeError):  # pragma: no cover - malformed manifest
        return []
    project = data.get("project", {})
    deps = list(project.get("dependencies", []) or [])
    for group in (project.get("optional-dependencies", {}) or {}).values():
        deps.extend(group or [])
    poetry = data.get("tool", {}).get("poetry", {}).get("dependencies", {}) or {}
    deps.extend(str(name) for name in poetry)
    return [str(d) for d in deps]


def _deps_from_requirements(path: Path) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):  # pragma: no cover
        return []
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith(("#", "-"))]


def _deps_from_package_json(path: Path) -> list[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):  # pragma: no cover
        return []
    out: list[str] = []
    for key in ("dependencies", "devDependencies"):
        section = data.get(key) or {}
        if isinstance(section, dict):
            out.extend(f"{name}@{version}" for name, version in section.items())
    return out


def _detect_conventions(
    base: Path,
    manifests: Sequence[str],
    dependencies: Sequence[str],
    all_files: Sequence[str],
) -> list[str]:
    conventions: list[str] = []
    dep_blob = " ".join(dependencies).lower()
    files = set(all_files)
    if (base / "src").is_dir():
        conventions.append("src/ layout")
    if (base / "tests").is_dir() or any(f.startswith("test") for f in files):
        conventions.append("tests live under tests/")
    if "pytest" in dep_blob or (base / "tests").is_dir() or "conftest.py" in files:
        conventions.append("pytest")
    if "ruff" in dep_blob or any(f == "ruff.toml" or f == ".ruff.toml" for f in files):
        conventions.append("ruff lint")
    if "mypy" in dep_blob or (base / "mypy.ini").exists():
        conventions.append("mypy typing")
    if "pyproject.toml" in manifests or "package.json" in manifests:
        conventions.append("declared manifest")
    if "Makefile" in manifests:
        conventions.append("make targets")
    if any(f.endswith((".md", ".rst")) for f in all_files):
        conventions.append("markdown docs")
    return conventions


def _render_tree(
    all_files: Sequence[str],
    *,
    max_depth: int,
    max_entries: int,
    max_chars: int,
) -> str:
    """Bounded directory-first rendering; never explodes the context window."""
    tree: dict[str, object] = {}
    for rel in all_files:
        parts = rel.split("/")
        node: dict[str, object] = tree
        for part in parts[:-1][:max_depth]:
            child = node.setdefault(part, {})
            if not isinstance(child, dict):  # pragma: no cover - defensive
                break
            node = child
        else:
            if len(parts) <= max_depth:
                node.setdefault(parts[-1], None)

    lines: list[str] = []

    def emit(node: dict[str, object], prefix: str, depth: int) -> None:
        if len(lines) >= max_entries:
            return
        entries = sorted(node.items(), key=lambda kv: (kv[1] is None, str(kv[0])))
        for name, child in entries:
            if len(lines) >= max_entries:
                return
            if child is None:
                lines.append(f"{prefix}{name}")
            elif isinstance(child, dict):
                lines.append(f"{prefix}{name}/")
                if depth + 1 < max_depth:
                    emit(child, prefix + "  ", depth + 1)

    emit(tree, "", 0)
    if len(lines) >= max_entries:
        lines.append("... [tree truncated]")
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[: max_chars - 24] + "\n... [tree truncated]"
    return text


def _rank_relevant(
    all_files: Sequence[str],
    keywords: Sequence[str],
    entry_points: Sequence[str],
    test_paths: Sequence[str],
    *,
    limit: int,
) -> list[str]:
    entries = set(entry_points)
    tests = set(test_paths)
    terms = [k.lower() for k in keywords if len(k) > 2]

    def score(path: str) -> tuple[int, int, str]:
        lowered = path.lower()
        hit = sum(1 for term in terms if term in lowered)
        bonus = 0
        if path in entries:
            bonus += 2
        if path in tests:
            bonus += 1
        return (-hit, -bonus, path)

    ranked = sorted(all_files, key=score)
    if terms:
        matched = [p for p in ranked if any(t in p.lower() for t in terms)]
        selected = matched or ranked
    else:
        selected = ranked
    return sorted(selected[:limit])
