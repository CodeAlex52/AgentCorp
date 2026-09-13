#!/usr/bin/env python
"""Validate benchmark reports against the SPEC §6 schema (stdlib only).

Usage::

    uv run python scripts/validate_benchmark.py benchmarks/self_hosting_sim.json
    uv run python scripts/validate_benchmark.py benchmarks/*.json

Exit code 0 = every file valid; 1 = at least one problem (printed to stderr).
Also writes ``benchmarks/report_schema.json`` when ``--write-schema`` is given,
for consumers that do have a JSON-Schema validator (jsonschema is deliberately
not a dependency of this project).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from agentcorp.report import REPORT_SCHEMA, validate_report  # noqa: E402

REQUIRED_TOP_LEVEL = set(REPORT_SCHEMA["required"])


def validate_file(path: Path) -> list[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{path}: not valid JSON: {exc}"]
    if not isinstance(payload, dict):
        return [f"{path}: report must be a JSON object"]
    problems = validate_report(payload)
    missing = REQUIRED_TOP_LEVEL - set(payload)
    problems.extend(f"missing required key {key!r}" for key in sorted(missing))
    return [f"{path}: {problem}" for problem in problems]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="*", type=Path)
    parser.add_argument(
        "--write-schema",
        action="store_true",
        help="write docs/report_schema.json and exit",
    )
    args = parser.parse_args(argv)

    if args.write_schema:
        target = REPO_ROOT / "docs" / "report_schema.json"
        target.write_text(json.dumps(REPORT_SCHEMA, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"schema written to {target}")
        return 0

    if not args.reports:
        parser.error("at least one report path is required (or pass --write-schema)")
    problems: list[str] = []
    for report in args.reports:
        if not report.exists():
            problems.append(f"{report}: file does not exist")
            continue
        problems.extend(validate_file(report))
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        print(f"FAILED: {len(problems)} problem(s)", file=sys.stderr)
        return 1
    for report in args.reports:
        payload = json.loads(report.read_text(encoding="utf-8"))
        print(
            f"OK {report}: status={payload['status']} tasks={payload['tasks']['done']}/"
            f"{payload['tasks']['total']} events={payload['events_count']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
