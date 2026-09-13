"""Every prompt the system sends, in one module.

Two reasons this is not scattered across the callers:

1. **Auditability.**  When an agent does something surprising, the question is
   always "what exactly did we ask it?" — one file answers that.
2. **Contract stability.**  Each prompt carries a ``CONTRACT: <name>`` marker
   and an explicit JSON schema.  The mock provider routes on the marker, and
   the parsers validate against the schema, so a prompt change that breaks the
   contract fails loudly in tests instead of silently degrading output quality.
"""

from __future__ import annotations

from enum import StrEnum

from .models import Requirement, Review, Task, WorkerOutcome

__all__ = ["Contract", "marker", "worker_messages", "reviewer_messages", "planner_messages", "prd_messages", "decomposer_messages"]


class Contract(StrEnum):
    PRD = "prd"
    PLANNER = "planner"
    WORKER = "worker"
    REVIEWER = "reviewer"
    DECOMPOSER = "decomposer"


def marker(contract: Contract) -> str:
    return f"CONTRACT: {contract.value}"


_PREAMBLE = """You are an agent inside AgentCorp, a recursive delivery system.
You are one node of a task DAG. Other agents handle the other nodes; you must not
attempt work outside your task.

Rules:
- Obey the output contract exactly. Respond with a single JSON object and nothing else.
- Never invent file paths that were not given to you and do not exist.
- If you cannot complete the task, say so honestly via the "blocked" status and
  explain precisely what you learned. A blocked task with useful information is a
  good outcome; a fabricated success is the worst possible outcome.
- Be concise. Do not restate the task description back to the caller.
"""


def _system(contract: Contract, body: str) -> str:
    return f"{_PREAMBLE}\n{marker(contract)}\n\n{body.strip()}\n"


WORKER_SCHEMA = """{
  "status": "success" | "blocked" | "failed",
  "summary": "one or two sentences on what you did",
  "files": [{"path": "relative/path.py", "content": "complete new file content", "mode": "write"}],
  "diff": "optional unified diff instead of files",
  "tests_run": ["exact command you ran"],
  "tests_passed": true,
  "evidence": ["concrete facts that support the summary"],
  "reason": "required when status is blocked or failed",
  "new_information": ["what you learned that changes the plan"],
  "recommended_subtasks": ["a smaller, independently verifiable unit of work"],
  "followups": ["work that is genuinely out of scope for this task"]
}"""


def worker_messages(
    task: Task,
    *,
    repo_context: str = "",
    dependency_outputs: str = "",
    repair_hint: str = "",
    allow_writes: bool = True,
) -> list[dict[str, str]]:
    """Build the system+user pair for a worker call."""
    system = _system(
        Contract.WORKER,
        f"""You execute exactly one task and report the result.

Output contract:
{WORKER_SCHEMA}""",
    )
    parts = []
    if repo_context:
        parts.append("# Repository context\n" + repo_context)
    parts.append("# Your task\n" + task.to_prompt_block())
    if dependency_outputs:
        parts.append("# Results of tasks you depended on\n" + dependency_outputs)
    parts.append(
        "# Instructions\n"
        + (
            "Produce the changes your task requires. Put the complete content of each file you "
            "create or modify in the `files` array, and run the tests named in your acceptance "
            "criteria (report the exact commands in `tests_run`)."
            if allow_writes
            else "This task is analysis-only in the current configuration: return your findings "
            "in `summary`/`evidence` and leave `files` empty."
        )
    )
    if repair_hint:
        parts.append(
            "# Previous attempt rejected — fix these specific problems\n"
            f"{repair_hint}\nDo not repeat the same mistake."
        )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": "\n\n".join(parts)},
    ]


REVIEWER_SCHEMA = """{
  "verdict": "PASS" | "REJECT",
  "score": 0.0,
  "rationale": "why you reached this verdict, referencing specific evidence",
  "issues": [{"severity": "low|medium|high|critical", "message": "...", "file": "path", "line": 12}]
}"""


def reviewer_messages(
    task: Task,
    outcome: WorkerOutcome,
    *,
    diff: str = "",
    deterministic_checks: dict[str, bool] | None = None,
) -> list[dict[str, str]]:
    """Build the system+user pair for a reviewer call."""
    system = _system(
        Contract.REVIEWER,
        f"""You review one task's output. You do not implement anything.

Judge only these questions:
1. Does the output satisfy every acceptance criterion, with evidence?
2. Does it introduce a regression or break an existing contract?
3. Are edge cases and error paths handled?
4. Is the change minimal, or did it sprawl beyond the task?

Reject when a claim is unsupported by the diff. Approve only what you can verify.

Output contract:
{REVIEWER_SCHEMA}""",
    )
    checks = deterministic_checks or {}
    checks_text = "\n".join(f"  - {name}: {'pass' if ok else 'FAIL'}" for name, ok in checks.items()) or "  (none)"
    files = "\n".join(f"  - {f.path} ({len(f.content)} bytes)" for f in outcome.files) or "  (none)"
    parts = [
        "# Task under review\n" + task.to_prompt_block(),
        "# Worker's self-report\n"
        f"status: {outcome.status}\nsummary: {outcome.summary}\n"
        f"tests_run: {', '.join(outcome.tests_run) or '(none)'}\n"
        f"tests_passed: {outcome.tests_passed}\nevidence:\n  "
        + "\n  ".join(outcome.evidence or ["(none)"]),
        "# Files touched\n" + files,
        "# Deterministic checks already run\n" + checks_text,
        "# DIFF:\n" + (diff.strip() or "(empty)"),
    ]
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": "\n\n".join(parts)},
    ]


PLANNER_SCHEMA = """{
  "tasks": [
    {
      "key": "A1",
      "title": "short imperative title",
      "objective": "what must be true when this is done",
      "kind": "research|implementation|test|refactor|docs|integration|verification",
      "depends_on": ["A0"],
      "expected_output": "the artefact produced",
      "acceptance_criteria": [
        {"statement": "checkable claim", "verification": "test|command|static|review", "command": "pytest -q"}
      ],
      "touch_paths": ["src/module.py"],
      "risk": "low|medium|high|critical"
    }
  ]
}"""


def planner_messages(
    requirement: Requirement,
    repo_context: str,
    *,
    max_tasks: int = 12,
) -> list[dict[str, str]]:
    system = _system(
        Contract.PLANNER,
        f"""You decompose a requirement into a dependency-ordered task DAG.

Guidelines:
- One concern per task. If a task needs an "and" to describe it, split it.
- Research before implementation; tests after implementation; verification last.
- Every task needs acceptance criteria a third party could check.
- `depends_on` refers to other tasks' `key` values in this same response. No cycles.
- Prefer one task that a single agent can finish in one sitting.
- Produce at most {max_tasks} tasks. If the work is larger, produce the first
  coherent slice and let the runtime decompose further.

Output contract:
{PLANNER_SCHEMA}""",
    )
    criteria = "\n".join(f"  - [{ac.verification}] {ac.statement}" for ac in requirement.acceptance_criteria) or "  (none)"
    user = "\n\n".join(
        [
            "# Requirement\n" + requirement.raw_text.strip(),
            "# Structured summary\n"
            f"goal: {requirement.goal}\n"
            f"deliverables:\n  "
            + "\n  ".join(requirement.deliverables or ["(unspecified)"])
            + "\nconstraints:\n  "
            + "\n  ".join(requirement.constraints or ["(none stated)"])
            + f"\nacceptance criteria:\n{criteria}",
            "# Repository\n" + (repo_context or "(not analysed)"),
        ]
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


PRD_SCHEMA = """{
  "goal": "one sentence",
  "deliverables": ["concrete artefact or behaviour"],
  "constraints": ["explicit limitation from the request"],
  "acceptance_criteria": [{"statement": "...", "verification": "test|command|static|review"}],
  "keywords": ["salient domain terms for scope checking"]
}"""


def prd_messages(raw_text: str) -> list[dict[str, str]]:
    system = _system(
        Contract.PRD,
        f"""You convert a natural-language request into a structured requirement.

Extract only what is stated or unambiguously implied. Do not add features the
requester did not ask for. If acceptance criteria are missing, propose the
minimum set that would prove the request was fulfilled.

Output contract:
{PRD_SCHEMA}""",
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": "# Request\n" + raw_text.strip()},
    ]


DECOMPOSER_SCHEMA = """{
  "should_split": true,
  "reason": "why this task is not a single unit of work",
  "subtasks": [
    {
      "title": "short imperative title",
      "objective": "what must be true when this is done",
      "kind": "research|implementation|test|refactor|docs|integration|verification",
      "depends_on_siblings": [],
      "acceptance_criteria": [{"statement": "...", "verification": "review"}],
      "touch_paths": []
    }
  ]
}"""


def decomposer_messages(
    task: Task,
    *,
    signal: str,
    repo_context: str = "",
    max_subtasks: int = 5,
) -> list[dict[str, str]]:
    system = _system(
        Contract.DECOMPOSER,
        f"""You split an oversized task into smaller independent tasks.

A task must be split when it: spans several concerns, has acceptance criteria
that cannot be checked independently, depends on an unknown that must be
resolved first, or needs more than one sitting.

Split into at most {max_subtasks} subtasks with a clear internal order
(`depends_on_siblings` indexes sibling positions, 0-based). Subtasks must
together cover the parent objective with nothing lost and nothing added.

Output contract:
{DECOMPOSER_SCHEMA}""",
    )
    user = "\n\n".join(
        [
            "# Task to split\n" + task.to_prompt_block(),
            f"# Why this is being split\n{signal}",
            "# Repository\n" + (repo_context or "(not analysed)"),
        ]
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def review_to_repair_hint(review: Review) -> str:
    """Turn a rejection into the instruction appended to the worker's retry."""
    lines = [f"- [{issue.severity.value}] {issue.message}" for issue in review.issues]
    if review.rationale:
        lines.append(f"- reviewer rationale: {review.rationale}")
    return "\n".join(lines) if lines else "- reviewer rejected the output without a specific reason"
