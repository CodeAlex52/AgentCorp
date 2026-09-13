"""Independent review loop (SPEC C12).

Review is a *separate role*: the reviewer is a different object, a different
runtime instance and a different ``CONTRACT:`` marker.  The scheduler refuses to
review a task with the same identity that produced it (``enforce_independence``).

Verdicts combine two independent signals:

* deterministic checks — cheap, offline, about verifiable facts (did the worker
  claim success, is there a diff for a code task, did reported tests pass);
* the reviewer model — asked to judge acceptance criteria against the artefact.

A deterministic failure always rejects, even if the model approves.  A model
outage downgrades to the deterministic verdict, which is recorded in the review
(``reviewer = "deterministic-degraded"``) so a degraded run is visible in the
report rather than disguised as a full review.
"""

from __future__ import annotations

from typing import Any

from .errors import PermanentError, SchemaError
from .models import Review, ReviewIssue, Severity, Task, TaskKind, WorkerOutcome
from .parsing import extract_json_object
from .prompts import reviewer_messages
from .runtime import AgentRuntime

__all__ = [
    "Reviewer",
    "deterministic_checks",
    "enforce_independence",
    "validate_review",
    "ReviewOutcomeError",
]

_CODE_KINDS: frozenset[TaskKind] = frozenset(
    {TaskKind.IMPLEMENTATION, TaskKind.FIX, TaskKind.REFACTOR, TaskKind.TEST, TaskKind.INTEGRATION}
)

_MAX_REVIEW_ATTEMPTS = 2


def enforce_independence(worker_identity: str, reviewer_identity: str) -> None:
    """A reviewer must never be the agent that produced the artefact."""
    if worker_identity == reviewer_identity:
        msg = (
            f"self-review is forbidden: worker and reviewer identity are both "
            f"{worker_identity!r}"
        )
        raise PermanentError(msg)


def deterministic_checks(task: Task, outcome: WorkerOutcome) -> dict[str, bool]:
    """Facts that need no model judgement."""
    checks: dict[str, bool] = {
        "worker_reported_success": outcome.status == "success",
        "objective_not_empty": bool(task.objective.strip()),
        "acceptance_criteria_present": bool(task.acceptance_criteria),
    }
    if task.kind in _CODE_KINDS:
        checks["artefact_for_code_task"] = bool(outcome.files) or bool(outcome.diff)
    if outcome.tests_run:
        checks["reported_tests_passed"] = outcome.tests_passed is True
        checks["failed_tests_are_not_approvable"] = outcome.tests_passed is not False
    checks["evidence_or_summary"] = bool(outcome.evidence) or bool(outcome.summary.strip())
    machine = [ac for ac in task.acceptance_criteria if ac.is_machine_verifiable]
    if machine:
        checks["machine_verifiable_criteria"] = all(
            (ac.command in outcome.tests_run) if ac.command else True for ac in machine
        )
    return checks


class Reviewer:
    """Reviews one worker attempt; never implements anything."""

    role = "reviewer"
    identity = "reviewer"

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        project_id: str = "unknown",
        use_provider: bool = True,
        max_attempts: int = _MAX_REVIEW_ATTEMPTS,
    ) -> None:
        self.runtime = runtime
        self.project_id = project_id
        self.use_provider = use_provider
        self.max_attempts = max(1, max_attempts)

    async def review(
        self,
        task: Task,
        outcome: WorkerOutcome,
        *,
        worker_identity: str = "worker",
        diff: str = "",
        repair_round: int = 0,
    ) -> Review:
        enforce_independence(worker_identity, self.identity)
        checks = deterministic_checks(task, outcome)
        deterministic_pass = all(checks.values())

        if not self.use_provider:
            return self._verdict(
                task,
                checks,
                rationale="deterministic review (provider review disabled)",
                reviewer="deterministic",
            )

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            messages = reviewer_messages(
                task,
                outcome,
                diff=diff,
                deterministic_checks=checks,
            )
            if attempt > 1:
                messages = [
                    *messages,
                    {
                        "role": "user",
                        "content": (
                            "# Contract violation\n"
                            "Reply with a single JSON object: "
                            '{"verdict": "PASS"|"REJECT", "score": 0.0, "rationale": "...", "issues": [...]}'
                        ),
                    },
                ]
            result = await self.runtime.call(
                messages,
                role=self.role,
                task_id=task.id,
                metadata={
                    "project_id": self.project_id,
                    "stage": "review",
                    "attempt": attempt,
                    "repair_round": repair_round,
                },
            )
            try:
                payload = extract_json_object(result.text)
                return self._verdict_from_payload(
                    task, outcome, checks, payload, deterministic_pass
                )
            except SchemaError as exc:
                last_error = exc
        degraded_reason = f"reviewer output unusable after {self.max_attempts} attempts: {last_error}"
        return self._verdict(
            task,
            checks,
            rationale=degraded_reason,
            reviewer="deterministic-degraded",
            provider_error=str(last_error) if last_error else None,
        )

    # ------------------------------------------------------------------ verdict
    def _verdict_from_payload(
        self,
        task: Task,
        outcome: WorkerOutcome,
        checks: dict[str, bool],
        payload: dict[str, Any],
        deterministic_pass: bool,
    ) -> Review:
        if outcome.status != "success":
            msg = (
                f"reviewer was asked to approve a {outcome.status!r} worker outcome; "
                "only successful attempts can be approved"
            )
            raise SchemaError(msg)
        verdict = str(payload.get("verdict") or "").strip().upper()
        if verdict not in {"PASS", "REJECT"}:
            msg = f"reviewer verdict must be PASS or REJECT, got {verdict!r}"
            raise SchemaError(msg)
        try:
            score = float(payload.get("score", 1.0))
        except (TypeError, ValueError):
            score = 1.0
        issues: list[ReviewIssue] = []
        for item in payload.get("issues") or []:
            if isinstance(item, str):
                issues.append(ReviewIssue(severity=Severity.MEDIUM, message=item))
                continue
            if not isinstance(item, dict):
                continue
            severity_raw = str(item.get("severity") or "medium").lower()
            try:
                severity = Severity(severity_raw)
            except ValueError:
                severity = Severity.MEDIUM
            message = str(item.get("message") or "").strip()
            if not message:
                continue
            issues.append(
                ReviewIssue(
                    severity=severity,
                    message=message,
                    file=str(item["file"]) if item.get("file") else None,
                    line=int(item["line"]) if isinstance(item.get("line"), int) else None,
                )
            )
        accepted = verdict == "PASS" and deterministic_pass
        if not deterministic_pass:
            failed = sorted(name for name, ok in checks.items() if not ok)
            issues.append(
                ReviewIssue(
                    severity=Severity.HIGH,
                    message=(
                        "deterministic checks failed: "
                        + ", ".join(failed)
                        + " (the reviewer model cannot override verifiable facts)"
                    ),
                )
            )
            verdict = "REJECT"
            score = min(score, 0.4)
        return Review(
            task_id=task.id,
            run_id=None,
            verdict="PASS" if accepted else "REJECT",
            score=score,
            issues=issues,
            checks=checks,
            rationale=str(payload.get("rationale") or ""),
            reviewer="model+deterministic",
        )

    def _verdict(
        self,
        task: Task,
        checks: dict[str, bool],
        *,
        rationale: str,
        reviewer: str,
        provider_error: str | None = None,
    ) -> Review:
        passed = all(checks.values())
        issues: list[ReviewIssue] = []
        if not passed:
            failed = sorted(name for name, ok in checks.items() if not ok)
            issues.append(
                ReviewIssue(
                    severity=Severity.HIGH,
                    message="deterministic checks failed: " + ", ".join(failed),
                )
            )
        if provider_error:
            issues.append(
                ReviewIssue(
                    severity=Severity.MEDIUM,
                    message=f"reviewer model unavailable: {provider_error}",
                )
            )
        return Review(
            task_id=task.id,
            verdict="PASS" if passed else "REJECT",
            score=0.9 if passed else 0.3,
            issues=issues,
            checks=checks,
            rationale=rationale,
            reviewer=reviewer,
        )


class ReviewOutcomeError(PermanentError):
    """A review verdict could not be established (used by engine assertions)."""


def validate_review(review: Review) -> None:
    """Guard used by the scheduler before acting on a verdict."""
    if review.verdict == "REJECT" and not review.issues:
        msg = f"review of {review.task_id} rejects without any issue"
        raise ReviewOutcomeError(msg)
    if review.verdict == "PASS" and review.must_fix:  # pragma: no cover - defensive
        msg = f"review of {review.task_id} is contradictory (PASS with blocking issues)"
        raise ReviewOutcomeError(msg)
