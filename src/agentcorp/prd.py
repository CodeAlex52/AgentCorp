"""PRD → structured requirement.

Two paths, one contract:

* :func:`parse_requirement` asks the model (through the runtime, so retries,
  budget and run recording all apply) and validates the reply against the PRD
  schema.  A malformed reply is retried with a stricter instruction; after the
  bounded number of retries it raises :class:`~agentcorp.errors.SchemaError` —
  a run whose requirement cannot be parsed must fail loudly, not guess.
* :func:`heuristic_requirement` is a deterministic, offline extractor used by
  tests and by ``--prd-mode heuristic``.  It never invents acceptance criteria
  beyond the generic checkable defaults.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .errors import SchemaError
from .models import AcceptanceCriterion, Requirement
from .parsing import as_list_of_str, extract_json_object
from .prompts import prd_messages
from .runtime import AgentRuntime

__all__ = ["parse_requirement", "heuristic_requirement", "VALID_VERIFICATIONS"]

VALID_VERIFICATIONS = frozenset({"test", "command", "static", "manual", "review"})

_MAX_PRD_ATTEMPTS = 2


async def parse_requirement(
    runtime: AgentRuntime,
    raw_text: str,
    *,
    project_id: str = "unknown",
    max_attempts: int = _MAX_PRD_ATTEMPTS,
) -> Requirement:
    """Ask the model to structure ``raw_text`` into a :class:`Requirement`."""
    if not raw_text.strip():
        msg = "PRD text is empty; nothing to plan"
        raise SchemaError(msg)
    repair = ""
    last_error: Exception | None = None
    for attempt in range(1, max(1, max_attempts) + 1):
        messages = prd_messages(raw_text)
        if repair:
            messages = [
                *messages,
                {
                    "role": "user",
                    "content": (
                        "# Contract violation\n"
                        f"Your previous reply was rejected: {repair}\n"
                        "Reply with a single JSON object matching the schema exactly."
                    ),
                },
            ]
        result = await runtime.call(
            messages,
            role="prd",
            task_id=None,
            metadata={"project_id": project_id, "attempt": attempt},
        )
        try:
            return requirement_from_payload(
                extract_json_object(result.text), raw_text=raw_text
            )
        except SchemaError as exc:
            last_error = exc
            repair = str(exc)
    assert last_error is not None
    raise last_error


def requirement_from_payload(payload: dict[str, Any], *, raw_text: str) -> Requirement:
    """Validate a model/JSON payload into a :class:`Requirement` (strict)."""
    goal = str(payload.get("goal") or "").strip()
    if not goal:
        msg = "requirement payload has no non-empty 'goal'"
        raise SchemaError(msg)
    criteria_payload = payload.get("acceptance_criteria") or []
    if isinstance(criteria_payload, dict):
        criteria_payload = [criteria_payload]
    if not isinstance(criteria_payload, list):
        msg = "acceptance_criteria must be a list"
        raise SchemaError(msg)
    criteria = [_criterion_from(item) for item in criteria_payload]
    keywords = as_list_of_str(payload.get("keywords"))
    if not keywords:
        keywords = _keywords(raw_text)
    deliverable_text = as_list_of_str(payload.get("deliverables")) or [
        "Deliver the change described in the PRD"
    ]
    return Requirement(
        raw_text=raw_text,
        goal=goal,
        deliverables=deliverable_text,
        constraints=as_list_of_str(payload.get("constraints")),
        acceptance_criteria=criteria,
        keywords=keywords,
    )


def _criterion_from(item: Any) -> AcceptanceCriterion:
    if isinstance(item, str):
        return AcceptanceCriterion(statement=item, verification="review")
    if not isinstance(item, dict):
        msg = f"acceptance criterion must be an object or string, got {type(item).__name__}"
        raise SchemaError(msg)
    statement = str(item.get("statement") or item.get("text") or "").strip()
    if not statement:
        msg = "acceptance criterion has no statement"
        raise SchemaError(msg)
    verification = str(item.get("verification") or "review").lower()
    if verification not in VALID_VERIFICATIONS:
        msg = f"unknown verification mode {verification!r}"
        raise SchemaError(msg)
    command = item.get("command")
    return AcceptanceCriterion(
        statement=statement,
        verification=verification,  # type: ignore[arg-type]
        command=str(command) if command else None,
        expected=str(item["expected"]) if item.get("expected") else None,
    )


def heuristic_requirement(raw_text: str) -> Requirement:
    """Deterministic offline extraction (no provider involved).

    Bullets become deliverables; lines containing "must"/"should not"/"without"
    become constraints; acceptance criteria default to a review check on the
    first deliverable so tasks always have something checkable.
    """
    deliverables: list[str] = []
    constraints: list[str] = []
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("-", "*", "•")):
            deliverables.append(line.lstrip("-*• ").strip())
        lowered = line.lower()
        if any(marker in lowered for marker in ("must ", "must not", "should not", "without ", "no network")):
            constraints.append(line)
    if not deliverables:
        first_sentence = raw_text.strip().split(".")[0].strip()
        deliverables = [first_sentence or "Deliver the requested change"]
    goal = deliverables[0]
    criteria = [
        AcceptanceCriterion(
            statement=f"{deliverable} is complete and reviewable",
            verification="review",
        )
        for deliverable in deliverables[:3]
    ]
    return Requirement(
        raw_text=raw_text,
        goal=goal,
        deliverables=deliverables,
        constraints=constraints,
        acceptance_criteria=criteria,
        keywords=_keywords(raw_text),
    )


_STOPWORDS = frozenset(
    {
        "the", "and", "for", "with", "that", "this", "into", "from", "must", "should",
        "will", "your", "you", "are", "not", "all", "any", "its", "their", "then",
        "than", "when", "have", "has", "was", "were", "been", "they", "them", "our",
    }
)


def _keywords(text: str, limit: int = 20) -> list[str]:
    import re

    words: Sequence[str] = re.findall(r"[A-Za-z][A-Za-z0-9_]{2,}", text.lower())
    out: list[str] = []
    for word in words:
        if word in _STOPWORDS or word in out:
            continue
        out.append(word)
        if len(out) >= limit:
            break
    return out
