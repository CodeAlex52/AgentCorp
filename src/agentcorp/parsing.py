"""Extracting structured data from model output.

Models wrap JSON in prose and fences.  A single robust extractor is used by the
planner, worker and reviewer, so "the model returned something almost-JSON" is
handled in exactly one place and every parser benefits from the fix.
"""

from __future__ import annotations

import json
import re
from typing import Any

from .errors import SchemaError

__all__ = ["extract_json", "extract_json_object", "coerce_bool", "normalise_keys", "as_list_of_str"]

_FENCE_RE = re.compile(r"```(?:json|JSON|javascript|js)?\s*(.*?)```", re.DOTALL)


def _balanced_candidates(text: str) -> list[str]:
    """Every balanced ``{...}`` / ``[...]`` span, outermost first."""
    out: list[str] = []
    for opener, closer in (("{", "}"), ("[", "]")):
        depth = 0
        start = -1
        in_string = False
        escaped = False
        for index, char in enumerate(text):
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == opener:
                if depth == 0:
                    start = index
                depth += 1
            elif char == closer and depth > 0:
                depth -= 1
                if depth == 0 and start >= 0:
                    out.append(text[start : index + 1])
                    start = -1
    return out


def extract_json(text: str) -> Any:
    """Best-effort JSON extraction from model prose.

    Order of attempts: fenced blocks, then balanced spans, then the whole
    string.  Raises :class:`SchemaError` (retryable) if nothing parses —
    the worker turns that into a repair attempt rather than a hard failure.
    """
    if not text or not text.strip():
        raise SchemaError("model returned an empty response")

    candidates: list[str] = []
    for match in _FENCE_RE.findall(text):
        candidates.append(match.strip())
    candidates.extend(_balanced_candidates(text))
    candidates.append(text.strip())

    errors: list[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as exc:
            errors.append(str(exc))
            continue
        except RecursionError:  # pragma: no cover - pathological nesting
            errors.append("recursion limit while parsing")
            continue

    preview = text.strip()[:180].replace("\n", " ")
    detail = errors[0] if errors else "no JSON-like span found"
    raise SchemaError(f"could not extract JSON from model output ({detail}); got: {preview!r}")


def extract_json_object(text: str) -> dict[str, Any]:
    """Like :func:`extract_json` but guarantees an object.

    A model that answers with a bare list is common enough to be worth
    unwrapping when the list holds a single object.
    """
    data = extract_json(text)
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        dicts = [item for item in data if isinstance(item, dict)]
        if len(dicts) == 1:
            return dicts[0]
        for key in ("tasks", "files", "issues", "deliverables"):
            wrapped = {key: data}
            if all(isinstance(item, dict) for item in data):
                return wrapped
    raise SchemaError(f"expected a JSON object, got {type(data).__name__}")


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "y", "1", "pass", "passed", "ok"}:
            return True
        if lowered in {"false", "no", "n", "0", "fail", "failed", ""}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def normalise_keys(data: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    """Rename keys a model may have spelled differently (``acceptance`` →
    ``acceptance_criteria``) without silently dropping them."""
    out = dict(data)
    for canonical, variants in _group_aliases(aliases).items():
        if canonical in out:
            continue
        for variant in variants:
            if variant in out:
                out[canonical] = out.pop(variant)
                break
    return out


def _group_aliases(aliases: dict[str, str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for variant, canonical in aliases.items():
        grouped.setdefault(canonical, []).append(variant)
    return grouped


def as_list_of_str(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [p.strip(" -•\t") for p in re.split(r"[\n;]", value)]
        return [p for p in parts if p]
    if isinstance(value, dict):
        return [f"{k}: {v}" for k, v in value.items()]
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for item in value:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                for key in ("statement", "text", "title", "description", "message"):
                    if key in item and isinstance(item[key], str):
                        out.append(item[key])
                        break
                else:
                    out.append(json.dumps(item))
            else:
                out.append(str(item))
        return out
    return [str(value)]
