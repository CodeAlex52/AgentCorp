"""Mock provider: a deterministic, offline stand-in for a real model.

This is not a stub that returns ``"ok"``.  It simulates an agent that
understands AgentCorp's three wire contracts (plan, worker, review) by reading
the contract marker that :mod:`agentcorp.prompts` puts in every system message.
That is what makes it possible to run the *entire* pipeline — planning,
decomposition, review, chaos, benchmark — in CI with zero network and zero API
spend, while still exercising every code path a real model would.

Determinism matters: ``skill`` and ``seed`` pin behaviour per task id, so a
benchmark run is reproducible bit-for-bit.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
from collections.abc import Callable, Sequence

from ..errors import ContextOverflowError, RateLimitError, TimeoutError_
from ..models import Usage
from ..util import Clock, SystemClock
from .base import AgentProvider, CompletionRequest, CompletionResponse, estimate_tokens

__all__ = ["MockProvider", "Behavior"]

Behavior = Callable[[CompletionRequest], str] | str

_CONTRACT_RE = re.compile(r"CONTRACT:\s*([a-z_]+)")
_TASK_RE = re.compile(r"TASK:\s*(\S+)")
_TITLE_RE = re.compile(r"TITLE:\s*(.+)")
_TOUCH_RE = re.compile(r"TOUCH PATHS:\s*(.+)", re.IGNORECASE)
_SIMULATE_RE = re.compile(r"SIMULATE:\s*([a-z_]+)")
_MAX_TOKENS_RE = re.compile(r"context window of (\d+) tokens")


class MockProvider(AgentProvider):
    """Scriptable, deterministic provider.

    Parameters
    ----------
    skill:
        Probability that a worker task succeeds, in ``[0, 1]``.  Failures are
        seeded per task so re-running the same task gives the same outcome —
        which is what makes "retry eventually succeeds" testable.
    latency:
        Simulated per-call latency in seconds (0 disables sleeping).
    script:
        Explicit response override: either a fixed string or a callable taking
        the request.
    """

    name = "mock"
    default_model = "mock-agent-v1"

    def __init__(
        self,
        *,
        skill: float = 1.0,
        seed: int = 0,
        latency: float = 0.0,
        script: Behavior | None = None,
        clock: Clock | None = None,
        context_window: int | None = None,
        rng: random.Random | None = None,
        max_subtasks: int = 5,
    ) -> None:
        self.skill = skill
        self.seed = seed
        self.latency = latency
        self.script = script
        self.clock = clock or SystemClock()
        self.context_window = context_window
        self.max_subtasks = max(max_subtasks, 1)
        self._rng = rng or random.Random(seed)
        self.calls: list[CompletionRequest] = []
        self.responses: list[str] = []

    # ------------------------------------------------------------------ public
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        self.calls.append(request)
        if self.latency:
            await asyncio.sleep(self.latency)

        if self.context_window is not None:
            prompt_tokens = estimate_tokens(request.text())
            if prompt_tokens > self.context_window:
                raise ContextOverflowError(
                    f"prompt of ~{prompt_tokens} tokens exceeds context window {self.context_window}",
                    tokens=prompt_tokens,
                )

        text = self._render(request)
        self.responses.append(text)

        tokens_in = estimate_tokens(request.text())
        tokens_out = estimate_tokens(text)
        return CompletionResponse(
            text=text,
            model=request.model or self.default_model,
            provider=self.name,
            usage=Usage(
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                calls=1,
                cost_usd=0.0,
                duration_s=self.latency,
            ),
            finish_reason="stop",
            raw={"mock": True, "contract": self._contract_of(request)},
        )

    # ----------------------------------------------------------------- routing
    @staticmethod
    def _contract_of(request: CompletionRequest) -> str:
        match = _CONTRACT_RE.search(request.system) or _CONTRACT_RE.search(request.text())
        return match.group(1) if match else request.role

    def _render(self, request: CompletionRequest) -> str:
        simulate = _SIMULATE_RE.search(request.text())
        if simulate:
            forced = simulate.group(1)
            if forced == "rate_limit":
                raise RateLimitError("mock: simulated 429", retry_after=1.0)
            if forced == "timeout":
                raise TimeoutError_("mock: simulated timeout")
            if forced == "invalid_json":
                return "Sure! Here is my answer: {not really json, sorry."
            if forced in {"blocked", "crash"}:
                return self._blocked_worker(request, forced)
            if forced == "context_overflow":
                raise ContextOverflowError("mock: simulated context overflow", tokens=10**6)

        if isinstance(self.script, str):
            return self.script
        if callable(self.script):
            return self.script(request)

        contract = self._contract_of(request)
        handler = {
            "planner": self._plan,
            "prd": self._requirement,
            "worker": self._worker,
            "reviewer": self._review,
            "review": self._review,
            "decomposer": self._decompose,
        }.get(contract, self._worker)
        return handler(request)

    # ----------------------------------------------------------------- workers
    def _worker(self, request: CompletionRequest) -> str:
        task_id = self._task_id(request)
        title = self._title(request)
        roll = self._roll(task_id, "worker")
        if roll > self.skill:
            return self._blocked_worker(request, "insufficient_skill")

        touch = self._touch_paths(request)
        wants_investigation = any(
            word in title.lower() for word in ("investigate", "research", "analyze", "analyse", "explore")
        )
        if wants_investigation or not touch:
            body: dict[str, object] = {
                "status": "success",
                "summary": f"Investigated: {title}. Found the relevant modules and described "
                "how they interact; no code changes required for this investigation task.",
                "files": [],
                "evidence": [
                    "read repository layout and entry points",
                    "traced the call path for the described feature",
                ],
                "followups": [f"implementation work for {title}"],
            }
            return json.dumps(body, indent=2)

        files: list[dict[str, str]] = []
        for path in touch:
            files.append({"path": path, "content": self._file_body(path, title), "mode": "write"})
        body = {
            "status": "success",
            "summary": f"Implemented {title} in {len(files)} file(s).",
            "files": files,
            "tests_run": ["pytest -q"],
            "tests_passed": True,
            "evidence": [f"wrote {p}" for p in touch],
        }
        return json.dumps(body, indent=2)

    def _blocked_worker(self, request: CompletionRequest, why: str) -> str:
        title = self._title(request)
        body: dict[str, object] = {
            "status": "blocked",
            "summary": f"Cannot complete '{title}' as one unit.",
            "reason": f"mock:{why} — the objective spans several independent concerns",
            "new_information": [
                "the work touches both the transport layer and its callers",
                "the acceptance criteria cannot be checked without an interface change",
            ],
            "recommended_subtasks": [
                f"Define the interface for {title}",
                f"Implement {title} against the interface",
                f"Test {title} end to end",
            ],
        }
        return json.dumps(body, indent=2)

    # ---------------------------------------------------------------- reviewer
    def _review(self, request: CompletionRequest) -> str:
        text = request.text()
        forced = "reject" if "SIMULATE: reject" in text else None
        roll = self._roll(self._task_id(request), "reviewer")
        empty_diff = "DIFF: (empty)" in text
        reject = forced == "reject" or empty_diff or roll > max(self.skill, 0.95)
        if reject:
            body = {
                "verdict": "REJECT",
                "score": 0.35,
                "rationale": "mock reviewer: no verifiable artefact for the stated acceptance criteria",
                "issues": [
                    {
                        "severity": "high",
                        "message": "implementation produced no diff while the task demands code changes",
                    }
                ],
            }
        else:
            body = {
                "verdict": "PASS",
                "score": 0.9,
                "rationale": "mock reviewer: diff present, acceptance criteria addressed, no blocking issues",
                "issues": [],
            }
        return json.dumps(body, indent=2)

    # ----------------------------------------------------------------- planner
    def _plan(self, request: CompletionRequest) -> str:  # noqa: ARG002 - handler signature is uniform across contracts
        """Emit a small but structurally valid DAG for the mock repository."""
        titles = [
            ("Investigate current structure", "research", []),
            ("Implement the change", "implementation", [0]),
            ("Add tests", "test", [1]),
            ("Integration verification", "integration", [2]),
        ]
        tasks = []
        for index, (title, kind, deps) in enumerate(titles):
            tasks.append(
                {
                    "key": f"A{index}",
                    "title": title,
                    "objective": f"{title} as required by the PRD.",
                    "kind": kind,
                    "depends_on": [f"A{d}" for d in deps],
                    "expected_output": title,
                    "acceptance_criteria": [
                        {
                            "statement": f"{title} is complete and verifiable",
                            "verification": "review",
                        }
                    ],
                    "risk": "medium",
                }
            )
        return json.dumps({"tasks": tasks}, indent=2)

    def _requirement(self, request: CompletionRequest) -> str:
        text = request.text()
        return json.dumps(
            {
                "goal": "Deliver the change described in the PRD.",
                "deliverables": [line.strip("- ").strip() for line in text.splitlines() if line.strip().startswith("-")][:8]
                or ["Deliver the requested change"],
                "constraints": [],
                "acceptance_criteria": [
                    {"statement": "the requested behaviour is implemented", "verification": "review"}
                ],
                "keywords": _keywords(text),
            },
            indent=2,
        )

    # -------------------------------------------------------------- decomposer
    def _decompose(self, request: CompletionRequest) -> str:
        """Deterministic SPLIT decision for the decomposer contract.

        The baseline had no decomposer handler, so this prompt fell through to
        the worker handler and always failed to parse (BASELINE_AUDIT DEF-09).
        """
        text = request.text()
        title = self._title(request)
        objectives: list[str] = []
        for line in text.splitlines():
            stripped = line.strip(" -•\t")
            if stripped.lower().startswith(("define ", "implement ", "test ")):
                objectives.append(stripped)
        if not objectives:
            objectives = [
                f"Define the interface for {title}",
                f"Implement {title} against the interface",
                f"Test {title} end to end",
            ]
        subtasks: list[dict[str, object]] = []
        for index, objective in enumerate(objectives[: self.max_subtasks]):
            subtasks.append(
                {
                    "title": objective[:80],
                    "objective": objective,
                    "kind": "implementation" if index == 1 else ("test" if index >= 2 else "research"),
                    "depends_on_siblings": [index - 1] if index > 0 else [],
                    "acceptance_criteria": [
                        {"statement": f"{objective} is complete and checkable", "verification": "review"}
                    ],
                    "touch_paths": [],
                }
            )
        # The parent's touch paths spread across the actionable children so the
        # split does not silently drop the write scope.
        touches = self._touch_paths(request)
        if touches and subtasks:
            subtasks[min(1, len(subtasks) - 1)]["touch_paths"] = touches
        return json.dumps(
            {
                "should_split": True,
                "reason": "deterministic mock split: the objective spans several concerns",
                "subtasks": subtasks,
            },
            indent=2,
        )

    # ------------------------------------------------------------------ helpers
    def _seed_for(self, key: str, salt: str) -> random.Random:
        digest = hashlib.sha256(f"{self.seed}:{salt}:{key}".encode()).digest()
        return random.Random(int.from_bytes(digest[:8], "big"))

    def _roll(self, key: str, salt: str) -> float:
        return self._seed_for(key, salt).random()

    @staticmethod
    def _task_id(request: CompletionRequest) -> str:
        match = _TASK_RE.search(request.text())
        return match.group(1) if match else "unknown"

    @staticmethod
    def _title(request: CompletionRequest) -> str:
        match = _TITLE_RE.search(request.text())
        return match.group(1).strip() if match else "the requested change"

    @staticmethod
    def _touch_paths(request: CompletionRequest) -> list[str]:
        match = _TOUCH_RE.search(request.text())
        if not match:
            return []
        raw = match.group(1).strip()
        if not raw or raw.lower() in {"(unspecified)", "unspecified"}:
            return [f"src/{_slug(MockProvider._title(request))}.py"]
        parts = [p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()]
        return parts[:4]

    @staticmethod
    def _file_body(path: str, title: str) -> str:
        slug = _slug(title)
        if path.endswith(".py"):
            return (
                f'"""Module produced by the mock agent for: {title}."""\n\n'
                f"from __future__ import annotations\n\n"
                f"__all__ = ['{slug}']\n\n\n"
                f"def {slug}() -> str:\n"
                f'    """Stand-in implementation for {title}."""\n'
                f'    return "{slug}"\n'
            )
        if path.endswith((".md", ".rst")):
            return f"# {title}\n\nDocumentation generated for: {title}.\n"
        return f"# generated for {title}\n"


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower()).strip("_")
    return cleaned or "generated_symbol"


_STOPWORDS = frozenset(
    {"the", "and", "for", "with", "that", "this", "into", "from", "must", "should", "will",
     "task", "your", "you", "are", "not", "all", "any", "its", "their", "then", "than"}
)


def _keywords(text: str) -> list[str]:
    """Salient lowercase terms, used for requirement 'vocabulary coverage' checks."""
    words = re.findall(r"[A-Za-z][A-Za-z0-9_]{2,}", text.lower())
    seen: list[str] = []
    for word in words:
        if word in _STOPWORDS or word in seen:
            continue
        seen.append(word)
    return seen[:20]


class FlakyProvider(MockProvider):
    """Mock that fails the first ``fail_times`` calls, then succeeds.

    Used by tests that assert retry/resume behaviour without simulating chaos.
    """

    def __init__(self, *args: object, fail_times: int = 1, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.fail_times = fail_times
        self.failures = 0

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        if self.failures < self.fail_times:
            self.failures += 1
            self.calls.append(request)
            raise RateLimitError("flaky provider: simulated 429", retry_after=0.0)
        return await super().complete(request)


class ScriptedProvider(AgentProvider):
    """Returns a fixed sequence of responses; raises if the script runs out.

    This is the most precise test double: a test can assert exactly what the
    worker does with reply #3.
    """

    name = "scripted"
    default_model = "scripted-v1"

    def __init__(self, responses: Sequence[str | BaseException], *, latency: float = 0.0) -> None:
        self.responses = list(responses)
        self.latency = latency
        self.calls: list[CompletionRequest] = []

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        self.calls.append(request)
        if not self.responses:
            raise AssertionError(f"scripted provider exhausted after {len(self.calls)} calls")
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        if self.latency:
            await asyncio.sleep(self.latency)
        text = item
        return CompletionResponse(
            text=text,
            model=self.default_model,
            provider=self.name,
            usage=Usage(
                tokens_in=estimate_tokens(request.text()),
                tokens_out=estimate_tokens(text),
                calls=1,
            ),
        )


__all__ += ["FlakyProvider", "ScriptedProvider"]
