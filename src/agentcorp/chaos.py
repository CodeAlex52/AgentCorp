"""Chaos mode: deliberately break things to prove the orchestrator recovers.

Injected at the *transport* boundary as a decorator over any
:class:`~agentcorp.providers.base.AgentProvider`, because that is exactly where
a real provider fails.  Nothing downstream (worker, scheduler, store) needs to
know chaos is enabled — if the recovery logic only works when told it is being
tested, it does not work.

Faults are drawn from a seeded RNG, so a failing run is reproducible from its
seed alone: ``agentcorp run ... --chaos 0.3 --chaos-seed 7``.
"""

from __future__ import annotations

import random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .errors import (
    ContextOverflowError,
    ProviderError,
    RateLimitError,
    TimeoutError_,
    TransientError,
)
from .providers.base import AgentProvider, CompletionRequest, CompletionResponse

__all__ = [
    "FaultKind",
    "ChaosConfig",
    "ChaosController",
    "ChaosProvider",
    "ChaosReport",
]


class FaultKind(StrEnum):
    RATE_LIMIT = "429"
    TIMEOUT = "timeout"
    INVALID_JSON = "invalid_json"
    AGENT_CRASH = "agent_crash"
    TOOL_FAILURE = "tool_failure"
    WORKER_STUCK = "worker_stuck"
    DUPLICATE_RESPONSE = "duplicate_response"
    CONTEXT_OVERFLOW = "context_overflow"


#: Which faults are recoverable by a plain retry, and which need different
#: handling.  This mapping is the contract between chaos and the runtime.
RETRY_RECOVERABLE: frozenset[FaultKind] = frozenset(
    {FaultKind.RATE_LIMIT, FaultKind.TIMEOUT, FaultKind.AGENT_CRASH, FaultKind.TOOL_FAILURE, FaultKind.WORKER_STUCK}
)
SCHEMA_RECOVERABLE: frozenset[FaultKind] = frozenset({FaultKind.INVALID_JSON})
CONTEXT_RECOVERABLE: frozenset[FaultKind] = frozenset({FaultKind.CONTEXT_OVERFLOW})
IDEMPOTENT_RECOVERABLE: frozenset[FaultKind] = frozenset({FaultKind.DUPLICATE_RESPONSE})


@dataclass
class ChaosConfig:
    """How aggressive the injector is.

    ``probability`` is per attempt, not per task: a 0.3 setting with
    ``max_attempts=3`` usually still gets work done, which is the point —
    the run should *succeed despite* faults.
    """

    enabled: bool = False
    probability: float = 0.2
    kinds: tuple[FaultKind, ...] = tuple(FaultKind)
    seed: int = 0
    max_injections: int | None = None
    roles: tuple[str, ...] = ()  # empty = all roles
    fail_forever: bool = False  # escalate: every eligible attempt faults

    def normalised(self) -> ChaosConfig:
        """Validate and canonicalise.

        ``kinds`` arrives as plain strings from the CLI (``--chaos-kinds 429,timeout``),
        so coerce them to :class:`FaultKind` here: the downstream reporting code
        reads ``kind.value`` and would raise ``AttributeError`` on a bare string.
        """
        if not 0.0 <= self.probability <= 1.0:
            msg = "chaos probability must be in [0, 1]"
            raise ValueError(msg)
        try:
            self.kinds = tuple(FaultKind(k) for k in self.kinds)
        except ValueError as exc:
            valid = ", ".join(f.value for f in FaultKind)
            msg = f"unknown chaos fault kind ({exc}); valid kinds: {valid}"
            raise ValueError(msg) from exc
        if not self.kinds:
            self.kinds = tuple(FaultKind)
        return self


@dataclass
class ChaosReport:
    injections: Counter[str] = field(default_factory=Counter)
    recovered: Counter[str] = field(default_factory=Counter)
    tasks_hit: set[str] = field(default_factory=set)

    @property
    def total(self) -> int:
        return sum(self.injections.values())

    def record(self, kind: FaultKind, task_id: str | None = None) -> None:
        self.injections[kind.value] += 1
        if task_id:
            self.tasks_hit.add(task_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_injections": self.total,
            "by_kind": dict(self.injections),
            "tasks_hit": sorted(self.tasks_hit),
            "recoveries": dict(self.recovered),
        }


class ChaosController:
    """Decides whether to fault, and which fault to raise."""

    def __init__(
        self,
        config: ChaosConfig | None = None,
        *,
        rng: random.Random | None = None,
        on_inject: Callable[[FaultKind, str | None, str], None] | None = None,
    ) -> None:
        self.config = (config or ChaosConfig()).normalised()
        self.rng = rng or random.Random(self.config.seed)
        self.on_inject = on_inject
        self.report = ChaosReport()
        self._last_response: dict[str, str] = {}

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def should_fault(self, role: str, task_id: str | None) -> bool:
        if not self.enabled:
            return False
        if self.config.roles and role not in self.config.roles:
            return False
        if self.config.max_injections is not None and self.report.total >= self.config.max_injections:
            return False
        if self.config.fail_forever:
            return True
        return self.rng.random() < self.config.probability

    def pick(self) -> FaultKind:
        return self.rng.choice(self.config.kinds)

    def note(self, kind: FaultKind, task_id: str | None, detail: str = "") -> None:
        self.report.record(kind, task_id)
        if self.on_inject is not None:
            self.on_inject(kind, task_id, detail)

    def note_recovery(self, kind: FaultKind) -> None:
        self.report.recovered[kind.value] += 1

    def remember(self, key: str, text: str) -> None:
        self._last_response[key] = text

    def recall(self, key: str) -> str | None:
        return self._last_response.get(key)


class ChaosProvider(AgentProvider):
    """Wraps another provider and injects faults into some calls.

    ``DUPLICATE_RESPONSE`` replays the previous response for the same task
    instead of failing — a real idempotency hazard, and the only fault here
    that a correct system should be able to detect rather than retry.
    """

    def __init__(self, inner: AgentProvider, controller: ChaosController) -> None:
        self.inner = inner
        self.controller = controller
        self.name = f"chaos({inner.name})"
        self.default_model = inner.default_model
        self.pricing = inner.pricing
        self.injections: list[FaultKind] = []

    async def aclose(self) -> None:
        await self.inner.aclose()

    def describe(self) -> dict[str, Any]:
        return {**self.inner.describe(), "chaos": self.controller.report.to_dict()}

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        key = request.task_id or request.role
        if self.controller.should_fault(request.role, request.task_id):
            kind = self.controller.pick()
            self.injections.append(kind)
            self.controller.note(kind, request.task_id, f"role={request.role}")

            if kind == FaultKind.RATE_LIMIT:
                raise RateLimitError("chaos: injected 429", retry_after=0.1)
            if kind == FaultKind.TIMEOUT:
                raise TimeoutError_("chaos: injected timeout")
            if kind == FaultKind.AGENT_CRASH:
                raise ProviderError("chaos: injected agent crash")
            if kind == FaultKind.TOOL_FAILURE:
                raise TransientError("chaos: injected tool failure")
            if kind == FaultKind.WORKER_STUCK:
                raise TimeoutError_("chaos: injected stuck worker (no progress)")
            if kind == FaultKind.CONTEXT_OVERFLOW:
                raise ContextOverflowError("chaos: injected context overflow", tokens=10**6)
            if kind == FaultKind.INVALID_JSON:
                return CompletionResponse(
                    text='I think the answer is: {"status": "success", oops: not json',
                    model=self.default_model,
                    provider=self.name,
                    raw={"chaos": kind.value},
                )
            if kind == FaultKind.DUPLICATE_RESPONSE:
                previous = self.controller.recall(key)
                if previous is not None:
                    return CompletionResponse(
                        text=previous,
                        model=self.default_model,
                        provider=self.name,
                        raw={"chaos": kind.value, "duplicate": True},
                    )
                # Nothing to duplicate yet; this is not a failure.

        response = await self.inner.complete(request)
        self.controller.remember(key, response.text)
        return response


def chaos_provider(provider: AgentProvider, config: ChaosConfig | None = None, **kwargs: Any) -> ChaosProvider:
    return ChaosProvider(provider, ChaosController(config, **kwargs))

