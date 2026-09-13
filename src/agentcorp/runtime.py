"""Agent runtime: the single path every model call goes through.

Composition order matters and is deliberate::

    circuit breaker  -> fail fast before spending anything
    budget           -> refuse before spending anything
    rate limiter     -> pace before spending anything
    retry loop       -> spend, and recover from transient failure
    run recording    -> capture prompt+response for replay/budget, every attempt

Because every call funnels through :meth:`AgentRuntime.call`, no agent role can
accidentally bypass retries, budget accounting or tracing — a property the
adversarial review explicitly checks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .budget import BudgetManager
from .errors import ContextOverflowError, TransientError
from .events import EventEmitter, EventType
from .models import AgentRun, Usage
from .providers.base import AgentProvider, CompletionRequest, Message, estimate_tokens
from .reliability import CircuitBreaker, RetryOutcome, RetryPolicy, TokenBucket, call_with_retry
from .util import Clock, Sleeper, SystemClock, content_hash, system_sleep

__all__ = ["AgentRuntime", "RuntimeResult", "RunRecorder"]


@dataclass
class RuntimeResult:
    """Everything a caller needs to know about a completed call."""

    text: str
    usage: Usage
    run: AgentRun
    attempts: int = 1
    recovered_from: list[str] = field(default_factory=list)
    shrunk_context: int = 0
    delay_total: float = 0.0


#: ``record(run)`` — called at start and at completion.  The engine turns these
#: into AGENT_RUN_STARTED / AGENT_RUN_FINISHED events.  A run that is started
#: but never finished is how ``resume`` detects a crash.
RunRecorder = Callable[[AgentRun], None]


class AgentRuntime:
    """Policy layer around a provider."""

    def __init__(
        self,
        provider: AgentProvider,
        *,
        retry: RetryPolicy | None = None,
        breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucket | None = None,
        budget: BudgetManager | None = None,
        emit: EventEmitter | None = None,
        record_run: RunRecorder | None = None,
        clock: Clock | None = None,
        sleep: Sleeper = system_sleep,
        max_context_shrinks: int = 2,
        run_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.provider = provider
        self.retry = retry or RetryPolicy()
        self.clock = clock or SystemClock()
        self.breaker = breaker or CircuitBreaker(clock=self.clock, name=getattr(provider, "name", "provider"))
        self.rate_limiter = rate_limiter
        self.budget = budget
        self.emit = emit
        self.record_run = record_run
        self.sleep = sleep
        self.max_context_shrinks = max_context_shrinks
        self._run_id_factory = run_id_factory
        self.call_count = 0

    # ------------------------------------------------------------------ public
    async def call(
        self,
        messages: Sequence[Message | dict[str, str]],
        *,
        role: str = "worker",
        task_id: str | None = None,
        model: str | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
        metadata: dict[str, Any] | None = None,
        estimated_tokens: int | None = None,
        timeout: float | None = None,
    ) -> RuntimeResult:
        """Run one logical agent call, with all policy applied."""
        normalised = _coerce_messages(messages)
        request = CompletionRequest(
            messages=normalised,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            metadata=metadata or {},
            role=role,
            task_id=task_id,
        )
        estimate = (
            estimated_tokens if estimated_tokens is not None else estimate_tokens(request.text())
        )

        self.breaker.before_call()
        if self.budget is not None:
            self.budget.check(task_id=task_id, estimated_tokens=estimate)

        run = AgentRun(
            project_id=str((metadata or {}).get("project_id", "unknown")),
            task_id=task_id,
            role=role,
            provider=getattr(self.provider, "name", "unknown"),
            model=model or str(getattr(self.provider, "default_model", "unknown")),
            prompt=request.text(),
            request_hash=content_hash(request.text()),
            started_at=self.clock.now(),
        )
        if self._run_id_factory is not None:
            run.id = self._run_id_factory()
        self._record(run)

        outcome = RetryOutcome()
        shrinks = 0
        recovered: list[str] = []
        started_monotonic = self.clock.monotonic()
        attempts_seen = 0

        async def attempt_closure(attempt: int) -> str:
            nonlocal shrinks, attempts_seen
            attempts_seen = max(attempts_seen, attempt)
            run.attempt = attempt
            if self.rate_limiter is not None:
                await self.rate_limiter.acquire(sleep=self.sleep)
            try:
                response = await self.provider.complete(request)
            except ContextOverflowError as exc:
                # Shrink the largest user message and retry — but only a couple
                # of times, then let the error surface to the caller.  The
                # shrink must re-raise something *retryable*: the taxonomy marks
                # ContextOverflowError non-retryable precisely because
                # retrying it unchanged is pointless (BASELINE_AUDIT DEF-03).
                if shrinks >= self.max_context_shrinks:
                    raise
                shrinks += 1
                request.messages = _shrink_largest(request.messages)
                recovered.append(f"context_overflow_shrink_{shrinks}")
                msg = (
                    f"context overflow ({exc}); context shrunk to fit, retrying "
                    f"with {len(request.messages)} message(s)"
                )
                raise TransientError(msg) from exc
            run.usage = response.usage
            run.response = response.text
            run.response_hash = content_hash(response.text)
            run.model = response.model
            chaos = response.raw.get("chaos") if isinstance(response.raw, dict) else None
            if chaos:
                run.chaos_injections = [*run.chaos_injections, str(chaos)]
            return response.text

        try:
            text = await call_with_retry(
                attempt_closure,
                self.retry,
                sleep=self.sleep,
                on_attempt=lambda attempt, exc, delay: self._on_attempt(run, attempt, exc, delay),
                outcome=outcome,
            )
        except BaseException as exc:
            self.breaker.on_failure()
            run.ok = False
            run.error = str(exc)
            run.error_type = type(exc).__name__
            run.finished_at = self.clock.now()
            run.usage.duration_s = round(self.clock.monotonic() - started_monotonic, 4)
            run.usage.calls = attempts_seen or outcome.attempts or 1
            self._record(run, finished=True)
            if self.budget is not None:
                self.budget.record(run.usage, task_id=task_id)
            raise
        else:
            self.breaker.on_success()
            run.ok = True
            run.finished_at = self.clock.now()
            run.usage.duration_s = round(self.clock.monotonic() - started_monotonic, 4)
            run.usage.calls = attempts_seen or 1
            run.usage.cost_usd = (
                run.usage.cost_usd
                if run.usage.cost_usd
                else self.provider.cost_of(run.usage.tokens_in, run.usage.tokens_out)
            )
            self._record(run, finished=True)
            if self.budget is not None:
                self.budget.record(run.usage, task_id=task_id)

        self.call_count += 1
        return RuntimeResult(
            text=text,
            usage=run.usage,
            run=run,
            attempts=outcome.attempts,
            recovered_from=recovered,
            shrunk_context=shrinks,
            delay_total=round(sum(outcome.delays), 4),
        )

    async def aclose(self) -> None:
        await self.provider.aclose()

    # ----------------------------------------------------------------- helpers
    def _record(self, run: AgentRun, *, finished: bool = False) -> None:
        if self.record_run is None:
            return
        if finished:
            run.finished_at = run.finished_at or self.clock.now()
        self.record_run(run)

    def _on_attempt(self, run: AgentRun, attempt: int, exc: BaseException | None, delay: float) -> None:
        if exc is None or self.emit is None:
            return
        self.emit(
            EventType.NOTE,
            run.task_id,
            {
                "stage": "agent_retry",
                "run_id": run.id,
                "attempt": attempt,
                "role": run.role,
                "error": f"{type(exc).__name__}: {exc}",
                "delay_s": round(delay, 3),
                "summary": f"{run.role} attempt {attempt} failed ({type(exc).__name__}), retry in {delay:.2f}s",
            },
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "provider": getattr(self.provider, "name", "unknown"),
            "model": getattr(self.provider, "default_model", "unknown"),
            "calls": self.call_count,
            "circuit": self.breaker.snapshot(),
            "retry": {
                "max_attempts": self.retry.max_attempts,
                "base_delay": self.retry.base_delay,
                "max_delay": self.retry.max_delay,
                "jitter": self.retry.jitter,
            },
            "rate_limit": self.rate_limiter.snapshot() if self.rate_limiter else None,
        }


def _coerce_messages(messages: Sequence[Message | dict[str, str]]) -> list[Message]:
    out: list[Message] = []
    for message in messages:
        out.append(
            message if isinstance(message, Message) else Message.model_validate(message)
        )
    return out


def _shrink_largest(messages: list[Message], keep_ratio: float = 0.5) -> list[Message]:
    """Halve the biggest non-system message.

    Context overflow is usually caused by the repository dump, not the task, so
    the system prompt and the task block survive intact and the model still
    knows what it is doing.
    """
    candidates = [(i, m) for i, m in enumerate(messages) if m.role != "system"]
    if not candidates:
        candidates = list(enumerate(messages))
    index, biggest = max(candidates, key=lambda pair: len(pair[1].content))
    content = biggest.content
    keep = max(int(len(content) * keep_ratio), 512)
    if keep >= len(content):
        keep = max(len(content) // 2, 1)
    head = content[: int(keep * 0.7)]
    tail = content[-int(keep * 0.3) :] if keep > 100 else ""
    shrunk = head + "\n\n... [context truncated to fit the model window] ...\n\n" + tail
    out = list(messages)
    out[index] = Message(role=biggest.role, content=shrunk)
    return out


async def gather_limited(coros: Sequence[Any], limit: int = 4) -> list[Any]:
    """Run awaitables with a concurrency cap, preserving order."""
    semaphore = asyncio.Semaphore(max(limit, 1))

    async def run(coro: Any) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(*(run(c) for c in coros))

