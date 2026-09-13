"""Resilience primitives: retry, backoff, jitter, rate limiting, circuit breaking.

Everything here is deterministic under an injected :class:`~agentcorp.util.Clock`
and an injected sleeper, so the failure-handling tests assert on *computed
delays* rather than actually waiting.  No module-level global state: two
projects in the same process cannot corrupt each other's breaker.
"""

from __future__ import annotations

import asyncio
import random
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .errors import CircuitOpenError, RateLimitError, is_retryable
from .util import Clock, Sleeper, SystemClock, system_sleep

__all__ = [
    "RetryPolicy",
    "RetryOutcome",
    "call_with_retry",
    "CircuitState",
    "CircuitBreaker",
    "TokenBucket",
    "CircuitStats",
]


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with configurable jitter.

    ``jitter=0.0`` gives exact exponential backoff (used in tests);
    ``jitter=1.0`` gives AWS-style *full jitter*, which is the sane default
    against a rate-limited API because it decorrelates retrying workers.
    """

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: float = 1.0
    respect_retry_after: bool = True

    def nominal_delay(self, attempt: int) -> float:
        """Deterministic backoff for ``attempt`` (1-based), before jitter."""
        if attempt < 1:
            msg = "attempt is 1-based"
            raise ValueError(msg)
        raw = self.base_delay * (self.multiplier ** (attempt - 1))
        return min(raw, self.max_delay)

    def delay_for(self, attempt: int, rng: random.Random | None = None) -> float:
        nominal = self.nominal_delay(attempt)
        if self.jitter <= 0:
            return nominal
        r = rng or random
        # Blend: jitter=0 -> nominal, jitter=1 -> uniform(0, nominal).
        full_jitter = r.uniform(0.0, nominal)
        return nominal * (1.0 - self.jitter) + full_jitter * self.jitter


@dataclass
class RetryOutcome:
    """Bookkeeping for one logical call (which may span several attempts)."""

    attempts: int = 0
    errors: list[str] = field(default_factory=list)
    delays: list[float] = field(default_factory=list)
    recovered: bool = False

    @property
    def exhausted(self) -> bool:
        return not self.recovered


async def call_with_retry[T](
    fn: Callable[[int], Awaitable[T]],
    policy: RetryPolicy | None = None,
    *,
    sleep: Sleeper = system_sleep,
    rng: random.Random | None = None,
    on_attempt: Callable[[int, BaseException | None, float], None] | None = None,
    retry_on: Callable[[BaseException], bool] = is_retryable,
    outcome: RetryOutcome | None = None,
) -> T:
    """Call ``fn(attempt)`` with backoff until it succeeds or attempts run out.

    ``fn`` receives the 1-based attempt number so callers can vary the prompt on
    a repair retry.  ``retry_on`` decides retryability, defaulting to the
    error taxonomy in :mod:`agentcorp.errors`; a non-retryable error escapes
    immediately rather than burning the remaining attempts.
    """
    policy = policy or RetryPolicy()
    book = outcome if outcome is not None else RetryOutcome()
    last_error: BaseException | None = None

    for attempt in range(1, policy.max_attempts + 1):
        book.attempts = attempt
        try:
            result = await fn(attempt)
        except BaseException as exc:  # noqa: BLE001 - re-raised below unless retryable
            last_error = exc
            book.errors.append(f"{type(exc).__name__}: {exc}")
            retryable = retry_on(exc)
            if not retryable or attempt >= policy.max_attempts:
                if on_attempt is not None:
                    on_attempt(attempt, exc, 0.0)
                raise
            delay = policy.delay_for(attempt, rng)
            if policy.respect_retry_after and isinstance(exc, RateLimitError) and exc.retry_after:
                delay = max(delay, float(exc.retry_after))
            if isinstance(exc, CircuitOpenError) and exc.retry_after:
                delay = max(delay, float(exc.retry_after))
            delay = min(delay, policy.max_delay)
            book.delays.append(delay)
            # Exactly one callback per failed attempt, carrying the real delay
            # (the baseline fired twice, duplicating every retry NOTE event).
            if on_attempt is not None:
                on_attempt(attempt, exc, delay)
            await sleep(delay)
        else:
            book.recovered = True
            if on_attempt is not None:
                on_attempt(attempt, None, 0.0)
            return result

    assert last_error is not None  # loop always returns or raises
    raise last_error


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitStats:
    opened_count: int = 0
    short_circuited: int = 0
    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    last_opened_at: float | None = None


class CircuitBreaker:
    """Fail fast after repeated failures, then probe for recovery.

    ``CLOSED`` -> normal.  ``OPEN`` after ``failure_threshold`` consecutive
    failures: calls raise :class:`CircuitOpenError` until ``reset_timeout`` has
    elapsed, at which point the breaker becomes ``HALF_OPEN`` and admits up to
    ``half_open_max_calls`` probes.  Any probe failure re-opens it; a success
    closes it.  Time comes from the injected clock, so tests do not sleep.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        clock: Clock | None = None,
        name: str = "default",
    ) -> None:
        if failure_threshold < 1:
            msg = "failure_threshold must be >= 1"
            raise ValueError(msg)
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls
        self.clock = clock or SystemClock()
        self.name = name
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at: float | None = None
        self._half_open_in_flight = 0
        # Reentrant: several public methods call each other while holding the
        # lock (before_call -> time_until_available, snapshot -> ...).  A plain
        # Lock deadlocks here.
        self._lock = threading.RLock()
        self.stats = CircuitStats()

    # ------------------------------------------------------------------ state
    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._opened_at is not None
            and self.clock.monotonic() - self._opened_at >= self.reset_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._half_open_in_flight = 0

    def time_until_available(self) -> float:
        with self._lock:
            self._maybe_half_open()
            if self._state is not CircuitState.OPEN or self._opened_at is None:
                return 0.0
            return max(self.reset_timeout - (self.clock.monotonic() - self._opened_at), 0.0)

    def before_call(self) -> None:
        """Raise :class:`CircuitOpenError` if this call must not proceed."""
        with self._lock:
            self._maybe_half_open()
            if self._state is CircuitState.CLOSED:
                return
            if self._state is CircuitState.OPEN:
                self.stats.short_circuited += 1
                remaining = (
                    max(self.reset_timeout - (self.clock.monotonic() - self._opened_at), 0.0)
                    if self._opened_at is not None
                    else self.reset_timeout
                )
                msg = f"circuit {self.name!r} is open"
                raise CircuitOpenError(msg, retry_after=remaining)
            if self._half_open_in_flight >= self.half_open_max_calls:
                self.stats.short_circuited += 1
                msg = f"circuit {self.name!r} half-open probe already in flight"
                raise CircuitOpenError(msg, retry_after=self.reset_timeout)
            self._half_open_in_flight += 1

    def on_success(self) -> None:
        with self._lock:
            self.stats.successes += 1
            self._failures = 0
            self.stats.consecutive_failures = 0
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_in_flight = max(self._half_open_in_flight - 1, 0)
            self._state = CircuitState.CLOSED
            self._opened_at = None

    def on_failure(self) -> None:
        with self._lock:
            self.stats.failures += 1
            self._failures += 1
            self.stats.consecutive_failures = self._failures
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_in_flight = max(self._half_open_in_flight - 1, 0)
                self._trip()
                return
            if self._failures >= self.failure_threshold:
                self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self.clock.monotonic()
        self.stats.opened_count += 1
        self.stats.last_opened_at = self._opened_at

    def reset(self) -> None:
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failures = 0
            self._opened_at = None
            self._half_open_in_flight = 0
            self.stats.consecutive_failures = 0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._maybe_half_open()
            return {
                "name": self.name,
                "state": self._state.value,
                "consecutive_failures": self._failures,
                "failure_threshold": self.failure_threshold,
                "opens": self.stats.opened_count,
                "short_circuited": self.stats.short_circuited,
                "time_until_available": self.time_until_available(),
            }


class TokenBucket:
    """Client-side rate limiter.

    Refills continuously at ``rate_per_s`` up to ``capacity``.  ``acquire``
    refuses to wait longer than ``max_wait`` and raises :class:`RateLimitError`
    instead — a guard against the infinite-wait bug you get when a rate limiter
    is driven by a clock that only advances on sleep.
    """

    def __init__(
        self,
        rate_per_s: float,
        capacity: float | None = None,
        *,
        clock: Clock | None = None,
        max_wait: float = 60.0,
    ) -> None:
        if rate_per_s <= 0:
            msg = "rate_per_s must be positive"
            raise ValueError(msg)
        self.rate = rate_per_s
        self.capacity = capacity if capacity is not None else max(rate_per_s, 1.0)
        self.clock = clock or SystemClock()
        self.max_wait = max_wait
        self._tokens = self.capacity
        self._last = self.clock.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self.clock.monotonic()
        elapsed = max(now - self._last, 0.0)
        self._last = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def time_until(self, tokens: float = 1.0) -> float:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            return (tokens - self._tokens) / self.rate

    async def acquire(self, tokens: float = 1.0, *, sleep: Sleeper = system_sleep) -> None:
        while True:
            wait = self.time_until(tokens)
            if wait <= 0.0:
                if self.try_acquire(tokens):
                    return
                continue
            if wait > self.max_wait:
                msg = f"rate limit wait of {wait:.1f}s exceeds max_wait={self.max_wait:.1f}s"
                raise RateLimitError(msg, retry_after=wait)
            await sleep(wait)

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def snapshot(self) -> dict[str, Any]:
        return {"rate_per_s": self.rate, "capacity": self.capacity, "available": round(self.available, 3)}


class AsyncLimiter:
    """Concurrency limiter that also honours an optional token bucket.

    Serialises the *permit* rather than the body so callers can hold a slot
    across an await without blocking the event loop.
    """

    def __init__(self, concurrency: int, bucket: TokenBucket | None = None) -> None:
        self._sem = asyncio.Semaphore(concurrency)
        self.bucket = bucket

    async def __aenter__(self) -> AsyncLimiter:
        await self._sem.acquire()
        if self.bucket is not None:
            try:
                await self.bucket.acquire()
            except BaseException:
                self._sem.release()
                raise
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._sem.release()
