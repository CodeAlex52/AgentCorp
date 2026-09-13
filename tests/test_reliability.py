"""C6 resilience primitives: retry policy, breaker, limiter, error taxonomy."""

from __future__ import annotations

import asyncio
import random

import pytest

from agentcorp import (
    CircuitBreaker,
    CircuitOpenError,
    ContextOverflowError,
    PermanentError,
    RateLimitError,
    RetryPolicy,
    SchemaError,
    TimeoutError_,
    TokenBucket,
    TransientError,
    call_with_retry,
    is_retryable,
)
from agentcorp.reliability import AsyncLimiter, CircuitState
from agentcorp.util import FakeClock


def test_nominal_delay_is_exponential_and_capped() -> None:
    policy = RetryPolicy(base_delay=1.0, multiplier=2.0, max_delay=5.0, jitter=0.0)
    assert [policy.nominal_delay(n) for n in (1, 2, 3, 4)] == [1.0, 2.0, 4.0, 5.0]


def test_delay_for_rejects_zero_attempt() -> None:
    with pytest.raises(ValueError):
        RetryPolicy().nominal_delay(0)


def test_jitter_bounds() -> None:
    policy = RetryPolicy(base_delay=4.0, jitter=1.0)
    rng = random.Random(0)
    samples = [policy.delay_for(1, rng) for _ in range(200)]
    assert all(0.0 <= delay <= 4.0 for delay in samples)
    assert len(set(samples)) > 50  # actually jittering
    assert RetryPolicy(base_delay=4.0, jitter=0.0).delay_for(1) == 4.0


async def test_call_with_retry_succeeds_after_transient_failures() -> None:
    attempts: list[int] = []
    delays: list[float] = []

    async def flaky(attempt: int) -> str:
        attempts.append(attempt)
        if attempt < 3:
            raise TransientError("boom")
        return "ok"

    async def sleeper(seconds: float) -> None:
        delays.append(seconds)

    result = await call_with_retry(
        flaky, RetryPolicy(max_attempts=3, base_delay=1.0, jitter=0.0), sleep=sleeper
    )
    assert result == "ok"
    assert attempts == [1, 2, 3]
    assert delays == [1.0, 2.0]


async def test_call_with_retry_stops_on_permanent_error() -> None:
    calls = 0

    async def f(attempt: int) -> None:
        nonlocal calls
        calls += 1
        raise PermanentError("nope")

    with pytest.raises(PermanentError):
        await call_with_retry(f, RetryPolicy(max_attempts=5), sleep=lambda s: asyncio.sleep(0))
    assert calls == 1


async def test_call_with_retry_exhausts_and_raises_last_error() -> None:
    calls = 0

    async def f(attempt: int) -> None:
        nonlocal calls
        calls += 1
        raise TransientError("always")

    with pytest.raises(TransientError):
        await call_with_retry(f, RetryPolicy(max_attempts=3), sleep=lambda s: asyncio.sleep(0))
    assert calls == 3


async def test_on_attempt_called_exactly_once_per_failed_attempt() -> None:
    seen: list[tuple[int, float]] = []

    async def f(attempt: int) -> None:
        raise RateLimitError("429", retry_after=0.0)

    def on_attempt(attempt: int, exc: BaseException | None, delay: float) -> None:
        seen.append((attempt, delay))

    with pytest.raises(RateLimitError):
        await call_with_retry(
            f,
            RetryPolicy(max_attempts=2, base_delay=1.0, jitter=0.0),
            sleep=lambda s: asyncio.sleep(0),
            on_attempt=on_attempt,
        )
    assert seen == [(1, 1.0), (2, 0.0)]


async def test_retry_after_is_respected() -> None:
    delays: list[float] = []

    async def f(attempt: int) -> None:
        raise RateLimitError("429", retry_after=7.5)

    async def sleeper(seconds: float) -> None:
        delays.append(seconds)

    with pytest.raises(RateLimitError):
        await call_with_retry(
            f, RetryPolicy(max_attempts=2, base_delay=0.1, jitter=0.0), sleep=sleeper
        )
    assert delays == [7.5]


def test_breaker_opens_after_threshold_and_recovers() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, reset_timeout=10.0, clock=clock)
    assert breaker.state is CircuitState.CLOSED
    for _ in range(2):
        breaker.before_call()
        breaker.on_failure()
    assert breaker.state is CircuitState.CLOSED
    breaker.before_call()
    breaker.on_failure()
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError) as excinfo:
        breaker.before_call()
    assert excinfo.value.retry_after == pytest.approx(10.0)
    clock.advance(10.0)
    assert breaker.state is CircuitState.HALF_OPEN
    breaker.before_call()
    breaker.on_success()
    assert breaker.state is CircuitState.CLOSED


def test_breaker_half_open_limits_probes() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=1.0, half_open_max_calls=1, clock=clock)
    breaker.before_call()
    breaker.on_failure()
    clock.advance(1.0)
    breaker.before_call()  # the single probe
    with pytest.raises(CircuitOpenError):
        breaker.before_call()
    breaker.on_failure()  # probe failed → reopen
    assert breaker.state is CircuitState.OPEN


def test_breaker_failure_threshold_must_be_positive() -> None:
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)


def test_breaker_snapshot_shape() -> None:
    breaker = CircuitBreaker(name="unit")
    snapshot = breaker.snapshot()
    assert snapshot["state"] == "closed" and snapshot["name"] == "unit"
    assert "opens" in snapshot and "time_until_available" in snapshot


def test_token_bucket_refills_with_injected_clock() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate_per_s=2.0, capacity=2.0, clock=clock)
    assert bucket.try_acquire()
    assert bucket.try_acquire()
    assert not bucket.try_acquire()
    clock.advance(1.0)
    assert bucket.available == pytest.approx(2.0)
    assert bucket.try_acquire()
    assert bucket.snapshot()["rate_per_s"] == 2.0


async def test_token_bucket_acquire_waits_then_succeeds() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate_per_s=10.0, capacity=1.0, clock=clock)
    assert bucket.try_acquire()

    async def sleeper(seconds: float) -> None:
        clock.advance(seconds)

    await asyncio.wait_for(bucket.acquire(sleep=sleeper), timeout=1.0)
    assert clock.monotonic() > 1_000_000.0


async def test_token_bucket_refuses_absurd_wait() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate_per_s=0.001, capacity=1.0, clock=clock, max_wait=1.0)
    bucket.try_acquire()
    with pytest.raises(RateLimitError):
        await bucket.acquire(sleep=lambda s: asyncio.sleep(0))


async def test_async_limiter_caps_concurrency() -> None:
    active = 0
    peak = 0
    limiter = AsyncLimiter(concurrency=2)

    async def worker() -> None:
        nonlocal active, peak
        async with limiter:
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1

    await asyncio.gather(*(worker() for _ in range(8)))
    assert peak <= 2


def test_error_taxonomy_retryability() -> None:
    assert is_retryable(TransientError("x"))
    assert is_retryable(RateLimitError("x"))
    assert is_retryable(TimeoutError_("x"))
    assert is_retryable(SchemaError("x"))
    assert is_retryable(TimeoutError())
    assert is_retryable(ConnectionError())
    assert not is_retryable(PermanentError("x"))
    assert not is_retryable(ContextOverflowError("x"))
    assert not is_retryable(ValueError("x"))
