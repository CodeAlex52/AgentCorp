"""Small shared primitives: time, ids, hashing.

Everything time-related in AgentCorp goes through a :class:`Clock` so that tests
and benchmarks can make time move without sleeping.  Nothing in the codebase is
allowed to call :func:`datetime.now` or :func:`time.monotonic` directly.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "Clock",
    "SystemClock",
    "FakeClock",
    "Sleeper",
    "system_sleep",
    "utcnow",
    "new_id",
    "short_hash",
    "stable_json",
]


def utcnow() -> datetime:
    """Timezone-aware wall clock. Used only for serialisation boundaries."""
    return datetime.now(UTC)


class Clock(Protocol):
    """Wall clock + monotonic clock, injectable."""

    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemClock:
    """:class:`Clock` backed by the operating system."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        import time

        return time.monotonic()


class FakeClock:
    """Deterministic clock for tests.

    ``advance`` moves both the wall clock and the monotonic clock forward, which
    is what retry/backoff/timeout logic observes.
    """

    def __init__(self, start: datetime | None = None, monotonic_start: float = 1_000_000.0) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=UTC)
        self._monotonic = monotonic_start

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            msg = "cannot advance a clock backwards"
            raise ValueError(msg)
        self._now = self._now + timedelta(seconds=seconds)
        self._monotonic += seconds


Sleeper = Callable[[float], Awaitable[None]]


async def system_sleep(seconds: float) -> None:
    """Default sleeper. Tests inject a recording/no-op sleeper instead."""
    await asyncio.sleep(seconds)


def new_id(prefix: str) -> str:
    """Short, prefixed, sortable-enough identifier (``T-3f2a1c9d``)."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def stable_json(value: Any) -> str:
    """Canonical JSON for hashing/dedupe (sorted keys, compact separators)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def short_hash(value: Any, length: int = 12) -> str:
    """Stable content hash used for prompt/response fingerprints."""
    return hashlib.sha256(stable_json(value).encode()).hexdigest()[:length]


def content_hash(text: str, length: int = 12) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:length]


@runtime_checkable
class Comparable(Protocol):  # pragma: no cover - structural helper
    def __lt__(self, other: Any) -> bool: ...
