"""Budget accounting.

Three ceilings are enforced (tokens, USD, agent calls) at two scopes (project,
task).  The manager is deliberately *pessimistic*: it refuses a call that
*could* cross a ceiling given the pre-flight estimate rather than discovering
the overshoot afterwards, because an agent call is not cancellable once the
provider has been paid for it.

Ceilings are not warnings.  Exceeding one raises
:class:`~agentcorp.errors.BudgetExceededError`, which the scheduler catches and
converts into a supervisor escalation — the project pauses instead of quietly
burning the budget.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import BudgetExceededError
from .events import EventEmitter, EventType
from .models import BudgetLimits, BudgetSnapshot, Usage
from .util import Clock, SystemClock

__all__ = ["BudgetManager", "BudgetStatus"]

#: Emit a BUDGET_WARNING once this fraction of the tightest ceiling is consumed.
WARN_PRESSURE = 0.8


@dataclass
class BudgetStatus:
    """Answer to 'how much room is left, and did we just get refused?'"""

    allowed: bool
    reason: str = ""
    limit: str = ""
    scope: str = "project"
    remaining_tokens: int | None = None
    remaining_cost_usd: float | None = None
    remaining_calls: int | None = None
    remaining_tasks: int | None = None
    remaining_seconds: float | None = None


@dataclass
class _Ledger:
    usage: Usage = field(default_factory=Usage)

    def add(self, other: Usage) -> None:
        self.usage.add_in_place(other)


class BudgetManager:
    """Tracks and enforces resource ceilings for one project.

    ``seed``/``seed_by_task`` exist for ``resume``: after a restart the manager
    is rehydrated from the projection instead of starting from zero, otherwise
    a crash loop would silently reset the budget.
    """

    def __init__(
        self,
        limits: BudgetLimits | None = None,
        *,
        emit: EventEmitter | None = None,
        seed: Usage | None = None,
        seed_by_task: dict[str, Usage] | None = None,
        emit_every: int = 1,
        clock: Clock | None = None,
        started_monotonic: float | None = None,
        tasks_started: int = 0,
    ) -> None:
        self.limits = limits or BudgetLimits()
        self._emit = emit
        self.clock = clock or SystemClock()
        self.project = _Ledger(seed or Usage())
        self.by_task: dict[str, _Ledger] = {k: _Ledger(v) for k, v in (seed_by_task or {}).items()}
        self._emit_every = max(emit_every, 1)
        self._records_since_emit = 0
        self.refusals: list[BudgetStatus] = []
        #: Wall-clock ceiling is measured from here (monotonic seconds).
        self.started_monotonic = (
            started_monotonic if started_monotonic is not None else self.clock.monotonic()
        )
        #: Dispatches, not calls: `max_tasks` counts distinct tasks that were
        #: claimed or accounted for.  Derived automatically from `record()` so a
        #: caller that forgets `note_task_started()` cannot silently disable the
        #: ceiling (FIND-006).
        self.tasks_started = tasks_started
        self._seen_tasks: set[str] = set()
        #: Tokens promised to calls that are in flight but have not settled yet.
        #: Admission control must see them or concurrent calls overshoot (C7).
        self._reserved_tokens = 0
        self._warned = False

    # ------------------------------------------------------------------- clock
    def elapsed_seconds(self) -> float:
        return max(self.clock.monotonic() - self.started_monotonic, 0.0)

    # --------------------------------------------------------------- reservation
    def reserve(self, estimated_tokens: int) -> int:
        """Reserve tokens for an admitted call; returns the reservation id/amount.

        The reservation is released by :meth:`release` when the call settles
        (success or failure), at which point the *actual* usage is booked by
        :meth:`record`.  Reservations make the token ceiling a hard stop under
        concurrency: two calls can no longer both pass a check that only sees
        already-booked usage.
        """
        amount = max(int(estimated_tokens), 0)
        self._reserved_tokens += amount
        return amount

    def release(self, reservation: int) -> None:
        self._reserved_tokens = max(self._reserved_tokens - max(int(reservation), 0), 0)

    @property
    def reserved_tokens(self) -> int:
        return self._reserved_tokens

    def note_task_started(self, count: int = 1, *, task_id: str | None = None) -> None:
        """Count a dispatch.  Idempotent per task id when one is supplied."""
        if task_id is not None:
            if task_id in self._seen_tasks:
                return
            self._seen_tasks.add(task_id)
        self.tasks_started += count

    def _would_consume_task_slot(self, task_id: str | None) -> bool:
        """Fail-closed: an unspecified task counts as a fresh dispatch."""
        if task_id is None:
            return True
        return task_id not in self._seen_tasks

    # ------------------------------------------------------------- accounting
    def record(self, usage: Usage, *, task_id: str | None = None) -> None:
        """Book an agent call's usage against the project and its task."""
        self.project.add(usage)
        if task_id is not None:
            self.by_task.setdefault(task_id, _Ledger()).add(usage)
            if task_id not in self._seen_tasks:
                # Auto-derive the task count from accounting: even a caller that
                # never calls note_task_started() gets a fail-closed ceiling.
                self._seen_tasks.add(task_id)
                self.tasks_started += 1
        self._records_since_emit += 1
        if self._emit is not None and self._records_since_emit >= self._emit_every:
            self._records_since_emit = 0
            self._emit(
                EventType.BUDGET_UPDATED,
                task_id,
                {"budget": self.snapshot(task_id).model_dump(mode="json")},
            )
        if self._emit is not None and not self._warned and self.pressure() >= WARN_PRESSURE:
            self._warned = True
            self._emit(
                EventType.BUDGET_WARNING,
                task_id,
                {
                    "reason": f"budget pressure at {self.pressure():.0%}",
                    "budget": self.snapshot(task_id).model_dump(mode="json"),
                },
            )

    # ------------------------------------------------------------ enforcement
    def reseed(
        self,
        *,
        usage: Usage | None = None,
        by_task: dict[str, Usage] | None = None,
        started_monotonic: float | None = None,
        tasks_started: int | None = None,
    ) -> None:
        """Rehydrate from the projection after a restart (C5).

        A crash loop must not silently reset the budget, so ``resume`` seeds the
        manager with the usage already recorded in the store.
        """
        if usage is not None:
            self.project.usage = usage.model_copy()
        if by_task is not None:
            self.by_task = {k: _Ledger(v.model_copy()) for k, v in by_task.items()}
        if started_monotonic is not None:
            self.started_monotonic = started_monotonic
        if tasks_started is not None:
            self.tasks_started = tasks_started
        self.refusals.clear()
        self._warned = False
        self._seen_tasks.clear()
        self._reserved_tokens = 0

    def status(
        self,
        *,
        task_id: str | None = None,
        estimated_tokens: int = 0,
        new_task: bool | None = None,
    ) -> BudgetStatus:
        """Would a call costing ``estimated_tokens`` be allowed right now?

        The ``max_tasks`` ceiling is evaluated for any call whose ``task_id`` has
        not consumed a slot yet — including calls that omit ``task_id`` — so the
        default check path is fail-closed (SPEC §5.3, FIND-006).  Pass
        ``new_task=False`` to explicitly exempt an in-flight call.
        """
        lim = self.limits
        used = self.project.usage
        used_tokens = used.total_tokens
        used_cost = used.cost_usd
        used_calls = used.calls
        remaining_tokens = self.remaining_tokens(estimated_tokens)
        remaining_cost = (
            None if lim.max_cost_usd is None else max(lim.max_cost_usd - used_cost, 0.0)
        )
        remaining_calls = (
            None if lim.max_agent_calls is None else max(lim.max_agent_calls - used_calls, 0)
        )
        remaining_tasks = (
            None if lim.max_tasks is None else max(lim.max_tasks - self.tasks_started, 0)
        )
        remaining_seconds = (
            None
            if lim.max_wall_seconds is None
            else max(lim.max_wall_seconds - self.elapsed_seconds(), 0.0)
        )

        def refuse(reason: str, limit: str, scope: str = "project") -> BudgetStatus:
            return BudgetStatus(
                False,
                reason,
                limit,
                scope,
                remaining_tokens=remaining_tokens,
                remaining_cost_usd=remaining_cost,
                remaining_calls=remaining_calls,
                remaining_tasks=remaining_tasks,
                remaining_seconds=remaining_seconds,
            )

        if lim.max_agent_calls is not None and used_calls >= lim.max_agent_calls:
            return refuse(
                f"project agent-call ceiling: {used_calls}/{lim.max_agent_calls} calls used",
                "max_agent_calls",
            )
        consumes_slot = self._would_consume_task_slot(task_id) if new_task is None else new_task
        if lim.max_tasks is not None and consumes_slot and self.tasks_started >= lim.max_tasks:
            return refuse(
                f"project task ceiling: {self.tasks_started}/{lim.max_tasks} tasks dispatched",
                "max_tasks",
            )
        if lim.max_tokens is not None and (
            used_tokens >= lim.max_tokens
            or used_tokens + self._reserved_tokens + estimated_tokens > lim.max_tokens
        ):
            return refuse(
                f"project token ceiling: used {used_tokens} + reserved {self._reserved_tokens}"
                f" + est {estimated_tokens} > {lim.max_tokens}",
                "max_tokens",
            )
        if lim.max_cost_usd is not None and used_cost >= lim.max_cost_usd:
            return refuse(
                f"project cost ceiling: used ${used_cost:.4f} >= ${lim.max_cost_usd:.4f}",
                "max_cost_usd",
            )
        if lim.max_wall_seconds is not None and self.elapsed_seconds() >= lim.max_wall_seconds:
            return refuse(
                f"project wall-clock ceiling: {self.elapsed_seconds():.1f}s >= {lim.max_wall_seconds:.1f}s",
                "max_wall_seconds",
            )
        if task_id is not None and lim.max_tokens_per_task is not None:
            task_used = self.by_task.get(task_id, _Ledger()).usage.total_tokens
            if task_used + estimated_tokens > lim.max_tokens_per_task:
                return refuse(
                    f"task token ceiling: used {task_used} + est {estimated_tokens} > {lim.max_tokens_per_task}",
                    "max_tokens_per_task",
                    "task",
                )
        return BudgetStatus(
            True,
            remaining_tokens=remaining_tokens,
            remaining_cost_usd=remaining_cost,
            remaining_calls=remaining_calls,
            remaining_tasks=remaining_tasks,
            remaining_seconds=remaining_seconds,
        )

    def check(
        self,
        *,
        task_id: str | None = None,
        estimated_tokens: int = 0,
        new_task: bool | None = None,
    ) -> None:
        """Raise :class:`BudgetExceededError` if the next call must not happen."""
        status = self.status(task_id=task_id, estimated_tokens=estimated_tokens, new_task=new_task)
        if status.allowed:
            return
        self.refusals.append(status)
        if self._emit is not None:
            self._emit(
                EventType.BUDGET_EXHAUSTED,
                task_id,
                {
                    "reason": status.reason,
                    "limit": status.limit,
                    "scope": status.scope,
                    "budget": self.snapshot(task_id).model_dump(mode="json"),
                },
            )
        raise BudgetExceededError(status.reason, scope=status.scope, limit=status.limit)

    def remaining_tokens(self, estimated: int = 0) -> int | None:
        if self.limits.max_tokens is None:
            return None
        return max(
            self.limits.max_tokens
            - self.project.usage.total_tokens
            - self._reserved_tokens
            - estimated,
            0,
        )

    @property
    def exhausted(self) -> bool:
        return not self.status().allowed

    # -------------------------------------------------------------- reporting
    def snapshot(self, task_id: str | None = None) -> BudgetSnapshot:  # noqa: ARG002 - per-task view is a future extension; signature kept for events
        return BudgetSnapshot(limits=self.limits, used=self.project.usage)

    def task_usage(self, task_id: str) -> Usage:
        ledger = self.by_task.get(task_id)
        return ledger.usage if ledger else Usage()

    def pressure(self) -> float:
        """Fraction of the tightest ceiling consumed, 0..1. Used by supervisor."""
        ratios: list[float] = []
        lim, used = self.limits, self.project.usage
        if lim.max_tokens:
            ratios.append(used.total_tokens / lim.max_tokens)
        if lim.max_cost_usd:
            ratios.append(used.cost_usd / lim.max_cost_usd)
        if lim.max_agent_calls:
            ratios.append(used.calls / lim.max_agent_calls)
        if lim.max_tasks:
            ratios.append(self.tasks_started / lim.max_tasks)
        if lim.max_wall_seconds:
            ratios.append(self.elapsed_seconds() / lim.max_wall_seconds)
        return max(ratios) if ratios else 0.0

    def summary(self) -> str:
        lim, used = self.limits, self.project.usage
        parts = [f"tokens {used.total_tokens}/{lim.max_tokens if lim.max_tokens else '∞'}"]
        parts.append(f"cost ${used.cost_usd:.4f}/{f'${lim.max_cost_usd:.4f}' if lim.max_cost_usd else '∞'}")
        parts.append(f"calls {used.calls}/{lim.max_agent_calls if lim.max_agent_calls else '∞'}")
        return " | ".join(parts)
