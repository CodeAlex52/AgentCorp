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

__all__ = ["BudgetManager", "BudgetStatus"]


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
    ) -> None:
        self.limits = limits or BudgetLimits()
        self._emit = emit
        self.project = _Ledger(seed or Usage())
        self.by_task: dict[str, _Ledger] = {k: _Ledger(v) for k, v in (seed_by_task or {}).items()}
        self._emit_every = max(emit_every, 1)
        self._records_since_emit = 0
        self.refusals: list[BudgetStatus] = []

    # ------------------------------------------------------------- accounting
    def record(self, usage: Usage, *, task_id: str | None = None) -> None:
        """Book an agent call's usage against the project and its task."""
        self.project.add(usage)
        if task_id is not None:
            self.by_task.setdefault(task_id, _Ledger()).add(usage)
        self._records_since_emit += 1
        if self._emit is not None and self._records_since_emit >= self._emit_every:
            self._records_since_emit = 0
            self._emit(
                EventType.BUDGET_UPDATED,
                task_id,
                {"budget": self.snapshot(task_id).model_dump(mode="json")},
            )

    # ------------------------------------------------------------ enforcement
    def status(self, *, task_id: str | None = None, estimated_tokens: int = 0) -> BudgetStatus:
        """Would a call costing ``estimated_tokens`` be allowed right now?"""
        lim = self.limits
        used_tokens = self.project.usage.total_tokens
        used_cost = self.project.usage.cost_usd
        used_calls = self.project.usage.calls

        if lim.max_agent_calls is not None and used_calls >= lim.max_agent_calls:
            return BudgetStatus(
                False,
                f"project agent-call ceiling: {used_calls}/{lim.max_agent_calls} calls used",
                "max_agent_calls",
                "project",
            )
        if lim.max_tokens is not None and used_tokens + estimated_tokens > lim.max_tokens:
            return BudgetStatus(
                False,
                f"project token ceiling: used {used_tokens} + est {estimated_tokens} > {lim.max_tokens}",
                "max_tokens",
                "project",
            )
        if lim.max_cost_usd is not None and used_cost >= lim.max_cost_usd:
            return BudgetStatus(
                False,
                f"project cost ceiling: used ${used_cost:.4f} >= ${lim.max_cost_usd:.4f}",
                "max_cost_usd",
                "project",
            )
        if task_id is not None and lim.max_tokens_per_task is not None:
            task_used = self.by_task.get(task_id, _Ledger()).usage.total_tokens
            if task_used + estimated_tokens > lim.max_tokens_per_task:
                return BudgetStatus(
                    False,
                    f"task token ceiling: used {task_used} + est {estimated_tokens} > {lim.max_tokens_per_task}",
                    "max_tokens_per_task",
                    "task",
                )
        return BudgetStatus(
            True,
            remaining_tokens=self.remaining_tokens(estimated_tokens),
            remaining_cost_usd=(
                None if lim.max_cost_usd is None else max(lim.max_cost_usd - self.project.usage.cost_usd, 0.0)
            ),
            remaining_calls=(
                None if lim.max_agent_calls is None else max(lim.max_agent_calls - self.project.usage.calls, 0)
            ),
        )

    def check(self, *, task_id: str | None = None, estimated_tokens: int = 0) -> None:
        """Raise :class:`BudgetExceededError` if the next call must not happen."""
        status = self.status(task_id=task_id, estimated_tokens=estimated_tokens)
        if status.allowed:
            return
        self.refusals.append(status)
        if self._emit is not None:
            self._emit(
                EventType.BUDGET_EXCEEDED,
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
        return max(self.limits.max_tokens - self.project.usage.total_tokens - estimated, 0)

    @property
    def exhausted(self) -> bool:
        return not self.status().allowed

    # -------------------------------------------------------------- reporting
    def snapshot(self, task_id: str | None = None) -> BudgetSnapshot:
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
        return max(ratios) if ratios else 0.0

    def summary(self) -> str:
        lim, used = self.limits, self.project.usage
        parts = [f"tokens {used.total_tokens}/{lim.max_tokens if lim.max_tokens else '∞'}"]
        parts.append(f"cost ${used.cost_usd:.4f}/{f'${lim.max_cost_usd:.4f}' if lim.max_cost_usd else '∞'}")
        parts.append(f"calls {used.calls}/{lim.max_agent_calls if lim.max_agent_calls else '∞'}")
        return " | ".join(parts)
