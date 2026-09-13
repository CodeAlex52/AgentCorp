"""The ``AgentProvider`` interface.

A provider is a *dumb transport*: it takes messages, returns text and token
counts.  It knows nothing about tasks, retries, budgets or contracts — those
belong to :mod:`agentcorp.runtime`.  Keeping the boundary here is what lets the
entire orchestration be tested against :class:`~agentcorp.providers.mock.MockProvider`
with no network, and lets a new vendor be added in ~40 lines.
"""

from __future__ import annotations

import abc
from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..models import Usage

__all__ = ["Message", "CompletionRequest", "CompletionResponse", "AgentProvider", "estimate_tokens"]

Role = Literal["system", "user", "assistant"]


def estimate_tokens(text: str) -> int:
    """Cheap token estimate (~4 chars/token).

    Only used when a provider does not report usage, and for pre-flight budget
    estimates.  Being approximate is fine here; being *absent* is not, because
    the budget manager needs a number before the call.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


class Message(BaseModel):
    model_config = ConfigDict(frozen=True)

    role: Role
    content: str


class CompletionRequest(BaseModel):
    """One provider call."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    messages: list[Message]
    model: str | None = None
    temperature: float = 0.0
    max_tokens: int | None = None
    timeout: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    #: Task/role context, passed through for tracing and for the mock's routing.
    role: str = "worker"
    task_id: str | None = None

    def text(self) -> str:
        """Full prompt text — used for hashing, logging and token estimation."""
        return "\n\n".join(f"[{m.role}]\n{m.content}" for m in self.messages)

    @property
    def system(self) -> str:
        return "\n".join(m.content for m in self.messages if m.role == "system")

    @property
    def prompt_chars(self) -> int:
        return sum(len(m.content) for m in self.messages)


class CompletionResponse(BaseModel):
    text: str
    model: str = "unknown"
    provider: str = "unknown"
    usage: Usage = Field(default_factory=Usage)
    finish_reason: str = "stop"
    raw: dict[str, Any] = Field(default_factory=dict)


class AgentProvider(abc.ABC):
    """Transport interface. Implementations must be safe to call concurrently."""

    name: str = "abstract"
    default_model: str = "unknown"
    #: USD per 1M tokens, ``(input, output)``.  ``(0, 0)`` means "free/unknown".
    pricing: tuple[float, float] = (0.0, 0.0)

    @abc.abstractmethod
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Perform one completion.

        Must raise an error from :mod:`agentcorp.errors` for anything the
        runtime could plausibly recover from (rate limit, timeout, 5xx,
        context overflow, malformed reply) so that retry policy stays
        provider-agnostic.
        """

    async def aclose(self) -> None:
        """Release transport resources. Overridden by HTTP providers."""
        return

    def cost_of(self, tokens_in: int, tokens_out: int) -> float:
        pin, pout = self.pricing
        return (tokens_in * pin + tokens_out * pout) / 1_000_000.0

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "model": self.default_model,
            "pricing_per_mtok": {"input": self.pricing[0], "output": self.pricing[1]},
        }

    @staticmethod
    def coalesce_messages(messages: Sequence[Message]) -> list[Message]:
        """Merge consecutive same-role messages (some APIs reject them)."""
        out: list[Message] = []
        for message in messages:
            if out and out[-1].role == message.role:
                out[-1] = Message(role=message.role, content=out[-1].content + "\n\n" + message.content)
            else:
                out.append(message)
        return out
