"""Error taxonomy.

The orchestrator's recovery behaviour is driven entirely by *which class* an
error is.  Adding a new failure mode means adding a class here and declaring
whether it is retryable, not sprinkling ``except Exception`` through the engine.
"""

from __future__ import annotations

__all__ = [
    "AgentCorpError",
    "TransientError",
    "RateLimitError",
    "TimeoutError_",
    "ProviderError",
    "ContextOverflowError",
    "SchemaError",
    "PermanentError",
    "BudgetExceededError",
    "CircuitOpenError",
    "DecompositionError",
    "GraphError",
    "StateError",
    "is_retryable",
]


class AgentCorpError(Exception):
    """Base class for every error AgentCorp raises deliberately."""

    retryable: bool = False


class TransientError(AgentCorpError):
    """Something that is expected to succeed if we simply try again."""

    retryable = True


class RateLimitError(TransientError):
    """Provider told us to slow down (HTTP 429 / quota)."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class TimeoutError_(TransientError):
    """The agent call exceeded its deadline.

    Named with a trailing underscore to avoid shadowing the builtin, which we
    still catch separately in the transport layer.
    """


class ProviderError(TransientError):
    """5xx / connection reset / DNS blip from the provider transport."""


class ContextOverflowError(AgentCorpError):
    """Prompt exceeded the model's context window.

    Not retryable *as-is*: the runtime must shrink the context first, then retry.
    """

    retryable = False

    def __init__(self, message: str, tokens: int | None = None) -> None:
        super().__init__(message)
        self.tokens = tokens


class PermanentError(AgentCorpError):
    """Retrying will not help: bad credentials, unknown model, malformed task."""

    retryable = False


class SchemaError(AgentCorpError):
    """The model replied, but not in the contract we asked for (invalid JSON).

    Retryable: a fresh sample usually fixes it, and the worker's second attempt
    adds a stricter repair instruction to the prompt.
    """

    retryable = True


class BudgetExceededError(AgentCorpError):
    """A budget ceiling (tokens / cost / calls) was hit."""

    retryable = False

    def __init__(self, message: str, scope: str = "project", limit: str = "max_cost") -> None:
        super().__init__(message)
        self.scope = scope
        self.limit = limit


class CircuitOpenError(TransientError):
    """We are failing fast because the circuit breaker is open."""

    def __init__(self, message: str, retry_after: float = 0.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class DecompositionError(AgentCorpError):
    """A task could not be split any further but is still not executable."""


class GraphError(AgentCorpError):
    """Cycle, dangling dependency, or illegal status transition in the DAG."""


class StateError(AgentCorpError):
    """Persisted state is inconsistent with the event log."""


def is_retryable(error: BaseException) -> bool:
    """Single source of truth for 'should the retry loop try again?'."""
    if isinstance(error, AgentCorpError):
        return error.retryable
    return isinstance(error, (TimeoutError, ConnectionError, OSError))
