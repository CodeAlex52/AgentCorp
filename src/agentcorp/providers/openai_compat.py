"""OpenAI-compatible HTTP provider.

One implementation covers OpenAI, Azure OpenAI, DeepSeek, OpenRouter, Groq,
Together, Fireworks, vLLM, llama.cpp's server and Ollama's OpenAI shim — they
all speak ``POST {base_url}/chat/completions``.  The interesting part is the
error translation: the runtime's retry policy only works if HTTP failures are
mapped onto AgentCorp's error taxonomy correctly, in particular 429 →
:class:`RateLimitError` with ``retry_after`` preserved.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from ..errors import (
    ContextOverflowError,
    PermanentError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TimeoutError_,
)
from ..models import Usage
from .base import AgentProvider, CompletionRequest, CompletionResponse, Message

__all__ = ["OpenAICompatProvider", "DEFAULT_BASE_URL", "PRESETS"]

DEFAULT_BASE_URL = "https://api.openai.com/v1"

#: name -> (base_url, env var for the key, model, price per Mtok in/out)
PRESETS: dict[str, tuple[str, str, str, tuple[float, float]]] = {
    "openai": ("https://api.openai.com/v1", "OPENAI_API_KEY", "gpt-4o-mini", (0.15, 0.60)),
    "deepseek": ("https://api.deepseek.com/v1", "DEEPSEEK_API_KEY", "deepseek-chat", (0.27, 1.10)),
    "openrouter": ("https://openrouter.ai/api/v1", "OPENROUTER_API_KEY", "openai/gpt-4o-mini", (0.15, 0.60)),
    "groq": ("https://api.groq.com/openai/v1", "GROQ_API_KEY", "llama-3.3-70b-versatile", (0.59, 0.79)),
    "ollama": ("http://localhost:11434/v1", "OLLAMA_API_KEY", "qwen2.5-coder", (0.0, 0.0)),
    "vllm": ("http://localhost:8000/v1", "VLLM_API_KEY", "local-model", (0.0, 0.0)),
}


class OpenAICompatProvider(AgentProvider):
    name = "openai_compat"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        api_key: str | None = None,
        model: str = "gpt-4o-mini",
        preset: str | None = None,
        timeout: float = 120.0,
        max_retries: int = 0,
        pricing: tuple[float, float] | None = None,
        extra_headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if preset:
            base_url, env_key, preset_model, preset_pricing = PRESETS[preset]
            api_key = api_key or os.environ.get(env_key)
            model = model or preset_model
            pricing = pricing or preset_pricing
            self.name = preset
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or os.environ.get("AGENTCORP_API_KEY")
        self.default_model = model
        self.pricing = pricing or (0.0, 0.0)
        self.timeout = timeout
        self.extra_headers = extra_headers or {}
        self._owns_client = client is None
        # max_retries=0: AgentCorp owns retries. Double-retrying (httpx inside
        # our retry loop) multiplies call volume and breaks budget accounting.
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout), follow_redirects=True
        )
        self._max_retries = max_retries

    # ------------------------------------------------------------------ public
    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        if not self.api_key and "localhost" not in self.base_url and "127.0.0.1" not in self.base_url:
            msg = (
                f"no API key for provider {self.name!r}: set the matching environment variable "
                f"or pass api_key="
            )
            raise PermanentError(msg)

        payload: dict[str, Any] = {
            "model": request.model or self.default_model,
            "messages": [
                {"role": m.role, "content": m.content} for m in self.coalesce_messages(request.messages)
            ],
        }
        if request.temperature is not None:
            payload["temperature"] = request.temperature
        if request.max_tokens:
            payload["max_tokens"] = request.max_tokens

        headers = {"Content-Type": "application/json", **self.extra_headers}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        try:
            response = await self._client.post(
                f"{self.base_url}/chat/completions", json=payload, headers=headers
            )
        except httpx.TimeoutException as exc:
            raise TimeoutError_(f"{self.name}: request timed out after {self.timeout}s") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(f"{self.name}: transport error: {exc}") from exc

        self._raise_for_status(response)
        return self._parse(response, request)

    # ----------------------------------------------------------------- parsing
    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        body = response.text[:800]
        status = response.status_code
        if status == 429:
            retry_after = _parse_retry_after(response)
            raise RateLimitError(f"{self.name}: 429 rate limited: {body}", retry_after=retry_after)
        if status in {408, 504, 598, 599}:
            raise TimeoutError_(f"{self.name}: {status} timeout: {body}")
        if status in {400, 422} and _looks_like_context_overflow(body):
            raise ContextOverflowError(f"{self.name}: context length exceeded: {body}")
        if 500 <= status < 600:
            raise ProviderError(f"{self.name}: {status} server error: {body}")
        raise PermanentError(f"{self.name}: {status} {response.reason_phrase}: {body}")

    def _parse(self, response: httpx.Response, request: CompletionRequest) -> CompletionResponse:
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise SchemaError(f"{self.name}: response was not JSON: {response.text[:200]}") from exc

        choices = data.get("choices") or []
        if not choices:
            raise SchemaError(f"{self.name}: no choices in response: {str(data)[:200]}")
        message = choices[0].get("message") or {}
        text = message.get("content") or ""
        if not isinstance(text, str) or not text.strip():
            raise SchemaError(f"{self.name}: empty message content")
        if message.get("reasoning_content") and not text.strip():
            text = str(message["reasoning_content"])

        usage_raw = data.get("usage") or {}
        tokens_in = int(usage_raw.get("prompt_tokens") or 0)
        tokens_out = int(usage_raw.get("completion_tokens") or 0)
        if tokens_in == 0 and tokens_out == 0:
            from .base import estimate_tokens

            tokens_in = estimate_tokens(request.text())
            tokens_out = estimate_tokens(text)

        return CompletionResponse(
            text=text,
            model=str(data.get("model") or request.model or self.default_model),
            provider=self.name,
            usage=Usage(
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                calls=1,
                cost_usd=self.cost_of(tokens_in, tokens_out),
            ),
            finish_reason=str(choices[0].get("finish_reason") or "stop"),
            raw={"id": data.get("id"), "usage": usage_raw},
        )


def _parse_retry_after(response: httpx.Response) -> float | None:
    """``Retry-After`` is either seconds or an HTTP date; handle both."""
    raw = response.headers.get("retry-after") or response.headers.get("x-ratelimit-reset-requests")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        from datetime import UTC, datetime

        when = parsedate_to_datetime(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        return max((when - datetime.now(UTC)).total_seconds(), 0.0)
    except (TypeError, ValueError):
        return None


def _looks_like_context_overflow(body: str) -> bool:
    markers = (
        "context length",
        "context_length_exceeded",
        "maximum context",
        "too many tokens",
        "reduce the length",
        "prompt is too long",
    )
    lowered = body.lower()
    return any(marker in lowered for marker in markers)


def messages_from_prompt(system: str, user: str) -> list[Message]:
    return [Message(role="system", content=system), Message(role="user", content=user)]
