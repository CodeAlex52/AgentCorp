"""Provider construction from a human-readable spec string.

``agentcorp run --provider openai`` (or ``deepseek``, ``ollama``,
``mock:skill=0.7``, ``cli:claude -p``) resolves through here.  Keeping
construction in one place means the CLI, the benchmark and the tests all get
identical provider semantics.
"""

from __future__ import annotations

import os
from typing import Any

from ..errors import PermanentError
from .base import AgentProvider
from .cli_provider import CLIProvider
from .mock import MockProvider
from .openai_compat import PRESETS, OpenAICompatProvider

__all__ = ["build_provider", "list_providers", "known_specs"]

_LOCAL_PREFIXES = ("localhost", "127.0.0.1", "0.0.0.0")


def known_specs() -> list[str]:
    return ["mock", *PRESETS.keys(), "cli"]


def list_providers() -> dict[str, dict[str, Any]]:
    """Capability report for ``agentcorp doctor``."""
    out: dict[str, dict[str, Any]] = {
        "mock": {"kind": "offline", "needs_key": False, "model": "mock-agent-v1"},
    }
    for name, (base_url, env_key, model, pricing) in PRESETS.items():
        out[name] = {
            "kind": "http",
            "base_url": base_url,
            "needs_key": True,
            "env": env_key,
            "key_present": bool(os.environ.get(env_key)),
            "model": model,
            "pricing_per_mtok": {"input": pricing[0], "output": pricing[1]},
        }
    out["cli"] = {"kind": "subprocess", "needs_key": False, "usage": "cli:<command>"}
    return out


def build_provider(spec: str | None = None, **overrides: Any) -> AgentProvider:
    """Build a provider from ``spec``.

    Supported forms::

        mock                     deterministic offline agent
        openai                   OpenAI (needs OPENAI_API_KEY)
        deepseek | openrouter | groq | ollama | vllm
        cli:claude -p            any CLI agent; prompt on stdin, reply on stdout
    """
    spec = (spec or os.environ.get("AGENTCORP_PROVIDER") or "mock").strip()

    if spec.startswith("cli:"):
        command = spec[4:].strip()
        if not command:
            msg = "cli provider requires a command, e.g. --provider 'cli:claude -p'"
            raise PermanentError(msg)
        return CLIProvider(command, **overrides)

    if spec.startswith("mock"):
        params = _parse_params(spec)
        params.update(overrides)
        return MockProvider(**params)

    if spec in PRESETS:
        base_url, env_key, model, pricing = PRESETS[spec]
        http_params: dict[str, Any] = {
            "preset": spec,
            "base_url": base_url,
            "model": model,
            "pricing": pricing,
        }
        api_key = os.environ.get(env_key)
        if api_key is None and not any(p in base_url for p in _LOCAL_PREFIXES):
            msg = (
                f"provider {spec!r} needs {env_key} in the environment "
                f"(or use --provider mock for an offline run)"
            )
            raise PermanentError(msg)
        if api_key:
            http_params["api_key"] = api_key
        http_params.update(overrides)
        return OpenAICompatProvider(**http_params)

    if spec.startswith(("http://", "https://")):
        url_params: dict[str, Any] = {"base_url": spec.rstrip("/"), "model": "local-model"}
        url_params.update(overrides)
        return OpenAICompatProvider(**url_params)

    msg = f"unknown provider spec {spec!r}; known: {', '.join(known_specs())}"
    raise PermanentError(msg)


def _parse_params(spec: str) -> dict[str, Any]:
    """``mock:skill=0.7,latency=0.01`` -> ``{"skill": 0.7, "latency": 0.01}``."""
    if ":" not in spec:
        return {}
    raw = spec.split(":", 1)[1]
    params: dict[str, Any] = {}
    for chunk in raw.split(","):
        if not chunk.strip():
            continue
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        params[key.strip()] = _coerce(value.strip())
    return params


def _coerce(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value
