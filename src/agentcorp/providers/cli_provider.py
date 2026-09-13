"""CLI-backed provider.

Wraps a command-line coding agent (``claude``, ``codex``, ``opencode``, ``aider``,
``llm``, a local script…) behind the same :class:`~agentcorp.providers.base.AgentProvider`
interface as the HTTP providers.  The prompt goes in on stdin and the reply is
read from stdout, which is the lowest common denominator across those tools and
avoids argument-length limits.

Because these tools are themselves agents, they are slow and expensive; the
orchestrator therefore treats them as leaves — one CLI call per task, no
internal retry stacking.
"""

from __future__ import annotations

import asyncio
import os
import shlex
from pathlib import Path

from ..errors import PermanentError, ProviderError, TimeoutError_
from ..models import Usage
from .base import AgentProvider, CompletionRequest, CompletionResponse, estimate_tokens

__all__ = ["CLIProvider"]


class CLIProvider(AgentProvider):
    name = "cli"

    def __init__(
        self,
        command: str,
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 900.0,
        model: str | None = None,
        pricing: tuple[float, float] = (0.0, 0.0),
        prompt_header: str = "",
    ) -> None:
        if not command.strip():
            msg = "CLIProvider requires a non-empty command"
            raise ValueError(msg)
        self.command = command
        self.cwd = str(cwd) if cwd else None
        self.env = env or {}
        self.timeout = timeout
        self.default_model = model or command.split()[0]
        self.pricing = pricing
        self.prompt_header = prompt_header

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        argv = shlex.split(self.command)
        if not argv:
            msg = "CLIProvider command parsed to nothing"
            raise PermanentError(msg)

        prompt = (self.prompt_header + "\n\n" if self.prompt_header else "") + request.text()
        env = {**os.environ, **self.env}
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.cwd,
                env=env,
            )
        except FileNotFoundError as exc:
            raise PermanentError(f"CLI provider: executable not found: {argv[0]}") from exc

        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(prompt.encode()), timeout=self.timeout
            )
        except TimeoutError as exc:
            proc.kill()
            await proc.wait()
            raise TimeoutError_(f"CLI provider timed out after {self.timeout}s") from exc

        stdout = stdout_b.decode(errors="replace")
        stderr = stderr_b.decode(errors="replace")
        if proc.returncode != 0:
            raise ProviderError(
                f"CLI provider exited {proc.returncode}: {stderr.strip()[:400] or stdout.strip()[:400]}"
            )

        tokens_in = estimate_tokens(prompt)
        tokens_out = estimate_tokens(stdout)
        return CompletionResponse(
            text=stdout.strip(),
            model=request.model or self.default_model,
            provider=self.name,
            usage=Usage(
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                calls=1,
                cost_usd=self.cost_of(tokens_in, tokens_out),
            ),
            raw={"returncode": proc.returncode, "stderr": stderr[-500:]},
        )
