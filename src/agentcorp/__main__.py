"""``python -m agentcorp`` → the same CLI as the ``agentcorp`` console script.

Kept separate from :mod:`agentcorp.cli` so tests and the SIGKILL recovery test
can launch the real entry point in a subprocess without relying on the installed
script being on ``PATH``.
"""

from __future__ import annotations

from .cli import app

if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    app()
