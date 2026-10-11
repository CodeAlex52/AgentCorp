"""``agentcorp --version`` stays wired to the installed distribution metadata."""

from __future__ import annotations

import re

from typer.testing import CliRunner

from agentcorp.cli import app


def test_version_flag_prints_package_version() -> None:
    result = CliRunner().invoke(app, ["--version"])
    assert result.exit_code == 0
    assert re.fullmatch(r"agentcorp \d+\.\d+\.\d+\n", result.output)
