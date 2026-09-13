#!/usr/bin/env bash
# One-command reproduction of the whole v0 contract: tests, demo, benchmark
# validation and the static gates.  Offline; no network, no API keys.
#
#   ./scripts/reproduce_all.sh            # everything
#   FAST=1 ./scripts/reproduce_all.sh     # skip the slow SIGKILL suite
set -euo pipefail

cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"
UV="${UV:-uv}"

echo "==> pytest (offline, deterministic)"
if [[ "${FAST:-0}" == "1" ]]; then
  "$PY" -m pytest -q -m "not slow"
else
  "$PY" -m pytest -q
fi

echo "==> deterministic end-to-end demo + benchmark"
"$PY" examples/end_to_end.py --quiet
"$PY" scripts/validate_benchmark.py benchmarks/self_hosting_sim.json

echo "==> mypy (strict)"
"$UV" run mypy src/agentcorp

echo "==> ruff"
"$UV" run ruff check

echo
echo "all green. artefacts:"
echo "  benchmarks/self_hosting_sim.json"
echo "  docs/SPEC_v0.md        (completion contract)"
echo "  STATUS.md              (current status + known gaps)"
