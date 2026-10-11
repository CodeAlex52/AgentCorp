# Contributing to AgentCorp

Thanks for considering a contribution. AgentCorp is deliberately **offline-verifiable**:
the whole reliability suite runs with no network and no API keys, and that property is
part of the product contract — please help us keep it.

## Development setup

- Python **>= 3.12**
- [uv](https://docs.astral.sh/uv/) is recommended; a plain venv works too

```bash
uv venv --python 3.12
uv pip install -e ".[dev]"
```

## The one command that matters

```bash
./scripts/reproduce_all.sh          # pytest + e2e demo + benchmark + mypy strict + ruff
FAST=1 ./scripts/reproduce_all.sh   # skip the slow SIGKILL recovery suite
```

CI runs the same gates (`pytest -q -m "not slow"`, `ruff check`, `mypy src/agentcorp`)
on every pull request.

## House rules

- **Deterministic and offline.** Every test must pass with no network and no API keys.
  Real providers are opt-in behind `-m smoke` and must never run by default.
- **The event ledger is the source of truth.** New state must be produced via events,
  and every projection must stay rebuildable from the append-only log.
- **Strict gates stay green.** `mypy src/agentcorp` (strict) and `ruff check` must be
  clean; new code needs types.
- **Contracts are documented.** Behavior changes update `docs/SPEC_v0.md`; capability
  status and known gaps live in `STATUS.md`.

## Pull requests

1. One concern per PR. Describe the failure mode first, then the fix.
2. Add or adjust tests under `tests/`; regression tests for fixed bugs are expected.
3. User-visible behavior changes update `README.md` and the relevant `docs/`.
4. CI must be green before review.

## Issues

Use the provided templates:

- **Bug report** — include the failing command, expected vs actual, and (if possible)
  a minimal reproduction against `examples/demo_repo` or `--provider mock`.
- **Feature request** — describe the reliability concern it addresses; this project is
  built around failure paths, so that framing helps.

## License

MIT. By contributing you agree your contributions are licensed under the same terms.
