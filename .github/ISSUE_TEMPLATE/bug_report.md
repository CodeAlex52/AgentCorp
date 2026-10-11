---
name: Bug report
about: Something misbehaves — include the failing command and expected vs actual
title: "[BUG] "
labels: bug
---

**Command / entry point**

```bash
# e.g. agentcorp run --project examples/demo_repo --prd ... --provider mock
```

**Expected vs actual**

**Reproduction notes**

- Does it reproduce with `--provider mock` and `FAST=1 ./scripts/reproduce_all.sh`?
- Run id / DB path (`.agentcorp/...`) if the ledger is involved:

**Environment**

- `agentcorp --version`:
- Python / OS:
