"""``agentcorp`` — the command line interface (SPEC C16).

Commands: ``run`` / ``resume`` / ``status`` / ``graph`` / ``report`` /
``providers`` / ``cancel``.  Exit codes: ``0`` success, ``1`` business failure
(run FAILED / CANCELLED / DEADLOCK), ``2`` usage error (typer), ``3`` budget
exhausted.

All commands work fully offline with ``--provider mock``; the real providers are
opt-in and only ever touched through :mod:`agentcorp.providers`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any

import typer

from .chaos import ChaosConfig, FaultKind
from .engine import Engine, EngineConfig
from .models import BudgetLimits
from .providers.registry import list_providers
from .report import build_run_report, validate_report, write_report
from .scheduler import SchedulerConfig
from .store import Store
from .supervisor import SupervisorConfig

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="AgentCorp — local-first, event-sourced multi-agent delivery orchestrator.",
)

DEFAULT_DB = ".agentcorp/agentcorp.db"


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        try:
            payload: Any = json.loads(message)
            if not isinstance(payload, dict):
                payload = {"msg": payload}
        except (json.JSONDecodeError, TypeError):
            payload = {"msg": message}
        payload.setdefault("level", record.levelname)
        payload.setdefault("logger", record.name)
        return json.dumps(payload, sort_keys=True, default=str)


def _configure_logging(json_logs: bool) -> None:
    handler = logging.StreamHandler(sys.stderr)
    if json_logs:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def _budget_limits(
    *,
    max_tokens: int | None,
    max_cost: float | None,
    max_tasks: int | None,
    max_wall: float | None,
    max_calls: int | None,
) -> BudgetLimits:
    return BudgetLimits(
        max_tokens=max_tokens,
        max_cost_usd=max_cost,
        max_tasks=max_tasks,
        max_wall_seconds=max_wall,
        max_agent_calls=max_calls,
    )


def _chaos_config(
    *,
    probability: float,
    seed: int,
    kinds: str,
    fail_forever: bool,
) -> ChaosConfig | None:
    if probability <= 0 and not fail_forever and not kinds:
        return None
    parsed = tuple(FaultKind(k.strip()) for k in kinds.split(",") if k.strip()) if kinds else tuple(FaultKind)
    return ChaosConfig(
        enabled=True,
        probability=max(min(probability, 1.0), 0.0),
        kinds=parsed,
        seed=seed,
        fail_forever=fail_forever,
    )


def _install_signal_handlers(engine: Engine) -> None:
    loop = asyncio.get_running_loop()

    def handler(signum: int, _frame: Any) -> None:
        name = signal.Signals(signum).name
        loop.call_soon_threadsafe(engine.request_cancel, f"received {name}")
        typer.echo(f"… {name} received, draining run (second signal ignored)", err=True)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handler)
        except ValueError:  # pragma: no cover - non-main thread
            continue


def _open_store(db: str) -> Store:
    return Store(db)


@app.command()
def providers() -> None:
    """List known providers and whether their credentials are present."""
    table = list_providers()
    for name, info in sorted(table.items()):
        model = info.get("model", "")
        needs = "key" if info.get("needs_key") else "offline"
        present = info.get("key_present")
        suffix = ""
        if needs == "key":
            suffix = " (key present)" if present else " (key MISSING)"
        typer.echo(f"{name:<12} {info.get('kind', ''):<10} {needs:<8} {model}{suffix}")


@app.command()
def run(
    repo: str = typer.Option(".", "--repo", help="Repository the run operates on."),
    prd: Path | None = typer.Option(None, "--prd", help="Path to a PRD text file.", exists=True, dir_okay=False),
    prd_text: str | None = typer.Option(None, "--prd-text", help="Inline PRD text (alternative to --prd)."),
    db: str = typer.Option(DEFAULT_DB, "--db", help="SQLite workspace path."),
    provider: str = typer.Option("mock", "--provider", help="Provider spec, e.g. 'mock', 'openai', 'cli:claude -p'."),
    prd_mode: str = typer.Option("provider", "--prd-mode", help="provider | heuristic"),
    concurrency: int = typer.Option(4, "--concurrency", min=1, help="Maximum parallel tasks."),
    max_attempts: int = typer.Option(3, "--max-attempts", min=1, help="Attempts per task before quarantine."),
    max_reworks: int = typer.Option(2, "--max-reworks", min=0, help="Review rework rounds per task."),
    max_depth: int = typer.Option(3, "--max-depth", min=0, help="Recursive decomposition depth limit."),
    max_total_tasks: int = typer.Option(200, "--max-total-tasks", min=1, help="Global task ceiling."),
    max_tokens: int | None = typer.Option(None, "--max-tokens", help="Token budget for the run."),
    max_cost: float | None = typer.Option(None, "--max-cost", help="USD budget for the run."),
    max_tasks: int | None = typer.Option(None, "--max-tasks", help="Task-dispatch budget for the run."),
    max_wall: float | None = typer.Option(None, "--max-wall", help="Wall-clock budget in seconds."),
    max_calls: int | None = typer.Option(None, "--max-calls", help="Agent-call budget for the run."),
    review: bool = typer.Option(True, "--review/--no-review", help="Run the independent review loop."),
    writes: bool = typer.Option(True, "--writes/--no-writes", help="Apply worker file writes to the repo."),
    chaos: float = typer.Option(0.0, "--chaos", min=0.0, max=1.0, help="Fault-injection probability per call."),
    chaos_seed: int = typer.Option(0, "--chaos-seed", help="Seed for reproducible chaos."),
    chaos_kinds: str = typer.Option("", "--chaos-kinds", help="Comma-separated fault kinds (default all)."),
    chaos_fail_forever: bool = typer.Option(False, "--chaos-fail-forever", help="Every eligible call faults."),
    report_out: Path | None = typer.Option(None, "--report-out", help="Write the run report JSON here."),
    json_logs: bool = typer.Option(False, "--json-logs", help="Emit structured JSON logs on stderr."),
    run_id: str | None = typer.Option(None, "--run-id", help="Use a specific run id."),
) -> None:
    """Plan a PRD into a task DAG and execute it."""
    _configure_logging(json_logs)
    text = _prd_text_from(prd, prd_text)
    config = EngineConfig(
        repo_path=repo,
        provider_spec=provider,
        db_path=db,
        prd_mode=prd_mode,
        budget=_budget_limits(
            max_tokens=max_tokens,
            max_cost=max_cost,
            max_tasks=max_tasks,
            max_wall=max_wall,
            max_calls=max_calls,
        ),
        scheduler=SchedulerConfig(
            max_concurrency=concurrency,
            max_reworks=max_reworks,
            review=review,
            log_events=json_logs,
        ),
        supervisor=SupervisorConfig(),
        apply_writes=writes,
        chaos=_chaos_config(
            probability=chaos, seed=chaos_seed, kinds=chaos_kinds, fail_forever=chaos_fail_forever
        ),
        max_attempts=max_attempts,
        max_depth_override=max_depth,
        max_total_tasks_override=max_total_tasks,
    )
    summary = asyncio.run(_run_async(config, text, run_id=run_id))
    _finish(summary.status, summary.reason, summary.report, report_out)


async def _run_async(config: EngineConfig, text: str, *, run_id: str | None) -> Any:
    engine = Engine(config)
    _install_signal_handlers(engine)
    try:
        summary = await engine.run_prd(text, run_id=run_id)
    finally:
        await engine.aclose()
    return summary


@app.command()
def resume(
    run_id: str = typer.Argument(..., help="Run id or project name to resume."),
    repo: str | None = typer.Option(None, "--repo", help="Override the repository path."),
    db: str = typer.Option(DEFAULT_DB, "--db", help="SQLite workspace path."),
    provider: str = typer.Option("mock", "--provider", help="Provider spec."),
    concurrency: int = typer.Option(4, "--concurrency", min=1),
    chaos: float = typer.Option(0.0, "--chaos", min=0.0, max=1.0),
    chaos_seed: int = typer.Option(0, "--chaos-seed"),
    json_logs: bool = typer.Option(False, "--json-logs"),
    report_out: Path | None = typer.Option(None, "--report-out"),
) -> None:
    """Continue a crashed run: DONE tasks are never re-executed."""
    _configure_logging(json_logs)
    config = EngineConfig(
        repo_path=repo or ".",
        provider_spec=provider,
        db_path=db,
        scheduler=SchedulerConfig(max_concurrency=concurrency, log_events=json_logs),
        chaos=_chaos_config(probability=chaos, seed=chaos_seed, kinds="", fail_forever=False),
    )
    summary = asyncio.run(_resume_async(config, run_id))
    _finish(summary.status, summary.reason, summary.report, report_out)


async def _resume_async(config: EngineConfig, run_id: str) -> Any:
    engine = Engine(config)
    _install_signal_handlers(engine)
    try:
        summary = await engine.resume(run_id)
    finally:
        await engine.aclose()
    return summary


@app.command()
def status(
    run_id: str | None = typer.Argument(None, help="Run id or name (default: latest)."),
    db: str = typer.Option(DEFAULT_DB, "--db"),
    events: int = typer.Option(10, "--events", min=0, help="How many recent events to show."),
) -> None:
    """Print a run/task/budget snapshot as JSON."""
    store = _open_store(db)
    try:
        project = store.resolve_project(run_id)
        if project is None:
            typer.echo(json.dumps({"error": f"unknown run {run_id!r}", "runs": [p.id for p in store.list_projects()]}, indent=2))
            raise typer.Exit(1)
        payload = _status_payload(store, project.id, events=events)
    finally:
        store.close()
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _status_payload(store: Store, project_id: str, *, events: int) -> dict[str, Any]:
    recent = store.list_events(project_id)[-max(events, 0) :]
    counts = store.status_counts(project_id)
    project = store.get_project(project_id)
    return {
        "run_id": project_id,
        "project": project.model_dump(mode="json") if project else None,
        "run_summary": store.get_document(project_id, "run_summary"),
        "tasks": {status.value: count for status, count in counts.items() if count},
        "budget": store.get_document(project_id, "budget"),
        "usage": store.usage_for_project(project_id).model_dump(mode="json"),
        "recent_events": [event.compact() for event in recent],
        "unfinished_runs": [run.id for run in store.unfinished_runs(project_id)],
    }


@app.command()
def graph(
    run_id: str | None = typer.Argument(None, help="Run id or name (default: latest)."),
    db: str = typer.Option(DEFAULT_DB, "--db"),
    fmt: str = typer.Option("ascii", "--format", help="ascii | mermaid | dot"),
) -> None:
    """Render the task DAG."""
    store = _open_store(db)
    try:
        project = store.resolve_project(run_id)
        if project is None:
            typer.echo(f"unknown run {run_id!r}", err=True)
            raise typer.Exit(1)
        from .graph import TaskGraph

        task_graph = TaskGraph(store.list_tasks(project.id))
        if fmt == "ascii":
            typer.echo(task_graph.to_ascii())
        elif fmt == "mermaid":
            typer.echo(task_graph.to_mermaid())
        elif fmt == "dot":
            typer.echo(task_graph.to_dot())
        else:
            typer.echo(f"unknown format {fmt!r}; use ascii|mermaid|dot", err=True)
            raise typer.Exit(2)
    finally:
        store.close()


@app.command()
def report(
    run_id: str | None = typer.Argument(None, help="Run id or name (default: latest)."),
    db: str = typer.Option(DEFAULT_DB, "--db"),
    out: Path | None = typer.Option(None, "--out", help="Write the report JSON to this path."),
    validate: bool = typer.Option(True, "--validate/--no-validate", help="Check the report against SPEC §6."),
) -> None:
    """Print (and optionally write) the SPEC §6 run report."""
    store = _open_store(db)
    try:
        project = store.resolve_project(run_id)
        if project is None:
            typer.echo(f"unknown run {run_id!r}", err=True)
            raise typer.Exit(1)
        payload = build_run_report(store, project.id)
    finally:
        store.close()
    if validate:
        problems = validate_report(payload)
        if problems:
            typer.echo("report failed schema validation:", err=True)
            for problem in problems:
                typer.echo(f"  - {problem}", err=True)
            raise typer.Exit(1)
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if out is not None:
        write_report(out, payload)
        typer.echo(f"report written to {out}")
    else:
        typer.echo(text)


@app.command()
def cancel(
    run_id: str = typer.Argument(..., help="Run id or name to cancel."),
    db: str = typer.Option(DEFAULT_DB, "--db"),
    reason: str = typer.Option("cancelled by operator", "--reason"),
) -> None:
    """Request cancellation of a running run (durable, visible to its process)."""
    store = _open_store(db)
    try:
        project = store.resolve_project(run_id)
        if project is None:
            typer.echo(f"unknown run {run_id!r}", err=True)
            raise typer.Exit(1)
        store.put_document(project.id, "cancel_request", {"reason": reason})
    finally:
        store.close()
    typer.echo(f"cancellation requested for {run_id}")


def _prd_text_from(prd: Path | None, prd_text: str | None) -> str:
    if prd_text:
        return prd_text
    if prd is not None:
        return prd.read_text(encoding="utf-8")
    typer.echo(
        "provide either --prd <file> or --prd-text '<text>' (usage error)",
        err=True,
    )
    raise typer.Exit(2)


def _finish(status: str, reason: str, payload: dict[str, Any], out: Path | None) -> None:
    if out is not None and payload:
        write_report(out, payload)
        typer.echo(f"report written to {out}")
    if status == "DONE":
        typer.echo(f"run status: {status}")
        raise typer.Exit(0)
    exit_code = 3 if status == "BUDGET_EXHAUSTED" else 1
    if reason:
        typer.echo(f"run status: {status} — {reason[:300]}", err=True)
    else:
        typer.echo(f"run status: {status}", err=True)
    raise typer.Exit(exit_code)


def main() -> None:  # pragma: no cover - console-script shim
    app()


__all__ = ["app", "main", "DEFAULT_DB"]
