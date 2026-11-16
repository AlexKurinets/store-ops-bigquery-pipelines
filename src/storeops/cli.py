"""storeops command line. Every Cloud Run job runs one of these commands."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import typer

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Store operations pipelines.")
check_app = typer.Typer(no_args_is_help=True, help="Freshness, personal-data and reconciliation checks.")
app.add_typer(check_app, name="check")


def _setup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    for noisy in ("httpx", "urllib3", "google", "paramiko"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _ctx():
    from storeops.context import build_context

    return build_context()


def _print(obj) -> None:
    typer.echo(json.dumps(obj, indent=2, default=str))


@app.command("local-secrets")
def local_secrets_cmd(directory: Path = typer.Option(Path("/secrets"), "--dir")) -> None:
    """Create random local secrets that don't exist yet (Compose stands this in for Secret Manager)."""
    from storeops.secrets import local_secrets

    created = local_secrets(directory)
    typer.echo(f"created {len(created)} secrets in {directory}" if created else "secrets already present")


@app.command()
def run(source: str, batch_date: str = typer.Option(..., "--date", help="YYYY-MM-DD, or auto for yesterday in "
                                                                       "the contract's time zone"),
        attempts: int = typer.Option(1, help="attempts in this process; Cloud Run's task retry adds one more"),
        trigger: str = "manual") -> None:
    """Run one source for one business date: extract, apply the contract, land, load, scan."""
    _setup()
    from zoneinfo import ZoneInfo

    from storeops.runner import run_with_retries

    ctx = _ctx()
    if batch_date == "auto":
        tz = ZoneInfo(ctx.contracts[source].timezone)
        day = ctx.now().astimezone(tz).date() - timedelta(days=1)
        trigger = "schedule" if trigger == "manual" else trigger
    else:
        day = date.fromisoformat(batch_date)
    first = int(os.environ.get("CLOUD_RUN_TASK_ATTEMPT", "0")) + 1  # set by Cloud Run on each task retry
    total = int(os.environ.get("STOREOPS_TOTAL_ATTEMPTS", str(first + attempts - 1)))
    res = run_with_retries(ctx, source, day, max_attempts=attempts, first_attempt=first, total_attempts=total,
                           trigger=trigger)
    _print({"run_id": res.run_id, "source": source, "batch_date": day, "status": res.status,
            "rows_loaded": res.rows_loaded, "tables": res.tables, "notes": res.notes, "error": res.error})
    raise typer.Exit(0 if res.status == "success" else 1)


@app.command()
def backfill(source: str, start: str = typer.Option(..., "--from"), end: str = typer.Option(..., "--to")) -> None:
    """Run a source for each date in a range, one date at a time, stopping at the first failure."""
    _setup()
    from storeops.runner import run_with_retries

    ctx = _ctx()
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        res = run_with_retries(ctx, source, d, trigger="backfill")
        typer.echo(f"{source} {d}: {res.status} ({res.rows_loaded} rows)")
        if res.status != "success":
            raise typer.Exit(1)
        d += timedelta(days=1)


@app.command("run-due")
def run_due(at: str = typer.Option(None, help="ISO time; defaults to now")) -> None:
    """Run every job whose Cloud Scheduler cron fired in the last five minutes."""
    _setup()
    from storeops import schedule
    from storeops.runner import run_with_retries

    ctx = _ctx()
    when = datetime.fromisoformat(at) if at else ctx.now()
    jobs = schedule.due(ctx.contracts, when)
    for source, batch_date, fired in jobs:
        res = run_with_retries(ctx, source, batch_date, scheduled_for=fired)
        typer.echo(f"{source} {batch_date}: {res.status}")
    if not jobs:
        typer.echo("nothing due")


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def transform(typer_ctx: typer.Context) -> None:
    """Run dbt (default: build) against the configured target; extra arguments go to dbt."""
    _setup()
    from storeops.transform import run_dbt

    row = run_dbt(_ctx(), list(typer_ctx.args) or None)
    _print(row)
    raise typer.Exit(0 if row["status"] == "success" else 1)


@check_app.command("freshness")
def check_freshness() -> None:
    _setup()
    from storeops import checks

    rows = checks.freshness(_ctx())
    _print(rows)
    raise typer.Exit(0 if all(r["status"] == "on_time" for r in rows) else 1)


@check_app.command("pii")
def check_pii() -> None:
    """Sweep every text column outside the restricted datasets; exit 1 on any hit."""
    _setup()
    from storeops import checks

    out = checks.pii_sweep(_ctx())
    _print(out)
    raise typer.Exit(0 if not out["hits"] and not out["name_violations"] else 1)


@check_app.command("reconciliation")
def check_reconciliation() -> None:
    _setup()
    from storeops import checks

    out = checks.reconciliation(_ctx())
    _print(out)
    raise typer.Exit(0 if out["meets_target"] else 1)


@app.command("seed-synthetic")
def seed_synthetic(vendorsim_url: str = typer.Option("http://vendorsim:8000", envvar="STOREOPS_VENDORSIM_URL"),
                   force: bool = False, first: str = "2026-09-01", last: str = "2026-10-30",
                   build: bool = True) -> None:
    """Load the synthetic data set through the real pipeline, then build and check the warehouse."""
    _setup()
    from storeops.synthetic import http_clock, seed

    out = seed(_ctx(), http_clock(vendorsim_url), date.fromisoformat(first), date.fromisoformat(last), force=force,
               build=build)
    _print(out)
    if not out.get("skipped") and build:
        bad = out["dbt"]["status"] != "success" or out["pii"]["hits"] or not out["reconciliation"]["meets_target"]
        raise typer.Exit(1 if bad else 0)


@app.command()
def contracts() -> None:
    """Print each source's schedule, freshness SLA and field classes."""
    from collections import Counter

    from storeops.contracts import load_contracts
    from storeops.settings import Settings

    for c in load_contracts(Settings.from_env().contracts_dir).values():
        typer.echo(f"{c.source}: {c.system} ({c.protocol})")
        typer.echo(f"  schedule {c.schedule} {c.timezone}; freshness: {c.describe_freshness()}")
        for t in c.tables.values():
            counts = Counter(f.cls for f in t.fields)
            typer.echo(f"  {t.name}: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))


@app.command()
def docs(out: Path = typer.Option(Path("docs/freshness.md"))) -> None:
    """Regenerate docs/freshness.md from the contracts."""
    from storeops.docs import freshness_markdown

    out.write_text(freshness_markdown(), encoding="utf-8")
    typer.echo(f"wrote {out}")


@app.command()
def mcp(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Serve the read-only MCP server over streamable HTTP."""
    from storeops.mcp_server import serve

    serve(host, port)


@app.command("wait-for")
def wait_for_cmd(url: str, seconds: int = 120) -> None:
    from storeops.synthetic import wait_for

    wait_for(url, seconds)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
