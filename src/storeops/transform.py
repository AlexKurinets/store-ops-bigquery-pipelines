"""Runs the dbt project (seeds, models and tests) and records the outcome in ops.transform_runs."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime

from storeops.ops import ts


def dbt_env(settings, secrets) -> dict[str, str]:
    env = {"STOREOPS_PG_HOST": settings.pg_host, "STOREOPS_PG_PORT": str(settings.pg_port),
           "STOREOPS_PG_DATABASE": settings.pg_database, "STOREOPS_PG_USER": settings.pg_user}
    if settings.warehouse == "postgres":
        env["STOREOPS_PG_PASSWORD"] = secrets.get(f"{settings.pg_user}_password")
    if settings.bq_project:
        env["STOREOPS_BQ_PROJECT"] = settings.bq_project
    return env


def run_dbt(ctx, args: list[str] | None = None) -> dict:
    from dbt.cli.main import dbtRunner

    settings = ctx.settings
    os.environ.update(dbt_env(settings, ctx.secrets))
    os.environ.setdefault("DO_NOT_TRACK", "1")  # no usage reporting from pipeline runs
    target = "prod" if settings.warehouse == "bigquery" else "local"
    project = str(settings.dbt_project_dir)
    started = utcnow()
    argv = (args or ["build"]) + ["--project-dir", project, "--profiles-dir", project, "--target", target]
    result = dbtRunner().invoke(argv)
    counts = {"pass": 0, "warn": 0, "fail": 0, "error": 0, "success": 0}
    failures = []
    for r in getattr(result.result, "results", None) or []:
        status = str(getattr(r, "status", "")).lower()
        counts[status] = counts.get(status, 0) + 1
        if status in ("fail", "error", "warn"):
            failures.append({"node": r.node.unique_id, "status": status, "message": (r.message or "")[:300]})
    row = {"run_id": uuid.uuid4().hex[:12], "started_at": ts(started), "finished_at": ts(utcnow()),
           "status": "success" if result.success else "failed", "nodes_ok": counts["success"],
           "tests_pass": counts["pass"], "tests_warn": counts["warn"], "tests_fail": counts["fail"],
           "errors": counts["error"], "failures": json.dumps(failures)}
    if args is None or args[:1] == ["build"]:
        ctx.wh.insert("ops", "transform_runs", [row])
        if not result.success:
            ctx.ops.alert("critical", "warehouse", "dbt_build", f"dbt build failed: {failures[:3]}")
    return row


def utcnow() -> datetime:
    return datetime.now().astimezone()
