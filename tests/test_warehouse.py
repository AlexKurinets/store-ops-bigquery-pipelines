"""End to end against Postgres: the full synthetic data set through the real pipeline, dbt, checks, MCP and the
dashboard. The seed runs once per session (see conftest.seeded)."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import date

import psycopg
import pytest
from conftest import ROOT, SECRETS, role_conn

from storeops import checks
from storeops.runner import run_source

pytestmark = pytest.mark.warehouse


def one(ctx, sql, params=None):
    rows = ctx.wh.query(sql, params)
    return rows[0] if rows else None


def test_seed_loaded_every_day_and_dbt_passed(seeded):
    s = seeded["summary"]
    assert s["dbt"]["status"] == "success" and s["dbt"]["tests_fail"] == 0 and s["dbt"]["errors"] == 0
    assert s["failed_after_retry"] == 1  # the planted POS outage, fixed by the on-call rerun
    ctx = seeded["ctx"]
    latest = {r["source"]: r["d"] for r in ctx.wh.query(
        "select source, max(batch_date) as d from ops.pipeline_runs where status = 'success' group by source")}
    assert latest == {s: date(2026, 10, 30) for s in ("pos", "timekeeping", "audits", "budgets", "distributor")}


def test_reconciliation_meets_target_and_explains_every_miss(seeded):
    r = seeded["summary"]["reconciliation"]
    assert r["store_days"] == 1197 and r["matched"] == 1192 and r["meets_target"]
    assert r["calendar_date_match_rate"] < 0.93  # the rule that failed first, kept for comparison
    reasons = {x["reason"]: x["n"] for x in seeded["ctx"].wh.query(
        "select reason, count(*) as n from metrics.key_reconciliation_daily where status = 'mismatch' group by 1")}
    assert reasons == {"pos_changed_after_labor_import": 3, "missing_in_labor_system": 1, "unexplained": 1}


def test_renumbered_store_keeps_one_history(seeded):
    days = {r["business_date"] for r in seeded["ctx"].wh.query(
        "select distinct business_date from conformed.fct_pos_lines where store_key = 9")}
    assert date(2026, 10, 2) in days and date(2026, 10, 6) in days
    assert not days & {date(2026, 10, 3), date(2026, 10, 4), date(2026, 10, 5)}  # closed for the remodel


def test_only_the_new_audit_site_is_unmapped(seeded):
    rows = seeded["ctx"].wh.query("select system, source_code, first_seen from conformed.unmapped_keys")
    assert [(r["system"], r["source_code"]) for r in rows] == [("audits", "W8K4")]


def test_budget_correction_replaces_the_typo(seeded):
    row = one(seeded["ctx"], "select budget_labor_hours, budget_version from conformed.fct_store_budget_month "
                             "where store_key = 15 and fiscal_month = '2026-10'")
    assert row["budget_version"] == 2 and float(row["budget_labor_hours"]) < 3000


def test_no_personal_data_outside_the_restricted_datasets(seeded):
    ctx = seeded["ctx"]
    sweep = checks.pii_sweep(ctx)
    assert sweep["hits"] == 0 and sweep["name_violations"] == [] and sweep["values_scanned"] > 1_000_000
    cols = {(d, t, c) for d, t, c, _ in ctx.wh.columns_in(["raw", "raw_restricted"])}
    assert ("raw_restricted", "timekeeping_employees", "pay_rate") in cols
    assert not any(c == "pay_rate" for d, t, c in cols if d == "raw")
    masked = one(ctx, "select count(*) as n from conformed.fct_checklist_items where comment like '%[masked:%'")
    assert masked["n"] == 4
    hits = one(ctx, """select coalesce(sum(s.hits), 0) as n from ops.pii_scans s join ops.pipeline_runs r using (run_id)
                       where s.scope = 'partition' and r.status <> 'quarantined'""")
    assert hits["n"] == 0  # the only hits are from the deliberate quarantine test below


def test_agents_and_dashboard_can_only_read_their_datasets(seeded):
    pg = seeded["pg"]
    with role_conn(pg, "agent_reader") as conn:
        assert conn.execute("select count(*) from metrics.store_day_sales_labor").fetchone()[0] > 1000
        for table in ("raw_restricted.timekeeping_employees", "raw.pos_transaction_lines", "ops.pipeline_runs",
                      "restricted.labor_cost_store_day"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(f"select 1 from {table} limit 1")
    with role_conn(pg, "dashboard_reader") as conn:
        assert conn.execute("select count(*) from ops.pipeline_runs").fetchone()[0] > 0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("select 1 from raw.timekeeping_employees limit 1")


def test_every_metrics_table_and_column_is_documented_in_the_warehouse(seeded):
    rows = seeded["ctx"].wh.query("""
        select c.relname as t, a.attname as col, col_description(a.attrelid, a.attnum) as d,
               obj_description(c.oid, 'pg_class') as td
        from pg_attribute a join pg_class c on c.oid = a.attrelid join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'metrics' and c.relkind = 'r' and a.attnum > 0 and not a.attisdropped""")
    missing = [f"{r['t']}.{r['col']}" for r in rows if not r["d"] or not r["td"]]
    assert rows and missing == []


def test_freshness_is_on_time_at_the_evaluation_time(seeded):
    assert {r["source"]: r["status"] for r in seeded["summary"]["freshness"]} == {
        s: "on_time" for s in ("pos", "timekeeping", "audits", "budgets", "distributor")}


def test_rerunning_a_day_replaces_its_partition(seeded):
    ctx = seeded["ctx"]
    count = "select count(*) as n from raw.pos_transaction_lines where _batch_date = :d"
    before = one(ctx, count, {"d": date(2026, 10, 30)})["n"]
    res = run_source(ctx, "pos", date(2026, 10, 30), trigger="manual")
    assert res.status == "success" and one(ctx, count, {"d": date(2026, 10, 30)})["n"] == before == res.rows_loaded


def test_a_contract_mistake_is_quarantined_not_loaded(seeded):
    ctx = seeded["ctx"]
    good = ctx.contracts
    audits = good["audits"]
    table = audits.tables["audit_checklist_items"]
    leaky = replace(table, fields=tuple(replace(f, cls="keep") if f.source_name == "comment" else f
                                        for f in table.fields))
    ctx.contracts = {**good, "audits": replace(audits, tables={"audit_checklist_items": leaky})}
    try:
        res = run_source(ctx, "audits", date(2026, 9, 15), trigger="manual")  # the day a card number was typed
    finally:
        ctx.contracts = good
    assert res.status == "quarantined" and "card" in res.error
    gone = one(ctx, "select count(*) as n from raw.audit_checklist_items where _batch_date = :d",
               {"d": date(2026, 9, 15)})
    assert gone["n"] == 0
    alert = one(ctx, "select kind from ops.alerts where run_id = :r", {"r": res.run_id})
    assert alert["kind"] == "pii_detected"
    assert run_source(ctx, "audits", date(2026, 9, 15), trigger="manual").status == "success"  # the fix: rerun


def test_mcp_tools_serve_documented_tables(seeded):
    from mcp import Client

    from storeops.mcp_server import build_server
    from storeops.metrics_reader import MetricsReader
    from storeops.warehouse.postgres import PostgresWarehouse

    pg = seeded["pg"]
    reader = MetricsReader(PostgresWarehouse(psycopg.conninfo.make_conninfo(
        host=pg["host"], port=pg["port"], dbname=pg["dbname"], user="agent_reader",
        password=SECRETS["agent_reader_password"])))
    server = build_server(lambda: reader)

    async def go():
        async with Client(server) as client:
            names = {t.name for t in (await client.list_tools()).tools}
            tables = (await client.call_tool("list_tables", {})).structured_content
            desc = (await client.call_tool("describe_table", {"table": "store_day_sales_labor"})).structured_content
            rows = (await client.call_tool("query_table", {"table": "store_day_sales_labor", "store_key": 4,
                                                           "date_from": "2026-10-01", "date_to": "2026-10-07",
                                                           "limit": 5})).structured_content
            fresh = (await client.call_tool("source_freshness", {})).structured_content
            bad = await client.call_tool("query_table", {"table": "raw.timekeeping_employees"})
            return names, tables, desc, rows, fresh, bad

    names, tables, desc, rows, fresh, bad = asyncio.run(go())
    assert names == {"list_tables", "describe_table", "query_table", "source_freshness"}
    listed = {t["table"] for t in (tables["result"] if isinstance(tables, dict) and "result" in tables else tables)}
    assert {"store_day_sales_labor", "key_reconciliation_daily", "source_freshness"} <= listed
    assert any(c["column"] == "reconciled" and "0.5%" in c["description"] for c in desc["columns"])
    assert rows["row_count"] == 5 and rows["truncated"] and {r["store_key"] for r in rows["rows"]} == {4}
    assert rows["rows"][0]["business_date"] == "2026-10-01"
    fresh_rows = fresh["result"] if isinstance(fresh, dict) and "result" in fresh else fresh
    assert {r["status"] for r in fresh_rows} == {"on_time"}
    assert bad.is_error and "unknown table" in bad.content[0].text


@pytest.mark.parametrize("page", ["pipeline_health", "reconciliation", "controls", "store_metrics"])
def test_dashboard_pages_render_without_errors(seeded, page, monkeypatch):
    from streamlit.testing.v1 import AppTest

    pg = seeded["pg"]
    secrets_dir = seeded["tmp"] / "secrets"
    secrets_dir.mkdir(exist_ok=True)
    (secrets_dir / "dashboard_reader_password").write_text(SECRETS["dashboard_reader_password"])
    for k, v in {"STOREOPS_PG_HOST": pg["host"], "STOREOPS_PG_PORT": str(pg["port"]),
                 "STOREOPS_PG_DATABASE": pg["dbname"], "STOREOPS_PG_USER": "dashboard_reader",
                 "STOREOPS_SECRETS": f"file:{secrets_dir}", "STOREOPS_CONTRACTS_DIR": str(ROOT / "contracts")}.items():
        monkeypatch.setenv(k, v)
    script = (f"import sys; sys.path.insert(0, {json.dumps(str(ROOT / 'dashboard'))})\n"
              f"import views\nviews.{page}()\n")
    at = AppTest.from_string(script, default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.title and at.title[0].value
