"""The dashboard's four pages: pipeline health, key reconciliation, personal-data controls and store metrics.

It reads the same ops, conformed and metrics tables everything else does, as the dashboard_reader role, which has
no access to raw or restricted data.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st
from common import INK_MUTED, SERIES, STATUS, evaluated_at, money, pct, q, safe_md

STATUS_LABEL = {"success": "✓ success", "failed": "✕ failed", "quarantined": "⚠ quarantined"}
FRESH_LABEL = {"on_time": "✓ on time", "late": "⚠ late"}


def ct(ts) -> str:
    if ts is None or pd.isna(ts):
        return ""
    return pd.Timestamp(ts).tz_convert("America/Chicago").strftime("%b %d %H:%M")


# ------------------------------------------------------------------------------------------------- pipeline health
def pipeline_health() -> None:
    st.title("Pipeline health")
    st.caption(f"Synthetic data set, business dates Sep 1 to Oct 30, 2026, checked {evaluated_at()}. "
               "Each source is its own job with one automatic retry; a run that still fails alerts and is rerun.")
    fresh = q("select source, status, sla, expected_batch, latest_batch, landed_at from metrics.source_freshness "
              "order by source")
    runs = q("select source, batch_date, attempt, trigger, scheduled_for, status, rows_extracted, rows_loaded, "
             "http_retries, token_refreshes, notes, error from ops.pipeline_runs")
    alerts = q("select raised_at, severity, source, kind, message from ops.alerts order by raised_at desc")
    dbt = q("select status, nodes_ok, tests_pass, tests_warn, tests_fail, errors, finished_at from ops.transform_runs "
            "order by finished_at desc limit 1")

    scheduled = runs[(runs.trigger == "schedule") & (runs.attempt == 1)]
    first_ok = (scheduled.status == "success").mean() if len(scheduled) else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sources on time", f"{(fresh.status == 'on_time').sum()} of {len(fresh)}")
    c2.metric("Runs succeeding first time", pct(first_ok), help=f"{len(scheduled)} scheduled runs")
    c3.metric("Alerts raised", f"{len(alerts)}", help=f"{(alerts.severity == 'critical').sum()} critical")
    if len(dbt):
        d = dbt.iloc[0]
        c4.metric("dbt build", f"{int(d.tests_pass)} tests passed",
                  help=f"{int(d.nodes_ok)} models and seeds built, {int(d.tests_fail)} tests failed")

    st.subheader("Freshness against each contract's SLA")
    f = fresh.assign(status=fresh.status.map(FRESH_LABEL), landed_at=fresh.landed_at.map(ct))
    st.dataframe(f.rename(columns={"source": "Source", "status": "Status", "sla": "SLA",
                                   "expected_batch": "Expected", "latest_batch": "Latest loaded",
                                   "landed_at": "Landed (CT)"}), hide_index=True, width="stretch")

    st.subheader("Run attempts by business date")
    daily = runs.groupby(["batch_date", "status"]).size().reset_index(name="attempts")
    daily["Outcome"] = daily.status.map(STATUS_LABEL)
    order = [STATUS_LABEL[s] for s in ("success", "failed", "quarantined")]
    chart = alt.Chart(daily).mark_bar(width=7, cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
        x=alt.X("batch_date:T", title="Business date", axis=alt.Axis(format="%b %d", grid=False)),
        y=alt.Y("attempts:Q", title="Attempts", axis=alt.Axis(tickMinStep=1)),
        color=alt.Color("Outcome:N", scale=alt.Scale(domain=order, range=[STATUS[s] for s in
                                                                          ("success", "failed", "quarantined")]),
                        legend=alt.Legend(orient="top", title=None)),
        order=alt.Order("status:N"),
        tooltip=[alt.Tooltip("batch_date:T", title="Business date", format="%b %d"), "Outcome:N", "attempts:Q"],
    ).properties(height=220)
    st.altair_chart(chart, width="stretch")

    retried = runs[(runs.status == "success") & (runs.http_retries > 0)]
    st.subheader("Runs that needed a person")
    st.caption(f"Another {len(retried)} runs hit a 429 or 5xx and recovered on their own through HTTP retries "
               f"({int(retried.http_retries.sum())} retries in all), so they aren't listed.")
    attention = runs[(runs.status != "success") | (runs.attempt > 1) | (runs.trigger != "schedule")
                     | runs.notes.fillna("").str.contains("rejected")]
    attention = attention.sort_values(["batch_date", "scheduled_for"])
    view = pd.DataFrame({
        "Business date": attention.batch_date, "Source": attention.source,
        "Started (CT)": attention.scheduled_for.map(ct), "Attempt": attention.attempt,
        "Trigger": attention.trigger, "Outcome": attention.status.map(STATUS_LABEL),
        "HTTP retries": attention.http_retries, "Rows loaded": attention.rows_loaded,
        "What happened": attention.error.fillna(attention.notes.fillna("")).str.slice(0, 160)})
    st.dataframe(view, hide_index=True, width="stretch")

    st.subheader("Alerts")
    a = alerts.assign(raised_at=alerts.raised_at.map(ct))
    st.dataframe(a.rename(columns={"raised_at": "Raised (CT)", "severity": "Severity", "source": "Source",
                                   "kind": "Kind", "message": "Message"}), hide_index=True, width="stretch")


# ------------------------------------------------------------------------------------------------ reconciliation
def reconciliation() -> None:
    st.title("POS and labor key reconciliation")
    st.caption("POS sales per store and business date against the sales the timekeeping system imported from the "
               "POS at 05:00 the next morning. A match is within 0.5% or a dollar. Target: 99%.")
    r = q("select r.*, s.store_name from metrics.key_reconciliation_daily r join conformed.dim_store s "
          "using (store_key)")
    if r.empty:
        st.info("No reconciliation rows yet.")
        return
    rate = (r.status == "matched").mean()
    cal = r.matched_calendar_date.mean()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Store-days matched", pct(rate), delta=f"{(rate - 0.99) * 100:+.1f} pts vs 99% target")
    c2.metric("Store-days compared", f"{len(r):,}")
    c3.metric("Mismatches", f"{(r.status != 'matched').sum()}")
    c4.metric("Same data on calendar dates", pct(cal), help="POS bucketed by local calendar date instead of the "
                                                            "04:00 business day, the rule that failed first")

    by_day = r.groupby("business_date").agg(business=("status", lambda s: (s == "matched").mean()),
                                            calendar=("matched_calendar_date", "mean")).reset_index()
    long = by_day.melt("business_date", var_name="rule", value_name="rate")
    names = {"business": "Business date (04:00 cutoff)", "calendar": "Local calendar date"}
    long["Rule"] = long.rule.map(names)
    base = alt.Chart(long).encode(
        x=alt.X("business_date:T", title="Business date", axis=alt.Axis(format="%b %d", grid=False)),
        y=alt.Y("rate:Q", title="Stores matched", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0.8, 1.0])),
        color=alt.Color("Rule:N", scale=alt.Scale(domain=list(names.values()), range=SERIES[:2]),
                        legend=alt.Legend(orient="top", title=None, labelLimit=400)),
        tooltip=[alt.Tooltip("business_date:T", title="Business date", format="%b %d"), "Rule:N",
                 alt.Tooltip("rate:Q", format=".1%", title="Stores matched")])
    target = alt.Chart(pd.DataFrame({"y": [0.99]})).mark_rule(strokeDash=[4, 4], color=INK_MUTED).encode(y="y:Q")
    st.altair_chart((base.mark_line(strokeWidth=2) + base.mark_point(size=40, filled=True) + target)
                    .properties(height=260), width="stretch")

    st.subheader("Mismatched store-days")
    ex = r[r.status != "matched"].sort_values("business_date")
    st.dataframe(pd.DataFrame({
        "Store": ex.store_name, "Business date": ex.business_date, "POS": ex.pos_net_sales.map(money),
        "Timekeeping import": ex.labor_system_net_sales.map(money), "Difference": ex.difference.map(money),
        "POS as of import": ex.pos_net_sales_at_import.map(money), "Reason": ex.reason}),
        hide_index=True, width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Codes with no crosswalk row")
        um = q("select system, source_code, row_count, first_seen, last_seen from conformed.unmapped_keys "
               "order by system, source_code")
        st.dataframe(um.rename(columns={"system": "System", "source_code": "Code", "row_count": "Rows held back",
                                        "first_seen": "First seen", "last_seen": "Last seen"}),
                     hide_index=True, width="stretch")
    with right:
        st.subheader("Crosswalk rows with validity dates")
        xr = q("""select x.system, x.source_code, s.store_name, x.valid_from, x.valid_to, x.is_reportable, x.note
                  from reference.store_xref x left join reference.stores s using (store_key)
                  where x.valid_to is not null or not x.is_reportable
                     or (x.system = 'pos' and x.store_key in (select store_key from reference.store_xref
                         where system = 'pos' group by store_key having count(*) > 1))
                  order by x.system, x.valid_from""")
        st.dataframe(xr.rename(columns={"system": "System", "source_code": "Code", "store_name": "Store",
                                        "valid_from": "Valid from", "valid_to": "Valid to",
                                        "is_reportable": "Reportable", "note": "Note"}),
                     hide_index=True, width="stretch")


# ------------------------------------------------------------------------------------------- personal data controls
def controls() -> None:
    from storeops.contracts import load_contracts
    from storeops.settings import Settings

    st.title("Personal data controls")
    st.caption("Every source has a field contract that denies by default. Fields are kept, hashed, masked, sent to "
               "the restricted dataset or dropped before anything is written, and every loaded partition is "
               "scanned again for SSNs and card numbers.")
    contracts = load_contracts(Settings.from_env().contracts_dir)
    fields = []
    for c in contracts.values():
        for t in c.tables.values():
            for fl in t.fields:
                if fl.cls != "keep":
                    fields.append({"Source": c.source, "Table": t.name, "Field": fl.source_name, "Class": fl.cls,
                                   "Personal data": fl.pii or "",
                                   "Requested": "no ($select)" if (t.entity and fl.cls == "drop") else "yes",
                                   "Lands in": {"hash": "raw, as an HMAC", "mask": "raw, masked",
                                                "restrict": "raw_restricted only", "drop": "nowhere"}[fl.cls]})
    fdf = pd.DataFrame(fields)
    scans = q("select scope, sum(values_scanned) as scanned, sum(hits) as hits, max(scanned_at) as last "
              "from ops.pii_scans group by scope")
    masked = q("select count(*) as n from conformed.fct_checklist_items where comment like '%[masked:%'")
    drift = q("select source, table_name, field, batch_date, first_seen_at from ops.schema_drift "
              "order by first_seen_at")
    rejected = q("select source, batch_date, file_name, reason from ops.rejected_files order by batch_date")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Fields never written", f"{(fdf.Class == 'drop').sum()}")
    c2.metric("Fields hashed or restricted", f"{fdf.Class.isin(['hash', 'restrict']).sum()}")
    c3.metric("Comments masked", f"{int(masked.n.iloc[0])}")
    hits = int(scans.hits.sum()) if len(scans) else 0
    c4.metric("Scan hits", f"{hits}", help=f"{int(scans.scanned.sum()):,} values scanned in partitions and sweeps"
              if len(scans) else None)

    st.subheader("Fields the contracts don't keep as-is")
    st.dataframe(fdf, hide_index=True, width="stretch", height=36 * (len(fdf) + 1) + 4)

    st.subheader("Masked comments")
    mc = q("select s.store_name, c.business_date, c.comment from conformed.fct_checklist_items c "
           "join conformed.dim_store s using (store_key) where c.comment like '%[masked:%' order by c.business_date")
    st.dataframe(mc.rename(columns={"store_name": "Store", "business_date": "Business date", "comment": "Comment"}),
                 hide_index=True, width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Who can read what")
        roles = ["agent_reader", "dashboard_reader"]
        schemas = ["raw", "raw_restricted", "staged", "conformed", "restricted", "metrics", "ops"]
        acc = q("select r.role, s.schema, has_schema_privilege(r.role, s.schema, 'USAGE') as ok "
                "from unnest(%s::text[]) as r(role) cross join unnest(%s::text[]) as s(schema)", (roles, schemas))
        grid = acc.assign(ok=acc.ok.map({True: "✓ reads", False: "✕ no access"})).pivot(
            index="schema", columns="role", values="ok").reindex(schemas)
        st.dataframe(grid.rename_axis("Dataset").rename(columns={"agent_reader": "Agents (MCP)",
                                                                 "dashboard_reader": "This dashboard"}),
                     width="stretch")
    with right:
        st.subheader("Scans")
        st.dataframe(pd.DataFrame({"Scope": scans.scope.map({"partition": "Each new partition, after load",
                                                             "sweep": "Full sweep after the build"}),
                                   "Values scanned": scans.scanned.map(lambda v: f"{int(v):,}"),
                                   "Hits": scans.hits, "Last run (CT)": scans["last"].map(ct)}),
                     hide_index=True, width="stretch")
        st.subheader("New fields dropped as schema drift")
        st.dataframe(drift.rename(columns={"source": "Source", "table_name": "Table", "field": "Field",
                                           "batch_date": "First seen", "first_seen_at": "Alerted"})
                     .drop(columns=["Alerted"]), hide_index=True, width="stretch")
        st.subheader("Files rejected by the manifest check")
        st.dataframe(rejected.rename(columns={"source": "Source", "batch_date": "Delivery date",
                                              "file_name": "File", "reason": "Reason"}),
                     hide_index=True, width="stretch")


# ------------------------------------------------------------------------------------------------- store metrics
def store_metrics() -> None:
    st.title("Store metrics")
    st.caption("The metrics tables the agents read through MCP, for one store.")
    stores = q("select store_key, store_name from conformed.dim_store order by store_key")
    f1, f2, _ = st.columns([2, 1, 3])
    name = f1.selectbox("Store", stores.store_name.tolist(), index=0)
    month = f2.selectbox("Fiscal month", ["2026-10", "2026-09"], index=0)
    key = int(stores.loc[stores.store_name == name, "store_key"].iloc[0])

    b = q("select * from metrics.store_month_vs_budget where store_key = %s and fiscal_month = %s", (key, month))
    daily = q("select business_date, net_sales, transactions, labor_hours, sales_per_labor_hour, labor_cost_pct, "
              "reconciled from metrics.store_day_sales_labor where store_key = %s and to_char(business_date, "
              "'YYYY-MM') = %s order by business_date", (key, month))
    c1, c2, c3, c4 = st.columns(4)
    if len(b):
        row = b.iloc[0]
        c1.metric("Sales vs budget to date", pct(row.sales_vs_budget_pct),
                  help=safe_md(f"{money(row.net_sales)} against {money(row.budget_net_sales_to_date)} budgeted for "
                               f"{int(row.days_with_data)} of {int(row.days_in_month)} days "
                               f"(budget version {int(row.budget_version)})"))
        c2.metric("Labor hours vs budget to date", pct(row.labor_hours_vs_budget_pct),
                  help=f"{row.labor_hours:,.0f} hours so far; {row.budget_labor_hours:,.0f} budgeted for the month")
    if len(daily):
        c3.metric("Sales per labor hour", money(daily.net_sales.sum() / daily.labor_hours.sum()))
        c4.metric("Labor cost", pct(daily.labor_cost_pct.mean()), help="Share of sales, from restricted pay rates")

    left, right = st.columns(2)
    with left:
        st.subheader("Net sales by business date")
        st.altair_chart(alt.Chart(daily).mark_bar(width=9, color=SERIES[0], cornerRadiusTopLeft=2,
                                                  cornerRadiusTopRight=2).encode(
            x=alt.X("business_date:T", title=None, axis=alt.Axis(format="%b %d", grid=False)),
            y=alt.Y("net_sales:Q", title="Net sales ($)"),
            tooltip=[alt.Tooltip("business_date:T", format="%b %d", title="Business date"),
                     alt.Tooltip("net_sales:Q", format="$,.2f", title="Net sales"),
                     alt.Tooltip("transactions:Q", title="Tickets"), alt.Tooltip("reconciled:N", title="Reconciled")]
        ).properties(height=230), width="stretch")
    with right:
        st.subheader("Average sales by daypart")
        dp = q("select daypart, daypart_order, avg(net_sales) as avg_sales from metrics.store_daypart_sales "
               "where store_key = %s and to_char(business_date, 'YYYY-MM') = %s group by daypart, daypart_order "
               "order by daypart_order", (key, month))
        st.altair_chart(alt.Chart(dp).mark_bar(color=SERIES[0], cornerRadiusTopLeft=2,
                                               cornerRadiusTopRight=2).encode(
            x=alt.X("daypart:N", sort=dp.daypart.tolist(), title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("avg_sales:Q", title="Average per day ($)"),
            tooltip=["daypart:N", alt.Tooltip("avg_sales:Q", format="$,.2f", title="Average per day")]
        ).properties(height=230), width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Fresh food sell-through")
        ff = q("select item_name, units_received, units_sold, sell_through_pct, week_start "
               "from metrics.fresh_food_weekly where store_key = %s and week_start = (select max(week_start) "
               "from metrics.fresh_food_weekly where store_key = %s and is_complete_week) "
               "order by sell_through_pct", (key, key))
        st.caption(f"Week of {ff.week_start.iloc[0]:%b %d}, the last full week loaded" if len(ff) else "")
        st.dataframe(pd.DataFrame({"Item": ff.item_name, "Received": ff.units_received, "Sold": ff.units_sold,
                                   "Sell-through": ff.sell_through_pct.map(lambda v: pct(v, 0))}),
                     hide_index=True, width="stretch")
    with right:
        st.subheader("Opening checklists by week")
        ck = q("select week_start, open_days, opening_checklists_done, completion_pct, items_failed, pass_rate_pct "
               "from metrics.store_checklist_weekly where store_key = %s order by week_start", (key,))
        st.dataframe(pd.DataFrame({"Week of": ck.week_start, "Open days": ck.open_days,
                                   "Done": ck.opening_checklists_done,
                                   "Completion": ck.completion_pct.map(lambda v: pct(v, 0)),
                                   "Failed items": ck.items_failed,
                                   "Pass rate": ck.pass_rate_pct.map(pct)}),
                     hide_index=True, width="stretch")
