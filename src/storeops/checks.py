"""Checks that run after loads: freshness against each contract's SLA, a full personal-data sweep, and the
POS-to-labor key reconciliation against its 99% target."""

from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from storeops import pii
from storeops.ops import ts

SWEPT_DATASETS = ["raw", "staged", "conformed", "metrics"]
RESTRICTED_DATASETS = {"raw_restricted", "restricted"}
RECON_TARGET = 0.99


def expected_daily_batch(contract, now: datetime) -> tuple[date, datetime]:
    """The latest business date whose SLA has passed at `now`, and when it was due."""
    tz = ZoneInfo(contract.timezone)
    local = now.astimezone(tz)
    landed_by = time.fromisoformat(contract.landed_by)
    due_today = datetime.combine(local.date(), landed_by, tzinfo=tz)
    if local >= due_today:
        return local.date() - timedelta(days=1), due_today
    return local.date() - timedelta(days=2), due_today - timedelta(days=1)


def expected_month(contract, now: datetime) -> tuple[str, datetime]:
    tz = ZoneInfo(contract.timezone)
    local = now.astimezone(tz)
    day = int(contract.freshness["day_of_month"])
    due = datetime.combine(local.date().replace(day=day), time.fromisoformat(contract.landed_by), tzinfo=tz)
    if local >= due:
        return f"{local:%Y-%m}", due
    prev = local.date().replace(day=1) - timedelta(days=1)
    return f"{prev:%Y-%m}", datetime.combine(prev.replace(day=day), time.fromisoformat(contract.landed_by), tzinfo=tz)


def freshness(ctx, now: datetime | None = None, alert: bool = True) -> list[dict]:
    now = now or ctx.now()
    rows = []
    for source, contract in ctx.contracts.items():
        latest = ctx.ops.latest_success(source)
        if contract.freshness["kind"] == "monthly":
            month, due = expected_month(contract, now)
            got = ctx.wh.query("select max(fiscal_month) as m from {raw}.budget_store_month")
            latest_month = got[0]["m"] if got else None
            ok = latest_month is not None and latest_month >= month
            expected_batch = date.fromisoformat(f"{month}-01")
            latest_batch = date.fromisoformat(f"{latest_month}-01") if latest_month else None
        else:
            expected_batch, due = expected_daily_batch(contract, now)
            latest_batch = latest["batch_date"] if latest else None
            ok = latest_batch is not None and latest_batch >= expected_batch
        row = {"checked_at": ts(now), "source": source, "expected_batch": expected_batch.isoformat(),
               "latest_batch": latest_batch.isoformat() if latest_batch else None,
               "landed_at": ts(latest["finished_at"]) if latest else None, "due_at": ts(due),
               "status": "on_time" if ok else "late", "sla": contract.describe_freshness()}
        rows.append(row)
        if not ok and alert:
            ctx.ops.alert("critical", source, "late", f"expected {row['expected_batch']} by {due:%Y-%m-%d %H:%M %Z}, "
                                                      f"latest loaded is {row['latest_batch']}")
    ctx.wh.insert("ops", "freshness_checks", rows)
    return rows


def pii_sweep(ctx) -> dict:
    """Scan every text column outside the restricted datasets, and every column name everywhere but there."""
    now = ts(ctx.now())
    scan_id = uuid.uuid4().hex[:12]
    rows, hits, name_violations = [], 0, []
    for dataset, table, column, typ in ctx.wh.columns_in(SWEPT_DATASETS):
        if pii.PII_COLUMN.search(column):
            name_violations.append(f"{dataset}.{table}.{column}")
        if typ != "string":
            continue
        scanned, candidates = ctx.wh.candidates(dataset, table, column, pii.COARSE)
        found, kinds = 0, set()
        for v in candidates:
            k = pii.find(str(v))
            if k:
                found += 1
                kinds.update(k)
        hits += found
        rows.append({"scan_id": scan_id, "scanned_at": now, "scope": "sweep", "dataset": dataset,
                     "table_name": table, "column_name": column, "batch_date": None, "values_scanned": scanned,
                     "hits": found, "kinds": ",".join(sorted(kinds)) or None, "run_id": None})
    ctx.ops.record_scans(rows)
    if hits or name_violations:
        ctx.ops.alert("critical", "warehouse", "pii_sweep",
                      f"{hits} values look like SSNs or card numbers; columns with personal names: {name_violations}")
    return {"columns_scanned": len(rows), "values_scanned": sum(r["values_scanned"] for r in rows), "hits": hits,
            "name_violations": name_violations}


def reconciliation(ctx, alert: bool = True) -> dict:
    rows = ctx.wh.query("""
        select count(*) as store_days,
               sum(case when status = 'matched' then 1 else 0 end) as matched,
               sum(case when matched_calendar_date then 1 else 0 end) as matched_calendar,
               min(business_date) as first_day, max(business_date) as last_day
        from {metrics}.key_reconciliation_daily""")
    r = rows[0]
    total = int(r["store_days"] or 0)
    rate = (int(r["matched"]) / total) if total else 0.0
    out = {"store_days": total, "matched": int(r["matched"] or 0), "match_rate": rate,
           "calendar_date_match_rate": (int(r["matched_calendar"]) / total) if total else 0.0,
           "first_day": r["first_day"], "last_day": r["last_day"], "target": RECON_TARGET,
           "meets_target": rate >= RECON_TARGET}
    if alert and total and rate < RECON_TARGET:
        ctx.ops.alert("critical", "warehouse", "reconciliation",
                      f"POS and labor matched on {rate:.1%} of store-days, below the {RECON_TARGET:.0%} target")
    return out
