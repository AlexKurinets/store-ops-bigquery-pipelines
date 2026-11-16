"""One run of one source for one business date: extract, apply the contract, land, load, scan, record.

This is the body of the Cloud Run job. Cloud Scheduler starts it with the source name; the job's task retry gives
a second attempt; a run that still fails alerts, and the runbook's fix is a rerun for the same date, which is safe
because every load replaces its partition.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from storeops import pii
from storeops.connectors import build
from storeops.landing import landing_path
from storeops.ops import Ops, ts

log = logging.getLogger("storeops")


class PiiFound(RuntimeError):
    pass


class RowCountMismatch(RuntimeError):
    pass


@dataclass
class Context:
    settings: object
    contracts: dict
    wh: object
    landing: object
    secrets: object
    ops: Ops
    now: Callable[[], datetime]
    connector_factory: Callable | None = None
    advance: Callable[[timedelta], None] = lambda delta: None  # the synthetic driver moves its clock here

    def connector(self, source: str):
        if self.connector_factory:
            return self.connector_factory(source)
        return build(source, self.contracts[source], self.settings, self.secrets, ops=self.ops)

    @property
    def pseudonym_key(self) -> str:
        return self.secrets.get("pseudonym_key")


@dataclass
class RunResult:
    run_id: str
    source: str
    batch_date: date
    attempt: int
    status: str = "running"
    rows_extracted: int = 0
    rows_landed: int = 0
    rows_loaded: int = 0
    tables: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    error: str | None = None


def scan_partition(ctx: Context, dataset: str, table: str, columns, batch_date: date, run_id: str) -> list[dict]:
    """Read back every text column of the new partition and look for SSNs and card numbers."""
    rows = []
    for col in columns:
        if col.type != "string":
            continue
        scanned, candidates = ctx.wh.candidates(dataset, table, col.name, pii.COARSE, batch_date)
        hits, kinds = 0, set()
        for v in candidates:
            found = pii.find(v)
            if found:
                hits += 1
                kinds.update(found)
        rows.append({"scan_id": uuid.uuid4().hex[:12], "scanned_at": ts(ctx.now()), "scope": "partition",
                     "dataset": dataset, "table_name": table, "column_name": col.name,
                     "batch_date": batch_date.isoformat(), "values_scanned": scanned, "hits": hits,
                     "kinds": ",".join(sorted(kinds)) or None, "run_id": run_id})
    return rows


def run_source(ctx: Context, source: str, batch_date: date, *, attempt: int = 1, trigger: str = "schedule",
               scheduled_for: datetime | None = None) -> RunResult:
    contract = ctx.contracts[source]
    res = RunResult(run_id=uuid.uuid4().hex[:12], source=source, batch_date=batch_date, attempt=attempt)
    started_at, t0 = ctx.now(), time.monotonic()
    retries = refreshes = 0
    connector = None
    loaded_tables: list[tuple[str, str]] = []
    try:
        connector = ctx.connector(source)
        ext = connector.extract(batch_date)
        retries, refreshes = ext.http_retries, ext.token_refreshes
        res.notes.extend(ext.notes)
        known_drift = ctx.ops.known_drift(source)
        loaded_at = ts(ctx.now())
        meta = {"_batch_date": batch_date.isoformat(), "_run_id": res.run_id, "_loaded_at": loaded_at}
        for table_name, records in ext.tables.items():
            tc = contract.tables[table_name]
            applied = tc.apply(records, ctx.pseudonym_key)
            outputs = [("raw", tc.columns(), applied.rows, False)]
            if tc.restricted_key:
                outputs.append(("raw_restricted", tc.restricted_columns(), applied.restricted_rows, True))
            counts = {}
            for dataset, columns, rows, restricted in outputs:
                uri = ctx.landing.write(landing_path(source, table_name, batch_date, res.run_id),
                                        [{**r, **meta} for r in rows], restricted=restricted)
                landed = ctx.landing.read(uri)
                ctx.wh.ensure_table(dataset, table_name, columns, cluster_by=tc.cluster_by)
                loaded = ctx.wh.replace_partition(dataset, table_name, batch_date, uri, landed, columns,
                                                  tc.cluster_by)
                loaded_tables.append((dataset, table_name))
                if not (len(rows) == len(landed) == loaded):
                    raise RowCountMismatch(f"{dataset}.{table_name}: {len(rows)} rows applied, {len(landed)} landed, "
                                           f"{loaded} loaded")
                counts[dataset] = loaded
            if len(records) != counts["raw"]:
                raise RowCountMismatch(f"{table_name}: {len(records)} extracted, {counts['raw']} loaded")
            res.rows_extracted += len(records)
            res.rows_landed += counts["raw"]
            res.rows_loaded += counts["raw"]
            res.tables[table_name] = counts
            ctx.ops.record_field_actions(res.run_id, source, table_name, applied.actions)
            for fld in sorted(applied.drift):
                if (table_name, fld) not in known_drift:
                    ctx.ops.record_drift(source, table_name, fld, res.run_id, batch_date)
                    ctx.ops.alert("warning", source, "schema_drift",
                                  f"new field {fld} in {table_name} was dropped (not in the contract)",
                                  res.run_id, batch_date)
            scans = scan_partition(ctx, "raw", table_name, tc.columns(), batch_date, res.run_id)
            ctx.ops.record_scans(scans)
            hit = [s for s in scans if s["hits"]]
            if hit:
                raise PiiFound("; ".join(f"{s['table_name']}.{s['column_name']}: {s['hits']} values look like "
                                         f"{s['kinds']}" for s in hit))
        for name, reason in ext.rejected:
            ctx.ops.record_rejected(source, batch_date, name, reason, res.run_id)
            ctx.ops.alert("warning", source, "file_rejected", f"{name}: {reason}", res.run_id, batch_date)
            res.notes.append(f"rejected {name}: {reason}")
        if ext.processed_objects:
            ctx.ops.record_processed(source, ext.processed_objects, batch_date, res.run_id)
        res.status = "success"
    except PiiFound as exc:
        for dataset, table_name in loaded_tables:  # quarantine: nothing from this run stays readable
            ctx.wh.delete_partition(dataset, table_name, batch_date)
        res.status, res.error = "quarantined", str(exc)
        ctx.ops.alert("critical", source, "pii_detected", f"partition {batch_date} removed: {exc}", res.run_id,
                      batch_date)
    except Exception as exc:  # noqa: BLE001 - every failure is recorded and alerted, not swallowed
        res.status, res.error = "failed", f"{type(exc).__name__}: {exc}"
        log.exception("run %s %s %s failed", source, batch_date, res.run_id)
    finally:
        if connector is not None:
            # Counted on the connector too, so a run that fails still records how hard it tried.
            retries = max(retries, getattr(getattr(connector, "http", None), "retries", 0))
            refreshes = max(refreshes, getattr(getattr(connector, "auth", None), "refreshes", 0))
            connector.close()
    elapsed = timedelta(seconds=time.monotonic() - t0)
    ctx.ops.record_run({
        "run_id": res.run_id, "source": source, "batch_date": batch_date.isoformat(), "attempt": attempt,
        "trigger": trigger, "scheduled_for": ts(scheduled_for or started_at), "started_at": ts(started_at),
        "finished_at": ts(started_at + elapsed), "status": res.status, "rows_extracted": res.rows_extracted,
        "rows_landed": res.rows_landed, "rows_loaded": res.rows_loaded, "http_retries": retries,
        "token_refreshes": refreshes, "tables": json.dumps(res.tables), "notes": "; ".join(res.notes)[:2000],
        "error": res.error})
    log.info("%s %s attempt %d: %s (%d rows, %d retries)", source, batch_date, attempt, res.status,
             res.rows_loaded, retries)
    return res


def run_with_retries(ctx: Context, source: str, batch_date: date, *, max_attempts: int = 2, first_attempt: int = 1,
                     total_attempts: int | None = None, trigger: str = "schedule",
                     scheduled_for: datetime | None = None) -> RunResult:
    """Mirrors a Cloud Run job with one task retry: a failed attempt is retried, and the last failure alerts.

    Deployed, each Cloud Run task attempt is one process (first_attempt comes from CLOUD_RUN_TASK_ATTEMPT), so only
    the final attempt raises the alert. Locally, and in the synthetic seed, both attempts run in one process.
    """
    total = total_attempts or (first_attempt + max_attempts - 1)
    res = None
    for attempt in range(first_attempt, first_attempt + max_attempts):
        res = run_source(ctx, source, batch_date, attempt=attempt, trigger=trigger, scheduled_for=scheduled_for)
        if res.status != "failed":
            break
        if attempt < first_attempt + max_attempts - 1:
            ctx.advance(timedelta(minutes=1))
    if res.status == "failed" and res.attempt >= total:
        ctx.ops.alert("critical", source, "run_failed",
                      f"{batch_date} failed on attempt {res.attempt} of {total}: {res.error}", res.run_id, batch_date)
    return res

