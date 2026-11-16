"""The ops dataset (run log, field actions, drift, scans, freshness, alerts) and the alert sender."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime

import httpx

log = logging.getLogger("storeops")


def ts(dt: datetime) -> str:
    return dt.isoformat()


class Ops:
    def __init__(self, wh, webhook_url: str | None = None, now=None):
        self.wh = wh
        self.webhook_url = webhook_url
        self.now = now or (lambda: datetime.now().astimezone())

    def record_run(self, row: dict) -> None:
        self.wh.insert("ops", "pipeline_runs", [row])

    def record_field_actions(self, run_id: str, source: str, table: str, actions) -> None:
        rows = [{"run_id": run_id, "source": source, "table_name": table, "field": f, "action": a,
                 "value_count": n, "recorded_at": ts(self.now())} for (f, a), n in sorted(actions.items())]
        self.wh.insert("ops", "field_actions", rows)

    def known_drift(self, source: str) -> set[tuple[str, str]]:
        rows = self.wh.query("select table_name, field from {ops}.schema_drift where source = :source",
                             {"source": source})
        return {(r["table_name"], r["field"]) for r in rows}

    def record_drift(self, source: str, table: str, field: str, run_id: str, batch_date: date) -> None:
        self.wh.insert("ops", "schema_drift", [{"source": source, "table_name": table, "field": field,
                                                "first_seen_run": run_id, "first_seen_at": ts(self.now()),
                                                "batch_date": batch_date.isoformat()}])

    def record_rejected(self, source: str, batch_date: date, name: str, reason: str, run_id: str) -> None:
        self.wh.insert("ops", "rejected_files", [{"source": source, "batch_date": batch_date.isoformat(),
                                                  "file_name": name, "reason": reason, "run_id": run_id,
                                                  "rejected_at": ts(self.now())}])

    def loaded_objects(self, source: str) -> dict[tuple[str, int], date]:
        rows = self.wh.query("select object_name, generation, batch_date from {ops}.processed_objects "
                             "where source = :source", {"source": source})
        return {(r["object_name"], int(r["generation"])): r["batch_date"] for r in rows}

    def record_processed(self, source: str, objects, batch_date: date, run_id: str) -> None:
        known = self.loaded_objects(source)
        rows = [{"source": source, "object_name": n, "generation": g, "batch_date": batch_date.isoformat(),
                 "run_id": run_id, "processed_at": ts(self.now())} for n, g in objects if (n, g) not in known]
        self.wh.insert("ops", "processed_objects", rows)

    def record_scans(self, rows: list[dict]) -> None:
        self.wh.insert("ops", "pii_scans", rows)

    def latest_success(self, source: str) -> dict | None:
        rows = self.wh.query("select batch_date, finished_at from {ops}.pipeline_runs where source = :source "
                             "and status = 'success' order by batch_date desc, finished_at desc limit 1",
                             {"source": source})
        return rows[0] if rows else None

    def alert(self, severity: str, source: str, kind: str, message: str, run_id: str | None = None,
              batch_date: date | None = None) -> None:
        delivered = False
        if self.webhook_url:
            try:
                r = httpx.post(self.webhook_url, json={"text": f"[{severity}] {source} {kind}: {message}"}, timeout=5)
                delivered = r.status_code < 300
            except httpx.HTTPError as exc:  # an alert that can't be posted is still recorded
                log.warning("alert webhook failed: %s", exc)
        self.wh.insert("ops", "alerts", [{"alert_id": uuid.uuid4().hex[:12], "raised_at": ts(self.now()),
                                          "severity": severity, "source": source, "kind": kind, "message": message,
                                          "run_id": run_id,
                                          "batch_date": batch_date.isoformat() if batch_date else None,
                                          "delivered": delivered}])
        log.warning("alert %s %s %s: %s", severity, source, kind, message)
