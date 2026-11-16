"""The warehouse adapter interface and the ops tables both adapters create.

Raw tables are partitioned by `_batch_date`, the business date a run was scheduled for. A run replaces its own
partition, so rerunning a day replaces that day instead of adding to it. Ops tables are append-only, which suits
BigQuery (no UPDATE against a streaming buffer) and keeps a full history of every attempt.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Protocol

from storeops.contracts import Column

RAW_META = [Column("_batch_date", "date", "Business date of the run that loaded the row (partition)"),
            Column("_run_id", "string", "Pipeline run that loaded the row"),
            Column("_loaded_at", "timestamp", "When the row was loaded")]

OPS_TABLES: dict[str, list[Column]] = {
    "pipeline_runs": [Column(n, t) for n, t in [
        ("run_id", "string"), ("source", "string"), ("batch_date", "date"), ("attempt", "integer"),
        ("trigger", "string"), ("scheduled_for", "timestamp"), ("started_at", "timestamp"),
        ("finished_at", "timestamp"), ("status", "string"), ("rows_extracted", "integer"),
        ("rows_landed", "integer"), ("rows_loaded", "integer"), ("http_retries", "integer"),
        ("token_refreshes", "integer"), ("tables", "string"), ("notes", "string"), ("error", "string")]],
    "field_actions": [Column(n, t) for n, t in [
        ("run_id", "string"), ("source", "string"), ("table_name", "string"), ("field", "string"),
        ("action", "string"), ("value_count", "integer"), ("recorded_at", "timestamp")]],
    "schema_drift": [Column(n, t) for n, t in [
        ("source", "string"), ("table_name", "string"), ("field", "string"), ("first_seen_run", "string"),
        ("first_seen_at", "timestamp"), ("batch_date", "date")]],
    "alerts": [Column(n, t) for n, t in [
        ("alert_id", "string"), ("raised_at", "timestamp"), ("severity", "string"), ("source", "string"),
        ("kind", "string"), ("message", "string"), ("run_id", "string"), ("batch_date", "date"),
        ("delivered", "boolean")]],
    "pii_scans": [Column(n, t) for n, t in [
        ("scan_id", "string"), ("scanned_at", "timestamp"), ("scope", "string"), ("dataset", "string"),
        ("table_name", "string"), ("column_name", "string"), ("batch_date", "date"), ("values_scanned", "integer"),
        ("hits", "integer"), ("kinds", "string"), ("run_id", "string")]],
    "freshness_checks": [Column(n, t) for n, t in [
        ("checked_at", "timestamp"), ("source", "string"), ("expected_batch", "date"), ("latest_batch", "date"),
        ("landed_at", "timestamp"), ("due_at", "timestamp"), ("status", "string"), ("sla", "string")]],
    "processed_objects": [Column(n, t) for n, t in [
        ("source", "string"), ("object_name", "string"), ("generation", "integer"), ("batch_date", "date"),
        ("run_id", "string"), ("processed_at", "timestamp")]],
    "rejected_files": [Column(n, t) for n, t in [
        ("source", "string"), ("batch_date", "date"), ("file_name", "string"), ("reason", "string"),
        ("run_id", "string"), ("rejected_at", "timestamp")]],
    "transform_runs": [Column(n, t) for n, t in [
        ("run_id", "string"), ("started_at", "timestamp"), ("finished_at", "timestamp"), ("status", "string"),
        ("nodes_ok", "integer"), ("tests_pass", "integer"), ("tests_warn", "integer"), ("tests_fail", "integer"),
        ("errors", "integer"), ("failures", "string")]],
}

PARAM = re.compile(r"(?<![:\w]):([a-z_][a-z0-9_]*)")


class Warehouse(Protocol):
    dialect: str

    def ensure_table(self, dataset: str, table: str, columns: list[Column], partitioned: bool = True,
                     cluster_by: tuple[str, ...] = ()) -> None: ...

    def replace_partition(self, dataset: str, table: str, batch_date: date, uri: str, rows: list[dict],
                          columns: list[Column], cluster_by: tuple[str, ...] = ()) -> int: ...

    def delete_partition(self, dataset: str, table: str, batch_date: date) -> None: ...

    def candidates(self, dataset: str, table: str, column: str, pattern: str,
                   batch_date: date | None = None) -> tuple[int, list[str]]: ...

    def insert(self, dataset: str, table: str, rows: list[dict]) -> None: ...

    def query(self, sql: str, params: dict | None = None) -> list[dict]: ...

    def columns_in(self, datasets: list[str]) -> list[tuple[str, str, str, str]]: ...


def ensure_ops(wh: Warehouse) -> None:
    for name, cols in OPS_TABLES.items():
        wh.ensure_table("ops", name, cols, partitioned=False)
