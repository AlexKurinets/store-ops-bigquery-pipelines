"""BigQuery adapter: the production target.

Raw tables are day-partitioned on `_batch_date` and clustered on each contract's cluster_by columns. A run loads its
landed object straight from Cloud Storage into the partition decorator (`table$YYYYMMDD`) with WRITE_TRUNCATE, so
the replace is atomic and idempotent. Streaming inserts were tried for POS first and dropped: a retried page after
a timeout produced duplicates that best-effort insertId deduplication didn't catch.

CI exercises this adapter against a fake client (tests/test_bigquery_adapter.py); it is not run against a real
project in this repository.
"""

from __future__ import annotations

from datetime import date, datetime

from storeops.contracts import Column
from storeops.warehouse.base import PARAM, RAW_META

BQ_TYPES = {"string": "STRING", "integer": "INT64", "numeric": "NUMERIC", "timestamp": "TIMESTAMP", "date": "DATE",
            "boolean": "BOOL"}


class BigQueryWarehouse:
    dialect = "bigquery"

    def __init__(self, project: str, location: str = "US", client=None):
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(project=project, location=location)
        self.client = client
        self.project = project
        self.location = location

    def _schema(self, columns: list[Column]):
        from google.cloud import bigquery

        return [bigquery.SchemaField(c.name, BQ_TYPES[c.type], description=c.description or None) for c in columns]

    def _ref(self, dataset: str, table: str) -> str:
        return f"{self.project}.{dataset}.{table}"

    def ensure_table(self, dataset, table, columns, partitioned=True, cluster_by=()):
        from google.cloud import bigquery

        self.client.create_dataset(bigquery.Dataset(f"{self.project}.{dataset}"), exists_ok=True)
        t = bigquery.Table(self._ref(dataset, table), schema=self._schema(list(columns) + (RAW_META if partitioned
                                                                                            else [])))
        if partitioned:
            t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                            field="_batch_date")
            if cluster_by:
                t.clustering_fields = list(cluster_by)
        self.client.create_table(t, exists_ok=True)

    def replace_partition(self, dataset, table, batch_date, uri, rows, columns, cluster_by=()):
        from google.cloud import bigquery

        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=self._schema(list(columns) + RAW_META),
            time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="_batch_date"),
            clustering_fields=list(cluster_by) or None,
            ignore_unknown_values=False,
        )
        dest = f"{self._ref(dataset, table)}${batch_date:%Y%m%d}"
        job = self.client.load_table_from_uri(uri, dest, job_config=config)
        job.result()
        return int(job.output_rows or 0)

    def delete_partition(self, dataset, table, batch_date):
        self.client.delete_table(f"{self._ref(dataset, table)}${batch_date:%Y%m%d}", not_found_ok=True)

    def candidates(self, dataset, table, column, pattern, batch_date=None) -> tuple[int, list[str]]:
        """Counts in BigQuery and returns only values matching the coarse pattern, so a sweep of a large table
        moves a handful of rows, not the column."""
        where = "_batch_date = @d AND " if batch_date else ""
        params = {"p": pattern, **({"d": batch_date} if batch_date else {})}
        ref = f"`{self._ref(dataset, table)}`"
        n = self.query(f"SELECT COUNT(`{column}`) AS n FROM {ref} WHERE {where}TRUE", params)[0]["n"]
        rows = self.query(f"SELECT CAST(`{column}` AS STRING) AS v FROM {ref} "
                          f"WHERE {where}REGEXP_CONTAINS(CAST(`{column}` AS STRING), @p)", params)
        return int(n), [r["v"] for r in rows]

    def insert(self, dataset, table, rows):
        if rows:
            errors = self.client.insert_rows_json(self._ref(dataset, table), [_jsonable(r) for r in rows])
            if errors:
                raise RuntimeError(f"insert into {dataset}.{table} failed: {errors[:3]}")

    def render(self, text: str) -> str:
        for name in ("ops", "raw", "metrics", "conformed"):
            text = text.replace("{" + name + "}", f"`{self.project}.{name}`")
        return PARAM.sub(r"@\1", text)

    def query(self, text: str, params: dict | None = None) -> list[dict]:
        from google.cloud import bigquery

        qp = []
        for k, v in (params or {}).items():
            if isinstance(v, (list, tuple)):
                qp.append(bigquery.ArrayQueryParameter(k, "STRING", list(v)))
            else:
                qp.append(bigquery.ScalarQueryParameter(k, _param_type(v), v))
        job = self.client.query(self.render(text), job_config=bigquery.QueryJobConfig(query_parameters=qp))
        return [dict(r.items()) for r in job.result()]

    def columns_in(self, datasets):
        out = []
        for ds in datasets:
            for r in self.query(f"SELECT table_name, column_name, data_type FROM `{self.project}.{ds}`"
                                f".INFORMATION_SCHEMA.COLUMNS ORDER BY table_name, ordinal_position"):
                out.append((ds, r["table_name"], r["column_name"],
                            "string" if r["data_type"] == "STRING" else r["data_type"].lower()))
        return out



def _param_type(v) -> str:
    if isinstance(v, bool):
        return "BOOL"
    if isinstance(v, int):
        return "INT64"
    if isinstance(v, float):
        return "FLOAT64"
    if isinstance(v, datetime):
        return "TIMESTAMP"
    if isinstance(v, date):
        return "DATE"
    return "STRING"


def _jsonable(row: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, (date, datetime)) else v) for k, v in row.items()}
