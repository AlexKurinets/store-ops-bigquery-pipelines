"""Read-only access to the metrics dataset, with the documentation dbt persisted into the warehouse.

Used by the MCP server. It connects as a role that can read the metrics dataset and nothing else, so even a bug
here can't reach raw, restricted or ops data. Tables and columns are validated against the warehouse catalog
before any identifier goes into SQL.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal

IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
DATE_COLUMNS = ("business_date", "week_start", "fiscal_month")
MAX_ROWS = 500


class MetricsReader:
    def __init__(self, wh, dataset: str = "metrics"):
        self.wh = wh
        self.dataset = dataset

    def tables(self) -> list[dict]:
        if self.wh.dialect == "bigquery":
            rows = self.wh.query("select t.table_name, o.option_value as description from "
                                 "{metrics}.INFORMATION_SCHEMA.TABLES t left join "
                                 "{metrics}.INFORMATION_SCHEMA.TABLE_OPTIONS o on o.table_name = t.table_name "
                                 "and o.option_name = 'description' order by 1")
        else:
            rows = self.wh.query("""
                select c.relname as table_name, obj_description(c.oid, 'pg_class') as description
                from pg_class c join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = :dataset and c.relkind in ('r', 'v') and has_table_privilege(c.oid, 'select')
                order by 1""", {"dataset": self.dataset})
        return [{"table": r["table_name"], "description": (r["description"] or "").strip('"')} for r in rows]

    def columns(self, table: str) -> list[dict]:
        self._check_table(table)
        if self.wh.dialect == "bigquery":
            rows = self.wh.query("select column_name, data_type, description from "
                                 "{metrics}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS where table_name = :t",
                                 {"t": table})
        else:
            rows = self.wh.query("""
                select a.attname as column_name, format_type(a.atttypid, a.atttypmod) as data_type,
                       col_description(a.attrelid, a.attnum) as description
                from pg_attribute a
                join pg_class c on c.oid = a.attrelid
                join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = :dataset and c.relname = :t and a.attnum > 0 and not a.attisdropped
                order by a.attnum""", {"dataset": self.dataset, "t": table})
        return [{"column": r["column_name"], "type": r["data_type"], "description": r["description"] or ""}
                for r in rows]

    def _check_table(self, table: str) -> None:
        if not IDENT.match(table or "") or table not in {t["table"] for t in self.tables()}:
            raise ValueError(f"unknown table {table!r}; call list_tables for the tables you can read")

    def rows(self, table: str, columns: list[str] | None = None, store_key: int | None = None,
             date_from: str | None = None, date_to: str | None = None, limit: int = 100) -> dict:
        cols = [c["column"] for c in self.columns(table)]
        wanted = columns or cols
        bad = [c for c in wanted if c not in cols]
        if bad:
            raise ValueError(f"unknown columns {bad} in {table}; available: {cols}")
        q = '"' if self.wh.dialect == "postgres" else "`"
        where, params = [], {}
        if store_key is not None:
            if "store_key" not in cols:
                raise ValueError(f"{table} has no store_key")
            where.append(f"{q}store_key{q} = :store_key")
            params["store_key"] = int(store_key)
        date_col = next((c for c in DATE_COLUMNS if c in cols), None)
        for bound, op, key in ((date_from, ">=", "date_from"), (date_to, "<=", "date_to")):
            if bound:
                if date_col is None:
                    raise ValueError(f"{table} has no date column to filter on")
                value = date.fromisoformat(bound)
                params[key] = value.strftime("%Y-%m") if date_col == "fiscal_month" else value
                where.append(f"{q}{date_col}{q} {op} :{key}")
        limit = max(1, min(int(limit), MAX_ROWS))
        order = [c for c in (date_col, "store_key") if c and c in cols]
        sql = (f"select {', '.join(f'{q}{c}{q}' for c in wanted)} from {{metrics}}.{q}{table}{q}"
               + (f" where {' and '.join(where)}" if where else "")
               + (f" order by {', '.join(f'{q}{c}{q}' for c in order)}" if order else "")
               + f" limit {limit + 1}")
        out = [{k: _plain(v) for k, v in r.items()} for r in self.wh.query(sql, params)]
        return {"table": table, "rows": out[:limit], "row_count": min(len(out), limit), "truncated": len(out) > limit}

    def freshness(self) -> list[dict]:
        return self.rows("source_freshness", limit=20)["rows"]


def _plain(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v
