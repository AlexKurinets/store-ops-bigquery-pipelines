"""Postgres stand-in for BigQuery, used locally and in CI.

BigQuery datasets map to Postgres schemas, a partition is the set of rows with one `_batch_date`, and the load job
is a DELETE plus COPY in one transaction, so a reader never sees a half-replaced day.
"""

from __future__ import annotations

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from storeops.warehouse.base import PARAM, RAW_META

PG_TYPES = {"string": "text", "integer": "bigint", "numeric": "numeric", "timestamp": "timestamptz", "date": "date",
            "boolean": "boolean"}
TEXT_TYPES = ("text", "character varying")


class PostgresWarehouse:
    dialect = "postgres"

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)

    @classmethod
    def from_settings(cls, settings, password: str) -> PostgresWarehouse:
        dsn = psycopg.conninfo.make_conninfo(host=settings.pg_host, port=settings.pg_port,
                                             dbname=settings.pg_database, user=settings.pg_user, password=password)
        return cls(dsn)

    def close(self) -> None:
        self.conn.close()

    def ensure_table(self, dataset, table, columns, partitioned=True, cluster_by=()):
        cols = list(columns) + (RAW_META if partitioned else [])
        ident = sql.Identifier(dataset, table)
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(dataset)))
            body = sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c.name), sql.SQL(PG_TYPES[c.type]))
                                      for c in cols)
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(ident, body))
            for c in cols:  # a field added to the contract becomes a new nullable column
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    ident, sql.Identifier(c.name), sql.SQL(PG_TYPES[c.type])))
            if partitioned:
                cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (_batch_date)").format(
                    sql.Identifier(f"{table}_batch_date_idx"), ident))

    def replace_partition(self, dataset, table, batch_date, uri, rows, columns, cluster_by=()):
        names = [c.name for c in columns] + [c.name for c in RAW_META]
        ident = sql.Identifier(dataset, table)
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("DELETE FROM {} WHERE _batch_date = %s").format(ident), (batch_date,))
                stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(ident, sql.SQL(", ").join(map(sql.Identifier, names)))
                with cur.copy(stmt) as copy:
                    for r in rows:
                        copy.write_row(tuple(r.get(n) for n in names))
                cur.execute(sql.SQL("SELECT count(*) AS n FROM {} WHERE _batch_date = %s").format(ident),
                            (batch_date,))
                return cur.fetchone()["n"]

    def delete_partition(self, dataset, table, batch_date):
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {} WHERE _batch_date = %s").format(sql.Identifier(dataset, table)),
                        (batch_date,))

    def candidates(self, dataset, table, column, pattern, batch_date=None) -> tuple[int, list[str]]:
        """Non-null values in a column (or one partition of it), and those matching a coarse regex."""
        where = sql.SQL("_batch_date = %(d)s AND ") if batch_date else sql.SQL("")
        params = {"d": batch_date, "p": pattern}
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT count({c}) AS n FROM {t} WHERE {w} TRUE").format(
                c=sql.Identifier(column), t=sql.Identifier(dataset, table), w=where), params)
            n = cur.fetchone()["n"]
            cur.execute(sql.SQL("SELECT {c}::text AS v FROM {t} WHERE {w} {c}::text ~ %(p)s").format(
                c=sql.Identifier(column), t=sql.Identifier(dataset, table), w=where), params)
            return n, [r["v"] for r in cur]

    def insert(self, dataset, table, rows):
        if not rows:
            return
        names = list(rows[0].keys())
        stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(dataset, table), sql.SQL(", ").join(map(sql.Identifier, names)),
            sql.SQL(", ").join(sql.Placeholder() * len(names)))
        with self.conn.cursor() as cur:
            cur.executemany(stmt, [tuple(r.get(n) for n in names) for r in rows])

    def query(self, text: str, params: dict | None = None) -> list[dict]:
        rendered = PARAM.sub(r"%(\1)s", text.replace("{ops}", "ops").replace("{raw}", "raw")
                             .replace("{metrics}", "metrics").replace("{conformed}", "conformed"))
        with self.conn.cursor() as cur:
            cur.execute(rendered, params or None)  # no params: a literal % in the SQL stays literal
            return cur.fetchall() if cur.description else []

    def columns_in(self, datasets: list[str]) -> list[tuple[str, str, str, str]]:
        rows = self.query("""
            select c.table_schema, c.table_name, c.column_name, c.data_type
            from information_schema.columns c
            join information_schema.tables t using (table_schema, table_name)
            where c.table_schema = any(:schemas)
            order by 1, 2, c.ordinal_position""", {"schemas": datasets})
        return [(r["table_schema"], r["table_name"], r["column_name"],
                 "string" if r["data_type"] in TEXT_TYPES else r["data_type"]) for r in rows]

