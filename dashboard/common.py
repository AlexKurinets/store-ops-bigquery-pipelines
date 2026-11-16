"""Shared helpers for the dashboard: a read-only connection as dashboard_reader, and the chart palette."""

from __future__ import annotations

import pandas as pd
import psycopg
import streamlit as st
from psycopg.rows import dict_row

from storeops.secrets import provider_from_spec
from storeops.settings import Settings

# Status colors carry state (run outcome, freshness); categorical slots carry identity (source). Both come from a
# validated palette, and every status color is shown with its label, never alone.
STATUS = {"success": "#0ca30c", "late": "#fab219", "quarantined": "#ec835a", "failed": "#d03b3b"}
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
INK_MUTED = "#52514e"


@st.cache_resource
def connection():
    settings = Settings.from_env()
    password = provider_from_spec(settings.secrets).get(f"{settings.pg_user}_password")
    return psycopg.connect(host=settings.pg_host, port=settings.pg_port, dbname=settings.pg_database,
                           user=settings.pg_user, password=password, autocommit=True, row_factory=dict_row)


@st.cache_data(ttl=30)
def q(sql: str, params: tuple | dict | None = None) -> pd.DataFrame:
    with connection().cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    for c in df.columns:
        if df[c].dtype == object and len(df) and type(df[c].dropna().iloc[0]).__name__ == "Decimal":
            df[c] = df[c].astype(float)
    return df


def money(v) -> str:
    if v is None or pd.isna(v):
        return ""
    return f"-${abs(v):,.2f}" if v < 0 else f"${v:,.2f}"


def pct(v, digits: int = 1) -> str:
    return "" if v is None or pd.isna(v) else f"{v * 100:.{digits}f}%"


def safe_md(text: str) -> str:
    """Two dollar signs in one Streamlit markdown string render as LaTeX."""
    return text.replace("$", "\\$")


def evaluated_at() -> str:
    df = q("select max(checked_at) as t from ops.freshness_checks")
    t = df["t"].iloc[0] if len(df) else None
    if t is None or pd.isna(t):
        return ""
    return pd.Timestamp(t).tz_convert("America/Chicago").strftime("%b %d, %Y %H:%M CT")
