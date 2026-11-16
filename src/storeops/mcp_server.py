"""MCP server the agents use to read the metrics dataset.

Four tools: list the tables, describe one (columns and their descriptions, straight from the warehouse), return rows
with store and date filters and a row cap, and report each source's freshness. There is no free-form SQL tool; an
agent asks for a table by name and gets back at most 500 rows.
"""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from storeops.metrics_reader import MetricsReader

INSTRUCTIONS = (
    "Read-only store metrics for a convenience store chain (synthetic data). Start with list_tables, then "
    "describe_table before querying. Business dates run 04:00 to 04:00 local. Call source_freshness first and "
    "say so if a source is late; skip store-days where reconciled is false when exact sales matter."
)


def build_server(reader_factory) -> MCPServer:
    server = MCPServer(name="storeops-metrics", instructions=INSTRUCTIONS, version="1.0.0")

    @server.tool(description="List the metrics tables you can read, with what each one holds.")
    def list_tables() -> list[dict]:
        return reader_factory().tables()

    @server.tool(description="Columns, types and descriptions for one metrics table.")
    def describe_table(table: str) -> dict[str, Any]:
        r = reader_factory()
        desc = next((t["description"] for t in r.tables() if t["table"] == table), "")
        try:
            return {"table": table, "description": desc, "columns": r.columns(table)}
        except ValueError as exc:  # the agent sees the message and can correct itself
            raise ToolError(str(exc)) from exc

    @server.tool(description="Rows from one metrics table. Filter by store_key and by date (YYYY-MM-DD, applied to "
                             "business_date, week_start or fiscal_month). At most 500 rows.")
    def query_table(table: str, columns: list[str] | None = None, store_key: int | None = None,
                    date_from: str | None = None, date_to: str | None = None, limit: int = 100) -> dict[str, Any]:
        try:
            return reader_factory().rows(table, columns, store_key, date_from, date_to, limit)
        except ValueError as exc:
            raise ToolError(str(exc)) from exc

    @server.tool(description="Whether each source system is loaded up to date against its freshness SLA.")
    def source_freshness() -> list[dict]:
        return reader_factory().freshness()

    return server


def _reader_from_env():
    from storeops.secrets import provider_from_spec
    from storeops.settings import Settings
    from storeops.warehouse import connect

    settings = Settings.from_env()
    wh = connect(settings, provider_from_spec(settings.secrets))
    return MetricsReader(wh)


def serve(host: str, port: int) -> None:
    cache = {}

    def factory():
        if "r" not in cache:
            cache["r"] = _reader_from_env()
        return cache["r"]

    build_server(factory).run("streamable-http", host=host, port=port, streamable_http_path="/mcp")
