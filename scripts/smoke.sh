#!/usr/bin/env bash
# Brings the whole stack up from nothing and runs every command block in the README, in order.
set -euo pipefail
cd "$(dirname "$0")/.."
DASHBOARD_PORT="${DASHBOARD_PORT:-8560}"
VENDORSIM_PORT="${VENDORSIM_PORT:-8250}"
out="$(mktemp -d)"

echo "== up (secrets, sources, warehouse, the seed job, MCP, dashboard)"
docker compose up -d --build --wait --wait-timeout 1200

echo "== what the seed job did"
docker compose logs --no-color pipeline > "$out/pipeline.log"
grep -E "failed on attempt|rejected|schema_drift|Done\. PASS|already loaded" "$out/pipeline.log"
grep -qE "Done\. PASS=|already loaded" "$out/pipeline.log"  # a fresh stack builds; an existing one skips

echo "== checks"
docker compose run --rm pipeline storeops check freshness > "$out/freshness.json"
docker compose run --rm pipeline storeops check pii > "$out/pii.json"
docker compose run --rm pipeline storeops check reconciliation > "$out/recon.json"
cat "$out/recon.json"
grep -q '"hits": 0' "$out/pii.json"
grep -q '"meets_target": true' "$out/recon.json"

echo "== rerun one day (replaces its partition)"
docker compose run --rm pipeline storeops run pos --date 2026-10-30 > "$out/rerun.json"
grep -q '"status": "success"' "$out/rerun.json"

echo "== contracts"
docker compose run --rm pipeline storeops contracts

echo "== dashboard"
curl -fsS "http://localhost:${DASHBOARD_PORT}/_stcore/health"
echo

echo "== MCP: what an agent sees"
docker compose exec -T mcp python - <<'PY'
import asyncio
from mcp import Client

async def main():
    async with Client("http://localhost:8000/mcp") as c:
        tables = (await c.call_tool("list_tables", {})).structured_content
        rows = (await c.call_tool("query_table", {"table": "key_reconciliation_daily", "store_key": 3,
                                                  "date_from": "2026-09-12", "date_to": "2026-09-12"}))
        fresh = (await c.call_tool("source_freshness", {})).structured_content
        tables = tables.get("result", tables) if isinstance(tables, dict) else tables
        fresh = fresh.get("result", fresh) if isinstance(fresh, dict) else fresh
        print([t["table"] for t in tables])
        print(rows.structured_content["rows"][0])
        print({f["source"]: f["status"] for f in fresh})
        assert {f["status"] for f in fresh} == {"on_time"}

asyncio.run(main())
PY

echo "== alerts posted to the Slack-style webhook"
curl -fsS "http://localhost:${VENDORSIM_PORT}/hooks/slack" > "$out/hooks.json"
grep -o '"text":"[^"]*"' "$out/hooks.json"

echo "smoke OK"
