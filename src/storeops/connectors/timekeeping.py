"""Timekeeping connector: OData v4 with basic auth.

The contract's non-dropped fields become the $select, so SSNs, birth dates and names are never requested and never
cross the wire. The contract is still applied to whatever comes back, in case a service ignores $select.
"""

from __future__ import annotations

import base64
from datetime import date, timedelta

from storeops.connectors.base import Connector, ExtractResult, business_window, z
from storeops.http import RetryingHttp, SourceError


class TimekeepingConnector(Connector):
    source = "timekeeping"

    def __init__(self, contract, http: RetryingHttp, base_url: str, user: str, password: str, page_size: int = 500):
        super().__init__(contract)
        self.page_size = page_size
        self.http = http
        self.base_url = base_url.rstrip("/")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {token}", "Accept": "application/json"}

    def _entity(self, table: str, flt: str | None) -> list[dict]:
        t = self.contract.tables[table]
        params = {"$select": ",".join(t.requested_fields()), "$top": str(self.page_size)}
        if flt:
            params["$filter"] = flt
        url, out = f"{self.base_url}/{t.entity}", []
        while url:
            resp = self.http.get(url, params=params, headers=self.headers)
            if resp.status_code != 200:
                raise SourceError(f"OData {t.entity}: HTTP {resp.status_code}")
            body = resp.json()
            out.extend(body["value"])
            url, params = body.get("@odata.nextLink"), None  # nextLink already carries the query
        return out

    def extract(self, batch_date: date) -> ExtractResult:
        lo, hi = business_window(self.contract, batch_date)
        first = batch_date - timedelta(days=self.contract.lookback_days)
        tables = {
            "timekeeping_shifts": self._entity("timekeeping_shifts", f"ClockIn ge {z(lo)} and ClockIn lt {z(hi)}"),
            "timekeeping_daily_sales": self._entity(
                "timekeeping_daily_sales", f"BusinessDate ge {first.isoformat()} and BusinessDate le {batch_date}"),
            "timekeeping_employees": self._entity("timekeeping_employees", None),
        }
        return ExtractResult(tables=tables, http_retries=self.http.retries)
