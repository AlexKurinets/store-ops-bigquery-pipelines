"""Store audit connector: REST with an API key header and page paging. Each submission is flattened to one row
per checklist item, and the free-text comment is masked by the contract before it is written."""

from __future__ import annotations

from datetime import date

from storeops.connectors.base import Connector, ExtractResult, business_window, z
from storeops.http import RetryingHttp, SourceError


class AuditsConnector(Connector):
    source = "audits"

    def __init__(self, contract, http: RetryingHttp, base_url: str, api_key: str):
        super().__init__(contract)
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-Api-Key": api_key}

    def extract(self, batch_date: date) -> ExtractResult:
        lo, hi = business_window(self.contract, batch_date)
        rows, page, pages = [], 1, 1
        while page <= pages:
            resp = self.http.get(f"{self.base_url}/v1/submissions", headers=self.headers,
                                 params={"submitted_from": z(lo), "submitted_to": z(hi), "page": page,
                                         "page_size": 100})
            if resp.status_code != 200:
                raise SourceError(f"audit submissions: HTTP {resp.status_code}")
            body = resp.json()
            pages = body["total_pages"]
            for sub in body["data"]:
                header = {k: v for k, v in sub.items() if k != "items"}
                rows.extend({**header, **item} for item in sub["items"])
            page += 1
        return ExtractResult(tables={"audit_checklist_items": rows}, http_retries=self.http.retries)
