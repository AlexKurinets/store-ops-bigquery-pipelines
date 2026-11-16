"""POS connector: REST with OAuth2 client credentials and cursor paging.

A 401 in the middle of paging means the token expired; the connector gets a new one and repeats the same cursor,
so no page is skipped or read twice. Each run reads its business date plus the lookback days, which picks up
tickets from terminals that were offline and voids posted after close.
"""

from __future__ import annotations

from datetime import date

from storeops.connectors.base import Connector, ExtractResult, business_window, z
from storeops.http import OAuth2ClientCredentials, RetryingHttp, SourceError

PAGE_SIZE = 500


class PosConnector(Connector):
    source = "pos"

    def __init__(self, contract, http: RetryingHttp, base_url: str, client_id: str, client_secret: str,
                 page_size: int = PAGE_SIZE):
        super().__init__(contract)
        self.page_size = page_size
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.auth = OAuth2ClientCredentials(http, f"{self.base_url}/oauth/token", client_id, client_secret)

    def extract(self, batch_date: date) -> ExtractResult:
        lo, hi = business_window(self.contract, batch_date)
        lines: list[dict] = []
        cursor = None
        pages = 0
        while True:
            params = {"closed_from": z(lo), "closed_to": z(hi), "limit": self.page_size}
            if cursor:
                params["cursor"] = cursor
            resp = self.http.get(f"{self.base_url}/v1/transactions", params=params, headers=self.auth.header())
            if resp.status_code == 401:
                self.auth.invalidate()
                resp = self.http.get(f"{self.base_url}/v1/transactions", params=params, headers=self.auth.header())
            if resp.status_code != 200:
                raise SourceError(f"POS transactions: HTTP {resp.status_code}")
            body = resp.json()
            pages += 1
            for txn in body["transactions"]:
                header = {k: v for k, v in txn.items() if k != "lines"}
                for line in txn["lines"]:
                    lines.append({**header, **line})
            cursor = body.get("next_cursor")
            if not cursor:
                break
        return ExtractResult(tables={"pos_transaction_lines": lines},
                             notes=[f"{pages} pages, window {z(lo)} to {z(hi)}"],
                             http_retries=self.http.retries, token_refreshes=self.auth.refreshes)
