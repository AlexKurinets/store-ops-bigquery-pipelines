"""HTTP with retries: 429 and 5xx back off exponentially with jitter, and Retry-After wins when the server sends it."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field

import httpx

RETRY_STATUS = {429, 500, 502, 503, 504}


class SourceError(RuntimeError):
    pass


@dataclass
class RetryingHttp:
    client: httpx.Client
    max_attempts: int = 5
    base_seconds: float = 1.0
    max_wait: float = 30.0
    sleep: callable = time.sleep
    retries: int = 0
    events: list[str] = field(default_factory=list)

    def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        last = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self.client.request(method, url, **kwargs)
            except httpx.TransportError as exc:
                last = f"{type(exc).__name__}: {exc}"
                resp = None
            if resp is not None and resp.status_code not in RETRY_STATUS:
                return resp
            if resp is not None:
                last = f"HTTP {resp.status_code}"
            if attempt == self.max_attempts:
                break
            wait = self._wait(resp, attempt)
            self.retries += 1
            self.events.append(f"{last}, retry {attempt} in {wait:.1f}s")
            self.sleep(wait)
        raise SourceError(f"{method} {url.split('?')[0]} failed after {self.max_attempts} attempts ({last})")

    def _wait(self, resp: httpx.Response | None, attempt: int) -> float:
        if resp is not None and "Retry-After" in resp.headers:
            try:
                return min(float(resp.headers["Retry-After"]), self.max_wait)
            except ValueError:
                pass
        backoff = self.base_seconds * (2 ** (attempt - 1))
        return min(backoff * random.uniform(0.5, 1.0), self.max_wait)

    def get(self, url: str, **kwargs) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> httpx.Response:
        return self.request("POST", url, **kwargs)


@dataclass
class OAuth2ClientCredentials:
    http: RetryingHttp
    token_url: str
    client_id: str
    client_secret: str
    token: str | None = None
    refreshes: int = 0

    def header(self) -> dict[str, str]:
        if self.token is None:
            resp = self.http.post(self.token_url, data={"grant_type": "client_credentials",
                                                        "client_id": self.client_id,
                                                        "client_secret": self.client_secret})
            if resp.status_code != 200:
                raise SourceError(f"token request failed: HTTP {resp.status_code}")
            self.token = resp.json()["access_token"]
            self.refreshes += 1
        return {"Authorization": f"Bearer {self.token}"}

    def invalidate(self) -> None:
        self.token = None
