"""Connector interface. A connector only extracts: it returns raw records per contract table and never decides
what is kept. The runner applies the contract, lands, loads and records the run."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from storeops.contracts import Contract


@dataclass
class ExtractResult:
    tables: dict[str, list[dict]] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    rejected: list[tuple[str, str]] = field(default_factory=list)  # (file, reason)
    processed_objects: list[tuple[str, int]] = field(default_factory=list)
    http_retries: int = 0
    token_refreshes: int = 0


class Connector:
    source: str = ""

    def __init__(self, contract: Contract):
        self.contract = contract

    def extract(self, batch_date: date) -> ExtractResult:
        raise NotImplementedError

    def close(self) -> None:
        pass


def business_window(contract: Contract, batch_date: date) -> tuple[datetime, datetime]:
    """UTC window covering every store's business days from batch_date - lookback to batch_date.

    A business day runs from the cutoff (04:00) local to the next day's cutoff. With stores in two time zones the
    window starts at the earliest local cutoff and ends at the latest, and overlap between runs is removed in the
    staged layer, which keeps the latest version of each record.
    """
    cutoff = time.fromisoformat(contract.business_day.get("cutoff", "00:00"))
    zones = [ZoneInfo(z) for z in contract.business_day.get("timezones", [contract.timezone])]
    first = batch_date - timedelta(days=contract.lookback_days)
    starts = [datetime.combine(first, cutoff, tzinfo=z).astimezone(UTC) for z in zones]
    ends = [datetime.combine(batch_date + timedelta(days=1), cutoff, tzinfo=z).astimezone(UTC) for z in zones]
    return min(starts), max(ends)


def z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
