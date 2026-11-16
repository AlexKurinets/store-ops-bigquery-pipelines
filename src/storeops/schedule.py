"""Cloud Scheduler semantics: each contract's cron expression, evaluated in its own time zone.

`due` answers which jobs fire at a given time and for which business date, which is what `storeops run-due` uses
locally and what the deploy script turns into Cloud Scheduler jobs.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from croniter import croniter


def fire_time(contract, batch_date: date) -> datetime:
    """When the scheduled run for `batch_date` fires: the first cron time on the following calendar day."""
    tz = ZoneInfo(contract.timezone)
    start = datetime.combine(batch_date + timedelta(days=1), datetime.min.time(), tzinfo=tz)
    return croniter(contract.schedule, start - timedelta(seconds=1)).get_next(datetime)


def due(contracts: dict, at: datetime, window: timedelta = timedelta(minutes=5)) -> list[tuple[str, date, datetime]]:
    out = []
    for source, c in contracts.items():
        tz = ZoneInfo(c.timezone)
        local = at.astimezone(tz)
        prev = croniter(c.schedule, local).get_prev(datetime)
        if local - prev <= window:
            out.append((source, prev.date() - timedelta(days=1), prev))
    return sorted(out, key=lambda x: x[2])
