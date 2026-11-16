"""Drives the synthetic data set through the real pipeline, one simulated morning at a time.

For each business date it moves the vendor simulator's clock to each job's Cloud Scheduler fire time and runs the
job exactly as a deployed Cloud Run job would, with one task retry. A run that still fails gets the runbook's fix,
a rerun for the same date by the on-call person about 100 minutes later. Then dbt builds the warehouse and the
checks run at the frozen evaluation time (STOREOPS_NOW).
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta

import httpx

from storeops import checks, schedule
from storeops.runner import run_source, run_with_retries
from storeops.transform import run_dbt

log = logging.getLogger("storeops")
FIRST_DAY, LAST_DAY = date(2026, 9, 1), date(2026, 10, 30)
ON_CALL_DELAY = timedelta(minutes=100)


class SimClock:
    """The pipeline's notion of now during the seed, kept in step with the vendor simulator's clock."""

    def __init__(self, on_set=None):
        self.on_set = on_set
        self.at = datetime(2026, 9, 1, tzinfo=UTC)
        self.t0 = time.monotonic()

    def set(self, at: datetime) -> None:
        self.at, self.t0 = at.astimezone(UTC), time.monotonic()
        if self.on_set:
            self.on_set(self.at)

    def now(self) -> datetime:
        return self.at + timedelta(seconds=time.monotonic() - self.t0)


def wait_for(url: str, seconds: int = 120) -> None:
    deadline = time.monotonic() + seconds
    while True:
        try:
            if httpx.get(url, timeout=5).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"{url} did not come up")
        time.sleep(1)


def already_seeded(ctx) -> bool:
    for source in ctx.contracts:
        latest = ctx.ops.latest_success(source)
        if latest is None or latest["batch_date"] < LAST_DAY:
            return False
    return True


def http_clock(vendorsim_url: str):
    url = vendorsim_url.rstrip("/")
    wait_for(f"{url}/health")

    def set_remote(at: datetime) -> None:
        httpx.post(f"{url}/_sim/clock", json={"now": at.strftime("%Y-%m-%dT%H:%M:%SZ")}, timeout=60).raise_for_status()

    return set_remote


def seed(ctx, set_vendor_clock=None, first: date = FIRST_DAY, last: date = LAST_DAY, force: bool = False,
         build: bool = True) -> dict:
    if not force and already_seeded(ctx):
        log.info("synthetic data already loaded; skipping (use --force to reload)")
        return {"skipped": True}
    clock = SimClock(set_vendor_clock)
    evaluation_time = ctx.settings.now()
    ctx.now = clock.now
    ctx.advance = lambda delta: clock.set(clock.now() + delta)
    order = sorted(ctx.contracts, key=lambda s: schedule.fire_time(ctx.contracts[s], first).time())
    runs = failed = 0
    day = first
    while day <= last:
        for source in order:
            fire = schedule.fire_time(ctx.contracts[source], day)
            clock.set(fire)
            res = run_with_retries(ctx, source, day, scheduled_for=fire)
            runs += res.attempt
            if res.status == "failed":
                failed += 1
                clock.set(fire + ON_CALL_DELAY)
                run_source(ctx, source, day, trigger="manual_rerun", scheduled_for=clock.now())
                runs += 1
        log.info("loaded business date %s", day)
        day += timedelta(days=1)
    clock.set(evaluation_time)
    ctx.now = lambda: evaluation_time
    summary = {"runs": runs, "failed_after_retry": failed}
    if build:
        summary["dbt"] = run_dbt(ctx)
        summary["freshness"] = checks.freshness(ctx)
        summary["pii"] = checks.pii_sweep(ctx)
        summary["reconciliation"] = checks.reconciliation(ctx)
    return summary
