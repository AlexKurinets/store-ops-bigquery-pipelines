from datetime import UTC, date, datetime

import pytest
from conftest import CREDS, make_http, memory_budget_sink

from storeops.connectors.base import business_window
from storeops.connectors.budgets import BudgetsConnector
from storeops.connectors.distributor import DistributorConnector, LocalFileSource
from storeops.connectors.pos import PosConnector
from storeops.connectors.timekeeping import TimekeepingConnector
from storeops.http import SourceError
from storeops.landing import MemoryObjectStore
from vendorsim import data
from vendorsim.sftp import write_distributor_files


def set_clock(app, iso: str):
    app.state.sim.set_clock(datetime.fromisoformat(iso))


def test_window_covers_every_stores_business_days(contracts):
    lo, hi = business_window(contracts["pos"], date(2026, 9, 10))
    assert lo == datetime(2026, 9, 8, 9, 0, tzinfo=UTC)  # 04:00 CDT, two days back
    assert hi == datetime(2026, 9, 11, 10, 0, tzinfo=UTC)  # 04:00 MDT the next morning


def test_pos_pages_refresh_the_token_and_honor_retry_after(contracts, sim):
    app, client = sim
    set_clock(app, "2026-09-14T09:30:00+00:00")
    sleeps = []
    conn = PosConnector(contracts["pos"], make_http(client, sleeps), "http://testserver/pos", CREDS.pos_client_id,
                        CREDS.pos_client_secret, page_size=100)
    res = conn.extract(date(2026, 9, 13))  # window ends on the 14th: the sim sends page 2 a 429 with Retry-After: 1
    lines = res.tables["pos_transaction_lines"]
    expected = sum(len(t["lines"]) for d in (date(2026, 9, 11), date(2026, 9, 12), date(2026, 9, 13))
                   for s in data.STORES for t in data.transactions(s.key, d)
                   if data.pos_view(t, datetime(2026, 9, 14, 9, 30, tzinfo=UTC)))
    assert len(lines) >= expected - 400  # the window edges differ by store time zone; the bulk is all there
    assert len({(r["transaction_id"], r["line_number"]) for r in lines}) == len(lines)  # no page read twice
    assert 1.0 in sleeps  # Retry-After was honored
    assert res.token_refreshes >= 3  # about 90 pages; the sim expires a token after 25 calls


def test_pos_hides_late_tickets_until_received(contracts, sim):
    app, client = sim
    store, day, (lo_h, hi_h) = data.PLANTED["late_pos_upload"][0]
    conn = PosConnector(contracts["pos"], make_http(client), "http://testserver/pos", CREDS.pos_client_id,
                        CREDS.pos_client_secret)
    number = data.STORE_BY_KEY[store].pos_number

    def count():
        rows = conn.extract(day).tables["pos_transaction_lines"]
        return len({r["transaction_id"] for r in rows if r["store_number"] == number and r["transaction_id"]
                    .split("-")[1] == f"{day:%Y%m%d}"})

    set_clock(app, "2026-09-13T09:30:00+00:00")
    before = count()
    set_clock(app, "2026-09-14T09:30:00+00:00")
    assert count() > before  # the offline terminal's tickets show up a day later, inside the lookback


def test_pos_outage_fails_after_bounded_retries(contracts, sim):
    app, client = sim
    set_clock(app, "2026-09-24T09:30:00+00:00")
    conn = PosConnector(contracts["pos"], make_http(client), "http://testserver/pos", CREDS.pos_client_id,
                        CREDS.pos_client_secret)
    with pytest.raises(SourceError, match="5 attempts"):
        conn.extract(date(2026, 9, 23))


def test_odata_select_keeps_ssns_off_the_wire(contracts, sim):
    app, client = sim
    set_clock(app, "2026-09-10T10:00:00+00:00")
    conn = TimekeepingConnector(contracts["timekeeping"], make_http(client), "http://testserver/timekeeping/odata",
                                CREDS.timekeeping_user, CREDS.timekeeping_password, page_size=50)
    res = conn.extract(date(2026, 9, 9))
    emp = res.tables["timekeeping_employees"]
    assert len(emp) == len(data.employees()) and all("SSN" not in r and "Notes" not in r for r in emp)
    shifts = res.tables["timekeeping_shifts"]
    expected = {sh["ShiftId"] for d in (date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9)) for s in data.STORES
                for sh in data.shifts(s.key, d) if sh["ModifiedAt"] <= "2026-09-10T10:00:00Z"}
    assert {r["ShiftId"] for r in shifts} >= expected and len(shifts) > 50  # several pages joined by nextLink


def test_contract_still_drops_ssns_if_a_service_ignores_select(contracts, sim):
    app, client = sim
    app.state.sim.ignore_select = True
    set_clock(app, "2026-09-10T10:00:00+00:00")
    conn = TimekeepingConnector(contracts["timekeeping"], make_http(client), "http://testserver/timekeeping/odata",
                                CREDS.timekeeping_user, CREDS.timekeeping_password)
    emp = conn.extract(date(2026, 9, 9)).tables["timekeeping_employees"]
    assert "SSN" in emp[0]  # the service sent everything
    applied = contracts["timekeeping"].tables["timekeeping_employees"].apply(emp, "k")
    assert not any("987-65" in str(v) for r in applied.rows for v in r.values())


def test_distributor_rejects_a_truncated_file_and_loads_the_resend(contracts, tmp_path):
    write_distributor_files(tmp_path)
    res = DistributorConnector(contracts["distributor"], LocalFileSource(tmp_path)).extract(date(2026, 9, 18))
    assert res.rejected == [("invoices_20260918.csv", "SHA-256 does not match the manifest")]
    rows = res.tables["distributor_invoice_lines"]
    assert rows and {r["source_file"] for r in rows} == {"invoices_20260918_v2.csv"}
    assert len(rows) == len(data.invoice_rows(date(2026, 9, 18)))


def test_distributor_without_a_manifest_fails_rather_than_loading_nothing(contracts, tmp_path):
    with pytest.raises(SourceError, match="no manifest"):
        DistributorConnector(contracts["distributor"], LocalFileSource(tmp_path)).extract(date(2026, 9, 18))


def test_budgets_load_each_generation_once_and_reruns_reload_theirs(contracts):
    store = MemoryObjectStore()
    sink = memory_budget_sink(store)
    sink("budgets/store-budget-2026-10.xlsx", "2026-10", 1)
    first = BudgetsConnector(contracts["budgets"], store, {}).extract(date(2026, 10, 1))
    assert first.processed_objects == [("budgets/store-budget-2026-10.xlsx", 1)]
    rows = first.tables["budget_store_month"]
    assert len(rows) == 20 and rows[0]["fiscal_month"] == "2026-10"
    sink("budgets/store-budget-2026-10.xlsx", "2026-10", 2)  # finance re-uploads a correction
    loaded = {("budgets/store-budget-2026-10.xlsx", 1): date(2026, 10, 1)}
    second = BudgetsConnector(contracts["budgets"], store, loaded).extract(date(2026, 10, 8))
    assert second.processed_objects == [("budgets/store-budget-2026-10.xlsx", 2)]
    rerun = BudgetsConnector(contracts["budgets"], store, loaded).extract(date(2026, 10, 1))
    assert ("budgets/store-budget-2026-10.xlsx", 1) in rerun.processed_objects  # a rerun never loses its file
