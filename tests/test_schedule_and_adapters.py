from datetime import UTC, date, datetime
from types import SimpleNamespace

from storeops import checks, schedule
from storeops.contracts import Column
from storeops.warehouse.bigquery import BigQueryWarehouse
from vendorsim import data


def test_fire_time_follows_each_contracts_cron_and_timezone(contracts):
    assert schedule.fire_time(contracts["pos"], date(2026, 10, 30)) == datetime(2026, 10, 31, 10, 15, tzinfo=UTC)
    assert schedule.fire_time(contracts["distributor"], date(2026, 10, 30)) == datetime(2026, 10, 31, 12, 30,
                                                                                         tzinfo=UTC)


def test_due_returns_the_jobs_that_just_fired(contracts):
    jobs = schedule.due(contracts, datetime(2026, 10, 31, 10, 17, tzinfo=UTC))
    assert [(s, d) for s, d, _ in jobs] == [("pos", date(2026, 10, 30))]


def test_expected_daily_batch_flips_at_the_sla(contracts):
    c = contracts["distributor"]  # landed by 09:00 CT
    assert checks.expected_daily_batch(c, datetime(2026, 10, 31, 13, 59, tzinfo=UTC))[0] == date(2026, 10, 29)
    assert checks.expected_daily_batch(c, datetime(2026, 10, 31, 14, 1, tzinfo=UTC))[0] == date(2026, 10, 30)


def test_expected_budget_month_is_due_on_the_third(contracts):
    c = contracts["budgets"]
    assert checks.expected_month(c, datetime(2026, 11, 3, 10, 0, tzinfo=UTC))[0] == "2026-10"
    assert checks.expected_month(c, datetime(2026, 11, 3, 12, 0, tzinfo=UTC))[0] == "2026-11"


class FakeJob:
    output_rows = 1234

    def result(self):
        return self


class FakeClient:
    def __init__(self):
        self.calls = []

    def load_table_from_uri(self, uri, dest, job_config):
        self.calls.append(("load", uri, dest, job_config))
        return FakeJob()

    def query(self, sql, job_config):
        self.calls.append(("query", sql, job_config))
        return SimpleNamespace(result=lambda: [])


def test_bigquery_load_overwrites_one_partition_from_the_landed_object():
    client = FakeClient()
    wh = BigQueryWarehouse("acme-data", client=client)
    cols = [Column("transaction_id", "string"), Column("extended_price", "numeric")]
    n = wh.replace_partition("raw", "pos_transaction_lines", date(2026, 10, 30),
                             "gs://landing/pos/x.ndjson.gz", [], cols, ("store_number",))
    _, uri, dest, cfg = client.calls[0]
    assert n == 1234 and uri == "gs://landing/pos/x.ndjson.gz"
    assert dest == "acme-data.raw.pos_transaction_lines$20261030"
    assert cfg.write_disposition == "WRITE_TRUNCATE" and cfg.source_format == "NEWLINE_DELIMITED_JSON"
    assert cfg.time_partitioning.field == "_batch_date" and cfg.clustering_fields == ["store_number"]
    assert [f.name for f in cfg.schema][-3:] == ["_batch_date", "_run_id", "_loaded_at"]
    assert cfg.schema[1].field_type == "NUMERIC"


def test_bigquery_queries_use_named_parameters_and_qualified_datasets():
    client = FakeClient()
    wh = BigQueryWarehouse("acme-data", client=client)
    wh.query("select * from {ops}.pipeline_runs where source = :source and batch_date >= :d",
             {"source": "pos", "d": date(2026, 10, 1)})
    _, sql, cfg = client.calls[0]
    assert sql == "select * from `acme-data.ops`.pipeline_runs where source = @source and batch_date >= @d"
    assert {(p.name, p.type_) for p in cfg.query_parameters} == {("source", "STRING"), ("d", "DATE")}


def test_labor_system_sales_equal_pos_as_of_its_import():
    for store in (1, 4, 17):
        day = date(2026, 9, 20)
        rec = data.labor_daily_sales(store, day)
        assert rec["NetSales"] == round(sum(data.txn_net(t) for t in data.transactions(store, day)), 2)


def test_planted_problems_are_where_the_docs_say():
    store, day = data.PLANTED["missing_labor_import"]
    assert data.labor_daily_sales(store, day) is None and data.transactions(store, day)
    assert data.STORE_BY_KEY[9].pos_number_on(date(2026, 10, 6)) == "0131"
    assert not data.transactions(9, date(2026, 10, 4))
