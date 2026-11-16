"""Fixtures. Unit tests run anywhere; tests marked `warehouse` need TEST_DATABASE_URL pointing at a Postgres
superuser connection (CI uses a postgres:17 service). They create a throwaway database with the same schemas and
roles as the Compose stack, load the full synthetic data set through the real pipeline once per session, and run
the dbt project against it."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import psycopg
import pytest
from fastapi.testclient import TestClient

from storeops.connectors.audits import AuditsConnector
from storeops.connectors.budgets import BudgetsConnector
from storeops.connectors.distributor import DistributorConnector, LocalFileSource
from storeops.connectors.pos import PosConnector
from storeops.connectors.timekeeping import TimekeepingConnector
from storeops.contracts import load_contracts
from storeops.http import RetryingHttp
from storeops.landing import LocalLanding, MemoryObjectStore
from storeops.secrets import StaticSecrets
from storeops.settings import ROOT, Settings
from vendorsim import data
from vendorsim.app import Credentials, create_app
from vendorsim.sftp import write_distributor_files

CREDS = Credentials("test-client", "test-secret", "svc_test", "tk-pass", "audit-key", "sftp-pass")
SECRETS = {"pos_client_id": CREDS.pos_client_id, "pos_client_secret": CREDS.pos_client_secret,
           "timekeeping_user": CREDS.timekeeping_user, "timekeeping_password": CREDS.timekeeping_password,
           "audits_api_key": CREDS.audits_api_key, "pseudonym_key": "test-pseudonym-key",
           "pipeline_password": "pipe-test", "dashboard_reader_password": "dash-test",
           "agent_reader_password": "agent-test"}
EVALUATED_AT = "2026-10-31T09:30:00-05:00"
TEST_DB = "storeops_test"


@pytest.fixture(scope="session")
def contracts():
    return load_contracts(ROOT / "contracts")


@pytest.fixture
def sim(tmp_path):
    app = create_app(CREDS, state_dir=tmp_path)
    return app, TestClient(app, base_url="http://testserver")


def make_http(client, sleeps=None) -> RetryingHttp:
    return RetryingHttp(client, max_attempts=5, base_seconds=0.0,
                        sleep=(sleeps.append if sleeps is not None else (lambda s: None)))


def connector_factory(contracts, client, files_dir: Path, store: MemoryObjectStore, ops_getter):
    def build(source: str):
        c = contracts[source]
        if source == "pos":
            return PosConnector(c, make_http(client), "http://testserver/pos", CREDS.pos_client_id,
                                CREDS.pos_client_secret)
        if source == "timekeeping":
            return TimekeepingConnector(c, make_http(client), "http://testserver/timekeeping/odata",
                                        CREDS.timekeeping_user, CREDS.timekeeping_password)
        if source == "audits":
            return AuditsConnector(c, make_http(client), "http://testserver/audits", CREDS.audits_api_key)
        if source == "distributor":
            return DistributorConnector(c, LocalFileSource(files_dir))
        if source == "budgets":
            return BudgetsConnector(c, store, ops_getter().loaded_objects("budgets"))
        raise ValueError(source)

    return build


def memory_budget_sink(store: MemoryObjectStore):
    versions: dict[str, int] = {}

    def sink(name: str, month: str, version: int) -> None:
        if versions.get(name, 0) < version:
            store.put(name, data.budget_workbook(month, version))
            versions[name] = version

    return sink


# ------------------------------------------------------------------------------------------------- warehouse
def _admin_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not set")
    return url


def _role_sql(db: str) -> str:
    sql = (ROOT / "db" / "init" / "roles.sql.in").read_text()
    sql = sql.replace(':"db"', f'"{db}"')
    for var, secret in (("pipe", "pipeline_password"), ("dash", "dashboard_reader_password"),
                        ("agent", "agent_reader_password")):
        sql = sql.replace(f":'{var}'", f"'{SECRETS[secret]}'")
    return sql


@pytest.fixture(scope="session")
def pg():
    """A fresh database with the stack's schemas and roles. Yields connection settings for the pipeline role."""
    admin = _admin_url()
    u = urlparse(admin)
    if os.environ.get("STOREOPS_TEST_REUSE_DB") == "1":  # local iteration only: keep a database already seeded
        return {"host": u.hostname, "port": u.port or 5432, "dbname": TEST_DB}
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(f'drop database if exists "{TEST_DB}" with (force)')
        for role in ("pipeline", "dashboard_reader", "agent_reader"):
            conn.execute(f"drop role if exists {role}")
        conn.execute(f'create database "{TEST_DB}"')
    with psycopg.connect(admin.rsplit("/", 1)[0] + f"/{TEST_DB}", autocommit=True) as conn:
        conn.execute(_role_sql(TEST_DB))
    return {"host": u.hostname, "port": u.port or 5432, "dbname": TEST_DB}


def role_conn(pg, role: str):
    return psycopg.connect(host=pg["host"], port=pg["port"], dbname=pg["dbname"], user=role,
                           password=SECRETS[f"{role}_password"], autocommit=True)


def pg_settings(pg, tmp: Path, user: str = "pipeline") -> Settings:
    return Settings(warehouse="postgres", pg_host=pg["host"], pg_port=pg["port"], pg_database=pg["dbname"],
                    pg_user=user, landing="local", local_landing_dir=tmp / "landing", secrets="static",
                    contracts_dir=ROOT / "contracts", dbt_project_dir=ROOT / "transform", now_override=EVALUATED_AT)


@pytest.fixture(scope="session")
def seeded(pg, contracts, tmp_path_factory):
    """The full synthetic data set loaded through the pipeline, built with dbt and checked. Runs once."""
    from storeops.context import build_context
    from storeops.synthetic import seed
    from storeops.warehouse.postgres import PostgresWarehouse

    tmp = tmp_path_factory.mktemp("seed")
    app = create_app(CREDS, state_dir=tmp)
    client = TestClient(app, base_url="http://testserver")
    store = MemoryObjectStore()
    app.state.sim.budget_sink = memory_budget_sink(store)
    files = tmp / "outbound"
    write_distributor_files(files)
    settings = pg_settings(pg, tmp)
    wh = PostgresWarehouse(psycopg.conninfo.make_conninfo(host=pg["host"], port=pg["port"], dbname=pg["dbname"],
                                                          user="pipeline", password=SECRETS["pipeline_password"]))
    holder = {}
    ctx = build_context(settings, secrets=StaticSecrets(SECRETS), wh=wh, landing=LocalLanding(tmp / "landing"),
                        connector_factory=connector_factory(contracts, client, files, store, lambda: holder["ops"]))
    holder["ops"] = ctx.ops
    summary = seed(ctx, app.state.sim.set_clock)
    if summary.get("skipped"):  # reused database: rebuild the summary from what is already there
        from storeops import checks

        ctx.now = settings.now
        app.state.sim.set_clock(settings.now())
        dbt = ctx.wh.query("select * from ops.transform_runs order by finished_at desc limit 1")[0]
        summary = {"dbt": dbt, "freshness": checks.freshness(ctx, alert=False),
                   "reconciliation": checks.reconciliation(ctx, alert=False),
                   "failed_after_retry": len(ctx.wh.query("select 1 from ops.alerts where kind = 'run_failed'"))}
    return {"ctx": ctx, "summary": summary, "app": app, "client": client, "store": store, "files": files,
            "settings": settings, "pg": pg, "tmp": tmp}


@pytest.fixture
def frozen_now():
    return datetime.fromisoformat(EVALUATED_AT).astimezone(UTC)
