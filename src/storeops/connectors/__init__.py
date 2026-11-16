"""Builds the connector for a source from settings and secrets."""

from __future__ import annotations

import httpx

from storeops.http import RetryingHttp


def build(source: str, contract, settings, secrets, ops=None, http_client: httpx.Client | None = None):
    def http() -> RetryingHttp:
        return RetryingHttp(http_client or httpx.Client(timeout=30), max_attempts=settings.max_request_attempts,
                            base_seconds=settings.retry_base_seconds)

    if source == "pos":
        from storeops.connectors.pos import PosConnector

        return PosConnector(contract, http(), settings.pos_base_url, secrets.get("pos_client_id"),
                            secrets.get("pos_client_secret"))
    if source == "timekeeping":
        from storeops.connectors.timekeeping import TimekeepingConnector

        return TimekeepingConnector(contract, http(), settings.timekeeping_base_url, secrets.get("timekeeping_user"),
                                    secrets.get("timekeeping_password"))
    if source == "audits":
        from storeops.connectors.audits import AuditsConnector

        return AuditsConnector(contract, http(), settings.audits_base_url, secrets.get("audits_api_key"))
    if source == "distributor":
        from storeops.connectors.distributor import DistributorConnector, SftpFileSource

        files = SftpFileSource(settings.sftp_host, settings.sftp_port, "distributor", secrets.get("sftp_password"),
                               secrets.get("sftp_host_key.pub"))
        return DistributorConnector(contract, files)
    if source == "budgets":
        from storeops.connectors.budgets import BudgetsConnector
        from storeops.landing import GcsObjectStore

        loaded = ops.loaded_objects("budgets") if ops else {}
        return BudgetsConnector(contract, GcsObjectStore(settings.budget_bucket), loaded)
    raise ValueError(f"unknown source {source}")
