"""Runtime settings, read from the environment the way a Cloud Run job receives them."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    warehouse: str = "postgres"  # postgres locally and in CI, bigquery in production
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "warehouse"
    pg_user: str = "pipeline"
    bq_project: str | None = None
    bq_location: str = "US"
    landing: str = "gcs"  # gcs, or local for unit tests
    landing_bucket: str = "storeops-landing"
    restricted_bucket: str = "storeops-landing-restricted"
    local_landing_dir: Path = ROOT / ".landing"
    secrets: str = "file:/secrets"  # file:<dir> (Cloud Run secret mounts) or gcp:<project>
    pos_base_url: str = "http://vendorsim:8000/pos"
    timekeeping_base_url: str = "http://vendorsim:8000/timekeeping/odata"
    audits_base_url: str = "http://vendorsim:8000/audits"
    sftp_host: str = "vendorsim"
    sftp_port: int = 2222
    budget_bucket: str = "finance-drop"
    alert_webhook_url: str | None = None
    retry_base_seconds: float = 1.0
    max_request_attempts: int = 5
    contracts_dir: Path = ROOT / "contracts"
    dbt_project_dir: Path = ROOT / "transform"
    now_override: str | None = None
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            warehouse=_env("STOREOPS_WAREHOUSE", "postgres"),
            pg_host=_env("STOREOPS_PG_HOST", "localhost"),
            pg_port=int(_env("STOREOPS_PG_PORT", "5432")),
            pg_database=_env("STOREOPS_PG_DATABASE", "warehouse"),
            pg_user=_env("STOREOPS_PG_USER", "pipeline"),
            bq_project=_env("STOREOPS_BQ_PROJECT"),
            bq_location=_env("STOREOPS_BQ_LOCATION", "US"),
            landing=_env("STOREOPS_LANDING", "gcs"),
            landing_bucket=_env("STOREOPS_LANDING_BUCKET", "storeops-landing"),
            restricted_bucket=_env("STOREOPS_RESTRICTED_BUCKET", "storeops-landing-restricted"),
            local_landing_dir=Path(_env("STOREOPS_LOCAL_LANDING_DIR", str(ROOT / ".landing"))),
            secrets=_env("STOREOPS_SECRETS", "file:/secrets"),
            pos_base_url=_env("STOREOPS_POS_BASE_URL", "http://vendorsim:8000/pos"),
            timekeeping_base_url=_env("STOREOPS_TIMEKEEPING_BASE_URL", "http://vendorsim:8000/timekeeping/odata"),
            audits_base_url=_env("STOREOPS_AUDITS_BASE_URL", "http://vendorsim:8000/audits"),
            sftp_host=_env("STOREOPS_SFTP_HOST", "vendorsim"),
            sftp_port=int(_env("STOREOPS_SFTP_PORT", "2222")),
            budget_bucket=_env("STOREOPS_BUDGET_BUCKET", "finance-drop"),
            alert_webhook_url=_env("STOREOPS_ALERT_WEBHOOK_URL"),
            retry_base_seconds=float(_env("STOREOPS_RETRY_BASE_SECONDS", "1.0")),
            max_request_attempts=int(_env("STOREOPS_MAX_REQUEST_ATTEMPTS", "5")),
            contracts_dir=Path(_env("STOREOPS_CONTRACTS_DIR", str(ROOT / "contracts"))),
            dbt_project_dir=Path(_env("STOREOPS_DBT_PROJECT_DIR", str(ROOT / "transform"))),
            now_override=_env("STOREOPS_NOW"),
        )

    def now(self) -> datetime:
        """Wall-clock time, or the frozen time the synthetic data set is evaluated at."""
        if self.now_override:
            return datetime.fromisoformat(self.now_override).astimezone(UTC)
        return datetime.now(UTC)
