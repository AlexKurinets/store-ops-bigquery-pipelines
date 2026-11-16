"""Secrets come from Secret Manager in production and from mounted files locally.

Cloud Run can mount a Secret Manager secret as a file, so the file provider is the same code path a deployed job
uses with volume mounts. No credential is stored in this repository: `local_secrets` generates random values on
first start of the Compose stack.
"""

from __future__ import annotations

import secrets as pysecrets
from pathlib import Path
from typing import Protocol

LOCAL_SECRET_NAMES = [
    "pos_client_id", "pos_client_secret", "timekeeping_user", "timekeeping_password", "audits_api_key",
    "sftp_password", "pseudonym_key", "postgres_password", "pipeline_password", "dashboard_reader_password",
    "agent_reader_password",
]


class SecretProvider(Protocol):
    def get(self, name: str) -> str: ...


class FileSecrets:
    def __init__(self, directory: Path):
        self.directory = Path(directory)

    def get(self, name: str) -> str:
        path = self.directory / name
        if not path.exists():
            raise KeyError(f"secret {name} not found in {self.directory}")
        return path.read_text().strip()


class GcpSecretManager:
    """Reads the latest version of each secret; a rotation takes effect on the job's next start."""

    def __init__(self, project: str):
        from google.cloud import secretmanager

        self.project = project
        self.client = secretmanager.SecretManagerServiceClient()
        self._cache: dict[str, str] = {}

    def get(self, name: str) -> str:
        if name not in self._cache:
            path = f"projects/{self.project}/secrets/{name.replace('_', '-')}/versions/latest"
            self._cache[name] = self.client.access_secret_version(name=path).payload.data.decode().strip()
        return self._cache[name]


class StaticSecrets:
    """For tests."""

    def __init__(self, values: dict[str, str]):
        self.values = values

    def get(self, name: str) -> str:
        return self.values[name]


def provider_from_spec(spec: str) -> SecretProvider:
    kind, _, target = spec.partition(":")
    if kind == "file":
        return FileSecrets(Path(target))
    if kind == "gcp":
        return GcpSecretManager(target)
    raise ValueError(f"unknown secrets spec {spec!r}")


def local_secrets(directory: Path) -> list[str]:
    """Create any missing local secret with a random value. Returns the names it created."""
    directory.mkdir(parents=True, exist_ok=True)
    created = []
    for name in LOCAL_SECRET_NAMES:
        path = directory / name
        if path.exists():
            continue
        value = {"timekeeping_user": "svc_storeops", "pos_client_id": f"storeops-{pysecrets.token_hex(4)}"}.get(
            name, pysecrets.token_urlsafe(24))
        path.write_text(value + "\n")
        path.chmod(0o644)
        created.append(name)
    return created
