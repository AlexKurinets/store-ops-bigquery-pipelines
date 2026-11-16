"""Landing zone: one gzipped NDJSON object per source table, batch date and run, written before any load.

In production this is Cloud Storage and the BigQuery load job reads the object by URI. Locally the same code talks
to fake-gcs-server through the official client. Restricted rows go to a separate bucket with tighter IAM.
"""

from __future__ import annotations

import gzip
import json
import os
from datetime import date
from pathlib import Path
from typing import Protocol


def landing_path(source: str, table: str, batch_date: date, run_id: str) -> str:
    return f"{source}/{table}/batch_date={batch_date.isoformat()}/run_id={run_id}.ndjson.gz"


def encode(rows: list[dict]) -> bytes:
    body = "".join(json.dumps(r, separators=(",", ":"), default=str) + "\n" for r in rows)
    return gzip.compress(body.encode(), mtime=0)


def decode(payload: bytes) -> list[dict]:
    text = gzip.decompress(payload).decode()
    return [json.loads(line) for line in text.splitlines() if line]


def gcs_client():
    from google.cloud import storage

    if os.environ.get("STORAGE_EMULATOR_HOST"):
        return storage.Client(project="local")  # the client skips authentication against the emulator
    return storage.Client()


class Landing(Protocol):
    def write(self, path: str, rows: list[dict], restricted: bool = False) -> str: ...
    def read(self, uri: str) -> list[dict]: ...


class GcsLanding:
    def __init__(self, bucket: str, restricted_bucket: str):
        self.client = gcs_client()
        self.buckets = {False: self._bucket(bucket), True: self._bucket(restricted_bucket)}

    def _bucket(self, name: str):
        b = self.client.bucket(name)
        if not b.exists():
            b = self.client.create_bucket(name)
        return b

    def write(self, path: str, rows: list[dict], restricted: bool = False) -> str:
        bucket = self.buckets[restricted]
        blob = bucket.blob(path)
        blob.upload_from_string(encode(rows), content_type="application/x-ndjson")
        return f"gs://{bucket.name}/{path}"

    def read(self, uri: str) -> list[dict]:
        bucket, _, path = uri.removeprefix("gs://").partition("/")
        return decode(self.client.bucket(bucket).blob(path).download_as_bytes())


class LocalLanding:
    def __init__(self, directory: Path):
        self.directory = Path(directory)

    def write(self, path: str, rows: list[dict], restricted: bool = False) -> str:
        target = self.directory / ("restricted" if restricted else "landing") / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(encode(rows))
        return target.as_uri()

    def read(self, uri: str) -> list[dict]:
        from urllib.parse import unquote, urlparse

        p = urlparse(uri)
        path = unquote(p.path)
        if os.name == "nt" and path.startswith("/"):
            path = path[1:]
        return decode(Path(path).read_bytes())


class ObjectStore(Protocol):
    def list(self, prefix: str) -> list[tuple[str, int]]: ...
    def get(self, name: str, generation: int) -> bytes: ...


class GcsObjectStore:
    """Read-only view of a source bucket, by object generation, so a re-uploaded file is a new version."""

    def __init__(self, bucket: str):
        self.client = gcs_client()
        self.bucket = self.client.bucket(bucket)

    def list(self, prefix: str) -> list[tuple[str, int]]:
        return [(b.name, int(b.generation)) for b in self.client.list_blobs(self.bucket, prefix=prefix)]

    def get(self, name: str, generation: int) -> bytes:
        return self.bucket.blob(name, generation=generation).download_as_bytes()


class MemoryObjectStore:
    def __init__(self):
        self.objects: dict[str, list[bytes]] = {}

    def put(self, name: str, payload: bytes) -> int:
        self.objects.setdefault(name, []).append(payload)
        return len(self.objects[name])

    def list(self, prefix: str) -> list[tuple[str, int]]:
        return [(n, len(v)) for n, v in sorted(self.objects.items()) if n.startswith(prefix)]

    def get(self, name: str, generation: int) -> bytes:
        return self.objects[name][generation - 1]
