"""Distributor connector: nightly CSV files over SFTP, checked against the day's manifest.

Every file must match the manifest's SHA-256 and row count. A file that doesn't is rejected and alerted, and when
the distributor resends it with a version suffix (`_v2`), the highest version that passes is the one loaded. The
SFTP host key is pinned, so a man-in-the-middle can't pose as the distributor.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import re
from datetime import date
from pathlib import Path
from typing import Protocol

from storeops.connectors.base import Connector, ExtractResult
from storeops.http import SourceError

KINDS = {"invoices": "distributor_invoice_lines", "counts": "distributor_inventory_counts"}


class FileSource(Protocol):
    def list(self) -> list[str]: ...
    def read(self, name: str) -> bytes: ...


class LocalFileSource:
    def __init__(self, directory: Path):
        self.directory = Path(directory)

    def list(self) -> list[str]:
        return sorted(p.name for p in self.directory.iterdir())

    def read(self, name: str) -> bytes:
        return (self.directory / name).read_bytes()


class SftpFileSource:
    def __init__(self, host: str, port: int, user: str, password: str, host_key_line: str,
                 directory: str = "/outbound"):
        import paramiko

        key_type, key_b64 = host_key_line.split()[:2]
        self.client = paramiko.SSHClient()
        pinned = paramiko.PKey.from_type_string(key_type, base64.b64decode(key_b64))
        self.client.get_host_keys().add(f"[{host}]:{port}" if port != 22 else host, key_type, pinned)
        self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        self.client.connect(host, port=port, username=user, password=password, look_for_keys=False,
                            allow_agent=False, timeout=20)
        self.sftp = self.client.open_sftp()
        self.directory = directory

    def list(self) -> list[str]:
        return sorted(self.sftp.listdir(self.directory))

    def read(self, name: str) -> bytes:
        with self.sftp.open(f"{self.directory}/{name}", "rb") as f:
            return f.read()

    def close(self) -> None:
        self.sftp.close()
        self.client.close()


def version_of(name: str) -> int:
    m = re.search(r"_v(\d+)\.csv$", name)
    return int(m.group(1)) if m else 1


class DistributorConnector(Connector):
    source = "distributor"

    def __init__(self, contract, files: FileSource):
        super().__init__(contract)
        self.files = files

    def extract(self, batch_date: date) -> ExtractResult:
        stamp = f"{batch_date:%Y%m%d}"
        names = self.files.list()
        manifest_name = f"manifest_{stamp}.json"
        if manifest_name not in names:
            raise SourceError(f"no manifest for {batch_date}; the distributor's drop hasn't arrived")
        manifest = {e["name"]: e for e in json.loads(self.files.read(manifest_name))["files"]}
        result = ExtractResult(tables={t: [] for t in KINDS.values()})
        for prefix, table in KINDS.items():
            candidates = sorted((n for n in names if re.fullmatch(rf"{prefix}_{stamp}(_v\d+)?\.csv", n)),
                                key=version_of, reverse=True)
            chosen = None
            for name in candidates:
                payload = self.files.read(name)
                entry = manifest.get(name)
                reason = None
                if entry is None:
                    reason = "not listed in the manifest"
                elif hashlib.sha256(payload).hexdigest() != entry["sha256"]:
                    reason = "SHA-256 does not match the manifest"
                else:
                    rows = list(csv.DictReader(io.StringIO(payload.decode())))
                    if len(rows) != entry["rows"]:
                        reason = f"{len(rows)} rows, manifest says {entry['rows']}"
                if reason:
                    result.rejected.append((name, reason))
                    continue
                if chosen is None:
                    chosen = name
                    result.tables[table] = [{**r, "source_file": name} for r in rows]
            expected = [n for n in manifest if n.startswith(f"{prefix}_{stamp}")]
            if expected and chosen is None:
                raise SourceError(f"every {prefix} file for {batch_date} failed its manifest check")
            if chosen:
                result.notes.append(f"{chosen}: {len(result.tables[table])} rows")
        return result

    def close(self) -> None:
        if hasattr(self.files, "close"):
            self.files.close()
