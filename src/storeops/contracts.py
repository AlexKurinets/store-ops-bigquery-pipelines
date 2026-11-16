"""Field contracts: one YAML file per source system, listing every field the pipeline may keep.

The contract is applied in memory, before anything is written to Cloud Storage. It denies by default: a field the
contract doesn't list is dropped and reported as schema drift, so a vendor adding a column can't widen what reaches
the warehouse. Each listed field has a class:

  keep      typed against the contract and landed
  hash      HMAC-SHA256 with the pseudonym key, so IDs still join across sources
  mask      free text kept after SSNs, card numbers, emails and phone numbers are replaced
  restrict  written only to the restricted dataset, with the table's restricted_key columns
  drop      never written; dropped values are counted per run
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

from storeops import pii

CLASSES = {"keep", "hash", "mask", "restrict", "drop"}
TYPES = {"string", "integer", "numeric", "timestamp", "date", "boolean"}
PII_TAGS = {"ssn", "dob", "name", "address", "email", "phone", "card", "employee_id", "pay", "free_text"}


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class FieldSpec:
    source_name: str
    cls: str
    type: str = "string"
    column: str | None = None
    pii: str | None = None
    note: str | None = None
    description: str | None = None

    @property
    def out(self) -> str:
        return self.column or self.source_name


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str | None = None


@dataclass
class ApplyResult:
    rows: list[dict] = field(default_factory=list)
    restricted_rows: list[dict] = field(default_factory=list)
    actions: Counter = field(default_factory=Counter)  # (field, action) -> values affected
    drift: set[str] = field(default_factory=set)
    masked: Counter = field(default_factory=Counter)  # kind -> values masked


@dataclass(frozen=True)
class TableContract:
    name: str
    description: str
    key: tuple[str, ...]
    fields: tuple[FieldSpec, ...]
    cluster_by: tuple[str, ...] = ()
    restricted_key: tuple[str, ...] = ()
    entity: str | None = None

    def field(self, name: str) -> FieldSpec:
        for f in self.fields:
            if f.source_name == name:
                return f
        raise KeyError(name)

    def requested_fields(self) -> list[str]:
        """Source fields worth asking for, for APIs that support a field list such as OData $select."""
        return [f.source_name for f in self.fields if f.cls != "drop"]

    def columns(self) -> list[Column]:
        return [Column(f.out, f.type, f.description) for f in self.fields if f.cls in ("keep", "hash", "mask")]

    def restricted_columns(self) -> list[Column]:
        if not self.restricted_key:
            return []
        keys = [c for c in self.columns() if c.name in self.restricted_key]
        return keys + [Column(f.out, f.type, f.description) for f in self.fields if f.cls == "restrict"]

    def apply(self, records: list[dict], pseudonym_key: str) -> ApplyResult:
        res = ApplyResult()
        specs = {f.source_name: f for f in self.fields}
        for rec in records:
            row: dict = {}
            restricted: dict = {}
            for name, value in rec.items():
                spec = specs.get(name)
                if spec is None:
                    res.drift.add(name)
                    if value is not None:
                        res.actions[(name, "drop_unlisted")] += 1
                    continue
                if spec.cls == "drop":
                    if value not in (None, ""):
                        res.actions[(name, "drop")] += 1
                    continue
                typed = coerce(value, spec.type, f"{self.name}.{name}")
                if spec.cls == "keep":
                    row[spec.out] = typed
                elif spec.cls == "hash":
                    row[spec.out] = pii.pseudonym(pseudonym_key, typed)
                    if typed is not None:
                        res.actions[(name, "hash")] += 1
                elif spec.cls == "mask":
                    masked, counts = pii.mask(typed)
                    row[spec.out] = masked
                    for kind, n in counts.items():
                        res.masked[kind] += n
                    if counts:
                        res.actions[(name, "mask")] += 1
                elif spec.cls == "restrict":
                    restricted[spec.out] = typed
                    if typed is not None:
                        res.actions[(name, "restrict")] += 1
            for spec in self.fields:  # fields the source omitted land as null
                if spec.cls in ("keep", "hash", "mask"):
                    row.setdefault(spec.out, None)
            res.rows.append(row)
            if self.restricted_key and restricted:
                res.restricted_rows.append({**{k: row[k] for k in self.restricted_key}, **restricted})
        return res


@dataclass(frozen=True)
class Contract:
    source: str
    system: str
    protocol: str
    schedule: str
    timezone: str
    freshness: dict
    lookback_days: int
    tables: dict[str, TableContract]
    business_day: dict
    owner: str
    path: Path

    @property
    def landed_by(self) -> str:
        return self.freshness["landed_by"]

    def describe_freshness(self) -> str:
        if self.freshness["kind"] == "monthly":
            return (f"current month's file loaded by day {self.freshness['day_of_month']}, "
                    f"{self.landed_by} {short_tz(self.timezone)}")
        return f"previous business date loaded by {self.landed_by} {short_tz(self.timezone)}"


def short_tz(tz: str) -> str:
    return {"America/Chicago": "CT", "America/Denver": "MT"}.get(tz, tz)


def coerce(value, typ: str, where: str):
    if value is None or value == "":
        return None
    try:
        if typ == "string":
            return str(value)
        if typ == "integer":
            if isinstance(value, float) and not value.is_integer():
                raise ValueError("not an integer")
            return int(value)
        if typ == "numeric":
            return float(Decimal(str(value)))
        if typ == "boolean":
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("true", "1", "yes", "y")
        if typ == "date":
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            return date.fromisoformat(str(value)[:10]).isoformat()
        if typ == "timestamp":
            if isinstance(value, datetime):
                dt = value
            else:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                raise ValueError("timestamp without an offset")
            return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except (ValueError, InvalidOperation, TypeError) as exc:
        raise ContractError(f"{where}: cannot read {value!r} as {typ} ({exc})") from exc
    raise ContractError(f"{where}: unknown type {typ}")


def _table(name: str, spec: dict, path: Path) -> TableContract:
    fields = []
    for fname, f in spec["fields"].items():
        f = f or {}
        cls = f.get("class")
        if cls not in CLASSES:
            raise ContractError(f"{path.name}:{name}.{fname}: class must be one of {sorted(CLASSES)}")
        typ = f.get("type", "string")
        if typ not in TYPES:
            raise ContractError(f"{path.name}:{name}.{fname}: unknown type {typ}")
        tag = f.get("pii")
        if tag and tag not in PII_TAGS:
            raise ContractError(f"{path.name}:{name}.{fname}: unknown pii tag {tag}")
        fields.append(FieldSpec(fname, cls, typ, f.get("as"), tag, f.get("note"), f.get("description")))
    t = TableContract(name=name, description=spec.get("description", "").strip(), key=tuple(spec.get("key", [])),
                      fields=tuple(fields), cluster_by=tuple(spec.get("cluster_by", [])),
                      restricted_key=tuple(spec.get("restricted_key", [])), entity=spec.get("entity"))
    outs = [c.name for c in t.columns()]
    if len(outs) != len(set(outs)):
        raise ContractError(f"{path.name}:{name}: duplicate output columns")
    missing = [k for k in t.key if k not in outs]
    if missing:
        raise ContractError(f"{path.name}:{name}: key columns {missing} are not landed")
    if any(f.cls == "restrict" for f in fields) and not t.restricted_key:
        raise ContractError(f"{path.name}:{name}: restrict fields need a restricted_key")
    return t


def load_contract(path: Path) -> Contract:
    spec = yaml.safe_load(path.read_text())
    tables = {name: _table(name, t, path) for name, t in spec["tables"].items()}
    return Contract(source=spec["source"], system=spec["system"], protocol=spec["protocol"],
                    schedule=spec["schedule"], timezone=spec["timezone"], freshness=spec["freshness"],
                    lookback_days=int(spec.get("lookback_days", 0)), tables=tables,
                    business_day=spec.get("business_day", {}), owner=spec.get("owner", ""), path=path)


def load_contracts(directory: Path) -> dict[str, Contract]:
    out = {}
    for path in sorted(Path(directory).glob("*.yaml")):
        c = load_contract(path)
        out[c.source] = c
    return out
