"""Generates docs/freshness.md from the contracts, so the documented SLAs can't drift from the monitored ones."""

from __future__ import annotations

from collections import Counter

from storeops.contracts import load_contracts
from storeops.settings import Settings


def freshness_markdown() -> str:
    contracts = load_contracts(Settings.from_env().contracts_dir)
    out = ["# Source freshness and field classes", "",
           "Generated from `contracts/*.yaml` by `storeops docs`. A test fails if this file is out of date.", "",
           "| Source | System and protocol | Cloud Scheduler | Freshness SLA | Owner |",
           "|---|---|---|---|---|"]
    for c in contracts.values():
        out.append(f"| {c.source} | {c.system}; {c.protocol} | `{c.schedule}` {c.timezone} | "
                   f"{c.describe_freshness()} | {c.owner} |")
    out += ["", "## Fields by class", "",
            "| Table | keep | hash | mask | restrict | drop | Dropped or restricted fields |",
            "|---|---|---|---|---|---|---|"]
    for c in contracts.values():
        for t in c.tables.values():
            n = Counter(f.cls for f in t.fields)
            names = ", ".join(f"`{f.source_name}` ({f.cls})" for f in t.fields if f.cls in ("drop", "restrict"))
            out.append(f"| {t.name} | {n['keep']} | {n['hash']} | {n['mask']} | {n['restrict']} | {n['drop']} | "
                       f"{names or 'none'} |")
    return "\n".join(out) + "\n"
