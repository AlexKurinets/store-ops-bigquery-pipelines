"""Budget connector: finance drops one workbook per fiscal month into a bucket and re-uploads it when corrected.

Each object generation is loaded once and recorded with the batch date that loaded it. A rerun of that batch date
reloads the same generations, so replacing the partition never loses a workbook. Finance's sheets start with title
rows, so the header is found by looking for the "Store" column rather than assuming row 1.
"""

from __future__ import annotations

import io
import re
from datetime import date

from storeops.connectors.base import Connector, ExtractResult
from storeops.http import SourceError
from storeops.landing import ObjectStore

COLUMNS = {"Store": "store_name", "Budget Net Sales": "budget_net_sales", "Budget Labor Hours": "budget_labor_hours",
           "Budget Labor Cost": "budget_labor_cost"}
MONTH = re.compile(r"(\d{4}-\d{2})\.xlsx$")


def parse_workbook(payload: bytes) -> list[dict]:
    from openpyxl import load_workbook

    ws = load_workbook(io.BytesIO(payload), read_only=True, data_only=True).worksheets[0]
    header = None
    out = []
    for row in ws.iter_rows(values_only=True):
        cells = [c for c in row]
        if header is None:
            if cells and "Store" in cells:
                header = {i: COLUMNS[c] for i, c in enumerate(cells) if c in COLUMNS}
            continue
        if not cells or cells[0] in (None, ""):
            continue
        out.append({header[i]: cells[i] for i in header})
    if header is None:
        raise SourceError("budget workbook has no 'Store' header row")
    return out


class BudgetsConnector(Connector):
    source = "budgets"

    def __init__(self, contract, store: ObjectStore, already_loaded: dict[tuple[str, int], date]):
        super().__init__(contract)
        self.store = store
        self.already_loaded = already_loaded

    def extract(self, batch_date: date) -> ExtractResult:
        result = ExtractResult(tables={"budget_store_month": []})
        current = self.store.list("budgets/")
        wanted = [(n, g) for n, g in current if (n, g) not in self.already_loaded]
        wanted += [key for key, d in self.already_loaded.items() if d == batch_date and key not in wanted]
        for name, generation in sorted(wanted):
            m = MONTH.search(name)
            if not m:
                result.notes.append(f"skipped {name}: no fiscal month in the name")
                continue
            for row in parse_workbook(self.store.get(name, generation)):
                result.tables["budget_store_month"].append({**row, "fiscal_month": m.group(1),
                                                            "source_object": name, "source_generation": generation})
            result.processed_objects.append((name, generation))
            result.notes.append(f"{name} generation {generation}")
        return result
