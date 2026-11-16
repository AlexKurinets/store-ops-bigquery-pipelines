"""Vendor simulator: the POS (REST + OAuth2), the timekeeping system (OData v4), the audit app (REST + API key),
the distributor's SFTP drop and finance's budget bucket, all serving the synthetic data in vendorsim.data.

It also hosts a Slack-style webhook sink so alerts are visible, and a simulated clock the seed script moves
forward one business day at a time. Faults are deterministic, so every fresh stack tells the same story.
"""

from __future__ import annotations

import base64
import json
import os
import re
import secrets
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from vendorsim import data
from vendorsim.data import UTC, iso

START_CLOCK = datetime(2026, 9, 1, 9, 0, tzinfo=UTC)
TOKEN_USES = 25  # the sim POS expires a token after this many calls, so refresh mid-paging gets exercised


@dataclass
class Credentials:
    pos_client_id: str
    pos_client_secret: str
    timekeeping_user: str
    timekeeping_password: str
    audits_api_key: str
    sftp_password: str

    @classmethod
    def from_dir(cls, d: Path) -> Credentials:
        def read(name: str) -> str:
            return (d / name).read_text().strip()

        return cls(read("pos_client_id"), read("pos_client_secret"), read("timekeeping_user"),
                   read("timekeeping_password"), read("audits_api_key"), read("sftp_password"))


@dataclass
class SimState:
    creds: Credentials
    clock: datetime = START_CLOCK
    clock_file: Path | None = None
    tokens: dict[str, int] = field(default_factory=dict)
    faults_fired: set[str] = field(default_factory=set)
    hook_messages: list[dict] = field(default_factory=list)
    ignore_select: bool = False
    request_log: list[str] = field(default_factory=list)
    budget_sink: object | None = None  # callable(name, payload, version) or None

    def set_clock(self, now: datetime) -> None:
        self.clock = now.astimezone(UTC)
        if self.clock_file:
            self.clock_file.write_text(iso(self.clock))
        publish_budgets(self)

    def once(self, key: str) -> bool:
        """True the first time a fault key is seen."""
        if key in self.faults_fired:
            return False
        self.faults_fired.add(key)
        return True


def publish_budgets(state: SimState) -> None:
    if state.budget_sink is None:
        return
    for name, month, version, published in data.BUDGET_PUBLICATIONS:
        if published <= state.clock:
            state.budget_sink(name, month, version)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode()


def decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode().split(":")[1])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, "bad cursor") from exc


@lru_cache(maxsize=64)
def _pos_window(closed_from: str, closed_to: str, as_of: str) -> tuple[dict, ...]:
    lo, hi, now = parse_ts(closed_from), parse_ts(closed_to), parse_ts(as_of)
    out = []
    d = lo.date() - timedelta(days=1)
    while d <= hi.date():
        for s in data.STORES:
            for t in data.transactions(s.key, d):
                if not (closed_from <= t["closed_at"] < closed_to):
                    continue
                seen = data.pos_view(t, now)
                if seen is not None:
                    out.append(seen)
        d += timedelta(days=1)
    out.sort(key=lambda t: (t["closed_at"], t["transaction_id"]))
    return tuple(out)


ODATA_FILTER = re.compile(r"^\s*(\w+)\s+(eq|ne|ge|gt|le|lt)\s+('?[^' ]+'?)\s*$")


def odata_filter(expr: str | None):
    if not expr:
        return lambda row: True
    clauses = []
    for part in re.split(r"\s+and\s+", expr.strip()):
        m = ODATA_FILTER.match(part)
        if not m:
            raise HTTPException(400, f"unsupported $filter clause: {part}")
        fld, op, val = m.groups()
        clauses.append((fld, op, val.strip("'")))

    ops = {"eq": lambda a, b: a == b, "ne": lambda a, b: a != b, "ge": lambda a, b: a >= b,
           "gt": lambda a, b: a > b, "le": lambda a, b: a <= b, "lt": lambda a, b: a < b}

    def keep(row: dict) -> bool:
        for fld, op, val in clauses:
            if fld not in row:
                raise HTTPException(400, f"unknown property {fld}")
            if row[fld] is None or not ops[op](str(row[fld]), val):
                return False
        return True

    return keep


def employee_record(e: data.Employee) -> dict:
    return {"EmployeeId": e.employee_id, "FirstName": e.first_name, "LastName": e.last_name, "SSN": e.ssn,
            "BirthDate": e.birth_date.isoformat(), "HomeAddress": e.home_address,
            "HomeLocationCode": e.home_location, "JobCode": e.job_code, "HireDate": e.hire_date.isoformat(),
            "TerminationDate": None, "PayRate": e.pay_rate, "Notes": e.notes}


def odata_rows(entity: str, clock: datetime, flt) -> list[dict]:
    now = iso(clock)
    rows: list[dict] = []
    if entity == "Employees":
        rows = [employee_record(e) for e in data.employees()]
    elif entity == "Shifts":
        for d in data.business_days() + [data.LAST_DAY + timedelta(days=1)]:
            for s in data.STORES:
                rows.extend(sh for sh in data.shifts(s.key, d) if sh["ModifiedAt"] <= now)
            rows.extend(sh for sh in data.training_shifts(d) if sh["ModifiedAt"] <= now)
    elif entity == "DailySales":
        for d in data.business_days():
            for s in data.STORES:
                rec = data.labor_daily_sales(s.key, d)
                if rec and rec["ImportedAt"] <= now:
                    rows.append(rec)
    else:
        raise HTTPException(404, f"no entity set {entity}")
    return [r for r in rows if flt(r)]


def create_app(creds: Credentials | None = None, *, state_dir: Path | None = None, sftp_root: Path | None = None,
               sftp_port: int | None = None, gcs_bucket: str | None = None) -> FastAPI:
    secrets_dir = Path(os.environ.get("VENDORSIM_SECRETS_DIR", "/secrets"))
    creds = creds or Credentials.from_dir(secrets_dir)
    state = SimState(creds=creds)
    state_dir = state_dir or (Path(os.environ["VENDORSIM_STATE_DIR"]) if "VENDORSIM_STATE_DIR" in os.environ else None)
    hooks_file = state_dir / "hooks.jsonl" if state_dir else None
    if state_dir:
        state.clock_file = state_dir / "clock"
        if state.clock_file.exists():
            state.clock = parse_ts(state.clock_file.read_text().strip())
        if hooks_file.exists():  # alerts survive a restart of the simulator
            state.hook_messages = [json.loads(line) for line in hooks_file.read_text().splitlines() if line]
    gcs_bucket = gcs_bucket or os.environ.get("VENDORSIM_BUDGET_BUCKET")
    sftp_root = sftp_root or (Path(os.environ["VENDORSIM_SFTP_ROOT"]) if "VENDORSIM_SFTP_ROOT" in os.environ else None)
    sftp_port = sftp_port or int(os.environ.get("VENDORSIM_SFTP_PORT", "0") or 0)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        server = None
        if gcs_bucket:
            from vendorsim.publish import gcs_budget_sink

            state.budget_sink = gcs_budget_sink(gcs_bucket)
            publish_budgets(state)
        if sftp_root and sftp_port:
            from vendorsim.sftp import start_sftp, write_distributor_files

            write_distributor_files(sftp_root / "outbound")
            server = await start_sftp(sftp_root, sftp_port, creds.sftp_password, secrets_dir,
                                      state_dir or Path(tempfile.mkdtemp()))
        yield
        if server:
            server.close()

    app = FastAPI(title="vendor simulator", lifespan=lifespan)
    app.state.sim = state

    @app.get("/health")
    def health():
        return {"ok": True, "clock": iso(state.clock)}

    @app.get("/_sim/clock")
    def get_clock():
        return {"now": iso(state.clock)}

    @app.post("/_sim/clock")
    async def set_clock(request: Request):
        body = await request.json()
        state.set_clock(parse_ts(body["now"]))
        return {"now": iso(state.clock)}

    # ------------------------------------------------------------------------------------------- POS (OAuth2)
    @app.post("/pos/oauth/token")
    def token(grant_type: str = Form(...), client_id: str = Form(...), client_secret: str = Form(...)):
        if grant_type != "client_credentials":
            raise HTTPException(400, "unsupported_grant_type")
        if not (secrets.compare_digest(client_id, creds.pos_client_id)
                and secrets.compare_digest(client_secret, creds.pos_client_secret)):
            return JSONResponse({"error": "invalid_client"}, status_code=401)
        tok = secrets.token_urlsafe(24)
        state.tokens[tok] = 0
        return {"access_token": tok, "token_type": "Bearer", "expires_in": 3600}

    @app.get("/pos/v1/transactions")
    def pos_transactions(closed_from: str, closed_to: str, cursor: str | None = None, limit: int = 500,
                         authorization: str | None = Header(default=None)):
        tok = (authorization or "").removeprefix("Bearer ").strip()
        if tok not in state.tokens or state.tokens[tok] >= TOKEN_USES:
            return JSONResponse({"error": "invalid_token"}, status_code=401)
        state.tokens[tok] += 1
        lo, hi = data.PLANTED["pos_outage_utc"]
        if lo <= state.clock < hi:
            return JSONResponse({"error": "service unavailable"}, status_code=503)
        offset = decode_cursor(cursor)
        page = offset // max(limit, 1)
        day = parse_ts(closed_to).day
        if day % 7 == 0 and page == 1 and state.once(f"429|{closed_to}|{iso(state.clock)}"):
            return JSONResponse({"error": "rate limited"}, status_code=429, headers={"Retry-After": "1"})
        if day % 5 == 0 and page == 0 and state.once(f"503|{closed_to}|{iso(state.clock)}"):
            return JSONResponse({"error": "upstream timeout"}, status_code=503)
        limit = min(limit, 500)
        rows = _pos_window(closed_from, closed_to, iso(state.clock))
        chunk = list(rows[offset:offset + limit])
        nxt = offset + limit
        state.request_log.append(f"pos {closed_from} {offset}")
        return {"transactions": chunk, "next_cursor": encode_cursor(nxt) if nxt < len(rows) else None}

    # ------------------------------------------------------------------------------ timekeeping (OData v4)
    @app.get("/timekeeping/odata/{entity}")
    def odata(entity: str, request: Request, authorization: str | None = Header(default=None)):
        expected = base64.b64encode(f"{creds.timekeeping_user}:{creds.timekeeping_password}".encode()).decode()
        if not authorization or not secrets.compare_digest(authorization, f"Basic {expected}"):
            return JSONResponse({"error": {"code": "Unauthorized"}}, status_code=401)
        q = request.query_params
        top = min(int(q.get("$top", "500")), 500)
        skip = int(q.get("$skiptoken", "0"))
        flt = q.get("$filter")
        if entity == "Shifts" and skip == 0 and flt and state.once(f"odata503|{flt}|{iso(state.clock)}"):
            m = re.search(r"ClockIn ge (\S+)", flt)
            if m and parse_ts(m.group(1)).day % 6 == 0:
                return JSONResponse({"error": {"code": "ServiceUnavailable"}}, status_code=503)
        rows = odata_rows(entity, state.clock, odata_filter(flt))
        rows.sort(key=lambda r: tuple(str(v) for v in r.values()))
        select = q.get("$select")
        if select and not state.ignore_select:
            wanted = [s.strip() for s in select.split(",")]
            unknown = [w for w in wanted if rows and w not in rows[0]]
            if unknown:
                raise HTTPException(400, f"unknown properties in $select: {unknown}")
            rows = [{k: r[k] for k in wanted} for r in rows]
        chunk = rows[skip:skip + top]
        body = {"@odata.context": f"{request.base_url}timekeeping/odata/$metadata#{entity}", "value": chunk}
        if skip + top < len(rows):
            params = dict(q)
            params["$skiptoken"] = str(skip + top)
            body["@odata.nextLink"] = str(request.url.include_query_params(**params))
        return body

    # ------------------------------------------------------------------------------------ audits (API key)
    @app.get("/audits/v1/submissions")
    def audits(submitted_from: str, submitted_to: str, page: int = 1, page_size: int = 100,
               x_api_key: str | None = Header(default=None)):
        if not x_api_key or not secrets.compare_digest(x_api_key, creds.audits_api_key):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        now = iso(state.clock)
        lo, hi = parse_ts(submitted_from), parse_ts(submitted_to)
        rows = []
        d = lo.date() - timedelta(days=1)
        while d <= hi.date():
            rows.extend(s for s in data.audit_submissions(d)
                        if submitted_from <= s["submitted_at"] < submitted_to and s["submitted_at"] <= now)
            d += timedelta(days=1)
        rows.sort(key=lambda s: (s["submitted_at"], s["submission_id"]))
        page_size = min(page_size, 100)
        total_pages = max(1, -(-len(rows) // page_size))
        return {"data": rows[(page - 1) * page_size: page * page_size], "page": page, "total_pages": total_pages}

    # ------------------------------------------------------------------------------- Slack-style webhook
    @app.post("/hooks/slack")
    async def hook(request: Request):
        message = await request.json()
        state.hook_messages.append(message)
        if hooks_file:
            with hooks_file.open("a") as f:
                f.write(json.dumps(message) + "\n")
        return {"ok": True}

    @app.get("/hooks/slack")
    def hook_list():
        return {"messages": state.hook_messages}

    return app

