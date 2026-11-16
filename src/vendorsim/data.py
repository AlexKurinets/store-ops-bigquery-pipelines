"""Deterministic synthetic data behind the vendor simulator.

Every store, town, person, phone number, card number and figure here is made up. The generator is seeded per
store and business date, so any process (the simulator, the tests) rebuilds exactly the same records.

The planted problems are listed in PLANTED so tests and docs can point at them.
"""

from __future__ import annotations

import csv
import hashlib
import io
import math
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import cache, lru_cache
from zoneinfo import ZoneInfo

SEED = "storeops-synthetic-v1"
FIRST_DAY = date(2026, 9, 1)
LAST_DAY = date(2026, 10, 30)
CENTRAL, MOUNTAIN = "America/Chicago", "America/Denver"
DAY_START_HOUR = 4  # the timekeeping system closes a store's business day at 04:00 local
LABOR_IMPORT_HOUR = 5  # the timekeeping system imports POS daily sales at 05:00 local the next morning

# Problems the synthetic sources carry on purpose, so the pipeline has something real to catch.
PLANTED = {
    "renumbered_store": {"store_key": 9, "old": "0109", "new": "0131", "closed": ("2026-10-03", "2026-10-05")},
    "late_pos_upload": [(3, date(2026, 9, 12), (14, 18)), (12, date(2026, 10, 17), (10, 13))],
    "post_close_void": (8, date(2026, 10, 9)),
    "missing_labor_import": (6, date(2026, 10, 21)),
    "edited_labor_sales": (14, date(2026, 9, 29), 1.03),
    "pos_outage_utc": (datetime(2026, 9, 24, 9, 0, tzinfo=UTC), datetime(2026, 9, 24, 11, 0, tzinfo=UTC)),
    "pos_new_field_from": date(2026, 10, 14),
    "bad_distributor_file": date(2026, 9, 18),
    "budget_correction": {"month": "2026-10", "store_key": 15,
                          "published_utc": datetime(2026, 10, 8, 11, 0, tzinfo=UTC)},
    "unmapped_audit_site": {"site_id": "W8K4", "first_day": date(2026, 10, 27)},
}


def rng(*parts: object) -> random.Random:
    digest = hashlib.sha256("|".join([SEED, *map(str, parts)]).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


@dataclass(frozen=True)
class Store:
    key: int
    name: str
    state: str
    tz: str
    pos_number: str
    location_code: str
    ship_to: str
    site_id: str
    budget_name: str
    open_hour: int
    close_hour: int
    size: float

    @property
    def is_24h(self) -> bool:
        return self.open_hour == 0 and self.close_hour == 24

    def pos_number_on(self, day: date) -> str:
        p = PLANTED["renumbered_store"]
        if self.key == p["store_key"] and day >= date(2026, 10, 6):
            return p["new"]
        return self.pos_number

    def is_closed(self, day: date) -> bool:
        p = PLANTED["renumbered_store"]
        lo, hi = (date.fromisoformat(d) for d in p["closed"])
        return self.key == p["store_key"] and lo <= day <= hi


STORES = [
    Store(1, "Cedar Bend", "TX", CENTRAL, "0101", "TX-214", "ST448210", "A7Q2", "Cedar Bend #1", 5, 23, 1.10),
    Store(2, "Cedar Bend 2", "TX", CENTRAL, "0102", "TX-219", "ST448231", "B3K9", "CEDAR BEND 2", 5, 23, 0.95),
    Store(3, "Holloway", "TX", CENTRAL, "0103", "TX-305", "ST448307", "C8M1", "Holloway", 5, 23, 1.00),
    Store(4, "Harlow Junction", "TX", CENTRAL, "0104", "TX-311", "ST448318", "D2P6", "Harlow Jct", 0, 24, 1.35),
    Store(5, "Brackett Ford", "TX", CENTRAL, "0105", "TX-322", "ST448325", "E5R3", "Brackett Ford", 5, 23, 0.85),
    Store(6, "Juniper Gap", "TX", CENTRAL, "0106", "TX-340", "ST448342", "F9T7", "Juniper Gap TX", 5, 23, 0.90),
    Store(7, "Lark Hollow", "TX", CENTRAL, "0107", "TX-356", "ST448359", "G4V2", "Lark Hollow", 6, 22, 0.75),
    Store(8, "Mesquite Flats", "TX", CENTRAL, "0108", "TX-371", "ST448376", "H6W8", "Mesquite Flats", 5, 23, 1.05),
    Store(9, "Wolf Creek", "TX", CENTRAL, "0109", "TX-388", "ST448390", "J1X5", "Wolf Creek (remodel)", 5, 23, 1.00),
    Store(10, "Bluebonnet Springs", "TX", CENTRAL, "0110", "TX-402", "ST448411", "K7Y3", "Bluebonnet Spgs", 5, 23,
          1.15),
    Store(11, "Kettle Rock", "OK", CENTRAL, "0111", "OK-118", "ST731120", "L2Z9", "Kettle Rock", 5, 23, 0.90),
    Store(12, "Elmwood Bend", "OK", CENTRAL, "0112", "OK-126", "ST731134", "M8A4", "Elmwood Bend", 5, 23, 1.00),
    Store(13, "Garnet Prairie", "OK", CENTRAL, "0113", "OK-133", "ST731141", "N3B6", "Garnet Prairie", 6, 22, 0.80),
    Store(14, "Calder", "OK", CENTRAL, "0114", "OK-147", "ST731155", "P5C1", "Calder", 5, 23, 0.95),
    Store(15, "Prairie Hill", "OK", CENTRAL, "0115", "OK-152", "ST731168", "Q9D7", "Prairie Hill", 5, 23, 1.05),
    Store(16, "Dalby Crossing", "NM", MOUNTAIN, "0116", "NM-061", "ST870063", "R4E2", "Dalby Crossing", 6, 22, 0.85),
    Store(17, "Tesoro Mesa", "NM", MOUNTAIN, "0117", "NM-074", "ST870079", "S6F8", "Tesoro Mesa 24hr", 0, 24, 1.25),
    Store(18, "Coyote Draw", "NM", MOUNTAIN, "0118", "NM-082", "ST870086", "T1G5", "Coyote Draw", 6, 22, 0.80),
    Store(19, "Sable Wells", "NM", MOUNTAIN, "0119", "NM-095", "ST870098", "U7H3", "Sable Wells", 6, 22, 0.90),
    Store(20, "Mica Ridge", "NM", MOUNTAIN, "0120", "NM-103", "ST870105", "V2J9", "Mica Rdg", 6, 22, 0.95),
]
STORE_BY_KEY = {s.key: s for s in STORES}
TRAINING_LOCATION = "TRAIN-01"


@dataclass(frozen=True)
class Item:
    plu: str
    name: str
    category: str
    is_fresh: bool
    price: float
    distributor_item: str | None
    units_per_case: int
    weights: tuple[float, float, float, float, float]  # breakfast, lunch, afternoon, dinner, late night


ITEMS = [
    Item("1001", "Breakfast taco, egg and cheese", "fresh_food", True, 1.99, "D-40118", 6, (9, 1, 0.3, 0.2, 1.5)),
    Item("1002", "Breakfast taco, potato and egg", "fresh_food", True, 1.99, "D-40121", 6, (7, 1, 0.3, 0.2, 1.2)),
    Item("1003", "Sausage kolache", "fresh_food", True, 1.79, "D-40135", 6, (6, 1.5, 0.5, 0.3, 1.0)),
    Item("1004", "Chicken tender basket", "fresh_food", True, 6.99, "D-40142", 6, (0.2, 4, 1.5, 4, 1.5)),
    Item("1005", "Cheeseburger", "fresh_food", True, 5.49, "D-40156", 6, (0.2, 4, 1.2, 3.5, 1.5)),
    Item("1006", "Pizza slice, pepperoni", "fresh_food", True, 2.99, "D-40163", 8, (0.2, 4, 2, 4, 2.5)),
    Item("1007", "Pizza slice, cheese", "fresh_food", True, 2.79, "D-40170", 8, (0.2, 2.5, 1.5, 2.5, 1.5)),
    Item("1008", "Hot dog", "fresh_food", True, 1.99, "D-40184", 8, (0.3, 3, 2.5, 3, 3)),
    Item("1009", "Beef taquito", "fresh_food", True, 1.69, "D-40191", 8, (1, 2.5, 2.5, 2.5, 3)),
    Item("1010", "Chicken fajita wrap", "fresh_food", True, 4.49, "D-40205", 6, (0.3, 3, 1, 2, 0.8)),
    Item("2001", "Coffee, 16 oz", "fountain_coffee", False, 1.89, None, 1, (10, 2, 2, 1, 3)),
    Item("2002", "Coffee, 24 oz", "fountain_coffee", False, 2.29, None, 1, (7, 1.5, 1.5, 0.8, 2.5)),
    Item("2003", "Fountain drink, 32 oz", "fountain_coffee", False, 1.49, None, 1, (1.5, 6, 6, 5, 3)),
    Item("2004", "Iced tea, 32 oz", "fountain_coffee", False, 1.49, None, 1, (1, 3.5, 3.5, 2.5, 1)),
    Item("2005", "Cola, 20 oz bottle", "packaged_beverage", False, 2.29, "D-51012", 24, (2, 5, 5, 4, 3)),
    Item("2006", "Lemon-lime soda, 20 oz bottle", "packaged_beverage", False, 2.29, "D-51029", 24, (1, 3, 3, 2.5, 2)),
    Item("2007", "Spring water, 1 L", "packaged_beverage", False, 1.99, "D-51036", 12, (2, 4, 5, 3, 2)),
    Item("2008", "Energy drink, 16 oz", "packaged_beverage", False, 3.29, "D-51043", 24, (5, 3, 4, 3, 5)),
    Item("2009", "Sports drink, 28 oz", "packaged_beverage", False, 2.49, "D-51050", 15, (1.5, 3, 4, 2.5, 1.5)),
    Item("3001", "Potato chips, 2.5 oz", "packaged_snack", False, 2.19, "D-62014", 48, (1, 4, 4, 3, 3)),
    Item("3002", "Beef jerky, 2.85 oz", "packaged_snack", False, 7.99, "D-62021", 12, (0.8, 1.5, 2, 1.5, 2)),
    Item("3003", "Chocolate bar", "packaged_snack", False, 1.89, "D-62038", 36, (1, 2.5, 3.5, 2.5, 3)),
    Item("3004", "Roasted peanuts", "packaged_snack", False, 1.49, "D-62045", 24, (0.5, 1.5, 2, 1.5, 1.5)),
    Item("3005", "Chocolate chip cookies, 2-pack", "packaged_snack", False, 1.99, "D-62052", 24, (1, 2, 3, 2, 2)),
    Item("3006", "Bag of ice, 10 lb", "other", False, 3.49, None, 1, (0.5, 2, 3, 2, 0.5)),
    Item("3007", "Spearmint gum", "packaged_snack", False, 1.79, "D-62069", 40, (1, 1.5, 1.5, 1.5, 1.5)),
    # Fuel is rung up inside as a prepay line with a variable amount; it is never picked as a basket item.
    Item("5001", "Fuel, regular unleaded", "fuel", False, 0.0, None, 1, (0, 0, 0, 0, 0)),
]
FUEL = ITEMS[-1]
FUEL_SHARE = 0.35
ITEM_BY_PLU = {i.plu: i for i in ITEMS}

HOUR_WEIGHT = {0: 1.4, 1: 1.1, 2: 0.9, 3: 0.9, 4: 1.2, 5: 3, 6: 6, 7: 8, 8: 7, 9: 5, 10: 4, 11: 6, 12: 7, 13: 5,
               14: 4, 15: 5, 16: 6, 17: 6, 18: 5, 19: 4, 20: 3, 21: 3, 22: 2, 23: 2}
DOW_FACTOR = (0.95, 0.93, 0.97, 1.0, 1.12, 1.08, 0.95)  # Monday first


def daypart_index(hour: int) -> int:
    if 4 <= hour <= 9:
        return 0
    if 10 <= hour <= 13:
        return 1
    if 14 <= hour <= 16:
        return 2
    if 17 <= hour <= 20:
        return 3
    return 4


def business_days() -> list[date]:
    return [FIRST_DAY + timedelta(days=i) for i in range((LAST_DAY - FIRST_DAY).days + 1)]


@cache
def zone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def local_dt(store_tz: str, day: date, hour: float) -> datetime:
    """Local wall time `hour` hours after midnight of `day` (hours past 24 run into the next calendar day)."""
    whole = int(hour)
    naive = datetime.combine(day, time(0)) + timedelta(hours=whole, minutes=round((hour - whole) * 60))
    return naive.replace(tzinfo=zone(store_tz))


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    u = dt.astimezone(UTC)
    return f"{u.year:04d}-{u.month:02d}-{u.day:02d}T{u.hour:02d}:{u.minute:02d}:{u.second:02d}Z"


# ---------------------------------------------------------------- employees ----------------------------------------

FIRST_NAMES = ["Avery", "Jordan", "Riley", "Casey", "Morgan", "Quinn", "Reese", "Rowan", "Skyler", "Emerson", "Hayden",
               "Parker", "Dakota", "Kendall", "Logan", "Peyton", "Sawyer", "Tatum", "Blair", "Marlow"]
LAST_NAMES = ["Alder", "Brightwater", "Castell", "Dunmore", "Ellery", "Fairbank", "Garrow", "Hollins", "Iverly",
              "Jessop", "Kirkland", "Lowell", "Merriweather", "Norcross", "Oakhurst", "Pellam", "Quarles", "Rendell"]


@dataclass(frozen=True)
class Employee:
    employee_id: str
    first_name: str
    last_name: str
    ssn: str
    birth_date: date
    home_address: str
    home_location: str
    job_code: str
    hire_date: date
    pay_rate: float
    notes: str
    store_key: int | None


JOB_PAY = {"MGR": (21.0, 24.0), "LEAD": (15.0, 17.5), "CASH": (11.5, 13.0), "FOOD": (12.0, 14.0), "TRAIN": (19, 21)}


@lru_cache(maxsize=1)
def employees() -> tuple[Employee, ...]:
    out: list[Employee] = []
    used: set[str] = set()
    notes_planted = {(3, 2), (11, 4), (17, 1)}  # SSNs typed into the free-text notes field
    for s in STORES:
        r = rng("employees", s.key)
        mix = ["MGR", "LEAD", "LEAD", "LEAD"] + ["CASH"] * (8 if s.is_24h else 5) + ["FOOD"] * (5 if s.is_24h else 3)
        for i, job in enumerate(mix):
            eid = str(r.randint(100000, 999999))
            while eid in used:
                eid = str(r.randint(100000, 999999))
            used.add(eid)
            lo, hi = JOB_PAY[job]
            ssn = f"987-65-432{r.randint(0, 9)}"  # SSA's range reserved for advertising; never a real number
            out.append(Employee(
                employee_id=eid, first_name=r.choice(FIRST_NAMES), last_name=r.choice(LAST_NAMES), ssn=ssn,
                birth_date=date(r.randint(1966, 2006), r.randint(1, 12), r.randint(1, 28)),
                home_address=f"{r.randint(100, 9899)} Example Ave, {s.name}, {s.state}",
                home_location=s.location_code, job_code=job,
                hire_date=date(r.randint(2016, 2026), r.randint(1, 8), r.randint(1, 28)),
                pay_rate=round(r.uniform(lo, hi), 2),
                notes=(f"Direct deposit form pending. SSN {ssn} confirmed by phone." if (s.key, i) in notes_planted
                       else ""),
                store_key=s.key))
    r = rng("employees", "trainer")
    out.append(Employee("100777", "Sloane", "Whitcombe", "987-65-4329", date(1984, 3, 14),
                        "77 Example Ave, Cedar Bend, TX", TRAINING_LOCATION, "TRAIN", date(2019, 5, 6),
                        round(r.uniform(19, 21), 2), "", None))
    return tuple(out)


def store_employees(store_key: int) -> dict[str, list[Employee]]:
    by_job: dict[str, list[Employee]] = {}
    for e in employees():
        if e.store_key == store_key:
            by_job.setdefault(e.job_code, []).append(e)
    return by_job


# ------------------------------------------------------------------- shifts ----------------------------------------

def _slots(s: Store, day: date) -> list[tuple[str, float, float]]:
    if s.is_24h:
        slots = [("LEAD", 5.75, 14.25), ("CASH", 6, 14.5), ("FOOD", 5.5, 12.5), ("LEAD", 13.75, 22.25),
                 ("CASH", 14, 22.5), ("FOOD", 15, 21), ("LEAD", 21.75, 30.25), ("CASH", 22, 30.5)]
    elif s.open_hour == 5:
        slots = [("LEAD", 4.5, 13.0), ("FOOD", 5, 12), ("CASH", 10.5, 18.5), ("LEAD", 14.5, 23.5),
                 ("CASH", 17, 23)]
    else:
        slots = [("LEAD", 5.5, 14), ("FOOD", 6, 12), ("CASH", 10, 18), ("LEAD", 13.5, 22.5)]
    if day.weekday() < 5:
        slots.append(("MGR", 8, 17))
    return slots


@cache
def shifts(store_key: int, day: date) -> tuple[dict, ...]:
    """Shifts that start on business date `day` at a store (times are UTC instants)."""
    s = STORE_BY_KEY[store_key]
    if s.is_closed(day):
        return ()
    r = rng("shifts", store_key, day)
    staff = store_employees(store_key)
    day_index = (day - FIRST_DAY).days
    out = []
    for slot_index, (job, start, end) in enumerate(_slots(s, day)):
        pool = staff[job]
        emp = pool[(day_index + slot_index * 3) % len(pool)]
        clock_in = local_dt(s.tz, day, start) + timedelta(minutes=r.randint(-8, 6))
        clock_out = local_dt(s.tz, day, end) + timedelta(minutes=r.randint(-5, 12))
        hours = (clock_out - clock_in).total_seconds() / 3600
        out.append({
            "ShiftId": f"SH-{s.location_code}-{day:%Y%m%d}-{slot_index:02d}",
            "EmployeeId": emp.employee_id, "LocationCode": s.location_code, "JobCode": job,
            "ClockIn": iso(clock_in), "ClockOut": iso(clock_out), "BreakMinutes": 30 if hours > 6 else 0,
            "ModifiedAt": iso(clock_out + timedelta(minutes=r.randint(1, 20))),
        })
    return tuple(out)


@cache
def training_shifts(day: date) -> tuple[dict, ...]:
    if day.weekday() not in (1, 3):
        return ()
    ci = local_dt(CENTRAL, day, 9)
    co = local_dt(CENTRAL, day, 15)
    return ({"ShiftId": f"SH-{TRAINING_LOCATION}-{day:%Y%m%d}-00", "EmployeeId": "100777",
             "LocationCode": TRAINING_LOCATION, "JobCode": "TRAIN", "ClockIn": iso(ci), "ClockOut": iso(co),
             "BreakMinutes": 0, "ModifiedAt": iso(co + timedelta(minutes=5))},)


@cache
def _register_staff(store_key: int, day: date) -> tuple[tuple[str, str, str], ...]:
    """(clock in, clock out, employee) for shifts that can ring sales on business date `day`."""
    out = []
    for d in (day - timedelta(days=1), day):
        out.extend((sh["ClockIn"], sh["ClockOut"], sh["EmployeeId"]) for sh in shifts(store_key, d)
                   if sh["JobCode"] in ("LEAD", "CASH"))
    return tuple(out)


def _on_shift(store_key: int, day: date, at_iso: str) -> list[str]:
    return [e for ci, co, e in _register_staff(store_key, day) if ci <= at_iso <= co]


# ------------------------------------------------------------- transactions ----------------------------------------

@cache
def transactions(store_key: int, day: date) -> tuple[dict, ...]:
    """POS transactions for business date `day`, in their final state (voids applied, late uploads included)."""
    s = STORE_BY_KEY[store_key]
    if s.is_closed(day) or day < FIRST_DAY:  # the synthetic world's POS history starts on FIRST_DAY
        return ()
    r = rng("pos", store_key, day)
    count = round(150 * s.size * DOW_FACTOR[day.weekday()] * r.uniform(0.92, 1.08))
    hours = list(range(4, 28)) if s.is_24h else list(range(s.open_hour, s.close_hour))
    weights = [HOUR_WEIGHT[h % 24] for h in hours]
    late = {(k, d): window for k, d, window in PLANTED["late_pos_upload"]}.get((store_key, day))
    picks = sorted(r.choices(hours, weights=weights, k=count))
    item_cum = _item_cum_weights()
    number = s.pos_number_on(day)
    out = []
    for i, hour in enumerate(picks):
        closed = local_dt(s.tz, day, hour) + timedelta(minutes=r.randint(0, 59), seconds=r.randint(0, 59))
        received = closed + timedelta(seconds=r.randint(2, 90))
        if late and late[0] <= hour < late[1]:
            received = closed + timedelta(hours=26)
        part = daypart_index(hour % 24)
        n_lines = r.choices([1, 2, 3, 4], weights=[45, 33, 15, 7])[0]
        chosen = r.choices(ITEMS, cum_weights=item_cum[part], k=n_lines)
        lines = []
        if r.random() < FUEL_SHARE:
            amount = round(r.uniform(15, 65), 2)
            lines.append({"line_number": 1, "plu": FUEL.plu, "description": FUEL.name, "quantity": 1,
                          "unit_price": amount, "extended_price": amount})
        for j, it in enumerate(chosen, start=len(lines) + 1):
            qty = 2 if r.random() < 0.12 else 1
            lines.append({"line_number": j, "plu": it.plu, "description": it.name, "quantity": qty,
                          "unit_price": it.price, "extended_price": round(qty * it.price, 2)})
        tender = r.choices(["card", "cash", "mobile"], weights=[62, 30, 8])[0]
        operators = _on_shift(store_key, day, iso(closed))
        txn = {
            "transaction_id": f"{number}-{day:%Y%m%d}-{i + 1:04d}",
            "store_number": number, "register_id": f"R{r.randint(1, 3)}",
            "operator_id": r.choice(operators) if operators else None,
            "closed_at": iso(closed), "received_at": iso(received), "voided_at": None,
            "updated_at": iso(received), "tender_type": tender,
            "card_number": f"************{r.randint(0, 9999):04d}" if tender == "card" else None,
            "lines": lines,
        }
        if day >= PLANTED["pos_new_field_from"]:
            txn["loyalty_member_id"] = f"LM{r.randint(10**7, 10**8 - 1)}" if r.random() < 0.2 else None
        out.append(txn)
    if (store_key, day) == PLANTED["post_close_void"]:
        # A large order rung up in the evening and voided the next morning, after the labor system's import.
        evening = [t for t in out if t["closed_at"] > iso(local_dt(s.tz, day, 18))]
        t = evening[0]
        t["lines"] = [{"line_number": 1, "plu": "3002", "description": ITEM_BY_PLU["3002"].name, "quantity": 4,
                       "unit_price": 7.99, "extended_price": 31.96},
                      {"line_number": 2, "plu": "2008", "description": ITEM_BY_PLU["2008"].name, "quantity": 2,
                       "unit_price": 3.29, "extended_price": 6.58}]
        voided = local_dt(s.tz, day + timedelta(days=1), 9.25)
        t["voided_at"] = iso(voided)
        t["updated_at"] = iso(voided)
    return tuple(out)


@lru_cache(maxsize=1)
def _item_cum_weights() -> tuple[tuple[float, ...], ...]:
    from itertools import accumulate

    return tuple(tuple(accumulate(it.weights[p] for it in ITEMS)) for p in range(5))


def txn_net(t: dict) -> float:
    return round(sum(line["extended_price"] for line in t["lines"]), 2)


def pos_view(t: dict, as_of: datetime) -> dict | None:
    """What the POS API returns for a transaction at time `as_of`: hidden until received, void shown once voided."""
    if t["received_at"] > iso(as_of):
        return None
    if t["voided_at"] and t["voided_at"] > iso(as_of):
        return {**t, "voided_at": None, "updated_at": t["received_at"]}
    return t


def labor_import_time(s: Store, day: date) -> datetime:
    return local_dt(s.tz, day + timedelta(days=1), LABOR_IMPORT_HOUR)


@cache
def labor_daily_sales(store_key: int, day: date) -> dict | None:
    """The timekeeping system's own copy of daily sales, imported from the POS at 05:00 the next morning."""
    s = STORE_BY_KEY[store_key]
    if s.is_closed(day) or (store_key, day) == PLANTED["missing_labor_import"]:
        return None
    imported = labor_import_time(s, day)
    total = 0.0
    for t in transactions(store_key, day):
        seen = pos_view(t, imported)
        if seen and not seen["voided_at"]:
            total += txn_net(seen)
    k, d, factor = PLANTED["edited_labor_sales"]
    if (store_key, day) == (k, d):
        total *= factor
    return {"LocationCode": s.location_code, "BusinessDate": day.isoformat(), "NetSales": round(total, 2),
            "ImportedAt": iso(imported)}


@cache
def units_sold(store_key: int, day: date) -> dict[str, int]:
    out: dict[str, int] = {}
    for t in transactions(store_key, day):
        if t["voided_at"]:
            continue
        for line in t["lines"]:
            out[line["plu"]] = out.get(line["plu"], 0) + line["quantity"]
    return out


# ------------------------------------------------------------------- audits ----------------------------------------

OPENING_ITEMS = [("OPN-01", "Coffee station clean and stocked", "pass_fail"),
                 ("OPN-02", "Hot case at or above 140F", "temp_hot"),
                 ("OPN-03", "Walk-in cooler at or below 41F", "temp_cold"),
                 ("OPN-04", "Restrooms checked", "pass_fail"),
                 ("OPN-05", "Fuel pump screens working", "pass_fail"),
                 ("OPN-06", "Sales floor swept", "pass_fail"),
                 ("OPN-07", "Safe count matches", "pass_fail"),
                 ("OPN-08", "Food labels dated", "pass_fail")]
SAFETY_ITEMS = [(f"FS-{i:02d}", q, k) for i, (q, k) in enumerate([
    ("Hand sink stocked", "pass_fail"), ("Sanitizer at correct strength", "pass_fail"),
    ("Hot holding at or above 140F", "temp_hot"), ("Cold holding at or below 41F", "temp_cold"),
    ("Thermometers calibrated", "pass_fail"), ("Raw and ready-to-eat stored apart", "pass_fail"),
    ("Date labels on prepped food", "pass_fail"), ("Pest control log current", "pass_fail"),
    ("Food handler cards on file", "pass_fail"), ("Grease trap log current", "pass_fail")], start=1)]
BENIGN_COMMENTS = ["Restocked lids", "Replaced bulb over pump 4", "Cooler door seal worn, ticket opened",
                   "Hot case recalibrated", "Mopped after delivery", "Low on cups, ordered", "Fixed shelf tag"]
PII_COMMENTS = {
    (3, date(2026, 9, 15)): "Customer left a card at register 2, number 4111 1111 1111 1111, put in safe",
    (12, date(2026, 10, 2)): "Call the repair tech at 555-0142 about the hot case",
    (17, date(2026, 10, 20)): "Employee gave SSN 987-65-4325 for the vendor form, told them not to write it here",
    (5, date(2026, 9, 28)): "Send photos to areamanager@example.com",
}


def _submission(s_tz: str, site: str, store_key: int | None, day: date, kind: str, hour: float, r) -> dict:
    items = OPENING_ITEMS if kind == "opening" else SAFETY_ITEMS
    out_items = []
    for code, question, check in items:
        temp = None
        if check == "temp_hot":
            temp = round(r.uniform(136, 168), 1)
            result = "pass" if temp >= 140 else "fail"
        elif check == "temp_cold":
            temp = round(r.uniform(33, 43), 1)
            result = "pass" if temp <= 41 else "fail"
        else:
            result = "fail" if r.random() < 0.035 else "pass"
        comment = r.choice(BENIGN_COMMENTS) if r.random() < 0.06 else None
        out_items.append({"item_code": code, "question": question, "result": result, "temperature_f": temp,
                          "comment": comment})
    planted = PII_COMMENTS.get((store_key, day)) if kind == "opening" else None
    if planted:
        out_items[0]["comment"] = planted
    submitted = local_dt(s_tz, day, hour) + timedelta(minutes=r.randint(20, 55))
    first, last = r.choice(FIRST_NAMES), r.choice(LAST_NAMES)
    return {"submission_id": f"SUB-{site}-{day:%Y%m%d}-{kind[:3].upper()}", "site_id": site, "checklist": kind,
            "submitted_at": iso(submitted), "submitted_by_name": f"{first} {last}",
            "submitted_by_email": f"{first.lower()}.{last.lower()}@example.com", "items": out_items}


@cache
def audit_submissions(day: date) -> tuple[dict, ...]:
    out = []
    if day < FIRST_DAY:
        return ()
    for s in STORES:
        if s.is_closed(day):
            continue
        r = rng("audits", s.key, day)
        if r.random() >= 0.03:
            out.append(_submission(s.tz, s.site_id, s.key, day, "opening", 6 if s.is_24h else s.open_hour, r))
        if day.weekday() == 2:
            out.append(_submission(s.tz, s.site_id, s.key, day, "food_safety", 14, r))
    site = PLANTED["unmapped_audit_site"]
    if day >= site["first_day"]:
        out.append(_submission(CENTRAL, site["site_id"], None, day, "opening", 7, rng("audits", "new", day)))
    return tuple(out)


# --------------------------------------------------------------- distributor ---------------------------------------

DRIVERS = ["Hollis Brand", "Marisol Teague", "Dewey Fairchild", "Lorna Pickett"]
INVOICE_COLUMNS = ["invoice_no", "line_no", "ship_to", "delivery_date", "item_number", "description", "cases",
                   "units_per_case", "case_cost", "extended_cost", "driver_name"]
COUNT_COLUMNS = ["ship_to", "count_date", "item_number", "units_on_hand"]


DELIVERY_DAYS = {0: 2, 2: 2, 4: 3}  # Monday covers Monday and Tuesday, Wednesday two days, Friday the weekend


def invoice_rows(day: date) -> list[dict]:
    if day.weekday() not in DELIVERY_DAYS:
        return []
    rows = []
    for s in STORES:
        if s.is_closed(day):
            continue
        r = rng("invoice", s.key, day)
        line = 0
        if day.weekday() not in DELIVERY_DAYS:
            continue
        cover: dict[str, int] = {}
        for k in range(DELIVERY_DAYS[day.weekday()]):
            for plu, n in units_sold(s.key, day + timedelta(days=k)).items():
                cover[plu] = cover.get(plu, 0) + n
        for it in ITEMS:
            if not it.distributor_item:
                continue
            need = cover.get(it.plu, 0) * (r.uniform(1.04, 1.12) if it.is_fresh else r.uniform(1.0, 1.1))
            cases = math.ceil(need / it.units_per_case) if need > 0 else 0
            if cases == 0:
                continue
            line += 1
            case_cost = round(it.price * 0.62 * it.units_per_case, 2)
            rows.append({"invoice_no": f"INV{s.ship_to[-4:]}{day:%m%d}", "line_no": line, "ship_to": s.ship_to,
                         "delivery_date": day.isoformat(), "item_number": it.distributor_item,
                         "description": it.name.upper(), "cases": cases, "units_per_case": it.units_per_case,
                         "case_cost": case_cost, "extended_cost": round(case_cost * cases, 2),
                         "driver_name": DRIVERS[s.key % len(DRIVERS)]})
    return rows


def count_rows(day: date) -> list[dict]:
    if day.weekday() != 0:
        return []
    rows = []
    for s in STORES:
        if s.is_closed(day):
            continue
        r = rng("counts", s.key, day)
        for it in ITEMS:
            if it.distributor_item:
                rows.append({"ship_to": s.ship_to, "count_date": day.isoformat(), "item_number": it.distributor_item,
                             "units_on_hand": r.randint(0, it.units_per_case * 2)})
    return rows


def to_csv(rows: list[dict], columns: list[str]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()


def distributor_files(day: date) -> dict[str, bytes]:
    """Files the distributor drops for deliveries made on `day`, including the manifest."""
    import json

    files: dict[str, bytes] = {}
    entries = []
    inv = invoice_rows(day)
    if inv:
        full = to_csv(inv, INVOICE_COLUMNS)
        name = f"invoices_{day:%Y%m%d}.csv"
        entries.append({"name": name, "sha256": hashlib.sha256(full).hexdigest(), "rows": len(inv)})
        if day == PLANTED["bad_distributor_file"]:
            files[name] = to_csv(inv[:-15], INVOICE_COLUMNS)  # cut off mid-transfer
            v2 = f"invoices_{day:%Y%m%d}_v2.csv"
            files[v2] = full
            entries.append({"name": v2, "sha256": hashlib.sha256(full).hexdigest(), "rows": len(inv)})
        else:
            files[name] = full
    cnt = count_rows(day)
    if cnt:
        data = to_csv(cnt, COUNT_COLUMNS)
        name = f"counts_{day:%Y%m%d}.csv"
        files[name] = data
        entries.append({"name": name, "sha256": hashlib.sha256(data).hexdigest(), "rows": len(cnt)})
    files[f"manifest_{day:%Y%m%d}.json"] = json.dumps({"delivery_date": day.isoformat(), "files": entries},
                                                       indent=2).encode()
    return files


# ------------------------------------------------------------------- budgets ---------------------------------------

def budget_rows(month: str, version: int = 1) -> list[dict]:
    year, mon = map(int, month.split("-"))
    first = date(year, mon, 1)
    nxt = date(year + (mon == 12), mon % 12 + 1, 1)
    days = [first + timedelta(days=i) for i in range((nxt - first).days)]
    rows = []
    for s in STORES:
        r = rng("budget", s.key, month)
        ref_days = [d for d in days if FIRST_DAY <= d <= LAST_DAY] or [d - timedelta(days=30) for d in days]
        sales = sum(sum(txn_net(t) for t in transactions(s.key, d)) for d in ref_days)
        hours = sum(sum(_shift_hours(sh) for sh in shifts(s.key, d)) for d in ref_days)
        scale = len(days) / len(ref_days)
        labor_hours = round(hours * scale * r.uniform(0.95, 1.03))
        p = PLANTED["budget_correction"]
        if month == p["month"] and s.key == p["store_key"] and version == 1:
            labor_hours *= 10  # a typo finance corrected a week into the month
        rows.append({"Store": s.budget_name, "Budget Net Sales": round(sales * scale * r.uniform(0.96, 1.05), -1),
                     "Budget Labor Hours": labor_hours,
                     "Budget Labor Cost": round(hours * scale * 14.6 * r.uniform(0.97, 1.03), -1)})
    return rows


def _shift_hours(sh: dict) -> float:
    ci = datetime.fromisoformat(sh["ClockIn"].replace("Z", "+00:00"))
    co = datetime.fromisoformat(sh["ClockOut"].replace("Z", "+00:00"))
    return (co - ci).total_seconds() / 3600 - sh["BreakMinutes"] / 60


def budget_workbook(month: str, version: int = 1) -> bytes:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Store budget"
    ws.append([f"Store budget, fiscal period {month}"])
    ws.append([f"Prepared by finance. Version {version}."])
    ws.append([])
    cols = ["Store", "Budget Net Sales", "Budget Labor Hours", "Budget Labor Cost"]
    ws.append(cols)
    for row in budget_rows(month, version):
        ws.append([row[c] for c in cols])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


BUDGET_PUBLICATIONS = [
    # (object name, month, version, published at)
    ("budgets/store-budget-2026-09.xlsx", "2026-09", 1, datetime(2026, 8, 28, 15, 0, tzinfo=UTC)),
    ("budgets/store-budget-2026-10.xlsx", "2026-10", 1, datetime(2026, 9, 29, 15, 0, tzinfo=UTC)),
    ("budgets/store-budget-2026-10.xlsx", "2026-10", 2, PLANTED["budget_correction"]["published_utc"]),
    ("budgets/store-budget-2026-11.xlsx", "2026-11", 1, datetime(2026, 10, 29, 15, 0, tzinfo=UTC)),
]
