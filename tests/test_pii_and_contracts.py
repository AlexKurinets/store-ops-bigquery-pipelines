from collections import Counter

import pytest

from storeops import pii
from storeops.contracts import ContractError, coerce
from storeops.docs import freshness_markdown
from storeops.settings import ROOT

KEY = "k"


def test_ssn_and_itin_shapes_are_found_but_ids_are_not():
    assert pii.find("SSN 987-65-4325 on file") == ["ssn"]
    assert pii.find("ITIN 912 70 1234") == ["ssn"]  # 9xx is a personal tax ID too
    for value in ["0101-20260901-0001", "SH-TX-214-20260901-00", "2026-09-01T10:00:00.000000Z", "000-12-3456"]:
        assert pii.find(value) == [], value


def test_card_numbers_need_a_card_shape_and_a_valid_luhn():
    assert pii.find("card 4111 1111 1111 1111 left at register") == ["card"]
    assert pii.find("4111111111111111") == ["card"]
    assert pii.find("4111 1111 1111 1112") == []  # fails Luhn
    assert pii.find("************1234") == []  # already masked by the POS


def test_mask_replaces_each_kind_and_counts_it():
    text, counts = pii.mask("Card 4111 1111 1111 1111, call 555-0142, mail a.b@example.com, SSN 987-65-4321")
    assert "4111" not in text and "555-0142" not in text and "example.com" not in text and "987-65" not in text
    assert counts == {"card": 1, "phone": 1, "email": 1, "ssn": 1}


def test_pseudonym_is_keyed_and_joins_across_sources():
    a = pii.pseudonym(KEY, "100234")
    assert a == pii.pseudonym(KEY, " 100234 ")  # same employee from the POS and from timekeeping
    assert a != pii.pseudonym("other-key", "100234")
    assert len(a) == 16 and pii.pseudonym(KEY, None) is None


def test_every_personal_field_is_never_kept_as_is(contracts):
    for c in contracts.values():
        for t in c.tables.values():
            for f in t.fields:
                if f.pii:
                    assert f.cls in ("drop", "hash", "mask", "restrict"), f"{c.source}.{t.name}.{f.source_name}"


def test_no_landed_column_name_looks_personal(contracts):
    for c in contracts.values():
        for t in c.tables.values():
            for col in t.columns():
                assert not pii.PII_COLUMN.search(col.name), f"{t.name}.{col.name}"


def test_contract_denies_by_default_and_reports_drift(contracts):
    t = contracts["pos"].tables["pos_transaction_lines"]
    rec = {"transaction_id": "0101-20260901-0001", "line_number": 1, "store_number": "0101", "operator_id": "100234",
           "closed_at": "2026-09-01T12:00:00Z", "card_number": "************1234", "loyalty_member_id": "LM12345678",
           "extended_price": "3.98"}
    res = t.apply([rec], KEY)
    row = res.rows[0]
    assert "card_number" not in row and "loyalty_member_id" not in row and "operator_id" not in row
    assert row["operator_key"] == pii.pseudonym(KEY, "100234")
    assert row["extended_price"] == 3.98 and row["closed_at"] == "2026-09-01T12:00:00.000000Z"
    assert row["voided_at"] is None  # listed fields the source omitted land as null
    assert res.drift == {"loyalty_member_id"}
    assert res.actions[("card_number", "drop")] == 1 and res.actions[("loyalty_member_id", "drop_unlisted")] == 1


def test_pay_rate_goes_only_to_the_restricted_rows(contracts):
    t = contracts["timekeeping"].tables["timekeeping_employees"]
    res = t.apply([{"EmployeeId": "100234", "SSN": "987-65-4321", "FirstName": "Avery", "PayRate": 14.25,
                    "HomeLocationCode": "TX-214", "Notes": "SSN 987-65-4321 confirmed"}], KEY)
    assert "pay_rate" not in res.rows[0] and all("987" not in str(v) for v in res.rows[0].values())
    assert res.restricted_rows == [{"employee_key": pii.pseudonym(KEY, "100234"), "pay_rate": 14.25}]
    assert Counter(a for (_, a) in res.actions) == Counter({"drop": 3, "hash": 1, "restrict": 1})


def test_odata_requests_never_ask_for_dropped_fields(contracts):
    asked = contracts["timekeeping"].tables["timekeeping_employees"].requested_fields()
    assert "SSN" not in asked and "Notes" not in asked and "PayRate" in asked


def test_bad_values_fail_loudly():
    with pytest.raises(ContractError):
        coerce(12.5, "integer", "t.f")
    with pytest.raises(ContractError):
        coerce("2026-09-01T10:00:00", "timestamp", "t.f")  # no offset: ambiguous across time zones
    assert coerce("3", "integer", "t.f") == 3 and coerce("2026-09-01", "date", "t.f") == "2026-09-01"


def test_freshness_doc_matches_the_contracts():
    assert (ROOT / "docs" / "freshness.md").read_text(encoding="utf-8") == freshness_markdown(), \
        "run `storeops docs` to regenerate docs/freshness.md"
