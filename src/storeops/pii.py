"""Pattern detection for personal data, used twice: to mask free text before it is written, and to scan what
was loaded so a contract mistake fails the run instead of sitting in the warehouse."""

from __future__ import annotations

import hashlib
import hmac
import re

# SSNs and ITINs with separators. ITINs (9xx) are personal tax IDs too, so the 9xx area is not excluded.
SSN = re.compile(r"\b(?!000|666)\d{3}[- ](?!00)\d{2}[- ](?!0000)\d{4}\b")
# Card numbers as they are actually written: 13-19 contiguous digits, 4-4-4-4 groups, or Amex 4-6-5, starting with
# 3-6 and standing alone. A looser "digits with dashes" pattern flagged POS transaction IDs such as
# 0101-20260901-0001, about one in ten of which pass the Luhn check by chance.
CARD = re.compile(r"(?<![\w-])(?:[3-6]\d{12,18}|[3-6]\d{3}(?:[ -]\d{4}){3}|3[47]\d{2}[ -]\d{6}[ -]\d{5})(?![\w-])")
EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE = re.compile(r"(?<![\d-])(?:\(?\d{3}\)?[-. ])?\d{3}-\d{4}\b(?!-)")

# A coarse superset of SSN and card shapes, without lookarounds, so Postgres (~) and BigQuery (REGEXP_CONTAINS, RE2)
# can pick candidate values in the warehouse; only candidates come back to be checked exactly with find().
COARSE = r"[0-9]{3}[- ][0-9]{2}[- ][0-9]{4}|[3-6][0-9]{3}[ -]?[0-9]{4,6}[ -]?[0-9]{4,5}|[3-6][0-9]{12,18}"

# Column names that should never appear outside the restricted datasets.
PII_COLUMN = re.compile(
    r"(^|_)(ssn|social_security|birth|dob|first_name|last_name|full_name|home_address|email|phone|card_number|"
    r"pay_rate|wage_rate|bank|routing)(_|$)")


def luhn_ok(digits: str) -> bool:
    total, parity = 0, len(digits) % 2
    for i, ch in enumerate(digits):
        d = int(ch)
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _cards(text: str) -> list[re.Match]:
    out = []
    for m in CARD.finditer(text):
        digits = re.sub(r"\D", "", m.group(0))
        if 13 <= len(digits) <= 19 and luhn_ok(digits):
            out.append(m)
    return out


def find(text: str | None) -> list[str]:
    """Kinds of hard-stop personal data in a value: SSN-shaped numbers and Luhn-valid card numbers."""
    if not text:
        return []
    kinds = []
    if SSN.search(text):
        kinds.append("ssn")
    if _cards(text):
        kinds.append("card")
    return kinds


def mask(text: str | None) -> tuple[str | None, dict[str, int]]:
    """Replace card numbers, SSNs, emails and phone numbers in free text with markers."""
    counts: dict[str, int] = {}
    if not text:
        return text, counts
    for m in reversed(_cards(text)):
        text = text[:m.start()] + "[masked:card]" + text[m.end():]
        counts["card"] = counts.get("card", 0) + 1
    for kind, pattern in (("ssn", SSN), ("email", EMAIL), ("phone", PHONE)):
        text, n = pattern.subn(f"[masked:{kind}]", text)
        if n:
            counts[kind] = counts.get(kind, 0) + n
    return text, counts


def pseudonym(key: str, value: object) -> str | None:
    """Keyed HMAC-SHA256, truncated to 16 hex characters. The same key is used for every source, so an employee
    ID hashed from the POS joins to the same ID hashed from the timekeeping system."""
    if value is None or value == "":
        return None
    return hmac.new(key.encode(), str(value).strip().encode(), hashlib.sha256).hexdigest()[:16]
