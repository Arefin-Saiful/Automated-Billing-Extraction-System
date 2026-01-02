# F:\telco_ingest\parsers\base.py
# -*- coding: utf-8 -*-
"""
Common parser base for Telco PDF → standardized invoice package.

All concrete vendor parsers MUST implement:
  - parse_raw(pdf_path) -> dict       (vendor-specific full parse)
  - extract(pdf_path)   -> InvoicePackage (DB-ready envelope)

Standard envelope (what DB upsert expects):
{
  "invoice": {
      "vendor": "maxis|celcom|digi",
      "invoice_number": str|None,
      "account_number": str|None,
      "bill_date": "YYYY-MM-DD"|None,
      "period_start": "YYYY-MM-DD"|None,
      "period_end": "YYYY-MM-DD"|None,
      "currency": "MYR",
      "subtotal": number|None,
      "tax_total": number|None,
      "grand_total": number|None
  },
  "numbers": [
      {
        "msisdn": str,
        "description": str|None,
        "subscriber": str|None,
        "monthly_items": list,         # pass-through list of dicts
        "detail_of_charges": list,     # pass-through list of dicts
        "line_total": number|None
      }, ...
  ],
  "charges": [
      { "category": str, "label": str, "amount": number }, ...
  ],
  "raw": { ... }  # original vendor parse (optional but recommended)
}

Nothing here changes any vendor logic—this module only provides
shared helpers and a thin abstract base for consistency.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union


# ----------------------------- Exceptions -----------------------------

class ParseError(Exception):
    """Raised when a PDF cannot be parsed into the expected raw dict."""


class SchemaError(Exception):
    """Raised when a standardized package fails basic validation."""


# --------------------------- Typed Structures -------------------------

NumberLike = Union[int, float, Decimal, None]

class InvoiceDict(TypedDict, total=False):
    vendor: str
    invoice_number: Optional[str]
    account_number: Optional[str]
    bill_date: Optional[str]         # ISO YYYY-MM-DD
    period_start: Optional[str]      # ISO
    period_end: Optional[str]        # ISO
    currency: str
    subtotal: NumberLike
    tax_total: NumberLike
    grand_total: NumberLike

class NumberLine(TypedDict, total=False):
    msisdn: str
    description: Optional[str]
    subscriber: Optional[str]
    monthly_items: List[Dict[str, Any]]
    detail_of_charges: List[Dict[str, Any]]
    line_total: NumberLike

class ChargeRow(TypedDict):
    category: str
    label: str
    amount: NumberLike

class InvoicePackage(TypedDict, total=False):
    invoice: InvoiceDict
    numbers: List[NumberLine]
    charges: List[ChargeRow]
    raw: Dict[str, Any]


# --------------------------- JSON Utilities ---------------------------

class DecimalJSONEncoder(json.JSONEncoder):
    """Safely encode Decimal to JSON (as str or float)."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            # keep scale to 2 dp; DB side can CAST as money/decimal(18,2)
            return str(obj.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        return super().default(obj)


def dumps_json(data: Any, **kwargs: Any) -> str:
    """json.dumps with Decimal-safe encoder and sane defaults."""
    return json.dumps(data, cls=DecimalJSONEncoder, ensure_ascii=False, **kwargs)


# ------------------------------ Helpers -------------------------------

_MONTHS = {
    "jan": "01", "january": "01",
    "feb": "02", "february": "02",
    "mar": "03", "march": "03",
    "apr": "04", "april": "04",
    "may": "05",
    "jun": "06", "june": "06",
    "jul": "07", "july": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "oct": "10", "october": "10",
    "nov": "11", "november": "11",
    "dec": "12", "december": "12",
}

_DATE_RX = re.compile(r"^\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s*$")


def to_iso_date(d: Optional[str]) -> Optional[str]:
    """Convert '28 July 2025' → '2025-07-28'. Returns None if not matched."""
    if not d:
        return None
    m = _DATE_RX.match(d)
    if not m:
        return None
    day, mon, year = m.groups()
    mm = _MONTHS.get(mon.lower())
    if not mm:
        return None
    return f"{year}-{mm}-{int(day):02d}"


def split_period(period: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract start/end from '28 July 2025 - 27 Aug 2025' (or 'to').
    Returns (iso_start, iso_end).
    """
    if not period:
        return None, None
    m = re.search(
        r"(\d{1,2}\s[A-Za-z]+\s\d{4})\s*(?:-|to)\s*(\d{1,2}\s[A-Za-z]+\s\d{4})",
        period, flags=re.I
    )
    if not m:
        return None, None
    return to_iso_date(m.group(1)), to_iso_date(m.group(2))


def parse_amount_decimal(text: Optional[str]) -> Optional[Decimal]:
    """Parse currency-like text to Decimal (RM-aware; tolerant to commas/spaces)."""
    if text is None:
        return None
    try:
        cleaned = (
            text.replace(",", "")
                .replace("RM", "")
                .replace("rm", "")
                .strip()
        )
        if cleaned == "":
            return None
        return Decimal(cleaned)
    except (InvalidOperation, AttributeError):
        return None


def as_2dp(n: NumberLike) -> Optional[Decimal]:
    """Normalize number to Decimal(2dp); returns None if input is None."""
    if n is None:
        return None
    if isinstance(n, Decimal):
        val = n
    else:
        try:
            val = Decimal(str(n))
        except InvalidOperation:
            return None
    return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def coalesce_str(*vals: Optional[str]) -> Optional[str]:
    """Return first non-empty string or None."""
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None


# ------------------------------ Registry ------------------------------

@dataclass
class ParserInfo:
    vendor: str
    cls: type


_PARSER_REGISTRY: Dict[str, ParserInfo] = {}


def register_parser(vendor: str):
    """
    Class decorator to register a parser by vendor key ('maxis'|'celcom'|'digi').
    Usage:

        @register_parser("digi")
        class DigiParser(ParserBase): ...
    """
    vendor = vendor.lower().strip()

    def _wrap(cls: type) -> type:
        _PARSER_REGISTRY[vendor] = ParserInfo(vendor=vendor, cls=cls)
        return cls

    return _wrap


def get_parser(vendor: str) -> "ParserBase":
    info = _PARSER_REGISTRY.get(vendor.lower().strip())
    if not info:
        raise KeyError(f"No parser registered for vendor '{vendor}'")
    return info.cls()


# ------------------------------ Base Class -----------------------------

class ParserBase(ABC):
    """
    Abstract base for vendor parsers.

    Concrete parsers MUST:
      - set self.vendor (e.g., 'maxis'|'celcom'|'digi')
      - implement parse_raw(pdf_path) and extract(pdf_path)
    """

    vendor: str = "unknown"

    # --------- Required API ---------

    @abstractmethod
    def parse_raw(self, pdf_path: str) -> Dict[str, Any]:
        """
        Return vendor-specific parse (free-form dict).
        Should raise ParseError on fatal issues.
        """
        raise NotImplementedError

    @abstractmethod
    def extract(self, pdf_path: str) -> InvoicePackage:
        """
        Return standardized invoice package (see top-level docstring).
        Should raise SchemaError if the shape is invalid.
        """
        raise NotImplementedError

    # --------- Optional helpers (shared) ---------

    def build_db_payload(self, pdf_path: str) -> InvoicePackage:
        """
        Wrapper used by the ingestion layer before calling the DB upsert.
        Does a light schema check to catch obvious mistakes early.
        """
        pkg = self.extract(pdf_path)
        self._validate_package(pkg)
        return pkg

    # ----------------- Validation (thin, non-invasive) -----------------

    def _validate_package(self, pkg: InvoicePackage) -> None:
        """Minimal shape validation; does NOT mutate data."""
        if not isinstance(pkg, dict):
            raise SchemaError("Package must be a dict.")

        # invoice
        inv = pkg.get("invoice")
        if not isinstance(inv, dict):
            raise SchemaError("Package['invoice'] must be a dict.")

        required_invoice_keys = {"vendor", "currency"}
        missing = required_invoice_keys - set(inv.keys())
        if missing:
            raise SchemaError(f"Invoice missing required keys: {sorted(missing)}")

        if not isinstance(inv["vendor"], str) or not inv["vendor"]:
            raise SchemaError("Invoice.vendor must be a non-empty string.")
        if not isinstance(inv["currency"], str) or not inv["currency"]:
            raise SchemaError("Invoice.currency must be a non-empty string.")

        # numbers
        numbers = pkg.get("numbers", [])
        if numbers is not None and not isinstance(numbers, list):
            raise SchemaError("Package['numbers'] must be a list if present.")
        for i, n in enumerate(numbers or []):
            if not isinstance(n, dict):
                raise SchemaError(f"numbers[{i}] must be a dict.")
            if "msisdn" not in n or not n["msisdn"]:
                raise SchemaError(f"numbers[{i}].msisdn is required.")

        # charges
        charges = pkg.get("charges", [])
        if charges is not None and not isinstance(charges, list):
            raise SchemaError("Package['charges'] must be a list if present.")
        for i, c in enumerate(charges or []):
            if not isinstance(c, dict):
                raise SchemaError(f"charges[{i}] must be a dict.")
            for k in ("category", "label", "amount"):
                if k not in c:
                    raise SchemaError(f"charges[{i}] missing key '{k}'.")

        # raw (optional but recommended)
        raw = pkg.get("raw")
        if raw is not None and not isinstance(raw, dict):
            raise SchemaError("Package['raw'] must be a dict if present.")


# --------------------------- Convenience mixin -------------------------

class DateAmountMixin:
    """Optional mixin for concrete parsers: reuse date/amount helpers."""
    to_iso_date = staticmethod(to_iso_date)
    split_period = staticmethod(split_period)
    parse_amount_decimal = staticmethod(parse_amount_decimal)
    as_2dp = staticmethod(as_2dp)
    coalesce_str = staticmethod(coalesce_str)


# ---------------------------- CLI Utilities ----------------------------

def cli_dump_json(pkg: InvoicePackage, pretty: bool = True) -> str:
    """Return JSON string for console or file output."""
    if pretty:
        return dumps_json(pkg, indent=2)
    return dumps_json(pkg)
