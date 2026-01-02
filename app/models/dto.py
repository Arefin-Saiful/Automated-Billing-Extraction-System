# F:\telco_ingest\app\models\dto.py
# -*- coding: utf-8 -*-
"""
Data Transfer Objects (DTOs) for the telco ingest pipeline.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator, root_validator


# ============================================================
# Helpers
# ============================================================

_MONEY_RX = re.compile(r"[,\s]")        # strip commas/spaces
_MSISDN_RX = re.compile(r"[^\d+]")      # strip non-digits (allow +)


def _to_decimal_2(v: Any) -> Optional[Decimal]:
    """Convert to Decimal(2dp) safely."""
    if v is None:
        return None

    if isinstance(v, Decimal):
        try:
            return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        except Exception:
            return v

    if isinstance(v, (int, float)):
        try:
            return Decimal(str(v)).quantize(Decimal("0.01"))
        except Exception:
            return None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            s = _MONEY_RX.sub("", s)
            return Decimal(s).quantize(Decimal("0.01"))
        except Exception:
            return None

    return None


def _to_iso_date(v: Any) -> Optional[date]:
    """Convert string/datetime → date."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str) and v.strip():
        try:
            return date.fromisoformat(v.strip())
        except Exception:
            return None
    return None


# ============================================================
# Line items
# ============================================================

class MonthlyItem(BaseModel):
    description: str
    amount: Optional[Decimal] = None

    _v_amount = validator("amount", pre=True, allow_reuse=True)(_to_decimal_2)


class DataDetail(BaseModel):
    category: Optional[str] = None
    access_point: Optional[str] = None
    volume_kb: Optional[int] = None
    amount: Optional[Decimal] = None

    _v_amount = validator("amount", pre=True, allow_reuse=True)(_to_decimal_2)


class NumberLine(BaseModel):
    msisdn: str = Field(..., description="Mobile number (digits only)")
    description: Optional[str] = None
    subscriber: Optional[str] = None
    monthly_items: List[MonthlyItem] = Field(default_factory=list)
    detail_of_charges: List[DataDetail] = Field(default_factory=list)
    line_total: Optional[Decimal] = None

    _v_line_total = validator("line_total", pre=True, allow_reuse=True)(_to_decimal_2)

    @validator("msisdn", pre=True)
    def _clean_msisdn(cls, v: str) -> str:
        if not v:
            return ""
        return _MSISDN_RX.sub("", v)


# ============================================================
# Charges
# ============================================================

class ChargeItem(BaseModel):
    category: str
    label: str
    amount: Optional[Decimal] = None

    _v_amount = validator("amount", pre=True, allow_reuse=True)(_to_decimal_2)

    @validator("category", "label", pre=True)
    def _strip(cls, v: str) -> str:
        return (v or "").strip()


# ============================================================
# Invoice
# ============================================================

class Invoice(BaseModel):
    vendor: str
    invoice_number: Optional[str] = None
    account_number: Optional[str] = None
    bill_date: Optional[date] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    currency: str = "MYR"
    subtotal: Optional[Decimal] = None
    tax_total: Optional[Decimal] = None
    grand_total: Optional[Decimal] = None

    _v_bill_date = validator("bill_date", pre=True, allow_reuse=True)(_to_iso_date)
    _v_period_start = validator("period_start", pre=True, allow_reuse=True)(_to_iso_date)
    _v_period_end = validator("period_end", pre=True, allow_reuse=True)(_to_iso_date)

    _v_subtotal = validator("subtotal", pre=True, allow_reuse=True)(_to_decimal_2)
    _v_tax_total = validator("tax_total", pre=True, allow_reuse=True)(_to_decimal_2)
    _v_grand_total = validator("grand_total", pre=True, allow_reuse=True)(_to_decimal_2)

    @validator("vendor", pre=True)
    def _vendor_lower(cls, v: str) -> str:
        return (v or "").strip().lower()

    @validator("currency", pre=True)
    def _currency_norm(cls, v: str) -> str:
        v = (v or "").strip()
        return v if v else "MYR"

    class Config:
        json_encoders = {
            date: lambda d: d.isoformat(),
            Decimal: lambda d: str(d.quantize(Decimal("0.01"))) if d is not None else None,
        }


# ============================================================
# Main Package
# ============================================================

class IngestPackage(BaseModel):
    invoice: Invoice
    numbers: List[NumberLine] = Field(default_factory=list)
    charges: List[ChargeItem] = Field(default_factory=list)

    # raw is optional because Celcom may not always include it
    raw: Optional[Dict[str, Any]] = Field(default_factory=dict)

    _meta: Optional[Dict[str, Any]] = None

    @root_validator
    def _ensure_defaults(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        inv: Invoice = values.get("invoice")
        if inv and not inv.currency:
            inv.currency = "MYR"
        return values

    def to_db_json(self) -> str:
        return json.dumps(self.dict(), ensure_ascii=False, default=str)

    def to_minimal_json(self) -> str:
        data = self.dict()
        data.pop("raw", None)
        return json.dumps(data, ensure_ascii=False, default=str)


# ============================================================
# Vendor detection result
# ============================================================

class VendorDetectResult(BaseModel):
    vendor: str = "unknown"
    confidence: float = 0.0
    features: Optional[Dict[str, Any]] = None

    @validator("vendor", pre=True)
    def _vendor_norm(cls, v: str) -> str:
        return (v or "unknown").strip().lower()

    @validator("confidence", pre=True)
    def _conf_range(cls, v: float) -> float:
        try:
            x = float(v)
        except Exception:
            return 0.0
        return min(max(x, 0.0), 1.0)


# ============================================================
# Folder ingest summaries
# ============================================================

class FolderIngestItem(BaseModel):
    file: str
    sha256: Optional[str] = None
    vendor: Optional[str] = None
    ok: bool = False
    upserted: Optional[bool] = None
    message: Optional[str] = None
    error: Optional[str] = None

    @validator("file", "vendor", pre=True)
    def _strip(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v


class FolderIngestSummary(BaseModel):
    total: int
    success: int
    failed: int
    items: List[FolderIngestItem] = Field(default_factory=list)

    @classmethod
    def from_items(cls, items: List[FolderIngestItem]) -> "FolderIngestSummary":
        success = sum(1 for it in items if it.ok)
        return cls(
            total=len(items),
            success=success,
            failed=len(items) - success,
            items=items
        )
