# F:\telco_ingest\ingest\upload_json.py
# -*- coding: utf-8 -*-
"""
Persist normalized invoice package via:
  - PROC   : vendor-specific stored procedures
  - TABLES : table inserts using TelcoLoader

Supports running BOTH in one request via TELCO_PERSIST_MODE=BOTH (or "TABLES+PROC").
"""

from __future__ import annotations

import os
import json
import traceback
from typing import Dict, Any, Optional, Tuple, List

# pyodbc only required in TABLES mode and for Celcom PROC direct call
try:
    import pyodbc  # type: ignore
except Exception:
    pyodbc = None

from app.core.db import call_upsert_invoice_json
from app.core.config import settings
from app.services.telco_loader import TelcoLoader

_TRUE_SET = {"1", "true", "yes", "y", "on"}


# --------------------------------------------------------------------
# AUTO-FIXER (IMPORTANT)
# --------------------------------------------------------------------
def _auto_fix_package(pkg: Dict[str, Any], source_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Ensures the structure always includes:
        package["invoice"] = { vendor, invoice_number, account_number, invoice_date }
    This is required by both PROC and TABLES mode.
    """
    # Already correct
    if isinstance(pkg.get("invoice"), dict):
        # fallback invoice number (avoid DB errors)
        if not pkg["invoice"].get("invoice_number"):
            pkg["invoice"]["invoice_number"] = os.path.basename(source_file or "unknown.pdf")
        return pkg

    # Celcom raw extractor shape: {"bills":[{...}]}
    bills = pkg.get("bills")
    if bills and isinstance(bills, list) and bills:
        b = bills[0]
        pkg["invoice"] = {
            "vendor": b.get("vendor") or "celcom",
            "invoice_number": b.get("bill_statement_number") or "",
            "account_number": b.get("account_number"),
            "invoice_date": b.get("bill_date"),
        }

    # Add fallback for invoice number (avoid DB errors)
    if isinstance(pkg.get("invoice"), dict) and not pkg["invoice"].get("invoice_number"):
        pkg["invoice"]["invoice_number"] = os.path.basename(source_file or "unknown.pdf")

    return pkg


# --------------------------------------------------------------------
# VALIDATION
# --------------------------------------------------------------------
def _validate_package(pkg: Dict[str, Any]) -> Optional[str]:
    if not isinstance(pkg, dict):
        return "Package must be a JSON object."

    inv = pkg.get("invoice") or {}
    if not inv.get("vendor"):
        return "Missing invoice.vendor"
    if not inv.get("invoice_number"):
        return "Missing invoice.invoice_number"

    return None


def _resolve_conn_str() -> str:
    """Resolve SQL Server connection string."""
    return (
        getattr(settings, "odbc_conn_str", None)
        or os.getenv("DB_ODBC_STR")
        or os.getenv("SQLSERVER_CONN_STR")
        or ""
    ).strip()


# --------------------------------------------------------------------
# MODE PARSING (PROC / TABLES / BOTH)
# --------------------------------------------------------------------
def _parse_modes(mode_raw: str) -> List[str]:
    """
    Accepts:
      PROC
      TABLES
      BOTH
      TABLES+PROC / PROC+TABLES
      TABLES,PROC
    """
    m = (mode_raw or "PROC").strip().upper()

    if m == "BOTH":
        return ["TABLES", "PROC"]

    for sep in [",", "+", "|", " "]:
        if sep in m:
            parts = [p.strip().upper() for p in m.split(sep) if p.strip()]
            parts = [p for p in parts if p in {"PROC", "TABLES"}]
            if parts:
                # de-dupe while preserving order
                out: List[str] = []
                for p in parts:
                    if p not in out:
                        out.append(p)
                return out

    return [m] if m in {"PROC", "TABLES"} else ["PROC"]


# --------------------------------------------------------------------
# CELCOM PROC PAYLOAD BUILDER
# --------------------------------------------------------------------
def _celcom_flat_from_invoice_package(pkg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Celcom SP expects flat JSON with keys like:
      bills, current_charges_breakdown, registered, monthly_items, discount_rebate_items

    If your extractor already outputs those keys, we will use it.
    If it outputs invoice-package style (invoice/numbers/charges_summary),
    we synthesize a compatible flat payload here.
    """
    # If already flat (good)
    if isinstance(pkg.get("bills"), list) and isinstance(pkg.get("current_charges_breakdown"), list):
        # Ensure required arrays exist (avoid NULL JSON_QUERY)
        pkg.setdefault("registered", [])
        pkg.setdefault("monthly_items", [])
        pkg.setdefault("discount_rebate_items", [])
        return {
            "bills": pkg.get("bills", []),
            "current_charges_breakdown": pkg.get("current_charges_breakdown", []),
            "registered": pkg.get("registered", []),
            "monthly_items": pkg.get("monthly_items", []),
            "discount_rebate_items": pkg.get("discount_rebate_items", []),
        }

    inv = pkg.get("invoice") or {}
    invoice_no = inv.get("invoice_number")
    account_no = inv.get("account_number")

    # ---- bills[]
    bills = [{
        "bill_statement_number": invoice_no,
        "account_number": account_no,
        "bill_date": inv.get("bill_date"),
        "billing_from": inv.get("period_start"),
        "billing_to": inv.get("period_end"),
        "credit_limit": inv.get("credit_limit"),
        "deposit": inv.get("deposit"),
        "plan_name": inv.get("plan_name"),
        "previous_balance": inv.get("previous_balance"),
        "overdue_charges": inv.get("total_overdue_charges") or inv.get("previous_balance"),
        "current_charges": inv.get("total_current_charges"),
        "total_amount_due": inv.get("grand_total"),
        "rounding_adjustment": inv.get("rounding_adjustment"),
    }]

    # ---- current_charges_breakdown[] from charges_summary
    ccb = []
    for r in (pkg.get("charges_summary") or []):
        ccb.append({
            "bill_statement_number": invoice_no,
            "category": r.get("label"),
            "total": r.get("total") if r.get("total") is not None else 0,
        })

    # ---- registered[] from numbers
    registered = []
    monthly_items = []
    discount_rebate_items = []  # usually empty unless you add itemized discounts

    for n in (pkg.get("numbers") or []):
        mob = n.get("mobile")
        monthly_amt = None
        usage_amt = None
        disc_amt = None

        for ch in (n.get("charges") or []):
            cat = (ch.get("category") or "").lower()
            if cat == "monthly":
                monthly_amt = ch.get("amount")
            elif cat == "usage":
                usage_amt = ch.get("amount")
            elif cat == "discounts":
                disc_amt = ch.get("amount")
            elif cat == "monthlyitem":
                monthly_items.append({
                    "bill_statement_number": invoice_no,
                    "mobile": mob,
                    "description": ch.get("label"),
                    "from_date": ch.get("from"),
                    "to_date": ch.get("to"),
                    "amount_rm": ch.get("amount"),
                })

        total_amt = 0.0
        for v in [monthly_amt, usage_amt, disc_amt]:
            if isinstance(v, (int, float)):
                total_amt += float(v)

        registered.append({
            "bill_statement_number": invoice_no,
            "mobile": mob,
            "credit_limit": None,
            "one_time_amount": 0,
            "monthly_amount": monthly_amt,
            "usage_amount": usage_amt,
            "discounts_rebates": disc_amt,
            "total_amount_rm": round(total_amt, 2),
        })

    return {
        "bills": bills,
        "current_charges_breakdown": ccb,
        "registered": registered,
        "monthly_items": monthly_items,
        "discount_rebate_items": discount_rebate_items,
    }


def _call_celcom_proc(conn_str: str, payload_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call dbo.sp_Upsert_InvoicePackage_JSON_Celcom safely.
    IMPORTANT: we json.dumps() so SQL sees valid JSON (ISJSON=1).
    """
    if pyodbc is None:
        raise RuntimeError("pyodbc is required to call Celcom PROC but is not available.")

    payload = json.dumps(payload_obj, ensure_ascii=False)

    cn = pyodbc.connect(conn_str)
    try:
        cn.autocommit = False
        cur = cn.cursor()
        # Celcom proc signature: (@CelcomJson nvarchar(max))
        cur.execute("EXEC dbo.sp_Upsert_InvoicePackage_JSON_Celcom @CelcomJson = ?", (payload,))
        cn.commit()
        return {"ok": True}
    except Exception:
        # rollback + surface full traceback
        try:
            cn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cn.close()
        except Exception:
            pass


# =====================================================================
#                         PUBLIC ENTRYPOINT
# =====================================================================
def upload_invoice_package(
    package: Dict[str, Any],
    source_file: Optional[str] = None,
    schema: Optional[str] = None,
) -> Dict[str, Any]:

    package = _auto_fix_package(package, source_file)

    err = _validate_package(package)
    if err:
        return {"error": err}

    inv = package["invoice"]
    meta = package.get("__meta__", {})
    src = source_file or meta.get("file_name") or "API"

    mode_raw = (os.getenv("TELCO_PERSIST_MODE", "PROC") or "PROC").strip()
    modes = _parse_modes(mode_raw)

    write_enabled = os.getenv("ENABLE_DB_WRITE", "true").strip().lower() in _TRUE_SET
    if not write_enabled:
        return {
            "ok": True,
            "invoice_number": inv["invoice_number"],
            "vendor": inv["vendor"],
            "source": src,
            "mode": mode_raw,
            "modes_run": [],
            "write_enabled": False,
            "note": "DB write disabled by ENABLE_DB_WRITE",
        }

    conn_str = _resolve_conn_str()

    tables_result: Optional[Dict[str, Any]] = None
    proc_result: Optional[Dict[str, Any]] = None
    errors: List[Dict[str, Any]] = []

    vendor = (inv.get("vendor") or "").strip().lower()

    # Run TABLES if requested
    if "TABLES" in modes:
        try:
            if pyodbc is None:
                raise RuntimeError("TABLES mode selected but pyodbc is not available.")
            if not conn_str:
                raise RuntimeError("Missing SQL Server ODBC connection string.")

            loader = TelcoLoader(conn_str)
            tables_result = loader.save(package)
            tables_result["source"] = src
            tables_result["mode"] = "TABLES"
            tables_result["write_enabled"] = True

        except Exception as e:
            errors.append({
                "mode": "TABLES",
                "error": repr(e),
                "args": getattr(e, "args", None),
                "traceback": traceback.format_exc(),
            })

    # Run PROC if requested
    if "PROC" in modes:
        try:
            if vendor == "celcom":
                if not conn_str:
                    raise RuntimeError("Missing SQL Server ODBC connection string (required for Celcom PROC).")
                celcom_payload = _celcom_flat_from_invoice_package(package)
                _call_celcom_proc(conn_str, celcom_payload)
                proc_result = {"ok": True, "mode": "PROC", "vendor": vendor, "proc": "sp_Upsert_InvoicePackage_JSON_Celcom"}
            else:
                # Keep existing behavior for other vendors
                call_upsert_invoice_json(package, source_file=src)
                proc_result = {"ok": True, "mode": "PROC", "vendor": vendor}

        except Exception as e:
            errors.append({
                "mode": "PROC",
                "error": repr(e),
                "args": getattr(e, "args", None),
                "traceback": traceback.format_exc(),
            })

    # Build response
    ok = len(errors) == 0

    resp: Dict[str, Any] = {
        "ok": ok,
        "invoice_number": inv.get("invoice_number"),
        "vendor": inv.get("vendor"),
        "source": src,
        "mode": mode_raw,
        "modes_run": modes,
        "write_enabled": True,
        "tables_result": tables_result,
        "proc_result": proc_result,
    }

    if errors:
        resp["error"] = "One or more persistence modes failed."
        resp["errors"] = errors

    return resp
