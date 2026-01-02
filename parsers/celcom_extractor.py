# -*- coding: utf-8 -*-
"""
Celcom bill extractor (PDF → structured package for DB),
DB-aligned and interface-compatible with your Maxis extractor.

❖ Public API:
    - parse_raw(pdf_path: str, include_calls: bool = True) -> dict
        Core Celcom parsing logic (regex-based).

    - to_invoice_package(parsed: dict) -> dict
        Adapts Celcom raw parse → common invoice envelope
        (for generic invoice usage / logging).

    - extract(pdf_path: str, include_calls: bool = True) -> dict
        Convenience: parse_raw + to_invoice_package.

    - build_flat_json(pdf_path: str,
                      include_calls: bool = True,
                      include_one_time: bool = False) -> dict
        High-level helper for the SQL stored procedure
        [dbo].[sp_Upsert_InvoicePackage_JSON_Celcom].

        Usage (in Python):
            flat = build_flat_json("11-CelcomDigiBill.pdf", include_calls=True)
            payload = json.dumps(flat, ensure_ascii=False)
            # then execute proc with @CelcomJson = payload

        Top-level keys expected by the proc:
            bills,
            current_charges_breakdown,
            registered,
            monthly_items,
            discount_rebate_items,
            discounts_rebates,
            local_calls_messages,
            calls_to_celcom,
            calls_to_non_celcom
"""

from __future__ import annotations
import re
import os
import argparse
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
from datetime import datetime

# --------------------------- Common regex ---------------------------
NUM     = r"\(?-?[0-9][0-9,]*\.?[0-9]*\)?"
DATE    = r"\d{2}[/-]\d{2}[/-]\d{4}"
TIME    = r"\d{2}:\d{2}:\d{2}"
MOBILE  = r"(?:\+?6?0)?\d{2,3}[- ]?\d{6,8}"
NUM_RM  = r"(?:RM\s*)?" + NUM  # allow optional "RM " prefix

# Headings
HDR_DETAILED        = r"DETAILED\s+CHARGES"
HDR_PREV_PAY        = r"Previous\s+Payment\s+Details"
HDR_REGISTERED      = r"Registered\s+Mobile\s+Number(?:s)?"
HDR_MONTHLY_WORD    = r"Monthly\s+Amount"
HDR_MONTHLY         = r"(?:Detail(?:ed)?\s+Charges\s*[-–—]\s*Monthly|Monthly\s+Amount)"
HDR_DISC            = r"Discounts?\s*&\s*Rebates"
HDR_CELCOM          = r"Your\s+Calls\s+To\s+Celcom\s+Numbers"
HDR_NONCEL          = r"Your\s+Calls\s+To\s+Non[- ]Celcom\s+Numbers"
HDR_VAS             = r"Value\s+Added\s+Services"
HDR_LOCAL           = r"Local\s+Calls\s*&\s*Messages"   # Local Calls section

# --------------------------- Helpers ---------------------------
def _norm(s: str) -> str:
    return (s or "").replace("\u00A0", " ").strip()

def _ws_collapse(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00A0"," ")).strip()

def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = s.strip()
    if not s or s in {"—", "-"}:
        return None
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "").replace("RM", "").strip()
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None

def _find_first(pat: str, text: str, flags=re.I | re.S) -> Optional[str]:
    m = re.search(pat, text, flags)
    return m.group(1) if m else None

def _slice_between(
    text: str,
    start_pat: str,
    end_pats: List[str],
    span: int = 200000,
    flags=re.I | re.S | re.M
) -> str:
    m = re.search(start_pat, text, flags)
    if not m:
        return ""
    start = m.end()
    end = min(len(text), start + span)
    sub = text[start:end]
    for ep in end_pats:
        n = re.search(ep, sub, flags)
        if n:
            return text[start:start + n.start()]
    return text[start:end]

def _get_all_texts(pdf) -> Tuple[str, List[str]]:
    pages = []
    for p in pdf.pages:
        t = ""
        try:
            t = p.extract_text(x_tolerance=1.5, y_tolerance=3.0) or p.extract_text() or ""
        except Exception:
            t = p.extract_text() or ""
        pages.append(t)
    return "\n".join(pages), pages

def _key_norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\s\-\(&\)]+", "", s)
    return s

def _mobile_normalize(m: str) -> str:
    s = re.sub(r"[^\d+]", "", m or "")
    if s.startswith("+60"):
        return s
    if s.startswith("60"):
        return "+" + s
    if s.startswith("0"):
        return "+60" + s[1:]
    return s

def _sum_amount(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
    vals = [r.get(key) for r in rows if isinstance(r.get(key), (int, float))]
    return round(sum(vals), 2) if vals else None

def _hms_to_seconds(hms: str) -> int:
    if not hms:
        return 0
    parts = [int(x) for x in hms.split(":")]
    if len(parts) == 2:
        mm, ss = parts
        return mm * 60 + ss
    hh, mm, ss = parts
    return hh * 3600 + mm * 60 + ss

def _seconds_to_hms(total_seconds: int) -> str:
    if total_seconds <= 0:
        return "00:00:00"
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _sum_duration(rows: List[Dict[str, Any]], key: str) -> Optional[str]:
    durs = [r.get(key) for r in rows
            if isinstance(r.get(key), str)
            and re.fullmatch(r"\d{1,3}:\d{2}(?::\d{2})?", r.get(key))]
    if not durs:
        return None
    total = sum(_hms_to_seconds(d) for d in durs)
    return _seconds_to_hms(total)

# --------------------------- Header / Summary ---------------------------
def _parse_header(first_page_text: str, all_text: str) -> Dict[str, Any]:
    out = {
        "bill_statement_month": None, "service_number": None, "account_number": None,
        "bill_statement_number": None, "bill_date": None,
        "billing_period": {"from": None, "to": None},
        "credit_limit": None, "deposit": None, "customer_name": None, "plan_name": None,
    }

    out["bill_statement_month"] = _find_first(r"Bill Statement\s+([A-Za-z]+\s+\d{4})", first_page_text)
    out["service_number"]       = _find_first(r"Service Number\s*:\s*([0-9\-]+)", first_page_text)
    out["account_number"]       = _find_first(r"Account Number\s*:\s*(\d+)", first_page_text)
    out["bill_statement_number"]= _find_first(r"Bill Statement Number\s*:\s*(\d+)", first_page_text)
    out["bill_date"]            = _find_first(r"Bill Date\s*:\s*(" + DATE + ")", first_page_text)

    m = re.search(
        r"Billing Period\s*:\s*(" + DATE + r")\s*[–\-]\s*(" + DATE + r")",
        first_page_text, re.I
    )
    if m:
        out["billing_period"]["from"], out["billing_period"]["to"] = m.group(1), m.group(2)

    out["credit_limit"] = _to_float(_find_first(
        r"Credit Limit\s*:\s*(" + NUM + ")", first_page_text))
    out["deposit"]      = _to_float(_find_first(
        r"Deposit\s*:\s*(" + NUM + ")", first_page_text))

    out["customer_name"] = (
        _find_first(r"(?:^|\n)\s*Name\s*:\s*([^\n]+)", all_text) or
        _find_first(r"Hello\s+(.+?),", all_text) or
        _find_first(r"Customer Name\s*:\s*([^\n]+)", all_text)
    )

    out["plan_name"] = (
        _find_first(r"(MEGA[^\n]{0,50})", first_page_text) or
        _find_first(r"(Lightning\s*\d+\b[^\n]{0,30})", first_page_text)
    )
    return out


def _parse_account_summary(first_page_text: str, all_text: str = "") -> Dict[str, Any]:
    out = {
        "previous_balance": None, "total_payments": None,
        "overdue_charges": None,  "current_charges": None,
        "due_date": None,         "total_amount_due": None,
        "monthly_charges_rm": None, "service_tax_6pct": None,
        "rounding_adjustment": None, "total_current_charges": None,
    }

    # Combine because some PDFs scatter values across extraction order
    combined_text = (first_page_text or "") + "\n" + (all_text or "")

    # --- TOP SUMMARY TABLE: Overdue / Current Charges / Due Date / Amount Due ---
    m = re.search(
        r"(Overdue Charges\s+Current Charges\s+(?:Payment\s+)?Due Date\s+Amount Due)\s+"
        r"RM\s*(" + NUM + r")\s+RM\s*(" + NUM + r")\s+(" + DATE + r")\s+RM\s*(" + NUM + r")",
        combined_text, re.I | re.S
    )
    if m:
        # NOTE: here "Current Charges" is usually AFTER-SST already in many templates,
        # but we will override later with "Total Current Charges" if we can find it.
        out["overdue_charges"]  = _to_float(m.group(2))
        out["current_charges"]  = _to_float(m.group(3))
        out["due_date"]         = m.group(4)
        out["total_amount_due"] = _to_float(m.group(5))
    else:
        out["overdue_charges"]  = _to_float(_find_first(
            r"Overdue Charges\s*RM\s*(" + NUM + ")", combined_text))
        out["current_charges"]  = _to_float(_find_first(
            r"Current Charges\s*RM\s*(" + NUM + ")", combined_text))
        out["due_date"]         = (
            _find_first(r"(?:Payment\s+)?Due Date\s*(" + DATE + ")", combined_text) or
            _find_first(r"(?:Payment\s+)?Due\s*Date\s*(" + DATE + ")", combined_text)
        )
        out["total_amount_due"] = _to_float(_find_first(
            r"Amount\s+Due\s*RM?\s*(" + NUM + ")", combined_text))

    # --- Other summary fields ---
    out["previous_balance"] = _to_float(_find_first(
        r"Previous Balance\s*(" + NUM + ")", combined_text))
    out["total_payments"]   = _to_float(_find_first(
        r"Total Payments\s*(" + NUM + ")", combined_text))
    out["monthly_charges_rm"] = _to_float(_find_first(
        r"Monthly Charges\s*\(RM\)\s*(" + NUM + ")", combined_text))

    out["service_tax_6pct"] = _to_float(_find_first(
        r"Service\s*Tax\s*6%\s*(?:RM)?\s*(" + NUM + ")",
        combined_text
    ))
    out["rounding_adjustment"] = _to_float(_find_first(
        r"Rounding\s*Adjustment\s*(?:RM)?\s*(" + NUM + ")",
        combined_text
    ))

    # ✅ PRIORITY: AFTER-SST figure (what you want as "Current Charges")
    out["total_current_charges"] = _to_float(_find_first(
        r"Total Current Charges\s*RM?\s*(" + NUM + ")",
        combined_text
    ))

    # ✅ Force "current_charges" to AFTER-SST:
    # 1) Total Current Charges (best)
    # 2) Amount Due (often same when overdue=0)
    # 3) Monthly + SST + Rounding (computed fallback)
    if isinstance(out["total_current_charges"], (int, float)):
        out["current_charges"] = out["total_current_charges"]
    elif isinstance(out["total_amount_due"], (int, float)):
        out["current_charges"] = out["total_amount_due"]
    else:
        mc = out.get("monthly_charges_rm")
        st = out.get("service_tax_6pct")
        ra = out.get("rounding_adjustment") or 0.0
        if isinstance(mc, (int, float)) and isinstance(st, (int, float)):
            out["current_charges"] = round(float(mc) + float(st) + float(ra), 2)

    return out

# --------------------------- Current Charges breakdown ---------------------------
def _parse_current_charges_breakdown(first_page_text: str) -> List[Dict[str, Any]]:
    block = _slice_between(
        first_page_text,
        r"Current Charges\s+Non-?Taxable",
        [r"Monthly Charges\s*\(RM\)", r"Total Current Charges", r"Total Amount Due",
         r"Page\s+\d+\s+of"],
        span=120000
    )
    rows: List[Dict[str, Any]] = []
    if not block:
        return rows

    HEAD = re.compile(r"^(?:Non-?taxable\s*\(RM\)\s+)?Taxable(?:\s*\(RM\))?\s+Total(?:\s*\(RM\))?$", re.I)
    RMROW = re.compile(r"^\(RM\)(?:\s+\(RM\)){2,}$", re.I)

    def _clean_inline_noise(ln: str) -> str:
        ln = re.sub(r"\b(?:Previous Balance|Total Payments|Total Overdue Charges)\s+" + NUM + r"\b",
                    "", ln, flags=re.I)
        ln = re.sub(r"(?i)Note:.*?\.\s*", "", ln)
        ln = re.sub(r"(?i)\bPayment Slip\b.*$", "", ln)
        return _ws_collapse(ln)

    buf = ""
    section = None

    def _emit(candidate: str) -> bool:
        if HEAD.match(candidate) or RMROW.match(candidate):
            return False
        m = re.search(r"(.+?)\s+(" + NUM + r")\s+(" + NUM + r")\s+(" + NUM + r")$",
                      candidate)
        if not m:
            return False
        category = _ws_collapse(m.group(1))
        if section and category.lower().startswith(section.lower() + " "):
            category = category[len(section) + 1:].strip()
        category = re.sub(r"^(?:Additional\s+Charges)\s+", "", category, flags=re.I)
        rows.append({
            "category": _norm(category),
            "non_taxable": _to_float(m.group(2)),
            "taxable": _to_float(m.group(3)),
            "total": _to_float(m.group(4)),
        })
        return True

    for raw in block.splitlines():
        ln = _clean_inline_noise(_ws_collapse(raw))
        if not ln:
            buf = ""
            continue
        if HEAD.match(ln) or RMROW.match(ln):
            buf = ""
            continue
        if re.match(r"^(Current Charges|Non-?Taxable|Taxable|Total)$", ln, re.I):
            continue
        if re.match(r"^Additional\s+Charges$", ln, re.I):
            section = "Additional Charges"
            buf = ""
            continue
        if ln.strip().lower() == "messages":
            if rows and rows[-1]["category"].endswith("&"):
                rows[-1]["category"] = rows[-1]["category"] + " Messages"
            continue
        candidate = (buf + " " + ln).strip() if buf else ln
        if _emit(candidate):
            buf = ""
        else:
            buf = candidate

    return rows

# --------------------------- Previous Payments ---------------------------
def _parse_previous_payments(det_text: str) -> List[Dict[str, Any]]:
    blk = _slice_between(det_text, HDR_PREV_PAY,
                         [HDR_REGISTERED, HDR_MONTHLY, HDR_DISC, HDR_CELCOM,
                          HDR_NONCEL, HDR_VAS, r"Page\s+\d+\s+of"])
    out: List[Dict[str, Any]] = []
    for raw in blk.splitlines():
        ln = _norm(raw)
        if not ln or re.search(r"^(Previous Payment Details|Description|Total)\b", ln, re.I):
            continue
        m = re.search(r"(.+?)\s+(" + DATE + r")\s+(" + NUM + r")\s*$", ln)
        if m:
            out.append({
                "description": _norm(m.group(1)),
                "date": m.group(2),
                "amount": _to_float(m.group(3))
            })
    return out

# ===== Registered Mobile Number (multi-page table + fuzzy) =====

# Make sure NUM_RM matches negatives + commas + optional RM
# Example accepted: "-15.00", "1,200.00", "RM 80.00"
NUM_RM = r"(?:(?i:rm)\s*)?-?\d{1,3}(?:,\d{3})*(?:\.\d{2})"
def _registered_row_fuzzy(cells: List[str]) -> Optional[Dict[str, Any]]:
    row_str = _ws_collapse(" ".join(c for c in (cells or []) if c is not None))
    if not row_str:
        return None

    m = re.match(
        r"^\s*((?:\+?6?0)?\s*(?:\d{2,3}[\s-]?\d{3,4}[\s-]?\d{4}|\d{2,3}[\s-]?\d{6,8}))\b",
        row_str
    )
    if not m:
        return None

    mobile = _mobile_normalize(m.group(1))

    # Pull ALL numeric tokens (including negatives, commas)
    nums = re.findall(NUM_RM, row_str)
    if len(nums) < 6:
        return None

    def clean(v: str) -> Optional[float]:
        v = _ws_collapse(v)
        v = re.sub(r"(?i)^rm\s*", "", v)
        v = v.replace(",", "")
        return _to_float(v)

    six = nums[-6:]
    return {
        "mobile": mobile,
        "credit_limit":      clean(six[0]),
        "one_time_amount":   clean(six[1]),
        "monthly_amount":    clean(six[2]),
        "usage_amount":      clean(six[3]),
        "discounts_rebates": clean(six[4]),
        "total_amount_rm":   clean(six[5]),
    }


def _parse_registered_from_tables_pages(pages, pages_text):
    HEADER_PAT = re.compile(HDR_REGISTERED, re.I)

    COL_ALIASES = {
        "mobile": [r"^mobile\s*(?:no\.?|number|numbers?)?$", r"^registered\s*mobile\s*number"],
        "credit_limit": [r"^credit\s*limit$"],
        "one_time_amount": [r"^(?:one[\s-]*time|one\s*time)\s*amount$"],
        "monthly_amount": [r"^monthly\s*amount$"],
        "usage_amount": [r"^usage\s*amount$"],
        "discounts_rebates": [
            r"^discounts?\s*&\s*rebates$",
            r"^discount\s*&\s*rebates$",
            r"^discounts?/?rebates$",
            r"^discount$",
        ],
        "total_amount_rm": [
            r"^(?:amount|amount\s*\(rm\)|total\s*amount\s*\(rm\)|amount\s*rm|amount\s*\(rm\))$"
        ],
    }

    def canon_col(name: str) -> Optional[str]:
        n = _ws_collapse(name).lower()
        n = re.sub(r"\s+", " ", n)
        for key, pats in COL_ALIASES.items():
            for p in pats:
                if re.match(p, n, re.I):
                    return key
        return None

    # Find first page that contains the section header
    pg_idx = next((i for i, t in enumerate(pages_text) if re.search(HEADER_PAT, t)), None)
    if pg_idx is None:
        return []

    # ✅ IMPORTANT FIX:
    # The registered table can span multiple pages (your PDF runs Page 3–6).
    # So we continue scanning forward until we hit the next major section.
    STOP_PAT = re.compile(
        r"(?:^|\b)(Monthly\s+Amount|Discount\s*&\s*Rebates|Value\s+Added\s+Services|DETAILED\s+CHARGES)\b",
        re.I
    )

    candidate_pages = []
    for i in range(pg_idx, len(pages)):
        t = pages_text[i] or ""
        if i == pg_idx or re.search(HEADER_PAT, t):
            candidate_pages.append(pages[i])
            continue
        # stop when the next section starts (typically right after the table ends)
        if STOP_PAT.search(t):
            break
        # If neither header nor stop words, still try 1-2 pages more, but don’t drift too far
        if len(candidate_pages) >= 6:
            break

    def tables_from_page(pg):
        out = []
        cfgs = [
            dict(vertical_strategy="lines", horizontal_strategy="lines",
                 intersection_tolerance=5, snap_tolerance=3, keep_blank_chars=True),
            dict(vertical_strategy="lines", horizontal_strategy="text",
                 text_tolerance=2, intersection_tolerance=5, keep_blank_chars=True),
            dict(vertical_strategy="text", horizontal_strategy="text",
                 text_tolerance=2, intersection_tolerance=5, keep_blank_chars=True),
        ]
        for cfg in cfgs:
            try:
                out.extend(pg.extract_tables(cfg) or [])
            except TypeError:
                cfg2 = {k: v for k, v in cfg.items() if k != "keep_blank_chars"}
                out.extend(pg.extract_tables(cfg2) or [])
        return out

    rows_out: List[Dict[str, Any]] = []
    seen = set()

    def parse_amount(cell: Optional[str]) -> Optional[float]:
        if cell is None:
            return None
        c = _ws_collapse(cell)
        c = re.sub(r"(?i)^rm\s*", "", c)
        c = c.replace(",", "")
        return _to_float(c)

    def looks_like_mobile(s: str) -> bool:
        return re.fullmatch(
            r"(?:\+?6?0)?\s*(?:\d{2,3}[\s-]?\d{3,4}[\s-]?\d{4}|\d{2,3}[\s-]?\d{6,8})",
            _ws_collapse(s) or ""
        ) is not None

    for pg in candidate_pages:
        for tbl in tables_from_page(pg):
            cleaned = [[_ws_collapse(c) for c in (row or [])] for row in (tbl or [])]
            if not cleaned:
                continue

            header_idx = None
            for r_i, row in enumerate(cleaned[:8]):
                lows = [c.lower() for c in row]
                if any("mobile" in c for c in lows) and any(
                    re.search(r"amount|rm|limit|discount|usage", c) for c in lows
                ):
                    header_idx = r_i
                    break
            if header_idx is None:
                continue

            header = cleaned[header_idx]
            colmap: Dict[int, str] = {}
            for j, name in enumerate(header):
                k = canon_col(name)
                if k:
                    colmap[j] = k
            if not colmap or "mobile" not in colmap.values():
                continue

            for row in cleaned[header_idx + 1:]:
                if row and re.match(r"(?i)^total\b", row[0] or ""):
                    continue

                rec: Dict[str, Any] = {}
                for j, cell in enumerate(row):
                    key = colmap.get(j)
                    if not key:
                        continue
                    if key == "mobile":
                        rec["mobile"] = _ws_collapse(cell)
                    else:
                        rec[key] = parse_amount(cell)

                mob = rec.get("mobile")
                numeric_fields = [
                    "credit_limit", "one_time_amount", "monthly_amount",
                    "usage_amount", "discounts_rebates", "total_amount_rm"
                ]
                have_nums = sum(1 for k in numeric_fields if isinstance(rec.get(k), (int, float)))

                # fallback fuzzy if table cells are broken
                if (not mob) or (have_nums < 4) or (mob and not looks_like_mobile(mob)):
                    fuzzy = _registered_row_fuzzy(row)
                    if fuzzy:
                        rec = fuzzy
                        mob = rec["mobile"]
                        have_nums = 6

                if not mob or not looks_like_mobile(mob) or have_nums < 4:
                    continue

                mob_norm = _mobile_normalize(mob)
                rec["mobile"] = mob_norm

                if mob_norm in seen:
                    for r in rows_out:
                        if r["mobile"] == mob_norm:
                            for k, v in rec.items():
                                if k != "mobile" and v is not None:
                                    r[k] = v
                            break
                else:
                    rows_out.append(rec)
                    seen.add(mob_norm)

    if rows_out:
        return rows_out

    # Fallback: fuzzy across rows (no header)
    for pg in candidate_pages:
        for tbl in tables_from_page(pg):
            for row in (tbl or []):
                fuzzy = _registered_row_fuzzy([_ws_collapse(c) for c in (row or [])])
                if not fuzzy:
                    continue
                mob = _mobile_normalize(fuzzy["mobile"])
                if mob in seen:
                    continue
                fuzzy["mobile"] = mob
                rows_out.append(fuzzy)
                seen.add(mob)

    return rows_out


def _parse_registered_text_block(text: str) -> List[Dict[str, Any]]:
    """
    Keep your existing text-block parser as last fallback (unchanged),
    but now table parser should already return far more mobiles.
    """
    MOBILE_FLEX = (
        r"(?:\+?6?0)?\s*(?:"
        r"\d{2,3}[\s-]?\d{3,4}[\s-]?\d{4}"
        r"|\d{2,3}[\s-]?\d{6,8}"
        r")"
    )
    NUM_TOK = NUM_RM

    blk = _slice_between(
        text,
        HDR_REGISTERED,
        [HDR_MONTHLY, HDR_DISC, HDR_CELCOM, HDR_NONCEL,
         r"Local\s+Calls\s*&\s*Messages", HDR_VAS, r"Page\s+\d+\s+of"],
        span=250000
    )
    out: List[Dict[str, Any]] = []
    if not blk:
        return out

    lines = [_ws_collapse(l) for l in blk.splitlines() if _ws_collapse(l)]

    def looks_like_mobile(s: str) -> Optional[str]:
        m = re.match(r"^\s*(" + MOBILE_FLEX + r")\b", s, re.I)
        return _ws_collapse(m.group(1)) if m else None

    current: Optional[Dict[str, Any]] = None
    nums_buffer: List[str] = []

    def flush_if_complete():
        nonlocal current, nums_buffer
        if current and len(nums_buffer) >= 6:
            six = nums_buffer[-6:]
            current.update({
                "credit_limit":      _to_float(re.sub(r"^RM\s*", "", six[0], flags=re.I)),
                "one_time_amount":   _to_float(re.sub(r"^RM\s*", "", six[1], flags=re.I)),
                "monthly_amount":    _to_float(re.sub(r"^RM\s*", "", six[2], flags=re.I)),
                "usage_amount":      _to_float(re.sub(r"^RM\s*", "", six[3], flags=re.I)),
                "discounts_rebates": _to_float(re.sub(r"^RM\s*", "", six[4], flags=re.I)),
                "total_amount_rm":   _to_float(re.sub(r"^RM\s*", "", six[5], flags=re.I)),
            })
            out.append(current)
            current = None
            nums_buffer = []

    HEADER_ROW = re.compile(
        r"^(?:Discounts?\s*&\s*Rebates|Description(?:\s+Amount\s*\(RM\))?|Amount\s*\(RM\))$",
        re.I
    )
    REGISTERED_TOTAL_6 = re.compile(r"^Total(?:\s+" + NUM_TOK + r"){6}\s*$", re.I)

    for ln in lines:
        if HEADER_ROW.search(ln) or REGISTERED_TOTAL_6.match(ln):
            current = None
            nums_buffer = []
            continue

        if current is None:
            mob = looks_like_mobile(ln)
            if mob:
                current = {"mobile": _mobile_normalize(mob)}
                tail = ln[ln.lower().find(mob.lower()) + len(mob):].strip()
                nums = re.findall(NUM_TOK, tail, flags=re.I)
                nums_buffer.extend(nums)
            continue

        nums = re.findall(NUM_TOK, ln, flags=re.I)
        if nums:
            nums_buffer.extend(nums)
            flush_if_complete()

    flush_if_complete()
    return out

# --------------------------- Detailed Charges (Monthly) ---------------------------
def _parse_monthly_items(det_text: str) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    items: List[Dict[str, Any]] = []
    total: Optional[float] = None

    HEADER_HINT = re.compile(
        r"(?:^|\n)\s*(?:Description\b|From(?:\s+Date)?\b|To(?:\s+Date)?\b|"
        r"Period\s+From\b|Period\s+To\b|Amount\s*\(RM\))",
        re.I
    )

    starts = [m.end() for m in re.finditer(
        r"DETAILED\s+CHARGES.{0,200}?Monthly\b", det_text, re.I | re.S)]
    blk = ""
    for st in starts:
        sub = det_text[st:st + 600_000]
        if HEADER_HINT.search(sub):
            end_pat = re.compile("|".join([
                HDR_DISC, HDR_CELCOM, HDR_NONCEL, HDR_VAS,
                HDR_REGISTERED, r"Page\s+\d+\s+of"
            ]), re.I)
            m_end = end_pat.search(sub)
            blk = sub[:m_end.start()] if m_end else sub
            break

    if not blk:
        blk = _slice_between(
            det_text,
            HDR_MONTHLY,
            [HDR_DISC, HDR_CELCOM, HDR_NONCEL, HDR_VAS,
             HDR_REGISTERED, r"Page\s+\d+\s+of"],
            span=500_000
        )

    if not blk:
        return items, total

    carry: List[str] = []
    pending: Optional[Dict[str, Any]] = None
    pending_amount: Optional[float] = None
    pending_desc: Optional[str] = None

    def _clean_amt(s: str) -> Optional[float]:
        return _to_float(re.sub(r"(?i)^RM\s*", "", (s or "").strip()))

    for raw in blk.splitlines():
        ln = _ws_collapse(raw)
        if not ln:
            continue
        if re.match(
            r"^(?:Monthly\b.*|Detail(?:ed)?\s*Charges\b.*Monthly\b|Description|"
            r"From(?:\s+Date)?|To(?:\s+Date)?|Period\s+From|Period\s+To|"
            r"Amount\s*\(RM\))$",
            ln, re.I
        ):
            continue

        mt = re.match(r"^Total\s+(" + NUM_RM + r")$", ln, re.I)
        if mt:
            total = _clean_amt(mt.group(1))
            continue

        mA = re.match(
            r"(.+?)\s+(" + DATE + r")\s+(" + DATE + r")\s+(" + NUM_RM + r")$",
            ln, re.I
        )
        if mA:
            desc = " ".join(carry + [mA.group(1)]).strip() if carry else _norm(mA.group(1))
            items.append({
                "description": desc,
                "from_date": mA.group(2),
                "to_date": mA.group(3),
                "amount_rm": _clean_amt(mA.group(4))
            })
            carry.clear()
            pending = None
            pending_amount = None
            pending_desc = None
            continue

        mA_prime = re.match(
            r"^(" + DATE + r")\s+(" + DATE + r")\s+(" + NUM_RM + r")$",
            ln, re.I
        )
        if mA_prime and (carry or pending_desc):
            desc = " ".join(carry).strip() if carry else pending_desc
            items.append({
                "description": desc,
                "from_date": mA_prime.group(1),
                "to_date": mA_prime.group(2),
                "amount_rm": _clean_amt(mA_prime.group(3))
            })
            carry.clear()
            pending = None
            pending_amount = None
            pending_desc = None
            continue

        mB = re.match(
            r"(.+?)\s*\(\s*(" + DATE + r")\s*[–\-]\s*(" + DATE + r")\s*\)\s+(" + NUM_RM + r")$",
            ln, re.I
        )
        if mB:
            desc = " ".join(carry + [mB.group(1)]).strip() if carry else _norm(mB.group(1))
            items.append({
                "description": desc,
                "from_date": mB.group(2),
                "to_date": mB.group(3),
                "amount_rm": _clean_amt(mB.group(4))
            })
            carry.clear()
            pending = None
            pending_amount = None
            pending_desc = None
            continue

        mA2 = re.match(
            r"(.+?)\s+(" + DATE + r")\s+(" + DATE + r")$",
            ln, re.I
        )
        if mA2:
            desc = " ".join(carry + [mA2.group(1)]).strip() if carry else _norm(mA2.group(1))
            pending = {
                "description": desc,
                "from_date": mA2.group(2),
                "to_date": mA2.group(3)
            }
            carry.clear()
            continue

        mB2 = re.match(
            r"(.+?)\s*\(\s*(" + DATE + r")\s*[–\-]\s*(" + DATE + r")\s*\)$",
            ln, re.I
        )
        if mB2:
            desc = " ".join(carry + [mB2.group(1)]).strip() if carry else _norm(mB2.group(1))
            pending = {
                "description": desc,
                "from_date": mB2.group(2),
                "to_date": mB2.group(3)
            }
            carry.clear()
            continue

        mDatesOnly = re.match(r"^(" + DATE + r")\s+(" + DATE + r")$", ln, re.I)
        if mDatesOnly and (carry or pending_desc):
            desc = " ".join(carry).strip() if carry else pending_desc
            if pending_amount is not None:
                items.append({
                    "description": desc,
                    "from_date": mDatesOnly.group(1),
                    "to_date": mDatesOnly.group(2),
                    "amount_rm": _clean_amt(str(pending_amount))
                })
                pending_amount = None
                pending_desc = None
                carry.clear()
                continue
            else:
                pending = {
                    "description": desc,
                    "from_date": mDatesOnly.group(1),
                    "to_date": mDatesOnly.group(2)
                }
                carry.clear()
                continue

        if re.match(r"^" + NUM_RM + r"$", ln, re.I):
            if pending:
                pending["amount_rm"] = _clean_amt(ln)
                items.append(pending)
                pending = None
                continue
            if carry and not pending:
                pending_amount = _clean_amt(ln)
                pending_desc = " ".join(carry).strip()
                carry.clear()
                continue

        if not re.fullmatch(r"\d{1,3}$", ln):
            carry.append(ln)

    if (total is None or abs(total) < 1e-9) and items:
        total = _sum_amount(items, "amount_rm")

    return items, total

def _parse_discount_items(det_text: str) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    MOBILE_FLEX = (
        r"(?:\+?6?0)?\s*(?:"
        r"\d{2,3}[\s-]?\d{3,4}[\s-]?\d{4}"
        r"|\d{2,3}[\s-]?\d{6,8}"
        r")"
    )

    blk = _slice_between(
        det_text,
        HDR_DISC,
        [
            HDR_CELCOM, HDR_NONCEL, r"Local\s+Calls\s*&\s*Messages",
            HDR_VAS, HDR_REGISTERED, HDR_MONTHLY, r"Page\s+\d+\s+of"
        ],
        span=200000
    )
    items: List[Dict[str, Any]] = []
    total: Optional[float] = None
    if not blk:
        return items, total

    lines = [_ws_collapse(l) for l in blk.splitlines() if _ws_collapse(l)]

    HEADER_ROW = re.compile(
        r"^(?:Discounts?\s*&\s*Rebates|Description(?:\s+Amount\s*\(RM\))?|Amount\s*\(RM\))$",
        re.I
    )
    REG_ROW_6  = re.compile(r"^" + MOBILE_FLEX + r"(?:\s+" + NUM_RM + r"){6}\s*$", re.I)
    REG_TOT_6  = re.compile(r"^Total(?:\s+" + NUM_RM + r"){6}\s*$", re.I)
    ANY_MOBILE = re.compile(MOBILE_FLEX, re.I)

    BAN_WORDS = re.compile(
        r"\b(?:DETAILED\s+CHARGES|Monthly(?:\s+Amount)?|From\s+Date|To\s+Date|"
        r"Bill\s+Statement|Account\s+Number|Mobile\s+Number|Description|"
        r"Amount\s*\(RM\)|Registered\s+Mobile\s+Number|Your\s+Calls|"
        r"Value\s+Added\s+Services)\b",
        re.I
    )

    pending_desc: List[str] = []
    started = False

    def _clean_amt(s: str) -> Optional[float]:
        return _to_float(re.sub(r"(?i)^RM\s*", "", (s or "").strip()))

    def _emit_pending_with_amount(amount_str: str):
        nonlocal pending_desc, started
        desc = _ws_collapse(" ".join(pending_desc)).strip()
        amt = _clean_amt(amount_str)
        if desc and amt is not None and amt <= 0:
            items.append({"description": desc, "amount_rm": amt})
            started = True
        pending_desc = []

    for ln in lines:
        if HEADER_ROW.match(ln) or REG_TOT_6.match(ln) or REG_ROW_6.match(ln):
            pending_desc = []
            started = True
            continue

        if ANY_MOBILE.search(ln):
            continue

        m_total = re.match(r"^Total\s+(" + NUM_RM + r")$", ln, re.I)
        if m_total:
            total = _clean_amt(m_total.group(1))
            pending_desc = []
            started = True
            continue

        if not started:
            if ln.count(":") >= 1 or BAN_WORDS.search(ln):
                continue

        m_tail = re.search(r"(" + NUM_RM + r")\s*$", ln, re.I)
        if m_tail:
            head = ln[:m_tail.start()].strip()
            if re.fullmatch(r"(?i)total", head):
                continue
            if head and not BAN_WORDS.search(head):
                pending_desc.append(head)
                _emit_pending_with_amount(m_tail.group(1))
                continue

        if pending_desc and re.fullmatch(NUM_RM, ln, re.I):
            _emit_pending_with_amount(ln)
            continue

        if not BAN_WORDS.search(ln) and not re.fullmatch(r"[—\-]+", ln):
            pending_desc.append(ln)

    if (total is None or abs(total) < 1e-9) and items:
        total = _sum_amount(items, "amount_rm")

    return items, total

# --------------------------- Calls / Local Calls ---------------------------
def _parse_calls(det_text: str, hdr: str) -> Tuple[List[Dict[str, Any]], Optional[float], Optional[str]]:
    """
    Generic call-section parser.

    hdr can be:
      - HDR_CELCOM          → "Your Calls To Celcom Numbers"
      - HDR_NONCEL          → "Your Calls To Non-Celcom Numbers"
      - HDR_LOCAL           → "Local Calls & Messages"
    """
    if hdr == HDR_LOCAL:
        end_pats = [HDR_CELCOM, HDR_NONCEL, HDR_VAS, HDR_REGISTERED, r"Page\s+\d+\s+of"]
    else:
        end_pats = [HDR_NONCEL if hdr == HDR_CELCOM else HDR_CELCOM,
                    HDR_VAS, HDR_REGISTERED, r"Page\s+\d+\s+of"]

    blk = _slice_between(det_text, hdr, end_pats)
    rows: List[Dict[str, Any]] = []
    total_amt = None
    total_dur = None
    for raw in blk.splitlines():
        ln = _norm(raw)
        if not ln or re.search(
            r"^(Date\s+Time|Called Number|Duration|Free Calls|Amount \(RM\))$",
            ln, re.I
        ):
            continue

        mt = re.search(
            r"^Total\s+([0-9]{1,3}:[0-9]{2}(?::[0-9]{2})?)\s+(" + NUM + r")\s+(" + NUM + r")\s*$",
            ln, re.I
        )
        if mt:
            total_dur = mt.group(1)
            total_amt = _to_float(mt.group(3))
            continue

        m = re.search(
            r"(" + DATE + r")\s+(" + TIME + r")\s+(\+?6?0?\s?\d[\d\- ]+)\s+"
            r"([0-9]{1,3}:[0-9]{2}(?::[0-9]{2})?)\s+(" + NUM + r")\s+(" + NUM + r")\s*$",
            ln
        )
        if m:
            rows.append({
                "date": m.group(1),
                "time": m.group(2),
                "called_number": m.group(3),
                "duration": m.group(4),
                "free_calls": _to_float(m.group(5)),
                "amount_rm": _to_float(m.group(6))
            })

    if (total_amt is None or abs(total_amt) < 1e-9) and rows:
        total_amt = _sum_amount(rows, "amount_rm")
    if total_dur is None and rows:
        total_dur = _sum_duration(rows, "duration")

    return rows, total_amt, total_dur

def _parse_vas(det_text: str) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    blk = _slice_between(det_text, HDR_VAS, [HDR_REGISTERED, r"Page\s+\d+\s+of"])
    rows: List[Dict[str, Any]] = []
    total = None
    last_row: Optional[Dict[str, Any]] = None

    for raw in blk.splitlines():
        ln = _norm(raw)
        if not ln or re.search(
            r"^(Date\s+Time|Description|Called Number|Amount \(RM\)|Value Added Services)$",
            ln, re.I
        ):
            continue

        mt = re.search(r"^Total\s+(" + NUM + r")\s*$", ln, re.I)
        if mt:
            total = _to_float(mt.group(1))
            continue

        m = re.match(r"^(" + DATE + r")(?:\s+(" + TIME + r"))?\s+(.*)$", ln)
        if m:
            date, tm, tail = m.groups()
            m_amt = re.search(r"(" + NUM + r")\s*$", tail)
            amt = _to_float(m_amt.group(1)) if m_amt else None
            if m_amt:
                tail = tail[:m_amt.start()].strip()
            m_num = re.search(r"(\+?6?0?\d[\d\-]+)\s*$", tail)
            called = None
            if m_num:
                called = m_num.group(1)
                tail = tail[:m_num.start()].strip()
            row = {
                "date": date, "time": tm, "description": tail or None,
                "called_number": called, "amount_rm": amt
            }
            rows.append(row)
            last_row = row
        else:
            if last_row is not None:
                last_row["description"] = _ws_collapse(
                    (last_row.get("description") or "") + " " + ln
                )

    if (total is None or abs(total) < 1e-9) and rows:
        total = _sum_amount(rows, "amount_rm")

    return rows, total

# --------------------------- One bill assembly ---------------------------
def _parse_single_bill(pages: List[Any], pages_text: List[str]) -> Dict[str, Any]:
    first_txt = pages_text[0]
    all_txt   = "\n".join(pages_text)

    header = _parse_header(first_txt, all_txt)
    acct   = _parse_account_summary(first_txt, all_txt)
    ccb    = _parse_current_charges_breakdown(first_txt)

    # Detailed charges (from the first "DETAILED CHARGES" onward)
    det_text_all = ""
    for i, txt in enumerate(pages_text):
        if re.search(HDR_DETAILED, txt, re.I):
            det_text_all = "\n".join(pages_text[i:])
            break

    prev_pay: List[Dict[str, Any]] = []
    registered: List[Dict[str, Any]] = []
    monthly_items: List[Dict[str, Any]] = []
    disc_items: List[Dict[str, Any]] = []
    calls_cel, calls_non, vas_rows = [], [], []
    local_calls: List[Dict[str, Any]] = []
    monthly_total = None
    disc_total = None
    cel_total = None
    non_total = None
    vas_total = None
    local_total = None

    # duration totals
    cel_dur_total = None
    non_dur_total = None
    local_dur_total = None

    if det_text_all:
        prev_pay = _parse_previous_payments(det_text_all)

        # Registered section (table-first + fuzzy), fallback to text block
        registered = _parse_registered_from_tables_pages(pages, pages_text)
        if not registered:
            registered = _parse_registered_text_block(det_text_all)

        monthly_items, monthly_total = _parse_monthly_items(det_text_all)
        disc_items,    disc_total    = _parse_discount_items(det_text_all)

        # Local Calls & Messages section (if present)
        local_calls, local_total, local_dur_total = _parse_calls(det_text_all, HDR_LOCAL)

        # Calls to Celcom / Non-Celcom
        calls_cel,     cel_total, cel_dur_total  = _parse_calls(det_text_all, HDR_CELCOM)
        calls_non,     non_total, non_dur_total  = _parse_calls(det_text_all, HDR_NONCEL)

        # VAS
        vas_rows,      vas_total     = _parse_vas(det_text_all)

    # Safety net for Registered
    if not registered:
        registered = _parse_registered_text_block(all_txt)
    if not registered:
        for txt in pages_text:
            rows = _parse_registered_text_block(txt)
            if rows:
                registered = rows
                break

    # --------- Synthesize monthly line items per mobile if missing ---------
    billing_from = (header.get("billing_period") or {}).get("from")
    billing_to   = (header.get("billing_period") or {}).get("to")
    plan_name    = header.get("plan_name") or "Monthly Commitment"

    synth_items_per_mobile: Dict[str, List[Dict[str, Any]]] = {}
    if not monthly_items and registered:
        monthly_total = _sum_amount(registered, "monthly_amount")
        for r in registered:
            mob = r.get("mobile")
            amount = r.get("monthly_amount")
            if mob and isinstance(amount, (int, float)) and amount != 0:
                synth_items_per_mobile[mob] = [{
                    "description": f"{plan_name} - Monthly Fee",
                    "from_date": billing_from,
                    "to_date": billing_to,
                    "amount_rm": round(float(amount), 2),
                }]

    if (monthly_total is None or abs(monthly_total) < 1e-9) and monthly_items:
        monthly_total = _sum_amount(monthly_items, "amount_rm")
    if (disc_total is None or abs(disc_total) < 1e-9) and disc_items:
        disc_total = _sum_amount(disc_items, "amount_rm")
    if (cel_total is None or abs(cel_total) < 1e-9) and calls_cel:
        cel_total = _sum_amount(calls_cel, "amount_rm")
    if (non_total is None or abs(non_total) < 1e-9) and calls_non:
        non_total = _sum_amount(calls_non, "amount_rm")
    if (vas_total is None or abs(vas_total) < 1e-9) and vas_rows:
        vas_total = _sum_amount(vas_rows, "amount_rm")
    if (cel_dur_total is None) and calls_cel:
        cel_dur_total = _sum_duration(calls_cel, "duration")
    if (non_dur_total is None) and calls_non:
        non_dur_total = _sum_duration(calls_non, "duration")
    if (local_total is None or abs(local_total) < 1e-9) and local_calls:
        local_total = _sum_amount(local_calls, "amount_rm")
    if local_dur_total is None and local_calls:
        local_dur_total = _sum_duration(local_calls, "duration")

    # Fallback: if no explicit Local section, synthesize local duration from cel/non
    if local_dur_total is None and (cel_dur_total or non_dur_total):
        def _safe_hms(s: Optional[str]) -> str:
            return s if isinstance(s, str) and ":" in s else "00:00:00"
        local_dur_total = _seconds_to_hms(
            _hms_to_seconds(_safe_hms(cel_dur_total)) +
            _hms_to_seconds(_safe_hms(non_dur_total))
        )

    # Per-number block
    per_number_details: Dict[str, Any] = {}
    mobiles = [r["mobile"] for r in registered] or [header.get("service_number") or "UNKNOWN"]
    bill_discount_items = disc_items or []

    for mob in mobiles:
        line_items = monthly_items if monthly_items else synth_items_per_mobile.get(mob, [])
        monthly_commit_desc = (line_items[0]["description"] if line_items else None)

        # Choose local calls list: explicit local section if present, else Celcom+NonCelcom union
        if local_calls:
            local_calls_for_mob = local_calls
        else:
            local_calls_for_mob = (calls_cel + calls_non)

        per_number_details[mob] = {
            "customer_name": header.get("customer_name"),
            "account_number": header.get("account_number"),
            "monthly_amount": monthly_total,
            "usage_amount": (cel_total or 0.0) + (non_total or 0.0),
            "discounts_rebates": disc_total,
            "total_amount_rm": None,
            "monthly_line_items": line_items,
            "monthly_commit_desc": monthly_commit_desc,
            "discount_rebate_items": (bill_discount_items if len(mobiles) == 1 else []),
            "local_calls_messages": local_calls_for_mob,
            "value_added_services": vas_rows,
            "calls_to_celcom": calls_cel,
            "calls_to_non_celcom": calls_non,
            "calls_to_celcom_duration_total": cel_dur_total,
            "calls_to_non_celcom_duration_total": non_dur_total,
            "local_calls_duration_total": local_dur_total,
        }

    # Reconcile with Registered totals if present
    def _sum(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
        vals = [r.get(key) for r in rows if isinstance(r.get(key), (int, float))]
        return round(sum(vals), 2) if vals else None

    reg_map = {r["mobile"]: r for r in (registered or [])}
    for mob, d in per_number_details.items():
        r = reg_map.get(mob)
        if r:
            if r.get("monthly_amount") is not None:
                d["monthly_amount"] = r.get("monthly_amount")
                if not monthly_items and not d.get("monthly_line_items"):
                    d["monthly_line_items"] = [{
                        "description": f"{plan_name} - Monthly Fee",
                        "from_date": billing_from,
                        "to_date": billing_to,
                        "amount_rm": r.get("monthly_amount"),
                    }]
                    d["monthly_commit_desc"] = d["monthly_line_items"][0]["description"]
            if r.get("usage_amount") is not None:
                d["usage_amount"] = r.get("usage_amount")
            if r.get("discounts_rebates") is not None:
                d["discounts_rebates"] = r.get("discounts_rebates")

        total_calc = (
            (d.get("monthly_amount") or 0.0)
            + (d.get("usage_amount") or 0.0)
            + (d.get("discounts_rebates") or 0.0)
        )
        d["total_amount_rm"] = round(total_calc, 2)

    registered_totals = {
        "count_numbers": len(registered or []),
        "sum_one_time": _sum(registered, "one_time_amount"),
        "sum_monthly": _sum(registered, "monthly_amount"),
        "sum_usage": _sum(registered, "usage_amount"),
        "sum_discounts": _sum(registered, "discounts_rebates"),
        "sum_total_rm": _sum(registered, "total_amount_rm"),
    }

    if monthly_total is None:
        monthly_total = registered_totals.get("sum_monthly")

    return {
        **header,
        "account_summary": acct,
        "current_charges_breakdown": ccb,
        "payment_slip": {
            "amount_paid": None,
            "total_amount_due": acct.get("total_amount_due")
        },
        "previous_payments": prev_pay,
        "registered_mobile_numbers": registered,
        "registered_numbers_note": None,
        "registered_totals": registered_totals,
        "per_number_details": per_number_details,
        "discount_rebate_items_bill": disc_items or [],
        "totals": {
            "monthly_amount": monthly_total,
            "discounts_rebates": disc_total,
            "calls_celcom": cel_total,
            "calls_noncelcom": non_total,
            "value_added_services": vas_total,
            "duration_celcom": cel_dur_total,
            "duration_noncelcom": non_dur_total,
            "duration_local_calls_messages": local_dur_total,
        }
    }

# --------------------------- Public parse() (raw) ---------------------------
def parse_raw(pdf_path: str, include_calls: bool = True) -> Dict[str, Any]:
    out = {"bills": []}
    with pdfplumber.open(pdf_path) as pdf:
        _all_txt, pages_txt = _get_all_texts(pdf)
        bill = _parse_single_bill(pdf.pages, pages_txt)
        if not include_calls:
            for mob in bill["per_number_details"].values():
                mob["local_calls_messages"] = []
                mob["value_added_services"] = []
                mob["calls_to_celcom"] = []
                mob["calls_to_non_celcom"] = []
                mob["calls_to_celcom_duration_total"] = None
                mob["calls_to_non_celcom_duration_total"] = None
                mob["local_calls_duration_total"] = None
        out["bills"].append(bill)
    return out

# =========================== DB ADAPTER (no logic change except local_calls) ==========================
def _to_iso(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(d, fmt).date().isoformat()
        except ValueError:
            continue
    return d  # leave as-is if unknown format

def to_invoice_package(parsed: dict) -> dict:
    """
    Map Celcom raw dict → common invoice package for generic usage.

    Output envelope:
    {
      "invoice": {...},
      "numbers": [ { "mobile":..., "customer_name":..., "account_number":..., "charges":[...], "duration_totals": {...} } ],
      "charges_summary": [ {label, non_taxable, taxable, total} ],
      "previous_payments": [...]
    }
    """
    b = (parsed.get("bills") or [{}])[0]
    acct = b.get("account_summary") or {}
    period = b.get("billing_period") or {}

    invoice = {
        "vendor": "celcom",
        "invoice_number": b.get("bill_statement_number"),
        "account_number": b.get("account_number"),
        "bill_date": _to_iso(b.get("bill_date")),
        "period_start": _to_iso(period.get("from")),
        "period_end": _to_iso(period.get("to")),
        "currency": "MYR",

        # DB-friendly extras
        "credit_limit": b.get("credit_limit"),
        "deposit": b.get("deposit"),
        "plan_name": b.get("plan_name"),
        "previous_balance": acct.get("previous_balance"),
        "total_overdue_charges": acct.get("overdue_charges"),

        # Financials (taken from Account Summary)
        "subtotal": acct.get("monthly_charges_rm"),
        "tax_total": acct.get("service_tax_6pct"),
        "rounding_adjustment": acct.get("rounding_adjustment"),
        "total_current_charges": acct.get("total_current_charges"),

        "grand_total": acct.get("total_amount_due"),
    }

    numbers: List[dict] = []
    reg = b.get("registered_mobile_numbers") or []
    mobiles = [r.get("mobile") for r in reg if r.get("mobile")] or [
        b.get("service_number") or "UNKNOWN"
    ]

    per = b.get("per_number_details") or {}
    for mob in mobiles:
        d = per.get(mob, {})
        charges = []
        if d.get("monthly_amount") is not None:
            charges.append({
                "category": "Monthly",
                "label": d.get("monthly_commit_desc") or "Monthly Fee",
                "amount": d.get("monthly_amount"),
                "from": _to_iso((b.get("billing_period") or {}).get("from")),
                "to": _to_iso((b.get("billing_period") or {}).get("to")),
            })
        if d.get("usage_amount") is not None:
            charges.append({
                "category": "Usage",
                "label": "Local Calls & Messages (Celcom/Non-Celcom)",
                "amount": d.get("usage_amount"),
            })
        if d.get("discounts_rebates") is not None:
            charges.append({
                "category": "Discounts",
                "label": "Discounts & Rebates",
                "amount": d.get("discounts_rebates"),
            })
        for it in d.get("monthly_line_items") or []:
            charges.append({
                "category": "MonthlyItem",
                "label": it.get("description"),
                "amount": it.get("amount_rm"),
                "from": _to_iso(it.get("from_date")),
                "to": _to_iso(it.get("to_date")),
            })

        numbers.append({
            "mobile": mob,
            "customer_name": d.get("customer_name") or b.get("customer_name"),
            "account_number": d.get("account_number") or b.get("account_number"),
            "charges": charges,
            "duration_totals": {
                "celcom": d.get("calls_to_celcom_duration_total"),
                "non_celcom": d.get("calls_to_non_celcom_duration_total"),
                "local_total": d.get("local_calls_duration_total"),
            }
        })

    charges_summary = []
    for r in (b.get("current_charges_breakdown") or []):
        charges_summary.append({
            "label": r.get("category"),
            "non_taxable": r.get("non_taxable"),
            "taxable": r.get("taxable"),
            "total": r.get("total"),
        })

    pkg = {
        "invoice": invoice,
        "numbers": numbers,
        "charges_summary": charges_summary,
        "previous_payments": b.get("previous_payments") or []
    }

    return pkg

def extract(pdf_path: str, include_calls: bool = True) -> dict:
    """Convenience: parse_raw → to_invoice_package."""
    return to_invoice_package(parse_raw(pdf_path, include_calls=include_calls))

# --------------------------- Flat JSON for DB (Stored Procedure Payload) ---------------------------
def _flatten_for_db(
    parsed: Dict[str, Any],
    include_one_time: bool = False
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Flatten raw parsed Celcom bill into arrays used for:
      - sp_Upsert_InvoicePackage_JSON_Celcom (@CelcomJson)
    """
    flat = dict(
        bills=[], current_charges_breakdown=[], registered=[], previous_payments=[],
        per_number_totals=[], monthly_items=[], discount_rebate_items=[],
        discounts_rebates=[], local_calls_messages=[], value_added_services=[],
        calls_to_celcom=[], calls_to_non_celcom=[]
    )
    for bi, b in enumerate(parsed.get("bills", []), start=1):
        asu = b.get("account_summary") or {}
        flat["bills"].append({
            "bill_seq": bi,
            "bill_statement_month": b.get("bill_statement_month"),
            "bill_date": b.get("bill_date"),
            "bill_statement_number": b.get("bill_statement_number"),
            "service_number": b.get("service_number"),
            "account_number": b.get("account_number"),
            "credit_limit": b.get("credit_limit"),
            "deposit": b.get("deposit"),
            "name": b.get("customer_name"),
            "customer_name": b.get("customer_name"),
            "plan_name": b.get("plan_name"),
            "billing_from": (b.get("billing_period") or {}).get("from"),
            "billing_to": (b.get("billing_period") or {}).get("to"),
            "previous_balance": asu.get("previous_balance"),
            "total_payments": asu.get("total_payments"),
            "monthly_charges_rm": asu.get("monthly_charges_rm"),
            "service_tax_6pct": asu.get("service_tax_6pct"),
            "rounding_adjustment": asu.get("rounding_adjustment"),
            "total_current_charges": asu.get("total_current_charges"),
            "current_charges": asu.get("current_charges"),
            "overdue_charges": asu.get("overdue_charges"),
            "due_date": asu.get("due_date"),
            "total_amount_due": asu.get("total_amount_due"),
        })

        for r in (b.get("current_charges_breakdown") or []):
            flat["current_charges_breakdown"].append({
                "bill_seq": bi,
                "bill_statement_number": b.get("bill_statement_number"),
                "category": r.get("category"),
                "non_taxable": r.get("non_taxable"),
                "taxable": r.get("taxable"),
                "total": r.get("total"),
            })

        for r in (b.get("registered_mobile_numbers") or []):
            flat["registered"].append({
                "bill_seq": bi,
                "bill_statement_number": b.get("bill_statement_number"),
                "mobile": r.get("mobile"),
                "credit_limit": r.get("credit_limit"),
                "one_time_amount": r.get("one_time_amount"),
                "monthly_amount": r.get("monthly_amount"),
                "usage_amount": r.get("usage_amount"),
                "discounts_rebates": r.get("discounts_rebates"),
                "total_amount_rm": r.get("total_amount_rm"),
            })

        pnd = b.get("per_number_details") or {}
        reg_map = {r.get("mobile"): r for r in (b.get("registered_mobile_numbers") or [])}
        for mob, d in pnd.items():
            total_amount = d.get("total_amount_rm")
            if include_one_time:
                one_time = (reg_map.get(mob) or {}).get("one_time_amount")
                if isinstance(one_time, (int, float)) and one_time:
                    total_amount = (total_amount or 0.0) + one_time
            flat["per_number_totals"].append({
                "bill_seq": bi,
                "bill_statement_number": b.get("bill_statement_number"),
                "mobile": mob,
                "customer_name": d.get("customer_name"),
                "account_number": d.get("account_number"),
                "monthly_amount": d.get("monthly_amount"),
                "usage_amount": d.get("usage_amount"),
                "discounts_rebates": d.get("discounts_rebates"),
                "total_amount_rm": total_amount,
                "monthly_commit_desc": d.get("monthly_commit_desc"),
                "calls_to_celcom_duration_total": d.get("calls_to_celcom_duration_total"),
                "calls_to_non_celcom_duration_total": d.get("calls_to_non_celcom_duration_total"),
                "local_calls_duration_total": d.get("local_calls_duration_total"),
            })

            for it in (d.get("monthly_line_items") or []):
                flat["monthly_items"].append({
                    "bill_seq": bi,
                    "bill_statement_number": b.get("bill_statement_number"),
                    "mobile": mob,
                    "description": it.get("description"),
                    "from_date": it.get("from_date"),
                    "to_date": it.get("to_date"),
                    "amount_rm": it.get("amount_rm"),
                })
            for it in (d.get("discount_rebate_items") or []):
                flat["discount_rebate_items"].append({
                    "bill_seq": bi,
                    "bill_statement_number": b.get("bill_statement_number"),
                    "mobile": mob,
                    "description": it.get("description"),
                    "amount_rm": it.get("amount_rm"),
                })
            if not d.get("discount_rebate_items") and d.get("discounts_rebates") is not None:
                flat["discounts_rebates"].append({
                    "bill_seq": bi,
                    "bill_statement_number": b.get("bill_statement_number"),
                    "mobile": mob,
                    "description": "Discount & Rebates (Total)",
                    "total": d.get("discounts_rebates"),
                })

            def _push_calls(bucket: str, rows: List[Dict[str, Any]]):
                for r in rows or []:
                    flat[bucket].append({
                        "bill_seq": bi,
                        "bill_statement_number": b.get("bill_statement_number"),
                        "mobile": mob,
                        "date": r.get("date"),
                        "time": r.get("time"),
                        "called_number": r.get("called_number"),
                        "duration": r.get("duration"),
                        "free_calls": r.get("free_calls"),
                        "amount_rm": r.get("amount_rm"),
                    })

            _push_calls("calls_to_celcom", d.get("calls_to_celcom"))
            _push_calls("calls_to_non_celcom", d.get("calls_to_non_celcom"))

            for r in (d.get("value_added_services") or []):
                flat["value_added_services"].append({
                    "bill_seq": bi,
                    "bill_statement_number": b.get("bill_statement_number"),
                    "mobile": mob,
                    "date": r.get("date"),
                    "time": r.get("time"),
                    "description": r.get("description"),
                    "called_number": r.get("called_number"),
                    "amount_rm": r.get("amount_rm"),
                })

            # Local calls: prefer explicit local_calls_messages; fall back to cel+non union
            local_src = d.get("local_calls_messages") or (
                (d.get("calls_to_celcom") or []) + (d.get("calls_to_non_celcom") or [])
            )
            for r in local_src or []:
                flat["local_calls_messages"].append({
                    "bill_seq": bi,
                    "bill_statement_number": b.get("bill_statement_number"),
                    "mobile": mob,
                    "date": r.get("date"),
                    "time": r.get("time"),
                    "called_number": r.get("called_number"),
                    "duration": r.get("duration"),
                    "free_calls": r.get("free_calls"),
                    "amount_rm": r.get("amount_rm"),
                })

        for r in (b.get("previous_payments") or []):
            flat["previous_payments"].append({
                "bill_seq": bi,
                "bill_statement_number": b.get("bill_statement_number"),
                "description": r.get("description"),
                "date": r.get("date"),
                "amount": r.get("amount"),
            })

        if not flat["discount_rebate_items"]:
            for it in (b.get("discount_rebate_items_bill") or []):
                flat["discount_rebate_items"].append({
                    "bill_seq": bi,
                    "bill_statement_number": b.get("bill_statement_number"),
                    "mobile": "",
                    "description": it.get("description"),
                    "amount_rm": it.get("amount_rm"),
                })

    return flat

# ---------- Helper to build flat JSON payload for Celcom stored procedure ----------
def build_flat_json(
    pdf_path: str,
    include_calls: bool = True,
    include_one_time: bool = False
) -> Dict[str, Any]:
    """
    High-level helper: parse_raw() + _flatten_for_db().

    Use this to feed [dbo].[sp_Upsert_InvoicePackage_JSON_Celcom]:
        flat = build_flat_json("celcom.pdf", include_calls=True)
        payload = json.dumps(flat, ensure_ascii=False)
        EXEC sp_Upsert_InvoicePackage_JSON_Celcom @CelcomJson = payload;
    """
    parsed = parse_raw(pdf_path, include_calls=include_calls)
    return _flatten_for_db(parsed, include_one_time=include_one_time)

# --------------------------- Minimal CLI (debugging only) ---------------------------
def _cli():
    ap = argparse.ArgumentParser(
        description="Celcom PDF extractor (DB-aligned, Maxis-style interface)"
    )
    ap.add_argument("pdf", help="Path to the Celcom PDF")
    ap.add_argument("--out-json", help="Write the invoice package JSON to this path")
    ap.add_argument("--print-json", action="store_true",
                    help="Print invoice package JSON to stdout")
    ap.add_argument("--flat-json", help="Write the flat Celcom JSON (for stored proc) to this path")
    ap.add_argument("--print-flat", action="store_true",
                    help="Print flat Celcom JSON (for stored proc) to stdout")
    ap.add_argument("--no-itemized", action="store_true",
                    help="Skip itemized call/VAS extraction")
    ap.add_argument("--include-one-time", action="store_true",
                    help="Add one-time charges into per-number totals")
    args = ap.parse_args()

    pdf_path = os.path.abspath(args.pdf)
    if not os.path.isfile(pdf_path):
        print(f"Error: PDF not found: {pdf_path}")
        raise SystemExit(2)

    import json

    try:
        # Invoice-style JSON
        if args.out_json or args.print_json:
            pkg = extract(pdf_path, include_calls=not args.no_itemized)
            if args.out_json:
                with open(args.out_json, "w", encoding="utf-8") as f:
                    json.dump(pkg, f, ensure_ascii=False, indent=2)
                print("Invoice package JSON written:", os.path.abspath(args.out_json))
            if args.print_json and not args.out_json:
                print(json.dumps(pkg, ensure_ascii=False, indent=2))

        # Flat JSON for stored proc
        if args.flat_json or args.print_flat:
            flat = build_flat_json(
                pdf_path,
                include_calls=not args.no_itemized,
                include_one_time=args.include_one_time
            )
            if args.flat_json:
                with open(args.flat_json, "w", encoding="utf-8") as f:
                    json.dump(flat, f, ensure_ascii=False, indent=2)
                print("Flat Celcom JSON written:", os.path.abspath(args.flat_json))
            if args.print_flat and not args.flat_json:
                print(json.dumps(flat, ensure_ascii=False, indent=2))

    except Exception as e:
        print("Error:", e)
        raise SystemExit(1)

if __name__ == "__main__":
    _cli()
