"""
Digi bill extractor (PDF → standardized invoice package for DB upsert)

- Parsing logic preserved (verbatim behavior)
- Output envelope aligns with your existing Maxis/Celcom schema:
    {
      "invoice": {...},
      "numbers": [...],
      "charges": [...],
      "raw": {...}
    }
- Helper `build_db_payload(pdf_path)` returns the same package, ready to JSON.dump()
- Optional CLI: raw vs standardized

Dependencies:
  pip install pdfplumber
"""

from __future__ import annotations
import re
import json
import argparse
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

# ---------------------------------------------------------------------------
# -------------------------- Original helper logic --------------------------
# (VERBATIM semantics; no parsing logic changed)
# ---------------------------------------------------------------------------

def parse_amount(text: str) -> float:
    """Convert string amount (RM/KB) to float safely."""
    try:
        return float(
            text.replace(",", "")
                .replace("RM", "")
                .replace("kb", "")
                .strip()
        )
    except Exception:
        return 0.0


def _normalize_lines(text: str) -> str:
    """Keep newlines but collapse extra spaces per line."""
    return "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines())


# ---------- Service Summary helpers ----------

# Uppercase-ish company with SDN / BERHAD (+ optional BHD), tolerant to extra spaces
_SUBSCR_REGEX = re.compile(
    r"[A-Z][A-Z '&\.\-]+(?:SDN|BERHAD)(?:\s+BHD)?",
    re.I
)


def _pick_best_subscriber(text: str) -> str:
    """Pick the longest uppercase subscriber chunk from a window."""
    cands = _SUBSCR_REGEX.findall(text)
    if not cands:
        return ""
    best = max(cands, key=len)
    # normalize SDN / BHD / BERHAD casing & spacing
    best = re.sub(r"\bSdn\b", "SDN", best, flags=re.I)
    best = re.sub(r"\bBhd\b", "BHD", best, flags=re.I)
    best = re.sub(r"\bBerhad\b", "BERHAD", best, flags=re.I)
    best = re.sub(r"\bSDN\s*(BHD)?\b", lambda m: "SDN BHD" if m.group(1) else "SDN", best, flags=re.I)
    return re.sub(r"\s{2,}", " ", best).strip()


def _compose_description(window_text: str) -> str:
    """
    Build 'CelcomDigi Business Postpaid 5G 80' regardless of order,
    strip any subscriber fragments/BHD that bled into the same line.
    """
    cel = re.search(r"CelcomDigi\s+Business", window_text, re.I)
    post = re.search(r"Postpaid\s*\d+\s*G\s*\d+", window_text, re.I)

    if cel and post:
        desc = f"{cel.group(0)} {post.group(0)}"
    elif cel:
        desc = f"{cel.group(0)} Postpaid 5G 80"
    elif post:
        desc = f"CelcomDigi Business {post.group(0)}"
    else:
        desc = "CelcomDigi Business Postpaid 5G 80"

    # IMPORTANT: _SUBSCR_REGEX is compiled — use .sub()
    desc = _SUBSCR_REGEX.sub("", desc).strip()
    desc = re.sub(r"\bBHD\b", "", desc).strip()
    desc = re.sub(r"\s{2,}", " ", desc)
    return desc


def _parse_service_summary(block: str) -> dict:
    """
    Robust Service Summary parsing:
      • Row window = from one MSISDN (01xxxxxxxx) to the next or Subtotal.
      • Subscriber = longest ALL-CAPS company-like chunk with SDN/BERHAD (+ optional BHD).
      • Description = 'CelcomDigi Business Postpaid 5G 80' rebuilt cleanly.
      • Row total = last numeric amount in that window.
      • Also parse 'Service Tax 6% / 8%' and 'Current Bill Amount'.
    """
    lines = [ln.strip() for ln in block.split("\n") if ln.strip()]

    # mobile hits anywhere in line
    hits = []
    for i, ln in enumerate(lines):
        m = re.search(r"(01\d{7,8})", ln)
        if m:
            hits.append((i, m.group(1)))

    entries = []
    for idx, (start, msisdn) in enumerate(hits):
        end = hits[idx + 1][0] if idx + 1 < len(hits) else len(lines)
        for j in range(start + 1, end):
            if "Subtotal" in lines[j]:
                end = j
                break

        window = lines[start:end]
        window_text = " ".join(window)  # joins wrapped pieces: "... SDN" + "BHD" → "... SDN BHD"

        subscriber = _pick_best_subscriber(window_text)
        description = _compose_description(window_text)

        # remove subscriber if it still lingers inside description
        if subscriber:
            description = description.replace(subscriber, "")
            description = re.sub(r"\s{2,}", " ", description).strip()

        # pick last numeric total in this window
        amounts = []
        for s in window:
            if "Subtotal" in s:
                continue
            amounts += [m.group(1) for m in re.finditer(r"([\d,]+\.\d{2})", s)]
        total_val = parse_amount(amounts[-1]) if amounts else 0.0

        # UPDATED: expose both "Total" and "Current Bill Amount"
        entries.append({
            "Mobile No": msisdn,
            "Description": description,
            "Subscriber": subscriber,
            "Total": total_val,
            "Current Bill Amount": total_val
        })

    # subtotal
    subtotal = None
    m_sub = re.search(r"Subtotal\s+([\d,]+\.\d{2})", block, re.I)
    if m_sub:
        subtotal = parse_amount(m_sub.group(1))

    # Service Tax 6% / 8%
    service_tax = {}
    m_tax = re.search(r"Service\s*Tax\s*6%\s*/\s*8%(.*?)(?:Current\s*Bill\s*Amount|$)", block, re.S | re.I)
    if m_tax:
        tax_chunk = _normalize_lines(m_tax.group(1))
        for ln in tax_chunk.split("\n"):
            ln = ln.strip()
            if not ln:
                continue
            m1 = re.match(r"(.+?-\s*\d+\s*percent)\s+([\-\d,]+\.\d{2})$", ln, re.I)  # Others - 6 percent  -9.90
            m2 = re.match(r"(Total)\s+([\-\d,]+\.\d{2})$", ln, re.I)                 # Total  42.90
            if m1:
                service_tax[m1.group(1).strip()] = parse_amount(m1.group(2))
            elif m2:
                service_tax[m2.group(1).strip()] = parse_amount(m2.group(2))

    # current bill amount (overall, from the block footer)
    current_bill_amount = None
    m_cba = re.search(r"Current\s*Bill\s*Amount\s+([\d,]+\.\d{2})", block, re.I)
    if m_cba:
        current_bill_amount = parse_amount(m_cba.group(1))

    return {
        "lines": entries,
        "subtotal": subtotal,
        "service_tax": service_tax,
        "current_bill_amount": current_bill_amount
    }


# ------------------------- MAIN (original) PARSER -------------------------

def parse_digi_bill(pdf_path: str) -> dict:
    # Extract full text
    with pdfplumber.open(pdf_path) as pdf:
        all_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    all_text = _normalize_lines(all_text)

    # ---------- Header ----------
    account_no = re.search(r"Account\s*No\.?\s*[:\-]?\s*([0-9]+)", all_text, re.I)
    invoice_no = re.search(r"Invoice\s*No\.?\s*[:\-]?\s*([0-9]+)", all_text, re.I)
    invoice_date = re.search(r"Invoice\s*Date\.?\s*[:\-]?\s*(\d{1,2}\s\w+\s\d{4})", all_text, re.I)
    due_date = re.search(r"(?:Payment\s*Due\s*Date|Due\s*Date).*?(\d{1,2}\s\w+\s\d{4})", all_text, re.I)
    if not due_date:
        due_date = re.search(r"Current\s*Bill\s*:\s*[\d,]+\.\d{2}\s+(\d{1,2}\s\w+\s\d{4})", all_text, re.I)
    period = re.search(
        r"(?:Invoice\s*Period|Period)[:\s]+(\d{1,2}\s\w+\s\d{4})\s*(?:to|-)*\s*(\d{1,2}\s\w+\s\d{4})",
        all_text, re.I
    )
    no_of_lines = re.search(r"No\.?\s*of\s*Lines\.?\s*[:\-]?\s*(\d+)", all_text, re.I)

    total_outstanding = re.search(r"Total\s*Outstanding.*?([\d,]+\.\d{2})", all_text, re.I)
    current_bill = re.search(r"Current\s*Bill.*?([\d,]+\.\d{2})", all_text, re.I)

    header = {
        "Account No": account_no.group(1) if account_no else None,
        "Invoice No": invoice_no.group(1) if invoice_no else None,
        "Invoice Date": invoice_date.group(1) if invoice_date else None,
        "Invoice Period": f"{period.group(1)} - {period.group(2)}" if period else None,
        "No of Lines": no_of_lines.group(1) if no_of_lines else None,
        "Due Date": due_date.group(1) if due_date else None,
        "Total Outstanding": parse_amount(total_outstanding.group(1)) if total_outstanding else None
    }

    # ---------- Charges Summary ----------
    def extract_amt(pattern: str):
        m = re.search(pattern, all_text, re.I)
        return parse_amount(m.group(1)) if m else None

    charges_summary = {
        "Previous Bill(s)": extract_amt(r"Previous\s*Bill\(s\).*?([\d,]+\.\d{2})"),
        "Payments": -extract_amt(r"Payments.*?([\d,]+\.\d{2})") if extract_amt(r"Payments.*?([\d,]+\.\d{2})") else None,
        "Adjustments": extract_amt(r"Adjustments.*?([\d,]+\.\d{2})"),
        "Previous Overdue Amount": extract_amt(r"Previous\s*Overdue\s*Amount.*?([\d,]+\.\d{2})"),
        "Monthly Fixed Charges": extract_amt(r"Monthly\s*Fixed\s*Charges.*?([\d,]+\.\d{2})"),
        "Usage": extract_amt(r"\bUsage\b.*?([\d,]+\.\d{2})"),
        "Other Credits": extract_amt(r"Other\s*Credits?.*?([\d,]+\.\d{2})"),
        "Discounts": extract_amt(r"Discounts?.*?([\d,]+\.\d{2})"),
        "Service Tax": extract_amt(r"Service\s*Tax.*?([\d,]+\.\d{2})"),
        "Current Bill": parse_amount(current_bill.group(1)) if current_bill else None,
        "Total Outstanding": parse_amount(total_outstanding.group(1)) if total_outstanding else None
    }

    # ---------- Service Summary ----------
    ss_match = re.search(
        r"Service Summary(.*?)(?:Previous Payment Details|Payment Details|Page \d+/\d+|$)",
        all_text, re.S | re.I
    )
    service_summary = {"lines": [], "subtotal": None, "service_tax": {}, "current_bill_amount": None}
    if ss_match:
        service_summary = _parse_service_summary(ss_match.group(1))

    # Build lookup from summary → used to enrich per-line details
    summary_lookup = {
        row["Mobile No"]: {
            "Description": row.get("Description", ""),
            "Subscriber": row.get("Subscriber", "")
        }
        for row in service_summary.get("lines", [])
        if row.get("Mobile No")
    }

    # ---------- Per-line details ----------
    service_details: Dict[str, Any] = {}
    per_line_blocks = re.split(r"Mobile No\.?\s*0", all_text)[1:]
    for blk in per_line_blocks:
        blk = "0" + blk
        lines = [ln.strip() for ln in blk.split("\n") if ln.strip()]
        if not lines:
            continue
        m_msisdn = re.match(r"0\d+", lines[0])
        mobile_no = m_msisdn.group(0) if m_msisdn else None
        if not mobile_no:
            continue

        # seed with description/subscriber from service summary
        desc = summary_lookup.get(mobile_no, {}).get("Description", "")
        subs = summary_lookup.get(mobile_no, {}).get("Subscriber", "")

        service_details.setdefault(
            mobile_no,
            {"Mobile No": mobile_no, "Description": desc, "Subscriber": subs, "Itemised Bill": [], "Detail of Charges": []}
        )

        for ln in lines:
            if re.search(r"(Postpaid|Secure|Rebate|Discount|OCC|Other Credit)", ln, re.I):
                m_amt = re.search(r"([\-\d,]+\.\d{2})", ln)
                if m_amt:
                    service_details[mobile_no]["Itemised Bill"].append({
                        "description": ln.strip(),
                        "amount": parse_amount(m_amt.group(1))
                    })
            elif re.search(r"(digisecure|diginet)", ln, re.I):
                parts = ln.split()
                if len(parts) >= 2:
                    service_details[mobile_no]["Detail of Charges"].append({
                        "category": "Internet/Data",
                        "access_point": parts[0],
                        "volume_kb": int(re.sub(r"[^0-9]", "", parts[1])) if re.search(r"\d", parts[1]) else 0,
                        "amount": parse_amount(parts[-1])
                    })

        # backfill description/subscriber if still empty using the block text
        if not service_details[mobile_no]["Description"] or not service_details[mobile_no]["Subscriber"]:
            win_txt = " ".join(lines)
            if not service_details[mobile_no]["Description"]:
                service_details[mobile_no]["Description"] = _compose_description(win_txt)
            if not service_details[mobile_no]["Subscriber"]:
                service_details[mobile_no]["Subscriber"] = _pick_best_subscriber(win_txt)

    # ---------- Payments ----------
    payment_history = [{"Date": d, "Amount": parse_amount(a)}
                       for d, a in re.findall(r"(\d{1,2}\s\w+\s\d{4})\s+([\d,]+\.\d{2})", all_text)]

    return {
        "header": header,
        "charges_summary": charges_summary,
        "service_summary": service_summary,
        "service_details": list(service_details.values()),
        "payment_history": payment_history
    }


# ---------------------------------------------------------------------------
# ----------------------------- Adapter layer -------------------------------
# (Aligns with your DB/Maxis/Celcom envelope; parsing unchanged)
# ---------------------------------------------------------------------------

_MONTHS = {
    'jan': '01','january': '01','feb': '02','february': '02','mar': '03','march': '03','apr': '04','april': '04',
    'may': '05','jun': '06','june': '06','jul': '07','july': '07','aug': '08','august': '08','sep': '09',
    'sept': '09','september': '09','oct': '10','october': '10','nov': '11','november': '11','dec': '12','december': '12'
}

def _to_iso(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", d.strip())
    if not m:
        return None
    day, mon, year = m.groups()
    mon_num = _MONTHS.get(mon.lower())
    if not mon_num:
        return None
    return f"{year}-{mon_num}-{int(day):02d}"

def _split_period(period: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not period:
        return None, None
    m = re.match(r"(\d{1,2}\s[A-Za-z]+\s\d{4})\s*[-to]+\s*(\d{1,2}\s[A-Za-z]+\s\d{4})", period, re.I)
    if not m:
        return None, None
    return _to_iso(m.group(1)), _to_iso(m.group(2))

def parse_raw(pdf_path: str) -> Dict[str, Any]:
    """Return the original Digi parse (unchanged logic)."""
    return parse_digi_bill(pdf_path)

def extract(pdf_path: str) -> Dict[str, Any]:
    """
    Return standardized invoice package aligned to your DB schema:
      - invoice: single-object header (ISO dates)
      - numbers: per-line list with itemized details
      - charges: flat list of {category,label,amount}
      - raw: full original parse for downstream tables
    """
    raw = parse_raw(pdf_path)
    hdr = raw.get("header", {})
    chs = raw.get("charges_summary", {})

    period_start, period_end = _split_period(hdr.get("Invoice Period"))

    invoice = {
        "vendor": "digi",
        "invoice_number": hdr.get("Invoice No"),
        "account_number": hdr.get("Account No"),
        "bill_date": _to_iso(hdr.get("Invoice Date")),
        "period_start": period_start,
        "period_end": period_end,
        "currency": "MYR",
        # keep numeric fields as-is (DB JSON handles nulls)
        "subtotal": chs.get("Current Bill"),
        "tax_total": chs.get("Service Tax"),
        "grand_total": chs.get("Total Outstanding") or chs.get("Current Bill"),
    }

    def row(cat: str, label: str, amount: Optional[float]) -> Optional[Dict[str, Any]]:
        if amount is None:
            return None
        return {"category": cat, "label": label, "amount": amount}

    charges: List[Dict[str, Any]] = []
    for r in filter(None, [
        row("Previous", "Previous Balance", chs.get("Previous Bill(s)")),
        row("Payments", "Payment Received", chs.get("Payments")),
        row("Adjustments", "Adjustment", chs.get("Adjustments")),
        row("Other", "Overdue Amount", chs.get("Previous Overdue Amount")),
        row("Monthly", "Monthly Fixed Charges", chs.get("Monthly Fixed Charges")),
        row("Usage", "Usage", chs.get("Usage")),
        # 🔽 ONLY LINE CHANGED: category "Other Credits" → "Other"
        row("Other", "Other Credits", chs.get("Other Credits")),
        row("Discounts", "Discounts", chs.get("Discounts")),
        row("Tax", "Service Tax", chs.get("Service Tax")),
        row("Other", "Current Charges (card)", chs.get("Current Bill")),
    ]):
        charges.append(r)

    numbers: List[Dict[str, Any]] = []
    for line in raw.get("service_details", []):
        msisdn = line.get("Mobile No")
        if not msisdn:
            continue
        item_total = sum((it.get("amount") or 0.0) for it in line.get("Itemised Bill", []))
        numbers.append({
            "msisdn": msisdn,
            "description": line.get("Description"),
            "subscriber": line.get("Subscriber"),
            "monthly_items": line.get("Itemised Bill") or [],
            "detail_of_charges": line.get("Detail of Charges") or [],
            "line_total": round(float(item_total), 2) if item_total else None,
        })

    package = {
        "invoice": invoice,
        "numbers": numbers,
        "charges": charges,
        "raw": raw,  # keep full parse for downstream tables
    }
    return package


# ---------------------- DB Convenience / CLI helpers ----------------------

def build_db_payload(pdf_path: str) -> Dict[str, Any]:
    """
    Convenience wrapper: same structure your DB upsert expects.
    Example usage with pyodbc (pseudo):
        payload = json.dumps(build_db_payload(path), ensure_ascii=False)
        cursor.execute("EXEC dbo.sp_Upsert_InvoicePackage_JSON ?", payload)
    """
    return extract(pdf_path)

def _cli():
    ap = argparse.ArgumentParser(description="Digi (CelcomDigi) PDF → standardized invoice package")
    ap.add_argument("pdf", help="Path to the Digi PDF")
    ap.add_argument("--raw", action="store_true", help="Print original raw parse instead of standardized package")
    args = ap.parse_args()

    pkg = parse_raw(args.pdf) if args.raw else extract(args.pdf)
    print(json.dumps(pkg, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    _cli()
