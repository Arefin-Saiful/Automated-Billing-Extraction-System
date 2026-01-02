# app/services/maxis_extractor.py
import re, json, hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Any
from decimal import Decimal, InvalidOperation

import pdfplumber, pandas as pd

# ---------------- Utilities

def _to_num(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().replace(",", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return None

def _dur_s(s):
    s = str(s)
    m = re.search(r"(\d{2}):(\d{2}):(\d{2})", s)
    if not m:
        return None
    h, m2, s2 = map(int, m.groups())
    return h * 3600 + m2 * 60 + s2

def _clean(df_like):
    df = pd.DataFrame(df_like).fillna("")
    if df.empty:
        return df
    df = df.loc[(df != "").any(axis=1)]
    if df.empty:
        return df
    df = df.loc[:, (df != "").any(axis=0)]
    if df.empty:
        return df
    df = df.loc[:, ~df.T.duplicated()]
    return df.reset_index(drop=True)

def _uniq_cols(row):
    seen, out = {}, []
    for val in [str(v).strip() for v in row.tolist()]:
        key = val or "col"
        seen[key] = seen.get(key, 0) + 1
        out.append(key if seen[key] == 1 else f"{key}_{seen[key]}")
    return out

def _header_and_body(df, prefer_calls=False):
    if df.empty:
        return df
    for i in range(min(3, len(df))):
        row_text = " ".join(df.iloc[i].astype(str)).lower()
        if prefer_calls and ("date" in row_text or "tarikh" in row_text) and ("time" in row_text or "masa" in row_text):
            body = df.iloc[i + 1 :].reset_index(drop=True)
            body.columns = _uniq_cols(df.iloc[i])
            return body
        if (not prefer_calls) and ("item" in row_text or "barang" in row_text):
            body = df.iloc[i + 1 :].reset_index(drop=True)
            body.columns = _uniq_cols(df.iloc[i])
            return body
    body = df.iloc[1:].reset_index(drop=True)
    body.columns = _uniq_cols(df.iloc[0])
    return body

def _norm_calls(df):
    if df.empty:
        return df
    low = {c.lower(): c for c in df.columns}
    def pick(*alts):
        for a in alts:
            for k, v in low.items():
                if a in k:
                    return v
        return None
    m = {
        "Date": pick("date", "tarikh"),
        "Time": pick("time", "masa"),
        "From": pick("from", "dari"),
        "To": pick("to", " ke"),
        "Number Called": pick("number called", "no. panggilan"),
        "Duration": pick("duration", "tempoh"),
        "Period": pick("period", "kadar"),
        "Gross Amount": pick("gross", "kasar"),
        "Total (RM)": pick("total", "jumlah"),
    }
    take = {k: df[v] for k, v in m.items() if v}
    if not take:
        return df
    out = pd.DataFrame(take)
    mask_sub = out.apply(
        lambda r: "subtotal" in (" ".join(map(str, r.values)).lower())
                  or "jumlah kecil" in (" ".join(map(str, r.values)).lower()),
        axis=1
    )
    out = out.loc[~mask_sub].copy()
    if "Duration" in out:
        out["Duration_s"] = out["Duration"].map(_dur_s)
    if "Total (RM)" in out:
        out["Total (RM)"] = out["Total (RM)"].map(_to_num)
    if "Gross Amount" in out:
        out["Gross Amount"] = out["Gross Amount"].map(_to_num)
    return out.reset_index(drop=True)

def _norm_charges(df):
    if df.empty:
        return df
    low = {c.lower(): c for c in df.columns}
    def pick(*alts):
        for a in alts:
            for k, v in low.items():
                if a in k:
                    return v
        return None
    m = {
        "Item/Barang": pick("item", "barang"),
        "Date/Period": pick("date", "period", "tarikh", "tempoh"),
        "Amount (RM)": pick("amount", "amaun"),
        "Total (RM)": pick("total", "jumlah"),
    }
    take = {k: df[v] for k, v in m.items() if v}
    if not take:
        return df
    out = pd.DataFrame(take)
    for c in ("Amount (RM)", "Total (RM)"):
        if c in out:
            out[c] = out[c].map(_to_num)
    return out.reset_index(drop=True)

def _is_calls_table(df: pd.DataFrame) -> bool:
    if df.empty or df.shape[1] < 3:
        return False
    header = " ".join(df.iloc[0].astype(str)).lower()
    if any(k in header for k in ["date", "time", "duration", "number", "tarikh", "masa", "tempoh"]):
        return True
    for _, row in df.iterrows():
        cells = [str(x) for x in row.tolist()]
        if any(re.search(r"\b\d{2}/\d{2}/\d{4}\b", c) for c in cells) and \
           any(re.search(r"\b\d{2}:\d{2}:\d{2}\b", c) for c in cells):
            return True
    return False

def _find_subtotal_in_table(df: pd.DataFrame) -> Optional[float]:
    try:
        for _, row in df.iterrows():
            line = " ".join(map(str, row.values)).lower()
            if "subtotal" in line or "jumlah kecil" in line:
                nums = re.findall(r"[0-9,]+\.\d{2}", " ".join(map(str, row.values)))
                if nums:
                    return _to_num(nums[-1])
    except Exception:
        pass
    return None

# --------------- text fallbacks

DATE_RX = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
TIME_RX = re.compile(r"\b\d{2}:\d{2}:\d{2}\b")
AMOUNT_AT_END = re.compile(r"([0-9,]*\.\d{2})\s*$")
AMOUNT_ANY = re.compile(r"\(?[0-9,]+\.\d{2}\)?")  # allows parentheses

def _fallback_charges_from_text(page_text: str) -> Optional[pd.DataFrame]:
    if not page_text:
        return None
    rows = []
    pat_item  = re.compile(r'^\s*Y\s+.+?([0-9,]*\.\d{2})\s*$', re.I)
    pat_total = re.compile(r'^\s*Total Line Charges.*?([0-9,]*\.\d{2})\s*$', re.I)
    for ln in page_text.splitlines():
        L = ln.strip()
        if not L:
            continue
        m_item = pat_item.search(L)
        if m_item:
            amt = _to_num(m_item.group(1))
            desc = L[:m_item.start(1)].strip()
            rows.append({"Item/Barang": desc, "Date/Period": "", "Amount (RM)": amt, "Total (RM)": amt})
            continue
        m_total = pat_total.search(L)
        if m_total:
            amt = _to_num(m_total.group(1))
            rows.append({"Item/Barang": "Total Line Charges (excluding Svc. Tax)", "Date/Period": "", "Amount (RM)": amt, "Total (RM)": amt})
            break
    if not rows:
        return None
    return pd.DataFrame(rows)

def _fallback_calls_from_text(page_text: str) -> Optional[pd.DataFrame]:
    if not page_text:
        return None
    recs = []
    for ln in page_text.splitlines():
        if not (DATE_RX.search(ln) and TIME_RX.search(ln)):
            continue
        times = [m.group(0) for m in TIME_RX.finditer(ln)]
        if not times:
            continue
        time_val = times[0]
        duration_val = times[-1]
        date_val = DATE_RX.search(ln).group(0)
        number_val = None
        for m in re.finditer(r'\b(?:0\d{8,11}|6\d{7,12})\b', ln):
            number_val = m.group(0)
        amt_m = AMOUNT_AT_END.search(ln)
        total_val = _to_num(amt_m.group(1)) if amt_m else None
        period_m = re.search(r'\b([A-Z])\b\s+[0-9,]*\.\d{2}\s*$', ln)
        period_val = period_m.group(1) if period_m else None
        if date_val and time_val and duration_val and number_val and total_val is not None:
            recs.append({
                "Date": date_val,
                "Time": time_val,
                "Number Called": number_val,
                "Duration": duration_val,
                "Period": period_val,
                "Gross Amount": 0.0 if total_val == 0 else None,
                "Total (RM)": total_val,
            })
    if not recs:
        return None
    df = pd.DataFrame(recs)
    if "Duration" in df:
        df["Duration_s"] = df["Duration"].map(_dur_s)
    return df

# --------------- PDF regex & helpers

RE_MSISDN_SPACED  = re.compile(r"60(?:\s*\d){8,9}")
RE_PLAN           = re.compile(r"Business\s+Postpaid\s+\d+(?:\s+[A-Za-z].*)?", re.I)

RE_STATEMENT_DATE = re.compile(r"(?:Statement\s*Date|Tarikh\s*Penyata)\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})", re.I)
RE_BILL_PERIOD    = re.compile(r"(?:Billing\s*Period|Tempoh\s*Bil).*?(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})", re.I)

RE_AMOUNT_TRAILING = re.compile(r"([0-9,]+\.\d{2})\s*$")
RE_PAYLAST   = re.compile(r"Payment\s*Last\s*Date.*?(\d{2}/\d{2}/\d{4})", re.I)

RE_CURR_MOBILE = re.compile(r"^\s*MOBILE\s+([0-9,]+\.\d{2})\s*$", re.I)
RE_CURR_LINE   = re.compile(r"^\s*(60(?:\s*\d){8,9})\s*[-–]\s*(Business\s+Postpaid\s+\d+(?:\s+[A-Za-z].*)?)\s+([0-9,]+\.\d{2})\s*$", re.I)
RE_TOT_EXCL    = re.compile(r"Total\s+Charges\s*\(excluding\s*Svc\.\s*Tax\)\s*([0-9,]+\.\d{2})", re.I)
RE_SVCTAX      = re.compile(r"Service\s*Tax\s*\((\d+(?:\.\d+)?)%\s*.*?\)\s*([0-9,]+\.\d{2})", re.I)
RE_TOT_CURR    = re.compile(r"TOTAL\s+CURRENT\s+CHARGES.*?([0-9,]+\.\d{2})", re.I)

def _first_date_near(lines, i, window=4):
    for j in range(i, min(i + window, len(lines))):
        m = DATE_RX.search(lines[j])
        if m:
            return m.group(0)
    return None

def _two_dates_near(lines, i, window=4):
    block = " ".join(lines[i:min(i + window, len(lines))])
    ds = DATE_RX.findall(block)
    if len(ds) >= 2:
        return ds[0], ds[1]
    return None, None

def _amount_near(lines, i, window=4):
    block = " ".join(lines[i:min(i + window, len(lines))])
    nums = AMOUNT_ANY.findall(block)
    if nums:
        return _to_num(nums[-1])
    return None

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

# ---------- Payment row normalization & merging ----------

def _normalize_payments_table(body: pd.DataFrame) -> List[Dict[str, Any]]:
    if body is None or body.empty:
        return []

    low = {c.lower(): c for c in body.columns}
    def pick(*alts):
        for a in alts:
            for k, v in low.items():
                if a in k:
                    return v
        return None

    c_desc = pick("description", "penerangan")
    c_sid  = pick("service identifier", "pengecam")
    c_date = pick("date", "tarikh")
    c_amt  = pick("amount", "amaun")
    c_svc  = pick("svc", "cukai")
    c_tot  = pick("total", "jumlah")

    for c in [c_desc, c_sid, c_date, c_amt, c_svc, c_tot]:
        if c and c in body.columns:
            body[c] = body[c].astype(str).str.strip()

    out, carry_desc, carry_sid = [], None, None

    for _, r in body.iterrows():
        desc = (str(r.get(c_desc, "")) if c_desc else "").strip()
        if re.search(r"payment\s*&\s*adjustment", desc, re.I):
            continue

        sid  = (str(r.get(c_sid, "")) if c_sid else "").strip()
        date = (str(r.get(c_date, "")) if c_date else "").strip()
        amt  = _to_num(r.get(c_amt)) if c_amt else None
        svc  = _to_num(r.get(c_svc)) if c_svc else None
        tot  = _to_num(r.get(c_tot)) if c_tot else None

        has_value = bool(date) or amt is not None or svc is not None or tot is not None

        if (desc and not has_value) and (not c_amt or r.get(c_amt) in ("", None)) and (not c_tot or r.get(c_tot) in ("", None)):
            carry_desc = f"{carry_desc} - {desc}" if carry_desc else desc
            carry_sid  = sid or carry_sid
            continue

        full_desc = " ".join([p for p in [carry_desc, desc] if p]).strip() if (carry_desc or desc) else None
        full_sid  = sid or carry_sid

        if has_value:
            out.append({
                "description": full_desc or desc or None,
                "service_identifier": full_sid,
                "date": date or None,
                "amount": amt,
                "svc_tax": svc,
                "total": tot,
            })
            carry_desc, carry_sid = None, None

    merged = []
    skip = False
    for i, r in enumerate(out):
        if skip:
            skip = False
            continue
        desc = (r.get("description") or "").strip()
        if re.fullmatch(r"payment", desc, re.I) and i + 1 < len(out):
            nxt = out[i + 1]
            nxt_desc = (nxt.get("description") or "").strip()
            if nxt_desc and (nxt.get("date") or nxt.get("amount") is not None):
                merged.append({
                    "description": f"PAYMENT - {nxt_desc}",
                    "service_identifier": nxt.get("service_identifier") or r.get("service_identifier"),
                    "date": nxt.get("date"),
                    "amount": nxt.get("amount"),
                    "svc_tax": nxt.get("svc_tax") if nxt.get("svc_tax") is not None else r.get("svc_tax"),
                    "total": nxt.get("total") if nxt.get("total") is not None else r.get("total"),
                })
                skip = True
                continue
        if not re.search(r"payment\s*&\s*adjustment", desc, re.I):
            merged.append(r)

    cleaned = [
        rr for rr in merged
        if rr.get("date") or rr.get("amount") is not None or rr.get("total") is not None or rr.get("svc_tax") is not None
    ]
    return cleaned

# --------------- models

@dataclass
class NumberSection:
    service_no: str
    plan: Optional[str] = None
    pages: List[int] = field(default_factory=list)
    account_name: Optional[str] = None
    share_product_service_no: Optional[str] = None
    charges: List[pd.DataFrame] = field(default_factory=list)
    calls: List[pd.DataFrame] = field(default_factory=list)
    calls_subtotal_rm: Optional[float] = None

# --------------- main extractor

class MaxisExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.sections: Dict[str, NumberSection] = {}
        self.bill_statement: Dict[str, Any] = {}
        self.payment_adjustments: List[Dict[str, Any]] = []

    def _tables_from_page(self, page) -> List[pd.DataFrame]:
        tbs = []
        settings = [
            None,
            {"vertical_strategy":"lines","horizontal_strategy":"lines","snap_tolerance":3,"join_tolerance":3,"edge_min_length":30,"min_words_vertical":1,"min_words_horizontal":1,"text_tolerance":3,},
            {"vertical_strategy":"text","horizontal_strategy":"text","snap_tolerance":3,"join_tolerance":3,"edge_min_length":30,"min_words_vertical":1,"min_words_horizontal":1,"text_tolerance":3,},
        ]
        for how in settings:
            try:
                if how is None:
                    raw = page.extract_tables() or []
                else:
                    try:
                        raw = page.extract_tables(table_settings=how) or []
                    except TypeError:
                        raw = page.extract_tables(how) or []
                for t in raw:
                    df = _clean(t)
                    if not df.empty:
                        tbs.append(df)
            except Exception:
                continue
        return tbs

    # ---- A) Bill Statement (Page 1 + 2)
    def _parse_bill_statement(self, pdf) -> None:
        p1_text = pdf.pages[0].extract_text() or ""
        lines = [l.strip() for l in p1_text.splitlines() if l.strip()]

        account_number = None
        bill_reference = None
        statement_date = None
        billing_from = None
        billing_to = None
        overdue = previous = payrecv = adjust = None
        payment_last_date = None

        for i, l in enumerate(lines):
            ll = l.lower()

            if statement_date is None and ("statement date" in ll or "tarikh penyata" in ll):
                m = RE_STATEMENT_DATE.search(l)
                statement_date = m.group(1) if m else _first_date_near(lines, i, window=4)

            if (billing_from is None or billing_to is None) and ("billing period" in ll or "tempoh bil" in ll):
                mm = RE_BILL_PERIOD.search(l)
                if mm: billing_from, billing_to = mm.groups()
                else:  billing_from, billing_to = _two_dates_near(lines, i, window=4)

            if account_number is None and ("account no" in ll or "no. akaun" in ll):
                span = " ".join(lines[i:i+4])
                nums = re.findall(r"\b\d{8,12}\b", span)
                if nums: account_number = nums[-1]

            if bill_reference is None and ("bill reference" in ll or "no. rujukan" in ll):
                span = " ".join(lines[i:i+4])
                nums = re.findall(r"\b\d{6,15}\b", span)
                if nums: bill_reference = nums[-1]

            if overdue is None and ("overdue amount" in ll or "caj tertunggak" in ll):
                m = RE_AMOUNT_TRAILING.search(l)
                overdue = _to_num(m.group(1)) if m else _amount_near(lines, i, window=4)

            if previous is None and ("previous balance" in ll or "baki terdahulu" in ll):
                m = RE_AMOUNT_TRAILING.search(l)
                previous = _to_num(m.group(1)) if m else _amount_near(lines, i, window=4)

            if payrecv is None and ("payment received" in ll or "bayaran diterima" in ll):
                m = RE_AMOUNT_TRAILING.search(l)
                payrecv = _to_num(m.group(1)) if m else _amount_near(lines, i, window=4)

            if adjust is None and ("adjustment" in ll or "pelarasan" in ll):
                m = RE_AMOUNT_TRAILING.search(l)
                adjust = _to_num(m.group(1)) if m else _amount_near(lines, i, window=4)

            if payment_last_date is None and ("payment last date" in ll or "tarikh akhir bayaran" in ll):
                m = RE_PAYLAST.search(l)
                payment_last_date = m.group(1) if m else _first_date_near(lines, i, window=4)

        # Current charges (p2)
        mobile_total = None; lines_cc = []; tot_excl = None; svc_tax = None; svc_rate = None; tot_curr = None
        p2_text = pdf.pages[1].extract_text() or ""
        for l in p2_text.splitlines():
            m0 = RE_CURR_MOBILE.search(l)
            if m0:
                mobile_total = _to_num(m0.group(1)); continue
            m1 = RE_CURR_LINE.search(l)
            if m1:
                msisdn = re.sub(r"\s+","", m1.group(1))
                plan = m1.group(2).strip()
                amt  = _to_num(m1.group(3))
                lines_cc.append({"service_no": msisdn, "plan": plan, "amount": amt})
                continue
            m2 = RE_TOT_EXCL.search(l);   tot_excl = _to_num(m2.group(1)) if m2 else tot_excl
            m3 = RE_SVCTAX.search(l)
            if m3:
                svc_rate = _to_num(m3.group(1))
                svc_tax  = _to_num(m3.group(2))
            m4 = RE_TOT_CURR.search(l);   tot_curr = _to_num(m4.group(1)) if m4 else tot_curr

        self.bill_statement = {
            "account_number": account_number,
            "bill_reference": bill_reference,
            "statement_date": statement_date,
            "billing_period": {"from": billing_from, "to": billing_to} if billing_from and billing_to else None,
            "overdue_amount": overdue,
            "previous_balance": previous,
            "payment_received": payrecv,
            "adjustment": adjust,
            "payment_last_date": payment_last_date,
            "current_charges": {
                "mobile_total": mobile_total,
                "lines": lines_cc,
                "total_charges_excl_tax": tot_excl,
                "service_tax": svc_tax,
                "service_tax_rate": svc_rate,
                "total_current_charges": tot_curr,
            }
        }

        # Payments & Adjustments
        self.payment_adjustments = []
        tables = []
        try:
            tables += self._tables_from_page(pdf.pages[1])
        except Exception:
            pass

        matched = False
        for df in tables:
            if df.empty or df.shape[1] < 3:
                continue
            header = " ".join(df.iloc[0].astype(str)).lower()
            if ("description" in header or "penerangan" in header) and ("date" in header or "tarikh" in header):
                body = _header_and_body(df, prefer_calls=False)
                rows = _normalize_payments_table(body)
                if rows:
                    self.payment_adjustments.extend(rows)
                    matched = True
                    break

        if not matched:
            for l in p2_text.splitlines():
                if not (DATE_RX.search(l) or AMOUNT_ANY.search(l)):
                    continue
                if not re.search(r'JomPay|PAYMENT|GIRO|FPX|BANK|CREDIT', l, re.I):
                    continue
                if re.search(r"payment\s*&\s*adjustment", l, re.I):
                    continue
                date = DATE_RX.search(l)
                nums = AMOUNT_ANY.findall(l)
                amt = tot = None
                if nums:
                    tot = _to_num(nums[-1])
                    if len(nums) > 1:
                        amt = _to_num(nums[-2])
                desc = re.sub(r'\s*\(?[0-9,]+\.\d{2}\)?\s*$', '', l).strip()
                self.payment_adjustments.append({
                    "description": desc,
                    "service_identifier": None,
                    "date": date.group(0) if date else None,
                    "amount": amt,
                    "svc_tax": None,
                    "total": tot,
                })

        if self.bill_statement.get("payment_received") is None:
            totals = [p.get("total") for p in self.payment_adjustments if p.get("total") is not None]
            if totals:
                self.bill_statement["payment_received"] = float(sum(totals))

    # ---- B) Discover per-number sections + parse their pages
    def _discover_sections(self, pdf) -> None:
        cur: Optional[str] = None
        for i, page in enumerate(pdf.pages, 1):
            txt = page.extract_text() or ""
            if not txt:
                continue
            m_num  = RE_MSISDN_SPACED.search(txt)
            m_plan = RE_PLAN.search(txt)
            if m_num and m_plan:
                msisdn = re.sub(r"\s+","", m_num.group(0))
                plan   = m_plan.group(0).strip()
                if msisdn not in self.sections:
                    self.sections[msisdn] = NumberSection(service_no=msisdn, plan=plan, pages=[i])
                else:
                    if i not in self.sections[msisdn].pages:
                        self.sections[msisdn].pages.append(i)
                    if not self.sections[msisdn].plan:
                        self.sections[msisdn].plan = plan
                an = re.search(r"Account Name\s*/\s*Nama Akaun\s*:\s*(.+)", txt)
                sp = re.search(r"Share Product Service No\.\s*:\s*([0-9 ]+)", txt)
                if an: self.sections[msisdn].account_name = an.group(1).strip()
                if sp: self.sections[msisdn].share_product_service_no = re.sub(r"\s+","", sp.group(1))
                cur = msisdn
            elif cur:
                if i not in self.sections[cur].pages:
                    self.sections[cur].pages.append(i)

    def _parse_number_pages(self, pdf) -> None:
        for msisdn, section in self.sections.items():
            if not section.pages:
                continue
            charges_parts, calls_parts = [], []
            for pno in section.pages:
                page = pdf.pages[pno-1]
                txt  = page.extract_text() or ""
                tables = self._tables_from_page(page)

                got_any = False
                for df in tables:
                    if df.empty or df.shape[1] < 3:
                        continue

                    sub = _find_subtotal_in_table(df)
                    if sub is not None:
                        section.calls_subtotal_rm = sub

                    if _is_calls_table(df):
                        body = _header_and_body(df, prefer_calls=True)
                        norm = _norm_calls(body)
                        if not norm.empty:
                            calls_parts.append(norm); got_any = True
                        continue

                    top = " ".join(df.iloc[0].astype(str)).lower()
                    if "item" in top or "barang" in top:
                        body = _header_and_body(df, prefer_calls=False)
                        norm = _norm_charges(body)
                        if not norm.empty:
                            charges_parts.append(norm); got_any = True

                if not got_any:
                    ch_f = _fallback_charges_from_text(txt)
                    if ch_f is not None and not ch_f.empty:
                        charges_parts.append(ch_f); got_any = True
                    ca_f = _fallback_calls_from_text(txt)
                    if ca_f is not None and not ca_f.empty:
                        calls_parts.append(ca_f); got_any = True

            if charges_parts:
                section.charges = [pd.concat(charges_parts, ignore_index=True)]
            if calls_parts:
                section.calls = [pd.concat(calls_parts, ignore_index=True)]

    # ---- Public API (raw)
    def extract(self) -> Dict[str, Any]:
        with pdfplumber.open(self.pdf_path) as pdf:
            self._parse_bill_statement(pdf)
            self._discover_sections(pdf)
            self._parse_number_pages(pdf)

        lines_out = []
        for msisdn, sec in self.sections.items():
            chg_df = pd.concat(sec.charges, ignore_index=True) if sec.charges else pd.DataFrame()
            cal_df = pd.concat(sec.calls,    ignore_index=True) if sec.calls    else pd.DataFrame()
            lines_out.append({
                "service_no": msisdn,
                "plan": sec.plan,
                "account_name": sec.account_name,
                "share_product_service_no": sec.share_product_service_no,
                "charges": chg_df.to_dict("records") if not chg_df.empty else [],
                "calls":   cal_df.to_dict("records") if not cal_df.empty else [],
                "calls_subtotal_rm": sec.calls_subtotal_rm,
            })

        return {
            "bill_statement": self.bill_statement,
            "payment_adjustments": self.payment_adjustments,
            "lines": lines_out,
        }

    def export(self, out_dir: str) -> Dict[str, str]:
        out, out_path = {}, Path(out_dir); out_path.mkdir(parents=True, exist_ok=True)
        all_calls, all_charges = [], []
        for msisdn, sec in self.sections.items():
            xlsx = out_path / f"{msisdn}.xlsx"
            # ^ keep original export flow
            with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
                (pd.concat(sec.charges, ignore_index=True) if sec.charges else
                 pd.DataFrame({"info":["No charges rows parsed."]})).to_excel(w, "Charges", index=False)
                (pd.concat(sec.calls, ignore_index=True) if sec.calls else
                 pd.DataFrame({"info":["No calls rows parsed."]})).to_excel(w, "Calls", index=False)
            out[msisdn] = str(xlsx)
            if sec.calls:
                for d in sec.calls:
                    dd = d.copy(); dd["Service No"] = msisdn; dd["Plan"] = sec.plan; all_calls.append(dd)
            if sec.charges:
                for d in sec.charges:
                    dd = d.copy(); dd["Service No"] = msisdn; dd["Plan"] = sec.plan; all_charges.append(dd)

        if all_calls:
            pd.concat(all_calls, ignore_index=True).to_csv(out_path / "All_Calls.csv", index=False)
            out["All_Calls.csv"] = str(out_path / "All_Calls.csv")
        if all_charges:
            pd.concat(all_charges, ignore_index=True).to_csv(out_path / "All_Charges.csv", index=False)
            out["All_Charges.csv"] = str(out_path / "All_Charges.csv")

        (out_path / "manifest.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        return out


# -----------------------
# Adapter to DB JSON shape
# -----------------------

PARSER_VERSION = "maxis-raw-bridge-1.1.0"

def _normalize_plan(plan: Optional[str]) -> Optional[str]:
    if not plan:
        return plan
    s = re.sub(r"\s{2,}", " ", plan).strip()
    if re.search(r"Business\s+Postpaid\s*79", s, re.I):
        return "Business Postpaid 79"
    return s

# ---- NEW: ISO date normalizer (DB-alignment) ----
def _to_iso(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})$", d.strip())
    if not m:
        return d
    dd, mm, yyyy = m.groups()
    return f"{yyyy}-{mm}-{dd}"

def _guess_numbers(lines: List[Dict[str, Any]], current_lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map per-line totals using 'current_charges.lines' amounts where possible."""
    by_msisdn_amt = {row["service_no"]: row.get("amount") for row in (current_lines or [])}
    out = []
    for ln in lines:
        msisdn = ln.get("service_no")
        total = by_msisdn_amt.get(msisdn)
        if total is None:
            ch = ln.get("charges") or []
            try:
                total = float(sum(_to_num(r.get("Total (RM)")) or 0 for r in ch))
            except Exception:
                total = None
        out.append({
            "msisdn": msisdn,
            "plan_name": _normalize_plan(ln.get("plan")),
            "subscriber": ln.get("account_name"),
            "total_amount": total
        })
    return out

def _build_charges(bs: Dict[str, Any]) -> List[Dict[str, Any]]:
    c = []
    def add(cat, label, amount):
        if amount is None:
            return
        c.append({"category": cat, "label": label, "amount": float(amount)})

    add("Previous",  "Previous Balance", bs.get("previous_balance"))
    add("Other",     "Overdue Amount", bs.get("overdue_amount"))
    if bs.get("payment_received") is not None:
        add("Payments", "Payment Received", -abs(float(bs["payment_received"])))
    add("Adjustments","Adjustment", bs.get("adjustment"))

    cur = (bs.get("current_charges") or {})
    add("Monthly",   "Total Charges (excluding Svc. Tax)", cur.get("total_charges_excl_tax"))
    if cur.get("service_tax") is not None:
        add("Tax",   f"Service Tax ({cur.get('service_tax_rate') or ''}%)", cur.get("service_tax"))
    add("Current",   "TOTAL CURRENT CHARGES", cur.get("total_current_charges"))
    return c

def _build_taxes(bs: Dict[str, Any]) -> List[Dict[str, Any]]:
    cur = (bs.get("current_charges") or {})
    amt = cur.get("service_tax")
    if amt is None:
        return []
    rate = cur.get("service_tax_rate")
    return [{"name": "Service Tax", "rate_pct": float(rate) if rate is not None else None, "amount": float(amt)}]

def _build_payments(pay_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in (pay_rows or []):
        date = _to_iso(r.get("date"))
        amt = r.get("total")
        if amt is None:
            amt = r.get("amount")
        if date and amt is not None:
            out.append({"date": date, "amount": float(amt)})
    return out

def _build_usage(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for ln in (lines or []):
        msisdn = ln.get("service_no")
        for call in (ln.get("calls") or []):
            out.append({
                "msisdn": msisdn,
                "usage_type": "Call",
                "access_point": None,
                "volume_kb": None,
                "duration_sec": call.get("Duration_s"),
                "period_band": call.get("Period"),
                "amount": call.get("Total (RM)"),
            })
    return out

# ---- NEW: builders for Maxis_Line Calls / Maxis_Line Charges ----

def _build_line_calls(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten per-number call details into row dicts that match Maxis_Line Calls.
    """
    out: List[Dict[str, Any]] = []
    for ln in (lines or []):
        msisdn = ln.get("service_no")
        plan   = ln.get("plan")
        for call in (ln.get("calls") or []):
            out.append({
                "service_no": msisdn,
                "plan": plan,
                "Date": call.get("Date"),
                "Time": call.get("Time"),
                "Number Called": call.get("Number Called"),
                "Duration": call.get("Duration"),
                "Period": call.get("Period"),
                "Gross Amount": call.get("Gross Amount"),
                "Total (RM)": call.get("Total (RM)"),
                "Duration_s": call.get("Duration_s"),
            })
    return out

def _build_line_charges(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten per-number charge details into row dicts that match Maxis_Line Charges.
    """
    out: List[Dict[str, Any]] = []
    for ln in (lines or []):
        msisdn = ln.get("service_no")
        plan   = ln.get("plan")
        for row in (ln.get("charges") or []):
            out.append({
                "service_no": msisdn,
                "plan": plan,
                "Item/Barang": row.get("Item/Barang"),
                "Date/Period": row.get("Date/Period"),
                "Amount (RM)": row.get("Amount (RM)"),
                "Total (RM)": row.get("Total (RM)"),
            })
    return out

def build_invoice_package(pdf_path: str) -> Dict[str, Any]:
    """
    Build JSON package that matches dbo.sp_Upsert_InvoicePackage_JSON contract.
    """
    ex = MaxisExtractor(pdf_path)
    raw = ex.extract()
    bs = raw.get("bill_statement") or {}
    cur = (bs.get("current_charges") or {})

    invoice_number = bs.get("bill_reference")
    account_number = bs.get("account_number")
    bill_date = _to_iso(bs.get("statement_date"))
    period = bs.get("billing_period") or {}
    period_start = _to_iso(period.get("from"))
    period_end = _to_iso(period.get("to"))
    currency = "MYR"

    subtotal = cur.get("total_charges_excl_tax")
    tax_total = cur.get("service_tax")
    grand_total = cur.get("total_current_charges")

    file_hash = _sha256_file(Path(pdf_path))

    invoice = {
        "vendor": "maxis",
        "invoice_number": invoice_number,
        "account_number": account_number,
        "bill_date": bill_date,
        "period_start": period_start,
        "period_end": period_end,
        "currency": currency,
        "subtotal": subtotal if subtotal is not None else None,
        "tax_total": tax_total if tax_total is not None else None,
        "grand_total": grand_total if grand_total is not None else None,
        "file_hash": file_hash,
        "parser_version": PARSER_VERSION,
        "source_filename": Path(pdf_path).name
    }

    numbers      = _guess_numbers(raw.get("lines") or [], cur.get("lines") or [])
    charges      = _build_charges(bs)
    taxes        = _build_taxes(bs)
    payments     = _build_payments(raw.get("payment_adjustments") or [])
    usage        = _build_usage(raw.get("lines") or [])
    line_calls   = _build_line_calls(raw.get("lines") or [])
    line_charges = _build_line_charges(raw.get("lines") or [])

    pkg = {
        "invoice": invoice,
        "numbers": numbers,
        "charges": charges,
        "taxes": taxes,
        "payments": payments,
        "usage": usage,
        "line_calls": line_calls,
        "line_charges": line_charges,
    }
    return pkg


# ---- Thin wrapper expected by `parsers.*` imports (no logic change) ----
def extract(pdf_path: str) -> Dict[str, Any]:
    """Standard entrypoint: returns DB-ready package for a Maxis PDF."""
    return build_invoice_package(pdf_path)


# Debug CLI: prints DB-ready JSON
if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "sample_maxis_bill.pdf"
    out = build_invoice_package(path)
    print(json.dumps(out, indent=2, ensure_ascii=False))
