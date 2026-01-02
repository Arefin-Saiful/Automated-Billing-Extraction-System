# app/reports/vendor_report_pdf.py

from datetime import date
from decimal import Decimal
from pathlib import Path
import os
import pdfkit

from jinja2 import Environment, FileSystemLoader, select_autoescape
from app.core.db import db_cursor


# ============================================================
# TEMPLATE ENGINE CONFIG
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "ui" / "templates"

env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"])
)

# ============================================================
# PDFKIT CONFIG (WKHTMLTOPDF)
# ============================================================
WKHTMLTOPDF_PATH = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
config = pdfkit.configuration(wkhtmltopdf=WKHTMLTOPDF_PATH)


# ============================================================
# DISPLAY NAMES
# ============================================================
VENDOR_META = {
    "maxis":  {"vendor_display": "Maxis Broadband Sdn Bhd 199201002549 (234053-D)"},
    "celcom": {"vendor_display": "Celcom Mobile Sdn Bhd (199701022942 / 443372-M)"},
    "digi":   {"vendor_display": "Digi Telecommunications Sdn Bhd (201701035949 / 125231-W)"},
}

# ============================================================
# SQL STRINGS
# ============================================================
MAXIS_SUMMARY_SQL = """
SELECT
    [Account No] AS account_no,
    [Invoice No] AS invoice_no,
    [Invoice Date] AS invoice_date,
    [Total Outstanding] AS amount_myr,
    ISNULL([Adjustments],0) AS rounding_adjustment
FROM [Telco Bills].[dbo].[Maxis_Bill Statement]
WHERE [Invoice Date] BETWEEN ? AND ?
ORDER BY [Invoice Date], [Invoice No];
"""

MAXIS_DETAIL_SQL = """
SELECT
    c.[Invoice No] AS invoice_no,
    h.[Invoice Date] AS invoice_date,
    'MAXIS' AS telco,
    c.[PhoneNorm] AS phone_number,
    c.[Upon Name] AS opun,
    c.[amount] AS amount,
    '6%' AS sst,
    c.[Total Amount with 6% SST] AS total_with_sst
FROM [Telco Bills].[dbo].[Maxis_Current Charges] c
JOIN [Telco Bills].[dbo].[Maxis_Bill Statement] h
    ON c.[Invoice No] = h.[Invoice No]
WHERE h.[Invoice Date] BETWEEN ? AND ?
ORDER BY h.[Invoice Date], c.[Invoice No], c.[PhoneNorm];
"""

# ✅ CELCOM SUMMARY: Amount(MYR) must use [Current Charges]
CELCOM_SUMMARY_SQL = """
SELECT
    [Account No] AS account_no,
    [Invoice No] AS invoice_no,
    [Invoice Date] AS invoice_date,
    CAST(ISNULL([Current Charges],0) AS decimal(18,2)) AS amount_myr
FROM [Telco Bills].[dbo].[Celcom_Bill & Account Summary]
WHERE [Invoice Date] BETWEEN ? AND ?
ORDER BY [Invoice Date], [Invoice No];
"""

# ✅ CELCOM DETAIL remains from Celcom_Registered Mobile Number (values)
CELCOM_DETAIL_SQL = """
SELECT
    r.[Invoice No] AS invoice_no,
    h.[Invoice Date] AS invoice_date,
    'CELCOM' AS telco,
    r.[PhoneNorm] AS phone_number,
    r.[Upon Name] AS opun,
    r.[amount(RM)] AS amount,
    '6%' AS sst,
    r.[Total Amount with 6% SST] AS total_with_sst
FROM [Telco Bills].[dbo].[Celcom_Registered Mobile Number] r
JOIN [Telco Bills].[dbo].[Celcom_Bill & Account Summary] h
    ON r.[Invoice No] = h.[Invoice No]
WHERE h.[Invoice Date] BETWEEN ? AND ?
ORDER BY h.[Invoice Date], r.[Invoice No], r.[PhoneNorm];
"""

DIGI_SUMMARY_SQL = """
SELECT
    h.[Account No] AS account_no,
    h.[Invoice No] AS invoice_no,
    h.[Invoice Date] AS invoice_date,
    h.[Total Outstanding] AS amount_myr,
    ISNULL(s.[Adjustments],0) AS rounding_adjustment
FROM [Telco Bills].[dbo].[Digi_Invoice Header] h
LEFT JOIN [Telco Bills].[dbo].[Digi_Charges Summary] s
    ON h.[Invoice No] = s.[Invoice No]
WHERE h.[Invoice Date] BETWEEN ? AND ?
ORDER BY h.[Invoice Date], h.[Invoice No];
"""

DIGI_DETAIL_SQL = """
SELECT
    s.[Invoice No] AS invoice_no,
    h.[Invoice Date] AS invoice_date,
    'DIGI' AS telco,
    s.[PhoneNorm] AS phone_number,
    s.[Upon Name] AS opun,
    s.[Current Bill Amount] AS amount,
    '6%' AS sst,
    s.[Total Amount with 6% SST] AS total_with_sst
FROM [Telco Bills].[dbo].[Digi_Service Summary] s
JOIN [Telco Bills].[dbo].[Digi_Invoice Header] h
    ON s.[Invoice No] = h.[Invoice No]
WHERE h.[Invoice Date] BETWEEN ? AND ?
ORDER BY h.[Invoice Date], s.[Invoice No], s.[PhoneNorm];
"""

# ============================================================
# SQL SELECTOR
# ============================================================
def get_sql_for_vendor(vendor: str):
    v = vendor.lower()
    if v == "maxis":
        return {"summary": MAXIS_SUMMARY_SQL, "detail": MAXIS_DETAIL_SQL, "rounding": None, "footer_type": "maxis_digi"}
    if v == "celcom":
        # ✅ rounding SQL removed (no longer needed)
        return {"summary": CELCOM_SUMMARY_SQL, "detail": CELCOM_DETAIL_SQL, "rounding": None, "footer_type": "celcom"}
    if v == "digi":
        return {"summary": DIGI_SUMMARY_SQL, "detail": DIGI_DETAIL_SQL, "rounding": None, "footer_type": "maxis_digi"}
    raise ValueError(f"Unknown vendor: {vendor}")


# ============================================================
# FETCH DB ROWS
# ============================================================
def fetch_all(sql: str, params: tuple):
    with db_cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ============================================================
# MAIN REPORT FUNCTION (HTML + PDF)
# ============================================================
def generate_vendor_pdf(vendor: str, from_date: date, to_date: date, output_path: str):
    vendor_lower = vendor.lower()
    sql = get_sql_for_vendor(vendor_lower)
    params = (from_date, to_date)

    summary = fetch_all(sql["summary"], params)
    detail = fetch_all(sql["detail"], params)

    def to_dec(v):
        return Decimal(str(v or 0))

    # ✅ NEW (ALL VENDORS): total row for SUMMARY OF ALL BILLS
    summary_total_amount = sum(to_dec(r.get("amount_myr", 0)) for r in summary)

    # ======================================================
    # ✅ CELCOM CHANGES ONLY:
    # - Remove rounding + overdue logic completely
    # - Total Amount column (in template) will display total_with_sst value
    # - Grand Total = sum(total_with_sst)
    # ======================================================
    if vendor_lower == "celcom":
        # Keep sorting consistent (same as before)
        detail.sort(key=lambda x: (
            x.get("invoice_date") or date.min,
            str(x.get("invoice_no") or ""),
            str(x.get("phone_number") or "")
        ))

        # Grand total is sum of Total w/ SST (Registered table)
        grand_total = sum(to_dec(r.get("total_with_sst", 0)) for r in detail)

        # keep vars for template compatibility
        monthly_charges = Decimal("0")
        rounding = Decimal("0")
        total_rounding = None
        total_overdue = None

    # ======================================================
    # MAXIS + DIGI LOGIC (unchanged)
    # ======================================================
    else:
        monthly_charges = sum(to_dec(r.get("total_with_sst", 0)) for r in detail)
        rounding = sum(to_dec(r.get("rounding_adjustment", 0)) for r in summary)
        grand_total = monthly_charges + rounding

        total_rounding = None
        total_overdue = None

    # ======================================================
    # GROUP DETAILS BY OPUN
    # ======================================================
    opun_groups = {}
    for r in detail:
        op = (r.get("opun") or "").strip() or "UNMAPPED"
        opun_groups.setdefault(op, []).append(r)

    opun_sections = []
    for op, rows in opun_groups.items():
        rows.sort(key=lambda x: (
            x.get("invoice_date") or date.min,
            str(x.get("invoice_no") or ""),
            str(x.get("phone_number") or "")
        ))

        # ✅ CELCOM: OPUN total must be sum of total_with_sst (will be shown as Total Amount)
        total = sum(to_dec(x.get("total_with_sst", 0)) for x in rows)

        opun_sections.append({
            "opun": op,
            "rows": rows,
            "total": total
        })

    opun_sections.sort(key=lambda x: x["opun"])

    # ======================================================
    # RENDER HTML
    # ======================================================
    template = env.get_template("vendor_report.html")

    html = template.render(
        vendor=vendor_lower,
        vendor_display=VENDOR_META[vendor_lower]["vendor_display"],
        from_date=from_date,
        to_date=to_date,
        summary_rows=summary,
        detail_rows=detail,
        monthly_charges=monthly_charges,
        rounding=rounding,
        total_rounding=total_rounding,
        total_overdue=total_overdue,
        grand_total=grand_total,
        summary_total_amount=summary_total_amount,  # ✅ used for the new total row in summary table
        footer_type=sql["footer_type"],
        opun_sections=opun_sections
    )

    # ======================================================
    # PDFKIT EXPORT
    # ======================================================
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pdfkit.from_string(
        html,
        output_path,
        configuration=config,
        options={
            "--enable-local-file-access": "",
            "--quiet": "",
            "--page-size": "A4",
            "--margin-top": "10mm",
            "--margin-bottom": "12mm",
            "--margin-left": "8mm",
            "--margin-right": "8mm"
        }
    )

    return output_path
