from __future__ import annotations
import json
import pyodbc
from typing import Any, Dict, Iterable, List, Tuple, Optional
from collections import defaultdict

# =========================
# TABLE NAMES (exact)
# =========================

# ✅ UPDATED (Maxis): removed line_calls + line_charges because you DROPPED those tables
T_MAXIS = {
    "bill_stmt":        "[dbo].[Maxis_Bill Statement]",
    "current":          "[dbo].[Maxis_Current Charges]",
    "pay_adjust":       "[dbo].[Maxis_Payments & Adjust]",
}

# ✅ UPDATED: removed the 3 call tables because you want to DROP them
T_CELCOM = {
    "bill_stmt":        "[dbo].[Celcom_Bill & Account Summary]",
    "current":          "[dbo].[Celcom_Current Charges]",
    "detail_monthly":   "[dbo].[Celcom_Detail Charges - Monthly Amount]",
    "discounts":        "[dbo].[Celcom_Discount & Rebates]",
    "prev_pay":         "[dbo].[Celcom_Previous payment details]",  # optional
    "registered":       "[dbo].[Celcom_Registered Mobile Number]",
}

T_DIGI = {
    "invoice_header":   "[dbo].[Digi_Invoice Header]",
    "charges_summary":  "[dbo].[Digi_Charges Summary]",
    "payment_history":  "[dbo].[Digi_Payment History]",
    # NO itemised table in your current Digi schema
    "svc_summary":      "[dbo].[Digi_Service Summary]",
    "svc_tax":          "[dbo].[Digi_service_tax]",   # separate tax table
}

# =========================
# helpers
# =========================
def _cols(cur, table: str) -> List[str]:
    cur.execute(f"SELECT TOP 0 * FROM {table}")
    return [d[0] for d in cur.description]


def _prune(row: Dict[str, Any], cols: List[str]) -> Dict[str, Any]:
    cmap = {c.lower(): c for c in cols}
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if k is None:
            continue
        key = cmap.get(str(k).lower())
        if key:
            out[key] = v
    return out


def _sql_lit(val: Optional[str]) -> str:
    """Return a safe SQL string literal (or NULL) for RAW injections."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


def _raw(val: str) -> str:
    """Mark a value as RAW SQL (no params)."""
    return f"{val}::raw"


def _exec_safe(cur: "pyodbc.Cursor", sql: str, params: List[Any], tag: str) -> None:
    """
    Safety net: ensure '?' markers match param count and give a clear error if not.
    Also wrap pyodbc errors with SQL + params so 07002 is debuggable.
    """
    markers = sql.count("?")
    if markers != len(params):
        raise ValueError(
            f"[{tag}] SQL placeholder mismatch: markers={markers}, params={len(params)}\n"
            f"SQL: {sql}\n"
            f"Params: {params}"
        )
    try:
        cur.execute(sql, params)
    except pyodbc.Error as e:
        raise RuntimeError(
            f"[{tag}] ODBC error: {e.args!r}\n"
            f"SQL: {sql}\n"
            f"Params: {params}"
        ) from e


def _insert_many(cur, table: str, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Bulk insert with support for RAW values marked via _raw().
    """
    rows = [r for r in rows if r]
    if not rows:
        return 0

    cols = _cols(cur, table)

    # normalize & split into parameterized + raw per row
    norm: List[Tuple[List[str], List[Any], List[Tuple[str, str]]]] = []
    for r in rows:
        r = _prune(r, cols)
        if not r:
            continue
        pcols: List[str] = []
        pvals: List[Any] = []
        rcols: List[Tuple[str, str]] = []   # (col, raw_sql)
        for k, v in r.items():
            if isinstance(v, str) and v.endswith("::raw"):
                rcols.append((k, v[:-5]))
            else:
                pcols.append(k)
                pvals.append(v)
        norm.append((pcols, pvals, rcols))

    if not norm:
        return 0

    # bucket rows sharing the same raw signature
    buckets: Dict[str, List[Tuple[List[str], List[Any], List[Tuple[str, str]]]]] = defaultdict(list)
    for pcols, pvals, rcols in norm:
        sig = "|".join(pcols + [f"{c}={sql}" for c, sql in rcols])
        buckets[sig].append((pcols, pvals, rcols))

    MAX_PARAMS = 2000  # keep under SQL Server 2100 limit with margin
    total = 0

    for sig, items in buckets.items():
        pcols, _, rcols = items[0]
        all_cols = pcols + [c for c, _ in rcols]
        col_sql = ", ".join(f"[{c}]" for c in all_cols)
        per_row_params = len(pcols)

        if per_row_params == 0:
            # Only RAW values, no params
            values_sql: List[str] = []
            for _, _, rc in items:
                raw_sql = ",".join(sql for _, sql in rc)
                values_sql.append("(" + raw_sql + ")")
            sql = f"INSERT INTO {table} ({col_sql}) VALUES " + ",".join(values_sql)
            _exec_safe(cur, sql, [], tag=f"insert_many:{table}:{sig}")
            total += len(items)
            continue

        max_rows_per_batch = max(1, MAX_PARAMS // per_row_params)

        for i in range(0, len(items), max_rows_per_batch):
            chunk = items[i:i + max_rows_per_batch]
            values_sql: List[str] = []
            params: List[Any] = []

            for pc, pv, rc in chunk:
                raw_sql = ",".join(sql for _, sql in rc)
                if pc and rc:
                    values_sql.append(
                        "(" + ",".join(["?"] * len(pc)) + ("," + raw_sql if raw_sql else "") + ")"
                    )
                    params.extend(pv)
                elif pc and not rc:
                    values_sql.append("(" + ",".join(["?"] * len(pc)) + ")")
                    params.extend(pv)
                else:
                    values_sql.append("(" + raw_sql + ")")

            sql = f"INSERT INTO {table} ({col_sql}) VALUES " + ",".join(values_sql)
            _exec_safe(
                cur,
                sql,
                params,
                tag=f"insert_many:{table}:{sig}:chunk{i//max_rows_per_batch}",
            )
            total += len(chunk)

    return total


def _merge_by_keys(cur, table: str, key_map: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """
    Generic MERGE helper:
      - key_map: columns used in ON (all parameterized)
      - payload: columns to update/insert (supports ::raw)
    """
    cols = _cols(cur, table)
    payload = _prune(payload, cols)

    if not payload:
        return

    for k in key_map.keys():
        if k not in cols:
            raise ValueError(f"Key column '{k}' not found in {table}")

    pcols: List[str] = []
    pvals: List[Any] = []
    rcols: List[Tuple[str, str]] = []
    for k, v in payload.items():
        if isinstance(v, str) and v.endswith("::raw"):
            rcols.append((k, v[:-5]))
        else:
            pcols.append(k)
            pvals.append(v)

    set_sql = [f"[{k}]=?" for k in pcols] + [f"[{k}]={sql}" for k, sql in rcols]
    ins_cols = list(payload.keys())
    ins_vals = ",".join(["?"] * len(pcols) + [sql for _, sql in rcols])

    key_inline = " AND ".join(f"T.[{k}]=?" for k in key_map.keys())
    key_params = list(key_map.values())

    sql = f"""
    MERGE {table} AS T
    USING (SELECT 1 AS x) S
      ON {key_inline}
    WHEN MATCHED THEN UPDATE SET {", ".join(set_sql)}
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(f'[{c}]' for c in ins_cols)}) VALUES ({ins_vals});
    """

    params = key_params + pvals + pvals
    _exec_safe(cur, sql, params, tag=f"merge:{table}")


def _get_scalar(cur, sql: str, params: Tuple[Any, ...]) -> Optional[Any]:
    _exec_safe(cur, sql, list(params), tag="get_scalar")
    row = cur.fetchone()
    return None if not row else row[0]


def _sum(charges: List[Dict[str, Any]], want: str) -> float:
    w = want.lower()
    return float(
        sum((c.get("amount") or 0) for c in charges if (c.get("category") or "").lower() == w)
    )


def _other_pos(charges: List[Dict[str, Any]]) -> float:
    tot = 0.0
    for c in charges:
        if (c.get("category") or "").lower() == "other":
            amt = float(c.get("amount") or 0)
            if amt > 0:
                tot += amt
    return tot


def _other_neg(charges: List[Dict[str, Any]]) -> float:
    tot = 0.0
    for c in charges:
        if (c.get("category") or "").lower() == "other":
            amt = float(c.get("amount") or 0)
            if amt < 0:
                tot += amt
    return tot


# =========================
# main loader
# =========================
class TelcoLoader:
    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    def save(self, pkg: Dict[str, Any]) -> Dict[str, Any]:
        inv = pkg["invoice"]
        vendor = (inv.get("vendor") or "").lower()
        stats: Dict[str, int] = {}
        with pyodbc.connect(self.conn_str, autocommit=False) as conn:
            cur = conn.cursor()
            try:
                if vendor == "maxis":
                    stats = self._maxis(cur, pkg)
                elif vendor == "celcom":
                    stats = self._celcom(cur, pkg)
                elif vendor == "digi":
                    stats = self._digi(cur, pkg)
                else:
                    raise ValueError(f"Unsupported vendor '{vendor}'")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return {
            "ok": True,
            "vendor": vendor,
            "invoice_number": inv.get("invoice_number"),
            "table_writes": stats,
        }

    # ---------------- MAXIS ----------------
    # ✅ UPDATED: removed all logic for dropped tables Maxis_Line Calls + Maxis_Line Charges
    def _maxis(self, cur, pkg: Dict[str, Any]) -> Dict[str, int]:
        stats = {
            "bill_stmt": 0,
            "current": 0,
            "pay_adjust": 0,
        }

        inv      = pkg["invoice"]
        inv_no   = inv.get("invoice_number")
        charges  = pkg.get("charges", []) or []
        numbers  = pkg.get("numbers", []) or []
        pays     = pkg.get("payments", []) or []

        # ---------- PURGE EXISTING ROWS FOR THIS INVOICE ----------
        purge_order = (
            "pay_adjust",
            "current",
            "bill_stmt",
        )
        for key in purge_order:
            _exec_safe(
                cur,
                f"DELETE FROM {T_MAXIS[key]} WHERE [Invoice No] = ?",
                [inv_no],
                tag=f"maxis_purge:{key}",
            )
        # -----------------------------------------------------------

        # Header: Maxis_Bill Statement  (MERGE on Account No + Invoice No)
        _merge_by_keys(
            cur,
            T_MAXIS["bill_stmt"],
            key_map={
                "Account No": inv.get("account_number"),
                "Invoice No": inv.get("invoice_number"),
            },
            payload={
                "Account No":              inv.get("account_number"),
                "Invoice No":              inv.get("invoice_number"),
                "Invoice Date":            inv.get("bill_date"),
                "Billing From":            inv.get("period_start"),
                "Billing To":              inv.get("period_end"),
                "Due Date":                inv.get("due_date"),
                "Previous Bill(s)":        _sum(charges, "previous"),
                "Payments":                _sum(charges, "payments"),
                "Adjustments":             _sum(charges, "adjustments"),
                "Previous Overdue Amount": _other_pos(charges),
                "Monthly Fixed Charges":   _sum(charges, "monthly"),
                "Usage":                   _sum(charges, "usage"),
                "Other Credits":           _other_neg(charges),
                "Discounts":               _sum(charges, "discounts"),
                "Service Tax (6%)":        inv.get("tax_total"),
                "Current Bill Amount":     _sum(charges, "current") or inv.get("grand_total"),
                "Total Outstanding":       inv.get("grand_total"),
                "CreatedAt":               _raw("SYSUTCDATETIME()"),
            },
        )
        stats["bill_stmt"] = 1

        # Maxis_Current Charges
        rows_cur: List[Dict[str, Any]] = []
        if numbers:
            for n in numbers:
                ms = n.get("msisdn")
                rows_cur.append({
                    "Invoice No": inv.get("invoice_number"),
                    "service_no": ms,
                    "Upon Name":  None,
                    "UponID":     None,
                    "plan":       n.get("plan_name"),
                    "amount":     n.get("total_amount"),
                    "PhoneNorm":  _raw(f"dbo.fn_NormalizePhone({_sql_lit(ms)})") if ms else None,
                })
        else:
            for c in charges:
                cat = (c.get("category") or "").lower()
                if cat in {"monthly", "current", "discounts", "other", "usage", "payments", "adjustments", "previous"}:
                    rows_cur.append({
                        "Invoice No": inv.get("invoice_number"),
                        "service_no": None,
                        "plan":       c.get("label"),
                        "amount":     c.get("amount"),
                        "PhoneNorm":  None,
                    })
        stats["current"] = _insert_many(cur, T_MAXIS["current"], rows_cur)

        # ---- MAP UPON FOR MAXIS CURRENT CHARGES ----
        _exec_safe(
            cur,
            "EXEC dbo.sp_MapUpon_Maxis_CurrentCharges ?",
            [inv_no],
            tag="map_upon_maxis",
        )

        # Maxis_Payments & Adjust
        rows_pay: List[Dict[str, Any]] = []
        for p in (pkg.get("pay_adjust") or pkg.get("payment_adjustments") or []):
            rows_pay.append({
                "Invoice No":         inv.get("invoice_number"),
                "service_identifier": inv.get("account_number"),
                "Upon Name":          None,
                "PhoneNorm":          _raw(f"dbo.fn_NormalizePhone({_sql_lit(inv.get('account_number'))})"),
                "UponID":             None,
                "description":        p.get("description") or "Payment / Adjustment",
                "date":               p.get("date") or inv.get("bill_date"),
                "amount":             p.get("amount"),
                "svc_tax":            p.get("svc_tax") or 0,
                "total":              p.get("total")   or p.get("amount"),
            })

        if not rows_pay and pays:
            for p in pays:
                rows_pay.append({
                    "Invoice No":         inv.get("invoice_number"),
                    "service_identifier": inv.get("account_number"),
                    "Upon Name":          None,
                    "PhoneNorm":          _raw(f"dbo.fn_NormalizePhone({_sql_lit(inv.get('account_number'))})"),
                    "UponID":             None,
                    "description":        "Payment Received",
                    "date":               p.get("date") or inv.get("bill_date"),
                    "amount":             p.get("amount"),
                    "svc_tax":            0,
                    "total":              p.get("amount"),
                })

        if not rows_pay:
            for c in charges:
                cat = (c.get("category") or "").lower()
                if cat in ("payments", "adjustments"):
                    rows_pay.append({
                        "Invoice No":         inv.get("invoice_number"),
                        "service_identifier": inv.get("account_number"),
                        "Upon Name":          None,
                        "PhoneNorm":          _raw(f"dbo.fn_NormalizePhone({_sql_lit(inv.get('account_number'))})"),
                        "UponID":             None,
                        "description":        (c.get("label") or cat.title()),
                        "date":               inv.get("bill_date"),
                        "amount":             c.get("amount"),
                        "svc_tax":            0,
                        "total":              c.get("amount"),
                    })

        stats["pay_adjust"] = _insert_many(cur, T_MAXIS["pay_adjust"], rows_pay)
        return stats

    # ---------------- CELCOM ----------------
    def _celcom(self, cur, pkg: Dict[str, Any]) -> Dict[str, int]:
        stats = {
            "bill_stmt": 0,
            "current": 0,
            "detail_monthly": 0,
            "discounts": 0,
            "prev_pay": 0,
            "registered": 0,
        }

        inv = pkg["invoice"]
        inv_no = inv.get("invoice_number")

        numbers         = pkg.get("numbers", []) or []
        charges_summary = pkg.get("charges_summary", []) or []
        prev_payments   = (
            pkg.get("previous_payments", [])
            or pkg.get("payments", [])
            or []
        )

        def _as_float(x: Any) -> Optional[float]:
            try:
                if x is None:
                    return None
                return float(x)
            except Exception:
                return None

        def _sum_cs_match(pred) -> float:
            tot = 0.0
            for cs in charges_summary:
                if pred((cs.get("label") or "").lower()):
                    tot += float(cs.get("total") or 0.0)
            return float(tot)

        def _num_sum(n: Dict[str, Any], want_cat: str) -> float:
            want = want_cat.lower()
            tot = 0.0
            for c in (n.get("charges") or []):
                if (c.get("category") or "").lower() == want:
                    tot += float(c.get("amount") or 0.0)
            return float(tot)

        total_payments = None
        if prev_payments:
            vals = [
                float(p.get("amount") or 0.0)
                for p in prev_payments
                if _as_float(p.get("amount")) is not None
            ]
            if vals:
                total_payments = round(sum(vals), 2)

        # =====================================================
        # ✅ SAFETY: derive rounding adjustment if missing
        # =====================================================
        if inv.get("rounding_adjustment") is None:
            try:
                tc  = inv.get("total_current_charges")
                mc  = inv.get("subtotal")          # Monthly Charges
                tax = inv.get("tax_total")         # Service Tax

                if tc is not None and mc is not None and tax is not None:
                    inv["rounding_adjustment"] = round(
                        float(tc) - float(mc) - float(tax),
                        2
                    )
                else:
                    inv["rounding_adjustment"] = 0.0
            except Exception:
                inv["rounding_adjustment"] = 0.0
        # =====================================================

        bills = [{
            "bill_statement_number": inv_no,
            "account_number": inv.get("account_number"),
            "bill_date": inv.get("bill_date"),
            "billing_from": inv.get("period_start"),
            "billing_to": inv.get("period_end"),
            "credit_limit": inv.get("credit_limit"),
            "deposit": inv.get("deposit"),
            "plan_name": inv.get("plan_name"),
            "overdue_charges": inv.get("total_overdue_charges"),
            "current_charges": inv.get("subtotal") or inv.get("total_current_charges"),
            "total_amount_due": inv.get("grand_total"),
            "previous_balance": inv.get("previous_balance"),
            "total_payments": total_payments,
            "monthly_charges_rm": inv.get("subtotal"),
            "service_tax_6pct": inv.get("tax_total"),
            "rounding_adjustment": inv.get("rounding_adjustment"),
            "total_current_charges": inv.get("total_current_charges"),
        }]

        ccb = []
        for cs in charges_summary:
            ccb.append({
                "bill_statement_number": inv_no,
                "category": cs.get("label"),
                "total": cs.get("total"),
            })

        reg = []
        for n in numbers:
            ms = n.get("mobile") or n.get("msisdn")
            if not ms:
                continue
            monthly_amount = _num_sum(n, "monthly")
            usage_amount = _num_sum(n, "usage")
            disc_amount = _num_sum(n, "discounts")
            total_amount = round(monthly_amount + usage_amount + disc_amount, 2)

            reg.append({
                "bill_statement_number": inv_no,
                "mobile": ms,
                "credit_limit": inv.get("credit_limit"),
                "one_time_amount": 0,
                "monthly_amount": monthly_amount or None,
                "usage_amount": usage_amount or None,
                "discounts_rebates": disc_amount or None,
                "total_amount_rm": total_amount or None,
            })

        monthly_items = []
        for n in numbers:
            ms = n.get("mobile") or n.get("msisdn")
            for chg in (n.get("charges") or []):
                cat = (chg.get("category") or "").lower()
                if cat not in ("monthlyitem",):
                    continue
                monthly_items.append({
                    "bill_statement_number": inv_no,
                    "description": chg.get("label") if not ms else f"{ms} - {chg.get('label')}",
                    "from_date": chg.get("from_date") or chg.get("from"),
                    "to_date": chg.get("to_date") or chg.get("to"),
                    "from": chg.get("from"),
                    "to": chg.get("to"),
                    "amount_rm": chg.get("amount"),
                })

        disc_items = []
        for n in numbers:
            ms = n.get("mobile") or n.get("msisdn")
            for chg in (n.get("charges") or []):
                lbl = (chg.get("label") or "").lower()
                cat = (chg.get("category") or "").lower()
                if cat == "discounts" or ("discount" in lbl) or ("rebate" in lbl):
                    disc_items.append({
                        "bill_statement_number": inv_no,
                        "description": chg.get("label") if not ms else f"{ms} - {chg.get('label')}",
                        "amount_rm": chg.get("amount"),
                    })

        disc_total = _sum_cs_match(lambda lbl: ("discount" in lbl) or ("rebate" in lbl))
        disc_totals = []
        if (not disc_items) and abs(disc_total) > 1e-9:
            disc_totals.append({
                "bill_statement_number": inv_no,
                "description": "Discounts & Rebates (Total)",
                "total": disc_total,
            })

        flat_payload = {
            "bills": bills,
            "current_charges_breakdown": ccb,
            "registered": reg,
            "monthly_items": monthly_items,
            "discount_rebate_items": disc_items,
            "discounts_rebates": disc_totals,
        }

        payload_json = json.dumps(flat_payload, ensure_ascii=False)

        _exec_safe(
            cur,
            "EXEC [dbo].[sp_Upsert_InvoicePackage_JSON_Celcom] ?",
            [payload_json],
            tag="celcom_sp",
        )

        def _count(table: str) -> int:
            v = _get_scalar(cur, f"SELECT COUNT(1) FROM {table} WHERE [Invoice No] = ?", (inv_no,))
            return int(v or 0)

        if _count(T_CELCOM["discounts"]) == 0:
            rows_disc: List[Dict[str, Any]] = []

            for n in numbers:
                for chg in (n.get("charges") or []):
                    cat = (chg.get("category") or "").lower()
                    lbl = (chg.get("label") or "").lower()
                    if cat == "discounts" or "discount" in lbl or "rebate" in lbl:
                        rows_disc.append({
                            "Invoice No":  inv_no,
                            "Description": chg.get("label"),
                            "Amount (RM)": chg.get("amount"),
                        })

            if not rows_disc:
                for cs in charges_summary:
                    lbl = (cs.get("label") or "").lower()
                    if "discount" in lbl or "rebate" in lbl:
                        rows_disc.append({
                            "Invoice No":  inv_no,
                            "Description": cs.get("label"),
                            "Amount (RM)": cs.get("total"),
                        })

            if rows_disc:
                _insert_many(cur, T_CELCOM["discounts"], rows_disc)

        if _count(T_CELCOM["detail_monthly"]) == 0:
            rows_dm: List[Dict[str, Any]] = []
            for n in numbers:
                for chg in (n.get("charges") or []):
                    if (chg.get("category") or "").lower() != "monthlyitem":
                        continue
                    rows_dm.append({
                        "Invoice No": inv_no,
                        "Description": chg.get("label"),
                        "From Date": chg.get("from") or inv.get("period_start"),
                        "To Date": chg.get("to") or inv.get("period_end"),
                        "Total Amount (RM)": chg.get("amount"),
                    })
            if rows_dm:
                _insert_many(cur, T_CELCOM["detail_monthly"], rows_dm)

        if _count(T_CELCOM["registered"]) == 0 and numbers:
            rows_reg: List[Dict[str, Any]] = []
            for n in numbers:
                ms = n.get("mobile") or n.get("msisdn")
                if not ms:
                    continue

                n_charges = n.get("charges") or []

                n_monthly = sum(
                    (c.get("amount") or 0.0)
                    for c in n_charges
                    if (c.get("category") or "").lower() == "monthly"
                )
                n_usage = sum(
                    (c.get("amount") or 0.0)
                    for c in n_charges
                    if (c.get("category") or "").lower() == "usage"
                )
                n_discounts = sum(
                    (c.get("amount") or 0.0)
                    for c in n_charges
                    if (c.get("category") or "").lower() == "discounts"
                )

                total_amount = n_monthly + n_usage + n_discounts

                rows_reg.append({
                    "Invoice No":         inv_no,
                    "Mobile Number":      ms,
                    "Upon Name":          None,
                    "PhoneNorm":          _raw(f"dbo.fn_NormalizePhone({_sql_lit(ms)})"),
                    "UponID":             None,
                    "Credit Limit":       None,
                    "One Time Amount":    0,
                    "Monthly Amount":     n_monthly or None,
                    "Usage Amount":       n_usage or None,
                    "Discount & Rebates": n_discounts or None,
                    "amount(RM)":         total_amount or None,
                })
            if rows_reg:
                _insert_many(cur, T_CELCOM["registered"], rows_reg)

        try:
            if _count(T_CELCOM["prev_pay"]) == 0 and prev_payments:
                rows_pp: List[Dict[str, Any]] = []
                for p in prev_payments:
                    rows_pp.append({
                        "Invoice No":      inv_no,
                        "Date":            p.get("date") or inv.get("bill_date"),
                        "Payment Method":  p.get("payment_method"),
                        "Receipt No":      p.get("receipt_no"),
                        "Reference":       p.get("reference"),
                        "Description":     p.get("description") or "Payment Received",
                        "Amount (RM)":     p.get("amount"),
                    })
                if rows_pp:
                    _insert_many(cur, T_CELCOM["prev_pay"], rows_pp)
        except Exception:
            pass

        _exec_safe(
            cur,
            "EXEC dbo.sp_MapUpon_Celcom_RegisteredMobile ?",
            [inv_no],
            tag="map_upon_celcom",
        )

        stats["bill_stmt"]      = _count(T_CELCOM["bill_stmt"])
        stats["current"]        = _count(T_CELCOM["current"])
        stats["detail_monthly"] = _count(T_CELCOM["detail_monthly"])
        stats["discounts"]      = _count(T_CELCOM["discounts"])
        stats["registered"]     = _count(T_CELCOM["registered"])

        try:
            stats["prev_pay"] = _count(T_CELCOM["prev_pay"])
        except Exception:
            stats["prev_pay"] = 0

        return stats

    # ---------------- DIGI ----------------
    # ✅ UNCHANGED (your exact code)
    def _digi(self, cur, pkg: Dict[str, Any]) -> Dict[str, int]:
        stats = {
            "invoice_header": 0,
            "charges_summary": 0,
            "payment_history": 0,
            "svc_summary": 0,
            "svc_tax": 0,
        }

        inv      = pkg["invoice"]
        charges  = pkg.get("charges", []) or []
        numbers  = pkg.get("numbers", []) or []
        raw      = pkg.get("raw", {}) or {}
        inv_no   = inv.get("invoice_number")

        raw_svc_summary = raw.get("service_summary", {}) or {}
        raw_svc_details = raw.get("service_details", []) or {}
        raw_pay_history = raw.get("payment_history", []) or []

        for key in ("svc_summary", "svc_tax", "charges_summary", "payment_history"):
            _exec_safe(
                cur,
                f"DELETE FROM {T_DIGI[key]} WHERE [Invoice No] = ?",
                [inv_no],
                tag=f"digi_purge:{key}",
            )

        _merge_by_keys(
            cur,
            T_DIGI["invoice_header"],
            key_map={
                "Account No": inv.get("account_number"),
                "Invoice No": inv.get("invoice_number"),
            },
            payload={
                "Account No":        inv.get("account_number"),
                "Invoice No":        inv.get("invoice_number"),
                "Invoice Date":      inv.get("bill_date"),
                "Period start":      inv.get("period_start"),
                "Period end":        inv.get("period_end"),
                "No. of Lines":      len(numbers) if numbers else None,
                "Due Date":          inv.get("due_date"),
                "Total Outstanding": inv.get("grand_total"),
            },
        )
        stats["invoice_header"] = 1

        stats["charges_summary"] = _insert_many(
            cur,
            T_DIGI["charges_summary"],
            [{
                "Invoice No":              inv.get("invoice_number"),
                "Previous Bill(s)":        _sum(charges, "previous"),
                "Payments":                _sum(charges, "payments"),
                "Adjustments":             _sum(charges, "adjustments"),
                "Previous Overdue Amount": _other_pos(charges),
                "Monthly Fixed Charges":   _sum(charges, "monthly"),
                "Usage":                   _sum(charges, "usage"),
                "Other Credits":           _other_neg(charges),
                "Discounts":               _sum(charges, "discounts"),
                "Service Tax":             inv.get("tax_total"),
                "Current Bill":            _sum(charges, "current") or inv.get("grand_total"),
                "Total Outstanding":       inv.get("grand_total"),
            }],
        )

        pays: List[Dict[str, Any]] = pkg.get("payments", []) or []
        if not pays and raw_pay_history:
            for ph in raw_pay_history:
                pays.append({
                    "date":   ph.get("Date"),
                    "amount": ph.get("Amount"),
                })

        rows_pay: List[Dict[str, Any]] = []
        for p in pays:
            rows_pay.append({
                "Invoice No":  inv.get("invoice_number"),
                "Date":        p.get("date") or inv.get("bill_date"),
                "Amount (RM)": p.get("amount"),
            })
        stats["payment_history"] = _insert_many(cur, T_DIGI["payment_history"], rows_pay)

        svc_lines = raw_svc_summary.get("lines", []) or []

        rows_sum_raw: List[Dict[str, Any]] = []
        for line in svc_lines:
            ms = line.get("Mobile No")
            if not ms:
                continue

            rows_sum_raw.append({
                "Invoice No":          inv.get("invoice_number"),
                "Mobile No":           ms,
                "Upon Name":           None,
                "PhoneNorm":           _raw(f"dbo.fn_NormalizePhone({_sql_lit(ms)})"),
                "UponID":              None,
                "Description":         line.get("Description"),
                "Subscriber":          line.get("Subscriber"),
                "Current Bill Amount": line.get("Current Bill Amount"),
            })

        seen: set = set()
        rows_sum: List[Dict[str, Any]] = []
        for r in rows_sum_raw:
            key = (
                r.get("Invoice No"),
                r.get("Mobile No"),
                r.get("Description"),
                r.get("Subscriber"),
                r.get("Current Bill Amount"),
            )
            if key in seen:
                continue
            seen.add(key)
            rows_sum.append(r)

        stats["svc_summary"] = _insert_many(cur, T_DIGI["svc_summary"], rows_sum)

        _exec_safe(
            cur,
            "EXEC dbo.sp_MapUpon_Digi_ServiceSummary ?",
            [inv_no],
            tag="map_upon_digi",
        )

        svc_tax = raw_svc_summary.get("service_tax", {}) or {}
        others_6 = svc_tax.get("Others - 6 percent")
        access_6 = svc_tax.get("Access - 6 percent")
        total_tax = svc_tax.get("Total")

        if any(v is not None for v in (others_6, access_6, total_tax)):
            _merge_by_keys(
                cur,
                T_DIGI["svc_tax"],
                key_map={"Invoice No": inv.get("invoice_number")},
                payload={
                    "Invoice No":         inv.get("invoice_number"),
                    "Others - 6 percent": others_6,
                    "Access - 6 percent": access_6,
                    "Total":              total_tax,
                },
            )
            stats["svc_tax"] = 1
        else:
            stats["svc_tax"] = 0

        return stats
