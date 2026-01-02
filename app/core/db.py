# F:\telco_ingest\app\core\db.py
# -*- coding: utf-8 -*-
"""
Database helpers for SQL Server via pyodbc.

Enhancements in this version:
- Stronger fallback logic for missing stored procedures
- More defensive vendor resolution
- Better diagnostics in SP introspection
- Better Unicode handling for JSON (SQL NVARCHAR compatibility)
- Clean connection lifecycle
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Generator, Optional, Any, List, Tuple

import pyodbc

from .config import settings, get_sp_name_for_vendor


# -------------------------------------------------------------------
# CONNECTION HELPERS
# -------------------------------------------------------------------

def get_conn(*, autocommit: bool = True) -> pyodbc.Connection:
    """
    Standard SQL Server connection factory.
    """
    return pyodbc.connect(
        settings.odbc_conn_str,
        autocommit=autocommit,
        **settings.pyodbc_kwargs,
    )


def get_connection(*, autocommit: bool = True) -> pyodbc.Connection:
    """
    Compatibility alias (older modules expect get_connection).
    """
    return get_conn(autocommit=autocommit)


@contextmanager
def db_cursor(*, autocommit: bool = True) -> Generator[pyodbc.Cursor, None, None]:
    """
    Context manager for a cursor that always cleans up.
    """
    conn = get_conn(autocommit=autocommit)
    try:
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
    finally:
        conn.close()


# -------------------------------------------------------------------
# HEALTH CHECKS
# -------------------------------------------------------------------

def health_check() -> bool:
    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False


def current_db() -> str:
    with db_cursor() as cur:
        cur.execute("SELECT DB_NAME()")
        return cur.fetchone()[0]


# -------------------------------------------------------------------
# PROCEDURE PARAMETER DISCOVERY
# -------------------------------------------------------------------

def _get_proc_params(cur: pyodbc.Cursor, schema: str, proc: str) -> List[Tuple[str, int]]:
    """
    Retrieve ordered parameter list from sys.parameters.
    """
    try:
        cur.execute(
            """
            SELECT p.name, p.parameter_id
            FROM sys.parameters AS p
            WHERE p.object_id = OBJECT_ID(?)
            ORDER BY p.parameter_id
            """,
            f"{schema}.{proc}"
        )
        rows = cur.fetchall()
        return [(str(r[0]), int(r[1])) for r in rows]
    except Exception as e:
        # Returning empty list triggers fallback logic
        return []


def _classify_param(pname: str) -> str:
    """
    Attempt to determine the purpose of a parameter.
    """
    n = pname.lstrip("@").lower().replace("_", "")
    if any(tok in n for tok in ("packagejson", "payload", "json", "data", "body")):
        return "payload"
    if any(tok in n for tok in ("sourcefile", "source", "filepath", "file", "src")):
        return "source"
    return "other"


# -------------------------------------------------------------------
# UPSERT ENGINE
# -------------------------------------------------------------------

def call_upsert_invoice_json(package: dict, source_file: str | None = None) -> None:
    """
    Vendor-based stored procedure dispatch for invoice ingestion.
    Automatically binds parameters by inspecting sys.parameters.
    """

    # Ensure JSON is safe for NVARCHAR(MAX)
    payload_json = json.dumps(package, ensure_ascii=False)
    source = source_file or package.get("__meta__", {}).get("file_name", "API")

    # Vendor resolution
    invoice = package.get("invoice") or {}
    vendor = str(invoice.get("vendor", "")).strip().lower()

    if not vendor:
        raise ValueError("Missing vendor in package.invoice.vendor")

    schema = settings.DB_SCHEMA
    proc = get_sp_name_for_vendor(vendor)

    if not proc:
        raise ValueError(f"No stored procedure mapped for vendor '{vendor}'")

    with db_cursor(autocommit=True) as cur:
        params = _get_proc_params(cur, schema, proc)

        # -----------------------------------------------------------
        # CASE: no parameters discovered → assume (payload, source)
        # -----------------------------------------------------------
        if not params:
            sql = f"EXEC [{schema}].[{proc}] @PackageJson=?, @SourceFile=?"
            cur.execute(sql, (payload_json, source))
            return

        roles = [(_classify_param(nm), nm) for nm, _ in params]
        payload_param = next((nm for role, nm in roles if role == "payload"), None)
        source_param = next((nm for role, nm in roles if role == "source"), None)

        # -----------------------------------------------------------
        # CASE: Only one parameter → must be payload
        # -----------------------------------------------------------
        if len(params) == 1:
            sql = f"EXEC [{schema}].[{proc}] ?"
            cur.execute(sql, (payload_json,))
            return

        # -----------------------------------------------------------
        # CASE: Payload + Source detected by naming
        # -----------------------------------------------------------
        if payload_param and source_param:
            sql = f"EXEC [{schema}].[{proc}] {payload_param}=?, {source_param}=?"
            cur.execute(sql, (payload_json, source))
            return

        # -----------------------------------------------------------
        # CASE: Only payload detected → send NULL to others
        # -----------------------------------------------------------
        if payload_param and not source_param:
            other = [nm for nm, _ in params if nm != payload_param]
            placeholders = [f"{payload_param}=?"] + [f"{nm}=NULL" for nm in other]
            sql = f"EXEC [{schema}].[{proc}] " + ", ".join(placeholders)
            cur.execute(sql, (payload_json,))
            return

        # -----------------------------------------------------------
        # CASE: Neither detected → positional best-guess
        # -----------------------------------------------------------
        if len(params) >= 2:
            sql = f"EXEC [{schema}].[{proc}] ?, ?"
            cur.execute(sql, (payload_json, source))
            return

        # -----------------------------------------------------------
        # LAST fallback
        # -----------------------------------------------------------
        sql = f"EXEC [{schema}].[{proc}] ?"
        cur.execute(sql, (payload_json,))


# -------------------------------------------------------------------
# RAW SQL EXECUTION
# -------------------------------------------------------------------

def execute_sql(sql: str, params: Optional[tuple[Any, ...]] = None) -> list[tuple]:
    """
    Execute SELECT queries with optional params.
    """
    with db_cursor() as cur:
        cur.execute(sql, params or ())
        try:
            return cur.fetchall()
        except pyodbc.ProgrammingError:
            return []
