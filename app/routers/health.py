# F:\telco_ingest\app\routers\health.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import platform
from datetime import datetime, timezone
from typing import Dict, Any, List

from fastapi import APIRouter, HTTPException

# Safe pyodbc import → prevents API crash if driver not installed
try:
    import pyodbc
except Exception:
    pyodbc = None

# Central config (consistent with ingest_service + upload_json)
from app.core.config import settings, get_conn_str

router = APIRouter(prefix="/health", tags=["health"])

_PROCESS_START = time.time()


# ---------------------------------------------------------------------------
# Basic metadata helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uptime_seconds() -> float:
    return round(time.time() - _PROCESS_START, 3)


def _runtime_meta() -> Dict[str, Any]:
    """Runtime metadata without secrets."""
    return {
        "now_utc": _utc_now_iso(),
        "uptime_seconds": _uptime_seconds(),
        "python": {
            "version": sys.version.split()[0],
            "implementation": platform.python_implementation(),
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "process": {"pid": os.getpid()},
        "app": {
            "name": os.getenv("APP_NAME", "telco_ingest"),
            "version": os.getenv("APP_VERSION", "0.1.0"),
            "env": os.getenv("APP_ENV", "dev"),
        },
    }


# ---------------------------------------------------------------------------
# Health Endpoints
# ---------------------------------------------------------------------------

@router.get("", summary="Basic health status", name="health:root")
@router.get("/", include_in_schema=False)
def health_root() -> Dict[str, Any]:
    return {"status": "ok", **_runtime_meta()}


@router.get("/ping", summary="Quick liveness ping")
def ping() -> Dict[str, str]:
    return {"pong": "ok"}


@router.get("/live", summary="Detailed liveness probe")
def live() -> Dict[str, Any]:
    return {"status": "live", **_runtime_meta()}


@router.get("/ready", summary="Readiness probe")
def ready() -> Dict[str, Any]:
    return {"status": "ready", **_runtime_meta()}


# ---------------------------------------------------------------------------
# ODBC Redaction
# ---------------------------------------------------------------------------

def _redact_odbc(conn_str: str) -> str:
    """
    Mask sensitive parts of an ODBC connection string:
    - PWD / Password
    - UID / User ID
    """
    if not conn_str:
        return "<empty>"

    parts: List[str] = []
    for seg in conn_str.split(";"):
        kv = seg.split("=", 1)
        if len(kv) != 2:
            if seg.strip():
                parts.append(seg)
            continue

        key_raw, val = kv
        key = key_raw.strip().lower()

        if key in {"pwd", "password"}:
            val = "******"
        elif key in {"uid", "user id"}:
            val = "<redacted>"

        parts.append(f"{key_raw}={val}")

    return ";".join(parts) + (";" if conn_str.endswith(";") else "")


# ---------------------------------------------------------------------------
# Database Health Check
# ---------------------------------------------------------------------------

@router.get("/db", summary="Database connectivity check")
def db_health() -> Dict[str, Any]:

    # Ensure pyodbc is available (avoid server crash)
    if pyodbc is None:
        raise HTTPException(
            status_code=500,
            detail="pyodbc not installed or SQL Server ODBC driver missing"
        )

    conn_str = get_conn_str()
    if not conn_str:
        raise HTTPException(
            status_code=500,
            detail="No SQL Server connection string configured"
        )

    redacted = _redact_odbc(conn_str)

    try:
        with pyodbc.connect(
            conn_str,
            autocommit=True,
            **settings.pyodbc_kwargs,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DB_NAME()")
                dbname = cur.fetchone()[0]

        return {
            "ok": True,
            "database": dbname,
            "server": settings.DB_SERVER,
            "driver": settings.DB_DRIVER,
            "timeout": settings.DB_TIMEOUT,
            "user": "<redacted>",
            "odbc": redacted,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"DB connect failed: {e}"
        )
