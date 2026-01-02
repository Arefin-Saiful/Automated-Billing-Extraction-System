# F:\telco_ingest\app\services\ingest_service.py
# -*- coding: utf-8 -*-

"""
UPDATED VERSION — supports Celcom Digi 2025 bill format
- Correct Celcom import (extract_celcom_bill)
- Registry updated
- Validation now allows charges OR charges_summary
"""

from __future__ import annotations

import fnmatch
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---- Optional DB dependency
try:
    import pyodbc
except Exception:
    pyodbc = None

from fastapi import UploadFile

# Centralized settings
try:
    from app.core.config import (
        settings,
        get_conn_str,
        get_persist_mode,
        db_write_enabled,
    )
except Exception:
    settings = None

    def get_conn_str():
        return ""

    def get_persist_mode():
        return "PROC"

    def db_write_enabled():
        return False

# DB helpers
try:
    from app.core.db import call_upsert_invoice_json, health_check
except Exception:

    def call_upsert_invoice_json(package: dict, source_file=None):
        raise RuntimeError("Stored proc not available")

    def health_check():
        return False

# Utils
from app.utils.vendor_detect import VendorDetector
from app.utils.hashing import sha256_bytes

# TABLE loader
try:
    from app.services.telco_loader import TelcoLoader
except Exception:
    TelcoLoader = None


# ---------------------- FIXED IMPORTS ---------------------- #
try:
    from parsers.maxis_extractor import extract as maxis_extract
except Exception:
    maxis_extract = None

try:
    from parsers.celcom_extractor import extract_celcom_bill as celcom_extract
except Exception:
    celcom_extract = None

try:
    from parsers.digi_extractor import extract as digi_extract
except Exception:
    digi_extract = None
# ----------------------------------------------------------- #


# --------------------- Parser Registry ---------------------- #
class _ParserRegistry:
    """Maps vendor slugs → callable(path) returning standardized package."""
    def __init__(self) -> None:
        self._reg: Dict[str, Any] = {}

        if maxis_extract:
            self._reg["maxis"] = maxis_extract

        if celcom_extract:
            self._reg["celcom"] = celcom_extract  # FIXED

        if digi_extract:
            self._reg["digi"] = digi_extract

    def has(self, vendor: str) -> bool:
        return vendor.lower() in self._reg

    def parse(self, vendor: str, pdf_path: str) -> Dict[str, Any]:
        key = vendor.lower()
        if key not in self._reg:
            raise ValueError(f"No parser registered for vendor '{vendor}'.")
        return self._reg[key](pdf_path)

    def list_vendors(self) -> List[str]:
        return sorted(self._reg.keys())


# ------------------------ Ingest Service ---------------------- #
class IngestService:
    def __init__(self, conn_str_env: str = "SQLSERVER_CONN_STR") -> None:
        self.detector = VendorDetector()
        self.parsers = _ParserRegistry()

        # Connection String
        self.conn_str = (get_conn_str() or "").strip()
        if not self.conn_str:
            self.conn_str = (
                os.getenv("DB_ODBC_STR")
                or os.getenv(conn_str_env)
                or os.getenv("SQLSERVER_CONN_STR")
                or ""
            ).strip()

        # Mode
        self.persist_mode = (get_persist_mode() or "PROC").strip().upper()
        self.enable_db_write = bool(db_write_enabled())

        # Table Loader (optional)
        self._table_loader: Optional[Any] = None
        if self.conn_str and TelcoLoader is not None and pyodbc:
            try:
                self._table_loader = TelcoLoader(self.conn_str)
            except Exception:
                self._table_loader = None

    # Status
    def db_ready(self) -> bool:
        if not self.conn_str or not self.enable_db_write:
            return False
        try:
            return bool(health_check())
        except Exception:
            return False

    def supported_vendors(self) -> List[str]:
        return self.parsers.list_vendors()

    # ------------------------ Persistence ----------------------- #
    def _persist_package(self, package: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        if not self.enable_db_write:
            return False, {"error": "DB write disabled"}

        valid, err = self.validate_package_shape(package)
        if not valid:
            return False, {"error": err}

        mode = (self.persist_mode or "PROC").strip().upper()

        # TABLES mode
        if mode == "TABLES":
            if not self._table_loader:
                return False, {"error": "TABLES mode unavailable"}
            try:
                resp = self._table_loader.save(package)
                return True, resp if isinstance(resp, dict) else {"result": str(resp)}
            except Exception as e:
                return False, {"error": f"TABLES write failed: {e}"}

        # PROC mode
        try:
            call_upsert_invoice_json(package)
            return True, {"result": "OK"}
        except Exception as e:
            return False, {"error": f"Stored proc failed: {e}"}

    # ------------------------- Processing -------------------------- #
    def process_path(self, path: str, persist=False) -> Dict[str, Any]:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(path)

        vendor, confidence, hints = self.detector.detect_from_path(str(p))
        if not vendor:
            raise ValueError("Unable to detect vendor")

        package = self.parsers.parse(vendor, str(p))

        provenance = {
            "source_path": str(p.resolve()),
            "source_sha256": sha256_bytes(p.read_bytes()),
            "vendor_detected": vendor,
            "vendor_confidence": confidence,
            "detector_hints": hints,
        }

        result = {
            "ok": True,
            "vendor": vendor,
            "confidence": confidence,
            "package": package,
            "provenance": provenance,
            "persisted": False,
            "db_ready": self.db_ready(),
            "persist_mode": self.persist_mode,
            "db_write_enabled": self.enable_db_write,
        }

        if persist:
            ok, resp = self._persist_package(package)
            result["persisted"] = ok
            if ok:
                result["db_response"] = resp
            else:
                result["persist_error"] = resp["error"]

        return result

    # ------------------- Validation (UPDATED) ---------------------- #
    @staticmethod
    def validate_package_shape(package: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Updated to support:
        - charges (Maxis/Digi)
        - OR charges_summary (Celcom)
        """

        if not isinstance(package, dict):
            return False, "Package must be a dict"

        if "invoice" not in package:
            return False, "Missing invoice"

        if "numbers" not in package:
            return False, "Missing numbers"

        # Accept either charges OR charges_summary
        if ("charges" not in package) and ("charges_summary" not in package):
            return False, "Missing charges or charges_summary"

        inv = package["invoice"]
        required_inv = ["vendor", "invoice_number", "bill_date", "currency"]

        for rk in required_inv:
            if inv.get(rk) in (None, "", []):
                return False, f"invoice.{rk} is required"

        if not isinstance(package["numbers"], list):
            return False, "numbers must be a list"

        return True, None
