# F:\telco_ingest\app\services\parse_service.py
# -*- coding: utf-8 -*-
"""
ParseService
------------
Lightweight service focused on:
- Vendor detection (by path or bytes)
- Parsing via registered vendor adapters (extract -> standardized package)
- Zero DB side-effects (no persistence)
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import UploadFile

from app.utils.vendor_detect import VendorDetector
from app.utils.hashing import sha256_bytes

# ---------------------------------------------------------------
# Vendor adapters (SOFT IMPORTS)
# ---------------------------------------------------------------
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


# ---------------------------------------------------------------
# Parser Registry
# ---------------------------------------------------------------
class _ParserRegistry:
    """Maps vendor slugs → callable(path) returning a standardized package."""
    def __init__(self) -> None:
        self._reg: Dict[str, Any] = {}
        if maxis_extract:
            self._reg["maxis"] = maxis_extract
        if celcom_extract:
            self._reg["celcom"] = celcom_extract
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


# ---------------------------------------------------------------
# ParseService
# ---------------------------------------------------------------
class ParseService:
    """
    High-level API:
      - supported_vendors()
      - detect_for_path(path)
      - parse_path(path)
      - parse_upload(file)
      - parse_bytes(data, filename)
    """
    def __init__(self) -> None:
        self.detector = VendorDetector()
        self.parsers = _ParserRegistry()

    # ---------------------- Public Helpers ----------------------
    def supported_vendors(self) -> List[str]:
        return self.parsers.list_vendors()

    def detect_for_path(self, path: str) -> Dict[str, Any]:
        p = Path(path)
        if not p.exists() or not p.is_file():
            return {
                "ok": False,
                "path": str(p),
                "vendor": None,
                "confidence": 0.0,
                "reason": "File not found",
            }

        vendor, conf, hints = self.detector.detect_from_path(str(p))
        return {
            "ok": vendor is not None,
            "path": str(p),
            "vendor": vendor,
            "confidence": conf,
            "hints": hints,
        }

    # ------------------------- Core Parse ------------------------
    def parse_path(self, path: str) -> Dict[str, Any]:
        p = Path(path)
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"File not found: {path}")

        vendor, confidence, hints = self.detector.detect_from_path(str(p))
        if not vendor:
            raise ValueError("Unable to detect vendor for this PDF.")

        package = self.parsers.parse(vendor, str(p))

        return {
            "ok": True,
            "vendor": vendor,
            "confidence": confidence,
            "package": package,
            "provenance": {
                "source_path": str(p.resolve()),
                "source_sha256": sha256_bytes(p.read_bytes()),
                "vendor_detected": vendor,
                "vendor_confidence": confidence,
                "detector_hints": hints,
            },
        }

    def parse_upload(self, file: UploadFile) -> Dict[str, Any]:
        data = file.file.read()
        return self.parse_bytes(data, filename=file.filename or "upload.pdf")

    def parse_bytes(self, data: bytes, filename: Optional[str] = None) -> Dict[str, Any]:
        if not data:
            raise ValueError("Empty payload.")

        vendor, confidence, hints = self.detector.detect_from_bytes(
            data, filename=filename or "upload.pdf"
        )

        if not vendor:
            raise ValueError("Unable to detect vendor for this PDF.")

        # Parsers require a filesystem path
        with tempfile.NamedTemporaryFile(prefix="telco_", suffix=".pdf", delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name

        try:
            package = self.parsers.parse(vendor, tmp_path)
            return {
                "ok": True,
                "vendor": vendor,
                "confidence": confidence,
                "package": package,
                "provenance": {
                    "upload_filename": filename,
                    "temp_path": tmp_path,
                    "source_sha256": sha256_bytes(data),
                    "vendor_detected": vendor,
                    "vendor_confidence": confidence,
                    "detector_hints": hints,
                },
            }
        finally:
            with suppress(Exception):
                os.unlink(tmp_path)

    # ----------------------- Validation --------------------------
    @staticmethod
    def validate_package(package: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validation compatible with ALL vendors (Celcom / Maxis / Digi)."""

        if not isinstance(package, dict):
            return False, "Package must be a dict."

        # invoice + numbers must exist
        if "invoice" not in package:
            return False, "Missing 'invoice' in package."
        if "numbers" not in package:
            return False, "Missing 'numbers' in package."

        # charges OR charges_summary (Celcom)
        if ("charges" not in package) and ("charges_summary" not in package):
            return False, "Missing 'charges' or 'charges_summary' in package."

        # invoice fields
        inv = package.get("invoice", {})
        required = ("vendor", "invoice_number", "bill_date", "currency")
        missing = [k for k in required if not inv.get(k)]
        if missing:
            return False, f"Missing invoice fields: {', '.join(missing)}"

        # types
        if not isinstance(package.get("numbers"), list):
            return False, "'numbers' must be a list."

        return True, None
