# F:\telco_ingest\ingest\import_folder.py
# Updated: vendor normalization + Celcom structure fix + strong validation

from __future__ import annotations
import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional

# Helpers
from app.utils.vendor_detect import detect_vendor
from app.utils.hashing import file_sha256
from app.core.config import db_write_enabled, get_persist_mode

# Safe upload loader
try:
    from app.ingest.upload_json import upload_invoice_package
except ImportError:
    def upload_invoice_package(package: Dict[str, Any], source_file: str = "") -> Dict[str, Any]:
        raise RuntimeError(
            "upload_invoice_package() missing. Implement it OR run without --upsert."
        )

# -----------------------------
# Vendor extractors (FIXED)
# -----------------------------
from parsers import maxis_extractor, celcom_extractor, digi_extractor

_EXTRACTORS = {
    "maxis": maxis_extractor.extract,
    "celcom": celcom_extractor.extract,     # FIXED
    "digi": digi_extractor.extract,
}

_TRUE_SET = {"1", "true", "yes", "y", "on"}


# -----------------------------
# Dataclass for result response
# -----------------------------
@dataclass
class FileResult:
    path: str
    vendor: str
    confidence: float
    sha256: str
    ok: bool
    error: Optional[str] = None
    upserted: bool = False
    db_result: Optional[Dict[str, Any]] = None


# -----------------------------
# PDF iterator
# -----------------------------
def _iter_pdfs(root: Path, pattern: str, recurse: bool):
    if recurse:
        yield from (p for p in root.rglob(pattern) if p.is_file())
    else:
        yield from (p for p in root.glob(pattern) if p.is_file())


# -----------------------------
# Vendor extractor wrapper
# -----------------------------
def _extract_with_vendor(vendor: str, pdf_path: str):
    v = vendor.lower()
    if v not in _EXTRACTORS:
        raise RuntimeError(f"Unsupported vendor '{vendor}'")
    return _EXTRACTORS[v](pdf_path)


# -----------------------------
# DB write flag
# -----------------------------
def _write_enabled() -> bool:
    try:
        return db_write_enabled()
    except Exception:
        return os.getenv("ENABLE_DB_WRITE", "true").lower() in _TRUE_SET


# -----------------------------
# Persist mode
# -----------------------------
def _persist_mode() -> str:
    try:
        return get_persist_mode()
    except Exception:
        return (os.getenv("TELCO_PERSIST_MODE", "PROC") or "PROC").upper()


# -----------------------------
# MAIN IMPORT FUNCTION
# -----------------------------
def import_folder(folder: str, pattern="*.pdf", recurse=True, upsert=False):

    root = Path(folder).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Folder not found: {root}")

    results = []
    total = ok_count = upsert_count = 0

    for pdf in _iter_pdfs(root, pattern, recurse):
        total += 1

        try:
            sha = file_sha256(pdf)

            # -------------------------
            # Vendor detection
            # -------------------------
            det = detect_vendor(str(pdf))
            info = det if isinstance(det, dict) else {"vendor": det, "confidence": 1.0}

            vendor = (info.get("vendor") or "unknown").lower()
            conf = float(info.get("confidence") or 0.0)

            if vendor == "unknown":
                results.append(FileResult(str(pdf), vendor, conf, sha, False,
                                          "Vendor undetected"))
                continue

            # -------------------------
            # Extract JSON
            # -------------------------
            package = _extract_with_vendor(vendor, str(pdf))

            # -------------------------
            # STRUCTURE FIX (Celcom)
            # -------------------------
            # new celcom parser returns: { "bills": [ {...} ] }
            if vendor == "celcom" and isinstance(package, dict) and "bills" in package:
                if isinstance(package["bills"], list) and len(package["bills"]) > 0:
                    package = package["bills"][0]

            # Validation
            if not isinstance(package, dict):
                raise RuntimeError(f"Parser returned invalid structure for {pdf.name}")

            # -------------------------
            # Metadata
            # -------------------------
            package.setdefault("__meta__", {})
            package["__meta__"].update({
                "source_file": pdf.name,
                "source_path": str(pdf),
                "sha256": sha,
                "vendor_detect": info,
            })

            # -------------------------
            # DB Upsert
            # -------------------------
            did_upsert = False
            db_res = None

            if upsert and _write_enabled():
                db_res = upload_invoice_package(package, source_file=str(pdf))
                did_upsert = bool(db_res and db_res.get("ok"))
                if did_upsert:
                    upsert_count += 1

            results.append(FileResult(
                str(pdf), vendor, conf, sha, True, None, did_upsert, db_res
            ))
            ok_count += 1

        except Exception as e:
            results.append(FileResult(
                str(pdf), vendor, conf, sha, False, str(e)
            ))

    return {
        "folder": str(root),
        "pattern": pattern,
        "recurse": recurse,
        "upsert": upsert,
        "total": total,
        "parsed_ok": ok_count,
        "upserted": upsert_count,
        "files": [asdict(r) for r in results],
        "persist_mode": _persist_mode(),
        "write_enabled": _write_enabled(),
    }


# -----------------------------
# CLI wrapper
# -----------------------------
def run_import(folder, pattern="*.pdf", recurse=True, upsert=False):
    return import_folder(folder, pattern, recurse, upsert)


# -----------------------------
# Standalone execution
# -----------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--pattern", default="*.pdf")
    ap.add_argument("--no-recurse", action="store_true")
    ap.add_argument("--upsert", action="store_true")
    args = ap.parse_args()

    summary = import_folder(args.input, args.pattern,
                            not args.no_recurse, args.upsert)

    print(json.dumps(summary, indent=2))
    print(
        f"\nIngest complete. Success: {summary['parsed_ok']}/{summary['total']} | Upserted: {summary['upserted']}"
    )
