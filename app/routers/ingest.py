# F:\telco_ingest\app\routers\ingest.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse

# --- App config helpers
from app.core.config import db_write_enabled

# --- Utilities
from app.utils.vendor_detect import detect_vendor
from app.utils.hashing import file_sha256

# --- Folder import & DB helpers
from ingest.import_folder import run_import
from ingest.upload_json import upload_invoice_package

# --- Vendor extractors
from parsers import maxis_extractor, digi_extractor
from parsers.celcom_extractor import extract as celcom_extract

router = APIRouter(prefix="/ingest", tags=["ingest"])

_EXTRACTORS: Dict[str, Any] = {
    "maxis": maxis_extractor.extract,
    "celcom": celcom_extract,
    "digi": digi_extractor.extract,
}

# -------------------------------------------------------------------

def _ensure_pdf(file: UploadFile) -> None:
    fname = (file.filename or "").lower()
    ctype = (file.content_type or "").lower()
    if not fname.endswith(".pdf") and "pdf" not in ctype:
        raise HTTPException(status_code=415, detail="Only PDF files are accepted.")

def _save_temp_pdf(file: UploadFile) -> Path:
    safe_name = (file.filename or "upload.pdf").replace(" ", "_")
    tmp_dir = Path(tempfile.mkdtemp(prefix="telco_ingest_"))
    tmp_path = tmp_dir / safe_name
    with tmp_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return tmp_path

def _normalize_vendor(det: Any) -> str:
    if isinstance(det, str):
        v = det.strip().lower()
        if v:
            return v
    elif isinstance(det, dict):
        for k in ("vendor", "label", "name", "provider"):
            val = det.get(k)
            if isinstance(val, str) and val.strip():
                return val.strip().lower()
    raise HTTPException(status_code=422, detail=f"Could not normalize vendor: {det!r}")

def _detect_and_extract(pdf_path: Path) -> Dict[str, Any]:
    det = detect_vendor(str(pdf_path))
    vendor = _normalize_vendor(det)

    if vendor not in _EXTRACTORS:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported vendor '{vendor}' for file: {pdf_path.name}"
        )

    extractor = _EXTRACTORS[vendor]
    try:
        package = extractor(str(pdf_path))
    except Exception as e:
        # keep the original exception message (helps debugging)
        raise HTTPException(status_code=500, detail=f"Failed to parse {vendor} PDF: {e}")

    # Normalize package shape
    package.setdefault("invoice", {}).setdefault("vendor", vendor)
    meta = package.setdefault("__meta__", {})
    meta.setdefault("file_name", pdf_path.name)
    meta.setdefault("vendor", vendor)
    meta.setdefault("detector_result", det)

    return package

def _maybe_persist(package: Dict[str, Any], persist_flag: bool) -> Optional[Dict[str, Any]]:
    if not persist_flag:
        return None

    try:
        return upload_invoice_package(package)
    except Exception as e:
        # IMPORTANT: return repr(e) so you can see driver args & types
        return {"error": repr(e), "args": getattr(e, "args", None)}

def _cleanup(path: Path) -> None:
    try:
        if path.exists():
            shutil.rmtree(path.parent, ignore_errors=True)
    except Exception:
        pass

# -------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------

@router.post("/file", summary="Ingest a single PDF (auto-detect vendor)")
async def ingest_file(
    background: BackgroundTasks,
    file: UploadFile = File(...),
    persist: Optional[bool] = Form(None),
) -> JSONResponse:
    _ensure_pdf(file)
    tmp_path = _save_temp_pdf(file)
    sha256 = file_sha256(str(tmp_path))

    persist_flag = db_write_enabled() if persist is None else bool(persist)

    try:
        package = _detect_and_extract(tmp_path)
        package.setdefault("__meta__", {})["sha256"] = sha256

        db_result = _maybe_persist(package, persist_flag)

        # ✅ If persist=true but DB returns an error, reflect that in response
        ok = True
        if persist_flag and isinstance(db_result, dict) and db_result.get("error"):
            ok = False

        return JSONResponse(
            status_code=200 if ok else 500,
            content={
                "status": "ok" if ok else "error",
                "persisted": bool(persist_flag),
                "db_result": db_result,
                "package": package,
            },
        )

    finally:
        background.add_task(_cleanup, tmp_path)

@router.post("/batch", summary="Ingest multiple PDFs")
async def ingest_batch(
    background: BackgroundTasks,
    files: List[UploadFile] = File(...),
    persist: Optional[bool] = Form(None),
) -> JSONResponse:
    results: List[Dict[str, Any]] = []
    persist_flag = db_write_enabled() if persist is None else bool(persist)
    tmp_paths: List[Path] = []

    for f in files:
        _ensure_pdf(f)
        tmp_paths.append(_save_temp_pdf(f))

    try:
        for p in tmp_paths:
            sha256 = file_sha256(str(p))
            try:
                package = _detect_and_extract(p)
                package.setdefault("__meta__", {})["sha256"] = sha256
                db_result = _maybe_persist(package, persist_flag)

                ok = True
                if persist_flag and isinstance(db_result, dict) and db_result.get("error"):
                    ok = False

                results.append({
                    "file": p.name,
                    "status": "ok" if ok else "error",
                    "persisted": bool(persist_flag),
                    "db_result": db_result,
                    "package": package,
                })

            except Exception as e:
                results.append({"file": p.name, "status": "error", "detail": str(e)})

        # If any failed, return 500 to help UI detect problem
        any_failed = any(r.get("status") == "error" for r in results)
        return JSONResponse(
            status_code=200 if not any_failed else 500,
            content={"count": len(results), "results": results},
        )

    finally:
        for p in tmp_paths:
            background.add_task(_cleanup, p)

@router.get("/detect")
def detect(path: str = Query(...)) -> Dict[str, Any]:
    pdf_path = Path(path)
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    if pdf_path.suffix.lower() != ".pdf":
        raise HTTPException(status_code=415, detail="Only PDF files are accepted.")

    det = detect_vendor(str(pdf_path))
    vendor = _normalize_vendor(det)
    return {"file": pdf_path.name, "vendor": vendor, "detector_result": det}

@router.post("/folder")
def ingest_folder(
    folder: str = Form(...),
    persist: Optional[bool] = Form(None),
    pattern: Optional[str] = Form(None),
):
    folder_path = Path(folder)
    if not folder_path.exists():
        raise HTTPException(status_code=404, detail=f"Folder not found: {folder}")

    persist_flag = db_write_enabled() if persist is None else bool(persist)

    try:
        results = run_import(str(folder_path), persist=persist_flag, pattern=pattern)
        return {"status": "ok", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Folder import failed: {e}")

@router.post("/upsert")
def upsert_package(package: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(package, dict) or "invoice" not in package:
        raise HTTPException(status_code=422, detail="Invalid package payload.")
    try:
        result = upload_invoice_package(package)
        ok = not (isinstance(result, dict) and result.get("error"))
        return JSONResponse(
            status_code=200 if ok else 500,
            content={"status": "ok" if ok else "error", "db_result": result},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB upsert failed: {e}")
