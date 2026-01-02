from fastapi import APIRouter, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse
from datetime import datetime
from pathlib import Path
import uuid

from app.reports.vendor_report_pdf import generate_vendor_pdf

# ---------------------------------------------------------
# ROOT ROUTER ( "/" )
# ---------------------------------------------------------
router_root = APIRouter()


@router_root.get("/", response_class=HTMLResponse, name="ui_index")
async def ui_home(request: Request):
    """Home page / Dashboard"""
    return request.app.state.templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


# ---------------------------------------------------------
# UI ROUTER ( "/ui/... )
# ---------------------------------------------------------
router = APIRouter(prefix="/ui")


# ---------------------- UPLOAD PAGE -----------------------
@router.get("/upload", response_class=HTMLResponse, name="ui_upload")
async def ui_upload_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "upload.html",
        {"request": request}
    )


# ---------------------- INGEST JOBS PAGE ------------------
@router.get("/ingests", response_class=HTMLResponse, name="ui_ingests")
async def ui_ingests_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "reports.html",
        {"request": request}
    )


# ---------------------- REPORT PAGE ------------------------
@router.get("/reports", response_class=HTMLResponse, name="ui_reports")
async def ui_reports_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "reports.html",
        {"request": request}
    )


# ------------------ REPORT GENERATION POST -----------------
@router.post("/reports/generate", name="ui_reports_generate")
async def ui_reports_generate(
    request: Request,
    background_tasks: BackgroundTasks,
    vendor: str = Form(...),
    from_date: str = Form(...),
    to_date: str = Form(...),
):
    """
    Generate vendor PDF report and download immediately.
    File is deleted automatically after response is sent.
    """

    # Parse dates
    fdate = datetime.strptime(from_date, "%Y-%m-%d").date()
    tdate = datetime.strptime(to_date, "%Y-%m-%d").date()

    # Windows-safe absolute path
    OUTPUT_DIR = Path("F:/telco_ingest/output_reports")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Temporary unique filename
    temp_filename = f"{vendor.upper()}-{uuid.uuid4()}.pdf"
    pdf_path = OUTPUT_DIR / temp_filename

    # Generate PDF (WeasyPrint)
    generate_vendor_pdf(vendor, fdate, tdate, str(pdf_path))

    # Schedule automatic deletion after response
    background_tasks.add_task(pdf_path.unlink)

    # Stream PDF to browser
    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename=f"{vendor.upper()}_{fdate}_{tdate}.pdf"
    )
