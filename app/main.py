# F:\telco_ingest\app\main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from contextlib import suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Settings
from app.core.config import settings

# Routers
from app.routers import health, ingest, ui

APP_VERSION = "1.0.0"

# =========================================================
# 🚀 FastAPI Application Init
# =========================================================
app = FastAPI(title="Telco Ingest API", version=APP_VERSION)

# =========================================================
# 🚀 TEMPLATE ENGINE
# =========================================================
TEMPLATE_DIR = Path(__file__).parent / "ui" / "templates"
app.state.templates = Jinja2Templates(directory=str(TEMPLATE_DIR))

# =========================================================
# 🚀 STATIC FILES (CSS / JS ONLY)
# =========================================================
static_dir = Path(__file__).parent / "ui" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# =========================================================
# 🚀 CORS SETUP
# =========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS or ["*"],
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=[settings.CORS_ALLOW_HEADERS] if settings.CORS_ALLOW_HEADERS else ["*"],
)

# =========================================================
# 🚀 STARTUP: Ensure required directories exist
# =========================================================
@app.on_event("startup")
def _ensure_dirs():
    for d in (settings.UPLOAD_DIR, settings.IMPORT_WATCH_DIR, settings.EXPORT_DIR):
        with suppress(Exception):
            Path(d).mkdir(parents=True, exist_ok=True)

# =========================================================
# 🚀 ROUTERS
# =========================================================
app.include_router(ui.router_root)  # "/"
app.include_router(ui.router)       # "/ui/..."
app.include_router(ingest.router)   # "/ingest/..."
app.include_router(health.router)

# =========================================================
# 🚀 DEBUG ENDPOINTS
# =========================================================
@app.get("/version")
def version():
    return {
        "app": "telco-ingest",
        "version": APP_VERSION,
        "env": os.getenv("APP_ENV", "dev"),
        "db_server": settings.DB_SERVER,
        "db_database": settings.DB_DATABASE,
        "db_driver": settings.DB_DRIVER,
    }

@app.get("/debug/config")
def debug_config():
    """Show redacted ODBC connection string."""
    redacted = settings.odbc_conn_str.replace(
        f"PWD={settings.DB_PASSWORD}", "PWD=******"
    )
    return {
        "odbc": redacted,
        "schema": settings.DB_SCHEMA,
        "sp_upsert": settings.DB_SP_UPSERT_INVOICE,
    }

# =========================================================
# 🚀 LOCAL RUN ENTRYPOINT
# =========================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.APP_HOST or "127.0.0.1",
        port=int(os.getenv("PORT", settings.APP_PORT)),
        reload=True,
    )
