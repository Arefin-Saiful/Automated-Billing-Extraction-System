# F:\telco_ingest\app\core\config.py
"""
Centralized settings/config for the Telco Ingest app.

Priority for DB connection string:
  1) SQLSERVER_CONN_STR        <-- NEW highest priority
  2) DB_ODBC_STR               <-- legacy/alt full string
  3) Build from parts (DB_* or SQLSERVER_* pieces)

Also adds:
  - TELCO_PERSIST_MODE: "TABLES" or "SP"
  - ENABLE_DB_WRITE: true/false
"""

from __future__ import annotations
import os
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=_PROJECT_ROOT / ".env")


def _getenv_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _getenv_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _csv(key: str, default: str = "") -> List[str]:
    raw = os.getenv(key, default)
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _ensure_dir(path_like: str) -> str:
    if not path_like:
        return ""
    p = Path(path_like)
    p.mkdir(parents=True, exist_ok=True)
    return str(p.resolve())


class Settings:
    # ---------------- App ----------------
    APP_ENV: str = os.getenv("APP_ENV", "dev")
    APP_HOST: str = os.getenv("APP_HOST", "127.0.0.1")
    APP_PORT: int = _getenv_int("APP_PORT", 8000)
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    UI_TITLE: str = os.getenv("UI_TITLE", "Telco Ingest")
    UI_FAVICON: str = os.getenv("UI_FAVICON", "")

    # ---------------- Paths ----------------
    UPLOAD_DIR: str = _ensure_dir(os.getenv("UPLOAD_DIR", r"F:\telco_ingest\uploads"))
    IMPORT_WATCH_DIR: str = _ensure_dir(os.getenv("IMPORT_WATCH_DIR", r"F:\telco_ingest\import_queue"))
    EXPORT_DIR: str = _ensure_dir(os.getenv("EXPORT_DIR", r"F:\telco_ingest\exports"))

    # ---------------- Upload/limits ----------------
    MAX_UPLOAD_MB: int = _getenv_int("MAX_UPLOAD_MB", 25)

    # ---------------- Security / Hashing ----------------
    HASH_SECRET: str = os.getenv("HASH_SECRET", "change-me")

    # ---------------- Vendor detection (tunable) ----------------
    ALLOWED_VENDORS: List[str] = _csv("ALLOWED_VENDORS", "maxis,celcom,digi")
    VENDOR_MIN_CONF: float = float(os.getenv("VENDOR_MIN_CONF", "0.50"))
    VENDOR_HINT_MAXIS: str = os.getenv("VENDOR_HINT_MAXIS", "Maxis|Business Postpaid")
    VENDOR_HINT_CELCOM: str = os.getenv("VENDOR_HINT_CELCOM", "Celcom|CELCOM|CelcomDigi")
    VENDOR_HINT_DIGI: str = os.getenv("VENDOR_HINT_DIGI", "Digi|CelcomDigi|CELCOMDIGI")

    # ---------------- CORS ----------------
    CORS_ORIGINS: List[str] = _csv("CORS_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000")
    CORS_ALLOW_CREDENTIALS: bool = _getenv_bool("CORS_ALLOW_CREDENTIALS", True)
    CORS_ALLOW_HEADERS: str = os.getenv("CORS_ALLOW_HEADERS", "*")

    # ---------------- Persistence knobs (NEW) ----------------
    TELCO_PERSIST_MODE: str = (os.getenv("TELCO_PERSIST_MODE", "SP").strip().upper() or "SP")
    ENABLE_DB_WRITE: bool = _getenv_bool("ENABLE_DB_WRITE", True)

    # ---------------- Database (new first, legacy fallback) ----------------
    # Full-string overrides (highest priority)
    SQLSERVER_CONN_STR: str = os.getenv("SQLSERVER_CONN_STR", "").strip()  # NEW top priority
    DB_ODBC_STR: str = os.getenv("DB_ODBC_STR", "").strip()                # legacy/alt

    # Granular values (used only when both full strings are empty)
    DB_DRIVER: str = os.getenv("DB_DRIVER") or os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
    DB_SERVER: str = os.getenv("DB_SERVER") or os.getenv("SQLSERVER_HOST", "localhost")
    DB_DATABASE: str = os.getenv("DB_DATABASE") or os.getenv("SQLSERVER_DB", "Telco Bills")
    DB_USERNAME: str = os.getenv("DB_USERNAME") or os.getenv("SQLSERVER_USER", "sa")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD") or os.getenv("SQLSERVER_PASSWORD", "")

    # TLS & timeouts
    DB_ENCRYPT: str = os.getenv("DB_ENCRYPT", "no")         # yes/no
    DB_TRUST_CERT: str = os.getenv("DB_TRUST_CERT", "yes")  # yes/no
    DB_TIMEOUT: int = _getenv_int("DB_TIMEOUT", 30)

    # Schema / objects
    DB_SCHEMA: str = os.getenv("DB_SCHEMA", "dbo")

    # Default upsert SP (kept for backward compatibility; used for Maxis by default)
    DB_SP_UPSERT_INVOICE: str = os.getenv("DB_SP_UPSERT_INVOICE", "sp_Upsert_InvoicePackage_JSON")

    # ✅ Vendor-specific SP names
    DB_SP_UPSERT_INVOICE_MAXIS: str = os.getenv(
        "DB_SP_UPSERT_INVOICE_MAXIS",
        DB_SP_UPSERT_INVOICE,
    )
    DB_SP_UPSERT_INVOICE_CELCOM: str = os.getenv(
        "DB_SP_UPSERT_INVOICE_CELCOM",
        "sp_Upsert_InvoicePackage_JSON_Celcom",
    )
    DB_SP_UPSERT_INVOICE_DIGI: str = os.getenv(
        "DB_SP_UPSERT_INVOICE_DIGI",
        "sp_Upsert_InvoicePackage_JSON_Digi",
    )

    # Generic table names (kept for compatibility; not used in TABLES mode)
    TABLE_INVOICES: str = os.getenv("TABLE_INVOICES", "dbo.Invoices")
    TABLE_NUMBERS: str = os.getenv("TABLE_NUMBERS", "dbo.InvoiceNumbers")
    TABLE_CHARGES: str = os.getenv("TABLE_CHARGES", "dbo.InvoiceCharges")
    TABLE_IMPORT_LOG: str = os.getenv("TABLE_IMPORT_LOG", "dbo.ImportLog")

    # ---------------- Legacy aliases (for older modules) ----------------
    @property
    def SQLSERVER_HOST(self) -> str:  # pragma: no cover
        return self.DB_SERVER

    @property
    def SQLSERVER_DB(self) -> str:  # pragma: no cover
        return self.DB_DATABASE

    @property
    def SQLSERVER_USER(self) -> str:  # pragma: no cover
        return self.DB_USERNAME

    @property
    def SQLSERVER_PASSWORD(self) -> str:  # pragma: no cover
        return self.DB_PASSWORD

    @property
    def SQLSERVER_DRIVER(self) -> str:  # pragma: no cover
        return self.DB_DRIVER

    # ---------------- Helpers ----------------
    @property
    def odbc_conn_str(self) -> str:
        """
        Return the final ODBC string for pyodbc, honoring overrides.
        Order: SQLSERVER_CONN_STR > DB_ODBC_STR > built from parts.
        """
        if self.SQLSERVER_CONN_STR:
            return self.SQLSERVER_CONN_STR
        if self.DB_ODBC_STR:
            return self.DB_ODBC_STR

        driver = f"{{{self.DB_DRIVER}}}"
        parts = [
            f"Driver={driver}",
            f"Server={self.DB_SERVER}",
            f"Database={self.DB_DATABASE}",
        ]
        if self.DB_USERNAME:
            parts += [f"UID={self.DB_USERNAME}", f"PWD={self.DB_PASSWORD}"]
        else:
            parts.append("Trusted_Connection=yes")

        if self.DB_ENCRYPT:
            parts.append(f"Encrypt={self.DB_ENCRYPT}")
        if self.DB_TRUST_CERT:
            parts.append(f"TrustServerCertificate={self.DB_TRUST_CERT}")

        return ";".join(parts) + ";"

    @property
    def pyodbc_kwargs(self) -> Dict[str, Any]:
        return {"timeout": self.DB_TIMEOUT}

    def debug_snapshot(self) -> Dict[str, Any]:
        def _mask(v: str) -> str:
            if not v:
                return ""
            return v[:2] + "***" + v[-2:] if len(v) > 4 else "***"
        return {
            "env": self.APP_ENV,
            "persist_mode": self.TELCO_PERSIST_MODE,
            "db_write": self.ENABLE_DB_WRITE,
            "db": {
                "using_sqlserver_conn_str": bool(self.SQLSERVER_CONN_STR != ""),
                "using_db_odbc_str": bool(self.DB_ODBC_STR != ""),
                "driver": self.DB_DRIVER,
                "server": self.DB_SERVER,
                "database": self.DB_DATABASE,
                "username": self.DB_USERNAME,
                "password_masked": _mask(self.DB_PASSWORD),
                "encrypt": self.DB_ENCRYPT,
                "trust_cert": self.DB_TRUST_CERT,
                "timeout": self.DB_TIMEOUT,
            }
        }


# --------------- Singleton ---------------
settings = Settings()

# --------------- Tiny helpers (for easy import) ---------------
def get_conn_str() -> str:
    return settings.odbc_conn_str

def get_schema() -> str:
    return settings.DB_SCHEMA

def get_persist_mode() -> str:
    # always normalized to upper
    return (settings.TELCO_PERSIST_MODE or "SP").upper()

def db_write_enabled() -> bool:
    return bool(settings.ENABLE_DB_WRITE)

def get_sp_name_for_vendor(vendor: str) -> str:
    """
    Return the correct stored procedure name for a given vendor.
    Normalizes vendor to lower-case.
    """
    v = (vendor or "").strip().lower()
    if v == "celcom":
        return settings.DB_SP_UPSERT_INVOICE_CELCOM
    if v == "digi":
        return settings.DB_SP_UPSERT_INVOICE_DIGI
    # default / maxis / unknown → legacy SP
    return settings.DB_SP_UPSERT_INVOICE_MAXIS


if __name__ == "__main__":
    snap = settings.debug_snapshot()
    print("CONFIG SNAPSHOT:", snap)
    print("ODBC:", settings.odbc_conn_str)
