# Telco Bills Ingest (Maxis · Celcom · Digi)

A small, production-ready pipeline for parsing Malaysian telco bills (PDF) into a **standardized invoice package** and upserting it into **SQL Server**.

- ✅ Preserves each vendor’s original parsing logic
- ✅ Normalizes to one JSON envelope: `{ invoice, numbers, charges, raw }`
- ✅ Windows-friendly paths (e.g., `F:\telco_ingest\…`)
- ✅ FastAPI UI + REST API
- ✅ CLI utilities to batch-import folders / upload JSON

---

## 1) Project Layout

TELCO_INGEST/
│
├── app/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── db.py
│   │   └── models.py
│   │
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── health.py
│   │   ├── ingest.py
│   │   └── ui.py
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── ingest_service.py
│   │   ├── parse_service.py
│   │
│   ├── ui/
│   │   ├── static/
│   │   │   └── app.css
│   │   └── templates/
│   │       ├── base.html
│   │       ├── index.html
│   │       ├── invoice_detail.html
│   │       └── invoices.html
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── hashing.py
│   │   └── vendor_detect.py
│   │
│   └── ingest/
│       ├── __init__.py
│       ├── import_folder.py
│       ├── upload_json.py
│       └── main.py
│
├── parsers/
│   ├── __init__.py
│   ├── base.py                # Base extractor class (shared logic)
│   ├── celcom_extractor.py    # Celcom-specific extractor
│   ├── digi_extractor.py      # Digi-specific extractor
│   └── maxis_extractor.py     # Maxis-specific extractor
│
├── _uploads/                  # Folder for storing uploaded files
│
├── .env                       # Environment variables
├── README.md                  # Project description
├── requirements.txt           # Python dependencies


---

## 2) Standard Invoice Package (DB-aligned)

Every parser returns the same shape:

```json
{
  "invoice": {
    "vendor": "maxis|celcom|digi",
    "invoice_number": "string",
    "account_number": "string|null",
    "bill_date": "YYYY-MM-DD|null",
    "period_start": "YYYY-MM-DD|null",
    "period_end": "YYYY-MM-DD|null",
    "currency": "MYR",
    "subtotal": 0.0,
    "tax_total": 0.0,
    "grand_total": 0.0
  },
  "numbers": [
    {
      "msisdn": "01XXXXXXXX",
      "description": "Plan/Bundle text",
      "subscriber": "COMPANY SDN BHD",
      "monthly_items": [ { "description": "...", "amount": 0.0 } ],
      "detail_of_charges": [ { "category": "...", "amount": 0.0, "...": "..." } ],
      "line_total": 0.0
    }
  ],
  "charges": [
    { "category": "Previous|Payments|Monthly|Usage|Tax|Discounts|Other|Adjustments|Other Credits",
      "label": "string",
      "amount": 0.0 }
  ],
  "raw": { "...": "vendor-specific full parse for downstream tables" }
}
