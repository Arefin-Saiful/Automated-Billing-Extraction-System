# F:\telco_ingest\app\utils\vendor_detect.py
# -*- coding: utf-8 -*-
"""
Vendor detector for telco PDFs.

Public API
----------
detect_vendor(pdf_path: str) -> dict
    Returns e.g. {
        "vendor": "maxis" | "celcom" | "digi" | "unknown",
        "confidence": 0.0..1.0,
        "source": "filename" | "text" | "fallback",
        "matches": {"maxis": ["Maxis Broadband Sdn Bhd", ...], ...}
    }

is_vendor(pdf_path: str, vendor: str) -> bool
    Convenience helper wrapping detect_vendor.

Notes
-----
- We keep detection lightweight (only first 1–2 pages) for speed.
- Celcom vs Digi after the CelcomDigi merger:
    * If the text contains explicit 'Digi Telecommunications Sdn Bhd', we bias to 'digi'.
    * If it contains Celcom-specific artefacts (e.g., 'MEGA Lightning', 'Celcom Axiata'),
      we bias to 'celcom'.
    * If it contains only 'CelcomDigi' branding without the explicit company lines,
      we fall back to the stronger of the two scores; if still tied, prefer 'digi' only when
      'CelcomDigi Business Postpaid' patterns appear (typical on Digi/CelcomDigi bills).
"""

from __future__ import annotations
import re
from typing import Dict, List, Tuple, Any, Optional

try:
    import pdfplumber  # lightweight text extraction
except Exception:
    pdfplumber = None


# ---------- Keyword libraries (ordered by specificity → general) ----------

KEYWORDS: Dict[str, List[re.Pattern]] = {
    "maxis": [
        re.compile(r"\bMaxis Broadband Sdn Bhd\b", re.I),
        re.compile(r"\bMaxis(?: Berhad)?\b", re.I),
        re.compile(r"\bmaxis\.com\.my\b", re.I),
        re.compile(r"\bMaxis Business\b", re.I),
    ],
    "celcom": [
        re.compile(r"\bCelcom(?: \(Malaysia\))?\s*Axiata\b", re.I),
        re.compile(r"\bCelcom(?:Digi)?\s+Bill Statement\b", re.I),
        re.compile(r"\bMEGA\s+Lightning\b", re.I),  # common Celcom plan branding
        re.compile(r"\bCelcom(?:Digi)?\b", re.I),   # generic Celcom/CelcomDigi
        re.compile(r"\bHello\s+TRADEWINDS\b", re.I),  # appears on some Celcom bills
    ],
    "digi": [
        re.compile(r"\bDigi Telecommunications Sdn Bhd\b", re.I),
        re.compile(r"\bCelcomDigi\s+Business\b", re.I),
        re.compile(r"\bPostpaid\s*5G\s*\d+\b", re.I),
        re.compile(r"\bCelcomDigi\b", re.I),  # generic CelcomDigi (we resolve later)
    ],
}

# Extra disambiguation hints (high weight if present)
HARD_HINTS = {
    "celcom": [
        re.compile(r"\bCelcom\s*\(Malaysia\)\s*Berhad\b", re.I),
        re.compile(r"\bCelcom\s*Axiata\b", re.I),
        re.compile(r"\bMEGA\s+Lightning\s*\d+\b", re.I),
    ],
    "digi": [
        re.compile(r"\bDigi Telecommunications Sdn Bhd\b", re.I),
        re.compile(r"\bCelcomDigi\s+Business\s+Postpaid\b", re.I),
    ],
}

# Filename heuristics
FILENAME_HINTS = {
    "maxis": [re.compile(r"\bmaxis\b", re.I), re.compile(r"\bME\d{6,}\b", re.I)],
    "celcom": [re.compile(r"\bcelcom\b", re.I)],
    "digi": [re.compile(r"\bdigi\b", re.I), re.compile(r"\bcelcomdigi\b", re.I)],
}


def _score_with_patterns(text: str, patterns: List[re.Pattern]) -> Tuple[int, List[str]]:
    hits = []
    score = 0
    for rx in patterns:
        for m in rx.finditer(text):
            frag = m.group(0)
            if frag not in hits:
                hits.append(frag)
                score += 1
    return score, hits


def _peek_text(pdf_path: str, max_pages: int = 2) -> str:
    if not pdfplumber:
        return ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            chunks = []
            for page in pdf.pages[:max_pages]:
                t = page.extract_text() or ""
                chunks.append(t)
            text = "\n".join(chunks)
            # normalize whitespace a bit
            text = re.sub(r"[ \t]+", " ", text)
            return text
    except Exception:
        return ""


def _from_filename(filename: str) -> Optional[Tuple[str, Dict[str, List[str]]]]:
    filename = filename or ""
    matches: Dict[str, List[str]] = {"maxis": [], "celcom": [], "digi": []}
    best_vendor = None
    best_hits = 0
    for vendor, patterns in FILENAME_HINTS.items():
        s, hits = _score_with_patterns(filename, patterns)
        if s > 0:
            matches[vendor].extend(hits)
        if s > best_hits:
            best_vendor, best_hits = vendor, s
    if best_vendor:
        return best_vendor, matches
    return None


def _resolve_celcom_vs_digi(text: str, base_scores: Dict[str, int], matches: Dict[str, List[str]]) -> str:
    """Resolve ambiguity when 'CelcomDigi' appears in both buckets."""
    # Hard hints first (each hit counts as +3)
    for v in ("celcom", "digi"):
        for rx in HARD_HINTS[v]:
            hits = rx.findall(text)
            if hits:
                base_scores[v] += 3 * len(hits)
                for h in hits:
                    if h not in matches[v]:
                        matches[v].append(h)

    # If still tied, prefer 'digi' when explicit business postpaid patterns exist
    if base_scores["celcom"] == base_scores["digi"]:
        if re.search(r"\bCelcomDigi\s+Business\s+Postpaid\b", text, re.I):
            base_scores["digi"] += 1

    # Choose the higher score
    return "celcom" if base_scores["celcom"] > base_scores["digi"] else "digi"


def detect_vendor(pdf_path: str) -> Dict[str, Any]:
    """
    Detect vendor from a PDF path.

    Returns:
        {
          "vendor": "maxis"|"celcom"|"digi"|"unknown",
          "confidence": float,
          "source": "filename"|"text"|"fallback",
          "matches": {"maxis":[...], "celcom":[...], "digi":[...]}
        }
    """
    result = {
        "vendor": "unknown",
        "confidence": 0.0,
        "source": "fallback",
        "matches": {"maxis": [], "celcom": [], "digi": []},
    }

    # 1) Filename hints
    fn = str(pdf_path)
    fn_guess = _from_filename(fn)
    if fn_guess:
        vendor, matches = fn_guess
        result["vendor"] = vendor
        result["confidence"] = 0.70  # good but not final; we may still look at text
        result["source"] = "filename"
        result["matches"] = matches

    # 2) Light text peek (first 1–2 pages)
    text = _peek_text(pdf_path, max_pages=2)
    if text:
        scores = {"maxis": 0, "celcom": 0, "digi": 0}
        for vendor, patterns in KEYWORDS.items():
            s, hits = _score_with_patterns(text, patterns)
            scores[vendor] += s
            if hits:
                prev = result["matches"].setdefault(vendor, [])
                for h in hits:
                    if h not in prev:
                        prev.append(h)

        # Resolve Celcom vs Digi overlap if both have non-zero
        chosen = None
        if scores["celcom"] > 0 or scores["digi"] > 0 or scores["maxis"] > 0:
            if scores["celcom"] > 0 and scores["digi"] > 0 and scores["maxis"] == 0:
                chosen = _resolve_celcom_vs_digi(text, scores, result["matches"])
            else:
                # Pick the outright winner
                chosen = max(scores.items(), key=lambda kv: kv[1])[0]

        if chosen:
            total_hits = sum(scores.values()) or 1
            conf = min(1.0, 0.55 + 0.10 * scores[chosen] + 0.05 * (scores[chosen] / total_hits))
            result.update({"vendor": chosen, "confidence": conf, "source": "text"})

    return result


def is_vendor(pdf_path: str, vendor: str) -> bool:
    """Convenience wrapper."""
    v = (vendor or "").strip().lower()
    if v not in {"maxis", "celcom", "digi"}:
        return False
    return detect_vendor(pdf_path).get("vendor") == v


# ---- Simple CLI (optional) ----
if __name__ == "__main__":
    import json
    import argparse
    ap = argparse.ArgumentParser(description="Detect vendor for a telco PDF")
    ap.add_argument("pdf", help="Path to PDF")
    args = ap.parse_args()
    print(json.dumps(detect_vendor(args.pdf), indent=2, ensure_ascii=False))
