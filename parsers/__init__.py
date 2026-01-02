# -*- coding: utf-8 -*-
"""
Parsers facade: expose vendor-specific `extract_*` callables.

These thin wrappers import the real implementations from app.services.*
and present a uniform function signature the rest of the app expects.
"""

from __future__ import annotations

# Maxis
try:
    from .maxis_extractor import extract as extract_maxis  # type: ignore
except Exception as _e:  # pragma: no cover
    raise ImportError(f"Maxis parser not available: {_e}")

# Celcom
try:
    from .celcom_extractor import extract as extract_celcom  # type: ignore
except Exception as _e:  # pragma: no cover
    # If you haven't wired Celcom yet, you can temporarily stub it:
    # def extract_celcom(_path: str) -> dict: raise NotImplementedError("Celcom parser not wired")
    raise ImportError(f"Celcom parser not available: {_e}")

# Digi
try:
    from .digi_extractor import extract as extract_digi  # type: ignore
except Exception as _e:  # pragma: no cover
    # Same note as Celcom above if not ready yet.
    raise ImportError(f"Digi parser not available: {_e}")
