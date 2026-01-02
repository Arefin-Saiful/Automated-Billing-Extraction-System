# F:\telco_ingest\app\utils\hashing.py
# -*- coding: utf-8 -*-
"""
Hashing utilities for telco_ingest.

Features
--------
- file_sha256 / file_md5: streaming, memory-friendly
- bytes_sha256 / text_sha256: convenience wrappers
- stable_json_dumps / stable_json_hash: canonical JSON hashing (sorted keys)
- json_sha256: convenience wrapper for stable_json_hash
- file_signature: fast signature using size, mtime, and file head/tail
- combine_hashes: deterministic roll-up of hex digests
- HMAC helper: hmac_sha256_hex
- stream_sha256: hash any binary stream

Backward-compatibility:
- sha256_file(...)      -> alias of file_sha256(...)
- stable_json_hash(obj) -> provided (and json_sha256(obj) kept)
"""

from __future__ import annotations
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional, Union

# ------------------------ Core helpers ------------------------

_CHUNK_SIZE = 1024 * 1024  # 1 MiB default for streaming

PathLike = Union[str, os.PathLike]

def _to_path(p: PathLike) -> Path:
    return Path(p).expanduser().resolve()

def _readable_file(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    if not p.is_file():
        raise IsADirectoryError(f"Not a file: {p}")

# ------------------------ File hashing ------------------------

def file_sha256(path: PathLike, chunk_size: int = _CHUNK_SIZE) -> str:
    """
    Compute SHA-256 hex digest of a file via streaming reads.
    """
    p = _to_path(path)
    _readable_file(p)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def file_md5(path: PathLike, chunk_size: int = _CHUNK_SIZE) -> str:
    """
    Compute MD5 hex digest of a file via streaming reads.
    Useful for quick duplicate detection (not for security).
    """
    p = _to_path(path)
    _readable_file(p)
    h = hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

# Legacy alias expected by app.main import
def sha256_file(path: PathLike, chunk_size: int = _CHUNK_SIZE) -> str:
    """Alias for file_sha256 (backward-compat)."""
    return file_sha256(path, chunk_size=chunk_size)

# ------------------------ In-memory hashing ------------------------

def bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def text_sha256(text: str, encoding: str = "utf-8") -> str:
    return bytes_sha256(text.encode(encoding))

# ------------------------ Stable JSON hashing ------------------------

def stable_json_dumps(obj: Any, *, ensure_ascii: bool = False) -> str:
    """
    Canonical JSON serialization:
      - sort_keys=True
      - separators=(',', ':') to remove whitespace variance
    """
    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=ensure_ascii,
        separators=(",", ":"),
        default=str,  # fallback for non-serializable objects
    )

def stable_json_hash(obj: Any, *, ensure_ascii: bool = False) -> str:
    """
    SHA-256 over canonical JSON (stable_json_dumps).
    """
    return text_sha256(stable_json_dumps(obj, ensure_ascii=ensure_ascii))

# Convenience name kept from your earlier draft
def json_sha256(obj: Any, ensure_ascii: bool = False) -> str:
    """Alias to stable_json_hash for convenience."""
    return stable_json_hash(obj, ensure_ascii=ensure_ascii)

# ------------------------ File signature (fast) ------------------------

def file_signature(path: PathLike, head_bytes: int = 64 * 1024, tail_bytes: int = 64 * 1024) -> str:
    """
    Quick-but-robust signature:
      sha256( size || mtime_ns || head[:N] || tail[:M] )

    Useful for dedup/changed-file checks without hashing the full file.
    Falls back to full file hash if file is smaller than head+tail window.
    """
    p = _to_path(path)
    _readable_file(p)
    st = p.stat()
    size = st.st_size

    with p.open("rb") as f:
        if size <= head_bytes + tail_bytes:
            # small file: full hash
            h = hashlib.sha256()
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                h.update(chunk)
            meta = f"{size}:{st.st_mtime_ns}".encode("utf-8")
            return hashlib.sha256(meta + h.digest()).hexdigest()

        # head
        head = f.read(head_bytes) if head_bytes > 0 else b""
        # tail
        f.seek(max(0, size - tail_bytes))
        tail = f.read(tail_bytes) if tail_bytes > 0 else b""

    meta = f"{size}:{st.st_mtime_ns}".encode("utf-8")
    return hashlib.sha256(meta + head + tail).hexdigest()

# ------------------------ Combine / roll-up ------------------------

def combine_hashes(digests: Iterable[str], algo: str = "sha256") -> str:
    """
    Combine multiple hex digests deterministically.
    Sorts inputs to avoid order sensitivity.
    """
    try:
        h = getattr(hashlib, algo)()
    except AttributeError as e:
        raise ValueError(f"Unsupported hash algorithm: {algo}") from e

    for d in sorted(digests):
        if not isinstance(d, str):
            raise TypeError("All digests must be strings (hex).")
        h.update(d.encode("ascii"))
    return h.hexdigest()

# ------------------------ HMAC helper ------------------------

def hmac_sha256_hex(key: Union[bytes, str], data: Union[bytes, str]) -> str:
    """
    Compute HMAC-SHA256 hex digest. Accepts bytes or str inputs.
    """
    if isinstance(key, str):
        key = key.encode("utf-8")
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hmac.new(key, data, hashlib.sha256).hexdigest()

# ------------------------ Stream hashing ------------------------

def stream_sha256(reader: io.BufferedReader, chunk_size: int = _CHUNK_SIZE) -> str:
    """
    Hash any readable binary stream (e.g., HTTP response, zipfile member).
    Caller is responsible for managing the stream’s position/close.
    """
    h = hashlib.sha256()
    for chunk in iter(lambda: reader.read(chunk_size), b""):
        h.update(chunk)
    return h.hexdigest()

# ------------------------ Public API ------------------------

__all__ = [
    # file hashing
    "file_sha256", "file_md5", "sha256_file",
    # in-memory
    "bytes_sha256", "text_sha256",
    # json hashing
    "stable_json_dumps", "stable_json_hash", "json_sha256",
    # misc
    "file_signature", "combine_hashes", "hmac_sha256_hex", "stream_sha256",
]
