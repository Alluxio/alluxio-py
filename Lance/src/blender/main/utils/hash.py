from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timezone
from decimal import Decimal
from typing import Any
from typing import Mapping
from typing import Optional
from typing import Sequence

import numpy as np
import pandas as pd


# ------------------------- low-level hashing -------------------------
def sha256_bytes(b: bytes) -> str:
    """Return hex sha256 of raw bytes."""
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def sha256_stream(path: str, chunk_size: int = 4 * 1024 * 1024) -> str:
    """Stream file content to compute sha256 without loading the whole file into memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def file_metadata(path: str) -> dict:
    """Return file-level metadata with streaming sha256."""
    st = os.stat(path)
    return {
        "artifact_sha256": sha256_stream(path),
        "size_bytes": int(st.st_size),
        "mtime": float(st.st_mtime),
        "path": path,
    }


# ----------------------- normalization utilities -----------------------
def _tz_to_utc_iso(dt: datetime) -> str:
    """Normalize datetime to UTC ISO-8601 string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def normalize_scalar(v: Any) -> Any:
    """
    Normalize a scalar into a JSON-stable representation:
    - NaN/NaT -> None
    - numpy scalar -> Python native
    - datetime/date/time -> ISO string (UTC for datetime)
    - Decimal -> float
    - bytes -> hex string
    """
    # pandas NA / numpy NaN
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # numpy scalar -> python
    if isinstance(v, np.generic):
        v = v.item()

    # numbers: normalize -0.0 to 0.0 for stability
    if isinstance(v, float):
        if v == 0.0:
            return 0.0

    # datetimes
    if isinstance(v, (datetime, pd.Timestamp)):
        return _tz_to_utc_iso(pd.Timestamp(v).to_pydatetime())
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, time):
        # keep raw time (no timezone)
        return v.isoformat()

    if isinstance(v, Decimal):
        return float(v)

    if isinstance(v, bytes):
        return v.hex()  # or base64.b64encode(v).decode("ascii")

    if isinstance(v, np.ndarray):
        return v.tolist()

    return v


def normalize_for_json(obj: Any) -> Any:
    """
    Recursively normalize complex structures (dict/list/scalar) into
    a deterministic, JSON-serializable form.
    - dict keys sorted
    - lists kept in given order
    """
    if isinstance(obj, Mapping):
        return {
            k: normalize_for_json(normalize_scalar(v))
            for k, v in sorted(obj.items())
        }
    if isinstance(obj, (list, tuple)):
        return [normalize_for_json(normalize_scalar(x)) for x in obj]
    return normalize_scalar(obj)


def canonical_json_bytes(obj: Any) -> bytes:
    """
    Serialize normalized object to canonical JSON bytes:
    - sorted keys
    - compact separators
    - ensure_ascii=False (UTF-8)
    """
    norm = normalize_for_json(obj)
    return json.dumps(
        norm, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


# ----------------------- row-level stable hashing -----------------------
def row_sha256(
    row: Mapping[str, Any],
    include_keys: Optional[Sequence[str]] = None,
    exclude_keys: Optional[Sequence[str]] = None,
) -> str:
    """
    Compute a stable sha256 for a row (mapping).
    - You can select columns via include_keys or exclude_keys.
    - Cells are normalized (NaN->None, timestamps->iso, bytes->hex, etc.).
    """
    if include_keys is not None:
        payload = {k: row.get(k) for k in include_keys}
    else:
        payload = dict(row)
        if exclude_keys:
            for k in exclude_keys:
                payload.pop(k, None)
    return sha256_bytes(canonical_json_bytes(payload))


# ----------------------- numpy tensor hashing -----------------------
def tensor_to_bytes(arr: np.ndarray, *, order: str = "C") -> bytes:
    """
    Convert numpy array to contiguous bytes in a given memory order (default 'C').
    This ensures consistent hashing across platforms.
    """
    if not isinstance(arr, np.ndarray):
        raise TypeError("arr must be a numpy.ndarray")
    if order not in ("C", "F"):
        raise ValueError("order must be 'C' or 'F'")
    if order == "C":
        arr = np.ascontiguousarray(arr)
    else:
        arr = np.asfortranarray(arr)
    return memoryview(arr).tobytes(order=order)


def tensor_sha256(arr: np.ndarray, *, order: str = "C") -> str:
    """Hash a numpy tensor deterministically."""
    return sha256_bytes(tensor_to_bytes(arr, order=order))


def tensor_fingerprint(arr: np.ndarray) -> dict:
    """
    Return a compact fingerprint for the tensor: dtype, shape, sha256.
    Useful for logging or row construction.
    """
    return {
        "dtype": str(arr.dtype),
        "shape": list(arr.shape),
        "sha256": tensor_sha256(arr),
    }
