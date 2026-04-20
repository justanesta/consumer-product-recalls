from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


def _strip_none(value: Any) -> Any:
    """
    Recursively remove None values from dicts before hashing.
    Prevents Optional field defaults from creating spurious hash churn across ingestions.
    List elements are left intact — None values inside lists are preserved because
    arrays in all five sources are semantically ordered and must not be altered.
    """
    if isinstance(value, dict):
        return {k: _strip_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_strip_none(item) for item in value]
    return value


def _json_default(obj: Any) -> Any:
    """
    json.dumps default handler per ADR 0007.
    datetime → UTC ISO-8601 with microsecond precision.
    Everything else (Decimal, UUID, etc.) → str.
    """
    if isinstance(obj, datetime):
        return obj.astimezone(UTC).isoformat(timespec="microseconds")
    return str(obj)


def content_hash(record: dict[str, Any]) -> str:
    """
    Compute a stable SHA-256 hash of a bronze record dict.

    Implementation is PINNED per ADR 0007 — any change to this function invalidates
    every previously-computed bronze hash and constitutes a schema migration requiring
    documentation and a re-dedup plan.

    Serialization contract:
    - None values stripped recursively (prevents Optional default churn).
    - Dict keys sorted recursively (json.dumps sort_keys is recursive for nested dicts).
    - No whitespace (separators=(',', ':')).
    - UTF-8 characters preserved literally (ensure_ascii=False).
    - datetime → UTC ISO-8601 microseconds via _json_default.
    - Decimal, UUID → str via _json_default fallback.
    - Arrays preserve source order — semantically ordered in all five sources.
    - Floats must be pre-rounded by callers (see normalize_float); content_hash does not
      round to avoid silently masking unexpected precision changes in source data.
    """
    clean = _strip_none(record)
    serialized = json.dumps(
        clean,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def normalize_float(value: float, places: int = 6) -> float:
    """
    Round a float to `places` decimal places to prevent platform-dependent repr() drift.
    Call this at Pydantic field validation time, not inside content_hash().
    Recall payloads rarely contain floats; when they do this helper lives alongside
    content_hash in this module so the two conventions stay co-located.
    """
    return round(value, places)
