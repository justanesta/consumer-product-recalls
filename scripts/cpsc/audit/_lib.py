"""
Shared helpers for CPSC recall field audit scripts (Phase 6a foundation audit).

Mirrors ``scripts/usda_recalls/audit/_lib.py`` in shape — the summarize helpers
are source-agnostic so we duplicate them rather than cross-import (per
``feedback_audit_all_precedent_files_before_drafting``: per-source ``_lib.py``
is the project convention; cross-source refactor is deferred until 3+ sources
need a shared lib).

Two responsibilities:

1. Per-field statistics over a list of dict-shaped records. CPSC's recall API
   returns a flat JSON array of records (no envelope) — the same shape as
   USDA's recall API. The bronze loader writes that array verbatim to R2.

2. R2 download + local cache resolution under ``data/exploratory/cpsc/``
   (gitignored). Mirrors the FDA/USDA precedent — strip ``.gz`` from R2
   basename, write decompressed bytes on cache miss, return ``Path`` to the
   cached file.

CPSC-specific list-handling extension: 13 of the 21 bronze columns are
JSONB pass-throughs of API collections (Manufacturers, Retailers, Hazards,
Images, etc.). These elements are dicts, not hashables — distinct-count
via ``set()`` would crash on unhashable types. ``summarize_field`` detects
list-of-dict and reports element_count distribution + samples without
attempting a distinct count. For nested-key validation (e.g., "what are
the unique Manufacturers[].CompanyID values?") use the companion SQL
script ``scripts/sql/cpsc/bronze/inspect_array_field_population.sql``.

Sibling scripts add this directory to ``sys.path`` so they can ``import _lib``
when invoked directly.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any

# Repo-root anchored cache. parents[3] is:
#   scripts/cpsc/audit/_lib.py
#   parents[0] = scripts/cpsc/audit/
#   parents[1] = scripts/cpsc/
#   parents[2] = scripts/
#   parents[3] = <repo root>
DEFAULT_CACHE_DIR: Path = Path(__file__).resolve().parents[3] / "data" / "exploratory" / "cpsc"


# ----- Field summary -----


def _is_null(v: Any) -> bool:
    """Treat None, CPSC's '' sentinel (Finding F — SoldAtLabel/Type/etc.), and
    empty list ``[]`` as null.

    CPSC routinely returns empty strings for unpopulated optional scalars
    (per cassette: Products[].Description, Products[].Model, Products[].Type,
    Products[].CategoryID, Hazards[].HazardType, Hazards[].HazardTypeID all
    appear as ``""`` not ``null``). Counting these as null is consistent with
    USDA's Finding C treatment and matches the audit's "documented-empty-
    by-source" framing in ``documentation/cpsc/field_audit_2026_w22.md`` §5.
    """
    if v is None or v == "":
        return True
    return isinstance(v, list) and len(v) == 0


def _truncate_repr(v: Any, max_len: int) -> str:
    s = repr(v)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def summarize_field(values: list[Any], field_name: str, sample_size: int = 3) -> dict[str, Any]:
    """Compute per-field statistics across a list of values (one per record).

    Returns a dict with:
      - n_records, null_count, null_pct, distinct (all fields)
      - min_length, max_length (string fields only)
      - distribution: list of (value, count) tuples (low-cardinality, <= 20 distinct)
      - samples: list of N non-null values (high-cardinality)
      - element_count_distribution, total_elements (any list-typed field)
      - element_distribution, distinct_elements (hashable-element list fields only)

    CPSC list-of-dict path: when list elements are dicts (Manufacturers,
    Hazards, Images, etc.), the function reports element_count distribution
    and samples of full elements, but skips distinct-count and per-element
    distribution (dicts aren't hashable; use a SQL nested-key query instead).
    """
    n = len(values)
    nulls = sum(1 for v in values if _is_null(v))
    non_null = [v for v in values if not _is_null(v)]

    is_list_field = any(isinstance(v, list) for v in non_null)

    summary: dict[str, Any] = {
        "field": field_name,
        "n_records": n,
        "null_count": nulls,
        "null_pct": (nulls / n * 100) if n else 0.0,
    }

    if is_list_field:
        list_values = [v for v in non_null if isinstance(v, list)]
        element_counts = [len(v) for v in list_values]
        if element_counts:
            summary["element_count_distribution"] = Counter(element_counts).most_common()

        # Flatten elements for inspection. CPSC arrays are list-of-dict
        # (Manufacturers, Hazards, Images, etc.), USDA-style hashable arrays
        # would be list-of-str/int. Detect which.
        all_elements = [elem for v in list_values for elem in v]
        if all_elements:
            summary["total_elements"] = len(all_elements)
            hashable_elements = [e for e in all_elements if isinstance(e, str | int | float | bool)]
            if hashable_elements:
                # Hashable-element list (str/int/etc.) — distinct count + top values
                summary["distinct_elements"] = len(set(hashable_elements))
                summary["element_distribution"] = Counter(hashable_elements).most_common(20)
                summary["distinct"] = summary["distinct_elements"]
            else:
                # Dict-element list (the CPSC default for arrays) — sample only
                summary["element_samples"] = all_elements[:sample_size]
                summary["distinct"] = 0  # placeholder; not meaningful for dict elements
        else:
            summary["distinct"] = 0
    else:
        hashable_non_null = [v for v in non_null if isinstance(v, str | int | float | bool)]
        distinct = len(set(hashable_non_null)) if hashable_non_null else 0
        summary["distinct"] = distinct

        str_values = [v for v in non_null if isinstance(v, str)]
        if str_values:
            summary["min_length"] = min(len(s) for s in str_values)
            summary["max_length"] = max(len(s) for s in str_values)

        if 0 < distinct <= 20 and hashable_non_null:
            summary["distribution"] = Counter(hashable_non_null).most_common()
        else:
            summary["samples"] = non_null[:sample_size]

    return summary


def format_summary(summary: dict[str, Any]) -> str:
    lines = [f"Field: {summary['field']}"]
    lines.append(f"  N records: {summary['n_records']}")
    lines.append(f"  NULL: {summary['null_count']} ({summary['null_pct']:.1f}%)")
    lines.append(f"  Distinct: {summary['distinct']}")
    if "min_length" in summary:
        lines.append(f"  Length: min={summary['min_length']}, max={summary['max_length']}")
    if "distribution" in summary:
        lines.append("  Distribution:")
        for v, count in summary["distribution"]:
            lines.append(f"    {count:>6d}  {_truncate_repr(v, 80)}")
    elif "samples" in summary:
        lines.append("  Samples:")
        for v in summary["samples"]:
            lines.append(f"    {_truncate_repr(v, 120)}")
    if "element_count_distribution" in summary:
        lines.append("  Element-count per record (count → records):")
        for elem_n, count in summary["element_count_distribution"]:
            lines.append(f"    len={elem_n:>3d}: {count} records")
    if "element_distribution" in summary:
        lines.append(
            f"  List contents: {summary['total_elements']} total elements, "
            f"{summary['distinct_elements']} distinct"
        )
        lines.append("  Top element values:")
        for elem, count in summary["element_distribution"]:
            lines.append(f"    {count:>6d}  {_truncate_repr(elem, 80)}")
    elif "element_samples" in summary:
        lines.append(f"  Total elements: {summary.get('total_elements', 0)}")
        lines.append("  Element samples (dict elements — use SQL for nested-key stats):")
        for elem in summary["element_samples"]:
            lines.append(f"    {_truncate_repr(elem, 200)}")
    return "\n".join(lines)


def summarize_records(records: list[dict[str, Any]], fields: list[str] | None = None) -> str:
    """Build a per-field summary block across a list of records.

    If ``fields`` is None, the union of all keys observed across records is
    used (sorted alphabetically). Pass ``fields`` to restrict to specific columns.
    """
    if not records:
        return "No records to summarize."

    if fields is None:
        fields = sorted({k for r in records for k in r})

    output = [f"=== Per-field summary across {len(records)} records ===", ""]
    for field in fields:
        values = [r.get(field) for r in records]
        output.append(format_summary(summarize_field(values, field)))
        output.append("")
    return "\n".join(output)


# ----- R2 cache resolver -----


def _fetch_from_r2(key: str) -> bytes:
    """Decompressed R2 bytes for ``key``. Lazy-imports so local-only callers don't pay."""
    from src.config.settings import Settings  # noqa: PLC0415
    from src.landing.r2 import R2LandingClient  # noqa: PLC0415

    settings = Settings()  # type: ignore[call-arg]
    client = R2LandingClient(settings)
    return client.get_raw(key)


def resolve_cached_payload(raw_landing_path: str, cache_dir: Path | None = None) -> Path:
    """Return the local cache path for an R2 key; fetch from R2 on cache miss.

    The cached file name is the R2 basename with the ``.gz`` suffix stripped
    (``R2LandingClient.get_raw`` decompresses the outer gzip before returning,
    so the cached bytes are the raw payload — decompressed JSON for CPSC).

    Cache hit/miss messages go to stderr so analysis output on stdout is clean.
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    basename = Path(raw_landing_path).name
    if basename.endswith(".gz"):
        basename = basename[:-3]
    cached = cache_dir / basename
    if cached.exists():
        print(f"# Cache hit: {cached}", file=sys.stderr)
        return cached
    print(f"# Cache miss; fetching from R2: {raw_landing_path}", file=sys.stderr)
    raw_bytes = _fetch_from_r2(raw_landing_path)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(raw_bytes)
    print(f"# Saved to: {cached}", file=sys.stderr)
    return cached
