"""
Shared helpers for USDA FSIS Establishment Listing field audit scripts
(Phase 6a foundation audit).

Mirrors ``scripts/usda_recalls/audit/_lib.py`` and ``scripts/fda/audit/_lib.py``
in shape — per-source ``_lib.py`` per project convention; cross-source refactor
is deferred until 3+ sources need a shared lib.

Two responsibilities:

1. Per-field statistics over a list of dict-shaped records — USDA's
   Establishment Listing API returns a flat JSON array of records (no
   envelope). Each record has snake_case keys (no ``field_`` prefix —
   different from the recall API).

2. R2 download + local cache resolution under
   ``data/exploratory/usda_establishments/`` (gitignored). Mirrors
   ``scripts/nhtsa/tsv_analysis/inspect_archive_row.py``'s ``_resolve_cached``
   pattern.

Sibling scripts add this directory to ``sys.path`` so they can ``import _lib``
when invoked directly.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path
from typing import Any

# Repo-root anchored cache. parents[3] is:
#   scripts/usda_establishments/audit/_lib.py
#   parents[0] = scripts/usda_establishments/audit/
#   parents[1] = scripts/usda_establishments/
#   parents[2] = scripts/
#   parents[3] = <repo root>
DEFAULT_CACHE_DIR: Path = (
    Path(__file__).resolve().parents[3] / "data" / "exploratory" / "usda_establishments"
)


# ----- Field summary -----


def _is_null(v: Any) -> bool:
    """Treat None, '' (per Finding C), the JSON boolean ``False`` sentinel
    (per Finding C in establishment_api_observations.md), and the empty list
    ``[]`` as null.

    The establishment API uses JSON ``false`` (not ``null``, not ``""``) as a
    missing sentinel on ``geolocation`` and ``county`` — about ~1.5% of records.
    Bronze stores those as the string ``"false"`` per ADR 0027; the raw R2
    payload still carries the literal JSON boolean ``False``. Both forms are
    treated as null here so NULL-rate distributions reflect the source's actual
    missingness rather than a literal-bool surprise in the distribution table.

    Empty list ``[]`` is treated as null for ``dbas`` (~50%+ of records have
    no DBAs) and similar JSON-array fields where ``[]`` is the source's
    "no value" representation, not an explicit "empty collection" semantic.
    """
    if v is None or v == "" or v is False:
        return True
    return isinstance(v, list) and len(v) == 0


def _truncate_repr(v: Any, max_len: int) -> str:
    s = repr(v)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def summarize_field(values: list[Any], field_name: str, sample_size: int = 3) -> dict[str, Any]:
    """Compute per-field statistics across a list of values (one per record).

    Handles list-typed fields (e.g., ``activities``, ``dbas``) specially:
    distinct counting hashes lists as tuples, and a per-element ``Counter`` is
    computed so the operator can see what individual values appear inside the
    list field across the corpus (e.g., 'Meat Processing' as an element of
    ``activities`` lists).
    """
    n = len(values)
    nulls = sum(1 for v in values if _is_null(v))
    non_null = [v for v in values if not _is_null(v)]

    # Detect list-typed field: any non-null value is a list.
    is_list_field = any(isinstance(v, list) for v in non_null)

    summary: dict[str, Any] = {
        "field": field_name,
        "n_records": n,
        "null_count": nulls,
        "null_pct": (nulls / n * 100) if n else 0.0,
    }

    if is_list_field:
        # Hash list values as tuples so distinct counting works on whole-list values.
        tupled: list[Any] = [tuple(v) if isinstance(v, list) else v for v in non_null]
        hashable_tupled = [v for v in tupled if isinstance(v, str | int | float | bool | tuple)]
        distinct = len(set(hashable_tupled))
        summary["distinct"] = distinct

        if 0 < distinct <= 20 and hashable_tupled:
            # Convert tuples back to lists for readable display.
            summary["distribution"] = [
                (list(v) if isinstance(v, tuple) else v, count)
                for v, count in Counter(hashable_tupled).most_common()
            ]
        else:
            summary["samples"] = [
                list(v) if isinstance(v, tuple) else v for v in tupled[:sample_size]
            ]

        # Per-element cardinality: flatten every list and count individual elements.
        all_elements = [elem for v in non_null if isinstance(v, list) for elem in v]
        if all_elements:
            summary["total_elements"] = len(all_elements)
            summary["distinct_elements"] = len(set(all_elements))
            summary["element_distribution"] = Counter(all_elements).most_common(20)
    else:
        # Scalar field path.
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
    if "element_distribution" in summary:
        lines.append(
            f"  List contents: {summary['total_elements']} total elements, "
            f"{summary['distinct_elements']} distinct"
        )
        lines.append("  Top element values:")
        for elem, count in summary["element_distribution"]:
            lines.append(f"    {count:>6d}  {_truncate_repr(elem, 80)}")
    return "\n".join(lines)


def summarize_records(records: list[dict[str, Any]], fields: list[str] | None = None) -> str:
    """Build a per-field summary block across a list of records."""
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

    The cached file name is the R2 basename with the ``.gz`` suffix stripped.
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
