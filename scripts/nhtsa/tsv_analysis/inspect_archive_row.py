"""Inspect raw NHTSA archive rows by (campno, mfr_comp_ptno).

Downloads a NHTSA archive from R2 (or reads a local .zip), filters the inner
TSV by supplied identity values, and dumps the chosen field cells with
character-level visibility via ``repr()``. Used for byte-level confirmation
of bronze-vs-source discrepancies surfaced by
``scripts/sql/nhtsa/bronze/diagnose_null_regression.sql`` and related drift
diagnostics. For each matched row, compare the raw TSV cell against what
bronze stored:

  * H1 — UPSTREAM-DRIVEN. The raw TSV cell matches what bronze stored
         (e.g., empty bytes for a populated→NULL transition; "Software"
         for an empty→populated transition; a new date for a value-A→
         value-B transition). Operationally normal — bronze captures
         what NHTSA published.
  * H2 — EXTRACTOR MIS-PARSE. The raw TSV cell differs from what bronze
         stored. Bug in cell-to-Pydantic mapping in
         ``src/extractors/nhtsa.py``.

Fields to dump are configurable via ``--show-field`` (CSV of lowercase
field names from RCL.txt; default ``bgman,endman`` for the original
NULL-regression workflow). For mfr_comp_desc-class population events use
``--show-field mfr_comp_desc,mfr_comp_name``.

R2 archives are cached locally under ``data/exploratory/nhtsa/`` (the project's
gitignored TSV-analysis scratch directory) keyed by the R2 basename with the
``.gz`` suffix stripped (since ``R2LandingClient.get_raw`` decompresses the
outer gzip wrapper before returning). Repeated invocations against the same
``--raw-landing-path`` skip the R2 round-trip. Override the cache directory
via ``--cache-dir`` if needed.

Use after a bronze drift diagnostic confirms ``rows_in_path == 1`` in
every (10-tuple, path) cell (replacement, not additive). Cross-reference
the cell values from this output against the diagnostic's Q2 row dump
to determine which TSV cells correspond to which bronze stored values.

Usage (Mack 2026-05-09 NULL-regression cluster — canonical H1 case;
default show-field of bgman,endman):

    python scripts/nhtsa/tsv_analysis/inspect_archive_row.py \\
        --raw-landing-path nhtsa/2026-05-09/78530d14-7794-4f53-9316-f7eb2a83a89a.zip.gz \\
        --campno 26V261000 \\
        --mfr-comp-ptno 24710104

Usage (Pierce 2026-05-15 mfr_comp_desc empty→"Software" population event):

    python scripts/nhtsa/tsv_analysis/inspect_archive_row.py \\
        --raw-landing-path <post-amendment-archive>.zip.gz \\
        --campno 26V217000 \\
        --mfr-comp-ptno "Any version prior to 08.15" \\
        --show-field mfr_comp_desc,mfr_comp_name

Usage with a pre-fetched local zip (skips R2 + cache lookup):

    python scripts/nhtsa/tsv_analysis/inspect_archive_row.py \\
        --zip data/exploratory/nhtsa/78530d14-7794-4f53-9316-f7eb2a83a89a.zip \\
        --campno 26V261000 \\
        --mfr-comp-ptno 24710104
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import (  # noqa: E402 — sys.path manipulation must come first
    NAME_TO_INDEX,
    inner_sha256_prefix,
    iter_tsv_rows,
)

CAMPNO_IDX = NAME_TO_INDEX["campno"]
MAKETXT_IDX = NAME_TO_INDEX["maketxt"]
MODELTXT_IDX = NAME_TO_INDEX["modeltxt"]
YEARTXT_IDX = NAME_TO_INDEX["yeartxt"]
MFR_COMP_PTNO_IDX = NAME_TO_INDEX["mfr_comp_ptno"]

DEFAULT_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "exploratory" / "nhtsa"


def _fetch_from_r2(key: str) -> bytes:
    from src.config.settings import Settings  # noqa: PLC0415
    from src.landing.r2 import R2LandingClient  # noqa: PLC0415

    settings = Settings()  # type: ignore[call-arg]
    client = R2LandingClient(settings)
    return client.get_raw(key)


def _resolve_cached(raw_landing_path: str, cache_dir: Path) -> Path:
    """Return the local cache path for an R2 key; fetch from R2 on cache miss."""
    basename = Path(raw_landing_path).name
    if basename.endswith(".gz"):
        basename = basename[:-3]
    cached = cache_dir / basename
    if cached.exists():
        print(f"# Cache hit: {cached}", file=sys.stderr)
        return cached
    print(f"# Cache miss; fetching from R2: {raw_landing_path}", file=sys.stderr)
    zip_bytes = _fetch_from_r2(raw_landing_path)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(zip_bytes)
    print(f"# Saved to: {cached}", file=sys.stderr)
    return cached


def _inspect(
    zip_path: Path,
    campno: str,
    mfr_comp_ptno: str,
    show_fields: tuple[str, ...],
) -> int:
    sha = inner_sha256_prefix(zip_path)
    print(f"# Archive:           {zip_path.name}")
    print(f"# Inner-TSV SHA-256: {sha}…")
    print(f"# Filter:            campno={campno!r}, mfr_comp_ptno={mfr_comp_ptno!r}")
    print(f"# Showing fields:    {', '.join(f.upper() for f in show_fields)}")
    print()

    field_indices: list[tuple[str, int]] = [(name, NAME_TO_INDEX[name]) for name in show_fields]
    label_width = max(len(name.upper()) for name in show_fields)
    counters: dict[str, dict[str, int]] = {
        name: {"empty": 0, "populated": 0} for name in show_fields
    }

    matched = 0
    total = 0

    for fields in iter_tsv_rows(zip_path):
        total += 1
        if fields[CAMPNO_IDX] != campno:
            continue
        if fields[MFR_COMP_PTNO_IDX] != mfr_comp_ptno:
            continue
        matched += 1

        cells: list[tuple[str, str]] = []
        for name, idx in field_indices:
            raw = fields[idx] if len(fields) > idx else ""
            cells.append((name, raw))
            counters[name]["empty" if raw == "" else "populated"] += 1

        print(f"--- match #{matched} ---")
        print(f"  RECORD_ID    : {fields[0]}")
        print(f"  CAMPNO       : {fields[CAMPNO_IDX]}")
        print(f"  MAKETXT      : {fields[MAKETXT_IDX]}")
        print(f"  MODELTXT     : {fields[MODELTXT_IDX]}")
        print(f"  YEARTXT      : {fields[YEARTXT_IDX]}")
        print(f"  MFR_COMP_PTNO: {fields[MFR_COMP_PTNO_IDX]}")
        for name, raw in cells:
            label = name.upper().ljust(label_width)
            print(f"  {label} (raw): {raw!r}  (len={len(raw)})")
        print()

    print(f"# Total TSV rows scanned: {total}")
    print(f"# Matched rows:           {matched}")
    print()
    for name in show_fields:
        print(f"# {name.upper()} distribution among matched rows:")
        print(f"#   empty ('')           : {counters[name]['empty']}")
        print(f"#   populated            : {counters[name]['populated']}")
    print()
    print("# Verdict guidance:")
    print("#   For each matched row, compare the raw TSV cell value(s) above")
    print("#   against the corresponding bronze cell:")
    print("#     TSV value matches bronze stored value")
    print("#       → H1 (upstream-driven; NHTSA published this).")
    print("#     TSV value differs from bronze stored value")
    print("#       → H2 (extractor mis-parse; bug in src/extractors/nhtsa.py).")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--raw-landing-path",
        help=(
            "R2 key (e.g. 'nhtsa/2026-05-09/<uuid>.zip.gz'); "
            "fetched via R2LandingClient and cached."
        ),
    )
    source_group.add_argument(
        "--zip",
        type=Path,
        help="Path to a local NHTSA .zip file (already unwrapped from .gz).",
    )
    parser.add_argument(
        "--campno",
        required=True,
        help="Filter TSV rows to this campno (e.g. '26V261000').",
    )
    parser.add_argument(
        "--mfr-comp-ptno",
        required=True,
        help="Filter TSV rows to this mfr_comp_ptno (e.g. '24710104').",
    )
    parser.add_argument(
        "--show-field",
        default="bgman,endman",
        help=(
            "Comma-separated lowercase TSV field names to dump per matched row. "
            "Default 'bgman,endman' matches the original NULL-regression workflow. "
            "For mfr_comp_desc-class population events use 'mfr_comp_desc,mfr_comp_name'. "
            "Known fields are listed in scripts/nhtsa/tsv_analysis/_lib.py FIELD_NAMES."
        ),
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Directory to cache R2 downloads (default: {DEFAULT_CACHE_DIR}). Gitignored.",
    )
    args = parser.parse_args()

    show_fields = tuple(name.strip().lower() for name in args.show_field.split(","))
    for name in show_fields:
        if name not in NAME_TO_INDEX:
            known = ", ".join(sorted(NAME_TO_INDEX))
            print(
                f"ERROR: Unknown --show-field {name!r}. Known fields: {known}",
                file=sys.stderr,
            )
            return 2

    if args.raw_landing_path:
        try:
            zip_path = _resolve_cached(args.raw_landing_path, args.cache_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: R2 fetch failed: {exc}", file=sys.stderr)
            return 2
        return _inspect(zip_path, args.campno, args.mfr_comp_ptno, show_fields)
    if not args.zip.exists():
        print(f"ERROR: {args.zip} not found", file=sys.stderr)
        return 2
    return _inspect(args.zip, args.campno, args.mfr_comp_ptno, show_fields)


if __name__ == "__main__":
    sys.exit(main())
