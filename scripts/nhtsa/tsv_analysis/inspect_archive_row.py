"""Inspect raw NHTSA archive rows by (campno, mfr_comp_ptno).

Downloads a NHTSA archive from R2 (or reads a local .zip), filters the inner
TSV by supplied identity values, and dumps the BGMAN/ENDMAN cells with
character-level visibility via ``repr()``. Discriminates between two failure
modes for populated→NULL bronze transitions surfaced by
``scripts/sql/nhtsa/bronze/diagnose_null_regression.sql``:

  * H1 — UPSTREAM DEPOPULATION. NHTSA emitted empty cells. BGMAN/ENDMAN
         render as ``''`` (empty string, len=0). Operationally normal —
         a recall amendment expanded scope to units of unknown
         manufacturing date.
  * H2 — EXTRACTOR MIS-PARSE. NHTSA emitted populated dates but our
         ``FlatFileExtractor`` produced NULL in bronze. BGMAN/ENDMAN
         render as a non-empty string (e.g., ``'20250708'``).
         Operationally a bug — investigate the cell-to-Pydantic-date
         mapping in ``src/extractors/nhtsa.py``.

R2 archives are cached locally under ``data/exploratory/nhtsa/`` (the project's
gitignored TSV-analysis scratch directory) keyed by the R2 basename with the
``.gz`` suffix stripped (since ``R2LandingClient.get_raw`` decompresses the
outer gzip wrapper before returning). Repeated invocations against the same
``--raw-landing-path`` skip the R2 round-trip. Override the cache directory
via ``--cache-dir`` if needed.

Use after ``diagnose_null_regression.sql`` confirms ``rows_in_path == 1`` in
every (10-tuple, path) cell (replacement, not additive). Cross-reference the
populated/empty cells from this output against the same script's Q2 row dump
to determine which TSV cells correspond to which bronze NULLs.

Usage (Mack 2026-05-09 NULL-regression cluster, the canonical case):

    python scripts/nhtsa/tsv_analysis/inspect_archive_row.py \\
        --raw-landing-path nhtsa/2026-05-09/78530d14-7794-4f53-9316-f7eb2a83a89a.zip.gz \\
        --campno 26V261000 \\
        --mfr-comp-ptno 24710104

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
BGMAN_IDX = NAME_TO_INDEX["bgman"]
ENDMAN_IDX = NAME_TO_INDEX["endman"]

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


def _inspect(zip_path: Path, campno: str, mfr_comp_ptno: str) -> int:
    sha = inner_sha256_prefix(zip_path)
    print(f"# Archive:           {zip_path.name}")
    print(f"# Inner-TSV SHA-256: {sha}…")
    print(f"# Filter:            campno={campno!r}, mfr_comp_ptno={mfr_comp_ptno!r}")
    print()

    matched = 0
    total = 0
    empty_bgman = 0
    populated_bgman = 0
    empty_endman = 0
    populated_endman = 0

    for fields in iter_tsv_rows(zip_path):
        total += 1
        if fields[CAMPNO_IDX] != campno:
            continue
        if fields[MFR_COMP_PTNO_IDX] != mfr_comp_ptno:
            continue
        matched += 1

        bgman_raw = fields[BGMAN_IDX] if len(fields) > BGMAN_IDX else ""
        endman_raw = fields[ENDMAN_IDX] if len(fields) > ENDMAN_IDX else ""

        if bgman_raw == "":
            empty_bgman += 1
        else:
            populated_bgman += 1
        if endman_raw == "":
            empty_endman += 1
        else:
            populated_endman += 1

        print(f"--- match #{matched} ---")
        print(f"  RECORD_ID    : {fields[0]}")
        print(f"  CAMPNO       : {fields[CAMPNO_IDX]}")
        print(f"  MAKETXT      : {fields[MAKETXT_IDX]}")
        print(f"  MODELTXT     : {fields[MODELTXT_IDX]}")
        print(f"  YEARTXT      : {fields[YEARTXT_IDX]}")
        print(f"  MFR_COMP_PTNO: {fields[MFR_COMP_PTNO_IDX]}")
        print(f"  BGMAN  (raw) : {bgman_raw!r}  (len={len(bgman_raw)})")
        print(f"  ENDMAN (raw) : {endman_raw!r}  (len={len(endman_raw)})")
        print()

    print(f"# Total TSV rows scanned: {total}")
    print(f"# Matched rows:           {matched}")
    print()
    print("# BGMAN distribution among matched rows:")
    print(f"#   empty ('')           : {empty_bgman}")
    print(f"#   populated            : {populated_bgman}")
    print("# ENDMAN distribution among matched rows:")
    print(f"#   empty ('')           : {empty_endman}")
    print(f"#   populated            : {populated_endman}")
    print()
    print("# Verdict guidance:")
    print("#   Any matched cell empty AND bronze stored that cell as NULL")
    print("#     → H1 (upstream depopulation; normal recall amendment).")
    print("#   Any matched cell populated AND bronze stored that cell as NULL")
    print("#     → H2 (extractor mis-parse; bug in src/extractors/nhtsa.py).")
    print("#   Cross-reference each matched (maketxt, modeltxt, yeartxt) row's")
    print("#   BGMAN/ENDMAN above against diagnose_null_regression.sql Q2 output")
    print("#   for the same raw_landing_path.")

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
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Directory to cache R2 downloads (default: {DEFAULT_CACHE_DIR}). Gitignored.",
    )
    args = parser.parse_args()

    if args.raw_landing_path:
        try:
            zip_path = _resolve_cached(args.raw_landing_path, args.cache_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: R2 fetch failed: {exc}", file=sys.stderr)
            return 2
        return _inspect(zip_path, args.campno, args.mfr_comp_ptno)
    if not args.zip.exists():
        print(f"ERROR: {args.zip} not found", file=sys.stderr)
        return 2
    return _inspect(args.zip, args.campno, args.mfr_comp_ptno)


if __name__ == "__main__":
    sys.exit(main())
