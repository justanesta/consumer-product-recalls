"""Characterize inner-TSV byte drift between two NHTSA archives.

Investigates the Section M.5 finding (2026-05-25,
``documentation/nhtsa/incremental_delta_findings.md``) that
``response_inner_content_sha256`` can transition CHANGED across days even when
the bronze content_hash dedup finds zero row-level deltas (e.g., 2026-05-17 and
2026-05-18 NHTSA runs: ``inner_transition = CHANGED, records_inserted = 0``).

Four candidate mechanisms for the inner-hash noise floor on no-change days:

  * RECORD_ID — NHTSA's TSV places RECORD_ID (the per-build sequence number,
                Finding C) in column 0. RECORD_ID is reassigned across daily
                archive builds even when underlying recall content is
                unchanged. ADR 0030's ``hash_exclude_fields={source_recall_id}``
                instructs bronze to ignore this column when canonicalizing.
                This script's default ``--strip-record-id`` mode drops column
                0 before diffing so the comparison reaches the logical
                content, not the build-sequence noise.
  * REORDER  — Same logical rows in a different physical order. Inner SHA
               differs; set of stripped lines is identical; sorted-content
               SHA matches.
  * WHITESPACE — Trailing whitespace or column padding differs on some rows.
                 Inner SHA differs; set of stripped lines differs at the byte
                 level; each differing line has a whitespace-normalized
                 partner in the other archive.
  * REAL_CHANGE — A line truly differs in content (different field value)
                  even after stripping RECORD_ID. Would normally produce
                  ``records_inserted > 0`` via bronze dedup; surfacing this
                  on a "no-change day" would falsify the framing.

The script reports a verdict per pair: IDENTICAL / REORDER / WHITESPACE /
MIXED / REAL_CHANGE. The verdict applies to the post-strip view by default;
pass ``--include-record-id`` to see the raw byte-level diff (which will
nearly always classify as REAL_CHANGE for NHTSA per the column-0 re-numbering
artifact — useful for confirming the artifact's prevalence but not for
characterizing logical content drift).

R2 archives are cached locally under ``data/exploratory/nhtsa/`` (matches
``inspect_archive_row.py``'s convention). Repeated invocations against the
same ``--raw-landing-path`` values skip the R2 round-trip.

Usage (canonical case: 5/16, 5/17, 5/18 NHTSA archives):

    python scripts/nhtsa/tsv_analysis/diff_inner_tsv.py \\
        --raw-landing-path nhtsa/2026-05-16/4521edda-d165-4e8d-90ec-577ac51abb3a.zip.gz \\
        --raw-landing-path nhtsa/2026-05-17/8a545adc-28f6-4cf4-acca-8d4bc9bb0c87.zip.gz \\
        --raw-landing-path nhtsa/2026-05-18/89f6cad0-6709-45b2-b07f-b228e11bc5dc.zip.gz

The script diffs each adjacent pair (16↔17, 17↔18). Pass more or fewer paths
as needed; minimum two.

For a pair from local pre-fetched zips (skip R2 + cache):

    python scripts/nhtsa/tsv_analysis/diff_inner_tsv.py \\
        --zip data/exploratory/nhtsa/4521edda-...zip \\
        --zip data/exploratory/nhtsa/8a545adc-...zip
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import inner_sha256_prefix  # noqa: E402 — sys.path manipulation must come first

DEFAULT_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "exploratory" / "nhtsa"

REORDER_VERDICT = "REORDER"
WHITESPACE_VERDICT = "WHITESPACE"
MIXED_VERDICT = "MIXED"
REAL_CHANGE_VERDICT = "REAL_CHANGE"
IDENTICAL_VERDICT = "IDENTICAL"


def _fetch_from_r2(key: str) -> bytes:
    from src.config.settings import Settings  # noqa: PLC0415
    from src.landing.r2 import R2LandingClient  # noqa: PLC0415

    settings = Settings()  # type: ignore[call-arg]
    client = R2LandingClient(settings)
    return client.get_raw(key)


def _resolve_cached(raw_landing_path: str, cache_dir: Path) -> Path:
    """Return the local cache path; fetch from R2 on cache miss."""
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


def _inner_txt_name(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith(".txt"):
                return name
    raise ValueError(f"No .txt member found inside {zip_path}")


def _strip_record_id_column(line: bytes) -> bytes:
    """Drop column 0 (RECORD_ID) from a tab-delimited TSV line.

    NHTSA's TSV emits RECORD_ID as the leftmost column, reassigned per
    archive build (Finding C). Stripping it surfaces the logical content
    drift; keeping it surfaces the per-build sequence-number artifact.
    Returns the line as-is if no tab is found (malformed / empty line).
    """
    tab_idx = line.find(b"\t")
    if tab_idx == -1:
        return line
    return line[tab_idx + 1 :]


def _read_lines(zip_path: Path, strip_record_id: bool) -> list[bytes]:
    """Read the inner TSV as a list of line bytes (line endings stripped).

    When ``strip_record_id=True`` (the default for NHTSA semantics), column 0
    (RECORD_ID) is dropped from each line before returning.
    """
    inner = _inner_txt_name(zip_path)
    with zipfile.ZipFile(zip_path) as zf, zf.open(inner) as f:
        # Strip CR/LF on each line so trailing newline style doesn't dominate the diff.
        lines = [line.rstrip(b"\r\n") for line in f]
    if strip_record_id:
        lines = [_strip_record_id_column(line) for line in lines]
    return lines


def _sorted_sha(lines: Iterable[bytes]) -> str:
    """SHA-256 over the sorted set of lines — order-independent fingerprint."""
    h = hashlib.sha256()
    for line in sorted(lines):
        h.update(line)
        h.update(b"\n")
    return h.hexdigest()[:16]


def _whitespace_normalized(line: bytes) -> bytes:
    """Aggressive whitespace normalization for the WHITESPACE verdict heuristic.

    Each TSV field (tab-delimited) has leading/trailing whitespace stripped.
    The number of fields is preserved so two lines that differ only in
    field-internal whitespace are still distinguishable.
    """
    return b"\t".join(field.strip() for field in line.split(b"\t"))


def _diff_pair(a_path: Path, b_path: Path, strip_record_id: bool) -> dict[str, object]:
    """Compute the diff verdict + supporting metrics for a single pair."""
    a_lines = _read_lines(a_path, strip_record_id=strip_record_id)
    b_lines = _read_lines(b_path, strip_record_id=strip_record_id)

    a_inner_sha = inner_sha256_prefix(a_path)
    b_inner_sha = inner_sha256_prefix(b_path)
    a_sorted_sha = _sorted_sha(a_lines)
    b_sorted_sha = _sorted_sha(b_lines)

    a_count = len(a_lines)
    b_count = len(b_lines)

    # Multiset semantics: same line appearing twice in A and once in B is a delta.
    # Use a Counter under the hood.
    from collections import Counter

    a_counter = Counter(a_lines)
    b_counter = Counter(b_lines)
    only_a = a_counter - b_counter  # multiset subtraction
    only_b = b_counter - a_counter

    n_only_a = sum(only_a.values())
    n_only_b = sum(only_b.values())

    # Normalize whitespace for residual lines and re-diff. Lines that match
    # post-normalization were whitespace-class drift.
    a_norm_counter = Counter(_whitespace_normalized(line) for line in only_a.elements())
    b_norm_counter = Counter(_whitespace_normalized(line) for line in only_b.elements())
    residual_only_a = a_norm_counter - b_norm_counter
    residual_only_b = b_norm_counter - a_norm_counter
    n_residual_a = sum(residual_only_a.values())
    n_residual_b = sum(residual_only_b.values())

    n_whitespace_pairs = (
        n_only_a - n_residual_a
    )  # lines from A that found a normalized partner in B

    # Verdict.
    if a_inner_sha == b_inner_sha:
        verdict = IDENTICAL_VERDICT
    elif a_sorted_sha == b_sorted_sha:
        verdict = REORDER_VERDICT
    elif n_residual_a == 0 and n_residual_b == 0:
        verdict = WHITESPACE_VERDICT
    elif n_whitespace_pairs == 0:
        verdict = REAL_CHANGE_VERDICT
    else:
        verdict = MIXED_VERDICT

    samples = {
        "only_a_samples": [
            line.decode("utf-8", errors="replace")[:200] for line in list(only_a.elements())[:5]
        ],
        "only_b_samples": [
            line.decode("utf-8", errors="replace")[:200] for line in list(only_b.elements())[:5]
        ],
        "residual_only_a_samples": [
            line.decode("utf-8", errors="replace")[:200]
            for line in list(residual_only_a.elements())[:5]
        ],
        "residual_only_b_samples": [
            line.decode("utf-8", errors="replace")[:200]
            for line in list(residual_only_b.elements())[:5]
        ],
    }

    return {
        "a_path": str(a_path),
        "b_path": str(b_path),
        "a_inner_sha": a_inner_sha,
        "b_inner_sha": b_inner_sha,
        "a_sorted_sha": a_sorted_sha,
        "b_sorted_sha": b_sorted_sha,
        "a_line_count": a_count,
        "b_line_count": b_count,
        "n_only_a": n_only_a,
        "n_only_b": n_only_b,
        "n_whitespace_pairs": n_whitespace_pairs,
        "n_residual_only_a": n_residual_a,
        "n_residual_only_b": n_residual_b,
        "verdict": verdict,
        "samples": samples,
    }


def _print_pair(result: dict[str, object]) -> None:
    print(f"=== {Path(str(result['a_path'])).name} <-> {Path(str(result['b_path'])).name} ===")
    print(f"  A inner SHA-256:  {result['a_inner_sha']}…")
    print(f"  B inner SHA-256:  {result['b_inner_sha']}…")
    print(f"  A sorted-SHA-256: {result['a_sorted_sha']}…")
    print(f"  B sorted-SHA-256: {result['b_sorted_sha']}…")
    print(f"  A line count:     {result['a_line_count']}")
    print(f"  B line count:     {result['b_line_count']}")
    print(f"  Lines only in A:  {result['n_only_a']}")
    print(f"  Lines only in B:  {result['n_only_b']}")
    print(f"  Whitespace-paired drift count: {result['n_whitespace_pairs']}")
    print(f"  Residual-only-A (real or padding):  {result['n_residual_only_a']}")
    print(f"  Residual-only-B (real or padding):  {result['n_residual_only_b']}")
    print()
    print(f"  VERDICT: {result['verdict']}")
    print()

    verdict = result["verdict"]
    samples = result["samples"]  # type: ignore[index]

    if verdict in (REORDER_VERDICT, IDENTICAL_VERDICT):
        return

    if verdict == WHITESPACE_VERDICT:
        print("  Sample of whitespace-pair drift (A version):")
        for line in samples["only_a_samples"][:3]:  # type: ignore[index]
            print(f"    {line!r}")
        print("  Sample (B version):")
        for line in samples["only_b_samples"][:3]:  # type: ignore[index]
            print(f"    {line!r}")
        print()
        return

    # MIXED or REAL_CHANGE — show residual samples (the genuine deltas)
    print("  Sample residual-only-A (no whitespace partner in B):")
    for line in samples["residual_only_a_samples"][:5]:  # type: ignore[index]
        print(f"    {line!r}")
    print("  Sample residual-only-B (no whitespace partner in A):")
    for line in samples["residual_only_b_samples"][:5]:  # type: ignore[index]
        print(f"    {line!r}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--raw-landing-path",
        action="append",
        default=[],
        help="R2 raw landing path (e.g. nhtsa/2026-05-17/...zip.gz). Repeat for >=2 paths.",
    )
    parser.add_argument(
        "--zip",
        action="append",
        default=[],
        help="Local zip path (skips R2 fetch). Repeat for >=2 paths.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="Local cache directory for R2 fetches (default: data/exploratory/nhtsa).",
    )
    parser.add_argument(
        "--include-record-id",
        action="store_true",
        help=(
            "Compare lines with RECORD_ID (column 0) included. Default is to "
            "strip RECORD_ID since it's a per-build sequence number "
            "(Finding C) and ADR 0030's bronze dedup excludes it. Use this "
            "flag to see the raw byte-level diff for diagnostic purposes."
        ),
    )
    args = parser.parse_args()
    strip_record_id = not args.include_record_id

    if args.raw_landing_path and args.zip:
        print("Error: pass either --raw-landing-path or --zip, not both.", file=sys.stderr)
        return 2

    if args.raw_landing_path:
        zip_paths = [_resolve_cached(rp, args.cache_dir) for rp in args.raw_landing_path]
    elif args.zip:
        zip_paths = [Path(z) for z in args.zip]
    else:
        print("Error: provide --raw-landing-path or --zip (>=2 paths).", file=sys.stderr)
        return 2

    if len(zip_paths) < 2:
        print("Error: need at least 2 paths to diff.", file=sys.stderr)
        return 2

    mode = (
        "RECORD_ID stripped (logical content)"
        if strip_record_id
        else "raw bytes (RECORD_ID included)"
    )
    print(f"# Diff mode: {mode}")
    print()

    # Diff each adjacent pair in order.
    for a, b in zip(zip_paths, zip_paths[1:], strict=False):
        result = _diff_pair(a, b, strip_record_id=strip_record_id)
        _print_pair(result)

    return 0


if __name__ == "__main__":
    sys.exit(main())
