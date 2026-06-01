"""Chunked, resumable driver for the USCG manufacturer-detail historical seed.

The full detail seed is ~16,263 detail-page fetches at ~1s/page ≈ ~4.5h. Run as a
single monolithic ``deep-rescan`` it accumulates every page in memory and writes
bronze ONCE at the very end — so any blip in 4.5h (a dropped wifi connection, a
Neon DNS hiccup, a laptop sleep) discards the whole run. (Observed 2026-06-01:
~68 min of scraping lost to a ``could not translate host name … neon.tech``
DNS failure on plane wifi, which surfaced only at the final DB write.)

This driver instead loops the **incremental** ``extract`` path in chunks:

    recalls extract uscg_manufacturer_details --limit N --change-type=historical_seed

Why that is resumable (no new extractor code, no checkpoint table):
- ``UscgManufacturerDetailExtractor`` sources its work-list from
  ``uscg_manufacturers_bronze`` and (incremental path) includes only MICs with no
  detail row yet OR whose listing row is newer than their detail row, ordered by
  ``uscg_directory_id`` (``_build_work_list(incremental=True)``).
- Each ``extract`` invocation is one ``run()`` → one bronze transaction, COMMITTED
  before the next chunk starts. A loaded MIC gets a detail row and DROPS OUT of the
  next chunk's work-list. So the work-list strictly shrinks by ``loaded`` each chunk
  and the loop is guaranteed to terminate.
- ``--limit N`` caps each chunk to the first N work-list items (already tested:
  tests/extractors/test_uscg_manufacturer_detail_extractor.py).

Running each chunk as a FRESH SUBPROCESS is deliberate and the key robustness win on
a flaky connection: every chunk re-resolves DNS and opens a clean SQLAlchemy engine /
connection pool, so a transient network failure costs at most one chunk (~N seconds),
never the whole corpus — and re-running this driver simply resumes from the
shrunk work-list. On a chunk failure the driver stops and prints the (identical)
resume command; nothing is lost.

NOTE the seed uses the INCREMENTAL ``extract`` path, NOT ``deep-rescan``. On an empty
detail table the two work-lists are identical (every MIC needs a first fetch), and only
``extract``'s work-list shrinks as MICs load — ``deep-rescan``'s full-sweep work-list
does not, so ``deep-rescan --limit`` would re-fetch the same first N every chunk. The
deep-rescan loader stays the right tool for periodic detail-only ``Date Modified``
drift re-scans (Phase 7), not the initial seed.

This script SHELLS OUT to ``recalls`` (it does not touch the DB or network directly);
all extraction, validation, dedup, and bronze writes happen inside the proven CLI path.

Usage (run yourself; this spawns the real extractor → network + DB):
    python scripts/uscg/seed_manufacturer_details_chunked.py
    python scripts/uscg/seed_manufacturer_details_chunked.py --limit 500 --sleep 5
    python scripts/uscg/seed_manufacturer_details_chunked.py --dry-run
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from typing import NamedTuple

# The chunk command. uscg_manufacturer_details is the only source that honors
# --limit (its work-list is bronze-derived); change-type=historical_seed labels
# the extraction_runs rows so recall_event_history can filter the seed wave.
_SOURCE = "uscg_manufacturer_details"
_RECALLS_BIN = "recalls"

# The CLI run-summary line: "<source>: fetched=N loaded=N rejected=N"
# (src/cli/main.py::_print_run_summary). Specific enough not to match JSON log lines.
_SUMMARY_RE = re.compile(r"fetched=(\d+)\s+loaded=(\d+)\s+rejected=(\d+)")


class ChunkResult(NamedTuple):
    """Parsed fetched/loaded/rejected counts from one chunk's run summary."""

    fetched: int
    loaded: int
    rejected: int


def parse_chunk_output(stdout: str) -> ChunkResult | None:
    """Extract the (fetched, loaded, rejected) summary from a chunk's stdout.

    Scans for the LAST match of the run-summary pattern (the summary is the final
    line a successful ``extract`` prints; scanning last is robust to any trailing
    output). Returns None if no summary line is present (e.g. the chunk crashed
    before printing one).
    """
    matches = list(_SUMMARY_RE.finditer(stdout))
    if not matches:
        return None
    m = matches[-1]
    return ChunkResult(fetched=int(m.group(1)), loaded=int(m.group(2)), rejected=int(m.group(3)))


def decide_after_chunk(result: ChunkResult, limit: int) -> str:
    """Decide the loop's next move from one chunk's result. Pure.

    Returns one of:
    - ``"done"``     — the work-list is drained (this chunk fetched fewer than the
      cap, or nothing at all); stop successfully.
    - ``"continue"`` — a full chunk that made progress; more work remains.
    - ``"stall"``    — a FULL chunk (fetched == limit) that loaded NOTHING. The
      work-list did not shrink, so continuing would loop forever on the same items
      (e.g. persistent validation failures at the front of the list). Stop and flag.

    Termination guarantee: a ``"continue"`` requires ``loaded >= 1``, and every
    loaded MIC permanently drops from the work-list, so the work-list strictly
    shrinks until ``fetched < limit`` (done) or ``fetched == 0`` (done).
    """
    if result.fetched == 0:
        return "done"
    if result.fetched < limit:
        return "done"
    # fetched == limit (a full chunk)
    if result.loaded == 0:
        return "stall"
    return "continue"


def _chunk_command(limit: int, change_type: str) -> list[str]:
    """Build the ``recalls extract`` argv for one chunk."""
    return [
        _RECALLS_BIN,
        "extract",
        _SOURCE,
        "--limit",
        str(limit),
        "--change-type",
        change_type,
    ]


def _run_chunk(cmd: list[str]) -> tuple[int, str]:
    """Run one chunk subprocess, tee-ing its output live AND capturing it.

    Streams the child's combined stdout/stderr to our stdout as it arrives (so the
    operator sees live progress) while accumulating it for summary parsing. Returns
    ``(returncode, captured_output)``. I/O boundary — not unit-tested with a real
    process; the pure parse/decide logic above is.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    captured: list[str] = []
    assert proc.stdout is not None  # PIPE
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        captured.append(line)
    returncode = proc.wait()
    return returncode, "".join(captured)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Detail pages per chunk (default 500 ≈ ~8 min at ~1s/page).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=5.0,
        help="Seconds to pause between chunks (lets a flaky connection settle; default 5).",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=100,
        help="Safety backstop on the chunk count (default 100; ~16.3k/500 ≈ 33 real chunks).",
    )
    parser.add_argument(
        "--chunk-retries",
        type=int,
        default=2,
        help="Retries for a failed chunk before aborting (default 2; for transient DNS/wifi).",
    )
    parser.add_argument(
        "--change-type",
        default="historical_seed",
        help="extraction_runs label for each chunk (default historical_seed).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the chunk command and config, then exit without running.",
    )
    args = parser.parse_args()

    if args.limit < 1:
        print("ERROR: --limit must be >= 1", file=sys.stderr)
        return 2

    cmd = _chunk_command(args.limit, args.change_type)
    if args.dry_run:
        print("=== dry run — would loop the following per chunk ===")
        print("  " + " ".join(cmd))
        print(
            f"  ... repeating until the work-list drains "
            f"(loaded MICs drop out each chunk), max {args.max_chunks} chunks, "
            f"{args.sleep}s between chunks, {args.chunk_retries} retries per failed chunk."
        )
        return 0

    total_fetched = 0
    total_loaded = 0
    total_rejected = 0
    for chunk_num in range(1, args.max_chunks + 1):
        print(f"\n=== chunk {chunk_num} (limit={args.limit}) ===", flush=True)

        returncode, output = _run_chunk(cmd)
        attempt = 0
        while returncode != 0 and attempt < args.chunk_retries:
            attempt += 1
            print(
                f"chunk {chunk_num} exited {returncode}; retry {attempt}/{args.chunk_retries} "
                f"after {args.sleep}s (transient connectivity?)",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(args.sleep)
            returncode, output = _run_chunk(cmd)

        if returncode != 0:
            print(
                f"\nABORTED: chunk {chunk_num} failed after {args.chunk_retries} retries "
                f"(exit {returncode}). Completed chunks are committed — fix connectivity and "
                f"re-run this same command to resume from the remaining work-list.",
                file=sys.stderr,
            )
            return 1

        result = parse_chunk_output(output)
        if result is None:
            print(
                f"\nABORTED: chunk {chunk_num} exited 0 but printed no run summary "
                f"('fetched=… loaded=… rejected=…'). Cannot confirm progress; re-run to resume.",
                file=sys.stderr,
            )
            return 1

        total_fetched += result.fetched
        total_loaded += result.loaded
        total_rejected += result.rejected
        decision = decide_after_chunk(result, args.limit)
        print(
            f"chunk {chunk_num}: fetched={result.fetched} loaded={result.loaded} "
            f"rejected={result.rejected} → {decision} "
            f"(cumulative loaded={total_loaded})",
            flush=True,
        )

        if decision == "done":
            print(
                f"\nDONE: work-list drained in {chunk_num} chunk(s). "
                f"total fetched={total_fetched} loaded={total_loaded} rejected={total_rejected}."
            )
            return 0
        if decision == "stall":
            print(
                f"\nSTALL: chunk {chunk_num} fetched a full {args.limit} but loaded 0 — the "
                f"work-list is not shrinking (persistent validation failures at the front?). "
                f"Stopping to avoid an infinite loop. Inspect "
                f"uscg_manufacturer_details_rejected, then resume.",
                file=sys.stderr,
            )
            return 1
        time.sleep(args.sleep)

    print(
        f"\nSTOPPED: hit the --max-chunks guard ({args.max_chunks}) with work remaining. "
        f"total loaded so far={total_loaded}. Re-run to continue, or raise --max-chunks.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
