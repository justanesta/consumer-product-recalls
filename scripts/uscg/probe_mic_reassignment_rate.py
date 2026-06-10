"""Reassignment-rate probe for the USCG manufacturer directory (detail pages).

Decision tool for `project_scope/implementation_plan.md` Step 7 follow-up:
should we build Path B (detail-page enrichment) + an SCD-2 `firm_uscg_attributes`
dim + a HIN-build-date time-aware recall->manufacturer join? That depends on how
often a MIC has held more than one business over time — and, more sharply, on how
often a *recalled* MIC has been reassigned (the actual misattribution surface).

The listing pages we ingest today carry no succession signal. The per-manufacturer
detail page (`manufacturers-identification-detail.php?id=N`, confirmed to answer a
direct GET despite its `class="iframe"` markup — see
`documentation/uscg/manufacturer_scraping_observations.md` §M.2) carries
`Past Company 1-3 (OOB year)`, `In Business`, `Parent MIC`, `DBA`, and `Date Modified`.
This probe samples those detail pages and reports the reassignment rate.

"Reassigned" here = the detail page lists >=1 non-empty `Past Company`. (A rename
under the same owner can also produce a Past Company entry; `Parent MIC` / `DBA`,
captured here, help distinguish true code-recycling from corporate renames — read
the sample dump and the parent-MIC stat before drawing a hard conclusion. Honors
the project's observation-vs-inference discipline.)

This is an ad-hoc exploratory probe (mirrors `scripts/uscg/inspect_landing_ndjson.py`
in spirit: forensic, no production-pipeline coupling). It reuses the polite-scraper
conventions from `src/extractors/_html_scraping.py` — honest User-Agent, >=1s
minimum-inter-request throttle, fresh client per fetch — but does NOT import the
extractor, so it cannot accidentally write bronze.

PARSER CAVEAT: the detail-page HTML structure was inferred from the live render
(§M.2), not from a saved fixture. The value-extraction strategy mirrors the recalls
`_parse_details_page` (`<strong>LABEL</strong>` -> sibling cell). The probe prints
the fully-parsed dict for the first `--sample-dump` records (default 3) BEFORE the
aggregate stats so you can eyeball that values land in the right fields. If the dump
shows garbage, stop and tell me — the parser needs a structural tweak, not the stats.

Usage:

    # Default run: random 1,000 ids, 1s throttle, ~18 min, cached.
    python scripts/uscg/probe_mic_reassignment_rate.py

    # Quick smoke test against the two known-reassigned records first.
    python scripts/uscg/probe_mic_reassignment_rate.py --ids 655,1786 --sample-dump 2

    # Bigger sample + the decision-critical bridge metric (needs DB env).
    python scripts/uscg/probe_mic_reassignment_rate.py --sample-size 2000 --db-bridge

    # EXACT misattribution surface: probe only the recalled MICs (needs DB env).
    python scripts/uscg/probe_mic_reassignment_rate.py --recalled-only

The bridge metric (`--db-bridge`) joins the sampled reassigned MICs against
`uscg_recalls_bronze.mic` (case-insensitive, per §L.3) to size how many MICs we
actually have recalls for are reassigned — the number that decides whether the
time-aware join is worth building.

`--recalled-only` is the EXACT alternative to the sampled bridge: instead of
sampling, it probes only the detail pages of MICs that appear in
`uscg_recalls_bronze` (resolved to their stored `uscg_directory_id`). ~700
fetches, no extrapolation — the precise count of recalls whose manufacturer
attribution is time-sensitive, plus the affected MIC list in the output artifact.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from collections import Counter
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup, Tag

_BASE_URL = "https://uscgboating.org/content"
_DETAIL_URL = f"{_BASE_URL}/manufacturers-identification-detail.php"

# Corpus size per Finding D (Records Found: 16263). Detail ids are the
# alphabetical rank of the MIC (confirmed §M.1), so the valid id range is
# [1, 16263].
_DEFAULT_MAX_ID = 16263

_DEFAULT_CACHE_DIR = Path("data/exploratory/uscg_manufacturers/detail_probe_cache")
_DEFAULT_OUT = Path("data/exploratory/uscg_manufacturers/reassigned_mics.json")

# Normalized detail-page label -> result key. Normalization: strip, drop a
# trailing colon, lowercase (mirrors uscg.py `_normalize_label`). Unknown
# labels are recorded and reported, not fatal — this is an exploratory probe.
_LABEL_MAP: dict[str, str] = {
    "mic": "mic",
    "company": "company",
    "dba": "dba",
    "parent company": "parent_company",
    "parent mic": "parent_mic",
    "past company 1": "past_company_1",
    "past company 2": "past_company_2",
    "past company 3": "past_company_3",
    "address": "address",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "country": "country",
    "phone": "phone",
    "fax": "fax",
    "status": "status",
    "company official": "company_official",
    "in business": "in_business",
    "out of business": "out_of_business",
    "date modified": "date_modified",
    "type": "type",
    "additional address": "additional_address",
}

_PAST_COMPANY_KEYS = ("past_company_1", "past_company_2", "past_company_3")
# Matches "(OOB 1978)", "(OOB 2008)", "(OOB)" — captures the 4-digit year if present.
_OOB_RE = re.compile(r"\(OOB\s*(\d{4})?\s*\)", re.IGNORECASE)
# Any "(OOB..." marker, year-or-not: higher-confidence "prior holder ceased,
# MIC recycled" signal than a bare predecessor name (which may be a rename).
_OOB_MARKER_RE = re.compile(r"\(OOB", re.IGNORECASE)
# Trailing "M/D/YYYY" or "YYYY" date used to pull a year out of Date Modified.
_YEAR_RE = re.compile(r"(\d{4})")


def _normalize_label(text: str) -> str:
    """strip → drop trailing colon → lowercase (matches uscg.py)."""
    return text.strip().rstrip(":").strip().lower()


def _user_agent() -> str:
    """Honest UA matching `_html_scraping.HtmlScrapingExtractor._headers`."""
    try:
        ver = _pkg_version("consumer-product-recalls")
    except Exception:  # noqa: BLE001 — best-effort UA construction
        ver = "0.0.0"
    return f"consumer-product-recalls/{ver} (contact: adriannesta@gmail.com)"


def _value_for_label(strong: Tag) -> str:
    """Return the value paired with a `<strong>LABEL</strong>` cell.

    The detail page lays each visual row out as 5 cells:
    `[left-label][left-value][&nbsp; spacer][right-label][right-value]`
    (confirmed against cached HTML for ids 655/1786). The value is the label
    cell's IMMEDIATE next-sibling `<td>`; an empty value cell yields "". The
    Strategy 2/3 fallbacks below only run when there is no sibling cell at all
    (non-table layout) — defensive, not exercised by the current page.
    """
    # Strategy 1 — recalls pattern: parent <td>/<th> → next sibling cell.
    label_cell = strong.find_parent(["td", "th"])
    if isinstance(label_cell, Tag):
        value_cell = label_cell.find_next_sibling("td")
        if isinstance(value_cell, Tag):
            # Immediate next cell IS the value. `&nbsp;` decodes to U+00A0,
            # which str.strip() removes, so an empty value cell yields "".
            # Do NOT skip empty cells: the spacer sits AFTER the value, so
            # skipping would bleed the next label in (the original bug that
            # produced parent_company == "Parent MIC:").
            return value_cell.get_text(" ", strip=True)

    # Strategy 2 — label and value share a parent: parent text minus the label.
    parent = strong.parent
    if isinstance(parent, Tag):
        parent_text = parent.get_text(" ", strip=True)
        label_text = strong.get_text(" ", strip=True)
        remainder = parent_text.replace(label_text, "", 1).strip(" : ")
        if remainder:
            return remainder

    # Strategy 3 — value is the strong's immediate next sibling text.
    nxt = strong.next_sibling
    while nxt is not None and isinstance(nxt, str) and not nxt.strip():
        nxt = nxt.next_sibling
    if isinstance(nxt, str) and nxt.strip():
        return nxt.strip(" : ")
    return ""


def parse_detail(html: bytes) -> tuple[dict[str, Any], set[str]]:
    """Parse a manufacturer detail page → (fields, unknown_labels).

    `fields` carries every recognized label's value (empty string if blank).
    `unknown_labels` is the set of bolded labels not in `_LABEL_MAP` — surfaced
    so the probe reports any new detail-page fields.
    """
    soup = BeautifulSoup(html, "lxml")
    fields: dict[str, Any] = {}
    unknown: set[str] = set()
    for strong in soup.find_all(["strong", "b"]):
        if not isinstance(strong, Tag):
            continue
        raw = strong.get_text(strip=True)
        if not raw:
            continue
        norm = _normalize_label(raw)
        key = _LABEL_MAP.get(norm)
        if key is None:
            # Heuristic: ignore obvious non-labels (long text blocks).
            if len(norm) <= 40:
                unknown.add(norm)
            continue
        fields[key] = _value_for_label(strong)
    return fields, unknown


def fetch_detail(
    record_id: int,
    cache_dir: Path | None,
    timeout: float,
    retries: int = 3,
) -> bytes | None:
    """Fetch one detail page (cache-first). Fresh client per call (no session pinning).

    Returns the HTML bytes, or None on persistent failure. Caches the raw
    bytes under `cache_dir/<id>.html` so re-runs don't re-hit the source.
    """
    if cache_dir is not None:
        cached = cache_dir / f"{record_id}.html"
        if cached.exists():
            return cached.read_bytes()

    url = f"{_DETAIL_URL}?id={record_id}"
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            # Fresh client per fetch — mirrors _fetch_page_once (Finding M:
            # no cookie/session persistence).
            with httpx.Client(timeout=timeout, headers={"User-Agent": _user_agent()}) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                body = resp.content
                if cache_dir is not None:
                    (cache_dir / f"{record_id}.html").write_bytes(body)
                return body
            if resp.status_code == 429:
                time.sleep(float(resp.headers.get("Retry-After", 5)))
                continue
            # 4xx/5xx other than rate limit: back off briefly and retry.
            last_exc = RuntimeError(f"HTTP {resp.status_code} for id={record_id}")
        except httpx.TransportError as exc:
            last_exc = exc
        time.sleep(0.5 * (attempt + 1))
    print(f"  [warn] id={record_id} failed after {retries} attempts: {last_exc}", file=sys.stderr)
    return None


def is_reassigned(fields: dict[str, Any]) -> bool:
    """True if the record lists >=1 non-empty Past Company."""
    return any((fields.get(k) or "").strip() for k in _PAST_COMPANY_KEYS)


def oob_years(fields: dict[str, Any]) -> list[int]:
    """Years pulled from `(OOB YYYY)` annotations on Past Company entries."""
    years: list[int] = []
    for k in _PAST_COMPANY_KEYS:
        val = fields.get(k) or ""
        m = _OOB_RE.search(val)
        if m and m.group(1):
            years.append(int(m.group(1)))
    return years


def has_oob_marker(fields: dict[str, Any]) -> bool:
    """True if any Past Company carries an `(OOB...)` marker (year or not).

    Higher-confidence "the MIC was recycled from a now-defunct prior holder"
    signal than mere Past Company presence — a bare predecessor name (e.g.
    FOUR WINNS' "SAF-T-MATE", no OOB) may be a rename of the same firm rather
    than a true reassignment. AXY/COP carry `(OOB 1978)` / `(OOB 2008)`.
    """
    return any(_OOB_MARKER_RE.search(fields.get(k) or "") for k in _PAST_COMPANY_KEYS)


def _select_ids(args: argparse.Namespace) -> list[int]:
    if args.ids:
        return [int(x) for x in args.ids.split(",") if x.strip()]
    rng = random.Random(args.seed)
    return sorted(rng.sample(range(1, args.max_id + 1), k=min(args.sample_size, args.max_id)))


def _recalled_directory_ids() -> tuple[list[int], int]:
    """Detail-page ids for the distinct MICs that appear in uscg_recalls_bronze.

    Returns ``(latest directory id per recalled MIC, total distinct recalled MICs)``.
    The MIC join is case-insensitive (per §L.3 lowercase-MIC data quality, matching
    `firm.sql`); for each recalled MIC we take its latest bronze version's
    ``uscg_directory_id`` (the id is the alphabetical rank — stable per MIC but
    versioned). Local imports so the probe runs without DB env unless
    --recalled-only / --db-bridge is used.
    """
    import sqlalchemy as sa  # noqa: PLC0415 — intentional local import

    from src.config.settings import Settings  # noqa: PLC0415

    settings = Settings()  # type: ignore[call-arg]
    engine = sa.create_engine(settings.neon_database_url.get_secret_value(), pool_pre_ping=True)
    with engine.connect() as conn:
        total = conn.execute(
            sa.text(
                "select count(distinct upper(trim(mic))) from uscg_recalls_bronze "
                "where mic is not null and trim(mic) <> ''"
            )
        ).scalar_one()
        rows = conn.execute(
            sa.text(
                "with recalled as ("
                "  select distinct upper(trim(mic)) as mic from uscg_recalls_bronze "
                "  where mic is not null and trim(mic) <> ''"
                ") "
                "select distinct on (upper(trim(m.source_recall_id))) m.uscg_directory_id "
                "from uscg_manufacturers_bronze m "
                "join recalled rc on upper(trim(m.source_recall_id)) = rc.mic "
                "where m.uscg_directory_id is not null "
                "order by upper(trim(m.source_recall_id)), m.extraction_timestamp desc"
            )
        ).all()
    ids = sorted({int(row[0]) for row in rows})
    return ids, int(total)


def _run_bridge(reassigned: list[dict[str, Any]]) -> None:
    """Optional: size the recalled-MIC reassignment overlap against uscg_recalls_bronze.

    Imports are local so the probe runs without DB env when --db-bridge is off.
    """
    import sqlalchemy as sa  # noqa: PLC0415 — intentional local import

    from src.config.settings import Settings  # noqa: PLC0415

    def _mic_set(recs: list[dict[str, Any]]) -> list[str]:
        return sorted(
            {(r.get("mic") or "").strip().upper() for r in recs if (r.get("mic") or "").strip()}
        )

    mics = _mic_set(reassigned)
    recycle_mics = _mic_set([r for r in reassigned if has_oob_marker(r)])
    if not mics:
        print("\n[bridge] no reassigned MICs in sample — nothing to join.", file=sys.stderr)
        return
    settings = Settings()  # type: ignore[call-arg]
    engine = sa.create_engine(settings.neon_database_url.get_secret_value(), pool_pre_ping=True)
    with engine.connect() as conn:
        total_recall_mics = conn.execute(
            sa.text(
                "select count(distinct upper(trim(mic))) from uscg_recalls_bronze "
                "where mic is not null and trim(mic) <> ''"
            )
        ).scalar_one()
        overlap = conn.execute(
            sa.text(
                "select count(distinct upper(trim(mic))) from uscg_recalls_bronze "
                "where upper(trim(mic)) = any(:mics)"
            ),
            {"mics": mics},
        ).scalar_one()
        overlap_recycle = (
            conn.execute(
                sa.text(
                    "select count(distinct upper(trim(mic))) from uscg_recalls_bronze "
                    "where upper(trim(mic)) = any(:mics)"
                ),
                {"mics": recycle_mics},
            ).scalar_one()
            if recycle_mics
            else 0
        )
    print("\n=== Bridge metric — reassigned MICs that we have recalls for ===")
    print(f"  reassigned MICs in sample:          {len(mics)}")
    print(f"  distinct recalled MICs in bronze:   {total_recall_mics}")
    print(f"  reassigned ∩ recalled:              {overlap}")
    print(f"  ...of which (OOB)-recycled:         {len(recycle_mics)}")
    print(f"  recalled & (OOB)-recycled (hi-conf): {overlap_recycle}")
    print("  ^ misattribution surface: upper bound (any reassign) vs (OOB)-recycled.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe USCG manufacturer detail pages for the MIC reassignment rate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Random ids to sample (default 1000; ~+/-3%% at 95%% CI).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="RNG seed for a reproducible sample (default 1337).",
    )
    parser.add_argument(
        "--max-id",
        type=int,
        default=_DEFAULT_MAX_ID,
        help=f"Max detail id (default {_DEFAULT_MAX_ID}).",
    )
    parser.add_argument(
        "--ids",
        help="Comma-separated explicit ids to probe instead of sampling (e.g. 655,1786).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=1.0,
        help="Min seconds between fetches (default 1.0, polite).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout seconds (default 30).",
    )
    parser.add_argument(
        "--sample-dump",
        type=int,
        default=3,
        help="Print the full parsed dict for the first N records (default 3) to verify the parser.",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(_DEFAULT_CACHE_DIR),
        help="Dir to cache fetched HTML (default under data/exploratory/...).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable the HTML cache (always re-fetch).",
    )
    parser.add_argument(
        "--out",
        default=str(_DEFAULT_OUT),
        help="Where to write the reassigned-MIC artifact (JSON).",
    )
    parser.add_argument(
        "--db-bridge",
        action="store_true",
        help="Also join reassigned MICs against uscg_recalls_bronze (needs DB env).",
    )
    parser.add_argument(
        "--recalled-only",
        action="store_true",
        help="Probe ONLY the detail pages of MICs that appear in uscg_recalls_bronze "
        "(via their stored uscg_directory_id) for the EXACT misattribution surface. "
        "Needs DB env; ignores --sample-size / --seed / --max-id.",
    )
    args = parser.parse_args()

    cache_dir: Path | None = None if args.no_cache else Path(args.cache_dir)
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)

    total_recalled = 0
    if args.recalled_only:
        ids, total_recalled = _recalled_directory_ids()
        print(
            f"--recalled-only: {len(ids)} directory ids for "
            f"{total_recalled} distinct recalled MICs.",
            file=sys.stderr,
        )
    else:
        ids = _select_ids(args)
    cache_desc = "off" if args.no_cache else cache_dir
    print(
        f"Probing {len(ids)} detail pages (throttle={args.throttle}s, cache={cache_desc}).",
        file=sys.stderr,
    )

    records: list[dict[str, Any]] = []
    unknown_labels: set[str] = set()
    n_failed = 0
    last_ts = 0.0
    for i, rid in enumerate(ids, 1):
        # Min-inter-request throttle; cache hits don't sleep.
        cache_hit = cache_dir is not None and (cache_dir / f"{rid}.html").exists()
        if not cache_hit:
            wait = last_ts + args.throttle - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        body = fetch_detail(rid, cache_dir, args.timeout)
        if not cache_hit:
            last_ts = time.monotonic()
        if body is None:
            n_failed += 1
            continue
        fields, unknown = parse_detail(body)
        fields["_id"] = rid
        unknown_labels |= unknown
        records.append(fields)
        if i <= args.sample_dump:
            print(f"\n--- sample dump id={rid} ---")
            print(json.dumps({k: v for k, v in fields.items() if k != "_id"}, indent=2))
        if i % 100 == 0:
            print(f"  ...{i}/{len(ids)} fetched ({n_failed} failed)", file=sys.stderr)

    n_ok = len(records)
    if n_ok == 0:
        print(
            "No records parsed — check connectivity and the parser (sample dump).", file=sys.stderr
        )
        return 1

    # --- Aggregate stats ---
    reassigned = [r for r in records if is_reassigned(r)]
    recycled = [r for r in records if has_oob_marker(r)]
    rename_or_unannotated = len(reassigned) - len(recycled)
    n_oob_year = sum(1 for r in records if oob_years(r))
    # Distinct from Past-Company (OOB): this is the CURRENT holder's own
    # out-of-business date (top-level field) — the MIC's current company is
    # defunct, NOT evidence of reassignment. Both signals matter for the dim.
    n_current_oob = sum(1 for r in records if (r.get("out_of_business") or "").strip())
    past_count_dist = Counter(
        sum(1 for k in _PAST_COMPANY_KEYS if (r.get(k) or "").strip()) for r in records
    )
    oob_hist = Counter(y for r in reassigned for y in oob_years(r))
    n_parent_mic = sum(1 for r in records if (r.get("parent_mic") or "").strip())
    mod_year_hist = Counter(
        int(m.group(1)) for r in records if (m := _YEAR_RE.search(r.get("date_modified") or ""))
    )

    print("\n=== Reassignment-rate probe results ===")
    print(f"  sampled / parsed / failed:          {len(ids)} / {n_ok} / {n_failed}")
    pct = 100.0 * len(reassigned) / n_ok
    recycle_pct = 100.0 * len(recycled) / n_ok
    parent_pct = 100.0 * n_parent_mic / n_ok
    current_oob_pct = 100.0 * n_current_oob / n_ok
    print(f"  >=1 Past Company (UPPER bound):     {len(reassigned)} ({pct:.1f}%)")
    print(f"  ...of which (OOB)-marked (recycle): {len(recycled)} ({recycle_pct:.1f}%)")
    print(f"  ...Past Company but no (OOB):       {rename_or_unannotated} (rename or unannotated)")
    print(f"  records w/ a parseable OOB year:    {n_oob_year}")
    print(f"  Past-Company count distribution:    {dict(sorted(past_count_dist.items()))}")
    print(f"  records with a Parent MIC:          {n_parent_mic} ({parent_pct:.1f}%)")
    print(f"  current-holder out-of-business:     {n_current_oob} ({current_oob_pct:.1f}%)")
    if oob_hist:
        recent = sorted(oob_hist.items(), reverse=True)[:10]
        print(f"  OOB-year histogram (top 10 recent): {dict(recent)}")
    if mod_year_hist:
        mod_years = dict(sorted(mod_year_hist.items(), reverse=True))
        print(f"  Date-Modified year histogram:       {mod_years}")
    if unknown_labels:
        print(f"  [note] unrecognized bold labels seen: {sorted(unknown_labels)}")

    if args.recalled_only:
        print("\n=== EXACT misattribution surface (recalled MICs only; not sampled) ===")
        print(f"  distinct recalled MICs in bronze:    {total_recalled}")
        print(f"  ...resolved to a directory id:       {len(ids)}")
        print(f"  ...probed OK:                        {n_ok}")
        print(f"  recalled & reassigned (>=1 Past Co): {len(reassigned)} ({pct:.1f}% of probed)")
        print(
            f"  recalled & (OOB)-recycled (hi-conf): {len(recycled)} ({recycle_pct:.1f}% of probed)"
        )
        print("  ^ these recalls' manufacturer attribution is time-sensitive (flag in silver).")

    # --- Artifact ---
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    artifact = [
        {
            "id": r["_id"],
            "mic": r.get("mic"),
            "company": r.get("company"),
            "past_companies": [r.get(k) for k in _PAST_COMPANY_KEYS if (r.get(k) or "").strip()],
            "oob_years": oob_years(r),
            "has_oob_marker": has_oob_marker(r),
            "parent_mic": r.get("parent_mic"),
            "date_modified": r.get("date_modified"),
            "in_business": r.get("in_business"),
        }
        for r in reassigned
    ]
    out_path.write_text(json.dumps(artifact, indent=2))
    print(f"\n  wrote {len(artifact)} reassigned-MIC records → {out_path}")

    if args.db_bridge and not args.recalled_only:
        # --recalled-only already IS the exact surface; the sampled bridge is redundant.
        _run_bridge(reassigned)

    print(
        "\nDecision gate (implementation_plan.md Step 7 follow-up): a material recalled-MIC "
        "reassignment surface → build Path B + SCD-2 + the HIN-build-date join; negligible → "
        "document as a known limitation and keep the 'current MIC holder' silver semantic."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
