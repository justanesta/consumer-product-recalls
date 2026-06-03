"""Probe FDA's Tier-2 lookup endpoint to settle the ``*short``/``*indicator`` question.

The capture-expansion audit needs to know, from *observed data* (not the
Definitions PDF), whether the lookup-only ``*short`` fields are cleanly
derivable from the full-length fields we already land in bronze:

    productdescriptionshort  ?=  LEFT(productdescriptiontxt, N)
    recallreasonshort        ?=  LEFT(productshortreasontxt, N)
    codeinfoshort            ?=  LEFT(codeinformation, N)

and what the three ``*indicator`` fields actually hold.

If the shorts are clean character prefixes (and the indicators are just
"was-truncated" flags) → skip Tier-2, derive in silver, now proven. If FDA
truncates smartly (word boundaries / ellipsis / curation) → the shorts are NOT
cleanly derivable and Tier-2 enrichment earns its place.

## Why this is a script and not a chat paste

``codeinformation`` is unbounded free text (up to ~205k chars per the bronze
schema). Pasting even a handful of full Tier-2 payloads into a conversation
blows the context window — and the comparison is mechanical anyway. This script
fetches each product, runs the prefix/boundary/ellipsis checks in memory, and
prints only a compact verdict. The full payloads are saved to
``data/exploratory/fda/probes/`` (gitignored) for re-analysis without re-calling
the API; **stdout stays tiny**.

Does NOT land to R2, does NOT load to bronze, does NOT touch the watermark.
Read-only against bronze (to pick a length-spread of diagnostic product ids)
plus N point GETs against the lookup endpoint.

## HTTP stack

Mirrors ``scripts/fda/audit/probe_displaycolumns.py`` and production's
``src/extractors/fda.py``: httpx + Mozilla UA + auth headers only. The lookup
endpoint is ``GET /recalls/product/{productid}`` (per
``bruno/fda/lookup/get_product_by_id.yml``), which — unlike the bulk POST —
*does* return the ``*short``/``*indicator`` columns (Finding K0: those 406 on
the bulk POST). FDA's success STATUSCODE for this endpoint is ``400`` (its
quirk; see the Bruno tests and ``probe_displaycolumns.py``).

## Two questions, two selection modes

  • **Truncation mechanics** (default) — pull a length-spread of *extreme* ids
    (longest codeinfo + shortest desc) and dump a per-record verdict. Answers
    "when a short exists, is it a prefix / where is it cut?".
  • **Prevalence** (``--sample N``) — pull N *random* ids and report population
    rates + the whitespace-blind ``content ⊆ full`` count. Answers "how often is
    a short populated, and does it ever carry content not already in bronze?".
    The decision-grade number is ``content ⊆ full``: if it equals the populated
    count for every field, the shorts are whitespace-normalized truncations of
    data we already store → zero net-new content → skip Tier-2.

## Usage

```
# Truncation mechanics — extreme ids
python scripts/fda/audit/probe_tier2_shorts.py
python scripts/fda/audit/probe_tier2_shorts.py --product-ids 219875,344120

# Prevalence — random sample (cost: N GETs at --sleep each)
python scripts/fda/audit/probe_tier2_shorts.py --sample 200

# Offline re-analysis of a saved run — zero API/DB calls
python scripts/fda/audit/probe_tier2_shorts.py \\
    --from-file data/exploratory/fda/probes/tier2_shorts_<ts>.json
```
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import sqlalchemy as sa
from sqlalchemy import Engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import DEFAULT_CACHE_DIR  # noqa: E402

from src.config.db import make_engine  # noqa: E402
from src.config.settings import Settings  # noqa: E402

_FDA_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"

# Exact UA from src/extractors/fda.py — the default python-httpx UA is suspected
# to trip FDA's anti-abuse throttle on the first request (Finding N).
_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

_DEFAULT_PROBE_DIR = DEFAULT_CACHE_DIR / "probes"

# (short_field, full_field): the lookup-only *short variant and the full-length
# field it is hypothesized to truncate. Lowercased; lookup matches case-insensitively.
_SHORT_FULL_PAIRS: list[tuple[str, str]] = [
    ("productdescriptionshort", "productdescriptiontxt"),
    ("recallreasonshort", "productshortreasontxt"),
    ("codeinfoshort", "codeinformation"),
]
_INDICATOR_FIELDS: list[str] = [
    "productdescriptionindicator",
    "recallreasonindicator",
    "codeinfoindicator",
]

_ELLIPSIS_MARKERS: tuple[str, ...] = ("…", "...")

# Sample-mode persistence caps each full-text field at this many chars (well past
# any observed *short length) so a large random sample can't write a multi-GB file.
_SAVE_FULL_CAP = 4000


# --------------------------------------------------------------------------- #
# Pure comparison layer (unit-tested in tests/scripts/test_probe_tier2_shorts.py)
# --------------------------------------------------------------------------- #


def rows_from_result(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise FDA's RESULT into a list of dict rows.

    The lookup endpoint may return RESULT as a list of dicts, or in columnar
    form (``RESULT.COLUMNS`` + ``RESULT.DATA``). Mirrors the ``rowsFromResult``
    helper in ``bruno/fda/lookup/get_product_by_id.yml``.
    """
    result = body.get("RESULT")
    if isinstance(result, list):
        return [r for r in result if isinstance(r, dict)]
    if isinstance(result, dict):
        cols = result.get("COLUMNS")
        data = result.get("DATA")
        if isinstance(cols, list) and isinstance(data, list):
            return [dict(zip(cols, row, strict=False)) for row in data if isinstance(row, list)]
    return []


def get_field(record: dict[str, Any], field: str) -> Any:
    """Case-insensitive field lookup (FDA may return UPPER, lower, or mixed keys)."""
    target = field.lower()
    for key, value in record.items():
        if key.lower() == target:
            return value
    return None


def _trailing_ellipsis(text: str) -> str | None:
    for marker in _ELLIPSIS_MARKERS:
        if text.endswith(marker):
            return marker
    return None


def _strip_ws(text: str) -> str:
    """All whitespace removed — newlines, tabs, runs of spaces — for a content-only
    comparison. FDA's ``*short`` fields strip newlines and collapse tabs, so a raw
    prefix test reports ``not_prefix`` on what is really the same text reflowed."""
    return "".join(text.split())


def compare_short_to_full(short: Any, full: Any) -> dict[str, Any]:
    """Classify how a ``*short`` value relates to its full-length field.

    The ``verdict`` is the load-bearing output:

      - ``null``                        — one or both values absent
      - ``identical``                   — short == full (no truncation happened)
      - ``not_prefix``                  — short is NOT a character prefix of full
                                          (curated / different text → NOT derivable)
      - ``ellipsis_truncation``         — short is ``<prefix> + "..."`` / "…"
                                          (truncated with an explicit marker)
      - ``clean_prefix_word_boundary``  — naive prefix, cut lands at a word boundary
      - ``clean_prefix_midword``        — naive prefix, cut lands mid-word
                                          (strongest signal of a fixed-N char cut)

    ``content_subset`` is the decision-grade signal: it ignores ALL whitespace
    and asks whether the short's text sits at the head of the full's text. It is
    ``True`` even for ``not_prefix`` rows whose only divergence is FDA stripping
    newlines / collapsing tabs — i.e. the short carries no content beyond a
    whitespace-normalized truncation of ``full``. ``False`` means genuinely
    different (curated) text.

    ``cut_at_word_boundary`` is ``None`` when not applicable (null / identical /
    not-a-prefix). Per-record boundary hits can be coincidental; the aggregate
    (is ``short_len`` constant? is the cut ever mid-word?) is what decides
    fixed-N vs. curation.
    """
    short_ok = isinstance(short, str) and short != ""
    full_ok = isinstance(full, str) and full != ""
    out: dict[str, Any] = {
        "short_len": len(short) if short_ok else None,
        "full_len": len(full) if full_ok else None,
        "short_null": not short_ok,
        "full_null": not full_ok,
        "is_clean_prefix": None,
        "has_trailing_ellipsis": None,
        "cut_at_word_boundary": None,
        "content_subset": None,
        "verdict": "null",
    }
    if not short_ok or not full_ok:
        return out

    marker = _trailing_ellipsis(short)
    core = short[: len(short) - len(marker)] if marker else short
    out["has_trailing_ellipsis"] = marker is not None
    out["content_subset"] = _strip_ws(full).startswith(_strip_ws(core))

    if short == full:
        out.update(is_clean_prefix=True, cut_at_word_boundary=None, verdict="identical")
        return out

    is_prefix = full.startswith(core)
    out["is_clean_prefix"] = is_prefix
    if not is_prefix:
        out["verdict"] = "not_prefix"
        return out

    boundary: bool | None = None
    if 0 < len(core) < len(full):
        char_after = full[len(core)]
        last_char = core[-1]
        boundary = char_after.isspace() or last_char.isspace()
    out["cut_at_word_boundary"] = boundary

    if marker:
        out["verdict"] = "ellipsis_truncation"
    elif boundary:
        out["verdict"] = "clean_prefix_word_boundary"
    else:
        out["verdict"] = "clean_prefix_midword"
    return out


def _preview(value: Any, head: int = 48) -> str:
    """A bounded, single-line repr so a 205k-char field contributes ~48 chars."""
    if not isinstance(value, str):
        return repr(value)
    flattened = value.replace("\n", "⏎").replace("\r", "")
    if len(flattened) <= head:
        return repr(flattened)
    return repr(flattened[:head] + "…")


def _head(value: Any, n: int = 100) -> str:
    """First ``n`` chars as a single-line repr — bounded regardless of input size,
    so dumping a not_prefix head can never blow the context window."""
    if not isinstance(value, str):
        return repr(value)
    flattened = value.replace("\n", "⏎").replace("\r", "")
    return repr(flattened[:n])


def _project_for_save(record: dict[str, Any]) -> dict[str, Any]:
    """Reduce a record to the analyzed fields, capping long full-text values.

    A random prevalence sample can span thousands of products, some carrying a
    multi-megabyte ``codeinformation``; saving raw payloads would write a
    multi-GB file. Sample-mode persistence keeps only the analyzed columns and
    caps each full-text field at ``_SAVE_FULL_CAP`` chars (well beyond any
    observed ``*short`` length, so prefix re-tests stay valid) plus its true
    length in a ``_<field>_full_len`` sidecar key.
    """
    projected: dict[str, Any] = {}
    for short_f, full_f in _SHORT_FULL_PAIRS:
        projected[short_f] = get_field(record, short_f)
        full_v = get_field(record, full_f)
        if isinstance(full_v, str) and len(full_v) > _SAVE_FULL_CAP:
            projected[full_f] = full_v[:_SAVE_FULL_CAP]
            projected[f"_{full_f}_full_len"] = len(full_v)
        else:
            projected[full_f] = full_v
    for ind_f in _INDICATOR_FIELDS:
        projected[ind_f] = get_field(record, ind_f)
    for id_field in ("productid", "recalleventid"):
        projected[id_field] = get_field(record, id_field)
    return projected


def probed_from_payload(payload: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Rebuild the ``probed`` list from a saved probe JSON (offline re-analysis).

    Mirrors the ``{"records": {product_id: record}}`` shape written by
    ``_save_payloads``; tolerates anything else by returning an empty list.
    """
    records = payload.get("records")
    if not isinstance(records, dict):
        return []
    return [(str(pid), rec) for pid, rec in records.items() if isinstance(rec, dict)]


def build_verdict_report(probed: list[tuple[str, dict[str, Any]]]) -> str:
    """Build the compact, paste-safe stdout report from fetched records.

    ``probed`` is a list of ``(product_id, record_dict)``. Output is one line
    per (record, field-pair) plus an aggregate; full field values never appear
    beyond a bounded preview.
    """
    if not probed:
        return "No records fetched — nothing to compare."

    lines: list[str] = ["=== Tier-2 *short vs full-field comparison ==="]
    pair_cmps: dict[tuple[str, str], list[dict[str, Any]]] = {p: [] for p in _SHORT_FULL_PAIRS}
    indicator_values: dict[str, list[Any]] = {ind: [] for ind in _INDICATOR_FIELDS}

    for product_id, record in probed:
        lines.append(f"\n--- product {product_id} ---")
        for short_f, full_f in _SHORT_FULL_PAIRS:
            short_v = get_field(record, short_f)
            full_v = get_field(record, full_f)
            cmp = compare_short_to_full(short_v, full_v)
            pair_cmps[(short_f, full_f)].append(cmp)
            lines.append(
                f"  {short_f} <-> {full_f}: {cmp['verdict']} | "
                f"short_len={cmp['short_len']} full_len={cmp['full_len']} | "
                f"ellipsis={cmp['has_trailing_ellipsis']} boundary={cmp['cut_at_word_boundary']} | "
                f"short={_preview(short_v)}"
            )
        ind_parts = []
        for ind in _INDICATOR_FIELDS:
            value = get_field(record, ind)
            indicator_values[ind].append(value)
            ind_parts.append(f"{ind}={value!r}")
        lines.append("  indicators: " + " ".join(ind_parts))

    lines.append("\n=== Aggregate ===")
    for pair in _SHORT_FULL_PAIRS:
        cmps = pair_cmps[pair]
        verdict_counts = dict(Counter(c["verdict"] for c in cmps))
        short_lens = sorted({c["short_len"] for c in cmps if c["short_len"] is not None})
        lines.append(f"  {pair[0]} <-> {pair[1]}:")
        lines.append(f"    verdicts: {verdict_counts}")
        lines.append(f"    short_len distinct: {short_lens}")
    lines.append("  indicators (distinct raw values observed):")
    for ind in _INDICATOR_FIELDS:
        distinct = sorted({repr(v) for v in indicator_values[ind]})
        lines.append(f"    {ind}: {distinct}")

    lines.append("\n=== How to read this ===")
    lines.append(
        "  clean_prefix_midword across records + constant short_len -> naive LEFT(full, N); "
        "derivable in silver, skip Tier-2."
    )
    lines.append(
        "  ellipsis_truncation / not_prefix / varying short_len on word boundaries -> "
        "curated; NOT cleanly derivable, Tier-2 earns its place."
    )
    return "\n".join(lines)


def build_population_report(
    probed: list[tuple[str, dict[str, Any]]],
    total_products: int | None = None,
    max_not_prefix_examples: int = 5,
) -> str:
    """Prevalence-focused report over a RANDOM sample.

    Answers the two numbers the include/skip decision needs: how often each
    ``*short`` is populated, and — when populated — whether it is a derivable
    truncation (``clean_prefix_*``) or curated net-new text (``not_prefix``).
    Aggregate-only: no per-record dump, and full fields appear only as bounded
    heads for the handful of ``not_prefix`` cases.
    """
    if not probed:
        return "No records fetched — nothing to scan."

    n = len(probed)
    lines: list[str] = ["=== Tier-2 population scan (random sample) ==="]
    lines.append(f"  sample size (successful GETs): {n}")
    denom = str(total_products) if total_products is not None else "unknown"
    lines.append(f"  bronze distinct products (denominator): {denom}")

    # net_new = populated shorts whose content (ignoring whitespace) is NOT a head
    # of the full field. These are the only genuinely-curated cases; a raw
    # not_prefix that is content_subset==True is just whitespace reflow.
    net_new_cases: list[tuple[str, str, Any, Any]] = []

    lines.append("\n=== Prevalence + nature when populated ===")
    for short_f, full_f in _SHORT_FULL_PAIRS:
        populated = 0
        content_subset_count = 0
        verdicts_when_pop: Counter[str] = Counter()
        short_lens: list[int] = []
        for product_id, record in probed:
            short_v = get_field(record, short_f)
            full_v = get_field(record, full_f)
            cmp = compare_short_to_full(short_v, full_v)
            if cmp["short_null"]:
                continue
            populated += 1
            verdicts_when_pop[cmp["verdict"]] += 1
            if cmp["short_len"] is not None:
                short_lens.append(cmp["short_len"])
            if cmp["content_subset"]:
                content_subset_count += 1
            else:
                net_new_cases.append((product_id, short_f, short_v, full_v))
        pct = (populated / n * 100) if n else 0.0
        lines.append(f"  {short_f} <-> {full_f}:")
        lines.append(f"    populated: {populated}/{n} ({pct:.1f}%)")
        lines.append(
            f"    content ⊆ full (whitespace-blind): {content_subset_count}/{populated}  "
            "<- the decision number"
        )
        lines.append(f"    raw verdicts when populated: {dict(verdicts_when_pop)}")
        if short_lens:
            lines.append(
                f"    short_len: min={min(short_lens)} max={max(short_lens)} "
                f"distinct={len(set(short_lens))}"
            )

    lines.append("\n=== Indicator cross-tab (does indicator == 'short present'?) ===")
    for (short_f, _full_f), ind_f in zip(_SHORT_FULL_PAIRS, _INDICATOR_FIELDS, strict=True):
        crosstab: dict[str, Counter[str]] = {}
        for _product_id, record in probed:
            ind_v = repr(get_field(record, ind_f))
            short_v = get_field(record, short_f)
            present = (
                "short_present" if isinstance(short_v, str) and short_v != "" else "short_null"
            )
            crosstab.setdefault(ind_v, Counter())[present] += 1
        rendered = {k: dict(v) for k, v in crosstab.items()}
        lines.append(f"  {ind_f}: {rendered}")

    if net_new_cases:
        shown = net_new_cases[:max_not_prefix_examples]
        lines.append(
            f"\n=== GENUINELY net-new cases (content NOT ⊆ full even ignoring whitespace) — "
            f"{len(net_new_cases)} total, showing {len(shown)} ==="
        )
        for product_id, field, short_v, full_v in shown:
            lines.append(f"  product {product_id} {field}:")
            lines.append(f"    short head: {_head(short_v)}")
            lines.append(f"    full  head: {_head(full_v)}")
    else:
        lines.append(
            "\n=== net-new cases: NONE — every populated short's content is ⊆ its full "
            "field (whitespace-blind). No content Tier-2 could add. ==="
        )

    lines.append("\n=== How to read this ===")
    lines.append(
        "  content ⊆ full == populated for every field -> the shorts are whitespace-normalized "
        "truncations of data already in bronze; zero net-new content, skip Tier-2."
    )
    lines.append(
        "  a non-trivial net-new count -> some shorts are genuinely curated text; only THEN "
        "does Tier-2 carry content worth the ~134K-GET fan-out."
    )
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# I/O layer (side effects behind functions; not unit-tested)
# --------------------------------------------------------------------------- #


def fetch_diagnostic_product_ids(engine: Engine, n_long: int, n_short: int) -> list[str]:
    """Pick a length-spread of product ids from bronze.

    ``n_long`` rows with the longest ``code_information`` (truncation should be
    visible if the shorts are prefixes) plus ``n_short`` rows with a short
    ``product_description_txt`` (where short should equal full if it's a naive
    truncation). De-duplicated, long ids first.
    """
    long_sql = sa.text(
        "SELECT source_recall_id FROM fda_recalls_bronze "
        "WHERE code_information IS NOT NULL "
        "ORDER BY length(code_information) DESC LIMIT :n"
    )
    short_sql = sa.text(
        "SELECT source_recall_id FROM fda_recalls_bronze "
        "WHERE product_description_txt IS NOT NULL "
        "AND length(product_description_txt) BETWEEN 20 AND 60 "
        "ORDER BY source_recall_id LIMIT :n"
    )
    ids: list[str] = []
    with engine.connect() as conn:
        for (sid,) in conn.execute(long_sql, {"n": n_long}):
            ids.append(str(sid))
        for (sid,) in conn.execute(short_sql, {"n": n_short}):
            text_id = str(sid)
            if text_id not in ids:
                ids.append(text_id)
    return ids


def count_distinct_products(engine: Engine) -> int:
    """Denominator for prevalence: distinct product ids currently in bronze."""
    sql = sa.text("SELECT count(DISTINCT source_recall_id) FROM fda_recalls_bronze")
    with engine.connect() as conn:
        return int(conn.execute(sql).scalar_one())


def fetch_random_product_ids(engine: Engine, n: int) -> list[str]:
    """A corpus-fair random sample of N distinct product ids (NOT extremes).

    ``ORDER BY random()`` over the distinct-id set — fine at bronze's scale
    (~10^5 products). This is the right selection for measuring *prevalence*;
    ``fetch_diagnostic_product_ids`` (extremes) is for truncation *mechanics*.
    """
    sql = sa.text(
        "SELECT source_recall_id FROM "
        "(SELECT DISTINCT source_recall_id FROM fda_recalls_bronze "
        "WHERE source_recall_id IS NOT NULL) s "
        "ORDER BY random() LIMIT :n"
    )
    with engine.connect() as conn:
        return [str(sid) for (sid,) in conn.execute(sql, {"n": n})]


def fetch_product(
    client: httpx.Client, settings: Settings, product_id: str
) -> tuple[dict[str, Any] | None, str | None]:
    """GET one product from the Tier-2 lookup endpoint.

    Returns ``(record, None)`` on success or ``(None, error_message)`` on any
    failure. FDA's success STATUSCODE for this endpoint is 400.
    """
    assert settings.fda_authorization_user is not None  # validated by caller
    assert settings.fda_authorization_key is not None
    url = f"{_FDA_BASE_URL}/recalls/product/{product_id}?signature={int(time.time())}"
    try:
        response = client.get(
            url,
            headers={
                "Authorization-User": settings.fda_authorization_user.get_secret_value(),
                "Authorization-Key": settings.fda_authorization_key.get_secret_value(),
            },
        )
    except httpx.TransportError as exc:
        return None, f"network failure: {exc}"

    if "text/html" in response.headers.get("Content-Type", ""):
        return None, (
            f"anti-abuse throttle (HTTP {response.status_code}, HTML body). "
            "Wait >=30 min before retrying."
        )
    if response.status_code == 204:
        return None, "HTTP 204 No Content (Akamai edge silent block / rate limit)."
    if response.status_code != 200:
        return None, f"HTTP {response.status_code}: {response.text[:200]}"

    body = response.json()
    status = body.get("STATUSCODE")
    if status != 400:
        return None, f"FDA STATUSCODE {status}: {body.get('MESSAGE')}"

    rows = rows_from_result(body)
    if not rows:
        return None, "STATUSCODE 400 but RESULT held no rows."
    return rows[0], None


def _save_payloads(
    probed: list[tuple[str, dict[str, Any]]],
    probe_dir: Path,
    total_products: int | None = None,
) -> Path:
    probe_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = probe_dir / f"tier2_shorts_{ts}.json"
    path.write_text(
        json.dumps(
            {
                "fetched_at": ts,
                "total_products": total_products,
                "records": {pid: record for pid, record in probed},
            },
            indent=2,
            default=str,
        )
    )
    return path


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--product-ids",
        default=None,
        help="Comma-separated product ids to probe (overrides bronze self-pull).",
    )
    parser.add_argument(
        "--long",
        type=int,
        default=4,
        help="N longest-codeinformation rows to pull from bronze (default: 4).",
    )
    parser.add_argument(
        "--short",
        type=int,
        default=3,
        help="N short-description rows to pull from bronze (default: 3).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help=(
            "PREVALENCE MODE: probe N RANDOM products (not extremes) and report "
            "per-field population rates + nature-when-populated. Use this to size "
            "productdescriptionshort/recallreasonshort for the Tier-2 decision. "
            "Cost is N GETs at --sleep each."
        ),
    )
    parser.add_argument(
        "--from-file",
        type=Path,
        default=None,
        help=(
            "OFFLINE: re-analyze a saved probe JSON (prevalence report) with zero "
            "API/DB calls. Use to re-classify a prior --sample run after changing "
            "the comparison logic."
        ),
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=8.0,
        help="Seconds to pause between GETs to respect FDA's throttle (default: 8.0).",
    )
    parser.add_argument(
        "--probe-dir",
        type=Path,
        default=_DEFAULT_PROBE_DIR,
        help=f"Where to save full payloads (default: {_DEFAULT_PROBE_DIR}). Gitignored.",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Throwaway probe — do not persist the full payloads to disk.",
    )
    args = parser.parse_args()

    # Offline re-analysis: no auth, no DB, no API. Re-classify a saved run.
    if args.from_file is not None:
        payload = json.loads(args.from_file.read_text())
        probed = probed_from_payload(payload)
        total = payload.get("total_products") if isinstance(payload, dict) else None
        print(
            f"# Offline re-analysis of {len(probed)} record(s) from {args.from_file}.",
            file=sys.stderr,
        )
        print(build_population_report(probed, total))
        return 0 if probed else 2

    settings = Settings()  # type: ignore[call-arg]
    if settings.fda_authorization_user is None or settings.fda_authorization_key is None:
        print(
            "ERROR: FDA_AUTHORIZATION_USER and FDA_AUTHORIZATION_KEY must be set.",
            file=sys.stderr,
        )
        return 2

    total_products: int | None = None
    if args.product_ids:
        product_ids = [p.strip() for p in args.product_ids.split(",") if p.strip()]
        print(f"# Using {len(product_ids)} product id(s) from --product-ids.", file=sys.stderr)
    elif args.sample:
        engine = make_engine(settings.neon_database_url.get_secret_value())
        try:
            total_products = count_distinct_products(engine)
            product_ids = fetch_random_product_ids(engine, args.sample)
        finally:
            engine.dispose()
        print(
            f"# Random sample of {len(product_ids)} from {total_products} distinct "
            "bronze products (prevalence mode).",
            file=sys.stderr,
        )
    else:
        engine = make_engine(settings.neon_database_url.get_secret_value())
        try:
            product_ids = fetch_diagnostic_product_ids(engine, args.long, args.short)
        finally:
            engine.dispose()
        print(
            f"# Pulled {len(product_ids)} diagnostic id(s) from bronze "
            f"({args.long} long-codeinfo + up to {args.short} short-desc).",
            file=sys.stderr,
        )
    if not product_ids:
        print("ERROR: no product ids to probe.", file=sys.stderr)
        return 2

    probed: list[tuple[str, dict[str, Any]]] = []
    failures: list[tuple[str, str]] = []
    with httpx.Client(
        timeout=60.0,
        follow_redirects=True,
        headers={"User-Agent": _USER_AGENT},
    ) as client:
        for i, product_id in enumerate(product_ids):
            if i > 0:
                time.sleep(args.sleep)
            print(f"# GET product {product_id} ({i + 1}/{len(product_ids)})", file=sys.stderr)
            record, error = fetch_product(client, settings, product_id)
            if error is not None:
                print(f"#   FAILED: {error}", file=sys.stderr)
                failures.append((product_id, error))
                continue
            assert record is not None
            probed.append((product_id, record))

    if not args.no_save and probed:
        to_save = [(pid, _project_for_save(rec)) for pid, rec in probed] if args.sample else probed
        saved = _save_payloads(to_save, args.probe_dir, total_products)
        kind = "projected" if args.sample else "full"
        print(f"# Saved {len(to_save)} {kind} payload(s) to: {saved}", file=sys.stderr)

    if args.sample and not args.product_ids:
        print(build_population_report(probed, total_products))
    else:
        print(build_verdict_report(probed))

    if failures:
        print(f"\n# {len(failures)} id(s) failed:", file=sys.stderr)
        for product_id, error in failures:
            print(f"#   {product_id}: {error}", file=sys.stderr)
    return 0 if probed else 2


if __name__ == "__main__":
    sys.exit(main())
