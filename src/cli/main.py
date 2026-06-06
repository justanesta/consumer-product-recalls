from __future__ import annotations

import csv
import os
from datetime import date
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

from src.bronze.recovery import (
    RECOVERY_CONFIG_BY_SOURCE_NAME,
    reason_contains,
    recover_quarantined,
    recoverable_past_date_sanity,
)
from src.config.db import make_engine
from src.config.logging import configure_logging
from src.config.settings import Settings
from src.config.source_loader import load_source_config
from src.config.source_registry import (
    DEEP_RESCAN_BY_SOURCE_NAME,
    EXTRACTOR_BY_SOURCE_NAME,
    build_extractor_kwargs,
)
from src.enrichment.crosswalk_writer import audit_rollup_clusters, resolve_firm_crosswalk

if TYPE_CHECKING:
    from src.extractors._base import Extractor

app = typer.Typer(name="recalls", help="Consumer product recalls pipeline CLI")

# Allowed values for extraction_runs.change_type (per ADR 0027 + ADR 0028,
# extended 2026-05-10 with etag_audit). DB-level CHECK constraint enforces
# the same set (migrations 0009 + 0012); the CLI validates here so the
# operator gets a clear error before the run even starts.
_ALLOWED_CHANGE_TYPES = {
    "routine",
    "schema_rebaseline",
    "hash_helper_rebaseline",
    "historical_seed",
    "etag_audit",
}

# Sources that support change_type=etag_audit. Restricted to the two USDA
# endpoints because they are the only sources currently using HTTP
# conditional GET (If-None-Match / If-Modified-Since); CPSC and FDA capture
# ETag headers passively, NHTSA uses content-MD5 on flat files. Audit-run
# semantics are only meaningful for the conditional-GET case.
_ETAG_AUDIT_SOURCES = {"usda", "usda_establishments"}

# Sources that implement ``override_watermark_lookback()`` (= sources with a
# date-watermarked incremental cursor). Other sources accept --lookback-days
# for CLI shape parity but emit a per-source notice. Driven by source name
# rather than ``hasattr(extractor, ...)`` because MagicMock auto-attributes
# would defeat the latter under test.
_LOOKBACK_DAYS_SOURCES = {"cpsc", "fda"}

# Sources that honor --limit (cap the work-list to the first N items). The
# bronze-work-list-driven extractors support it — for cheap dev validation
# (a few records exercise the full fetch->R2->bronze->dbt path) and for
# chunked/resumable seeding. Other sources emit an ignored-notice for parity.
_WORK_LIST_LIMIT_SOURCES = {"uscg_manufacturer_details", "fda_press_releases"}

# Per-source notices when --lookback-days is passed to a source that does not
# honor it (no override_watermark_lookback method). Keeps the user's existing
# CLI feedback intact under the new generic dispatch.
_LOOKBACK_NO_OP_MESSAGES: dict[str, str] = {
    "usda": "usda: --lookback-days has no effect (full-dump every run; see Finding D).",
    "usda_establishments": (
        "usda_establishments: --lookback-days has no effect (full-dump every run; see Finding A)."
    ),
    "nhtsa": (
        "nhtsa: --lookback-days has no effect (flat-file full-dump every run; see Findings B + C)."
    ),
    "uscg": (
        "uscg: --lookback-days has no effect (page-0 precheck + full HTML "
        "re-scrape on cache miss; see scraping_observations.md Finding J)."
    ),
    "uscg_manufacturers": (
        "uscg_manufacturers: --lookback-days has no effect (page-0 Records-Found "
        "precheck + full HTML re-scrape on cache miss; see "
        "manufacturer_scraping_observations.md Finding K)."
    ),
    "uscg_manufacturer_details": (
        "uscg_manufacturer_details: --lookback-days has no effect (work-list is a "
        "listing-delta cursor over bronze; see phase-5d-uscg-manufacturers-detail.md)."
    ),
    "fda_press_releases": (
        "fda_press_releases: --lookback-days is not wired; the press-release watermark "
        "defaults to today-1 on first run. Use --limit for dev, or deep-rescan to seed."
    ),
}

# Per-source notices when deep-rescan ignores --start-date/--end-date.
_DEEP_RESCAN_NO_DATE_WINDOW_MESSAGES: dict[str, str] = {
    "cpsc": (
        "cpsc: --start-date / --end-date are ignored "
        "(fixed LastPublishDateStart=1970-01-01 floor; full ~9,800-record corpus every run)."
    ),
    "usda": "usda: --start-date / --end-date are ignored (full-dump every run; see Finding D).",
    "nhtsa": (
        "nhtsa: --start-date / --end-date are ignored "
        "(archives partitioned by DATEA at the source; see Finding H Q2)."
    ),
    "uscg": (
        "uscg: --start-date / --end-date are ignored "
        "(USCG listing has no date-range query surface; full re-scrape every run)."
    ),
    "uscg_manufacturers": (
        "uscg_manufacturers: --start-date / --end-date are ignored "
        "(directory has no date-range query surface; full re-scrape every run)."
    ),
    "uscg_manufacturer_details": (
        "uscg_manufacturer_details: --start-date / --end-date are ignored "
        "(detail pages have no date-range query surface; full sweep on deep-rescan)."
    ),
    "fda_press_releases": (
        "fda_press_releases: --start-date / --end-date are ignored (the deep-rescan is a "
        "full event sweep; chunk it with --limit + --resume-after-event-id instead)."
    ),
}


def _validate_change_type(value: str) -> str:
    """Return the value or raise typer.Exit with a clear message."""
    if value not in _ALLOWED_CHANGE_TYPES:
        allowed = ", ".join(sorted(_ALLOWED_CHANGE_TYPES))
        typer.echo(
            f"Invalid --change-type {value!r}; must be one of: {allowed}",
            err=True,
        )
        raise typer.Exit(code=1)
    return value


def _validate_etag_audit_source(change_type: str, source: str) -> None:
    """Reject change_type=etag_audit for sources that don't use conditional GET."""
    if change_type == "etag_audit" and source not in _ETAG_AUDIT_SOURCES:
        allowed = ", ".join(sorted(_ETAG_AUDIT_SOURCES))
        typer.echo(
            f"--change-type=etag_audit is only supported for: {allowed} "
            f"(got source={source!r}); other sources don't use HTTP conditional GET.",
            err=True,
        )
        raise typer.Exit(code=1)


def _parse_nhtsa_since(value: str | None) -> date | None:
    """Parse the NHTSA --since flag (YYYY-MM-DD) and emit the dev-mode notice."""
    if value is None:
        return None
    try:
        since_date = date.fromisoformat(value)
    except ValueError:
        typer.echo(f"nhtsa: --since must be YYYY-MM-DD; got {value!r}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(
        f"nhtsa: --since {since_date.isoformat()} active — "
        "dev-mode RCDATE filter; bronze will be a date-bounded subset."
    )
    return since_date


def _print_run_summary(prefix: str, result: object) -> None:
    """Render the standard fetched/loaded/rejected summary line."""
    fetched = result.records_fetched  # type: ignore[attr-defined]
    loaded = result.records_loaded  # type: ignore[attr-defined]
    rejected = (
        result.records_rejected_validate  # type: ignore[attr-defined]
        + result.records_rejected_invariants  # type: ignore[attr-defined]
    )
    typer.echo(f"{prefix}fetched={fetched} loaded={loaded} rejected={rejected}")


@app.command()
def version() -> None:
    """Print the current version."""
    typer.echo(f"consumer-product-recalls {_pkg_version('consumer-product-recalls')}")


@app.command()
def extract(
    source: Annotated[str, typer.Argument(help="Source to extract (e.g. cpsc, fda)")],
    lookback_days: Annotated[
        int | None,
        typer.Option("--lookback-days", help="Override watermark with N days ago"),
    ] = None,
    change_type: Annotated[
        str,
        typer.Option(
            "--change-type",
            help=(
                "How to label this run in extraction_runs. One of: routine "
                "(default), schema_rebaseline, hash_helper_rebaseline, "
                "historical_seed, etag_audit. Required to be set explicitly when "
                "re-baselining after a schema or hashing-helper change so "
                "recall_event_history can filter the wave out of edit detection. "
                "Use etag_audit (usda + usda_establishments only) to force an "
                "unconditional GET and verify ETag-validation honesty against the "
                "most recent prior 200 — see scripts/sql/_pipeline/etag_audit_check.sql."
            ),
        ),
    ] = "routine",
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help=(
                "NHTSA only: drop rows whose RCDATE is earlier than YYYY-MM-DD. "
                "Intended for free-tier-aware dev workflows on the Neon dev "
                "branch — production historical seed uses the deep-rescan path "
                "which has no --since filter and lands the full corpus. "
                "Ignored for non-NHTSA sources."
            ),
        ),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            help=(
                "Cap the work-list to the first N items. Only "
                "uscg_manufacturer_details honors it (its work-list is derived "
                "from bronze): use a small N for cheap dev validation, or repeated "
                "capped runs for chunked/resumable seeding. Ignored by other sources."
            ),
        ),
    ] = None,
) -> None:
    """Run the incremental extractor for a given source.

    Per ADR 0012, source-specific configuration (URL, timeout, etag_enabled)
    lives in ``config/sources/<source>.yaml`` and is loaded by the
    ``source_loader``. The CLI's role is to materialize CLI-flag-specific
    behavior (lookback, since, etag_audit) on top of the YAML-driven
    extractor instance.
    """
    configure_logging()
    change_type = _validate_change_type(change_type)
    _validate_etag_audit_source(change_type, source)

    if limit is not None and limit < 1:
        typer.echo("--limit must be >= 1", err=True)
        raise typer.Exit(code=1)

    if source not in EXTRACTOR_BY_SOURCE_NAME:
        typer.echo(f"Unknown source: {source}", err=True)
        raise typer.Exit(code=1)

    if since is not None and source != "nhtsa":
        typer.echo(f"{source}: --since is only honored for nhtsa; ignored.")

    # Validate CLI flag values upfront so format errors surface BEFORE the
    # heavier ``Settings()`` / ``load_source_config`` work — keeps
    # "must be YYYY-MM-DD" errors visible without an env-var setup ritual.
    since_date: date | None = None
    if source == "nhtsa":
        since_date = _parse_nhtsa_since(since)

    config = load_source_config(source)
    settings = Settings()  # type: ignore[call-arg]  # reads from env vars
    extractor_cls = EXTRACTOR_BY_SOURCE_NAME[source]
    kwargs = build_extractor_kwargs(config, extractor_cls, settings)

    # NHTSA-only: --since is a CLI flag, not a YAML field. Always set the
    # constructor kwarg (None when the flag is absent) since NhtsaExtractor
    # declares ``since`` as a required Pydantic field with default None.
    if source == "nhtsa":
        kwargs["since"] = since_date

    extractor: Extractor = extractor_cls(**kwargs)

    # --lookback-days: only CPSC + FDA implement override_watermark_lookback;
    # other sources ignore the flag with a per-source notice.
    if lookback_days is not None:
        if source in _LOOKBACK_DAYS_SOURCES:
            extractor.override_watermark_lookback(lookback_days)  # type: ignore[attr-defined]
        else:
            typer.echo(
                _LOOKBACK_NO_OP_MESSAGES.get(
                    source, f"{source}: --lookback-days has no effect for this source."
                )
            )

    # --change-type=etag_audit: post-construction mutation. Only reachable for
    # USDA recall + USDA establishments (gated by _validate_etag_audit_source).
    # Forces unconditional GET so the audit-check SQL can verify ETag honesty.
    if change_type == "etag_audit":
        extractor.etag_enabled = False  # type: ignore[attr-defined]

    # --limit: only the bronze-work-list-driven detail extractor caps its
    # work-list (mirrors the etag_audit post-construction mutation). Other
    # sources get an ignored-notice for CLI shape parity.
    if limit is not None:
        if source in _WORK_LIST_LIMIT_SOURCES:
            extractor.work_list_limit = limit  # type: ignore[attr-defined]
        else:
            typer.echo(
                f"{source}: --limit has no effect "
                "(only uscg_manufacturer_details caps a bronze work-list)."
            )

    result = extractor.run(change_type=change_type)
    _print_run_summary(f"{source}: ", result)


@app.command(name="deep-rescan")
def deep_rescan(
    source: Annotated[str, typer.Argument(help="Source to deep-rescan (e.g. fda, usda)")],
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="Start date (YYYY-MM-DD); required for FDA"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="End date (YYYY-MM-DD); required for FDA"),
    ] = None,
    change_type: Annotated[
        str,
        typer.Option(
            "--change-type",
            help=(
                "How to label this run in extraction_runs. Use historical_seed for "
                "one-time multi-year backfills (e.g., the CPSC 2005-2024 gap per "
                "ADR 0028 Mechanism A); leave at routine for periodic edit-detection "
                "rescans. One of: routine (default), schema_rebaseline, "
                "hash_helper_rebaseline, historical_seed."
            ),
        ),
    ] = "routine",
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            help=(
                "fda_press_releases only: cap the event sweep to the first N events "
                "(recall_event_id order). Pair with --resume-after-event-id for "
                "chunked/resumable seeding. Ignored by other sources."
            ),
        ),
    ] = None,
    resume_after_event_id: Annotated[
        int | None,
        typer.Option(
            "--resume-after-event-id",
            help=(
                "fda_press_releases only: skip events with recall_event_id <= N — the "
                "chunked-seed cursor. Run --limit N, note the last event id from the "
                "work-list log, then --resume-after-event-id <id> --limit N. Ignored "
                "by other sources."
            ),
        ),
    ] = None,
) -> None:
    """Run a historical / deep-rescan load for a given source over a date window.

    Same loader+registry pattern as ``extract``; the source-specific extras
    are FDA's ``set_date_range`` mutator (post-construction) and the
    ``etag_enabled`` exclusion that preserves ``UsdaDeepRescanLoader``'s
    class invariant against YAML's ``etag_enabled: true`` (correct for the
    routine path but invariant-violating for deep-rescan).
    """
    configure_logging()
    change_type = _validate_change_type(change_type)

    if source not in DEEP_RESCAN_BY_SOURCE_NAME:
        typer.echo(f"Deep-rescan not implemented for source: {source}", err=True)
        raise typer.Exit(code=1)

    # Date-window pre-validation per source.
    if source == "fda":
        # Three-way: neither date → full-corpus historical seed (filter:"[]");
        # both → eventlmd window; exactly one → error. The neither-check MUST come
        # first — a single `... is None or ... is None` would route the full-corpus
        # case into the error branch.
        if start_date is None and end_date is None:
            pass  # full-corpus seed; set_full_corpus() below
        elif start_date is None or end_date is None:
            typer.echo(
                "fda deep-rescan: provide both --start-date and --end-date for a "
                "window, or neither for the full-corpus historical seed",
                err=True,
            )
            raise typer.Exit(code=1)
    elif start_date is not None or end_date is not None:
        typer.echo(
            _DEEP_RESCAN_NO_DATE_WINDOW_MESSAGES.get(
                source, f"{source}: --start-date/--end-date have no effect for this source."
            )
        )

    config = load_source_config(source)
    settings = Settings()  # type: ignore[call-arg]
    loader_cls = DEEP_RESCAN_BY_SOURCE_NAME[source]

    # Deep-rescan invariant: ``UsdaDeepRescanLoader.etag_enabled = False`` is
    # a class fact, not a YAML knob. Drop the YAML value before construction
    # so a routine-path ``etag_enabled: true`` doesn't override the invariant.
    # FDA + NHTSA deep-rescan loaders don't have this concern (their classes
    # don't declare etag_enabled) but the exclude is harmless across the board.
    kwargs = build_extractor_kwargs(
        config,
        loader_cls,
        settings,
        exclude=frozenset({"etag_enabled"}),
    )
    loader: Extractor = loader_cls(**kwargs)

    # FDA-only: post-construction mode mutation (the date / full-corpus fields are
    # PrivateAttrs, so they cannot flow through constructor kwargs).
    fda_full_corpus = False
    if source == "fda":
        if start_date is None and end_date is None:
            loader.set_full_corpus()  # type: ignore[attr-defined]
            fda_full_corpus = True
        else:
            assert start_date is not None and end_date is not None  # validated above
            loader.set_date_range(  # type: ignore[attr-defined]
                start_date=date.fromisoformat(start_date),
                end_date=date.fromisoformat(end_date),
            )

    # fda_press_releases-only: post-construction chunking knobs for the large event
    # sweep. --limit + --resume-after-event-id march through the corpus in
    # recall_event_id order across runs (content-hash dedup makes overlaps idempotent) —
    # the operator-controlled way to fit the seed under the Actions 6h job limit.
    if source == "fda_press_releases":
        if limit is not None and limit < 1:
            typer.echo("--limit must be >= 1", err=True)
            raise typer.Exit(code=1)
        if limit is not None:
            loader.work_list_limit = limit  # type: ignore[attr-defined]
        if resume_after_event_id is not None:
            loader.resume_after_event_id = resume_after_event_id  # type: ignore[attr-defined]
    elif limit is not None or resume_after_event_id is not None:
        typer.echo(
            f"{source}: --limit / --resume-after-event-id apply only to "
            "fda_press_releases; ignored."
        )

    result = loader.run(change_type=change_type)
    if source == "fda":
        prefix = (
            "fda deep-rescan [full-corpus]: "
            if fda_full_corpus
            else f"fda deep-rescan [{start_date} → {end_date}]: "
        )
    else:
        prefix = f"{source} deep-rescan: "
    _print_run_summary(prefix, result)


@app.command(name="recover-rejected")
def recover_rejected(
    source: Annotated[
        str, typer.Argument(help="Source whose quarantined records to recover (e.g. fda)")
    ],
    landing_path: Annotated[
        str | None,
        typer.Option(
            "--landing-path",
            help="raw_landing_path to scope to (default: the source's most-recent rejection).",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Reconstruct + report the recovery plan without writing."),
    ] = False,
    reason_contains_text: Annotated[
        str | None,
        typer.Option(
            "--reason-contains",
            help=(
                "Override the default predicate: recover invariant-stage rejections whose "
                "failure_reason contains this text. Default scope is the confirmed "
                "'>70 years in the past' date-sanity class. Census the rejected table first."
            ),
        ),
    ] = None,
) -> None:
    """Recover quarantined-but-valid bronze records for a source.

    Reads ``<source>_rejected``, reconstructs each record from its stored payload, and
    re-loads it via ``BronzeLoader`` (no API re-fetch, no watermark mutation, content-hash
    idempotent). Recovery BYPASSES ``check_invariants`` on purpose — only ever run after a
    census confirms the rejection class is a false positive (e.g.
    ``scripts/sql/fda/bronze/explore_seed_rejections.sql``). Supported for the sources that
    call ``check_date_sanity``; an unsupported source exits 1.
    """
    configure_logging()

    if source not in RECOVERY_CONFIG_BY_SOURCE_NAME:
        supported = ", ".join(sorted(RECOVERY_CONFIG_BY_SOURCE_NAME))
        typer.echo(
            f"recover-rejected not implemented for source: {source} (supported: {supported})",
            err=True,
        )
        raise typer.Exit(code=1)

    predicate = (
        reason_contains(reason_contains_text)
        if reason_contains_text is not None
        else recoverable_past_date_sanity
    )

    settings = Settings()  # type: ignore[call-arg]
    engine = make_engine(settings.neon_database_url.get_secret_value())

    result = recover_quarantined(
        engine,
        source=source,
        config=RECOVERY_CONFIG_BY_SOURCE_NAME[source],
        is_recoverable=predicate,
        landing_path=landing_path,
        dry_run=dry_run,
    )

    prefix = f"{source} recover-rejected: "
    dry = " [dry-run]" if result.dry_run else ""
    if result.landing_path is None:
        typer.echo(f"{prefix}no rejections found — nothing to recover.{dry}")
        return
    if result.candidates == 0:
        typer.echo(
            f"{prefix}0 recoverable rows at {result.landing_path} (predicate matched none).{dry}"
        )
        return
    if result.dry_run:
        typer.echo(
            f"{prefix}[dry-run] candidates={result.candidates} at {result.landing_path} "
            "— no write performed."
        )
        return
    typer.echo(
        f"{prefix}candidates={result.candidates} inserted={result.inserted} "
        f"(landing_path={result.landing_path})"
    )
    if result.inserted < result.candidates:
        typer.echo(
            f"  ({result.candidates - result.inserted} already present — content-hash dedup "
            "skipped them; re-running is idempotent.)"
        )


@app.command(name="resolve-firms")
def resolve_firms(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Report the row counts without writing firm_crosswalk."),
    ] = False,
    rollup: Annotated[
        bool,
        typer.Option(
            "--rollup/--no-rollup",
            help="Tier 2 entity rollup (>=2 shared distinctive tokens). On by default; "
            "--no-rollup ships Tier 1 only (near-identical name repair).",
        ),
    ] = True,
    fei_merge: Annotated[
        bool,
        typer.Option(
            "--fei-merge/--no-fei-merge",
            help="DEFERRED (default off, ADR 0037): opt into Tier 0 FDA-FEI merging. Establishment-"
            "grain FEI chains unrelated firms across owner changes; FEI is an attribute, not a "
            "merge key. Requires firm_fei_edges if enabled.",
        ),
    ] = False,
    rollup_threshold: Annotated[
        float,
        typer.Option(
            "--rollup-threshold",
            help="Tier 2 token_set_ratio merge threshold 0-100 (default 90; higher = stricter).",
        ),
    ] = 90.0,
) -> None:
    """Rebuild firm_crosswalk from all-source staging firm names (Phase 6b firm resolution).

    Maps each distinct firm name (CPSC / FDA / USDA / NHTSA / USCG staging views) through
    ``src.enrichment.firm_normalization`` (``clean_firm_name`` geo-suffix + DBA strip;
    ``extract_firm_dba`` / ``extract_paren_aliases`` for alternate names — no parenthetical
    strip, that proved too blunt cross-source, ADR 0037) and truncate-reloads
    ``firm_crosswalk``, keyed by ``md5(upper(trim(name)))`` so the silver firm models join it
    for ``canonical_firm_id`` / ``clean_name`` / ``alternate_names``. Then overlays the name-grain
    ``src.enrichment.firm_resolution``: Tier 1 = name-variant / typo repair, Tier 2 (``--rollup``)
    = >=2-token entity rollup with a place/compound guard. The result is a name/brand-grain firm
    dimension, uniform with the other four sources (whose structured ids are attributes, not merge
    keys); FDA's FEI rides on ``firm.observed_company_ids`` likewise. Tier 0 FEI merging is opt-in
    and deferred (``--fei-merge``). Idempotent; no API/watermark side effects. Run AFTER
    ``dbt build --select staging`` (it reads the ``stg_*`` views).
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    engine = make_engine(settings.neon_database_url.get_secret_value())
    summary = resolve_firm_crosswalk(
        engine,
        dry_run=dry_run,
        rollup=rollup,
        fei_merge=fei_merge,
        rollup_threshold=rollup_threshold,
    )
    dry = " [dry-run]" if summary.dry_run else ""
    typer.echo(
        f"resolve-firms{dry}: distinct_names={summary.distinct_names} "
        f"written={summary.rows_written} cleaned={summary.cleaned_count} "
        f"aliased={summary.alias_count} fei_merged={summary.fei_merged} "
        f"fuzzy_merged={summary.fuzzy_merged} fei_gated={summary.fei_gated}"
    )


def _load_reviewed_ok(path: Path) -> frozenset[str]:
    """Confirmed-legit cluster signatures (one per line; blank / #-comment lines ignored)."""
    if not path.exists():
        return frozenset()
    return frozenset(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


@app.command(name="audit-firm-rollups")
def audit_firm_rollups(
    out: Annotated[
        Path,
        typer.Option("--out", help="Where to write the ranked review CSV."),
    ] = Path("data/exploratory/cross_source/firm_rollup_review.csv"),
    reviewed_ok: Annotated[
        Path,
        typer.Option(
            "--reviewed-ok",
            help="Allowlist of confirmed-legit cluster signatures (one per line; # comments) — "
            "filtered out so each cycle shows only NEW merges.",
        ),
    ] = Path("documentation/audit/firm_rollup_reviewed_ok.txt"),
    low_score: Annotated[
        float,
        typer.Option(
            "--low-score",
            help="A rollup whose lowest score is below this (or min Jaccard < 0.5) counts as "
            "high-risk for the scheduled-alert tally.",
        ),
    ] = 95.0,
) -> None:
    """Rank Tier-2 (rapidfuzz_rollup) firm clusters by false-merge suspicion for manual review.

    The 6b.6 precision review loop (operations.md "Firm resolution review loop"): reads
    ``firm_crosswalk``, ranks every rollup cluster by how WEAK the merge is (low distinctive-token
    overlap / no rare anchor / borderline score), drops the ``--reviewed-ok`` allowlist, and writes
    a CSV most-suspect first. A confirmed FALSE merge is fixed by editing ``place_words.py`` /
    ``GENERIC_WORDS`` / ``never_merge.py`` then re-resolving; a confirmed-LEGIT cluster's signature
    goes into ``--reviewed-ok``. Read-only. Run after ``resolve-firms``; the GHA cron runs it
    monthly and opens an issue when the high-risk tally spikes.
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    engine = make_engine(settings.neon_database_url.get_secret_value())
    ok = _load_reviewed_ok(reviewed_ok)
    reviews = audit_rollup_clusters(engine, reviewed_ok=ok)
    high_risk = [
        r
        for r in reviews
        if r.min_jaccard < 0.5 or (r.min_score is not None and r.min_score < low_score)
    ]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "risk_rank",
                "canonical_name",
                "n_members",
                "min_jaccard",
                "weakest_anchor_df",
                "min_score",
                "shared_tokens",
                "members",
                "signature",
            ]
        )
        for i, r in enumerate(reviews, 1):
            writer.writerow(
                [
                    i,
                    r.canonical_name,
                    r.n_members,
                    r.min_jaccard,
                    r.weakest_anchor_df,
                    "" if r.min_score is None else r.min_score,
                    " | ".join(r.shared_tokens),
                    " || ".join(r.members),
                    r.signature,
                ]
            )
    typer.echo(
        f"audit-firm-rollups: rollup_clusters={len(reviews)} high_risk={len(high_risk)} "
        f"reviewed_ok={len(ok)} -> {out}"
    )
    # GHA cron: surface the high-risk tally so the workflow can open/refresh an alert issue.
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with Path(gh_out).open("a", encoding="utf-8") as fh:
            fh.write(f"high_risk_count={len(high_risk)}\n")
            fh.write(f"rollup_clusters={len(reviews)}\n")
            fh.write(f"report_path={out}\n")


if __name__ == "__main__":  # pragma: no cover
    app()
