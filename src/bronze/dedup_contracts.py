"""Per-source bronze dedup contracts — the single source of truth for the dedup oracle.

A source's *dedup oracle* is the pair ``(identity_fields, hash_exclude_fields)``: the
columns that decide which rows are "the same logical row" and which fields are excluded
from the content hash. This pair **must be byte-identical across a source's incremental
load, deep-rescan load, and quarantine-recovery paths** — otherwise the same logical row
produces a different identity bucket or a different ``content_hash`` depending on which
path wrote it, and dedup silently breaks (this is exactly the NHTSA deep-rescan latent
bug: its loader keyed on the regen-unstable ``source_recall_id`` while the incremental
path used the 11-tuple and excluded ``source_recall_id`` from the hash). Centralizing the
oracle here makes that divergence structurally impossible — there is one place to read it.

Why this lives in typed Python and **not** in ``config/sources/*.yaml``: the oracle is a
semantic dedup contract, not operator-tunable transport config. A YAML edit to
``identity_fields`` would silently corrupt bronze dedup with no type checking and no
review. YAML keeps owning transport config (urls, timeouts, ``etag_enabled``); the dedup
contract stays here.

This module is a **leaf**: it imports nothing from ``src`` (pure data — column-name
strings + flags), so both ``src.extractors.*`` and ``src.bronze.recovery`` can import it
without any cycle. Consume a contract via :meth:`BronzeLoader.from_contract`.

Operational flags (``within_batch_dedup``, ``allow_null_identity``) legitimately vary by
*mode*: e.g. FDA's deep-rescan enables ``within_batch_dedup`` to collapse tie-boundary
straddle duplicates that the incremental window does not produce. The contract carries
the **incremental** value as the canonical default; deep-rescan / recovery override per
mode at the call site. They are deliberately NOT part of the oracle-equality contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DedupContract:
    """The dedup oracle for one source, plus the incremental-mode defaults for the
    operational flags.

    ``identity_fields`` + ``hash_exclude_fields`` are the *oracle* — the part that must be
    identical across every load path for the source. ``default_within_batch_dedup`` and
    ``default_allow_null_identity`` are mode-overridable defaults, not part of the oracle.

    ``default_track_presence`` enables the ADR 0026 per-run presence manifest
    (``extraction_run_identities``) for the source — USDA-only initially.
    ``Extractor._record_run`` reads it via this registry and writes the manifest on every
    successful run of the source. Unlike ``default_within_batch_dedup`` /
    ``default_allow_null_identity`` it is a per-source **policy**, not a mode-overridable
    default.

    **Hard prerequisite — the source must return the FULL corpus on every run this flag
    covers.** The manifest exists to tell "absent upstream (retracted)" from "unchanged
    (dedup-skipped)", which only holds if a run enumerates everything currently published.
    This is the real gate on enabling a source, NOT merely "does it retract":

      * **USDA qualifies** — it full-dumps every run (Finding D: no server-side filter
        works), so each daily run is a complete presence snapshot.
      * **CPSC / FDA do NOT qualify on their daily path** — those runs are watermark-filtered
        (``LastPublishDate`` / ``eventlmd``), i.e. partial, so a manifest built from one
        would mark every unchanged recall as "absent" (false retraction). They full-enumerate
        only on the deep-rescan path, which this *source-level* flag cannot single out (it
        fires on all of a source's modes) — they need per-mode gating, not a flag flip.
      * **NHTSA does NOT need a table** — its flat file IS already a full-snapshot manifest
        (``recall_lifecycle`` derives presence from bronze directly), and its
        ``source_recall_id`` (RECORD_ID) is regen-unstable (excluded from the hash), so the
        recall-grain builder would key presence on the wrong field.
      * **USCG** full-enumerates (listing scrape) except on short-circuit runs and has no
        retraction evidence yet — plausible later, unmeasured.

    So enabling a *qualifying* (full-enumerating, stable-recall-key) source is a one-line
    flip; a partial-incremental source needs design first. See ADR 0026 + the Phase 6c plan.
    """

    identity_fields: tuple[str, ...]
    hash_exclude_fields: frozenset[str] = field(default_factory=frozenset)
    default_within_batch_dedup: bool = False
    default_allow_null_identity: bool = False
    default_track_presence: bool = False


# Keys MUST match ``EXTRACTOR_BY_SOURCE_NAME`` in src/config/source_registry.py
# (a test asserts this). Each value is transcribed verbatim from that source's *current
# incremental* ``load_bronze`` — the incremental path is the canonical oracle, and a
# divergence audit (2026-06-01) confirmed every source's incremental / deep-rescan /
# recovery oracle already agrees *except* NHTSA's deep-rescan, which this contract fixes.
DEDUP_CONTRACT_BY_SOURCE_NAME: dict[str, DedupContract] = {
    # cpsc.py — pure BronzeLoader defaults.
    "cpsc": DedupContract(identity_fields=("source_recall_id",)),
    # fda.py — RID excluded (query-position counter, Finding F); deep-rescan overrides
    # within_batch_dedup=True for the productid tie-boundary straddle collapse.
    "fda": DedupContract(
        identity_fields=("source_recall_id",),
        hash_exclude_fields=frozenset({"rid"}),
    ),
    # usda.py — composite identity for bilingual English/Spanish siblings sharing
    # field_recall_number (Finding F); press-release bodies excluded from the hash.
    # track_presence ON (ADR 0026): USDA is the only source with confirmed implicit
    # retraction (state-4 toggles observed within hours, Phase 5b) + non-atomic bilingual
    # updates, so the per-run presence manifest is load-bearing for its lifecycle dims.
    "usda": DedupContract(
        identity_fields=("source_recall_id", "langcode"),
        hash_exclude_fields=frozenset({"en_press_release", "press_release"}),
        default_track_presence=True,
    ),
    # usda_establishment.py — latest_mpi_active_date excluded (weekly FSIS republish
    # churn, ADR 0032).
    "usda_establishments": DedupContract(
        identity_fields=("source_recall_id",),
        hash_exclude_fields=frozenset({"latest_mpi_active_date"}),
    ),
    # nhtsa.py — 11-tuple identity; RECORD_ID (source_recall_id) is regen-unstable
    # (Finding K) so it is excluded from the hash; within-batch dedup + null identity
    # per ADR 0030. THE deep-rescan path must use this exact oracle (was the latent bug).
    "nhtsa": DedupContract(
        identity_fields=(
            "campno",
            "maketxt",
            "modeltxt",
            "yeartxt",
            "compname",
            "rcl_cmpt_id",
            "mfr_comp_ptno",
            "mfr_comp_desc",
            "mfr_comp_name",
            "endman",
            "bgman",
        ),
        hash_exclude_fields=frozenset({"source_recall_id"}),
        default_within_batch_dedup=True,
        default_allow_null_identity=True,
    ),
    # uscg.py — details_url excluded (volatile, not content).
    "uscg": DedupContract(
        identity_fields=("source_recall_id",),
        hash_exclude_fields=frozenset({"details_url"}),
    ),
    # uscg_manufacturer.py — detail_url + uscg_directory_id excluded (volatile).
    "uscg_manufacturers": DedupContract(
        identity_fields=("source_recall_id",),
        hash_exclude_fields=frozenset({"detail_url", "uscg_directory_id"}),
    ),
    # uscg_manufacturer_detail.py — same exclusions as the manufacturer listing.
    "uscg_manufacturer_details": DedupContract(
        identity_fields=("source_recall_id",),
        hash_exclude_fields=frozenset({"detail_url", "uscg_directory_id"}),
    ),
    # fda_press_release.py — Tier-3 press releases (capture-expansion (b) PR). Composite
    # identity: one event (source_recall_id = RECALLEVENTID) can carry several releases,
    # so the URL is the second key. No hash exclusions — all 3 payload fields are content.
    # within_batch_dedup on (defensive): a single event's response could repeat a URL;
    # collapse byte-identical copies, raise only on same-identity-different-content.
    "fda_press_releases": DedupContract(
        identity_fields=("source_recall_id", "press_release_url"),
        default_within_batch_dedup=True,
    ),
}
