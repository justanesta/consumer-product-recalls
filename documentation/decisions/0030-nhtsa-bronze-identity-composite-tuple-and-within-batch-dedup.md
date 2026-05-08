# 0030 — NHTSA bronze identity: composite tuple + within-batch dedup (RECORD_ID regen-unstable; TSV ships byte-duplicate rows)

- **Status:** Accepted
- **Date:** 2026-05-07
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0007 (extends `hash_exclude_fields` use beyond FDA's RID position counter); ADR 0012 (concrete `identity_fields` choice for NHTSA); ADR 0014 (RECORD_ID is not a per-row natural key despite RCL.txt's "uniquely identifies the record" wording).

## Context

### What prompted this ADR

Phase 5c Step 3 first-extraction analysis (2026-05-07) revealed two independent failure modes in the original NHTSA bronze identity (`identity_fields=("source_recall_id",)` where `source_recall_id` carried RCL.txt field 1 `RECORD_ID`):

1. **Across regenerations:** NHTSA reassigns `RECORD_ID` on each file rebuild. The same logical recall row gets a different `RECORD_ID` across consecutive snapshots. A `recalls extract nhtsa --since 2024-01-01` run on May 5 loaded 66,057 rows; a re-run on May 7 inserted 66,078 *additional* rows instead of skipping the 66,057 prior duplicates. Pinpointed via `scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql` Q4: `source_recall_id=255795` describes a Vermeer BC900XL lug-nut recall on May 5 and a Mercedes-Benz Sprinter 2500 instrument-cluster recall on May 7.

2. **Within a single regeneration:** NHTSA's TSV emits multiple rows that are byte-identical except for `RECORD_ID`. 477 rows out of 66,078 (~0.7%) collide on the maximal natural-key tuple; column-by-column distinct-count analysis (`investigate_residual_collisions.sql` Q1/Q2) confirms every data field is constant within each collision set. Verified byte-for-byte against the exact May 7 R2 wrapper that produced the bronze (inner-TSV `sha256 = c955c37153d1…` matches `extraction_runs.response_inner_content_sha256`); the duplicates are NHTSA's, not parsing artifacts.

Both failure modes are explained by NHTSA's TSV generation:

- RCL.txt field 1 description: *"Running Sequence Number, Which Uniquely Identifies The Record"* — explicitly a counter assigned at file-generation time. The "uniquely identifies" qualifier holds within a single TSV; RCL.txt is silent on cross-file stability.
- `documentation/nhtsa/Import_Instructions_Recalls.pdf` step 17 instructs users importing the TSV to MS Access to *"Let Access add primary key"* — official NHTSA documentation that no field carries a natural per-row primary key.

The `src/schemas/nhtsa.py:148` docstring asserting RECORD_ID is "NHTSA's stable per-row natural key per RCL.txt" was a misreading of "Running Sequence Number" — empirically wrong on both grounds.

### What was already decided

Three relevant prior decisions:

- **ADR 0007 (lineage via bronze snapshots and content hashing):** dedup is `(identity_fields, content_hash)` against the most recent existing bronze row. The `BronzeLoader` accepts a `hash_exclude_fields` parameter, currently used only for FDA's RID query-position counter — but the mechanism is the right tool for any field that's part of the row's wire representation but not part of its content.
- **ADR 0012 (extractor pattern):** `identity_fields` is per-source-configurable. CPSC and FDA use single-field identities; USDA recall uses a 2-tuple `("source_recall_id", "langcode")` to handle bilingual pairs. Composite identities are already supported.
- **ADR 0027 (bronze storage-forced transforms only):** bronze faithfully preserves source bytes for non-storage-forced fields. Bronze cannot dedup at ingest-time silently; any duplicate-collapse must be deliberate and documented.

### The architectural question

What `identity_fields`, `hash_exclude_fields`, and within-batch dedup behavior should NHTSA use, given:

- No single TSV field is row-stable. `RECORD_ID` is regen-unstable. `RCL_CMPT_ID` (RCL.txt field 24) is component-grain and repeats across rows within a recall (Q-B of `verify_natural_key_candidate.sql` showed one ID across 139 rows). `MFR_COMP_PTNO` (field 27) repeats across rows when `RCL_CMPT_ID` differs (Q1 of `verify_six_tuple_identity.sql`: same ptno appears 45× in the same campaign).
- The TSV ships byte-duplicate rows for some recalls (~0.7% of the corpus).
- ADR 0027 mandates bronze preserves source bytes; we cannot silently collapse duplicates at parse time without an explicit decision.

## Decision

**NHTSA bronze uses a composite 7-tuple identity, excludes `source_recall_id` from the content hash, and deduplicates `(identity, hash)` pairs within each extract batch before insert.**

### Identity fields

```python
# src/extractors/nhtsa.py BronzeLoader configuration
identity_fields=(
    "campno",         # NHTSA recall ID (e.g., 24V930000) — public, stable
    "maketxt",        # Vehicle/equipment make
    "modeltxt",       # Vehicle/equipment model
    "yeartxt",        # Vehicle model year (or "9999" unknown/N/A)
    "compname",       # NHTSA component taxonomy node
    "rcl_cmpt_id",    # NHTSA per-(COMPNAME, recall) component ID
    "mfr_comp_ptno",  # Manufacturer-supplied part number
)
```

Empirical row-uniqueness within a single TSV (May 7 snapshot, `investigate_tire_collision.sql` Q3): 65,601 distinct 7-tuples across 66,078 rows. The 477-row residue is byte-duplicate rows handled by within-batch dedup (below).

Stability across regenerations:

- `campno`, `maketxt`, `modeltxt`, `yeartxt`, `compname` — invariant by definition of the recall. NHTSA cannot renumber `campno` without breaking external references (news articles, manufacturer notices, NHTSA's own consumer-facing site).
- `rcl_cmpt_id` — empirically stable across the May 5 ↔ May 7 regenerations (`investigate_tire_collision.sql` Q2): 16,263 5-tuples present in both snapshots, 0 set-mismatches under array-equality comparison.
- `mfr_comp_ptno` — empirically stable across the same regenerations (`verify_six_tuple_identity.sql` Q2): 16,263 5-tuples, 0 mismatches.

Null-rate caveat: `MFR_COMP_PTNO` was added to the TSV on 2020-03-23 per RCL.txt change-log entry 4. Within the post-2024 dev scope it has 0% null rate (`verify_six_tuple_identity.sql` Q3), but the historical-seed path (Phase 7, deep-rescan covering 1966–2009 via `FLAT_RCL_PRE_2010.zip`) will encounter widespread NULL values. Postgres groups NULLs together in `GROUP BY`, so identity collisions in pre-2020 records will collapse — acceptable since the historical seed runs once and pre-2020 records are not edited.

### Hash exclusion

```python
# src/extractors/nhtsa.py BronzeLoader configuration
hash_exclude_fields=frozenset({"source_recall_id"})
```

Excluding `source_recall_id` (= `RECORD_ID`) from the content hash means:

- **Across regenerations:** the same logical row, with the same data fields and a different RECORD_ID, hashes to the same value. Dedup correctly skips it on rerun.
- **Within a regeneration:** byte-duplicate rows (which differ only in RECORD_ID per the empirical analysis) hash to the same value. Combined with the within-batch dedup step below, they collapse to one bronze row per logical fact.

`source_recall_id` remains stored on the bronze row for audit/lineage but is not load-bearing for dedup.

### Within-batch dedup

`NhtsaExtractor.extract()` (or `validate_records()`) deduplicates on `(identity_tuple, content_hash)` before handing records to `BronzeLoader.load()`. Without this, the loader's existing-hash check is against bronze, not within-batch — so all 4 NISSAN-style byte-duplicate rows would land on first extract.

Implementation: after Pydantic validation, group records by `(identity_tuple, content_hash)` and emit one record per group. The choice of which record to keep is arbitrary because the records are byte-equivalent post-hash-exclusion.

## Consequences

### Positive

- **Dedup correctness restored.** Daily incremental extracts will land only genuine net-new and net-edited rows. No more 66k-row re-insert per run.
- **Byte-duplicate handling is honest.** The 477 NHTSA-shipped duplicate rows collapse to ~120 logical rows at extract time; bronze represents one row per logical recall × vehicle × component × part fact. Silver doesn't have to repeat this work.
- **Lineage clarity preserved.** Bronze `content_hash` changes iff the source's content changed (modulo `RECORD_ID`, which doesn't carry information). ADR 0027's lineage promise extends cleanly to NHTSA.
- **`hash_exclude_fields` precedent broadens.** The mechanism existed for FDA's RID counter (a query-position artifact); NHTSA shows it's also the right tool for source-side row counters. Future flat-file sources may need similar treatment.

### Negative

- **NHTSA-specific complexity.** Three pieces of configuration (composite identity, hash exclusion, within-batch dedup) where CPSC has zero. Code reviewers and future contributors need this ADR to understand the rationale.
- **Within-batch dedup is a new code path.** Tests must verify both (a) byte-duplicates collapse correctly and (b) legitimate within-batch identity-collisions with *different* content surface as a hard error rather than silently overwriting — though no such case has been observed for NHTSA.
- **Schema and extractor docstrings must be rewritten.** `src/schemas/nhtsa.py:148` and `src/extractors/nhtsa.py:414-416` currently assert RECORD_ID is "NHTSA's stable per-row natural key per RCL.txt" — empirically wrong, must be replaced with the actual identity scheme and a pointer to this ADR.
- **Existing 132,135-row polluted bronze must be discarded** before the new identity scheme is installed. The pollution is the May 5 + May 7 loads under the old `source_recall_id`-as-identity scheme, which conflated different recalls under the same key. New identity dedup against polluted prior content would produce wrong results.

### Neutral

- ADR 0010 deep-rescan workflow is unaffected. `NhtsaDeepRescanLoader` inherits the same identity scheme via subclass relationship to `NhtsaExtractor`.
- The `source_watermarks.last_cursor` column remains NULL for NHTSA (no per-row cursor exists per `_touch_freshness` design); this ADR doesn't change watermark behavior.
- Pre-2020 records loaded via the historical-seed path (Phase 7) will have NULL `mfr_comp_ptno` and may identity-collide. Acceptable: pre-2020 records are not re-extracted incrementally, so collision risk is one-time.

## Empirical evidence

The reasoning above cites diagnostic queries; their full output is captured in `project_scope/current_branch_staged_tasks.md` (this branch's notes). The load-bearing artifacts:

| Source | What it shows |
|---|---|
| `scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql` Q4 | RECORD_ID `255795` = Vermeer on May 5, Mercedes-Benz on May 7 → cross-regen instability |
| `scripts/sql/nhtsa/bronze/verify_natural_key_candidate.sql` Q-A | Same Vermeer recall, each row's RECORD_ID changes across regenerations (e.g., Lug Nut: 255795 → 267221) |
| `scripts/sql/nhtsa/bronze/investigate_tire_collision.sql` Q2 | rcl_cmpt_id stability across regenerations: 16,263 5-tuples, 0 mismatches |
| `scripts/sql/nhtsa/bronze/verify_six_tuple_identity.sql` Q2 | mfr_comp_ptno stability across regenerations: 16,263 5-tuples, 0 mismatches |
| `scripts/sql/nhtsa/bronze/investigate_tire_collision.sql` Q3 | 7-tuple uniqueness: 65,601 / 66,078 rows distinct (~99.3%); 477 residue |
| `scripts/sql/nhtsa/bronze/investigate_residual_collisions.sql` Q1/Q2 | 477-row residue is byte-identical except RECORD_ID + content_hash (NISSAN 4 dups, ACHILLES 5 dups) |
| `scripts/nhtsa/verify_collisions_raw_tsv.sh` against May 7 R2 wrapper | Inner-TSV SHA `c955c37153d1…` matches `extraction_runs.response_inner_content_sha256`; the bronze duplicates are NHTSA-shipped, not parsing artifacts |
| `documentation/nhtsa/RCL.txt` line 30 | NHTSA documents RECORD_ID as "Running Sequence Number" — a counter, by their own description |
| `documentation/nhtsa/Import_Instructions_Recalls.pdf` step 17 | NHTSA tells MS Access importers to auto-generate a synthetic primary key — official acknowledgment that no field is row-natural |

Findings K (RECORD_ID is regenerated per file build) and L (TSV ships byte-duplicate rows) in `documentation/nhtsa/flat_file_observations.md` document these observations as the per-source observation log alongside the existing Findings A–J.

## Implementation

Phase 5c Step 2 is revised to incorporate this ADR. Three changes in `src/extractors/nhtsa.py`:

1. `BronzeLoader` instantiation in `load_bronze()` changes from `identity_fields=("source_recall_id",)` to the 7-tuple, with `hash_exclude_fields=frozenset({"source_recall_id"})` added.
2. Within-batch dedup step in `NhtsaExtractor.extract()` (or as a new transform in `validate_records()`) groups records by `(identity_tuple, content_hash)` and emits one per group.
3. Schema docstring (`src/schemas/nhtsa.py:148`) and extractor docstring (`src/extractors/nhtsa.py:414-416`) rewritten to reflect empirical reality and reference this ADR.

Tests in `tests/unit/extractors/test_nhtsa.py` (or equivalent) verify:

- Byte-duplicate input rows produce one bronze row.
- Same logical row across two regenerations (same identity, RECORD_ID-changed) produces no new insert on the second extract.
- Hard error on within-batch identity collision with *different* content (a defensive check; not observed in production but worth pinning down so a future NHTSA format change surfaces loudly).

Bronze-table cleanup: `truncate table nhtsa_recalls_bronze` and `truncate table nhtsa_recalls_rejected` on dev before re-extracting. The polluted 132,135 rows from the May 5 + May 7 loads are discarded; `extraction_runs` history is retained for audit per the action-plan in `project_scope/current_branch_staged_tasks.md`.

## Cross-source implications

**Standing requirement for future flat-file and HTML-scrape sources** (Phase 5d USCG, future): empirically verify identity stability across at least two source regenerations before trusting any field's documentation claim of uniqueness or stability. The Phase 5c experience demonstrates that source-published field descriptions can mislead — NHTSA's "uniquely identifies the record" wording for both `RECORD_ID` and `RCL_CMPT_ID` was within-file only at most, and even that failed once byte-duplicate rows were considered.

`hash_exclude_fields` is now in active use beyond FDA's narrow RID-counter case. Future extractors may need similar treatment for any field that participates in the row's wire representation but does not carry information — for example, source-side row counters, page-position artifacts, or per-export timestamps.

This ADR's existence is itself an argument for the per-source-Step-3 "first-extraction findings" doc as a non-optional deliverable: this finding would have been catastrophic if first surfaced in Phase 7 production cron rather than dev investigation.
