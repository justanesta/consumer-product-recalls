# 0030 — NHTSA bronze identity: composite tuple + within-batch dedup (RECORD_ID regen-unstable; TSV ships byte-duplicate rows)

- **Status:** Accepted (amended 2026-05-08 after TSV-level analysis + implementation; amended 2026-06-01 — dedup-contract SSOT + deep-rescan bug fix, see "Amendment (2026-06-01)")
- **Date:** 2026-05-07; amendments 2026-05-08, 2026-06-01
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0007 (extends `hash_exclude_fields` use beyond FDA's RID position counter); ADR 0012 (concrete `identity_fields` choice for NHTSA); ADR 0014 (RECORD_ID is not a per-row natural key despite RCL.txt's "uniquely identifies the record" wording).
- **Extended by:** ADR 0031 (silver-row fragmentation strategy) — uses this ADR's 11-tuple as the basis for NHTSA's silver `recall_product_id = md5(11-tuple)` recipe; documents the cross-run drift class observed against this identity choice and the v1 reconciliation policy.
- **Refined by:** ADR 0041 (set-based staging-table dedup lookup) — the chunked existing-hash lookup in this ADR's "Implementation-side: text-canonical IN + chunked existing-hash lookup" subsection becomes the **small-batch path only**; NHTSA-scale batches route to a `pg_temp` staging-table JOIN. The 11-tuple identity, `hash_exclude_fields`, within-batch dedup, `allow_null_identity`, and the `_identity_text_expr` text-canonical comparison are unchanged.

## Amendment summary (2026-05-08)

After this ADR's initial 7-tuple proposal landed, two additional rounds of empirical work shifted the decision:

1. **TSV-level analysis widened the identity tuple from 7 to 11 fields.** The original bronze-side diagnostics (May 7 snapshot, post-2024 slice) found a 477-row residue and characterized it as byte-duplicate via spot-checked NISSAN + ACHILLES samples. Subsequent full-corpus analysis via `scripts/nhtsa/tsv_analysis/identity_search.py` against both POST_2010 regenerations (May 7 SHA `c955c37153d1` + older SHA `f11119e4d864`) revealed an 822-anomaly residue across the full TSV that the bronze-narrow scope had missed (the bronze data was `--since 2024-01-01` filtered, but the deep-rescan path needs the full corpus). Iterative widening converged identically on both regenerations to four additional fields: `mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman`. The widened 11-tuple is the actual identity.
2. **Loader implementation surfaced two unrelated SQL-level issues that needed fixes for the architecture to work end-to-end on real data**, both described under Decision below: type-canonical IN comparison (to handle empty-string binding into `TIMESTAMPTZ` parameter slots, which `bgman`/`endman` introduce), and chunked existing-hash lookup (to stay under Postgres' bind-parameter ceiling and planner-memory limits at 65k+ row batches).

Empirical end-to-end validation: post-amendment, `recalls extract nhtsa --since 2024-01-01` loads 65,732 rows on a fresh run; a re-run with `--since 2023-12-01` adds 6,343 rows (genuinely-new December 2023 records, the post-2024 slice deduplicates correctly). The 11-tuple is row-unique on what landed (`scripts/sql/nhtsa/bronze/verify_eleven_tuple_row_unique.sql` returns `excess_rows=0`).

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

**NHTSA bronze uses a composite 11-tuple identity, excludes `source_recall_id` from the content hash, deduplicates `(identity, hash)` pairs within each extract batch before insert, and accepts empty/null values as valid identity-bucket components.**

### Identity fields (11-tuple, amended 2026-05-08)

```python
# src/extractors/nhtsa.py BronzeLoader configuration
identity_fields=(
    "campno",          # NHTSA recall ID (e.g., 24V930000) — public, stable
    "maketxt",         # Vehicle/equipment make
    "modeltxt",        # Vehicle/equipment model
    "yeartxt",         # Vehicle model year (or "9999" unknown/N/A)
    "compname",        # NHTSA component taxonomy node
    "rcl_cmpt_id",     # NHTSA per-(COMPNAME, recall) component ID
    "mfr_comp_ptno",   # Manufacturer-supplied part number
    "mfr_comp_desc",   # Manufacturer-supplied component description
    "mfr_comp_name",   # Manufacturer-supplied component name
    "endman",          # End of manufacturing date range (TIMESTAMPTZ)
    "bgman",           # Begin of manufacturing date range (TIMESTAMPTZ)
)
```

The first 7 fields were the original ADR proposal (2026-05-07). Fields 8–11 were added 2026-05-08 after `scripts/nhtsa/tsv_analysis/identity_search.py` surfaced 822 anomaly groups against the full POST_2010 corpus that the bronze-narrow analysis missed. The widening was identity-iterative: each additional field resolved the most anomaly groups in the residue (`mfr_comp_desc`: 689/822, then `mfr_comp_name`: 128/145 of remainder, then `endman`: 10/17, then `bgman`: 7/7 → 0). Two POST_2010 regenerations converge identically to this 11-tuple.

Empirical row-uniqueness across the full POST_2010 TSV: 239,036 distinct 11-tuples across 240,158 rows; the 1,122-row residue is byte-duplicates (987 collision groups) handled by within-batch dedup. PRE_2010 has zero collisions at any tuple width — the additional 4 fields are constant-empty for all pre-2010 rows, so the wider tuple is harmless there.

Stability across regenerations:

- `campno`, `maketxt`, `modeltxt`, `yeartxt`, `compname` — invariant by definition of the recall. NHTSA cannot renumber `campno` without breaking external references.
- `rcl_cmpt_id` — empirically stable (`investigate_tire_collision.sql` Q2: 16,263 5-tuples, 0 set-mismatches across May 5 ↔ May 7 regenerations).
- `mfr_comp_ptno` — empirically stable (`verify_six_tuple_identity.sql` Q2: 16,263 5-tuples, 0 mismatches).
- `mfr_comp_desc`, `mfr_comp_name`, `endman`, `bgman` — stability inferred from the convergence of two `identity_search.py` runs (May 7 c955c37 + older f11119e SHAs) producing identical iteration logs and identical collision counts. Direct cross-regen field-stability testing for these four fields is deferred to `cross_regen_stability.py` (a planned tier-3 script in `scripts/nhtsa/tsv_analysis/`); a daily-extract-rerun against the same corpus produces 0 false-positive inserts (verified 2026-05-08), which is the load-bearing operational property.

Null-rate caveat:

- `mfr_comp_ptno`, `mfr_comp_desc`, `mfr_comp_name` were added 2020-03-23 per RCL.txt change-log entry 4. Pre-2020 rows have empty strings for all three; the post-2024 dev scope has 0% null rate.
- `bgman`, `endman` are populated for vehicle (V) recalls and frequently empty for equipment (E), tire (T), and child-seat (C) recalls. The empty-value handling is what makes `allow_null_identity=True` (below) load-bearing.

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

```python
within_batch_dedup=True
```

`BronzeLoader._dedup_within_batch()` (a new method, ADR-0030-introduced) deduplicates on `(identity_tuple, content_hash)` before the loader's existing-hash check against bronze. Without this, the loader's check is against bronze only, not within-batch — so 4 NISSAN-style byte-duplicate rows would all land on first extract.

Implementation: after Pydantic validation and identity/hash computation, group records by `(identity_tuple, content_hash)` and keep the first occurrence per group. Same identity with *different* content_hash is the defensive-error case (`WithinBatchIdentityCollisionError`); not observed in NHTSA data after the 11-tuple lock-in.

### Allow-null-identity (added 2026-05-08)

```python
allow_null_identity=True
```

Four of the eleven identity fields (`bgman`, `endman`, `mfr_comp_desc`, `mfr_comp_name`) are legitimately empty for many rows. `BronzeLoader`'s default behavior raises `ValueError` when an identity-component value is `None` or `""` — useful safety check for sources where every identity field is always populated (CPSC, FDA, USDA), but wrong for NHTSA. The flag relaxes the check: empty strings and `None` both normalize to `""` and contribute that "" sentinel as a valid identity-bucket component. Two rows with `bgman=None` deduplicate together; a row with `bgman=None` and a row with `bgman=2024-01-01` have distinct identities.

### Implementation-side: text-canonical IN + chunked existing-hash lookup (added 2026-05-08)

Two non-obvious SQL-level fixes were required for the 11-tuple identity to work at production scale. Both live in `src/bronze/loader.py`; both apply to any future source whose `identity_fields` includes typed (datetime, integer, etc.) columns and/or runs at high record volume.

**Text-canonical IN comparison** (`_identity_text_expr`). With the 11-tuple, `identity_fields` includes `bgman`/`endman` `TIMESTAMPTZ` columns. The loader's identity-value normalization produces strings (`""` for empty, ISO-8601-Z for populated). When `tuple_(*cols).in_(identity_keys)` compiles, SQLAlchemy types each bind parameter from the corresponding column's type — empty string binds as TIMESTAMPTZ → Postgres returns `DataError: invalid input syntax for type timestamp with time zone: ""`. Fix: cast both sides of the IN comparison to text (datetime columns via `to_char(col AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')` to match Pydantic's serialization; other columns via `cast(col, Text)`); coalesce NULL → `""`. Empty strings bind cleanly as TEXT regardless of underlying type.

**Chunked existing-hash lookup** (`_PG_PARAM_SAFETY_LIMIT`). The composite `tuple_(*cols).in_(identity_keys)` clause contributes `len(identity_fields) × len(identity_keys)` parameters. NHTSA's 11-tuple at 65k records = ~723k parameters, over Postgres' ~65,535 wire-protocol cap and stressing planner memory on small Neon free-tier compute (observed empirically as `OperationalError`). Fix: chunk `identity_keys` at `60_000 // n_cols` per query (~5,400 keys per chunk for the 11-tuple), run the existing query per chunk, merge result dicts. Per-row dedup is unaffected — each identity tuple appears in exactly one chunk's result.

Both fixes are necessary and orthogonal: text-canonical IN solves type-mismatch (would still fire on empty values even with chunking); chunking solves size (would still fire on large batches even with text-canonical IN).

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

The load-bearing artifacts split into two layers — bronze-side diagnostics that surfaced the original 7-tuple proposal, and TSV-level diagnostics that widened it to 11.

### Bronze-side (initial 2026-05-07 evidence — post-2024 slice)

| Source | What it shows |
|---|---|
| `scripts/sql/nhtsa/bronze/diagnose_full_reinsert.sql` Q4 | RECORD_ID `255795` = Vermeer on May 5, Mercedes-Benz on May 7 → cross-regen instability |
| `scripts/sql/nhtsa/bronze/verify_natural_key_candidate.sql` Q-A | Same Vermeer recall, each row's RECORD_ID changes across regenerations (e.g., Lug Nut: 255795 → 267221) |
| `scripts/sql/nhtsa/bronze/investigate_tire_collision.sql` Q2 | rcl_cmpt_id stability across regenerations: 16,263 5-tuples, 0 mismatches |
| `scripts/sql/nhtsa/bronze/verify_six_tuple_identity.sql` Q2 | mfr_comp_ptno stability across regenerations: 16,263 5-tuples, 0 mismatches |
| `scripts/sql/nhtsa/bronze/investigate_tire_collision.sql` Q3 | 7-tuple uniqueness on the post-2024 slice: 65,601 / 66,078 rows distinct (~99.3%); 477-row residue |
| `scripts/sql/nhtsa/bronze/investigate_residual_collisions.sql` Q1/Q2 | 477-row residue is byte-identical except RECORD_ID + content_hash on the spot-checked NISSAN 4-row + ACHILLES 5-row groups |
| `scripts/nhtsa/verify_collisions_raw_tsv.sh` against May 7 R2 wrapper | Inner-TSV SHA `c955c37153d1…` matches `extraction_runs.response_inner_content_sha256`; the bronze duplicates are NHTSA-shipped, not parsing artifacts |

### TSV-level (amended 2026-05-08 evidence — full POST_2010 + PRE_2010 corpora)

| Source | What it shows |
|---|---|
| `scripts/nhtsa/tsv_analysis/identity_search.py --zip may7-bronze.zip` | Iterative identity search converges 7→11 tuple over POST_2010: iter 0 has 822 anomalies, +mfr_comp_desc → 145, +mfr_comp_name → 17, +endman → 7, +bgman → 0. 987 byte-duplicate groups remain (handled by within_batch_dedup). |
| `scripts/nhtsa/tsv_analysis/identity_search.py --zip FLAT_RCL_POST_2010.zip` (older f11119e SHA) | Identical iteration log on the older POST_2010 regeneration: same 11 fields, same anomaly counts at each iteration, same 987 byte-duplicate groups. Cross-regen evidence for the tuple's stability. |
| `scripts/nhtsa/tsv_analysis/identity_search.py --zip FLAT_RCL_PRE_2010.zip` | PRE_2010: 0 collisions on the original 7-tuple → 11-tuple is over-specified (4 added fields are constant-empty for pre-2010 rows) but harmless. Same loader config works for both incremental and historical-seed paths. |
| `scripts/sql/nhtsa/bronze/verify_eleven_tuple_row_unique.sql` after 11-tuple landed | `excess_rows = 0` on bronze post-extraction → 11-tuple is row-unique on what landed. Validates dedup-on-rerun: re-extract against existing bronze inserts only the genuinely-new records (e.g., 65,732 new + 19 dedup-skipped on the second `--since 2024-01-01` run). |

### Source-side acknowledgments

| Source | What it shows |
|---|---|
| `documentation/nhtsa/RCL.txt` line 30 | NHTSA documents RECORD_ID as "Running Sequence Number" — a counter, by their own description |
| `documentation/nhtsa/Import_Instructions_Recalls.pdf` step 17 | NHTSA tells MS Access importers to auto-generate a synthetic primary key — official acknowledgment that no field is row-natural |

Findings K (RECORD_ID is regenerated per file build) and L (TSV ships byte-duplicate rows) in `documentation/nhtsa/flat_file_observations.md` document these observations as the per-source observation log alongside the existing Findings A–J.

## Implementation

Phase 5c Step 2's "Post-bronze identity-and-dedup revision" subsection in `project_scope/implementation_plan.md` tracks the implementation work. Final shape (as of 2026-05-08):

**`src/bronze/loader.py`** (generic, applies beyond NHTSA):
1. `BronzeLoader.__init__` accepts `within_batch_dedup: bool = False` and `allow_null_identity: bool = False` (defaults preserve CPSC/FDA/USDA behavior).
2. `_dedup_within_batch()` method collapses `(identity, hash)` duplicates within a batch; raises `WithinBatchIdentityCollisionError` on same-identity-different-hash (defensive).
3. `_identity_text_expr()` method returns text-canonical SQL expressions for identity columns — `to_char` with ISO-Z format for datetime types, `cast → text` with NULL coalesce for others. Used by `_fetch_existing_hashes` so the IN comparison is uniformly text-vs-text and empty-string binding works for `TIMESTAMPTZ` columns.
4. `_fetch_existing_hashes()` chunks `identity_keys` at `_PG_PARAM_SAFETY_LIMIT // n_cols` per query (60_000 // 11 = ~5_454 per chunk for NHTSA), running the existing query per chunk via `_fetch_existing_hashes_chunk()` and merging dicts.

**`src/extractors/nhtsa.py`**:
1. `load_bronze()` instantiates `BronzeLoader` with the 11-tuple `identity_fields`, `hash_exclude_fields=frozenset({"source_recall_id"})`, `within_batch_dedup=True`, `allow_null_identity=True`. Updated docstring spells out each piece.
2. Module-level "Identity:" docstring paragraph rewritten to reflect the 11-tuple and reference the TSV-analysis suite that determined it.

**`src/schemas/nhtsa.py`**:
1. `source_recall_id` field comment rewritten to reflect Findings K/L: stored for audit/lineage, not load-bearing for dedup.
2. Module docstring's field-naming note updated.

**Tests** (`tests/bronze/test_loader.py`, `tests/extractors/test_nhtsa_extractor.py`):
- Byte-duplicate input rows produce one bronze row.
- Cross-regeneration dedup: same logical row (same identity, RECORD_ID-changed) produces no new insert on second extract — verified in production via spot-check SQL.
- `WithinBatchIdentityCollisionError` raised on same-identity-different-hash (defensive — not observed for NHTSA).
- `allow_null_identity=False` (default) raises on empty identity component (regression guard for CPSC/FDA/USDA).
- `allow_null_identity=True` accepts empty identity component.
- NHTSA extractor's `BronzeLoader` config asserted at all four pieces: 11-tuple, hash_exclude_fields, within_batch_dedup, allow_null_identity.

**TSV-level analysis suite** (`scripts/nhtsa/tsv_analysis/`):
- `_lib.py` — shared helpers (TSV streaming, SHA-256 prefix, group-by, differing-fields).
- `identity_search.py` — iterative identity-tuple widening (load-bearing for the 11-tuple decision).
- `uniqueness_at_tuple.py` — single-tuple uniqueness check for ad-hoc spot-checks.
- `find_differentiator.py` — column-by-column distinct-count for chosen group keys with optional row filter.
- (Tier-3 deferred: `null_rate.py`, `cross_regen_stability.py`, `analyze_tsv.sh` runner, `documentation/nhtsa/tsv_analysis_guide.md`.)

**Bronze-table cleanup procedure used during the rollout:**

`truncate table nhtsa_recalls_bronze, nhtsa_recalls_rejected` on dev before re-extracting. The polluted 132,135 rows from the May 5 + May 7 loads (under the original `source_recall_id`-as-identity scheme that conflated different recalls under the same key) were discarded. `extraction_runs` history was retained for audit. Subsequent extracts repopulated bronze with the 11-tuple identity scheme; verification via `scripts/sql/nhtsa/bronze/verify_eleven_tuple_row_unique.sql` confirmed `excess_rows = 0`.

## Cross-source implications

**Standing requirement for future flat-file and HTML-scrape sources** (Phase 5d USCG, future): empirically verify identity stability across at least two source regenerations before trusting any field's documentation claim of uniqueness or stability. The Phase 5c experience demonstrates that source-published field descriptions can mislead — NHTSA's "uniquely identifies the record" wording for both `RECORD_ID` and `RCL_CMPT_ID` was within-file only at most, and even that failed once byte-duplicate rows were considered.

`hash_exclude_fields` is now in active use beyond FDA's narrow RID-counter case. Future extractors may need similar treatment for any field that participates in the row's wire representation but does not carry information — for example, source-side row counters, page-position artifacts, or per-export timestamps.

This ADR's existence is itself an argument for the per-source-Step-3 "first-extraction findings" doc as a non-optional deliverable: this finding would have been catastrophic if first surfaced in Phase 7 production cron rather than dev investigation.

## Amendment (2026-06-01) — dedup-contract single source of truth; deep-rescan bug fixed

The decision above (11-tuple identity, `hash_exclude_fields={source_recall_id}`, within-batch dedup, `allow_null_identity=True`) **stands unchanged**. This amendment records the *enforcement mechanism* added during the `src/` soundness consolidation (`project_scope/archive/src-consolidation-plan.md`; findings in `documentation/audit/src_soundness_audit.md`) and the latent bug it eliminated.

**The bug.** Each source's `BronzeLoader` dedup config was hand-copied in three places: the incremental `load_bronze`, the deep-rescan `load_bronze`, and `recovery.py`'s `RECOVERY_CONFIG_BY_SOURCE_NAME`. For NHTSA those copies had drifted — `NhtsaDeepRescanLoader.load_bronze` keyed on `identity_fields=("source_recall_id",)` and hashed the regen-unstable `RECORD_ID`, while the incremental path used this ADR's 11-tuple and excluded `RECORD_ID` from the hash. Both write `nhtsa_recalls_bronze`, so the deep-rescan path (reachable via the weekly `deep-rescan-nhtsa.yml` cron and `recalls deep-rescan nhtsa --change-type=historical_seed`) disagreed with the incremental path on both the identity bucket and the content hash — re-inserting the corpus as phantom rows on every NHTSA file regen.

**The fix.** Each source now has exactly one `DedupContract` (`src/bronze/dedup_contracts.py`) carrying the oracle (`identity_fields` + `hash_exclude_fields`) plus incremental-mode defaults for the operational flags. The incremental path, deep-rescan path, and recovery all construct their loader via `BronzeLoader.from_contract(...)`, so they physically cannot disagree on the oracle. Mode-varying flags (`within_batch_dedup`, `allow_null_identity`) remain per-call overrides defaulted from the contract — e.g. FDA's deep-rescan still passes `within_batch_dedup=True`. The contract lives in typed Python, never the operator-facing YAML: a dedup-oracle change must be a reviewed code edit, not a config tweak (an errant YAML edit to `identity_fields` would silently corrupt bronze dedup). A unit regression guard (`tests/bronze/test_dedup_contracts.py`) asserts a `RECORD_ID`-churned NHTSA row produces an identical identity tuple **and** identical content hash, so the divergence cannot recur.

**Data impact.** The buggy deep-rescan had already seeded the `main` branch's (empty-beforehand) `nhtsa_recalls_bronze`, so those rows carry `RECORD_ID`-polluted hashes; the truncate-and-reseed remediation is tracked in the plan doc.
