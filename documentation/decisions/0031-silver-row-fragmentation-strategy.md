# 0031 — Silver-row fragmentation strategy: per-source surrogate keys, drift detection, and reconciliation tiers

- **Status:** Accepted
- **Date:** 2026-05-08
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0002 (extends scope: ADR 0002 covers firm-level cross-source resolution; this ADR adds product-level fragmentation strategy); ADR 0030 (this ADR is the silver-side companion to ADR 0030's bronze identity choice for NHTSA); ADR 0007 (Tier 1 mechanism — content-hash dedup is a build-time prevention layer in this framework).

## Context

### What prompted this ADR

Phase 5c step 5 needs to extend silver to incorporate NHTSA. The empirical investigation of NHTSA bronze identity stability (see `documentation/nhtsa/incremental_delta_findings.md`) surfaced findings that don't fit cleanly under existing ADRs:

1. **Cross-run drift on natural-key fields is real.** The NHTSA cross-corpus stability test (2026-05-08, `scripts/nhtsa/tsv_analysis/cross_corpus_stability.py`) caught 1 case of NHTSA editing `maketxt` for an existing recall (`'AC DELCO'` → `'ACDELCO'`, campno 22E002000). The drift can land on any of the 11 bronze identity fields, not just the batch-level ones.

2. **No deterministic surrogate built from natural keys is drift-immune.** A `recall_product_id` derived as `md5(natural_keys...)` will fragment whenever NHTSA normalizes any of those fields. This is exactly the failure mode Kimball's "natural keys as PK" anti-pattern warns about.

3. **ADR 0002's RapidFuzz machinery handles firms only.** ADR 0002 specifies fuzzy-match for cross-source firm reconciliation — explicitly NOT for product-level reconciliation. The current `dbt/models/silver/firm.sql:82` keys firms on `md5(upper(trim(name)))` — exact normalized-name match, not fuzzy. Fuzzy matching is gated to Phase 6 per `project_scope/implementation_plan.md:599`.

4. **The same fragmentation pattern will apply to other sources** as detection coverage expands. CPSC's products array can reorder; FDA's PRODUCTID can theoretically renumber; future sources (USCG) are unknowns. A multi-source strategy is needed, not a NHTSA-specific patch.

### What was already decided

- **ADR 0007 (lineage via bronze snapshots and content hashing):** at the bronze layer, `content_hash` deduplication prevents identical-content re-inserts. This is the Tier 1 prevention layer from this ADR's perspective.
- **ADR 0030 (NHTSA bronze identity):** an 11-tuple composite identity makes NHTSA bronze row-unique. This ADR builds on ADR 0030 — bronze stays at the 11-tuple grain (audit-quality), and silver derives a deterministic surrogate from those same 11 fields.
- **ADR 0002 (header / line / firm normalization):** fuzzy-match infrastructure for cross-source firm entity resolution. This ADR forward-references ADR 0002 for firm-level reconciliation; this ADR is responsible for product-level fragmentation.

### The architectural question

For each source, what `recall_product_id` derivation does silver use, what fragmentation modes does it have, how do we detect them, and what (if anything) do we do about them? The answer must be uniform in framing across sources but adapted in specifics — CPSC's products[] array reorder is not the same failure mode as NHTSA's `endman` edit, but both need a place in the strategy.

## Decision

**Silver-row fragmentation across sources is handled by a three-tier strategy: build-time prevention, ongoing detection, and v1-deferred reconciliation.** Each source applies the framework to its own surrogate-key derivation and fragmentation modes. Product-level reconciliation is explicitly v1-deferred to Phase 6 and triggered by Tier 2 evidence.

### Tier 1 — Build-time prevention

Silver `recall_product_id` is a deterministic md5 hash of carefully-chosen bronze fields. The choice trades off:

- **Coverage** — including more fields → finer grain → fewer collisions but more drift surface
- **Stability** — including fewer fields → coarser grain → more collisions but smaller drift surface
- **Cross-source consistency** — every source has a `recall_product_id` derivable at silver build time without external state

Existing prevention mechanisms (already in place):

- **Bronze content-hash dedup (ADR 0007).** Idempotent re-fetches don't grow bronze. Prevents the trivial case of identical content landing twice.
- **Bronze identity choice (per source — ADR 0030 for NHTSA).** Within-corpus row-uniqueness is enforced at bronze. NHTSA's 11-tuple, USDA's `(source_recall_id, langcode)`, FDA's `PRODUCTID`, CPSC's `RecallNumber`. Silver inherits cleanly from these.
- **Silver name-normalization (`firm.sql:82`).** UPPER+TRIM for firm names. Catches casing/whitespace variants. Does NOT catch character-level normalizations like `"AC DELCO"` ↔ `"ACDELCO"`.

### Tier 2 — Detection

Per-source assertion scripts that quantify fragmentation rates over time. NHTSA already has a complete suite (Tier 2 reference implementation):

| Script | Purpose |
|---|---|
| `scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql` | Bronze-level cross-run drift, all 10 non-anchor identity fields |
| `scripts/sql/nhtsa/bronze/assert_nine_tuple_identity_stable.sql` | Bronze-level cross-run drift on the 9 silver-canonical fields (drops bgman/endman) |
| `scripts/nhtsa/tsv_analysis/cross_corpus_stability.py` | Full-corpus cross-TSV-capture drift (avoids spending bronze inserts) |
| `scripts/nhtsa/tsv_analysis/uniqueness_at_tuple.py` | Within-corpus row-uniqueness for any tuple shape |
| `scripts/nhtsa/tsv_analysis/find_differentiator.py` | What field(s) differentiate residual collisions |

Other sources need parity scripts as **future work**, sequenced opportunistically:

- **CPSC** — ordinality-shift detection: count cases where `(recall_event_id, product_name, model)` appears with multiple distinct ordinals across `extraction_timestamp` snapshots
- **FDA** — `PRODUCTID` stability assertion across `extraction_timestamp` snapshots; expected to be 0 (FDA documents PRODUCTID as stable) but worth verifying
- **USDA** — `field_recall_number` stability assertion; trivially 0 expected (single-row-per-recall grain)
- **USCG** — apply the framework when Phase 5d lands

### Tier 3 — Reconciliation (v1: scoped; full Phase 6)

Two reconciliation surfaces, both ultimately Phase 6 work, with different scopes:

- **Firm-level (existing scope, ADR 0002):** RapidFuzz cross-source firm reconciliation. Resolves both intra-source drift (`'AC DELCO'` ↔ `'ACDELCO'` within NHTSA) and cross-source drift (Honda spelled differently across CPSC, FDA, NHTSA). Already specified in ADR 0002; implementation gated to Phase 6 per `implementation_plan.md:599`. **No change in this ADR.**
- **Product-level (NEW scope, this ADR):** v1 explicitly **accepts** product-level fragmentation as a known limitation. Each source documents its expected fragmentation rate and class. Phase 6 revisit triggered by Tier 2 detection rates exceeding documented thresholds (per source, see table below). Reconciliation mechanism choices (mapping table, fuzzy-match on product attributes, source-specific normalization rules) are deferred — the trigger first, the mechanism second.

### Per-source surrogate + fragmentation profile

| Source | `recall_event.source_recall_id` | `recall_product_id` recipe | Known fragmentation modes | Tier 2 detection status | Phase 6 revisit threshold |
|---|---|---|---|---|---|
| **CPSC** | `RecallNumber` (stable upstream) | `md5(event_id\|name\|model\|product_ordinal)` per `recall_product.sql:31-46` | (a) `products[]` array reorder shifts ordinals → all later products fragment, (b) name/model character-level normalization | `scripts/sql/cpsc/bronze/assert_products_array_append_only.sql` + `assert_name_model_normalization_stable.sql`. dbt singular wrappers at `severity=warn` under `dbt/tests/source_assumptions/`. Empirical baselines in `documentation/cpsc/array_stability_findings.md`. | >0.1% silver row count fragmented per quarter |
| **FDA** | `RECALLEVENTID::text` (stable upstream) | `md5('FDA'\|PRODUCTID)` per `recall_product.sql:51` | PRODUCTID renumber if FDA changes its internal ID scheme | `scripts/sql/fda/bronze/assert_productid_stable.sql`. dbt singular wrapper at `severity=warn` under `dbt/tests/source_assumptions/`. Empirical baselines in `documentation/fda/productid_stability_findings.md`. (EVENTLMD reliability — a noise-quantification rather than fragmentation assumption — also tracked there via `assert_eventlmd_correlates_with_content_change.sql`.) | Any non-zero rate (FDA stability is contractual) |
| **USDA** | `field_recall_number` (stable upstream) | `recall_product_id = recall_event_id` (1:1 — one product row per recall, ADR 0002 defers structured parsing of free-text `product_items`) | None at product level — single-row-per-recall grain. Free-text `product_items` lives in `source_specific_attrs`. | N/A for fragmentation. **History-correctness assertions** (bilingual atomicity, `last_modified_date` reliability) live at `scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql` + `assert_field_last_modified_date_advances_on_edit.sql`, with dbt wrappers and baselines in `documentation/usda/bilingual_and_lmd_findings.md`. They feed Phase 6's `recall_event_history` design (ADR 0022), not this ADR's fragmentation framework. | N/A |
| **NHTSA** | `campno` (stable upstream — verified across both archives) | **`md5('NHTSA'\|campno\|maketxt\|modeltxt\|yeartxt\|compname\|rcl_cmpt_id\|mfr_comp_ptno\|mfr_comp_desc\|mfr_comp_name\|bgman\|endman)`** (11-tuple matching ADR 0030 bronze identity) | Cross-run drift on any of 11 identity fields. Baselines (two substrates): **TSV-substrate** 2026-05-08 — 1 case/day (AC DELCO `maketxt` normalization, via `cross_corpus_stability.py`). **Bronze-substrate** 2026-05-09 — 3 drift groups across 240k rows (~0.00125%): 2 cases on `mfr_comp_ptno` (Ferrari 12Cilindri privacy-window part-number typo correction `000788416 ↔ 000788418`) + 1 case on `endman` (Western Star 47X manufacturing-window extension `2026-02-03 → 2026-04-10` — exactly the trade-off this ADR's "Why option 3b" section anticipated) | Implemented — `assert_*_identity_stable.sql` + `cross_corpus_stability.py`; dbt singular wrapper at `dbt/tests/source_assumptions/assert_nhtsa_eleven_tuple_identity_stable.sql` (severity=warn) | >0.01% silver row count fragmented per month, OR systematic drift on a previously-stable field |
| **USCG** | TBD (Phase 5d) | TBD | TBD | TBD — apply this ADR's framing when source lands | TBD |

### Why option 3b (11-tuple hash) for NHTSA, not option 3a (9-tuple + ordinal)

The NHTSA derivation **mirrors CPSC's recipe structurally** (`md5(parent_id || distinguishing_fields || disambiguator)`):

- CPSC's distinguishing fields: `name + model`. Disambiguator: `product_ordinal` from `LATERAL jsonb_array_elements WITH ORDINALITY`.
- NHTSA's distinguishing fields: 9 identity components from the 11-tuple. Disambiguator: `bgman + endman` (the batch-level fields).

Including the actual `bgman/endman` values in the hash rather than computing an ordinal via `ROW_NUMBER() OVER (PARTITION BY 9-tuple ORDER BY bgman, endman)` avoids the **batch-insertion vulnerability**: when NHTSA adds a newly-discovered production batch to an existing recall, ordinal-based surrogates would shift for all batches with later dates. Direct value inclusion gives the new batch its own surrogate and leaves existing surrogates unchanged. The trade-off — an `endman` edit fragments the row — is operationally similar in scale to CPSC's array-reorder vulnerability (~1 case/day rate observed for NHTSA; CPSC's rate is unmeasured but plausibly similar).

CPSC's design implicitly assumes `products[]` is append-only. NHTSA's analog assumption is that `bgman/endman` values for a given (campno + 9-tuple) don't get re-edited. The assumption is empirically violated at ~0.0005% per day (1 case in 240k rows in 1 day) — small, bounded, monitored via Tier 2.

## Consequences

### Positive

- **Cross-source consistency.** Every source now has a documented `recall_event.source_recall_id` and `recall_product_id` recipe in one table, with documented fragmentation modes and detection coverage. The `recall_event_history` model's `(source, source_recall_id)` partition (per ADR 0022) works uniformly.
- **NHTSA fits the harmonized silver schema.** No NHTSA-specific tables (`recall_component_batch` etc.) needed — the existing `recall_event` + `recall_product` + `firm` + `recall_event_firm` shape absorbs NHTSA cleanly. Batch-level fields live in `recall_product.source_specific_attrs` JSONB, consistent with how CPSC/FDA/USDA handle source-specific extras.
- **Drift detection is a uniform discipline.** Every source gets a Tier 2 assertion script. NHTSA's reference implementation (`scripts/nhtsa/tsv_analysis/`, `scripts/sql/nhtsa/bronze/assert_*.sql`) is the template.
- **Reconciliation deferred but not avoided.** Product-level fragmentation is acknowledged with documented thresholds, not swept under the rug. Phase 6 has a concrete trigger.
- **NHTSA `recall_product_id` mirrors CPSC structurally.** The pattern of `md5(parent || distinguishing || disambiguator)` is now established for two distinct shapes (JSONB array, multi-batch) and is extensible to USCG.

### Negative

- **v1 silver will have ~150 fragmented NHTSA `recall_product` rows per year** at the current 0.0004%/day drift rate (extrapolation). Downstream consumers querying NHTSA at the 11-tuple grain will see two rows for the AC DELCO case and equivalents. Documented as a known limitation; reconciliation triggers from Phase 6 if rate grows.
- **Other sources have only TBD detection coverage.** Tier 2 is uniformly framed but not uniformly implemented. CPSC/FDA/USDA assertion scripts must be written before this ADR's promise of "uniform detection" is real.
- **The Phase 6 reconciliation mechanism is unspecified.** Mapping table? Fuzzy-match on product attributes? Per-source normalization? Deferred until first source's Tier 2 evidence triggers a revisit. Risk: when triggered, design starts from scratch under time pressure.
- **Threshold values in the per-source table are educated guesses.** "0.1% per quarter" for CPSC and ">0.01% per month" for NHTSA are starting points. They should be revised as detection data accumulates.

### Neutral

- **Bronze remains audit-quality** — captures every distinct row NHTSA reports, including drift events. Both pre-edit and post-edit versions exist in bronze; silver only consumes the most recent per identity. This is the layered-warehouse pattern working as designed.
- **Front-end search (Phase 8) is not affected.** Per `implementation_plan.md:601` and `:663`, search lives in gold + API and is mechanism-TBD (Postgres FTS, external engine, etc.). This ADR doesn't constrain or enable that.

## Empirical evidence

| Source | What it shows |
|---|---|
| `documentation/nhtsa/incremental_delta_findings.md` Section G | 9-tuple PRE_2010 row-unique; POST_2010 0.007% multi-batch residue (17 anomaly groups, all explained by endman/bgman); cross-corpus 1-event drift (AC DELCO maketxt) |
| `dbt/models/silver/firm.sql:1-90` | Current firm exact-match normalization (no fuzzy yet) |
| `dbt/models/silver/recall_product.sql:14-46` | CPSC's `WITH ORDINALITY` + composite md5 pattern (NHTSA mirrors structurally) |
| `dbt/models/silver/recall_product.sql:49-69` | FDA's 1:1 bronze→product pattern (`md5('FDA'\|PRODUCTID)`) |
| `dbt/models/silver/recall_product.sql:71-89` | USDA's coarse-grain pattern (`recall_product_id = recall_event_id`) |
| `documentation/decisions/0002-unit-of-analysis-header-line-firm.md:36` | ADR 0002's quote: "cross-agency resolution requires fuzzy name matching" — firm scope, not product scope |
| `project_scope/implementation_plan.md:599-610` | Phase 6 firm-resolution deliverables; RapidFuzz dependency not yet in `pyproject.toml` |
| `pyproject.toml` | Confirms RapidFuzz isn't a dependency yet |

## Implementation

### dbt models — added in this PR

**`dbt/models/staging/stg_nhtsa_recalls.sql`** (new):
- Surface bronze columns silver consumes
- `DISTINCT ON (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman) ORDER BY ..., extraction_timestamp DESC` — pick the most-recent bronze row per 11-tuple

**`dbt/models/silver/recall_event.sql`** (modified):
- Add `nhtsa_events` CTE per the per-source table above
- `source_recall_id = campno`
- `DISTINCT ON (campno) ORDER BY campno, extraction_timestamp DESC`

**`dbt/models/silver/recall_product.sql`** (modified):
- Add `nhtsa_products` CTE per the per-source table above
- `recall_product_id` per the 11-tuple md5 recipe

**`dbt/models/silver/firm.sql`** (modified):
- Add `nhtsa_normalized` CTE: role='manufacturer', raw_name=mfgname, normalized_name=upper(trim(mfgname))

**`dbt/models/silver/recall_event_firm.sql`** (modified):
- Add NHTSA bridge: 1 row per unique (campno, mfgname) with role='manufacturer'

### Cross-source future work (tracked, not landed in this PR)

- ~~CPSC, FDA, USDA Tier 2 assertion scripts (per the per-source table)~~ **Closed 2026-05-08** — landed under `scripts/sql/<source>/bronze/assert_*.sql` and `dbt/tests/source_assumptions/` (severity=warn). The audit doc at `documentation/source_assumption_audit.md` is the canonical catalogue.
- Phase 6 product-level reconciliation mechanism (when a Tier 2 threshold first fires)
- USCG entries in the per-source table (when Phase 5d lands)
- **Severity escalation** for the dbt singular tests (warn → error) once empirical baselines stabilize and threshold-aware assertions become natural — deferred to the Soda Core / Great Expectations migration in Phase 7 per `implementation_plan.md:643`.

### Threshold revisit policy

The per-source revisit thresholds in the table above should be reviewed every 6 months or when a Tier 2 assertion produces a non-zero baseline result, whichever comes first. Revisit means: assess whether the threshold still represents an acceptable v1 limitation given accumulated empirical data, and either tighten the threshold (if rates have trended low) or trigger Phase 6 reconciliation work (if rates have crossed the threshold).

## Cross-source implications

**Standing requirement for future sources** (Phase 5d USCG, future): each new source's silver landing PR must include an entry in this ADR's per-source table — `recall_event.source_recall_id`, `recall_product_id` recipe, known fragmentation modes, Tier 2 detection plan, Phase 6 revisit threshold. The cross-source coverage is part of the standing per-source 5-step deliverable list.

**ADR 0002 boundary clarification:** ADR 0002's RapidFuzz infrastructure is firm-level only. Product-level reconciliation is this ADR's scope. When Phase 6 lands the firm-level RapidFuzz work, this ADR's product-level reconciliation can either reuse the RapidFuzz dependency or pick a different mechanism — that decision is deferred to Phase 6 and depends on what the Tier 2 evidence looks like.

**Front-end search (Phase 8) is unaffected.** Search via `GET /products/search` and the gold-layer denormalized index is a separate concern, mechanism TBD per `implementation_plan.md:601, 663`. This ADR's product-level reconciliation does not gate or enable the search work.
