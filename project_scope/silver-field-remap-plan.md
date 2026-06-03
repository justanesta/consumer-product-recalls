# Silver field remap — plan

- **Status:** Active — in progress on `feature/silver-field-remap`. W0 (this plan + ADR 0036 stub + master-plan pointer) is the first commit; W1–W6 pending.
- **Owns:** the "(a) silver-remap PR" execution — corrected cross-source silver field mappings, canonical column naming, and the dbt test suite, all grounded in **full-corpus bronze profiling**. Sits in the hard chain `6a → 6a.5 → this → 6b/6c` per `phase-6-execution-plan.md` § Sequencing Constraints.
- **Points at** (single-home — this plan does not restate any of them):
  - `documentation/audit/bronze_corpus_profile.md` (W1) — *what the full-corpus bronze looks like* (shape / null / enum / length / fragmentation / grain).
  - `documentation/audit/cross_source_consolidation.md` (W2) — *the semantic field → canonical-column map* that drives the SQL edits. Single home for the mapping + rename ledger + deferral registers.
  - `documentation/audit/methodology.md` — the audit method this continues (per-source Phase 6a audits, done); `documentation/<source>/field_audit_2026_w22.md` — the per-source evidence homes.
  - `phase-6-execution-plan.md` § "Phase 6a" + § "Sequencing Constraints"; `branch_sequencing_strategy.md` — cross-branch order.
  - ADR 0036 (cross-source canonical naming — this branch); ADR 0035 (cross-source SCD-2 authority — the SCD-applicability **verdict folds in here**); ADR 0033 (NHTSA SCD proof, cited); ADR 0031 (fragmentation baselines refreshed by W1); ADR 0027 (bronze storage-forced only → value normalization is silver's job).

## Context

The silver layer was built CPSC+FDA-first (Phases 5a–5c) and carries known semantic bugs — the headline: FDA `recall_event.description` is sourced from `distribution_area_summary_txt` (a geographic distribution list) instead of the product reason. It is wired only partially for NHTSA/USCG/USDA, uses per-source ad-hoc column choices rather than canonical cross-source names, and has thin dbt tests. Phase 6a (per-source API-doc-grounded field audits, all five `field_audit_2026_w22.md`) and Phase 6a.5 (full-corpus backfill — FDA re-seeded 2026-06-02 → 134,450 distinct; CPSC/NHTSA/USDA/USCG already seeded) are done, so the corrected silver can now be designed against the **full corpus**, not a dev slice. Building firm resolution (6b), history (6c), and gold (6e) on top of the current silver bakes in rework; this branch fixes the foundation and produces the two `documentation/audit/` artifacts the rest of Phase 6 builds off of.

## Scope contract — the "(a) PR" is silver-only

Every decision sorts into exactly one bucket (per `capture_expansion_backlog.md` § "Workflow for the (a) PR" + `methodology.md`):

- **THIS branch** — pure silver/staging projection over fields **already in bronze**: mismapping swaps, JSONB→column lifts, canonical renames, derives, element filters, CPSC Retailers-out-of-firm, NHTSA filer/manufacturer role split, dbt tests, the SCD-applicability *verdict* + the canonical-naming ADR.
- **6b (firm resolution)** — fuzzy matching / regex name-cleaning / DBA extraction / suffix-stripping / per-recall disambiguation / `firm.alternate_names`. Leave `firm.sql` / `recall_event_firm.sql` clean and in lockstep; do **not** build these.
- **(b) capture-expansion PR** — any column **not already in bronze**. Per user decision (2026-06-02): **FDA firm-address + firm-continuity fields** (`firmcitynam`, `firmstatecd`, `firmsurvivingnam`, `firmsurvivingfei`, …) — though now physically in bronze (migration 0019) — are **deferred to (b)**, placed per this branch's SCD verdict. Recorded in `cross_source_consolidation.md` §7 deferral register. Rationale: FDA is the only firm-dim address contributor (USDA/USCG addresses live in their own attribute dims), so the cross-source naming pass and the flat-vs-SCD placement are better decided once, in (b), informed by W3's verdict.

## Guardrails

- **The user runs all code** (psql, dbt, python) — workstreams describe SQL + commands, never run them. SQL lands under `scripts/sql/<source>/<layer>/<purpose>.sql`; results pasted back and folded into the audit docs.
- **UNION column-list parity** across all 5 source branches in `recall_event.sql` / `recall_product.sql` — every new column NULL-cast to the right type in *every* branch. Define the canonical column list once (from `cross_source_consolidation.md`) and fill each branch with source value or `cast(null as <type>)`. This is the top correctness risk.
- **No bronze schema change / migration / extractor edit / deep-rescan** (that's the (b) PR). The only non-`dbt/` + non-`documentation/` touch is adding `dbt/packages.yml` (test infra).
- **Bronze profiling is population/shape/enum/fragmentation EVIDENCE, not a re-derivation of field selection** (Phase 6a, done — see `methodology.md` "bronze is not the starting point"). "Empty in bronze" ≠ "stop capturing"; route such findings as observed-empty, never as capture-removal.
- **Distinguish measured from hypothesized** in the SCD verdict (NHTSA/USCG measured; CPSC/FDA/USDA hypothesized → measure now).
- Don't disturb the **`firm.sql` ↔ `recall_event_firm.sql` lockstep** (comment at `recall_event_firm.sql:22`/`:98`).

## Workstreams (done-markers)

| # | Workstream | Status |
|---|---|---|
| W0 | This plan + ADR 0036 stub + master-plan Phase-6 pointer | 🟡 in progress |
| W1 | **Full-corpus bronze profiling** → `documentation/audit/bronze_corpus_profile.md` + per-source `field_audit_2026_w22.md` §9 refresh. Reuse-first: rerun existing `explore_bronze_shape.sql` / `inspect_*` (refresh hardcoded date windows — FDA Q15 gap-window, USDA all-time cadence). NEW query files: `fda/bronze/inspect_field_population.sql`, `cpsc/bronze/inspect_firm_name_fragmentation.sql`, `uscg_manufacturers/bronze/explore_bronze_shape.sql`, `uscg_manufacturer_details/bronze/explore_bronze_shape.sql`, USDA recalls/establishments extends, optional `cross_source/bronze/profile_grain_and_keys.sql` | 🟢 **COMPLETE** (2026-06-02) — all 8 tables in `bronze_corpus_profile.md` §1–§6 + per-source §9 (FDA §8 / CPSC §9 / USDA §9 / NHTSA §9 / USCG §9 + `manufacturer_scraping_observations.md` §N) |
| W2 | **`cross_source_consolidation.md`** — semantic→canonical map (3 tables: `recall_event` / `recall_product` / `firm`+`recall_event_firm`+attribute-dims), §5 rename ledger, §6 null-fill / documented-empty matrix, §7 deferred-to-6b/(b) registers, §8 SCD verdict | 🟢 drafted + §0 decisions (D1–D7) resolved 2026-06-02; one external follow-up (USCG `severity` semantics → future `severity_rank` derive only) |
| W3 | **Cross-source SCD-applicability investigation** (read-only): measure CPSC (ordinality-shift), FDA (PRODUCTID stability), USDA (1:1 grain + `status_regulated_est` flip) edit-rate + silver-identity fragmentation at corpus scale. **Per-field designations + the monitor fleet live in `documentation/audit/scd_field_designations.md`** (catalogue + validation loop); the new `cross_source/scd_monitors/assert_classification_stable.sql` validates the `classification`/`severity` Type-2-BENEFIT call. Verdict → consolidation §8 + amend ADR 0035; canonical-naming decision → ADR 0036 | 🟢 designations catalogue + 3 monitors built; **ADR 0036 (naming policy + D1–D7) + ADR 0035 amendment (applicability verdict) written 2026-06-02** — both `Proposed`, ratify at PR merge. Forward-measurement of the BENEFIT amendment rates continues via the monitors. |
| W4 | **Silver/staging SQL remap** — Phase A (staging projections) → B (`recall_event`, incl. FDA Bug 1) → C (`recall_product`, USDA/NHTSA `type` bugs + USCG `model` bug) → D (`firm`+`recall_event_firm`: CPSC Retailers-out Option B + NHTSA filer/mfr split) → E (`dbas` element filter). Green `dbt build --select <model>+` per commit | ⏳ |
| W5 | **dbt tests** — `dbt/packages.yml` (dbt_utils); `accepted_values` from the W1 enum catalogue (generic test) — **except USDA `recall_reason`/`processing`, which are comma-joined multi-value and need a singular *exploding* test** (`unnest(string_to_array(col,','))` membership vs the 9/10-token taxonomy; a generic `accepted_values` on the raw 26/20 combinations would false-fail ~30% of `recall_reason` rows); create missing `stg_usda_fsis_establishments.yml`; singular regression guards (`assert_fda_recall_reason_not_distribution.sql` etc.); `recall_event_firm.role` list +`filer` −`retailer`; promote transitive-uniqueness comments to `unique_combination_of_columns` | ⏳ |
| W6 | **Doc sync** — `documentation/silver_design_notes.md` 2→5 sources (points at consolidation, no restate); `_silver.yml` descriptions (documented-empty-by-source, new roles, two-table firm join) | ⏳ |

## Per-source known fixes (inputs to W4 — from the Phase 6a audits)

- **FDA** — Bug 1: `recall_event.recall_reason ← product_short_reason_txt` (was `distribution_area_summary_txt`); the geographic list moves to its own `distribution_area_summary`. Bug 3: `recall_product.product_description ← product_description_txt`. Lift FDA event-date/notification fields already in bronze. (FDA `product_name` Bug 2 + firm-address → see Open questions / (b).)
  - **Normalization (data-scoped 2026-06-02 — `bronze_corpus_profile.md` §7):** Tier-0 `nullify_sentinels` staging macro on `product_distributed_quantity` (kept TEXT) + `distribution_area_summary`; Tier-1 silver derive `distribution_scope` (Nationwide/International/Regional/Unspecified — **no** negation guard, data shows ~0 true negations). Tier-2 quantity→value/unit + distribution→state/country parsing **deferred** → `project_scope/freetext-enrichment-backlog.md`.
- **USDA** — Bug 1: `recall_product.type ← processing` (was `recall_type`); derive `recall_event.risk_level` from `recall_classification` (1:1); lift `states`/`related_to_outbreak`/`archive_recall`/`closed_at`/`company_media_contact`/`labels`/`distro_list`; `firm_establishment_attributes.dbas` strip `'N/A'`/`'None'`.
  - **Comma-separated multi-value (data-scoped 2026-06-02 — `bronze_corpus_profile.md` §4):** `recall_reason` + `processing` are comma-joined multi-value (30.3% / 2.0% of rows); silver **preserves the comma-joined TEXT** — no Tier-1 split (they are legitimately multi-value, can't collapse to one single-value derive). W5 validates them with a **singular exploding** `accepted_values` on the 9 / 10-token taxonomy. Tier-2 split → `text[]` arrays (`recall_reason_tokens` / `processing_categories`) **deferred** → `project_scope/freetext-enrichment-backlog.md`. (`states` is comma-joined too but messier — `Nationwide`/`Midwest` mixed in — so it joins the cross-source state-normalizer item, not the clean enum split. `risk_level` is a *single-value* Tier-1 derive, unrelated to this.)
- **CPSC** — §6 Option B: lift `retailers` out of the firm dim → `recall_event.sales_channel_narrative`; lift `remedies`/`remedy_options`/`injuries`/`images`/`consumer_contact`/`manufacturer_countries`/`in_conjunctions`/`product_upcs`. (Suffix-strip/DBA = 6b.)
- **NHTSA** — Bug 1: `recall_product.type ← rcltype`; filer/manufacturer role split (`mfgname`=filer + `mfgtxt`=manufacturer); lift `corrective_action`/`consequence_of_defect`(typo-fixed)/`notes`/`mfgcampno`/`influenced_by`/`fmvss`/`do_not_drive`/`park_outside`/`odate`.
- **USCG** — Bug: `recall_product.model ← NULL` (was a dup of product_name); `hin`/`model_year`/`severity` sentinel normalization at staging; lift `disposition`/`case_close_date`/`campaign_*`/`last_date`/`company_official`. Bug 3 (firm raw_name) likely already resolved by the merged Step-7 directory enrichment — **verify before editing**.
- **Cross-source rename** (decided once against the union): `recall_event.description → recall_event.recall_reason` (all 5 sources populate it with defect/reason narrative once FDA Bug 1 lands). Grep `dbt/models/gold/` (empty) + tests before renaming.

## Suggested PR / commit sequence

W0 → **W3 measurement (read-only)** → **W1 profiling** (wave-by-wave: FDA/CPSC/USDA-recalls first = highest leverage; then USDA establishments + USCG; NHTSA `assert_*` rerun in parallel; user runs, results fold into §9 + `bronze_corpus_profile.md`) → **W2 consolidation** → **W3 ADRs** → **W4** (A→B→C→D→E, each a green-build commit) → **W5 tests** → **W6 doc sync** + PR-time honesty checklist. May land as one PR, or split W1/W2 audit docs from W4/W5 SQL if the diff grows — decide at PR time; cross-branch order stays in `branch_sequencing_strategy.md`, not here.

## Open questions

- **FDA `product_name` (Bug 2).** Keep `product_name ← product_description_txt` (status quo) vs `LEFT(product_description_txt, N)`. Recommend keep — the preferred `productdescriptionshort` is a (b) add; record in consolidation §7.
- **USCG Bug 3.** Verify the merged Step-7 directory enrichment (`coalesce(directory.company_name, …)`) already satisfies the audit's company-name-first recommendation before touching the lockstep'd USCG firm branch.
- **ADR 0036 vs 0035 split.** Refinement of the approved plan: ADR 0036 = cross-source canonical naming only; the SCD-applicability verdict folds into ADR 0035 (the cross-source SCD-2 authority) + consolidation §8, keeping SCD reasoning single-homed.

## Related

- `documentation/audit/methodology.md`, `documentation/audit/capture_expansion_backlog.md` (the sibling "(b) PR" parking lot) — the audit framework.
- `project_scope/phase-6-execution-plan.md` (parent), `project_scope/branch_sequencing_strategy.md` (cross-branch order), `project_scope/silver_v15_migration_plan.md` (NHTSA SCD workstream that consumes the remapped silver).
- `project_scope/freetext-enrichment-backlog.md` — Tier-2 structured-extraction enrichment deferred out of this branch (Phase 6/7).
- ADRs 0036 / 0035 / 0033 / 0031 / 0027 / 0002 as above.
