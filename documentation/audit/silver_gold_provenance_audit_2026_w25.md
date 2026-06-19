# Silver/Gold Data-Provenance Audit (2026-W25)

**Date:** 2026-06-19 · **Branch:** `chore/data-provenance` · **Status:** findings surfaced; fixes staged (see §7)

A three-way reconciliation of every medallion object's **CONTRACT** (what the ADRs promise),
**DOCUMENTED** shape (what the schema docs + dbt yml say), and **PRODUCED** shape (what the model SQL
actually emits) — plus the runnable query catalog to verify the served shapes against live Neon.
Triggered by the empty-string finding (CPSC/NHTSA `''` leaking into silver/gold); widened to a full
provenance + documentation audit per the W25 request.

## 1. Method

Multi-agent workflow (`silver-gold-provenance-audit`), all agents read-only (`sql-reviewer`: Read/Grep/Glob,
no DB/network/secret access):

| Phase | Output |
|---|---|
| **Contract** | 276 Silver/Gold shape clauses extracted from all 42 ADRs |
| **Documented** | 558 documented column-shapes harvested from `data_schemas.md`, the design notes, `scd_field_designations.md`, all dbt `.yml` |
| **Derive** | produced shape (type/enum/nullability/grain/empty-string-risk) of all 43 models + 4 snapshots |
| **Reconcile** | **106 discrepancies** + **123 runnable audit queries** across 7 object groups |
| **Verify** | adversarial confirmation at file:line + SQL validation (every discrepancy + query) |
| **Critic** | coverage gaps, cross-cutting themes, prioritized fixes |

Verify verdicts: **~100 confirmed/adjusted, 3 refuted, ~10 needs-live-data.** The adversarial layer earned
its place — it refuted 3 "positive-control" false flags (`distribution_scope`, `role`, `unit_category` —
all correctly tested) and corrected the derive step's mis-typing of date columns as text. Run-1 was
partially rate-limited (server-side); the analytic layer was recovered from agent transcripts and the
verify tail re-run, so the result set is complete.

> The full structured data (per-discrepancy verdicts, all 123 queries with verify-corrected SQL) is in the
> session working set; the runnable queries are materialized under
> `scripts/sql/cross_source/provenance_audit/` (§6).

## 1b. LIVE RESULTS — what the catalog run verified (2026-06-19, prod)

The catalog was run against prod (`data/exploratory/provenance_audit/*.out`). **This section is
authoritative and supersedes the static inferences below wherever they disagree.** Headline: the **served
Silver/Gold surface is empty-string clean and structurally sound**; the FDA-classification drift is the one
confirmed contract defect; several static empty-string findings were **refuted by the data**; and one new
finding surfaced (`event_type` absent).

| Finding | Static call | **Live verdict** | Evidence |
|---|---|---|---|
| FDA `classification` = `{1,2,3,NC}` not `Class I/II/III` | HIGH | **CONFIRMED** | `recall_event` + `mart_recall_summary` + `fct_recalls_by_classification`: FDA `1`=7523/`2`=34165/`3`=8902/`NC`=12 |
| USDA firm-sidecar `''` leak (`establishment_name`/`city`/`state`/`zip`) | HIGH bug | **REFUTED** | `firm_usda_attributes` + snapshot: **all 0 empty**. Columns lack `nullif` but the source never blanks them → *latent gap, no live occurrence* |
| `recall_event.url` `''` leak (CPSC/USCG) | MED bug | **REFUTED** | `mart_recall_summary.url` + `mart_product_search.url` = 0 empty |
| `release_type` `''` | MED | **REFUTED** | domain = `{Firm, FDA, State}`, no `''` |
| `firm_crosswalk.match_confidence` `rapidfuzz_high` drift | MED (#4) | **REFUTED** | live value is `rapidfuzz_rollup`; all 13 bridge values ⊆ the 17-value accepted set |
| All other served free-text (`consumer_contact`, `corrective_action`, `consequence_of_defect`, `fmvss`, `recall_reason`, `notes`, product free-text, FDA/USCG firm sidecars) | risk | **CLEAN (0 `''`)** | `recall_event`/`recall_product`/`mart_*` sweeps all 0 |
| `event_type` column (ADR 0003) | uncovered | **NEW FINDING — column ABSENT** | `recall_event` has no `event_type`; ADR 0003's `TEXT NOT NULL DEFAULT 'RECALL'` was never implemented |
| `recall_event_id` md5 recipe | unverified | **CONFIRMED CORRECT** | `bad_recipe = 0` (byte-exact `md5(source\|source_recall_id)`) |
| `terminated_year` = FDA+USDA+USCG (doc says USDA+USCG) | doc_wrong | **CONFIRMED** | FDA `n_nonnull` = 41,645 |
| `mart_recall_summary` distribution arrays NULLABLE (#6) | MED | **CONFIRMED** | 53,345 NULL `distribution_state_codes`/`_country_codes` (vs O1 arrays = 0 null) |
| `quantity_basis` domain | docs say `{per_product, total_all_products}` | **NEW — 3rd value `unknown`** (3,948 rows) | `recall_product` |
| `recall_lifecycle` NHTSA presence | docs "USDA-only" | **STALE — now USDA+NHTSA** | NHTSA `is_currently_active` non-null = 30,075 (C16 manifest banked) |
| accepted_values domains (#5: `classification`/`lifecycle_status`/`risk_level`/`initiated_by`/uscg `status`) | proposed | **DOMAINS CONFIRMED** (tests well-founded) | per-source enum runs all within the proposed sets |
| Grain/uniqueness/FK/O1-arrays/key-recipe/index/date-sanity (≈40 checks) | — | **ALL CONFORM** | incl. FEI functional index present, `recall_product_id` 470,973 unique, gold_meta single-row |

**Revised actionable list** (the empty-string *code* fixes are now largely unnecessary — the served data is
already clean):

1. **FDA `classification` doc reconciliation** (ADR 0042 ✓done, `_gold.yml` ✓done; remaining: `data_schemas.md`) + add `accepted_values(warn)` on `recall_event.classification` over the real union. **Confirmed.**
2. **`event_type`** — implement ADR 0003's column (`TEXT NOT NULL DEFAULT 'RECALL'`) **or** mark ADR 0003 deferred/superseded; it is currently absent. **New, decision needed.**
3. **`accepted_values(warn)` tests** for `classification`/`lifecycle_status`/`risk_level`/`initiated_by` (+ uscg `status`) — domains are now empirically confirmed (#5).
4. **`mart` distribution arrays** — `coalesce(...,'{}')` or document NULL-vs-`{}` for the API (#6, confirmed: 53,345 NULL).
5. **Doc-only**: `data_schemas.md` `terminated_year` → FDA+USDA+USCG; `recall_lifecycle`/`_silver.yml` presence → USDA+NHTSA; document `quantity_basis = 'unknown'`; `database_overview.md` ER `text[]` (confirmed `ARRAY`/`_text`); `_silver.yml` `recall_event` inventory (`is_active` etc.).
6. **Optional / DROP** — USDA-sidecar `nullif`, `recall_event.url` `nullif`, `release_type` `nullif`, `firm_crosswalk` test: **refuted by data**. Add `nullif` only as defensive hardening for a latent gap, not a fix.

### Applied on `chore/data-provenance` (2026-W25)

- **ADR 0042** + **`_gold.yml`** — FDA `classification` corrected to `{1,2,3,NC}` (done earlier this branch).
- **ADR 0003** — `event_type` recorded as deferred-not-implemented, with the two ship triggers (PHA-semantics clarification / a recall-adjacent feed).
- **`_silver.yml`** — added `classification` / `lifecycle_status` / `risk_level` / `initiated_by` `accepted_values(warn)` tests + `firm_uscg_attributes.status` test (domains live-confirmed) **and** column docs for `classification`/`lifecycle_status`/`is_active`/`risk_level`/`initiated_by`/`terminated_year`; `is_currently_active`/`was_ever_retracted` → USDA+NHTSA. **Author-only — run on the next `dbt build`/`dbt test`.**
- **`_gold.yml`** — `mart_recall_summary.distribution_state_codes`/`_country_codes` documented as **NULLable at mart grain** (decided: document, not coalesce — NULL = no distribution-area row vs `'{}'` = matched/empty; API models `list | None`).
- **`data_schemas.md`** — `terminated_year` → FDA+USDA+USCG. **`database_overview.md`** — ER arrays annotated `text[]`.
- **Catalog** — 5 query bugs fixed (3 type-mismatch `''` probes on non-text columns, the graceful `event_type` existence probe, the missing `url` probe).
- **`_silver.yml` `recall_event` inventory — COMPLETE:** all **52/52** columns now documented (diffed against the live type sweep — zero missed, zero typo'd; per-source field→source map stays single-homed in `cross_source_consolidation.md` §1).
- **Deferred:** only the `nullif` defensive-hardening items (refuted by data — latent gap, not a defect).
- **Capstone DONE:** API handover written to `consumer-product-recalls-api/project_scope/data-side-provenance-handover-2026-06-19.md`. A broad search of that repo found its **current code + `provenance-analysis-2026-06-17.md` are already correct** (FDA `1/2/3/NC`, distribution arrays `list|None`, presence flags pruned, `event_type` absent) — so the handover is framed as **independent cross-repo confirmation** + closing their two `UNVERIFIED` items with our live measurements + flagging the **stale `build/01–05`** planning docs (which still say FDA `Class I/II/III`, contradicting their own code). No API code change required by this branch.

## 2. Headline finding — published-contract enum drift (FDA `classification`)

**ADR 0042 (line 16) states `classification` is source-native "FDA/USDA `Class I/II/III`".** That is **wrong
for FDA.** The enforced + served FDA value is `{1, 2, 3, NC}`:

- `stg_fda_recalls.yml:41-46` — `accepted_values: ['1','2','3','NC']`, description "1/2/3 + NC".
- `recall_event.sql:138` — FDA branch passes `center_classification_type_txt` through verbatim (no `Class I` mapping).
- USDA's fourth value is the literal **`Public Health Alert`**, not the abbreviation `PHA` some docs imply.

So the real produced `classification` domain is
`{1,2,3,NC}` (FDA) ∪ `{Class I,Class II,Class III,Public Health Alert}` (USDA) ∪ `{H,L,M,S}` (USCG) ∪ `NULL` (CPSC/NHTSA).
The mismatch propagates into **six** documents (ADR 0042, `_gold.yml` `fct_recalls_by_classification`,
`data_schemas.md`, `database_overview.md`, the derive layer's enum, and the anchor facts seeded into this
very audit). The `recalls-api` models its response enums on these "raw shapes" (ADR 0042), so a consumer
trusting the ADR would build `Class I/II/III` and break on `1`. **HIGH.**

There is also **no `accepted_values` test on silver `recall_event.classification`** (ADR 0011 + 0015 name it
explicitly), so the drift is unguarded. Fix #1.

## 3. Empty-string normalization gaps (the trigger, generalized)

The trigger fix (`nullif(trim(...))` on CPSC/NHTSA free-text in `recall_product`/`recall_event`) closed the
*reported* columns but not the *class*. ADR-0027 `''`→NULL is owned inconsistently across four layers, and
`assert_no_blank_freetext_serving` covers only `recall_product` + `recall_event`. Confirmed leaks **outside**
that guard:

| Object.column | Evidence | Lands in | Sev |
|---|---|---|---|
| `firm_usda_attributes` `establishment_name`/`city`/`state`/`zip` | `stg_usda_fsis_establishments.sql:32,35-37` raw vs siblings nullif'd (the file's own header line 11 declares the discipline it breaks) | published `mart_firm_profile.firm_usda_attributes` (ADR 0042) | **HIGH** |
| `firm_usda_attributes.county` / `geolocation` | `stg_usda_fsis_establishments.sql:38,40` = `nullif(col,'false')` only, no `nullif('')`; bronze default `''` | same | MED |
| `recall_event.url` (CPSC + USCG) | `recall_event.sql:52,379` raw; `stg_cpsc:25`/`stg_uscg:88` no nullif | published `mart_recall_summary.url` (a user-visible link) | MED |
| `recall_event_press_release.release_type` | FDA returns null and `''`; undocumented passthrough | press-release child | MED |
| `recall_event.consumer_contact` (CPSC) | CPSC-only free-text, not in the fixed set, not nullif'd | silver | LOW |

**Note (audit self-correction):** the FDA and USCG firm snapshots are **clean** — their check-cols are
`nullif`'d / CASE-mapped to NULL upstream (`stg_fda_recalls.sql:52-59`, `stg_uscg_manufacturer_details.sql:42-61`),
so the derive step's empty-string flags there are false. The leak is **USDA-establishment-specific** + the
`recall_event`/press-release columns above. Fix #2, #7.

## 4. Missing ADR-mandated `accepted_values` tests (silver)

ADR 0011 + 0015 require `accepted_values` on silver enum-like columns; `_silver.yml` has none for these:

| Column | Produced domain | Sev |
|---|---|---|
| `recall_event.classification` | `{1,2,3,NC,Class I,Class II,Class III,Public Health Alert,H,L,M,S}` + NULL | HIGH (fix #1) |
| `recall_event.lifecycle_status` | `{Ongoing,Terminated,Completed,Active Recall,Closed Recall,Public Health Alert,Open,Closed}` | MED |
| `recall_event.initiated_by` | `{firm,agency}` (ADR 0036) | LOW |
| `recall_event.risk_level` | `{High - Class I,Low - Class II,Marginal - Class III,Public Health Alert}` + NULL (USDA-only) | MED |
| `firm_uscg_attributes.status` | `{In Business,Inactive,Federal or State Agency}` (SCD-2 check-col, unguarded) | LOW |

The staging models mostly DO guard these per-source; the gap is a single silver-layer assertion of the
produced cross-source union. Fix #5.

## 5. Documentation single-home gaps

- **`_silver.yml` documents ~10% of `recall_event`'s ~60 columns.** Load-bearing ADR-0042 columns are
  entirely absent: `is_active` (tri-state, feeds `fct_recall_status` + API), `terminated_year`, `risk_level`,
  `reason_category_tokens`, `initiated_by`, `distribution_states`. A future schema change has no complete
  contract to review against. **HIGH.** Same pattern: `recall_product` (`label_artifact_name`,
  `distribution_list_artifact_name`), `mart_recall_summary` (`has_been_edited`, `edit_event_count`,
  `primary_firm_name` absent from `_gold.yml`), all four snapshots (no per-column `_snapshots.yml` entries).
- **`silver_design_notes.md` §6** lists NHTSA in the "`''`-club {FDA, USDA, NHTSA}" that "staging wraps in
  `nullif`" — but NHTSA's free-text is **not** nullif'd at staging (that's the trigger bug; the fix lives in
  the serving models). Doc-vs-code defect. **Fixed in this PR.**
- **`database_overview.md` ER diagram** types `distribution_state_codes` / `distribution_country_codes` as
  scalar `text`; they are `text[]` (`recall_distribution_area.sql:239,248,256,258`). **Fixed in this PR.**
- **`data_schemas.md`** says `terminated_year` is "USDA+USCG only"; `recall_event.sql:159` populates it for
  **FDA** too. Doc-wrong, LOW.

## 6. Runnable query catalog

`scripts/sql/cross_source/provenance_audit/` — **123 verified queries + coverage-gap queries**, read-only,
for you to run against Neon. Organized by layer; each block states the **conforming** result so a deviation
is a finding. Dimensions: type, enum-cardinality, nullability, empty-string, grain/uniqueness, range-sanity.

| File | Scope |
|---|---|
| `10_staging_audit.sql` | 10 staging views |
| `20_silver_event_audit.sql` | `recall_event` + children |
| `21_silver_product_audit.sql` | `recall_product` + distribution + lifecycle |
| `22_silver_firm_audit.sql` | `firm` + sidecars |
| `30_snapshots_audit.sql` | SCD-2 snapshots |
| `40_gold_marts_audit.sql` | serving marts (ADR 0042 contract) |
| `41_gold_facts_audit.sql` | `fct_*` + `dim_date` |
| `90_coverage_gap_queries.sql` | event_type, key-recipe, crosswalk vocab, distribution conflation (critic gaps) |

Verify-corrected SQL is used where the reconcile draft was wrong (e.g. seeds are in `public`:
`public.us_state_abbr.abbr`, `public.country_iso.alpha2`). **Exclude from the `''` probe:** date columns
(`bgman`/`endman`/`in_business`/`out_of_business`), `status_regulated_est` (`''` is meaningful), and the
history tables (`''` by design) — flagged in each file header.

## 7. Prioritized fixes

Ranked by the critic, annotated with verification status (✓ = I confirmed at file:line; ◻ = needs your live query).
Code fixes are **staged for your dbt build/test gates**, not applied blind.

| # | Fix | Type | Status |
|---|---|---|---|
| 1 | Reconcile FDA `classification` = `{1,2,3,NC}` across the 6 docs (incl. ADR 0042); add `accepted_values(warn)` on `recall_event.classification` over the real union | doc + dbt test | ✓ confirmed |
| 2 | `nullif(trim())` on `establishment_name`/`city`/`state`/`zip`(/`county`/`geolocation`) in `stg_usda_fsis_establishments.sql`; extend a blank-freetext guard to the firm sidecars (they lack a `source` col → sidecar-specific test) | dbt model + test | ✓ confirmed |
| 3 | `accepted_values` on `recall_event.event_type` (`{RECALL}`) + `WHERE event_type='RECALL'` (or documented equivalent) on every gold model | dbt test + models | ◻ static |
| 4 | `accepted_values(error)` on `firm_crosswalk.match_confidence` matching the bridge's 17-value set; resolve `rapidfuzz_high` vs `rapidfuzz_rollup` | dbt test | ◻ live |
| 5 | `accepted_values(warn)` on `lifecycle_status`, `initiated_by`, `risk_level` in `_silver.yml` | dbt test | ✓ confirmed |
| 6 | `mart_recall_summary.distribution_state_codes`/`_country_codes`: `coalesce(...,'{}')` at mart OR document NULL-vs-`{}` for the API | dbt model OR doc | ◻ live |
| 7 | `nullif(trim())` on `recall_event.url` (CPSC/USCG) + `release_type`; add both to `assert_no_blank_freetext_serving` | dbt model + test | ✓ confirmed |
| 8 | Complete `_silver.yml` column inventory for `recall_event` (~60 cols); drop `database_overview.md` `and_50_more_cols` placeholder → pointer | doc | ✓ confirmed |
| 9 | Record the functional `firm_fda_attributes((firm_fei_num::text))` index + drop-then-create rationale in `index_audit.md` | doc | ✓ confirmed |
| 10 | `accepted_values(warn)` + `_silver.yml` block for `firm_uscg_attributes.status` | dbt test + doc | ✓ confirmed |

## 8. Coverage gaps (for a follow-on)

The audit scoped staging→gold. **Not** covered: the **bronze layer** (content-hash determinism ADR 0007,
`strict=True` coercion ADR 0014, within-batch dedup ADR 0030, replay-timestamp ordering ADR 0028) — zero
queries. `recall_product_id` recipe byte-verification (per-source 7-tuple/ordinal). These are candidate
queries (marked `[CONFIRM NAMES]`) in `90_coverage_gap_queries.sql`.

## 9. Audit transparency (false positives)

For trust calibration: the reconcile pass produced **3 refuted** items (correctly killed by verify) and
several derive-layer mis-typings (also caught). Refuted: `recall_event.distribution_scope`,
`recall_event_firm.role`, `fct_units_recalled.unit_category` — all are correctly documented **and** tested;
no discrepancy. Derive mis-typed `bgman`/`endman`/`in_business`/`out_of_business` (timestamptz) as text and
`geolocation` (text) as jsonb — corrected; these must be excluded from / included in the `''` probe
accordingly. One *secondary* reconcile claim (a fabricated `{1,2,3,NC}` mapping "in `stg_fda_recalls.sql`")
was wrong — but the **core** finding was right, located in `stg_fda_recalls.yml`. Lesson: enum domains are
enforced in the `.yml` `accepted_values`, not the `.sql`.
