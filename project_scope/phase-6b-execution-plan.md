# Phase 6b Execution Plan — Firm Entity Resolution

- **Status:** Active — sequenced PR-by-PR plan for `feature/6b-firm-entitiy-resolution`. **PR 6b.0 substrate drafted 2026-06-03** (migration 0024, `enrichment.firm_crosswalk` source, `recall_event_firm.match_confidence`, `firm.alternate_names`, `src/enrichment/` skeleton, G0 gate, `rapidfuzz` dep); ruff/pyright/yaml gates green; awaits user-run `alembic upgrade head` + `dbt build`. See the PR 6b.0 "as-built refinements" note.
- **Owning master plan:** `project_scope/implementation_plan.md` Phase 6. Supersedes the "Phase 6b — Firm Entity Resolution" section of `project_scope/phase-6-execution-plan.md` (lines ~166-277), which was written BEFORE silver-remap PR #58 and carries stale `firm.sql` line references and stale "greenfield" assumptions.
- **Branch lineage confirmed:** both `728a9a3` (#58 silver-remap) and `f630beb` (FDA FEI sidecar) are ancestors of HEAD, so the post-#58 shape AND `firm_fda_attributes.sql` are present in the working tree.
- **Hard operating constraints (every actionable step):** the USER runs ALL code (extractors, alembic, psql, dbt, the new CLI); agents only specify the exact command/SQL. SQL exploration lives under `scripts/sql/<source>/<layer>/<purpose>.sql` — never pasted multi-line into prose. Any helper under `scripts/` or `src/` meets the same bar as `src/`: `ruff check`, `ruff format --check`, `pyright`, and `pytest` coverage of pure logic. Every data figure below is tagged confirmed-in-doc / inferred / needs-corpus-requery; needs-requery figures are gated, never hard-coded.
- **⚠ Read `Appendix C` (Adversarial review corrections) before building any PR.** It carries BINDING mechanics-level fixes from a post-design critique — notably the `canonical_firm_id` regroup/lockstep (C1–C3), the `normalized_name` unique-test drop moving to 6b.1 (C5), and the NHTSA corporate-form regex ownership (C8). The PR bodies below state design intent; Appendix C states the precise mechanics that keep the build green.

---

## 1. Plan vs reality reconciliation (post-#58)

### 1.1 What PR #58 (silver-remap, `728a9a3`) + `f630beb` already delivered

The firm dimension is a **5-model family**, all plain `materialized='table'`, NO SCD-2 columns, NO fuzzy/normalization logic. Verified against the actual files (trust files over the old plan's line numbers):

- **`firm.sql`** (182 lines): conformed firm dim, grain one row per `normalized_name` (= `upper(trim(raw_name))`), PK `firm_id = md5(normalized_name)`. Columns: `firm_id, normalized_name, canonical_name, observed_names (jsonb), observed_company_ids (jsonb)`. Identity is md5-of-name ONLY; FEI/MIC/establishment_number ride inside `observed_company_ids`, they are NOT identity keys.
- **CPSC retailer lift-out (Option B) ALREADY LANDED.** `firm.sql`/`recall_event_firm.sql` union only manufacturer/importer/distributor (no retailer branch). Retailers relocated to `recall_event.sales_channel_narrative`. `_silver.yml` role `accepted_values` is `['manufacturer','importer','distributor','establishment','filer']` (no `retailer`). Guarded by `dbt/tests/assert_no_retailer_role.sql`. **Do not re-plan the retailer lift-out — it is done.**
- **NHTSA filer/manufacturer split ALREADY LANDED.** NHTSA now emits TWO firms/bridge rows per recall: `mfgname` role `'filer'` (firm.sql:91-114) + `mfgtxt` role `'manufacturer'`. Guarded by `dbt/tests/assert_nhtsa_filer_and_manufacturer_roles.sql`.
- **FDA firm role relabeled `'manufacturer'` → `'establishment'`** with `company_id = firm_fei_num::text` written into `observed_company_ids` (firm.sql:64-76).
- **Three per-source sidecars EXIST** (none is SCD-2; all plain tables): `firm_establishment_attributes` (USDA, one row/establishment_number), `firm_manufacturer_attributes` (USCG, one row/mic, listing-only truncated address, Phase 5d), `firm_fda_attributes` (FDA, one row/firm_fei_num via DISTINCT ON latest, added by `f630beb`).
- **`recall_event_firm.sql`** is the M:N bridge, grain one row per `(recall_event_id, firm_id, role)`, `firm_id = md5(upper(trim(name)))` in every branch, kept in **strict lockstep** with `firm.sql` (the relationships test polices it; 1549 orphans observed 2026-05-30 before alignment).
- **`dbt/tests/` is an established singular-test directory** (10 asserts today). `dbt/macros/`, `dbt/snapshots/` hold only `.gitkeep`. No `dbt/seeds/`. Latest alembic migration is `0023`. `rapidfuzz` is absent from `pyproject.toml` (version `0.13.0`).

### 1.2 Stale claims in the old Phase-6 plan — corrected

| Old plan claim | Reality (trust this) |
|---|---|
| AC DELCO drift "produces 2 rows per `firm.sql:21-22`" | Comment is at **firm.sql:24-25**. AC DELCO/ACDELCO is a **`maketxt` (vehicle MAKE) drift** on campno 22E002000, TSV substrate only, **0 occurrences as a firm-name value** (confirmed-in-doc). It fragments **recall_PRODUCT rows** via the 11-tuple `recall_product_id`, NOT firm rows. Fix belongs in `recall_product.sql`, not the firm dim (ADR 0031). |
| USDA join at `firm.sql:75-86` | Now **firm.sql:78-89** (`usda_normalized` CTE). Logic unchanged. Re-locate by CTE name, never line number. |
| "NHTSA contributes a single scalar `mfgname` manufacturer" | NHTSA emits **two** firms (filer + manufacturer). Full-corpus net-new is **3,940 distinct firms** (3,569 filers + 2,836 mfrs, 2,465 both; 95.9% disjoint when differing) — confirmed-in-doc at 321,592 rows. The old "+24" was a 74,604-row dev slice (superseded). |
| "Implement matching with FDA `firm_fei_num` as anchor" (implies FEI keys the firm dim) | `firm.sql` keys ALL sources on `md5(upper(trim(name)))`; FEI rides only in `observed_company_ids` and keys the `firm_fda_attributes` **sidecar**. Whether FEI anchors a separate `firm_identifier` table is the OPEN question (old plan line 67) — see Critical Decision #2. |
| "`firm_manufacturer_attributes` is greenfield" + "FDA FEI sidecar is future work" | Both sidecars **already exist**. The USCG SCD-2 work is a **conversion** of an existing model, not a create. The FDA FEI sidecar shipped in `f630beb`. |
| "Removing Retailers[] is part of the 6b Option B change" | Already shipped by #58. |

### 1.3 Corrected starting point

All of Phase 6b is greenfield ON TOP of the #58 shape, EXCEPT the two existing sidecars (USCG is converted to SCD-2; FDA stays as-is). The remaining net-new work is: `rapidfuzz` dep; a shared deterministic name-cleaning macro; CPSC suffix-strip + DBA-extract; USDA recall→establishment disambiguation; FDA FEI deterministic pre-merge edges; NHTSA name normalization; the RapidFuzz cross-source crosswalk; the AC DELCO product-level fix; the USCG SCD-2 snapshot + time-sensitive join; and the Honda/Tyson cross-source rollup tests. Two new silver columns (`firm.alternate_names`, `recall_event_firm.match_confidence`) plus `recall_event_firm.establishment_number` and an additive `canonical_firm_id` are all **dbt-computed silver columns** — they need NO alembic migration. The ONLY migration (0024) is the Python-written `firm_crosswalk` source table.

---

## 2. Normalization-engine architecture decision (the spine)

**DECISION: HYBRID (Lane F / D1).** Three layers, each in the venue that fits it:

1. **Deterministic name cleaning** (CPSC suffix-strip + DBA-extract, NHTSA corporate-form/regional-suffix cleanup, AC DELCO trivial alias) runs **in dbt-SQL** via a single reusable macro `clean_firm_name()` (plus `extract_firm_dba()`), called from the **silver firm models** so the cleaning is single-homed. Per ADR 0027, deterministic value-normalization belongs in dbt-SQL, not bronze, not Python. Uses stock Postgres `regexp_replace`/`regexp_match` — **no `CREATE EXTENSION`** (pg_trgm/fuzzystrmatch are absent from all project code and Neon has no dbt-python runtime — confirmed-in-doc).
2. **Edit-distance clustering** (the genuinely hard residual SQL can't do — e.g. `TOYOTA MOTOR ENGINEERING & MANUFACTURING` ↔ `TOYOTA MOTOR CORPORATION`) runs in a new tested Python module `src/enrichment/firm_resolution.py`, driven by a `recalls resolve-firms` Typer subcommand **the USER runs**, writing a `firm_crosswalk` table registered as a dbt SOURCE.
3. **`match_confidence`** is a dbt-computed column on `recall_event_firm`; deterministic tiers are CASE expressions, the `rapidfuzz_*` tiers arrive pre-stamped on `firm_crosswalk` and ride through the LEFT JOIN.

**Rationale.** (a) Keeps Neon free of a Python runtime it lacks; (b) respects the `firm.sql ↔ recall_event_firm.sql` lockstep by normalizing UPSTREAM of both (the shared macro is applied identically in both files, so md5 inputs can't diverge); (c) concentrates the portfolio-worthy edit-distance work on the ~residual that needs it; (d) maximizes DE breadth (SQL regexp + tested Python clustering + dbt source-join + dbt snapshot SCD-2). Pure-SQL (pg_trgm/levenshtein) is rejected — weak for multi-token company names, buries the algorithm, and needs a CREATE EXTENSION migration not present. Pure-Python is rejected — it over-engineers the 99% deterministic cases and re-implements set-based work the warehouse does better.

**firm_id key decision (Critical Decision #2): KEEP `md5(normalized_name)` as `firm_id`; add an ADDITIVE `canonical_firm_id`** sourced from `firm_crosswalk` via `coalesce(crosswalk.canonical_firm_id, md5(normalized_name))`. Non-breaking: the relationships test and the three sidecar `observed_company_ids` joins survive unchanged; a firm with no cluster IS its own canonical (the coalesce default). A FEI/MIC/establishment-keyed `firm_identifier` table can't be primary because CPSC and NHTSA (3,940 firms) carry no structured id, and 53 FDA firms (0.4%) lack an FEI. ADR 0002 explicitly declines to prescribe a `firm_identifier` table.

**Lockstep invariant (binding, every PR that touches firm identity):** any change to `normalized_name`/`firm_id`/the crosswalk join MUST be applied IDENTICALLY in BOTH `firm.sql` and `recall_event_firm.sql` in the SAME PR. The `recall_event_firm.firm_id` → `firm.firm_id` relationships test is the guard. New columns added to the bridge must be NULL-cast across ALL union branches (5 source branches; note NHTSA contributes TWO branches: filer + manufacturer).

---

## 3. PR sequence overview

| PR | Title | Depends on | Gates before build |
|---|---|---|---|
| **6b.0** | Substrate: `rapidfuzz` dep, shared `match_confidence` column + accepted_values, `clean_firm_name`/`extract_firm_dba` macros (scaffold), `firm_crosswalk` migration 0024 + source registration, `firm.alternate_names` column | — | G0 (distinct-name count + overlap) |
| **6b.1** | CPSC firm-name normalization (suffix-strip + DBA-extract) | 6b.0 | G1 (comma-optional strip coverage), G1b (d/b/a form), G1c (bronze single-shot) |
| **6b.2** | USDA recall→establishment disambiguation (`recall_event_establishment_resolution` + signal hierarchy) | 6b.0 | G2 (dup-name split @ name,city,state), G3 (Signal-1 product_items coverage), G4 (field_states pollution), G5 (cold-storage activities) |
| **6b.3** | FDA FEI deterministic edges + NHTSA name normalization + AC DELCO product fix | 6b.0, 6b.1 (macro) | G6 (FDA FEI artifact), G7 (NHTSA mfgname artifact), G8 (maketxt space-collapse safety) |
| **6b.4** | RapidFuzz cross-source crosswalk (`recalls resolve-firms` CLI + firm_id remap) | 6b.0, 6b.1, 6b.3 | G0 (re-confirm scale/blocking) |
| **6b.5** | USCG SCD-2 snapshot + time-sensitive recall join (ADR 0035 acceptance) | 6b.0 | G9 (MIC recycle set re-confirm); BLOCKED on ADR 0035 acceptance |
| **6b.6** | Cross-source rollup dbt tests (Honda, Tyson) + escalate quality gates | 6b.1, 6b.2, 6b.3, 6b.4 | — |

PRs 6b.1, 6b.2, 6b.5 are largely independent after 6b.0 and could land in parallel; 6b.3 reuses 6b.1's macro; 6b.4 needs the deterministic strip in place; 6b.6 is the acceptance gate. 6b.5 is split-friendly (it's the only PR blocked on ADR 0035) — if ADR 0035 stalls, ship 6b.0-6b.4 + 6b.6 and defer 6b.5 to a 6c follow-up.

---

## PR 6b.0 — Substrate (deps, shared column, macros scaffold, crosswalk table)

> **As-built refinements (2026-06-03).** Two risk-reducing deltas vs the original synthesis scope below:
> - **R1 — `canonical_firm_id` model wiring deferred entirely to PR 6b.4.** The no-op coalesce in 6b.0 would still require restructuring the lockstep-critical `firm.sql` grouped-select (an outer SELECT wrapping the grouped CTE — critic C1) and adding an outer SELECT over `recall_event_firm`'s bare 5-CTE union (critic C3). Touching those two files **once** in 6b.4 (where the crosswalk is populated and the regroup mechanics are specified, C2) beats a no-op now + the real change later. So 6b.0 lands the `firm_crosswalk` table + source **unused** (a registered source with 0 rows is valid dbt); 6b.4 owns all `firm`/`recall_event_firm` join + regroup edits.
> - **R2 — `clean_firm_name` / `extract_firm_dba` macros owned by PR 6b.1**, not scaffolded empty here. 6b.1 is their first consumer (creates + fills + wires CPSC); 6b.3 extends them for NHTSA corporate-form strings (critic C8). No empty-macro artifact in 6b.0.
>
> **6b.0 actually shipped:** `migrations/versions/0024_firm_crosswalk.py`; `enrichment.firm_crosswalk` in `dbt/models/staging/_sources.yml` (`schema: public`, critic C9); `recall_event_firm.match_confidence` (default `'exact_name'`, all 6 branch selects); `firm.alternate_names` (null placeholder); the `match_confidence` `accepted_values` enum at `severity: warn` in `_silver.yml` (incl. a reserved `rapidfuzz_medium`, critic C12); `src/enrichment/__init__.py`; `scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql` (G0); `rapidfuzz>=3.10,<4` in `pyproject.toml`. The `normalized_name` unique test is **left intact** in 6b.0 (it only breaks once CPSC strip merges within-source — dropped in 6b.1 per critic C5). Version bump (`0.13.0`→`0.14.0`) left to the user.

**Scope.** Land everything the resolution PRs share so they don't collide: the `rapidfuzz` dependency, the single shared `recall_event_firm.match_confidence` column with its complete namespaced `accepted_values` list, the empty `clean_firm_name`/`extract_firm_dba` macros (scaffold returning `upper(trim())` as a no-op until 6b.1 fills them — keeps the call sites stable), the `firm.alternate_names` column (empty until 6b.1), the `firm_crosswalk` source table (migration 0024 + `_sources.yml` registration + the additive `canonical_firm_id` coalesce wired but resolving to a no-op until 6b.4 populates the table), and the `src/enrichment/` package skeleton.

**Corpus gate (run FIRST).** G0 — size the RapidFuzz input and the re-key blast radius.
- Script: `scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql` (NEW). Q1 distinct `normalized_name` per source; Q2 total distinct + cross-source name overlap; Q3 pairwise comparison count per first-token block.
- User runs: `psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql`
- Expected signal: distinct-name count in the low tens of thousands → brute-force feasible; if larger, the resolve-firms CLI defaults to first-token blocking. Sizes the `canonical_firm_id` remap magnitude for 6b.4.

**Files — new.**
- `dbt/macros/clean_firm_name.sql` (scaffold: returns `upper(trim({{ col }}))`; 6b.1 fills the regex)
- `dbt/macros/extract_firm_dba.sql` (scaffold: returns `cast(null as text)`; 6b.1 fills)
- `migrations/versions/0024_firm_crosswalk.py`
- `src/enrichment/__init__.py`
- `scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql`

**Files — modified.**
- `pyproject.toml` — add `rapidfuzz>=3.10,<4` to `[project.dependencies]` with a `# Phase 6b — cross-source firm entity resolution` comment (mirrors the beautifulsoup4/lxml precedent at lines 24-27). Bump `version` `0.13.0` → `0.14.0` (USER does the manual edit; `recalls version` reads it via importlib.metadata).
- `dbt/models/staging/_sources.yml` — register a new source `enrichment` with table `firm_crosswalk` (tests: `firm_id` not_null+unique, `canonical_firm_id` not_null).
- `dbt/models/silver/firm.sql` — add `alternate_names` (empty `jsonb_agg ... filter` placeholder) to the final select; LEFT JOIN `source('enrichment','firm_crosswalk')` and emit `coalesce(x.canonical_firm_id, md5(normalized_name)) as canonical_firm_id` (additive; resolves to `md5(normalized_name)` until 6b.4 populates the table). **`firm_id` recipe unchanged.**
- `dbt/models/silver/recall_event_firm.sql` — add `match_confidence` (default `'exact_name'`) and `canonical_firm_id` (same coalesce) to EVERY branch; NULL/literal-cast the new columns across all 5 source branches. **`firm_id` recipe unchanged (lockstep).**
- `dbt/models/silver/_silver.yml` — add `recall_event_firm.match_confidence` with `accepted_values` listing the full shared set at `severity: warn` (NULL-allowed): `exact_name, cpsc_suffix_strip_exact, cpsc_dba_extract_exact, usda_unambiguous, usda_product_items_extract, usda_state_match, usda_processing_match, usda_multi_signal, usda_ambiguous_null, uscg_mic_unambiguous, uscg_mic_time_sensitive_unresolved, uscg_mic_build_date_resolved, fei_exact, rapidfuzz_high, rapidfuzz_low_ambiguous_null, singleton`. Add `firm.alternate_names` (nullable jsonb, untested). Add `firm.canonical_firm_id` not_null. Update model descriptions.

**Migration 0024 (the ONLY migration in Phase 6b).** `revision='0024'`, `down_revision='0023'`. `CREATE TABLE firm_crosswalk (firm_id text PRIMARY KEY, canonical_firm_id text NOT NULL, canonical_name text NOT NULL, match_confidence text NOT NULL, match_score numeric, resolver_version text NOT NULL, resolved_at timestamptz NOT NULL DEFAULT now())` + `CREATE INDEX ix_firm_crosswalk_canonical ON firm_crosswalk(canonical_firm_id)`. Module docstring cites ADR 0002, the Lane-F hybrid decision, and old-plan line 67. Follows the 0022 docstring/typed-vars/reversed-drop convention.

**dbt tests.** `firm_crosswalk` source tests (above). The existing `firm`/`recall_event_firm` unique + relationships tests must stay green (proves the additive coalesce didn't break lockstep).

**Verification.**
- User runs: `alembic upgrade head` (creates the empty `firm_crosswalk`).
- User runs: `dbt build --select firm recall_event_firm +source:enrichment.firm_crosswalk` (the source has no rows yet; coalesce falls back to md5 — clean build expected).
- User runs the specific test: `dbt test --select recall_event_firm` → relationships + unique_combination still pass.
- Spot-check SQL: `scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql` output recorded for 6b.4 threshold-setting.

**Fits current design.** Purely additive on the #58 shape: `firm_id` recipe untouched, the new columns NULL-cast across branches exactly as NHTSA `company_id = cast(null as text)` already does. The crosswalk-as-source mirrors how every bronze table lands. The shared-column-defined-once approach resolves the three-workstream `match_confidence` collision blocker up front.

**Risks.** (1) If 6b.4 never runs `resolve-firms`, the coalesce stays a no-op — acceptable (a firm with no cluster is its own canonical). (2) Defining the full `accepted_values` at `warn` keeps later PRs from breaking the build as they populate their values.

---

## PR 6b.1 — CPSC firm-name normalization (deterministic suffix-strip + DBA-extract)

**Scope.** Fill the `clean_firm_name`/`extract_firm_dba` macros with the deterministic CPSC suffix-strip (trailing `, of <geo>`, `, dba <name>`, `, doing business as <name>`) and DBA-brand capture; route CPSC `normalized_name`/`firm_id` through the macro in BOTH firm models (lockstep); populate `firm.alternate_names` from the extracted DBA; stamp CPSC `match_confidence` values on the bridge. Deterministic only — no Python, no rapidfuzz.

**Corpus gates (run FIRST).**
- **G1** — true comma-OPTIONAL `of` strip coverage + over-strip risk. The published 62.8%/5.7% (confirmed-in-doc) came from a comma-REQUIRED simulation pattern and UNDERCOUNT comma-less `of` (`Aria Child Inc. of Dedham, Mass.`). Status: needs-corpus-requery.
  - Script: `scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql` (NEW). Q1 collapse count under comma-required vs comma-optional; Q2 the comma-less `of` cohort isolated; Q3 a sample of would-be-stripped names to eyeball legal-name false positives (e.g. `Bank of America`).
  - User runs: `psql "$NEON_DATABASE_URL" -f scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql`
  - Expected signal: ADOPT comma-optional `of` IF the comma-less cohort is materially real AND the legal-name false-positive count is near-zero; else keep comma-REQUIRED.
- **G1b** — d/b/a slash-form prevalence. The simulation regex includes `d/b/a` but no verbatim example is cited.
  - Script: extend `scripts/sql/cpsc/bronze/inspect_firm_name_fragmentation.sql` (EXISTS) Q4 with a per-form breakdown (`dba` vs `d/b/a` vs `doing business as`).
  - Expected signal: KEEP the `d/b/a` alternation branch if >0 occurrences; DROP it (dead alternation) otherwise.
- **G1c** — confirm `cpsc_recalls_bronze` is the single-shot 9,828-record seed (the fragmentation script reads bronze raw with no extraction-version dedup). Expected signal: PROCEED if 9,828 distinct single-shot rows; otherwise add a `row_number()`-latest CTE before trusting collapse counts.

**Files — modified.**
- `dbt/macros/clean_firm_name.sql` — fill: order-sensitive `regexp_replace` (1: strip `,?\s+(dba|d/b/a|doing business as)\s+.*$`; 2: strip `,?\s+of\s+[^,]+\.?\s*$` — comma-optionality per G1; 3: collapse trailing `[\s,]+$`). Comment cross-references the lockstep rule.
- `dbt/macros/extract_firm_dba.sql` — fill: `nullif(trim((regexp_match(col, '(?:dba|d/b/a|doing business as)\s+(.+?)(?:,?\s+of\s+.*)?$','i'))[1]),'')`.
- `dbt/models/silver/firm.sql` — `cpsc_normalized` CTE: `normalized_name = upper(trim({{ clean_firm_name("firm_json ->> 'name'") }}))`; add `extracted_dba = {{ extract_firm_dba(...) }}`; add `cast(null as text) as extracted_dba` to the other 4 source CTEs (width-align the union); final select `jsonb_agg(distinct extracted_dba) filter (where extracted_dba is not null) as alternate_names`.
- `dbt/models/silver/recall_event_firm.sql` — `cpsc_event_firms` CTE: `firm_id = md5(upper(trim({{ clean_firm_name(...) }})))` (SAME macro → lockstep); emit CPSC `match_confidence` (`cpsc_dba_extract_exact` when a DBA was extracted, else `cpsc_suffix_strip_exact` when cleaned ≠ raw, else `exact_name`).
- `dbt/models/silver/_silver.yml` — `firm.normalized_name` unique stays for now (CPSC-only strip won't merge across sources yet); descriptions updated.
- `scripts/sql/cpsc/bronze/inspect_firm_name_fragmentation.sql` — extended for G1b.

**Files — new.**
- `dbt/tests/assert_no_cpsc_name_suffix.sql` — singular guard (plan's loud-failure check): returns any `firm.normalized_name` still matching `,\s*of\s+` or `\y(dba|d/b/a|doing business as)\y`.
- `dbt/models/silver/_unit_tests.yml` — native dbt `unit_tests` for `clean_firm_name`/`extract_firm_dba` over the Lane-B verbatim strings (preferred), with `dbt/tests/assert_clean_firm_name_examples.sql` (a `values()` fixture CTE) as the portable fallback.
- `scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql`
- `scripts/sql/cpsc/silver/measure_normalization_kpi.sql` — KPI over silver `firm`: total CPSC firms, strip-fired count, extracted-DBA count, firm-row reduction vs pre-6b baseline (house `\set ON_ERROR_STOP` + `\echo Qn` style).

**Unit-test cases (the macro examples, all Lane-B verbatim).** `ZOLIQUEX, of China`→`ZOLIQUEX`; `Apex Gaming PCs Inc., of Houston, Texas`→`APEX GAMING PCS INC.`; `Mobility Source Medical Technology Co., Ltd. of China` (comma-less, gated G1)→`MOBILITY SOURCE MEDICAL TECHNOLOGY CO., LTD.`; `Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China`→clean `CHEYOUHANG...CO., LTD.`, dba `ZOLIQUEX`; `Shenzhen Maikeer Industrial Co., Ltd., doing business as MalkerDirect, of China`→dba `MALKERDIRECT`; `3M Company, of {St. Paul, Minnesota / Saint Paul, Minnesota / St. Paul, Minn.}`→ all `3M COMPANY` (collapse demo); **negative regression** `Bank of America`→must NOT strip to `BANK`.

**Verification.**
- User runs: `dbt build --select clean_firm_name extract_firm_dba firm recall_event_firm` (macros compile via the models).
- User runs the unit tests: `dbt test --select test_type:unit` (or the singular fallback `dbt test --select assert_clean_firm_name_examples`).
- User runs the guard: `dbt test --select assert_no_cpsc_name_suffix` → 0 rows.
- User runs the lockstep guard: `dbt test --select recall_event_firm` → relationships still pass.
- Spot-check: `scripts/sql/cpsc/silver/measure_normalization_kpi.sql`.

**Fits current design.** Only changes HOW `normalized_name` is computed inside the existing `cpsc_normalized`/`cpsc_event_firms` CTEs; grain/PK unchanged; both models route through the SAME macro (lockstep by construction). ADR 0027 honored (deterministic strip in silver-SQL). Macros land in the configured-but-empty `dbt/macros/`. Additive `alternate_names`/`match_confidence` columns.

**Risks.** Over-strip on `of`-bearing legal names (mitigated by G1 + the negative unit test + the singular guard; no allowlist designed — a follow-up if G1 surfaces false positives). DBA-extract greediness on the extreme parenthetical case (covered by a unit test). Lockstep drift if a future edit changes only one call site (the relationships test fails loudly).

---

## PR 6b.2 — USDA recall→establishment disambiguation

**Scope.** New silver model `recall_event_establishment_resolution.sql` (one row per USDA `recall_event_id`) resolving each recall to AT MOST ONE `establishment_number` via a deterministic 5-signal precedence hierarchy, stamping `match_confidence`. Carry `establishment_number` + `match_confidence` onto the bridge's `usda_event_firms` CTE (NULL-cast across all branches). Add same-facility collapse (`establishment_group_id`) and a `cold_storage_flag` to `firm_establishment_attributes`. Precision-over-recall: every signal emits only when it collapses to exactly one facility-group, else `usda_ambiguous_null` with a NULL establishment_number. The only Python is a pure, pytest-covered Signal-1 regex extractor mirrored 1:1 as a SQL `regexp_match` — no rapidfuzz (USDA is exact-ID selection, not edit distance).

**Corpus gates (run FIRST).**
- **G2** — re-derive the duplicate-name category split on the post-2026-05-31-reseed 7979-establishment corpus, GROUPED BY `(name, city, state)`, with `groups_with_inactive_member` per category. The 276/103/77 groups (60/23/17%) and Lineage-59 figures were measured on the 7970 snapshot and NOT re-derived; never grouped by city. Status: needs-corpus-requery.
  - Script: extend `scripts/sql/usda_establishments/bronze/inspect_duplicate_names.sql` (EXISTS) Q3 with `count(distinct city)` and a `(name,city,state)` group key.
  - Expected signal: CONFIRM `establishment_group_id = (name,city,state)` if multi_grant groups have `distinct_cities=1`; else drop the key to `(name,state)`. CONFIRM split stable if 7979 ratios match 7970 within rounding.
- **G3** — Signal-1 coverage: of recalls whose `field_establishment` fans out to >1 candidate, how many have `field_product_items` matching the establishment-number regex that resolves (after `+` split) to exactly one candidate. Status: needs-corpus-requery (never measured; field is 40.5% empty, confirmed-in-doc).
  - Script: `scripts/sql/usda_recalls/bronze/probe_product_items_embedded_estab_number.sql` (NEW).
  - Expected signal: build Signal-1 first + set the `usda_ambiguous_null < 10%` test to error IF a high fraction resolves; else accept ambiguous_null for the mixed bucket in v1 + keep that test at `warn`.
- **G4** — `field_states` non-state pollution: explode comma-separated states, classify each token as US state/territory vs non-state region (Nationwide/Midwest/…), report frequency. Status: needs-corpus-requery (never measured).
  - Script: `scripts/sql/usda_recalls/bronze/probe_field_states_tokenization.sql` (NEW).
  - Expected signal: build the Signal-2 blocklist + confirm viability if non-state tokens are a small minority; de-prioritize Signal-2 if pollution is large (state set is 28.4% NULL already, confirmed-in-doc).
- **G5** — cold-storage operators: profile establishments whose `activities` indicate storage/freezing-only, cross-reference Lineage/Americold. Status: needs-corpus-requery.
  - Script: `scripts/sql/usda_establishments/bronze/probe_cold_storage_activities.sql` (NEW).
  - Expected signal: CONFIRM a deterministic activities-based `cold_storage_flag` if Lineage/Americold reliably carry storage-only activities; else cold-storage recalls fall to `usda_ambiguous_null` only via the all-candidates-ambiguous path.

**Files — new.**
- `dbt/models/silver/recall_event_establishment_resolution.sql` — CTE pipeline: `recalls` → `candidates` (join `firm_establishment_attributes` on `upper(trim(name))`, carry `establishment_group_id`, `cold_storage_flag`) → `counted` (`n_facility_groups` window) → `sig1..sig5` (each emits only on collapse-to-one) → `cold_storage_recalls` (all-candidates cold-storage → downgrade) → `resolved` (precedence: `unambiguous > multi_signal > product_items_extract > state_match > processing_match > mpi_proximity`) → final select with `coalesce(..., 'usda_ambiguous_null')`.
- `src/enrichment/usda_estab_number.py` — pure `extract_establishment_numbers(product_items) -> list[str]` (the Signal-1 regex `establishment\s+number\s+([MPIGV]-?\d+[A-Z]?(?:\s*\+\s*[MPIGV]-?\d+[A-Z]?)*)`, `+`-split, upper-normalize). No I/O.
- `tests/scripts/test_usda_estab_number.py` — `parents[2]` sys.path shim; cases: happy (`P-33901`), composite split (`P-44 + M-44`), case-insensitive (`p-9999z`), empty/None → `[]`, no-trigger-phrase → `[]`, false-friend (`establishment number TBD`) → `[]`, regression (phrase twice → first-match documented).
- `dbt/tests/assert_usda_ambiguous_null_under_threshold.sql` — singular: of USDA bridge rows fanning out to >1 facility-group, fraction `usda_ambiguous_null` must be `< 0.10` (severity per G3).
- `dbt/tests/assert_resolved_establishment_in_candidates.sql` — singular: every non-null `resolved_establishment_number` is a member of that recall's name-matched candidate set.
- `scripts/sql/usda_recalls/bronze/probe_product_items_embedded_estab_number.sql`, `scripts/sql/usda_recalls/bronze/probe_field_states_tokenization.sql`, `scripts/sql/usda_establishments/bronze/probe_cold_storage_activities.sql`.

**Files — modified.**
- `dbt/models/silver/recall_event_firm.sql` — `usda_event_firms` LEFT JOINs `recall_event_establishment_resolution` on `recall_event_id`, selecting `resolved_establishment_number as establishment_number, match_confidence`; all OTHER branches `cast(null as text) as establishment_number, cast(null as text) as match_confidence` (or keep `'exact_name'`). **`firm_id` recipe + `firm.sql usda_normalized` unchanged → lockstep intact.**
- `dbt/models/silver/firm_establishment_attributes.sql` — add `establishment_group_id = md5(upper(trim(establishment_name))||'|'||coalesce(upper(trim(city)),'')||'|'||coalesce(upper(trim(state)),''))` (key per G2) and `cold_storage_flag` (per G5).
- `dbt/models/silver/_silver.yml` — add `recall_event_firm.establishment_number` (no test, nullable); `recall_event_establishment_resolution.recall_event_id` not_null+unique, `match_confidence` not_null+accepted_values (USDA subset of the shared enum).
- `scripts/sql/usda_establishments/bronze/inspect_duplicate_names.sql` — extended for G2.

**No migration** — all new columns are dbt-computed silver columns.

**Verification.**
- User runs the pure-logic tests: `pytest tests/scripts/test_usda_estab_number.py`; plus `ruff check src/enrichment/usda_estab_number.py`, `ruff format --check`, `pyright`.
- User runs: `dbt build --select recall_event_establishment_resolution firm_establishment_attributes recall_event_firm`.
- User runs the specific tests: `dbt test --select recall_event_establishment_resolution assert_usda_ambiguous_null_under_threshold assert_resolved_establishment_in_candidates`.
- Spot-check: a Tyson/Lineage recall in `recall_event_establishment_resolution` to confirm a multi-grant collapse and a cold-storage downgrade behave as expected (query under `scripts/sql/usda_recalls/silver/`).

**Fits current design.** `firm.sql usda_normalized` and the `md5(upper(trim(name)))` grain are UNCHANGED; the resolution is bridge-level enrichment, so lockstep holds and no re-key occurs. Same-facility collapse is a column on the EXISTING one-row-per-establishment sidecar, not a redundant sibling. Cold-storage producer-extraction is correctly deferred (Phase 6/7); v1 flags + downgrades. Signal-1 Python mirrors the SQL `regexp_match` and is pytest-covered per the scripts bar.

**Risks.** The `<10%` target depends on G3 (unmeasured, 40.5%-empty field) — hence warn-then-promote. `(name,city,state)` collapse assumes co-location (G2 confirms). SQL/Python regex must stay byte-equivalent (the pytest is the contract; a comment in both cross-references). `cold_storage_flag` is heuristic — precision-safe (no wrong attribution) but may lose a resolvable recall.

---

## PR 6b.3 — FDA FEI deterministic edges + NHTSA name normalization + AC DELCO product fix

**Scope.** Three deterministic pieces, no rapidfuzz yet (clustering is 6b.4): (1) a thin `firm_fei_edges.sql` surfacing the FDA shared-FEI + `firm_surviving_fei` succession edges so the 12.4% of FEIs mapping to >1 name (confirmed-in-doc) collapse WITHOUT fuzzy and BEFORE 6b.4 runs; (2) route NHTSA `mfgname`/`mfgtxt` through the shared `clean_firm_name` macro (corporate-form/regional suffix strip) in both firm models (lockstep), respecting the filer/manufacturer role split; (3) the AC DELCO product-level fix in `recall_product.sql`.

**AC DELCO correction (load-bearing).** AC DELCO/ACDELCO is a `maketxt` drift (0 firm occurrences, confirmed-in-doc), NOT firm work. The concrete fix is a 1-expression internal-whitespace collapse on `maketxt` INSIDE the NHTSA 11-tuple `recall_product_id` md5 recipe at `recall_product.sql:128-135` (maketxt is line 130), preserving the displayed value: `... || regexp_replace(upper(trim(maketxt)),'\s+','','g') || ...` so `AC DELCO` and `ACDELCO` hash to one product row. Putting AC DELCO in the firm crosswalk would yield ZERO observable change.

**Corpus gates (run FIRST).**
- **G6** (artifact-capture only — already confirmed-in-doc) — re-run the FDA FEI profile and capture Q1-Q4 (14,285 names / 0.4% no-FEI / 15.3% multi-address / 12.4% multi-name).
  - Script: `scripts/sql/fda/bronze/profile_firm_fei_for_sidecar.sql` (EXISTS — re-run, capture output to `documentation/fda/`). Expected: reproduces; confirms FEI viable as the deterministic pre-merge block.
- **G7** (artifact-capture only — already confirmed-in-doc) — re-run NHTSA mfgname-vs-mfgtxt, capture the full-corpus 3,940-firm figure.
  - Script: `scripts/sql/nhtsa/bronze/inspect_mfgname_vs_mfgtxt.sql` (EXISTS — re-run, capture to `documentation/nhtsa/`). Expected: Q7 ≈ 3,940 combined, Q3 ≈ 95.9% disjoint.
- **G8** — maketxt space-collapse safety: does `regexp_replace(maketxt,'\s+','')` over-merge two genuinely distinct makes? Status: needs-corpus-requery.
  - Script: `scripts/sql/nhtsa/bronze/probe_maketxt_space_collapse_safety.sql` (NEW).
  - Expected signal: CONFIRM the product-level fix if zero distinct-make false-merges; else use a targeted alias map instead.

**Files — new.**
- `dbt/models/silver/firm_fei_edges.sql` — distinct `(firm_id=md5(upper(trim(firm_legal_nam))), normalized_name, firm_fei_num, firm_surviving_fei)` from `stg_fda_recalls where firm_fei_num is not null`. Consumed by the 6b.4 clusterer as forced-merge constraints.
- `dbt/tests/assert_no_ac_delco_firm_drift.sql` — guard that maketxt-class space-drift does not reappear as distinct firm rows (regression backstop keeping the fix product-level).
- `scripts/sql/nhtsa/bronze/probe_maketxt_space_collapse_safety.sql`.

**Files — modified.**
- `dbt/models/silver/recall_product.sql` — NHTSA `recall_product_id` md5 recipe (lines 128-135): wrap `maketxt` with `regexp_replace(.,'\s+','','g')` INSIDE the hash only. No firm involvement.
- `dbt/models/silver/firm.sql` — `nhtsa_normalized` (both `mfgname` filer and `mfgtxt` manufacturer CTEs): `normalized_name = upper(trim({{ clean_firm_name(col) }}))`. Macro is role-agnostic; it clusters on NAME only, roles stay on the bridge.
- `dbt/models/silver/recall_event_firm.sql` — `nhtsa_event_firms` (both CTEs): `firm_id = md5(upper(trim({{ clean_firm_name(col) }})))` (SAME macro → lockstep).
- `dbt/models/silver/_silver.yml` — `firm_fei_edges` column tests (firm_id not_null; firm_fei_num not_null).
- ADR 0031 — amendment row documenting the maketxt space-collapse + G8 result.

**No migration** — all changes are dbt model edits.

**Verification.**
- User runs: `dbt build --select firm_fei_edges firm recall_event_firm recall_product`.
- User runs: `dbt test --select assert_no_ac_delco_firm_drift assert_nhtsa_filer_and_manufacturer_roles recall_event_firm` (the filer/manufacturer split guard must still pass; lockstep relationships pass).
- Spot-check: query `recall_product` for campno 22E002000 — both `AC DELCO` and `ACDELCO` source rows now share one `recall_product_id` (script under `scripts/sql/nhtsa/silver/`).

**Fits current design.** FEI stays an attribute/edge, not the firm PK (preserves the 5-model family + ADR 0002). NHTSA cleanup reuses 6b.1's macro (lockstep, single-homed). AC DELCO is correctly routed to `recall_product.sql` per ADR 0031, not the firm dim. Honors the NHTSA two-branch split.

**Risks.** Over-strip on corporate-form tokens (the suffix list is conservative; the `assert_no_cpsc_name_suffix` guard catches under-stripping, not over-stripping — eyeball G1/G7 output). FEI succession cycles/dangling survivor edges (the 6b.4 clusterer must guard against cycles). Reviewers must understand AC DELCO is intentionally NOT in the firm crosswalk.

---

## PR 6b.4 — RapidFuzz cross-source crosswalk (`recalls resolve-firms` + firm_id remap)

**Scope.** The genuine edit-distance layer. A `recalls resolve-firms` Typer subcommand (USER-run) reads distinct cleaned firm names + the FDA FEI edges, applies RapidFuzz `token_set_ratio` with a blocking key + the FEI/succession edges as forced merges (union-find), writes `firm_crosswalk` (truncate-and-reload), and the additive `canonical_firm_id` coalesce in firm.sql/recall_event_firm.sql (already wired in 6b.0) now resolves to real cluster ids. `match_confidence` carries `fei_exact` (forced), `rapidfuzz_high` (≥ threshold), `rapidfuzz_low_ambiguous_null` (below threshold, left unmerged), `singleton`.

**Corpus gate (run FIRST).** Re-confirm G0 scale (from 6b.0) now that deterministic strip has landed; plus a residual-after-strip count.
- Script: `scripts/sql/cross_source/silver/residual_after_deterministic_strip.sql` (NEW). How many firm_ids deterministic strip + FEI edges already collapse vs how many additional merges RapidFuzz produces at the candidate threshold.
- Expected signal: PROCEED with the clusterer if fuzzy merges are a small, clean minority above threshold; if large/noisy, ship deterministic-only for v1 and defer the clusterer. Sets the `rapidfuzz_high` threshold from observed merge counts (store the chosen value in `resolver_version`).

**Files — new.**
- `src/enrichment/firm_resolution.py` — PURE logic (no DB/IO): `block_key(name)` (first-token bucket), `score_pair(a,b)` (token_set_ratio), `cluster_names(names, fei_edges, threshold) -> list[FirmCluster]` (union-find injecting FEI edges as forced merges, guarding cycles), `pick_canonical(cluster)`. Frozen dataclasses. Mirrors the extractor `_parse_*` separation.
- `src/enrichment/crosswalk_writer.py` — I/O boundary: read distinct names + FEI edges via `make_engine(settings.neon_database_url.get_secret_value())` (precedent: `recover_rejected` at `src/cli/main.py:505`), run `cluster_names`, truncate-and-reload `firm_crosswalk` in a transaction, return a fetched/clustered/ambiguous summary.
- `tests/enrichment/__init__.py`, `tests/enrichment/test_firm_resolution.py`, `tests/enrichment/test_crosswalk_writer.py` — happy path, empty input, a synthetic `AC DELCO`/`ACDELCO` clustering unit (note: a synthetic unit only — it must NOT be expected to change any real firm row), a FEI forced-merge case, a below-threshold non-merge case. (These live in `tests/enrichment/` — normal package import, same ruff/pyright/pytest bar.)
- `tests/cli/test_resolve_firms_command.py`.
- `scripts/sql/cross_source/silver/residual_after_deterministic_strip.sql`.

**Files — modified.**
- `src/cli/main.py` — add `@app.command(name="resolve-firms")` wiring `crosswalk_writer` + the `_print_run_summary` idiom (main.py:162-170). USER runs it.
- `dbt/models/silver/firm.sql` — final select now groups on `coalesce(x.canonical_firm_id, md5(normalized_name))`; `canonical_name` prefers `x.canonical_name`. (The LEFT JOIN was added in 6b.0; this PR makes the grouping canonical-aware.) **Mirror EXACTLY in `recall_event_firm.sql`.**
- `dbt/models/silver/recall_event_firm.sql` — `match_confidence` for fuzzy/singleton rows carried from `firm_crosswalk` via the LEFT JOIN.
- `dbt/models/silver/_silver.yml` — `firm.normalized_name` unique test REMOVED (one canonical firm now spans multiple normalized names); `firm.firm_id` unique stays (now canonical-grain). `firm_crosswalk` relationships `canonical_firm_id → firm.firm_id`.

**No migration** — `firm_crosswalk` was created in 6b.0; this PR only populates it.

**Verification.**
- User runs the pure-logic tests: `pytest tests/enrichment/ tests/cli/test_resolve_firms_command.py`; `ruff check`/`ruff format --check`/`pyright` on `src/enrichment/`.
- User runs the resolver: `recalls resolve-firms` (prints fetched/clustered/ambiguous; writes `firm_crosswalk`).
- User runs: `dbt build --select firm recall_event_firm` (now remaps to canonical ids).
- User runs: `dbt test --select recall_event_firm` → relationships still pass (proves the canonical coalesce is identical on both sides).
- Spot-check: `scripts/sql/cross_source/silver/residual_after_deterministic_strip.sql` + a manual look at a Toyota cluster.

**Fits current design.** The crosswalk-as-source mirrors every bronze table; the CLI mirrors `recover-rejected`; the coalesce is additive and applied identically in both firm models (lockstep). RapidFuzz is confined to the residual the deterministic layer + FEI edges leave. No CREATE EXTENSION, no dbt-python runtime.

**Risks.** Re-key blast radius (the canonical grouping changes firm_id for the fuzzy-clustered minority — G0/residual gate sizes it; the relationships test polices both sides). O(n²) feasibility under the USER-run CLI (the blocking key ships from day one; the gate decides bucket granularity). Crosswalk staleness between runs (new firms fall back to their own md5 via coalesce — correct; document re-running after large seeds).

---

## PR 6b.5 — USCG SCD-2 snapshot + time-sensitive recall join (BLOCKED on ADR 0035)

**Scope.** Convert `firm_manufacturer_attributes` from a Type-1 listing-only sidecar to the current-view over a new dbt SCD-2 snapshot built from the (currently unconsumed) detail staging view; add the time-sensitive attribution flag on the bridge. This is BOTH a source-switch (listing → detail) AND new snapshot infra. v1 attributes to the current MIC holder but FLAGS the ambiguity; the as-of-BUILD-date (HIN) join is deferred (only ~13/205 OOB years parseable, confirmed-in-doc).

**Confirmed in-scope for 6b (2026-06-03).** The USCG SCD-2 silver model is correctly placed here, not deferred to another stage: `silver_v15_migration_plan.md:216,220` assigns it to "Phase 6 work … ADR 0035," **"deliberately kept off the detail-capture branch (would otherwise collide with Phase 6b on `firm.sql`)"**; and `scd_field_designations.md:33` records the USCG firm anchor as the corpus's **only Type-2 NEED**, MEASURED + monitor-confirmed (365 prior / 205 OOB-recycled of 718 recalled MICs). The bronze prerequisite (`feature/uscg-manufacturers-detail-addition`) has already landed (migrations 0017/0018, `stg_uscg_manufacturer_details`). Only the *cross-source* SCD-2 policy (CPSC/FDA/USDA) stays out of 6b (post-Layer-3).

**Prerequisite (not code): settle ADR 0035.** It is `Proposed (stub) / Deferred`; phase-5d §11 says it must be Accepted before the snapshot is built. Recommendation: in THIS PR, flip ADR 0035 to Accepted SCOPED to the USCG instance only — storage option (a) dbt snapshot `strategy='check'` on the stable `mic` anchor + Policy C (latest-wins current view + first-class peer history = the snapshot table). Leave FDA/CPSC/USDA SCD-2 deferred (0 edit-versions). Also: fill ADR 0031's TBD USCG row, and exempt `silver_snapshots` from ADR 0007 pruning. Confirm `silver_snapshots` schema is provisioned + dbt has create-schema rights on the Neon dev branch.

**Corpus gate (run FIRST).** G9 (re-confirm / artifact — already confirmed-in-doc) — re-run the MIC reassignment monitor and enumerate the recalled+recycled MICs (current vs OOB-marked prior holders).
- Script: `scripts/sql/cross_source/scd_monitors/assert_mic_holder_stable.sql` (EXISTS — re-run Q2-Q4, capture to `documentation/uscg/`). Expected: 205 OOB-recycled / 365 prior / 718 recalled (reproduced exactly 2026-06-02). The Q4 sample list IS the set the flag stamps.

**Files — new.**
- `dbt/snapshots/uscg_manufacturer_attributes_snapshot.sql` — `strategy='check'`, `unique_key='mic'` ALONE (a reassignment is a new version of the same anchor; `mic+company` would fork lineage), `check_cols=[company_name,address,city,state,zip,country,status,parent_company,parent_mic,past_company_1,past_company_2,past_company_3,out_of_business]`, `target_schema='silver_snapshots'`, driven from `stg_uscg_manufacturer_details` with `upper(trim(mic))` baked in and `where company_name is not null` (drops sentinel/null-name anchors).
- `dbt/snapshots/_snapshots.yml` — `unique_combination_of_columns: [mic, dbt_valid_from]` on the snapshot (replaces single-mic uniqueness under SCD-2).
- `dbt/tests/assert_uscg_mic_reassignment_flag_present.sql` — singular: every USCG bridge row whose MIC is in the recycle set carries `uscg_mic_time_sensitive_unresolved`.
- `dbt/tests/assert_uscg_scd2_no_forked_lineage.sql` — singular: no MIC has >1 `dbt_valid_to IS NULL` current row.

**Files — modified.**
- `dbt/models/silver/firm_manufacturer_attributes.sql` — repoint from the listing view to `ref('uscg_manufacturer_attributes_snapshot') where dbt_valid_to is null`; add detail fields (parent_*, past_company_*, status, dba, out_of_business, full untruncated address — fixes Finding F.1) + derived `mic_has_prior_holder` / `mic_oob_recycled` booleans + `prior_holders` jsonb. Grain stays one current row per mic.
- `dbt/models/silver/recall_event_firm.sql` — `uscg_event_firms` LEFT JOINs `firm_manufacturer_attributes` on `upper(trim(mic))` and stamps `match_confidence` (`uscg_mic_time_sensitive_unresolved` when `mic_oob_recycled` or `mic_has_prior_holder`, else `uscg_mic_unambiguous`). **`firm_id` recipe UNCHANGED → lockstep preserved (additive LEFT JOIN only).**
- `dbt/models/silver/_silver.yml` — keep `firm_manufacturer_attributes.mic` not_null+unique (current view); add the new boolean columns; `recall_event_firm.match_confidence` accepted_values already includes the USCG values from 6b.0.
- ADRs: `0035` (Accept, USCG-scoped), `0031` (fill USCG row), `0007` (silver_snapshots pruning exemption).

**No alembic migration** — dbt manages the snapshot DDL; `match_confidence` + the new dim columns are dbt-computed.

**Verification.**
- User (one-time): confirm/provision `silver_snapshots`; `dbt snapshot --select uscg_manufacturer_attributes_snapshot` (first run banks the current state).
- User runs: `dbt build --select firm_manufacturer_attributes recall_event_firm`.
- User runs: `dbt test --select assert_uscg_mic_reassignment_flag_present assert_uscg_scd2_no_forked_lineage recall_event_firm`.
- User re-runs `dbt snapshot` on an unchanged corpus to confirm 0 spurious new versions (the `strategy='check'` idempotency check).
- Spot-check: a recycled MIC (from G9's Q4 list) shows `uscg_mic_time_sensitive_unresolved` on its bridge row.

**Fits current design.** `firm.sql uscg_normalized` and the `md5(...)` firm grain are UNTOUCHED; the flag is an additive LEFT JOIN on the same mic key the bridge already joins (lockstep holds — the 1549-orphan mode is a divergent firm_id recipe, which this doesn't alter). The snapshot is the first consumer of the already-built-but-unused detail staging view. Mirrors the FDA sidecar's current-view half; the history half is the snapshot (do NOT copy the FDA Type-1 sidecar for history).

**Risks.** Snapshot non-idempotency on re-seed (mitigated by `strategy='check'` + latest-per-MIC staging; verify on the first two runs). ADR 0035 scope creep (accept ONLY the USCG slice). `silver_snapshots` not exempt from ADR 0007 pruning → silent history loss (the ADR 0007 edit is a required deliverable). As-of-recall-date is itself imperfect (a recall for a pre-reassignment hull still attributes to the current holder) — v1 FLAGS rather than silently misattributes; the HIN as-of-build-date join is the deferred correct fix.

---

## PR 6b.6 — Cross-source rollup dbt tests (Honda, Tyson) + gate escalation

**Scope.** The acceptance gate: two new singular tests asserting Honda and Tyson each resolve to exactly ONE canonical firm across all sources; escalate the quality gates whose corpus signals are now known (USDA ambiguous_null, fuzzy thresholds).

**Files — new.**
- `dbt/tests/assert_honda_rolls_up_to_one_firm.sql` — `count(distinct canonical_firm_id)` for firms whose `observed_names ILIKE '%HONDA%'` must be 1. Keys on `firm.canonical_firm_id` / `observed_names`, **NOT bridge role** (FDA is role `'establishment'`, so a `role='manufacturer'` filter would MISS FDA Honda rows — Lane D). Honda appears across NHTSA filer+manufacturer + FDA establishment.
- `dbt/tests/assert_tyson_rolls_up_to_one_firm.sql` — same shape for Tyson (USDA establishment post-disambiguation + FDA establishment). Must assert on `canonical_firm_id` AFTER disambiguation, not on raw establishment_number.

**Files — modified.**
- `dbt/models/silver/_silver.yml` — escalate `recall_event_firm.match_confidence` `accepted_values` from `warn` to `error` once all source value sets have landed; escalate `assert_usda_ambiguous_null_under_threshold` per G3.

**Verification.**
- User runs: `dbt build` (full).
- User runs: `dbt test --select assert_honda_rolls_up_to_one_firm assert_tyson_rolls_up_to_one_firm` → 0 rows each (one canonical firm).
- If a test fails with >1 canonical firm, inspect the `%HONDA%`/`%TYSON%` `observed_names` for an over-broad ILIKE (e.g. an unrelated `HONDAI`) before tuning the RapidFuzz threshold; re-run `recalls resolve-firms` if a real cluster was missed.
- Bump `version` to `0.15.0` (the `resolve-firms` CLI surface is user-visible) — USER does the edit.

**Fits current design.** Uses the established `dbt/tests/` singular pattern (`assert_nhtsa_filer_and_manufacturer_roles.sql` template). Keys on the conformed `canonical_firm_id` regardless of role, correctly handling #58's FDA role relabel.

**Risks.** Over-broad ILIKE causing false failures (review `observed_names` before escalating from `warn` to `error`). The tests can only pass once 6b.1+6b.3+6b.4 have unified the names — they are the acceptance gate, so they start at `warn` until the unifying layer lands.

---

## Appendix A — Corpus validation gates (every script the USER runs)

Run order: per-PR, BEFORE the build. `psql "$NEON_DATABASE_URL" -f <path>`. Tag = confirmed-in-doc / needs-requery / artifact-capture.

| Gate | Script (NEW unless noted) | PR | Tag | Gating decision |
|---|---|---|---|---|
| G0 | `scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql` | 6b.0, 6b.4 | needs-requery | brute-force vs blocking; re-key magnitude |
| G1 | `scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql` | 6b.1 | needs-requery | comma-optional vs comma-required `of` |
| G1b | `scripts/sql/cpsc/bronze/inspect_firm_name_fragmentation.sql` (EXTEND) | 6b.1 | needs-requery | keep/drop `d/b/a` branch |
| G1c | `scripts/sql/cpsc/bronze/inspect_firm_name_fragmentation.sql` (EXISTS) | 6b.1 | confirm-in-doc | bronze single-shot 9,828; add dedup CTE if not |
| G2 | `scripts/sql/usda_establishments/bronze/inspect_duplicate_names.sql` (EXTEND, +city) | 6b.2 | needs-requery | `establishment_group_id` key = (name,city,state) vs (name,state) |
| G3 | `scripts/sql/usda_recalls/bronze/probe_product_items_embedded_estab_number.sql` | 6b.2 | needs-requery | Signal-1 first? ambiguous_null test severity |
| G4 | `scripts/sql/usda_recalls/bronze/probe_field_states_tokenization.sql` | 6b.2 | needs-requery | Signal-2 viability + blocklist |
| G5 | `scripts/sql/usda_establishments/bronze/probe_cold_storage_activities.sql` | 6b.2 | needs-requery | deterministic cold_storage_flag? |
| G6 | `scripts/sql/fda/bronze/profile_firm_fei_for_sidecar.sql` (EXISTS, re-run) | 6b.3 | artifact-capture | FEI viable as pre-merge block (already confirmed) |
| G7 | `scripts/sql/nhtsa/bronze/inspect_mfgname_vs_mfgtxt.sql` (EXISTS, re-run) | 6b.3 | artifact-capture | RapidFuzz blast radius (3,940 firms, already confirmed) |
| G8 | `scripts/sql/nhtsa/bronze/probe_maketxt_space_collapse_safety.sql` | 6b.3 | needs-requery | maketxt space-collapse safe? |
| G9 | `scripts/sql/cross_source/scd_monitors/assert_mic_holder_stable.sql` (EXISTS, re-run) | 6b.5 | artifact-capture | recycle set (205/365/718, already confirmed) |

Do NOT block the build on G6/G7/G9 — they are already confirmed-full-corpus; capture the artifact only. Block on G0/G1/G1b/G2/G3/G4/G5/G8.

## Appendix B — Schema changes (every new column / table / migration / extension)

**Alembic migrations:** exactly ONE — `0024_firm_crosswalk.py` (`down_revision='0023'`), creating the `firm_crosswalk` table + `ix_firm_crosswalk_canonical` index. No other migrations: all other new columns are dbt-computed silver columns.

**Extensions:** NONE. No `pg_trgm`/`fuzzystrmatch`/`CREATE EXTENSION` (confirmed absent; RapidFuzz runs in local Python).

**New tables / dbt objects:**
- `firm_crosswalk` (Postgres table, Python-written, dbt source) — PR 6b.0.
- `silver_snapshots.uscg_manufacturer_attributes_snapshot` (dbt snapshot; dbt-managed DDL incl. `dbt_valid_from/dbt_valid_to/dbt_scd_id/dbt_updated_at`) — PR 6b.5.
- `recall_event_establishment_resolution` (silver model) — PR 6b.2.
- `firm_fei_edges` (silver model) — PR 6b.3.

**New silver columns (dbt-computed, no migration):**
- `firm.alternate_names` (jsonb), `firm.canonical_firm_id` (text) — 6b.0/6b.1/6b.4.
- `recall_event_firm.match_confidence` (text, shared namespaced enum), `recall_event_firm.canonical_firm_id` (text) — 6b.0; `recall_event_firm.establishment_number` (text) — 6b.2.
- `firm_establishment_attributes.establishment_group_id` (text), `firm_establishment_attributes.cold_storage_flag` (bool) — 6b.2.
- `firm_manufacturer_attributes.mic_has_prior_holder` (bool), `mic_oob_recycled` (bool), `prior_holders` (jsonb), + repointed detail fields — 6b.5.

**Dependency / version:** `rapidfuzz>=3.10,<4` added to `[project.dependencies]` (6b.0). Version: `0.13.0` → `0.14.0` (6b.0, substrate+CPSC) → `0.15.0` (6b.4/6b.6, the `resolve-firms` CLI surface). USER does all version edits.

**ADR changes:** 0035 (Accept, USCG-scoped, 6b.5), 0031 (AC DELCO maketxt amendment 6b.3 + USCG row 6b.5), 0007 (silver_snapshots pruning exemption 6b.5).
---

## Appendix C — Adversarial review corrections (BINDING — fold into the named PR before building)

A completeness/adversarial critic reviewed this plan against the actual files. No scope item is missing, but the items below are **binding corrections**: apply each in its named PR. Severity 1–2 are build-breakers if implemented as the body text literally reads; the body above is the design intent, these are the precise mechanics.

**S1 — canonical_firm_id / lockstep (the most under-specified area; get this right first).**

- **C1 (6b.0).** The `canonical_firm_id` LEFT JOIN must NOT be bolted onto `firm.sql`'s grouped select — `firm.sql` does `select md5(normalized_name) as firm_id ... group by normalized_name`, so a bare `x.canonical_firm_id` there is a GROUP BY error. Wrap the existing grouped query in an outer CTE and join the crosswalk there: `final as (<existing group-by query>) select f.*, coalesce(x.canonical_firm_id, f.firm_id) as canonical_firm_id from final f left join {{ source('enrichment','firm_crosswalk') }} x on x.firm_id = f.firm_id`. In 6b.0 the crosswalk is empty, so this resolves to `f.firm_id` (a clean no-op).
- **C2 (6b.4).** Regrouping `firm.sql` on `canonical_firm_id` re-grains the dim: one firm row then spans multiple `normalized_name`s, so `normalized_name` can no longer be a bare column — it must become an aggregate (e.g. `jsonb_agg(distinct normalized_name) as observed_normalized_names`, with a representative `normalized_name` chosen deterministically, e.g. `min(normalized_name)`). 6b.4 MUST restate the full final-select shape (row key = `canonical_firm_id`; what is aggregated; what `normalized_name`/`canonical_name`/`observed_company_ids` become) and prove the sidecar `observed_company_ids = jsonb_agg(distinct company_id)` contract still holds across merged members on a worked **Honda** example before merging.
- **C3 (6b.4, lockstep).** `recall_event_firm.sql` today is a bare `union all` of 5 CTEs with NO outer select and NO grouping; the relationships test polices `recall_event_firm.firm_id → firm.firm_id`. If 6b.4 makes `firm.firm_id` the canonical id, the bridge MUST emit canonical as its `firm_id` too (a genuine re-key), wired via a single outer SELECT over the union that LEFT JOINs `firm_crosswalk` once — NOT five inline per-branch joins. Decide and document explicitly: the FK stays on `firm_id` and the bridge remaps `firm_id → canonical`. The body's "relationships test still passes / sidecar joins survive unchanged" is only true once this re-key is spelled out — do not treat it as automatic.

**S2 — migration ordering & house conventions.**

- **C4 (6b.0).** Cite **0023** (`0023_seed_fda_press_releases_watermark`) as the immediate-precedent template for `0024`, and verify 0023's revision-id string format (`'0023'` vs a hash) so `down_revision` chains correctly.
- **C5 (move to 6b.1, NOT 6b.4).** Dropping `firm.normalized_name`'s `unique` test must happen in **6b.1**: CPSC suffix-strip merges *within* CPSC (`ZOLIQUEX, of China` + `ZOLIQUEX` → both `ZOLIQUEX`), so `normalized_name` becomes non-unique as early as 6b.1. The body's "normalized_name unique stays for now (CPSC-only strip won't merge across sources)" is WRONG — it merges within-source. Keep the existing `firm_id` unique test; drop/convert the `normalized_name` unique test in 6b.1.
- **C6 (6b.1).** Native dbt `unit_tests` require dbt-core ≥1.8. Add a precondition to 6b.1 to confirm the pinned dbt-core version; if <1.8, ship only the singular `assert_clean_firm_name_examples.sql` fixture test and skip `_unit_tests.yml`.

**S3 — coverage gaps vs scope.**

- **C7 (6b.2).** The `cold_storage_flag` derives from `activities`, so 6b.2 must ALSO project `activities` into `firm_establishment_attributes` (it is currently the flag's INPUT and may not be surfaced). Add `activities` to the file-modified list.
- **C8 (6b.1 or 6b.3 — decide ownership).** NHTSA needs corporate-form / parent-parenthetical cleanup (`INC/LLC/CORP`, `Chrysler (FCA US, LLC) (Stellantis)`), which is DIFFERENT regex from the CPSC `, of <geo>`/`dba` suffix strip. Either extend `clean_firm_name` in 6b.1 to include corporate-form branches (and add NHTSA verbatim unit cases there), or have 6b.3 extend the macro. "Reuse 6b.1's macro as-is" under-delivers the NHTSA cleanup scope item — pick the owning PR and add the NHTSA test cases to it.
- **C9 (6b.0).** The `enrichment` dbt source must declare the `schema:` (and `database:` if needed) matching where migration 0024 creates `firm_crosswalk` (public/bronze schema, same as every other source table), or `source('enrichment','firm_crosswalk')` won't resolve.

**S4 — evidence discipline / precision-over-recall.**

- **C10 (6b.6).** Make the `assert_usda_ambiguous_null_under_threshold` escalation **conditional**: "IF G3 shows Signal-1 resolves the mixed bucket, promote `<10%` to error; ELSE keep `warn`." G3 is unmeasured — an unconditional escalation risks a perpetually-red test.
- **C11 (6b.2).** The `unambiguous` signal must not silently `min(establishment_number)` when a single facility-GROUP still holds multiple distinct `establishment_number`s (the M/P/V multi-grant case). Either return a number only when the group resolves to exactly one establishment_number, or define a multi-grant group's representative number deliberately and stamp a DISTINCT confidence (not `unambiguous`). An undocumented `min()` tiebreak violates precision-over-recall.
- **C16 (6b.2).** `src/enrichment/usda_estab_number.py` lives in `src/`, so its test belongs in `tests/enrichment/` with a normal package import — NOT `tests/scripts/` with the `parents[2]` shim. The shim is only for modules under `scripts/`. Align with 6b.4's `tests/enrichment/` placement.

**S5 — smaller corrections.**

- **C12 (6b.0/6b.4).** The `match_confidence` enum hard-codes a two-tier `rapidfuzz_high`/`rapidfuzz_low_ambiguous_null`. Reserve room for a `rapidfuzz_medium` tier, or explicitly accept an enum edit in 6b.4 if the residual gate shows a third tier is warranted.
- **C13.** `firm.sql` is **181** lines (not 182). Locate everything by CTE name, never line number.
- **C14 (6b.3).** The displayed `maketxt` is ALSO emitted at `recall_product.sql:153` (`'maketxt', maketxt` in the JSONB attributes blob). The space-collapse fix touches ONLY the md5 input at line 130 — flag 153 so a reviewer does not "consistency-fix" it.
- **C15 (6b.5).** `firm_manufacturer_attributes` currently reads `source_recall_id as mic` from the LISTING view (`stg_uscg_manufacturers`, ~lines 20-27). The detail-snapshot source-switch must map the detail view's real `mic` column cleanly — note the aliasing quirk so the rename is not silent.

