# Gold → recalls-api audit workstream

- **Status:** Active — drafted 2026-06-14 on `feature/gold-api-refinement`. An empirical re-audit of the
  **gold serving + aggregate layer** against the `recalls-api` read surface, run *after*
  `serving-layer-gold-readiness-plan.md` (its work-item IDs **R1–R7** — unrelated to Cloudflare **R2**,
  the landing bucket) was executed. **All three inputs are folded in:** static analysis, the live catalog
  inventory (`audit_schema_and_indexes.sql`), and the per-source coverage run (`audit_coverage.sql`) — both
  run 2026-06-14; a second schema-inventory run confirms G0 unchanged.
- **Type:** phase/feature plan (documentation_model.md type 4) — the bounded set of **gold/pipeline
  changes** that make the open, read-only `recalls-api` serve the most sensible data. Every item is a
  pipeline-repo change (a dbt model / `config(indexes)` / `post_hook` / `_gold.yml` / doc edit).
- **Audience:** a Claude Code terminal executing in **this** pipeline repo, + the API maintainer reading
  the SAFE-NOW / BLOCKED split.
- **Relationship to siblings.** `serving-layer-gold-readiness-plan.md` is the *first* gold-hardening pass
  (R1–R7); **this** doc re-audits the result, **confirms what landed, repudiates what didn't** (see the
  G0 R2 regression), and adds the next tranche. The *why-it's-a-contract* rationale is single-homed in
  [ADR 0042](../documentation/decisions/0042-gold-serving-marts-published-read-contract.md); indexing
  policy in [ADR 0038](../documentation/decisions/0038-gold-layer-modeling-and-indexing-strategy.md).
- **Provenance.** Live inventory `data/exploratory/gold/audit_schema.txt` (run as owner against prod,
  build stamp `2026-06-14 19:43:24 UTC`); the 18-object + critic fan-out workflow `gold-api-audit`;
  static reads of all 15 gold models + 3 silver firm sidecars. **All DB access was SELECT-only; the
  operator ran every query.** Pending: `scripts/sql/gold/audit_coverage.sql`.

---

## §0 — Method, artifacts, and the object inventory

**Method.** (1) Read every gold model + `_gold.yml` + the 3 silver firm sidecars; (2) one audit agent per
object (static per-column analysis + authored a SELECT-only probe) + a cross-cutting critic; (3) a live
catalog inventory (`information_schema`, `pg_indexes`, `pg_stat_user_tables`) for materialized
types/nullability, the indexes actually built, and ANALYZE freshness — **the live data overrides any
static "confirmed" claim** (it caught G0).

**Artifacts.**

| Artifact | Purpose | State |
|---|---|---|
| `scripts/sql/gold/audit_schema_and_indexes.sql` | live columns/types/nullability + indexes + ANALYZE freshness | **run** → `data/exploratory/gold/audit_schema.txt` |
| `scripts/sql/gold/audit_coverage.sql` | per-source population %, enum domains, array cardinality, GROUPING SETS rollup integrity | **pending operator run** (run as owner) |
| workflow `gold-api-audit` | 18 per-object audits + critic synthesis | complete |

**ADR touchpoints** (reserve any new number in `decisions/README.md`, never here): G1/G3/G5 indexing →
**ADR 0038** (no new ADR — additive). Contract-surface gating → **ADR 0042**. G6 firm fuzzy/FTS search →
**reverses [ADR 0037](../documentation/decisions/0037-no-trigram-fuzzy-search.md)** (pg_trgm deliberately
off) → needs an ADR 0037 amendment + sign-off *only if pursued* (default: don't).

**Object inventory (18; live row counts).**

| Object | Kind | Live rows | Indexes | API role |
|---|---|---:|---|---|
| `mart_recall_summary` | table | 93,378 | 4 btree (+R2 **missing**, see G0) | `GET /recalls` list + detail — **the filter surface** |
| `mart_product_search` | table | 471,442 | 5 btree + 2 GIN | `GET /products/search` |
| `mart_firm_profile` | table | 24,331 | 2 btree | `GET /firms/{id}` |
| `fct_recalls_by_geography` | **table** | 421 | 2 btree | `/stats/by-state` (the only materialized fct_) |
| `fct_recalls_by_month`/`_week`/`_year` | view | — | none (view) | `/stats/timeseries` |
| `fct_recalls_monthly_trend` | view | — | none | `/trends` |
| `fct_recalls_by_firm` | view | — | none | `/firms/top` |
| `fct_recalls_by_classification` | view | — | none | `/stats/classification` |
| `fct_recall_status` | view | — | none | `/stats/status` |
| `fct_recalls_by_country` | view | — | none | `/stats/by-country` |
| `fct_units_recalled` | view | — | none | `/stats/units` |
| `dim_date` | table | 32,142 | 1 unique | internal (joined by 5 date-grained fct_) |
| `gold_meta` | table | 1 | none | `/meta` ETag/freshness |
| `firm_usda_attributes` | silver table | 8,003 | 2 btree | sidecar → `/firms/{id}` jsonb + geography |
| `firm_uscg_attributes` | silver table | 16,260 | 2 btree | sidecar → `/firms/{id}` jsonb + geography |
| `firm_fda_attributes` | silver table | 13,418 | 3 btree (+`::text` functional) | sidecar → `/firms/{id}` jsonb + geography |

> **DB-level nullability caveat.** Every gold column reports `is_nullable = YES` — the marts are
> `CREATE TABLE AS SELECT`, so there are **no NOT NULL constraints**; `not_null` lives only as a dbt
> *test*. The API must not assume the DB enforces non-null; it's contract-by-test, not constraint.

---

## §1 — Per-source coverage matrix (the API filter surface)

Legend: **✓** populated by construction · **✗** NULL by construction (structural) · **~** partial ·
_(% pending)_ exact magnitude from `audit_coverage.sql`. **⚠ param-trap** = single-source; filtering it
silently drops the other sources — **keep returning, do not filter**.

### `mart_recall_summary` (one row per recall)

| Column | CPSC 9,853 | FDA 50,552 | USDA 1,217 | NHTSA 30,075 | USCG 1,681 | Filter verdict |
|---|:--:|:--:|:--:|:--:|:--:|---|
| `source` | ✓ | ✓ | ✓ | ✓ | ✓ | **GOOD** — indexed (lead of `(source,published_at)`) |
| `source_recall_id` | ✓ | ✓ | ✓ | ✓ | ✓ | GOOD exact-only — **not indexed** (G4: btree vs md5 route) |
| `published_at` | 100% | 100% | 100% | 100% | 100% | **GOOD** sort/keyset anchor — index **regressed (G0)** |
| `announced_at` | 100% | **−20 null** | 100% | 100% | 100% | GOOD date-range — exactly 20 FDA null (confirmed); **unindexed (G4)** |
| `distribution_scope` | ✓ | ✓ | ✓ | ✓ | ✓ | **GOOD, clean** — 100% NOT NULL, 4 values; just add the dbt test (G2) |
| `lifecycle_status` | ✗0 | ✓ | ✓ | ✗0 | ✓ | GOOD — null CPSC/NHTSA; **unindexed (G4)** |
| `is_active` (tri-state) | ✗ | ✓ | ✓ | ✗ | ✓ | **GOOD** indexed — f 46,265 · NULL 39,928 · t 7,185 |
| `classification` | ✗ | ✓ | ✓ | ✗ | ✓ | indexed but **source-native** (`Class I/II/III` · `H/L/M/S`) — filter only within source |
| `distribution_state_codes` `text[]` | ✗0 | 36,344 | 793 | ✗0 | ✗0 | GOOD array, FDA/USDA only — **no GIN (G1)** |
| `distribution_country_codes` `text[]` | ✗0 | 7,237 | ✗0 | ✗0 | ✗0 | GOOD array, **FDA-only in practice**; `US`=0 confirmed; **no GIN (G1)** |
| `risk_level` | ✗0 | ✗0 | 1,217 | ✗0 | ✗0 | **⚠ param-trap (USDA-only) — confirmed** |
| `reason_category` | ✗0 | ✗0 | 1,202 | ✗0 | ✗0 | **⚠ param-trap (USDA-only) — confirmed** |
| `distribution_states` (scalar) | ✗0 | ✗0 | 872 | ✗0 | ✗0 | render-only USDA passthrough; redundant w/ `_state_codes` (G7) |
| `product_upcs` `jsonb` | 453 | ✗0 | ✗0 | ✗0 | ✗0 | recall-level UPCs — CPSC-only, **sparse**; no GIN |

### `mart_product_search` identifier coverage (one row per recall_product, 470,562)

| Column | Who carries it | Index | Note |
|---|---|---|---|
| `model` | CPSC 11,839 + NHTSA 321,223 | btree ✓ | exact lookup |
| `hin` | USCG 914 (of 1,681) | btree ✓ | exact lookup |
| `model_year` | NHTSA 290,815 + USCG 1,137 | none | text, needs cast for a vehicle facet |
| `upc` | **0 — 100% NULL** | btree (empty) | **drop from API surface + drop btree** (G5) |
| `recall_product_upcs` `jsonb` | **CPSC 466 only** | **GIN ✓ (R3)** | real UPC path — but **barely populated** |
| `search_vector` `tsvector` | all (0 empty) | **GIN ✓** | product FTS; folds `recall_title` |
| `type` | all (incompatible vocabularies) | none | **not** a cross-source filter (G7) |

---

## §2 — Prioritized gold changes

Break-risk is per ADR 0042: **adding an index = none** (indexes aren't contract); **adding a column =
backward-compatible** (no `schema_version` bump); **rename/retype/re-key = breaking** (coordinate + bump).

**Indexing budget.** Postgres caps neither index count nor width meaningfully, but every gold table is
`materialized='table'` and **dropped + recreated on each nightly `dbt build`**, so each index is **rebuilt
from scratch every night** (rebuild time + Neon storage/compute) and slows that rebuild's writes. So index
**deliberately** — only confirmed hot-path filters that are well-populated. Concretely: **restore R2** (G0)
and **add the two array GINs** (G1; the FDA/USDA arrays are well-populated), and **drop the all-NULL `upc`
btree** (G5). Do **not** index `distribution_scope` (4-value enum — a seq/bitmap scan beats a btree),
`lifecycle_status` (low-card), or `announced_at` / `source_recall_id` until real traffic proves them hot.

> **Batch 1 implemented 2026-06-15 (this branch):** G1 (two GINs on `mart_recall_summary`), G2
> (`distribution_scope` `accepted_values`+`not_null` test), G5 (dropped the all-NULL `upc` btree), G7
> (`mic_renamed_not_recycled` documented in `_silver.yml`), and the G0 CI gate
> (`dbt/tests/assert_gold_serving_indexes_present.sql`). All additive / non-breaking — apply with a
> `dbt build`. **Batch 2 resolved 2026-06-15 with the recalls-api chat:** G3 (Option B recall `search_vector`)
> **BUILT**; G4 (`announced_at` btree) **declined** by the API (sub-ms unindexed scan; ADR 0038); G6 (firm
> fuzzy) **stays best-effort** (no pg_trgm, locked decision 5). API consumes G1/G2 (geo filters + scope enum)
> on its side; `/recalls/search` is a v1.1 fast-follow.

### G0 — 🟢 ROOT-CAUSED + FIXED (was 🔴 P0): R2 keyset index oscillation (dbt post_hook name collision)

- **OBSERVED (fact, `audit_schema.txt` §C):** `mart_recall_summary` has exactly **4** indexes — `(recall_event_id)` unique, `(source, published_at)`, `(is_active)`, `(classification)`. **None** is the `(published_at DESC, recall_event_id)` keyset index. The other 7 mart indexes (incl. both GINs on `mart_product_search`) and all 4 `config(indexes)` btrees here **are** present — only the **`post_hook`-declared** R2 index is missing.
- **DECLARED:** `mart_recall_summary.sql:10-11` declares it via `post_hook` (`create index if not exists {{ this.name }}_published_at_desc_evt …`); `verify_gold_readiness.sql §3` checks for it and reportedly **PASSed 2026-06-14**.
- **INFERENCE (verify, don't assume):** the index **regressed out of the live catalog** at the `19:43` build despite being declared — even though the *second* post_hook (`analyze`) on the same model **did** run (`last_analyze 19:44:18 > rebuilt_at 19:43:24`). Cause unconfirmed (partial build path? post_hook that silently no-op'd? dev/prod branch divergence?).
- **Impact:** this index backs `GET /recalls` **default recency sort + keyset pagination** over 93,378 rows. Absent it, the global (unfiltered) list does a full sort each page; `(source, published_at)` only helps when a `source` filter is present.
- **Re-run 2026-06-14 (reproducible, not transient):** a second `audit_schema_and_indexes.sql` still shows the same 4 indexes and `gold_meta.rebuilt_at` unchanged (`19:43:24`) — no rebuild has happened, so the index is **stably absent** on this build.
- **ROOT CAUSE (confirmed 2026-06-15) — NOT a one-off, a deterministic oscillation:** the `dbt run --select mart_recall_summary` restore recreated R2 (§3 `PASS`), but the **next** `dbt build` (from the now-present state) **lost it again**. Postgres index names are unique **per schema**, so the fixed-name `create index if not exists {{ this.name }}_published_at_desc_evt` collides with the same-named index still attached to dbt's `…__dbt_backup` table mid-rebuild, **no-ops**, and the backup drop then removes the only index with that name. Present-on-prior-build → lost; absent-on-prior-build → created (it flips every other build). Only fixed-name `post_hook` indexes are affected; the hash-named `config(indexes)` are immune. `firm_fda_attributes.idx_firm_fda_attributes_fei_text` carried the **same latent bug**.
- **FIX (2026-06-15):** both post_hooks switched to **`drop index if exists` + `create index`** (no `if not exists`) — the drop frees the stale `__dbt_backup` name first, so the index rebuilds deterministically every run (`mart_recall_summary.sql`, `firm_fda_attributes.sql`). **Break-risk: none.**
- **DONE (Batch 1):** added `dbt/tests/assert_gold_serving_indexes_present.sql` — a singular test (severity=error) that fails if the R2 keyset index or R3 UPC GIN is missing from the live catalog. It runs in `transform.yml`'s existing final `dbt test` step (no workflow edit needed), so a recurrence fails CI instead of silently degrading the API.
- **CONFIRMED FIXED 2026-06-15:** rebuilt `mart_recall_summary` **twice consecutively** — `verify_gold_readiness §3` read `R2 = PASS` after *both* (pre-fix the 2nd build would have flipped it to `FAIL`); `firm_fda_attributes` rebuilt once to bank its fix. Oscillation gone. The `dbt test` index gate remains as defense-in-depth.

### G1 — 🟠 P1: GIN on `distribution_state_codes` + `distribution_country_codes`

- The two **confirmed GOOD array filters** have **no index on the mart** (the GINs exist only on silver `recall_distribution_area`). `@>` / `&&` array filters in `GET /recalls` run a seq-scan over 93k rows.
- **Action:** add to `mart_recall_summary` config: `{'columns':['distribution_state_codes'],'type':'gin'}` and `{'columns':['distribution_country_codes'],'type':'gin'}`. **Break-risk: none. Effort: S.**

### G2 — 🟢 P2 (downgraded): add the `distribution_scope` `accepted_values` test

- **Coverage CONFIRMS the API's assumption:** `distribution_scope` is **100% NOT NULL across all 5 sources**, exactly **4 values** — `Nationwide` 42,183 · `Regional` 32,255 · `Unspecified` 11,973 · `International` 6,967 (sum = 93,378 = row count). A clean all-source enum filter; **no index needed** (low-card).
- **Gap:** it's **unguarded** — no `accepted_values`/`not_null` dbt test, so a future silver change could add a 5th value or a NULL and the API wouldn't be warned.
- **Action:** add `accepted_values: [Nationwide, Regional, Unspecified, International]` + `not_null` to `_gold.yml`. **Break-risk: none. Effort: S.**

### G3 — 🟢 BUILT 2026-06-15 (was 🟡 P2): Option B — recall-grain `search_vector` for `GET /recalls/search`

- **Decision (cross-repo, recalls-api `build/07`+`08`):** YES, as a **v1.1 fast-follow** — the endpoint ships post-v1 on the API's `feature/recalls-search`; the gold column is staged **now** (additive/non-breaking, ADR 0042 → obligates nothing). Recall-grain (not dedup-the-product-vector) because recall-in/recall-out wants recall grain + clean `ts_rank`/keyset; folding `product_names` makes it **subsume** the product vector's reach for recall search.
- **Built:** `search_vector` tsvector + GIN on `mart_recall_summary` via `config(indexes)` (hash-named → **immune to the G0 oscillation class**, unlike a fixed-name post_hook). `setweight` buckets (4, 1:1) — **A=title · B=`product_names`(flattened)+`primary_firm_name` (what/who) · C=`recall_reason` (why) · D=`consequence_of_defect` (harm tail)**, so the API can rank a real brand/product match above a narrative-only mention; `corrective_action`+`hazards` excluded (boilerplate / opaque). The field→bucket map is the gold contract; the API tunes the numeric `ts_rank_cd` `{D,C,B,A}` weights at query time (no rebuild).
- **Index gate:** left R2/R3-only for now (the new GIN is config-named/stable and unconsumed until v1.1); fold it into `assert_gold_serving_indexes_present` when `/recalls/search` ships. **Break-risk: none.**

### G4 — 🟡 P2: index decisions for the remaining filters

- `announced_at` (date range) — **unindexed**; add btree if range-filtered hot.
- `lifecycle_status` — **unindexed**; low-card, likely fine unindexed.
- `source_recall_id` — **unindexed**; **decision:** does the API filter it directly (→ `(source, source_recall_id)` btree) or always derive `recall_event_id` via the md5 recipe (→ no index)? See §5.
- **Break-risk: none. Effort: S each** (gated on the §5 decisions).

### G5 — 🟢 P3: drop the all-NULL `upc` placeholder (and its btree) from `mart_product_search`

- `upc` is **100% NULL** (cast null in all 5 source branches); its btree is an empty index; the model header invites the drop. Route UPC search through `recall_product_upcs` (R3 GIN). **Break-risk:** dropping the *column* is breaking post-freeze (coordinate); dropping the *btree* is free. **Effort: S.**

### G6 — 🟢 P3: firm-name search for `/firms/search` (gated)

- `mart_firm_profile` has only `firm_id` + `normalized_name` btrees — **contains/fuzzy firm search is unserved** (pg_trgm off per ADR 0037, no tsvector here). **Action (if pursued):** add a firm `search_vector`+GIN (Postgres FTS, no ADR reversal) **or** enable `pg_trgm` (**reverses ADR 0037** — needs amendment + sign-off). Default: FTS, not trgm. **Break-risk: none** (additive). **Effort: M.**

### G7 — 🟢 P3: naming / semantic / contract-drift items (mostly *document*, ADR 0042 makes renames breaking)

- **`source` enum is inconsistent across objects** — 6-value w/ `ALL` (by_month/week/year/classification/geography/status), 5-value no `ALL` (monthly_trend, the two product/firm marts), 4-value no-CPSC-no-`ALL` (units), 3-value FDA/USDA/ALL (country). The monthly_trend/units/country omissions are **structurally correct** — *document*, don't "fix". A codegen assuming "every fct_ has an `ALL` row" is wrong.
- **`ALL` means different coverage** — on `by_country` it spans only FDA+USDA, elsewhere all 5. Same string, different denominator.
- **time-axis column name divergence** — `period` (by_month/week/year, units, geography) vs `month` (monthly_trend). A `/stats` union must alias.
- **`distribution_states` (scalar, USDA) vs `distribution_state_codes` (text[], FDA+USDA)** — near-identical names, different type/population; the array is derived from the scalar for USDA. Rename is breaking; **document** the distinction (ADR 0042 already flags it).
- **two active signals** (`is_active` indexed/canonical vs `is_currently_active` lifecycle-derived) and **two edit counters** (`edit_count` raw-nullable vs `edit_event_count` coalesced-0) on one row — pick & document which the API uses.
- **contract drift:** `firm_uscg_attributes.mic_renamed_not_recycled` is in the model SELECT (and rides into `/firms/{id}` via `to_jsonb`) but is **missing from `_silver.yml`** — add the column doc.
- **`fct_recalls_by_firm.active_recalls`** wasn't renamed to the fct_ convention (`event_count`/`product_count` were) and **undercounts CPSC/NHTSA** (NULL `is_active` → not-active). `event_count_rank` uses `rank()` (skips after ties).

### Landed-item verdicts (confirm / repudiate)

| Item (claimed landed) | Verdict | Evidence |
|---|---|---|
| **R2** keyset index `(published_at DESC, recall_event_id)` | **was absent → ✅ RESTORED 2026-06-15** | missing after the 19:43 build; `dbt run --select mart_recall_summary` recreated it (post_hook functional). Add a CI gate (G0) |
| **R3** GIN on `mart_product_search.recall_product_upcs` | ✅ confirmed | `audit_schema.txt §C` — GIN present (jsonb `@>`) |
| `search_vector` GIN on `mart_product_search` | ✅ confirmed | `audit_schema.txt §C` |
| `gold_meta(rebuilt_at, schema_version)` | ✅ confirmed | 1 row, both populated; rebuilt by **both** `transform.yml` `dbt build` steps (refreshes `rebuilt_at` every nightly run) — confirmed |
| `fct_recalls_by_month` / `fct_units_recalled` | ✅ confirmed | exist as views, correct shape |
| **R5** sidecar rename `firm_{usda,uscg,fda}_attributes` | ✅ confirmed | live column names on `mart_firm_profile` |
| **R7** ANALYZE freshness | ✅ confirmed | all serving marts `last_analyze ≥ rebuilt_at`; `gold_meta` "STALE" is immaterial (1-row table, no analyze post_hook — add a one-line `analyze` post_hook to silence the flag; not required) |

---

## §3 — API changes: SAFE-NOW vs BLOCKED-ON-GOLD

**SAFE-NOW** (API-only predicate; coverage-confirmed; index already present):

| Predicate / route | Backing | Note |
|---|---|---|
| `?source=` | `(source, published_at)` btree | closed UPPERCASE enum |
| `?is_active=` | `(is_active)` btree | tri-state — expose true/false/unknown |
| `?classification=` (within a source) | `(classification)` btree | source-native; pair with `?source=` |
| `GET /products` by `model` / `hin` | btrees | exact lookup |
| `GET /products` UPC containment | `recall_product_upcs` GIN (R3) | dedup (array repeats across product rows) |
| `GET /meta` | `gold_meta` | already read internally for ETag |
| **return** `risk_level`, `reason_category` | — | ⚠ do **not** expose as filters |

**BLOCKED-ON-GOLD** (needs a §2 change first):

| Predicate / route | Blocked on | Item |
|---|---|---|
| `GET /recalls` default recency + keyset pagination | the R2 index is **missing** | **G0** |
| `?distribution_state_codes=` / `?distribution_country_codes=` (array `@>`) | no GIN | **G1** |
| `?distribution_scope=` (as a *trusted* enum) | no test/verified domain | **G2** |
| `GET /recalls/search` (recall FTS) | ✅ `search_vector`+GIN built (G3) — endpoint is v1.1 | **G3 done** |
| `?announced_at` range (at scale) | unindexed | **G4** |
| `?source_recall_id=` direct | unindexed (or route via md5) | **G4 / §5** |
| `GET /firms/search?name=` | no firm FTS/trgm | **G6** |

---

## §4 — New endpoint candidates (underused gold objects)

The `fct_*` views are **not** in the ADR 0042 contract surface — each joins it when its endpoint ships.

- **`GET /stats/timeseries?grain={week|month|year}&source=<src|ALL>&from=&to=`** — ONE multiplexed endpoint over `fct_recalls_by_{week,month,year}` (identical `(period, source, event_count)` shape). Default `source=ALL`; reject SUM-across-source (double-count). Beats 3 near-duplicate endpoints.
- **`GET /trends?source=<required>&from=&to=`** — `fct_recalls_monthly_trend` (rolling 3/12-mo avg + YoY). `source` REQUIRED (no `ALL` row; cross-source rows not comparable).
- **`GET /stats/by-state`** (`fct_recalls_by_geography`, the materialized table) — `geography_basis` REQUIRED (default `distribution`), `source` default `ALL`, optional `state_code`. **Forbid cross-state SUM as a national total** (multi-count, documented). `distribution` lens is FDA/USDA-only.
- **`GET /stats/by-country`** — `fct_recalls_by_country`; `US` is a **derived synthetic cell** absent from `distribution_country_codes[]`; join `country_iso` seed for names.
- **`GET /stats/classification?source=` and `/stats/risk-level?source=USDA`** — `fct_recalls_by_classification`; per-source (non-conformed enums); risk-level is USDA-locked.
- **`GET /stats/status`** — `fct_recall_status`; document `ALL` rollup + CPSC/NHTSA 100% `unknown`.
- **`GET /stats/units?source=<required>&unit_category=`** (+ `/units/largest`) — `fct_units_recalled`; MUST require `source` (no `ALL`, CPSC absent); surface unit-parse coverage % (`recalls_with_units` vs full denominator).
- **`GET /firms/top`** — `fct_recalls_by_firm`; `?sort=product_count|active`, `?since=`, `?min_recalls=`. Document `rank()` skip-on-tie (LIMIT rows, not rank).
- **`GET /products/by-upc/{upc}` · `/by-hin/{hin}` · `/by-model/{model}`** — exact lookups (R3 GIN / btrees).
- **`GET /recalls?edited=true` / `?retracted=true`** + a "recently changed" feed sorted by `last_seen_at` — booleans already materialized (`has_been_edited`, `was_ever_retracted`).
- **`GET /firms/by-fei/{fei}`** — exact FDA FEI (unique btree + `::text` functional index); optional `formerly_known_as` from `firm_surviving_nam/_fei` (informational, **not** identity collapse, ADR 0037).
- **Deferred / need a new model:** `fct_recalls_by_fiscal_year` (`dim_date.us_fiscal_year` is built but **unwired** — federal data is fiscal-year-native), `fct_recalls_by_quarter` (`quarter_start` unused), a **cross-source `ALL` monthly trend** (not derivable from the per-source-partitioned trend view), a status-over-time model.

---

## §5 — Open questions (not resolved by static analysis)

**Empirical — RESOLVED by `audit_coverage.sql` (2026-06-14):**
- `distribution_scope` — **100% NOT NULL, 4 values** (Nationwide 42,183 / Regional 32,255 / Unspecified 11,973 / International 6,967) → G2 downgraded to "add the test".
- `lifecycle_status` — **source-native, NOT conformed** (like `classification`); populated FDA/USDA/USCG only, NULL for CPSC/NHTSA. Domain: FDA `Terminated` 41,631 / `Ongoing` 6,847 / `Completed` 2,074; USCG `Closed` 1,511 / `Open` 170; USDA `Closed Recall` 1,049 / `Public Health Alert` 163 / `Active Recall` 5. **Filter only within a source** — the conformed cross-source active/inactive signal is `is_active` (tri-state).
- `announced_at` nulls — **exactly 20, all FDA** (50,532/50,552); 100% elsewhere → the "~20 FDA null" claim is exact.
- arrays — `distribution_state_codes` 40,064 non-null (2,927 empty), nonempty FDA 36,344 + USDA 793; `distribution_country_codes` 40,064 non-null (32,827 empty), nonempty **FDA-only 7,237** (USDA 0); **`@> ARRAY['US']` = 0** (US-exclusion confirmed); max 126 foreign countries on one recall.
- `mart_product_search` — `upc` **100% NULL**; `model` CPSC 11,839 + NHTSA 321,223; `hin` USCG 914/1,681; `model_year` NHTSA 290,815 + USCG 1,137; `recall_product_upcs` nonempty **only CPSC 466** (UPC search barely populated); `search_vector` **0 empty**.
- `fct_recalls_by_country` — US cell **50,897** of **51,769** distinct FDA+USDA recalls; **~872 foreign-only**; 142 distinct countries (top foreign CA 4,483, DE 3,419).
- `mart_firm_profile` — **46% of firms (11,227/24,331) carry no sidecar block**; 113 span 2 sources, 10 span 3; `normalized_name` is **fully unique** (exact-name lookup already served); max distinct_products 48,006 (NHTSA component inflation).
- **GROUPING SETS rollup integrity — PASS** (by_month/week/year/status: `ALL` = Σ sources, 0 mismatch). Spine is **sparse** (by_month 565/615 months present → consumers must zero-fill; `monthly_trend` is dense).
- sidecars — USDA `status_regulated_est` ''=active 7,206 / Inactive 797; `size` 'N / A' dirty = 811; USCG `mic_oob_recycled` = **3,024** (≫ the paren-only 205, as expected); USCG `country`/`state` are **dirty** (CANADA/usa/UNTIED-STATES typos, Canadian provinces in `state`); FDA `firm_state_cd` 85.0% / `firm_postal_cd` 92.7%, clean country names.
- `dim_date` — 1940-01-01..2027-12-31, 0 nulls, contiguous, **fully covers** the recall range (1975-04-07..2026-06-13).

**Decisions (not resolvable from data):**
1. **G0 root cause** — why did the post_hook R2 index not persist, and should it be a CI assertion?
2. **`source_recall_id`** — does the API filter it directly (→ btree) or always derive `recall_event_id` via md5 (→ no index)? (Same shape: does `/products/search` need a `source` filter index?)
3. **Which active signal** does the API filter — `is_active` (indexed) vs `is_currently_active` (not)? Document the open-recall semantics.
4. **G3 de-scope** — if both the recall and product `search_vector` ship, they overlap (product already folds `recall_title`); confirm the boundary so `/recalls/search` and `/products/search` don't duplicate.
5. **`gold_meta.schema_version` enforcement** — the bump is manual (`--vars`); nothing in dbt/CI forces a breaking change to ship with a bump. Want a CI guard before the API freezes `openapi.json`?

---

## Appendix A — live index inventory (`audit_schema.txt §C`, build 2026-06-14 19:43)

```
mart_recall_summary : (recall_event_id)·unique  (source, published_at)  (is_active)  (classification)
                      [MISSING: (published_at DESC, recall_event_id) ← G0]
mart_product_search : (recall_product_id)·unique  (recall_event_id)  (hin)  (model)  (upc)·empty
                      (recall_product_upcs)·GIN  (search_vector)·GIN
mart_firm_profile   : (firm_id)·unique  (normalized_name)
fct_recalls_by_geography : (geography_basis, source, state_code)  (state_code)    -- only materialized fct_
dim_date            : (date_day)·unique
firm_usda_attributes: (establishment_id)·unique  (state)
firm_uscg_attributes: (mic)·unique  (state)
firm_fda_attributes : (firm_fei_num)·unique  (firm_state_cd)  ((firm_fei_num)::text)·functional
```
