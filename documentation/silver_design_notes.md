# Silver Design Notes — Bronze-to-Silver Mapping

**Scope:** the design decisions behind unifying all five **recall** sources (CPSC, FDA, USDA, NHTSA, USCG) from bronze into the shared silver schema. (The pipeline has **nine** bronze extractors; the other four — `usda_establishments`, `uscg_manufacturers`, `uscg_manufacturer_details`, `fda_press_releases` — feed the firm SCD-2 sidecars + the press-release child, *not* the recall union, so "five sources" throughout this doc means the recall-union grain.) This doc owns the *patterns* — per-source staging dedup, event/product grain, surrogate-key recipes, UNION-parity null-handling, and the value-normalization discipline. It does **not** restate the semantic field→canonical-column **map**: that is single-homed in `documentation/audit/cross_source_consolidation.md` (the W2 SSOT — §1 `recall_event`, §2 `recall_product`, §3 firm/bridge/sidecars, §5 rename ledger, §6 null-handling). For full-corpus shape/enum evidence see `documentation/audit/bronze_corpus_profile.md`; for system architecture `documentation/architecture.md`; for the canonical schema reference `documentation/data_schemas.md`.

**Out of scope:**
- **Cross-source firm entity resolution (6b)** — fuzzy/RapidFuzz matching, suffix-stripping, DBA extraction, `firm.alternate_names`, per-recall disambiguation. `firm.sql`/`recall_event_firm.sql` are left in a clean state for 6b; no name-cleaning happens here.
- **Capture-expansion fields not yet in bronze ((b))** — FDA firm address/continuity, `productdescriptionshort` (the Bug-2 `product_name` target), cross-source `upc`/`hin`/`vin` merge. See consolidation §7.

> **Provenance:** renamed 2026-05-01 from `architecture_overview.md` (system-architecture content moved to `architecture.md`). Expanded from CPSC+FDA to all five sources 2026-06-03 (the `feature/silver-field-remap` W6 doc-sync), and the per-source field map relocated to the consolidation doc per the single-home rule.

---

## Field map → the consolidation doc

Which bronze field populates each canonical silver column — for every source and every column, with the bug-fix annotations (Bug 1 FDA `recall_reason`, Bug 3 FDA `product_description`, the USDA/NHTSA `type` fixes, USCG `model`→NULL) — lives in `cross_source_consolidation.md`. The sections below cover the *mechanics* that map relies on, not the mapping itself.

---

## Major Design Decisions

### 1. Per-source staging dedup + event/product grain

Bronze is content-hash-versioned (ADR 0007): a record edited at source lands as a *new* bronze row with a new `content_hash`. Each staging model resolves that edit history with `row_number() over (partition by <identity> order by extraction_timestamp desc)` keeping `rn = 1` — the latest version per logical identity. The identity (and therefore the silver grain) differs per source:

| Source | `recall_event` grain (business key) | `recall_product` grain | Staging dedup partition |
|---|---|---|---|
| **CPSC** | `RecallNumber` — one bronze row per event already | exploded `Products[]` element (+ ordinal) | `source_recall_id` |
| **FDA** | `RECALLEVENTID` — `DISTINCT ON` collapses product rows to one header | `PRODUCTID` — bronze is already one-row-per-product (flat) | `source_recall_id` (= PRODUCTID) |
| **USDA** | `field_recall_number` (English only) | one per recall (= `recall_event_id`) | `(source_recall_id, langcode)`, then filter `langcode = 'English'` |
| **NHTSA** | `campno` — `DISTINCT ON` collapses the 11-tuple rows to one header | the **7-tuple** `md5` (silver key since the 6c.7 cutover — §12) | the 11-tuple identity (ADR 0030) |
| **USCG** | `Number` — one bronze row per recall | one per recall (= `recall_event_id`) | `source_recall_id` |

Two sources need the extra event-level collapse because their bronze grain is *finer* than a recall event: FDA bronze is one row per product, NHTSA bronze is one row per vehicle × component × part × batch. `DISTINCT ON (<event key>) ORDER BY <event key>, extraction_timestamp DESC` picks a representative row; this is safe because event-level fields (`firm_legal_nam`, `phase_txt`; `desc_defect`, `corrective_action`) are identical across all the finer rows sharing an event key. USDA's dedup additionally drops the Spanish bilingual sibling (EN-primary); USCG and CPSC are already one-row-per-recall and only need edit-history resolution.

### 2. Flat vs. exploded product rows

Three shapes feed `recall_product`:

- **Exploded array (CPSC)** — products live as a JSONB array on the event row; `recall_product.sql` uses `lateral jsonb_array_elements() with ordinality`, and the surrogate key includes the **ordinal** because product names are not unique within a recall.
- **Already flat (FDA, NHTSA)** — each bronze row *is* a product instance, so the branch reads staging directly. FDA keys on `PRODUCTID` (a globally unique API sequence); NHTSA keys on the `md5` of its **7-tuple** silver key (§12; since the 6c.7 cutover — formerly the 11-tuple, where `bgman`/`endman` were the batch-level disambiguator, now demoted to SCD-2 attributes).
- **One-product-per-recall (USDA, USCG)** — these sources name a single product blob / boat model per recall, so `recall_product_id = recall_event_id`. This preserves referential integrity (every event has ≥1 product) without inventing structure the source doesn't provide; USDA's `product_items` free-text stays unparsed (ADR 0002 defers structured parsing — the raw lives in `source_specific_attrs.product_items_raw`).

### 3. The conformed `firm` dimension + per-source attribute sidecars

`firm` is the **conformed dimension** — keyed on `normalized_name = upper(trim(name))`, shared across all five sources, and the only firm object the fact bridge (`recall_event_firm`) joins to (on `firm_id`). Matching on normalized name gives implicit cross-source dedup: a firm appearing in several sources collapses to one row with all its structured IDs gathered in `observed_company_ids`. `firm.sql` and `recall_event_firm.sql` must stay in **lockstep** (identical `firm_id` recipes per branch) — the relationships test catches any divergence as orphan `firm_id`s.

Role model per source:

| Source | Roles emitted | Notes |
|---|---|---|
| **CPSC** | `manufacturer`, `importer`, `distributor` | **`retailer` removed (Option B)** — retailer names live in `recall_event.sales_channel_narrative`, not the firm graph (−44.2% of CPSC firm rows; suffix/DBA cleanup is 6b). |
| **FDA** | `establishment` | `firm_legal_nam` is the recalling FDA-registered establishment (analogous to USDA), not a manufacturer; `company_id = firm_fei_num`. |
| **USDA** | `establishment` | `company_id = establishment_number` via the HTML-decoded name join (~97% per-name); rich metadata → `firm_usda_attributes` sidecar. |
| **NHTSA** | `filer`, `manufacturer` | **filer/manufacturer split** — two bridge rows: `mfgname` = `filer` (filed the recall), `mfgtxt` = `manufacturer` (made the product); 95.9% disjoint when differing. `company_id = NULL`. |
| **USCG** | `manufacturer` | directory-enriched name `coalesce(directory.company_name, recalls.company_name, mic)`; `company_id = mic`; rich metadata → `firm_uscg_attributes` sidecar. |

**Sidecar (supertype/subtype) pattern:** USDA and USCG each publish a *separate structured firm registry* (FSIS Establishment Listing; USCG Manufacturer Directory), so their rich, non-conforming attributes (FSIS `grant_date`/`status_regulated_est`/`size`/`district`; USCG `detail_url`/`uscg_directory_id`) live in dedicated single-source dims keyed on the source's structured ID (`establishment_number`, `mic`) rather than bloating the conformed `firm` with 60–80% NULLs. CPSC/NHTSA expose only inline names (no registry → no sidecar); FDA has an FEI but its address/registry fields are deferred to (b). Facts join only the conformed `firm`; the sidecars are an opt-in dimension-to-dimension join via `observed_company_ids`.

### 4. Surrogate key strategy

All silver surrogate keys are deterministic `md5`, source-prefixed for global uniqueness — except `firm_id`, which is deliberately source-agnostic to enable cross-source dedup:

- `recall_event_id` → `md5('<SOURCE>' || '|' || <event business key>)`
- `recall_product_id` → `md5('<SOURCE>' || '|' || <product business key>)` (CPSC adds the ordinal; NHTSA is the **7-tuple** from the v1.5 snapshot current view since the 6c.7 cutover — §12, ADR 0033·0034 — formerly the 11-tuple)
- `firm_id` → `md5(upper(trim(<name>)))` — **no source prefix**, so same-named firms across sources collapse to one row

Composite-grain uniqueness is enforced explicitly via `dbt_utils.unique_combination_of_columns` on `recall_event (source, source_recall_id)` and `recall_event_firm (recall_event_id, firm_id, role)`.

### 5. UNION parity + null-filling

Both `recall_event` and `recall_product` are `UNION ALL` of five source-branch CTEs. The cardinal correctness rule: **every canonical column appears in all five branches, in the same position and type** — a source either supplies the value or `cast(null as <type>)`. The consolidation §1/§2 column lists are that parity checklist. A source-specific lift (e.g. CPSC `remedies`, USCG `hin`) is the source's value in its own branch and a typed NULL in the other four. `source_payload_raw` / `source_specific_attrs` carry only *genuinely-residual* fields — anything promoted to a first-class column is removed from the JSONB to avoid duplication.

Documented-empty-by-source columns stay silent-blank (noted in `_silver.yml`), e.g. CPSC `products[].description`/`.model` (100% empty), NHTSA `rpno` (dropped). `recall_product.upc` is NULL for all sources at this stage (the cross-source `upc`/`hin`/`vin` merge is (b)); `hin` is lifted to its own column as the USCG-only UPC analog.

### 6. The value-normalization discipline (ADR 0027)

Bronze stores source-verbatim values with storage-forced coercion only; **value-level normalization is silver's job**, applied in the staging views. The sentinel convention splits the sources two ways:

- **The `''`-club {FDA, USDA, NHTSA}** use empty string as the missing-value sentinel → staging wraps optional columns in `nullif(col, '')`. A naive `IS NULL` against bronze misreads these as present.
- **NULL + named sentinels {CPSC, USCG}** use genuine SQL NULL for missing scalars, but carry *named* trap values that must be stripped: USCG `'N/A'` (hin), `'9999'` (model_year), `'1970-01-01'` (opened_on epoch), `0`≡`00` (boat_type), plus disposition/severity case-folding (`lower()`/`upper()`).

Derived columns follow the same staging-first principle: USDA `risk_level` is derived 1:1 from `classification` (not lifted), the USDA `dbas` array is element-filtered for `'N/A'`/`'None'`/`''` placeholders, and the FDA `initiated_by` flag conforms the four raw `voluntary_type_txt` values to `firm`/`agency` while `recall_initiator` keeps the raw value.

### 7. Source freshness thresholds

Freshness configs live in `_sources.yml` (the SSOT); thresholds track each source's real publication cadence rather than a uniform default:

| Source | warn_after | error_after | Cadence rationale |
|---|---|---|---|
| CPSC | 48 h | 7 d | publishes all days — a 2-day gap is a genuine problem |
| FDA | 72 h | 7 d | Mon–Fri only; 72 h absorbs weekends without false alerts |
| NHTSA | 48 h | 7 d | near-daily flat-file refresh |
| USDA (recalls + establishments) | 14 d | 30 d | low-frequency recall feed + periodic establishment-listing snapshot |
| USCG | 8 d | 14 d | scraped listing, low cadence (tightens to 2 d / 5 d once the Step-6 short-circuit lands) |

All sources error at the outer bound regardless of cadence — that indicates a genuine outage, not a quiet publication window.

### 8. `recall_event_history` — event-edit synthesis (ADR 0022 + 0027)

`recall_event_history` is the field-level edit log of the recall **event** — distinct from the dimension SCD-2 snapshots and from `recall_lifecycle`. Two kinds of history live in this layer: the recall *fact* (this model + lifecycle, synthesized via `LAG()`/manifest, stateless) and the *dimensions* (firm SCD-2 snapshots, materialized, stateful). This model is the fact half (ADR 0022).

- **Mechanism.** Bronze is content-hash-deduped, so consecutive rows per identity *are* the distinct content versions (ADR 0007). The model `LAG()`s each tracked field over a recall's bronze snapshots and emits one row per changed field per interval. Uniform `LAG()`-over-bronze for all five sources — FDA's native history endpoints are empty, so there is no source-asymmetric path (ADR 0022).
- **Curated v1 field set:** `recall_reason`, `classification`, `lifecycle_status`, `title`, `terminated_at` — the editorially-meaningful event attributes that map to *direct* bronze columns. Noisy/jsonb/synthesized fields (e.g. `company_media_contact`, the USDA array fields, synth titles) are deliberately excluded; values are cast to text (history is an audit surface).
- **Grain per source.** CPSC/USCG event-grain directly; **FDA** collapses product-level bronze to the event (`recall_event_id`); **NHTSA** collapses 11-tuple line rows to the campaign (`campno` — NOT the regen-unstable `RECORD_ID`); **USDA** keeps `langcode` (EN/ES edit independently, ADR 0006). The collapsed event-level fields are stable across the collapsed rows, so the deterministic representative (content_hash tiebreak) is safe.
- **Three correctness rules.** (1) *Re-baseline exclusion* (ADR 0027): suppress events whose current snapshot is a `schema_rebaseline`/`hash_helper_rebaseline` run (parser/hash artifacts, not edits) — the rebaseline snapshot stays in the LAG sequence so the next routine edit compares against the reparsed baseline (no spurious carry-over). (2) *Cosmetic-noise folding* via `norm_text_for_change` (whitespace runs + `''`↔`NULL`) so whitespace churn (USDA Finding Q) and empty-representation flips don't synthesize edits. (3) *Creation ≠ edit*: emit only where a prior snapshot exists; a later NULL→value still counts.
- **Sparsity.** Post-6a.5-reseed the table is sparse (~1 version per identity); it grows as daily incrementals re-accumulate versions. The `dbt unit test` (`recall_event_history_unit_tests.yml`) verifies the emission logic against synthetic snapshots regardless of live-data sparsity.

### 9. `recall_lifecycle` — per-recall lifecycle summary (ADR 0026)

`recall_lifecycle` is the consumer-facing rollup that feeds gold/API "is this recall active, when first/last seen, how churny" questions — one row per `recall_event` (driven from `ref('recall_event')`, so the grain and per-source identity match exactly: FDA event, NHTSA campno, USDA English, USCG announced-not-null). It is the summary tier of Phase 6c's fact-history (alongside `recall_event_history`, the field-level log); the dimension-history tier is the SCD-2 snapshots.

- **Two dimension tiers.** `first_seen_at` / `last_seen_at` / `edit_count` are **manifest-independent** — computed from bronze (`extraction_timestamp`, `count(distinct content_hash)`) for all five sources. `is_currently_active` / `was_ever_retracted` need the presence manifest (`extraction_run_identities`) and so come from the `track_presence` sources — **USDA + NHTSA** (C16); NULL for CPSC/FDA/USCG. NHTSA presence is **gated to full-enumerating runs** (`change_type='historical_seed'`), so it stays NULL until a deep-rescan/seed banks a complete manifest (WS-H).
- **Honest bounds.** Post-6a.5-reseed, bronze history was wiped, so `first_seen_at` is "first seen by *our pipeline* since the reseed" — a pipeline-observation metric, **not the recall's age** (use `recall_event.announced_at` for age). `edit_count` is the count of distinct content versions (1 = never changed); for the multi-row sources (FDA products, NHTSA lines) it counts version diversity across the event's child rows — a proxy for event activity. Never derived from `last_modified_date` (ADR 0026 Phase-5c addendum: unreliable per-edit signal).
- **NHTSA presence — enabled + gated (C16).** Presence ("is it still published") requires comparing each run's full identity set, which only the manifest captures — bronze can't substitute (content-hash dedup means an unchanged campno produces no recent bronze row, so "present-but-unchanged" is indistinguishable from "absent"). NHTSA *full-enumerates* **only on a deep-rescan/seed** (`change_type='historical_seed'`); its routine cron runs are incremental, so `_passing_records` (the manifest source) holds only changed campnos → a partial manifest. `recall_lifecycle` therefore **gates NHTSA presence to `historical_seed` runs**: until one banks a complete manifest (WS-H H-b), NHTSA presence is NULL — never wrong. The signal is **observed feed presence** (a campno dropped from / present in the source pull), not an authoritative agency retraction (interpreting "withdrawn on date X" is v2). CPSC/FDA/USCG stay off — FDA/USDA/USCG have a native status field, and CPSC has no status in its API at all (next bullet).
- **Native status vs. presence — and CPSC's permanent blind spot.** `recall_event.is_active` is the *cross-source* active signal (feeds `fct_recall_status`); three sources populate it from a **native lifecycle field**, no manifest needed: FDA `phase_txt` (Ongoing/Completed/Terminated, `recall_event.sql:133`), USDA `recall_type` (Active/PHA/Closed, `:207`), USCG `disposition` (open/closed, `:363`). So FDA and USCG do **not** need `track_presence` to answer "is this active" — presence would only add an orthogonal *delisting* signal. **CPSC and NHTSA carry no native status** (`is_active = NULL`, `:48`/`:287`), and they differ in fixability: NHTSA full-enumerates, so `track_presence` is its lever; **CPSC has no status field in the API at all** — the v1.4 programmer's guide returns only `RecallID/RecallNumber/RecallDate/Description/URL/Title/ConsumerContact/LastPublishDate` (+ collections), with no status/phase/closed field, and `RecallDateStart`/`RecallDateEnd` are *search parameters*, not returned data. So CPSC `is_active` is permanently NULL **from source**, not an extraction gap; `LastPublishDate` (→ `published_at`) is "last touched," not a lifecycle state (and per ADR 0028 doesn't reliably advance on edits).
- **304-safe + Finding-R-safe.** The presence dims key on the **latest enumerating run** (a 304-Not-Modified run succeeds but writes no manifest, so latest-success would read as empty) and `trim()` the manifest's `source_recall_id` (folding the ~5 whitespace-contaminated USDA ids — and the one pre-trim-fix run — into the canonical identity). `was_ever_retracted` = present in fewer enumerating runs than exist since first appearance (captures both mid-lifespan toggles and end retraction).
- **Feeds the SCD monitors (6c.3).** `recall_event_history`'s `classification` / `lifecycle_status` slices are the `severity=warn` dbt monitors `assert_classification_stable` / `assert_lifecycle_stable` (`dbt/tests/source_assumptions/`) — a returned row graduates that field's designation ASSUMED → MEASURED in `documentation/audit/scd_field_designations.md`. Reusing the model (rebaseline-excluded, noise-folded) is strictly better than re-deriving the monitors from raw bronze.

### 10. SCD-2 dimension snapshots — the dimension-history tier (ADR 0035)

The firm attribute sidecars carry SCD-2 history via dbt snapshots in the `silver_snapshots` schema (Policy C: the snapshot table *is* the queryable peer history; the dim is its `dbt_valid_to is null` current view). This is the **dimension** half of Phase 6c's history — distinct from the recall-fact half (`recall_event_history` / `recall_lifecycle`), and it is *materialized + stateful* (the snapshot can't be dropped/rebuilt without losing history) where the fact half is synthesized + stateless.

- `firm_uscg_attributes` ← `firm_uscg_attributes_snapshot` (anchor `mic`) — the only Type-2 **NEED** (MIC reassignment; shipped 6b.5).
- `firm_usda_attributes` ← `firm_usda_attributes_snapshot` (anchor `establishment_number`) and `firm_fda_attributes` ← `firm_fda_attributes_snapshot` (anchor `firm_fei_num`) — Type-2 **BENEFIT**, built in 6c.4 (portfolio breadth). Stable anchors, 0 edit-versions post-reseed → they bank one version per anchor now and grow forward.
- **Heartbeat exclusion is the load-bearing `check_cols` rule** (else every re-scan spawns phantom versions): USCG excludes `date_modified`/`in_business`, USDA establishments exclude `latest_mpi_active_date` (ADR 0032). Repointing a dim to its snapshot keeps the dim's column contract identical, so consumers are unaffected — the snapshot is a purely additive history layer. CPSC has no such dim (name-keyed firm, no stable structured anchor) → monitors only.

### 11. USCG MIC time-sensitivity refinements (6c.5, ADR 0035 §5)

The `recall_event_firm` USCG `match_confidence` started as a binary flag (6b.5): `uscg_mic_time_sensitive_unresolved` for any recalled MIC with a prior holder, else `uscg_mic_unambiguous`. 6c.5 refines it into three tiers (all dbt-SQL, lockstep-safe — `firm_id` recipe untouched):

- **rename downgrade** — `firm_uscg_attributes.mic_renamed_not_recycled` (every prior slot is a source `(previous name)` marker, none OOB) → `uscg_mic_unambiguous` (same manufacturer, renamed).
- **build-year resolution** — `uscg_mic_reassignment_years` parses `(OOB YYYY)` → `current_holder_since_year`; a recall whose `model_year` ≥ that was built during the current holder's tenure → `uscg_mic_build_date_resolved` (current attribution confirmed). Uses `model_year` (a recall field), not a HIN parse.
- **time-sensitive** — the residual (real OOB / unmarked-distinct prior, build year unknown or pre-reassignment).

Gate `probe_uscg_refinement_gates.sql` confirmed all three are marginal in volume (2 renames / 23 dated MICs / ~dozens resolved) — built for completeness; the correctness NEED was already met by the 6b.5 flag. Detail + gate numbers: ADR 0035 amendment 2026-06-06.

### 12. NHTSA `recall_product` v1.5 — product-grain SCD-2 (6c.6, ADR 0033 7-tuple)

NHTSA is the one source whose `recall_product` carries drift-prone attributes IN its v1 surrogate key (the ADR 0030 11-tuple), so a single editorial edit (Pierce `26V217000`: `mfr_comp_desc '' → 'Software'`) fragments one logical product into two rows. The v1.5 migration (ADR 0033, `project_scope/archive/silver_v15_migration_plan.md`) fixes this by demoting the drift-prone fields to a dbt snapshot (Type-1 current + Type-2 history) while keeping the structural fields in the anchor. 6c.6 builds it as a **parallel** prototype — v1 `recall_product` is untouched; the cutover is 6c.7.

- **The anchor is the 7-tuple, not the 6-tuple ADR 0033 first proposed.** Building against the full corpus (the Layer-2 gate) showed the 6-tuple collapsed `recall_product` 321,540 → 194,377 (−40%); `characterize_v15_collapse.sql` attributed **99.4% of that to *structural* `mfr_comp_ptno` variation** — the §G multi-part fan-out (Takata-class tire recall `24T014000` = 139 components × 139 part numbers; the Fortune Tormenta `LT235/75R15` one-part-→-many-component-IDs case), not temporal drift. `mfr_comp_ptno` is the structural part identity, so it returns to the anchor. Implemented anchor = `(campno, normalize_maketxt(maketxt), modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno)` — ADR 0030's original 7-tuple. Full rationale: ADR 0033 amendment 2026-06-06.
- **`check_cols` = only the genuinely-drift-prone fields**, widened for current-view freshness: `mfr_comp_desc`, `mfr_comp_name` (Pierce class) + `bgman`, `endman` (batch-window class) + `rcltype`, `potaff`, `mfgname`, `mfgtxt`, `fmvss` (so an isolated business-field edit doesn't go stale in the current view). The 7 anchor fields, `model_year` (functionally determined by `yeartxt`), and `extraction_timestamp` (a per-regen heartbeat) are deliberately excluded.
- **Models.** `stg_nhtsa_recalls_current` (distinct-on-7-tuple latest; single-homes the 7-tuple `recall_product_id`) → `nhtsa_recall_product_snapshot` (the SCD-2 table in `silver_snapshots`) → `recall_product_v15` (the 6c.6 current view, `dbt_valid_to is null`, in `recall_product`'s exact column shape — folded into `recall_product`'s NHTSA branch at the 6c.7 cutover and dropped) + `recall_product_history` (full versioned view + `is_current`, kept). Event-grain narrative history stays `recall_event_history`'s `LAG()` job (ADR 0022) — this snapshot is product-grain only, which resolves migration-plan Open Q#3 (the two history layers are complementary, not competing).
- **The accepted residual.** With the 7-tuple, v1.5 ≈ v1 — only 1,237 rows (0.38%) still collapse. All 1,237 are *simultaneous* (not temporal) variation on `mfr_comp_desc`/`mfr_comp_name` within a fixed (vehicle, part), and `modeltxt` is populated in 832/832 collapsing groups, so **no model or part coverage is lost** (both are anchor fields) — only secondary manufacturer-supplied description text collapses (latest-wins, full set retained in bronze). The residual is *irreducible*: `mfr_comp_desc` is the Pierce drift field, so anchoring it would re-break the fragmentation fix. Evidence: `characterize_v15_residual.sql`, `inspect_v15_residual_modeltxt.sql`.
- **Cutover (6c.7, done; ADR 0034).** `recall_product`'s NHTSA branch now selects the snapshot current view (the `recall_product_v15` SELECT folded in, re-keyed to the 7-tuple); `recall_product_v15` is dropped, `recall_product_history` kept, and `stg_nhtsa_recalls_current` gained a deterministic tiebreaker so snapshot idempotency is structural, not just empirical (the 832 collapsing 7-tuples share one seed timestamp → arbitrary pick without it). ADR 0033 → Accepted. The Layer-2 *observation window* was **waived** — on a re-seeded corpus with forward-only snapshots it cannot surface live drift; the full-corpus diff above + the 6c.8 simulated-drift test are the substituted evidence. Blast radius was `recall_product` + its own tests only (no gold/firm/event consumer of `recall_product_id`; no `firm.sql` lockstep).
- **Two identities, two layers (why both "11-tuple" and "7-tuple" appear in these docs).** *Bronze* identity stays the **11-tuple** (ADR 0030) — the audit grain that banks every distinct version, *including* edits to the demoted fields. *Silver* `recall_product_id` is the **7-tuple `md5`** (current, since the 6c.7 cutover). Both are authoritative, at different layers: the four drift-prone fields (`mfr_comp_desc`, `mfr_comp_name`, `bgman`, `endman`) remain in the 11-tuple **bronze** key but are demoted to SCD-2 *attributes* in **silver**, leaving the seven stable fields (`campno`, `normalize_maketxt(maketxt)`, `modeltxt`, `yeartxt`, `compname`, `rcl_cmpt_id`, `mfr_comp_ptno`) as the silver key — single-homed in `stg_nhtsa_recalls_current`. So "11-tuple" = bronze/staging dedup grain; "7-tuple" = silver product key. Confirmed in both code (`stg_nhtsa_recalls_current.sql`) and docs (`_snapshots.yml`, ADR 0031/0033/0034).
