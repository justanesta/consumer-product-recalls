# Silver Design Notes — Bronze-to-Silver Mapping

**Scope:** the design decisions behind unifying all five sources (CPSC, FDA, USDA, NHTSA, USCG) from bronze into the shared silver schema. This doc owns the *patterns* — per-source staging dedup, event/product grain, surrogate-key recipes, UNION-parity null-handling, and the value-normalization discipline. It does **not** restate the semantic field→canonical-column **map**: that is single-homed in `documentation/audit/cross_source_consolidation.md` (the W2 SSOT — §1 `recall_event`, §2 `recall_product`, §3 firm/bridge/sidecars, §5 rename ledger, §6 null-handling). For full-corpus shape/enum evidence see `documentation/audit/bronze_corpus_profile.md`; for system architecture `documentation/architecture.md`; for the canonical schema reference `documentation/data_schemas.md`.

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
| **NHTSA** | `campno` — `DISTINCT ON` collapses the 11-tuple rows to one header | the 11-tuple (`md5`) per ADR 0031 | the 11-tuple identity (ADR 0030) |
| **USCG** | `Number` — one bronze row per recall | one per recall (= `recall_event_id`) | `source_recall_id` |

Two sources need the extra event-level collapse because their bronze grain is *finer* than a recall event: FDA bronze is one row per product, NHTSA bronze is one row per vehicle × component × part × batch. `DISTINCT ON (<event key>) ORDER BY <event key>, extraction_timestamp DESC` picks a representative row; this is safe because event-level fields (`firm_legal_nam`, `phase_txt`; `desc_defect`, `corrective_action`) are identical across all the finer rows sharing an event key. USDA's dedup additionally drops the Spanish bilingual sibling (EN-primary); USCG and CPSC are already one-row-per-recall and only need edit-history resolution.

### 2. Flat vs. exploded product rows

Three shapes feed `recall_product`:

- **Exploded array (CPSC)** — products live as a JSONB array on the event row; `recall_product.sql` uses `lateral jsonb_array_elements() with ordinality`, and the surrogate key includes the **ordinal** because product names are not unique within a recall.
- **Already flat (FDA, NHTSA)** — each bronze row *is* a product instance, so the branch reads staging directly. FDA keys on `PRODUCTID` (a globally unique API sequence); NHTSA keys on the `md5` of its 11-tuple (`bgman`/`endman` act as the batch-level disambiguator, the analog of CPSC's ordinal).
- **One-product-per-recall (USDA, USCG)** — these sources name a single product blob / boat model per recall, so `recall_product_id = recall_event_id`. This preserves referential integrity (every event has ≥1 product) without inventing structure the source doesn't provide; USDA's `product_items` free-text stays unparsed (ADR 0002 defers structured parsing — the raw lives in `source_specific_attrs.product_items_raw`).

### 3. The conformed `firm` dimension + per-source attribute sidecars

`firm` is the **conformed dimension** — keyed on `normalized_name = upper(trim(name))`, shared across all five sources, and the only firm object the fact bridge (`recall_event_firm`) joins to (on `firm_id`). Matching on normalized name gives implicit cross-source dedup: a firm appearing in several sources collapses to one row with all its structured IDs gathered in `observed_company_ids`. `firm.sql` and `recall_event_firm.sql` must stay in **lockstep** (identical `firm_id` recipes per branch) — the relationships test catches any divergence as orphan `firm_id`s.

Role model per source:

| Source | Roles emitted | Notes |
|---|---|---|
| **CPSC** | `manufacturer`, `importer`, `distributor` | **`retailer` removed (Option B)** — retailer names live in `recall_event.sales_channel_narrative`, not the firm graph (−44.2% of CPSC firm rows; suffix/DBA cleanup is 6b). |
| **FDA** | `establishment` | `firm_legal_nam` is the recalling FDA-registered establishment (analogous to USDA), not a manufacturer; `company_id = firm_fei_num`. |
| **USDA** | `establishment` | `company_id = establishment_number` via the HTML-decoded name join (~97% per-name); rich metadata → `firm_establishment_attributes` sidecar. |
| **NHTSA** | `filer`, `manufacturer` | **filer/manufacturer split** — two bridge rows: `mfgname` = `filer` (filed the recall), `mfgtxt` = `manufacturer` (made the product); 95.9% disjoint when differing. `company_id = NULL`. |
| **USCG** | `manufacturer` | directory-enriched name `coalesce(directory.company_name, recalls.company_name, mic)`; `company_id = mic`; rich metadata → `firm_manufacturer_attributes` sidecar. |

**Sidecar (supertype/subtype) pattern:** USDA and USCG each publish a *separate structured firm registry* (FSIS Establishment Listing; USCG Manufacturer Directory), so their rich, non-conforming attributes (FSIS `grant_date`/`status_regulated_est`/`size`/`district`; USCG `detail_url`/`uscg_directory_id`) live in dedicated single-source dims keyed on the source's structured ID (`establishment_number`, `mic`) rather than bloating the conformed `firm` with 60–80% NULLs. CPSC/NHTSA expose only inline names (no registry → no sidecar); FDA has an FEI but its address/registry fields are deferred to (b). Facts join only the conformed `firm`; the sidecars are an opt-in dimension-to-dimension join via `observed_company_ids`.

### 4. Surrogate key strategy

All silver surrogate keys are deterministic `md5`, source-prefixed for global uniqueness — except `firm_id`, which is deliberately source-agnostic to enable cross-source dedup:

- `recall_event_id` → `md5('<SOURCE>' || '|' || <event business key>)`
- `recall_product_id` → `md5('<SOURCE>' || '|' || <product business key>)` (CPSC adds the ordinal; NHTSA hashes the 11-tuple)
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
