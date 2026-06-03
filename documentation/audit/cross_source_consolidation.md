# Cross-source consolidation — the canonical silver schema map (W2)

- **Status:** Active — §0 naming decisions **resolved 2026-06-02** (D1–D7 confirmed; tables reflect the locked choices). One external follow-up: USCG `severity` semantics (D2) — the *column* decision is final; only the future `severity_rank` derive waits on the USCG-OII reply.
- **Purpose:** the SSOT the W4 silver-SQL edits trace to. One canonical column per concept, with each source's bronze field mapped onto it, so silver is cross-source *conformed* (one `firm` dim, one `recall_reason`, one `classification` domain) instead of per-source ad-hoc. Defines the canonical column list **once** so the W4 `UNION ALL` across 5 source branches has a parity checklist (§4 — the top correctness risk).
- **Single-home:** this doc owns the **semantic field → canonical-column map + the rename ledger + the deferral registers**. It does **not** restate per-source stats (those stay in each `field_audit_2026_w22.md`; cited by source), the shape matrices (`bronze_corpus_profile.md`), the enum value-sets (`bronze_corpus_profile.md` §4), or the SCD designations (`scd_field_designations.md` §3). The canonical-naming + SCD *decisions* fold into **ADR 0036** (this doc is its evidence base).
- **Methodology:** `documentation/audit/methodology.md`. Current silver: `dbt/models/silver/{recall_event,recall_product,firm,recall_event_firm}.sql`.

---

## §0. Naming decisions (resolved 2026-06-02)

Dn refs appear inline in §1–§2. All landed on the recommended default; D2/D4 carried clarifications.

| # | Decision | Resolution | Note |
|---|---|---|---|
| **D1** ✅ | `recall_reason` collision | narrative → **`recall_reason`** (the rename, all 5 sources); USDA's *structured* reason-enum → separate **`reason_category`** | — |
| **D2** ✅ | classification vs severity | **CONFORM** — USCG `severity` lands in **`classification`** source-native (`source` column disambiguates); per-source `accepted_values` `warn` | **Reversible at ~zero cost** (silver is rebuilt from bronze; splitting back out = SQL edit + `dbt build`, no migration). FDA `1/2/3`↔USDA `I/II/III` clean; `NC`/`PHA` non-graded outliers; **USCG `H/L/M/S` meanings unconfirmed** (USCG-OII email). `severity_rank` derive **deferred** until confirmed. |
| **D3** ✅ | `recall_product.type` (5 disjoint domains) | one **`type`** column, source-native, **per-source** accepted_values (no union test) | conform the *column*, not the *domain*. |
| **D4** ✅ | `recall_initiator` conform | **CONFORM** — FDA `voluntary_type` + NHTSA `influenced_by` co-live in **`recall_initiator`** source-native (**no value changes**) **+ additive derived `initiated_by ∈ {firm, agency}`** | **Only FDA + NHTSA** have the field (CPSC/USDA/USCG = NULL). The firm/agency flag is a *new* column; it never overwrites the raw values. |
| **D5** ✅ | `number_of_units` text vs int | **`number_of_units` TEXT** canonical **+ Tier-1 `unit_count` INTEGER** for NHTSA/USCG now; FDA/USDA/CPSC `unit_count` → Tier-2 | — |
| **D6** ✅ | firm sidecar dims | **Option A** — `firm_establishment_attributes` (USDA) + `firm_manufacturer_attributes` (USCG) kept **separate** | — |
| **D7** ✅ | `distribution_scope` cross-source | derive Tier-1 **`distribution_scope`** for **FDA *and* USDA**; structured `distribution_states[]` → Tier-2 | — |

---

## §1. `recall_event` canonical map

### §1a. Cross-source conformed columns (≥2 sources populate)
`bucket`: (a)=this PR · 6b=firm-resolution · (b)=capture-expansion · T2=Tier-2 enrichment.

| Canonical column | Type | CPSC | FDA | USDA | NHTSA | USCG | Bucket |
|---|---|---|---|---|---|---|---|
| `recall_reason` 🔵D1 | text | `description` | `product_short_reason_txt` *(Bug 1: was `distribution_area_summary_txt`)* | `summary` | `desc_defect` | `coalesce(problem_1,problem_2)` | (a) |
| `title` | text | `title` | synth (`recall_num — firm`) | `title` | synth (`campno — mfgname`) | synth (`company — model`) | (a) |
| `url` | text | `url` | — *(b: lookup)* | `recall_url` | — | `details_url` | (a) |
| `classification` 🔵D2 | text | — | `center_classification_type_txt` (1/2/3/NC) | `recall_classification` (Class I/II/III/PHA) | — | `severity` (H/L/M/S, `upper()`) | (a) |
| `status` / `lifecycle_status` | text | — | `phase_txt` | `recall_type` *(Bug-adjacent: was `active_notice`-derived)* | — *(drop the `do_not_drive`-derived hack)* | `disposition` (`lower()`) | (a) |
| `recall_initiator` 🔵D4 | text | — | `voluntary_type_txt` (normalize 2 variants) | — | `influenced_by` | — | (a) |
| `initiated_by` (derive) 🔵D4 | text | — | derive `firm`/`agency` | — | derive `firm`/`agency` | — | (a) |
| `announced_at` | timestamptz | `recall_date` | `recall_initiation_dt` | `recall_date` | `rcdate` | `case_open_date`/`opened_on` (1970→NULL) | (a) |
| `published_at` | timestamptz | `last_publish_date` | `coalesce(event_lmd,…)` | `coalesce(last_modified,recall_date)` | `coalesce(datea,rcdate)` | `coalesce(last_date,announced_at)` | (a) |
| `terminated_at` | timestamptz | — | `termination_dt` | `closed_date` | — | `case_close_date` | (a) |
| `distribution_scope` 🔵D7 | text (derive) | — | derive from `distribution_area_summary_txt` | derive from `states` | — | — | (a) |

### §1b. Single-source lift columns (1 source; NULL elsewhere)
These all currently sit in `source_payload_raw`; W4 promotes them. Cross-source schema parity = every column NULL-cast in the other 4 branches (§4).

| Canonical column | Type | Source | Bronze field | Bucket |
|---|---|---|---|---|
| `hazards` | jsonb | CPSC | `hazards` | (a) |
| `remedies` | jsonb | CPSC | `remedies` | (a) |
| `remedy_options` | jsonb/text[] | CPSC | `remedy_options` | (a) |
| `injuries` | jsonb | CPSC | `injuries` | (a) |
| `images` | jsonb | CPSC | `images` | (a) |
| `consumer_contact` | text | CPSC | `consumer_contact` | (a) |
| `manufacturer_countries` | jsonb/text[] | CPSC | `manufacturer_countries` | (a) |
| `coordinated_recall_urls` | jsonb/text[] | CPSC | `in_conjunctions` | (a) |
| `sales_channel_narrative` | jsonb | CPSC | `retailers[]` *(§6 Option B — out of firm dim)* | (a) |
| `product_upcs` | jsonb | CPSC | `product_upcs` *(recall-level, not per-product)* | (a) |
| `distribution_area_summary` | text | FDA | `distribution_area_summary_txt` *(Bug 1's other half — own column)* | (a) |
| `notification_method` | text | FDA | `initial_firm_notification_txt` | (a) |
| `reason_category` 🔵D1 | text | USDA | `recall_reason` (exploded tokens) | (a) |
| `risk_level` | text (derive) | USDA | derive 1:1 from `recall_classification` | (a) |
| `distribution_states` | text | USDA | `states` *(structured `[]` → T2)* | (a) |
| `related_to_outbreak` | bool | USDA | `related_to_outbreak` | (a) |
| `archived` | bool | USDA | `archive_recall` | (a) |
| `firm_contact_block_text` | text | USDA | `company_media_contact` | (a) |
| `corrective_action` | text | NHTSA | `corrective_action` | (a) |
| `consequence_of_defect` | text | NHTSA | `conequence_defect` *(typo-fixed at silver)* | (a) |
| `notes` | text | NHTSA | `notes` | (a) |
| `mfgcampno` | text | NHTSA | `mfgcampno` | (a) |
| `fmvss` | text | NHTSA | `fmvss` | (a) |
| `do_not_drive` | bool | NHTSA | `do_not_drive` | (a) |
| `park_outside` | bool | NHTSA | `park_outside` | (a) |
| `owner_notified_at` | timestamptz | NHTSA | `odate` (1901→NULL) | (a) |
| `disposition` | text | USCG | `disposition` (`lower()`) alongside synth `status` | (a) |
| `campaign_started_at` | timestamptz | USCG | `campaign_open_date` | (a) |
| `campaign_ended_at` | timestamptz | USCG | `campaign_close_date` | (a) |
| `last_editorial_date` | timestamptz | USCG | `last_date` | (a) |
| `firm_contact_person` | text | USCG | `company_official` *(low priority)* | (a) |
| `source_payload_raw` | jsonb | all | residual unmapped fields | (a) |

---

## §2. `recall_product` canonical map

### §2a. Cross-source conformed columns
| Canonical column | Type | CPSC | FDA | USDA | NHTSA | USCG | Bucket |
|---|---|---|---|---|---|---|---|
| `product_name` | text | `products[].name` | `product_description_txt` *(Bug 2: prefers `productdescriptionshort`)* | `title` | `compname` | `model_name` | (a); FDA name→(b) |
| `product_description` | text | `products[].description` (100% empty) | `product_description_txt` *(Bug 3: was `product_short_reason_txt`)* | `product_items` | `mfr_comp_desc` | `coalesce(problem_1,problem_2)` | (a) |
| `model` | text | `products[].model` (100% empty) | — | — | `modeltxt` | **NULL** *(Bug 1: was `model_name` dup)* | (a) |
| `type` 🔵D3 | text | `products[].type` | `product_type_short` | `processing` *(Bug 1: was `recall_type`; multi-value)* | `rcltype` *(Bug 1: was NULL)* | `boat_type` (numeric code) | (a) |
| `category_id` | text | `products[].category_id` | — | — | — | — | (a) |
| `number_of_units` 🔵D5 | text | `products[].number_of_units` | `product_distributed_quantity` | `qty_recovered` | `potaff` | `units` | (a) |
| `unit_count` 🔵D5 | int (derive) | — *(T2)* | — *(T2)* | — *(T2)* | `potaff::int` | `units::int` | (a) |

### §2b. Single-source product lift columns
| Canonical column | Type | Source | Bronze field | Bucket |
|---|---|---|---|---|
| `hin` | text | USCG | `hin` (`'N/A'`→NULL) — the UPC analog | (a) |
| `model_year` | text | USCG | `model_year` (`9999`→NULL) | (a) |
| `label_artifact_name` | text | USDA | `labels` (PDF filename) | (a) |
| `distribution_list_artifact_name` | text | USDA | `distro_list` (PDF filename) | (a) |
| `upc` | text | — | **deferred** — cross-source `upc`/`hin`/`vin` merge → (b)/6b | (b) |
| `source_specific_attrs` | jsonb | all | residual | (a) |

---

## §3. `firm` / `recall_event_firm` / attribute dims

The **conformed dimension** of the whole schema. Keyed on `normalized_name = upper(trim(raw_name))`; `observed_company_ids` JSONB rolls up the structured IDs across sources.

| Aspect | Decision |
|---|---|
| **`role` domain** | `{establishment, manufacturer, importer, distributor, filer}` — **remove `retailer`** (CPSC Option B), **add `filer`** (NHTSA split) |
| **CPSC** | M/I/D arrays → firm dim; **`retailers[]` removed** → `recall_event.sales_channel_narrative` (Option B, −44.2% of CPSC firm rows). Suffix/DBA fragmentation (Bug 2) → **6b**. `company_id` always empty. |
| **FDA** | `firm_legal_nam` → `raw_name`, role `establishment`; `company_id = firm_fei_num`. Firm-address fields → **(b)**. |
| **USDA** | `establishment` → `raw_name`, role `establishment`; `company_id = establishment_number` (join, 100% unique key). Sidecar **`firm_establishment_attributes`** (Option A). |
| **NHTSA** | **filer/manufacturer split** — emit 2 rows: `mfgname` role `filer` + `mfgtxt` role `manufacturer` (95.9% disjoint when differing). `company_id = NULL`. |
| **USCG** | `coalesce(directory.company_name, recalls.company_name, mic)` → `raw_name`, role `manufacturer`; `company_id = mic`. Sidecar **`firm_manufacturer_attributes`** (Option A). **`mic` is a Type-2-NEED SCD anchor** (reassignment) — see §8. |
| **`observed_company_ids`** | union of FDA FEI + USDA establishment_number + USCG MIC (3 namespaces). CPSC/NHTSA contribute none. |
| **Deferred** | all RapidFuzz / suffix-strip / DBA / `alternate_names` / per-recall disambiguation → **6b**. Keep `firm.sql`/`recall_event_firm.sql` clean for 6b; no fuzzy matching in (a). |

---

## §4. Canonical column checklist (UNION-parity — the top W4 risk)

The silver models `UNION ALL` 5 source branches; **every canonical column must appear in all 5** (source value or `cast(null as <type>)`). The §1/§2 column lists are that checklist — W4 fills each branch against them. Verify column count + type parity per branch before each commit (`dbt build --select <model>+`).

---

## §5. Rename ledger

| Old (current silver) | New (canonical) | Why | Scope |
|---|---|---|---|
| `recall_event.description` | **`recall_reason`** 🔵D1 | post-Bug-1 it's the defect/reason narrative for all 5 sources | grep `dbt/models/gold/` (believed empty — confirm at W4) + `_silver.yml` + tests before renaming |
| `conequence_defect` (NHTSA) | `consequence_of_defect` | source-side typo fixed at silver | (a) |
| `recall_event_firm.role` | +`filer`, −`retailer` | NHTSA split + CPSC Option B | update `accepted_values` |
| *(new)* USDA `recall_reason` enum | **`reason_category`** 🔵D1 | avoid collision with the narrative `recall_reason` | (a) |

---

## §6. Null-handling / documented-empty

Population rates: **`bronze_corpus_profile.md` §3** (do not restate). Highlights for the silver NOT-NULL contract:
- **near-NOT NULL (warn-tripwire):** `recall_reason` (FDA 0.1% / USDA 1.2% / CPSC 0% / NHTSA 2.6% / USCG ~6% empty).
- **NOT NULL:** `announced_at`, `classification` (where the source has it), `product_name` (CPSC warn at 3.3%).
- **`''`-sentinel sources** (FDA/USDA/NHTSA): staging `nullif(col,'')`; **NULL+named-sentinel sources** (CPSC/USCG): `nullif` + strip `N/A`/`9999`/`1970-01-01`/`UNK`/`-`/`0`≡`00`.
- **documented-empty-by-source** (keep silent-blank, note in `_silver.yml`): CPSC `products[].description`/`.model`; FDA `firmline2adr`; USDA `press_release`; NHTSA `rpno` (dropped).

---

## §7. Deferred registers

**→ 6b (firm resolution):** CPSC suffix/DBA name-cleaning (Bug 2), NHTSA suffix-strip, USCG name normalization, RapidFuzz, `firm.alternate_names`, USDA recall→establishment disambiguation (Signal 1 = `product_items` establishment-number; note: split the 67% `+`-joined multi-grant composites), FDA `firmsurvivingnam/fei` continuity.

**→ (b) capture-expansion** (`capture_expansion_backlog.md`): FDA Tier-1 firm address (`firmcitynam`/`firmstatecd`/…) + survivors + `postedinternetdt`; Tier-2 `codeinformation`; Tier-3 press releases; FDA `productdescriptionshort` (Bug 2 `product_name`); cross-source product-identifier merge (`upc`/`hin`/`vin` → one column).

**→ Tier-2 enrichment** (`freetext-enrichment-backlog.md`): FDA quantity → `quantity_value`/`unit`; FDA+USDA distribution → `distribution_states[]`; USDA comma-enums → `text[]`; USDA `product_items` parse; `unit_count` for the free-text quantity sources.

---

## §8. SCD verdict

Full per-field designations + monitors: **`scd_field_designations.md`**. Per-source shape: `bronze_corpus_profile.md` §5. Summary for ADR 0035/0036:
- **2 measured anchors:** NHTSA 11-tuple (Type-2, ADR 0033, 0 core drift) · USCG `mic` (Type-2-NEED, reassignment — monitor-confirmed 205 OOB-recycled of 718 recalled MICs).
- **3 snapshot-hypotheses** (FDA/CPSC/USDA): NEED low (stable keys, 0 edit-versions); revisit when incrementals re-bank history.
- **Type-2-BENEFIT, monitors seeded (measure-forward):** `classification`/`severity` + `lifecycle_status` (amendments suspected, unmeasured).
- **Type-1 (+ bronze audit trail):** `recall_reason` narrative + firm `normalized_name` (corrections; fragmentation → 6b normalization, not SCD).

---

*Drives: W4 silver SQL. Cited by: ADR 0036 (canonical naming + SCD verdict). Evidence: the five `field_audit_2026_w22.md`, `bronze_corpus_profile.md`, `scd_field_designations.md`.*
