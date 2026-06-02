# Bronze corpus profile — cross-source shape evidence

- **Status:** Active (append-only) — started 2026-06-02. **FDA profiled; CPSC / USDA recalls / USDA establishments / NHTSA / USCG (×3) pending** (W1 waves 2–3).
- **Scope:** the empirical *shape* of the full-corpus bronze — per-column population, cardinality, enum domains, length, grain/business-key, and identity-fragmentation — across all eight bronze tables. This is the evidence base the silver remap (`project_scope/silver-field-remap-plan.md`) builds its NOT NULL / `accepted_values` / length-sizing / SCD-grain decisions on.
- **Single-home boundary:** this doc owns the **cross-source shape matrices + the `accepted_values` value-set catalogue**. It does **not** own the semantic field→canonical-column *mapping* (that is `cross_source_consolidation.md`) nor the per-source narrative findings (those stay in each `documentation/<source>/field_audit_2026_w22.md`; this doc points at them for per-value counts rather than restating).
- **Methodology:** `documentation/audit/methodology.md`. Query files: `scripts/sql/<source>/<layer>/*.sql`.

> **CAVEAT (load-bearing — see `methodology.md` line ~29).** This is population / shape / enum / fragmentation **evidence for silver build decisions**. It is **not** a re-derivation of which fields to capture — that field-selection audit is Phase 6a, done (the eight `field_audit_2026_w22.md` docs). A field that is **empty in bronze describes the capture state, never a recommendation to stop capturing it.**

---

## 1. Corpus snapshot

| Source | Bronze table | Rows | Distinct business key | Seed provenance | Profiled |
|---|---|---|---|---|---|
| FDA | `fda_recalls_bronze` | 134,461 | ~134,450 products / 50,509 events | Phase 6a.5 full-corpus seed (re-seed 2026-06-02) | 2026-06-02 |
| CPSC | `cpsc_recalls_bronze` | — | — | seeded | pending |
| USDA recalls | `usda_fsis_recalls_bronze` | — | — | full snapshot each fetch | pending |
| USDA establishments | `usda_fsis_establishments_bronze` | — | — | full snapshot each fetch | pending |
| NHTSA | `nhtsa_recalls_bronze` | — | — | PRE+POST_2010 archives | pending |
| USCG recalls | `uscg_recalls_bronze` | — | — | full corpus (1,763) | pending |
| USCG manufacturers | `uscg_manufacturers_bronze` | — | — | directory listing | pending |
| USCG mfr details | `uscg_manufacturer_details_bronze` | — | — | detail pages | pending |

## 2. Grain & business-key matrix

| Source | Declared grain | Business key | Rows | Distinct key | Edit-versions (rows − distinct) | Notes |
|---|---|---|---|---|---|---|
| FDA | one row per **product** | `source_recall_id` (PRODUCTID) | 134,461 | ~134,450 | ~11 | aggregates to 50,509 events (`recall_event` DISTINCT ON event_id); max fan-out 470 products/event (event 70452). Near-1:1 product:row in the fresh seed — see §5 caveat. |
| (others pending) | | | | | | |

## 3. Population matrix — silver-relevant columns (corpus empty/NULL %)

`''` is counted as empty (FDA preserves both `null` and `''`; silver staging normalizes via `nullif`). Counts live in the per-source audit; this is the at-a-glance NOT-NULL driver.

### FDA → `recall_event`
| Silver column | FDA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `recall_reason` | `product_short_reason_txt` | 0.1% | near-NOT NULL (warn-tripwire) |
| `distribution_area_summary` | `distribution_area_summary_txt` | 0.1% | nullable |
| `classification` | `center_classification_type_txt` | 0.0% | NOT NULL |
| `status` | `phase_txt` | 0.0% | NOT NULL |
| `recall_initiator` | `voluntary_type_txt` (normalized) | ~0% (9 rows) | nullable |
| `notification_method` | `initial_firm_notification_txt` | 25.8% | nullable |
| `announced_at` | `recall_initiation_dt` | 0.0% | NOT NULL |
| `published_at` | `event_lmd` | ~0.15% (197) | nullable (archive tail; see §5) |
| `terminated_at` | `termination_dt` | 17.7% | nullable |

### FDA → `recall_product` / firm
| Silver column | FDA bronze source | Empty % | Silver nullability |
|---|---|---|---|
| `product_name` | `product_description_txt` | 0.0% | NOT NULL |
| `product_description` | `product_description_txt` | 0.0% | NOT NULL |
| `number_of_units` | `product_distributed_quantity` | 8.1% | nullable, **TEXT** (free-text) |
| firm name | `firm_legal_nam` | 0.0% | NOT NULL (avg len 25, max 102) |
| firm identifier | `firm_fei_num` | 0.1% | nullable |

## 4. Enum-domain catalogue — the `accepted_values` SSOT

Value **sets** for the dbt `accepted_values` tests (cross-source union per canonical column once consolidation runs). Per-value **counts** live in each source's `field_audit_2026_w22.md` §8.

| Canonical silver column | Source | Value set | Test posture |
|---|---|---|---|
| `recall_event.classification` | FDA | `1`, `2`, `3`, `NC` | error |
| `recall_event.status` | FDA | `Terminated`, `Ongoing`, `Completed` | error |
| `recall_event.recall_initiator` | FDA (post-normalize) | `Firm Initiated`, `FDA Requested`, `FDA Mandated` | warn |
| `recall_event.notification_method` | FDA | `Letter`, `Combination`, `Telephone`, `E-Mail`, `Press Release`, `FAX`, `Other`, `Visit` | **warn** (corpus surfaced FAX + Visit beyond the audit's 6-value assumption) |
| `recall_product.type` | FDA (`product_type_short`) | `Food`, `Devices`, `Drugs`, `Biologics`, `Veterinary`, `Cosmetics`, `Tobacco`, `Food and Cosmetics` | warn |
| *(cross-source severity/classification alignment — FDA 1/2/3/NC vs USDA Class I/II/III vs USCG severity)* | — | pending consolidation | — |

**Methodology note proven here:** the 447-record sample reported `voluntary_type_txt` = 2 values and `initial_firm_notification_txt` = 6 values; the full corpus surfaced **`FDA Requested`/`FDA Mandated`** and **`FAX`/`Visit`** respectively. Hardcoding `accepted_values` from the sample would have produced false-failing tests — the catalogue must come from corpus profiling.

## 5. Identity-fragmentation summary (SCD-applicability input — W3)

| Source | Edit-versions observed | NEED (fragmentation) | BENEFIT (attribute history) | Status |
|---|---|---|---|---|
| FDA | ~11 over 134,450 products in the fresh seed → near-1:1 | **low signal** | TBD (phase/classification edits over time) | hypothesis — **not yet measured for long-term edit-rate** |
| (others pending) | | | | |

> **FDA caveat (observation vs inference):** a single-shot seed shows near-1:1 product:row *now*, but cannot reveal how often the same PRODUCTID is re-extracted with changed content over time. The SCD-NEED verdict must weigh `scripts/sql/fda/bronze/assert_productid_stable.sql` + the daily-incremental history, not this snapshot alone. Recorded as a hypothesis, per the distinguish-inference-from-observation discipline.

## 6. Cross-source relationship / cardinality notes

- FDA recall→product fan-out: 1→N, mean ~2.66, max 470 (event 70452).
- (others pending)

## 7. Free-text normalization scoping (feeds the Tier model + the enrichment backlog)

FDA, 2026-06-02 (`scripts/sql/fda/bronze/profile_freetext_normalization.sql`):

- **`product_distributed_quantity` → `recall_product.number_of_units`:** 66% cleanly numeric (9.4% pure integer + 56.6% integer+unit); 20.9% messy (weights `total pounds`, multi-figure `(globally);(US)`, cross-product totals — same shape as USDA `qty_recovered`); 2.4% sentinel; 8.1% empty; 56,967 distinct normalized forms. **Decision:** Tier-0 cleanup (sentinel→NULL + whitespace/CR collapse) in staging, keep **TEXT**. Tier-2 `quantity_value`+`quantity_unit` parse → deferred (`project_scope/freetext-enrichment-backlog.md`).
- **`distribution_area_summary_txt` → `distribution_area_summary` + derived `distribution_scope`:** 31.4% Nationwide + 15.9% Worldwide/all-states = 47% national/intl; 33% regional (17.4% single-region + 15.4% state-code lists); 20% narrative/mid; **negation negligible (4 rows)**; 9,800 distinct surface forms collapse into the Nationwide flag (57,070 rows); embedded `\r` confirmed. **Decision:** Tier-0 cleanup + **Tier-1 silver derive** `distribution_scope ∈ {Nationwide, International, Regional, Unspecified}` — **no negation guard** (data shows ~0 true negations; a guard misclassifies real-nationwide rows, e.g. "No product was distributed to government accounts. The product was distributed nationwide"). Tier-2 `distribution_states[]`/`distribution_countries[]` → deferred (enrichment backlog).

---

*Per-source detail: `documentation/<source>/field_audit_2026_w22.md`. Semantic mapping: `documentation/audit/cross_source_consolidation.md` (W2). SCD verdict: this doc §5 → ADR 0035.*
