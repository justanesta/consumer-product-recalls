# 0033 — Silver row versioning via SCD on stable anchor (NHTSA `recall_product_id` migration to 6-tuple + Type 2 snapshot)

- **Status:** Proposed (2026-05-15)
- **Date:** 2026-05-15
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0031 (concrete forward path for silver-grain after the 9-tuple migration sunset of 2026-05-15); ADR 0030 (bronze 11-tuple identity unchanged — bronze stays audit-quality at the leaf grain; this ADR addresses the silver layer only); ADR 0022 (event-grain `recall_event_history` via `LAG()` over snapshots is unchanged; this ADR adds a parallel product-grain history layer via dbt snapshot); ADR 0007 (extends with an explicit Type 2 history-table mechanism at the silver product grain, alongside the existing bronze content-hash snapshot store)
- **Companion documents:** `project_scope/silver_v15_migration_plan.md` (3-layer rollout plan with decision gates); `documentation/nhtsa/incremental_delta_findings.md` Section K (Pierce event empirical record)

## Context

### What prompted this ADR

The 2026-05-15 Pierce ARROW XT family `26V217000` `mfr_comp_desc` empty→`'Software'` population event added **96 fragmented silver `recall_product` rows in a single editorial action** (full evidence trail in `incremental_delta_findings.md` Section K). This is the first surfaced instance of a real_drift class that affects a non-batch-window field — and it materially refuted the premise underpinning ADR 0031's 9-tuple silver-grain migration evaluation ("every observed real_drift case lives in batch-window fields"). The 9-tuple migration is sunset (per ADR 0031's "Re-baseline 2026-05-15" subsection + Stop criterion #1 firing).

The Pierce event also stress-tested ADR 0031's "v1 silver will have ~150 fragmented NHTSA rows per year" extrapolation. A single editorial event added 96 in one day. The estimate is materially stale, and downstream consumer queries against `recall_product` for Pierce return 192 rows instead of the semantically-correct 96 — a 2× overcount on one campaign that compounds as similar events recur.

### What was already decided

- **ADR 0030 — NHTSA bronze identity:** 11-tuple composite identity, row-unique on bronze. Bronze remains audit-quality and preserves every distinct content snapshot per ADR 0027. **Unchanged by this ADR.**
- **ADR 0022 — recall_event_history:** uniform `LAG()`-based history synthesis over bronze snapshots at the `(source, source_recall_id)` grain — the recall-event level. **Unchanged by this ADR.**
- **ADR 0031 — silver-row fragmentation strategy:** three-tier framework (Tier 1 prevention, Tier 2 detection, Tier 3 reconciliation); current NHTSA `recall_product_id = md5(11-tuple)`; documented v1 fragmentation acceptance; 9-tuple migration evaluation sunset 2026-05-15. **This ADR proposes a different forward path than the sunset 9-tuple migration.**
- **ADR 0007 — bronze snapshot lineage:** content-hash dedup, snapshot store as primary lineage mechanism. **Unchanged by this ADR; this ADR adds a separate Type 2 mechanism at the silver product grain.**

### The architectural question

What surrogate-key recipe and history mechanism should silver `recall_product` use for NHTSA, given that:

- Bronze faithfully captures every distinct 11-tuple version (per ADR 0030);
- Some 11-tuple fields are demonstrably drift-prone (Pierce showed `mfr_comp_desc`; prior events showed `bgman`/`endman`; AC DELCO showed `maketxt` once at TSV substrate);
- Consumer queries against silver should return one row per logical product, not one row per field-version pair;
- Audit history of field-level changes is a Phase 6 deliverable per ADR 0022 and downstream needs (`recall_event_history`, future Phase 8 BI/API).

The deeper realization: ADR 0031's md5(11-tuple) treats every field of the bronze identity as part of the silver surrogate key (effectively SCD Type 0 — never change, even when they do). This is the data-warehousing pattern that the **Slowly Changing Dimension (SCD)** taxonomy was designed to address. Industry-standard SCD frames the problem as a two-axis decomposition:

1. **Which fields anchor the logical entity (go in the surrogate key)** vs which fields describe its current state (latest-wins attributes)?
2. **How is field-version history materialized** — stateless query-time derivation (e.g., `LAG()` over raw snapshots) or stateful Type 2 history table (e.g., dbt snapshot)?

The 11-tuple recipe conflated the first axis. The 9-tuple migration tried to make a different cut but on the wrong premise (predicted stability of those fields, rather than key/attribute role). The 6-tuple recipe proposed below cuts on the right axis.

## Decision

**Silver `recall_product` for NHTSA migrates to a 6-tuple stable-anchor surrogate key with Type 1 latest-wins attribute updates, backed by a dbt snapshot table providing Type 2 product-grain history. Migration is phased over three layers with explicit decision gates per the companion `silver_v15_migration_plan.md`.**

This ADR records the *architectural decision*. The companion plan doc records the *execution phasing*. Both ship together (Layer 1 of the migration plan).

### The 6-tuple stable anchor

```python
# Silver `recall_product_id` recipe (proposed; replaces ADR 0031:84's 11-tuple)
md5(
    'NHTSA' || '|' || campno
        || '|' || coalesce(maketxt, '')
        || '|' || coalesce(modeltxt, '')
        || '|' || coalesce(yeartxt, '')
        || '|' || coalesce(compname, '')
        || '|' || coalesce(rcl_cmpt_id, '')
)
```

The 6 anchor fields name the *logical product identity*: which recall campaign, on which make+model+year+component, identified by which NHTSA-assigned per-(component, recall) ID. Conceptually: "this is *which* product" rather than "this is *what* the product currently looks like."

**Empirical drift profile (basis for choosing these 6 fields):**

| Field | Drift events observed | Substrate |
|---|---|---|
| `campno` | 0 | Bronze + TSV substrate (NHTSA never renumbers public recall IDs by policy) |
| `maketxt` | 1 (AC DELCO → ACDELCO, 2026-05-08) | TSV cross-corpus only; never on bronze substrate |
| `modeltxt` | 0 | Bronze + TSV substrate |
| `yeartxt` | 0 | Bronze + TSV substrate |
| `compname` | 0 | Bronze + TSV substrate |
| `rcl_cmpt_id` | 0 | Bronze cross-regen (per ADR 0030 `investigate_tire_collision.sql` Q2) |

Compare to the 5 attribute fields demoted from the key:

| Field | Drift events observed |
|---|---|
| `mfr_comp_ptno` | 0 real_drift (137 structural multi-batch as of 2026-05-15) |
| `mfr_comp_desc` | **96 real_drift** (Pierce 2026-05-15) |
| `mfr_comp_name` | 0 real_drift |
| `bgman` | 7 real_drift (Pacifica + Nissan + Mack) |
| `endman` | 4 real_drift (Western Star + Nissan + Mack) |

The 6-tuple is empirically near-zero-drift. The 5-tuple attribute set absorbs all observed real_drift classes except the AC DELCO normalization edge case.

### Pre-2008 edge case

`rcl_cmpt_id` was added 2008-03-14 per Finding F. Pre-2008 records have NULL `rcl_cmpt_id`. The `coalesce(rcl_cmpt_id, '')` in the recipe handles this — pre-2008 records key off the 5-tuple `(campno, maketxt, modeltxt, yeartxt, compname)`, which is empirically sufficient (no observed pre-2008 collisions, per the lower per-decade row counts in Finding H Decade distribution).

A defensive dbt test should assert: for any pre-2008 record (`rcdate < '2008-03-14'`), the 5-tuple is row-unique within the cohort. This locks in the assumption.

### Type 1 latest-wins for attributes

The 5 demoted fields (`mfr_comp_ptno`, `mfr_comp_desc`, `mfr_comp_name`, `bgman`, `endman`) become **Type 1 SCD attributes** on `silver/recall_product`: the silver row reflects the latest observed value per (6-tuple stable anchor, attribute) from the most recent bronze extraction. Stale versions are not exposed in `silver/recall_product`'s current-state view.

Implementation: `stg_nhtsa_recalls_current` uses `DISTINCT ON (6-tuple) ORDER BY 6-tuple, extraction_timestamp DESC` to pick the latest bronze row per logical product. Existing 11-tuple-grain rows are correctly collapsed because the most-recent extraction's row for each 6-tuple is selected.

### Type 2 product-grain history via dbt snapshot

```sql
-- dbt/snapshots/nhtsa_recall_product_snapshot.sql
{% snapshot nhtsa_recall_product_snapshot %}
{{
  config(
    target_schema='silver_snapshots',
    unique_key="md5('NHTSA' || '|' || campno || '|' || coalesce(maketxt,'') || '|' || coalesce(modeltxt,'') || '|' || coalesce(yeartxt,'') || '|' || coalesce(compname,'') || '|' || coalesce(rcl_cmpt_id,''))",
    strategy='check',
    check_cols=['mfr_comp_ptno', 'mfr_comp_desc', 'mfr_comp_name', 'bgman', 'endman'],
  )
}}

select
    -- 6-tuple anchor fields
    campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
    -- Type 1 attribute fields (versioned by dbt snapshot)
    mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman, endman,
    -- Other silver-consumed columns (mfgname, desc_defect, etc.)
    -- ... per existing recall_product.sql column list ...
    extraction_timestamp
from {{ ref('stg_nhtsa_recalls_current') }}
{% endsnapshot %}
```

dbt's snapshot mechanism is SCD Type 2: each invocation appends new versions when `check_cols` change, retains historical versions with `dbt_valid_from`/`dbt_valid_to` columns, and exposes a deterministic stable surrogate (`dbt_scd_id`).

### Real_drift taxonomy and reconciliation rules

Across Sections H/I/K/M of `documentation/nhtsa/incremental_delta_findings.md`, four distinct classes of real_drift have been empirically observed. The taxonomy was informally documented per-section as each class first surfaced; this subsection consolidates them as the canonical reference. Each class maps cleanly to the SCD mechanism above and produces a distinct `recall_event_history` event type for downstream consumers (per ADR 0022).

| Class | Empirical exemplar(s) | Reconciliation rule | `recall_event_history` event_type | SCD treatment |
|---|---|---|---|---|
| **Population** | Section K — Pierce ARROW XT 26V217000 `mfr_comp_desc` `'' → 'Software'` (96 rows in one editorial event) | Populated value supersedes empty; merge on the 6-tuple anchor | `field_filled` | Type 1 — current row holds the populated value; Type 2 history retains the empty pre-amendment version |
| **Depopulation** | Section H.4 — Mack 26V261000 `bgman`/`endman` populated → NULL (asymmetric per-yeartxt). Section I — Nissan CUBE 26V230000 (same shape) | Both retained for audit (Type 2 history); latest is canonical (Type 1 current) | `field_cleared` | Type 1 — current row holds NULL; Type 2 history preserves the prior populated value |
| **Value edit (batch-window or metadata)** | Section H.3 — Western Star 47X 26V079000 `endman` `2026-02-03 → 2026-04-10`; Chrysler Pacifica 26V189000 `bgman` `2022-05-10 → 2022-05-17`. Section M.2 — Ford 25V315000 `rcdate` backdating (5 days), Winnebago 24V733000 (8 days), Ford 25V343000 (21 days); Tesla 26V283000 `mfgcampno` `SB-26-00-016 → SB-26-00-001` | Latest wins. For batch-window fields specifically, a future extension could accumulate into a `production_window` range rather than overwriting — out of scope for v1.5 | `field_edited` | Type 1 — current row holds latest value; Type 2 history captures the transition with `dbt_valid_from`/`dbt_valid_to` |
| **Normalization** | Cross-corpus AC DELCO → ACDELCO `maketxt` event (2026-05-08, via `scripts/nhtsa/tsv_analysis/cross_corpus_stability.py`); TSV-substrate only, not yet observed in bronze | Fuzzy-match required because the anchor field itself changes — silver fragments at all candidate grains including the 6-tuple. Phase 6b RapidFuzz reconciliation handles the cross-source variant; the analogous NHTSA-internal mechanism would need a mapping layer. v1: manual review. ML-assisted later. | `field_normalized` | Cannot use Type 1 on the anchor field; requires explicit cross-row identity reconciliation **outside** the SCD mechanism |

**Why this matters for v1.5 architecture:** the first three classes (Population, Depopulation, Value edit) are **handled transparently by the Type 1 + Type 2 mechanism above** — the snapshot collapses pre- and post-amendment versions into one canonical current row plus a Type 2 history entry. The fourth class (Normalization on a 6-tuple anchor field) is the **only known class that v1.5 does NOT solve** and remains a Phase 6b deliverable. This is the precise scope boundary this ADR commits to: the "~99% of observed fragmentation" addressed claim (per the "Empirical evidence" section below) refers to the first three classes; the remaining ~1% is the Normalization class needing fuzzy reconciliation.

**Forward integration with Phase 6c `recall_event_history`:** when Phase 6c implements the event-grain history model, the `event_type` enum should include the four values in this table. The model's source data should be the dbt snapshot table (Layer 2 deliverable per `project_scope/silver_v15_migration_plan.md`), which exposes pre/post values per attribute change as Type 2 versions. Phase 6c's classifier then maps each version transition to its `event_type` per this table's "Reconciliation rule" column. This is open question #3 in the migration plan ("`recall_event_history` integration mode") — answering "snapshot directly" cleanly slots into this taxonomy without rework.

### Silver consumer surfaces

After Layer 3 migration:

| Model | Purpose | Cardinality for Pierce 26V217000 |
|---|---|---|
| `silver/recall_product` | Consumer-facing current state | 96 rows (one per logical product) |
| `silver/recall_product_history` | Audit/compliance view of all versions | 192 rows (96 current + 96 pre-amendment historical) |
| `bronze/nhtsa_recalls_bronze` | Source-faithful audit-quality (unchanged) | 192 rows (every distinct 11-tuple snapshot) |

### Relationship to ADR 0022 `recall_event_history`

ADR 0022 specifies `LAG()`-based history at the `(source, source_recall_id)` grain — i.e., per-event. That stays unchanged. This ADR adds a *product-grain* history layer via dbt snapshot. The two coexist:

- Event-grain question ("when did Pierce recall 26V217000 first appear? what were its top-level state changes?") → answered by `recall_event_history` (ADR 0022 mechanism)
- Product-grain question ("when did Pierce ARROW XT 2015 software's mfr_comp_desc change from empty to 'Software'?") → answered by `recall_product_history` (this ADR's snapshot mechanism)

The Phase 6 `recall_event_history` model can optionally consume from the product-grain snapshot to enrich event-level history with attribute-change details, but is not required to.

## Consequences

### Positive

- **Pierce-class fragmentation eliminated.** Consumer queries against `recall_product` return semantically correct cardinality (96 not 192 for Pierce, 1 not 2 for Nissan-class per-yeartxt depopulation, 1 not 2 for Western Star-class boundary edits).
- **Industry-aligned SCD pattern.** Standard data-warehouse vocabulary applies; future contributors can reason about behavior using well-known concepts.
- **Phase 6 reconciliation rules simplified.** Population/depopulation/value-edit drift classes degenerate to "snapshot updates the attribute, latest wins" — no bespoke reconciliation needed. Only the AC DELCO-class normalization on anchor fields requires fuzzy-match reconciliation, concentrating Phase 6 complexity on the actually-hard case.
- **Full product-grain history preserved and queryable.** dbt snapshot's `dbt_valid_from`/`dbt_valid_to` give explicit version effective dates, queryable without `LAG()` machinery at query time.
- **Bronze unchanged.** ADR 0030's 11-tuple identity stays; bronze remains audit-quality and source-faithful per ADR 0027.
- **Cross-source applicable.** Same framework extends to CPSC (where ordinality-shift is the analog hazard), USCG (when Phase 5d lands), and any future source with multiple drift-prone attributes.

### Negative

- **AC DELCO-class normalization still fragments at the 6-tuple grain.** `maketxt` (and the other 5 anchor fields, theoretically) can drift via NHTSA character-level normalization. The 6-tuple is empirically less drift-prone but not drift-immune. ~1 such event/year is the current rate baseline (TSV substrate). Phase 6 fuzzy-match reconciliation is the eventual remediation; for v1.5, accept as known limitation.
- **Pre-2008 records have NULL `rcl_cmpt_id`** and key off the 5-tuple effectively. Defensive dbt test required to assert no collisions in this cohort. Low risk; pre-2008 row counts are small and component-IDs were added because uniqueness without them became fragile.
- **Stateful dbt snapshot table adds operational complexity.** Cannot be dropped/rebuilt casually without losing history. Backup and migration procedures need to account for it. Snapshot tables grow over time; retention policy needed (probably "indefinite" given the audit purpose, but worth explicit decision).
- **Migration cost.** Silver model rewrite (`recall_product.sql`, new `recall_product_history.sql`, new snapshot file). Downstream consumers keyed on the current `md5(11-tuple)` `recall_product_id` (the Phase 6 in-flight `recall_event_history` model) re-key. Cost grows the longer migration is deferred; Layer 3 plan addresses sequencing.
- **Cross-source implementation is per-source work.** Each source needs its own snapshot + 6-tuple-equivalent decision. CPSC's equivalent (per `recall_product.sql:31-46`) would replace the `WITH ORDINALITY` ordinal with a stable-anchor recipe. FDA is already well-anchored (`PRODUCTID` stable) — minimal benefit. USDA is 1:1 grain — no fragmentation to fix at this layer.

### Neutral

- **Bronze 11-tuple identity unchanged.** ADR 0030's choices stay; this ADR is silver-layer only.
- **Event-grain `recall_event_history` unchanged.** ADR 0022's `LAG()`-based mechanism remains the source-uniform event-grain history.
- **The 9-tuple migration sunset (ADR 0031 Re-baseline 2026-05-15) stays coherent.** Sunset rationale: 9-tuple's premise was wrong (`mfr_comp_desc` is drift-prone). This ADR adopts a different cut (6-tuple) on a different premise (key/attribute role rather than predicted stability), which the Pierce event also informed.

## Empirical evidence

### NHTSA fragmentation events that motivate this ADR

| Event | Bronze impact | Silver impact (v1 md5(11-tuple)) | Silver impact (v1.5 md5(6-tuple)) | Source |
|---|---|---|---|---|
| Pierce 26V217000 mfr_comp_desc population | 96 new 11-tuple rows | +96 fragmented `recall_product` rows | 0 fragmented (Type 1 attribute update) | `incremental_delta_findings.md` Section K |
| Nissan CUBE 26V230000 endman/bgman asymmetric depopulation | 2 new 11-tuple rows | +2 fragmented `recall_product` rows | 0 fragmented (Type 1 attribute update) | `incremental_delta_findings.md` Section I |
| Mack 26V261000 bgman/endman depopulation | 5 new 11-tuple rows | +5 fragmented `recall_product` rows | 0 fragmented (Type 1 attribute update) | `incremental_delta_findings.md` Section H.4 |
| Chrysler Pacifica 26V189000 bgman edit | 4 new 11-tuple rows | +4 fragmented `recall_product` rows | 0 fragmented (Type 1 attribute update) | `incremental_delta_findings.md` Section H.3 |
| Western Star 47X 26V079000 endman edit | 1 new 11-tuple row | +1 fragmented `recall_product` row | 0 fragmented (Type 1 attribute update) | `incremental_delta_findings.md` Section H.3 |
| AC DELCO 22E002000 maketxt normalization | TSV substrate only (not yet observed in bronze) | Would fragment at all grains including 6-tuple | Would fragment (anchor field) — Phase 6 fuzzy reconciliation eventually | `cross_corpus_stability.py` 2026-05-08 |

Of 108 observed fragmenting events (107 + AC DELCO): **107 would be eliminated by the 6-tuple migration; 1 would still fragment** and would require Phase 6 fuzzy-match reconciliation regardless of any silver-grain choice. The 6-tuple migration addresses ~99% of observed fragmentation.

### 6-tuple anchor field drift baseline

See "The 6-tuple stable anchor" subsection above for the per-field drift table. Cumulative across all 6 anchor fields: 1 event in ~12 months of TSV-substrate observation; 0 events on bronze substrate.

### dbt snapshot mechanism — proven external

dbt snapshots are a stable, well-documented dbt-core feature: https://docs.getdbt.com/docs/build/snapshots. The `strategy='check'` mode with `check_cols` produces SCD Type 2 history correctly when invoked on each dbt run. This is the industry-standard primitive for this exact problem.

## Implementation

Per the companion `project_scope/silver_v15_migration_plan.md`, the implementation is phased over three layers with explicit decision gates. The plan doc is the source of truth for execution; this ADR is the architectural record.

**Layer 1 (this commit):** This ADR + migration plan doc. Conceptual scaffolding. No code changes.

**Layer 2 (deferred, ~1–2 days dbt work + ~2 weeks parallel observation):** Build the dbt snapshot and parallel silver models (`recall_product_v15`, `recall_product_history`). Run alongside existing v1 silver. Collect cardinality and behavior evidence. Detailed deliverables in plan doc.

**Layer 3 (deferred, conditional on Layer 2 evidence):** Migration cutover. Replace `silver/recall_product` with the v1.5 mechanism. Update downstream consumers. Requires a separate ADR (0034 if proceeding) citing the empirical evidence from Layer 2.

## Cross-source implications

**CPSC** — `recall_product.sql:31-46` uses `md5(event_id || name || model || product_ordinal)`. The product_ordinal is `WITH ORDINALITY`-derived, which is array-position-dependent. The analog 6-tuple equivalent for CPSC would be `md5(event_id || product_attrs_hash)` with a stable matching pass, or `md5(event_id || product_persistent_position)` with persistent position assignment via dbt snapshot's `unique_key`. Worth investigating once NHTSA v1.5 lands.

**FDA** — `recall_product.sql:51` uses `md5('FDA'||PRODUCTID)`. PRODUCTID is documented as stable upstream; FDA is already on a stable anchor. SCD Type 2 attribute history via dbt snapshot would still add value (Phase 6 `recall_event_history` enrichment), but the anchor-key choice is unchanged.

**USDA** — `recall_product_id = recall_event_id` (1:1 grain). No product-level fragmentation possible at this layer. SCD framework doesn't apply at the product grain; existing `recall_event_history` covers what's needed.

**USCG** — Phase 5d (landed 2026-05-30). **Confirmed cross-source SCD case** — and the one that stresses this framework hardest. The `AXY`/`COP` MIC reassignments are *whole-entity succession* under a stable anchor (`mic`), not single-field drift; and uniquely among our sources USCG **publishes its own succession lineage** (detail-page `Past Company N (OOB year)` + `In Business` + `Date Modified`), predating our observation window by decades. So the `firm_manufacturer_attributes` SCD-2 dim **seeds historical intervals from the source-native lineage and extends them forward with our own snapshots** (a hybrid NHTSA does not need), and the decision-forcing recall→manufacturer join is as-of-*build-date* (HIN chars 9–12), not as-of-recall-date. v1 treatment: flag-as-time-sensitive (surface is 205 recalled MICs `(OOB)`-recycled / 365 with any prior holder per the 2026-05-30 probe, but the source's reassignment dates are too sparse for precise resolution). Specified for **new ADR 0035** (cross-source SCD-2; 0034 reserved for this ADR's Layer 3), built in **Phase 6b** bundled with `firm.sql`/`recall_event_firm.sql`. Bronze-side detail capture landed on `feature/uscg-manufacturers-detail-addition`. Full design: `project_scope/phase-5d-uscg-manufacturers-detail.md` §11 + the Phase-6 cross-source SCD-2 item in `implementation_plan.md` + the cross-source application section of `silver_v15_migration_plan.md`. **Still TODO:** fill ADR 0031's per-source-table USCG row.

**Standing requirement for future sources:** apply the SCD framing (key/attribute decomposition + Type 1 attribute updates + Type 2 history) from initial silver design rather than starting with an all-fields-in-key recipe and migrating later. Update the per-source table in ADR 0031 with each new source's anchor + attribute decision.
