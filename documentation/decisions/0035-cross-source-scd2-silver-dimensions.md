# 0035 — Cross-source SCD-2 for silver dimensions

- **Status:** Proposed (stub — reserves the number; the decision is not yet made)
- **Date:** 2026-06-01 (number reserved)
- **Owning plans:**
  - [`project_scope/phase-5d-uscg-manufacturers-detail.md`](../../project_scope/phase-5d-uscg-manufacturers-detail.md) §11 (USCG `firm_manufacturer_attributes` SCD-2 design + the as-of-build-date / flag-as-time-sensitive recall join)
  - [`project_scope/implementation_plan.md`](../../project_scope/implementation_plan.md) Phase 6 — "Cross-source SCD-2 strategy for silver dimensions"
- **Generalizes:** [ADR 0033](0033-silver-row-versioning-via-scd-on-stable-anchor.md) (proven NHTSA-first) to `firm`, `firm_establishment_attributes`, `firm_manufacturer_attributes`, and other silver dims.

## Why this stub exists

Silver *dimensions* are currently materialized as `table` and rebuilt on every `dbt build` with **no attribute history preserved** (e.g., the 2026-05-15 USDA establishments `status_regulated_est` flips and the 2026-05-30 USCG `AXY`/`COP` MIC reassignments become unrecoverable after the next transform). ADR 0033 proved the SCD-on-stable-anchor pattern for NHTSA `recall_product`; this ADR will decide the *cross-source* strategy (storage architecture a/b/c and the value-selection policy A/B/C — see `implementation_plan.md` Phase 6) and the as-of-date dimensional-join surface (notably USCG's as-of-**build-date** MIC→manufacturer join).

This file is filed now only to **reserve the number and remove dangling references** to "ADR 0035" already present in the plan docs. The substantive design lives in the owning plans until the decision is made.

## Decision

**Deferred.** To be written early in Phase 6 (before `recall_event_history` lands, since the implementation may share `LAG()` plumbing), per `implementation_plan.md` Phase 6. The leading candidate as of 2026-05-17 is **Policy C** (latest-wins current view + a first-class peer history model), but this is an open decision the ADR will settle on evidence, not a commitment. Per-source SCD *builds* follow NHTSA's ADR 0033 / 0034 Layer-3 proof; USCG's `firm_manufacturer_attributes` is the first cross-source instance and bundles into the Phase 6b firm-resolution PR (respecting the `firm.sql` ↔ `recall_event_firm.sql` lockstep).

## Consequences

- Until this ADR is `Accepted`, silver dims carry only current state; attribute history is unrecoverable from silver/gold (bronze snapshots retain the raw signal).
- When filed, it becomes the cross-source SCD-2 authority and governs the as-of-date query surface that gold model design depends on.
