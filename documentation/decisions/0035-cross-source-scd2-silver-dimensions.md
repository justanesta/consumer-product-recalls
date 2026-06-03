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

## Amendment 2026-06-02 — cross-source SCD-applicability verdict (silver-field-remap W3)

The `feature/silver-field-remap` audit measured, per source, **whether** SCD-2 is warranted — the *applicability* verdict that feeds this ADR's eventual build decision. The build **architecture** (storage a/b/c, value-selection Policy A/B/C, the as-of-date join) **remains deferred to Phase 6** per the Decision above; this amendment only records the now-measured evidence (it does **not** change the `Proposed` status).

**Two-axis frame:** **NEED** = does the silver key *fragment* on drift (a correctness bug — duplicate rows / misattribution)? **BENEFIT** = is attribute *history* valuable even when the key is stable (a feature)?

**Verdict** — field-level detail + the validating monitors live in [`documentation/audit/scd_field_designations.md`](../audit/scd_field_designations.md); per-source edit-version shape in [`bronze_corpus_profile.md`](../audit/bronze_corpus_profile.md) §5:

- **Measured anchors (2):**
  - **NHTSA** `recall_product` 11-tuple — Type-2 (ADR 0033); 167 real cross-run edit-versions, natural-key core **0 drift** (`assert_eleven_tuple_identity_stable.sql`). Stable identity, history is BENEFIT.
  - **USCG** `firm_manufacturer_attributes` (`mic`) — **Type-2-NEED, monitor-confirmed.** MIC reassignment makes the same anchor denote a different firm over time, so a Type-1 "current holder" *misattributes* pre-reassignment recalls; `assert_mic_holder_stable.sql` = **205 OOB-recycled of 718 recalled MICs**. This is the **first cross-source SCD-2 instance** and the strongest NEED signal in the corpus; it bundles into Phase 6b (firm resolution) with the as-of-**build-date** MIC→manufacturer join.
- **Snapshot-hypotheses (3):** FDA / CPSC / USDA — NEED **low** (stable keys; **0 edit-versions** in the Phase 6a.5 re-seeds). Recorded as hypotheses; the monitors validate them forward as incrementals re-bank history.
- **Type-2-BENEFIT, measure-forward:** recall `classification`/`severity` + `lifecycle_status` — amendments/escalations suspected but **unmeasured** (re-seeds wiped versions); `assert_classification_stable.sql` / `assert_lifecycle_stable.sql` accrue the rate over time.
- **Not SCD (Type-1 + bronze as audit trail):** `recall_reason` narrative + `firm.normalized_name` — corrections, not history; the fragmentation concern is solved by 6b *normalization*, not SCD.

**Implication for the eventual build decision:** prioritize `firm_manufacturer_attributes` (the confirmed NEED) + its as-of-build-date join; the BENEFIT dims are deferrable features whose monitors will quantify the payoff before the build commits.
