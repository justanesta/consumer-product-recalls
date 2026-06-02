# 0036 — Cross-source canonical silver column naming

- **Status:** Proposed (stub — reserves the number; the decision is ratified when `cross_source_consolidation.md`'s rename ledger is finalized)
- **Date:** 2026-06-02 (number reserved)
- **Owning plan:** [`project_scope/silver-field-remap-plan.md`](../../project_scope/silver-field-remap-plan.md) (W2/W3)
- **Substantive home until accepted:** [`documentation/audit/cross_source_consolidation.md`](../audit/cross_source_consolidation.md) §5 (rename ledger) — the per-concept canonical names live there until this ADR ratifies the policy.
- **Relates to:** [ADR 0002](0002-unit-of-analysis-header-line-firm.md) (the `recall_event` / `recall_product` / `firm` tables whose columns this names); [ADR 0027](0027-bronze-storage-forced-transforms-only.md) (value-level normalization is silver's job, so silver is free to rename). **Scope boundary:** the cross-source SCD-applicability verdict is **not** here — it folds into [ADR 0035](0035-cross-source-scd2-silver-dimensions.md) + `cross_source_consolidation.md` §8, keeping SCD reasoning single-homed.

## Why this stub exists

The silver layer was built CPSC+FDA-first, so several columns carry per-source-derived names that no longer fit once all five sources populate them. The `feature/silver-field-remap` audit chooses **one canonical name per semantic concept against the union of all five sources**, which means *renaming* existing columns — most notably `recall_event.description → recall_event.recall_reason`, because once the FDA `description ← distribution_area_summary_txt` mismapping is fixed, all five sources populate that column with defect/reason narrative, not free-text description. "Rename to a union-chosen canonical name vs keep the per-source-derived names" is the decision a future reader would question.

This file is filed now only to **reserve the number and remove the dangling "ADR 0036" reference** in `silver-field-remap-plan.md`. The substantive content (the full rename ledger, per concept, with the full-corpus evidence justifying each) lives in `cross_source_consolidation.md` §5 until the remap SQL lands and the policy is ratified here.

## Decision

**Deferred.** To be `Accepted` when W2's consolidation doc finalizes the rename ledger against full-corpus bronze evidence (W1) and the W4 SQL applies it. The policy under consideration: *silver columns are named for the cross-source semantic concept, not the originating source's field; renames are applied once, against the union, never per source.*

## Consequences

- Renames are breaking column changes, but `dbt/models/gold/` is effectively empty today (only a stub), so blast radius is low — the cost is paid before gold/API consume the names.
- The canonical names become the contract that Phase 6b (firm resolution), 6c (history/lifecycle), and 6e (gold) build on; getting them right once here avoids a rename cascade later.
- Until `Accepted`, treat the `cross_source_consolidation.md` §5 ledger as the working source of truth for canonical names.
