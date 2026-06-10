# 0036 — Cross-source canonical silver column naming

- **Status:** Accepted — ratified at the **#58 merge** (`feature/silver-field-remap`, commit 728a9a3) with the renames applied (W4), per the PR-merge status-flip convention. Policy written 2026-06-02 (decisions **D1–D7** resolved in [`cross_source_consolidation.md`](../audit/cross_source_consolidation.md) §0).
- **Date:** 2026-06-02
- **Owning plan:** [`project_scope/silver-field-remap-plan.md`](../../project_scope/silver-field-remap-plan.md) (W2/W3)
- **Substantive home:** [`documentation/audit/cross_source_consolidation.md`](../audit/cross_source_consolidation.md) — §1–§3 the full field→canonical-column map, §5 the rename ledger, §0 the resolved naming decisions. This ADR records the **policy + rationale**; the per-concept detail stays there (single-home).
- **Relates to:** [ADR 0002](0002-unit-of-analysis-header-line-firm.md) (the `recall_event` / `recall_product` / `firm` tables this names); [ADR 0027](0027-bronze-storage-forced-transforms-only.md) (value-level normalization is silver's job, so silver is free to rename + conform). **Scope boundary:** the cross-source SCD-applicability verdict is **not** here — it lives in [ADR 0035](0035-cross-source-scd2-silver-dimensions.md)'s 2026-06-02 amendment + `cross_source_consolidation.md` §8 + `scd_field_designations.md`, keeping SCD reasoning single-homed.

## Context

The silver layer was built CPSC+FDA-first, so several columns carry per-source-derived names that no longer fit once all five sources populate them — the headline being `recall_event.description`, which (once the FDA `description ← distribution_area_summary_txt` mismapping is fixed, Bug 1) holds **defect/reason narrative** for all five sources, not a "description." The full-corpus audit (W1) + the consolidation map (W2) chose one canonical name per concept against the **union** of all five sources. "Rename to a union-chosen canonical name, and *conform* the values, vs keep per-source-derived names" is the decision a future reader would question.

## Decision

**Policy:** silver columns are named for the **cross-source semantic concept**, not the originating source's field; the canonical name and shape are chosen **once, against the union of all five sources, never per source.** How far the *values* conform follows a spectrum — the rationale behind decisions D1–D7:

1. **Fully conformed** — same concept *and* same domain (`announced_at`, `published_at`, `firm.normalized_name`).
2. **Conform the column, not the domain** — one column, **source-native values**, per-source `accepted_values` (`warn`). The `source` column disambiguates the encoding. **D2** `classification` (FDA `1/2/3/NC` + USDA `Class I/II/III/PHA` + USCG `H/L/M/S` coexist); **D3** `recall_product.type` (five disjoint domains — food-processing / vehicle-tire / boat-code / consumer-category — that never unify). Safe because silver is rebuilt from bronze: splitting a source back into its own column later is a SQL edit + `dbt build`, no migration, no data loss.
3. **Conform via derivation** — keep the raw value source-native, add a **derived** conformed attribute on top. **D4** `initiated_by ∈ {firm, agency}` over the untouched `recall_initiator` (FDA `voluntary_type` + NHTSA `influenced_by`, the only two sources with the field); **D7** `distribution_scope` (FDA + USDA); the deferred `severity_rank` (pending USCG `severity` semantics from USCG OII).
4. **Role-playing** — one dimension, multiple roles in the same fact: NHTSA `mfgname` (filer) + `mfgtxt` (manufacturer) both land in `firm`, discriminated by `role`.

**Load-bearing renames / decisions** (full ledger: `cross_source_consolidation.md` §5):
- **`recall_event.description → recall_reason`** — the headline rename (all five sources populate the defect/reason narrative post-Bug-1). USDA's *structured* reason-enum gets a separate **`reason_category`** to avoid the collision (**D1**).
- `conequence_defect → consequence_of_defect` (source typo fixed at silver); `recall_event_firm.role` **+`filer` −`retailer`** (NHTSA split + CPSC Option B).
- conform `classification` (D2) + `recall_initiator` (D4); per-source `type` (D3); `number_of_units` **TEXT** + derived `unit_count` INTEGER for the clean-integer sources (D5).

## Consequences

- Renames are breaking column changes, but `dbt/models/gold/` is effectively empty today → blast radius is low; the cost is paid **before** gold/API consume the names.
- These canonical names become the contract Phase 6b (firm resolution), 6c (history/lifecycle), and 6e (gold) build on; getting them right once here avoids a rename cascade later.
- **Reversibility is cheap** (silver is a derived, rebuilt-from-bronze layer), so the policy can conform aggressively now and reshape later at SQL-edit cost if a decision proves wrong — the asymmetry favors conforming.
- Until this ADR is `Accepted` (at PR merge), treat `cross_source_consolidation.md` §0/§5 as the working source of truth for canonical names + decisions.
