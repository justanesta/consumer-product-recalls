# 0034 — NHTSA silver v1.5: Layer-3 migration cutover

- **Status:** Accepted (2026-06-06; number reserved 2026-06-01)
- **Date:** 2026-06-06
- **Owning plan:** [`project_scope/archive/silver_v15_migration_plan.md`](../../project_scope/archive/silver_v15_migration_plan.md) — Layer 3 (now complete)
- **Builds on:** [ADR 0033](0033-silver-row-versioning-via-scd-on-stable-anchor.md) (silver row-versioning via SCD on a stable anchor) **and its 2026-06-06 amendment** (the 6-tuple → 7-tuple correction)
- **Updates:** [ADR 0031](0031-silver-row-fragmentation-strategy.md) per-source NHTSA row (recipe migrated `md5(11-tuple)` → `md5(7-tuple)`)
- **Unchanged by this ADR:** [ADR 0030](0030-nhtsa-bronze-identity-composite-tuple-and-within-batch-dedup.md) (bronze 11-tuple identity), [ADR 0022](0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md) (`recall_event_history` LAG over bronze)

## Context

ADR 0033 decided the *architecture* (migrate NHTSA `recall_product` off the all-fields-in-key `md5(11-tuple)` recipe to a stable-anchor surrogate with Type-1 latest-wins attributes + Type-2 history via a dbt snapshot) and deferred the `Proposed → Accepted` transition and the cutover authorization to this ADR, conditional on **Layer-2 empirical evidence**.

Layer 2 was built in **Phase 6c.6** (parallel prototype, no consumer impact) and produced two results:

1. **It corrected the anchor.** The first full-corpus build showed the ADR-0033 6-tuple collapsed `recall_product` **321,540 → 194,377 rows (−40%)**. `scripts/sql/nhtsa/silver/characterize_v15_collapse.sql` attributed **99.4% of the collapse to *structural* `mfr_comp_ptno` variation** (the documented §G multi-part fan-out — Takata-class tire recall `24T014000` = 139 components × 139 part numbers; the Fortune Tormenta `LT235/75R15` one-part-→-many-component-IDs case), not temporal drift. `mfr_comp_ptno` is the structural part identity, so the anchor was revised to the **7-tuple** (ADR 0033 amendment 2026-06-06). The 6-tuple would have shipped a 40% silver data-loss event — caught precisely because Layer 2 ran before the cutover.
2. **It validated the corrected design.** With the 7-tuple, v1.5 ≈ v1 (**320,303 vs 321,540** — a 1,237-row / 0.38% residual). `characterize_v15_residual.sql` + `inspect_v15_residual_modeltxt.sql` proved the residual is *all simultaneous* (not temporal) variation on `mfr_comp_desc`/`mfr_comp_name` within a fixed (vehicle, part), with `modeltxt` populated in 832/832 collapsing groups — so **no model or part coverage is lost** (both are anchor fields); only secondary manufacturer-supplied description text collapses (latest-wins, full set retained in bronze). The residual is irreducible: `mfr_comp_desc` is the Pierce drift field — anchoring it would re-break the fragmentation fix. The snapshot is idempotent (2nd `dbt snapshot` = 0 new versions) and the pre-2008 degenerate-anchor guard passes.

## Decision

**Authorize the Layer-3 cutover (executed in Phase 6c.7).** Specifically:

1. **`silver/recall_product`'s NHTSA branch consumes the snapshot.** `nhtsa_products` now selects `nhtsa_recall_product_snapshot` where `dbt_valid_to is null`, re-keyed to the 7-tuple `recall_product_id` = `md5('NHTSA' | campno | normalize_maketxt(maketxt) | modeltxt | yeartxt | compname | rcl_cmpt_id | mfr_comp_ptno)` (single-homed in `stg_nhtsa_recalls_current`). Column shape is byte-identical to the prior CTE, so UNION parity with the other four source branches holds. `recall_product_v15` is dropped (its SELECT folded in); `recall_product_history` is kept as the product-grain audit surface.
2. **`check_cols` widening (refinement on ADR 0033's original 5).** The snapshot versions `mfr_comp_desc`, `mfr_comp_name` (Pierce field-population class) + `bgman`, `endman` (batch-window edit class) + `rcltype`, `potaff`, `mfgname`, `mfgtxt`, `fmvss`. The last five are widened in so that an isolated business-field edit does not go stale in the current view (with `strategy='check'`, a non-check column edited alone is not refreshed until some check_col changes). The 7 anchor fields, `model_year` (functionally determined by `yeartxt`), and `extraction_timestamp` (a per-regen heartbeat) are deliberately excluded.
3. **Deterministic snapshot input.** `stg_nhtsa_recalls_current`'s `distinct on (7-tuple) order by … extraction_timestamp desc` gains a tiebreaker (`mfr_comp_desc, mfr_comp_name, bgman, endman`). The 832 collapsing 7-tuples share one seed-extraction timestamp, so without a tiebreaker the pick is arbitrary and a physical reorder could flip it and bank a phantom history version. The four demoted fields uniquely distinguish siblings within a 7-tuple, making the pick provably stable (idempotency becomes structural, not merely empirical). The cutover is paired with a one-time snapshot reset so it re-initializes under the deterministic pick.
4. **`recall_event_history` integration mode (migration-plan Open Q#3) — resolved: it stays on `LAG()`-over-bronze.** It is event-grain (`source, source_recall_id`); this snapshot is product-grain. They are complementary, not competing (ADR 0022 unchanged). `recall_event_history` does NOT consume the snapshot.

### Blast radius / reversibility

`recall_product_id` for NHTSA is self-contained (confirmed against ADR 0031's per-source table and a full repo trace): only `recall_product` itself and its own tests reference it. **Gold (`recalls_by_month`), `recall_event`, and the firm models do not join on `recall_product_id`**, and there is no lockstep with `firm.sql` (that is name-keyed). The change is a git-revertible code change in a dev project, not a production re-key — the migration plan's "one-way operation" framing applies to a prod pipeline with live BI consumers, which this is not.

### Observation-window waiver

The plan's Layer-2 → Layer-3 gate prescribed a ~2-week parallel-observation window. It is **waived**: on a re-seeded corpus with forward-only dbt snapshots, that window cannot surface the live drift or the −96 Pierce reduction it was meant to confirm (the coexisting versions were wiped; snapshots bank forward from current state). The full-corpus diff above + the **6c.8 simulated-drift test** (deterministic, repeatable) are the substituted evidence — stronger than waiting for NHTSA to happen to edit a row.

## Consequences

### Positive
- **Pierce-class fragmentation eliminated at the consumer grain** — an editorial edit to `mfr_comp_desc`/`mfr_comp_name`/`bgman`/`endman` versions the snapshot instead of minting a duplicate `recall_product` row.
- **Structural part granularity preserved** — `mfr_comp_ptno` in the anchor keeps the 126k Fortune-Tormenta-class rows (the 6-tuple would have discarded them).
- **Product-grain change history** is queryable via `recall_product_history` (`dbt_valid_from`/`dbt_valid_to`/`is_current`), complementing the event-grain `recall_event_history`.
- **Industry-standard SCD-2** (dbt snapshot) demonstrated end-to-end.

### Negative
- **A stateful snapshot is now load-bearing for `recall_product`.** It must be built by `dbt build` (not `dbt run`, which skips snapshots) and is exempt from `--full-refresh`; resetting it is a deliberate `DROP` (scripted at `scripts/sql/nhtsa/silver/reset_nhtsa_recall_product_snapshot.sql`). See `operations.md` → "SCD-2 snapshots."
- **0.38% irreducible residual** — secondary `mfr_comp_desc`/`mfr_comp_name` duplication within a (vehicle, part) collapses to latest-wins (full set retained in bronze). No model/part coverage loss.

### Neutral
- **Bronze unchanged** (ADR 0030 11-tuple identity stays — bronze remains audit-quality, every version retained).
- **The other four sources' `recall_product` recipes are unchanged** — this is NHTSA-only. Cross-source SCD application stays future work (ADR 0033 cross-source section; migration-plan Open Q#4).

## Evidence
- `documentation/decisions/0033-...md` amendment 2026-06-06 (the architectural correction)
- `scripts/sql/nhtsa/silver/characterize_v15_collapse.sql` (127,163 collapse → 99.4% structural)
- `scripts/sql/nhtsa/silver/characterize_v15_residual.sql` + `inspect_v15_residual_modeltxt.sql` (1,237 residual, all simultaneous, no coverage loss)
- `documentation/nhtsa/field_audit_2026_w22.md` §9 + `incremental_delta_findings.md` §G/§K (the fan-out + Pierce empirical record)
