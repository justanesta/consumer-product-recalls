# 0034 — NHTSA silver v1.5: Layer-3 migration cutover

- **Status:** Proposed (stub — reserves the number; the decision is not yet made)
- **Date:** 2026-06-01 (number reserved)
- **Owning plan:** [`project_scope/silver_v15_migration_plan.md`](../../project_scope/silver_v15_migration_plan.md) — Layer 3
- **Builds on:** [ADR 0033](0033-silver-row-versioning-via-scd-on-stable-anchor.md) (silver row-versioning via SCD on a stable anchor)

## Why this stub exists

ADR 0033 decided the *architecture* (migrate NHTSA `recall_product` from the `md5(11-tuple)` recipe to a 6-tuple stable anchor with Type-1 latest-wins attributes + Type-2 history via dbt snapshot) and frames the rollout as three layers. **Layer 3 is the irreversible cutover** — `silver/recall_product` is rewritten to consume the snapshot and downstream consumers re-key. ADR 0033 explicitly defers the `Proposed → Accepted` transition and the migration authorization to this ADR, *conditional on Layer-2 empirical evidence* (`silver_v15_migration_plan.md` Layer-3 deliverables + Layer-2 gate).

This file is filed now only to **reserve the number and remove dangling references** to "ADR 0034" that already exist in the plan docs. The substantive content lives in the owning plan until the Layer-2 gate is evaluated.

## Decision

**Deferred.** To be written when the Layer-2 prototype gate passes (per `silver_v15_migration_plan.md` Layer-2 → Layer-3 gate criteria + Layer-3 pre-conditions). At that point this ADR will: cite the Layer-2 empirical evidence, authorize the cutover, set Status to `Accepted`, and update ADR 0031's per-source NHTSA row.

Sequencing note: Layer 2's snapshot baseline must initialize against the **full-corpus** NHTSA bronze, so this work starts after Phase 6a.5, not merely after the audit — see `project_scope/branch_sequencing_strategy.md`.

## Consequences

- Until this ADR is `Accepted`, NHTSA `recall_product` keeps the ADR 0030 / ADR 0031 `md5(11-tuple)` recipe; no consumer is re-keyed.
- When filed, it becomes the irreversible-cutover authority; treat Layer 3 as a one-way operation per the owning plan.
