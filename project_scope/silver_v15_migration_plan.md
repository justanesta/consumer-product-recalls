# Silver v1.5 migration plan — SCD on stable anchor for NHTSA `recall_product`

- **Status:** Layer 1 in progress (2026-05-15)
- **Architectural decision:** [ADR 0033](../documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md)
- **Triggering event:** Pierce ARROW XT family `26V217000` `mfr_comp_desc` population event, 2026-05-15 (see `documentation/nhtsa/incremental_delta_findings.md` Section K)
- **Sunset prerequisite:** ADR 0031 9-tuple migration evaluation (sunset 2026-05-15 per Stop criterion #1 firing)

## Overview

This plan executes the architectural decision in ADR 0033 — migrate NHTSA silver `recall_product` from the current `md5(11-tuple)` recipe to a 6-tuple stable-anchor recipe with Type 1 latest-wins attribute updates and Type 2 product-grain history via dbt snapshot. Migration is phased across **three discrete layers** with explicit decision gates between them so that work can stop at any layer's completion and still capture real value.

**Companion to:** ADR 0033 (architectural decision). This plan is *how we execute*; ADR 0033 is *what we decided*.

## Goals

1. Eliminate (or dramatically reduce) silver `recall_product` fragmentation under NHTSA real_drift events
2. Establish stable consumer-facing semantics: one silver `recall_product` row per logical product
3. Preserve full audit history of attribute changes at the product grain
4. Align with industry SCD Type 2 standards (dbt snapshot mechanism)
5. Validate the approach empirically before committing to migration (avoid over-rotating on a single event)

## Non-goals

- **Bronze layer changes.** ADR 0030's 11-tuple identity remains. Bronze stays audit-quality and source-faithful per ADR 0027.
- **Event-grain history mechanism changes.** ADR 0022's `LAG()`-based `recall_event_history` synthesis stays unchanged. This plan adds a parallel product-grain layer; it does not replace the event-grain mechanism.
- **Cross-source migration.** This plan addresses NHTSA only. CPSC, FDA, USDA migrations (per ADR 0033's "Cross-source implications" subsection) are separate future work and out of scope here.
- **Layer 3 migration commitment.** This plan defines what migration would look like, but the commitment is gated on Layer 2 evidence. Approving this plan does not approve the migration.
- **USCG Phase 5d work.** Tracked separately; this plan does not block or depend on USCG sequencing. See "Branching strategy" below for parallel-work guidance.

## The three layers — summary

| Layer | Output | Cost | Reversibility | Commits us to |
|---|---|---|---|---|
| **Layer 1** | ADR 0033 + this plan doc | ~half a day of writing | Fully reversible (just words) | Nothing operationally |
| **Layer 2** | dbt snapshot + parallel silver models (`recall_product_v15`, `recall_product_history`) running alongside existing v1 silver | ~1–2 days of dbt work + ~2 weeks of parallel observation | Reversible (drop parallel models, no consumer impact) | Maintaining the snapshot table (small ongoing cost) |
| **Layer 3** | Migration cutover: `silver/recall_product` replaced with v1.5 mechanism; downstream consumers re-keyed | ~half a day of dbt work + consumer-update cycle | **Hard to reverse** (consumers re-keyed) | Real schema/key changes; future-proof to Phase 6 reconciliation framework |

**Each layer's output is a precondition for the next layer's decision.** Layer 1's documentation gives Layer 2 conceptual scaffolding. Layer 2's empirical evidence gates the Layer 3 decision. Layer 3 is the commitment.

## Layer 1 — Documentation (in progress, 2026-05-15)

### Deliverables

| Artifact | Purpose | Status |
|---|---|---|
| `documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md` | Architectural decision (SCD framing, 6-tuple anchor, snapshot mechanism, cross-source applicability, empirical evidence) | Draft complete 2026-05-15 |
| `project_scope/silver_v15_migration_plan.md` (this file) | 3-layer rollout plan with gates, deliverables, risks, branching, status tracking | Draft complete 2026-05-15 |

### Cost

~half a day of writing. No code changes.

### Reversibility

Fully reversible. Documents can be revised, marked superseded, or deleted without operational impact.

### Branch

Commits land on the current `docs/findings-2025-05-w3` branch alongside today's Pierce/Nissan documentation work (Section K addition, ADR 0031 sunset edits, Section I H1 confirmation, inspector generalization). Ships in one merge to `main` at end of week.

### Gate to Layer 2

Re-read both documents a few days after writing. Proceed to Layer 2 only if all three are true:

1. **The SCD framing captures the problem cleanly.** A future reader unfamiliar with the project should be able to read ADR 0033 and understand both *what* we decided and *why* it follows from the empirical evidence.
2. **The cross-source application is credible.** The framework should extend naturally to CPSC, FDA, USDA, USCG without bespoke per-source rewrites. If any source's situation defeats the framework, revisit the architectural choice in ADR 0033 before building.
3. **The 6-tuple cut is empirically defensible.** No new bronze-substrate observations should contradict the 6-tuple drift baseline (currently 0 events on `campno`/`modeltxt`/`yeartxt`/`compname`/`rcl_cmpt_id`; 1 TSV-only event on `maketxt`). If continued daily observation surfaces fresh 6-tuple drift on a different anchor field, revise.

If any answer changes the architecture, revise ADR 0033 (and this plan) before building. The cost of revision at the documentation layer is hours; revision after Layer 2 is days.

## Layer 2 — Parallel v1.5 prototype (deferred, ~1–2 days dbt work + ~2 weeks observation)

### Deliverables

| Artifact | Purpose |
|---|---|
| `dbt/snapshots/nhtsa_recall_product_snapshot.sql` | dbt snapshot with 6-tuple `unique_key`, `strategy='check'`, `check_cols` covering the 5 attribute fields |
| `dbt/models/staging/stg_nhtsa_recalls_current.sql` | New staging model: `DISTINCT ON (6-tuple) ORDER BY 6-tuple, extraction_timestamp DESC` to pick latest bronze row per logical product |
| `dbt/models/silver/recall_product_v15.sql` | Parallel current-state view selecting from snapshot where `dbt_valid_to is null`. Same column shape as `recall_product` so it can be diffed |
| `dbt/models/silver/recall_product_history.sql` | Full versioned view: snapshot rows with `dbt_valid_from`/`dbt_valid_to` and an `is_current` flag |
| `scripts/sql/nhtsa/silver/compare_v1_v15_cardinality.sql` | Diagnostic comparing `recall_product` (v1) vs `recall_product_v15` row counts by campno; should show v1.5 ≤ v1 with deltas matching known fragmentation events |
| `dbt/tests/source_assumptions/assert_pre_2008_six_tuple_unique.sql` | Defensive test: for any pre-2008 record, the 5-tuple `(campno, maketxt, modeltxt, yeartxt, compname)` should be row-unique within the cohort (covers the NULL `rcl_cmpt_id` edge case per ADR 0033) |

### Cost estimate

- ~1 day to write the snapshot + new models + comparison script + defensive test
- ~1 day to validate via initial run + verify snapshot initialization handles existing bronze backfill cleanly
- ~2 weeks of parallel-running observation alongside v1 silver, with daily NHTSA extracts continuing as normal

### Reversibility

Reversible. Drop the snapshot table, drop the parallel models, no consumer impact. Worst-case rollback cost: deleting four files and one dev-database table.

### Branch

New branch off `main` after `docs/findings-2025-05-w3` merges. Suggested name: **`feature/silver-v15-scd-prototype`**.

Rationale: starts from the merged-to-main docs state (so Layer 1 context is available). Isolated from USCG work and from any future docs branches. Survives multiple iterations of the prototype if early validation reveals adjustments are needed.

### What to validate during the parallel-running period

| Validation | Method | Pass criterion |
|---|---|---|
| Cardinality reduction matches predictions | `compare_v1_v15_cardinality.sql` against current bronze | v1.5 row count ≤ v1 row count; differences match known fragmentation events (Pierce: −96, Nissan: −2, etc.) |
| Snapshot mechanism behaves correctly across daily regen | Inspect `dbt_valid_from`/`dbt_valid_to` after each daily dbt run | New snapshot versions appear when (and only when) `check_cols` change; no spurious versions on idle days |
| History view returns expected shape for known events | Manual query against `recall_product_history` for Pierce 26V217000 | Returns 96 current rows (`mfr_comp_desc='Software'`, `is_current=true`) + 96 historical rows (`mfr_comp_desc=''`, `dbt_valid_to=<2026-05-15 timestamp>`) |
| Pre-2008 edge case handled | `assert_pre_2008_six_tuple_unique.sql` passes | No 5-tuple collisions in pre-2008 cohort |
| No new 6-tuple anchor drift surfaces during observation | Daily `decompose_eleven_tuple_drift.sql` runs (existing tooling) | No real_drift on `campno`/`maketxt`/`modeltxt`/`yeartxt`/`compname`/`rcl_cmpt_id` during the 2-week window |
| Snapshot initialization handles existing bronze backfill cleanly | Inspect snapshot rows on first dbt invocation against existing bronze state | One snapshot version per existing logical product (~5,000–10,000 estimated for full NHTSA bronze); no duplicate versions; `dbt_valid_from` reflects earliest observation time |

### Gate to Layer 3

After ~2 weeks observation, write a brief evidence summary and answer:

1. **Did v1.5's cardinality match the predicted reduction across all observed campaigns?** Yes / unexpected delta in [campaign].
2. **Did the snapshot mechanism behave correctly across NHTSA's daily regen cadence?** Yes / observed [issue].
3. **Were there unexpected drift events at the 6-tuple grain?** None / observed [event] on [field].
4. **Is anything downstream committed to the current `md5(11-tuple)` keys?** None / Phase 6 `recall_event_history` model has hard dependency on [shape] / Phase 8 API contract references [shape].

If answers are favorable, proceed to Layer 3 with a new ADR (0034) citing the evidence. If unfavorable, decide: revise architecture (revisit ADR 0033 + this plan), defer (run prototype longer for more evidence), or abandon (v1 stands; sunset this plan with a closing note).

## Layer 3 — Migration cutover (deferred, conditional on Layer 2 evidence)

### Deliverables

| Artifact | Action |
|---|---|
| `documentation/decisions/0034-nhtsa-silver-v15-migration.md` | New ADR (status Accepted) citing Layer 2 empirical evidence and authorizing the migration |
| `dbt/models/silver/recall_product.sql` | Rewrite to consume from `nhtsa_recall_product_snapshot` (effectively rename `recall_product_v15` → `recall_product`) |
| `dbt/models/silver/recall_product_v15.sql` | Drop (now redundant) |
| `dbt/models/silver/recall_product_history.sql` | Keep (production-grade audit surface) |
| Downstream models | Any model joining on `recall_product_id` re-keys against the new 6-tuple recipe; primarily the Phase 6 `recall_event_history` model and any future Phase 8 API contracts |
| `documentation/decisions/0031-silver-row-fragmentation-strategy.md` | Update per-source table NHTSA row to reflect new recipe; status note "migrated 2026-MM-DD per ADR 0034" |
| `documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md` | Status update: Proposed → Accepted |
| `project_scope/silver_v15_migration_plan.md` (this file) | Status update: Layer 3 complete; sunset the plan or transition to "monitoring" mode |

### Cost

~half a day of dbt model rewrites, plus a consumer-update cycle whose cost depends on how many downstream models reference the current key. As of 2026-05-15 the only known downstream is the in-flight Phase 6 `recall_event_history` model.

### Irreversibility

Once consumers re-key against the new 6-tuple `recall_product_id`, reverting to the 11-tuple recipe means re-keying them again. Treat Layer 3 as a one-way operation.

### Branch

Future branch off `main` at the time of Layer 3 trigger. Suggested name: **`feature/silver-v15-migration`**.

### Pre-conditions to fire Layer 3

All four must hold:

1. Layer 2 prototype validates per the gate criteria above
2. At least one additional novel drift event observed during Layer 2's ~2-week window (confirms Pierce isn't an outlier; if no events occur, defer Layer 3 by another observation cycle rather than commit)
3. Phase 6 design is far enough along to know what `recall_event_history` will consume from (snapshot directly vs. existing `LAG()` mechanism vs. a hybrid)
4. No active blocker on consumer side (no Phase 8 API contract released against the old key; no production BI dashboards depending on current cardinality)

If any pre-condition fails at gate-2 evaluation time, defer Layer 3 (keep v1.5 prototype running) and re-evaluate quarterly.

## Branching strategy — full picture

```
main
├── docs/findings-2025-05-w3 ← CURRENT (Layer 1 work + today's findings docs)
│       └── merge to main at end of week
│
├── (after merge) docs/findings-2025-05-w4 ← next week's daily findings
│
├── (after merge) feature/uscg-bronze ← independent USCG Phase 5d work
│
├── (after merge) feature/silver-v15-scd-prototype ← Layer 2 work
│       │
│       └── after ~2 weeks parallel observation + gate evaluation:
│           ├── if proceed: feature/silver-v15-migration ← Layer 3 work
│           ├── if defer: stay on feature/silver-v15-scd-prototype, observe longer
│           └── if abandon: close branch, update plan doc to "abandoned" status
```

### Concurrent-work guidance

| Branch pair | Conflict risk | Mitigation |
|---|---|---|
| `feature/uscg-bronze` × `feature/silver-v15-scd-prototype` | Low — different source code paths; USCG adds new files, v1.5 modifies NHTSA-specific dbt models | Both can run fully in parallel. Minor merge friction possible at `dbt/models/silver/recall_product.sql` if USCG adds a `uscg_products` CTE — resolve by hand at merge |
| `feature/uscg-bronze` × `docs/findings-2025-05-w4` | Very low — different file scopes | None needed |
| `feature/silver-v15-scd-prototype` × `docs/findings-2025-05-w4` | Low — findings docs may reference v1.5 prototype state | Coordinate at merge time; v1.5 prototype lands first usually |

### Daily NHTSA extracts

Continue regardless of which branch is checked out. Bronze writes land in the production Neon database + R2 unchanged by branch state. The branch only matters for code/doc/dbt-model commits, not for extraction-pipeline state.

**Exception:** if `feature/silver-v15-scd-prototype` is checked out and `dbt build` is run, the new `nhtsa_recall_product_snapshot` table will be created in your dev silver schema. This is fine — parallel to existing models, doesn't replace them. Just be aware of the operational footprint.

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| AC DELCO-class normalization fragments at 6-tuple grain | Low (1 event in ~12 months observed; possibly more frequent if NHTSA changes normalization practices) | Limited — affects a small number of campaigns over time; Phase 6 fuzzy-match reconciliation is the eventual remediation | Document as known limitation in ADR 0033; track in monthly Tier 2 review per ADR 0031's threshold revisit policy |
| Pre-2008 records have NULL `rcl_cmpt_id` and could collide at the 5-tuple grain | Low (pre-2008 row counts small, components reportedly unique at the make/model/year/component level historically) | Could create unexpected fragmentation in older recalls if a collision exists | Defensive dbt test `assert_pre_2008_six_tuple_unique.sql` as Layer 2 deliverable; fix anchor recipe if test fails |
| Snapshot mechanism state lost / corrupted | Very low if backups maintained; medium impact if it happens | Loss of product-grain history; bronze still has audit trail to reconstruct from | Standard Postgres backup procedure; document snapshot-rebuild process in Layer 2 deliverables (rebuild from bronze if needed) |
| Stateful complexity creates ops burden | Medium long-term | Adds one more thing to monitor / migrate / backup | Worth the cost given the consumer-facing semantics improvement; minimize by following dbt snapshot conventions strictly |
| Downstream consumer breaks at Layer 3 cutover | Medium (depends on Phase 6 state at trigger time) | Could delay Phase 6 work or require rework | Layer 3 pre-condition #4 explicitly gates on consumer readiness; coordinate cutover timing with Phase 6 leads |
| Re-baseline drift over time changes the empirical case for migration | Low-medium (NHTSA's editorial behavior could shift) | Could invalidate the 6-tuple anchor choice or change the cost/benefit | Quarterly review per ADR 0031's threshold revisit policy; update this plan if re-baseline shifts the calculus |

## Status tracking

| Layer | Status | Last update | Next action | Owner |
|---|---|---|---|---|
| Layer 1 — Documentation | In progress | 2026-05-15 | Both docs drafted; awaiting commit + a few-day review window before Layer 2 gate evaluation | (TBD — assign before commit) |
| Layer 2 — Parallel prototype | Not started | 2026-05-15 | Gate 1 → 2 evaluation when Layer 1 merge lands | (TBD) |
| Layer 3 — Migration | Not started | 2026-05-15 | Gated on Layer 2 evidence | (TBD) |

Update this table when status changes. Append a brief log entry below for material decisions or evidence captures.

### Status log

- **2026-05-15** — Plan created. ADR 0033 drafted in parallel. Both ship in `docs/findings-2025-05-w3` branch alongside Pierce-event documentation work.

## Open questions

These need resolution before or during the indicated layer:

1. **Snapshot retention policy** — Layer 2. Indefinite retention is the conservative choice given audit purpose. Confirm at Layer 2 deliverable time.
2. **Snapshot initialization timing** — Layer 2. First snapshot run against existing bronze backfill should create one version per existing logical product. Verify timing isn't disruptive on the dev database (Pierce-class events will produce ~5–10k existing versions; this is small but worth a dry-run check).
3. **`recall_event_history` integration mode** — Layer 3 pre-condition #3. Should the Phase 6 `recall_event_history` model consume from the snapshot table directly (richer attribute-change events) or stay on the `LAG()`-over-bronze mechanism (event-grain only)? Decision needed before Layer 3 cutover.
4. **Cross-source rollout order** — post-Layer 3. CPSC is the next candidate (ordinality-shift hazard). FDA and USDA are low-priority (already well-anchored or 1:1 grain). USCG should be designed with SCD framing from the start. USCG manufacturers now has a concrete confirmed reassignment case (2026-05-30) and a unique source-native-history property — see the "Cross-source application" section below. Sequence and timing TBD.
5. **dbt snapshot conventions for this project** — Layer 2. Where in the dbt project structure do snapshots live? `dbt/snapshots/` is the dbt-default convention; confirm the project's preferences match.
6. **Schema location for snapshot table** — Layer 2. ADR 0033 proposes `silver_snapshots` schema. Confirm this fits the existing dbt schema-naming conventions (or pick an alternative).

## Cross-source application — USCG manufacturers MIC reassignment (added 2026-05-30)

This plan's mechanism is NHTSA-scoped (see Non-goals), but the 2026-05-30 USCG discovery is the first **non-NHTSA** confirmation that ADR 0033's SCD-on-stable-anchor framing generalizes — and it stresses the framework in a new way worth recording here for the eventual cross-source ADR (implementation_plan.md Phase 6).

**The case.** The first incremental `uscg_manufacturers` run caught two MIC reassignments (`AXY`: ARMY SURPLUS → SOSA; `COP`: CONSER / COPALIS → COPALO), confirmed against the source's own detail-page lineage. Full evidence: `documentation/uscg/manufacturer_scraping_observations.md` §M.

**Mapping onto the ADR 0033 axes:**

| ADR 0033 concept | NHTSA `recall_product` | USCG `firm_manufacturer_attributes` |
|---|---|---|
| Stable anchor (surrogate key) | 6-tuple `(campno, make, model, year, compname, rcl_cmpt_id)` | 1-tuple `mic` (regulatory code; = first 3 chars of every hull's HIN per 33 CFR 181) |
| Type 1 latest-wins attributes | `mfr_comp_desc`, etc. | `company_name`, `address`, `city`, `state` (+ detail fields if Path B) |
| Type 2 history mechanism | dbt snapshot (`strategy='check'`) over our snapshots | our snapshots (forward-only from 2026-05-30) **and/or** source-native lineage |

**The twist — source-native history.** Unlike NHTSA (where Type 2 history is *synthesized* from our bronze snapshots and only reaches back to our first observation), the USCG source **publishes its own succession history**: the detail page exposes `Past Company 1–3 (OOB year)`, `In Business`, `Parent MIC`, and `Date Modified`, predating our observation window by decades. Capturing it requires a Path B detail-page enrichment pass (listing-only extraction does not — see implementation_plan.md Step 7 follow-up). So the SCD-2 model for this dim **seeds historical intervals from the source-native lineage (`Past Company (OOB year)` + `In Business`) and extends them forward with our own bronze snapshots** (forward-only from 2026-05-30) — a hybrid the NHTSA case does not need. Capturing that lineage is a bronze-side prerequisite (the `feature/uscg-manufacturers-detail-addition` branch, bronze-only); this SCD-2 silver model and the HIN-build-date join are Phase 6 work, deliberately kept off the detail-capture branch (would otherwise collide with Phase 6b on `firm.sql`).

**The decision-forcing query is as-of-*build-date*, not as-of-*recall-date*.** Because the MIC is embedded in the HIN, correct recall→manufacturer attribution joins on the **boat's build date** (HIN chars 9–12; recalls bronze carries `hin` and `model_year`), not the recall publication date. A recall on a pre-1978 ARMY SURPLUS hull must resolve to ARMY SURPLUS, not the current holder SOSA. This is a sharper instance of the "as-of-date dimensional join" surface the cross-source SCD-2 ADR must design. In practice the v1 join **flags** recalls on a reassigned MIC as "attribution time-sensitive" rather than resolving precisely — the source's reassignment dates are mostly unusable (only ~13 of 205 recalled-recycled MICs carry a parseable OOB year; `In Business` is contaminated by record-touches). The recall-directed probe (2026-05-30) measured the surface: **28.7% (205) of recalled MICs `(OOB)`-recycled, 51.1% (365) with any prior holder** — see `documentation/uscg/manufacturer_scraping_observations.md` §M.6.

**Execution stays out of this plan** (Non-goal: USCG Phase 5d tracked separately). Tracked in `project_scope/implementation_plan.md` (Phase 6 cross-source SCD-2 item + Step 7 follow-up + the reassignment-rate probe). Recorded here as cross-source applicability evidence for ADR 0033 and as input to Open Question #4 above.

## References

- [ADR 0033](../documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md) — architectural decision
- [ADR 0031](../documentation/decisions/0031-silver-row-fragmentation-strategy.md) — silver-row fragmentation strategy framework (the 9-tuple migration sunset in particular)
- [ADR 0030](../documentation/decisions/0030-nhtsa-bronze-identity-composite-tuple-and-within-batch-dedup.md) — NHTSA bronze 11-tuple identity (unchanged)
- [ADR 0022](../documentation/decisions/0022-fda-history-endpoints-empty-snapshot-synthesis-for-all-sources.md) — `recall_event_history` LAG()-based synthesis (unchanged)
- [ADR 0007](../documentation/decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md) — bronze snapshot lineage (extended at the silver layer)
- [Pierce event empirical record](../documentation/nhtsa/incremental_delta_findings.md) — Section K
- [Nissan event empirical record](../documentation/nhtsa/incremental_delta_findings.md) — Section I
- [dbt snapshots documentation](https://docs.getdbt.com/docs/build/snapshots) — external reference for the SCD Type 2 primitive
