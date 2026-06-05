# 0035 — Cross-source SCD-2 for silver dimensions

- **Status:** Proposed — **decision recorded 2026-06-05 on the A scope** (cross-source policy = Policy C; the only instance *built* now is USCG `firm_manufacturer_attributes`). Ratifies → **Accepted** at PR 6b.5 merge, per the PR-merge status-flip convention (mirrors ADR 0036). The 2026-06-01 stub rationale and the 2026-06-02 applicability verdict are retained below as history.
- **Date:** 2026-06-01 (number reserved) · 2026-06-02 (SCD-applicability verdict, W3) · 2026-06-05 (decision)
- **Supersedes:** —
- **Superseded by:** —
- **Generalizes:** [ADR 0033](0033-silver-row-versioning-via-scd-on-stable-anchor.md) (SCD-on-stable-anchor, proven NHTSA-first for `recall_product`) to the silver *firm* dimensions. ADR 0033's "Cross-source implications → USCG" subsection is the design seed for this ADR.
- **Clarifies / companion changes (land in PR 6b.5):** [ADR 0031](0031-silver-row-fragmentation-strategy.md) — fill its TBD USCG per-source row; [ADR 0007](0007-lineage-via-bronze-snapshots-and-content-hashing.md) — exempt `silver_snapshots` from bronze-snapshot pruning.
- **Companion documents:** `project_scope/phase-5d-uscg-manufacturers-detail.md` §11 (the USCG SCD-2 build spec); `project_scope/phase-6b-execution-plan.md` PR 6b.5; `project_scope/implementation_plan.md` Phase 6 (cross-source SCD-2 item, `:707`, Policy C `:715`); `project_scope/silver_v15_migration_plan.md` cross-source section; `documentation/audit/scd_field_designations.md` (the per-field NEED/BENEFIT verdicts + validating monitors).

## Context

### The problem

Silver *dimensions* are materialized as `table` and rebuilt on every `dbt build` with **no attribute history preserved**. Two concrete losses motivate the ADR:

- **A correctness bug (NEED).** A USCG **MIC** (Manufacturer Identification Code) is a stable anchor that is *reassigned* when a hull-number block changes hands — the 2026-05-30 `AXY`/`COP` reassignments are the confirmed exemplars. A Type-1 "current holder" view silently **misattributes** every pre-reassignment recall to whoever holds the MIC today. `assert_mic_holder_stable.sql` measured **221 OOB-recycled of 718 recalled MICs** (paren-form 205 + dash-form `- OOB` 16; the OOB regex was broadened to word-boundary 2026-06-05) and **365 with any prior holder**. This is the strongest Type-2 NEED signal in the corpus.
- **A lost feature (BENEFIT).** Stable-key dims (FDA/CPSC/USDA) lose attribute *history* (e.g. the 2026-05-15 USDA `status_regulated_est` flips) on every rebuild even though their keys do not fragment.

ADR 0033 proved the SCD-on-stable-anchor pattern for NHTSA `recall_product`. This ADR decides the *cross-source* strategy for the firm dimensions: the storage architecture, the value-selection policy, **which instances to build now**, and the as-of-date dimensional-join surface.

### Two-axis frame (established by the 2026-06-02 verdict, preserved verbatim below)

- **NEED** — does the silver key *fragment* on drift (a correctness bug: duplicate rows / misattribution)?
- **BENEFIT** — is attribute *history* valuable even when the key is stable (a feature)?

### Measured verdict (what is and isn't warranted)

- **USCG `firm_manufacturer_attributes` (`mic`) — Type-2-NEED, monitor-confirmed.** 221/718 OOB-recycled, 365/718 with any prior holder. The first cross-source SCD-2 instance and the only measured NEED.
- **FDA / CPSC / USDA — NEED low (snapshot-hypotheses).** Stable keys; **0 edit-versions** in the Phase 6a.5 re-seeds. Recorded as hypotheses; the monitors validate them forward as incrementals re-bank history.
- **Type-2-BENEFIT, measure-forward** — recall `classification`/`severity` + `lifecycle_status`: suspected, unmeasured (re-seeds wiped versions); monitors accrue the rate.

## Decision

### 1. Cross-source value-selection policy = **Policy C**

Each SCD-2 dimension exposes a **latest-wins current view** (one row per stable anchor, `dbt_valid_to is null`) plus a **first-class peer history model** — the dbt snapshot table *is* the history, queryable directly by `dbt_valid_from`/`dbt_valid_to`. Reject the `LAG()`-over-snapshots model (Policy reconstructs history at query time — heavier, and we want a materialized peer) and reject "defer entirely." This is the standing standard for all future SCD-2 silver dims.

### 2. Storage architecture = **dbt snapshot `strategy='check'` on the stable anchor** (ADR 0033 option a)

The same primitive ADR 0033 adopted for NHTSA. No bespoke history machinery; dbt manages `dbt_valid_from`/`dbt_valid_to`/`dbt_scd_id`.

### 3. Build scope **(this step) = USCG `firm_manufacturer_attributes` ONLY**

USCG is the only measured Type-2 NEED, so it is the only instance built in Phase 6b PR 6b.5. **No snapshot is created for FDA/CPSC/USDA now** — they have nothing to version (0 edit-versions), and their monitors will signal if/when that changes. This keeps the policy cross-source while the *build* stays scoped to the correctness need.

### 4. The USCG instance (the concrete build)

- **Snapshot** — `dbt/snapshots/uscg_manufacturer_attributes_snapshot.sql`: `strategy='check'`, `schema='silver_snapshots'` (the dbt 1.9+ snapshot config, which routes through `generate_schema_name`), **`unique_key='mic'` alone** (a reassignment is a *new version of the same anchor*; `mic+company` would wrongly fork lineage). Driven from `stg_uscg_manufacturer_details` (latest-per-MIC) with `upper(trim(mic))` baked in (the 7 lowercase recall MICs — `cec, blb, kis, lbb, ser, vky, zep` — join via `upper(trim())` in `firm.sql`/`recall_event_firm.sql`; without normalization they miss the SCD table). `where company_name is not null` drops sentinel/null-name anchors.
  - **Schema isolation:** a custom `dbt/macros/generate_schema_name.sql` makes custom schemas resolve VERBATIM (dbt's default would prefix `schema='silver_snapshots'` to `public_silver_snapshots`). So the snapshot lands in its own `silver_snapshots` schema — isolating irreplaceable SCD-2 history from the stateless rebuilt models in `public` and giving the ADR 0007 pruning exemption a clean schema-level boundary. The macro leaves every existing `public` model unchanged (the no-custom-schema branch returns `target.schema`).
- **`check_cols`** = `company_name, dba, parent_company, parent_mic, past_company_1, past_company_2, past_company_3, address, city, state, zip, country, status, out_of_business`. **Deliberately EXCLUDE `date_modified` and `in_business`** — both are record-touch / heartbeat fields contaminated by directory re-touches on active firms (the staging view warns `in_business ≈ date_modified ≈ 2025/2026` on MERCURY/VOLVO PENTA/CATERPILLAR). Including them would spawn phantom SCD-2 versions on every directory re-scan — the same hazard ADR 0032 solved for USDA's `latest_mpi_active_date` via hash exclusion.
- **`firm_manufacturer_attributes.sql`** repoints from the listing view (`stg_uscg_manufacturers`, `source_recall_id as mic`) to `ref('uscg_manufacturer_attributes_snapshot') where dbt_valid_to is null`; adds the detail fields (the **full untruncated address** fixes Finding F.1's ~30-char listing truncation) + derived `mic_has_prior_holder` / `mic_oob_recycled` booleans + a `prior_holders` jsonb. **Grain stays one current row per `mic`.**
- **Two OOB signals are DIFFERENT and must not be conflated** (per the staging-view caveat): the **top-level `out_of_business`** = the *current* holder is defunct (the SCD `valid_to` signal); a **Past Company `(OOB)`** = a *prior* holder ceased → the MIC was recycled. `mic_oob_recycled` derives from the *latter*; `mic_has_prior_holder` from any `past_company_*` being present.
- **Bridge wiring (lockstep-safe).** `recall_event_firm.sql`'s `uscg_event_firms` CTE LEFT JOINs `firm_manufacturer_attributes` on `upper(trim(mic))` and stamps `match_confidence` **in the branch** — `uscg_mic_time_sensitive_unresolved` when `mic_oob_recycled` or `mic_has_prior_holder`, else `uscg_mic_unambiguous` — carried through the existing `mapped`-CTE precedence (`coalesce(nullif(u.match_confidence,'exact_name'), x.match_confidence, …)`), exactly as USDA's resolution does. **The `firm_id` recipe is UNCHANGED → the `firm.sql` ↔ `recall_event_firm.sql` lockstep is preserved (an additive LEFT JOIN + a column the union already width-aligns).** The three `uscg_mic_*` enum values were reserved in PR 6b.0, so `_silver.yml` needs no enum edit.

### 5. A-scope boundary — what v1 does **NOT** do

v1 = the **forward snapshot** (banks reassignments we observe from 2026-05-30 onward) + the **source-native time-sensitive FLAG**. Deferred to a **Phase-6c follow-on**:

- The **historical-interval backfill** that would seed SCD intervals from the source-native lineage (`Past Company (OOB year)` + `In Business`). Only **~13 of 221** OOB years parse, so the payoff is ~6% and the intervals would be mostly open-ended / low-confidence.
- The **as-of-build-date correct-attribution join** (HIN chars 9–12 → hull build date → which holder held the MIC *then*), which needs HIN parsing that isn't built.
- A **rename-vs-recycle tier** on the flag. The 6b.5 swing-set probe (`scripts/sql/cross_source/scd_monitors/probe_mic_prior_holder_not_oob.sql`; 144 residual MICs read 2026-06-05) showed the binary flag's residual is **~84% genuine distinct-prior-firm reassignments** (real misattribution risk) but **~16% same-entity renames / name-variants** (`INC↔LLC`, abbreviation drift, the explicit `(previous name)` marker). Since the source's `(previous name)` marker reliably tags renames, a later tier could split `recycled` (OOB / unmarked-distinct-firm → flag hard) from `renamed` (same entity → downgrade to `uscg_mic_unambiguous`). The `match_confidence` enum has room; v1 keeps the honest binary `_unresolved` because over-flagging a rename is cheap and a rename's name still differs. **Needs validation** before building (sample the `(previous name)` coverage vs the unmarked-rename tail).

Rationale: the flag **discharges the correctness NEED** at precision-over-recall — a recall on a recycled MIC is surfaced as `uscg_mic_time_sensitive_unresolved` rather than silently misattributed. The backfill and the as-of-build-date join are *features* that refine recall (precise historical attribution), not correctness gaps; both are honest Phase-6c work, not silent omissions.

### 6. Companion changes (land in PR 6b.5)

- **ADR 0031** — fill the TBD USCG per-source row: anchor = `mic`; attributes = the detail fields (Type-1 latest-wins); history = the dbt snapshot.
- **ADR 0007** — exempt `silver_snapshots` from the bronze-snapshot pruning policy (pruning the snapshot table = silent history loss).

## Consequences

### Positive

- **The misattribution NEED is closed.** The 365 prior-holder MICs (which include the 221 OOB-recycled) surface as time-sensitive on the bridge instead of being wrongly attributed to the current holder — the flag fires on the broad any-prior-holder set, precision-over-recall (see §4 / the 6b.5 swing-set analysis).
- **Industry-standard SCD-2 vocabulary + primitive.** dbt snapshot `strategy='check'` is the same mechanism ADR 0033 adopted; future contributors reason about it with well-known concepts, and the cross-source policy (C) is now single-homed.
- **Finding F.1 fixed as a side effect** — the snapshot's full untruncated address replaces the listing view's ~30-char truncation.
- **History becomes a queryable peer.** `dbt_valid_from`/`dbt_valid_to` give explicit version effective dates without query-time `LAG()`.
- **Cross-source generalization is recorded but cost-controlled** — Policy C is the standard, yet only the NEED instance is built, so FDA/CPSC/USDA pay zero snapshot cost until their monitors justify it.

### Negative

- **Pre-2026-05-30 reassignments aren't precisely datable in v1.** The flag says "this MIC was recycled," not "holder X held it on date Y." The as-of-build-date join (Phase 6c) is the eventual precise fix.
- **A stateful snapshot table adds operational weight** — it can't be dropped/rebuilt casually without losing history; the ADR 0007 pruning exemption and the `silver_snapshots` schema provisioning are hard prerequisites.
- **`strategy='check'` correctness depends on the `check_cols` cut.** A drift-prone attribute left out of `check_cols` would miss a real version; a heartbeat field left in would spawn phantom ones (hence the explicit `date_modified`/`in_business` exclusion).

### Neutral

- **`firm.sql` `uscg_normalized` and the `md5(...)` firm grain are untouched** — this ADR is a sidecar + bridge-flag change, not a firm-identity change. The 1549-orphan failure mode (a divergent `firm_id` recipe) is not in play.
- **FDA/CPSC/USDA SCD-2 stays deferred** and is validated forward by the existing monitors; this ADR does not commit their build.

## Empirical evidence

- **USCG recycle set:** `assert_mic_holder_stable.sql` (2026-05-30, reproduced 2026-06-02; OOB regex broadened 2026-06-05) — **221 OOB-recycled (205 paren + 16 dash-form `- OOB`) / 365 with any prior holder / 718 recalled MICs**. The bridge flag stamps the broad **365** prior-holder set; the 221 OOB and the monitor's Q4 sample are the high-confidence subset. Re-confirmed as gate **G9** before the build.
- **The reassignment mechanism:** `AXY`/`COP` confirmed reassigned across businesses 2026-05-30 (MIC ⊂ HIN; codes recycled on ownership change).
- **Backfill yield:** only **~13/221** OOB years parse from `Past Company (OOB year)` — the basis for deferring the historical-interval seed.
- **Snapshot primitive:** dbt snapshots are a stable dbt-core feature; `strategy='check'` produces SCD Type 2 correctly on each run — same external proof cited by ADR 0033.

## Implementation

Bundled into **Phase 6b PR 6b.5** — the single PR that already edits `firm.sql`/`recall_event_firm.sql`, which §11 requires so the lockstep edit happens once (no cross-branch rebase risk). Sequenced per `phase-6b-execution-plan.md` PR 6b.5:

1. **Prereq (one-time, user):** provision/confirm the `silver_snapshots` schema + dbt create-schema rights on the Neon dev branch.
2. **Gate G9 (user):** re-run `assert_mic_holder_stable.sql`, capture the recycle set to `documentation/uscg/`.
3. Build the snapshot + `_snapshots.yml`; repoint `firm_manufacturer_attributes.sql`; wire the bridge flag; add `assert_uscg_mic_reassignment_flag_present` + `assert_uscg_scd2_no_forked_lineage`; the two companion ADR edits (0031, 0007).
4. **User-run:** `dbt snapshot --select uscg_manufacturer_attributes_snapshot` (banks current) → `dbt build --select firm_manufacturer_attributes recall_event_firm` → `dbt test` → re-run `dbt snapshot` on an unchanged corpus to confirm `strategy='check'` idempotency (0 spurious versions).

This ADR records the *architectural decision*; the plan doc records the *execution phasing*.

---

## Why this stub originally existed (2026-06-01, retained)

Filed to **reserve the number and remove dangling references** to "ADR 0035" already present in the plan docs, while the substantive design lived in the owning plans (phase-5d §11, `implementation_plan.md` Phase 6) until the decision was made. The decision above (2026-06-05) discharges that.

## Amendment 2026-06-02 — cross-source SCD-applicability verdict (silver-field-remap W3)

The `feature/silver-field-remap` audit measured, per source, **whether** SCD-2 is warranted — the *applicability* verdict that feeds this ADR's build decision. The build **architecture** (storage a/b/c, value-selection Policy A/B/C, the as-of-date join) was deferred to Phase 6 at the time of this amendment; the **2026-06-05 Decision above now settles it on the A scope.** This amendment records the measured evidence.

**Two-axis frame:** **NEED** = does the silver key *fragment* on drift (a correctness bug — duplicate rows / misattribution)? **BENEFIT** = is attribute *history* valuable even when the key is stable (a feature)?

**Verdict** — field-level detail + the validating monitors live in [`documentation/audit/scd_field_designations.md`](../audit/scd_field_designations.md); per-source edit-version shape in [`bronze_corpus_profile.md`](../audit/bronze_corpus_profile.md) §5:

- **Measured anchors (2):**
  - **NHTSA** `recall_product` 11-tuple — Type-2 (ADR 0033); 167 real cross-run edit-versions, natural-key core **0 drift** (`assert_eleven_tuple_identity_stable.sql`). Stable identity, history is BENEFIT.
  - **USCG** `firm_manufacturer_attributes` (`mic`) — **Type-2-NEED, monitor-confirmed.** MIC reassignment makes the same anchor denote a different firm over time, so a Type-1 "current holder" *misattributes* pre-reassignment recalls; `assert_mic_holder_stable.sql` = **205 OOB-recycled of 718 recalled MICs** (paren-only as measured 2026-06-02; re-measured to **221** incl. the dash-form `- OOB` after the OOB regex was broadened 2026-06-05 — see the Decision / Empirical-evidence sections above). This is the **first cross-source SCD-2 instance** and the strongest NEED signal in the corpus; it bundles into Phase 6b (firm resolution) with the as-of-**build-date** MIC→manufacturer join.
- **Snapshot-hypotheses (3):** FDA / CPSC / USDA — NEED **low** (stable keys; **0 edit-versions** in the Phase 6a.5 re-seeds). Recorded as hypotheses; the monitors validate them forward as incrementals re-bank history.
- **Type-2-BENEFIT, measure-forward:** recall `classification`/`severity` + `lifecycle_status` — amendments/escalations suspected but **unmeasured** (re-seeds wiped versions); `assert_classification_stable.sql` / `assert_lifecycle_stable.sql` accrue the rate over time.
- **Not SCD (Type-1 + bronze as audit trail):** `recall_reason` narrative + `firm.normalized_name` — corrections, not history; the fragmentation concern is solved by 6b *normalization*, not SCD.

**Implication for the build decision:** prioritize `firm_manufacturer_attributes` (the confirmed NEED) + its as-of-build-date join; the BENEFIT dims are deferrable features whose monitors will quantify the payoff before the build commits. *(Settled 2026-06-05: build the USCG flag now; defer the as-of-build-date join + the BENEFIT dims.)*
