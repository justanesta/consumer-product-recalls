# SCD field designations — the working catalogue + its monitors

- **Status:** Active (living working reference). The designations here are the **current best SCD-type assignments** per field — mostly **ASSUMED** (inferred from key structure), some **MEASURED** (validated against real edit-versions). W3 hardens the verdict into **ADR 0035**; the monitors in §4 keep these assumptions honest as data flows.
- **Why this doc exists:** silver is uniformly **Type 1** today (every `stg_*` is `row_number() over (partition by key order by extraction_timestamp desc)` → latest-wins). "Applying SCD-2" = selectively promoting bronze's already-captured history (bronze is a Type-4 store) into a versioned silver table — but only where a field's *real* change behaviour justifies it. This catalogue records that behaviour, the designation it implies, and the report that validates it.
- **Single-home:** this doc owns the **per-FIELD SCD designation + its validation status + its monitor**. `bronze_corpus_profile.md` §5 owns the per-**source** edit-version shape summary (points here for field detail); **ADR 0035** owns the *decision/why*; the monitor **scripts** own the measurement.
- **The decision procedure** (derived in the W3 learning exercise): (1) does the attribute actually change? *No* → **Type 0**. (2) Change = correction or real event? *Correction* → **Type 1** (bronze is the audit trail). (3) Does latest-only give a WRONG answer (misattribution/fragmentation)? *Yes* → **Type 2 — NEED**. (4) Latest-only correct but history valuable? → **Type 2 — BENEFIT** (deferrable). (5) Need fast-current + full history, or source hands bounded priors → **Type 6 / Type 3**.

## 1. SCD type vocabulary (quick reference)

`0` retain/immutable · `1` overwrite (current only) · `2` new row + `valid_from`/`valid_to`/`is_current` (full history) · `3` previous-value column (fixed depth) · `4` separate history table (**= bronze**) · `6` hybrid 1+2+3.

## 2. Status vocabulary

- **MEASURED** — validated against real cross-run edit-versions (the source banked history we could diff).
- **ASSUMED** — inferred from key structure / domain knowledge; **not yet** validated against edit-versions (usually because the Phase 6a.5 re-seeds wiped bronze's history → 0 edit-versions for FDA/CPSC/USDA/USCG). A monitor exists or is named to promote it to MEASURED as incrementals re-bank history.
- **HYPOTHESIS** — a plausible designation with neither measurement nor a strong structural argument yet.

## 3. The designations catalogue

### `recall_event` grain
| Canonical field | Source bronze columns | Silver today | Designation | NEED / BENEFIT | Status | Validating monitor |
|---|---|---|---|---|---|---|
| `announced_at` | `recall_initiation_dt` / `recall_date` / `rcdate` / `opened_on` | Type 1 | **Type 0** (immutable announce date) | — | ASSUMED (validate it never moves) | (classification-family pattern) |
| `classification` / `severity` | FDA `center_classification_type_txt`, USDA `recall_classification`, USCG `severity` | Type 1 (latest-wins) | **Type 2 — BENEFIT** (amendments / escalations; silver currently drops the escalation into bronze-only) | low NEED / escalation-history BENEFIT | **ASSUMED** — re-seeds wiped versions; amendments suspected, unmeasured | **`cross_source/scd_monitors/assert_classification_stable.sql`** (new) |
| `lifecycle_status` / `status` | FDA `phase_txt`, USDA `recall_type`, USCG `disposition` | Type 1 | **Type 2 — BENEFIT** (Ongoing→Terminated / Active→Closed lifecycle timeline) | low / lifecycle-history | ASSUMED | **`cross_source/scd_monitors/assert_lifecycle_stable.sql`** |
| `terminated_at` / `closed_at` | FDA `termination_dt`, USDA `closed_date`, USCG `case_close_date` | Type 1 | **Type 0-once-set** (NULL→date fill, then immutable) | — | ASSUMED | (classification-family pattern) |
| `recall_reason` / description | FDA `product_short_reason_txt`, CPSC `description`, USDA `summary`, NHTSA `desc_defect`, USCG `coalesce(problem_1,problem_2)` | Type 1 | **Type 1** (copy-edits = corrections; **bronze is the audit trail**) | avoid-fake-history | ASSUMED | — (bronze Type-4 history) |

### firm grain
| Canonical field | Source | Silver today | Designation | NEED / BENEFIT | Status | Validating monitor |
|---|---|---|---|---|---|---|
| `firm.normalized_name` / `canonical_name` | all 5 | Type 1 | **Type 1** (the fragmentation problem is solved by *normalization*, not SCD) | avoid-fragmentation → 6b | ASSUMED | (6b RapidFuzz; NHTSA `assert_*name*`) |
| USCG firm anchor (`mic` → company) | `uscg_manufacturers` + `uscg_manufacturer_details` | Type 1 (current holder) | **Type 2 — NEED** (MIC reassignment → a pre-reassignment hull is *misattributed* to the current holder) | **misattribution** / succession-history | **MEASURED + MONITOR-CONFIRMED** (§M AXY/COP; §M.6 = 365 prior / 221 OOB-recycled — paren 205 + dash-form `- OOB` 16, OOB regex broadened to word-boundary 2026-06-05 — of 718 recalled MICs; the paren-only 205 reproduced **exactly** by the SQL monitor 2026-06-02) | **`cross_source/scd_monitors/assert_mic_holder_stable.sql`** (Q1 dynamic + Q2–Q4 static all-slot `past_company_*` lineage → unfold to Type-2) |
| USDA establishment status | `usda_fsis_establishments.status_regulated_est` | Type 1 | **Type 2 — BENEFIT** (active↔Inactive flip analytics) | low / flip-time-series | ASSUMED | **`usda_establishments/bronze/list_status_flips.sql`** (exists) |

### `recall_product` grain
| Canonical field | Source | Silver today | Designation | NEED / BENEFIT | Status | Validating monitor |
|---|---|---|---|---|---|---|
| CPSC product surrogate (array C2/C3) | `cpsc_recalls_bronze.products` ordinal md5 | Type 1 | **Type 1, watch** (ordinal key could fragment if the array reorders/normalizes; now non-vacuous at 8.3% multi-product) | watch-fragmentation | ASSUMED / untested | **`cpsc/bronze/assert_products_array_append_only.sql`** + `assert_name_model_normalization_stable.sql` (exist) |
| NHTSA 11-tuple identity | `nhtsa_recalls_bronze` | Type 1 silver / Type 2 designed | **Type 2** (ADR 0033) | low (core stable) / versioning | **MEASURED** (167 edit-versions, natural-key core 0 drift) | **`nhtsa/bronze/assert_eleven_tuple_identity_stable.sql`** + `assert_nine_tuple_identity_stable.sql` (exist) |

### edit-timestamp signal (feeds any Type-2 build)
| Concern | Source | Status | Monitor |
|---|---|---|---|
| Is `last_modified_date` a reliable per-edit timestamp? | USDA | partially measured | **`usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql`** (exists) |
| Else fall back to bronze `extraction_timestamp` | all | — | — |

## 4. Monitor registry — the reports that validate designations

The repo already carries a scattered fleet of change-detection asserts; this section unifies them under SCD governance. Each is read-only and safe to re-run.

| Monitor | Validates designation | Reads ~0 today? |
|---|---|---|
| `cross_source/scd_monitors/assert_classification_stable.sql` | `classification`/`severity` Type-2-BENEFIT (amendment rate) | yes — re-seeds wiped versions; measure-forward |
| `cross_source/scd_monitors/assert_lifecycle_stable.sql` | `lifecycle_status` Type-2-BENEFIT (status-transition rate) | yes — measure-forward |
| `cross_source/scd_monitors/assert_mic_holder_stable.sql` | USCG firm anchor Type-2-NEED (MIC reassignment) | **no for Q2–Q4** — the static all-slot `past_company_*` lineage returns real data now (reproduces §M.6: 365 prior / 221 OOB-recycled — paren-only was 205, word-boundary OOB added 16 dash-form 2026-06-05); Q1 (edit-versions) is measure-forward |
| `nhtsa/bronze/assert_eleven_tuple_identity_stable.sql` | NHTSA 11-tuple Type-2 (core stability) | no — 167 edit-versions, gives real signal |
| `cpsc/bronze/assert_products_array_append_only.sql` | CPSC product ordinal key (C2 append-only) | yes — 0 edit-versions in the re-seed |
| `cpsc/bronze/assert_name_model_normalization_stable.sql` | CPSC C3 name/model normalization | yes |
| `usda_establishments/bronze/list_status_flips.sql` | USDA establishment status Type-2-BENEFIT | yes |
| `usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql` | USDA edit-timestamp reliability | yes |

## 5. The validation loop — observing data as it flows

The catalogue stays honest by a simple loop, not a one-time audit:

1. **Bank history.** Daily incrementals + the weekly deep-rescan (ADR 0010) re-accumulate content-hash edit-versions in bronze (the re-seeds reset this to 0; it grows from here).
2. **Run the monitors** (§4) periodically — naturally alongside the weekly deep-rescan, or ad hoc when investigating a suspected drift. They diff each changing attribute across versions and report the rate.
3. **Promote / correct designations.** When a monitor shows a field's real behaviour differs from its ASSUMED designation — e.g. `assert_classification_stable.sql` finds a non-trivial AMENDED rate — update that row here (ASSUMED → MEASURED, and the type if the data demands it), and note the date.
4. **Feed W3 / ADR 0035.** A MEASURED change in designation is the trigger to (re)open the SCD-2 build decision for that field. The verdict lives in ADR 0035; this catalogue is its evidence base.

**Wire monitors as `severity=warn` dbt singular tests** (mirroring the existing `dbt/tests/source_assumptions/`) once a designation graduates to MEASURED, so a future drift alarms in CI rather than waiting for a manual run.

**DONE — Phase 6c.3 (2026-06-06).** Four monitors are now wired as `severity=warn` singular tests under `dbt/tests/source_assumptions/` (auto-warn via `dbt_project.yml`); the psql scripts in §4 stay the rich ad-hoc / forensic surface (the dbt tests carry only the CI-alarm slice):
- `assert_classification_stable` / `assert_lifecycle_stable` — read the classification / lifecycle_status slices of `recall_event_history` (6c.1), which already excludes re-baselines (ADR 0027) and folds cosmetic noise. A returned row ⇒ amendment/transition detected ⇒ graduate that row's designation ASSUMED → MEASURED here.
- `assert_mic_holder_stable` — the **dynamic** half only: a MIC whose `company_name` changes across `firm_uscg_attributes_snapshot` versions (a forward-observed reassignment). The static source-native recycle surface is already enforced by `assert_uscg_mic_reassignment_flag_present` (6b).
- `assert_no_join_key_erasure_usda` — new erasure tripwire: a USDA join-key field (`establishment`, `company_media_contact`; jsonb arrays per Finding S) populated in a prior snapshot but empty/null in the latest.

---

*Per-source edit-version shape: `bronze_corpus_profile.md` §5. SCD-2 authority / decision: ADR 0035 (+ ADR 0033 NHTSA proof, ADR 0031 fragmentation). The W3 verdict folds the MEASURED rows here into ADR 0035.*
