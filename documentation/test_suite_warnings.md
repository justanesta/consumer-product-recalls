# Test-suite warnings register

A `severity=warn` dbt test that returns rows is **not a failure** — it's an intentional watch-list or
source-behavior monitor that surfaces a known, accepted condition without blocking the build (ADR 0015
posture; ADR 0031 for the source-assumption monitors). This register is the single place that records,
per warn-level test, **what it watches, its baseline, and the verdict** — so a recurring warning isn't
re-triaged from scratch. Triaged in Phase 6e.5 (2026-06-07).

**Verdict legend:** **KEEP** = benign monitor doing its job (document, don't remove/backfill). Removing
loses source-drift signal; "fixing" isn't possible because the data is genuinely absent / the source
genuinely behaves that way.

## Suite vs ADR 0015 target

ADR 0015 targets "~60–80 generic + 5 singular + freshness." Actual (post-Phase 7 WS-C/F): **~210 generic** (silver +
staging + sources + the 12 gold models), **~42 singular** (cross-model invariants + 14 source-assumption
monitors + the 4 added in 6e.5: date-sanity, units-sanity, and two gold reconciliation tests), and **8
source freshness** configs. The suite **meets and exceeds** the target; the deferred items (statistical /
baseline-distribution tests) still need production traffic (Phase 7).

## 1. Source-assumption monitors (14) — `dbt/tests/source_assumptions/`, all `warn` via `dbt_project.yml`

Single-home detail: `documentation/source_assumption_audit.md` (+ per-source findings docs). All **KEEP** —
these exist to surface source-API drift with known non-zero baselines (ADR 0031).

| Test | Watches | Verdict |
|---|---|---|
| `assert_classification_stable` | recall classification/severity amendments (SCD signal) | KEEP — measure-forward monitor |
| `assert_lifecycle_stable` | lifecycle-status transitions | KEEP |
| `assert_mic_holder_stable` | USCG MIC holder change (Type-2 NEED) | KEEP |
| `assert_cpsc_products_array_append_only` | CPSC `products[]` append-only | KEEP |
| `assert_cpsc_name_model_normalization_stable` | CPSC product name/model don't renormalize | KEEP |
| `assert_fda_productid_stable` | FDA PRODUCTID doesn't renumber | KEEP |
| `assert_fda_eventlmd_correlates_with_content_change` | FDA no silent edits | KEEP |
| `assert_nhtsa_eleven_tuple_identity_stable` | NHTSA 11-tuple identity (ADR 0030) | KEEP — ~1 AC DELCO drift/day baseline |
| `assert_pre_2008_seven_tuple_unique` | NHTSA pre-2008 7-tuple uniqueness | KEEP |
| `assert_no_join_key_erasure_usda` | USDA join-key fields not cleared upstream | KEEP — erasure tripwire (n=1 confirmed) |
| `assert_usda_field_last_modified_date_advances_on_edit` | USDA `last_modified_date` reliability (U3) | KEEP — confirmed unreliable; silver uses `extraction_timestamp` |
| `assert_usda_bilingual_atomic_update` | EN/ES `last_modified_date` sync (U2) — **re-framed 2026-06-09 as rate-ceiling monitor** (threshold 0.25, ~2x the ~13.3% benign baseline); emits a breach row only on material drift. Per-pair detail at `scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql`. | KEEP — green at steady state; breach = investigate FSIS translation cadence |
| `assert_usda_multi_establishment_recalls` | USDA recalls naming 2+ establishments — baseline **4** (2026-06-09). Resolution model picks one establishment per recall today; multi-establishment recalls genuinely have several (C4 grain-mismatch reasoning). A material increase signals the multi-establishment population is no longer a rounding error and the per-element resolution work is worth revisiting. Detail: `scripts/sql/usda_recalls/silver/inspect_multi_establishment_recalls.sql`. | KEEP — benign at baseline; increase = investigate resolution grain |
| `assert_fda_nhtsa_have_firms` | FDA and NHTSA recall_events must each carry ≥1 firm in `recall_event_firm` — **baseline 0** (hard invariant). FDA always names an establishment; NHTSA always names filer+manufacturer. A firmless FDA/NHTSA recall is a firm-extraction regression. USDA/CPSC/USCG excluded (documented firmless baselines: ~426/~37/~9). Detail: `scripts/sql/cross_source/silver/inspect_firmless_recalls.sql`. | KEEP — 0 baseline; any row = regression, investigate immediately |

## 2. Silver field watch-lists (5) — `dbt/models/silver/_silver.yml`

Nullable-by-reality fields tested `not_null` at `warn` so the count is a visible watch-list.

| Test | Baseline | Verdict |
|---|---|---|
| `recall_event.announced_at` not_null | ~20 — two classes: ~6 FDA archive recalls with no `recall_initiation_dt` + ~14 with a dropped-century typo (year 7/12/13/212) nulled in recall_event.sql (6e.5) | KEEP — `published_at` carries the date; nulling garbage beats trusting it (precision-over-recall) |
| `firm_fda_attributes.firm_country_nam` not_null | **0** (was 1; Visaris DOO → Serbia override, 6e.5) | KEEP — stays `warn` to flag the next null-country foreign firm |
| `firm_fda_attributes.firm_legal_nam` / `firm_city_nam` / `firm_line1_adr` not_null | ~0–handful (≈100% on full corpus) | KEEP — watch-list for FEI-grain address gaps |

## 3. Staging enum / null watch-lists (7) — `dbt/models/staging/*.yml`

`accepted_values` / `not_null` at `warn` — guard against a **new enum value** appearing without breaking the build.

| Test | Watches | Verdict |
|---|---|---|
| `stg_nhtsa_recalls.rcltype` accepted_values | NHTSA recall-type code set | KEEP — new code = investigate, not break |
| `stg_nhtsa_recalls.influenced_by` accepted_values | MFR/ODI/OVSC/ISSUE_INVGSTN | KEEP |
| `stg_usda_fsis_recalls.recall_type` accepted_values | USDA recall-type set | KEEP |
| `stg_usda_fsis_establishments.size` accepted_values | establishment size enum | KEEP |
| `stg_uscg_recalls.severity` accepted_values | **CLEARED** — `stg_uscg_recalls.sql` now NULL-casts out-of-set severity values (not in H/L/M/S) per ADR 0027 (bronze keeps raw). The accepted_values [H,L,M,S] test no longer returns the out-of-set row. See `documentation/silver_design_notes.md` §6 (sentinel/NULL-cast note). | ~~KEEP~~ **CLEARED 2026-06-09** |
| `stg_fda_recalls.event_lmd` not_null | FDA null-EVENTLMD archive tail | KEEP — Finding P; sound as-is |
| `stg_fda_recalls.initial_firm_notification_txt` not_null | FDA notification-method gaps | KEEP |

## Promotion / removal triggers

- A `warn` becomes a candidate for **error** promotion if its count holds at 0 across ~4 weeks of production
  (e.g., `firm_country_nam` after the override). Tracked for Phase 7.
- A monitor becomes a candidate for **removal** only if silver provably no longer depends on the assumption
  *and* the source signal has no diagnostic value — none qualify today.
