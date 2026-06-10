# Source-stability assumption audit

What does this pipeline trust about its upstream sources, and how would we
know if those trusts broke?

This document is the canonical catalogue of every load-bearing assumption
the pipeline makes about CPSC, FDA, USDA, NHTSA, and USCG — what each assumption
is, what evidence supports or refutes it today, what mechanism falsifies
it, what threshold triggers downstream action, and what would have to
change in the implementation if it were violated.

Companion artifacts:

- **ADR 0031** — silver-row fragmentation strategy (Tier 1 prevention,
  Tier 2 detection, Tier 3 reconciliation framework). This audit closes
  the "TBD" cells in ADR 0031's per-source detection-status column.
- **Per-source findings docs** — `documentation/cpsc/array_stability_findings.md`,
  `documentation/fda/productid_stability_findings.md`,
  `documentation/usda/bilingual_and_lmd_findings.md`. These hold the
  empirical baselines from running the falsification scripts.
- **Detection scripts** — `scripts/sql/<source>/bronze/assert_*.sql`
  (rich diagnostic, operator-facing) + `dbt/tests/source_assumptions/assert_*.sql`
  (thin pass/fail, CI-facing at `severity=warn`).

## Why this exists

Three things prompted the audit:

1. **TODO.md item, lines 39-41** — the canonical question: "The CPSC
   ordinal pattern is stable as long as CPSC doesn't reorder its
   products array … investigate empirically." Treating one source's
   open question as the entry point to a multi-source pattern.
2. **Some assumptions are already known to be partially-violated** —
   FDA's `EVENTLMD` is bumped by an archive migration (ADR 0023);
   ADR 0026 mentions a 13.3% USDA bilingual non-atomic-update rate
   that's never been re-computed; USDA Finding E is explicitly
   deferred and never probed. Untested gaps will compound into
   Phase 6 silver `recall_event_history` correctness if not
   measured first.
3. **NHTSA already provides the template.**
   `scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql`
   uses `count(distinct raw_landing_path) > 1` to isolate cross-run
   drift from within-run structural multiplicities. The same pattern
   transfers to CPSC/FDA/USDA with one query per assumption.

## How to read each assumption row

| Field | Meaning |
|---|---|
| **#** | Stable identifier (e.g., C2, F1, U3, N1) so other docs can cite assumptions without ambiguity. |
| **Assumption** | The trust statement — what the pipeline assumes is true about the source. |
| **Why load-bearing** | Where in the pipeline that trust is wired in (file path, ADR). Removing the trust would require changing this code. |
| **Evidence today** | What we actually know — verified, partially-violated, untested, or untestable. Honest about gaps. |
| **Falsification script** | The Tier 2 mechanism that would surface a violation. Links to both rich `.sql` and dbt singular test. |
| **ADR 0031 threshold** | At what observed violation rate the pipeline must respond (Phase 6 reconciliation). |
| **Downstream impact if violated** | What code/ADR/Phase changes if the assumption fails. |

---

## CPSC

### C1 — `RecallNumber` (`source_recall_id`) is stable upstream

| | |
|---|---|
| **Why load-bearing** | Bronze identity is `(source_recall_id,)` per `src/extractors/cpsc.py:234`. Silver `recall_event_id = md5('CPSC' || source_recall_id)`. |
| **Evidence today** | 1,193 rows analyzed (`documentation/cpsc/first_extraction_findings.md`); no observed violations. |
| **Falsification script** | Implicit — natural-key violation would surface as duplicate `recall_event_id` rows in silver, breaking the silver primary-key dbt test. |
| **ADR 0031 threshold** | Not separately tracked; failure is structurally caught by silver tests. |
| **Downstream impact** | Catastrophic — entire CPSC silver branch built on this. Recovery: switch to a content-based event surrogate. |

### C2 — `products[]` array is append-only

| | |
|---|---|
| **Why load-bearing** | Silver `recall_product_id` includes `product_ordinal` from `LATERAL jsonb_array_elements WITH ORDINALITY` (`dbt/models/silver/recall_product.sql:38-46`). ADR 0031:96 calls this an *implicit* assumption. |
| **Evidence today** | **Currently untestable on observed data** — every CPSC recall has exactly 1 product (`first_extraction_findings.md` Section A), so the failure mode physically cannot occur. |
| **Falsification script** | `scripts/sql/cpsc/bronze/assert_products_array_append_only.sql` + dbt `assert_cpsc_products_array_append_only.sql` |
| **ADR 0031 threshold** | >0.1% silver row count fragmented per quarter |
| **Downstream impact** | Mid-array insertion in a recall with N products fragments N-1 silver rows. Phase 6 reconciliation triggers; likely fix: switch to content-based product surrogate (`md5(name‖description‖model‖number_of_units)`), unifying CPSC's recipe structurally with NHTSA's. Affects `dbt/models/silver/recall_product.sql:21-54`. |

### C3 — Product `name` and `model` strings are not character-normalized after publication

| | |
|---|---|
| **Why load-bearing** | Same surrogate as C2 — raw `name`/`model` are inputs to the md5. Independent of array order. |
| **Evidence today** | Untested (no script existed before this audit). |
| **Falsification script** | `scripts/sql/cpsc/bronze/assert_name_model_normalization_stable.sql` + dbt `assert_cpsc_name_model_normalization_stable.sql` |
| **ADR 0031 threshold** | Same as C2 (treated as same fragmentation class). |
| **Downstream impact** | Same as C2 + product-level fuzzy resolution becomes a Phase 6 deliverable item alongside firm-level resolution at `implementation_plan.md:606-610`. |

### C4 — `LastPublishDate` advances on edits

| | |
|---|---|
| **Why load-bearing** | Originally intended as the incremental watermark per ADR 0010. |
| **Evidence today** | **Already empirically false.** `documentation/cpsc/last_publish_date_semantics.md` documents that `LastPublishDate` does not advance on edits. **Mitigation in place**: mandatory weekly deep-rescan (ADR 0010). |
| **Falsification script** | N/A — already known false; mitigated structurally. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | None additional; mitigation already deployed. |

---

## FDA

### F1 — `PRODUCTID` is permanent and never renumbers (contractual)

| | |
|---|---|
| **Why load-bearing** | FDA bronze identity is `(source_recall_id,)` where `source_recall_id = PRODUCTID` per `src/schemas/fda.py:88`. Silver: `md5('FDA' || PRODUCTID)` per `recall_product.sql:58`. The entire FDA silver branch depends on this. |
| **Evidence today** | 2,705 rows / 2,692 unique PRODUCTIDs in first extraction (`documentation/fda/first_extraction_findings.md`); no observed violations. |
| **Falsification script** | `scripts/sql/fda/bronze/assert_productid_stable.sql` + dbt `assert_fda_productid_stable.sql` |
| **ADR 0031 threshold** | Any non-zero rate (FDA stability is contractual). |
| **Downstream impact** | Catastrophic — Phase 6 must redesign FDA silver to a content-based or `recall_event_id`-grouped surrogate. ADR 0007's hash-exclusion machinery may need PRODUCTID added. |

### F2 — `EVENTLMD` advances only on real edits

| | |
|---|---|
| **Why load-bearing** | Used as the incremental-extraction watermark per `src/extractors/fda.py:269-270`. |
| **Evidence today** | **Already partially false.** ADR 0023 documents an active archive migration bumping `EVENTLMD` on records dated 2002–2019 without changing content. Mitigation: weekly deep-rescan + 5,000-row count guard at `tests/extractors/test_fda_extractor.py:127`. |
| **Falsification script** | `scripts/sql/fda/bronze/assert_eventlmd_correlates_with_content_change.sql` + dbt `assert_fda_eventlmd_correlates_with_content_change.sql` (test alerts only on Cell C — silent edits). |
| **ADR 0031 threshold** | Not in ADR 0031 (this is a noise-quantification assumption, not a fragmentation one — silver dedupes on content_hash). Cell C (silent edits) > 0 is the actionable signal. |
| **Downstream impact** | If silent edits exist (Cell C > 0): ADR 0010's deep-rescan cadence is the only data-loss mitigation; revisit cadence. If archive-migration noise (Cell B) accelerates relative to real edits (Cell A): revisit count guard. |

### F3 — `PRODUCTLMD` is null on initial creation, populates on first edit

| | |
|---|---|
| **Why load-bearing** | Documented FDA contract per `documentation/fda/api_observations.md:145-147`. Would be the per-record edit timestamp for Phase 6 `recall_event_history` if available. |
| **Evidence today** | **Empirically falsified on the lookup-endpoint surface (re-verification 2026-05-09, rebaseline-filtered).** Bruno re-runs of `get_product_by_id.yml` against 5 productids surfaced by `scripts/sql/fda/bronze/find_real_edit_productids.sql` (post-rebaseline filter mirroring `dbt/tests/source_assumptions/assert_fda_eventlmd_correlates_with_content_change.sql:26-27`) returned `PRODUCTLMD: null` for all 5. Two distinct events (98540 Philips Respironics CDRH, 98497 American Laboratories CFSAN), two distinct FDA centers, multiple recall types. The rebaseline filter did real work: an initial unfiltered probe included productid 218953 whose hash deltas turned out to be `schema_rebaseline`/`hash_helper_rebaseline` artifacts (ADR 0027); after filtering, 218953 dropped out and 219028 swapped in — all 5 productids in the rebaseline-clean population still nulled. FDA does not populate `PRODUCTLMD` even on records its own bronze content shows it has edited. Earlier observations (Findings H + K0 at `api_observations.md:146`, `:306`) showed null on un-edited products; FDA's native field-history endpoints returned `RESULTCOUNT: 0` across 4 events spanning 2002-2026 (Finding L, `api_observations.md:300-328`). Full re-verification table at `api_observations.md` K0.1. |
| **Falsification script** | Not applicable — capture decision deferred per `api_observations.md` K0.1 (no information-gain expected from adding the column). |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | None today, none expected. ADR 0007 was amended 2026-04-26 to abandon the FDA-history-endpoints lineage strategy; Phase 6 `recall_event_history` (per `project_scope/implementation_plan.md:611`) uses bronze-snapshot synthesis with `LAG()`, treating FDA the same as the other four sources. PRODUCTLMD plays no role. |
| **Follow-up** | Closed per `api_observations.md` K0.1. Re-open if a `content_hash`-edited product in `fda_recalls_bronze` ever returns a non-null `PRODUCTLMD` from the lookup endpoint. |

### F4 — FDA does not delete records

| | |
|---|---|
| **Why load-bearing** | ADR 0026 lifecycle tracking assumes records persist; "deletion" only signals via record absence from a snapshot. |
| **Evidence today** | 134k+ records persistent across snapshots; no observed deletions. |
| **Falsification script** | Implicit — `extraction_runs` row-count drop or ADR 0026 manifest signal would surface deletions. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | If FDA starts deleting: ADR 0026's signal interprets the absence; Phase 6 silver lifecycle handles the `is_currently_active=false` semantic. |

### F5 — `RID` is a query-position counter, not a record property

| | |
|---|---|
| **Why load-bearing** | Confirmed (`api_observations.md:108-115`); excluded from content_hash via `hash_exclude_fields={"rid"}` per `src/extractors/fda.py:257-264`. |
| **Evidence today** | Verified by Finding F. |
| **Falsification script** | N/A — already mitigated. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | None; mitigation already deployed. |

---

## USDA

### U1 — `field_recall_number` is stable; `(source_recall_id, langcode)` is the natural identity

| | |
|---|---|
| **Why load-bearing** | Composite bronze identity per `src/extractors/usda.py:311-318` (ADR 0006). |
| **Evidence today** | 2,003 rows, 789 bilingual pairs, 0 Spanish orphans (`documentation/usda/first_extraction_findings.md`); composite key works in production. |
| **Falsification script** | Implicit — violation would surface as duplicate `recall_event_id` in silver. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | None observed. |

### U2 — Bilingual EN/ES siblings are atomically updated

| | |
|---|---|
| **Why load-bearing** | ADR 0026's `recall_event_history` model (Phase 6) currently plans to track lifecycle per `source_recall_id`, not per `(source_recall_id, langcode)`. Atomicity makes this safe. |
| **Evidence today** | Documented in `recall_api_observations.md:163-164`; ADR 0026 references "13.3% non-atomic-update rate" but the figure has not been re-computed against current bronze. |
| **Falsification script** | `scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql` + dbt `assert_usda_bilingual_atomic_update.sql` |
| **ADR 0031 threshold** | Not in ADR 0031 (history-correctness, not fragmentation). Findings doc tracks. |
| **Downstream impact** | If non-atomic rate > ~25%: Phase 6 `recall_event_history` model must key on `(source_recall_id, langcode)`. ADR 0006 also needs amendment for orphan-detection semantics. |

### U3 — `last_modified_date` reliably advances when FSIS amends content

| | |
|---|---|
| **Why load-bearing** | Would be the per-record edit timestamp for Phase 6 `recall_event_history` if reliable. |
| **Evidence today** | **Explicitly deferred and never probed** (`recall_api_observations.md:140-151`, Finding E). |
| **Falsification script** | `scripts/sql/usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql` + dbt `assert_usda_field_last_modified_date_advances_on_edit.sql` |
| **ADR 0031 threshold** | Not in ADR 0031. Findings doc tracks. |
| **Downstream impact** | If reliability < 95%: Phase 6 falls back to bronze `extraction_timestamp` for USDA history. Per-record granularity loss but no incorrectness. |

### U4 — USDA does not delete records

| | |
|---|---|
| **Why load-bearing** | Same as F4 — ADR 0026 lifecycle assumption. |
| **Evidence today** | Full dump returns ~2,001 records every run; no deletions observed. |
| **Falsification script** | Implicit — manifest signal via ADR 0026. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | Same handling as F4. |

### U5 — Single-row-per-recall grain → no product-level fragmentation possible

| | |
|---|---|
| **Why load-bearing** | `recall_product.sql:78-97` sets `recall_product_id = recall_event_id` (1:1). |
| **Evidence today** | Structural — `product_items` is a free-text blob (ADR 0002 defers structured parsing). |
| **Falsification script** | N/A. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | If ADR 0002's deferred parsing ever lands, U5 stops being a structural truth and product-level fragmentation analysis becomes relevant for USDA. |

---

## NHTSA

### N1 — 11-tuple identity is row-unique within corpus and stable across runs

| | |
|---|---|
| **Why load-bearing** | ADR 0030 bronze identity. Silver `recall_product_id` is the md5 of the same 11 fields per `recall_product.sql:99-131`. |
| **Evidence today** | Verified end-to-end via `documentation/nhtsa/incremental_delta_findings.md` Section G. Empirical drift baseline: ~0.0005%/day (1 case in 240k — the AC DELCO `maketxt` normalization documented 2026-05-08 in ADR 0031:84). |
| **Falsification script** | Existing: `scripts/sql/nhtsa/bronze/assert_eleven_tuple_identity_stable.sql`, `assert_nine_tuple_identity_stable.sql`. dbt wrapper: `assert_nhtsa_eleven_tuple_identity_stable.sql`. |
| **ADR 0031 threshold** | >0.01% silver row count fragmented per month, OR systematic drift on a previously-stable field. |
| **Downstream impact** | If drift rate > threshold: ADR 0031 threshold-revisit policy kicks in (every 6 months or on threshold breach). May demote the drifting field from the identity tuple. |

### N2 — `bgman`/`endman` for a given (campno + 9-tuple) don't get re-edited

| | |
|---|---|
| **Why load-bearing** | These two fields serve as NHTSA's batch-level disambiguator in the 11-tuple identity (analog to CPSC's `product_ordinal`). |
| **Evidence today** | Empirically violated at ~0.0005%/day (the AC DELCO case). Bounded; monitored. |
| **Falsification script** | Same as N1. |
| **ADR 0031 threshold** | Same as N1. |
| **Downstream impact** | Same as N1. |

### N3 — `RECORD_ID` is a regen counter, not a stable identifier

| | |
|---|---|
| **Why load-bearing** | Excluded from content_hash per ADR 0030; would otherwise cause every TSV regeneration to look like a wholesale rewrite. |
| **Evidence today** | Confirmed via Finding K (`documentation/nhtsa/flat_file_observations.md`). Mitigation in place. |
| **Falsification script** | N/A — already mitigated. |
| **ADR 0031 threshold** | N/A. |
| **Downstream impact** | None additional. |

---

## USCG

USCG contributes three bronze sources: recalls, manufacturers (listing), and manufacturer details.
The MIC (Manufacturer Identification Code) is the identity anchor for the manufacturer surfaces.
Key findings backing these assumptions: `documentation/uscg/` findings docs; ADR 0035 §5 (SCD-2
for `firm_uscg_attributes`); ADR 0031 USCG row; `source_assumption_audit.md` USCG row in the
summary table below.

### W1 — MIC is a stable, permanent identifier for a given manufacturer

| | |
|---|---|
| **Why load-bearing** | Bronze identity for manufacturer and manufacturer-detail sources uses MIC as the anchor. Silver `firm_uscg_attributes` uses MIC as its SCD-2 unique key, and `uscg_mic_reassignment_years` derives the reassignment signal from it. |
| **Evidence today** | **Empirically violated (observed-forward cases documented).** USCG recycles MICs across businesses — AXY and COP are confirmed historical reassignment examples (2026-05-30 confirmation; `uscg_mic_temporal_identity.md`). The static source-native recycle surface: 365 prior-recycled + 221 out-of-boundary on 718 sampled MICs (ADR 0035 §5). |
| **Falsification script** | `dbt/tests/source_assumptions/assert_mic_holder_stable.sql` — detects a MIC whose `company_name` differs across its SCD-2 snapshot versions (the *forward-observed* half); `assert_uscg_mic_reassignment_flag_present.sql` enforces the static source-native recycle surface (the *historical* half). |
| **ADR 0031 threshold** | Any new reassignment observed in the forward snapshot is the signal (the SCD-2 Type-2 history exists specifically to bank them; a fresh row is a feature, not a failure). |
| **Downstream impact** | MIC recycling is the primary reason `firm_uscg_attributes` uses a full SCD-2 snapshot — a recycled MIC produces a new snapshot version with a new `canonical_firm_id`, keeping the prior holder's history intact. The `uscg_mic_reassignment_years` model derives the safe join window from `valid_from`/`valid_to`. HIN-embedded MIC lookups must respect the temporal window (see `uscg_mic_temporal_identity.md`). |

### W2 — MIC holder name is stable within a snapshot version

| | |
|---|---|
| **Why load-bearing** | `firm_uscg_attributes_snapshot` uses `company_name` as a `check_cols` field; a company_name change within the same MIC tenure (e.g. a DBA rename without a formal MIC reassignment) produces a new SCD-2 version. |
| **Evidence today** | ~0 observed today (snapshot just initialized → 1 version per MIC). The forward-observed monitor (`assert_mic_holder_stable`) will catch any that arrive. |
| **Falsification script** | `dbt/tests/source_assumptions/assert_mic_holder_stable.sql` — returns reassigned MICs; ~0 at baseline (`severity=warn`, inherited from the `source_assumptions/` directory). |
| **ADR 0031 threshold** | Warn on first non-zero result; treat each returned MIC as a new SCD-2 version to verify. |
| **Downstream impact** | Each new SCD-2 version is correct behavior by design; the monitor just surfaces it for review. No code change needed unless the rate becomes high enough to question the SCD-2 anchor choice. |

---

## Cross-source monitors

### X1 — Every FDA and NHTSA recall_event carries at least one firm (C20)

| | |
|---|---|
| **Why load-bearing** | Both FDA and NHTSA always name a recalling firm (FDA establishment, NHTSA filer + manufacturer). A firmless FDA/NHTSA recall indicates a firm-extraction regression, not a legitimate data gap. |
| **Evidence today** | **Baseline = 0 firmless recalls** for FDA and NHTSA (verified 2026-06-09). |
| **Falsification script** | `dbt/tests/source_assumptions/assert_fda_nhtsa_have_firms.sql` — returns any FDA/NHTSA `recall_event` with zero rows in `recall_event_firm`; `severity=warn` inherited from the directory. Probe: `scripts/sql/cross_source/silver/inspect_firmless_recalls.sql`. |
| **ADR 0031 threshold** | Any non-zero count is actionable (the documented firmless baselines for USDA ~426 / CPSC ~37 / USCG ~9 are excluded from this assertion). |
| **Downstream impact** | A non-zero result means the firm-extraction logic for FDA or NHTSA has regressed; investigate `recall_event_firm` joins and the relevant silver models. |

---

## Summary table — empirical status (baselined 2026-05-08)

| # | Source | Status | Empirical baseline | Notes |
|---|---|---|---|---|
| C1 | CPSC | **Verified** | 1,357 distinct recalls, no duplicates | Natural key works as designed |
| C2 | CPSC | **Untestable on observed data** | All 1,357 recalls have exactly 1 product | Failure mode physically can't fire on 1-element arrays; first observed CPSC edit (recall `00015`, 2026-05-07/08, retailers text typo) didn't touch products[] |
| C3 | CPSC | **One positive signal** | 0/1 cross-run cases violated | Only 1 cross-run case in corpus (recall `00015`); name/model byte-stable across the edit |
| C4 | CPSC | Already false → mitigated | Re-confirmed by recall `00015` | CPSC fixed a typo on 2026-05-08 without advancing `last_publish_date`; deep-rescan policy correct |
| F1 | FDA | **Verified** | 0 PRODUCTID renumbers across 5,529 bronze rows / 2,924 distinct PRODUCTIDs / 7 runs | "Any non-zero rate" threshold not at risk |
| F2 | FDA | **Verified within routine path** | 41 real edits, 0 silent edits, 0 archive-migration noise (all post rebaseline-filter) | Pre-filter showed 2,535 false silent edits; all attributable to 2026-05-01 architecture realignment (ADR 0027). Cell B = 0 contradicts ADR 0023's archive-migration narrative — worth periodic re-check |
| F3 | FDA | **Empirically falsified on lookup-endpoint surface** | n/a | Re-verification 2026-05-09 (rebaseline-filtered): 5/5 productids in the routine-only population (records of demonstrated FDA edits per `content_hash` deltas) returned `PRODUCTLMD: null`. Two events / two FDA centers (Philips Respironics Devices, American Laboratories Food). Filter did real work — productid 218953 dropped out as a rebaseline-only phantom; 219028 swapped in. Adds to prior null-on-un-edited and zero-rows-from-history-endpoints evidence (Findings H/K0/L). ADR 0007 already amended 2026-04-26 to bronze-snapshot synthesis; capture deferred per `api_observations.md` K0.1. |
| F4 | FDA | Verified | No deletions observed across 7 runs | Lifecycle handled via ADR 0026 manifest |
| F5 | FDA | Verified → mitigated | N/A | RID excluded from hash |
| U1 | USDA | Verified | 2,003 distinct (recall_id, langcode), 0 collisions | Composite key works |
| U2 | USDA | **Substantively false** | 13.31% non-atomic (105/789 pairs) | **Structural divergence**, not transient — top-10 EN/ES gaps range 740–1,701 days; 35% of recalls are EN-only entirely. Phase 6 lifecycle MUST key per `(recall_id, langcode)`; ADR 0026's design already does this |
| U3 | USDA | **Empirically violated** | 1/1 routine content edit was the U3 failure mode (PHA-04092026-01 `active_notice` flip without `last_modified_date` advance) | n=1 small but the case is qualitatively load-bearing — a lifecycle-meaningful field flipped without date advance. Independent corroboration (project owner's prior FSIS experience) confirms the "edit shortly after publish without bumping date" is a known FSIS pattern. Phase 6 must use `extraction_timestamp` for USDA history |
| U4 | USDA | Verified | Full dump returns ~2,001 records every run | No deletions |
| U5 | USDA | Structural truth (ADR 0002) | N/A | Not product-grained |
| N1 | NHTSA | Verified with bounded drift | **3 drift groups in bronze (~0.00125% of 240k rows), 2026-05-09** | Drift profile differs from ADR 0031's original baseline — see below |
| N2 | NHTSA | Empirically violated at small bounded rate | Same as N1 | Monitored via existing assertions; current cases are 2× `mfr_comp_ptno` (Ferrari 12Cilindri privacy-window part-number typo correction) + 1× `endman` (Western Star manufacturing-window extension — exactly the trade-off ADR 0031:94 anticipated) |
| N3 | NHTSA | Verified → mitigated | N/A | RECORD_ID excluded from hash |
| W1 | USCG | **Empirically violated (by design — SCD-2 handles it)** | AXY/COP confirmed recycled; 365 prior + 221 OOB on 718 sampled MICs (ADR 0035 §5) | SCD-2 snapshot banks each tenure; `assert_mic_holder_stable` monitors forward-observed reassignments |
| W2 | USCG | **~0 observed (snapshot just initialized)** | 1 version per MIC at baseline | Forward monitor active; any new version triggers a review |
| X1 | Cross-source | **Verified at baseline — 0 firmless FDA/NHTSA recalls** | 0 firmless recalls as of 2026-06-09 | USDA/CPSC/USCG documented firmless baselines excluded from assertion |

### Cross-cutting findings from the baseline run

- **The 2026-05-01 architecture realignment (ADR 0027) caused a hash-rebaseline wave** that shows up in every LAG-based assertion as false content-edit transitions. Affected scripts (FDA F2, USDA U3) now filter `change_type IN ('schema_rebaseline', 'hash_helper_rebaseline')` before LAG. Phase 6 `recall_event_history` will need the same filter (`implementation_plan.md:611`).
- **ADR 0023's archive-migration narrative isn't visible in current bronze** (Cell B = 0 for FDA F2). The migration may have completed or its EVENTLMD bumps may coincide with content changes. Re-check periodically as bronze accumulates.
- **The first observed CPSC edit landed via the routine extraction path** despite `last_publish_date` not advancing — suggests the incremental query window is wider than strict `> watermark` (likely `>=` at boundary). Doesn't change the deep-rescan policy but is a useful operational detail.
- **USDA bilingual non-atomicity is structural, not transient.** ADR 0026's empirical motivation is now confirmed and characterized; the design (per-langcode manifest) was correct.
- **NHTSA drift baseline measured against two different substrates.** ADR 0031's original "1 case (AC DELCO)" baseline was from `cross_corpus_stability.py` comparing two raw TSV captures in `data/exploratory/nhtsa/`. The bronze-level assertion (`assert_eleven_tuple_identity_stable.sql` + dbt wrapper) measures drift among rows actually loaded into bronze; on 2026-05-09 it shows 3 different cases (none of them AC DELCO). Both substrates are useful — TSV-substrate captures source-side edits before bronze, bronze-substrate captures what silver actually has to deal with. They're not directly comparable; document each separately.

## Standing requirements

- **For each new source** (future beyond USCG): the source's silver-landing PR must include an entry in this audit document — assumption table, falsification scripts, threshold, downstream impact. Mirrors ADR 0031's per-source-table standing requirement.
- **Threshold revisit**: every 6 months (or on first non-zero baseline result), each source's findings doc should be re-checked; thresholds tightened or Phase 6 reconciliation triggered.
- **Untestable-today gaps** (F3): convert to ADR proposals when revisited; close out as testable assumptions land.

## Out of scope

- The Phase 6 reconciliation **mechanism** itself (mapping table vs. content-hash surrogate vs. fuzzy-match) — ADR 0031 explicitly defers mechanism choice until a Tier 2 trigger fires. This audit makes the trigger machinery exist; it doesn't choose the mechanism.
- EPA (deferred per ADR 0001).
- Migrating the falsification scripts to a dedicated DQ framework — Soda Core ([ADR 0040](decisions/0040-data-quality-framework-soda-core.md)) is now adopted for the bronze/anomaly layer; the dbt singular tests remain the silver/gold transform-layer observability surface.
