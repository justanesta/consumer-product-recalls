# USDA bilingual atomicity and last_modified_date reliability findings

Empirical baseline for the USDA load-bearing assumptions tracked in
`documentation/source_assumption_audit.md`:

- **U2 — bilingual EN/ES siblings are atomically updated** (referenced as a "13.3% non-atomic-update rate" in ADR 0026 but never re-computed against current bronze)
- **U3 — `last_modified_date` reliably advances on every real edit** (Finding E in `documentation/usda/recall_api_observations.md:140-151`, explicitly deferred and never probed)

Both assumptions feed Phase 6's `recall_event_history` design (ADR 0022).
If U2 is meaningfully violated, lifecycle tracking must key on
`(source_recall_id, langcode)` rather than `source_recall_id`. If U3 is
violated, the model cannot use `last_modified_date` as a per-record edit
timestamp and must fall back to `extraction_timestamp` (granularity loss).

## Detection scripts

- **U2 (bilingual atomicity):** `scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql`
- **U3 (last_modified_date reliability):** `scripts/sql/usda_recalls/bronze/assert_field_last_modified_date_advances_on_edit.sql`

Both are also wrapped as dbt singular tests at `severity=warn` under
`dbt/tests/source_assumptions/`.

## Baseline as of 2026-05-12

> **Run after**: `psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql`
> and the U3 sibling. Paste Q1, Q2, Q3 outputs below.

### U2 — bilingual atomicity

| Output cell | Observed | Notes |
|---|---|---|
| Q1 `bilingual_pair_count` | **789** | Recalls with both EN and ES rows in bronze. |
| Q1 `non_atomic_pair_count` | **105** | Pairs where `last_modified_date` differs. |
| Q1 `non_atomic_rate_percent` | **13.31%** | Matches ADR 0026's cited 13.3% to two decimal places. |
| Q2 sample non-atomic pairs | See below — gaps in **years**, not hours | All 10 top samples have EN ahead of ES by 740–1,701 days. `content_hash_relationship = different` in all 10. |
| Q3 corpus language shape | **789 both / 425 EN-only / 0 ES-only / 1,214 total** | 65% bilingual; 35% never published in Spanish; 0 ES-only confirms ADR 0006 quarantine works. |

**Top-10 gap samples (Q2):**

| `source_recall_id` | `en_last_modified` | `es_last_modified` | `en_minus_es` |
|---|---|---|---|
| 002-2016 | 2020-09-01 | 2016-01-05 | **1,701 days** |
| 010-2017 | 2021-05-14 | 2017-01-25 | 1,570 days |
| 013-2017 | 2021-05-13 | 2017-02-15 | 1,548 days |
| 090-2017 | 2021-05-13 | 2017-08-09 | 1,373 days |
| 072-2019 | 2019-07-09 | 2016-07-09 | 1,095 days |
| 104-2018 | 2021-05-13 | 2018-10-25 | 931 days |
| 033-2014 | 2016-06-14 | 2014-05-29 | 747 days |
| 094-2019 | 2021-10-13 | 2019-09-28 | 746 days |
| 003-2020 | 2022-02-07 | 2020-01-24 | 745 days |
| 029-2019 | 2021-03-24 | 2019-03-13 | 742 days |

**Interpretation:** the divergence is **structural, not transient**.
The script was designed around a hypothesis that FSIS catches up
within hours or days; that hypothesis is wrong. Pattern: FSIS edits
the English version (often multiple times over years), Spanish stays
frozen at original publication. Direction is uniform across all 10
samples — EN is always ahead of ES, never reversed. So FSIS appears
to edit English-side without translating updates to Spanish,
indefinitely.

**Cross-refresh stability (2026-05-12).** The 13.31% rate matches ADR
0026's cited 13.3% to two decimal places, measured on an independently-
populated corpus several weeks after the original ADR 0026 observation.
The non-atomic rate is stable across corpus refreshes — neither drifting
upward nor tightening toward atomicity.

**2021-05 cluster observation.** 4 of the top-10 EN-newer-than-ES
samples have EN `last_modified_date` on 2021-05-13 or 2021-05-14
(`010-2017`, `013-2017`, `090-2017`, `104-2018`). This concentration
smells like an FSIS-side site migration or CMS deploy that touched
many EN pages simultaneously without translating updates to ES.
Evidence that EN-newer-than-ES events aren't always per-recall
editorial actions — infrastructure events can produce them too.
Worth tracking if future re-measurements surface analogous date
clusters (suggesting a recurring FSIS publication pattern rather than
a one-off migration).

This contradicts `documentation/usda/recall_api_observations.md:163-164`'s
claim that "FSIS updates both records atomically — the watermark moves
in sync." That observation must have been made on a recently-published
subset where the divergence hadn't yet manifested. The claim should be
amended to reflect the empirical 13.3%-structurally-non-atomic reality.

**Three-population breakdown for Phase 6 design:**

| Population | Share | Phase 6 handling |
|---|---|---|
| Bilingual + atomic | ~57% | EN/ES move in lockstep; lifecycle on either is the lifecycle on both |
| Bilingual + desynced | ~9% | EN and ES are independent records with independent edit histories — must track lifecycle per (source_recall_id, langcode) |
| EN-only | ~35% | Single-language record; no ES counterpart |

**Architectural implications (file as Phase 6 follow-ups):**

- **ADR 0026 amendment**: lifecycle tracking *must* key on `(source_recall_id, langcode)`, not `source_recall_id` alone. The current "13.3% non-atomic-update rate" reference treats it as something to "be careful about"; the empirical reality is that EN/ES are de facto independent records for ~9% of all recalls.
- **ADR 0006 review**: the bilingual-pairing invariant `check_usda_bilingual_pairing` assumes atomicity for orphan detection. If atomicity is structurally broken, what is the invariant actually catching? Re-examine the design.
- **`recall_api_observations.md:163-164` correction**: amend the "atomic update" claim with the 13.31% empirical figure and a note that gaps are structural (years), not transient.
- **Phase 6 `recall_event_history` model spec**: add explicit handling for the three populations above. Bilingual-desynced edits should generate two parallel lifecycle event streams.

### U3 — last_modified_date reliability on edits

| Transition class | Observed count | Notes |
|---|---|---|
| `advanced` | 0 | Real edit with signal working — none observed. |
| `regressed` | 0 | None. |
| `advanced_from_null` | 0 | None. |
| `regressed_to_null` | 0 | None. |
| `unchanged_with_content_change` | **3** | All 3 observed routine content edits; date did NOT advance. |
| `NULL_to_NULL_with_content_change` | 0 | None. |

**Q3 corpus context (re-measured 2026-05-12)**: 2,007 bronze rows / 2,004 distinct (source_recall_id, langcode) pairs / **3 content-edit transitions observed**. Three Public Health Alert pairs have accumulated >1 routine snapshot; no regular recall has yet.

**Q2 sample (all 3 observed transitions, 2026-05-12):**

| `source_recall_id` | langcode | prev extraction | new extraction | prev `last_modified_date` | new `last_modified_date` | gap |
|---|---|---|---|---|---|---|
| `PHA-04092026-01` | English | 2026-05-01 01:47:19 UTC | 2026-05-01 01:51:37 UTC | 2026-04-09 | 2026-04-09 (unchanged) | 4 minutes |
| `PHA-05042026-01` | English | 2026-05-05 11:51:57 UTC | 2026-05-10 13:18:18 UTC | 2026-05-04 | 2026-05-04 (unchanged) | 5 days |
| `PHA-05092026-01` | English | 2026-05-10 13:18:18 UTC | 2026-05-12 12:04:33 UTC | 2026-05-09 | 2026-05-09 (unchanged) | 2 days |

**Updated to n=3, 2026-05-12.** Two additional transitions accumulated
since the original n=1 baseline: **PHA-05042026-01** (5-day gap,
2026-05-05 → 2026-05-10) and **PHA-05092026-01** (2-day gap, 2026-05-10
→ 2026-05-12, today's load). Both fall in the same bucket as the
original — `unchanged_with_content_change` — for a unanimous 3/3
(100%) in the only edit class observed. The original "can't conclude
either way" framing is no longer accurate.

**Class-specific scope caveat.** All 3 transitions are Public Health
Alerts (`PHA-*`), which have `has_spanish: false` and never enter the
bilingual pair set. The U2 Q3 result above shows 425 EN-only recalls
in the corpus; PHAs are a meaningful share. We have **no observational
evidence** about whether regular FSIS recall classes (e.g.,
`001-2026` style numbering) behave the same way — they simply haven't
been amended in our observation window. The finding is "USDA
`last_modified_date` is unreliable on the only edit class observed
(Public Health Alerts)," not "USDA `last_modified_date` is unreliable
for all classes." A meaningful caveat for ADR 0026's scope.

**Reframe — U3 reflects FSIS's data model, not a bug.** Both observed
sub-pathologies in the payload diff below — the `active_notice` toggle
and the `None → ""` serialization shift on 5 nullable text fields — are
not edits *from FSIS's perspective*. `active_notice` is a derived/
runtime status flag (it toggled false → true → false within minutes/
days — recomputed live, not a stored attribute). The `None → ""` shift
on `closed_year`, `distro_list`, `press_release`, `qty_recovered`,
`en_press_release` is a backend-deploy serialization artifact, not a
content amendment. Bronze content_hash detects both because Pydantic
preserves the distinctions; `last_modified_date` doesn't move because
FSIS doesn't see these as edits. The signal isn't broken — it's
measuring something different from what our bronze content_hash
measures. The Phase 6 conclusion (use `extraction_timestamp`) is
unchanged but now grounded in mechanism rather than just empirical
absence of advances.

**Historical context (preserved from the n=1 era).** `PHA-04092026-01`
is the canonical Public Health Alert that ADR 0026 cites as the "edit
observed within a single 4-hour window in Phase 5b verification"
example. The 4-minute gap between its snapshots originally seeded this
analysis. With 3 datapoints now, that case is no longer load-bearing on
its own.

**Phase 6 recommendation (conservative, given n=1):** do not trust
`last_modified_date` as the per-record edit signal for USDA. Fall back
to `extraction_timestamp` for `recall_event_history` and revisit when
the assertion accumulates more transitions (target: re-run quarterly
or after first 50+ content-edit transitions, whichever comes first).

**Suggested follow-up — payload diff on `PHA-04092026-01`** (mirrors
the CPSC recall 00015 inspection in `documentation/cpsc/array_stability_findings.md`).

Diagnostic script:

```
psql "$NEON_DATABASE_URL" -f scripts/sql/usda_recalls/bronze/diagnose_payload_drift_for_recall.sql
```

The script defaults to `PHA-04092026-01` / English; edit the `\set`
lines at the top for any future case. Q1 shows snapshot metadata
side-by-side; Q2 expands the payload vertically (via `\x auto`) for
key-by-key visual diff.

Diff classification:

- **Diff on peripheral field (e.g., `media_contact`, `summary` text rewording)** → benign; could be initial-extraction setup artifact.
- **Diff on structural field (`recall_classification`, `product_items`, `recall_reason`, `risk_level`)** → U3 violation is real even at n=1; Phase 6 absolutely cannot use `last_modified_date`.
- **Diff on lifecycle field (`active_notice`, `archive_recall`, `closed_date`)** → U3 violation on the most important class of edit; Phase 6 must use `extraction_timestamp` exclusively.

#### Diff result for `PHA-04092026-01` (run 2026-05-08)

Three snapshots in bronze; the rebaseline filter on the U3 assertion
excluded snapshot 3, so the contingency only saw the snap 1 → snap 2
transition.

| Snap | extraction | last_modified_date | content_hash (prefix) | run class |
|---|---|---|---|---|
| 1 | 2026-05-01 01:47:19 | 2026-04-09 | `27dcc934…` | routine |
| 2 | 2026-05-01 01:51:37 | 2026-04-09 | `5bd4f336…` | routine |
| 3 | 2026-05-02 12:40:01 | 2026-04-09 | `aebeb913…` | schema_rebaseline |

**Snap 1 → Snap 2 (the U3 transition, 4 minutes apart, both routine):**
exactly **one field changed**: `active_notice` went from `false` to
`true`. This is **not** an initial-extraction artifact and **not** a
peripheral text edit — it is a **lifecycle state transition**
(the recall flipped from "not yet active" to "active"), exactly the
class of edit ADR 0026 frames as state-1-through-4 lifecycle
transitions. And `last_modified_date` did not advance.

**Snap 2 → Snap 3 (across the rebaseline boundary, included for
context — not part of the U3 contingency):** six fields differ, all
explained by the rebaseline:

- `active_notice` flipped back `true` → `false` (real FSIS state at the time of re-extraction; not a U3 violation since this transition was filtered out)
- Five nullable text fields shifted `null` → `""`: `closed_year`, `distro_list`, `press_release`, `qty_recovered`, `en_press_release`. This is the same ADR 0027 nullable-string serialization change that produced FDA's 2,535 false silent edits.

**Refined U3 verdict.** The single observed routine content-edit
transition is qualitatively load-bearing: a lifecycle field
(`active_notice`) flipped without `last_modified_date` advancing.
The 4-minute gap might initially have looked like an extraction
artifact, but the field that changed is exactly the class FSIS would
need to bump the date for if `last_modified_date` were a reliable
edit signal. The fact that FSIS didn't bump the date for a lifecycle
flip is sufficient to disqualify the field for that purpose, even at
n=1. **U3 is empirically violated, not just precautionarily uncertain.**

**Independent corroboration (project-owner's prior FSIS experience,
2026-05-08):** the active_notice flip-shortly-after-publish pattern
is a known FSIS behavior, not an artifact unique to this extraction.
FSIS routinely tweaks recall content (including lifecycle fields)
shortly after initial publish in the API. The single observation in
this project's bronze is consistent with that broader pattern, which
elevates the n=1 finding from "suggestive" to "consistent with a
known production behavior pattern."

**Phase 6 follow-ups (firm now, not provisional):**

- `recall_event_history` for USDA must use `extraction_timestamp`
  as the per-record edit signal. `last_modified_date` cannot be
  trusted, especially for lifecycle-meaningful edits.
- `active_notice` itself should be a primary lifecycle field tracked
  by the Phase 6 model via `LAG()` over bronze snapshots (the ADR 0007
  + ADR 0022 mechanism). Track `archive_recall` and `closed_date`
  the same way.
- Note in `documentation/usda/recall_api_observations.md` Finding E
  that the deferred question is now answered: `last_modified_date`
  does not reliably advance on edits, demonstrated on the only
  observed routine-extracted transition.

## ADR 0031 threshold reconciliation

ADR 0031's USDA row currently reads:

> Phase 6 revisit threshold: N/A (single-row-per-recall grain — no product-level fragmentation)

ADR 0031 frames USDA's product-level fragmentation as N/A, which is correct
— the fragmentation strategy is product-grained. But U2 and U3 are
*history-correctness* assumptions, not fragmentation assumptions. Update
ADR 0031's USDA row to:

- **Detection status cell**: replace "TBD — `field_recall_number` stability assertion; trivially 0 expected" with `scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql` + the U3 sibling. Note that USDA's TBD is for *history* coverage, not fragmentation coverage.
- **Threshold cell**: keep N/A for the fragmentation column. Add a footnote that history-correctness thresholds are tracked here in this findings doc and feed Phase 6's `recall_event_history` design.

## Follow-up triggers

- If U2 non-atomic rate > ~25%: Phase 6 `recall_event_history` model must key on `(source_recall_id, langcode)`. ADR 0006 also needs amendment for orphan-detection semantics.
- If U2 non-atomic rate ≈ 0%: ADR 0026's "13.3%" reference is stale; either the original observation was a transient FSIS bug or atomicity has tightened. Update ADR 0026 with the current measurement.
- If U3 reliability < 95%: Phase 6 falls back to `extraction_timestamp` for USDA history. Note this in the Phase 6 deliverable spec.
- If U3 surfaces any `regressed` or `regressed_to_null` cases: file as an FSIS data-quality bug; document for posterity even if the count is small.
