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

#### ES-newer direction — delayed Spanish translation, not content drift (2026-06-07, Phase 6e.5)

The "EN is always ahead of ES, never reversed" interpretation above reflects the **top-10-by-gap**
sample (all EN-newer, 740–1,701 days). A full re-measure of the non-atomic set during Phase 6e.5
splits the 105 non-atomic pairs three ways: **40 EN-newer, 18 ES-newer, 47 one-side-null** (the
42%-null `last_modified_date` from Finding D). So the ES-newer direction **does** occur — the earlier
"never reversed" claim was incomplete (it described the largest gaps, which are all EN-newer).

The 18 ES-newer gaps run **3–399 days — none are <2-day timestamp skew** — and cluster as **delayed
bulk Spanish translation republishes**: 7 share `es_modified = 2023-10-24` (a Spanish-side republish
batch), others annual-ish (`085-2015` +366d, `058-2018` +399d). Probe:
`scripts/sql/usda_recalls/bronze/investigate_spanish_newer.sql`.

**Empirical confirmation on the worst case (`058-2018`, +399 days).** A full English-vs-Spanish
recall-text comparison (2026-06-07) shows the Spanish is a **faithful, complete translation** of the
English — identical firm (Nueske's), weight (12,946 lbs / 12 946 libras), hazard (undeclared brown
rice flour + glycerin), packaging window, **lot codes**, EST. 20341, discovery date, and contacts.
**Zero new content.** So a later Spanish `last_modified_date` reflects **translation timing, not a
content edit**.

**Implication (confirmed, not inferred).** English remains the authoritative + complete version, so
**English-only silver is not stale** for ES-newer recalls. Bilingual non-atomicity in *both* directions
is benign for an English-only silver layer (EN-newer = English edited without retranslating; ES-newer =
Spanish translated/republished later). The dbt `assert_usda_bilingual_atomic_update` monitor stays
`severity=warn` as a benign **source-behavior watch**, not a pending defect. (Caveat: confirmed on the
worst case + the bulk-date pattern; a per-recall content diff of every ES-newer case would be exhaustive
but the evidence is strong.)

### U3 — last_modified_date reliability on edits

| Transition class | Observed count | Notes |
|---|---|---|
| `advanced` | **1** | First observed 2026-05-17: `PHA-04302026-01` / English, snap 2 → snap 3 — summary text changed AND `last_modified_date` advanced from `2026-04-30` → `2026-05-15`. See PHA-04302026-01 subsection below. |
| `regressed` | 0 | None. |
| `advanced_from_null` | 0 | None. |
| `regressed_to_null` | 0 | None. |
| `unchanged_with_content_change` | **3** (≥5 pending re-run) | 3 contingency-counted routine content edits as of the 2026-05-12 assertion run; date did NOT advance. The 2026-05-17 evening raw-landing probes surfaced 2 additional snap 2→3 transitions (`PHA-05092026-01`, `PHA-05042026-01`) during the post-rebaseline 2026-05-15 routine wave, both expected to count once the assertion is re-run — see paired diff subsection below. (`PHA-04302026-01` snap 1 → snap 2 is a qualitatively-similar additional silent-edit transition but crosses the schema_rebaseline boundary and is excluded by the assertion script's rebaseline filter.) |
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

**Updated to n=4 (plus 1 rebaseline-excluded), 2026-05-17.** The
routine 2026-05-17 USDA extraction (run `22f110f0-9419-4886-a0f0-`
`321b533d08a2`) inserted a single new bronze row: `PHA-04302026-01` /
English. This recall now has three snapshots and contributes **two new
transitions**:

- *Snap 2 → snap 3* (both post-rebaseline routine runs, IS counted by
  the assertion script): `summary` text changed and `last_modified_date`
  advanced from `2026-04-30` → `2026-05-15`. **First `advanced`
  observation in the U3 contingency since the project began tracking.**
  Demonstrates that FSIS *can* bump `last_modified_date` on a real
  edit — the unreliability finding is therefore "FSIS sometimes
  silently edits without bumping LMD" rather than "FSIS never advances
  LMD on edits." Intermittent reliability is still unreliability, so
  the Phase 6 conclusion (use `extraction_timestamp`) is unchanged,
  but the mechanism narrative tightens.
- *Snap 1 → snap 2* (across the schema_rebaseline boundary 2026-05-01
  → 2026-05-02 — excluded from the contingency, included here for
  completeness): summary gained a "Last Updated: May 1, 2026" notice,
  AND two populated fields cleared to `""` (`establishment`:
  `"Richelieu Foods, Inc."` → `""`; `company_media_contact`: full HTML
  contact block → `""`), AND `last_modified_date` did **NOT** advance.
  A 4th silent-edit observation qualitatively identical to the U3-
  violation pattern, with the additional finding of upstream-FSIS
  field erasure on populated values (separate from the ADR 0027
  null→`""` serialization artifact that hits 5 nullable text fields at
  the same boundary). Field erasure has its own follow-up implications
  for `usda_establishments` join coverage — see PHA-04302026-01 diff
  subsection below.

The same recall therefore exhibits both classes (silent edit at snap
1→2, properly-dated edit at snap 2→3). FSIS's LMD-advancement behavior
is non-deterministic at the per-recall level, not partitioned by
recall class — a per-edit decision (or per-edit forgetting).

**Updated 2026-05-17 (evening probes — n≥5 pending re-run, strongest
U3 violation yet observed).** Two additional raw-landing probes via
`scripts/usda_recalls/inspect_raw_landing_for_recall.py` against
`PHA-05092026-01` / English and `PHA-05042026-01` / English (the two
recalls that seeded the 2026-05-12 n=3 baseline) surfaced three new
findings:

1. *Two new snap 2→3 silent-edit transitions* during the post-
   rebaseline 2026-05-15 routine wave, both contingency-countable
   (pending assertion re-run). Brings bronze-level
   `unchanged_with_content_change` count to ≥5.
2. *Strongest U3 violation in evidence yet.* `PHA-05092026-01` snap
   2→3 shows a formal Editor's Note added to `summary` disclosing
   whole-genome-sequencing results — qualitatively more damning than
   the `active_notice` toggle that originally seeded the finding. A
   recall-class lifecycle flip is internal FSIS bookkeeping, but a
   WGS-results editorial amendment is exactly what consumers of FSIS
   data care most about catching. `last_modified_date` stayed at
   `2026-05-09`. See paired diff subsection below.
3. *Erasure null-result on n=2 controls.* Neither probed recall
   exhibited the `field_establishment` or `field_company_media_contact`
   populated→empty transition documented on `PHA-04302026-01`. The
   upstream-FSIS erasure finding stays at **n=1 confirmed + n=2
   controls negative** — rare, not routine. The Policy C motivation in
   `project_scope/implementation_plan.md` line 644 stays grounded;
   evidence does not yet warrant promoting to Policy B (latest-non-
   empty wins).

**Bronze-vs-silver attribution nuance.** Both 2026-05-17 snap 2→3
transitions also coincide with the 2026-05-15 whitespace-collapse wave
(Finding Q — `company_media_contact` cosmetic newline collapse on
1235 rows). At the bronze level the U3 contingency counts both as
`unchanged_with_content_change` indistinguishably, but they are
qualitatively different: `PHA-05092026-01` carries a real FSIS edit
(summary amendment) plus the cosmetic event; `PHA-05042026-01` shows
only the cosmetic event. The silver per-field whitespace normalization
plan (per `project_scope/implementation_plan.md` line 641) suppresses
the cosmetic noise while preserving the real edit — the silver
`recall_event_history` event count would correctly distinguish.
**Concrete vindication of the silver design choice on a real
production case.** Bronze stays faithful to source byte-state (ADR
0027 unchanged); silver carries the editorial semantics. The bronze-
level U3 contingency therefore overcounts; silver is the ground truth
for "did FSIS make a substantive edit."

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

#### Diff result for `PHA-04302026-01` (run 2026-05-17)

Three snapshots in bronze; the wave-drivers script (`scripts/sql/usda_`
`recalls/bronze/diagnose_wave_field_drivers.sql`) identified this recall
as the sole driver of the 2026-05-17 single-row wave, and the payload-
drift script (`scripts/sql/usda_recalls/bronze/diagnose_payload_drift_`
`for_recall.sql` with `\set recall_id 'PHA-04302026-01'`) produced the
per-snapshot diff.

| Snap | extraction | last_modified_date | content_hash (prefix) | run class |
|---|---|---|---|---|
| 1 | 2026-05-01 01:47:19 | 2026-04-30 | `bc2548ad…` | routine (pre-rebaseline) |
| 2 | 2026-05-02 12:40:01 | 2026-04-30 | `13b6ab29…` | schema_rebaseline |
| 3 | 2026-05-17 21:26:18 | 2026-05-15 | `2512e747…` | routine |

**Snap 1 → Snap 2 (across rebaseline — excluded from U3 contingency,
included here for completeness).** Two qualitatively distinct
mechanisms operate at the same snapshot boundary:

- *Rebaseline artifact* (ADR 0027 nullable-string serialization on
  6 fields): `closed_year`, `distro_list`, `press_release`,
  `product_items`, `qty_recovered`, `en_press_release` shift `null` →
  `""`. Same mechanism documented for `PHA-04092026-01` snap 2 → snap 3
  above. Not an FSIS edit.
- *Real upstream FSIS edit* (3 fields). `summary` gained a multi-
  paragraph prefix `"Last Updated: This release was last updated on
  May 1, 2026, to reflect additional affected products and their
  corresponding labels."` `establishment` went from
  `"Richelieu Foods, Inc."` → `""`. `company_media_contact` went from
  a populated HTML block listing both ALDI Inc. and Richelieu Foods
  Inc. contacts → `""`. **`last_modified_date` did NOT advance** despite
  the summary amendment being a substantive disclosure (additional
  affected products added to the alert). Qualitatively identical to
  the U3-violation pattern; excluded from the contingency only by the
  rebaseline filter.

**Snap 2 → Snap 3 (both routine, post-rebaseline — IS counted by the
assertion script).** One field changed semantically: `summary` updated
from `"Last Updated: May 1, 2026"` to `"Last Updated: May 15, 2026"`,
reflecting a second round of additional affected products.
**`last_modified_date` advanced from `2026-04-30` → `2026-05-15`** —
first `advanced` observation since the project began tracking U3.

**Significance for U3:**

- FSIS *does* advance `last_modified_date` on substantive amendments
  sometimes (snap 2 → snap 3). FSIS *does not* always advance it (snap
  1 → snap 2). The same recall exhibits both behaviors within a 16-day
  window. Per-edit, not per-recall-class.
- Both summary edits added "Last Updated: <date>" notices reflecting
  new affected products. The May 1 update *also* cleared two populated
  fields; the May 15 update did not. The presence of structural field
  changes does not correlate with LMD-advancement in any obvious
  direction in this 2-edit sample.
- Phase 6 conclusion (use `extraction_timestamp` exclusively for the
  per-record edit signal) is unchanged. Intermittent reliability is
  unreliability for the purposes of building a deterministic event
  history.

**Significance beyond U3 — upstream field erasure (NEW finding class):**

The snap 1 → snap 2 transition is the first observed instance of
**FSIS clearing populated semantic fields mid-recall-lifecycle** (as
opposed to the well-documented `null` ↔ `""` serialization shift that
preserves the absence of a value). `field_establishment` went from a
real firm name to empty, and `field_company_media_contact` went from a
populated HTML contact block to empty. Both fields remained empty in
snap 3, so this is not a transient FSIS-side cache issue — the values
are gone from the live API.

Operational implications:

- **`usda_establishments` join coverage**: `documentation/usda/`
  `establishment_join_coverage.md` measures recall→establishment join
  rates using `field_establishment`. If FSIS routinely erases this
  field mid-lifecycle, current-snapshot joins will be silently lossy
  for affected recalls. Re-measure join coverage with awareness of
  this risk; consider preserving the first-non-empty `establishment`
  value across snapshots (SCD-2 latest-non-null) for the join key
  rather than the current value.
- **Phase 6 silver SCD-2 design** (per `project_scope/`
  `implementation_plan.md` line 644 + ADR 0033): the upstream-erasure
  pattern is the canonical case where Type 2 history matters most. A
  Type 1 dim that overwrites the silver `firm` attribution with the
  current `""` would silently lose firm-resolution capability for any
  recall that experienced this erasure. The chosen SCD-2 approach
  (snapshot- or LAG-derived) needs an explicit policy for "latest
  non-empty wins" vs "latest wins" for erasable text fields. Track
  this case as an evaluation criterion when the silver SCD-2 ADR
  lands.
- **`recall_api_observations.md` Finding C addendum** (separate
  follow-up): the field-nullability map in Finding C is a snapshot-
  in-time measurement. Per-field "stability across snapshots" is a
  distinct property worth documenting. Until that addendum lands, this
  finding is the canonical reference for the erasure phenomenon.

**Verification — upstream-FSIS erasure confirmed (2026-05-17).**
`scripts/usda_recalls/inspect_raw_landing_for_recall.py` was run on
`PHA-04302026-01` / English on 2026-05-17. Output of the populated/
empty/null summary table:

| Snap (extraction) | `field_establishment` | `field_company_media_contact` |
|---|---|---|
| snap 1 (2026-05-01 01:47 UTC) | populated | populated |
| snap 2 (2026-05-02 12:40 UTC) | empty string | empty string |
| snap 3 (2026-05-17 21:26 UTC) | empty string | empty string |

Since ADR 0027 means bronze preserves `""` verbatim for `Optional[str]`
fields (`src/schemas/usda.py` confirmed independently — no `""` → None
validator on text columns), the empty strings observed in bronze are
exactly what FSIS returned. The erasure originated upstream between
2026-05-01 01:47 UTC and 2026-05-02 12:40 UTC (≤ 35-hour window) and
has persisted unchanged through the 2026-05-17 extraction. Erasure
is durable, not a transient FSIS-side cache miss. Raw landing sizes
(snap 1 = 11,517,089 B; snap 2 = 11,516,885 B; snap 3 = 11,486,908 B)
match the ~1.6 MB compressed full-dump size from Finding A — sanity
check on the extraction-and-store path.

Re-run the script for any future suspected-erasure case:

```
python scripts/usda_recalls/inspect_raw_landing_for_recall.py \
    --recall-id <id> --langcode <English|Spanish>
```

#### Diff result for `PHA-05092026-01` + `PHA-05042026-01` (probed 2026-05-17 evening)

Both recalls share lifecycle shape: a first-landing on day N, a
silent re-version on day N+~5, a third snapshot at the 2026-05-15
whitespace-collapse wave. The pair forms a natural substantive-edit
vs cosmetic-only contrast and serves as n=2 negative controls on the
upstream-erasure finding.

**`PHA-05092026-01` (Crawford Sausage Co., Inc.) — substantive edit case:**

| Snap | extraction | last_modified_date | content_hash (prefix) | run class |
|---|---|---|---|---|
| 1 | 2026-05-10 13:18:18 | 2026-05-09 | `efbcddd9…` | routine (first-landing) |
| 2 | 2026-05-12 12:04:33 | 2026-05-09 | `41637cdb…` | routine (silent edit, U3 n=3 baseline) |
| 3 | 2026-05-15 00:34:56 | 2026-05-09 | `335d7a68…` | routine (Editor's Note + whitespace wave) |

Snap 2 → snap 3 is the strongest U3 violation in evidence yet. The
`summary` gained an Editor's Note prefix dated 2026-05-14: `"Editor's
Note – May 14, 2026: Whole genome sequencing results show that
headcheese samples collected by FSIS and produced at Crawford
Sausage..."` — a formal lab-results-disclosure editorial amendment of
exactly the class that should obviously bump `last_modified_date` if
LMD were reliable. `field_company_media_contact` also shifted in the
10-leading-newlines → 1-leading-newline Finding Q whitespace pattern
at the same snapshot boundary, so this single bronze re-version
combines one substantive content edit with one cosmetic event.
`last_modified_date` stayed at `2026-05-09` throughout.

**`PHA-05042026-01` (Rana Meal Solutions, LLC) — cosmetic-only control:**

| Snap | extraction | last_modified_date | content_hash (prefix) | run class |
|---|---|---|---|---|
| 1 | 2026-05-05 11:51:57 | 2026-05-04 | `20ca5b1b…` | routine (first-landing) |
| 2 | 2026-05-10 13:18:18 | 2026-05-04 | `8197d4bd…` | routine (silent edit, U3 n=3 baseline) |
| 3 | 2026-05-15 00:34:56 | 2026-05-04 | `b0b645c8…` | routine (whitespace wave, cosmetic only) |

Snap 2 → snap 3 shows the same `field_company_media_contact`
whitespace-collapse pattern (10-leading-newlines → 1-leading-newline)
as `PHA-05092026-01` above, but the first 160 chars of `summary` are
identical across all 3 snapshots (no Editor's Note prefix).
`last_modified_date` stayed at `2026-05-04`. The canonical case of a
pure cosmetic re-version with no underlying FSIS editorial action.

*Confidence caveat:* the probe script truncates `summary` at 160
chars; an Editor's Note added past that position would be invisible
to the diff. The `PHA-05092026-01` pattern shows FSIS prefixes the
notice at the start, so absence in the visible prefix is strong but
not absolute evidence of true cosmetic-only. Re-run with
`--snippet-len 800` if absolute confirmation is needed.

**Erasure check (populated/empty/null) for both recalls:**

All snapshots of both recalls show `field_establishment` and
`field_company_media_contact` as `populated`. Neither exhibits the
populated→empty pattern observed on `PHA-04302026-01`. **The upstream-
FSIS erasure finding stays at n=1 confirmed + n=2 controls negative.**

**Implications:**

- *U3 contingency*: bronze-level count is now ≥5 silent-edit
  transitions pending assertion re-run. The Phase 6 "use
  `extraction_timestamp` exclusively" conclusion is unchanged but the
  evidence base is stronger and qualitatively more compelling — a
  formal lab-results-disclosure editorial amendment is harder to
  dismiss than the `active_notice` toggle that originally seeded the
  finding.
- *Erasure rarity*: confirmed in 1 of 3 sampled PHA recalls; not
  routine. Policy C in `project_scope/implementation_plan.md` line 644
  stays grounded as the leading SCD-2 candidate; evidence does not
  warrant promoting to Policy B (latest-non-empty wins) at this n.
- *Silver design vindication*: the substantive-edit + cosmetic-event
  co-occurrence on `PHA-05092026-01` snap 2→3 is the canonical example
  the silver per-field whitespace normalization plan (Finding Q
  mitigation per `project_scope/implementation_plan.md` line 641) is
  designed to resolve. `PHA-05042026-01` snap 2→3 is the canonical
  example of a pure cosmetic re-version that the same plan suppresses.
  The pair illustrates both sides of the design choice working
  correctly on a real production pair.
- *Methodology*: `scripts/usda_recalls/inspect_raw_landing_for_recall.py`
  is now the ground-truth oracle for "did FSIS make a real edit, or
  is this a bronze-side artifact." Recommended first-line investigation
  for any future single-row USDA wave without obvious attribution.

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
