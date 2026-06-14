# FDA PRODUCTID and EVENTLMD reliability findings

Empirical baseline for the FDA load-bearing assumptions tracked in
`documentation/source_assumption_audit.md`:

- **F1 — PRODUCTID never renumbers** (the linchpin keeping FDA silver coherent)
- **F2 — EVENTLMD advances only on real edits** (already partially false per ADR 0023; this measures the rate)

Assumption **F3** (PRODUCTLMD null-until-first-edit) is documented in
`documentation/fda/api_observations.md:145-147` but is **untestable today**:
the FDA bronze schema (migration 0004) does not capture PRODUCTLMD because
the extractor's `displaycolumns` request does not include it. Adding
PRODUCTLMD to the extractor + bronze schema is filed as a follow-up in the
audit doc.

## Detection scripts

- **F1 (PRODUCTID stability):** `scripts/sql/fda/bronze/assert_productid_stable.sql`
- **F2 (EVENTLMD/content correlation):** `scripts/sql/fda/bronze/assert_eventlmd_correlates_with_content_change.sql`

Both are also wrapped as dbt singular tests at `severity=warn` under
`dbt/tests/source_assumptions/`.

## Baseline as of 2026-05-08 (development slice)

> **Run after**: `psql "$NEON_DATABASE_URL" -f scripts/sql/fda/bronze/assert_productid_stable.sql`
> and the F2 sibling. Paste Q1, Q2, Q3 outputs below.

### F1 — PRODUCTID stability

| Output cell | Observed | Notes |
|---|---|---|
| Q1 `drift_group_count` | **0** | No `(recall_event_id, product_description_txt, recall_num)` candidate key has been observed with >1 distinct PRODUCTID across runs. |
| Q2 candidate-renumber groups | (0 rows) | Empty as expected given Q1 = 0. |
| Q3 corpus shape | **5,529 bronze rows / 2,924 distinct PRODUCTIDs / 873 distinct recall_events / 7 distinct runs** | 2,605-row excess over distinct PRODUCTIDs is the F2 territory (edit snapshots), not renumbers. |

**Interpretation**: F1 holds as of 2026-05-08 — no observed PRODUCTID
renumber. ADR 0031's "any non-zero rate" threshold is not yet at risk;
FDA silver's reliance on PRODUCTID as the product surrogate is
empirically supported.

**The 2,605 excess rows from Q3 are not a renumber signal.** Q1 would
have caught any case of the same logical product appearing under two
distinct PRODUCTIDs. The excess instead reflects FDA's edit-snapshot
behavior: bronze content-hash dedup keeps every distinct snapshot of a
PRODUCTID over time. On average that's ~1.89 bronze rows per PRODUCTID,
or ~372 re-snapshots per extraction run across the 7 runs in this
database. Whether those snapshots represent real content edits or
ADR 0023 archive-migration noise is the F2 question, addressed by
`assert_eventlmd_correlates_with_content_change.sql` below.

For comparison: CPSC has 1 edit-snapshot in 1,360 rows (~0.07% edit
rate). FDA's apparent edit-snapshot rate of 47% (2,605 / 5,529) is two
orders of magnitude higher — consistent with the ADR 0023 archive
migration if most of those snapshots are EVENTLMD-bumped no-ops, or
genuinely high if they're real edits. The F2 contingency will resolve
this.

### F2 — EVENTLMD vs. content_hash transition contingency

| Cell | Transition | Observed count | Expected behavior |
|---|---|---|---|
| A | EVENTLMD changed + content changed | **70** | Real edits — should dominate post-2020 records |
| B | EVENTLMD changed + content unchanged | **0** | Archive-migration noise per ADR 0023 — expected non-zero |
| C | EVENTLMD unchanged + content changed | **2,535** | **Silent edits — should be 0** |
| D | EVENTLMD unchanged + content unchanged | **0** | Impossible by content-hash dedup design — verify = 0 |

**Total transitions:** 2,605 — matches the 5,529 − 2,924 = 2,605 row excess from F1 Q3. ✓

**Apparent silent-edit rate (C / (A+C)):** **97.3%** — at face value, F2
is severely violated. ADR 0010's mandatory FDA deep-rescan would become
load-bearing for silver correctness, not just operational hygiene.
ADR 0023's archive-migration narrative would also need amendment
(Cell B = 0 contradicts it).

**But there is a strong alternative hypothesis before we conclude F2 is
violated.** All five Q2a (Cell C) samples share an identical
`prev_extraction_timestamp` (2026-04-29 00:49:15 UTC) and identical
`extraction_timestamp` (2026-05-02 23:14:46 UTC). That is not the
signature of 2,535 independent FDA edits — it is the signature of a
**single bronze-side rebaseline event** at that boundary, in which
the canonical-dict shape changed (ADR 0027) and content_hash
recomputed for many existing records without any source-side change.

The 04-29 → 05-02 boundary brackets the **2026-05-01 architecture
realignment** documented in `TODO.md:34`, which promoted ADR 0027
("bronze storage forced transforms only") from Draft to Accepted and
plausibly altered the canonical-dict shape, triggering a hash
rebaseline wave for all existing bronze rows. CPSC has the analog at
recalls `02012` and `26428` (see CPSC findings doc).

**Verification script:** `scripts/sql/fda/bronze/diagnose_silent_edit_attribution.sql`
joins each silent-edit transition to `extraction_runs.change_type`,
samples per change_type, and surfaces the (prev_extraction_timestamp,
extraction_timestamp) pair concentration so a single-boundary rebaseline
shows up clearly. Run with:

```
psql "$NEON_DATABASE_URL" -f scripts/sql/fda/bronze/diagnose_silent_edit_attribution.sql
```

**Verification result (2026-05-08):** all 2,535 silent edits are
`schema_rebaseline`. Q3 confirms the concentration: 2,523 transitions
spanned `2026-04-29 00:49:15 → 2026-05-02 23:14:46` and 12 spanned
`2026-04-28 23:56:37 → 2026-05-02 23:14:46`, both ending at the
2026-05-01-architecture-realignment rebaseline run.

**F2 is not genuinely violated.** Excluding rebaseline-driven
transitions, the contingency reduces to:

| Cell | Transition | Filtered count | Notes |
|---|---|---|---|
| A | EVENTLMD changed + content changed | 70 | Real edits during routine extraction |
| B | EVENTLMD changed + content unchanged | 0 | No archive-migration noise observed in this corpus |
| C | EVENTLMD unchanged + content changed | 0 | F2 holds within the routine path |
| D | EVENTLMD unchanged + content unchanged | 0 | Confirmed impossible by content-hash dedup |

**Filtered silent-edit rate (C / (A+C)):** 0 / 70 = **0%**. F2 is empirically
verified for the routine extraction path; Phase 6 `recall_event_history`
can use EVENTLMD as the per-record edit signal for FDA.

**Cell B (archive-migration noise) is also 0**, contrary to ADR 0023's
narrative. Two non-exclusive explanations: (1) the migration completed
before the routine extraction window we have bronze for, or (2) the
migration's EVENTLMD bumps coincide with content changes (so they fall
into Cell A, not Cell B). Either way the noise hasn't surfaced as
distinct from real edits in the current corpus. Worth re-running this
assertion periodically as more bronze accumulates.

### Tier 2 script rebaseline filter — applied 2026-05-08

The F2 assertion script + its dbt wrapper now exclude rebaseline rows
before LAG. Same fix applied prophylactically to the USDA
`assert_field_last_modified_date_advances_on_edit.sql` and its dbt
wrapper (same LAG pattern; vulnerable if a USDA rebaseline ever lands).
Filter shape:

```sql
LEFT JOIN extraction_runs r ON b.raw_landing_path = r.raw_landing_path
WHERE r.change_type IS NULL
   OR r.change_type NOT IN ('schema_rebaseline', 'hash_helper_rebaseline')
```

`LEFT JOIN` + `IS NULL` preserves pre-migration-0009 rows (when
`change_type` didn't exist) as routine.

**Post-filter F2 contingency (re-run 2026-05-08):**

| Cell | Transition | Count |
|---|---|---|
| A | EVENTLMD changed + content changed | 41 |
| B | EVENTLMD changed + content unchanged | 0 |
| C | EVENTLMD unchanged + content changed | 0 |
| D | EVENTLMD unchanged + content unchanged | 0 |

**Why pre-filter A=70 became post-filter A=41 (not 70).** Pre-filter,
some Cell A transitions had a rebaseline row as one side of the LAG
pair: FDA edited the record between the prior routine snapshot and
the rebaseline run, and the rebaseline re-extracted with the bumped
EVENTLMD (rebaseline isn't a pure DB-side hash recomputation; it's
a re-extraction from source). When the filter removes those rebaseline
rows from the LAG sequence and there's no subsequent routine snapshot
to anchor against, the transition disappears entirely. 29 real edits
became invisible to this assertion this way. They're not lost — the
rebaseline rows still exist in bronze with the new EVENTLMD — but this
particular assertion can't see them. Phase 6 `recall_event_history`
will face the same trade-off; for the assertion's purpose ("are there
silent edits in routine-extracted data?"), the filtered view is the
correct answer.

**Final F2 verdict:** verified for the routine extraction path. Phase 6
`recall_event_history` can use EVENTLMD as the per-record edit signal
for FDA, with the same rebaseline-exclusion filter applied.

Not affected by the filter: CPSC scripts (cross-run grouping on
name/model values; rebaseline rows with same name/model don't trigger
drift) and the USDA bilingual-atomicity script (uses `last_modified_date`
directly, not hash transitions).

## 2026-06-13 refresh — full-corpus production seed

F1 (PRODUCTID stability) re-run against the **full production corpus** after the FDA seed
(`scripts/sql/fda/bronze/assert_productid_stable.sql`, 2026-06-13):

| Output cell | Observed | Notes |
|---|---|---|
| Q1 `drift_group_count` | **0** | No `(recall_event_id, product_description_txt, recall_num)` candidate key observed with >1 distinct PRODUCTID across runs. |
| Q2 candidate-renumber groups | (0 rows) | Empty, as expected given Q1 = 0. |
| Q3 corpus shape | **134,642 bronze rows / 134,567 distinct PRODUCTIDs / 50,551 distinct recall_events / 6 distinct runs** | Only 75 excess rows over distinct PRODUCTIDs (~1.0006 rows/PRODUCTID) — a freshly-seeded corpus with almost no edit-snapshot accumulation yet, vs the dev slice's ~1.89. |

**Interpretation.** F1 **still holds at full-corpus scale** — 0 PRODUCTID renumbers across 134,642 rows
(≈24× the 2026-05-08 dev-slice row count), materially stronger evidence that FDA silver's reliance on
PRODUCTID as the product surrogate is sound. ADR 0031's "any non-zero rate" threshold remains a future
trigger, not at risk. (Only F1 was re-run here; the 2026-05-08 F2/EVENTLMD routine-path verdict stands —
re-run `assert_eventlmd_correlates_with_content_change.sql` to refresh it.)

## ADR 0031 threshold reconciliation

ADR 0031's FDA row currently reads:

> Phase 6 revisit threshold: Any non-zero rate (FDA stability is contractual)

After running the scripts:

- **Detection status cell**: replace "TBD" with `scripts/sql/fda/bronze/assert_productid_stable.sql` + the F2 sibling.
- **Threshold cell**: if F1 Q1 = 0, the "any non-zero rate" threshold stands as a future trigger. If F1 Q1 > 0, immediate Phase 6 escalation per ADR 0031.
- **Add F2 row**: ADR 0031 doesn't currently track EVENTLMD reliability as a fragmentation signal because it isn't one (silver dedupes on content_hash regardless). Document the noise-rate baseline here; threshold for action would be Cell C (silent edits) > 0, not Cell B.

## Follow-up triggers

- If F1 Q1 > 0: catastrophic — Phase 6 must redesign FDA silver surrogate. ADR 0031 trigger fires.
- If F2 Cell C > 0: silent edits — ADR 0010's deep-rescan cadence is the only mitigation; consider tightening cadence or adding alerting.
- If F2 Cell B grows materially relative to Cell A: archive migration is accelerating; revisit ADR 0010 count-guard at `tests/extractors/test_fda_extractor.py:127`.
- **Pre-empt F3 untestability**: file an ADR proposing `PRODUCTLMD` be added to the FDA extractor's `displaycolumns` request and bronze schema. Until that lands, F3 cannot be verified.
