-- Diagnostic — investigate populated→NULL transitions on NHTSA's
-- bgman/endman batch-window fields. Surfaces the raw_landing_paths
-- involved and discriminates three hypotheses for why a previously-
-- populated bgman/endman value became NULL in a later bronze row.
--
-- Default target: Nissan CUBE `26V230000` driver-airbag-inflator component
-- (`mfr_comp_ptno = '98560-7991C'`,
-- `mfr_comp_name = 'Driver Airbag Inflator'`,
-- compname = `'AIR BAGS:FRONTAL:DRIVER SIDE:INFLATOR MODULE'`).
-- This is the cluster surfaced 2026-05-13 by
-- `decompose_eleven_tuple_drift.sql` as 2 of the 11 real_drift cases
-- (the +2 delta vs. the 2026-05-12 baseline of 9). Spans two yeartxts:
--   * yeartxt=2009 — endman drift `2010-09-25 → <NULL>`
--   * yeartxt=2010 — bgman drift `2008-10-10 → <NULL>`
-- Both share the same (campno, mfr_comp_ptno), so one diagnose run
-- covers both 11-tuple drift groups.
--
-- Prior default (preserved for reference): Mack `26V261000`
-- brake-modulator component (`mfr_comp_ptno = '24710104'`,
-- `mfr_comp_name = 'Modulator'`, compname = `'SERVICE BRAKES,
-- AIR:ANTILOCK:CONTROL UNIT/MODULE'`). Classified H1 (upstream
-- depopulation) on 2026-05-12 — see `documentation/nhtsa/incremental_delta_findings.md`
-- (Re-baseline 2026-05-12 section) and the 2026-05-13 confirmation run.
--
-- To adapt to a different NULL-regression cluster, edit the two
-- `\set` lines below: `campno` selects the recall campaign and
-- `mfr_comp_ptno` selects the component within it. Both fields are
-- part of the 11-tuple identity (ADR 0030), so the pair is enough
-- to slice bronze to the affected component family.
--
-- Three hypotheses to distinguish:
--
--   H1 — UPSTREAM DEPOPULATION. NHTSA emitted empty bytes for
--        bgman/endman in the new archive. A scope amendment (e.g.,
--        recall expanded to cover production units of unknown
--        manufacturing date) is the canonical explanation.
--        Operationally normal; bronze captures what NHTSA published.
--
--   H2 — EXTRACTOR MIS-PARSE. The new archive's raw TSV has
--        populated dates in the bgman/endman cells, but our
--        `FlatFileExtractor` is producing NULL. Bug in cell-to-
--        Pydantic-field mapping. Operationally a bug.
--
--   H3 — SCOPE EXPANSION ADDING NEW ROWS. The new archive ADDED
--        new bronze rows for the same component at NULL bgman/
--        endman (covering newly-discovered units of unknown
--        manufacturing date), without removing the populated-
--        bgman rows. Silver emits MORE rows, not fragmented rows;
--        the assertion flags it as real_drift because per-path
--        value sets diverge ({A} vs {A, NULL}), but downstream
--        silver materialization is correct.
--
-- Decision rules:
--   Q1 rows_in_path == 1 for every (10-tuple, path) → H1 or H2
--      (true populated→NULL replacement). Proceed to TSV inspection
--      via `scripts/nhtsa/tsv_analysis/identity_search.py` against
--      the raw_landing_path's archive: empty bytes between tabs →
--      H1, populated date in TSV but NULL in bronze → H2.
--   Q1 rows_in_path > 1 for at least one (10-tuple, path) → H3
--      (additive scope expansion). No TSV inspection needed;
--      the regression is illusory and silver is correct. Note in
--      the findings doc and update ADR 0031:84's re-baseline
--      subsection to absorb this class.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\set campno '26V230000'
\set mfr_comp_ptno '98560-7991C'

\echo
\echo '=== Q1: per-(10-tuple, raw_landing_path) row counts and bgman/endman value sets ==='
\echo 'For each (10-tuple key, archive) pair, count bronze rows present and string-agg'
\echo 'the per-path value sets of bgman and endman.'
\echo
\echo 'Discriminator:'
\echo '  rows_in_path == 1 in every row of this output → H1 or H2 (replacement).'
\echo '  rows_in_path > 1 in any row of this output    → H3 (additive expansion).'

with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           count(*) as rows_in_path,
           string_agg(distinct coalesce(bgman::text, '<NULL>'),
                      ', ' order by coalesce(bgman::text, '<NULL>')) as bgman_set,
           string_agg(distinct coalesce(endman::text, '<NULL>'),
                      ', ' order by coalesce(endman::text, '<NULL>')) as endman_set
    from nhtsa_recalls_bronze
    where campno = :'campno'
      and mfr_comp_ptno = :'mfr_comp_ptno'
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, raw_landing_path
)
select maketxt, modeltxt, yeartxt,
       raw_landing_path,
       rows_in_path,
       bgman_set,
       endman_set
from per_path
order by maketxt, modeltxt, yeartxt, raw_landing_path;

\echo
\echo '=== Q2: all bronze rows for (campno, mfr_comp_ptno), sorted by (10-tuple, extraction_timestamp) ==='
\echo 'Eyeball view of the actual rows. Compare bgman/endman across consecutive snapshots'
\echo 'of the same (maketxt, modeltxt, yeartxt) tuple to see which extraction introduced'
\echo 'the NULL.'

select extraction_timestamp,
       maketxt, modeltxt, yeartxt,
       bgman, endman,
       raw_landing_path
from nhtsa_recalls_bronze
where campno = :'campno'
  and mfr_comp_ptno = :'mfr_comp_ptno'
order by maketxt, modeltxt, yeartxt, extraction_timestamp;

\echo
\echo '=== Q3: extraction_runs metadata for the raw_landing_paths involved ==='
\echo 'Links each affected archive to its NHTSA run: started_at, change_type, and the'
\echo 'inner-content SHA prefix (cross-reference with inner_content_cadence.sql Q2 to'
\echo 'identify the publication event). Helps determine whether the NULL-introducing'
\echo 'archive corresponds to a CHANGED inner-content transition.'

select r.started_at,
       r.change_type,
       r.records_inserted,
       left(r.response_inner_content_sha256, 16) as inner_sha,
       r.raw_landing_path
from extraction_runs r
where r.source = 'nhtsa'
  and r.raw_landing_path in (
      select distinct raw_landing_path
      from nhtsa_recalls_bronze
      where campno = :'campno'
        and mfr_comp_ptno = :'mfr_comp_ptno'
  )
order by r.started_at;
