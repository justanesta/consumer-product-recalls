-- Diagnostic — attribute the "silent edit" cell from
-- `assert_eventlmd_correlates_with_content_change.sql` to either
-- infrastructure noise (bronze rebaseline) or genuine FDA-side edits.
--
-- Context: the F2 assertion's Q1 contingency surfaced 2,535 transitions
-- where content_hash changed but EVENTLMD did not — at face value, a
-- 97.3% silent-edit rate. All sampled silent edits shared an identical
-- (prev_extraction_timestamp, extraction_timestamp) pair spanning the
-- 2026-05-01 architecture realignment (ADR 0027 promoted to Accepted),
-- which strongly suggests bronze-side hash recomputation rather than
-- 2,535 independent FDA edits.
--
-- This script joins each silent-edit transition to
-- `extraction_runs.change_type` for the destination row and buckets by
-- run class. Decision tree on the headline (Q1):
--   * Most rows = schema_rebaseline / hash_helper_rebaseline →
--     infrastructure noise. F2 is fine; the F2 assertion script needs
--     to filter out non-routine runs the same way Phase 6
--     `recall_event_history` will (per implementation_plan.md:611
--     and operations/re_baseline_playbook.md). Document the filter
--     decision in productid_stability_findings.md and re-run the
--     F2 assertion script with the filter in place.
--   * Most rows = routine / incremental / deep_rescan / historical_seed
--     → F2 is genuinely violated. ADR 0010's deep-rescan cadence is
--     the only data-loss mitigation; revisit cadence; Phase 6
--     recall_event_history cannot trust EVENTLMD as a per-record edit
--     signal for FDA.
--
-- Q2 samples per change_type so that any non-rebaseline cases can be
-- inspected directly via the recall_id pulled from the sample.
--
-- Mirrors the diagnose_* convention already established by
-- `diagnose_phantom_hash.sql` and `diagnose_rebaseline_column_diffs.sql`
-- — forensic queries that don't carry a pass/fail assertion semantics
-- and aren't wired into dbt.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: silent-edit attribution by destination-run change_type ==='
\echo 'Each silent-edit transition (Cell C from F2) is bucketed by the change_type'
\echo 'recorded for the run that produced the destination row. Routine /'
\echo 'incremental / deep_rescan / historical_seed counts mean genuine F2'
\echo 'violations; schema_rebaseline / hash_helper_rebaseline means infrastructure'
\echo 'noise. NULL means the destination row predates the change_type column'
\echo '(migration 0009).'

with ordered as (
    select
        source_recall_id,
        raw_landing_path,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(content_hash) over w as prev_content_hash,
        lag(event_lmd)    over w as prev_event_lmd
    from fda_recalls_bronze
    window w as (partition by source_recall_id order by extraction_timestamp)
)
select
    coalesce(r.change_type, '<NULL>') as this_run_change_type,
    count(*)                          as silent_edit_count
from ordered o
left join extraction_runs r on o.raw_landing_path = r.raw_landing_path
where prev_content_hash is not null
  and content_hash is distinct from prev_content_hash
  and event_lmd    is not distinct from prev_event_lmd
group by coalesce(r.change_type, '<NULL>')
order by silent_edit_count desc;

\echo
\echo '=== Q2: silent-edit samples per change_type (up to 5 each) ==='
\echo 'For each change_type bucket, surface representative PRODUCTIDs so any'
\echo 'non-rebaseline cases can be inspected directly. Use the recall_id from'
\echo 'a routine/incremental/deep_rescan sample to drive a follow-up payload'
\echo 'diff (mirror the recall_id 00015 inspection pattern from'
\echo 'documentation/cpsc/array_stability_findings.md).'

with ordered as (
    select
        source_recall_id,
        raw_landing_path,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(extraction_timestamp) over w as prev_extraction_timestamp,
        lag(content_hash)         over w as prev_content_hash,
        lag(event_lmd)            over w as prev_event_lmd
    from fda_recalls_bronze
    window w as (partition by source_recall_id order by extraction_timestamp)
),
silent_edits as (
    select
        o.source_recall_id,
        o.prev_extraction_timestamp,
        o.extraction_timestamp,
        o.event_lmd as event_lmd_unchanged,
        o.prev_content_hash,
        o.content_hash,
        coalesce(r.change_type, '<NULL>') as this_run_change_type,
        row_number() over (
            partition by coalesce(r.change_type, '<NULL>')
            order by o.extraction_timestamp desc, o.source_recall_id
        ) as rn
    from ordered o
    left join extraction_runs r on o.raw_landing_path = r.raw_landing_path
    where o.prev_content_hash is not null
      and o.content_hash is distinct from o.prev_content_hash
      and o.event_lmd    is not distinct from o.prev_event_lmd
)
select
    this_run_change_type,
    source_recall_id,
    prev_extraction_timestamp,
    extraction_timestamp,
    event_lmd_unchanged,
    prev_content_hash,
    content_hash
from silent_edits
where rn <= 5
order by this_run_change_type, extraction_timestamp desc;

\echo
\echo '=== Q3: extraction-run-pair concentration (context) ==='
\echo 'Counts silent-edit transitions per (prev_extraction_timestamp,'
\echo 'extraction_timestamp) pair. If one or two pairs dominate, that confirms'
\echo 'a single-event boundary (rebaseline signature) rather than scattered'
\echo 'FDA edits across many runs.'

with ordered as (
    select
        source_recall_id,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(extraction_timestamp) over w as prev_extraction_timestamp,
        lag(content_hash)         over w as prev_content_hash,
        lag(event_lmd)            over w as prev_event_lmd
    from fda_recalls_bronze
    window w as (partition by source_recall_id order by extraction_timestamp)
)
select
    prev_extraction_timestamp,
    extraction_timestamp,
    count(*) as silent_edit_count
from ordered
where prev_content_hash is not null
  and content_hash is distinct from prev_content_hash
  and event_lmd    is not distinct from prev_event_lmd
group by prev_extraction_timestamp, extraction_timestamp
order by silent_edit_count desc
limit 10;
