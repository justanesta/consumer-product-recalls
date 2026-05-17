-- Phase 5d Step 3 — drill-down on uscg_recalls_rejected.
--
-- Companion to ``explore_first_extraction.sql``. Where the explore script
-- reports counts + distributions, this one inspects actual raw_record
-- bytes per failure mode so the operator can see exactly which fields
-- on which recalls failed and why.
--
-- Rejection taxonomy (from the 2026-05-17 first-extraction run):
--   - failure_stage="validate_records": Pydantic ValidationError. Common
--     causes: unparseable date, unexpected key, type mismatch.
--   - failure_stage="invariants": one of the three invariant checks fired:
--       * check_null_source_id (source_recall_id is null/empty)
--       * check_date_sanity (opened_on >70yr in past, in the future, etc.)
--       * _check_year_prefix_consistency (source_recall_id[:2] vs opened_on year)
--
-- The 218 invariant rejections vs 33 validate_records rejections is itself
-- a signal: invariant failures dominate, suggesting either USCG data has
-- legitimate year-prefix patterns we didn't account for OR the invariant
-- is structurally too strict.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: distinct failure_reason strings, fully grouped ==='
\echo 'Aggressively grouped failure reasons so a 218-row systemic pattern'
\echo 'collapses to one row with count=218. Use this to spot dominant modes.'

with grouped as (
    select
        failure_stage,
        -- Normalize the dynamic parts of reason strings so similar
        -- failures collapse to one bucket. Reason text often includes
        -- specific values (e.g., dates, recall numbers); we strip them
        -- here to cluster on the underlying mechanism.
        regexp_replace(
            regexp_replace(
                failure_reason,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}',
                '<DATE>',
                'g'
            ),
            '''[^'']*''',
            '<VALUE>',
            'g'
        ) as reason_normalized,
        count(*) as n
    from uscg_recalls_rejected
    group by failure_stage, reason_normalized
)
select failure_stage, n, left(reason_normalized, 200) as reason_normalized
from grouped
order by n desc, failure_stage
limit 30;

\echo
\echo '=== Q2: raw failure_reason samples per stage (one per stage) ==='
\echo 'Pick one full failure_reason string per stage, untouched, so you can'
\echo 'see the entire Pydantic / invariant message.'

select distinct on (failure_stage)
    failure_stage,
    source_recall_id,
    failure_reason,
    rejected_at
from uscg_recalls_rejected
order by failure_stage, rejected_at desc;

\echo
\echo '=== Q3: validate_records failures — raw_record JSON shape ==='
\echo 'Pydantic-rejected rows. The raw_record JSON shows exactly what the'
\echo 'parser produced before the schema rejected it. Compare against'
\echo 'UscgRecallRecord field types to identify the offending field.'

select
    source_recall_id,
    raw_record,
    failure_reason
from uscg_recalls_rejected
where failure_stage = 'validate_records'
limit 10;

\echo
\echo '=== Q4: invariants failures — by which invariant fired ==='
\echo 'Heuristically classify invariant failures by inspecting the failure'
\echo 'reason text. null_id / date_sanity / year_prefix are the three'
\echo 'currently-implemented invariants per src/extractors/uscg.py.'

select
    case
        when failure_reason like '%source_recall_id is null%' then 'null_source_id'
        when failure_reason like '%is in the future%' or failure_reason like '%70 years in the past%' then 'date_sanity'
        when failure_reason like '%year-prefix mismatch%' then 'year_prefix'
        else 'other'
    end as invariant_class,
    count(*) as n
from uscg_recalls_rejected
where failure_stage = 'invariants'
group by invariant_class
order by n desc;

\echo
\echo '=== Q5: year-prefix mismatch — full enumeration ==='
\echo 'If the year_prefix invariant is the dominant failure (likely per Q4),'
\echo 'this query enumerates each violating recall: the source_recall_id,'
\echo 'the prefix we extracted, and the opened_on year it should have matched.'
\echo 'A high count of (prefix=26, opened_on_year=25) — or vice versa — would'
\echo 'suggest USCG uses fiscal-year or filing-year rather than opened-on-year.'
\echo ''
\echo 'Note: invariant-stage quarantine stores raw_record via model_dump(mode="json")'
\echo 'so keys are Python field names (source_recall_id), NOT validation aliases'
\echo '(number). validate_records-stage stores the raw extractor dict, keyed by'
\echo 'validation aliases. The asymmetry is a code smell we may want to align.'

select
    raw_record->>'source_recall_id' as source_recall_id,
    left(raw_record->>'source_recall_id', 2) as prefix,
    substring(raw_record->>'opened_on' from 1 for 4) as opened_on_year,
    raw_record->>'opened_on' as opened_on_raw,
    failure_reason
from uscg_recalls_rejected
where failure_stage = 'invariants'
  and failure_reason like '%year-prefix mismatch%'
order by raw_record->>'source_recall_id'
limit 50;

\echo
\echo '=== Q6: year-prefix mismatch — aggregated (prefix, opened_on year) heatmap ==='
\echo 'Counts the (prefix, opened_on_year) pairs of all mismatches. A dense'
\echo 'diagonal-adjacent cluster (e.g., prefix=26 paired with opened_on=2025)'
\echo 'signals a systematic encoding choice we misunderstood. Sparse off-diagonal'
\echo 'cells = real one-off mismatches. opened_on_year=1970 cluster = Unix epoch'
\echo 'sentinel ("no opened date known") and is informative on its own.'

select
    left(raw_record->>'source_recall_id', 2) as prefix,
    substring(raw_record->>'opened_on' from 1 for 4) as opened_on_year,
    count(*) as n
from uscg_recalls_rejected
where failure_stage = 'invariants'
  and failure_reason like '%year-prefix mismatch%'
group by prefix, opened_on_year
order by n desc
limit 30;

\echo
\echo '=== Q7: invariants — non-year-prefix failures sample ==='
\echo 'Date-sanity or null-id failures (other categories from Q4). Smaller'
\echo 'volume; samples here cover edge cases worth documenting in Section M'
\echo 'or N of scraping_observations.md.'

select
    source_recall_id,
    failure_reason,
    raw_record->>'opened_on' as opened_on,
    raw_record->>'company_name' as company_name
from uscg_recalls_rejected
where failure_stage = 'invariants'
  and failure_reason not like '%year-prefix mismatch%'
limit 20;
