-- Quarantine drift detector. Watches the four *_rejected tables for new
-- failures that signal a source-side schema change or a Pydantic-validator
-- assumption breaking.
--
-- Quarantine rows are written by BronzeLoader when validate_records() or
-- check_invariants() fails (ADR 0013). They're append-only — a row staying
-- there forever doesn't mean the failure is ongoing, just that it was never
-- cleaned up. The interesting signal is NEW rejections: a rejection with
-- a recent rejected_at that didn't exist yesterday.
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/quarantine_check.sql

\pset null '<NULL>'

with all_rejected as (
    select 'cpsc'::text                as source, source_recall_id, failure_stage,
           failure_reason,              rejected_at, raw_landing_path
    from cpsc_recalls_rejected
    union all
    select 'fda',                       source_recall_id, failure_stage,
           failure_reason,              rejected_at, raw_landing_path
    from fda_recalls_rejected
    union all
    select 'usda',                      source_recall_id, failure_stage,
           failure_reason,              rejected_at, raw_landing_path
    from usda_fsis_recalls_rejected
    union all
    select 'usda_establishments',      source_recall_id, failure_stage,
           failure_reason,              rejected_at, raw_landing_path
    from usda_fsis_establishments_rejected
)
-- 1. Quarantine totals per source — long-running view.
select
    source,
    count(*)                                                     as total_rejected,
    count(*) filter (where rejected_at > now() - interval '1 day')   as last_24h,
    count(*) filter (where rejected_at > now() - interval '7 days')  as last_7d,
    max(rejected_at)                                             as most_recent_rejection
from all_rejected
group by source
order by source;

-- 2. Failure-stage breakdown over the last 7 days.
--    failure_stage values come from validate_records() and check_invariants() —
--    "validate" means a Pydantic schema mismatch (most concerning: source
--    changed shape), "invariants" means a custom check fired (e.g. null source_id,
--    date sanity).
select
    source,
    failure_stage,
    count(*)                                                     as rejections,
    min(rejected_at)                                             as first_seen,
    max(rejected_at)                                             as last_seen
from all_rejected
where rejected_at > now() - interval '7 days'
group by source, failure_stage
order by source, rejections desc;

-- 3. Sample 5 most-recent rejections per source — look at the actual error
--    messages. If the same failure_reason keeps appearing across sources, that's
--    a shared-code bug. If it's source-specific and new, the source changed shape.
select source, source_recall_id, failure_stage,
       left(failure_reason, 120) as failure_reason_excerpt,
       rejected_at
from (
    select *, row_number() over (partition by source order by rejected_at desc) as rn
    from all_rejected
) ranked
where rn <= 5
order by source, rejected_at desc;
