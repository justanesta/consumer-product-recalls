-- Cross-source pipeline-health snapshot.
-- One look at the last extraction_runs across all sources — answers
-- "did everything run last night and how did it go?"
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/recent_runs.sql

\pset null '<NULL>'

-- 1. Latest run per source — health-at-a-glance.
select distinct on (source)
    source,
    change_type,
    status,
    records_extracted,
    records_inserted,
    records_rejected,
    started_at,
    finished_at - started_at as duration,
    case
        when error_message is not null then '!! ' || left(error_message, 80)
        else ''
    end as error_excerpt
from extraction_runs
order by source, started_at desc;

-- 2. Last 20 runs across all sources, newest first — short history view.
--    Useful for spotting a regression after a code change: e.g. records_inserted
--    suddenly jumps to thousands when it's been single-digit for days.
select
    source,
    change_type,
    status,
    records_extracted,
    records_inserted,
    records_rejected,
    started_at,
    finished_at - started_at as duration
from extraction_runs
order by started_at desc
limit 20;

-- 3. Daily insertion volume per source — drift detector.
--    A source whose insertion rate suddenly spikes 10x without a code change
--    suggests the watermark stopped advancing. A source whose rate drops to 0
--    suggests the API endpoint changed shape.
select
    started_at::date as run_date,
    source,
    count(*)                        as runs,
    sum(records_extracted)          as total_extracted,
    sum(records_inserted)           as total_inserted,
    sum(records_rejected)           as total_rejected,
    count(*) filter (where status != 'completed') as failed_runs
from extraction_runs
where started_at > now() - interval '14 days'
group by 1, 2
order by 1 desc, 2;
