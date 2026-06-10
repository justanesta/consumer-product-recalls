{{ config(materialized='view') }}

-- Yearly recall counts per source + 'ALL' rollup (Phase 6e, ADR 0038). period is Jan 1 of the year.
-- C11 (2026-06-09): period from dim_date (lossless join on published_at::date), not inline date_trunc.
select
    dd.year_start                      as period,
    coalesce(re.source, 'ALL')         as source,
    count(distinct re.recall_event_id) as event_count
from {{ ref('recall_event') }} re
join {{ ref('dim_date') }} dd on dd.date_day = re.published_at::date
group by grouping sets (
    (dd.year_start, re.source),
    (dd.year_start)
)
order by period desc, source
