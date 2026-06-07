{{ config(materialized='view') }}

-- Yearly recall counts per source + 'ALL' rollup (Phase 6e, ADR 0038). period is Jan 1 of the year.
select
    date_trunc('year', published_at)::date as period,
    coalesce(source, 'ALL')                as source,
    count(distinct recall_event_id)        as event_count
from {{ ref('recall_event') }}
group by grouping sets (
    (date_trunc('year', published_at), source),
    (date_trunc('year', published_at))
)
order by period desc, source
