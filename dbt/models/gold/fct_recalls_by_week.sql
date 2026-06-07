{{ config(materialized='view') }}

-- Weekly recall counts per source + 'ALL' rollup (Phase 6e, ADR 0038). period is the ISO week
-- start (date_trunc('week') = Monday). Same shape as fct_recalls_by_month at a finer grain.
select
    date_trunc('week', published_at)::date as period,
    coalesce(source, 'ALL')                as source,
    count(distinct recall_event_id)        as event_count
from {{ ref('recall_event') }}
group by grouping sets (
    (date_trunc('week', published_at), source),
    (date_trunc('week', published_at))
)
order by period desc, source
