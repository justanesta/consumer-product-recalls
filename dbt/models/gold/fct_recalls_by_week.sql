{{ config(materialized='view') }}

-- Weekly recall counts per source + 'ALL' rollup (Phase 6e, ADR 0038). period is the ISO week
-- start (date_trunc('week') = Monday). Same shape as fct_recalls_by_month at a finer grain.
-- C11 (2026-06-09): period from dim_date.iso_week_start (lossless join), not inline date_trunc.
select
    dd.iso_week_start                  as period,
    coalesce(re.source, 'ALL')         as source,
    count(distinct re.recall_event_id) as event_count
from {{ ref('recall_event') }} re
join {{ ref('dim_date') }} dd on dd.date_day = re.published_at::date
group by grouping sets (
    (dd.iso_week_start, re.source),
    (dd.iso_week_start)
)
order by period desc, source
