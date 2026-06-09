{{ config(materialized='view') }}

-- Monthly recall counts per source + an 'ALL' all-source rollup via GROUPING SETS (Phase 6e,
-- ADR 0038). Renamed from recalls_by_month. published_at is the guaranteed non-null contract date;
-- a per-source page filters source='CPSC', the cross-source dashboard reads source='ALL'.
-- C11 (2026-06-09): period comes from dim_date (lossless inner join on published_at::date) rather
-- than an inline date_trunc — centralizes the calendar (DRY; fiscal/holiday grains free later).
select
    dd.month_start                     as period,
    coalesce(re.source, 'ALL')         as source,
    count(distinct re.recall_event_id) as event_count
from {{ ref('recall_event') }} re
join {{ ref('dim_date') }} dd on dd.date_day = re.published_at::date
group by grouping sets (
    (dd.month_start, re.source),
    (dd.month_start)
)
order by period desc, source
