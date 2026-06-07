{{ config(materialized='view') }}

-- Monthly recall counts per source + an 'ALL' all-source rollup via GROUPING SETS (Phase 6e,
-- ADR 0038). Renamed from recalls_by_month. published_at is the guaranteed non-null contract date;
-- a per-source page filters source='CPSC', the cross-source dashboard reads source='ALL'.
select
    date_trunc('month', published_at)::date as period,
    coalesce(source, 'ALL')                 as source,
    count(distinct recall_event_id)         as event_count
from {{ ref('recall_event') }}
group by grouping sets (
    (date_trunc('month', published_at), source),
    (date_trunc('month', published_at))
)
order by period desc, source
