{{ config(materialized='view') }}

-- Active-vs-inactive recall window per source + 'ALL' rollup (Phase 6e, ADR 0038). The cross-source
-- signal is recall_event.is_active (FDA phase, USDA recall_type, USCG disposition); CPSC + NHTSA
-- carry no active/closed status -> 'unknown'. The richer "currently active" presence signal
-- (recall_lifecycle.is_currently_active) is USDA-only v1 and is surfaced on mart_recall_summary.
with statused as (
    select
        source,
        recall_event_id,
        case
            when is_active is true  then 'active'
            when is_active is false then 'inactive'
            else 'unknown'
        end as status
    from {{ ref('recall_event') }}
)

select
    coalesce(source, 'ALL')          as source,
    status,
    count(distinct recall_event_id)  as event_count
from statused
group by grouping sets (
    (source, status),
    (status)
)
order by source, status
