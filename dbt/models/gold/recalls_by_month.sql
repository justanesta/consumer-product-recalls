{{ config(materialized='view') }}

-- Monthly CPSC recall aggregation for dashboards. Phase 6 adds the other four
-- sources; the `source` column is already present in silver so this view
-- automatically expands when they land.

select
    date_trunc('month', published_at)::date    as month,
    source,
    count(*)                                   as event_count,
    count(distinct recall_event_id)            as distinct_events
from {{ ref('recall_event') }}
group by 1, 2
order by 1 desc, 2
