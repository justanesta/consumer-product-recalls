-- Singular test (2026-W25, fix/announced-at-date-join): the per-source rows of fct_recalls_by_week
-- must sum to the total recall_event count. Guards that the coalesce(announced_at, published_at) join
-- key stays LOSSLESS — every event falls in exactly one ISO week + source, none dropped by the move
-- off the non-null published_at. The 'ALL' rollup rows are excluded. Returns a row iff the totals
-- diverge (severity=error). Mirrors assert_fct_recalls_by_month_reconciles at the weekly grain.
with mart_sum as (
    select sum(event_count) as n from {{ ref('fct_recalls_by_week') }} where source <> 'ALL'
),
event_total as (
    select count(*) as n from {{ ref('recall_event') }}
)
select mart_sum.n as mart_sum, event_total.n as event_total
from mart_sum, event_total
where mart_sum.n <> event_total.n
