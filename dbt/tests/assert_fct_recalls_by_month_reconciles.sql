-- Singular test (Phase 6e gold reconciliation): the per-source rows of fct_recalls_by_month must
-- sum to the total recall_event count (each event falls in exactly one month + source). Guards
-- against the GROUPING SETS aggregate silently dropping or double-counting. The 'ALL' rollup rows
-- are excluded from the sum. Returns a row iff the totals diverge (severity=error).
with mart_sum as (
    select sum(event_count) as n from {{ ref('fct_recalls_by_month') }} where source <> 'ALL'
),
event_total as (
    select count(*) as n from {{ ref('recall_event') }}
)
select mart_sum.n as mart_sum, event_total.n as event_total
from mart_sum, event_total
where mart_sum.n <> event_total.n
