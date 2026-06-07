{{ config(materialized='view') }}

-- Recall counts by classification + risk_level per source + 'ALL' rollup (Phase 6e, ADR 0038).
-- classification/risk_level are source-native enums (FDA Class I/II/III; USDA Class I/II/III/PHA;
-- USCG H/L/M/S; CPSC/NHTSA have none -> NULL, shown as the "unclassified" bucket). The 'ALL'
-- rollup is descriptive only — the enums are not conformed across sources (ADR 0036 D2).
select
    coalesce(source, 'ALL')          as source,
    classification,
    risk_level,
    count(distinct recall_event_id)  as event_count
from {{ ref('recall_event') }}
group by grouping sets (
    (source, classification, risk_level),
    (classification, risk_level)
)
order by source, event_count desc
