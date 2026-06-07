{{ config(materialized='view') }}

-- Most-recalled firms (Phase 6e, ADR 0038). A lean ranking view over the already-materialized
-- mart_firm_profile (DRY — no re-aggregation), with a rank() window for "top recalled firms"
-- dashboards. The rich per-firm record (aliases, per-source breakdown, SCD-2 attributes) lives in
-- mart_firm_profile; this is the leaderboard.
select
    firm_id,
    canonical_name,
    total_recalls                                    as event_count,
    active_recalls,
    distinct_products                                as product_count,
    first_recall_at,
    last_recall_at,
    rank() over (order by total_recalls desc)        as event_count_rank
from {{ ref('mart_firm_profile') }}
order by total_recalls desc
