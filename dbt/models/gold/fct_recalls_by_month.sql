{{ config(materialized='view') }}

-- Monthly recall counts per source + an 'ALL' all-source rollup via GROUPING SETS (Phase 6e,
-- ADR 0038). Renamed from recalls_by_month. A per-source page filters source='CPSC', the
-- cross-source dashboard reads source='ALL'.
-- C11 (2026-06-09): period comes from dim_date (lossless join) rather than an inline date_trunc —
-- centralizes the calendar (DRY; fiscal/holiday grains free later).
-- 2026-W25 (fix/announced-at-date-join, ADR 0038 amendment): the join key is
-- coalesce(announced_at, published_at)::date — bucket on the TRUE announce date, not the publish
-- watermark. published_at is a last-published/modified date; FDA's (event_lmd) is bulk-stamped
-- ~2018-09 for the openFDA archive migration, which collapsed all pre-2018 FDA history into one
-- month. announced_at is the backfill-immune initiation date but nullable (~20 FDA), so the coalesce
-- falls back to the non-null published_at — keeping the join lossless (assert_fct_recalls_by_month_reconciles).
select
    dd.month_start                     as period,
    coalesce(re.source, 'ALL')         as source,
    count(distinct re.recall_event_id) as event_count
from {{ ref('recall_event') }} re
join {{ ref('dim_date') }} dd on dd.date_day = coalesce(re.announced_at, re.published_at)::date
group by grouping sets (
    (dd.month_start, re.source),
    (dd.month_start)
)
order by period desc, source
