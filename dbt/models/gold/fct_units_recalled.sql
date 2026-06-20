{{ config(materialized='view') }}

-- Units recalled per source × unit_category × month (Phase 6e, ADR 0038; FDA/USDA added C13).
-- Per-source the MEASURE means DIFFERENT things — always filter by source; there is deliberately
-- NO 'ALL' rollup:
--   * NHTSA / USCG — vehicles / boats POTENTIALLY AFFECTED (clean integer unit_count). category 'count'.
--   * FDA          — quantity DISTRIBUTED (the C13 free-text parse of product_distributed_quantity).
--   * USDA         — weight RECOVERED (qty_recovered, ~all pounds).
-- unit_category (count / weight / volume / grouping) keeps incommensurable units apart — never sum
-- across categories (1,000 cases is not 1,000 lbs), and 'grouping' (lots/components) is not a retail
-- count.
--
-- GRAIN TRAPS:
--   * NHTSA potaff repeats across a campaign's component rows → collapse to ONE max per recall
--     (verified CONSTANT 2026-06-07, so max is EXACT). USCG is 1 product/recall (no explosion).
--   * FDA/USDA distributed-quantity is RECALL-grain free text: the recall-wide figure repeats
--     identically across the recall's product rows (e.g. "15,779,607 units" on all 14 rows), so a SUM
--     over-counts by the row count. The recall-wide figure is the MAX per (recall, category) — listed
--     per-variety subtotals are always < the total. So `units = max(quantity_value)`, basis-agnostic
--     (2026-06-09 corpus dump — scripts/sql/cross_source/silver/dump_quantity_discordance.sql; replaces
--     the basis-sum/GREATEST logic). The v1 parser emits a value only for clean single-quantity strings
--     and NULLs messy multi-product breakdowns (quantity.py precision guards; messy tail → AI extractor
--     v2, freetext-enrichment-backlog), so the rows reaching here are clean → max is the recall total.
-- total_units is a SUM OF PER-RECALL magnitudes (a recall-magnitude measure), not unique items.
with clean_count as (
    select
        re.source,
        re.recall_event_id,
        coalesce(re.announced_at, re.published_at) as event_date,
        'count'::text               as unit_category,
        max(rp.unit_count)::numeric as units
    from {{ ref('recall_event') }} re
    join {{ ref('recall_product') }} rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('NHTSA', 'USCG') and rp.unit_count is not null
    group by re.source, re.recall_event_id, coalesce(re.announced_at, re.published_at)
),

parsed_qty as (
    select
        re.source,
        re.recall_event_id,
        coalesce(re.announced_at, re.published_at) as event_date,
        rp.quantity_category as unit_category,
        -- max, not sum: FDA/USDA quantity is recall-grain (repeats across product rows) — see header.
        max(rp.quantity_value) as units
    from {{ ref('recall_event') }} re
    join {{ ref('recall_product') }} rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('FDA', 'USDA')
      and rp.quantity_value is not null
      and rp.quantity_category is not null
    group by re.source, re.recall_event_id, coalesce(re.announced_at, re.published_at), rp.quantity_category
),

recall_units as (
    select * from clean_count
    union all
    select * from parsed_qty
)

-- C11 (2026-06-09): month period from dim_date (lossless join).
-- 2026-W25 (fix/announced-at-date-join, ADR 0038 amendment): the CTEs carry
-- event_date = coalesce(announced_at, published_at); the month period buckets on the announce date,
-- not the publish watermark (see fct_recalls_by_month). Lossless via the coalesce floor.
select
    ru.source,
    ru.unit_category,
    dd.month_start       as period,
    count(*)             as recalls_with_units,
    sum(ru.units)        as total_units,
    round(avg(ru.units)) as avg_units_per_recall,
    max(ru.units)        as max_units
from recall_units ru
join {{ ref('dim_date') }} dd on dd.date_day = ru.event_date::date
where ru.units is not null
group by ru.source, ru.unit_category, dd.month_start
order by period desc, source, unit_category
