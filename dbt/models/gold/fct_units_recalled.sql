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
--   * FDA `total_all_products` rows carry a recall-WIDE total that repeats on every product row, so
--     take the MAX (the distinct total), NOT a sum; `per_product` rows SUM across the recall's
--     products. The C13 `quantity_basis` flag drives this — without it, totals would be summed N×.
-- total_units is a SUM OF PER-RECALL magnitudes (a recall-magnitude measure), not unique items.
with clean_count as (
    select
        re.source,
        re.recall_event_id,
        re.published_at,
        'count'::text               as unit_category,
        max(rp.unit_count)::numeric as units
    from {{ ref('recall_event') }} re
    join {{ ref('recall_product') }} rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('NHTSA', 'USCG') and rp.unit_count is not null
    group by re.source, re.recall_event_id, re.published_at
),

parsed_qty as (
    select
        re.source,
        re.recall_event_id,
        re.published_at,
        rp.quantity_category as unit_category,
        case
            when bool_or(rp.quantity_basis = 'total_all_products')
            then max(rp.quantity_value) filter (where rp.quantity_basis = 'total_all_products')
            else sum(rp.quantity_value) filter (where rp.quantity_basis = 'per_product')
        end as units
    from {{ ref('recall_event') }} re
    join {{ ref('recall_product') }} rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('FDA', 'USDA')
      and rp.quantity_value is not null
      and rp.quantity_category is not null
    group by re.source, re.recall_event_id, re.published_at, rp.quantity_category
),

recall_units as (
    select * from clean_count
    union all
    select * from parsed_qty
)

-- C11 (2026-06-09): month period from dim_date (lossless join on published_at::date).
select
    ru.source,
    ru.unit_category,
    dd.month_start       as period,
    count(*)             as recalls_with_units,
    sum(ru.units)        as total_units,
    round(avg(ru.units)) as avg_units_per_recall,
    max(ru.units)        as max_units
from recall_units ru
join {{ ref('dim_date') }} dd on dd.date_day = ru.published_at::date
where ru.units is not null
group by ru.source, ru.unit_category, dd.month_start
order by period desc, source, unit_category
