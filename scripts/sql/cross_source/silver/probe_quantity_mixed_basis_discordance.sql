-- SUPERSEDED 2026-06-09 by probe_quantity_max_outliers.sql. fct_units_recalled now aggregates
-- max(quantity_value) (not GREATEST/sum), so the sum_per_product column below is NO LONGER in the
-- metric — its large gaps are expected leftovers of the retired basis-sum logic, not over-counts.
-- Kept only for historical comparison.
--
-- Mixed-basis discordance probe (2026-06-09): fct_units_recalled now uses
-- GREATEST(max_total, sum_per_product) per (source, recall_event, unit_category). This measures, for
-- groups carrying BOTH a total_all_products and a per_product quantity, how far apart the two are —
-- i.e. how often the old "always take max(total)" undercounted (per_product_wins = GREATEST now picks
-- the per-product sum the old logic dropped). RUN AFTER `recalls parse-quantities` + `dbt build` so it
-- reflects the ISSUE-3 N/M-lb parser fix.
with g as (
    select
        re.source,
        re.recall_event_id,
        rp.quantity_category as unit_category,
        max(rp.quantity_value) filter (where rp.quantity_basis = 'total_all_products') as max_total,
        sum(rp.quantity_value) filter (where rp.quantity_basis = 'per_product')        as sum_per_product
    from recall_event re
    join recall_product rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('FDA', 'USDA')
      and rp.quantity_value is not null
      and rp.quantity_category is not null
    group by re.source, re.recall_event_id, rp.quantity_category
)
select
    count(*) filter (where max_total is not null and sum_per_product is not null)         as both_bases_groups,
    count(*) filter (where max_total is not null and sum_per_product is not null
                       and max_total <> sum_per_product)                                 as discordant_groups,
    count(*) filter (where sum_per_product > max_total)                                   as per_product_wins,
    count(*) filter (where max_total > sum_per_product)                                   as total_wins,
    round(max(abs(coalesce(max_total, 0) - coalesce(sum_per_product, 0))), 2)             as max_abs_gap
from g;

-- The 20 widest gaps, to eyeball whether per_product_wins are real undercounts the parser should fix:
select
    g.source,
    g.recall_event_id,
    g.unit_category,
    g.max_total,
    g.sum_per_product,
    abs(coalesce(g.max_total, 0) - coalesce(g.sum_per_product, 0)) as abs_gap
from (
    select
        re.source,
        re.recall_event_id,
        rp.quantity_category as unit_category,
        max(rp.quantity_value) filter (where rp.quantity_basis = 'total_all_products') as max_total,
        sum(rp.quantity_value) filter (where rp.quantity_basis = 'per_product')        as sum_per_product
    from recall_event re
    join recall_product rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('FDA', 'USDA')
      and rp.quantity_value is not null
      and rp.quantity_category is not null
    group by re.source, re.recall_event_id, rp.quantity_category
) g
where g.max_total is not null and g.sum_per_product is not null and g.max_total <> g.sum_per_product
order by abs_gap desc
limit 20;
