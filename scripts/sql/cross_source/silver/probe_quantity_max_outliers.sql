-- fct_units_recalled now aggregates max(quantity_value) per (recall, category) — so the per_product
-- SUM (and its over-count) is RETIRED, and probe_quantity_mixed_basis_discordance's big sums are no
-- longer in the metric. The remaining risk with max() is a single MISPARSE it amplifies (a code/lot
-- number that slipped the v1 precision guards). This lists the largest max(value) per recall+category
-- WITH the raw string that produced it, to eyeball for implausible outliers (a real big pet-food recall
-- is tens of millions of cans — fine; a 10-digit code in `count` is a parser gap). Run after
-- `recalls parse-quantities`.
with per_recall as (
    select
        re.source,
        rp.recall_event_id,
        rp.quantity_category,
        max(rp.quantity_value) as max_value
    from recall_event re
    join recall_product rp on rp.recall_event_id = re.recall_event_id
    where re.source in ('FDA', 'USDA')
      and rp.quantity_value is not null
      and rp.quantity_category is not null
    group by re.source, rp.recall_event_id, rp.quantity_category
)
select
    pr.source,
    pr.recall_event_id,
    pr.quantity_category,
    pr.max_value,
    min(rp.number_of_units) as a_string_at_max
from per_recall pr
join recall_product rp
    on rp.recall_event_id = pr.recall_event_id
   and rp.quantity_category = pr.quantity_category
   and rp.quantity_value = pr.max_value
group by pr.source, pr.recall_event_id, pr.quantity_category, pr.max_value
order by pr.max_value desc
limit 40;
