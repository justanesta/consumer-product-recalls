-- Verify the fct_units_recalled grain fix (Phase 6e): collapsing potaff to ONE count per
-- recall_event before summing, vs the naive product-grain sum that overcounts ~100x.
--   psql "$DATABASE_URL" -f scripts/sql/cross_source/silver/verify_units_grain.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== product-grain sum (WRONG, ~96.5B) vs event-grain sum (one count per recall) ==='
with event_grain as (
    select re.source, re.recall_event_id, max(rp.unit_count) as units
    from recall_event re
    join recall_product rp using (recall_event_id)
    where re.source in ('NHTSA', 'USCG') and rp.unit_count is not null
    group by re.source, re.recall_event_id
)
select
    rp.source,
    count(distinct rp.recall_event_id)                                        as recalls,
    sum(rp.unit_count)                                                        as product_grain_sum,
    (select sum(units) from event_grain eg where eg.source = rp.source)       as event_grain_sum,
    (select round(avg(units)) from event_grain eg where eg.source = rp.source) as avg_per_recall,
    (select max(units) from event_grain eg where eg.source = rp.source)       as max_per_recall
from recall_product rp
join recall_event re using (recall_event_id)
where rp.source in ('NHTSA', 'USCG') and rp.unit_count is not null
group by rp.source
order by rp.source;

\echo '=== sanity: top 10 NHTSA recalls by affected count (should be real megarecalls — Takata, etc.) ==='
with event_grain as (
    select re.recall_event_id, re.title, max(rp.unit_count) as units
    from recall_event re
    join recall_product rp using (recall_event_id)
    where re.source = 'NHTSA' and rp.unit_count is not null
    group by re.recall_event_id, re.title
)
select left(title, 60) as title, units
from event_grain
order by units desc
limit 10;

\echo '=== is potaff CONSTANT within a campno (max == exact) or does it VARY across components (max == heuristic)? ==='
select
    count(*) filter (where n_distinct = 1) as constant_potaff_recalls,
    count(*) filter (where n_distinct > 1) as varying_potaff_recalls,
    round(100.0 * count(*) filter (where n_distinct = 1) / count(*), 1) as pct_constant
from (
    select re.recall_event_id, count(distinct rp.unit_count) as n_distinct
    from recall_event re
    join recall_product rp using (recall_event_id)
    where re.source = 'NHTSA' and rp.unit_count is not null
    group by re.recall_event_id
) z;
