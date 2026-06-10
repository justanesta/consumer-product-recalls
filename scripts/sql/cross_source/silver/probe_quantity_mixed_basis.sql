-- QTY mixed-basis verification (check-your-work sweep, 2026-06-09): fct_units_recalled aggregates
-- per_product (summed) and total_all_products (max'd) per recall+category. If a single recall_event
-- carries BOTH bases among its valued products, the basis-aware rollup could silently drop the
-- per_product sum. This finds any such recall_event.
--
-- EXPECT 0 rows (clears the dark-pattern concern — the bool_or/max/sum rollup is safe for the
-- single-basis-per-recall case). Non-zero = fct_units_recalled drops per_product amounts for those.
select
    recall_event_id,
    count(*) filter (where quantity_basis = 'total_all_products') as n_total_basis,
    count(*) filter (where quantity_basis = 'per_product')        as n_per_product_basis
from recall_product
where source in ('FDA', 'USDA')
  and quantity_value is not null
group by recall_event_id
having count(*) filter (where quantity_basis = 'total_all_products') > 0
   and count(*) filter (where quantity_basis = 'per_product') > 0
order by recall_event_id;
