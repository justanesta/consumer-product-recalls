-- ISSUE-3 blast radius (check-your-work sweep, 2026-06-09): parse_quantity reads "78/50-lb bags" as
-- 78 lb (weight) when 78 is a BAG COUNT, not a weight. Those land in recall_product with
-- quantity_category='weight' and a number_of_units matching the N/M-unit container pattern. This
-- sizes the live corruption of the fct_units_recalled weight bucket (count + summed value) so the
-- fix's before/after is measurable. (~586 rows estimated from the committed qty_fda_distinct.csv;
-- this query is the MEASURED prod count.)
select
    count(*)                        as misclassified_rows,
    count(distinct number_of_units) as distinct_strings,
    sum(quantity_value)             as summed_into_weight_bucket
from recall_product
where source in ('FDA', 'USDA')
  and quantity_category = 'weight'
  and number_of_units ~ '[0-9]+/[0-9]+[ -]?(lb|lbs|kg)';

-- A sample of the offending strings, for eyeballing the fix:
select distinct number_of_units, quantity_value
from recall_product
where source in ('FDA', 'USDA')
  and quantity_category = 'weight'
  and number_of_units ~ '[0-9]+/[0-9]+[ -]?(lb|lbs|kg)'
order by number_of_units
limit 40;
