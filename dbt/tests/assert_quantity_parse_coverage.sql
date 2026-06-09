{{ config(severity='warn') }}
-- C13 parse-coverage fence: the quantity parser must keep extracting a value from the bulk of
-- non-empty FDA product_distributed_quantity rows. Corpus baseline ~89%; if a future FDA API shape
-- change drops coverage below 80%, this warns — a format change to investigate (re-run
-- `recalls parse-quantities` / extend the parser), not a hard block. Returns one row when breached.
with fda as (
    select quantity_value
    from {{ ref('recall_product') }}
    where source = 'FDA' and number_of_units is not null
)

select 'fda_quantity_parse_coverage_below_80pct' as failure
from fda
having count(*) filter (where quantity_value is not null) < 0.80 * count(*)
