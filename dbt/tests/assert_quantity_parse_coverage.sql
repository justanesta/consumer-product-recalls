{{ config(severity='warn') }}
-- C13 parse-coverage fence: the quantity parser must keep extracting a value from the bulk of
-- non-empty FDA product_distributed_quantity rows. v1 is PRECISION-scoped (2026-06-09) — it
-- deliberately NULLs the ~10% messy shapes (lot/list/code/multi-product; deferred to the AI extractor
-- v2), so coverage measures the CLEAN single-quantity share, not every row. The 75% floor detects an
-- FDA API FORMAT change (a genuine regression), not the intentional precision NULLs — a warn to
-- investigate (re-run `recalls parse-quantities` / extend the parser), not a hard block. Confirm the
-- post-guard baseline on the next parse-quantities run and tighten the floor to ~5pts below it.
with fda as (
    select quantity_value
    from {{ ref('recall_product') }}
    where source = 'FDA' and number_of_units is not null
)

select 'fda_quantity_parse_coverage_below_80pct' as failure
from fda
having count(*) filter (where quantity_value is not null) < 0.75 * count(*)
