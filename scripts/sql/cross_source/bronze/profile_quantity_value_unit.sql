-- C13 scoping probe (2026-06-09): can we cleanly parse a (quantity_value, quantity_unit) pair out of
-- the free-text recall-quantity fields, across the FULL corpus, for FDA AND USDA? Sizes the build
-- decision (plan C13 / freetext-enrichment-backlog "FDA distributed-quantity"). Extends the Phase-6
-- coverage buckets in scripts/sql/fda/bronze/profile_freetext_normalization.sql Q1 with the UNIT
-- VOCABULARY + sample extractions + the USDA cross-source field.
--
-- Fields (both TEXT in bronze):
--   FDA  fda_recalls_bronze.product_distributed_quantity   per-product → recall_product.number_of_units
--   USDA usda_fsis_recalls_bronze.qty_recovered            per-recall, weight-shaped (lbs)
-- (The recalled product NAME is already in recall_product — this probe only sizes amount+unit.)
--
-- Parse target: leading number (commas/decimal ok) + a trailing unit word:
--   "1,200 cases" -> (1200, cases) · "919,616.31 lbs" -> (919616.31, lbs) · "500 bottles" -> (500, bottles)
--
-- Cred-less workflow — redirect to a gitignored file so it can be Read back for analysis:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/bronze/profile_quantity_value_unit.sql \
--     > data/exploratory/cross_source/quantity_profile.txt 2>&1

\echo '=== Q1: FDA product_distributed_quantity — population + coverage (filters non-exclusive) ==='
with q as (select btrim(coalesce(product_distributed_quantity, '')) as raw from fda_recalls_bronze)
select
  count(*)                                                                                 as total_rows,
  count(*) filter (where raw = '')                                                         as empty,
  count(*) filter (where lower(raw) ~ '^(undetermined|unknown|none|n/?a|tbd|not (determined|available|provided))') as sentinel,
  count(*) filter (where raw ~ '^[0-9][0-9,]*$')                                           as pure_integer,
  count(*) filter (where raw ~ '^[0-9][0-9,]*\.[0-9]+$')                                   as pure_decimal,
  count(*) filter (where raw ~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z]')                      as number_then_unit,
  count(*) filter (where raw <> '' and raw !~ '^[0-9]')                                    as no_leading_number,
  round(
    100.0 * count(*) filter (
      where raw ~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z]' or raw ~ '^[0-9][0-9,]*(\.[0-9]+)?$'
    ) / nullif(count(*) filter (where raw <> ''), 0), 1
  )                                                                                         as pct_parseable_of_nonempty
from q;

\echo '=== Q2: FDA extracted UNIT vocabulary — top 40 trailing unit tokens ==='
select
  lower((regexp_match(product_distributed_quantity, '^[0-9][0-9,]*(?:\.[0-9]+)?\s*([A-Za-z]+)'))[1]) as unit_token,
  count(*) as n
from fda_recalls_bronze
where product_distributed_quantity ~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z]'
group by 1
order by n desc
limit 40;

\echo '=== Q3: FDA sample — raw -> (amount, unit), 25 random clean rows ==='
select
  btrim(product_distributed_quantity)                                                                as raw,
  (regexp_match(product_distributed_quantity, '^([0-9][0-9,]*(?:\.[0-9]+)?)'))[1]                     as amount,
  lower((regexp_match(product_distributed_quantity, '^[0-9][0-9,]*(?:\.[0-9]+)?\s*([A-Za-z]+)'))[1])  as unit
from fda_recalls_bronze
where product_distributed_quantity ~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z]'
order by random()
limit 25;

\echo '=== Q4: FDA messy tail A — non-numeric-leading values (top 30 by frequency) ==='
select btrim(product_distributed_quantity) as raw, count(*) as n
from fda_recalls_bronze
where nullif(btrim(product_distributed_quantity), '') is not null
  and product_distributed_quantity !~ '^[0-9]'
group by 1
order by n desc
limit 30;

\echo '=== Q4b: FDA messy tail B — numeric-leading but NOT cleanly (number[+unit]) — top 25 ==='
select btrim(product_distributed_quantity) as raw, count(*) as n
from fda_recalls_bronze
where product_distributed_quantity ~ '^[0-9]'
  and btrim(product_distributed_quantity) !~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z .]*$'
group by 1
order by n desc
limit 25;

\echo '=== Q5: USDA qty_recovered — population + unit vocabulary (cross-source taxonomy) ==='
with q as (select btrim(coalesce(qty_recovered, '')) as raw from usda_fsis_recalls_bronze)
select
  count(*)                                                              as total_rows,
  count(*) filter (where raw = '')                                     as empty,
  count(*) filter (where raw ~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z]')  as number_then_unit
from q;
\echo '--- USDA unit vocabulary (top 25) ---'
select
  lower((regexp_match(qty_recovered, '^[0-9][0-9,]*(?:\.[0-9]+)?\s*([A-Za-z]+)'))[1]) as unit_token,
  count(*) as n
from usda_fsis_recalls_bronze
where qty_recovered ~ '^[0-9][0-9,]*(\.[0-9]+)?\s*[A-Za-z]'
group by 1
order by n desc
limit 25;
