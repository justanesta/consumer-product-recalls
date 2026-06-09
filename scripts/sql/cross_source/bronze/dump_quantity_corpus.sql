-- C13 full-corpus DUMP for parser design (2026-06-09). Profiles top-N samples live in
-- profile_quantity_value_unit.sql; THIS writes the *complete* universe to CSV so the parser can be
-- optimized against the real distribution. FDA + USDA, two views per source:
--   *_templates.csv — every number masked to '#' and lowercased → the distinct GRAMMARS the parser
--                     must handle, by frequency. Every row maps to exactly one template (nothing
--                     dropped); ~57k distinct raw strings collapse to a readable set of shapes.
--   *_distinct.csv  — full distinct raw values by frequency (complete; preserves case + exact numbers)
--                     for verifying number formats / within-template variety. Big (~57k FDA rows) —
--                     grep it rather than read it whole.
--
-- Cred-less workflow — writes gitignored CSVs the agent then Reads:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/bronze/dump_quantity_corpus.sql

\pset format csv

\echo 'writing FDA grammar templates...'
\o data/exploratory/cross_source/qty_fda_templates.csv
select
  lower(regexp_replace(btrim(product_distributed_quantity), '[0-9][0-9.,]*', '#', 'g')) as template,
  count(*) as n
from fda_recalls_bronze
where nullif(btrim(product_distributed_quantity), '') is not null
group by 1
order by n desc;
\o

\echo 'writing FDA full distinct raw...'
\o data/exploratory/cross_source/qty_fda_distinct.csv
select btrim(product_distributed_quantity) as raw, count(*) as n
from fda_recalls_bronze
where nullif(btrim(product_distributed_quantity), '') is not null
group by 1
order by n desc;
\o

\echo 'writing USDA grammar templates...'
\o data/exploratory/cross_source/qty_usda_templates.csv
select
  lower(regexp_replace(btrim(qty_recovered), '[0-9][0-9.,]*', '#', 'g')) as template,
  count(*) as n
from usda_fsis_recalls_bronze
where nullif(btrim(qty_recovered), '') is not null
group by 1
order by n desc;
\o

\echo 'writing USDA full distinct raw...'
\o data/exploratory/cross_source/qty_usda_distinct.csv
select btrim(qty_recovered) as raw, count(*) as n
from usda_fsis_recalls_bronze
where nullif(btrim(qty_recovered), '') is not null
group by 1
order by n desc;
\o

\echo 'done — wrote qty_{fda,usda}_{templates,distinct}.csv to data/exploratory/cross_source/'
