-- Dump the FULL CPSC comma-less "of"-strip cohort for holistic review (no LIMIT).
--
-- PURPOSE: the gate measure_comma_optional_of_strip.sql shows only LIMIT-N alphabetical
-- samples, so a verdict on the would-strip bucket reads only the first slice (A-D of ~505).
-- This writes EVERY M/I/D firm name that comma-OPTIONAL strips but comma-REQUIRED does NOT —
-- with the blocklist verdict and the simulated strip — to a CSV so the entire cohort can be
-- reviewed at once (residual FPs in the would-strip set; over-blocks in the blocklisted set).
-- data/exploratory/ is gitignored (.gitignore:64), so the dump is never committed.
--
-- Output: data/exploratory/cpsc/g1_comma_less_cohort.csv
--   columns: blocklisted (t/f), name (raw), opt_stripped (comma-optional strip result)
--   sorted blocklisted-then-name so the would-strip (f) and withheld (t) sets are contiguous.
-- Blocklist + strip regexes are IDENTICAL to the gate (state dropped per the 2026-06-03 run).
--
-- Run from the repo root with: psql ... -f scripts/sql/cpsc/bronze/dump_of_strip_cohorts.sql

\set ON_ERROR_STOP on
\pset format csv
\pset footer off

\echo 'Writing data/exploratory/cpsc/g1_comma_less_cohort.csv (full comma-less cohort, no limit) ...'
\o data/exploratory/cpsc/g1_comma_less_cohort.csv
with mid_names as (
  select m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select distinct
  (name ~* '\y(city|king|isle|cape|gulf|division|subsidiary|club|world|month|empire|centre|center|board|department|university|college|institute|bank|house|taste|scouts|county|district)\s+of\y|,\s*a\s+[a-z][a-z ]{0,25}\s+of\s|\yformerly\s+of\y|\yout\s+of\s+business\y')
                                                       as blocklisted,
  name,
  regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i')     as opt_stripped
from mid_names
where regexp_replace(name, ',?\s*\mof\s+.*$', '', 'i') <> name   -- comma-optional strips it
  and regexp_replace(name, ',\s*of\s+.*$', '', 'i')    = name   -- comma-required does NOT
order by 1, 2;
\o
\echo 'Done -> data/exploratory/cpsc/g1_comma_less_cohort.csv'
