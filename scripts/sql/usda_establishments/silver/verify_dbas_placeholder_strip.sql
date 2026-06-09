-- Phase 6 (feature/silver-field-remap, W4 Phase E) — verify the dbas
-- element-level placeholder strip applied in stg_usda_fsis_establishments.sql
-- and projected through firm_usda_attributes.
--
-- WHEN: after `dbt build --select +firm_usda_attributes`.
-- FEEDS: green-build spot-check for §7 decision #7 (strip the literal element
--   values 'N/A' / 'None' / '' before re-aggregating dbas). No doc update.
--
-- Run with:
--   psql -f scripts/sql/usda_establishments/silver/verify_dbas_placeholder_strip.sql

\echo '=== Q1: placeholder elements remaining (must be 0) ==='
select count(*) as rows_with_placeholder_dba
from firm_usda_attributes
where dbas @> '["N/A"]'::jsonb
   or dbas @> '["None"]'::jsonb
   or dbas @> '[""]'::jsonb;

\echo '=== Q2: dbas fill — real DBAs survive; DBA-less rows are NULL, never [] ==='
select
  count(*) filter (where dbas is not null)   as with_dbas,
  count(*) filter (where dbas is null)       as null_dbas,
  count(*) filter (where dbas = '[]'::jsonb) as empty_array_should_be_zero
from firm_usda_attributes;

\echo '=== Q3: sample surviving real DBAs ==='
select establishment_id, dbas
from firm_usda_attributes
where dbas is not null
limit 5;
