-- Phase 6 (feature/silver-field-remap, W1) — USDA establishment join-key + DBA profiling.
--
-- CAVEAT: shape / uniqueness / fill EVIDENCE for the silver firm architecture (§6
-- Option A) and the §7 DBA element-filter decision in
-- documentation/usda/field_audit_2026_w22.md. NOT a re-derivation of which fields to
-- capture (Phase 6a, done). An empty-in-bronze finding describes capture state only.
--
-- When to run: against the full-corpus usda_fsis_establishments_bronze. Sibling to:
--   • explore_bronze_shape.sql — status enum, optional-field null rates, false-sentinel,
--     activities/dbas ARRAY SHAPE (avg/max length), MPI date, state dist, re-version.
--   • probe_recall_join_coverage.sql — recall→establishment name/DBA join coverage.
-- Both should be RERUN at corpus scale alongside this. This file adds the two things
-- neither covers and the firm remap needs: the establishment_number JOIN KEY profile
-- (the company_id anchor — uniqueness/population/form, K) and the DBA fill DETAIL
-- (the exact 'N/A'/'None'/'' placeholder counts the §7 element-filter is sized on).
--
-- Latest-per-id (DISTINCT ON source_recall_id = establishment_id, ORDER BY
-- extraction_timestamp DESC) on every query — mirrors what silver staging sees and
-- avoids double-counting bronze re-version generations.
--
-- Output feeds: documentation/usda/field_audit_2026_w22.md §9 (corpus-scale re-val)
-- + documentation/audit/bronze_corpus_profile.md §2/§5 (USDA firm key + attributes).
--
-- Run with: psql ... -f scripts/sql/usda_establishments/bronze/inspect_join_key_and_dbas.sql

\echo '=== Q1: establishment_number — the company_id anchor (population + uniqueness, K) ==='
-- Option A keys firm_usda_attributes on establishment_number and uses it as
-- firm.company_id for USDA. The decision rests on it being 100% populated + 100%
-- unique. pct_unique < 100 or pct_empty > 0 would undermine the canonical-key choice.
with latest as (
  select distinct on (source_recall_id) *
  from usda_fsis_establishments_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*)                                                                  as establishments,
  count(*) filter (where nullif(trim(establishment_number), '') is not null) as populated,
  round(100.0 * count(*) filter (where nullif(trim(establishment_number), '') is null)
    / nullif(count(*), 0), 2)                                               as pct_empty,
  count(distinct establishment_number)                                      as distinct_numbers,
  round(100.0 * count(distinct establishment_number) / nullif(count(*), 0), 2) as pct_unique,
  min(length(establishment_number))                                         as min_len,
  round(avg(length(establishment_number)))                                  as avg_len,
  max(length(establishment_number))                                         as max_len
from latest;

\echo ''
\echo '=== Q2: establishment_number form breakdown (grant-prefix + multi-grant shape) ==='
-- The number is a grant-prefixed string (M=Meat, P=Poultry, I=Imports, G=Eggs,
-- V=Voluntary), with suffix letters (M1234A) and '+'-joined multi-grant forms
-- (M46712+P46712). This characterizes the key shape — relevant to the deferred
-- product_items embedded-number extraction (6b/7) that parses these out of free text.
with latest as (
  select distinct on (source_recall_id) *
  from usda_fsis_establishments_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  count(*)                                                            as establishments,
  count(*) filter (where establishment_number like '%+%')            as multi_grant_joined,
  count(*) filter (where establishment_number ~ '[A-Za-z]$')         as trailing_suffix_letter,
  count(*) filter (where left(establishment_number, 1) = 'M')        as prefix_m_meat,
  count(*) filter (where left(establishment_number, 1) = 'P')        as prefix_p_poultry,
  count(*) filter (where left(establishment_number, 1) = 'I')        as prefix_i_imports,
  count(*) filter (where left(establishment_number, 1) = 'G')        as prefix_g_eggs,
  count(*) filter (where left(establishment_number, 1) = 'V')        as prefix_v_voluntary,
  count(*) filter (where left(establishment_number, 1) !~ '[MPIGV]') as prefix_other
from latest;

\echo ''
\echo '=== Q3: establishment_name uniqueness (why the KEY is the number, not the name) ==='
-- §6/§5: establishment_name is NOT unique (~86% per R2) — same business under multiple
-- grants shares a name. Confirms the join key must be establishment_number. names_shared
-- = records whose normalized name collides with at least one other record.
with latest as (
  select distinct on (source_recall_id) *
  from usda_fsis_establishments_bronze
  order by source_recall_id, extraction_timestamp desc
),
named as (
  select upper(trim(establishment_name)) as nrm
  from latest
  where nullif(trim(establishment_name), '') is not null
)
select
  count(*)                                                       as named_records,
  count(distinct nrm)                                            as distinct_names,
  round(100.0 * count(distinct nrm) / nullif(count(*), 0), 1)    as pct_unique,
  count(*) - count(distinct nrm)                                 as collapse_from_shared_names
from named;

\echo ''
\echo '=== Q4: dbas fill + placeholder detail (the §7 element-filter sizing) ==='
-- §7 decision #7: silver filters the literal element values 'N/A' / 'None' (and '')
-- to null before re-aggregating dbas. This gives the exact corpus counts that filter
-- removes, plus the real-DBA fill rate (R2 saw ~32% with DBAs, 'N/A'×94, 'None'×15).
with latest as (
  select distinct on (source_recall_id) *
  from usda_fsis_establishments_bronze
  order by source_recall_id, extraction_timestamp desc
),
dba_elems as (
  select trim(d) as dba
  from latest, jsonb_array_elements_text(coalesce(dbas, '[]'::jsonb)) as d
)
select
  (select count(*) from latest)                                                            as establishments,
  (select count(*) filter (where dbas is not null and jsonb_array_length(dbas) > 0) from latest) as with_dbas,
  round(100.0 * (select count(*) filter (where dbas is not null and jsonb_array_length(dbas) > 0) from latest)
    / nullif((select count(*) from latest), 0), 1)                                         as pct_with_dbas,
  (select count(*) from dba_elems)                                                         as total_dba_elements,
  (select count(distinct dba) from dba_elems where dba not in ('N/A', 'None', ''))         as distinct_real_dbas,
  (select count(*) from dba_elems where dba = 'N/A')                                       as placeholder_na,
  (select count(*) from dba_elems where dba = 'None')                                      as placeholder_none,
  (select count(*) from dba_elems where dba = '')                                          as placeholder_empty;

\echo ''
\echo '=== Q5: size enum distribution (accepted_values SSOT — the undocumented "N / A") ==='
-- PDF says 3 values; R2 found 4 (Very Small / Small / Large + the undocumented
-- "N / A"). Corpus value set → the firm_usda_attributes.size accepted_values
-- list (warn). '' shown via nullif so a true blank is distinct from the "N / A" string.
with latest as (
  select distinct on (source_recall_id) *
  from usda_fsis_establishments_bronze
  order by source_recall_id, extraction_timestamp desc
)
select
  coalesce(nullif(size, ''), '<empty>') as size,
  count(*) as n,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from latest
group by 1
order by n desc;
