-- Phase 5b.2 Step 3 — recall→establishment join coverage probe.
--
-- The motivating use case for the establishment dataset is enriching USDA
-- recall events: stg_usda_fsis_recalls.establishment (free text, 65%
-- populated per Phase 5b dbt spot-check) joins to
-- usda_fsis_establishments_bronze.establishment_name (or .dbas array fallback)
-- on normalized name, attaching the FSIS establishment_id (stable FK), address,
-- geolocation, FIPS, and active-MPI status to recalls.
--
-- This probe answers: of the USDA recall records that name an establishment,
-- what fraction match a known establishment in the listing API? Result feeds
-- documentation/usda/establishment_join_coverage.md and informs the Phase 5b.2
-- Step 5 silver join shape (whether to require dbas fallback, whether fuzzy
-- matching is needed, etc.).
--
-- Both bronze tables are deduped to latest-per-id before the join to avoid
-- inflated counts from re-version generations.

\echo '=== Q1: top-line counts ==='
with latest_recalls as (
    select distinct on (source_recall_id, langcode) *
    from usda_fsis_recalls_bronze
    where langcode = 'English'
    order by source_recall_id, langcode, extraction_timestamp desc
),
latest_ests as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    (select count(*) from latest_recalls) as total_english_recalls,
    (select count(*) from latest_recalls
       where establishment is not null and trim(establishment) <> '') as recalls_with_establishment,
    (select count(distinct upper(trim(establishment))) from latest_recalls
       where establishment is not null and trim(establishment) <> '') as distinct_recall_establishments,
    (select count(*) from latest_ests) as total_establishments,
    (select count(distinct upper(trim(establishment_name))) from latest_ests) as distinct_establishment_names;

\echo ''
\echo '=== Q2: name-only match rate (no DBA fallback) ==='
-- This is the simplest possible join — recall.establishment normalized vs.
-- establishment.establishment_name normalized. Unmatched here either: (a) need
-- DBA fallback, (b) need fuzzy matching, or (c) refer to establishments that
-- have aged out of the active+inactive listing.
with latest_recalls as (
    select distinct on (source_recall_id, langcode) *
    from usda_fsis_recalls_bronze
    where langcode = 'English'
    order by source_recall_id, langcode, extraction_timestamp desc
),
latest_ests as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
),
recall_names as (
    select distinct upper(trim(establishment)) as nrm
    from latest_recalls
    where establishment is not null and trim(establishment) <> ''
),
est_names as (
    select distinct upper(trim(establishment_name)) as nrm
    from latest_ests
)
select
    count(*) as distinct_recall_names,
    count(est_names.nrm) as matched_via_name,
    count(*) - count(est_names.nrm) as unmatched,
    round(100.0 * count(est_names.nrm) / nullif(count(*), 0), 2) as match_pct
from recall_names left join est_names using (nrm);

\echo ''
\echo '=== Q3: name + DBA fallback match rate ==='
-- For establishments operating under a doing-business-as alias, the recall
-- might use the DBA name while the establishment record lists the legal name.
-- This query expands the establishment side to include each DBA value.
with latest_recalls as (
    select distinct on (source_recall_id, langcode) *
    from usda_fsis_recalls_bronze
    where langcode = 'English'
    order by source_recall_id, langcode, extraction_timestamp desc
),
latest_ests as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
),
recall_names as (
    select distinct upper(trim(establishment)) as nrm
    from latest_recalls
    where establishment is not null and trim(establishment) <> ''
),
est_lookup as (
    -- Both establishment_name and every dbas entry contribute to the lookup set.
    select distinct upper(trim(establishment_name)) as nrm from latest_ests
    union
    select distinct upper(trim(dba_value)) as nrm
    from latest_ests, jsonb_array_elements_text(dbas) as dba_value
    where trim(dba_value) <> ''
)
select
    count(*) as distinct_recall_names,
    count(est_lookup.nrm) as matched_via_name_or_dba,
    count(*) - count(est_lookup.nrm) as unmatched,
    round(100.0 * count(est_lookup.nrm) / nullif(count(*), 0), 2) as match_pct
from recall_names left join est_lookup using (nrm);

\echo ''
\echo '=== Q4: per-record match rate (not per distinct name) ==='
-- More directly answers "what fraction of USDA recall events would gain an
-- establishment FK in silver?" — a popular establishment with many recalls
-- counts each recall separately.
with latest_recalls as (
    select distinct on (source_recall_id, langcode) *
    from usda_fsis_recalls_bronze
    where langcode = 'English'
    order by source_recall_id, langcode, extraction_timestamp desc
),
latest_ests as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
),
est_lookup as (
    select distinct upper(trim(establishment_name)) as nrm from latest_ests
    union
    select distinct upper(trim(dba_value)) as nrm
    from latest_ests, jsonb_array_elements_text(dbas) as dba_value
    where trim(dba_value) <> ''
),
recalls_classified as (
    select
        case
            when establishment is null or trim(establishment) = '' then 'no_establishment_field'
            when upper(trim(establishment)) in (select nrm from est_lookup) then 'matched'
            else 'unmatched'
        end as match_status
    from latest_recalls
)
select match_status, count(*) as n,
       round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from recalls_classified
group by match_status
order by n desc;

\echo ''
\echo '=== Q5: sample 20 unmatched recall establishments — what do they look like? ==='
-- Manual review reveals whether unmatched names are typo variants
-- (fuzzy-matchable), historical establishments (gone from listing), or other.
with latest_recalls as (
    select distinct on (source_recall_id, langcode) *
    from usda_fsis_recalls_bronze
    where langcode = 'English'
    order by source_recall_id, langcode, extraction_timestamp desc
),
latest_ests as (
    select distinct on (source_recall_id) *
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
),
est_lookup as (
    select distinct upper(trim(establishment_name)) as nrm from latest_ests
    union
    select distinct upper(trim(dba_value)) as nrm
    from latest_ests, jsonb_array_elements_text(dbas) as dba_value
    where trim(dba_value) <> ''
)
select
    establishment as recall_establishment,
    count(*) as recalls_referencing,
    min(recall_date::date) as first_recall_date,
    max(recall_date::date) as last_recall_date
from latest_recalls
where establishment is not null
  and trim(establishment) <> ''
  and upper(trim(establishment)) not in (select nrm from est_lookup)
group by establishment
order by recalls_referencing desc, last_recall_date desc
limit 20;

\echo ''
\echo '=== Q6: establishments matching > 1 recall (multi-hit popularity) ==='
-- Counts how concentrated the recall→establishment relationship is. If most
-- matches concentrate on a small number of establishments, the firm dim has
-- high-value rollups. If matches are uniformly spread, less so.
with latest_recalls as (
    select distinct on (source_recall_id, langcode) *
    from usda_fsis_recalls_bronze
    where langcode = 'English'
    order by source_recall_id, langcode, extraction_timestamp desc
)
select
    upper(trim(establishment)) as normalized_name,
    count(*) as recall_count
from latest_recalls
where establishment is not null and trim(establishment) <> ''
group by normalized_name
having count(*) >= 5
order by recall_count desc
limit 15;
