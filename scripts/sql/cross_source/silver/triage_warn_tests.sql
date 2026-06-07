-- Triage of recurring warn-level tests (Phase 6e.5). Grounds the keep / backfill / remove verdict
-- with the actual rows behind each warning.
--   psql "$DATABASE_URL" -f scripts/sql/cross_source/silver/triage_warn_tests.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== (1) announced_at null (6 expected) — FDA archive tail; is ANY other date available? ==='
select source, source_recall_id, announced_at, published_at, left(title, 60) as title
from recall_event
where announced_at is null
order by source, source_recall_id;

\echo '=== (2) firm_fda_attributes null country (1 expected) — is country inferable from city/state? ==='
select firm_fei_num, firm_legal_nam, firm_city_nam, firm_state_cd,
       firm_state_prvnc_nam, firm_country_nam, firm_postal_cd
from firm_fda_attributes
where firm_country_nam is null;

\echo '=== (3) USDA bilingual non-atomic pairs — current count + direction (silver uses English-only) ==='
with latest as (
    select source_recall_id, langcode, last_modified_date,
           row_number() over (partition by source_recall_id, langcode order by extraction_timestamp desc) as rn
    from usda_fsis_recalls_bronze
),
lp as (select source_recall_id, langcode, last_modified_date from latest where rn = 1)
select
    count(*)                                                               as non_atomic_pairs,
    count(*) filter (where en.last_modified_date > es.last_modified_date)  as english_newer,
    count(*) filter (where es.last_modified_date > en.last_modified_date)  as spanish_newer
from lp en
join lp es on en.source_recall_id = es.source_recall_id
          and en.langcode = 'English' and es.langcode = 'Spanish'
where en.last_modified_date is distinct from es.last_modified_date;
