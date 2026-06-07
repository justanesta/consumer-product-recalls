-- Investigate the USDA bilingual pairs where the SPANISH version was modified MORE RECENTLY than
-- English (Phase 6e.5). This contradicts the "USDA updates English-first / English-only" working
-- assumption, and since silver consumes English-only, a real Spanish-only content update would
-- leave silver stale for that recall. Goal: is this a real Spanish-only edit, or a date artifact?
--   psql "$DATABASE_URL" -f scripts/sql/usda_recalls/bronze/investigate_spanish_newer.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Latest version per (recall, langcode) as a session temp table so both queries below can use it
-- (a CTE is scoped to a single statement, which is why an earlier version errored on the 2nd query).
create temporary table lp as
select source_recall_id, langcode, last_modified_date, recall_date, title
from (
    select source_recall_id, langcode, last_modified_date, recall_date, title,
           row_number() over (partition by source_recall_id, langcode order by extraction_timestamp desc) as rn
    from usda_fsis_recalls_bronze
) z
where rn = 1;

\echo '=== the Spanish-newer pairs: how big is the gap, and how old is the recall? ==='
select
    en.source_recall_id,
    en.last_modified_date as en_modified,
    es.last_modified_date as es_modified,
    (es.last_modified_date::date - en.last_modified_date::date) as es_minus_en_days,
    en.recall_date,
    left(en.title, 55) as en_title
from lp en
join lp es on en.source_recall_id = es.source_recall_id
          and en.langcode = 'English' and es.langcode = 'Spanish'
where es.last_modified_date > en.last_modified_date
order by es_minus_en_days desc;

\echo '=== gap-size buckets (1-day skew vs real multi-day Spanish republish) ==='
select
    case
        when d <= 1  then '0-1 day (skew)'
        when d <= 7  then '2-7 days'
        when d <= 30 then '8-30 days'
        else '30+ days (bulk republish)'
    end as gap_bucket,
    count(*) as n
from (
    select (es.last_modified_date::date - en.last_modified_date::date) as d
    from lp en
    join lp es on en.source_recall_id = es.source_recall_id
              and en.langcode = 'English' and es.langcode = 'Spanish'
    where es.last_modified_date > en.last_modified_date
) g
group by 1 order by 1;
