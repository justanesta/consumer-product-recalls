-- Diagnose what the date-sanity test flagged (Phase 6e.5): are these legitimately-old recalls
-- (so the floor should drop) or real garbage — sentinels / dropped-century typos (keep catching
-- them)? Looks at everything the ORIGINAL <1960-or-future condition would flag.
--   psql "$DATABASE_URL" -f scripts/sql/cross_source/silver/diagnose_date_sanity.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== flagged rows by source + violation band ==='
select
    source,
    count(*)                                                                                  as n,
    count(*) filter (where published_at > now())                                              as pub_future,
    count(*) filter (where published_at < '1940-01-01')                                       as pub_pre1940,
    count(*) filter (where published_at >= '1940-01-01' and published_at < '1960-01-01')       as pub_1940s_50s,
    count(*) filter (where announced_at > now())                                              as ann_future,
    count(*) filter (where announced_at < '1940-01-01')                                       as ann_pre1940,
    count(*) filter (where announced_at >= '1940-01-01' and announced_at < '1960-01-01')       as ann_1940s_50s
from recall_event
where published_at < '1960-01-01' or published_at > now()
   or (announced_at is not null and (announced_at < '1960-01-01' or announced_at > now()))
group by source order by source;

\echo '=== the actual rows (real mid-century recalls, or year-13 / sentinel garbage?) ==='
select
    source,
    source_recall_id,
    extract(year from announced_at) as announced_year,
    extract(year from published_at) as published_year,
    left(title, 50) as title
from recall_event
where published_at < '1960-01-01' or published_at > now()
   or (announced_at is not null and (announced_at < '1960-01-01' or announced_at > now()))
order by coalesce(announced_at, published_at);
