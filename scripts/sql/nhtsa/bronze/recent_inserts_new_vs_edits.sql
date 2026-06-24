-- Are the recent daily bronze inserts NEW recalls, or EDITS to existing ones?
--
-- Q context (2026-06-24): extraction_runs shows nonzero records_inserted on most
-- recent NHTSA run-days (50, 0, 384, 114, 89, 143, ...) yet no new recall has
-- surfaced in the mart for ~7 days. Content-hash dedup (ADR 0007) inserts a new
-- bronze row whenever ANY hashed field of an 11-tuple changes (ADR 0030), so a
-- nonzero insert count does NOT imply a new recall — it is dominated by the
-- documented "broadcast" amendments (a single editorial correction propagated to
-- every 11-tuple row of one recall; incremental_delta_findings.md Section M.3)
-- and by new component/part rows appended to EXISTING campaigns (the multi-batch
-- supersession fan-out, Section L.4).
--
-- This decomposes each recent run-day's inserts into:
--   * NEW-campaign rows   — campno's first-ever bronze appearance is that day
--   * edit / add-on rows  — campno already existed in bronze before that day
-- and counts distinct campnos in each bucket. A day full of inserts but with 0
-- distinct_new_campaigns is exactly "edits only, nothing new to surface."
--
-- CAVEAT: the monthly PRE_2010 deep-rescan day will show OLD archive recalls as
-- "new" (first appearance via the rescan). Q2 carries rcdate so you can tell a
-- genuinely-new recall (recent rcdate) from archive backfill (old rcdate).
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/bronze/recent_inserts_new_vs_edits.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per run-day insert decomposition (last 21 days) ==='
\echo 'rows_inserted should track extraction_runs.records_inserted for that UTC day.'
\echo 'distinct_new_campaigns = brand-new campnos that day; distinct_edited_campaigns ='
\echo 'pre-existing campnos that gained a new version/component row.'

with first_seen as (
    select campno, min(extraction_timestamp)::date as first_seen_day
    from nhtsa_recalls_bronze
    group by campno
)
select
    b.extraction_timestamp::date                                                       as insert_day,
    count(*)                                                                           as rows_inserted,
    count(*) filter (where fs.first_seen_day = b.extraction_timestamp::date)           as new_campaign_rows,
    count(*) filter (where fs.first_seen_day < b.extraction_timestamp::date)           as edit_or_addon_rows,
    count(distinct b.campno) filter (where fs.first_seen_day = b.extraction_timestamp::date) as distinct_new_campaigns,
    count(distinct b.campno) filter (where fs.first_seen_day < b.extraction_timestamp::date) as distinct_edited_campaigns
from nhtsa_recalls_bronze b
join first_seen fs using (campno)
where b.extraction_timestamp >= current_date - 21
group by insert_day
order by insert_day;

\echo
\echo '=== Q2: the distinct NEW campnos first seen in the last 14 days ==='
\echo 'is_genuinely_new = rcdate within 90 days (a real new recall) vs an old archive'
\echo 'recall backfilled by the PRE_2010 deep-rescan (old rcdate, recent first_seen).'
\echo 'If this returns 0 rows, NHTSA published no new campaign in the window — the'
\echo 'mart is correct to show nothing new.'

select
    campno,
    min(extraction_timestamp)::date            as first_seen,
    max(rcdate)::date                          as rcdate,
    (max(rcdate) >= current_date - 90)         as is_genuinely_new,
    max(mfgname)                               as mfgname
from nhtsa_recalls_bronze
group by campno
having min(extraction_timestamp) >= current_date - 14
order by is_genuinely_new desc, rcdate desc nulls last, first_seen desc;
