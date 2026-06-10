-- ISSUE-7 verification (check-your-work sweep, 2026-06-09): dim_date.sql starts the spine at
-- 1960-01-01, but assert_recall_event_date_sanity.sql's error floor is 1940-01-01. Four of the five
-- fct_ models INNER JOIN dim_date on published_at::date, so any published_at in [1940, 1959] passes
-- the sanity test yet finds no dim_date row and is SILENTLY DROPPED. published_at is the join column;
-- announced_at (where the known pre-1960 NHTSA dates live) is NOT — this probe distinguishes them.
--
-- EXPECT published_pre_1960 = 0 for every source. Non-zero -> lower the dim_date spine to 1940 (or
-- add reconciliation tests to the 4 unguarded fct_ models).
select
    source,
    min(published_at)                                   as min_published_at,
    count(*) filter (where published_at < '1960-01-01') as published_pre_1960,
    min(announced_at)                                   as min_announced_at,
    count(*) filter (where announced_at < '1960-01-01') as announced_pre_1960
from recall_event
group by source
order by source;
