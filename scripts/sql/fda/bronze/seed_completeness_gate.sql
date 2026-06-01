-- Phase 6a.5 — FDA full-corpus historical-seed COMPLETENESS gate.
--
-- When to run: immediately after `recalls deep-rescan fda --change-type=historical_seed`
-- lands the full corpus into fda_recalls_bronze on the production (main) branch.
-- Run whoami.sql first to confirm the branch.
--
-- Why this exists: `status=success` is MASKED for this seed. records_landed reports
-- the FETCHED count, not the INSERTED count, so a tie-boundary drop (recalleventid
-- sort has a non-deterministic tie boundary — plan §2) would report success while
-- silently short a row. This gate is the real check; it is a MANDATORY, non-skippable
-- step of the seed procedure (plan §0.6). Compare its output to the RESULTCOUNT the
-- operator captured at seed start (scripts/fda/audit/probe_corpus_completeness.py,
-- ~134,450 as of 2026-05-31).
--
-- A shortfall = a tie-boundary drop → re-run the seed (content-hash dedup makes it
-- idempotent; non-deterministic boundaries mean the dropped row lands on a re-run),
-- then re-gate.
--
-- No parameters. Run with:  psql -f scripts/sql/fda/bronze/seed_completeness_gate.sql

\pset null '<NULL>'

-- 1. Row + distinct counts. count(distinct source_recall_id) is the completeness
--    figure to compare against the captured RESULTCOUNT. total_rows >= distinct_ids
--    by design (re-extractions of edited products add rows); for a fresh single-seed
--    of an empty table they should be equal.
\echo '=== 1. row + distinct product counts (compare distinct_ids to the seed-start RESULTCOUNT) ==='
select
    count(*)                            as total_rows,
    count(distinct source_recall_id)    as distinct_product_ids,
    count(distinct recall_event_id)     as distinct_event_ids
from fda_recalls_bronze;

-- 2. Within-run duplicate check — must be 0. within_batch_dedup collapsed any
--    tie-boundary straddle copies (same PRODUCTID, byte-identical post rid-exclusion).
--    A non-zero count here means a straddle copy with DIFFERING content slipped
--    through (would have raised WithinBatchIdentityCollisionError) — investigate.
\echo ''
\echo '=== 2. within-run duplicate source_recall_id (must be 0) ==='
with latest_run as (
    select max(extraction_timestamp) as ts from fda_recalls_bronze
)
select source_recall_id, count(*) as copies
from fda_recalls_bronze, latest_run
where extraction_timestamp = latest_run.ts
group by source_recall_id
having count(*) > 1
order by copies desc, source_recall_id
limit 50;

-- 3. Null-field census (sort-immune; settles the "core never null" question for free).
--    event_lmd null count should match the probe gap (134,450 - 134,253 = 197 as of
--    2026-05-31). These are the rows the seed exists to capture and that the daily
--    incremental can NEVER re-sweep (a >= eventlmdfrom filter cannot match null) — so
--    a shortfall vs the expected ~197 is the highest-stakes signal in this gate.
\echo ''
\echo '=== 3. null-field census on the four migration-0020 fields ==='
select
    count(*) filter (where event_lmd is null)          as null_event_lmd,
    count(*) filter (where center_cd is null)          as null_center_cd,
    count(*) filter (where product_type_short is null) as null_product_type_short,
    count(*) filter (where firm_legal_nam is null)     as null_firm_legal_nam
from fda_recalls_bronze;

-- 4. Silver dependency: do any null-event_lmd rows ALSO lack recall_initiation_dt?
--    Silver coalesces published_at = coalesce(event_lmd, recall_initiation_dt). If
--    this returns any rows, those would produce a null published_at and FAIL the
--    silver not_null test — extend the coalesce (e.g. posted_internet_dt) per plan
--    §0.1. Expected: 0 rows.
\echo ''
\echo '=== 4. null-event_lmd rows that also lack recall_initiation_dt (silver published_at risk; want 0) ==='
select count(*) as null_published_at_risk
from fda_recalls_bronze
where event_lmd is null
  and recall_initiation_dt is null;

-- 5. Structural-vs-transient characterization of the null-event_lmd rows
--    (sort-immune — groups by recall year, not by a lexically-poisoned date sort).
--    A spread of OLD years => structural archive set (the daily incremental will
--    never pick them up); clustering in recent years => possibly transient/new.
\echo ''
\echo '=== 5. null-event_lmd rows by recall-initiation year (structural vs transient) ==='
select
    extract(year from recall_initiation_dt)::int as recall_year,
    count(*)                                      as rows
from fda_recalls_bronze
where event_lmd is null
group by extract(year from recall_initiation_dt)
order by recall_year;

-- 6. RECENT-RECALL LEAK DETECTOR — the decisive check for "do new recalls arrive
--    null and get missed by the daily incremental?" (the open Phase 7 question).
--    The daily incremental filters on eventlmdfrom >= watermark, so a null-event_lmd
--    record is invisible to it. If any null-event_lmd rows have a RECENT
--    recall_initiation_dt (within ~18 months of the seed), then new recalls CAN
--    arrive null — meaning the eventlmd-only incremental WOULD miss them until/unless
--    FDA later edits the record. That is the trigger to add a recall_initiation_dt
--    watermark leg (the field IS filterable: recallinitiationdtfrom/to per the iRES
--    usage PDF) and/or a periodic full-corpus rescan backstop.
--    EXPECTED if the 197 are a structural archive tail: 0 recent rows.
--    NON-ZERO recent rows => the leak is real; act on it before Phase 7 cron.
\echo ''
\echo '=== 6. RECENT null-event_lmd recalls (initiated since 2025-01-01; want 0 — else the incremental leaks) ==='
select
    count(*)                          as recent_null_eventlmd_rows,
    min(recall_initiation_dt)         as earliest_recent,
    max(recall_initiation_dt)         as latest_recent
from fda_recalls_bronze
where event_lmd is null
  and recall_initiation_dt >= timestamptz '2025-01-01';

-- 7. Full date-field cross-tab on the null-event_lmd subset — which OTHER dates do
--    these records carry? Answers "what could serve as an alternative watermark for
--    them?" A field that is mostly NON-null here is a viable secondary cursor leg;
--    a field that is also mostly null is not. recall_initiation_dt is the prime
--    candidate (a new recall inherently has an initiation date) — confirm its
--    populated-count here.
\echo ''
\echo '=== 7. date-field population WITHIN the null-event_lmd subset (alt-watermark viability) ==='
select
    count(*)                                                  as total_null_eventlmd,
    count(recall_initiation_dt)                               as has_recall_initiation_dt,
    count(center_classification_dt)                           as has_center_classification_dt,
    count(determination_dt)                                   as has_determination_dt,
    count(enforcement_report_dt)                              as has_enforcement_report_dt,
    count(termination_dt)                                     as has_termination_dt,
    count(posted_internet_dt)                                 as has_posted_internet_dt
from fda_recalls_bronze
where event_lmd is null;
