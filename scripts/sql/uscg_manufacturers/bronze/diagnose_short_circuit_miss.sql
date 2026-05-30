-- Diagnose: 2026-05-30 daily `recalls extract uscg_manufacturers` did NOT
-- short-circuit. The full 651-page walk ran (Records Found still 16263),
-- 16,263 fetched, 2 loaded, plus one `uscg_manufacturer.parse.empty_mic`
-- warning.
--
-- The empty_mic warning is already explained from code/docs: it is the
-- page-651 out-of-range placeholder row (empty MIC anchor + href ending
-- "id="), the normal end-of-pagination marker. It ONLY appears on a full
-- walk — a short-circuited run stops at page 0 and never reaches it
-- (manufacturer_scraping_observations.md §L.1 Finding D;
-- src/extractors/uscg_manufacturer.py:504-511). So it is a *symptom* of the
-- full walk, not an independent fault. This script answers the other two:
--
--   Q1  Which short-circuit gate failed?  (src/extractors/uscg_manufacturer.py:624)
--         Gate 1 (count):      _records_found_total == last_records_count
--         Gate 2 (membership): every page-0 MIC already in bronze
--       Both must pass to skip the walk. Decision tree:
--         - If the seed/prior run NEVER set last_records_count (deep-rescan
--           skips _update_records_count), today saw NULL  -> Gate 1 fell
--           through -> full walk. (Q2 change_type reveals this.)
--         - If last_records_count was 16263 and a prior run short-circuited
--           cleanly, Gate 1 was passing -> today Gate 2 failed: a MIC on
--           page 0 was not yet in bronze (i.e. one of the 2 loaded records
--           sits at the top of the ASCII sort). (Q6 tests this.)
--
--   Q3  What are the 2 loaded records, and are they NEW MICs or EDITS to
--       existing ones?  (Q4 + Q5)
--
-- Read-only. Run with no args.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: current source_watermarks row ==='
\echo 'CAVEAT: today''s run already finished load_bronze, so _update_records_count'
\echo 'has overwritten last_records_count to the value observed THIS run (expect'
\echo '16263). It therefore tells us the *current* baseline, NOT the value Gate 1'
\echo 'saw at the start of today''s run. Q2 reconstructs the prior state.'

select
    source,
    last_records_count,
    last_successful_extract_at,
    updated_at
from source_watermarks
where source = 'uscg_manufacturers';

\echo
\echo '=== Q2 (LINCHPIN): full extraction_runs history for uscg_manufacturers ==='
\echo 'Read top-to-bottom as the timeline since the source landed 2026-05-30.'
\echo '  - change_type:        "historical_seed"/"schema_rebaseline" = deep-rescan'
\echo '                        path (does NOT set last_records_count -> the NEXT'
\echo '                        incremental run sees NULL and CANNOT short-circuit).'
\echo '                        "routine" = incremental extract (DOES set it).'
\echo '  - was_short_circuited: TRUE on any prior run PROVES Gate 1 was passing'
\echo '                        (count baseline correct) -> today''s miss is Gate 2.'
\echo '                        If today is the FIRST run after a deep-rescan seed,'
\echo '                        the miss is Gate 1 (NULL baseline) instead.'
\echo '  - records_extracted ~16263 + records_inserted=2 on the latest row is the'
\echo '                        run under investigation.'

select
    started_at,
    finished_at - started_at as duration,
    change_type,
    status,
    was_short_circuited,
    records_extracted,
    records_inserted,
    records_rejected,
    run_id
from extraction_runs
where source = 'uscg_manufacturers'
order by started_at;

\echo
\echo '=== Q3: bronze row count vs distinct MICs ==='
\echo 'bronze is append-only / versioned (ADR 0007): an EDIT to an existing MIC'
\echo 'adds a new row sharing the same source_recall_id. extra_versions > 0 means'
\echo 'at least one MIC has been re-loaded with changed content over its lifetime.'

select
    count(*)                                          as total_rows,
    count(distinct source_recall_id)                  as distinct_mics,
    count(*) - count(distinct source_recall_id)       as extra_versions_from_edits
from uscg_manufacturers_bronze;

\echo
\echo '=== Q4: the rows inserted by the most recent run (the "2 loaded") ==='
\echo 'All rows at max(extraction_timestamp). load() stamps every inserted row'
\echo 'with one timestamp per run, so this isolates exactly the latest run''s'
\echo 'inserts. Eyeball company/city/state for plausibility.'

select
    source_recall_id,
    company_name,
    address,
    city,
    state,
    uscg_directory_id,
    detail_url,
    content_hash,
    extraction_timestamp
from uscg_manufacturers_bronze
where extraction_timestamp = (
    select max(extraction_timestamp) from uscg_manufacturers_bronze
)
order by source_recall_id;

\echo
\echo '=== Q5: NEW MIC vs EDIT — full bronze history for each just-loaded MIC ==='
\echo 'versions_for_mic = 1  -> brand-NEW MIC (first time the directory showed it;'
\echo '                         a genuine addition since the seed).'
\echo 'versions_for_mic > 1  -> EDIT: the MIC existed; company/address/city/state'
\echo '                         changed, producing a new content_hash. Compare the'
\echo '                         rows to see which field moved.'

with latest_ts as (
    select max(extraction_timestamp) as ts from uscg_manufacturers_bronze
),
loaded as (
    select distinct source_recall_id
    from uscg_manufacturers_bronze
    where extraction_timestamp = (select ts from latest_ts)
)
select
    b.source_recall_id,
    b.company_name,
    b.city,
    b.state,
    b.uscg_directory_id,
    b.content_hash,
    b.extraction_timestamp,
    count(*) over (partition by b.source_recall_id) as versions_for_mic
from uscg_manufacturers_bronze b
join loaded l on l.source_recall_id = b.source_recall_id
order by b.source_recall_id, b.extraction_timestamp;

\echo
\echo '=== Q6: are the loaded MICs on page 0? (Gate 2 test) ==='
\echo 'The directory paginates in the source''s own order; §B establishes that is'
\echo 'ASCII MIC order (digit block 101-126 sorts first, onto page 0), 25 rows per'
\echo 'page. So ascii_rank <= 25 strongly implies the MIC is on page 0 -> if it was'
\echo 'absent from bronze when today''s run started, Gate 2 (membership) would have'
\echo 'failed, forcing the walk. ascii_rank well above 25 means the MIC is deep in'
\echo 'the corpus, Gate 2 would NOT have seen it, so the miss must be Gate 1.'
\echo 'NOTE: rank is computed over current distinct MICs as a proxy for the live'
\echo 'page-0 set; treat as inference, not proof of source pagination order.'

with latest_ts as (
    select max(extraction_timestamp) as ts from uscg_manufacturers_bronze
),
loaded as (
    select distinct source_recall_id
    from uscg_manufacturers_bronze
    where extraction_timestamp = (select ts from latest_ts)
),
ranked as (
    select
        source_recall_id,
        row_number() over (order by source_recall_id) as ascii_rank
    from (select distinct source_recall_id from uscg_manufacturers_bronze) d
)
select
    r.source_recall_id,
    r.ascii_rank,
    case when r.ascii_rank <= 25
         then 'PAGE 0 -> Gate 2 failure candidate'
         else 'NOT page 0 -> points to Gate 1 (NULL/changed count)'
    end as page_inference
from ranked r
join loaded l on l.source_recall_id = r.source_recall_id
order by r.ascii_rank;

\echo
\echo '=== Q7: page-0 neighborhood (the 30 lowest MICs) for context ==='
\echo 'These are roughly the MICs Gate 2 checks for membership on page 0. The two'
\echo 'loaded MICs appearing here = Gate 2 story; absent here = Gate 1 story.'

select
    source_recall_id,
    company_name,
    state
from (
    select distinct on (source_recall_id)
        source_recall_id, company_name, state
    from uscg_manufacturers_bronze
    order by source_recall_id, extraction_timestamp desc
) latest
order by source_recall_id
limit 30;
