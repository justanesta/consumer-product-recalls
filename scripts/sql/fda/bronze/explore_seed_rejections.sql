-- Phase 6a.5 — FDA full-corpus historical-seed REJECTION (quarantine) census.
--
-- When to run: after `recalls deep-rescan fda --change-type=historical_seed` reports
-- a non-zero `rejected=` count (2026-06-01 seed: fetched=134450 loaded=134181
-- rejected=24). Pairs with seed_completeness_gate.sql — that gate proves nothing was
-- silently DROPPED; this one explains what was deliberately QUARANTINED and confirms
-- the rejections are benign (an ancient-archive tail), not a data-loss bug.
--
-- Where the 24 come from: extractor invariants (src/extractors/fda.py::check_invariants),
-- which run check_null_source_id then check_date_sanity on recall_initiation_dt
-- (src/bronze/invariants.py). check_date_sanity fails any date in the future or more
-- than 70 years in the past (_MAX_RECALL_AGE_DAYS = 70*365), producing failure_reason
-- "recall_initiation_dt is more than 70 years in the past: <iso>". A failing record is
-- routed to fda_recalls_rejected (NOT bronze) with its full payload in raw_record JSONB.
--
-- Hypothesis to confirm: all 24 are the SAME date-sanity failure — source rows carrying
-- a placeholder / sentinel recall_initiation_dt (e.g. a single ancient default date),
-- not genuine recent recalls. Queries 4-5 settle whether they cluster on one sentinel
-- value (benign source artifact) or spread across real old years (a real archive tail).
--
-- raw_record JSONB keys are the FdaRecord field names (snake_case; model_dump(mode=json)),
-- so dates are ISO strings — cast with ::timestamptz.
--
-- No parameters. Run with:  psql -f scripts/sql/fda/bronze/explore_seed_rejections.sql

\pset null '<NULL>'

-- 0. Run inventory. The rejected table has no run_id, so we anchor "this seed" to its
--    single landing file (one deep-rescan run = one raw_landing_path). If more than one
--    path appears, prior runs' rejects are still present — queries 1-5 scope to the most
--    recent path so the counts reflect THIS seed only.
\echo '=== 0. rejection runs present in fda_recalls_rejected (latest path = the row queries 1-5 scope to) ==='
select
    raw_landing_path,
    failure_stage,
    count(*)            as rejected_rows,
    min(rejected_at)    as first_rejected_at,
    max(rejected_at)    as last_rejected_at
from fda_recalls_rejected
group by raw_landing_path, failure_stage
order by max(rejected_at) desc, raw_landing_path;

-- 1. This-run total + failure_stage split. EXPECTED for the 2026-06-01 seed: 24 rows,
--    all failure_stage='invariants' (Pydantic validation rejected 0 — see the seed log
--    "validate.completed rejected=0"). Any 'validate_records' rows here would be a
--    different, schema-level problem worth a separate look.
\echo ''
\echo '=== 1. this-run rejected count by failure_stage (expect 24, all = invariants) ==='
with latest as (
    select raw_landing_path
    from fda_recalls_rejected
    order by rejected_at desc
    limit 1
)
select
    failure_stage,
    count(*) as rejected_rows
from fda_recalls_rejected
where raw_landing_path = (select raw_landing_path from latest)
group by failure_stage
order by rejected_rows desc;

-- 2. Reason categorization. split_part drops the trailing ": <iso-date>" so the variable
--    date does not fragment the buckets. Confirms whether all 24 share ONE reason
--    ("recall_initiation_dt is more than 70 years in the past") or mix in the "is in the
--    future" branch / a null-source-id failure. One bucket of 24 = clean, expected story.
\echo ''
\echo '=== 2. rejection reason categories (date stripped; expect one bucket: ">70 years in the past") ==='
with latest as (
    select raw_landing_path
    from fda_recalls_rejected
    order by rejected_at desc
    limit 1
)
select
    split_part(failure_reason, ': ', 1) as reason_category,
    count(*)                            as rows
from fda_recalls_rejected
where raw_landing_path = (select raw_landing_path from latest)
group by split_part(failure_reason, ': ', 1)
order by rows desc;

-- 3. The full quarantine listing — every rejected row with the offending date pulled
--    from raw_record plus enough identifying context to eyeball/document each one.
--    Ordered oldest-first. This is the table to paste into the field-audit / runbook
--    note: "the 24 quarantined seed rows were all <year> placeholder-dated archive rows".
\echo ''
\echo '=== 3. full rejected-row listing (offending date + identity context, oldest first) ==='
with latest as (
    select raw_landing_path
    from fda_recalls_rejected
    order by rejected_at desc
    limit 1
)
select
    source_recall_id,
    raw_record->>'recall_event_id'                      as recall_event_id,
    (raw_record->>'recall_initiation_dt')::timestamptz  as recall_initiation_dt,
    raw_record->>'phase_txt'                            as phase_txt,
    raw_record->>'firm_legal_nam'                       as firm_legal_nam,
    left(raw_record->>'product_description_txt', 60)    as product_desc_first60,
    failure_reason
from fda_recalls_rejected
where raw_landing_path = (select raw_landing_path from latest)
order by (raw_record->>'recall_initiation_dt')::timestamptz nulls first, source_recall_id;

-- 4. Year histogram of the offending recall_initiation_dt. A spike on a SINGLE ancient
--    year (e.g. all in 1900 / 1955) => a source sentinel/placeholder default, the most
--    benign explanation. A spread across several pre-1956 years => a genuine ancient
--    archive tail. Either way these predate the 70-year window and are correctly
--    quarantined; this just characterizes them for the docs.
\echo ''
\echo '=== 4. offending recall_initiation_dt by year (one sentinel year vs a real spread?) ==='
with latest as (
    select raw_landing_path
    from fda_recalls_rejected
    order by rejected_at desc
    limit 1
)
select
    extract(year from (raw_record->>'recall_initiation_dt')::timestamptz)::int as initiation_year,
    count(*)                                                                    as rows
from fda_recalls_rejected
where raw_landing_path = (select raw_landing_path from latest)
group by extract(year from (raw_record->>'recall_initiation_dt')::timestamptz)
order by initiation_year nulls first;

-- 5. Exact distinct date values + frequency. The decisive sentinel test: if the 24
--    collapse to one (or a couple) of identical timestamps (a classic placeholder like
--    1900-01-01T00:00:00), that is unambiguously a source default, not real recall dates.
--    Many distinct values => real (if very old) initiation dates.
\echo ''
\echo '=== 5. distinct offending date values (few identical values => source sentinel) ==='
with latest as (
    select raw_landing_path
    from fda_recalls_rejected
    order by rejected_at desc
    limit 1
)
select
    (raw_record->>'recall_initiation_dt')::timestamptz as recall_initiation_dt,
    count(*)                                            as rows
from fda_recalls_rejected
where raw_landing_path = (select raw_landing_path from latest)
group by (raw_record->>'recall_initiation_dt')::timestamptz
order by rows desc, recall_initiation_dt;

-- 6. TYPO-VS-ANCIENT corroboration. Queries 3-5 revealed years 7/12/13/212, with modern
--    firms+products (da Vinci S, HVAD, G7, MagNA Pure 96) — i.e. these are NOT ancient
--    records, they are RECENT recalls with a corrupted recall_initiation_dt year (leading
--    "20" dropped: 2007->0007, 2012->0012, 2013->0013; or transposed: 2012->0212). This
--    query PROVES it: the OTHER date fields (which survived the source intact) should all
--    fall in 2007/2012/2013. If center_classification_dt / determination_dt / termination_dt
--    / posted_internet_dt land in those modern years while recall_initiation_dt reads year
--    <300, the typo theory is confirmed and we are quarantining 14 genuine recall events.
--    (If the other dates ALSO read year <300, they would instead be a coherent ancient
--    record — but the product names already rule that out.)
\echo ''
\echo '=== 6. corroborating date fields on the 24 (modern other-dates => recall_initiation_dt year is a source typo) ==='
with latest as (
    select raw_landing_path
    from fda_recalls_rejected
    order by rejected_at desc
    limit 1
)
select
    source_recall_id,
    (raw_record->>'recall_initiation_dt')::timestamptz   as recall_initiation_dt_typo,
    (raw_record->>'center_classification_dt')::timestamptz as center_classification_dt,
    (raw_record->>'determination_dt')::timestamptz       as determination_dt,
    (raw_record->>'termination_dt')::timestamptz         as termination_dt,
    (raw_record->>'enforcement_report_dt')::timestamptz  as enforcement_report_dt,
    (raw_record->>'posted_internet_dt')::timestamptz     as posted_internet_dt,
    (raw_record->>'event_lmd')::timestamptz              as event_lmd
from fda_recalls_rejected
where raw_landing_path = (select raw_landing_path from latest)
order by (raw_record->>'recall_initiation_dt')::timestamptz nulls first, source_recall_id;
