-- Diagnostic — for a given USDA recalls run and a specific JSONB field
-- that diagnose_wave_field_drivers.sql flagged as a high-volume driver,
-- classify each old/new diff as whitespace-only or a real content change.
--
-- Motivating context (2026-05-15): the 1235-row wave on 2026-05-15 was
-- 82.9% driven by `company_media_contact` diffs. A single sampled recall
-- (`021-2020`) showed the diff was pure whitespace padding. This script
-- closes the empirical loop by checking ALL 1024 pairs that touched
-- `company_media_contact` (or any other field passed via -v field=).
--
-- Classification:
--   * whitespace_only      : regexp_replace(value, '\s', '', 'g') equal
--                            on both sides. The two strings differ ONLY
--                            in whitespace placement / count / kind
--                            (spaces, tabs, newlines). No real content
--                            change.
--   * real_content_change  : whitespace-stripped values differ. A real
--                            character change happened (different name,
--                            different phone, different address).
--   * null_transition      : one side is NULL, the other is non-NULL.
--                            Could be a real edit (contact added/removed)
--                            or an upstream representation change. Treat
--                            as its own bucket — neither pure-cosmetic
--                            nor fully real-content.
--
-- Decision rule for an ADR-0032-analog hash-exclude:
--   * 100% (or near-100%) whitespace_only across many waves
--     → airtight case for hash-exclude. Filing the ADR is safe.
--   * Mixed (e.g., 95% whitespace + 5% real)
--     → hash-exclude loses the 5% real signal. Decide whether the
--     real changes are worth keeping (they probably are if they
--     reflect actual contact/agency changes downstream consumers
--     would care about).
--   * Significant real_content_change rate
--     → DO NOT hash-exclude. The field carries real edit signal.
--
-- Parameters:
--   -v run_id='<uuid>'   (defaults to most recent successful usda run)
--   -v field='<name>'    (defaults to 'company_media_contact')
--
-- Output queries:
--   Q1 — classification distribution (the headline)
--   Q2 — up to 10 sample pairs from each classification bucket so the
--        operator can eyeball whether the heuristic agrees with their
--        own read of the diff.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\if :{?run_id}
\echo
\echo === Using passed run_id: :run_id ===
\else
select run_id as run_id
from extraction_runs
where source = 'usda' and status = 'success'
order by started_at desc
limit 1
\gset
\echo
\echo === Defaulted to most recent successful usda run: :run_id ===
\endif

\if :{?field}
\else
    \set field 'company_media_contact'
\endif

\echo Classifying diffs on field: :field
\echo

select started_at as run_started_at
from extraction_runs
where run_id = :'run_id'
\gset

-- Materialize the (new, old) payload pairs once.
drop table if exists wave_pairs;
create temp table wave_pairs as
with new_versions as (
    select
        source_recall_id, langcode,
        to_jsonb(b)
            - 'id' - 'content_hash' - 'extraction_timestamp' - 'raw_landing_path'
            as payload
    from usda_fsis_recalls_bronze b
    where extraction_timestamp >= :'run_started_at'::timestamptz
),
old_versions as (
    select distinct on (source_recall_id, langcode)
        source_recall_id, langcode,
        to_jsonb(b)
            - 'id' - 'content_hash' - 'extraction_timestamp' - 'raw_landing_path'
            as payload
    from usda_fsis_recalls_bronze b
    where extraction_timestamp < :'run_started_at'::timestamptz
    order by source_recall_id, langcode, extraction_timestamp desc
)
select
    n.source_recall_id,
    n.langcode,
    n.payload as new_payload,
    o.payload as old_payload
from new_versions n
join old_versions o using (source_recall_id, langcode);

-- Restrict to pairs where the chosen field actually differs.
drop table if exists field_diffs;
create temp table field_diffs as
select
    source_recall_id,
    langcode,
    new_payload->>:'field' as new_value,
    old_payload->>:'field' as old_value,
    case
        when (new_payload->:'field') is null
          or (old_payload->:'field') is null
            then 'null_transition'
        when regexp_replace(new_payload->>:'field', '\s', '', 'g')
           = regexp_replace(old_payload->>:'field', '\s', '', 'g')
            then 'whitespace_only'
        else 'real_content_change'
    end as classification
from wave_pairs
where (new_payload->:'field') is distinct from (old_payload->:'field');

\echo '=== Q1: classification distribution ==='
\echo 'Headline: what fraction of pairs that touched this field are pure'
\echo 'whitespace vs real content change vs null transition.'

select
    classification,
    count(*)                                              as n_pairs,
    round(100.0 * count(*) / sum(count(*)) over (), 2)    as pct
from field_diffs
group by classification
order by n_pairs desc;

\echo
\echo '=== Q2: sample pairs from each classification bucket ==='
\echo 'Up to 3 examples per bucket. For whitespace_only: confirm by eye'
\echo 'that the values are visually-similar-but-differently-padded. For'
\echo 'real_content_change: eyeball the substantive difference so we can'
\echo 'judge whether keeping the signal is worth the noise.'

with ranked as (
    select
        classification,
        source_recall_id,
        langcode,
        new_value,
        old_value,
        row_number() over (partition by classification order by source_recall_id, langcode) as rn
    from field_diffs
)
select
    classification,
    source_recall_id,
    langcode,
    left(old_value, 100) as old_value_first_100,
    left(new_value, 100) as new_value_first_100
from ranked
where rn <= 3
order by classification, source_recall_id;

drop table field_diffs;
drop table wave_pairs;
