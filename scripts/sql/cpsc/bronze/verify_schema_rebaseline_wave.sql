-- Verify the Step 4.5 schema_rebaseline wave for CPSC recalls.
-- Run after `recalls extract cpsc --change-type=schema_rebaseline --lookback-days=<N>`.
-- Pass the run_id printed in the extractor log via -v run_id='<uuid>'.
--
-- Per ADR 0027 line 211, CPSC predicts no wave: schema and staging are
-- unchanged, bronze hashes don't move, and CPSC has no hash_exclude_fields
-- delayed-absorption risk like FDA had. Expected outcome:
--   re_versioned ≈ 0  (only real source-side edits)
--   brand_new   ≈ however many records CPSC published since 2026-04-17

-- 1. Confirm the run is tagged correctly in extraction_runs.
select run_id, source, change_type, records_inserted, started_at, finished_at
from extraction_runs
where source = 'cpsc'
order by started_at desc
limit 5;

-- 2. Split this run's inserts into re-versions vs brand-new identities.
--    CPSC bronze identity is (source_recall_id,) per the BronzeLoader default
--    (src/extractors/cpsc.py:229 — no identity_fields override).
with this_run_pks as (
    select distinct source_recall_id
    from cpsc_recalls_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
prior_pks as (
    select distinct source_recall_id
    from cpsc_recalls_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
)
select
    count(*) filter (where prior_pks.source_recall_id is not null) as re_versioned,
    count(*) filter (where prior_pks.source_recall_id is null)     as brand_new,
    count(*)                                                       as total_inserts_this_run
from this_run_pks
left join prior_pks using (source_recall_id);

-- 3. Sanity: re-versioned identities should now have >= 2 bronze rows
--    with distinct content_hashes (predicted to be ~0 for CPSC).
select count(*) as identities_with_multiple_hashes
from (
    select source_recall_id
    from cpsc_recalls_bronze
    group by source_recall_id
    having count(distinct content_hash) >= 2
) t;
