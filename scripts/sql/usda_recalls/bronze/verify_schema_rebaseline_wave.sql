-- Verify the Step 4.5 schema_rebaseline wave for USDA recalls.
-- Run after `recalls extract usda --change-type=schema_rebaseline`.
-- Replace :run_id with the run_id printed in the extractor log.

-- 1. Confirm the run is tagged correctly in extraction_runs.
select run_id, source, change_type, records_inserted, started_at, finished_at
from extraction_runs
where source = 'usda'
order by started_at desc
limit 5;

-- 2. Split this run's inserts into re-versions vs brand-new identities.
--    USDA bronze identity is the compound (source_recall_id, langcode) per
--    src/extractors/usda.py:311 — bilingual English+Spanish siblings share
--    field_recall_number but live as separate bronze rows.
--    A re-version means the (source_recall_id, langcode) pair existed in bronze
--    before this run. A brand-new identity is one we'd never seen.
with this_run_pks as (
    select distinct source_recall_id, langcode
    from usda_fsis_recalls_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs
        where run_id = :'run_id'
    )
),
prior_pks as (
    select distinct source_recall_id, langcode
    from usda_fsis_recalls_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs
        where run_id = :'run_id'
    )
)
select
    count(*) filter (where prior_pks.source_recall_id is not null) as re_versioned,
    count(*) filter (where prior_pks.source_recall_id is null)     as brand_new,
    count(*)                                                       as total_inserts_this_run
from this_run_pks
left join prior_pks using (source_recall_id, langcode);

-- 3. Sanity: every re-versioned identity should now have >= 2 bronze rows
--    with distinct content_hashes (old code's hash + new code's hash).
--    Group by the compound identity so we don't conflate bilingual siblings.
select count(*) as identities_with_multiple_hashes
from (
    select source_recall_id, langcode
    from usda_fsis_recalls_bronze
    group by source_recall_id, langcode
    having count(distinct content_hash) >= 2
) t;
