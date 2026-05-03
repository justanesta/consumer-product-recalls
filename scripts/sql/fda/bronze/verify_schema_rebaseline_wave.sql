-- Verify the Step 4.5 schema_rebaseline wave for FDA recalls.
-- Run after `recalls extract fda --change-type=schema_rebaseline --lookback-days=<N>`.
-- Pass the run_id printed in the extractor log via -v run_id='<uuid>'.
--
-- FDA loaded=2742 / fetched=2742 (100% insertion) is the most extreme outcome,
-- so this script's job is mostly to determine whether that's brand-new
-- records (the lookback-days window pulled records bronze never had) or a
-- huge re-version wave. Query 2 splits them.

-- 1. Confirm the run is tagged correctly in extraction_runs.
select run_id, source, change_type, records_inserted, started_at, finished_at
from extraction_runs
where source = 'fda'
order by started_at desc
limit 5;

-- 2. Split this run's inserts into re-versions vs brand-new identities.
--    FDA bronze identity is (source_recall_id,) per the BronzeLoader default
--    (src/bronze/loader.py:78). FDA is single-keyed — no langcode axis.
with this_run_pks as (
    select distinct source_recall_id
    from fda_recalls_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
prior_pks as (
    select distinct source_recall_id
    from fda_recalls_bronze
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

-- 3. Sanity: every re-versioned identity should now have >= 2 bronze rows
--    with distinct content_hashes (old code's hash + new code's hash).
select count(*) as identities_with_multiple_hashes
from (
    select source_recall_id
    from fda_recalls_bronze
    group by source_recall_id
    having count(distinct content_hash) >= 2
) t;
