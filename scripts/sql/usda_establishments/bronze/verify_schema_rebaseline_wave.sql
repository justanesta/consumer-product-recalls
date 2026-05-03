-- Verify the Step 4.5 schema_rebaseline wave for USDA establishments.
-- This is the SECOND wave for establishments: ADR 0027 line 214 — the first
-- re-extract on 2026-05-01 absorbed the empty-string axis; this one absorbs
-- the `false`-sentinel reversal (None → "false" for geolocation/county) and
-- the whitespace-strip removal. ADR predicts ~14% of records re-version.
--
-- Run after `recalls extract usda_establishments --change-type=schema_rebaseline`.
-- Pass the run_id printed in the extractor log via -v run_id='<uuid>'.

-- 1. Confirm the run is tagged correctly in extraction_runs.
select run_id, source, change_type, records_inserted, started_at, finished_at
from extraction_runs
where source = 'usda_establishments'
order by started_at desc
limit 5;

-- 2. Split this run's inserts into re-versions vs brand-new identities.
--    Establishments bronze identity is (source_recall_id,) per
--    src/extractors/usda_establishment.py:218 — no langcode axis.
with this_run_pks as (
    select distinct source_recall_id
    from usda_fsis_establishments_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs
        where run_id = :'run_id'
    )
),
prior_pks as (
    select distinct source_recall_id
    from usda_fsis_establishments_bronze
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
left join prior_pks using (source_recall_id);

-- 3. Sanity: every re-versioned identity should now have >= 2 bronze rows
--    with distinct content_hashes (old code's hash + new code's hash).
select count(*) as identities_with_multiple_hashes
from (
    select source_recall_id
    from usda_fsis_establishments_bronze
    group by source_recall_id
    having count(distinct content_hash) >= 2
) t;
