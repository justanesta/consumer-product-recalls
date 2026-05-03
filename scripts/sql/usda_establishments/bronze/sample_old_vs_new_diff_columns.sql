-- Sample 5 re-versioned records and show their old + new bronze rows
-- side-by-side for the columns that showed diffs in
-- diagnose_rebaseline_column_diffs.sql.
--
-- Use this to eyeball whether the diffs are:
--   - Real semantic changes (e.g. "001234567" vs "1234567", "false" vs NULL,
--     ["a", "b"] vs ["a ", " b"]) — confirms case 2a (audit underestimation,
--     wave is benign).
--   - Identical character-by-character with only content_hash differing —
--     confirms case 2b (phantom hash change, refactor has a bug).
--
-- Pass the run_id via -v run_id='<uuid>'.

with this_run as (
    select * from usda_fsis_establishments_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
sample_pks as (
    -- Prefer records where duns_number changed, since it's the biggest
    -- unexplained diff. Falls back to any re-versioned record.
    select n.source_recall_id
    from this_run n
    join (
        select distinct on (source_recall_id) source_recall_id, duns_number, content_hash
        from usda_fsis_establishments_bronze
        where extraction_timestamp < (
            select started_at from extraction_runs where run_id = :'run_id'
        )
        order by source_recall_id, extraction_timestamp desc
    ) o using (source_recall_id)
    where n.duns_number is distinct from o.duns_number
    limit 5
)
select
    b.source_recall_id,
    b.extraction_timestamp,
    left(b.content_hash, 8) as hash_prefix,
    b.duns_number,
    b.phone,
    b.fips_code,
    b.geolocation,
    b.county,
    b.activities,
    b.dbas
from usda_fsis_establishments_bronze b
where b.source_recall_id in (select source_recall_id from sample_pks)
order by b.source_recall_id, b.extraction_timestamp;
