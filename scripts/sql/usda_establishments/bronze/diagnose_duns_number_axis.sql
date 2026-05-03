-- Drill into the surprise duns_number diff (87% of re-versioned records).
-- Establishes whether the change is:
--   - null ↔ ""    (an empty-string normalization the ADR didn't anticipate
--                   for this column)
--   - "00..." ↔ "..." (leading-zero loss/restoration)
--   - whitespace   ("123 " ↔ "123")
--   - actual value drift (would be very surprising)
--
-- Pass the run_id via -v run_id='<uuid>'.

\pset null '<NULL>'

-- Cross-tab of (old duns category) × (new duns category) for re-versioned records.
with new_versions as (
    select source_recall_id, duns_number
    from usda_fsis_establishments_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
old_versions as (
    select distinct on (source_recall_id) source_recall_id, duns_number
    from usda_fsis_establishments_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
    order by source_recall_id, extraction_timestamp desc
),
classified as (
    select
        case
            when o.duns_number is null then 'null'
            when o.duns_number = ''    then 'empty_string'
            else 'value'
        end as old_kind,
        case
            when n.duns_number is null then 'null'
            when n.duns_number = ''    then 'empty_string'
            else 'value'
        end as new_kind
    from new_versions n
    join old_versions o using (source_recall_id)
    where n.duns_number is distinct from o.duns_number
)
select old_kind, new_kind, count(*) as records
from classified
group by old_kind, new_kind
order by records desc;

-- For records that changed and have actual values on BOTH sides, show 5
-- examples so you can eye-check leading-zero or whitespace patterns.
with new_versions as (
    select source_recall_id, duns_number
    from usda_fsis_establishments_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
old_versions as (
    select distinct on (source_recall_id) source_recall_id, duns_number
    from usda_fsis_establishments_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
    order by source_recall_id, extraction_timestamp desc
)
select
    n.source_recall_id,
    '"' || o.duns_number || '"'  as old_duns_quoted,
    '"' || n.duns_number || '"'  as new_duns_quoted,
    length(o.duns_number)        as old_len,
    length(n.duns_number)        as new_len
from new_versions n
join old_versions o using (source_recall_id)
where n.duns_number is distinct from o.duns_number
  and n.duns_number is not null and n.duns_number <> ''
  and o.duns_number is not null and o.duns_number <> ''
limit 5;
