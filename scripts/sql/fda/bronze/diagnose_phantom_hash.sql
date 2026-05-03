-- Find records where the schema_rebaseline run produced a NEW content_hash but
-- no visible column-value difference across any hashable column. These are
-- "phantom" hashes — the hash function is producing different output for
-- input that is byte-for-byte identical at the bronze level.
--
-- Output: 5 such records with old and new bronze rows dumped as JSON, side
-- by side, for byte-level comparison. Cast each row to jsonb (which sorts
-- keys deterministically) so you can do a literal text comparison of the
-- two json strings.
--
-- Pass the run_id via -v run_id='<uuid>'.

\pset null '<NULL>'

with new_versions as (
    select * from fda_recalls_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
old_versions as (
    select distinct on (source_recall_id) *
    from fda_recalls_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
    order by source_recall_id, extraction_timestamp desc
),
phantom_pks as (
    -- Records where every hashable column matches but the hash differs.
    -- "Hashable" excludes: id (PK), content_hash, extraction_timestamp,
    -- raw_landing_path, source_recall_id (identity), rid (hash_exclude_fields).
    select n.source_recall_id
    from new_versions n
    join old_versions o using (source_recall_id)
    where n.content_hash != o.content_hash
      and n.recall_event_id                 is not distinct from o.recall_event_id
      and n.center_cd                       is not distinct from o.center_cd
      and n.product_type_short              is not distinct from o.product_type_short
      and n.event_lmd                       is not distinct from o.event_lmd
      and n.firm_legal_nam                  is not distinct from o.firm_legal_nam
      and n.firm_fei_num                    is not distinct from o.firm_fei_num
      and n.recall_num                      is not distinct from o.recall_num
      and n.phase_txt                       is not distinct from o.phase_txt
      and n.center_classification_type_txt  is not distinct from o.center_classification_type_txt
      and n.recall_initiation_dt            is not distinct from o.recall_initiation_dt
      and n.center_classification_dt        is not distinct from o.center_classification_dt
      and n.termination_dt                  is not distinct from o.termination_dt
      and n.enforcement_report_dt           is not distinct from o.enforcement_report_dt
      and n.determination_dt                is not distinct from o.determination_dt
      and n.initial_firm_notification_txt   is not distinct from o.initial_firm_notification_txt
      and n.distribution_area_summary_txt   is not distinct from o.distribution_area_summary_txt
      and n.voluntary_type_txt              is not distinct from o.voluntary_type_txt
      and n.product_description_txt         is not distinct from o.product_description_txt
      and n.product_short_reason_txt        is not distinct from o.product_short_reason_txt
      and n.product_distributed_quantity    is not distinct from o.product_distributed_quantity
    limit 5
)
-- Count first: how many records exhibit the phantom pattern?
select count(*) as phantom_record_count
from new_versions n
join old_versions o using (source_recall_id)
where n.content_hash != o.content_hash
  and n.recall_event_id                 is not distinct from o.recall_event_id
  and n.center_cd                       is not distinct from o.center_cd
  and n.product_type_short              is not distinct from o.product_type_short
  and n.event_lmd                       is not distinct from o.event_lmd
  and n.firm_legal_nam                  is not distinct from o.firm_legal_nam
  and n.firm_fei_num                    is not distinct from o.firm_fei_num
  and n.recall_num                      is not distinct from o.recall_num
  and n.phase_txt                       is not distinct from o.phase_txt
  and n.center_classification_type_txt  is not distinct from o.center_classification_type_txt
  and n.recall_initiation_dt            is not distinct from o.recall_initiation_dt
  and n.center_classification_dt        is not distinct from o.center_classification_dt
  and n.termination_dt                  is not distinct from o.termination_dt
  and n.enforcement_report_dt           is not distinct from o.enforcement_report_dt
  and n.determination_dt                is not distinct from o.determination_dt
  and n.initial_firm_notification_txt   is not distinct from o.initial_firm_notification_txt
  and n.distribution_area_summary_txt   is not distinct from o.distribution_area_summary_txt
  and n.voluntary_type_txt              is not distinct from o.voluntary_type_txt
  and n.product_description_txt         is not distinct from o.product_description_txt
  and n.product_short_reason_txt        is not distinct from o.product_short_reason_txt
  and n.product_distributed_quantity    is not distinct from o.product_distributed_quantity;

-- Then show 5 phantom records as old/new JSONB pairs. The
-- (to_jsonb(row) - excluded_fields) trick removes columns that are not part
-- of the hash input so the JSON dumps should be IDENTICAL if the hash is
-- truly only over bronze columns.
-- (CTE repeated because psql CTEs are scoped per-statement.)
with new_versions as (
    select * from fda_recalls_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
old_versions as (
    select distinct on (source_recall_id) *
    from fda_recalls_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
    order by source_recall_id, extraction_timestamp desc
),
phantom_pks as (
    select n.source_recall_id
    from new_versions n
    join old_versions o using (source_recall_id)
    where n.content_hash != o.content_hash
      and n.recall_event_id                 is not distinct from o.recall_event_id
      and n.center_cd                       is not distinct from o.center_cd
      and n.product_type_short              is not distinct from o.product_type_short
      and n.event_lmd                       is not distinct from o.event_lmd
      and n.firm_legal_nam                  is not distinct from o.firm_legal_nam
      and n.firm_fei_num                    is not distinct from o.firm_fei_num
      and n.recall_num                      is not distinct from o.recall_num
      and n.phase_txt                       is not distinct from o.phase_txt
      and n.center_classification_type_txt  is not distinct from o.center_classification_type_txt
      and n.recall_initiation_dt            is not distinct from o.recall_initiation_dt
      and n.center_classification_dt        is not distinct from o.center_classification_dt
      and n.termination_dt                  is not distinct from o.termination_dt
      and n.enforcement_report_dt           is not distinct from o.enforcement_report_dt
      and n.determination_dt                is not distinct from o.determination_dt
      and n.initial_firm_notification_txt   is not distinct from o.initial_firm_notification_txt
      and n.distribution_area_summary_txt   is not distinct from o.distribution_area_summary_txt
      and n.voluntary_type_txt              is not distinct from o.voluntary_type_txt
      and n.product_description_txt         is not distinct from o.product_description_txt
      and n.product_short_reason_txt        is not distinct from o.product_short_reason_txt
      and n.product_distributed_quantity    is not distinct from o.product_distributed_quantity
    limit 5
)
select
    b.source_recall_id,
    b.extraction_timestamp,
    left(b.content_hash, 12) as hash_prefix,
    to_jsonb(b)
        - 'id'
        - 'content_hash'
        - 'extraction_timestamp'
        - 'raw_landing_path'
        - 'rid'
        as hashable_columns_jsonb
from fda_recalls_bronze b
where b.source_recall_id in (select source_recall_id from phantom_pks)
order by b.source_recall_id, b.extraction_timestamp;
