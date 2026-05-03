-- Diagnose which columns are driving the schema_rebaseline wave for FDA recalls.
-- For each column, count how many re-versioned identities have a different value
-- before vs after this run. Run only if Query 2 of the verify file shows a
-- non-trivial re_versioned count.
--
-- ADR 0027 line 212 / line 198 expects diffs to concentrate in optional text
-- columns where FDA mixes null and '' for the same field across records
-- (Finding J in documentation/fda/api_observations.md). The empty-string axis
-- means new bronze rows preserve '' where old bronze rows had null.
--
-- Pre-flight expectation:
--   - Free-text fields (product_description_txt, distribution_area_summary_txt,
--     initial_firm_notification_txt, product_short_reason_txt) — likely high
--     diff counts.
--   - Coded short text (center_cd, product_type_short, voluntary_type_txt,
--     phase_txt) — likely low diff counts (these tend to be populated).
--   - Date/numeric columns (event_lmd, firm_fei_num, all the *_dt columns) —
--     should be near 0. Non-zero here would suggest a serialization or
--     timezone-handling change worth investigating.
--   - rid is excluded from hashing (hash_exclude_fields, fda.py:259), so its
--     diff doesn't matter — but recall_event_id is hashed and should be stable.
--
-- Pass the run_id via -v run_id='<uuid>'.

with new_versions as (
    select *
    from fda_recalls_bronze
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
)
select
    count(*) as total_compared,
    sum((n.recall_event_id                    is distinct from o.recall_event_id)::int)                    as recall_event_id_diff,
    sum((n.center_cd                          is distinct from o.center_cd)::int)                          as center_cd_diff,
    sum((n.product_type_short                 is distinct from o.product_type_short)::int)                 as product_type_short_diff,
    sum((n.event_lmd                          is distinct from o.event_lmd)::int)                          as event_lmd_diff,
    sum((n.firm_legal_nam                     is distinct from o.firm_legal_nam)::int)                     as firm_legal_nam_diff,
    sum((n.firm_fei_num                       is distinct from o.firm_fei_num)::int)                       as firm_fei_num_diff,
    sum((n.recall_num                         is distinct from o.recall_num)::int)                         as recall_num_diff,
    sum((n.phase_txt                          is distinct from o.phase_txt)::int)                          as phase_txt_diff,
    sum((n.center_classification_type_txt     is distinct from o.center_classification_type_txt)::int)     as center_classification_type_txt_diff,
    sum((n.recall_initiation_dt               is distinct from o.recall_initiation_dt)::int)               as recall_initiation_dt_diff,
    sum((n.center_classification_dt           is distinct from o.center_classification_dt)::int)           as center_classification_dt_diff,
    sum((n.termination_dt                     is distinct from o.termination_dt)::int)                     as termination_dt_diff,
    sum((n.enforcement_report_dt              is distinct from o.enforcement_report_dt)::int)              as enforcement_report_dt_diff,
    sum((n.determination_dt                   is distinct from o.determination_dt)::int)                   as determination_dt_diff,
    sum((n.initial_firm_notification_txt      is distinct from o.initial_firm_notification_txt)::int)      as initial_firm_notification_txt_diff,
    sum((n.distribution_area_summary_txt      is distinct from o.distribution_area_summary_txt)::int)      as distribution_area_summary_txt_diff,
    sum((n.voluntary_type_txt                 is distinct from o.voluntary_type_txt)::int)                 as voluntary_type_txt_diff,
    sum((n.product_description_txt            is distinct from o.product_description_txt)::int)            as product_description_txt_diff,
    sum((n.product_short_reason_txt           is distinct from o.product_short_reason_txt)::int)           as product_short_reason_txt_diff,
    sum((n.product_distributed_quantity       is distinct from o.product_distributed_quantity)::int)       as product_distributed_quantity_diff
from new_versions n
join old_versions o using (source_recall_id);
