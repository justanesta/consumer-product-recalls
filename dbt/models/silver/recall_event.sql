{{ config(materialized='table') }}

-- Header-level recall events (ADR 0002). One row per (source, source_recall_id).
-- CPSC: source_recall_id = RecallNumber (one row per recall event in bronze).
-- FDA: source_recall_id = RECALLEVENTID::text; DISTINCT ON collapses product-level
--   bronze rows up to a single event header — event-level fields (recall_num,
--   firm_legal_nam, phase_txt, center_classification_type_txt) are stable across
--   all products in the same event, so any representative row is correct.

with cpsc_events as (
    select
        md5('CPSC' || '|' || source_recall_id) as recall_event_id,
        'CPSC'                                 as source,
        source_recall_id,
        announced_at,
        published_at,
        title,
        description,
        url,
        cast(null as text)                     as classification,
        cast(null as text)                     as status,
        hazards,
        jsonb_build_object(
            'recall_id',              recall_id,
            'consumer_contact',       consumer_contact,
            'sold_at_label',          sold_at_label,
            'manufacturer_countries', manufacturer_countries,
            'product_upcs',           product_upcs,
            'remedies',               remedies,
            'remedy_options',         remedy_options,
            'in_conjunctions',        in_conjunctions,
            'images',                 images,
            'injuries',               injuries
        )                                      as source_payload_raw,
        content_hash,
        extraction_timestamp,
        raw_landing_path
    from {{ ref('stg_cpsc_recalls') }}
),

fda_events as (
    select distinct on (recall_event_id)
        md5('FDA' || '|' || recall_event_id::text)                       as recall_event_id,
        'FDA'                                                            as source,
        recall_event_id::text                                            as source_recall_id,
        recall_initiation_dt                                             as announced_at,
        event_lmd                                                        as published_at,
        coalesce(recall_num, center_cd || '-' || recall_event_id::text)
            || ' — ' || firm_legal_nam                                   as title,
        distribution_area_summary_txt                                    as description,
        cast(null as text)                                               as url,
        center_classification_type_txt                                   as classification,
        phase_txt                                                        as status,
        cast(null as jsonb)                                              as hazards,
        jsonb_build_object(
            'recall_num',                    recall_num,
            'center_cd',                     center_cd,
            'product_type_short',            product_type_short,
            'firm_fei_num',                  firm_fei_num,
            'center_classification_dt',      center_classification_dt,
            'termination_dt',                termination_dt,
            'enforcement_report_dt',         enforcement_report_dt,
            'determination_dt',              determination_dt,
            'initial_firm_notification_txt', initial_firm_notification_txt,
            'voluntary_type_txt',            voluntary_type_txt
        )                                                                as source_payload_raw,
        content_hash,
        extraction_timestamp,
        raw_landing_path
    from {{ ref('stg_fda_recalls') }}
    order by recall_event_id, extraction_timestamp desc
)

select * from cpsc_events
union all
select * from fda_events
