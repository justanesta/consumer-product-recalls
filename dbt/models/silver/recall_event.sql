{{ config(materialized='table') }}

-- Header-level recall events (ADR 0002). One row per (source, source_recall_id).
-- CPSC: source_recall_id = RecallNumber (one row per recall event in bronze).
-- FDA: source_recall_id = RECALLEVENTID::text; DISTINCT ON collapses product-level
--   bronze rows up to a single event header — event-level fields (recall_num,
--   firm_legal_nam, phase_txt, center_classification_type_txt) are stable across
--   all products in the same event, so any representative row is correct.
-- USDA: source_recall_id = field_recall_number; staging filters to English only.
--   published_at coalesces last_modified_date → recall_date because
--   last_modified_date is 42% null per Finding D.
-- NHTSA: source_recall_id = campno (recall campaign ID); DISTINCT ON (campno)
--   collapses many-rows-per-recall (each row is a vehicle × component × batch
--   under the campaign) to one event header. Event-level fields (desc_defect,
--   corrective_action, etc.) are stable across all rows sharing campno, so
--   the representative-row choice is safe. ADR 0031 documents the silver
--   layered design.
-- USCG: source_recall_id = USCG "Number" (year-prefix encoded, e.g. 26MF0158).
--   One row per recall in bronze after stg_uscg_recalls dedup. status derives
--   from the (case-folded per Finding R) disposition: 'open' → 'active',
--   'closed' → 'closed'. announced_at is the staging model's coalesced
--   case_open_date / opened_on with the 1970-01-01 sentinel mapped to NULL
--   per Finding O.

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
),

usda_events as (
    select
        md5('USDA' || '|' || source_recall_id)             as recall_event_id,
        'USDA'                                             as source,
        source_recall_id,
        announced_at,
        coalesce(published_at, announced_at)               as published_at,
        title,
        summary                                            as description,
        url,
        classification,
        case
            when active_notice is true  then 'active'
            when active_notice is false then 'closed'
            else null
        end                                                as status,
        cast(null as jsonb)                                as hazards,
        jsonb_build_object(
            'establishment',         establishment,
            'recall_type',           recall_type,
            'risk_level',            risk_level,
            'recall_reason',         recall_reason,
            'processing',            processing,
            'states',                states,
            'related_to_outbreak',   related_to_outbreak,
            'archive_recall',        archive_recall,
            'closed_at',             closed_at,
            'distro_list',           distro_list,
            'labels',                labels,
            'qty_recovered',         qty_recovered
        )                                                  as source_payload_raw,
        content_hash,
        extraction_timestamp,
        raw_landing_path
    from {{ ref('stg_usda_fsis_recalls') }}
),

nhtsa_events as (
    select distinct on (campno)
        md5('NHTSA' || '|' || campno)                                  as recall_event_id,
        'NHTSA'                                                        as source,
        campno                                                         as source_recall_id,
        rcdate                                                         as announced_at,
        coalesce(datea, rcdate)                                        as published_at,
        campno || ' — ' || mfgname                                     as title,
        desc_defect                                                    as description,
        cast(null as text)                                             as url,
        cast(null as text)                                             as classification,
        case
            when do_not_drive is true then 'do_not_drive'
            when park_outside is true then 'park_outside'
            else null
        end                                                            as status,
        cast(null as jsonb)                                            as hazards,
        jsonb_build_object(
            'desc_defect',       desc_defect,
            'corrective_action', corrective_action,
            'conequence_defect', conequence_defect,
            'mfgcampno',         mfgcampno,
            'influenced_by',     influenced_by,
            'mfgtxt',            mfgtxt,
            'rpno',              rpno,
            'fmvss',             fmvss,
            'do_not_drive',      do_not_drive,
            'park_outside',      park_outside,
            'notes',             notes,
            'potaff',            potaff,
            'odate',             odate
        )                                                              as source_payload_raw,
        content_hash,
        extraction_timestamp,
        raw_landing_path
    from {{ ref('stg_nhtsa_recalls') }}
    order by campno, extraction_timestamp desc
),

uscg_events as (
    -- Filter null-announced_at rows (~2.5% of USCG corpus per Finding O —
    -- the Unix-epoch sentinel 1970-01-01 maps to NULL in stg_uscg_recalls
    -- when both listing and details-page case_open_date are absent).
    -- Those rows remain in bronze for audit and in stg_uscg_recalls for
    -- direct queries, but the cross-source recall_event view requires a
    -- known announce date (matches the cross-source not_null contract +
    -- the precedent of USDA's English-only langcode filter).
    select
        md5('USCG' || '|' || source_recall_id)                         as recall_event_id,
        'USCG'                                                         as source,
        source_recall_id,
        announced_at,
        coalesce(last_date, announced_at)                              as published_at,
        coalesce(company_name, mic, source_recall_id)
            || ' — ' || coalesce(model_name, '(no model)')             as title,
        coalesce(problem_1, problem_2)                                 as description,
        details_url                                                    as url,
        cast(null as text)                                             as classification,
        case
            when disposition = 'open'   then 'active'
            when disposition = 'closed' then 'closed'
            else null
        end                                                            as status,
        cast(null as jsonb)                                            as hazards,
        jsonb_build_object(
            'mic',                  mic,
            'company_official',     company_official,
            'model_year',           model_year,
            'problem_1',            problem_1,
            'problem_2',            problem_2,
            'hin',                  hin,
            'disposition',          disposition,
            'case_open_date',       case_open_date,
            'case_close_date',      case_close_date,
            'campaign_open_date',   campaign_open_date,
            'campaign_close_date',  campaign_close_date,
            'last_date',            last_date,
            'units',                units,
            'boat_type',            boat_type,
            'severity',             severity
        )                                                              as source_payload_raw,
        content_hash,
        extraction_timestamp,
        raw_landing_path
    from {{ ref('stg_uscg_recalls') }}
    where announced_at is not null
)

select * from cpsc_events
union all
select * from fda_events
union all
select * from usda_events
union all
select * from nhtsa_events
union all
select * from uscg_events
