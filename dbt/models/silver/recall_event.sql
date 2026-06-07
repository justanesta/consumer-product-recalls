{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
      {'columns': ['source', 'source_recall_id']},
      {'columns': ['source', 'published_at']},
      {'columns': ['classification']},
    ]
) }}

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
        cast(null as timestamptz)              as first_posted_at,
        title,
        description                            as recall_reason,
        url,
        cast(null as text)                     as classification,
        cast(null as text)                     as lifecycle_status,
        hazards,
        cast(null as text)                     as distribution_area_summary,
        cast(null as boolean)                  as is_active,
        cast(null as text)                     as recall_initiator,
        cast(null as text)                     as initiated_by,
        cast(null as text)                     as risk_level,
        cast(null as text)                     as notification_method,
        cast(null as text)                     as reason_category,
        'Unspecified'                          as distribution_scope,
        cast(null as text)                     as distribution_states,
        cast(null as timestamptz)              as terminated_at,
        cast(null as timestamptz)              as campaign_started_at,
        cast(null as timestamptz)              as campaign_ended_at,
        cast(null as timestamptz)              as last_editorial_date,
        cast(null as timestamptz)              as owner_notified_at,
        cast(null as boolean)                  as do_not_drive,
        cast(null as boolean)                  as park_outside,
        remedies                               as remedies,
        remedy_options                         as remedy_options,
        injuries                               as injuries,
        images                                 as images,
        consumer_contact                       as consumer_contact,
        manufacturer_countries                 as manufacturer_countries,
        in_conjunctions                        as coordinated_recall_urls,
        retailers                              as sales_channel_narrative,
        product_upcs                           as product_upcs,
        cast(null as boolean)                  as related_to_outbreak,
        cast(null as boolean)                  as archived,
        cast(null as text)                     as firm_contact_block_text,
        cast(null as text)                     as corrective_action,
        cast(null as text)                     as consequence_of_defect,
        cast(null as text)                     as notes,
        cast(null as text)                     as mfgcampno,
        cast(null as text)                     as fmvss,
        cast(null as text)                     as firm_contact_person,
        -- B3b: source_payload_raw now holds only genuinely-residual fields —
        -- every mapped field was lifted to a first-class column in B1–B3, and
        -- sold_at_label is dropped (documented-empty in the CPSC corpus).
        jsonb_build_object(
            'recall_id', recall_id
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
        -- announced_at is the TRUE recall-initiation date, left NULLABLE by design.
        -- Two null classes: (a) ~6 early-2000s archive recalls whose recall_initiation_dt
        -- FDA's iRES never carried forward; (b) ~14 recalls with a DROPPED-CENTURY typo in
        -- recall_initiation_dt (parsed year 7/12/13/212 — e.g. Z-0660-2013 initiated "year 13"),
        -- nulled via the >= 1940 guard below rather than trusting a garbage year
        -- (precision-over-recall; the recall NUMBER carries the real year, and published_at
        -- (= event_lmd) is the hard NOT-NULL contract date downstream sorts on). We do NOT
        -- fabricate a date. announced_at's not_null is severity=warn (~20 baseline), a visible
        -- watch-list. Provenance: bronze check_date_sanity DID quarantine these (its >70yr-past
        -- branch, invariants.py); they are genuine recalls deliberately reinstated by
        -- `recalls recover-rejected fda` (false-positive quarantine, recovery.py), kept faithful
        -- to source per ADR 0027. Bronze stays the source of truth; this guard is the correct
        -- silver-side normalization of the source typo.
        case when recall_initiation_dt >= '1940-01-01'
             then recall_initiation_dt end                               as announced_at,
        -- event_lmd is nullable as of migration 0020: ~197 un-edited records have
        -- null EVENTLMD (Finding H). Coalesce to recall_initiation_dt (mirrors the
        -- USDA branch) so silver published_at stays non-null per the strict-silver
        -- contract. The post-seed gate censuses null recall_initiation_dt on the
        -- null-event_lmd subset; if any exist, extend this coalesce (plan §0.1).
        coalesce(event_lmd, recall_initiation_dt)                        as published_at,
        -- First public posting (W1b). Distinct from published_at (=event_lmd), which
        -- drifts when FDA re-touches archived records (Finding M). NULL pre-2022-10-25.
        posted_internet_dt                                               as first_posted_at,
        coalesce(recall_num, center_cd || '-' || recall_event_id::text)
            || ' — ' || firm_legal_nam                                   as title,
        -- Bug 1 fix: recall_reason is the defect reason (product_short_reason_txt),
        -- NOT the geographic distribution list it was mistakenly sourced from.
        product_short_reason_txt                                         as recall_reason,
        cast(null as text)                                               as url,
        center_classification_type_txt                                   as classification,
        phase_txt                                                        as lifecycle_status,
        cast(null as jsonb)                                              as hazards,
        -- Bug 1's other half: the distribution list moves to its own column.
        distribution_area_summary_txt                                    as distribution_area_summary,
        case phase_txt
            when 'Ongoing'    then true
            when 'Terminated' then false
            when 'Completed'  then false
        end                                                              as is_active,
        voluntary_type_txt                                               as recall_initiator,
        case
            when voluntary_type_txt in ('Firm Initiated', 'Voluntary: Firm Initiated') then 'firm'
            when voluntary_type_txt in ('FDA Requested', 'FDA Mandated') then 'agency'
        end                                                              as initiated_by,
        cast(null as text)                                               as risk_level,
        initial_firm_notification_txt                                    as notification_method,
        cast(null as text)                                               as reason_category,
        {{ classify_distribution_scope('distribution_area_summary_txt') }} as distribution_scope,
        cast(null as text)                                               as distribution_states,
        termination_dt                                                   as terminated_at,
        cast(null as timestamptz)                                        as campaign_started_at,
        cast(null as timestamptz)                                        as campaign_ended_at,
        cast(null as timestamptz)                                        as last_editorial_date,
        cast(null as timestamptz)                                        as owner_notified_at,
        cast(null as boolean)                                            as do_not_drive,
        cast(null as boolean)                                            as park_outside,
        cast(null as jsonb)                                              as remedies,
        cast(null as jsonb)                                              as remedy_options,
        cast(null as jsonb)                                              as injuries,
        cast(null as jsonb)                                              as images,
        cast(null as text)                                               as consumer_contact,
        cast(null as jsonb)                                              as manufacturer_countries,
        cast(null as jsonb)                                              as coordinated_recall_urls,
        cast(null as jsonb)                                              as sales_channel_narrative,
        cast(null as jsonb)                                              as product_upcs,
        cast(null as boolean)                                            as related_to_outbreak,
        cast(null as boolean)                                            as archived,
        cast(null as text)                                               as firm_contact_block_text,
        cast(null as text)                                               as corrective_action,
        cast(null as text)                                               as consequence_of_defect,
        cast(null as text)                                               as notes,
        cast(null as text)                                               as mfgcampno,
        cast(null as text)                                               as fmvss,
        cast(null as text)                                               as firm_contact_person,
        -- B3b: residual fields only. termination_dt → terminated_at,
        -- initial_firm_notification_txt → notification_method, and
        -- voluntary_type_txt → recall_initiator are now first-class columns.
        jsonb_build_object(
            'recall_num',               recall_num,
            'center_cd',                center_cd,
            'product_type_short',       product_type_short,
            'firm_fei_num',             firm_fei_num,
            'center_classification_dt', center_classification_dt,
            'enforcement_report_dt',    enforcement_report_dt,
            'determination_dt',         determination_dt
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
        cast(null as timestamptz)                          as first_posted_at,
        title,
        summary                                            as recall_reason,
        url,
        classification,
        recall_type                                        as lifecycle_status,
        cast(null as jsonb)                                as hazards,
        cast(null as text)                                 as distribution_area_summary,
        case recall_type
            when 'Active Recall'        then true
            when 'Public Health Alert'  then true
            when 'Closed Recall'        then false
        end                                                as is_active,
        cast(null as text)                                 as recall_initiator,
        cast(null as text)                                 as initiated_by,
        -- risk_level is DERIVED 1:1 from classification (W1 Q2 proof), not lifted.
        case classification
            when 'Class I'              then 'High - Class I'
            when 'Class II'             then 'Low - Class II'
            when 'Class III'            then 'Marginal - Class III'
            when 'Public Health Alert'  then 'Public Health Alert'
        end                                                as risk_level,
        cast(null as text)                                 as notification_method,
        recall_reason                                      as reason_category,
        {{ classify_distribution_scope('states') }}        as distribution_scope,
        states                                             as distribution_states,
        closed_at                                          as terminated_at,
        cast(null as timestamptz)                          as campaign_started_at,
        cast(null as timestamptz)                          as campaign_ended_at,
        cast(null as timestamptz)                          as last_editorial_date,
        cast(null as timestamptz)                          as owner_notified_at,
        cast(null as boolean)                              as do_not_drive,
        cast(null as boolean)                              as park_outside,
        cast(null as jsonb)                                as remedies,
        cast(null as jsonb)                                as remedy_options,
        cast(null as jsonb)                                as injuries,
        cast(null as jsonb)                                as images,
        cast(null as text)                                 as consumer_contact,
        cast(null as jsonb)                                as manufacturer_countries,
        cast(null as jsonb)                                as coordinated_recall_urls,
        cast(null as jsonb)                                as sales_channel_narrative,
        cast(null as jsonb)                                as product_upcs,
        related_to_outbreak                                as related_to_outbreak,
        archive_recall                                     as archived,
        company_media_contact                              as firm_contact_block_text,
        cast(null as text)                                 as corrective_action,
        cast(null as text)                                 as consequence_of_defect,
        cast(null as text)                                 as notes,
        cast(null as text)                                 as mfgcampno,
        cast(null as text)                                 as fmvss,
        cast(null as text)                                 as firm_contact_person,
        -- B3b: residual fields only. recall_type → lifecycle_status,
        -- recall_reason → reason_category, states → distribution_states,
        -- related_to_outbreak / archive_recall (→ archived) / closed_at
        -- (→ terminated_at) are now first-class columns. risk_level is kept as
        -- the source anchor for the classification-derived risk_level column;
        -- processing / distro_list / labels are recall_product-grain residuals.
        jsonb_build_object(
            'establishment',  establishment,
            'risk_level',     risk_level,
            'processing',     processing,
            'distro_list',    distro_list,
            'labels',         labels,
            'qty_recovered',  qty_recovered
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
        cast(null as timestamptz)                                      as first_posted_at,
        campno || ' — ' || mfgname                                     as title,
        desc_defect                                                    as recall_reason,
        cast(null as text)                                             as url,
        cast(null as text)                                             as classification,
        -- NHTSA has no lifecycle status; the old do_not_drive/park_outside "hack"
        -- is dropped — those become first-class booleans in B2b.
        cast(null as text)                                             as lifecycle_status,
        cast(null as jsonb)                                            as hazards,
        cast(null as text)                                             as distribution_area_summary,
        cast(null as boolean)                                          as is_active,
        influenced_by                                                  as recall_initiator,
        case
            when influenced_by = 'MFR'                              then 'firm'
            when influenced_by in ('ODI', 'OVSC', 'ISSUE_INVGSTN')  then 'agency'
        end                                                            as initiated_by,
        cast(null as text)                                             as risk_level,
        cast(null as text)                                             as notification_method,
        cast(null as text)                                             as reason_category,
        'Nationwide'                                                   as distribution_scope,  -- derived default: federal vehicle recalls are national (no state field)
        cast(null as text)                                             as distribution_states,
        cast(null as timestamptz)                                      as terminated_at,
        cast(null as timestamptz)                                      as campaign_started_at,
        cast(null as timestamptz)                                      as campaign_ended_at,
        cast(null as timestamptz)                                      as last_editorial_date,
        odate                                                          as owner_notified_at,
        do_not_drive                                                   as do_not_drive,
        park_outside                                                   as park_outside,
        cast(null as jsonb)                                            as remedies,
        cast(null as jsonb)                                            as remedy_options,
        cast(null as jsonb)                                            as injuries,
        cast(null as jsonb)                                            as images,
        cast(null as text)                                             as consumer_contact,
        cast(null as jsonb)                                            as manufacturer_countries,
        cast(null as jsonb)                                            as coordinated_recall_urls,
        cast(null as jsonb)                                            as sales_channel_narrative,
        cast(null as jsonb)                                            as product_upcs,
        cast(null as boolean)                                          as related_to_outbreak,
        cast(null as boolean)                                          as archived,
        cast(null as text)                                             as firm_contact_block_text,
        corrective_action                                              as corrective_action,
        conequence_defect                                              as consequence_of_defect,
        notes                                                          as notes,
        mfgcampno                                                      as mfgcampno,
        fmvss                                                          as fmvss,
        cast(null as text)                                             as firm_contact_person,
        -- B3b: residual fields only. desc_defect → recall_reason,
        -- corrective_action / conequence_defect (→ consequence_of_defect) /
        -- notes / mfgcampno / fmvss / do_not_drive / park_outside are now
        -- first-class columns; influenced_by → recall_initiator; odate →
        -- owner_notified_at. rpno is dropped (documented-empty). mfgtxt is the
        -- firm-grain residual; potaff (potentially-affected count) is unmapped.
        jsonb_build_object(
            'mfgtxt', mfgtxt,
            'potaff', potaff
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
        cast(null as timestamptz)                                      as first_posted_at,
        coalesce(company_name, mic, source_recall_id)
            || ' — ' || coalesce(model_name, '(no model)')             as title,
        coalesce(problem_1, problem_2)                                 as recall_reason,
        details_url                                                    as url,
        severity                                                       as classification,
        initcap(disposition)                                           as lifecycle_status,
        cast(null as jsonb)                                            as hazards,
        cast(null as text)                                             as distribution_area_summary,
        case disposition
            when 'open'   then true
            when 'closed' then false
        end                                                            as is_active,
        cast(null as text)                                             as recall_initiator,
        cast(null as text)                                             as initiated_by,
        cast(null as text)                                             as risk_level,
        cast(null as text)                                             as notification_method,
        cast(null as text)                                             as reason_category,
        'Unspecified'                                                  as distribution_scope,
        cast(null as text)                                             as distribution_states,
        case_close_date                                                as terminated_at,
        campaign_open_date                                             as campaign_started_at,
        campaign_close_date                                            as campaign_ended_at,
        last_date                                                      as last_editorial_date,
        cast(null as timestamptz)                                      as owner_notified_at,
        cast(null as boolean)                                          as do_not_drive,
        cast(null as boolean)                                          as park_outside,
        cast(null as jsonb)                                            as remedies,
        cast(null as jsonb)                                            as remedy_options,
        cast(null as jsonb)                                            as injuries,
        cast(null as jsonb)                                            as images,
        cast(null as text)                                             as consumer_contact,
        cast(null as jsonb)                                            as manufacturer_countries,
        cast(null as jsonb)                                            as coordinated_recall_urls,
        cast(null as jsonb)                                            as sales_channel_narrative,
        cast(null as jsonb)                                            as product_upcs,
        cast(null as boolean)                                          as related_to_outbreak,
        cast(null as boolean)                                          as archived,
        cast(null as text)                                             as firm_contact_block_text,
        cast(null as text)                                             as corrective_action,
        cast(null as text)                                             as consequence_of_defect,
        cast(null as text)                                             as notes,
        cast(null as text)                                             as mfgcampno,
        cast(null as text)                                             as fmvss,
        company_official                                               as firm_contact_person,
        -- B3b: residual fields only. company_official → firm_contact_person,
        -- severity → classification, case_close_date → terminated_at,
        -- campaign_open/close_date → campaign_started/ended_at, last_date →
        -- last_editorial_date are now first-class columns. problem_1/problem_2
        -- (coalesced into recall_reason) and disposition (initcap →
        -- lifecycle_status) are kept as faithful pre-derive originals; mic is
        -- the firm key; model_year / hin / units / boat_type are
        -- recall_product-grain; case_open_date anchors the coalesced
        -- announced_at.
        jsonb_build_object(
            'mic',            mic,
            'model_year',     model_year,
            'problem_1',      problem_1,
            'problem_2',      problem_2,
            'hin',            hin,
            'disposition',    disposition,
            'case_open_date', case_open_date,
            'units',          units,
            'boat_type',      boat_type
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
