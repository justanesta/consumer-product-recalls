{% snapshot uscg_manufacturer_attributes_snapshot %}
{{
  config(
    schema='silver_snapshots',
    unique_key='mic',
    strategy='check',
    check_cols=[
      'company_name', 'dba', 'parent_company', 'parent_mic',
      'past_company_1', 'past_company_2', 'past_company_3',
      'address', 'city', 'state', 'zip', 'country',
      'status', 'out_of_business',
    ],
  )
}}

-- SCD-2 history for the USCG manufacturer directory (ADR 0035 — the corpus's only
-- measured Type-2 NEED: a MIC is a finite 3-char code USCG RECYCLES to a new builder
-- when the prior one goes out of business, so the same anchor denotes a different firm
-- over time and a Type-1 "current holder" view misattributes pre-reassignment recalls).
--
--   * strategy='check' banks a new version whenever any check_col changes for a mic.
--   * unique_key='mic' ALONE — a reassignment is a NEW VERSION OF THE SAME ANCHOR;
--     mic+company would fork the lineage into parallel chains (ADR 0035 §4).
--   * The anchor is normalized to upper(trim(mic)) so versions align with the bridge
--     join (recall_event_firm.uscg_event_firms joins on upper(trim(mic))); the inner
--     row_number() collapses any case-variant rows to one current version per anchor
--     (defense-in-depth behind assert_uscg_scd2_no_forked_lineage).
--   * check_cols EXCLUDES date_modified and in_business: both are record-touch /
--     heartbeat fields contaminated by directory re-touches on active firms (the
--     staging view warns in_business ≈ date_modified ≈ 2025/2026 on live builders).
--     Versioning on them would bank phantom history on every re-scan — the same
--     hazard ADR 0032 solved for USDA's latest_mpi_active_date. They are carried as
--     point-in-time columns for the current-view sidecar, just not version triggers.
--   * out_of_business IS a check_col: top-level OOB = the CURRENT holder ceased (a
--     genuine SCD valid_to signal), distinct from a Past Company (OOB) recycle marker.
--   * where company_name is not null drops the sentinel/null-name anchors (already
--     NULL-coalesced from 'UNK'/'-'/'' in the staging view).

with latest as (
    select
        upper(trim(mic))                                            as mic,
        company_name,
        dba,
        parent_company,
        parent_mic,
        past_company_1,
        past_company_2,
        past_company_3,
        address,
        city,
        state,
        zip,
        country,
        status,
        out_of_business,
        in_business,
        date_modified,
        uscg_directory_id,
        detail_url,
        extraction_timestamp,
        row_number() over (
            partition by upper(trim(mic))
            order by extraction_timestamp desc
        )                                                           as _rn
    from {{ ref('stg_uscg_manufacturer_details') }}
    where company_name is not null
)

select
    mic,
    company_name,
    dba,
    parent_company,
    parent_mic,
    past_company_1,
    past_company_2,
    past_company_3,
    address,
    city,
    state,
    zip,
    country,
    status,
    out_of_business,
    in_business,
    date_modified,
    uscg_directory_id,
    detail_url,
    extraction_timestamp
from latest
where _rn = 1

{% endsnapshot %}
