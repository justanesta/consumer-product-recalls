{{ config(severity='warn') }}

-- ADR 0035 flag-wiring guard: every USCG bridge row for a recall on an OOB-recycled MIC
-- (the high-confidence ~221 set) must be HANDLED — either uscg_mic_time_sensitive_unresolved
-- OR, as of 6c.5 (c), uscg_mic_build_date_resolved (the boat's model_year confirms it was built
-- during the current holder's tenure, so the current attribution is correct, not time-sensitive).
-- A bare uscg_mic_unambiguous on an OOB-recycled MIC would be the wiring failure. This
-- reconstructs the bridge key the SAME way recall_event_firm.uscg_event_firms does
-- (md5 of the 3-way name coalesce), applies the SAME firm_crosswalk canonical remap, then
-- inspects the resulting bridge row's match_confidence. A non-empty result = a recycle that
-- the wiring failed to flag.
--
-- Severity = warn (not error): if a USCG OOB firm canonically merges (via firm_crosswalk)
-- with a same-named firm from another source whose match_confidence sorts earlier in the
-- final `distinct on (...) order by match_confidence`, that other confidence wins the
-- collapsed row and masks the flag. That is acceptable (the firm is well-identified by the
-- other source; MIC time-sensitivity is a USCG-attribution nuance), so this is a monitor.

with oob_mics as (
    select upper(trim(mic)) as mic
    from {{ ref('firm_manufacturer_attributes') }}
    where mic_oob_recycled
),

expected as (
    -- the USCG bridge contribution for recalls on an OOB-recycled MIC, pre-remap
    select distinct
        md5('USCG' || '|' || r.source_recall_id)                          as recall_event_id,
        md5(upper(trim(coalesce(m.company_name, r.company_name, r.mic))))  as raw_firm_id
    from {{ ref('stg_uscg_recalls') }} r
    left join {{ ref('stg_uscg_manufacturers') }} m
        on upper(trim(r.mic)) = upper(trim(m.mic))
    join oob_mics o
        on o.mic = upper(trim(r.mic))
    where coalesce(m.company_name, r.company_name, r.mic) is not null
      and trim(coalesce(m.company_name, r.company_name, r.mic)) <> ''
      and r.announced_at is not null
),

mapped_expected as (
    -- apply the same canonical remap the bridge's `mapped` CTE applies
    select
        e.recall_event_id,
        coalesce(x.canonical_firm_id, e.raw_firm_id) as firm_id
    from expected e
    left join {{ source('enrichment', 'firm_crosswalk') }} x
        on x.firm_id = e.raw_firm_id
)

select
    me.recall_event_id,
    me.firm_id,
    ref.match_confidence
from mapped_expected me
join {{ ref('recall_event_firm') }} ref
    on ref.recall_event_id = me.recall_event_id
   and ref.firm_id = me.firm_id
   and ref.role = 'manufacturer'
where ref.match_confidence not in ('uscg_mic_time_sensitive_unresolved', 'uscg_mic_build_date_resolved')
