{{ config(
    materialized='table',
    indexes=[
      {'columns': ['mic'], 'unique': True},
    ]
) }}

-- USCG MIC reassignment years — the source-native reassignment boundary per MIC, parsed from the
-- detail page's "Past Company (OOB YYYY)" markers (ADR 0035 §5 historical backfill; Phase 6c.5 (b)).
--
-- current_holder_since_year = the LATEST parsed OOB year = roughly when the current holder took over
-- the recycled MIC. The as-of-build-year join (recall_event_firm, part c) compares a recall's
-- model_year to it: model_year >= current_holder_since_year ⇒ the boat was built during the current
-- holder's tenure ⇒ the current-holder attribution is confident (uscg_mic_build_date_resolved).
--
-- Scope is DIRECTORY-WIDE: every OOB-recycled MIC in firm_manufacturer_attributes with a parseable
-- (OOB YYYY) year (393 of the directory's 3,015 OOB-recycled MICs), NOT just recalled MICs. This is
-- a reusable lookup dimension — the recall_event_firm join is exact on MIC, so the 379 rows no recall
-- references are inert. Only 14 MICs (21 recall rows) appear in a USCG recall; 6 of those recalls have
-- a model_year >= the reassignment year → resolved, the rest are older boats built under a prior holder
-- (correctly still time-sensitive). The gate probe_uscg_refinement_gates Q2 reports 23 because it
-- counts ANY year in the slots; this model is stricter — the year must FOLLOW the OOB marker
-- (OOB[^0-9]*YYYY), so a year sitting in a "(previous name … 1998)" fragment is correctly ignored.
-- MICs with no parseable year do not appear here (undated → cannot resolve → the recall stays
-- time-sensitive). `In Business` is deliberately NOT used (a record-touch heartbeat, noisy — ADR
-- 0035 §5 / ADR 0032). One row per MIC. reassignment_years is the full parsed set (audit); the join
-- uses the max. See scripts/sql/cross_source/scd_monitors/verify_uscg_reassignment_funnel.sql.

with detail as (
    -- firm_manufacturer_attributes is already latest-per-MIC + upper(trim) + has the recycle flag.
    select mic, past_company_1, past_company_2, past_company_3
    from {{ ref('firm_manufacturer_attributes') }}
    where mic_oob_recycled
),

oob_years as (
    -- unpivot the 3 slots; extract a 19xx/20xx year adjacent to an OOB marker (e.g. "(OOB 1991)").
    -- A slot marked OOB with no year (e.g. "(OOB)", "- OOB") yields NULL and is dropped below.
    select
        d.mic,
        (regexp_match(slot, 'OOB[^0-9]*((?:19|20)\d\d)'))[1]::int as oob_year
    from detail d
    cross join lateral unnest(array[d.past_company_1, d.past_company_2, d.past_company_3]) as s(slot)
    where slot ~ '\yOOB\y'
)

select
    mic,
    max(oob_year)                                                              as current_holder_since_year,
    to_jsonb(array_agg(oob_year order by oob_year) filter (where oob_year is not null)) as reassignment_years
from oob_years
group by mic
having max(oob_year) is not null
