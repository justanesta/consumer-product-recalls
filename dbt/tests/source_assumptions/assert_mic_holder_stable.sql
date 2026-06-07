-- SCD monitor (Type-2-NEED, measure-forward half) — a USCG MIC whose HOLDER changed.
--
-- The dynamic half of scripts/sql/cross_source/scd_monitors/assert_mic_holder_stable.sql (Q1):
-- a MIC whose company_name differs across its SCD-2 snapshot versions = a reassignment we
-- OBSERVED forward (AXY/COP-style). The static source-native recycle surface (that script's
-- Q2–Q4, 365 prior / 221 OOB of 718) is already ENFORCED by assert_uscg_mic_reassignment_flag_present
-- (6b), so it is not re-asserted here; this catches NEW reassignments as we bank them.
--
-- Returns reassigned MICs; ~0 today (snapshot just initialized → 1 version per MIC), warns when
-- the snapshot banks a holder change. severity=warn via dbt_project.yml.

select mic, count(distinct company_name) as distinct_holders
from {{ ref('uscg_manufacturer_attributes_snapshot') }}
group by mic
having count(distinct company_name) > 1
