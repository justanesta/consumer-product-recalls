{{ config(materialized='view') }}

-- fct_recalls_by_country — recalls per distribution COUNTRY (Phase 7, C12 follow-on). The country
-- analogue of fct_recalls_by_geography's 'distribution' state lens: "where did the recalled product
-- go, at country grain." FDA + USDA only — the two sources with a distribution field (CPSC/NHTSA/USCG
-- carry none, the same caveat as the state distribution lens). Each recall contributes to 'US' if it
-- was US-distributed AND to every foreign country it reached, so a US+Canada recall counts once for US
-- and once for CA (multi-valued geography — per-country counts SUM TO MORE than the distinct-recall
-- total, an industry-reach reading, just like the state lens).
--
-- WHY 'US' IS DERIVED, NOT STORED: recall_distribution_area.distribution_country_codes is the FOREIGN
-- international-presence array by design (ADR 0036 D7 / C12 "international-presence array"). US
-- distribution is authoritatively carried by distribution_scope (Nationwide/Regional) +
-- distribution_state_codes, so duplicating a literal 'US' into silver would denormalize a fact we
-- already store. Instead the US cell is derived HERE from that signal; foreign cells unnest the array.
-- Empirically (2026-06-09) US distribution dominates: FDA Nationwide 12,006 + Regional 31,449 + 37,117
-- recalls with parsed states; only ~273 recalls are truly non-US. country_code is ISO-3166-1 alpha-2
-- ('US' for the United States). GROUPING SETS adds the 'ALL'-source rollup.
--
-- The US predicate: USDA (FSIS is US-only) OR scope in (Nationwide, Regional) OR >=1 parsed US state
-- OR the FDA text says "nationwide" (catches International-scope recalls that are US-nationwide AND
-- abroad, e.g. "Worldwide - US Nationwide and the country of Canada"). "nationwide" is an unambiguous
-- US signal; bare "US"/"U.S." is deliberately NOT used (it collides with "outside the US").

with us_lens as (
    select re.source, re.recall_event_id, 'US'::text as country_code
    from {{ ref('recall_event') }} re
    left join {{ ref('recall_distribution_area') }} rda using (recall_event_id)
    where re.source in ('FDA', 'USDA')
      and (
            re.source = 'USDA'
         or re.distribution_scope in ('Nationwide', 'Regional')
         or cardinality(rda.distribution_state_codes) > 0
         or re.distribution_area_summary ~* '\ynationwide\y'
      )
),

foreign_lens as (
    select re.source, re.recall_event_id, cc as country_code
    from {{ ref('recall_distribution_area') }} rda
    join {{ ref('recall_event') }} re using (recall_event_id)
    cross join lateral unnest(rda.distribution_country_codes) as cc
),

combined as (
    select * from us_lens
    union all
    select * from foreign_lens
)

select
    coalesce(source, 'ALL')         as source,
    country_code,
    count(distinct recall_event_id) as recall_count
from combined
group by grouping sets (
    (source, country_code),
    (country_code)
)
order by country_code, source
