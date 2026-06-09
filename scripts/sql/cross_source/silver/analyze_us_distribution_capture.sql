-- Empirical grounding for the C12 US-capture question: is the US itself a distribution "country"
-- we should record, and how is US distribution actually expressed in the corpus? Read-only.
-- Run after: dbt build --select country_iso recall_distribution_area

-- Q1: How FDA expresses US distribution in the free-text distribution_area_summary.
--     (Counts are non-exclusive — a row can match several.)
select
    count(*)                                                                          as fda_with_dist_text,
    count(*) filter (where distribution_area_summary ~* '\ynationwide\y')             as says_nationwide,
    count(*) filter (where distribution_area_summary ~* 'united states')              as says_united_states,
    count(*) filter (where distribution_area_summary ~* '\yusa\y')                    as says_usa,
    count(*) filter (where distribution_area_summary ~* '\yu\.?\s?s\.?a?\y')          as says_us_abbr,
    count(*) filter (where distribution_area_summary ~* 'worldwide|international|global') as says_intl_word,
    count(*) filter (where distribution_area_summary !~* '\ynationwide\y|united states|\yusa\y|\yu\.?\s?s\.?a?\y|worldwide|international|global') as says_none_of_these
from recall_event
where source = 'FDA' and distribution_area_summary is not null;

-- Q2: distribution_scope x parsed-geography cross-tab, per source. The key columns: how many
--     recalls are clearly US-distributed (Nationwide/Regional, or have parsed states) yet would be
--     INVISIBLE on a country map because distribution_country_codes is foreign-only today.
select
    re.source,
    re.distribution_scope,
    count(*)                                                              as n_recalls,
    count(*) filter (where cardinality(rda.distribution_state_codes)   > 0) as has_parsed_states,
    count(*) filter (where cardinality(rda.distribution_country_codes) > 0) as has_foreign_country
from recall_event re
left join recall_distribution_area rda using (recall_event_id)
where re.source in ('FDA', 'USDA')
group by re.source, re.distribution_scope
order by re.source, n_recalls desc;

-- Q3: Size of the candidate "US" population by a clean rule — a recall is US-distributed if it is
--     USDA (FSIS is US-only), OR scope in (Nationwide, Regional), OR it has >=1 parsed US state.
--     This is what a US country-map cell would count, and how many rows would gain 'US' if we put
--     it in the array.
select
    count(*) filter (where re.source = 'USDA')                                     as usda_all,
    count(*) filter (where re.source = 'FDA'  and re.distribution_scope = 'Nationwide') as fda_nationwide,
    count(*) filter (where re.source = 'FDA'  and re.distribution_scope = 'Regional')   as fda_regional,
    count(*) filter (where re.source = 'FDA'  and re.distribution_scope = 'International'
                          and cardinality(rda.distribution_state_codes) > 0)        as fda_intl_with_states,
    count(*) filter (where re.source = 'FDA'  and re.distribution_scope = 'International'
                          and cardinality(rda.distribution_state_codes) = 0)        as fda_intl_no_states
from recall_event re
left join recall_distribution_area rda using (recall_event_id);

-- Q4: Distinct nationwide/US phrasings (top 30) — to see the variety the parser would need to read
--     if 'US' were added via free-text rather than via the scope enum.
select left(distribution_area_summary, 90) as sample, count(*) as n
from recall_event
where source = 'FDA' and distribution_area_summary ~* '\ynationwide\y|united states|\yusa\y'
group by left(distribution_area_summary, 90)
order by n desc
limit 30;
