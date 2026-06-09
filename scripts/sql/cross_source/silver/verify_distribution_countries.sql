-- Verify the C12 distribution_country_codes[] build (recall_distribution_area).
-- Read-only. Run after: dbt build --select country_iso+
-- Each block is independent; run them one at a time.

-- 1) Grain census: how many rows carry states / countries / both / each alone.
--    Expect: state-bearing ≈ prior row count; country-bearing in the low thousands;
--    country-only (the grain expansion) in the ~2.7k range; zero empty-empty rows.
select
    count(*)                                                          as total_rows,
    count(*) filter (where cardinality(distribution_state_codes)   > 0) as has_states,
    count(*) filter (where cardinality(distribution_country_codes) > 0) as has_countries,
    count(*) filter (where cardinality(distribution_state_codes)   > 0
                       and cardinality(distribution_country_codes) > 0) as has_both,
    count(*) filter (where cardinality(distribution_state_codes)   = 0
                       and cardinality(distribution_country_codes) > 0) as country_only,
    count(*) filter (where cardinality(distribution_state_codes)   > 0
                       and cardinality(distribution_country_codes) = 0) as state_only,
    count(*) filter (where cardinality(distribution_state_codes)   = 0
                       and cardinality(distribution_country_codes) = 0) as empty_empty_BUG
from recall_distribution_area;

-- 2) Top country codes by recall count (sanity: Canada/Mexico/UK/Germany etc. should dominate;
--    no obvious junk codes — they are all constrained to the country_iso seed by construction).
select cc as alpha2, count(*) as n_recalls
from recall_distribution_area, unnest(distribution_country_codes) as cc
group by cc
order by n_recalls desc
limit 25;

-- 3) Precision eyeball: 30 country-bearing recalls, parsed codes vs the RAW distribution text.
--    Scan for false positives (a US place-name that leaked a country) and missed countries.
select
    rda.recall_event_id,
    rda.distribution_state_codes,
    rda.distribution_country_codes,
    left(re.distribution_area_summary, 240) as raw_distribution_text
from recall_distribution_area rda
join recall_event re using (recall_event_id)
where cardinality(rda.distribution_country_codes) > 0
order by rda.recall_event_id
limit 30;

-- 4) Country-only rows (the grain expansion) — these had NO row before C12. Confirm they are
--    genuinely international (nationwide-US+intl or intl-only), not domestic false positives.
select
    rda.distribution_country_codes,
    re.distribution_scope,
    left(re.distribution_area_summary, 240) as raw_distribution_text
from recall_distribution_area rda
join recall_event re using (recall_event_id)
where cardinality(rda.distribution_state_codes)   = 0
  and cardinality(rda.distribution_country_codes) > 0
order by rda.recall_event_id
limit 40;
