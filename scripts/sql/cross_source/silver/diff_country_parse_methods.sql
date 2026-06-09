-- Diagnostic: compare the OLD per-name country parse to the NEW single-pass alternation parse now
-- in recall_distribution_area. Shows every FDA recall where the two disagree, with the raw text, so
-- we can confirm the alternation is equivalent-or-more-correct (not silently dropping real countries).
-- Read-only. Expectation: diffs are substring-name over-matches the alternation correctly suppresses
-- (e.g. "IRELAND" inside "NORTHERN IRELAND"->GB).
with country_seed as (
    select upper(name) as name, alpha2 from country_iso
),
fda_raw as (
    select recall_event_id, distribution_area_summary as t
    from recall_event
    where source = 'FDA' and distribution_area_summary is not null
),
fda_intl as (
    select
        recall_event_id,
        regexp_replace(
            regexp_replace(t, '\ynew mexico\y', '  ', 'gi'),
            '^.*?(\mOUS\M|internationa|foreign|countr(y|ies)|worldwide|global|\mROW\M|outside\s+(the\s+)?u\.?\s*s|abroad|export)',
            '\1', 'i'
        ) as tail
    from fda_raw
    where t ~* '(\mOUS\M|internationa|foreign|countr(y|ies)|worldwide|global|\mROW\M|outside\s+(the\s+)?u\.?\s*s|abroad|export)'
),
pername as (
    select i.recall_event_id, array_agg(distinct cs.alpha2 order by cs.alpha2) as codes
    from fda_intl i
    join country_seed cs on i.tail ~* ('\y' || cs.name || '\y')
    group by i.recall_event_id
)
select
    count(*) over () as total_differing_recalls,
    p.codes                              as pername_codes,
    rda.distribution_country_codes       as alt_codes,
    array(select unnest(p.codes) except select unnest(rda.distribution_country_codes)) as only_in_pername,
    array(select unnest(rda.distribution_country_codes) except select unnest(p.codes)) as only_in_alt,
    left(re.distribution_area_summary, 120) as raw_text
from pername p
join recall_distribution_area rda using (recall_event_id)
join recall_event re using (recall_event_id)
where p.codes is distinct from rda.distribution_country_codes
order by p.recall_event_id
limit 60;
