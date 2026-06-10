-- Examine USDA multi-establishment recalls — the C4 / Finding S population.
-- Operator query; pairs with the dbt monitor dbt/tests/source_assumptions/
-- assert_usda_multi_establishment_recalls.sql. Full reasoning: recall_api_observations.md
-- Finding S. A growing count here is the signal to revisit per-element / grain-change resolution.

-- (1) establishment array-length distribution across the English corpus.
select
    coalesce(jsonb_array_length(establishment), 0) as n_establishments,
    count(*)                                       as recalls
from usda_fsis_recalls_bronze
where langcode = 'English'
group by 1
order by 1;

-- (2) the multi-establishment recalls: elements, whether the whole CSV matches a name (it won't),
--     per-element candidate counts, and whether they establishment-resolve today (they won't).
with multi as (
    select
        trim(source_recall_id)                                                              as recall,
        md5('USDA' || '|' || trim(source_recall_id))                                        as recall_event_id,
        establishment,
        (select string_agg(e, ', ') from jsonb_array_elements_text(establishment) e)        as csv_whole,
        (select string_agg(e, '  ||  ') from jsonb_array_elements_text(establishment) e)    as elements
    from usda_fsis_recalls_bronze
    where langcode = 'English'
      and jsonb_typeof(establishment) = 'array'
      and jsonb_array_length(establishment) > 1
)
select
    m.recall,
    m.elements,
    -- whole-CSV exact-name match count (the current join basis) — expected 0 for multi-estab
    (select count(*) from firm_usda_attributes fa
       where upper(trim(fa.establishment_name)) = upper(trim(m.csv_whole)))                 as csv_whole_candidates,
    -- per-element candidate counts (what a per-element join would see)
    (select string_agg(
                elem || ' [' || (select count(*) from firm_usda_attributes fa
                                   where upper(trim(fa.establishment_name)) = upper(trim(elem)))::text || ']',
                '  ')
       from jsonb_array_elements_text(m.establishment) elem)                                as per_element_candidates,
    (r.recall_event_id is not null)                                                         as establishment_resolved_today
from multi m
left join recall_event_establishment_resolution r on r.recall_event_id = m.recall_event_id
order by m.recall;
