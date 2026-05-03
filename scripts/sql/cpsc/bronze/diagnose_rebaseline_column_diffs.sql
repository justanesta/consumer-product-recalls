-- Diagnose which columns drove any unexpected re-versions for CPSC.
-- ADR 0027 predicts re_versioned ≈ 0; only run this if Query 2 in the verify
-- file shows non-zero re-versions. If diffs appear in any column, that's a
-- signal there's a hidden axis we haven't accounted for (CPSC was supposed to
-- be the control source).
--
-- Pass the run_id via -v run_id='<uuid>'.

with new_versions as (
    select *
    from cpsc_recalls_bronze
    where extraction_timestamp >= (
        select started_at from extraction_runs where run_id = :'run_id'
    )
),
old_versions as (
    select distinct on (source_recall_id) *
    from cpsc_recalls_bronze
    where extraction_timestamp < (
        select started_at from extraction_runs where run_id = :'run_id'
    )
    order by source_recall_id, extraction_timestamp desc
)
select
    count(*) as total_compared,
    sum((n.recall_id              is distinct from o.recall_id)::int)              as recall_id_diff,
    sum((n.recall_date            is distinct from o.recall_date)::int)            as recall_date_diff,
    sum((n.last_publish_date      is distinct from o.last_publish_date)::int)      as last_publish_date_diff,
    sum((n.title                  is distinct from o.title)::int)                  as title_diff,
    sum((n.description            is distinct from o.description)::int)            as description_diff,
    sum((n.url                    is distinct from o.url)::int)                    as url_diff,
    sum((n.consumer_contact       is distinct from o.consumer_contact)::int)       as consumer_contact_diff,
    sum((n.products               is distinct from o.products)::int)               as products_diff,
    sum((n.manufacturers          is distinct from o.manufacturers)::int)          as manufacturers_diff,
    sum((n.retailers              is distinct from o.retailers)::int)              as retailers_diff,
    sum((n.importers              is distinct from o.importers)::int)              as importers_diff,
    sum((n.distributors           is distinct from o.distributors)::int)           as distributors_diff,
    sum((n.manufacturer_countries is distinct from o.manufacturer_countries)::int) as manufacturer_countries_diff,
    sum((n.product_upcs           is distinct from o.product_upcs)::int)           as product_upcs_diff,
    sum((n.hazards                is distinct from o.hazards)::int)                as hazards_diff,
    sum((n.remedies               is distinct from o.remedies)::int)               as remedies_diff,
    sum((n.remedy_options         is distinct from o.remedy_options)::int)         as remedy_options_diff,
    sum((n.in_conjunctions        is distinct from o.in_conjunctions)::int)        as in_conjunctions_diff,
    sum((n.sold_at_label          is distinct from o.sold_at_label)::int)          as sold_at_label_diff,
    sum((n.images                 is distinct from o.images)::int)                 as images_diff,
    sum((n.injuries               is distinct from o.injuries)::int)               as injuries_diff
from new_versions n
join old_versions o using (source_recall_id);
