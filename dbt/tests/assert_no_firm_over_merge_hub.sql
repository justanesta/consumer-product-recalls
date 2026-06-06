-- 6b.6 over-merge ceiling (ADR 0037). No canonical firm should absorb more than 50 distinct
-- observed names. The measured legit max is 32 (Kawasaki Motors — all genuinely Kawasaki); the
-- old FEI-blob / subset-clusterer hubs were 100s. A row here = a hub regression (a rule loosened or
-- a denylist gap let unrelated firms chain). error-severity — block the build before it reaches gold.
select
    firm_id,
    canonical_name,
    jsonb_array_length(observed_names) as n_observed_names
from {{ ref('firm') }}
where jsonb_array_length(observed_names) > 50
