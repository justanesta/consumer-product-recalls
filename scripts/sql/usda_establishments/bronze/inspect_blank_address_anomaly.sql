-- Inspect blank/absent `address` values in the USDA FSIS establishment bronze —
-- the incident diagnostic behind the not_null_firm_usda_attributes_address
-- failure (2 current establishments with a NULL address).
--
-- Context: the 2026-04-29 field survey (Finding D,
-- documentation/usda/establishment_api_observations.md:220,426,438) found
-- `address` 100% populated ("Always present", 0% empty), which is why silver
-- `firm_usda_attributes.address` carries a not_null test at ERROR severity.
-- `stg_usda_fsis_establishments` nevertheless nullif('')-normalizes address
-- (source stores blanks verbatim in bronze), so a newly-blank address flows
-- through as NULL and trips the silver contract — the same shape as the
-- 2026-07-08 `est_status` glitch (Finding G addendum), one tier down.
--
-- This is the standing tripwire: it enumerates every establishment whose LATEST
-- bronze version has a blank/absent address (what staging + the snapshot see),
-- and shows the per-establishment version history so you can tell a transient
-- upstream glitch (real address -> blank flip) from a genuinely address-less
-- new establishment. Read-only.
--
-- When to run:
--   * When not_null_firm_usda_attributes_address fails (or its silver peers
--     state / zip, which share the same "always present" survey basis).
--   * Periodically, to confirm address population still holds upstream.
--
-- What to look for:
--   * Q1: how many establishments currently carry a blank address, and whether
--     the blank is a NULL or a literal empty string in bronze.
--   * Q2: per-establishment version history — did a real prior address flip to
--     blank (transient glitch, mirrors est_status) or was the record blank from
--     first sighting (a genuinely address-less establishment)?
--   * Q3: is the establishment otherwise fully populated (name/city/state/zip)?
--     A populated record missing only address is upstream garbage; a sparse
--     record may be a placeholder/pending registration.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: current-version establishments with a blank address ==='
\echo 'One row per establishment whose LATEST bronze version has address NULL or'
\echo 'empty-string. blank_kind distinguishes a true NULL from a literal '''' so you'
\echo 'know what the source actually served (staging nullif('''') maps both to NULL).'

with latest as (
    select distinct on (source_recall_id)
        source_recall_id, establishment_number, establishment_name,
        address, city, state, zip, status_regulated_est,
        latest_mpi_active_date, extraction_timestamp
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    source_recall_id as establishment_id,
    establishment_number,
    establishment_name,
    city,
    state,
    zip,
    case
        when address is null then 'NULL'
        when address = ''    then 'empty-string'
        else 'other-blank'
    end as blank_kind,
    status_regulated_est,
    latest_mpi_active_date,
    extraction_timestamp
from latest
where address is null or trim(address) = ''
order by source_recall_id;

\echo
\echo '=== Q2: full version history of each affected establishment ==='
\echo 'Every bronze version, so you can see whether a real address flipped to'
\echo 'blank (transient glitch) or the record was blank from first sighting.'

with affected as (
    select distinct on (source_recall_id) source_recall_id
    from usda_fsis_establishments_bronze
    where address is null or trim(address) = ''
    order by source_recall_id, extraction_timestamp desc
)
select
    b.source_recall_id as establishment_id,
    b.establishment_number,
    b.establishment_name,
    b.address,
    b.city,
    b.state,
    b.zip,
    b.extraction_timestamp,
    b.content_hash
from usda_fsis_establishments_bronze b
join affected a using (source_recall_id)
order by b.source_recall_id, b.extraction_timestamp;

\echo
\echo '=== Q3: completeness of the affected LATEST records ==='
\echo 'Is the current record otherwise fully populated (only address missing) —'
\echo 'i.e. upstream garbage — or broadly sparse (a placeholder/pending row)?'

with latest as (
    select distinct on (source_recall_id)
        source_recall_id, establishment_name, address, city, state, zip,
        county, phone, duns_number
    from usda_fsis_establishments_bronze
    order by source_recall_id, extraction_timestamp desc
)
select
    source_recall_id as establishment_id,
    (establishment_name is not null and trim(establishment_name) <> '') as has_name,
    (city  is not null and trim(city)  <> '') as has_city,
    (state is not null and trim(state) <> '') as has_state,
    (zip   is not null and trim(zip)   <> '') as has_zip,
    (phone is not null and trim(phone) <> '') as has_phone,
    (duns_number is not null and trim(duns_number) <> '') as has_duns
from latest
where address is null or trim(address) = ''
order by source_recall_id;
