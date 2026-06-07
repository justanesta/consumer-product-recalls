{{ config(materialized='view') }}

-- Full product-grain version history for NHTSA recall products (Phase 6c.6 Layer 2, ADR 0033) — the
-- audit/compliance peer of recall_product_v15 under Policy C (current view + history table). Every
-- snapshot version with dbt_valid_from / dbt_valid_to and an is_current flag. For Pierce 26V217000
-- this is where the mfr_comp_desc '' -> 'Software' transition becomes queryable (the current view
-- shows only 'Software'). Kept (NOT dropped) through the 6c.7 cutover.
--
-- NOTE on reach: dbt snapshots are forward-only, so this history starts at our FIRST snapshot run —
-- bronze edits observed before that are not reconstructed here. Bronze (every distinct 11-tuple
-- content version, ADR 0030) remains the deeper audit trail. The 6c.8 simulated-drift test is what
-- proves the versioning mechanism, since the re-seeded corpus carries no pre-observation history.

select
    recall_product_id,
    campno,
    maketxt,
    modeltxt,
    yeartxt,
    compname,
    rcl_cmpt_id,
    mfr_comp_ptno,
    mfr_comp_desc,
    mfr_comp_name,
    bgman,
    endman,
    rcltype,
    potaff,
    mfgname,
    mfgtxt,
    fmvss,
    model_year,
    extraction_timestamp,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null)                                 as is_current,
    dbt_scd_id
from {{ ref('nhtsa_recall_product_snapshot') }}
