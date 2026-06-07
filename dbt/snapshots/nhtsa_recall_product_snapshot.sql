{% snapshot nhtsa_recall_product_snapshot %}
{{
  config(
    schema='silver_snapshots',
    unique_key='recall_product_id',
    strategy='check',
    check_cols=[
      'mfr_comp_desc', 'mfr_comp_name', 'bgman', 'endman',
      'rcltype', 'potaff', 'mfgname', 'mfgtxt', 'fmvss',
    ],
  )
}}

-- SCD-2 product-grain history for NHTSA recall products (ADR 0033 + 2026-06-06 amendment / 0034;
-- Phase 6c.6 Layer 2). Stable anchor = the 7-tuple recall_product_id (single-homed in
-- stg_nhtsa_recalls_current). The v1 11-tuple recipe folded drift-prone attributes INTO the key, so
-- a single NHTSA edit (Pierce 26V217000 mfr_comp_desc '' -> 'Software') fragmented 96 products into
-- 192 rows. Here those attributes are DEMOTED to check_cols: an edit banks a Type-2 version
-- (dbt_valid_from/to) instead of minting a new product. dbt manages dbt_valid_from / dbt_valid_to /
-- dbt_scd_id.
--
-- ANCHOR vs ATTRIBUTE (the 2026-06-06 full-corpus correction): mfr_comp_ptno is in the ANCHOR, NOT
-- check_cols. The 6-tuple ADR 0033 first proposed demoted ptno to an attribute and thereby collapsed
-- 126,417 STRUCTURAL multi-part rows (Takata/Fortune-Tormenta fan-out) — legitimate distinct facts,
-- not fragmentation. Only the genuinely-drift-prone fields are check_cols:
--   - mfr_comp_desc, mfr_comp_name : the Pierce field-population class (96 real_drift rows);
--   - bgman, endman               : the batch-window edit class (17 real_drift rows).
-- WIDENED beyond those 4 (ADR 0034 refinement) to also cover rcltype, potaff, mfgname, mfgtxt, fmvss
-- so a non-check business field edited in isolation does not go stale in the current view.
-- EXCLUDED deliberately:
--   - the 7 anchor fields (identity — a change is a new product, or the normalize_maketxt class);
--   - model_year (functionally determined by the yeartxt anchor);
--   - extraction_timestamp (per-regen heartbeat — carried as the as-of stamp, never a version trigger).
-- Event-grain narrative (desc_defect, ...) is NOT here — that history is recall_event_history's
-- LAG() job (ADR 0022). This snapshot is product-grain ONLY (resolves migration-plan Open Q#3:
-- complementary, not competing). Lands in silver_snapshots (ADR 0007 pruning-exempt). Re-run on an
-- unchanged corpus must bank 0 new versions (idempotency — the Layer 2 gate).

select
    recall_product_id,
    -- 7-tuple anchor (carried for readability/audit; maketxt is the RAW survivor spelling)
    campno,
    maketxt,
    modeltxt,
    yeartxt,
    compname,
    rcl_cmpt_id,
    mfr_comp_ptno,
    -- demoted attribute fields (versioned by check_cols)
    mfr_comp_desc,
    mfr_comp_name,
    bgman,
    endman,
    rcltype,
    potaff,
    mfgname,
    mfgtxt,
    fmvss,
    -- carried (non-versioning) columns recall_product consumes
    model_year,
    extraction_timestamp
from {{ ref('stg_nhtsa_recalls_current') }}

{% endsnapshot %}
