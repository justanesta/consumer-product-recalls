{{ config(materialized='view') }}

-- Latest-per-recall projection over the NHTSA bronze table.
-- Bronze may contain multiple rows per 11-tuple identity (ADR 0030) when
-- content changes — content-hash dedup with hash_exclude_fields={source_recall_id}
-- prevents identical re-ingestion (RECORD_ID is regenerated per build per
-- Finding K), but genuine field edits (corrective_action lifecycle updates,
-- odate / endman / bgman revisions, etc. — see
-- documentation/nhtsa/incremental_delta_findings.md Section D) produce new
-- rows. Silver consumes only the most recent version per 11-tuple.
--
-- The 11-tuple matches ADR 0030's bronze identity exactly. Silver's downstream
-- recall_product_id derives from the same 11 fields per ADR 0031 — the
-- staging-layer DISTINCT ON ensures one row per logical recall × vehicle ×
-- component × part × batch lands in the silver model build.
--
-- source_recall_id (RECORD_ID) is preserved for audit/lineage but NOT
-- load-bearing for identity — it churns across runs. Use the 11-tuple for
-- joins; use source_recall_id only for cross-referencing back to a specific
-- bronze landing.

with ranked as (
    select
        *,
        row_number() over (
            partition by
                -- ADR 0033 Normalization class (Phase 6b 6b.3): canonicalize maketxt via the
                -- normalize_maketxt macro IN the identity grain so the 'AC DELCO' -> 'ACDELCO'
                -- normalization-drift class is ONE identity (latest-wins), not two. The macro is
                -- the SINGLE source of truth — recall_product.sql's md5 + the drift monitor call
                -- the same macro, so the sites can't diverge (no silent re-fragmentation). The
                -- maketxt column SELECTed below stays RAW (survivor's spelling is displayed).
                campno,
                {{ normalize_maketxt('maketxt') }},
                modeltxt, yeartxt, compname,
                rcl_cmpt_id, mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                endman, bgman
            order by extraction_timestamp desc
        ) as rn
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
)

select
    -- Lineage / audit
    source_recall_id,
    content_hash,
    extraction_timestamp,
    raw_landing_path,

    -- 11-tuple identity (matches ADR 0030 bronze identity)
    campno,
    maketxt,
    modeltxt,
    yeartxt,
    compname,
    rcl_cmpt_id,
    mfr_comp_ptno,
    mfr_comp_desc,
    mfr_comp_name,
    endman,
    bgman,

    -- W4 Phase A: derived model_year for the recall_product lift (9999 sentinel
    -- → NULL; yeartxt itself stays above as an 11-tuple key field).
    nullif(nullif(yeartxt, ''), '9999')                       as model_year,

    -- Manufacturer / event metadata
    mfgcampno,
    mfgname,
    mfgtxt,
    rcltype,
    potaff,
    odate,
    influenced_by,
    rcdate,
    datea,
    rpno,
    fmvss,

    -- Free-text recall narrative
    desc_defect,
    conequence_defect,
    corrective_action,
    notes,

    -- May-2025-added booleans
    do_not_drive,
    park_outside
from ranked
where rn = 1
