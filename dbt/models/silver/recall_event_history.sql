{{ config(materialized='table') }}

-- recall_event_history — field-level edit history of the recall EVENT (ADR 0022).
--
-- Synthesizes "what changed, when" by LAG()-ing each tracked field over a recall's bronze
-- snapshots (bronze is content-hash-deduped, so consecutive rows per identity ARE the
-- distinct content versions — ADR 0007). One output row per changed field per snapshot
-- interval. Uniform LAG()-over-bronze for all five sources — FDA's native field-history
-- endpoints are empty, so there is no source-asymmetric path (ADR 0022).
--
-- This is the recall-FACT history (edits to the event record). It is distinct from the
-- dimension SCD-2 snapshots (firm_uscg_attributes etc.) and from recall_lifecycle
-- (the presence/summary view, 6c.2). See documentation/silver_design_notes.md.
--
-- Tracked canonical fields (curated v1 — the editorially-meaningful event attributes that
-- map to DIRECT bronze columns; the noisy/jsonb/synth fields are deliberately excluded):
--   recall_reason · classification · lifecycle_status · title · terminated_at
-- Each source contributes the bronze columns recall_event.sql maps to these canonicals;
-- a source lacking a field emits NULL for it. Values are cast to text (this is an
-- audit/history surface; type fidelity lives in silver-current).
--
-- Three correctness rules (see the WHERE at the bottom + norm_text_for_change macro):
--   * change-type exclusion (ADR 0027): suppress events whose CURRENT snapshot came from a
--     schema_rebaseline / hash_helper_rebaseline run — those re-version waves are parser/
--     hash artifacts, not real edits. The rebaseline snapshot STAYS in the LAG sequence, so
--     the next routine edit compares against the reparsed baseline (no spurious carry-over).
--   * cosmetic-noise suppression: compare via norm_text_for_change() (whitespace + ''/NULL
--     folding) so whitespace churn and ''↔NULL flips don't synthesize edits.
--   * creation ≠ edit: emit only where a prior snapshot exists (prev_ts is not null); a
--     later NULL→value still counts (field populated post-creation).
--
-- Grain notes: USDA keeps langcode (EN/ES edit independently — ADR 0006); FDA collapses
-- product-level bronze to the event (recall_event_id) and NHTSA collapses 11-tuple line
-- rows to the campaign (campno) — both event-level fields are stable across the collapsed
-- rows, so the deterministic representative (content_hash tiebreak) is safe.
--
-- Post-reseed the table is SPARSE (Phase 6a.5 re-seeds left ~1 version per identity); it
-- grows as daily incrementals re-accumulate content versions. That is expected, not a bug.

with cpsc_snap as (
    select
        'CPSC'                      as source,
        source_recall_id,
        cast(null as text)          as langcode,
        extraction_timestamp,
        raw_landing_path,
        cast(description as text)   as recall_reason,
        cast(null as text)          as classification,
        cast(null as text)          as lifecycle_status,
        cast(title as text)         as title,
        cast(null as text)          as terminated_at
    from {{ source('cpsc', 'cpsc_recalls_bronze') }}
),

fda_snap as (
    -- Product-level bronze → event grain. Event-level fields (reason/classification/phase/
    -- termination) are stable across an event's products, so a deterministic representative
    -- (content_hash tiebreak) per (event, snapshot) is correct.
    select distinct on (recall_event_id, extraction_timestamp)
        'FDA'                                       as source,
        recall_event_id::text                       as source_recall_id,
        cast(null as text)                          as langcode,
        extraction_timestamp,
        raw_landing_path,
        cast(product_short_reason_txt as text)      as recall_reason,
        cast(center_classification_type_txt as text) as classification,
        cast(phase_txt as text)                     as lifecycle_status,
        cast(null as text)                          as title,
        cast(termination_dt as text)                as terminated_at
    from {{ source('fda', 'fda_recalls_bronze') }}
    order by recall_event_id, extraction_timestamp, content_hash
),

usda_snap as (
    -- Both langcodes: EN and ES are edited independently (ADR 0006 / Finding F), so langcode
    -- is part of the history identity.
    select
        'USDA'                          as source,
        source_recall_id,
        langcode,
        extraction_timestamp,
        raw_landing_path,
        cast(summary as text)           as recall_reason,
        cast(recall_classification as text) as classification,
        cast(recall_type as text)       as lifecycle_status,
        cast(title as text)             as title,
        cast(closed_date as text)       as terminated_at
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
),

nhtsa_snap as (
    -- 11-tuple line rows → campaign (campno) grain. campno is the stable event id; the
    -- bronze source_recall_id (RECORD_ID) is regen-unstable (ADR 0030) and is NOT used.
    -- desc_defect is the campaign narrative (stable across the campaign's lines).
    select distinct on (campno, extraction_timestamp)
        'NHTSA'                     as source,
        campno                      as source_recall_id,
        cast(null as text)          as langcode,
        extraction_timestamp,
        raw_landing_path,
        cast(desc_defect as text)   as recall_reason,
        cast(null as text)          as classification,
        cast(null as text)          as lifecycle_status,
        cast(null as text)          as title,
        cast(null as text)          as terminated_at
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    order by campno, extraction_timestamp, content_hash
),

uscg_snap as (
    select
        'USCG'                                      as source,
        source_recall_id,
        cast(null as text)                          as langcode,
        extraction_timestamp,
        raw_landing_path,
        cast(coalesce(problem_1, problem_2) as text) as recall_reason,
        cast(severity as text)                      as classification,
        cast(disposition as text)                   as lifecycle_status,
        cast(null as text)                          as title,
        cast(case_close_date as text)               as terminated_at
    from {{ source('uscg', 'uscg_recalls_bronze') }}
),

all_snap as (
    select * from cpsc_snap
    union all select * from fda_snap
    union all select * from usda_snap
    union all select * from nhtsa_snap
    union all select * from uscg_snap
),

-- One row per landing run, deduped on raw_landing_path (the bronze→run link), carrying the
-- run's change_type + run_id. 304/failed runs land no bronze, so their empty paths never
-- match; a bronze row whose run predates the link defaults to 'routine' below.
runs as (
    select distinct on (raw_landing_path)
        raw_landing_path,
        change_type,
        run_id
    from {{ source('pipeline', 'extraction_runs') }}
    where raw_landing_path is not null
      and raw_landing_path <> ''
    order by raw_landing_path, started_at desc
),

with_run as (
    select
        s.source,
        s.source_recall_id,
        s.langcode,
        s.extraction_timestamp,
        coalesce(r.change_type, 'routine')                                   as change_type,
        r.run_id                                                             as change_run_id,
        s.recall_reason,
        s.classification,
        s.lifecycle_status,
        s.title,
        s.terminated_at,
        -- snapshots per identity; single-version identities cannot have edits, so we drop
        -- them before the (expensive) unpivot + LAG. Post-reseed this removes the bulk.
        count(*) over (
            partition by s.source, s.source_recall_id, s.langcode
        )                                                                    as n_snapshots
    from all_snap s
    left join runs r on r.raw_landing_path = s.raw_landing_path
),

-- Long format: one row per (snapshot, tracked field). LATERAL VALUES unpivots the five
-- canonical fields uniformly so the LAG + normalization logic is written once.
unpivoted as (
    select
        w.source,
        w.source_recall_id,
        w.langcode,
        w.extraction_timestamp,
        w.change_type,
        w.change_run_id,
        f.field_name,
        f.value
    from with_run w
    cross join lateral unnest(
        array['recall_reason', 'classification', 'lifecycle_status', 'title', 'terminated_at'],
        array[w.recall_reason, w.classification, w.lifecycle_status, w.title, w.terminated_at]
    ) as f(field_name, value)
    where w.n_snapshots > 1
),

lagged as (
    select
        source,
        source_recall_id,
        langcode,
        field_name,
        value,
        change_type,
        change_run_id,
        extraction_timestamp,
        lag(value) over w               as prev_value,
        lag(extraction_timestamp) over w as prev_ts
    from unpivoted
    window w as (
        partition by source, source_recall_id, langcode, field_name
        order by extraction_timestamp
    )
)

select
    source,
    source_recall_id,
    langcode,
    field_name,
    prev_value                  as old_value,
    value                       as new_value,
    extraction_timestamp        as changed_at,
    change_type,
    change_run_id
from lagged
where prev_ts is not null                                                -- creation is not an edit
  and change_type not in ('schema_rebaseline', 'hash_helper_rebaseline') -- ADR 0027 exclusion
  and {{ norm_text_for_change('value') }}
      is distinct from {{ norm_text_for_change('prev_value') }}           -- a real (non-cosmetic) change
