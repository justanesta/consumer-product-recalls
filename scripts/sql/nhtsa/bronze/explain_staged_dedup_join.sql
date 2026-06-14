-- explain_staged_dedup_join.sql — confirm the staged-dedup fetch hash-joins
-- (not nested-loops) and measure its wall-clock.
--
-- Faithfully reproduces the query the refactored
-- BronzeLoader._fetch_existing_hashes_staged (src/bronze/loader.py:341) runs for
-- a full-corpus NHTSA batch: a TEMP table of the text-canonical 11-tuple
-- identities, ANALYZE'd, then the two-stage GROUP-BY-max(extraction_timestamp)
-- JOIN back to bronze. The text expressions below mirror `_identity_text_expr`
-- (loader.py:162) exactly: coalesce(cast(col AS text),'') for the 9 plain
-- columns; coalesce(to_char(col AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')
-- for the TIMESTAMPTZ columns endman/bgman.
--
-- This is a REPRESENTATIVE reproduction: the probe table is populated from
-- bronze's own DISTINCT identities (~= the set the daily flat-file dump stages),
-- so it confirms the PLAN SHAPE and order-of-magnitude wall-clock. The
-- authoritative real-run wall-clock is `extraction_runs.duration` (see
-- verify_dedup_refactor_run.sql §1). EXPLAIN (ANALYZE) actually RUNS the query.
--
-- What to look for:
--   * "Hash Join" nodes building on _dedup_probe (the small side), with a single
--     Seq Scan of nhtsa_recalls_bronze per stage — NOT "Nested Loop" with
--     per-row scans (the zero-stats failure mode the ANALYZE step prevents).
--   * "Execution Time" at the end (~1-2 min target vs the ~17 min chunked path).
--
-- Run as the runtime role (also a privilege smoke test: CREATE TEMP + ANALYZE
-- must succeed for recalls_app):
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/bronze/explain_staged_dedup_join.sql

\timing on

DROP TABLE IF EXISTS _dedup_probe;

-- N text identity columns, matching the loader's staging table (c0..c10).
CREATE TEMP TABLE _dedup_probe (
    c0  text, c1  text, c2  text, c3  text, c4  text, c5 text,
    c6  text, c7  text, c8  text, c9  text, c10 text
);

-- Populate with the text-canonical DISTINCT identities present in bronze — the
-- set the daily full-dump extract would stage.
INSERT INTO _dedup_probe
SELECT DISTINCT
    coalesce(cast(campno        as text), ''),
    coalesce(cast(maketxt       as text), ''),
    coalesce(cast(modeltxt      as text), ''),
    coalesce(cast(yeartxt       as text), ''),
    coalesce(cast(compname      as text), ''),
    coalesce(cast(rcl_cmpt_id   as text), ''),
    coalesce(cast(mfr_comp_ptno as text), ''),
    coalesce(cast(mfr_comp_desc as text), ''),
    coalesce(cast(mfr_comp_name as text), ''),
    coalesce(to_char(endman AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), ''),
    coalesce(to_char(bgman  AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')
FROM nhtsa_recalls_bronze;

-- Load-bearing: without stats the planner assumes ~1000 rows and may nested-loop.
ANALYZE _dedup_probe;

\echo
\echo '=== row count staged into the probe (the JOIN build side) ==='
SELECT count(*) AS probe_rows FROM _dedup_probe;

\echo
\echo '=== EXPLAIN (ANALYZE, BUFFERS) of the staged two-stage JOIN ==='
EXPLAIN (ANALYZE, BUFFERS)
WITH latest AS (
    SELECT
        coalesce(cast(b.campno        as text), '')                                          AS f0,
        coalesce(cast(b.maketxt       as text), '')                                          AS f1,
        coalesce(cast(b.modeltxt      as text), '')                                          AS f2,
        coalesce(cast(b.yeartxt       as text), '')                                          AS f3,
        coalesce(cast(b.compname      as text), '')                                          AS f4,
        coalesce(cast(b.rcl_cmpt_id   as text), '')                                          AS f5,
        coalesce(cast(b.mfr_comp_ptno as text), '')                                          AS f6,
        coalesce(cast(b.mfr_comp_desc as text), '')                                          AS f7,
        coalesce(cast(b.mfr_comp_name as text), '')                                          AS f8,
        coalesce(to_char(b.endman AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')     AS f9,
        coalesce(to_char(b.bgman  AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')     AS f10,
        max(b.extraction_timestamp)                                                          AS max_ts
    FROM nhtsa_recalls_bronze b
    JOIN _dedup_probe s
        ON  coalesce(cast(b.campno        as text), '')                                       = s.c0
        AND coalesce(cast(b.maketxt       as text), '')                                       = s.c1
        AND coalesce(cast(b.modeltxt      as text), '')                                       = s.c2
        AND coalesce(cast(b.yeartxt       as text), '')                                       = s.c3
        AND coalesce(cast(b.compname      as text), '')                                       = s.c4
        AND coalesce(cast(b.rcl_cmpt_id   as text), '')                                       = s.c5
        AND coalesce(cast(b.mfr_comp_ptno as text), '')                                       = s.c6
        AND coalesce(cast(b.mfr_comp_desc as text), '')                                       = s.c7
        AND coalesce(cast(b.mfr_comp_name as text), '')                                       = s.c8
        AND coalesce(to_char(b.endman AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')  = s.c9
        AND coalesce(to_char(b.bgman  AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')  = s.c10
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
SELECT l.f0, l.f1, l.f2, l.f3, l.f4, l.f5, l.f6, l.f7, l.f8, l.f9, l.f10, b.content_hash
FROM nhtsa_recalls_bronze b
JOIN latest l
    ON  coalesce(cast(b.campno        as text), '')                                       = l.f0
    AND coalesce(cast(b.maketxt       as text), '')                                       = l.f1
    AND coalesce(cast(b.modeltxt      as text), '')                                       = l.f2
    AND coalesce(cast(b.yeartxt       as text), '')                                       = l.f3
    AND coalesce(cast(b.compname      as text), '')                                       = l.f4
    AND coalesce(cast(b.rcl_cmpt_id   as text), '')                                       = l.f5
    AND coalesce(cast(b.mfr_comp_ptno as text), '')                                       = l.f6
    AND coalesce(cast(b.mfr_comp_desc as text), '')                                       = l.f7
    AND coalesce(cast(b.mfr_comp_name as text), '')                                       = l.f8
    AND coalesce(to_char(b.endman AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')  = l.f9
    AND coalesce(to_char(b.bgman  AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')  = l.f10
    AND b.extraction_timestamp = l.max_ts;

DROP TABLE IF EXISTS _dedup_probe;
