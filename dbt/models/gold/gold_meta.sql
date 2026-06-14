{{ config(materialized='table') }}

-- gold_meta — one row, the gold-layer rebuild stamp (serving-layer plan R4, ADR 0038 §2). Set to the
-- dbt run start time so every mart built in the same `dbt build` shares one deterministic `rebuilt_at`.
-- Read by the serving API (recalls-api) to compute a layer-wide ETag / Last-Modified for conditional
-- GET / 304 (gold exposes no other "when was gold last rebuilt" signal — `last_seen_at` is per-recall
-- observation time, not a layer-wide build stamp). Covered by the gold-folder grant post_hook
-- (recalls_readonly SELECT). `run_started_at` is a tz-aware datetime identical across every model in one
-- `dbt build`; `gold_schema_version` is a manual bump via `--vars '{gold_schema_version: "2"}'`.

select
    '{{ run_started_at.astimezone(modules.pytz.UTC).isoformat() }}'::timestamptz as rebuilt_at,
    '{{ var("gold_schema_version", "1") }}'::text                               as schema_version
