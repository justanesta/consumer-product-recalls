{{ config(materialized='table') }}

-- gold_meta — one row, the gold-layer rebuild stamp (serving-layer plan R4, ADR 0038 §2). Set to the
-- dbt run start time so every mart built in the same `dbt build` shares one deterministic `rebuilt_at`.
-- Read by the serving API (recalls-api) to compute a layer-wide ETag / Last-Modified for conditional
-- GET / 304 (gold exposes no other "when was gold last rebuilt" signal — `last_seen_at` is per-recall
-- observation time, not a layer-wide build stamp). Covered by the gold-folder grant post_hook
-- (recalls_readonly SELECT). `run_started_at` is a tz-aware UTC datetime (dbt tracking.py:
-- `datetime.now(tz=pytz.utc)`), identical across every model in one `dbt build`, so we render it
-- straight into a timestamptz literal: its str() carries the +00:00 offset, so `::timestamptz` parses it
-- as UTC regardless of the session TimeZone — no conversion needed. We deliberately do NOT call
-- `astimezone(modules.pytz.UTC)`: dbt exposes `modules.pytz` as a curated dict of `pytz.__all__`, which
-- contains lowercase `utc` but not `UTC`, so `modules.pytz.UTC` is Undefined (the original snippet's bug)
-- — and the conversion would be redundant anyway. `gold_schema_version` is a manual bump via
-- `--vars '{gold_schema_version: "3"}'`; the default below is the floor and is bumped per contract change.
-- v2 (2026-W26): mart_recall_summary gained the non-null `event_date` column and the default feed sort
-- repointed from published_at -> event_date = coalesce(announced_at, published_at) (ADR 0038 §2026-W26).

select
    '{{ run_started_at }}'::timestamptz            as rebuilt_at,
    '{{ var("gold_schema_version", "2") }}'::text as schema_version
