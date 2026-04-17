# 0008 — NHTSA: flat file as primary ingestion source; JSON API reserved for live vehicle lookup

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

NHTSA exposes recall data through two distinct channels:

- **Flat file** at `https://static.nhtsa.gov/odi/ffdd/rcl/`: tab-delimited, full-snapshot bulk download. `FLAT_RCL_POST_2010.zip` (~14 MB) covers post-2010 recalls; `FLAT_RCL_PRE_2010.zip` (~7 MB) covers pre-2010; year-bucketed `RCL_FROM_YYYY_YYYY.zip` files provide chunked access. Schema is 29 fields, last extended in May 2025 to add `DO_NOT_DRIVE` and `PARK_OUTSIDE`. Refreshed daily by NHTSA. ~300–500K total rows.
- **JSON API** at `api.nhtsa.gov`: `/recalls/recallsByVehicle?make=X&model=Y&modelYear=Z` returns recalls for one vehicle; `/recalls/campaignNumber?campaignNumber=X` returns recalls for one campaign. Designed for vehicle-lookup queries, not bulk download.

Ingesting via the JSON API would require enumerating every (make, model, year) tuple — thousands of API calls per refresh — to retrieve what one ZIP download provides atomically.

## Decision

- **Bulk ingestion uses the flat file.** A scheduled job downloads `FLAT_RCL_POST_2010.zip` (and `FLAT_RCL_PRE_2010.zip` once historically), unzips, parses tab-delimited rows into `nhtsa_recalls_bronze`. Content hashing per ADR 0007 skips unchanged rows on each refresh.
- **NHTSA's JSON API (`/recalls/recallsByVehicle`) is reserved for the consumer-facing app's vehicle-lookup feature.** When a user enters their make/model/year, the app calls NHTSA directly in real time, bypassing our database for that specific feature.
- **Schema-drift detection** is enforced via a Pydantic model of the 29-field flat-file schema. Ingestion fails loudly on unexpected fields (additions or removals) rather than silently dropping data — important because NHTSA has historically extended the schema without breaking changes.
- **Bronze granularity** matches the flat file's native shape: one row per `(CAMPNO × MAKETXT × MODELTXT × YEARTXT × COMPNAME)`. Silver-layer transformation is the subject of ADR 0009.

## Consequences

- One download → one parse → one set of inserts per refresh, vs. thousands of API calls. Operationally simpler, faster, and friendlier to NHTSA's infrastructure.
- Bronze always reflects a consistent point-in-time snapshot of NHTSA's full corpus — no partial-state risk from API failures mid-enumeration.
- The consumer-facing app's vehicle-lookup feature can return the freshest possible answer by calling NHTSA's API directly, without waiting for the weekly cron to catch up. Trade-off: the app inherits a runtime dependency on NHTSA API availability for that one feature.
- Pydantic-enforced schema validation surfaces NHTSA's periodic schema additions immediately on the next ingestion, preventing the "silent field drop" failure mode that's common with naive ETL.
- The pipeline does not capture per-VIN affected-vehicle data; the flat file is at make/model/year/component granularity. Per-VIN lookup is delegated to NHTSA's live API in the consumer app, not modeled in our database.
- Pre-2010 history is loaded once during initial seeding from `FLAT_RCL_PRE_2010.zip`; subsequent refreshes only need the post-2010 file plus delta detection.
