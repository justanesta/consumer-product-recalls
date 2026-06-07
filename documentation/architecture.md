# Architecture

System-level overview of the consumer-product-recalls EtLT medallion pipeline. Covers the four-layer medallion structure, end-to-end data flow, the components that implement each layer, and the load-bearing invariants that hold across them.

> ⚠️ **Note (2026-06-01):** the extractor table and registry counts below were corrected inline for the USCG scraping sources (live since 2026-05-15). A fuller architecture refresh — data-flow diagrams, medallion narrative, source lists — is scheduled in the Phase 6f doc-sync (`project_scope/phase-6-execution-plan.md` §6f).

This is the reader's-entry-point document. For:
- **Per-source silver mapping decisions** (column unification, surrogate keys, null-filling) — see [`silver_design_notes.md`](silver_design_notes.md).
- **Schema reference** (table-by-table column types, business keys, glossary) — see [`data_schemas.md`](data_schemas.md).
- **Local development** (setup, running tests, debugging) — see [`development.md`](development.md).
- **Command cheat sheet** (uv, recalls CLI, alembic, ruff, pyright, pytest, dbt, bru, R2, neonctl, psql, gh) — see [`commands.md`](commands.md).
- **Production operations** (monitoring queries, secret rotation, re-ingestion procedures) — see [`operations.md`](operations.md).
- **Why a particular choice was made** — see [`decisions/`](decisions/) (Architecture Decision Records).

---

## The four-layer medallion

The pipeline is built around the medallion architecture defined in [ADR 0004](decisions/0004-four-layer-medallion-pipeline.md):

```
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │   Landing    │ →  │    Bronze    │ →  │    Silver    │ →  │     Gold     │
   │              │    │              │    │              │    │              │
   │ raw payloads │    │  validated,  │    │   unified    │    │ pre-aggregated│
   │  on R2,      │    │  per-source, │    │   schema     │    │  views,       │
   │  immutable   │    │  insert-only │    │   across     │    │  search       │
   │              │    │              │    │   sources    │    │  indexes      │
   └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
       (R2)               (Postgres)          (Postgres)          (Postgres)
```

Each layer has a different audience and a different mutability story.

| Layer | Storage | Mutability | Audience | Schema |
|---|---|---|---|---|
| **Landing (T0)** | Cloudflare R2 | Append-only; immutable | Operators (forensic), re-ingest CLI | Source-native (raw JSON / TSV / HTML) |
| **Bronze** | Neon Postgres | Insert-only; content-hash-keyed dedup | Operators, dbt | Per-source Pydantic-validated tables |
| **Silver** | Neon Postgres | Rebuilt by dbt every transform run | Operators, dashboards, gold consumers | Unified across all sources (`recall_event` / `recall_product` / `firm` / `recall_event_firm`) |
| **Gold** | Neon Postgres | Rebuilt by dbt; views/materializations | Dashboards, FastAPI serving layer (Phase 8) | Denormalized, query-shape-driven |

The boundaries are not arbitrary — each one is enforced by a different mechanism:

- **Source → Landing** is enforced by the extractor's `land_raw()` step ([ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md)). Every byte fetched is persisted to R2 before validation. If anything downstream fails, raw is recoverable.
- **Landing → Bronze** is enforced by Pydantic strict validation ([ADR 0014](decisions/0014-schema-evolution-policy.md)) and content-hash-conditional inserts ([ADR 0007](decisions/0007-lineage-via-bronze-snapshots-and-content-hashing.md)). Records that fail structural or business-invariant checks route to per-source `_rejected` tables ([ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md)) — they never enter bronze proper.
- **Bronze → Silver** is enforced by dbt models with generic and singular tests ([ADR 0011](decisions/0011-transformation-framework-dbt-core.md), [ADR 0015](decisions/0015-testing-strategy.md)). dbt does not touch `_rejected` tables — they are forensic surfaces, not transformation inputs.
- **Silver → Gold** is enforced by dbt's view/materialization machinery. Gold is a query-shape projection of silver; no new business logic is introduced.

---

Succinct mental model of data transformations on each layer:
- **Landing** = byte-for-byte what is given from source that lands in R2 buckets
- **Bronze** = parsed and typed source values within column-type constraints (preserves source-verbatim where the column type allows).                                                        
- **Staging (pre-Silver)** = source-shape normalization (empty strings, sentinels, encoding, dedup, bilingual filter). Per-source cleanup, no cross-source thinking.
- **Silver** = cross-source unification (surrogate keys, unions, firm dedup, role assignment). The first layer where "give me all recall events across CPSC + FDA + USDA" is one query.       
- **Gold** = consumer-shaped serving + analytics (ADR 0038): denormalized serving marts (`mart_recall_summary` / `mart_firm_profile` / `mart_product_search`) for the Phase 8 API, plus aggregate `fct_*` marts (time series, by-firm, classification, geography two-lens, units) for dashboards. Postgres FTS (tsvector + GIN) powers product search.

End-to-end, a single record moves like this: the extractor fetches a source payload, lands the raw bytes in R2 untouched (Landing), Pydantic parses and types it, and the validated record is bulk-inserted into a per-source Postgres bronze table (Bronze). On the next dbt run, a per-source staging view normalizes source quirks, silver models union it into the cross-source `recall_event` and friends (Silver), and gold models project silver into consumer-shaped marts — denormalized serving tables for the API and pre-aggregated `fct_*` views for dashboards (Gold).

## End-to-end data flow

```
                                     ┌─────────────────────────────────────────┐
                                     │  GitHub Actions cron schedule (ADR 0010) │
                                     │  • daily: extract-cpsc, extract-fda,    │
                                     │           extract-usda                  │
                                     │  • weekly: extract-nhtsa, extract-uscg, │
                                     │            deep-rescan-cpsc/fda         │
                                     └────────────────┬────────────────────────┘
                                                      │
                                                      │ workflow_dispatch /
                                                      │ schedule trigger
                                                      ▼
              ┌──────────────────────────────────────────────────────────────────┐
              │                      Extractor (per source)                     │
              │                                                                 │
              │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────────┐  │
              │   │ extract  │ → │land_raw  │ → │ validate │ → │check_invar. │  │
              │   │ (live)   │   │  (R2)    │   │(Pydantic)│   │ (business)  │  │
              │   └────┬─────┘   └────┬─────┘   └────┬─────┘   └──────┬──────┘  │
              │        │              │               │                │         │
              │        │              │               ▼                ▼         │
              │        │              │           ┌────────────────────────┐    │
              │        │              │           │   _rejected tables     │    │
              │        │              │           │ (per-source forensic)  │    │
              │        │              │           └────────────────────────┘    │
              │        │              │                                          │
              │        │              ▼                                          │
              │        │     ┌──────────────────┐                                │
              │        │     │ Cloudflare R2    │   (T0 — raw, immutable)        │
              │        │     │ <source>/<date>/ │                                │
              │        │     └──────────────────┘                                │
              │        │                                                          │
              │        ▼                                                          │
              │   ┌──────────────────┐                                            │
              │   │  load_bronze     │   (content-hash conditional insert)        │
              │   └────┬─────────────┘                                            │
              └────────┼─────────────────────────────────────────────────────────┘
                       │
                       ▼
              ┌────────────────────────────────────┐
              │  Bronze (Postgres)                 │
              │  • <source>_recalls_bronze         │
              │  • <source>_recalls_rejected       │
              │  • source_watermarks               │
              │  • extraction_runs                 │
              └─────────────┬──────────────────────┘
                            │
                            │ scheduled: dbt build (Phase 7 transform workflow)
                            ▼
              ┌────────────────────────────────────┐
              │  Silver (Postgres, dbt-managed)    │
              │  • staging: stg_<source>_*         │
              │  • silver:  recall_event,          │
              │             recall_product, firm,  │
              │             recall_event_firm,     │
              │             recall_event_history   │
              │  • dbt tests: not_null, unique,    │
              │    accepted_values, relationships  │
              └─────────────┬──────────────────────┘
                            │
                            │ same dbt run, downstream models
                            ▼
              ┌────────────────────────────────────┐
              │  Gold (Postgres, dbt-managed)      │
              │  • aggregate views                 │
              │  • search-index materializations   │
              │  • feeds Phase 8 FastAPI layer     │
              └────────────────────────────────────┘
```

Three things happen per extraction run that the diagram above abbreviates:

1. **Run metadata is recorded.** `extraction_runs` gets a row with `source`, `started_at`, `status`, `records_extracted`, `records_inserted`, and `change_type` (per [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md), distinguishing routine runs from re-baselines). For presence-tracked sources (USDA initially, per [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md)), the same `_record_run` transaction also writes the per-run **presence manifest** (`extraction_run_identities`) — one row per recall identity returned by the run. Presence is distinct from content: bronze records *what changed*, the manifest records *what was present*, so silver can tell a retraction (absent upstream) from an unchanged record — both of which produce zero new bronze rows. The manifest is written here, not in `load_bronze`, because it FKs to the `extraction_runs` row, which is recorded after the bronze write.
2. **Watermarks advance** for sources that have one. `source_watermarks.last_extracted_at` is updated after a successful run ([ADR 0020](decisions/0020-pipeline-state-tracking.md)). Watermarks are advisory cursors — they tell the extractor where to start its next incremental query, but the bronze content-hash dedup is what actually prevents duplicates.
3. **Logs are emitted to stdout in JSON** via `structlog`, with a `run_id` correlation ID that ties together every log line from a single extraction ([ADR 0021](decisions/0021-structured-logging.md)).

---

## Components

### `src/extractors/` — the extraction layer

| File | Role |
|---|---|
| `_base.py` | `Extractor` ABC — defines the 5-step lifecycle (`extract`, `land_raw`, `validate`, `check_invariants`, `load_bronze`) shared by every source; also contains the `RestApiExtractor` (REST sources) operation-type subclass |
| `_flat_file.py` | `FlatFileExtractor` — operation-type subclass for tab-delimited downloads (NHTSA) |
| `_html_scraping.py` | `HtmlScrapingExtractor` — operation-type subclass for paginated HTML scrapes (BeautifulSoup). **In production for the three USCG sources** (was "reserved for future use" before the 2026-05-15 USCG reactivation) |
| `_fsis_headers.py` | Shared browser-fingerprint header helper for USDA FSIS endpoints (per ADR 0016 amendment — bot-manager fingerprinting) |
| `cpsc.py` / `fda.py` / `usda.py` / `usda_establishment.py` / `nhtsa.py` / `uscg.py` / `uscg_manufacturer.py` / `uscg_manufacturer_detail.py` | Per-source concrete subclasses |
| `fda_press_release.py` | FDA Tier-3 per-event press-release extractor (incremental + deep-rescan). `FdaPressReleaseCheckpointedSeedLoader` is the resumable **recent-first** historical seed: each batch is a normal `run()` that lands+loads+checkpoints into `deep_rescan_checkpoints` (cursor co-committed with the batch's bronze rows), looping to completion and resuming from the DB cursor — see [`fda-press-release-seed-plan.md`](../project_scope/fda-press-release-seed-plan.md) |

The hierarchy is **two layers deep**: `Extractor` (ABC) → operation-type subclass (`RestApiExtractor`, etc.) → per-source concrete subclass. This was deliberate per [ADR 0012](decisions/0012-extractor-pattern-custom-abc-and-per-source-subclasses.md): `Extractor` defines the lifecycle contract, the operation-type subclasses encode shape-specific concerns (pagination loops for REST, ZIP unpacking for flat files, BeautifulSoup parsing for scraping), and concrete subclasses encode source-specific quirks (auth headers, watermark column names, response-shape multiplexing).

### `src/landing/` — raw payload landing

| File | Role |
|---|---|
| `r2.py` | R2 client wrapper — writes raw extracted bytes to `<source>/<extraction_date>/<key>` keys |

R2 is the immutable substrate. Every extraction's raw payload lands here before validation, and stays forever (current retention policy: keep). This is the substrate for ADR 0014 schema-drift recovery and ADR 0028 backfill mechanisms B and C.

### `src/bronze/` — bronze loading and shared mechanisms

| File | Role |
|---|---|
| `loader.py` | `BronzeLoader` — content-hash conditional insert + quarantine routing |
| `manifest.py` | Pure builder for the per-run presence manifest (`extraction_run_identities`, ADR 0026) — turns a run's passing records into recall-grain presence rows; the write happens in `Extractor._record_run` |
| `hashing.py` | Canonical-serialization + SHA-256 helpers (per ADR 0007 — changes here are treated as schema migrations) |
| `retry.py` | `tenacity`-decorated retry policies, applied to lifecycle methods that contact external services |
| `invariants.py` | Cross-record / business-logic checks (e.g., USDA bilingual orphan, date sanity, null-ID guard) |

The bronze layer's job is "what arrived, what we kept, what we rejected, why" — it is the audit boundary between "bytes from the source" and "data we've taken responsibility for."

### `src/schemas/` — Pydantic bronze contracts

One file per source, each with `ConfigDict(extra='forbid', strict=True)` per [ADR 0014](decisions/0014-schema-evolution-policy.md). Required-by-default fields catch silent renames; `extra='forbid'` catches silent additions. Every drift event surfaces loud at the boundary, not silently downstream.

Per [ADR 0027](decisions/0027-bronze-storage-forced-transforms-only.md), schemas perform only storage-forced transforms (date string → datetime for `TIMESTAMPTZ`, `"True"`/`"False"` → bool for `BOOLEAN`). Value-level normalization (empty-string → null, whitespace strip, false-sentinel handling) lives in silver staging models, not here.

### `src/cli/` — Typer CLI dispatch

`recalls extract <source>`, `recalls deep-rescan <source>`, `recalls version`, and `recalls resolve-firms` (the firm-resolution stage — see [`src/enrichment/`](#srcenrichment--firm-resolution-stage-adr-0037) below). The CLI loads source-level config from `config/sources/<source>.yaml` via `src/config/source_loader.py`, looks up the target extractor class from a static dict in `src/config/source_registry.py` keyed on `source_name`, and constructs the extractor with kwargs filtered against the class's Pydantic `model_fields`. CLI flag-specific behavior (`--lookback-days`, `--since`, `--change-type=etag_audit`) is layered on top of the YAML-driven extractor instance via post-construction methods or attribute mutations. No business logic lives in CLI modules. See ADR 0012's "Implementation notes — source-config loader and registry (Wave 2, landed 2026-05-10)" section.

### `src/config/` — settings, source config, and structured logging

| File | Role |
|---|---|
| `settings.py` | `pydantic-settings` `Settings` model — loads `.env`, fails loud on missing required values, marks credentials as `SecretStr` |
| `source_loader.py` | YAML loader — reads `config/sources/<source>.yaml` and validates through the discriminated union in `source_registry.py`. Strict-fails on extra fields, missing required fields, or wrong `source_type`. |
| `source_registry.py` | Pydantic discriminated-union models (`RestApiSourceConfig`, `FlatFileSourceConfig`); static dicts `EXTRACTOR_BY_SOURCE_NAME` (8 entries) and `DEEP_RESCAN_BY_SOURCE_NAME` (7 entries); `build_extractor_kwargs` helper using `model_fields` introspection per ADR 0012. |
| `logging.py` | `structlog` configuration with `run_id` contextvar, stdlib bridge for third-party libraries |

### `migrations/versions/` — Alembic migrations

Per-source bronze + rejected tables, the shared `extraction_runs` and `source_watermarks` state tables, `extraction_runs.change_type` (added in Phase 5b.2 per ADR 0027), and the `extraction_run_identities` presence manifest (`0027`, Phase 6c per ADR 0026). Migrations are forward-only — there is no `downgrade()` body that does anything meaningful, by convention.

### `dbt/models/` — silver and gold transformations

| Subdirectory | Role |
|---|---|
| `staging/stg_<source>_*.sql` | Per-source views over bronze with type casting, latest-version dedup, value-level normalization |
| `silver/recall_event.sql` etc. | Unified cross-source models — one row per recall event regardless of source |
| `silver/recall_event_history.sql` | Field-level edit history (ADR 0022 / 6c.1) — `LAG()` over **all** bronze snapshots (not the latest-only staging views), excluding re-baseline waves (ADR 0027). The fact-history half of Phase 6c; the dimension half is the SCD-2 snapshots in `silver_snapshots` |
| `snapshots/nhtsa_recall_product_snapshot.sql` + `silver/recall_product_v15.sql` + `recall_product_history.sql` | NHTSA **product-grain** SCD-2 (ADR 0033 7-tuple / 6c.6) — demotes the drift-prone attributes (Pierce class) to Type-2 history while keeping the structural 7-tuple in the key, so an editorial edit versions instead of fragmenting. Built parallel to v1 `recall_product`; the cutover folds `recall_product_v15` into `recall_product` at 6c.7. The product-grain peer of the event-grain `recall_event_history` |
| `gold/mart_*.sql` (serving) + `gold/fct_*.sql` (aggregates) | Consumer-shaped gold (ADR 0038): denormalized serving marts for the API + pre-aggregated dashboards + the FTS product-search index. See [`gold_design_notes.md`](gold_design_notes.md) |

Generic dbt tests (`not_null`, `unique`, `accepted_values`, `relationships`) and singular tests (orphan detection, source count baselines) are configured per [ADR 0015](decisions/0015-testing-strategy.md). **dbt unit tests** (dbt 1.11) drive the *synthesized*-history logic on synthetic snapshots — `recall_event_history` (edit / whitespace-suppress / re-baseline-exclude / two-field / creation / field-clearing-erasure cases) and `recall_lifecycle` (presence + retraction) — which is the Phase-6 "history captures a drift event" quality gate: post-6a.5-reseed live bronze is too sparse (~1 version per identity) to exercise emission, so the logic is proven against fixtures instead. Threshold-aware monitor escalation (warn→error) and synthetic monitor-fire tests are deferred to the Phase-7 DQ framework (Soda/GE) per `dbt/dbt_project.yml`.

### `src/enrichment/` — firm-resolution stage (ADR 0037)

Cross-source firm entity resolution — collapsing the name variants exact-match can't (`"Fisher-Price of East Aurora, N.Y."` → `Fisher-Price`; `HONDA` ↔ `AMERICAN HONDA MOTOR CO`) — runs here, as a **Python stage that writes a table dbt reads as a source**, not as an in-warehouse SQL transform. The *why* (RapidFuzz `token_set_ratio` + union-find clustering can't run in Neon — no `pg_trgm`/`fuzzystrmatch`, no dbt-python runtime — and pg_trgm's char-trigram matching is the wrong model for multi-token company names) is recorded in [ADR 0037](decisions/0037-firm-resolution-python-stage-not-sql-fuzzy.md).

**Grain (ADR 0037):** one `firm` row = **one brand/name cluster** as it appears across the five sources. Every source's structured id — FDA FEI, USDA `establishment_number`, USCG MIC, CPSC `company_id` — is an **attribute** (`firm.observed_company_ids`), *not* a merge key, so the grain is uniform across sources. (FDA's FEI was briefly used as a merge key; because an FEI is an establishment and establishments change corporate hands, it chained unrelated firms into cross-corporate blobs, so it was pulled back to attribute-only — the deferred Tier 0 below.)

| File | Role |
|---|---|
| `firm_normalization.py` | Pure cleaning — `clean_firm_name` (universal DBA strip + a **source-gated** geo-suffix strip: on for CPSC/NHTSA, off for the FEI/establishment/MIC-backed sources, NHTSA guarded against integral-name over-strip; no parenthetical strip — paren-variants go to RapidFuzz, ADR 0037) + `extract_firm_dba` / `extract_paren_aliases` for brand aliases. No I/O. |
| `crosswalk_writer.py` | The I/O boundary — reads the all-source distinct firm names from the `stg_*` views, maps them through the cleaner, truncate-and-reloads `firm_crosswalk`. Pure `build_crosswalk_rows` is split from `resolve_firm_crosswalk` (mirrors the extractor `_parse_*` separation). |
| `firm_resolution.py` | Pure **name-grain** resolution — Tier 1 name repair (identical distinctive-token set / `token_sort_ratio` typo), Tier 2 optional `token_set_ratio` entity rollup (≥2 shared distinctive tokens, place-word guarded). Union-find; repoints `canonical_firm_id` to the representative. Tier 0 FDA-FEI grouping (`fei_resolve`) is retained, tested, but **deferred/opt-in** (`--fei-merge`, off — ADR 0037). |
| `place_words.py` | The Tier-2 weak-token denylists — `PLACE_WORDS` (geographic + compound: `SAN`, `ROCKY MOUNTAIN`, US states…) and `GENERIC_WORDS` (generic-business: `MARKETING`, `CONCEPTS`, `COOPERATIVE`…). A rollup is refused when every shared token is weak (`PLACE_WORDS \| GENERIC_WORDS`); Tier 1's identical-set merge uses `PLACE_WORDS` alone (else `Quality Foods`+`…Inc` would stop merging). Curated tail maintained from the operations.md review loop. |
| `never_merge.py` | Curated do-not-merge clean-name pairs — the manual override for the *two-real-token coincidence* (`Eagle Family` Stores / Foods) the denylists can't refuse; the symmetric counterpart to the FEI `must_link`. Populated from the `audit-firm-rollups` review report. |

**Where it sits in the flow.** It is a post-staging, pre-silver-firm step. The user runs `recalls resolve-firms` (Typer CLI), which reads the same `stg_*` views the silver `firm` model reads, clusters the distinct names, and writes `firm_crosswalk` — a Postgres table created by Alembic (migrations 0024/0025) and registered as the dbt source `enrichment.firm_crosswalk`. The silver `firm.sql` and `recall_event_firm.sql` then LEFT JOIN it for an **additive** `canonical_firm_id = coalesce(crosswalk.canonical_firm_id, md5(normalized_name))`. `firm_id` stays `md5(normalized_name)`; fuzzy merges express only through the additive canonical. Run-order is therefore **`dbt build` (staging) → `recalls resolve-firms` → `dbt build` (silver firm)**.

**Why the seam is safe** (the load-bearing property): the stage is *additive* by construction. Because silver coalesces to `md5(normalized_name)`, a missing, stale, or empty crosswalk degrades to "every firm is its own canonical" (no fuzzy merges) — never broken correctness — so the external stage is never load-bearing. And the JOIN keys match by construction: `firm_id = md5(upper(trim(name)))` is computed in Python over the *same* string Postgres computes, reading the *same* staging views `firm.sql` reads. The pure cluster logic is pytest-covered (`tests/enrichment/`); the landed table carries dbt source-tests (`firm_id` not_null+unique, `canonical_firm_id` not_null) — the two-framework testing split ADR 0037 calls for.

**How the resolution works** (ADR 0037). `firm_resolution.py` resolves the distinct cleaned names with **two name tiers** (plus a deferred FEI tier), all over a *blocking* index — names are partitioned by their first distinctive token (articles / corp-forms / high-document-frequency boilerplate skipped) and compared only within a block, for both speed (~28k names, no all-pairs scan) and precision (unrelated blocks never meet). These replaced an earlier single-rule "subset ⇒ merge" clusterer that catastrophically over-merged on shared common words and on a naive any-shared-FEI link (see the ADR 0037 amendments).

- **Tier 0 — FEI (`fei_exact`). DEFERRED / opt-in (`--fei-merge`, default off).** FDA's FEI is an *establishment* (facility) id, not a firm id; `fei_resolve` groups FDA names by their current establishment id (`coalesce(surviving_fei, fei)`) with a per-FEI fan-out gate. But because facilities are bought and sold, one FEI's recall history spans owners, and a shared ancestor/DBA name then chains *unrelated* firms across owner changes into cross-corporate blobs (the plasma/blood clusters). FDA's surviving fields are ~7%-sparse and absent on the bridge names, so nothing untangles it — hence FEI is attribute-only (`firm.observed_company_ids`), and this tier is retained for a future establishment dimension, not the firm grain.
- **Tier 1 — name repair (`name_variant_exact`, `name_typo_high`).** Within a block, merge names that are essentially the *same string*: an identical distinctive-token set (punctuation / case / spacing / corp-form — corp-forms dropped but **content words kept**), or a high `token_sort_ratio` (a spelling typo, `Bristol Meyers` ↔ `Bristol Myers`). Low risk; always on.
- **Tier 2 — entity rollup (`rapidfuzz_rollup`).** *Optional* (`--rollup`, default on). Merge names sharing **≥2 distinctive multi-character tokens** above `token_set_ratio` threshold (default 90) — `Kawasaki Motors Corp` ↔ `Kawasaki Motors Corp USA`. The residual false-merge mode is a *2-token coincidence* — geographic (`San Antonio Bakery` + `…Eye Bank`) or generic-business (`Great American Marketing` + `Great Lakes …Marketing`) — so a rollup is **refused when every shared token is weak** (`PLACE_WORDS | GENERIC_WORDS`, 6b.6). 1-character tokens are dropped from the ≥2-token count so shared initials can't anchor a rollup. The irreducible *two-real-token* residual (neither token denylistable — `Eagle Family` Stores / Foods) is handled by the curated `never_merge.py` override and surfaced for review by `audit-firm-rollups`.

Union-find merges the surviving links (Tier 1/2 within blocks; Tier 0 seeded first only when `--fei-merge` is on); each node keeps the strongest tier it merged by, and each cluster elects the shortest, simplest name as `canonical_firm_id`.

*Why two stop-sets (the load-bearing subtlety).* Blocking and Tier-2 scoring drop high-DF boilerplate (`AMERICAN`, `INTERNATIONAL`) so a rollup rests on genuinely-distinctive tokens; Tier-1's identical-set test drops *only* corp-forms, keeping content words. Without that split, common nouns dropped as "generic" (`VALLEY`, `FOODS`, `BLOOD`) collapse multi-word names onto a single common token and merge unrelated firms — the `Sun Valley Foods`→`{SUN}`→`Sun` hub class.

The exact `firm_crosswalk` / `firm` / `recall_event_firm` columns and the `match_confidence` vocabulary live in [data_schemas.md](data_schemas.md#firm-resolution); the **scheduled** Tier-2 precision review loop (`recalls audit-firm-rollups` ranked report → fix via `place_words.py` / `GENERIC_WORDS` / `never_merge.py` → re-resolve, with a monthly GHA cron that alerts on a high-risk spike) is in [operations.md](operations.md#firm-resolution-recalls-resolve-firms).

### `tests/` — pytest

Organized into `unit/`, `integration/`, and `e2e/` per [ADR 0015](decisions/0015-testing-strategy.md). Integration tests use VCR cassettes for replayable network scenarios; `respx` is the accepted pattern for hand-constructed error-path mocks.

---

## Load-bearing invariants

Five properties hold across the entire pipeline. Each is enforced by a specific mechanism, not by convention. If any of them break silently, multiple downstream guarantees break with it.

### 1. Idempotency

Re-running any extractor (or any backfill mechanism per [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md)) over the same window produces no duplicate bronze rows.

**Enforcement:** content-hash conditional insert in `BronzeLoader.load()`. The hash is computed via `src/bronze/hashing.py`'s canonical serialization, and any insert whose hash already exists for the same `(source_recall_id, [identity-suffix])` becomes a no-op.

**Consequences:** workflows can be safely retried. Cron overlap with ad-hoc deep-rescan runs is safe. Schema-drift recovery via R2 replay is safe. None of these need cross-coordination.

### 2. Schema-drift visibility

Every meaningful change in source response shape — added field, removed field, renamed field, type change — surfaces loudly at the bronze boundary, not silently downstream.

**Enforcement:** Pydantic `extra='forbid'` + `strict=True` + required-by-default ([ADR 0014](decisions/0014-schema-evolution-policy.md)). Added field → forbid error. Renamed field → missing-required error. Type change → strict-mode validation error. All three route to `<source>_recalls_rejected` with `failure_stage='validate'`.

**Consequences:** silver layer never has to second-guess what bronze means. The trade-off is that schema drift causes ingestion to halt for the affected source — operators need to amend the schema and re-ingest from R2 (per [ADR 0014](decisions/0014-schema-evolution-policy.md), [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B). This is the right trade-off because silent drift is worse than loud halt.

### 3. Watermarking + content-hash dedup composes correctly

The incremental cursor is advisory; the dedup is authoritative. A watermark that misses an edit, a deep rescan over a too-wide window, a clock-skew event — none of these duplicate data.

**Enforcement:** `source_watermarks` is updated after a successful run ([ADR 0020](decisions/0020-pipeline-state-tracking.md)) but the bronze loader does not rely on it for correctness. Content-hash dedup is the actual guard.

**Consequences:** weak-watermark sources (CPSC, USDA — see [ADR 0010](decisions/0010-ingestion-cadence-and-github-actions-cron.md) revision note) are handled by deep-rescan workflows that ignore the watermark and re-fetch wider windows. Bronze stays correct.

### 4. Raw payloads survive every failure mode

Anything fetched from a source is persisted to R2 before validation, so any downstream failure is recoverable.

**Enforcement:** `Extractor.land_raw()` is the second lifecycle step; nothing in steps 3–5 can prevent it from running. Network failures in step 1 fail loud (no raw to land); failures in steps 3–5 leave raw intact.

**Consequences:** R2 is the substrate for [ADR 0014](decisions/0014-schema-evolution-policy.md) re-ingest, [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism B (R2 replay), and [ADR 0028](decisions/0028-backfill-historical-reextraction-semantics.md) Mechanism C (manifest backfill from raw). R2 retention is load-bearing.

### 5. Failure routes are named and queryable

Every class of failure has a documented destination. Schema-violating records go to `<source>_recalls_rejected`. Transient network failures are retried per `tenacity` policy. Auth failures fail loud, no retry. Throttling has source-specific detection (see ADR 0013 amendment for FDA's HTML-redirect throttling). Bot-manager fingerprinting has its own surface (see ADR 0016 amendment for USDA's Akamai gating).

**Enforcement:** `tenacity`-decorated lifecycle methods (`src/bronze/retry.py`); `_rejected` tables ([ADR 0013](decisions/0013-error-handling-retries-idempotency-and-quarantine.md)); structured-log fields on failure events ([ADR 0021](decisions/0021-structured-logging.md)).

**Consequences:** "what failed and why" is one SQL query away. Operators don't need to read GHA logs to diagnose data-shape problems; the rejected tables hold the record + reason + raw R2 path.

---

## What's not in v1

These are deliberate omissions, each documented in an ADR or in `project_scope/implementation_plan.md` "Out of scope":

- **Frontend dashboard** — Phase 9, deferred. Phase 8 ships a FastAPI serving layer; downstream rendering is a separate decision.
- **Application monitoring beyond GHA UI** — formalized in [ADR 0029](decisions/0029-application-observability-and-alerting.md) with named upgrade triggers. v1 = GHA UI + structured logs + SQL queries from operations.md.
- **EPA integration** — deferred per [ADR 0001](decisions/0001-sources-in-scope.md). Re-evaluate when v1 ships.
- **Statistical drift detection** — needs baseline data; v2 effort per [ADR 0015](decisions/0015-testing-strategy.md).
- **Silver-layer interpretation of source-side deletions** — bronze captures the *signal* via [ADR 0026](decisions/0026-lifecycle-tracking-snapshot-presence-manifest.md)'s manifest, but silver in v1 reports `is_currently_active` only. Modeling deletion as a first-class lifecycle event is v2.
- **Authenticated API tier** — public read-only is sufficient for v1.

---

## Reading order for new contributors

1. `README.md` (repo root) — what the project does and how to run it.
2. This file (`architecture.md`) — system shape.
3. [`decisions/README.md`](decisions/README.md) — index of every architectural decision.
4. [`development.md`](development.md) — how to set up locally and run things.
5. [`commands.md`](commands.md) — quick-reference cheat sheet, kept open while you work.
6. [`data_schemas.md`](data_schemas.md) — when you need to know what a column means.
7. [`silver_design_notes.md`](silver_design_notes.md) — when you're adding a new source's silver mapping.
8. [`operations.md`](operations.md) — when you're operating in production.
