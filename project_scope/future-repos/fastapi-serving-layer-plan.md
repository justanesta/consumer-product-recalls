# FastAPI serving-layer plan (Phase 8 — future, separate repo)

- **Status:** Design (drafted 2026-06-08). Not started. Gated on Phase 7 (production cron + transform)
  shipping and on two prerequisite ADRs being filed in *this* repo (0024 + 0025 — see §0).
- **Repo:** a **new, separate greenfield repo** (working name `recalls-api`). It is *not* a directory
  inside this pipeline repo. The pipeline repo's job ends at gold; this repo only **reads** gold.
- **Type:** phase/feature execution plan (documentation_model.md type 5) for a different repo. It drives
  what we build there. World-state facts about the gold marts it consumes are **single-homed** in this
  pipeline repo's `documentation/gold_design_notes.md` + `documentation/data_schemas.md` — this plan
  *points at* them and never restates a column contract as gospel (the marts can evolve; re-read them
  at build time).
- **Audience:** Claude Code executing in the fresh `recalls-api` repo, plus future-me reviewing the
  shape before any code exists.

> **Why a separate repo (decision, not open question).** The pipeline repo is an EtLT system with a
> 14-table bronze schema, 9 extractors, dbt, Alembic, and a heavy test/cassette harness. The API is a
> stateless read-only consumer of one output (gold). Coupling them would (a) drag the API's deploy
> image through the pipeline's dependency tree (pandas, dbt-core, boto3, the Neon-branch test fixture),
> (b) blur the "pipeline writes / API reads" boundary that ADR 0005's read-only-Neon-from-`main`
> posture depends on, and (c) make the API's CI re-run pipeline gates it does not need. Separate repo,
> separate deploy, separate version line. This matches the project's narrow-scope instinct.

---

## §0 — Prerequisite ADRs (file in THIS repo before the API repo is created)

Both are **filed** (ADR 0024 and ADR 0025 were filed 2026-06-09 on branch `feature/phase-7-production-plus-todos` and are listed as Accepted in `documentation/decisions/README.md`). De-reservation is complete. This plan **pre-answered** their load-bearing questions; the ADRs ratify those answers. Start the API repo once Phase 7 (production cron) is complete and the pipeline's gold layer is stable.

### ADR 0024 — Serving-layer API design

Pre-answered decisions to ratify:

- **Endpoints (fixed contract, plan 854–863):** `GET /recalls` (list+filter), `GET
  /recalls/{source}/{recall_id}` (detail), `GET /products/search`, `GET /firms/{id}`,
  `GET /openapi.json` (auto). Plus operational `GET /health` and `GET /health/db`.
- **API↔gold relationship:** the API reads the `mart_*` serving marts **directly** (one keyed read per
  endpoint), per ADR 0038's "denormalized one-big-table" design. It does **not** re-join silver. The
  `fct_*` aggregate marts back optional dashboard endpoints (deferred — see §4 "dashboard endpoints").
- **Star-schema-vs-fct revisit (ADR 0038 §1 + plan 523 routed the deferral here):** **resolve as "no
  star."** The endpoint set is fixed and API-fed; per `gold_design_notes.md` "Deferred: a dimensional
  star schema," an API-fed fixed chart set means `fct_*` already *is* the dashboard layer and a star
  buys nothing. `dim_date` lands pre-Phase-8 in the pipeline repo regardless (already decided
  2026-06-08); the API never needs it.
- **Pagination:** keyset (seek) pagination, not OFFSET — see §3.
- **Auth / rate-limit posture:** public, read-only, **no auth tier** (plan 894). A light IP-based rate
  limit at the app layer (slowapi) to protect the free-tier DB; no API keys.
- **OpenAPI generation:** FastAPI's built-in generator is the source of truth; a committed
  `openapi.json` snapshot + a contract test guard drift (see §5).

### ADR 0025 — API deployment target

Pre-answered (full tradeoff in §8): **Fly.io**, with Render as the documented fallback. Cloudflare
Workers is **rejected** — its Python runtime (Pyodide/WASM) cannot run asyncpg's C extension or a
standard psycopg stack, which is exactly the "Workers' Python limits reshape the endpoint design"
risk plan 848 flagged. Rejecting Workers up front lets ADR 0024 assume a normal CPython async stack.
Evaluate against: cold-start, CPython compatibility, read-only Neon from `main` (ADR 0005), and GH
Actions CI/CD (ADR 0018-style).

---

## §1 — Recommended stack (and why, vs the alternatives)

| Concern | Choice | Why this over the alternative |
|---|---|---|
| Web framework | **FastAPI** | Async-native (matches asyncpg), Pydantic-v2 request/response models give validation + OpenAPI *for free* from the same type. Flask needs Flask-RESTX/marshmallow bolted on for the spec, is sync-by-default (a serverless cold DB call blocks a worker), and would duplicate the Pydantic discipline this project already lives by. FastAPI is the obvious fit. |
| Data validation | **Pydantic v2** | Same major version family the pipeline uses; v2's `model_config`, `Annotated` constraints, and `computed_field` cover every response shape. Mirrors the repo's "Pydantic for every schema" standard. |
| DB driver | **asyncpg** (via SQLAlchemy 2.x async engine) | asyncpg is the fastest Postgres driver and async; pairs with `create_async_engine`. We read gold, so we use **SQLAlchemy Core** (text/`select()` against reflected or lightly-declared tables) — **not** the full ORM. The marts are wide denormalized tables with jsonb columns; an ORM identity map buys nothing for read-only one-row reads and adds mapping overhead. |
| Query layer | **SQLAlchemy Core async** | Parameterized `select()`/`text()` with bind params (never f-string SQL — sql-integration-patterns: parameterize always). Keeps us one `pip install` from raw asyncpg if a hot path needs it. |
| Settings | **pydantic-settings** (`BaseSettings`) | Same fail-loud-at-boot posture as the pipeline's `settings.py` (ADR 0016): a missing `DATABASE_URL` raises `ValidationError` at import, not a `KeyError` mid-request. |
| Migrations | **none** | This repo owns no schema. Gold DDL is the pipeline's dbt. The API repo has **zero** Alembic. |
| Logging | **structlog** (JSON) | Mirror the pipeline's structured-logging standard; one log line per request with method/path/status/latency/row-count. |
| Test client | **httpx.AsyncClient + ASGITransport** | In-process ASGI calls (no live server) for fast integration tests; `pytest-asyncio`. |
| Lint/format/types | **ruff + pyright** | Identical gate to the pipeline (CLAUDE.md bar), so the muscle memory and config transfer. |

**Python version:** 3.12 (matches the pipeline runtime). `uv` for env + lockfile (matches the pipeline's
`uv`/direnv workflow — bare commands, `.venv/bin` on PATH).

Modern-python-patterns to apply throughout: `from __future__ import annotations`, `Annotated` types for
both FastAPI `Depends`/`Query` and Pydantic field constraints, `StrEnum` for the `source`/`classification`
filter enums, `match` for the source-dispatch in the detail endpoint, `|`-union types, no `Optional[...]`.

---

## §2 — Repo structure

```
recalls-api/
  pyproject.toml              # deps + ruff + pyright + pytest config; version 0.1.0 (manual bumps)
  uv.lock
  .env.example                # documents required env vars (no secrets committed)
  .envrc                      # direnv: load .env + .venv/bin (mirror pipeline)
  README.md                   # what it serves, how to run, links to pipeline gold docs
  Dockerfile                  # slim CPython 3.12, uvicorn entrypoint (Fly.io)
  fly.toml                    # Fly app config (or render.yaml fallback)
  .github/workflows/
    ci.yml                    # ruff + pyright + pytest (unit+integration) + contract test
    deploy.yml                # build+deploy on push to main (Fly deploy)
    openapi-check.yml         # optional: regenerate + diff committed openapi.json
  openapi.json                # committed snapshot; guarded by a contract test (§5)
  src/recalls_api/
    __init__.py               # __version__
    main.py                   # FastAPI() app factory, router includes, lifespan (pool open/close)
    settings.py               # pydantic-settings BaseSettings (DATABASE_URL, pool sizes, env)
    db.py                     # async engine + pool, get_session dependency, healthcheck query
    logging.py                # structlog config + request-logging middleware
    errors.py                 # exception types + handlers (taxonomy §6)
    pagination.py             # keyset cursor encode/decode + Page[T] envelope
    deps.py                   # shared Depends (db session, pagination params, common filters)
    routers/
      recalls.py              # GET /recalls, GET /recalls/{source}/{recall_id}
      products.py             # GET /products/search
      firms.py                # GET /firms/{id}
      health.py               # GET /health, GET /health/db
    models/                   # Pydantic v2 RESPONSE models (one module per resource)
      common.py               # Page[T], FirmRef, ProductRollup, Cursor, enums (Source, Classification)
      recalls.py              # RecallSummary, RecallDetail
      firms.py                # FirmProfile, SidecarAttributes
      products.py             # ProductSearchHit
    queries/                  # SQL builders (SQLAlchemy Core) — pure, unit-testable, no I/O
      recalls.py
      firms.py
      products.py
  tests/
    conftest.py               # async client fixture, seeded-test-DB fixture (§7)
    unit/                     # query-builder + pagination + model-coercion tests (no DB)
    integration/              # httpx ASGI against a seeded test DB
    contract/                 # openapi.json snapshot + response-schema conformance
    fixtures/
      seed_gold.sql           # minimal gold-mart rows that mirror real shapes (the "cassette")
```

**Pure-logic seam (CLAUDE.md scripts/src bar applies here too):** `queries/` builds SQL strings/params
with **no** DB handle, and `pagination.py` encodes/decodes cursors with no I/O — both unit-tested without
a database, mirroring the pipeline's `_parse_*`-separated-from-network discipline.

---

## §3 — Endpoint inventory, mapped 1:1 to gold marts

Each endpoint is **one keyed read** of one serving mart. Column lists below are sourced from the live
mart SQL (`dbt/models/gold/mart_*.sql`) as of 2026-06-08 — **re-read those files at build time**; this
plan is the map, not the contract.

### `GET /recalls` — list with filters → `mart_recall_summary`

- **Filters (query params, all optional):** `source` (enum: cpsc/fda/usda/nhtsa/uscg),
  `classification`, `is_active` (bool), `published_after` / `published_before` (date), `firm` (substring
  match against `primary_firm_name`, ILIKE), `q` (free-text — defer to `/products/search`; not on this
  list endpoint v1). Pagination params per §3-pagination.
- **Ordering:** `published_at DESC, recall_event_id` (deterministic tiebreak for keyset).
- **Response:** `Page[RecallSummary]`. `RecallSummary` projects the *list-relevant* subset of the mart
  (NOT the full wide row — keep list payloads small): `recall_event_id, source, source_recall_id, title,
  recall_reason, url, announced_at, published_at, classification, risk_level, lifecycle_status,
  is_active, reason_category, primary_firm_name, firm_count, product_count, edit_event_count,
  has_been_edited`.
- **Index reliance:** the mart's `(source, published_at)`, `(is_active)`, `(classification)` btree
  indexes (declared in `mart_recall_summary.sql` config) back every filter; the keyset sort uses
  `(published_at, recall_event_id)`.

### `GET /recalls/{source}/{recall_id}` — detail → `mart_recall_summary`

- **Path params:** `source` (enum), `recall_id` (source's native id, e.g. CPSC `"24-158"`).
- **Lookup:** by `(source, source_recall_id)` — NOT directly by the derived `recall_event_id` (the
  public URL uses the source-native id, which is what users have). **Compute-the-surrogate (recommended,
  zero upstream change):** `recall_event_id = md5(source || '|' || source_recall_id)` (confirmed in
  `_silver.yml` ~line 14), and the mart *already* carries a unique index on `recall_event_id` — so the
  API can md5 the two path params and hit the existing unique index for an O(1) lookup with **no new
  index needed**. This is the lower-footprint default. **Alternative (optional):** add a
  `(source, source_recall_id)` composite index to `mart_recall_summary.sql` — only worth it if you'd
  rather not put md5 logic in the API layer; it's a paired gold-doc + dbt change in the pipeline repo
  (C0c-dbt), not here. Without either, a `(source, source_recall_id)` filter is seq-scan-ish.
- **Response:** `RecallDetail` — the **full** wide row (project *every* `mart_recall_summary` column;
  re-read the SQL for the authoritative set). Includes the scalar detail fields the list view omits
  (`distribution_scope, distribution_states, corrective_action, consequence_of_defect`) and the jsonb
  rollups deserialized into typed sub-models: `firms` (→ `list[FirmRef]`),
  `product_names`/`models`/`hins`/`product_upcs`/`distribution_state_codes`/`hazards` (jsonb arrays →
  typed lists), lifecycle fields (`first_seen_at, last_seen_at, edit_count, is_currently_active,
  was_ever_retracted`), and history flags.
- **404** when no row (errors §6).

### `GET /products/search` — → `mart_product_search`

Two access paths, both backed by that mart's indexes:

- **Identifier lookup** (exact): `?hin=`, `?model=`, `?upc=` → btree indexes on `hin`/`model`/`upc`.
  This is the "is my boat/car recalled?" path.
- **Free-text keyword** (`?q=`): Postgres FTS over the stored `search_vector` (`to_tsvector('english',
  product_name||description||recall_title||firm_name)`), GIN-indexed. Query =
  `WHERE search_vector @@ websearch_to_tsquery('english', :q)`, ranked by
  `ts_rank_cd(search_vector, query)`. **Use `websearch_to_tsquery`** (not `plainto_`/`to_tsquery`) — it
  accepts user-facing quoting/`OR`/`-` syntax and never raises on malformed input, so we don't
  sanitize-and-pray. `pg_trgm` is **not** enabled (ADR 0037) → **no fuzzy/typo search**; document that
  honestly in the endpoint description and OpenAPI.
- **Response:** `Page[ProductSearchHit]`: `recall_product_id, recall_event_id, source, source_recall_id,
  product_name, product_description, model, type, model_year, hin, upc, recall_title, classification,
  risk_level, published_at, url, is_active, firm_name, recall_product_upcs` + a `rank` float for the FTS
  path. Keyset ordering on the FTS path is `(rank DESC, recall_product_id)`; on the identifier path
  `(published_at DESC, recall_product_id)`.
- **Honest-caveat carry-through (from the mart comment):** product-grain `upc` is NULL for every source
  today; recall-level UPCs ride `recall_product_upcs` (jsonb). Surface a `upc_is_recall_level: true`
  note in the response/description so a `?upc=` miss isn't misread as "not recalled."
- **Validation:** require at least one of `q|hin|model|upc`; 422 otherwise (errors §6).

### `GET /firms/{id}` — → `mart_firm_profile`

- **Path param:** `id` = canonical `firm_id` (the md5 cluster id; unique index on the mart).
- **Response:** `FirmProfile`: `firm_id, canonical_name, normalized_name, observed_names,
  observed_company_ids, alternate_names, total_recalls, active_recalls, first_recall_at, last_recall_at,
  roles, recalls_by_source (jsonb dict → `dict[str,int]`), distinct_products,` and the three per-source
  SCD-2 sidecar blocks `establishment_attributes / manufacturer_attributes / fda_attributes` (jsonb
  arrays → `list[SidecarAttributes]`, typed loosely as the sidecar shapes differ by source). 404 on miss.
  - **NOTE:** the three firm-sidecar names are being renamed to `firm_{usda,uscg,fda}_attributes` by the
    phase-7 branch (C19) before this repo is built — confirm the final mart column names against the live
    `mart_firm_profile.sql` at build time.

### Deferred — dashboard endpoints over `fct_*`

The `fct_recalls_by_{week,month,year}`, `_monthly_trend`, `_by_firm`, `_by_classification`,
`_recall_status`, `_by_geography`, `_units_recalled` marts back **optional** read-only aggregate
endpoints (e.g. `GET /stats/recalls-by-month?source=`). **Not in the v1 endpoint contract (plan 854–859
lists only the four).** The website plan's §5.5 dashboard inventory is the concrete trigger that
un-defers the `/stats/*` family; ADR 0024 resolves whether they ship in API-v1. If the website is in
scope for the portfolio milestone, `/stats/*` is effectively v1. Scope them only if/when the frontend
(Phase 9) needs them — and note the
geography lens trap: `fct_recalls_by_geography` has two non-interchangeable bases (`distribution` vs
`firm_location`); any endpoint MUST expose `geography_basis` and carry the "never read as consumer
impact" caveat from `gold_design_notes.md`. Don't quietly pick one.

---

## §4 — Pydantic response models + jsonb handling

- One Pydantic v2 model per response shape; `model_config = ConfigDict(from_attributes=True)` so a
  SQLAlchemy `Row` maps directly.
- **jsonb columns** come back from asyncpg already as Python `list`/`dict` (asyncpg decodes jsonb). Model
  them as typed `list[FirmRef]` / `dict[str, int]` etc. and let Pydantic coerce — but **be defensive**:
  the marts `coalesce(..., '[]'::jsonb)` some arrays and leave others NULL (`product_names`, `models`,
  `hins` are NULL when empty). Default those fields to `[]` in the model (`Field(default_factory=list)`)
  and use `| None` only where NULL is semantically meaningful (e.g. `first_recall_at`).
- **Enums** as `StrEnum` (`Source`, `Classification`) shared between request filters and response fields
  — single source of truth, and they render as OpenAPI enums automatically.
- **Datetimes:** the marts carry `timestamptz`; serialize as ISO-8601 (Pydantic default). Date-only
  filters (`published_after`) parse as `date` and compare against the `timestamptz` column server-side.
- **Decimal/bigint:** unit counts and `firm_count` are ints; keep them `int`, not `float`.

---

## §5 — Pagination, filtering, and the tsvector search

### Keyset (seek) pagination — not OFFSET

- **Why:** OFFSET re-scans and discards N rows per page; on a 130k-row recall mart deep pages get slow
  and are unstable under concurrent writes (the transform cron rewrites these tables nightly).
  sql-query-optimization: keyset is O(page size) regardless of depth and stable across the rebuild.
- **Mechanism:** the cursor encodes the last row's sort tuple. For `/recalls`:
  `WHERE (published_at, recall_event_id) < (:cursor_published_at, :cursor_recall_event_id)
   ORDER BY published_at DESC, recall_event_id DESC LIMIT :limit + 1`. Fetch `limit+1` to compute
  `has_next` without a count. Cursor = base64url of a signed/opaque `{published_at, recall_event_id}`
  (opaque so clients don't construct it). `limit` capped (e.g. default 25, max 100) via an `Annotated`
  `Query(le=100)`.
- **Envelope:** `Page[T] = { items: list[T], next_cursor: str | None, limit: int }`. **No total count**
  by default (a `COUNT(*)` over a filtered 130k mart per request is wasteful); offer `?with_total=true`
  as an opt-in that runs a separate count only when asked.
- The cursor encode/decode lives in `pagination.py` as **pure functions** with unit tests (round-trip,
  tamper/garbage → 400, boundary empties).

### Filtering

- All filters are `Annotated[... , Query(...)]` params with constraints declared inline (FastAPI
  validates + documents them). Build the `WHERE` with SQLAlchemy Core conditional `.where()` chaining
  (only append a predicate when its param is set) — **never** string-concatenate SQL. Every value is a
  bind param (sql-integration-patterns).
- `firm` substring → `primary_firm_name ILIKE '%'||:firm||'%'` (no firm index on that expression today;
  acceptable for a personal-scale API, but note it as a future trigram/expression-index candidate in the
  pipeline repo if it ever gets hot).

### tsvector product search

Covered in §3 `/products/search`. Key engineering notes: `websearch_to_tsquery('english', :q)` for
injection-safe, never-raising parsing; GIN index already exists; rank with `ts_rank_cd`; keyset on
`(rank, recall_product_id)`. The `search_vector` is **stored** in the mart (computed in dbt), so the API
never builds it at query time — it only matches against it.

---

## §6 — Error-handling taxonomy (python-error-handling lens)

A small exception hierarchy in `errors.py`, each mapped to one FastAPI exception handler that emits a
consistent JSON error envelope `{ "error": { "type", "detail", "request_id" } }`:

| Exception | HTTP | When | Notes |
|---|---|---|---|
| `ResourceNotFound` | 404 | detail/firm lookup returns 0 rows | message names the resource + id; no stack to client |
| `InvalidParameter` | 422 | filter fails Pydantic/Query constraint, or `/products/search` with no `q|hin|model|upc` | FastAPI's own `RequestValidationError` handler reshaped into the same envelope |
| `BadCursor` | 400 | cursor fails decode/signature | from `pagination.decode` |
| `UpstreamUnavailable` | 503 | DB pool can't acquire / Neon cold or asleep | **retry-friendly**: set `Retry-After`; this is the serverless-Neon cold-start case (§9) |
| `RateLimited` | 429 | slowapi limiter trips | `Retry-After` header |
| (unhandled) | 500 | bug | catch-all handler logs full traceback via structlog, returns opaque body — **never leak SQL/DSN/traceback to the client** |

- **Distinguish transient vs permanent** (mirrors the pipeline's retry philosophy): a DB connection
  blip = 503 the client may retry; a bad `recall_id` = 404 they should not. Don't retry inside the
  request for a transient DB error beyond the pool's one reconnect — fail fast to 503 and let the
  client/edge retry (a request shouldn't hang on a cold Neon).
- **`request_id`** (uuid) generated per request in middleware, attached to every log line and echoed in
  the error envelope for correlation.

---

## §7 — Testing strategy (layered; mirror this repo's cassette discipline)

python-testing + data-eng-testing-patterns lenses. Three layers, all under the same `pytest` gate:

1. **Unit (no DB, fast):**
   - `queries/` builders: assert the compiled SQL + bound params for each filter combination
     (parametrized). The builder is pure → trivially testable, like the pipeline's `_parse_*`.
   - `pagination.py`: cursor round-trip, tamper → `BadCursor`, empty-page boundary.
   - Pydantic models: jsonb→typed coercion, NULL-array→`[]` defaults, enum coercion. Use
     `hypothesis` for the cursor codec (property: decode(encode(x)) == x).

2. **Integration (httpx ASGI against a seeded test DB):**
   - `httpx.AsyncClient(transport=ASGITransport(app))` — in-process, no live server.
   - **Seeded test DB = the "cassette" analogue.** `tests/fixtures/seed_gold.sql` inserts a *small,
     hand-built* set of rows into stand-ins for the three serving marts (plus the FTS `search_vector`
     so `to_tsvector`/`@@` actually runs). This mirrors this repo's VCR-cassette discipline: a
     committed, deterministic, offline fixture that exercises the *real* code path (real SQL against
     real Postgres) without touching production Neon. The seeded rows are crafted to cover: an active
     recall, a retracted one, a multi-firm recall (jsonb rollup), a USCG HIN product, an NHTSA model
     product, a firm spanning two sources, and an FTS hit + miss.
   - DB provisioning options (decide in ADR 0025/CI): **(a)** a throwaway Postgres service container in
     GH Actions (`services: postgres`) seeded by `seed_gold.sql` — simplest, no Neon API needed and the
     gold marts have no pipeline dependencies to reproduce; **(b)** an ephemeral **Neon branch** via the
     Neon REST API (parity with this pipeline repo's ADR-0015 `test_db_url` fixture). **Recommend (a)**
     for the API repo's CI (faster, free, fewer secrets) and keep (b) as an optional "real-Neon smoke"
     job. Document the seam so it's swappable, exactly as the pipeline did.

3. **Contract tests:**
   - **OpenAPI snapshot:** generate `app.openapi()`, diff against the committed `openapi.json`; fail on
     drift (forces an intentional regen + review — same spirit as the pipeline's re-baseline gate).
   - **Response-schema conformance:** for each endpoint, assert the live response validates against its
     Pydantic model (it does by construction, but this catches a mart column rename upstream — a *gold
     schema drift* canary). Optionally validate sample responses against the OpenAPI schema with
     `openapi-core`/`schemathesis` (schemathesis can fuzz the spec → cheap property coverage).

Coverage gate: `--cov-fail-under` set to a real number (start 85, matching the pipeline). All tests
offline/deterministic; **no test hits production Neon.**

---

## §8 — OpenAPI generation + the api-spec-generator workflow

- FastAPI auto-derives the full OpenAPI 3.1 doc from the route signatures + Pydantic models; served at
  `/openapi.json`, Swagger UI at `/docs`, ReDoc at `/redoc`. **No hand-written spec.**
- **Enrich it where it earns clarity:** per-endpoint `summary`/`description` (carry the honest caveats —
  no fuzzy search, recall-level UPC, geography-lens warning), `response_model` on every route,
  `responses={404: ..., 422: ..., 503: ...}` so error shapes appear in the spec, and `examples` on the
  filter params and response models (FastAPI `Field(examples=[...])`).
- **api-spec-generator workflow:** treat the spec as a build artifact under version control.
  1. Write routes + Pydantic models.
  2. `python -m recalls_api.export_openapi > openapi.json` (a tiny script that imports the app and dumps
     `app.openapi()` — pure, testable).
  3. Commit `openapi.json`; the contract test (§7) and `openapi-check.yml` fail any PR where the
     regenerated spec differs from the committed one → spec can never silently drift from code.
  4. Downstream (Phase 9 frontend) generates a typed client from `openapi.json` (e.g.
     `openapi-typescript`) — the spec is the contract between repos.

---

## §9 — Performance (python-performance + python-async + sql-query-optimization)

- **Connection pooling for serverless cold starts (the dominant cost):** Neon free tier **auto-suspends**
  the compute after idle; the first request after suspension pays a cold-start wake (hundreds of ms to a
  few seconds). Mitigations, in order:
  - Open a **small async pool at app startup** (`lifespan` context manager opens
    `create_async_engine(..., pool_size=5, max_overflow=5, pool_pre_ping=True, pool_recycle=300)`).
    `pool_pre_ping` + a short `pool_recycle` handle Neon dropping idle connections out from under us.
  - Set asyncpg `command_timeout` and a connect timeout so a cold/asleep DB fails to a **503 with
    `Retry-After`** (errors §6) instead of hanging the request/worker.
  - **Pin the pooled (PgBouncer) Neon connection string** for the app role — Neon's pooler endpoint
    fronts the cold compute and survives reconnects better than a direct connection for a
    many-short-request API. (Decide the exact endpoint in ADR 0025.)
  - Optionally a tiny external cron pinging `/health/db` every few minutes to keep the compute warm —
    cheap, and the pipeline already owns scheduled-workflow patterns. Note the free-tier-hours tradeoff.
- **Read-only enforcement:** the app role is a **read-only** Postgres role on the `main` Neon branch
  (ADR 0005) — `GRANT SELECT` on the gold marts only, no write. This is defense-in-depth (the API issues
  only SELECTs anyway) and matches the pipeline's `*_rejected` REVOKE instinct. **Flag to the pipeline
  repo:** provisioning that read-only role + grants is a one-time DB task owned there (it owns DDL/roles),
  not here. Set `default_transaction_read_only = on` for the role.
- **Query/index reliance:** every endpoint rides an existing gold index (§3). The marts are
  `materialized='table'` and `ANALYZE`d post-rebuild by the pipeline (gold_design_notes "Indexing"), so
  the planner has fresh stats. The API does **no** joins (reads denormalized marts) → no join-plan risk.
  The detail endpoint needs no upstream index if it **computes the `recall_event_id` md5 surrogate** from
  the path params and hits the existing unique index (§3 detail — the recommended, zero-footprint path);
  the `(source, source_recall_id)` composite index (C0c-dbt) is an optional alternative only if you'd
  rather keep md5 out of the API layer. Confirm plans with `EXPLAIN (ANALYZE, BUFFERS)` against a
  representative copy before launch.
- **Async all the way:** every route is `async def`; every DB call `await`s the async session — no sync
  call blocks the event loop. Run multiple per-request reads (e.g. detail's optional fan-out) with
  `asyncio.gather` only if needed; the mart design means most endpoints are a single read, so usually
  there's nothing to gather.
- **Caching:** start with **HTTP cache headers** (`Cache-Control: public, max-age=...`,
  `ETag`/`Last-Modified` keyed off the mart rebuild time) so an edge/CDN absorbs repeat reads — the data
  only changes once nightly (transform cron). This is the highest-leverage, lowest-complexity cache for a
  nightly-batch dataset. Add an in-process TTL cache (`async-lru` / `cachetools`) for hot detail/firm
  reads only if metrics show it's needed — **don't pre-optimize.** No Redis (over-kill for free-tier,
  personal scale).

---

## §10 — Structured logging / observability

- **structlog JSON** to stdout (Fly/Render capture stdout): one request-log line with `request_id`,
  method, path, status, latency_ms, row_count, and (on error) the exception type. Bind `request_id` via
  middleware so every line in a request correlates.
- **`/health`** (liveness — process up) and **`/health/db`** (readiness — `SELECT 1` through the pool,
  reports cold-start wake latency). The deploy platform health-checks `/health`.
- **Stance: v1 = "operator looks at platform logs"**, exactly like the pipeline's ADR 0029 — **do not
  build Sentry/Datadog/OTel now.** Document named upgrade triggers (sustained error rate, p95 latency
  budget breach, real traffic) that would justify a future observability ADR in *this* repo. This mirrors
  the pipeline's deliberate-deferral discipline and avoids premature cost/complexity.
- Optional `/metrics` (prometheus-fastapi-instrumentator) is a cheap add if a trigger fires; leave it out
  of v1.

---

## §11 — CI (devops-cicd-patterns; mirror ADR 0018 shape)

`.github/workflows/ci.yml` on PR + push:

1. `uv sync`
2. `ruff check` + `ruff format --check`
3. `pyright`
4. `pytest` (unit + integration + contract) with `--cov-fail-under=85` — integration uses the
   `services: postgres` container seeded by `seed_gold.sql`.
5. OpenAPI drift check (regen `app.openapi()`, diff committed `openapi.json`).
6. `pre-commit run --all-files` in CI (same hooks locally and in CI — the gap ADR 0018 flags).

`deploy.yml` on push to `main`: build the image and `flyctl deploy` (Fly token in repo secrets).
Keep the whole PR pipeline **under a few minutes** (no pipeline-grade fixtures to drag). Branch-per-feature
+ one PR per feature (matches the pipeline's phase=commits+single-PR convention).

---

## §12 — Deployment: Fly.io vs Render (free-tier recommendation)

**Recommendation: Fly.io.** Rationale against the three candidates (this is ADR 0025's body):

| Factor | Fly.io | Render | Cloudflare Workers |
|---|---|---|---|
| Python runtime | Full CPython in a Docker image — asyncpg/SQLAlchemy/uvicorn run unmodified | Full CPython (native or Docker) — also fine | **Pyodide/WASM** — no asyncpg C-ext, no standard psycopg; would force an HTTP-driver rewrite (Neon serverless driver over HTTP) and reshape the stack. **Reject** (this is plan 848's risk). |
| Cold start | App can **scale-to-zero** on free allowances → its own cold start *on top of* Neon's. Mitigate with `min_machines_running=1` if free hours allow, or accept the wake. Fast machine boot. | Free web service **spins down after ~15 min idle** → notable first-request wake (tens of seconds reported). Heavier cold start than Fly. | N/A (always-warm edge) but unusable per row 1 |
| Read-only Neon from `main` | Standard outbound Postgres TLS to Neon pooler endpoint — works. Pin region near Neon's to cut latency. | Same — works. | Only via Neon's HTTP serverless driver — another reason it reshapes design |
| GH Actions CI/CD | `flyctl deploy` action, token secret — clean | Render deploy hook / blueprint — clean | Wrangler — but moot |
| Config-as-code | `fly.toml` + `Dockerfile` committed | `render.yaml` | `wrangler.toml` |
| Free-tier fit | Generous; Docker control; good Postgres-client story | Simple; the idle-spindown cold start is the main wart | Free but wrong runtime |

**Decision:** Fly.io for runtime control + the better cold-start story + Docker (which also makes the
image reproducible and the local/prod parity tight). **Render is the documented fallback** if Fly's free
allowances change — the app is a plain Dockerized ASGI service, so the switch is config-only (`render.yaml`
instead of `fly.toml`), no code change. **Cloudflare Workers rejected** for the Python-runtime reasons
above, which is exactly why ADR 0024 (design) can safely assume a normal CPython async stack.

**Cold-start + read-only-Neon interplay (the load-bearing deployment note):** two cold starts can stack
(platform scale-to-zero + Neon auto-suspend). Strategy: keep the **app** warm (Fly
`min_machines_running=1` within free hours) so only Neon's wake remains, front the DB with the **pooler
endpoint** + `pool_pre_ping`, fail a cold-DB request fast to **503 + `Retry-After`** rather than hanging,
and lean on **HTTP cache headers** so most reads never reach a cold DB at all (nightly-batch data caches
beautifully). Optionally a keep-warm ping. The app role is **read-only on `main`** (ADR 0005) — no writes
ever reach production from this repo.

---

## §13 — Phased commit plan (for the `recalls-api` repo)

Default convention: staged, signposted commits on one branch (`feature/api-scaffold`,
`feature/api-endpoints`, ...) each with **its own gates green** (`ruff check`, `ruff format --check`,
`pyright`, `pytest`), then **one PR per feature branch**. The user runs every command/migration/deploy;
these commits describe what Claude Code writes, not what it executes against live infra.

**Pre-work (in THIS pipeline repo, before the API repo exists):**
- C0a. File ADR 0024 (design) + de-reserve in `decisions/README.md`. *(1 commit)*
- C0b. File ADR 0025 (deployment) + de-reserve. *(1 commit)*
- C0c-dbt **[E]** (pipeline-repo, paired, `dbt build`-gated): add the **optional** `(source,
  source_recall_id)` composite index to `mart_recall_summary.sql` + its `_gold.yml` + a
  `gold_design_notes.md` note. **Only needed if the detail endpoint does NOT compute the
  `recall_event_id` md5 surrogate** (the recommended path hits the existing unique index — see §3 detail
  / §9 — and needs no upstream change; this commit is skippable in that case). *(1 commit)*
- C0c-ops **[U]** (operator-run, `psql`): provision the read-only API Postgres role + `GRANT SELECT` on
  the gold marts (+ `default_transaction_read_only = on`); owned by the pipeline repo (it owns DDL/roles,
  §9). *(1 operator DB step)*

**Branch `feature/api-scaffold`:**
- C1. Repo init: `pyproject.toml` (deps, ruff, pyright, pytest), `uv.lock`, `.envrc`/`.env.example`,
  README pointing at the pipeline gold docs, empty `src/recalls_api/` package + `__version__`. Gates green.
- C2. Settings (`settings.py`, pydantic-settings, fail-loud), `db.py` (async engine + pool + lifespan),
  `logging.py` (structlog + request middleware), `errors.py` (taxonomy + handlers), `main.py` app
  factory, `/health` + `/health/db`. First green `pytest` (unit: settings, error envelope).

**Branch `feature/api-recalls`:**
- C3. `pagination.py` (keyset cursor, pure, unit-tested) + `models/common.py` (`Page[T]`, enums,
  `FirmRef`). Unit tests + hypothesis cursor round-trip.
- C4. `GET /recalls` list: `queries/recalls.py` builder (pure, unit-tested for every filter combo),
  `models/recalls.py` (`RecallSummary`), router. Integration test against seeded gold.
- C5. `GET /recalls/{source}/{recall_id}` detail: `RecallDetail` with typed jsonb sub-models; 404 path.
  Integration tests (hit + miss + multi-firm rollup).

**Branch `feature/api-products`:**
- C6. `GET /products/search`: identifier paths (hin/model/upc) + FTS path (`websearch_to_tsquery` +
  `ts_rank_cd`), `ProductSearchHit`, "require one of q|hin|model|upc" 422. Seed includes a
  `search_vector` row so FTS runs for real. Integration: FTS hit/miss, HIN exact, recall-level-UPC note.

**Branch `feature/api-firms`:**
- C7. `GET /firms/{id}`: `FirmProfile` + per-source `SidecarAttributes` jsonb blocks, 404. Integration:
  a firm spanning two sources (cross-source rollup), `recalls_by_source` dict. **NOTE:** the three
  firm-sidecar names are being renamed to `firm_{usda,uscg,fda}_attributes` by the phase-7 branch (C19)
  before this repo is built — confirm the final mart column names against the live `mart_firm_profile.sql`
  at build time.

**Branch `feature/api-openapi-contract`:**
- C8. OpenAPI enrichment (summaries/descriptions/examples/`responses=`), `export_openapi.py`, committed
  `openapi.json`, contract tests (snapshot diff + response-schema conformance + optional schemathesis).

**Branch `feature/api-deploy`:**
- C9. `Dockerfile` (slim CPython 3.12 + uvicorn), `fly.toml` (+ `render.yaml` fallback), HTTP cache
  headers + slowapi rate limit, `Retry-After` on 503/429.
- C10. CI (`ci.yml`: ruff/pyright/pytest+postgres-service/openapi-check/pre-commit) and deploy
  (`deploy.yml`: flyctl). README run/deploy instructions. *(operator: create Fly app, set secrets,
  first deploy.)*

**Per-branch acceptance:** gates green; new endpoints covered by unit (builder/model) + integration
(seeded DB) tests; OpenAPI regenerated + committed; no test touches production Neon; the endpoint's
honest caveats are in its OpenAPI description.

---

## §14 — Open questions to settle in ADR 0024/0025 (pre-answers above; confirm at filing)

1. Detail-endpoint key: `(source, source_recall_id)` public id vs derived `recall_event_id` — plan
   recommends source-native, resolved by computing the `recall_event_id` md5 surrogate (no upstream
   change); the paired composite index (C0c-dbt) is the optional alternative.
2. CI test DB: Postgres service container (recommended) vs ephemeral Neon branch (parity option).
3. Keep-warm ping vs accept-cold-start — a free-tier-hours tradeoff to decide at deploy.
4. Whether any `fct_*` dashboard endpoints land in v1 (recommend: no, defer to Phase 9 need).
5. Exact Neon endpoint for the app role (pooled/PgBouncer recommended) + read-only role provisioning,
   owned by the pipeline repo.
