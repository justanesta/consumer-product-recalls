# Serving-layer gold-readiness plan

- **Status:** Active — drafted 2026-06-13; authored on `feature/pre-go-live-validation` the same day
  (groups 1–3: gold hardening, the `recalls_readonly` migration + grant mechanism, and the gold-contract
  ADR 0042). Awaiting the operator pass (apply migration 0034 as owner → activate `LOGIN PASSWORD` →
  `dbt build` → verify done-markers). Executable on the normal nightly `dbt build` cadence;
  **independent of the pipeline's own go-live**. Exactly one item (§2 **R1**, the read-only role) is a
  hard prerequisite for the `recalls-api` *deploy*; nothing here blocks the pipeline itself.
- **Type:** phase/feature plan (documentation_model.md type 4) — the bounded set of **gold/pipeline
  changes** that make the forthcoming `recalls-api` serving layer (a separate, open, read-only FastAPI
  repo) build and run smoothly.
- **Audience:** a Claude Code terminal executing in **this** pipeline repo.
- **Scope boundary.** The API repo owns **no** schema, migrations, or dbt — it only *reads* gold. So
  every change here is a pipeline-repo change: a dbt model / `config(indexes=[…])` / `post_hook`, an
  Alembic migration in `migrations/versions/`, or a `_gold.yml` / doc edit. This plan is the **single
  home** for "what gold/pipeline changes the API needs"; the *why our architecture is this way* lives
  in the cited ADRs (point at them, don't restate).
- **Provenance.** Derived from an audit of the gold marts at commit `39dcbda` (this branch,
  `feature/pre-go-live-validation`) against the API's needs. The fuller API-side audit + the
  authoritative per-mart schema contract live in the **API repo** at
  `../consumer-product-recalls-api/project_scope/build/` (esp. `01-ground-truth-gold-marts.md` and
  `07-gold-layer-recommendations.md`) — a cross-repo provenance pointer, not an execution dependency;
  this plan is self-contained for the operator.

---

## §0 — ADR touchpoints (reserve any new numbers in `decisions/README.md`, never here)

Per documentation_model.md, plan docs **point at** ADRs and never guess numbers. Decision rationale is
single-homed in the ADR; this plan only records *what to do*.

| Item | Relates to existing ADR | New ADR / amendment needed? |
|---|---|---|
| **R1** read-only role | extends the restricted-role posture of migration `0033` (ADR 0013 `*_rejected`/mutation-guard); satisfies the read-only intent of ADR 0024 §1 / ADR 0025 | No new ADR — it's a migration. Mention it in the next ADR-0013-adjacent amendment if you track role posture there. |
| **R2 / R3 / R7** gold indexes + stats | ADR 0038 §6 (first-principles indexing, declared in dbt config; ANALYZE post-rebuild) | No |
| **R4** `gold_meta` | ADR 0038 §2 (gold layer modeling) | No |
| **R5** sidecar rename | ADR 0036 (cross-source canonical naming) — the rename finishes aligning the **gold output** with the canonical policy the silver tables already follow | No new ADR; it completes the C19 intent. |
| **§1** gold-as-contract | ADR 0024 (the API consumes these marts) | **Filed as [ADR 0042](../documentation/decisions/0042-gold-serving-marts-published-read-contract.md)** (gold serving marts are a published read contract), reserved in `decisions/README.md` 2026-06-13. |
| **C1** `pg_trgm` trigram | **reverses** ADR 0037 ("pg_trgm/fuzzystrmatch deliberately NOT enabled") | **Yes, if pursued** — an inline amendment to ADR 0037 + operator sign-off. Default is *don't*. |

---

## §1 — Gold is now a consumed contract (read this first)

The serving marts (`mart_recall_summary`, `mart_product_search`, `mart_firm_profile`) are about to
become a **public read interface**. Several of their properties are **load-bearing for the API** and
must not be changed silently — coordinate with the API repo (re-freeze its `openapi.json`) before
touching any of them:

- **`recall_event_id = md5(source || '|' || source_recall_id)`** — the API computes this from URL path
  params to hit `UNIQUE(recall_event_id)`. Changing the recipe (or the per-source `source_recall_id`
  business key) silently breaks every detail-endpoint URL. (Confirmed for all five sources in
  `dbt/models/silver/recall_event.sql`.)
- **`recall_product_id` stability** — already migrated to a stable `(event, ordinal)` key (this commit);
  the API uses it as an opaque keyset cursor anchor. Keep it stable across rebuilds.
- **`source` enum is closed + UPPERCASE** (`CPSC FDA USDA NHTSA USCG`). The API uppercases path params
  before the md5; lower/mixed-case storage would break lookups.
- **The serving-mart column set + the sidecar output names** (after **R5**) — renames/drops are
  API-breaking once the API's `openapi.json` is frozen.
- **Contract facts the API models around — do NOT "normalize" these without telling the API:**
  `classification` is **source-native** (FDA/USDA `Class I/II/III`, USCG `H/L/M/S`, CPSC/NHTSA NULL),
  not a unified enum; `risk_level` is USDA-only; `is_active` is **tri-state** (NULL for CPSC/NHTSA);
  `announced_at` is nullable by design; `distribution_states` is a **scalar** text column distinct from
  the `distribution_state_codes` `text[]` array. (All single-homed in the API's `01-ground-truth`.)

**Filed:** [ADR 0042 — Gold serving marts are a published read contract](../documentation/decisions/0042-gold-serving-marts-published-read-contract.md)
declares the gold serving marts a versioned read contract and lists these invariants, so a future
pipeline change can't quietly break the API (reserved in `decisions/README.md` 2026-06-13).

---

## §2 — Work items (by priority)

Mechanisms (per ADR 0038 §6): an **index on a `mart_*` table** goes in that model's
`config(indexes=[…])` (re-created every `dbt build`); a **`DESC` / expression / opclass index** dbt's
column-list can't express goes in a `post_hook` (the `firm_fda_attributes((firm_fei_num::text))`
precedent in `index_audit.md`); a **role/grant/DB posture** is an Alembic migration (operator-run);
a **new column or meta table** is a dbt model change.

### R1 — New read-only role for the API · **REQUIRED (the only API-deploy blocker)**

**What.** The existing `recalls_app` role (migration `0033_recalls_app_role_posture.py`) is the
pipeline's **READ+WRITE** runtime role (`GRANT SELECT, INSERT, UPDATE ON ALL TABLES`, plus `ALTER
DEFAULT PRIVILEGES` so future tables inherit write). An open, no-auth, public API must **not** connect
as it. Provision a dedicated **`recalls_readonly`** role: `GRANT SELECT` only +
`default_transaction_read_only = on`, mirroring 0033's NOLOGIN-SQL-shell pattern (a SQL-created role is
**not** added to `neon_superuser`, whose `pg_write_all_data` would re-grant write — see 0033's header).

**Exact change.** New migration `migrations/versions/0034_recalls_readonly_role.py` (confirm `0034` is
the next free revision against `migrations/versions/`):

```python
r"""recalls_readonly — dedicated read-only role for the public serving API (recalls-api).

Separate from recalls_app (0033, the pipeline READ+WRITE runtime role). The open, no-auth API connects
as this role: SELECT only, plus a session-level default_transaction_read_only belt so even a SELECT-able
function or a planner surprise cannot mutate. Created as a NOLOGIN SQL shell for the SAME reason as 0033
(a SQL-created role is NOT added to neon_superuser, whose pg_write_all_data would re-grant write; no
password literal is committed). Operator activates once, out-of-band:
    ALTER ROLE recalls_readonly LOGIN PASSWORD '<strong pw>';
Then expose its connection string to the API as NEON_DATABASE_URL_RO (SecretStr).

Runs as the OWNER (operator-run, never in CI). Idempotent on the clean path.

Revision ID: 0034
Revises: 0033
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0034"
down_revision: str | None = "0033"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create as a NOLOGIN shell if absent; fail loudly on a pre-existing *dirty* role (any admin
    #    attribute or neon_superuser membership) — a non-superuser owner cannot restrict it, so the fix
    #    is delete-in-Neon-console + re-run, landing on the clean CREATE path (mirrors 0033 step 1).
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_readonly') THEN
                IF EXISTS (
                    SELECT 1 FROM pg_roles
                    WHERE rolname = 'recalls_readonly'
                      AND (rolsuper OR rolcreatedb OR rolcreaterole
                           OR rolreplication OR rolbypassrls)
                ) OR EXISTS (
                    SELECT 1 FROM pg_auth_members am
                    JOIN pg_roles g ON g.oid = am.roleid
                    JOIN pg_roles m ON m.oid = am.member
                    WHERE m.rolname = 'recalls_readonly' AND g.rolname = 'neon_superuser'
                ) THEN
                    RAISE EXCEPTION USING MESSAGE =
                        'recalls_readonly exists with elevated privileges; a non-superuser owner '
                        || 'cannot fully restrict it. Delete the role in the Neon console and '
                        || 're-run alembic upgrade head to recreate it clean via SQL.';
                END IF;
            ELSE
                CREATE ROLE recalls_readonly NOLOGIN;
            END IF;
        END $$;
        """
    )

    # 2. Read-only grants: SELECT on all CURRENT tables. USAGE on schema (needed to resolve objects).
    #    NO INSERT/UPDATE/DELETE/TRUNCATE, NO sequence privileges (read-only never advances a seq).
    op.execute("GRANT USAGE ON SCHEMA public TO recalls_readonly;")
    op.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO recalls_readonly;")

    # 3. FUTURE owner-created tables inherit SELECT (a new mart is readable without re-granting).
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO recalls_readonly;"
    )

    # 4. Belt-and-braces: force every session opened by this role to read-only.
    op.execute("ALTER ROLE recalls_readonly SET default_transaction_read_only = on;")


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_readonly') THEN
                ALTER ROLE recalls_readonly RESET default_transaction_read_only;
                ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    REVOKE SELECT ON TABLES FROM recalls_readonly;
                REVOKE ALL ON ALL TABLES IN SCHEMA public FROM recalls_readonly;
                REVOKE ALL ON SCHEMA public FROM recalls_readonly;
                DROP ROLE recalls_readonly;
            END IF;
        END $$;
        """
    )
```

Operator activation (mirrors `operations.md` "Restricted app role"; Neon needs a plaintext password):

```bash
alembic upgrade head
# then, out-of-band (do NOT commit the password):
psql "$NEON_OWNER_URL" -c "ALTER ROLE recalls_readonly LOGIN PASSWORD '<strong pw>';"
# hand the API the resulting connection string as NEON_DATABASE_URL_RO
```

**Grant-scope choice (operator).** `GRANT SELECT ON ALL TABLES` grants read on **all** public tables
(bronze/silver/audit included). To restrict the API to gold only, narrow step 2 to an explicit list —
`mart_recall_summary, mart_product_search, mart_firm_profile` (+ any `fct_*`/`gold_meta` the API later
reads) — and add a matching `ALTER DEFAULT PRIVILEGES` on those. Default-broad is fine for read-only;
gold-only is stricter. **Operator's call** (see §8).

**Done-marker.** `\du recalls_readonly` shows `NOLOGIN`→`LOGIN` after activation, no admin attributes,
not in `neon_superuser`; `SET ROLE recalls_readonly; SELECT 1` works and any `INSERT`/`UPDATE` raises
`cannot execute … in a read-only transaction`.

### R2 — `(published_at DESC, recall_event_id)` index on `mart_recall_summary` · **Recommended**

**What / why.** The headline `GET /recalls` list orders `(published_at DESC, recall_event_id)`. The only
`published_at`-bearing index is the composite `(source, published_at)`, usable for ordering **only** with
a leading `source` equality (leftmost-prefix rule). An **unfiltered** `/recalls` falls to a full `Sort`
over the whole mart. Add a descending total-order index matching the keyset tuple.

**Exact change.** dbt's column-list config can't express `DESC`, so use a `post_hook` in
`dbt/models/gold/mart_recall_summary.sql` (keep the existing `indexes=[…]` block as-is):

```python
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
      {'columns': ['source', 'published_at']},
      {'columns': ['is_active']},
      {'columns': ['classification']},
    ],
    post_hook="create index if not exists {{ this.name }}_published_at_desc_evt
               on {{ this }} (published_at desc, recall_event_id)"
) }}
```

`recall_event_id` is UNIQUE, so the pair is a total order → the keyset seek becomes a pure index range
scan, no `Sort`. dbt re-creates it each rebuild (table is freshly built, no `CONCURRENTLY` needed).

**Done-marker.** `EXPLAIN (ANALYZE, BUFFERS)` of `… ORDER BY published_at DESC, recall_event_id LIMIT 25`
shows an `Index Scan` on the new index, no `Sort` node.

### R3 — GIN index on `mart_product_search.recall_product_upcs` · **Recommended**

**What / why.** UPC search uses **recall-level jsonb containment** (`recall_product_upcs @> :upc`), not
the per-product `upc` column (NULL for every row today). That jsonb column has no index → a `Seq Scan`
with per-row containment recheck. Add the standard `jsonb_ops` GIN — dbt **can** express it as a
column-list (like the existing `search_vector` GIN), no post_hook needed. Edit
`dbt/models/gold/mart_product_search.sql`:

```python
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_product_id'], 'unique': True},
      {'columns': ['recall_event_id']},
      {'columns': ['hin']},
      {'columns': ['model']},
      {'columns': ['upc']},
      {'columns': ['search_vector'], 'type': 'gin'},
      {'columns': ['recall_product_upcs'], 'type': 'gin'},   -- NEW: recall-level UPC containment
    ]
) }}
```

(For a smaller containment-only index, a `post_hook` with `USING gin (recall_product_upcs jsonb_path_ops)`
is an option; default `jsonb_ops` is the safe choice.)

**Done-marker.** `EXPLAIN` of `… WHERE recall_product_upcs @> '["0123456789012"]'::jsonb` shows a
`Bitmap Index Scan` on the new GIN, not a Seq Scan.

### R4 — `gold_meta.rebuilt_at` for deterministic ETag / Last-Modified · **Recommended**

**What / why.** The API keys `Cache-Control`/`ETag`/`Last-Modified` off the nightly ~03:00 UTC rebuild,
but gold exposes **no queryable "when was gold last rebuilt" signal** (`last_seen_at` is per-recall
observation time, not a layer-wide build stamp). Emit a one-row meta table. New model
`dbt/models/gold/gold_meta.sql`:

```python
{{ config(materialized='table') }}

-- gold_meta — one row, the gold-layer rebuild stamp. Set to the dbt run start time so every mart built
-- in the same `dbt build` shares one deterministic rebuilt_at. Read by the serving API (recalls-api) to
-- compute a layer-wide ETag / Last-Modified for conditional GET / 304.

select
    '{{ run_started_at.astimezone(modules.pytz.UTC).isoformat() }}'::timestamptz as rebuilt_at,
    '{{ var("gold_schema_version", "1") }}'::text                               as schema_version
```

`run_started_at` is identical across all models in one `dbt build`, so every mart in a run shares one
`rebuilt_at`. **Resolved during build (2026-06-13):** the snippet's `astimezone(modules.pytz.UTC)` failed
compilation (*"tzinfo argument must be … not 'Undefined'"*) — **not** a pytz/version availability issue
but a casing bug: dbt-core 1.11 exposes `modules.pytz` as a curated dict of `pytz.__all__`, which has
lowercase `utc` but not `UTC`, so `modules.pytz.UTC` is Undefined (`modules.pytz.utc` would resolve).
Since `run_started_at` is already a tz-aware **UTC** datetime (`dbt/tracking.py`: `datetime.now(tz=pytz.utc)`),
the conversion is redundant, so the model renders it directly: `'{{ run_started_at }}'::timestamptz` (its
`str()` carries the `+00:00` offset → parsed as UTC independent of session TimeZone). No dbt-core change
needed; the fallback the plan named is the as-built.
**Add `gold_meta` to R1's grant set** (covered by broad `GRANT SELECT ON ALL TABLES`; include it
explicitly if R1 is scoped gold-only).

**Done-marker.** `SELECT rebuilt_at, schema_version FROM gold_meta` returns exactly one row whose
`rebuilt_at` advances after a `dbt build`.

### R5 — Rename sidecar OUTPUT columns to `firm_{usda,uscg,fda}_attributes` · **Recommended (do pre-go-live, before the API's first `openapi.json` freeze — API-BREAKING after)**

**What / why.** `mart_firm_profile`'s sidecar outputs are `establishment_attributes` (= **USDA**),
`manufacturer_attributes` (= **USCG** boat MIC), `fda_attributes` (= **FDA**). The
`establishment`/`manufacturer` names are misleading (they read generic but are source-specific). Rename
them to source-tagged names matching the already-renamed silver source tables and the C19 intent
(ADR 0036). **This is a dbt model edit, NOT an Alembic migration** (gold marts are dbt
`materialized='table'`; the `migrations/` are bronze-layer).

**Verified zero downstream dbt breakage.** The only `mart_firm_profile` consumer in the DAG,
`dbt/models/gold/fct_recalls_by_firm.sql`, selects only `firm_id, canonical_name, total_recalls,
active_recalls, distinct_products, first_recall_at, last_recall_at` — none of the three sidecar columns.

**Exact change — 2 spots in `dbt/models/gold/mart_firm_profile.sql`:** the three `as <name>` aliases in
the `firm_attrs` CTE, and the three `fa.<name>` columns in the final `select`:

```sql
-- (1) firm_attrs CTE aliases:
        jsonb_agg(est_json order by establishment_id) filter (...) as firm_usda_attributes,
        jsonb_agg(mfr_json order by mic)              filter (...) as firm_uscg_attributes,
        jsonb_agg(fda_json order by firm_fei_num)     filter (...) as firm_fda_attributes,
-- (2) final select:
    fa.firm_usda_attributes,
    fa.firm_uscg_attributes,
    fa.firm_fda_attributes
```

Then update `dbt/models/gold/_gold.yml` if it names these columns (descriptions/tests), and
`dbt build --select mart_firm_profile+`.

**Timing.** Cheap **now** (the API repo is fresh, no clients); a contract break **after** the API ships.
Coordinate with the API build session so it targets the new names from day one. If the API has already
frozen `openapi.json`, **decline** — not worth a contract break for cosmetics.

**Done-marker.** `mart_firm_profile` columns include `firm_usda_attributes`/`firm_uscg_attributes`/
`firm_fda_attributes`; `dbt build --select mart_firm_profile+` is green; `fct_recalls_by_firm` unaffected.

### R6 — Pipeline doc-hygiene fixes · **Recommended (hygiene)**

Doc-only (no code/index change). They matter because the API's OpenAPI caveat copy and any future
`/stats/*` work read these as truth, and they are stale vs the SQL:

| Item | Stale text | Authoritative reality | Fix |
|---|---|---|---|
| **a. `fct_units_recalled`** | `_gold.yml` description: "basis-aware: per_product rows summed, total_all_products max'd" | `fct_units_recalled.sql` uses `max(quantity_value)`, basis-agnostic (per the model's own dated comment) | Update the `_gold.yml` description to: units = `max(quantity_value)` per recall, basis-agnostic. |
| **b. `dim_date` spine year** | `_gold.yml` `dim_date` description: "1960-01-01.." | SQL spine starts **1940-01-01** (matches the `assert_recall_event_date_sanity` ERROR floor per `gold_design_notes.md`) | `_gold.yml`: `1960` → `1940`. |
| **c. `fct_recalls_by_country` missing** | The serving-layer/API deferred-`/stats/*` inventory lists ~7 `fct_*` and omits it | The model exists (FDA+USDA, derived `'US'` cell, per-country inflation) | Add `fct_recalls_by_country` (with caveats) to the deferred inventory; treat the API repo's `01` fct table as the authoritative list. |

**Done-marker.** The three doc strings match the SQL.

### R7 — Confirm ANALYZE / stats freshness covers the serving marts + new indexes · **Recommended**

**What / why.** ADR 0038 §6 / `gold_design_notes.md` note `ANALYZE` post_hooks on the firm-join-chain
tables so the planner has fresh stats right after rebuild. The API's latency depends on the planner
choosing the new R2/R3 indexes. Confirm `mart_recall_summary`, `mart_product_search`,
`mart_firm_profile` are `ANALYZE`d after each rebuild (a post_hook or a project on-run-end hook); if a
serving mart isn't covered, add `post_hook="analyze {{ this }}"` (ordered after the index post_hooks).

**Done-marker.** `pg_stat_user_tables.last_analyze` for the three marts is ≥ the last `dbt build` time;
`EXPLAIN` for the R2/R3 queries uses the new indexes (not a Seq Scan from stale stats).

### O1 — Coalesce `product_names`/`models`/`hins` to `'[]'::jsonb` · **Optional (consistency)**

In `mart_recall_summary`, `firms` is coalesced to `'[]'::jsonb` but these three are NULL-when-empty. The
API already defaults them to `[]`, so this is pure source-side consistency. Wrap the three rollup selects
in the final `select`:

```sql
    coalesce(pr.product_names, '[]'::jsonb) as product_names,
    coalesce(pr.models,        '[]'::jsonb) as models,
    coalesce(pr.hins,          '[]'::jsonb) as hins,
```

Safe — `product_count` already carries the "had products at all?" signal, so the NULL is noise. Skip if
you deliberately want the NULL-vs-`[]` distinction.

### O2 — Prune (or keep) the all-null `upc` btree on `mart_product_search` · **Optional**

The `{'columns': ['upc']}` btree indexes a column that is `NULL` for every row today (product-grain UPC
unimplemented), so it serves nothing and costs build time/storage. **Either** drop it from the
`indexes=[…]` block, **or** keep it as a forward-looking placeholder for when product-grain UPC lands
(a documented future enrichment). Low stakes; operator's call. (R3's recall-level GIN is the real UPC
path regardless.)

### O3 — Hand per-mart row counts to the API team · **Info (no code change)**

The API's pagination/cache sizing currently has no real row counts (the plan's "130k" is unverified).
Once R1's role exists, run `SELECT count(*)` on the three serving marts and share the numbers; do **not**
hard-code any figure in the marts.

### C1 — Trigram/expression index for `?firm=` ILIKE substring · **Conditional (default OFF)**

A `?firm=<substring>` ILIKE `'%…%'` is non-sargable and unindexed; the natural fix (`pg_trgm` trigram
GIN) is unavailable because **pg_trgm is disabled (ADR 0037)**. **Default: do nothing** — v1 ships
`?firm=` as documented best-effort substring (and steers exact/prefix lookups to `GET /firms/{id}` or
the indexed `normalized_name` prefix). Only if `pg_stat_statements` later shows `?firm=` is a real hot
path: an operator migration `CREATE EXTENSION IF NOT EXISTS pg_trgm;` (**reverses ADR 0037 — amend it
first**) + a dbt `post_hook` `using gin (primary_firm_name gin_trgm_ops)`. Re-opens the "do we offer
fuzzy product search?" question the project closed, so gate behind an ADR amendment + sign-off.

### D1 — `(source, source_recall_id)` composite index (plan C0c-dbt) · **DECLINE**

The detail endpoint computes `recall_event_id = md5(f"{SOURCE_UPPER}|{recall_id}")` and hits the existing
`UNIQUE(recall_event_id)` — O(1), no new index. The composite would index a `WHERE source=? AND
source_recall_id=?` query the API never issues. **Do not add it** to the gold mart. (If one already
exists on silver `recall_event` for the pipeline's own joins, leave it; this only declines adding it to
gold.) Recorded so it isn't re-litigated.

### N1 — FTS rank-keyset stability · **API-side note (no mart change)**

`ts_rank_cd` is query-time, not stored, and the GIN serves the `@@` match not the sort. **Do not**
materialize a per-document rank column (rank is query-dependent). The API pages the matched set with
`(rank DESC, recall_product_id)` keyset and uses `recall_product_id` (stable) as the tiebreaker;
`gold_meta.rebuilt_at` (R4) can invalidate cursors across a rebuild if strict stability is ever needed.
Recorded for completeness; nothing to do in the pipeline.

---

## §3 — Consolidated file touch-list

| File | Change | Item |
|---|---|---|
| `migrations/versions/0034_recalls_readonly_role.py` | **new** migration (read-only role) | R1 |
| `dbt/models/gold/mart_recall_summary.sql` | add `post_hook` `(published_at desc, recall_event_id)` index; (O1) coalesce 3 jsonb arrays | R2, O1 |
| `dbt/models/gold/mart_product_search.sql` | add `recall_product_upcs` GIN to `indexes`; (O2) optionally drop the `upc` btree | R3, O2 |
| `dbt/models/gold/gold_meta.sql` | **new** one-row meta model (`rebuilt_at`, `schema_version`) | R4 |
| `dbt/models/gold/mart_firm_profile.sql` | rename 3 sidecar aliases + final-select columns | R5 |
| `dbt/models/gold/_gold.yml` | sidecar column rename (R5); fct_units_recalled + dim_date description fixes (R6) ; document `gold_meta` (R4) | R4, R5, R6 |
| serving-layer/API deferred-`/stats/*` inventory doc | add `fct_recalls_by_country` | R6c |
| `dbt/models/gold/*` ANALYZE coverage (model `post_hook` or `on-run-end`) | confirm/extend for the 3 serving marts | R7 |
| `documentation/decisions/README.md` | **done** — reserved 0042 for the §1 gold-contract ADR (C1 pg_trgm amendment still unfiled by default) | §1, C1 |

---

## §4 — In-chunk sequencing & coordination

- **R1 is the only API-deploy gate** — it can land any time (independent of the gold rebuild); the API
  cannot go live without it but can be *built/tested* against a throwaway DB without it.
- **R5 has a hard coordination window:** it must land **before** the API freezes its first
  `openapi.json` (the API's contract test). Flag it to the API build session immediately; if missed,
  decline R5.
- **R2, R3, R4, R6, R7, O1, O2** are non-blocking and ride the **normal nightly `dbt build`** — group
  them into one gold-hardening change.
- **C1, D1, N1** require no work now (C1 is gated on measured load + an ADR amendment; D1/N1 are
  decisions recorded).
- **Suggested grouping** (one PR per branch, per the project's convention): branch
  `feat/api-readonly-role` (R1, alone — it's a migration + operator activation); branch
  `feat/gold-api-hardening` (R2+R3+R4+O1+O2+R7 — dbt model/config changes, one `dbt build`);
  branch `chore/gold-doc-hygiene` (R5 rename **+** R6 doc fixes, since both touch `_gold.yml` and R5 is
  coordination-sensitive — or split R5 onto its own branch if the API freeze timing demands it).

---

## §5 — Checklist

Flip `[x]` and append `Done YYYY-MM-DD (PR #N)` **once, at merge** (documentation_model.md graduation
rule). Branch-granular sub-tasks belong in the draft-PR body, not here.

- [ ] **R1** `0034_recalls_readonly_role.py` written, `alembic upgrade head` clean, operator activated `LOGIN PASSWORD` out-of-band, `NEON_DATABASE_URL_RO` handed to the API
- [ ] **R2** `mart_recall_summary` `(published_at desc, recall_event_id)` post_hook index; `EXPLAIN` shows Index Scan no Sort
- [ ] **R3** `mart_product_search` `recall_product_upcs` GIN; `EXPLAIN` of `@>` shows Bitmap Index Scan
- [ ] **R4** `gold_meta` model built; one row; `rebuilt_at` advances per build; in the RO grant set
- [ ] **R5** sidecar columns renamed (2 spots + `_gold.yml`); `dbt build --select mart_firm_profile+` green; **coordinated with API openapi freeze**
- [ ] **R6** `_gold.yml` fct_units_recalled + dim_date descriptions fixed; `fct_recalls_by_country` added to deferred-`/stats/*` inventory
- [ ] **R7** ANALYZE confirmed/added for the 3 serving marts; planner uses R2/R3 indexes
- [ ] **O1** (optional) coalesce product_names/models/hins to `'[]'::jsonb`
- [ ] **O2** (optional) prune or keep the all-null `upc` btree (decision recorded)
- [ ] **O3** row counts shared with the API team
- [ ] **C1 / D1 / N1** — no action (recorded decisions); revisit C1 only under measured `?firm=` load + ADR 0037 amendment

---

## §6 — Open items to confirm with the operator (mirror the API's "MUST re-verify")

1. **R1 role:** exact name (`recalls_readonly` proposed); grant scope **gold-only vs all tables**;
   whether `default_transaction_read_only` is set at the role (proposed) or per-session.
2. **Connection endpoint:** **pooled (PgBouncer `-pooler`) vs direct** Neon endpoint for the API's DSN.
   A small held pool (the API uses `pool_size~5`) generally wants the **direct** endpoint; pooled is
   for many short-lived connections. (Affects whether `statement_timeout`/server settings persist.)
3. **Env-var name** the API reads: **`NEON_DATABASE_URL_RO`** proposed (parallels the pipeline's
   `NEON_DATABASE_URL`, `SecretStr`, fail-loud at boot).
4. **R4:** ~~confirm dbt `run_started_at` / `modules.pytz` availability~~ — **resolved 2026-06-13:**
   both available (dbt-core 1.11); the snippet's bug was casing — `modules.pytz` exposes `utc` not `UTC`.
   `run_started_at` is tz-aware UTC, so the model renders it directly (see R4). No dbt-core change.
5. **R5 timing:** confirm the API's `openapi.json` is **not yet frozen** before renaming.
6. **O3:** per-mart row counts to replace the unverified "130k" sizing assumption.

---

## §7 — Pointers (single-home; do not restate)

- **Why the gold layer is shaped this way:** ADR 0038 (modeling + indexing), ADR 0036 (canonical
  naming), ADR 0037 (no pg_trgm / fuzzy upstream), ADR 0013 + migration `0033` (restricted-role
  posture), ADR 0024 / ADR 0025 (the API design + deploy that consume gold).
- **Per-mart authoritative schema + the full API-side audit:** API repo
  `project_scope/build/01-ground-truth-gold-marts.md` and `07-gold-layer-recommendations.md`.
- **Master index:** add a one-line pointer to this plan from `project_scope/implementation_plan.md`
  (its job per documentation_model type 3); flip this plan's `Status:` to `Active` when work starts on a
  branch and to `Complete (PR #N)` at merge.
