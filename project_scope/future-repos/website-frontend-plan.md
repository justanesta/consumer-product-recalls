# Website / frontend — greenfield plan (Phase 9, separate repo)

> **⚠️ API-contract reconciliation (2026-06-19, post API-side audit).** The framework/architecture strategy below (Astro + Cloudflare Pages + hybrid build-time/live + OpenAPI-generated client) is **current and validated** against the now-deployed API and the user's Cloudflare infra — no change. But several **API-contract specifics in §5 are stale or ahead of what the API actually exposes**; they are corrected inline below and tagged **[API-2026-06-19]**. The authoritative live contract is the API repo's [`project_scope/future-repos/frontend-api-handoff.md`](../../../consumer-product-recalls-api/project_scope/future-repos/frontend-api-handoff.md) + its `openapi.json`; per-source field provenance is in that repo's [`documentation/data_contract.md`](../../../consumer-product-recalls-api/documentation/data_contract.md). **Headlines:** the `/stats/*` + `fct_*` dashboard endpoints are **not built** (dashboards + landing charts are blocked); there is **no `?firm_id=` filter** on `/recalls`; the `/recalls` filter params differ from §5.2 (`published_after`/`is_active`/keyset `cursor`, not `date_from`/`status`/`page`); FDA `classification` is `1/2/3/NC` (not `Class I/II/III`); and the lifecycle/edit observability fields plus the per-product `upc` field were **pruned/dropped**. CORS is **open (`*`, GET-only)**, so browser fetch works with no proxy.

> **Status:** Draft scope for a **future, separate repository** (the consumer-facing website). Not built
> in the pipeline repo. This is a Claude-Code-ready plan: drop it into the new repo as `PLAN.md` and
> work the phased commit ladder at the bottom.
>
> **Owning context:** Phase 9 of the pipeline (`project_scope/implementation_plan.md` lines 873–885).
> Consumes the **Phase 8 FastAPI** (`GET /recalls`, `/recalls/{source}/{recall_id}`, `/products/search`,
> `/firms/{id}`) and, where the build-time path wins, reads the OpenAPI spec the API publishes at
> `/openapi.json`. The pipeline repo's gold marts (`mart_*` / `fct_*`) are the upstream data; this repo
> never touches Postgres directly — it goes through the API.
>
> **Single-home note (doc model):** this plan lives in the *pipeline* repo only as the scoping
> artifact. Once the frontend repo exists, its own `README.md` / `PLAN.md` own the live status; this
> file is the frozen scope, the way `project_vision_and_constraints.md` is for the pipeline.

---

## 0. The one decision that feeds back to the backend (read this first)

`documentation/gold_design_notes.md` §"Deferred: a dimensional star schema" says the **website chart
inventory is the gating question** that decides whether the backend builds a Kimball star (`dim_*`) or
keeps extending the `fct_*` aggregate marts. This plan answers that question by enumerating the v1
inventory (§5) and stating the verdict here so the backend can stop deferring it.

### Verdict for the backend: **No star. The existing `fct_*` aggregate marts are sufficient.**

Every v1 chart in §5 maps **one chart ← one `fct_*` (or `mart_*`) read** through an API endpoint. The
inventory is a **fixed, known, curated set** — not a BI tool, not a semantic layer, not user-driven
cross-dimensional pivoting. That is exactly the gold-design-notes branch that concludes "**No star**":

- API-fed with a fixed chart set → `fct_*` already *is* the dashboard layer. ✓ (this plan)
- A missing chart later → add one `fct_*` view + one endpoint, not a re-model.

**What the backend should do instead (feed this into ADR 0024):**

1. **Build `dim_date` regardless** — already decided (`implementation_plan.md` line 524; pre-Phase-8,
   no-regret). The frontend's time-axis charts (§5: monthly-trend, by-week/month/year) all benefit, but
   none *require* the star.
2. **Add exactly two small `fct_*`/mart fields the inventory needs that don't exist yet** (§5.6):
   - a **recall-detail timeline** payload (the lifecycle + edit-history already live in
     `mart_recall_summary`; the API just needs to project them) — no new model.
   - an **`/stats/overview`** endpoint backed by a trivial `fct_recalls_overview` (single-row KPIs:
     total recalls, distinct firms, sources, last-updated) **or** computed in the API from existing
     `fct_*`. One small view at most.
3. **Do not build `dim_firm` / `fct_recall_event` / role-played dims for v1.** Re-open the star only if
   a *future* version adds a self-serve pivot UI or a Metabase/Superset embed. Record that trigger in
   ADR 0024 and leave `dim_` reserved (ADR 0038 §2).

This verdict is a recommendation to ADR 0024 (serving-layer API design), which `implementation_plan.md`
line 523 and ADR 0038 §1 route the star-vs-`fct_*` decision into. The frontend does not need the star;
the API contract should be designed against `fct_*` + the three `mart_*`.

> **[API-2026-06-19] Status of the `/stats/*` surface this verdict asks for:** the API was **built and deployed** (the four entity endpoints + `/recalls/search` + health), but the **`/stats/*` + `fct_*` dashboard family was NOT included** — it is still deferred (ADR 0024 not yet ratified). **Consequence:** the landing charts (§5.1) and the entire dashboards page (§5.5) are **blocked** until either (a) the `/stats/*` endpoints are built, or (b) the *pipeline* exports the `fct_*` aggregates to static JSON for a build-time bake (the §9 fallback; the website still never queries Postgres directly). The entity/search pages (§5.2–5.4, §5.6) do **not** depend on this and are buildable now. Sequence the build so dashboards come after this decision lands.

---

## 1. Constraints this plan optimizes for

From `project_scope/project_vision_and_constraints.md` (frozen vision) and the project's portfolio goal
(breadth over polish, near-zero cost, demonstrable DE/SWE skill):

| Constraint | Implication for the stack |
|---|---|
| **Near-zero cost, free tier only** | Must deploy free and stay free at portfolio traffic. Rules out anything that needs a paid runtime. |
| **Backend is Python + Postgres + FastAPI** | The frontend is a *separate* concern. Server-side rendering against the DB is the backend's job, not the website's. The website is a presentation client. |
| **Solo data engineer, not a frontend specialist** | Minimize framework ceremony (no Redux, no heavy build config, no SSR server to operate). Favor a static-first model. |
| **Portfolio: must look credible + show data-viz skill** | Charting ecosystem and "wow per line of code" matter. The site is itself a deliverable judged by recruiters. |
| **Data is mostly slow-moving (daily/weekly cron)** | A build-time data pull is viable for dashboards; the live API is only needed for interactive search/browse. This is the key architectural lever (§3). |

---

## 2. Framework evaluation (the realistic four)

Evaluated against: **cost**, **build/host model** (static vs SSR), **data-fetch pattern** (call the live
FastAPI vs build-time pull from gold-via-API), **charting ecosystem**, and **frontend ceremony for a
solo DE**.

### 2a. Next.js + Vercel (the default the user is considering)

- **Cost:** Vercel Hobby is free *for personal use* but has a non-commercial license clause and
  metered serverless-function / bandwidth limits; a portfolio site is fine but you carry the
  cold-start + quota model and a vendor you'd have to migrate off if it ever went commercial.
- **Host model:** SSR/ISR-first. You *can* do static export (`output: 'export'`) but then you've
  bought Next's weight (App Router, RSC mental model, route handlers) to ship a static site —
  paying ceremony for capability you discarded.
- **Data-fetch:** Strong at both live SSR-fetch and build-time `generateStaticParams`. Overkill here.
- **Charting:** Recharts / visx / Tremor — excellent React ecosystem.
- **Ceremony:** **Highest** of the four for a solo DE. App Router + RSC + client/server boundary is a
  lot of concepts to carry for a data-viz portfolio site. This is the strongest *frontend* framework
  and the **weakest fit** for "a data engineer who wants charts on a free static host."

### 2b. Astro (islands)

- **Cost:** Static output → host **free anywhere** (Cloudflare Pages, Netlify, GitHub Pages). No vendor lock.
- **Host model:** Static-first, ships zero JS by default, hydrates only interactive "islands."
  Excellent for a mostly-content site (landing, methodology, dashboards) with a few interactive
  widgets (search, filters).
- **Data-fetch:** First-class **build-time fetch** (top-of-file `await fetch(API)`), and islands can
  hit the live API at runtime. Maps perfectly to the hybrid in §3.
- **Charting:** Framework-agnostic — drop a Svelte/React/vanilla island, or use D3/Observable Plot/
  Chart.js directly. Slightly less "batteries-included" than a single-framework chart lib.
- **Ceremony:** **Low.** `.astro` files are HTML-with-frontmatter; you reach for a JS framework only
  inside an island. Comfortable for a backend-leaning developer.

### 2c. SvelteKit

- **Cost:** With `adapter-static` → free static host anywhere. With the Cloudflare/Vercel adapters →
  SSR on a free tier.
- **Host model:** Flexible (static or SSR by adapter). Svelte is the least-boilerplate component model.
- **Data-fetch:** `load()` functions do build-time (prerender) or request-time fetch cleanly.
- **Charting:** LayerCake (Svelte-native, D3-based) is excellent and portfolio-flattering; smaller
  ecosystem than React's.
- **Ceremony:** **Low-medium.** Less than Next, a bit more than Astro for a content-heavy site
  (you're in a JS-framework mindset for every page, not just the interactive ones).

### 2d. Observable Framework (static, data-first)

- **Cost:** **Free** — builds to a **static site**, host anywhere (Cloudflare Pages, GitHub Pages).
- **Host model:** **Static-only by design.** Pages are Markdown with embedded JS code blocks.
  **Data loaders** run *at build time* (a Python/JS/SQL script whose stdout becomes a cached data
  file the page imports) — this is the canonical "build-time pull from gold" pattern, native.
- **Data-fetch:** Designed around build-time data loaders + Observable Plot. **But** it is weak at the
  things this site needs that *aren't* dashboards: a live "is my product recalled?" search over
  130k+ products and a paginated, deep-linkable recalls browser are runtime-interactive, server-backed
  features — not Observable Framework's sweet spot. You'd bolt a separate SPA-ish layer on for those.
- **Charting:** **Best-in-class for portfolio dashboards** (Observable Plot + D3 lineage). The most
  "data-person impressive" charts per line.
- **Ceremony:** **Lowest for dashboards**, but it is a *dashboard/notebook* tool, not a general website
  framework. Routing, forms, SEO per-page, and runtime API interaction are awkward.

### 2e. Scorecard

| | Cost (free) | Static-first | Build-time pull | Live-API fetch | Charting | Low ceremony for a DE | General-site (search/browse/SEO) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Next.js/Vercel | ⚠️ metered/license | ✗ (SSR-first) | ✓ | ✓✓ | ✓✓ | ✗ | ✓✓ |
| **Astro** | **✓✓** | **✓✓** | **✓✓** | **✓ (islands)** | **✓ (any lib)** | **✓✓** | **✓✓** |
| SvelteKit | ✓ | ✓ (adapter) | ✓ | ✓ | ✓ | ✓ | ✓ |
| Observable FW | ✓✓ | ✓✓ | ✓✓ | ⚠️ (not its job) | ✓✓ | ✓✓ (dashboards only) | ✗ |

---

## 3. The architectural lever: a **hybrid** data-fetch model

The data is **slow-moving** (daily/weekly cron) and splits cleanly into two access patterns:

- **Aggregates / dashboards** (`fct_*`): small, change once a day, identical for every visitor →
  **pull at build time**, ship as static JSON baked into the page. Zero runtime API load, instant
  render, works even if the API is cold/asleep. A scheduled rebuild (§9) refreshes them daily.
- **Per-entity + search** (`mart_recall_summary`, `mart_firm_profile`, `mart_product_search`): large
  cardinality (130k+ recalls/products), user-driven, deep-linkable → **fetch live from the API** in an
  interactive island, plus **statically pre-render the most-trafficked detail pages** at build time for
  SEO (a bounded set: recent + most-viewed recalls/firms via `getStaticPaths`).

This hybrid is exactly what **Astro** does natively (build-time `fetch` in frontmatter for dashboards;
runtime islands for search/browse; `getStaticPaths` for pre-rendered detail SEO). It is awkward in
Observable Framework (no runtime-interactive server features) and heavier than needed in Next.

---

## 4. Recommendation: **Astro + Cloudflare Pages**, charts via **Observable Plot** (+ D3 where needed)

**Astro** is the best fit and the recommendation. Rationale, not a list:

1. **It matches the data's two-speed access pattern** (§3) without forcing a server you operate.
   Dashboards pull from the API at build time and ship as static HTML+JSON; search and the recalls
   browser are small interactive islands that hit the live FastAPI. Next would make you operate SSR you
   don't need; Observable Framework can't do the live-interactive half well; SvelteKit can do it but
   makes you think in a JS framework on every page including the pure-content ones (landing,
   methodology, about-the-data).

2. **Lowest ceremony for a solo data engineer.** Content pages are HTML-with-frontmatter; you only
   reach for a component framework *inside* an island. A backend-leaning developer ships pages without
   carrying RSC / client-server-boundary / hydration concepts across the whole app.

3. **Free and unlocked.** Static output deploys free on **Cloudflare Pages** (generous free build
   minutes + unlimited bandwidth, no non-commercial license footnote, no cold-start). No vendor lock —
   the same `dist/` deploys to Netlify or GitHub Pages unchanged. (This also keeps the whole project on
   Cloudflare, matching R2 landing — a tidy portfolio narrative.)

4. **It keeps Observable Plot — the most portfolio-flattering charting — without adopting the whole
   Observable Framework.** Astro islands can render Observable Plot (or D3, or LayerCake) directly, so
   you get the data-viz quality of option (d) *and* the general-website capability option (d) lacks.

5. **SEO + static-gen for free.** Astro pre-renders to static HTML with real `<title>`/meta/OG tags and
   per-page SEO — important for a portfolio piece you want to be linkable and indexable. The
   `@astrojs/sitemap` integration and `getStaticPaths` give clean URLs for recalls and firms.

**Charting decision:** **Observable Plot** as the default (concise, grammar-of-graphics, beautiful
defaults, ideal for the time-series + bar + choropleth set in §5), dropping to **D3** only for the US
choropleth (geography) if Plot's geo support feels limiting. One framework-agnostic chart island
component wraps Plot so every chart is one `<Chart .../>`. Avoid pulling in a second chart lib.

**One honest caveat:** Astro's island/framework-agnostic flexibility means *you* pick the island
framework (Svelte recommended for the interactive widgets — smallest runtime, best DX). That is a
deliberate choice the ADR (§10) should record so the repo doesn't accumulate three island frameworks.

---

## 5. v1 page + chart inventory (this is the gating list for §0's verdict)

Five pages. Each chart/data block is annotated with **its upstream API endpoint ← gold model**, which
is what proves "one chart ← one `fct_*`/`mart_*` read" and therefore "no star needed."

### 5.1 Landing / overview (`/`)

Static, build-time data. The portfolio's front door — explain the project + headline numbers.

- **Hero KPI strip** — total recalls, distinct firms, sources covered, data freshness date.
  ← `GET /stats/overview` ← *(new, trivial)* `fct_recalls_overview` **or** API-computed from `fct_*`.
- **Recalls over time (all-source)** — area/line chart, monthly. ← `GET /stats/recalls-by-month?source=ALL` ← `fct_recalls_by_month` (`source='ALL'`).
- **By source** — small-multiples or stacked bar of recalls per source. ← `fct_recalls_by_month` per-source rows.
- **By classification** — bar, **source-native legends** (FDA `1/2/3/NC`, USDA `Class I/II/III`+`Public Health Alert`, USCG `H/L/M/S`). ← `GET /stats/by-classification` ← `fct_recalls_by_classification`. **[API-2026-06-19: FDA is `1/2/3/NC`, NOT `Class I/II/III`; `classification` is source-native and not unified, so legends/filters must be scoped per source. Endpoint not built — see §0 note.]**
- Methodology / "what this data is and isn't" copy block (static MDX) — links to the data-source notes;
  **must state the geography caveat** (registration ≠ harm) so the firm-geography chart isn't misread.

> **[API-2026-06-19]** The hero KPI strip and all three charts on this page read the **`/stats/*`** family (`/stats/overview`, `/stats/recalls-by-month`, `/stats/by-classification`), which **does not exist yet** (§0 note). This whole page is blocked until `/stats/*` is built or `fct_*` is exported for a build-time bake.

### 5.2 Recalls browser (`/recalls`)

The interactive heart. Live API, paginated, filterable, deep-linkable (filters in the URL querystring).

- **Filterable, paginated recalls table** — columns: date, source, title, classification, firm,
  status. Filters: source, classification, date range, firm, status. ← `GET /recalls?source=&classification=&published_after=&published_before=&firm=&is_active=&limit=&cursor=` ← `mart_recall_summary`. **[API-2026-06-19: corrected params — dates are `published_after`/`published_before` (also `announced_after`/`_before`), not `date_from/to`; status is the tri-state `is_active` (CPSC/NHTSA are `null` and match neither true nor false), not `status`; paging is **keyset** `cursor`+`limit`, not `page=`; `firm` is a case-insensitive substring on the PRIMARY firm name — **there is no `firm_id` filter** (see §5.4). The six categorical filters accept multi-value any-of via repeat or comma.]**
- **Result-count + active-filter chips**; URL is the source of truth so a filtered view is shareable. **[API-2026-06-19: `next_cursor` is opaque and tied to the exact filter set — put filters in the URL (shareable), treat the cursor as ephemeral forward-paging state (no "jump to page N"), and reset to page 1 on a `400 bad_cursor`. `with_total=true` adds a count via a second query.]**
- Row → recall detail page (5.3).

### 5.3 Recall detail (`/recalls/[source]/[recall_id]`)

Statically pre-rendered for a bounded popular set (SEO); falls back to live fetch for the long tail.

- **Header card** — title, source badge, classification, status, dates. ← `GET /recalls/{source}/{recall_id}` ← `mart_recall_summary`.
- **Products list** — product rollup (names, identifiers). ← same payload (`product` rollup). *(Identifier note: the per-product `upc` field was **removed from the API response [API-2026-06-19]** — it was NULL for every source; recall-level UPCs live on the `recall_product_upcs` jsonb and the `upc=` search matches those. Surface HIN/model, not a per-product UPC column.)*
- **Firms block** — linked firm chips (role + match confidence). ← same payload (`firms` jsonb) → links to 5.4.
- **Lifecycle timeline** — announced → published, status, and a `(revised)` badge. ← same payload
  (`announced_at`, `published_at`, `lifecycle_status`, `is_active`, `has_been_edited`).
  **[API-2026-06-19: the richer edit-history fields (`edit_count` / `edit_event_count` / `first_seen_at` /
  `last_seen_at` / `is_currently_active` / `was_ever_retracted`) were PRUNED from the API response — only the
  boolean `has_been_edited` remains (no dated edit history; a dated "last revised" is a future cross-repo
  item). Scope the timeline to the fields above.]**

### 5.4 Firm profile (`/firms/[firm_id]`)

Statically pre-rendered for top-N most-recalled firms (SEO); live fetch otherwise.

- **Identity card** — canonical name, aliases, per-source registered attributes. ← `GET /firms/{id}` ← `mart_firm_profile`.
- **Recalls-by-source mini-bar** — `recalls_by_source` jsonb. ← same payload.
- **This firm's recalls table** — ← `GET /recalls?firm_id={id}` ← `mart_recall_summary`. **[API-2026-06-19: ⚠️ `?firm_id=` does NOT exist on `/recalls` today. Options: (a) request the backend add a `?firm_id=` filter over the `firms` rollup (recommended — precise, includes co-recalled firms), or (b) use `?firm={canonical_name}` substring, which matches only the PRIMARY firm name and can over/under-match. This feature is blocked until (a).]**
  > **[PERF-2026-06-19] Pipeline-side prerequisite for the recommended option (a): a GIN index on the
  > `firms` rollup — DONE on this branch.** A `?firm_id=` filter resolves to a jsonb **containment** match
  > over `mart_recall_summary.firms` (the `jsonb_agg` of `{firm_id, name, role, match_confidence}` objects) —
  > e.g. `where firms @> '[{"firm_id": :id}]'`. Without an index that is a **sequential scan of the full
  > recalls corpus (130k+)** on every firm-profile load, so gold now **GIN-indexes `firms`** via
  > `config(indexes=[{'columns': ['firms'], 'type': 'gin'}])` in `mart_recall_summary` (hash-named →
  > oscillation-immune), alongside the existing GINs on `distribution_state_codes` /
  > `distribution_country_codes` / `search_vector` (and `mart_product_search`'s `recall_product_upcs` GIN for
  > the analogous UPC containment). The default `jsonb_ops` opclass serves `@>`; a tighter pure-containment
  > `jsonb_path_ops` opclass is a Phase-7 re-profile option (needs a `post_hook` — `config(indexes)` can't set
  > an opclass). Recorded in `documentation/index_audit.md`; the GIN is **built + catalog-verified** (gold
  > rebuilt 2026-06-19, `audit_schema_and_indexes.sql` §C). **Remaining (cross-repo, API side only):** the API
  > still has to add the `?firm_id=` param + containment predicate (`firms @> jsonb_build_array(jsonb_build_object('firm_id', :id))`);
  > the pipeline index it relies on is in place.
- **Firm rank context** — "Nth most-recalled firm." ← `GET /stats/firm-leaderboard` ← `fct_recalls_by_firm`. **[API-2026-06-19: `/stats/*` not built — see §0 note.]**

### 5.5 Dashboards (`/dashboards`)

Static, build-time. The data-viz showcase — the "look at the trends" page. **Every panel ← one `fct_*`.**

| Panel | Chart | Endpoint ← gold model |
|---|---|---|
| Monthly trend + rolling avg + YoY | line + band | `GET /stats/monthly-trend` ← `fct_recalls_monthly_trend` |
| By week / month / year (toggle) | bar | `fct_recalls_by_week` / `_by_month` / `_by_year` |
| By classification × source | grouped bar | `fct_recalls_by_classification` |
| Active vs inactive | stacked bar / donut | `fct_recall_status` |
| Most-recalled firms | horizontal bar (leaderboard) | `fct_recalls_by_firm` |
| Geography — **distribution lens** | US choropleth | `GET /stats/by-geography?basis=distribution` ← `fct_recalls_by_geography` |
| Geography — **firm-registration lens** | US choropleth (separate, captioned) | `…?basis=firm_registration` ← `fct_recalls_by_geography` |
| Units recalled (NHTSA/USCG only) | line, captioned "not cross-source comparable" | `fct_units_recalled` |

> **Panel↔model count note:** the 8 panels span **fewer than 8 distinct `fct_*`** — the by-week/month/year
> row is one toggle over **three** models (`fct_recalls_by_week`/`_by_month`/`_by_year`), and the two
> geography rows **share** `fct_recalls_by_geography` (one model, two `basis` values). "One chart ← one
> `fct_*`" still holds (no chart joins models), but a reader reconciling that against the **10 `fct_*` on
> disk** shouldn't expect a 1:1 panel↔model map — some panels share a model via a basis/grain switch.

> **Cross-plan `/stats/*` pointer:** these dashboards depend on a `/stats/*` endpoint family (and possibly
> a new `fct_recalls_overview`, §5.1). The sibling FastAPI plan
> (`project_scope/future-repos/fastapi-serving-layer-plan.md` §3 "Deferred — dashboard endpoints over `fct_*`")
> currently **defers** `/stats/*` out of API-v1, gated on Phase-9 need via **ADR 0024**. This dashboard
> inventory **is** that trigger — it un-defers the `/stats/*` surface in the FastAPI plan, so the two
> plans are consistent-by-reference (this plan asks; ADR 0024 ratifies).
>
> **[API-2026-06-19] Build status:** the API shipped **without** `/stats/*` — every panel in the table above is blocked until the `/stats/*` family is built (ADR 0024) **or** the pipeline exports the `fct_*` aggregates to static JSON for a build-time bake (§9 fallback). No panel here is buildable against the current deployed API.

> **Data-honesty requirement (carry the gold-notes caveats into the UI):** the two geography charts
> are **different questions** and **not interchangeable** (`gold_design_notes.md` §"Geography has two
> lenses"). They must be **two separate, captioned charts**, never one toggle that implies equivalence.
> The firm-registration chart caption must say *registration ≠ where the product went or harmed*; the
> units chart caption must say *recall-magnitude, not unique vehicles, NHTSA/USCG only*. (The
> `firm_location → firm_registration` rename is **done** on the pipeline side — C17, 2026-06-09;
> `fct_recalls_by_geography` emits `firm_registration` — so the API field and this caption should use
> `firm_registration` unconditionally.)

### 5.6 "Is my product recalled?" search (`/search`)

Live API, the consumer-facing utility. An interactive island.

- **Search box** — keyword over product name/description + recall title + firm name. ← `GET /products/search?q=` ← `mart_product_search` (Postgres FTS / tsvector; **no fuzzy/trigram** — ADR 0037 — so the UI copy says "keyword search," not "fuzzy," and offers tips for exact identifiers). **[API-2026-06-19: a recall-grain keyword search also exists — `GET /recalls/search?q=` (returns `RecallSummary`+`rank`) — if you want a "search recalls" mode distinct from product search.]**
- **Exact-identifier lookup** — HIN / model / UPC fields. ← `GET /products/search?hin=&model=&upc=` ← `mart_product_search` btree columns.
  > **UPC honesty caveat (carry it into the UI like geography/units):** product-grain `upc` is **NULL for every source today** (`mart_product_search.sql` — CPSC UPCs are recall-level, FDA returns none via the bulk endpoint), so a v1 UPC box at product grain returns **nothing**. UPC matching exists **only** as a containment filter over the recall-level `recall_product_upcs` jsonb. Ship the UPC field only against that jsonb path (or label it "coming when per-product UPC enrichment lands"); HIN/model btree lookup is genuinely populated.
- **Results** → recall detail (5.3). Empty-state copy: "No match ≠ not recalled — search is over our
  five sources only," with a link to the methodology block.

### 5.7 What this inventory implies (the §0 feedback, restated)

- **8 of 8 dashboard panels** are a single `fct_*` read. **4 of 4 entity/search features** are a single
  `mart_*` read. **Zero** panels require joining dims↔facts at query time, and **zero** require a pivot
  the marts don't pre-compute. → **No star.** Extend `fct_*` if a future chart appears.
- **New backend surface this inventory asks for (small):** (1) a `/stats/*` family of read-through
  endpoints over the existing `fct_*` (no new models except possibly one `fct_recalls_overview`); (2)
  the recall-detail timeline projection from the data already in `mart_recall_summary`. Route both
  through ADR 0024.

---

## 6. The API client + types (generate from OpenAPI — do not hand-write)

The single most important DX decision: **generate the TypeScript client + types from the API's
`/openapi.json`** (FastAPI publishes it for free — `implementation_plan.md` line 860). Never hand-write
response types; they'd drift from the backend.

- **Tooling:** `openapi-typescript` (types) + `openapi-fetch` (a tiny, typed `fetch` wrapper). Both are
  zero-runtime-overhead and far lighter than `orval`/generated-axios SDKs — right for a static site.
- **Generation step:** an npm script `gen:api` that runs `openapi-typescript <API_URL>/openapi.json -o
  src/lib/api/schema.d.ts`. Run it in CI before build so types track the deployed API. Commit the
  generated `schema.d.ts` so local dev type-checks offline.
- **Client shape:** `src/lib/api/client.ts` constructs a typed `createClient<paths>({ baseUrl })`.
  A thin `src/lib/api/recalls.ts` etc. wraps the four endpoint families with named functions
  (`listRecalls`, `getRecall`, `searchProducts`, `getFirm`, `getStats`). Pages/islands call those, not
  raw fetch.
- **Two base URLs:** build-time fetches use the API's prod URL (or, if the API is slow to wake, a
  pinned snapshot — see §9); runtime island fetches use the public API URL from a `PUBLIC_API_URL` env
  var (Astro exposes `PUBLIC_`-prefixed vars to the client).
- **Failure handling:** the API may cold-start (free tier, ADR 0025 candidates Fly/Render/Workers).
  Build-time fetch retries with backoff and **fails the build loudly** if the API is unreachable
  (better a stale deploy than a blank dashboard). Runtime islands show a friendly loading/cold-start
  state and a retry.

---

## 7. Styling, components, accessibility

- **Styling:** **Tailwind CSS** (`@astrojs/tailwind`) — fast, no runtime, easy to keep consistent solo.
  A small design-token layer (colors per source badge, status, classification) in the Tailwind config
  so the palette is single-homed and charts + badges share it.
- **Components** (`src/components/`): `Layout.astro` (shell, nav, footer, SEO slots); `Chart.svelte`
  (framework-agnostic Observable Plot wrapper — takes a spec + data, owns responsive resize + a11y
  table fallback); `RecallTable.svelte` (filter/paginate island); `SearchBox.svelte`;
  `SourceBadge.astro`, `StatusBadge.astro`, `ClassificationBadge.astro`; `KpiStat.astro`;
  `FilterChips.svelte`; `Choropleth.svelte`. Keep static pieces `.astro`, interactive pieces islands.
- **Accessibility (portfolio-credible, WCAG 2.1 AA target):**
  - Every chart island ships a **visually-hidden data table** fallback (Plot renders SVG; screen
    readers need the table) and an `aria-label`/`<figcaption>` describing the trend.
  - Color is **never the only encoding** — source/status/classification also carry text/shape; palette
    is colorblind-safe (checked against deuteranopia).
  - Keyboard-navigable table, filters, and search; visible focus rings; skip-to-content link.
  - Honest empty/loading/error states (the search empty-state copy in §5.6 is an a11y + trust feature).
  - `prefers-reduced-motion` respected for any chart transitions.

---

## 8. SEO / static generation

- **Per-page meta + OpenGraph** via the `Layout.astro` SEO slot (title, description, OG image). The
  landing + dashboards pages are the recruiter-facing ones — give them real OG cards.
- **`getStaticPaths` pre-render** a bounded set of detail pages for indexability: recent N recalls +
  top-N firms (build-time fetch a small index from the API). The long tail renders live on request from
  the island/client — set those to `noindex` or accept on-demand rendering; do **not** try to
  pre-render 130k pages (build time + free-tier limits).
- **`@astrojs/sitemap`** for the static set; `robots.txt`; canonical URLs.
- **Structured data:** optional `Dataset`/`Article` JSON-LD on detail pages — nice portfolio polish.

---

## 9. Deployment + CI on the free tier

- **Host:** **Cloudflare Pages** (static `dist/`, free, unlimited bandwidth, no cold start). Connect the
  GitHub repo; Pages builds on push to `main` and gives preview deployments per PR for free.
- **Build:** `astro build` → static `dist/`. Build-time data loaders fetch the `fct_*` aggregates from
  the API and bake them in.
- **CI (GitHub Actions, free for public repos):** one workflow —
  1. `npm ci`
  2. `npm run gen:api` (regenerate types from the deployed API's `/openapi.json`; **fail on drift** if
     the committed `schema.d.ts` changed — surfaces a backend contract change as a red check)
  3. `npm run lint` (ESLint + Prettier) and `npm run check` (`astro check` / `tsc --noEmit`)
  4. `npm run test` (Vitest — see §11)
  5. `npm run build` (also catches build-time-fetch failures)
  Cloudflare Pages does the deploy; CI is the gate. Mirrors the pipeline repo's CI-as-gate posture (ADR
  0018) for a consistent portfolio story.
- **Scheduled rebuild for fresh dashboards:** a **daily** GitHub Actions `schedule:` cron that triggers
  a Cloudflare Pages deploy hook (so the build-time-baked `fct_*` data refreshes after the pipeline's
  daily cron). This is the frontend analogue of the pipeline's Phase-7 cron and a clean DE narrative:
  *upstream cron lands data → downstream cron rebuilds the static site.* Bring this rung to the project's
  bar (it's the thinnest one above):
  - **Secret location:** the Pages deploy-hook URL is a secret — store it as a **repo secret**
    (`CF_PAGES_DEPLOY_HOOK`) and reference it via `secrets.*`; never inline the hook URL in the workflow.
  - **Default-branch only:** GitHub's `schedule:` trigger **only fires on the default branch** (`main`) —
    the cron workflow file must be merged to `main` to run at all; it won't fire from a feature branch.
  - **Idempotent / skippable when upstream is down:** the job must **not publish a broken or empty build**
    — probe the API first (or reuse §6's build-time-fetch failure mode) and **skip the deploy-hook call**
    if the API is unreachable, leaving the last-good static deploy live. A failed daily rebuild is a
    no-op, never a regression.
- **Cold-start resilience:** if the free-tier API (Fly/Render, ADR 0025) is asleep at build time, the
  build-step retry/backoff (§6) wakes it; if it stays down, the build fails rather than shipping blanks.
- **Prereq / risk — unmeasured cold-start budget (gate on ADR 0025):** the whole "daily rebuild +
  build-time pull wakes a cold API" lever **assumes** the Phase-8 deployment target (Fly/Render/Workers,
  **ADR 0025 — not yet filed**) can complete an HTTP wake **inside the GitHub Actions build window**. No
  Phase-8 artifact has measured that cold-start budget, so this is an unproven cross-phase dependency.
  Gate the build-time-pull design on ADR 0025 quantifying it. **Fallback if the wake is too slow/flaky:**
  fetch the aggregates **at request time / via ISR** in an island, **or** ship a small **build-time data
  snapshot** baked from gold (a pinned JSON exported alongside the pipeline's daily run) so the static
  dashboards never block on the API being warm.
- **Cost ledger:** Cloudflare Pages free + GitHub Actions free (public repo) + the API's own free tier =
  **$0**. No Vercel license footnote, no metered functions.

---

## 10. The ADR to file (recompute the number — the plan text is stale)

**File `documentation/decisions/0039-frontend-framework.md`** *(in the **pipeline** repo's ADR
register, since that is the project's single ADR home; the frontend repo links to it).*

- **Number = `0039`.** Recomputed from the SSOT: `documentation/decisions/README.md` line 112 — *"The
  next free number is **0039**."* The on-disk gap is 0024/0025 (reserved for Phase 8's API ADRs) and
  the highest filed is 0038; 0024/0025 land first (Phase 8), so the frontend ADR is the next free
  number after them: **0039**.
- **The plan text is stale:** `implementation_plan.md` line 881 says *"next free is 0036"* and refers to
  *"0034/0035 filed as Proposed stubs."* That predates 0036/0037/0038 being filed. Per README line 112's
  rule (*"plan docs must not reserve numbers independently"*), line 881 should be corrected to **0039**
  and the "0034/0035 Proposed stubs" clause dropped. *(This doc fix is a separate, already-identified
  documentation-sweep TODO on the current branch — flagged here, not done here.)*
- **ADR content (Nygard template):** Context = Phase 9 needs a frontend for a Python+Postgres+FastAPI
  backend at near-zero cost, solo DE. Decision = **Astro + Cloudflare Pages, Observable Plot charts,
  OpenAPI-generated TS client, hybrid build-time-aggregates / live-API-search data model** (this plan).
  Alternatives considered = Next.js/Vercel, SvelteKit, Observable Framework (record §2's reasoning).
  Consequences = static-first/free, but you own island-framework discipline (pick Svelte; don't sprawl)
  and a daily rebuild cron for dashboard freshness. **Status:** Proposed at Phase 9 start; ratifies when
  the frontend repo's first deploy lands (per the PR-merge status-flip convention).
- **Gated to Phase 9 start** (post-Phase-8). Do not file before the API contract (ADR 0024) is settled —
  the generated client depends on the OpenAPI spec existing.

---

## 11. Testing (held to the project's quality bar, frontend idiom)

The pipeline repo's bar is ruff/pyright/pytest; the frontend equivalent:

- **`astro check` + `tsc --noEmit`** — type safety end-to-end (the generated API types make this
  load-bearing: a backend contract change breaks the build).
- **ESLint + Prettier** (`--check` in CI) — the lint/format gate.
- **Vitest** for pure logic: the API client wrappers (mock `fetch`), filter→querystring serialization,
  chart-data transforms (the `fct_*` JSON → Plot spec mappers). Keep transforms pure and I/O behind the
  client so they're unit-testable without network — mirrors the pipeline's `_parse_*`/I/O separation.
- **Playwright** (optional, one happy-path each): landing renders KPIs, recalls browser filters,
  search returns a result, a detail page loads. Free in CI; high portfolio signal.
- **Accessibility check:** `axe`/`pa11y` against built pages in CI (catches missing chart fallbacks).

---

## 12. Phased, signposted commit plan (one branch, one end-of-branch PR)

Mirrors the pipeline's "phase = staged signposted commits on one branch + one PR" convention. Each
commit is independently green (lint + type-check + tests). The frontend ADR (0039) is filed in the
pipeline repo, not here.

1. **`chore: scaffold Astro + Tailwind + CF Pages config`** — `create astro`, Tailwind integration,
   `@astrojs/sitemap`, ESLint/Prettier, Vitest, base `Layout.astro` + nav/footer, deploy to CF Pages
   (verify the empty-site deploy works *first* — host before features).
2. **`feat: OpenAPI-generated typed API client`** — `gen:api` script, `schema.d.ts`, `client.ts`, the
   four endpoint wrappers + `/stats/*`, Vitest for the client + querystring serializer. (Against the
   live Phase-8 API or a committed sample `openapi.json` if the API isn't deployed yet.)
3. **`feat: design tokens + shared components`** — source/status/classification badges, `KpiStat`,
   the framework-agnostic `Chart.svelte` Observable Plot wrapper (with a11y table fallback).
4. **`feat: landing/overview page`** — build-time `fct_*` fetch, hero KPIs, recalls-over-time +
   by-source + by-classification charts, methodology copy with the geography caveat.
5. **`feat: dashboards page`** — all 8 `fct_*` panels (§5.5), the **two captioned geography choropleths**,
   the captioned units chart. Chart-data transform unit tests.
6. **`feat: recalls browser`** — filter/paginate island, URL-as-state, result chips, live `GET /recalls`.
7. **`feat: recall detail + firm profile`** — `getStaticPaths` pre-render of the popular set, live
   fallback, lifecycle timeline, firm linkage, per-firm recalls table + leaderboard rank.
8. **`feat: "is my product recalled?" search`** — keyword + exact-identifier island over
   `/products/search`, honest empty-state, FTS-not-fuzzy copy.
9. **`feat: SEO + a11y pass`** — per-page meta/OG, JSON-LD, sitemap/robots, `axe`/`pa11y` in CI, reduced-motion.
10. **`ci: GitHub Actions gate + daily rebuild cron`** — lint/check/test/build + `gen:api` drift gate;
    `schedule:` cron → CF Pages deploy hook for daily dashboard freshness.
11. **`docs: repo README + link to pipeline ADR 0039`** — what the site is, how to run it, the hybrid
    data model, the cost ledger; back-link to `0039-frontend-framework.md` in the pipeline repo.

**End-of-branch:** one PR. Ratify ADR 0039 (Proposed → Accepted) on merge per the status-flip convention.

---

## 13. Explicitly out of scope for v1 (and why)

- **A dimensional star / `dim_*` models** — §0: the fixed chart set doesn't need it; re-open only for a
  self-serve pivot UI or a BI-tool embed.
- **Fuzzy/trigram product search** — ADR 0037 disables `pg_trgm`; v1 search is exact-keyword FTS. The UI
  copy is honest about it; a future fuzzy tier is a backend ADR, not a frontend feature.
- **User accounts / saved searches / alerts ("notify me if X is recalled")** — stateful, needs auth +
  a store; a clear v2. The public read-only API posture (Phase 8) is intentionally accountless.
- **The project blog** — the user owns it (`TODO.md` line 12), excluded here.
- **SSR / a server runtime** — the hybrid static model (§3) removes the need; adopting one would add cost
  and ops the portfolio constraints reject.
```
