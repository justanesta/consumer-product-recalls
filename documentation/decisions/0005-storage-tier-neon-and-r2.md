# 0005 — Storage tier (Neon Postgres + Cloudflare R2)

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

ADR 0004 chose the four-layer pipeline architecture but deferred provider selection to Phase 2 volume validation. Phase 2 produced these projections (across all five in-scope sources):

- **Postgres total** (bronze + silver + gold + indexes): ~800 MB to 1.5 GB at v1 launch, growing ~50–100 MB/year.
- **Object storage raw archive** (gzipped JSON / HTML / flat-file snapshots): ~2–5 GB cumulative, growing ~500 MB to 1 GB/year.

Project constraints: near-zero cost, prefer free tiers, prefer open-source-friendly infrastructure.

Free-tier candidates evaluated:

| Provider | Tier | Verdict |
|---|---|---|
| Supabase Postgres | 500 MB free | Too tight — FDA bronze alone could approach the ceiling at v1 launch |
| Neon Postgres | 3 GB free | ~30–50% utilization at launch; multi-year growth headroom; serverless cold starts but instant database branching is a meaningful bonus for testing |
| Render Postgres | 1 GB free | Tight — would breach the <50% headroom rule within v1 |
| Fly.io Postgres | 1 GB free | Same as Render |
| Cloudflare R2 | 10 GB free, **zero egress fees** | Plenty of headroom; zero egress is meaningful for serving raw artifacts |
| Backblaze B2 | 10 GB free | Comparable but egress fees apply at scale |

Headroom rule established in Phase 2: target <50% of free-tier ceiling at launch.

## Decision

- **Postgres:** Neon free tier (3 GB).
- **Object storage:** Cloudflare R2 (10 GB free, zero egress fees).

## Consequences

- ~30–50% utilization at v1 launch with multi-year growth headroom under both ceilings.
- Migration paths if outgrown: Neon Launch tier (~$19/month, 10 GB) for hot data; or move historical NHTSA bronze to Parquet on R2 with a DuckDB query layer — the latter doubles as a portfolio-worthy lakehouse skill demonstration rather than just a cost dodge.
- Neon's serverless cold starts are real but acceptable for cron-driven pipeline jobs and a low-traffic personal API. If user-facing latency becomes painful, paid tier removes them.
- Neon's branching feature (instant DB clones) may inform the testing strategy in a future ADR.
- R2's zero-egress pricing matters because the consumer-facing app will fetch raw artifacts (recall PDFs, CPSC product images) directly without burning bandwidth budget.
- Both providers require account setup (Cloudflare for R2, Neon for Postgres). Credentials live in environment variables; a secrets-management ADR is forthcoming.
- Cost trigger: if either layer crosses 50% utilization in production use, that re-opens this ADR rather than triggering an auto-upgrade.

### Capacity sanity check — NHTSA line-level rows

ADR 0009 scopes NHTSA bronze to ~300–500K line-level rows. At ~500 bytes/row (29 tab-delimited fields plus bronze metadata), that's ~150–250 MB before indexes and before content-hash dedup collapses no-change refreshes. Comfortable headroom remains for CPSC, FDA, USDA, and USCG bronze plus all of silver/gold/state under the 3 GB free-tier ceiling. If observed NHTSA row sizes run meaningfully higher than the estimate, the cost-trigger rule above kicks in.

### Neon branch conventions

Neon's branching feature (instant clones via API, same free tier) is used for multiple purposes. To keep operator error from overwriting production, branches follow a fixed naming convention:

| Branch name | Purpose | Lifecycle | Who writes |
|---|---|---|---|
| `main` | Production database. All scheduled extractor and transform workflows (per ADR 0010) target this branch. | Permanent | Only via GitHub Actions workflows and controlled manual ops (per `operations.md`) |
| `dev` | Long-lived development branch. Local `uv run ...` commands and human-driven experiments target this by default. | Permanent; occasionally reset from `main` when schema diverges too far | Developers locally, not CI |
| `pr-<n>` | Ephemeral CI test branches created per pull request via the Neon API (per ADR 0015). | Created at CI start; deleted at CI teardown | CI only |

`NEON_DATABASE_URL` in local `.env` always points at `dev` (or a personal short-lived branch). `NEON_DATABASE_URL` in GitHub Actions repository secrets always points at `main`. This separation means running an extractor locally never writes to production, and a misconfigured personal `.env` has a narrow blast radius bounded by `dev`.

Developers may create additional named branches freely for experimentation (e.g., rehearsing a schema migration before a PR), provided they delete the branch when done — Neon's compute cost for idle branches is minimal, but the project's convention is that only the three branches above are long-lived.
