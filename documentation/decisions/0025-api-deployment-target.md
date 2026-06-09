# 0025 — API deployment target

**Status:** Accepted (2026-06-09)
**Date:** 2026-06-09

> **Build location:** like ADR 0024, this governs the **separate** serving-layer repo built after
> go-live. Recorded here because deployment constraints shaped the API design (0024) and the read-only
> Neon connection topology is a `main`-branch decision (ADR 0005). Execution detail:
> `project_scope/future-repos/fastapi-serving-layer-plan.md` §12.

## Context

The Phase-8 API (ADR 0024) needs a free-tier host that runs a long-lived async Python process with a
persistent asyncpg connection pool to Neon (read-only, `main` per ADR 0005), and integrates cleanly
with GitHub Actions CI/CD. Three candidates were evaluated: Fly.io, Render, Cloudflare Workers.

## Decision

**Deploy to Fly.io** (free allowance), with **Render as the documented fallback** (near-identical
shape — a deploy hook instead of `flyctl`). **Cloudflare Workers is rejected.**

| Factor | Fly.io (chosen) | Render (fallback) | Cloudflare Workers (rejected) |
|---|---|---|---|
| Python runtime | Full CPython (Docker) | Full CPython | Pyodide/WASM — **cannot run asyncpg's C extension** |
| Persistent pool | Yes (long-lived process) | Yes | No durable process / pool model |
| Cold start | Acceptable for a personal API (scale-to-zero with wake) | Similar | N/A |
| Neon read-only | Standard `postgresql+asyncpg://…` to `main` | Same | Would need an HTTP DB shim |
| GH Actions CD | `flyctl deploy` action + token secret | Deploy hook / blueprint | Wrangler — moot |

Cloudflare Workers' Python runtime (Pyodide/WASM) cannot load asyncpg's compiled driver nor hold a
durable connection pool, which would force a rewrite of the data layer (HTTP DB proxy) and contradict
ADR 0024's async-asyncpg stack. It is rejected on runtime grounds, not preference.

Deployment is `flyctl deploy` from a `deploy.yml` GitHub Actions workflow on push to `main`, with the
Fly API token in repo secrets. The container is slim CPython 3.12 + uvicorn.

## Consequences

- The serving repo ships a `Dockerfile` + `fly.toml` (and a `render.yaml` fallback stub), not a
  Workers `wrangler.toml`.
- The async-asyncpg stack (ADR 0024) is safe — both chosen targets run it natively.
- Read-only Neon access uses a dedicated restricted role (see ADR 0013 amendment + the pipeline's
  `*_rejected` mutation-guard work) — the API never holds write privileges.
- Free-tier cold starts are accepted; if the website does build-time data pulls (website plan §9), the
  build must tolerate a cold-API wake (retry/backoff) or fall back to request-time fetch.
