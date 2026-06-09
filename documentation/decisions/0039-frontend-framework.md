# 0039 — Frontend framework

**Status:** Accepted (2026-06-09)
**Date:** 2026-06-09

> **Build location:** the website is a **separate repository** built after go-live (no in-repo
> "Phase 9"). This ADR records the framework decision; the execution plan is
> `project_scope/future-repos/website-frontend-plan.md`.

## Context

The project website is gold's first BI-esque consumer and a portfolio piece. It needs: a recalls
browser with filters, an "is my product recalled?" search, firm profiles, and a small fixed dashboard
set — fed by the Phase-8 API (ADR 0024). Constraints: **free-tier hosting**, low operating cost, low
ceremony for a solo data engineer, good charting, and good SEO/static delivery. The user was
considering Vercel/Next.js but is open to a better fit. Candidates evaluated: Next.js/Vercel, Astro
(islands), SvelteKit, Observable Framework (full matrix in the website plan §2).

## Decision

**Adopt Astro (islands architecture).** Host the static output on a free static host (Cloudflare Pages
or Netlify free tier).

Rationale:
- **Cost / hosting:** static-first output deploys free and indefinitely on Pages/Netlify; no metered
  SSR compute, no Vercel-style usage limits.
- **Ceremony:** `.astro` files are HTML-with-frontmatter; you reach for a JS framework only inside an
  island that needs interactivity — minimal boilerplate for a content-heavy site with a few interactive
  surfaces (search, filters, charts).
- **Charting:** framework-agnostic islands — drop in Observable Plot / D3 / a Svelte or React chart
  island per panel, best tool per chart.
- **SEO / performance:** ships zero JS by default; recall/firm pages are statically generated and
  crawlable.
- **App-like surfaces:** the "is my product recalled?" search and filterable browser are handled by
  client islands calling the API — Astro does this cleanly, unlike Observable Framework (best for
  dashboards but weak at app-like search/filter pages).

Rejected/runner-up:
- **Next.js/Vercel** — SSR-first and more ceremony than a mostly-static portfolio site needs; Vercel's
  free tier is metered/licensed. Not wrong, just heavier than required.
- **SvelteKit** — close second; slightly more ceremony than Astro for a content-heavy site, no decisive
  advantage here.
- **Observable Framework** — best-in-class dashboards but weak at the interactive search/filter app
  pages, which would force bolting on a second SPA-ish layer.

Data feed: build-time pull from the API for static pages where freshness-of-a-day is fine, with a
request-time/ISR fallback for the search; the daily rebuild cron + cold-start budget are gated on
ADR 0025 (website plan §9).

## Consequences

- The website repo is an Astro project deployed to a free static host with a daily rebuild hook.
- Charts are per-panel islands (no single charting lock-in).
- The "no star schema" call (ADR 0024 §5) holds: the dashboard set is fixed and each panel reads one
  `fct_*` mart via the API, so Astro never needs a semantic/BI layer.
- If the site later needs heavy interactive cross-dimensional slicing, revisit both this ADR and the
  gold star deferral (ADR 0038 §1 / 0024 §5).
