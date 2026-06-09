# 0040 — Dedicated data-quality framework: Soda Core

**Status:** Accepted (2026-06-09)
**Date:** 2026-06-09

> Resolves the Phase-7 deliverable "Investigate dedicated data-quality framework (Soda Core or Great
> Expectations)" (`implementation_plan.md` Phase 7) and the master-plan C29 decision. The C29 draft
> recommended *deciding* on Soda Core but *deferring adoption*; the 2026-06-09 user directive is to
> **install it now if it genuinely earns a place in this repo.** This ADR makes that call: adopt,
> tightly scoped.

## Context

The pipeline's data-quality posture is already substantial: ~210 dbt generic tests + ~40 singular
tests + 12 `source_assumptions` monitors (ADR 0031), the three bronze business invariants
(`src/bronze/invariants.py`), dbt `source freshness`, and operator-facing `scripts/sql/**/assert_*.sql`.
The open question is whether a dedicated DQ framework adds **non-redundant** value or just duplicates
dbt.

The honest gap analysis: dbt tests run **post-transform** on silver/gold. They do **not** cleanly
cover (a) **bronze-layer** checks before the transform runs, (b) **row-count / null-rate anomaly**
checks expressed as thresholds rather than pass/fail, (c) **freshness SLAs** richer than dbt's source
freshness, or (d) a **standardized scan report/exit code** for alerting (today the `assert_*.sql`
files are eyeballed via `psql`). Both candidate frameworks (Soda Core, Great Expectations) fill these;
Soda Core's SodaCL is closest to the SQL-shaped assertions this project already writes and is
low-ceremony YAML (vs GE's heavier Python expectation-suite + data-docs machinery).

## Decision

**Adopt Soda Core (`soda-core-postgres`), scoped to where it complements — not duplicates — dbt.**

- **In scope for Soda:** bronze-layer checks (per-table freshness SLAs, row-count anomaly vs prior run,
  schema/column presence drift) as a **pre-/post-extract gate**, plus a few cross-source anomaly checks
  that dbt expresses awkwardly. These run against bronze tables that dbt tests do not cover.
- **Out of scope for Soda:** the existing ~210 dbt silver/gold tests + 12 source-assumption monitors
  **stay in dbt** — they are not migrated. dbt remains the transform-layer test authority (ADR 0015);
  Soda is the bronze + anomaly + freshness-SLA + standardized-report layer.
- **Packaging:** `soda-core-postgres` lives in a `dq` dependency-group (PEP 735) — installable, not in
  the core runtime. Config under `soda/` (`configuration.yml` reads Neon creds from env, never
  hardcoded; `checks/` holds the SodaCL check files).
- **Orchestration:** wired into a scheduled/CI scan in a follow-up (not this commit); the
  decision + scaffold + a starter check set land now. Severity-threshold escalation (the master-plan
  C30 warn→error work) builds on Soda's warn-vs-fail primitive.

Why now, not deferred: the framework earns its place on the **bronze + anomaly + freshness-SLA** niche
dbt structurally cannot cover pre-transform, and a recognized DQ framework is a deliberate
portfolio-breadth choice for this project. It is scoped narrowly so it never duplicates the dbt suite.

## Consequences

- A new `soda/` config tree + a `dq` dependency-group; `uv sync --group dq` installs it.
- Soda scans need a Neon connection (operator-run, like dbt); the first scan + the CI/scheduled wiring
  are follow-up steps, not part of the decision.
- DQ responsibility is now **layered**: bronze invariants (Python, fail-fast at load) → Soda
  (bronze freshness/anomaly/schema, reportable) → dbt tests (silver/gold transform correctness) →
  source-assumption monitors (ADR 0031, drift). Each layer owns a distinct surface; no overlap.
- Relates to ADR 0015 (testing strategy — Soda is the new bronze/anomaly tier), ADR 0031
  (source-assumption monitors stay in dbt), ADR 0029 (observability — Soda scan results feed the
  alerting-when-a-trigger-fires posture).
