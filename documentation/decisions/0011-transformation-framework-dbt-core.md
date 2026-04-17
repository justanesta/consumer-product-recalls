# 0011 — Transformation framework: dbt-core

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

ADR 0004 established the four-layer medallion pipeline with Postgres as the warehouse. The silver and gold layers require SQL transformations to produce the harmonized model (per ADR 0002) and serving views. A framework choice was evaluated across four options:

| Option | Notes |
|---|---|
| **dbt-core** | Apache 2.0 licensed. Jinja+SQL. Tests as first-class citizens (`schema.yml` generic + singular tests). `ref()`-based DAG auto-generation. `sources:` + `freshness:` matches bronze→silver→gold. Auto-generated docs with lineage graphs. Industry-standard recognition. |
| **SQLMesh** | Newer, technically strong (virtual environments, time travel, real incremental models). Smaller ecosystem; less hiring-manager recognition. |
| **Plain SQL via SQLAlchemy + Alembic** | Simplest. Total control. No DAG, no tests-as-data, no lineage, no docs. |
| **Python-native (pandas/polars)** | Wrong tool for this project. Round-tripping bronze → dataframe → Postgres wastes compute when Postgres *is* the warehouse. |

Two considerations dominate:

1. **Architectural fit.** dbt's source/model hierarchy maps directly onto the medallion shape from ADR 0004 — no fighting the tool.
2. **Portfolio value.** The project goal favors solutions demonstrating recognizable DE skill surfaces. dbt appears on virtually every data engineering job description; SQLMesh does not yet have the same saturation.

A third consideration: dbt-core's native incremental model + `snapshots` feature align naturally with the snapshot-and-derive lineage strategy from ADR 0007.

## Decision

Silver and gold transformations are built in **dbt-core** with the `dbt-postgres` adapter.

- Project layout at repo root:
  ```
  dbt/
    dbt_project.yml
    profiles.yml       (template; actual profile in env/secrets)
    models/
      staging/         -- 1:1 views over bronze tables (type casting, naming)
      silver/          -- recall_event, recall_product, firm, recall_event_firm, recall_event_history
      gold/            -- serving views, denormalized search index, dashboard aggregations
    tests/             -- singular tests (cross-model assertions)
    macros/            -- shared transformation utilities
    snapshots/         -- per ADR 0007 where dbt snapshots are used
  ```
- **Test coverage required at silver:**
  - `not_null` on every key column
  - `unique` on `(source, source_recall_id)` for `recall_event`
  - `accepted_values` on `source`, `event_type`, `classification`, `status`, `role`
  - `relationships` between `recall_product.recall_event_id` and `recall_event.recall_event_id`
- **Data contract division:** Pydantic enforces bronze load-time schema conformance; dbt tests enforce silver/gold data invariants. Clean separation — neither does the other's job.
- **dbt is invoked as a GitHub Actions step** (per ADR 0010) after all extractors for a cadence complete. `dbt run` + `dbt test` on every run; failures fail the workflow.

## Consequences

- Auto-generated lineage graph from `ref()` becomes a deployable portfolio artifact (`dbt docs generate` → static site on Cloudflare Pages).
- The `recall_event_history` pattern from ADR 0007 lands naturally as a dbt incremental/snapshot model.
- dbt's `freshness:` assertions on `sources:` catch stale bronze data — a useful pipeline health signal.
- Jinja+SQL is a real learning curve. Mitigated by dbt's extensive docs and community.
- Adds a build step (`dbt run` + `dbt test`) to CI; per-workflow isolation in ADR 0010 keeps failures localized to a source.
- If dbt's model system ever becomes constraining, migration to SQLMesh or plain SQL is incremental — Postgres remains the substrate, and the SQL is largely ANSI. The framework choice is not a lock-in to the storage layer.
- `dbt-postgres` adapter version pinned via `uv` to the version compatible with Neon's Postgres.
