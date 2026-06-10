# Soda Core — bronze data-quality scans

Decision + scope: **[ADR 0040](../documentation/decisions/0040-data-quality-framework-soda-core.md)**.

Soda owns the **bronze** quality surface — freshness SLAs, row-count floors, `content_hash`
integrity, and required-column schema-drift — the pre-/post-extract checks dbt's silver/gold
tests do **not** cover. It deliberately does **not** duplicate the dbt suite (~210 generic + ~40
singular + 12 source-assumption monitors), which remains the transform-layer authority (ADR 0015).

## Install (one-time, operator)

`soda-core-postgres` is in the **`dq` dependency-group** in `pyproject.toml` (resolved cleanly at
`3.5.6` against the project's version caps on 2026-06-09; the re-lock also bumped a few in-cap deps).
Install it into the venv with:

```bash
uv sync --group dq
```

## Run a scan (operator — needs a live Neon connection)

```bash
# env: NEON_HOST / NEON_USER / NEON_PASSWORD / NEON_DBNAME (same as dbt; see .env.example)
soda scan -d recalls_bronze -c soda/configuration.yml soda/checks/
```

Exit code is non-zero on a `fail`; `warn` results are reported but do not fail the scan — the
same warn-vs-fail posture ADR 0015 / 0031 use for dbt monitors.

## Layout

- `configuration.yml` — the `recalls_bronze` data source (Neon creds from env, never hardcoded).
- `checks/bronze_checks.yml` — the starter check set (freshness / row-floor / integrity / schema)
  for the 5 recall bronze tables + USDA establishments.

## Follow-ups (not in the scaffold)

- Wire a scheduled scan into CI / a GitHub Actions workflow (analogous to `firm-rollup-audit.yml`),
  reporting failures.
- Severity-threshold escalation (the master-plan C30 warn→error work) builds on Soda's warn/fail
  primitive.
- If Soda Cloud is ever added, promote the row-count floors to anomaly-score drift checks.
