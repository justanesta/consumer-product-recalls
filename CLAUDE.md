
# Python EtLT Medallion Pipeline

## Project Type
EtLT (Extract, light-transform, Load, Transform) data pipeline for US federal consumer product recall data. Uses a four-layer medallion architecture: raw bytes land in Cloudflare R2 (landing), Pydantic-validated records are bulk-inserted into Postgres bronze tables (bronze), and dbt handles all real transformation through staging → silver → gold layers.

## Project-Specific Standards

### Framework & Structure
- src/
  - extractors/ (5-step lifecycle: extract → land_raw → validate → check_invariants → load_bronze)
  - landing/ (Cloudflare R2 client — writes raw bytes before any validation)
  - bronze/ (BronzeLoader, content-hash dedup, quarantine routing, invariant checks, retry policies)
  - schemas/ (Pydantic bronze contracts — storage-forced type coercion only, not business normalization)
  - config/ (settings, source YAML loader, source registry, structured logging)
  - cli/ (Typer CLI: `recalls extract`, `recalls deep-rescan`, `recalls version`)
- dbt/ (staging/silver/gold transformation models)
- config/sources/ (per-source YAML configs)
- Modular design: Each pipeline step is independently testable

### Core Libraries
- pandas or polars for data transformation
- sqlalchemy for database connections
- pydantic for data validation and schemas
- requests or httpx for API extraction
- Consider: prefect, dagster, or airflow for scheduling (if needed)

### Data Validation
- Pydantic models for every data schema (input and output)
- Validate at each stage: post-extract (Pydantic, storage-forced coercion only), post-bronze (dbt tests), pre-gold
- Log validation failures, don't silently skip bad records

### Error Handling
- Robust error handling at each pipeline stage
- Distinguish: Transient errors (retry) vs. permanent errors (log and alert)
- Dead letter queue or error table for failed records
- Pipeline should be idempotent (safe to re-run)

### Testing Strategy
- pytest for all pipeline components
- Unit tests: Individual extractors, bronze loader, schema validation
- Integration tests: Full pipeline with test database/fixtures
- Mock external APIs in tests (use responses or httpx.mock)
- Test edge cases: Empty data, malformed data, connection failures

### Logging & Monitoring
- Structured logging (JSON format) with log levels
- Log: Records processed, failures, duration per stage
- Consider: Push logs to CloudWatch, Datadog, or local log aggregation

### Code Quality
- Type hints throughout (pyright enforced)
- Pydantic for all data schemas and config
- ruff for linting
- Keep functions pure where possible (easier to test)

### Helper scripts in `scripts/`
- Anything written under `scripts/` is held to the **same bar as `src/`** and MUST be verified *immediately after writing*, before handing it back — do not leave the user to discover failures at commit time (this has repeatedly cost debugging time).
- Required gates (the pre-commit hooks): `ruff check`, `ruff format --check`, and `pyright` (its config already includes `scripts/**/*.py`). Run all three on the new/changed file and fix everything before reporting done.
- **Test the pure logic with pytest.** Parsers, transforms, and helpers in a script need adequate `tests/scripts/test_<name>.py` coverage — happy path, the obvious edge/empty cases, and a regression test for any bug found. Run the suite and confirm it's green. Import the script via the repo-root `sys.path` shim (see `tests/scripts/test_refresh_user_agents.py`).
- Keep network/DB/pipeline side effects behind functions so the parse/transform layer is unit-testable without I/O (mirrors the extractor `_parse_*` separation).

### Documentation Requirements
- README.md: What data flows where, how to run pipeline
- documentation/
  - architecture.md: Pipeline stages, data flow diagram, dependencies
  - data-schemas.md: Input/output schemas, validation rules
  - development.md: Local setup, running tests, debugging
  - operations.md: Scheduling, monitoring, troubleshooting

### Database Practices
- Use connection pooling (SQLAlchemy engine)
- Batch inserts/updates (not row-by-row)
- Use transactions for atomicity
- Index destination tables appropriately
- Consider: Use COPY/bulk load for large data volumes

### Scheduling & Orchestration
- Start simple: cron or systemd timers for scheduling
- If complexity grows: Consider prefect (lightweight) or airflow (full-featured)
- Make pipeline trigger-able: CLI script with clear arguments

### Quality Gates (Before Next Feature)
- [ ] Pipeline runs end-to-end successfully
- [ ] Unit and integration tests passing
- [ ] Data validation catches known bad data
- [ ] Error handling tested (connection failures, bad data)
- [ ] Logging provides visibility into pipeline state
- [ ] ruff linting passes
- [ ] pyright type checking passes
- [ ] Documentation updated with any schema changes

### Cost Considerations
- Local PostgreSQL or DuckDB for destination (free)
- Cloud: Use free tiers (Supabase, Neon, AWS RDS free tier)
- Avoid expensive managed ETL tools initially (Fivetran, Stitch)
