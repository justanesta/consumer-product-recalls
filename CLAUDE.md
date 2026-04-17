
# Python ETL Pipeline

## Project Type
Extract, Transform, Load (ETL) data pipeline for data integration, cleaning, and warehouse loading.

## Project-Specific Standards

### Framework & Structure
- src/
  - extractors/ (data source connectors)
  - transformers/ (cleaning, enrichment logic)
  - loaders/ (destination writers)
  - pipeline.py (orchestration)
- config/ (connection strings, pipeline configs as YAML/TOML)
- Modular design: Each E/T/L step is independently testable

### Core Libraries
- pandas or polars for data transformation
- sqlalchemy for database connections
- pydantic for data validation and schemas
- requests or httpx for API extraction
- Consider: prefect, dagster, or airflow for scheduling (if needed)

### Data Validation
- Pydantic models for every data schema (input and output)
- Validate at each stage: post-extract, post-transform, pre-load
- Log validation failures, don't silently skip bad records

### Error Handling
- Robust error handling at each pipeline stage
- Distinguish: Transient errors (retry) vs. permanent errors (log and alert)
- Dead letter queue or error table for failed records
- Pipeline should be idempotent (safe to re-run)

### Testing Strategy
- pytest for all pipeline components
- Unit tests: Individual extractors, transformers, loaders
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
