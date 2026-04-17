# 0015 — Testing strategy

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The pipeline is multi-layered: Pydantic bronze schemas (ADR 0012, 0014), extractor classes (ADR 0012), bronze loaders with content hashing and quarantine routing (ADR 0007, 0013), dbt silver/gold transformations (ADR 0011), and (eventually) a FastAPI serving layer. Each layer has distinct failure modes that require different test shapes.

Two cross-cutting concerns shape the testing approach:

- **Determinism vs. reality.** Tests must be fast and reliable (no flakiness from live APIs), but also must exercise real behavior (not mocked into uselessness). API fixtures reconcile these.
- **Data-governance goals.** Per the project's stated goals and the policies in ADR 0013 + ADR 0014, test coverage is itself a demonstrable data-governance artifact, not just engineering hygiene.

Aggressive test coverage was considered against minimal coverage. The tradeoff is signal-to-noise: more tests catch more bugs but introduce triage fatigue if written as coverage trophies rather than targeted contracts. A test earns its place when it catches a class of bug that plausibly happens, fails unambiguously when it does, and costs less to maintain than the bug costs to debug in production.

## Decision

### Test pyramid

| Layer | Count | Scope | Tools |
|---|---|---|---|
| **Unit** | Most | Pydantic schemas, parsers, content-hash logic, `check_invariants()` | pytest + respx/responses |
| **Integration (per extractor)** | ~45 | Full `extract()` → `load_bronze()` against fixtures | pytest + pytest-vcr |
| **Integration (bronze/silver)** | Moderate | Bronze loader against real Postgres; dbt transformations against seeded data | pytest + pytest-postgresql / Neon branch |
| **End-to-end** | ~6 | Full fixture → bronze → silver → gold pipeline run | pytest + Neon branch |

### API fixtures — VCR.py for integration, respx for unit

Per the question raised in `project_scope/questions.md`, API fixtures serve five purposes: determinism, speed, safety (no quota burn, no credentials needed in CI), schema-drift detection via cassette diffs, and portfolio demonstration of the schema-evolution workflow from ADR 0014.

**VCR.py (via `pytest-vcr`)** for integration extractor tests:

- One cassette per test scenario, stored at `tests/fixtures/cassettes/<source>/<test_name>.yaml`
- Secrets scrubbed via `before_record_request` hook (strips `Authorization`, `X-API-Key`, FDA's `signature` param, any header matching `*-Key`)
- Committed to git — cassettes are the authoritative archive of how each API responded historically
- Re-record workflow: `pytest --record-mode=rewrite` hits real APIs and overwrites cassettes; secrets scrubbing applies on the way out; documented in `documentation/operations.md`

**`respx`** (for `httpx`) or **`responses`** (for `requests`) for unit tests where explicit, hand-constructed mock responses are more appropriate than captured full responses — pagination edge cases, specific error codes, malformed payloads.

### Integration test scenarios per extractor

Each of the five extractors ships with cassette-backed integration tests covering:

| Scenario | What it verifies |
|---|---|
| Happy path, single page | Full successful fetch → bronze landed correctly |
| Happy path, multi-page | Pagination correctly assembles full result |
| Empty result | Zero records handled without error |
| Partial page (last page < limit) | Pagination termination logic |
| 429 rate limit | Retry ladder per ADR 0013 kicks in, eventually succeeds |
| 500 transient | Exponential backoff + retry, eventually succeeds |
| 401 auth failure | Fails fast per ADR 0013; workflow exits non-zero |
| Malformed record in response | Routes to `_rejected` T1 per ADR 0013; other records proceed |
| Content-hash dedup | Run extractor twice against same cassette; bronze row count does not increase |

Approximately 9 × 5 = 45 cassettes. Cheap once recorded, valuable as a schema-drift archive.

### End-to-end scenarios

Run against a pristine Neon branch seeded with fixture data:

| Scenario | What it verifies |
|---|---|
| Full pipeline (5 fixtures → bronze → silver → gold) | Cross-layer contract holds |
| Firm entity resolution | Honda across NHTSA + CPSC fixtures collapses to one `firm_id` in silver |
| USDA EN/ES collapse | Bilingual fixture produces one silver event; Spanish summary populates `summary_alt_lang` per ADR 0006 |
| Re-ingest idempotency | Re-running the full pipeline produces no delta in silver |
| Lineage history view | Bronze snapshot diff produces expected `recall_event_history` rows per ADR 0007 |
| Schema drift quarantine | Fixture with an unexpected field routes to `_rejected` per ADR 0014; pipeline continues |

### Integration database strategy (with portability)

Primary: Neon branches via their REST API. Each CI run creates a branch, runs tests against it, deletes on teardown. Leverages the branching feature bundled with Neon's free tier per ADR 0005.

Portability requirement: all integration/e2e tests consume a single `test_db_url` pytest fixture. The fixture's implementation is swappable:

```python
@pytest.fixture(scope="session")
def test_db_url():
    provider = os.getenv("TEST_DB_PROVIDER", "neon")
    if provider == "neon":
        return provision_neon_branch()
    elif provider == "local":
        return provision_local_postgres()  # pytest-postgresql or docker compose
    elif provider == "testcontainers":
        return provision_testcontainers_postgres()
```

Tests never touch Neon-specific APIs directly (no time-travel queries, no branch-switching mid-test). Migrating off Neon becomes a `conftest.py` change, not a test rewrite.

### dbt test posture

**Generic tests on every silver model:**

- `not_null` on all primary and foreign keys
- `unique` on primary keys and natural composite keys (e.g., `(source, source_recall_id)` on `recall_event`)
- `accepted_values` on every enum-like column: `source`, `event_type`, `classification`, `status`, `role`, `identifier_type`
- `relationships` for every foreign key

**Singular tests for cross-model invariants:**

- Orphan detection: every `recall_product` references exactly one existing `recall_event`
- USDA dedup correctness: no duplicate `(source='USDA', source_recall_id)` rows in silver
- Date sanity: `recall_event.published_at` not in the future and not before 1960
- Value sanity: `recall_product.units_affected` not negative
- Baseline sanity: per-source event count within ±50% of historical average (guard against catastrophic data loss, e.g. a silently-returning-zero extractor)

**Source freshness:**

- Bronze tables tagged with freshness expectations: `warn_after: 48h` on daily sources (CPSC, FDA, USDA); `warn_after: 8d` on weekly sources (NHTSA, USCG).

**Deliberately excluded from v1:**

- Tests on fields the source does not actually guarantee (e.g., "every recall has a `remedy`" — not true for CPSC).
- Statistical distribution tests — require a baseline; wait for production data.
- Column-per-column trivial tests written for coverage-metric reasons. A test without an articulable bug class is noise.

### Test layout

```
tests/
  __init__.py
  conftest.py                  # shared fixtures, test_db_url provisioning
  unit/
    test_schemas/              # one file per source
    test_extractors/           # parser unit tests with respx mocks
    test_bronze_loader.py
    test_invariants.py
  integration/
    test_cpsc_extractor.py     # VCR-backed
    test_fda_extractor.py
    ...
  e2e/
    test_pipeline.py
  fixtures/
    cassettes/
      cpsc/, fda/, usda/, nhtsa/, uscg/
    sample_records/            # known-good JSON samples for round-trip tests
    known_bad_records/         # known-bad records for rejection-path tests

dbt/
  tests/                       # singular SQL tests
  models/.../schema.yml        # generic tests declared inline
```

### Tooling

- `pytest` — test runner (CLAUDE.md-mandated)
- `pytest-vcr` — VCR.py integration for integration tests
- `respx` (for `httpx`) or `responses` (for `requests`) — unit-test HTTP mocking
- `pytest-postgresql` or Docker Compose — local dev Postgres
- `pytest-cov` — coverage reporting
- `ruff`, `pyright` — lint and type check (CLAUDE.md-mandated)
- `pre-commit` — hooks: `ruff format`, `ruff check`, `pyright`, and a cassette-secret-scrub verifier that fails if any committed cassette contains an auth header
- `dbt test` — native dbt contract testing

### Coverage target

≥85% line coverage on `src/` (unit + integration combined) enforced by `pytest-cov --cov-fail-under=85` in CI. Exclusions explicit in `.coveragerc` and reviewed during PR — common exclusions are integration-only code paths and defensive error branches for conditions that cannot be reliably triggered in test.

## Consequences

- Comprehensive coverage with explicit signal-to-noise discipline: tests without an articulable bug class are deleted, not normalized.
- API fixtures double as a schema-drift archive — a PR that re-records a cassette and shows field changes is the visible form of ADR 0014's evolution playbook.
- Per-extractor integration matrix (~45 cassettes) is a portfolio-visible engineering artifact in its own right.
- End-to-end tests exercise the hardest cross-cutting concerns (firm resolution, EN/ES dedup, lineage history, schema-drift quarantine) — each demonstrates a specific ADR end-to-end.
- Neon branching earns its keep in CI; portability is preserved via the `test_db_url` abstraction so a future migration off Neon is a conftest change.
- dbt test posture is generous-but-targeted: ~60–80 generic tests + 5 singular tests + freshness. Enough to be visibly thorough without creating triage fatigue.
- Coverage floor of 85% forces tests to be written alongside code; exclusions are documented and reviewable.

### Open for revision as real-world API behavior and data patterns surface

- **Singular test list.** The v1 list is a floor, not a ceiling. Real data will reveal patterns that should be asserted (e.g., once baseline volumes are known, tighten the ±50% sanity guard).
- **Cassette refresh cadence.** How often to re-record fixtures against live APIs is TBD — suggested starting point: on every new schema-drift event, and on a quarterly scheduled refresh.
- **Statistical/distribution tests.** Out of scope for v1; candidates for v2 once a production baseline exists.
- **Portability migration.** If Neon becomes unsuitable (cost change, capability gap), the `test_db_url` abstraction is where we pivot. No test-suite rewrite expected.
- **dbt unit tests (model-level synthetic input tests, dbt 1.8+).** Not in v1. Add if/when a specific model's transformation logic proves gnarly enough to warrant it.
