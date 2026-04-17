# 0014 — Schema evolution policy

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

The pipeline ingests from five sources (ADR 0001), each with an independently-evolving API or flat-file schema. NHTSA has already demonstrated that schema change is routine — its most recent update (May 2025) extended field lengths on fields 19, 20, and 22 and added two new fields (`DO_NOT_DRIVE`, `PARK_OUTSIDE`). Similar changes should be expected from all five sources over the project's lifetime.

Schema drift is inevitable; the policy decision is how to **detect** it loudly and how to **respond** systematically.

Two failure modes to avoid:

- **Silent data loss.** An agency renames `firmlegalnam` → `firm_legal_name`; a permissive Pydantic model silently drops the new field and populates `None` where data used to be. Weeks later someone notices the firm rollups are empty.
- **Permissive-on-read paralysis.** A permissive model accepts arbitrary fields into JSONB; silver transformations depend on fields that may or may not exist; every dbt model becomes defensive.

Two classes of drift that require different detection machinery:

- **Structural drift** (fields added, renamed, removed, retyped) — Pydantic catches these when configured strictly.
- **Value-level semantic drift** (same schema, new valid values — e.g., USDA adds a new recall reason enum) — Pydantic's `str` type accepts without complaint; caught only by dbt tests against a known-values list.

## Decision

### Baseline posture on every bronze Pydantic model

```python
from pydantic import BaseModel, ConfigDict

class CpscRecallRecord(BaseModel):
    model_config = ConfigDict(
        extra='forbid',   # unknown fields → ValidationError
        strict=True,      # reject type coercions
    )
    RecallID: int                            # required — no Optional, no default
    RecallNumber: str                        # required
    LastPublishDate: datetime | None = None  # Optional — source documents nullable
    ...
```

- **`extra='forbid'`** — any unknown field raises `ValidationError`. Catches additions AND the "new name" side of renames.
- **`strict=True`** — type coercion (int-from-string, int-from-float, etc.) is rejected. Catches source-side type changes.
- **Required by default** — fields declared without `Optional` and without a default. Catches the "old name missing" side of renames. A field is marked `Optional[T] = None` **only when the source explicitly documents it as nullable**.
- **Silver-layer dbt tests** on nullable fields (`not_null`, `accepted_values`, `relationships`) provide a second net for value-level drift Pydantic can't catch.

### Drift detection → response playbook

| Detected drift | Caught by | Response |
|---|---|---|
| Source adds a new field | `extra='forbid'` rejects | Add field to Pydantic model; ship PR; re-ingest affected window from R2 |
| Source renames a field | Old-name missing (required-field error) **and** new-name present (forbid error) | Update Pydantic; optionally `Field(alias='new_name')` to preserve downstream attribute names; re-ingest |
| Source removes a field | Required-field error | Critical: alert stakeholders, deprecate downstream logic. Non-critical: remove from Pydantic model. |
| Source changes a field's type | `strict=True` rejects | Update Pydantic type; re-ingest |
| Source adds a new enum value | Pydantic passes; dbt `accepted_values` test fails on silver | Update dbt `accepted_values` list and enum taxonomy. Usually no re-ingest needed. |

### Re-ingestion procedure

Structural drift requires re-ingestion of rows that were validated under the old schema. Because raw payloads live in R2 T0 (ADR 0004) with content-hashed retention per ADR 0007, re-ingestion is a local operation — no upstream API hits needed.

- A `re-ingest` CLI command (built alongside the extractors) reads a source's R2 landing for a date range and re-runs `validate()` → `check_invariants()` → `load_bronze()` using the current Pydantic model.
- Content hashing (ADR 0007) makes re-ingestion idempotent — rows whose canonical content is unchanged since original ingestion do not re-insert.
- Quarantined records in `_rejected` tables (per ADR 0013) are eligible for reprocessing once the schema model is updated — they remain queryable and replayable.
- Re-ingestion is documented in `documentation/operations.md` as a standard procedure, not a one-off scripting exercise.

### Division of responsibility between Pydantic and dbt

Explicit separation of concerns:

- **Pydantic** is responsible for **structural** contracts at load time: field presence, field types, unknown-field rejection.
- **dbt tests** are responsible for **semantic** contracts at transformation time: valid values, referential integrity, business-rule constraints on derived silver columns.

Neither is asked to do the other's job.

## Consequences

- Structural drift of all kinds surfaces at ingestion time with a loud, actionable error — never silent data loss.
- Required-by-default is the policy that catches silent-rename drift; disciplined use of `Optional` is load-bearing. A careless `Optional[str] = None` defeats the rename safety net.
- Value-level semantic drift is explicitly the responsibility of silver-layer dbt tests, not Pydantic — clean division of concerns between the bronze contract and the silver contract.
- R2 T0 retention (ADR 0007) directly enables cheap re-ingestion. Schema changes become routine workflow, not emergency response.
- Operational cost of a drift event is a PR to the Pydantic model + a re-ingest command — small ergonomic friction, proportional to the rarity of the event.
- `ConfigDict(extra='forbid', strict=True)` is the default for every source's bronze schema, not opt-in per model. Uniformity matters here — one permissive model compromises the whole posture.

### Open for revision as real-world API behavior surfaces

- **Semantic-drift response ergonomics.** Once we see the rate and nature of new enum values in the wild (does USDA add recall reasons faster than we can update taxonomy?), the dbt-test-driven workflow may need automation (e.g., auto-generating enum lists from observed bronze values into a proposed dbt `accepted_values` PR).
- **Re-ingest CLI ergonomics.** The command is sketched here; design will refine once it's actually used during fixture-building and first schema-drift events.
- **Strictness exceptions.** If a specific source proves pathologically inconsistent (e.g., inconsistent whitespace in fields that are nominally enums), we may need narrow `@field_validator` exceptions. These should be documented and scoped, not used to weaken the baseline posture.
