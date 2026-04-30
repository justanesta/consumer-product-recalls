# USDA FSIS — First Extraction Findings

> **Status: Pending operator run.** This document is a template/placeholder
> populated during Phase 5b verification. Run `uv run recalls extract usda`
> against Neon dev once the migration is applied, then fill in the sections below.
> Mirror the shape of `documentation/cpsc/last_publish_date_semantics.md` and
> `documentation/fda/api_observations.md`.

## Run metadata

- **Date of first run:** YYYY-MM-DD
- **Neon branch:** dev (or whichever was used)
- **Migration head:** 0005 (`usda_fsis_recalls_bronze`)
- **CLI invocation:** `uv run recalls extract usda`
- **Run ID (from extraction_runs table):** ...
- **R2 landing path:** ...

## Bronze row counts vs Finding B (cardinality probe)

Reference targets from `recall_api_observations.md` Finding B:
- Total records (English + Spanish combined): 2,001
- English records: 1,212
- Spanish records: 789
- Archived: 1,829 (91.4%)
- Active: 172 (8.6%)

Observed:
- `SELECT COUNT(*) FROM usda_fsis_recalls_bronze;` → ...
- `SELECT langcode, COUNT(*) FROM usda_fsis_recalls_bronze GROUP BY langcode;` → ...
- `SELECT archive_recall, COUNT(*) FROM usda_fsis_recalls_bronze GROUP BY archive_recall;` → ...

Any deviation greater than ~5% from the cardinality probe should be investigated;
small differences are expected as FSIS publishes new records and the dataset moves.

## Quarantine routing

- `SELECT COUNT(*), failure_stage FROM usda_fsis_recalls_rejected GROUP BY failure_stage;` → ...

Expected: small/zero count. Any orphan-Spanish records (`failure_stage='invariants'`,
`failure_reason` mentions "Spanish") are valid quarantines per the bilingual invariant
(ADR 0006, `check_usda_bilingual_pairing`).

## Idempotency check

Re-run `uv run recalls extract usda` immediately. Confirm:
- bronze row count is unchanged.
- second run reported `loaded=0` (or close to it; small drift if FSIS published in the gap).

## ETag conditional-GET verification

Run `uv run recalls extract usda` a second time. Behavior depends on Finding N
(`recall_api_observations.md`):

- **If ETag is enabled and probes passed:** the second run should hit `usda.extract.not_modified`
  in logs, write 0 bronze rows, and bump `last_successful_extract_at` only.
  - `SELECT last_etag, last_cursor, last_successful_extract_at FROM source_watermarks WHERE source='usda';`
- **If ETag is disabled:** second run pulls full payload, dedup-skips all 2,001 records,
  bronze row count unchanged.

## Deep rescan check

Run `uv run recalls deep-rescan usda`. Confirm:
- `last_etag` is unchanged.
- `last_cursor` is unchanged.
- `last_successful_extract_at` is unchanged.
- Bronze row count: unchanged or small delta (idempotent dedup).

## Open follow-ups

- [ ] Verify `field_last_modified_date` reliability on a known-edited recall (Finding E).
- [ ] If Finding N showed ETag is unreliable, set `etag_enabled: false` in
      `config/sources/usda.yaml` and document the rationale here.
- [ ] VCR cassette recording for the 9 integration scenarios (Phase 5b
      `implementation_plan.md` line 189) — the natural next slice after this doc lands.
