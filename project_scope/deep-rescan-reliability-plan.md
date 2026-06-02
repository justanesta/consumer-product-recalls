# Deep-rescan reliability & workload — plan

- **Status:** Active — graduation + the PRE_2010-SHA column annotation landed via PR #50 (merged
  2026-06-02). Tier 1 (W2–W4: engine factory, GHA guards, doc fixes) is in progress on
  `refactor/deep-rescan-tier1-reliability`; Tiers 2–4 not yet started (each gets its own branch/PR).
- **Owns:** the fix ladder for deep-rescan reliability + workload ahead of Phase-7 scheduled GitHub
  Actions, and the PRE_2010 `response_inner_content_sha256` mitigation (#1–#3).
- **Points at:** `documentation/audit/deep_rescan_reliability_audit.md` for *what we found* (this doc
  does not restate it — single-home rule). ADR 0010 for the cadence decision this plan amends; ADR 0030
  for the dedup-contract identity the anti-join workstream must not break.

## Context

The audit (see findings doc) found two structurally distinct problems under TODO #55: **workload** — a
NHTSA no-op deep-rescan costs ~21 min because cost scales with corpus, not delta (full
`model_dump`+hash + ~59 IN-query round-trips, one transaction); and **reliability** — Neon drops long
single-connection runs (`server closed the connection unexpectedly`), every engine is
`pool_pre_ping`-only, `OperationalError` is in no retry filter, and the USCG-detail full sweep exceeds
the GHA 6h cap. Each recommendation below was adversarially verified; the verification killed or
reshaped half of them, and those corrections are baked into the workstream descriptions.

## Guardrails

- **Never advance `source_watermarks` on a deep-rescan** (the incremental path owns it). Holds today;
  must hold after every change.
- **Do not weaken the per-source dedup oracle** (`src/bronze/dedup_contracts.py`). Anything near
  `content_hash` / identity gets a concrete one-row trace first (the dedup-trap discipline; cf. the
  RECORD_ID and FDA productid-straddle bugs).
- **The user runs all code** (extractors, migrations, psql, dbt) — workstreams describe designs, not
  commands to run.

## Workstreams (done-markers)

| # | Workstream | Tier | Status |
|---|---|---|---|
| W0 | Audit graduation: this plan + `documentation/audit/deep_rescan_reliability_audit.md` + TODO #55 pointer + master-plan index row | — | ✅ PR #50 |
| W1 | **Mitigation #1** — annotate `extraction_runs.response_inner_content_sha256` as POST_2010-only-by-design (PRE_2010 SHA in the R2 manifest) at `_tables.py` + `NhtsaExtractor._augment_response_row` (comment-only) | — | ✅ PR #50 |
| W2 | **R3** — centralized engine factory → `NullPool` (single-threaded batch jobs; mirrors the surviving subprocess pattern) for all extractors + CLI + recovery; **carve out `uscg_manufacturer_details`** (its blocker is the 6h cap, not connections); add a guard test that every engine uses the factory | 1 | ✅ this PR |
| W3 | **R7-A** — add `timeout-minutes` (NHTSA 40, CPSC/USDA 30, FDA 60; USCG-detail deferred until a real single-run time exists) + `concurrency` (cancel-in-progress: false) to all five deep-rescan workflows (YAML only) | 1 | ✅ this PR |
| W4 | **D1** — fix the stale `loader.py` chunk-count comment/docstring (65k/~12 → ~321k/~59) and the `_flat_file.py` 304 claim | 1 | ✅ this PR |
| W5 | **R4** — extend `_TRANSIENT_RETRY`'s predicate with an `is_disconnect()` guard (matches "server closed the connection unexpectedly" / `connection_invalidated`); **not** a blanket `OperationalError` catch (the `_PG_PARAM_SAFETY_LIMIT` overflow must stay non-retried), **not** a second wrapper; keep it on `load_bronze`, never `run()` | 2 | ⏳ |
| W6 | **R1+R2** — NHTSA pre-extract short-circuit + `was_short_circuited` flag; gates on **both** inner SHAs (needs Mitigation #3's DB home + manifest backfill), placed in `NhtsaDeepRescanLoader` before download; `change_type` rebaseline bypass + NULL-baseline = proceed | 3 | ⏳ |
| W7 | **R6** — make `deep-rescan-fda.yml` cron-runnable: a "Resolve start date" step (mirror the existing end-date step) computing a rolling window; never pass the raw `""` input; validate the window vs ADR 0023 back-dating | 3 | ⏳ |
| W8 | **R8** — amend **ADR 0010** with per-source deep-rescan cadence: NHTSA/FDA weekly (cheap once W6 lands), CPSC weekly, USDA n/a (full snapshot), **USCG detail quarterly** (matches `phase-5d-uscg-manufacturers-detail.md`, not monthly); record the 1/12 rotation as a *future option* needing a range param + offset cursor (neither exists). Reorder secret-validation before `uv sync` (YAML) | 3 | ⏳ |
| W9 | **R5** — port the chunked-process pattern into the USCG-detail **deep-rescan** workflow (the incremental cron path already fits and needs nothing); benchmark real GHA page-rate first; `timeout-minutes` guard | 4 (defer) | ⏳ |
| W10 | **R9** — server-side staging-table anti-join for NHTSA (replace the ~59-chunk loop). **Defer**: depends on W6's short-circuit; must replicate `_identity_text_expr` text-canonical normalization (guardrail B); use SQLAlchemy bulk insert, not a new `psycopg2` COPY dependency; split read/write transactions; regression test vs the IN-query path | 4 (defer) | ⏳ |

## PRE_2010 `response_inner_content_sha256` mitigation (#1–#3)

The findings doc records *what is* (the column is POST_2010-only; the PRE_2010 SHA is in the R2
manifest, recoverable via `raw_landing_path`; bronze rows are complete and all carry `content_hash`).
The mitigations:

1. **Annotate the limitation at the point of use** — done this PR (W1): comment-only notes at the
   column definition and `_augment_response_row` so a future diff/monitor query author sees
   "POST-only; PRE in the R2 manifest." Converts a silent gap into a documented one.
2. **The R2 manifest is the system-of-record for the PRE_2010 SHA** — no code needed: locate it via
   `SELECT raw_landing_path FROM extraction_runs WHERE source='nhtsa' AND change_type LIKE '%seed%'`,
   then read that manifest object (both inner SHAs are in it).
3. **Structural fix, coupled to R1 (W6) — do NOT build ahead of its consumer.** When the short-circuit
   lands, give the PRE_2010 SHA a DB home — a JSONB `response_inner_content_sha256_by_archive` map
   (url→sha), forward-compatible with a hypothetical third NHTSA archive — populate it in
   `_augment_response_row`, and backfill the existing runs from their R2 manifests. Deferred because
   there is no live consumer today and nothing is lost by waiting. (Rejected: composite-hashing the
   existing column — breaks its "a real file's SHA / incremental-parity" meaning.)

## Suggested PR sequence

W0–W1 (PR #50) → **Tier 1** (W2 + W3 + W4 as one reliability/hygiene PR — no dedup/commit-semantics
risk) → **Tier 2** (W5) → **Tier 3** (W6 + W7 as the NHTSA short-circuit PR incl. W6's migration; W8's
ADR amendment can ride with any) → **Tier 4** (W9, W10) only if Phase-7 measurements justify them.
Cross-branch ordering, if it grows, belongs in `branch_sequencing_strategy.md`, not here.

## Open questions / benchmarks needed

- **USCG-detail real GHA page-rate.** The "fits in 6h at ~500/chunk" claim is unverified; observed was
  200/chunk over 7.75h. >1.3s/page → a full sweep exceeds 6h regardless. Measure with a small `--limit`
  test run on GHA before committing a chunk size (W9).
- **FDA 90-day window adequacy** vs ADR 0023 archive-migration back-dating; monitor via the Cell-B
  assertion in `documentation/fda/productid_stability_findings.md` (W7).

## Related
- `documentation/audit/deep_rescan_reliability_audit.md` — findings (what we learned).
- ADR 0010 — cadence decision amended by W8; ADR 0023 — FDA rescan window; ADR 0030 — NHTSA dedup
  identity protected by W10's guardrail.
