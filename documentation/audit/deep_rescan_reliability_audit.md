# Deep-rescan reliability & workload audit — Phase-7 scheduled-GHA readiness

- **Status:** Active (findings; 2026-06-02)
- **Type:** cross-cutting audit (per `documentation/documentation_model.md` type 6 — describes the
  world; the fix ladder it motivated is owned by `project_scope/deep-rescan-reliability-plan.md`,
  which this doc points at rather than prescribing here).
- **Method:** a read-only multi-agent workflow — four analysis dimensions (workload mechanics /
  connection–transaction reliability / per-source chunkability / GitHub-Actions cadence) → synthesis →
  adversarial verification of every recommendation — then direct verification of each load-bearing
  claim against `main` (HEAD `10b5c1d`). Prompted by TODO #55: the USCG-detail Neon timeouts in the
  2026-05-31→06-01 overnight seed (`logs/seed_uscg_detail_chunk.log`) and the observation that the
  NHTSA no-op deep-rescan costs ~21 min.

This audit found **two structurally distinct problems** under one TODO. What we learned is below; what
we will do about it is the plan doc.

---

## Problem 1 — Workload: deep-rescan cost scales with corpus size, not delta size

A deep-rescan that changes **nothing** still pays a full-corpus cost. The NHTSA second consecutive
deep-rescan took **~21 min for 0 inserts** (06:27→06:48, the 2026-06-02 idempotence check). There is no
corpus-level gate that can exit before `BronzeLoader.load()`.

- **O(corpus) hashing, unconditional.** `BronzeLoader.load()` (`src/bronze/loader.py`) iterates every
  record — `model_dump(mode="json")` → identity tuple → `content_hash(...)` — for all ~321k NHTSA rows
  before any DB query, with no early exit.
- **~59 Neon round-trips for NHTSA.** `_fetch_existing_hashes` chunks the existing-hash IN-query by
  `_PG_PARAM_SAFETY_LIMIT // n_cols` = `60_000 // 11` = **5,454 keys/chunk** → **~59 chunks** at ~321k
  rows, each a two-stage SQL (`GROUP BY max(extraction_timestamp)` per identity, then a join back for
  `content_hash`). This is the dominant share of the 21 min. **Measured 2026-06-02 (W6 acceptance,
  PR #54): the DB-compare is essentially the *entire* cost** — a full deep-rescan logged `load_bronze`
  ≈ 21.5 min vs ~13s for download+parse+validate combined (download 4.9s, parse 1.8s, validate 6.7s over
  322,672 rows). Converts the inference above into a measurement.
- **`response_inner_content_sha256` is write-only.** It is INSERTed after the load
  (`_augment_response_row`) and **never SELECTed back to gate a run.** NHTSA has no short-circuit on the
  deep-rescan path; the USCG `_should_short_circuit` two-gate pattern is explicitly disabled on the
  listing deep-rescan subclasses. **Resolved 2026-06-02 (W6, PR #54):** `NhtsaDeepRescanLoader` now reads
  the prior run's inner SHAs from `response_inner_content_sha256_by_archive` (migration 0021) and
  short-circuits a no-change deep-rescan in ~5s — see `project_scope/deep-rescan-reliability-plan.md`.
- **Whole load in one transaction.** `NhtsaDeepRescanLoader.load_bronze` wraps the entire ~59-chunk
  fetch + single batched insert in one `engine.begin()` — efficient for the insert, but holds one
  connection open for the full multi-minute DB phase.
- **Per-source no-op cost.** FDA windowed mode scales with *delta* (cheap); FDA full-corpus ≈ 4.5 min
  pagination; USCG detail holds **no** DB connection during its ~14k-page HTTP loop (it opens a fresh
  `engine.begin()` only for the final write).

## Problem 2 — Reliability: Neon drops long single-connection runs

The USCG-detail single-invocation seed dropped its Neon connection mid-run
(`server closed the connection unexpectedly`, run_id `be1f3edb`); the earlier non-chunked attempt
(`logs/seed_uscg_detail.log`) was lost outright. The 71-chunk workaround
(`scripts/uscg/seed_manufacturer_details_chunked.py`) survived only because each `--limit 200` chunk is
a fresh OS process with a fresh engine.

- **Every engine is `create_engine(url, pool_pre_ping=True)` — nothing else.** No `pool_recycle`, no
  `connect_args` (keepalives / `connect_timeout`), no `NullPool`, across all extractors + `cli/main.py`
  + `recovery.py`. `pool_pre_ping` fires `SELECT 1` only at **checkout**; it gives zero protection
  against a drop *during* an in-flight transaction or after a long idle extract phase. Neon serverless
  terminates idle connections (~300s) and can cold-start.
- **`OperationalError` is in no retry filter.** `_TRANSIENT_RETRY` retries only
  `TransientExtractionError` / `RateLimitError`; `_R2_RETRY` only the former. `run()` invokes
  `load_bronze` under `_TRANSIENT_RETRY`, so a Neon drop inside the load transaction is **never
  retried** — the whole run fails and rolls back to 0 inserts.
- **`_record_run` is best-effort.** It opens its own `engine.begin()` inside a bare `except Exception`
  that logs a warning and returns — a drop between the bronze commit and the run-record write means the
  run is silently never recorded (observed for `be1f3edb`).
- **Lifecycle ordering makes a USCG-detail timeout a total loss.** `run()` is
  extract → land_raw → validate → check_invariants → load_bronze; a 6h-cap kill during the ~4.5h
  `extract()` HTTP loop discards every fetched page before any bronze write.

## Per-source deep-rescan profile

| Source | Corpus | No-op cost | Fits GHA 6h cap? | Chunk lever today | Advances watermark? |
|---|---|---|---|---|---|
| nhtsa | ~321k | ~21 min | Yes (margin) | **None** (monolithic ZIP; PRE/POST split only; no `--limit`) | No |
| fda | ~134k | window-scaled / ~4.5 min full | Yes (margin) | `--start-date`/`--end-date` (eventlmd window) | No |
| uscg_manufacturer_details | ~14k pages | ~4.5–7.75h | **No — exceeds cap** | `--limit` on `extract` only, **not** `deep-rescan` | No |
| cpsc / usda | ~9.8k / ~2k | seconds | Yes | n/a (single call / full dump) | No |
| uscg / uscg_manufacturers (listing) | ~1.8k / ~16.3k | ~71s / ~11 min | Yes | short-circuit disabled on rescan | No |

The **never-advance-watermark** invariant holds on every deep-rescan path (no `_touch_freshness` /
`_update_watermark` call) and must be preserved by any change.

## GitHub-Actions as-built gaps (all five `deep-rescan-*.yml`)

`workflow_dispatch`-only (cron is Phase 7), `runs-on: ubuntu-latest` (6h per-job hard cap), single
`uv run recalls deep-rescan <src>` step. Verified **absent** in all: `timeout-minutes`, `concurrency`,
step-level retry, `matrix`.

- **`deep-rescan-uscg-manufacturers-detail.yml` would time out** as a cron (~4.5–7.75h > 6h), killed
  mid-`extract()` → total loss.
- **`deep-rescan-fda.yml` cannot run on cron as written** — it hard-requires `start_date`, and a
  `schedule` trigger passes empty-string (`""`), not `None`, so the CLI's `is None` full-corpus guard is
  bypassed and `date.fromisoformat("")` raises on every fire.
- No `timeout-minutes` → a hung run holds a runner to the 6h kill; no `concurrency` → a cron + manual
  dispatch can double-run.

## Two as-built facts that constrain any fix (surfaced by adversarial verification)

These are not bugs in current data — they are properties a fix must respect.

### A. The NHTSA short-circuit blind spot (POST_2010-only forensic SHA)

`extraction_runs.response_inner_content_sha256` carries the **POST_2010** inner-file SHA only — by
design (`NhtsaDeepRescanLoader.extract`: canonical = rolling-current archive, matching the incremental
path). The **PRE_2010** inner SHA is computed and persisted to the **R2 deep-rescan manifest**
(`land_raw`), keyed by the run's `raw_landing_path` — **not** to any DB column.

- **This is not a data gap.** Every bronze row (PRE and POST alike) carries its own `content_hash`
  (`NOT NULL` on `nhtsa_recalls_bronze`, migration 0011); the PRE_2010 rows are fully loaded
  (`extract` iterates `(pre_inner, post_inner)`). `response_inner_content_sha256` is a per-**run**
  file-level forensic column on `extraction_runs`, a different table from the bronze rows.
- **The PRE_2010 SHA is recoverable**, not lost: it is in every deep-rescan run's R2 manifest, locatable
  via `extraction_runs.raw_landing_path`. The manifest is its system-of-record.
- **The constraint:** a SHA-gate that reads this *column* (a future short-circuit) is PRE_2010-blind — a
  back-dated edit to a pre-2010 record with a byte-stable POST_2010 file would be silently skipped. Same
  family as the USCG "detail-only edit" short-circuit blind spot (see
  `project_scope/implementation_plan.md` → "Architectural follow-ups").

### B. The dedup text-canonical constraint (any server-side anti-join must honor it)

`_fetch_existing_hashes_chunk` casts NHTSA's nullable TIMESTAMPTZ identity columns (`bgman` / `endman`)
to a text-canonical form (`_identity_text_expr`) on **both** sides of the comparison, so empty-string
identity values bind as TEXT, matching the Python-side `identity_key`. Any server-side replacement
(e.g. a temp-table anti-join) that joins on the *typed* columns would re-introduce the
empty-string-vs-NULL mismatch — a re-insertion bug of the same character as the NHTSA deep-rescan
`RECORD_ID` bug just fixed (ADR 0030). The text-canonical representation is load-bearing, not incidental.

## Documentation drift observed

- `src/bronze/loader.py` — the `_PG_PARAM_SAFETY_LIMIT` block comment and `_fetch_existing_hashes`
  docstring still say "~12 chunks / 65k-row deep-rescan"; the current corpus is ~321k (~59 chunks).
- `src/extractors/_flat_file.py` — the module docstring claims a "304 → not-modified short-circuit"; the
  implementation sends no conditional-GET headers and has no 304 branch (the only 304-capable source is
  `UsdaExtractor`).

---

## Related
- `project_scope/deep-rescan-reliability-plan.md` — the fix ladder this audit motivated (what we will
  do, sequenced; owns the PRE_2010-SHA mitigation #1–#3).
- ADR 0010 (ingestion cadence + GitHub-Actions cron) — the cadence decision the plan amends.
- ADR 0030 (NHTSA dedup contract) — the text-canonical identity the anti-join constraint (B) protects.
- `documentation/audit/methodology.md` — how cross-source audits are run in this project.
