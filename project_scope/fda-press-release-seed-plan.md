# FDA press-release (Tier-3) historical seed + durable deep-rescan — plan

- **Status:** Active (2026-06-07). Step 0 landed; Steps 1–4 pending.
- **Branch:** `feature/capture-fda-press-release-backlog-b`
- **Owns:** completing the FDA Tier-3 press-release historical seed (the last backlogged
  recall-data capture for production) + the FDA-shaped durability work that the universal
  `deep-rescan-reliability-plan.md` does not already own.
- **Type:** execution plan (per `documentation/documentation_model.md` type 5). The world-state
  findings it rests on live in `documentation/fda/api_observations.md` + this branch's logs;
  this doc drives what we do.

## Context

The press-release seed is the final piece of backlogged recall data for the production Neon DB.
Press-release URLs are **lookup-endpoint-only** — `GET /search/pressreleaseurls/{eventid}`, one GET
per recall event (`FdaPressReleaseDeepRescanLoader`, `src/extractors/fda_press_release.py`). The
prior attempt (June 3–4, `logs/seed_pr_*.log`, `scripts/fda_press_releases/seed_chunked.sh`) failed
for two reasons that the earlier "Akamai rate-limit" framing got wrong:

1. **It crashed on a deterministic data bug, not a throttle.** Chunk 5 fetched 12 PRs then died in
   `load_bronze` with `WithinBatchIdentityCollisionError` on event 76385 — and failed *both* retries.
   The wrapper misread it as an anti-abuse throttle and burned 2×30-min cooldowns.
2. **It swept `recall_event_id` ascending from 0**, so it spent ~9–13 h of polite 1 req/s GETs on
   the ~25 k oldest events, which carry **zero** press releases (first PR at event 76385; ~0.24 %
   yield even past it). `recall_event_id` is an internal counter, a poor proxy for recency.

Net: essentially **nothing** is in `fda_press_releases_bronze` — treat this as a greenfield seed.

## Key finding — there is no bulk shortcut (decision-grade, adversarially verified)

The per-event GET is unavoidable. Verified against primary sources (iRES usage/definitions PDFs,
openFDA field YAMLs + live records, the Bruno collection, the extractor/schema):

- iRES bulk `POST /recalls/` returns **STATUSCODE 406** for every `pressrelease*` displaycolumn
  (Finding K0); there is **no `pressreleaseindicator`** in the 33-column datagroup.
- `/search/pressreleaseurls/{eventid}` requires a **numeric path `{eventid}`** (returns 421
  otherwise) — there is no list-all / date-windowed form.
- **openFDA enforcement** (`/food`, `/drug`, `/device` enforcement) is bulk and joinable on
  `event_id`/`recall_number` but carries **no press-release URL field at all**, and there is **no
  animal/veterinary enforcement endpoint** — which matters because the one known PR (`ucm542265.htm`)
  is a CVM/AnimalVet release. The fda.gov recalls feed has URLs but no usable join key.

→ Action item: record this as a new Finding (K0.4) in `documentation/fda/api_observations.md` so the
negative is not re-investigated. The 4-call seal probe (`bruno/fda/...`) is optional; docs are
conclusive.

## Settled decisions (2026-06-07)

| Decision | Choice | Why |
|---|---|---|
| Work-list bounding | **Full sweep (no floor), recent-first by `recall_initiation_dt` DESC** | Sampler (2026-06-07, 513 events) measured **~1,760 PRs, all in 2018+** (0/312 pre-2018, Wilson upper ≤~1.2%). The PR point-estimate is identical across floors, but legacy PRs provably exist (event 76385, 2017), so the user chose the full sweep for completeness — checkpointing makes the ~17 h one-time. Recent-first front-loads the dense PRs (15 %/4 % bands first) so an interruption never loses them. |
| Execution | **Local one-time, checkpointed** | ~17 h > the GHA 6 h/job cap; checkpointing makes laptop-sleep / Neon blips cost one batch. Routine *incremental* delta wires into GHA cron afterward |
| Pacing | **Keep 1 req/s** | Proven safe over ~5 h of back-to-back chunks; respectful of the shared Akamai throttle. Bottleneck is pacing, not bandwidth — fast internet does not change it. Do not tighten on speculation |

### Sampler result (2026-06-07, `pr_yield_estimate.json`)

| stratum (recall_initiation) | sampled | with PR | yield | est. PR-events |
|---|---|---|---|---|
| pre_2012 / 2012–2017 | 312 | **0** | 0 % | ~0 |
| 2018–Oct2022 | 123 | 5 | 4.1 % | ~469 |
| post-Oct2022 | 78 | 12 | 15.4 % | ~1,106 |

**~1,575 PR-bearing events / ~1,760 press releases**, concentrated in 2018+.

## Workstreams (done-markers)

| # | Workstream | Status |
|---|---|---|
| **S0** | **Dedup fix.** Widen the `fda_press_releases` oracle to the full natural key `(source_recall_id, press_release_url, press_release_type, press_release_issued_dt)` + `allow_null_identity=True`. Pure Python contract change — **no migration** (no UNIQUE constraint on the old 2-tuple; only a non-unique covering index, migration 0022). Regression tests at the oracle level (`tests/bronze/test_dedup_contracts.py::TestFdaPressReleaseOracle`) and loader level (`tests/bronze/test_loader.py::test_within_batch_dedup_fda_pr_*`). | ✅ 2026-06-07 (ruff/format/pyright/pytest 49 green) |
| **S1** | **Checkpoint table.** Migration `0030_deep_rescan_checkpoints.py` + shared `_tables.deep_rescan_checkpoints` — keyed `(source, change_type)`, opaque JSONB `cursor` (FDA: `{"init_dt", "event_id"}`) + counters + `status`. Generalizes to any cursor-ordered sweep. | ✅ 2026-06-07 |
| **S2** | **Checkpointed driver.** `FdaPressReleaseCheckpointedSeedLoader` — each batch is a normal `run()` over the next `batch_size` events past the DB cursor, **recent-first (`recall_initiation_dt` DESC, `recall_event_id` DESC; NULL dates sort last)**; the resume cursor is co-committed in the SAME `engine.begin()` as the bronze load (can never lead committed rows). `run_checkpointed()` loops to completion + marks `complete`; resumes from the DB cursor; fail-fast on a throttle. CLI `--checkpointed [--batch-size N] [--since DATE]`. Files: `src/extractors/fda_press_release.py`, `src/cli/main.py`. Loop + cursor unit-tested (`TestCheckpointedSeedDriver`); full suite **1265 green**. | ✅ 2026-06-07 |
| **S3** | **Retire the fragile wrapper.** SUPERSEDED by S2 — the in-process `run_checkpointed()` loop removes the bash loop entirely: deterministic/throttle errors fail the process fast (no blind 30-min cooldown), resume reads the DB cursor (no `START_CURSOR` log hand-off). Run under `systemd-inhibit` directly (see below). `scripts/fda_press_releases/seed_chunked.sh` is left for the legacy ascending-id chunked path; the seed no longer uses it. | ✅ (superseded) |
| **S4** | **Bounding + sampling probes.** Written + gate-green; **run 2026-06-07** → sampler result above. `scripts/sql/fda_press_releases/bronze/probe_worklist_{date_distribution,floor_sizing}.sql`, `sample_worklist_stratified.sql` (setseed `\gset` fix), `scripts/sample_fda_press_release_yield.py` (UA fix + drift guard). | ✅ |
| **S5** | **Doc-sync + version bump.** `documentation/fda/api_observations.md` (K0.4 no-bulk-shortcut; `createdt`≈`posted_internet_dt` deferral, `/recalls/event/` out of scope); `documentation/data_schemas.md` + `documentation/architecture.md` (new `deep_rescan_checkpoints` table); `documentation/operations.md` (checkpointed-seed runbook); version bump `0.19.0 → 0.20.0` in `pyproject.toml`. | ✅ 2026-06-07 |

## Run the seed (local, one-time)

```bash
# 1. Apply migration 0030 (creates deep_rescan_checkpoints) on production
alembic upgrade head

# 2. Full recent-first sweep, resumable, under systemd-inhibit so laptop sleep can't pause it
systemd-inhibit --what=sleep:idle --mode=block --why="FDA PR seed" \
  recalls deep-rescan fda_press_releases --checkpointed --change-type historical_seed \
  2>&1 | tee logs/seed_pr_checkpointed_$(date +%Y%m%dT%H%M%S).log
```

Resumable: a throttle/crash/sleep exits the process; **re-run the exact same command** and it
continues from the committed DB cursor. `--batch-size` defaults to 250 (~5 min/batch). After it
finishes, verify idempotency once: re-run → expect `already complete` (or 0 new rows).

> **Update 2026-06-08 (v0.20.1):** the driver is now **self-healing**, superseding the original
> "fail-fast on a throttle" design in S2/S3. A transient batch failure (network drop, a plain 5xx,
> or a **`503`-with-HTML** — a cached Akamai NetStorage "Accessdata Error" origin-failover page,
> verified 2026-06-08 from the captured response, which the first overnight run mis-classified as the
> permanent fingerprint block and crashed on) is caught: the driver sleeps an escalating cooldown
> (`--cooldown-base-seconds`, default 120 s, doubling, capped at 30 min) and re-runs the batch from
> the committed cursor; after `--max-consecutive-failures` (default 6) it trips a circuit breaker and
> exits cleanly (still resumable). So an unattended overnight run survives intermittent origin errors
> rather than dying on the first blip; the re-run replayed the failed batch (incl. event 91085) clean.
> See `documentation/fda/api_observations.md` finding N.1 and the `documentation/operations.md` runbook.

## Universal deep-rescan tie-in (new vs already owned)

`deep-rescan-reliability-plan.md` already owns and shipped: Neon disconnect retry (**W5**), `NullPool`
(**W2**), GHA `timeout-minutes`/`concurrency` (**W3**), the never-advance-watermark guardrail — the
checkpointed path inherits all of these. **Genuinely new** here: the `deep_rescan_checkpoints` table +
co-committed cursor, per-batch land+load inside one run, and CLI exit-code discrimination. Note **W9/R5**
(USCG "subprocess-per-chunk + shrinking work-list") **does not apply** to FDA PRs — empty events never
drop out of the work-list, so the checkpoint table is the FDA-shaped substitute. → add a cross-referencing
**W11** row to the reliability plan.

## Open empirical gates (run before the seed)

1. **Floor sizing + legacy-PR canary** — `probe_worklist_date_distribution.sql` then
   `probe_worklist_floor_sizing.sql` (Q2 `dropped_known_old`, Q4 canary on event 76385). Quantifies
   the recall hole before picking a floor year.
2. **Stratified yield sample** — `sample_worklist_stratified.sql` → `sample_fda_press_release_yield.py`
   (paced, ~1 %, ~10 min). Confirms whether old/unknown-date strata are ~0 yield (floor defensible) or
   not (keep them). The composite floor `recall_initiation_dt >= <floor> OR all dates NULL` retains
   date-unknown events (where legacy PRs hide); the floor year comes from this output.

## Residual risks (carry forward)

- Per-date PR density is still **inferred** — the only hard yield window is one 5 k-event chunk; ~25.5 k
  high-id events were never observed. Do not trust any floor before the probes run.
- After the first seed, **verify idempotency empirically** (re-run a small chunk → expect 0 inserts),
  exactly as NHTSA did at W6 — the datetime-in-identity canonicalization is proven by the NHTSA
  precedent but should be confirmed once on real data.

## Related
- `documentation/fda/api_observations.md` (Findings K0–K0.3; add K0.4)
- `documentation/audit/deep_rescan_reliability_audit.md` + `project_scope/deep-rescan-reliability-plan.md`
- `documentation/audit/capture_expansion_backlog.md` (FDA Tier-3 row)
- ADR 0030 (dedup contracts), ADR 0007 (bronze snapshots/lineage)
