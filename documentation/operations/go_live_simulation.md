# Go-live production simulation — validation record (WS-H)

- **Status:** In progress (fill in) — branch `feature/pre-go-live-validation`
- **Purpose:** the expected-vs-actual audit trail for WS-H-a (2–3 day daily simulation) and
  WS-H-b (deep-rescan validation), per `project_scope/phase-7-production-plus-todos-plan.md`.
  This consolidated cross-source record is the operator artifact reviewed before flipping the
  cron on (C40). *(The plan phrases this as per-`documentation/<source>/` docs; one consolidated
  operations doc is kept here as the more reviewable go-live gate — split per-source if preferred.)*

## How the numbers are produced (no re-runs needed)

The **actuals are already banked** in `extraction_runs` + the bronze tables — pull them with the
existing probe, don't re-extract:

```bash
pwr psql -f scripts/sql/_pipeline/simulation_daily_counts.sql   # Section A: per-run log (last 3 days); Section B: cumulative bronze rows/source
pwr psql -f scripts/sql/_pipeline/recent_runs.sql               # latest run per source — health at a glance
pwr psql -f scripts/sql/_pipeline/watermark_health.sql          # watermark advanced on incremental / UNCHANGED on deep-rescan
```

`records_inserted` (= "records_loaded") is the volume figure. `records_rejected` routes to the
`*_rejected` audit tables — a non-zero count is a flag to investigate, not necessarily a failure.

---

## WS-H-a — daily simulation (extract → transform), 2–3 consecutive days

Each simulation day = the full grid: the 5 daily extracts **then** the transform sequence
(`dbt build` → `resolve-firms` → `parse-quantities` → `dbt build` → `dbt snapshot` → `dbt test`).
NHTSA is Mon–Fri in prod but harmless to run daily here.

### Loaded counts per source per day (`records_inserted`)

Pulled from `simulation_daily_counts.sql` 2026-06-13. `records_inserted` shown. **06-10 is a
post-week-gap catch-up, not steady state**; the clean steady-state window is **06-11 → 06-13**.

| Source | 06-10 (post-gap) | 06-11 | 06-12 | 06-13 | Sane? | Notes |
|---|---|---|---|---|---|---|
| cpsc | 4 | 5 | 1 | 16 | ✓ | small deltas; extract≫load = content-hash dedup |
| fda | 37 | 41 | 25 | 78 | ✓ | small deltas |
| usda | 2 | — *(not run)* | 1 | 1 | ✓ | full-snapshot extract (2,006), tiny delta; **06-11 skip accepted** — USDA re-pulls the entire current snapshot each run (no incremental cursor to leave a gap), so a missed day self-heals on the next run; steady state confirmed by 06-12/13 |
| uscg | 0 *(37m full fetch)* | 0 *(s/c 2s)* | 0 *(s/c 2s)* | 0 *(s/c 2s)* | ✓ | two-gate short-circuit fires 06-11→13; 06-10 post-gap full fetch loaded 0 |
| nhtsa | 1,857 | 86 | 287 | 77 | ✓ | post-gap catch-up 06-10; **dedup 17min→~1min from 06-12 (ADR 0041 live)**; `was_short_circuited=f` (content changes daily) |

> Expectation note: the incremental path scales with the *delta*, not the corpus — most days are
> small. A day at ~corpus size on any source is the over-insert signature (none observed here).
> Non-obvious but correct: NHTSA `extracted` 241,859 (POST_2010 archive) vs bronze 323,899
> (+PRE_2010 ~82k); USDA bronze 4,015 vs extracted 2,006 (append-only version accumulation).

### Transform green

Daily transforms ran green across the window (operator-confirmed). Authoritative **final-tree**
run 2026-06-13 — includes the CPSC `recall_product` key migration + append-only→`error`:

| Step | Result |
|---|---|
| `dbt build` (pre-resolve) | PASS=47 WARN=0 ERROR=0 (3m39s) — `firm_uscg_attributes_snapshot` banked **11** SCD-2 versions (the MIC reassignments) |
| `recalls resolve-firms` | written=29,494 · fuzzy_merged=7,692 |
| `recalls parse-quantities` | written=58,780 · parsed_value=57,857 · parsed_unit=44,559 |
| `dbt build` (post-resolve) | PASS=47 WARN=0 ERROR=0 (1m05s) |
| `dbt snapshot` | PASS=4 WARN=0 ERROR=0 — all `INSERT 0 0` (idempotent) |
| `dbt test` | **PASS=305 WARN=7 ERROR=0** — 7 known informational monitors; **CPSC append-only (now `error`) PASS**, `recall_product_id` unique + FK PASS |

### Weekly-cadence jobs — run **once** across the window (real cadence)

One-liner (see "Commands" below):

| Source | Run? | `records_inserted` | Sane? | Notes |
|---|---|---|---|---|
| uscg_manufacturers | ✓ 06-13 | 11 | ✓ | first-post-seed full walk (36m, NULL baseline — one-time); 11 MIC edits incl. ~8 reassignments |
| uscg_manufacturer_details (incremental Tier-2) | ✓ 06-13 | 11 | ✓ | fetched exactly the 11 changed MICs (~12s), not the full ~16k — incremental chaining off the manufacturers delta |
| usda_establishments | ✓ 06-13 | 112 | ✓ | fetched 8,001 (full dump), delta 112 |
| fda_press_releases (plain — NOT `--checkpointed`) | ✓ 06-13 | 0 | ✓ | fetched 2, nothing new |

---

## WS-H-b — deep-rescan validation

**Approach:** every source was already deep-rescan-seeded, so H-b is satisfied by *verifying* those
seeds rather than re-running them (a full local re-seed is dominated by `uscg_manufacturer_details`'s
>6 h sweep). Seed-data validation **green** 2026-06-13 (`seed_verify.sql` + `watermark_health.sql`):

- **Migrations at head** (alembic `0033`). **All 8 bronze tables at full-corpus counts** — cpsc 9,854 ·
  fda 134,642 · nhtsa 323,899 · uscg_manufacturers 16,274 · uscg_manufacturer_details 16,274 ·
  uscg_recalls 1,763 · usda_establishments 8,091 · usda_recalls 4,015 — all meet/exceed the 6a.5 gates.
- **Watermark coverage + discipline:** every source has a watermark row; the incremental path advances
  it correctly (each 06-13 routine run = "advanced this run"), and the historical_seed runs did not
  clobber the cursors (they track the routine values). ✓

**Residual legs — sequencable; none blocks the daily-extract go-live:**

| Leg | Why it isn't covered by the seed verification | When it's actually needed |
|---|---|---|
| GHA deep-rescan workflow proof — dispatch **one** cheap source (e.g. `deep-rescan-cpsc`) via the Actions UI | seeds were run locally → the Actions path (restricted role + secrets + dispatch) is unproven | before the first **monthly** deep-rescan safety-net cron fires |
| ~~NHTSA presence (#71)~~ **DONE 2026-06-13** | a fresh `historical_seed` deep-rescan (run `26601d0b`) banked the manifest — 30,075 campnos; `verify_nhtsa_presence_closed.sql` → **PASS** (all 30,075 NHTSA recalls presence-populated, untracked sources clean). | #71 is data-valid; flips to `[x]` at H-f |
| USCG-detail `--shard 0 --shard-count 3` timing **on GHA** | confirm a 1/3 shard finishes under `timeout-minutes: 330` (raise `--shard-count` if long) | before its monthly cron |

---

## Commands (operator)

```bash
# Daily grid (each sim day) — extracts then transform
pwr bash -c 'for s in cpsc fda usda uscg nhtsa; do echo "== extract $s =="; recalls extract "$s"; done'
pwr bash -c 'dbt build --exclude-resource-type test && recalls resolve-firms && recalls parse-quantities && dbt build --exclude-resource-type test && dbt snapshot && dbt test'

# Weekly jobs — once across the window (one line)
pwr bash -c 'for s in uscg_manufacturers uscg_manufacturer_details usda_establishments fda_press_releases; do echo "== extract $s =="; recalls extract "$s"; done'

# Read the volumes back (no re-run)
pwr psql -f scripts/sql/_pipeline/simulation_daily_counts.sql
```

---

## Sign-off → go-live

- [x] H-a: 2–3 days of sane loaded counts + green transform recorded above
- [x] H-b (seed-data): seeds verified green (head 0033, full-corpus counts, watermark discipline). **Residual legs tracked above** — GHA workflow proof · NHTSA presence (#71) · USCG-detail shard timing — sequenced before their respective monthly crons; none blocks daily go-live
- [x] H-d: `pre-commit run --all-files` green (7/7) · #44 Dependabot pre-commit ecosystem confirmed · C21 overlay disposition documented (`development.md`, `implementation_plan.md`)
- [ ] H-e: version bumped (`pyproject.toml`)
- [ ] H-f: this doc + per-source results committed · TODO #71 **ready to close** (post-C16 NHTSA full-enum run verified 2026-06-13 — `verify_nhtsa_presence_closed.sql` = PASS)
- [ ] **C40:** flip `CRON_ENABLED` repo variable → `true`
