# Post-go-live residual validation runbook

Two GitHub Actions deep-rescan legs must be proven before their **safety-net crons** are relied upon.
Neither blocks the daily go-live (the daily extract + transform crons, gated by `CRON_ENABLED` / C40) —
both are deferred to just before the *deep-rescan* crons they validate. They are the H-b residual legs
from [`go_live_simulation.md`](go_live_simulation.md).

> **No code changes expected.** Both legs validate functionality that is already built and merged (the
> workflows, the CLI flags, the restricted role). A code/config change is warranted **only** if a run
> reveals a bug — or, for Leg 2, if the shard exceeds its time cap (and even that fix is a `--shard-count`
> bump, a dispatch input / cron default, not new code).

---

## Leg 1 — CPSC deep-rescan GHA proof

**Why.** Every source was deep-rescan-*seeded locally*, so the **Actions path** — restricted `recalls_app`
role + GitHub secrets + `workflow_dispatch` — is still unproven end-to-end. CPSC is the cheapest proof:
~9,800-row corpus, public source (no auth secrets), `timeout-minutes: 30`.

**Run.**
1. Confirm the repo has the `NEON_DATABASE_URL` (→ `recalls_app`) + `R2_*` secrets — the workflow's
   "Validate required secrets" step fails fast if any are missing.
2. Actions → **Deep Rescan CPSC Recalls** → **Run workflow** (`workflow_dispatch`) → `change_type: routine`.
3. Let it finish under the 30-min cap.

**Pass = all of:**
- "Validate required secrets" passes; `recalls deep-rescan cpsc` runs to completion **as `recalls_app`**
  (proves the restricted role works in CI).
- An `extraction_runs` row lands with `change_type=routine` and a **sane `records_inserted`** — small / ≈0
  on a re-run, since content-hash dedup absorbs the already-seeded corpus.
- `source_watermarks` is **unchanged** (deep-rescan never advances the watermark — by design).

**Needed before:** the weekly CPSC deep-rescan cron is relied upon (it's `CRON_ENABLED`-gated; C40).

**If it reveals a problem:** missing/misnamed secret → fix the repo secret (no code). Permission error as
`recalls_app` → a grant gap; run `scripts/sql/_pipeline/verify_recalls_app_grants.sql`. A genuine logic bug
→ fix + re-run.

---

## Leg 2 — USCG-detail `--shard` timing check

**Why.** `uscg_manufacturer_details` is too large to deep-rescan as one GHA job (> 6 h), so it runs a
**stateless 1/3 shard per month** (`--shard N --shard-count 3`, index derived from `(month-1) % 3`),
covering the full corpus each quarter. The monthly job carries `timeout-minutes: 330` (5.5 h). Confirm a
real 1/3 shard finishes under that cap on GHA before trusting the cron.

**Run.**
1. Actions → **deep-rescan-uscg-manufacturers-detail** → **Run workflow** → `shard: 0`, `shard_count: 3`,
   `change_type: routine` (or leave `shard` blank to derive it from the calendar month).
2. Time the run.

**Pass = both:**
- The 1/3 shard completes **under 330 min** (the workflow header estimates ~1.5–2.6 h — comfortable margin).
- `records_inserted` is sane (only the changed MICs in that shard's stride).

**Needed before:** the monthly USCG-detail deep-rescan cron is relied upon.

**If it reveals a problem:** if a shard approaches/exceeds 330 min, **raise `--shard-count`** (e.g. 4 or 6 =
smaller shards, full corpus over more months). That is a dispatch input + cron-default change, **not new
code** — the stateless month-derived rotation handles any `shard_count` automatically. A genuine bug →
fix + re-run.

---

## After both are green

- Record each result (run URL + wall-clock) in [`go_live_simulation.md`](go_live_simulation.md)'s H-b
  residual-legs table.
- The crons themselves stay `CRON_ENABLED`-gated until flipped on at go-live (C40).

**See also:** the two workflow files (`.github/workflows/deep-rescan-cpsc.yml`,
`.github/workflows/deep-rescan-uscg-manufacturers-detail.yml`), ADR 0010 (cron grid), and
`operations.md` (restricted-role runbook + scheduling).
