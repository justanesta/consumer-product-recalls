# FDA iRES historical-seed (full-corpus) implementation plan

- **Status:** Design locked 2026-05-31; not yet implemented. Code off this doc.
- **Phase:** 6a.5 historical backfill — the FDA leg (the last and trickiest source).
- **Branch:** `feature/phase-6a5-historical-backfill` (this branch).
- **Triggering analysis:** the 2026-05-31 skeptical workflow audit (run `w9fhhkl5u`) +
  the live pre-seed probes (`scripts/fda/audit/probe_seed_query_shape.py`).
- **Companions:**
  - `project_scope/phase-6-execution-plan.md` (§6a.5 — the FDA row this supersedes)
  - `documentation/fda/api_observations.md` (Findings A–O, K0, K0.2 — the source of truth)
  - `documentation/fda/field_audit_2026_w22.md` (§2 capture, §7a SHIP fields, migration 0019)
  - `scripts/fda/audit/probe_seed_query_shape.py` + `probe_corpus_completeness.py`

---

## 1. Context — why this exists

The Phase 6a.5 FDA seed was originally framed as "run `recalls deep-rescan fda` with an
early `--start-date`." The skeptical audit + live probes proved that path is **NO-GO as
wired** — it would silently land an incomplete corpus and report success:

- The FDA bulk POST filters on `eventlmd` (event last-modified). Per `api_observations.md`
  Finding H the `*lmd` columns are **null for un-edited records**, so an `eventlmdfrom`
  date window (NULL fails `>=`) **excludes** them. Live probe: unfiltered corpus =
  **134,450**; `eventlmdfrom=01/01/1900` window = **134,253** → the window silently drops
  **197** rows.
- Worse, even the no-filter `filter:"[]"` path (which *does* return all 134,450) would
  **quarantine those 197**, because `event_lmd` is non-nullable in both the schema
  (`schemas/fda.py:93`, `_FdaDate` raises on `None`/`""`) and the bronze table
  (`0004_fda_bronze.py:46`, `nullable=False`). The rejection is **silent**: 197/134,450 =
  0.15% « the 5% `rejection_threshold` (`_base.py`), and `records_landed = len(raw_records)`
  reports the fetched count, not the inserted count. You'd see "success, 134,450" with
  134,253 actually in bronze — and the 197 dropped are the very records the no-filter seed
  exists to capture.
- Three other "core" fields (`center_cd`, `product_type_short`, `firm_legal_nam`) are
  non-nullable on the same now-falsified "core identifiers never null" assumption
  (`api_observations.md:374`), so they carry the same silent-quarantine risk.
- `recalleventid` is non-unique (multi-product events share it; Finding F), and the server's
  tiebreaker is **not deterministic across requests** — the boundary probe found a
  `productid` straddling two adjacent 2500-row pages on a re-run → **duplicate-insert risk**,
  with `within_batch_dedup` currently OFF and **no unique constraint** on the table.

This plan implements the corrected, verified seed.

---

## 2. Empirically verified facts (do not re-derive)

Live probe results, 2026-05-31, against `POST https://www.accessdata.fda.gov/rest/iresapi/recalls/`:

| Fact | Evidence | Grade |
|---|---|---|
| Unfiltered corpus (`filter:"[]"`) = **134,450**; `RESULTCOUNT` is whole-dataset regardless of `rows` | probe_corpus_completeness + Finding E | empirical |
| `eventlmd` window misses **197** (null-`eventlmd`) rows | 134,450 − 134,253 | empirical |
| Full **32-col `_DISPLAY_COLUMNS`** is a valid bulk-POST datagroup (no 406) | Probe 1: STATUSCODE 400 | empirical |
| `sort=recalleventid asc` is a **stable total order** but has **ties** (`25001` twice) | Probe 2 | empirical |
| `recalleventid` tie boundaries are **non-deterministic** across requests (0 straddle, then 1: `productid 28785`) | Probe 3 (two runs) | empirical |
| `sort=recallinitiationdt desc` returns **empty / is unreliable** on bulk POST | Probe 2 (returned `[]`) | empirical |
| **5s/page** pacing → no throttle over 5 pages; full seed ≈ **54 pages ≈ ~5–6 min** | Probe 4 | empirical |
| `rid` is excluded from `content_hash` (`fda.py` `hash_exclude_fields={'rid'}`) → straddle copies of a row are byte-identical | code | empirical |
| The 197 null-`eventlmd` rows are **structural** (old archive), not transient self-healing | inferred (Finding M-extension); confirm post-seed via SQL | inferred |
| `event_lmd` floor "01/01/2023" / recall floor "01/01/2003" from earlier probes are **lexically poisoned** (date strings sort lexically), hence meaningless | Probe 2 empty result + the 12/31/2015 anomaly | confirmed-unsound |

**Implications:** seed with `filter:"[]"` (not a date window); make the 4 core fields nullable
so nothing silently quarantines; `within_batch_dedup=True` is mandatory; pace ≥5s/page; verify
completeness post-seed with a `COUNT(DISTINCT)` gate (do not trust `status=success`).

---

## 3. The locked seed-query shape

```
recalls deep-rescan fda --change-type=historical_seed       # NO --start-date/--end-date → full corpus

POST {base}/recalls/?signature=<unix_ts>
payLoad = {
  "displaycolumns": <the 32-col _DISPLAY_COLUMNS, fda.py:137 — unchanged, Probe 1 valid>,
  "filter":         "[]",                 # no eventlmd window → includes the 197 null-eventlmd rows
  "start":          1, then += 2500,
  "rows":           2500,                 # codeinformation page cap (_PAGE_SIZE, fda.py:127)
  "sort":           "recalleventid",
  "sortorder":      "asc"
}
paginate until len(page) < 2500           # ~54 pages, 5s inter-page sleep
load_bronze:  within_batch_dedup=True     # collapses tie-boundary straddle dups
```

`recalleventid asc` keeps the audit's choice (monotonic/append-only → new recalls sort to the
tail, no offset shift). Its tie-boundary non-determinism (Probe 3) is handled by
`within_batch_dedup` (dups) + the post-seed gate (rare drops). See §12 for the optional
`productid`-sort hardening.

---

## 4. Code changes (file by file)

> Line numbers are as of 2026-05-31; re-confirm at code time (`_DISPLAY_COLUMNS`/0019 shifted them).

### 4.1 `migrations/versions/0020_fda_core_fields_nullable.py` (NEW)

`revision="0020"`, `down_revision="0019"`. Drop `NOT NULL` on the four core fields so the
no-filter seed lands every row instead of silently quarantining nulls:

```python
_TABLE = "fda_recalls_bronze"
_COLS = ("event_lmd", "center_cd", "product_type_short", "firm_legal_nam")

def upgrade():
    for c in _COLS:
        op.alter_column(_TABLE, c, nullable=True)

def downgrade():
    for c in _COLS:
        op.alter_column(_TABLE, c, nullable=False)   # fails if nulls exist — acceptable for a rare downgrade
```

The `event_lmd` index (`ix_fda_recalls_bronze_event_lmd`, `0004:74`) is unaffected — a nullable
column indexes fine. Docstring must cite: the 197 null-`eventlmd` empirical finding, the
falsified "core never null" assumption (`api_observations.md:374`), the "permissive bronze /
strict silver" policy (ADR 0014), and that this lands *before* the seed (empty table → no backfill,
no content-hash churn).

### 4.2 `src/schemas/fda.py` — make the 4 fields nullable

| Line | From | To |
|---|---|---|
| 91 | `center_cd: str = Field(validation_alias="CENTERCD")` | `center_cd: str \| None = Field(default=None, validation_alias="CENTERCD")` |
| 92 | `product_type_short: str = Field(validation_alias="PRODUCTTYPESHORT")` | `... str \| None = Field(default=None, ...)` |
| 93 | `event_lmd: _FdaDate = Field(validation_alias="EVENTLMD")` | `event_lmd: _FdaNullableDate = Field(default=None, validation_alias="EVENTLMD")` |
| 94 | `firm_legal_nam: str = Field(validation_alias="FIRMLEGALNAM")` | `... str \| None = Field(default=None, ...)` |

`_FdaNullableDate` already exists (line 64). These move from the "core identifiers" block to the
"nullable scalars" block. Update the class docstring (the "core identifiers — non-nullable"
comment is now false for these four).

### 4.3 `src/extractors/fda.py`

**(a) Guard `max(event_lmd)` on the incremental path (latent crash).** `FdaExtractor.load_bronze`
line 292 does `max(r.event_lmd for r in records).date()` → `TypeError` once `event_lmd` can be
`None`. Change to:
```python
dates = [r.event_lmd for r in records if r.event_lmd is not None]
if dates:
    self._update_watermark(conn, max(dates).date())
```
(The deep-rescan overrides `load_bronze` and skips the watermark, so the *seed* is unaffected —
but this is a real incremental-path bug the nullability change exposes; fix it in the same PR.)

**(b) Per-page pacing + retry in `_paginate` (fda.py:298).** Add an extractor field
`inter_page_sleep_seconds: float = 0.0` (0 = no pacing, the incremental default). In the loop:
wrap the `_fetch_page` call so a transient retries **that page** (not the whole sweep — addresses
audit C11 amplification), and `time.sleep(inter_page_sleep_seconds)` between pages when > 0.
Keep the `len(page) < _PAGE_SIZE` terminator (Probe 3 confirmed page A returns a full 2500).

**(c) `FdaDeepRescanLoader` full-corpus mode (fda.py:535).** Add:
```python
_full_corpus: bool = PrivateAttr(default=False)
inter_page_sleep_seconds: float = 5.0          # Probe 4 floor

def set_full_corpus(self) -> None:
    self._full_corpus = True

def extract(self):                              # replaces fda.py:562
    if self._full_corpus:
        return self._paginate("[]", sort="recalleventid", sortorder="asc")
    # else: existing eventlmdfrom/eventlmdto window (unchanged)
```
**(d) `within_batch_dedup=True` in `FdaDeepRescanLoader.load_bronze` (fda.py:570).** Add it to the
`BronzeLoader(...)` construction (identity stays `source_recall_id`; `rid` already hash-excluded
so straddle copies collapse byte-identically rather than raising `WithinBatchIdentityCollisionError`).

### 4.4 `src/cli/main.py` — `deep-rescan fda` no-dates = full corpus

The current check (lines 327–329) **requires** `--start-date`/`--end-date` for fda. Relax to:
- both dates given → `loader.set_date_range(...)` (existing windowed re-pull, kept for targeted use)
- neither given → `loader.set_full_corpus()` (the historical seed)
- exactly one given → error ("provide both for a window, or neither for the full-corpus seed")

Pacing comes from the loader's `inter_page_sleep_seconds` default (5.0); optionally expose a
`--page-sleep` override on `deep-rescan` if convenient.

### 4.5 `scripts/sql/fda/bronze/seed_completeness_gate.sql` (NEW)

The real completeness check (run post-seed; `status=success` is masked — audit §3 #2). Sections:
1. **Row + distinct counts:** `count(*)`, `count(distinct source_recall_id)` in `fda_recalls_bronze`
   for the seed run. Operator compares `count(distinct source_recall_id)` to the API `RESULTCOUNT`
   captured at run start (the run log / a `probe_corpus_completeness.py` call). A shortfall = a
   tie-boundary drop → re-run the seed (content-hash dedup makes it idempotent; non-deterministic
   boundaries mean the dropped row lands on a re-run).
2. **Within-run duplicate check:** `source_recall_id` grouped over the latest `extraction_timestamp`
   `HAVING count(*) > 1` (should be 0 — `within_batch_dedup` collapsed straddles).
3. **Null-field census (sort-immune; settles C9/C10 for free):** counts of null `event_lmd` /
   `center_cd` / `product_type_short` / `firm_legal_nam`, plus a `extract(year from recall_initiation_dt)`
   histogram of the null-`event_lmd` rows — confirms whether the ~197 are old/structural or recent.

### 4.6 Tests

- `tests/schemas/test_fda_schema.py`: `event_lmd: None` and `""` now **validate to `None`** (were
  rejected); same for the 3 str fields; the existing required-field tests for these four are removed.
- `tests/extractors/test_fda_extractor.py`: `set_full_corpus()` → `extract()` posts `filter:"[]"`,
  `sort=recalleventid asc` (respx/mock `_fetch_page`); `FdaDeepRescanLoader.load_bronze` constructs
  `BronzeLoader(within_batch_dedup=True)`; `_paginate` sleeps when `inter_page_sleep_seconds>0` and
  retries a single page on a transient without restarting; `FdaExtractor.load_bronze` no longer
  raises when a record has `event_lmd=None` (and skips the watermark update).
- `tests/cli/test_main.py`: `deep-rescan fda` no-dates → `set_full_corpus()` called; one-date → exit
  with the new error; both-dates → `set_date_range()` (existing).
- **VCR caveat:** confirmed safe — FDA cassettes don't match on request body (`tests/conftest.py`
  has no `match_on`; the FDA override only filters `signature`), and the 21-field recorded responses
  still validate (new fields optional). No re-recording needed.

### 4.7 Documentation

- `documentation/fda/api_observations.md`: **new Finding (next letter)** consolidating the
  2026-05-31 corpus probe: 134,450 unfiltered vs 134,253 windowed; the 197 null-`eventlmd`;
  `recalleventid` tie-boundary non-determinism; `recallinitiationdt`-sort unreliable; the lexical
  date-sort that poisons sorted top-N; the 5s pacing floor; and **correct the stale "core
  identifiers never null" claim at line 374** (the 197 falsify it).
- `documentation/fda/field_audit_2026_w22.md`: note the full-corpus seed strategy + migration 0020.
- `project_scope/phase-6-execution-plan.md`: rewrite the FDA execution row — `recalls deep-rescan fda
  --change-type=historical_seed` (no dates), `filter:"[]"`, migration 0020 prereq, ~54 pages, the
  post-seed gate.

---

## 5. Resumability / per-page design — a decision to confirm

You chose "per-page checkpoint" when the FDA seed was framed as a 54-page sweep with high throttle
risk. Probe 4 changed the calculus: at 5s/page the **full seed is ~5–6 minutes**, and re-runs are
content-hash-dedup-safe. A *persisted* cross-invocation checkpoint buys almost nothing here, because
on any re-run the fetch (the expensive, throttle-exposed step) re-happens regardless — only the
re-*insert* would be skipped, which is cheap.

**Recommended (this plan):** per-page **retry** (§4.3b) to bound the in-run amplification (C11) +
5s pacing + the dedup-safe re-run as the "resume." No persisted offset, no per-page-commit refactor.

**If you still want a true checkpoint** (resume without re-fetching): switch the loader to per-page
**load-and-commit** (fetch page → validate → `load_bronze` that page → next), so a throttle abort
leaves completed pages committed. This breaks the standard `run()` extract→land→validate→load
lifecycle and adds real complexity/test surface for a 5-minute job — flagged here so you can pick at
code time. Default to the recommended unless you say otherwise.

---

## 6. Seed run procedure (after the code + migration land)

```bash
# 0. on main (verify: scripts/sql/_pipeline/whoami.sql), migrations current:
alembic upgrade head                 # applies 0020; alembic current → 0020

# 1. capture the corpus size to gate against (cheap):
python scripts/fda/audit/probe_corpus_completeness.py        # note RESULTCOUNT (~134,450)

# 2. seed (off-peak; ~5–6 min; watch for a text/html throttle → wait 30+ min, re-run):
recalls deep-rescan fda --change-type=historical_seed        # no dates → full corpus

# 3. GATE (do not trust status=success):
psql -f scripts/sql/fda/bronze/seed_completeness_gate.sql
#    PASS: count(distinct source_recall_id) == the RESULTCOUNT from step 1; 0 within-run dups.
#    SHORTFALL: a tie-boundary drop → re-run step 2 (idempotent) and re-gate.
psql -f scripts/sql/_pipeline/seed_verify.sql                # fda_recalls_bronze row count
```

---

## 7. Quality gates (this PR)

- `ruff check` + `ruff format --check` + `pyright` clean on all changed `src/`, `scripts/`, `tests/`.
- Full `pytest` green (the schema-nullability change touches widely-imported modules).
- `alembic heads` shows `0020` as the single head; `0019 → 0020` chain parses.
- The new SQL is referenced from the seed procedure (no inline multi-line SQL).
- Version bump consideration (`pyproject.toml`) when this lands — your call.

---

## 8. Residual risks & open decisions

| Item | Disposition |
|---|---|
| **Tie-boundary DROP** (rare; recalleventid non-determinism) | Caught by the §4.5 `COUNT(DISTINCT)` gate; healed by an idempotent re-run (different boundaries). Optionally eliminated by sorting on the unique `productid` — needs one probe (`sort=productid` valid? stable?) before relying on it. Not blocking. |
| **The 197 are structural vs transient** (inferred) | Settled for free post-seed by §4.5's null-`event_lmd` year histogram. Doesn't change the seed (full-corpus captures them either way); affects only whether the daily incremental would ever pick them up (it won't, if structural). |
| **Per-page checkpoint depth** | §5 decision — defaulting to per-page retry; confirm or upgrade to per-page-commit at code time. |
| **Akamai throttle on ~54 pages** | Low at 5s/page (Probe 4). Detected by the `text/html` guard (non-retryable abort); recovery = wait 30+ min, re-run (dedup-safe). |
| **`productid`-sort hardening** | Optional future improvement; one probe to validate, then swap the sort + keep dedup as belt-and-suspenders. |

---

## 9. Sequencing

- Lands on **`feature/phase-6a5-historical-backfill`** (this branch), alongside the CPSC deep-rescan
  loader + the FDA capture expansion (migration 0019) already shipped here.
- The FDA **seed run** is the last of the 6a.5 seeds (after CPSC ✅, USDA ✅, USDA-est ✅, NHTSA ✅,
  and the three USCG seeds). It is **not** a prerequisite for any other 6a.5 source.
- `feature/silver-field-remap` (next branch) needs full-corpus FDA **bronze**, so this seed must
  complete before that branch's silver decisions — but the FDA **silver mapping** of the migration-0019
  capture-expansion fields stays deferred to the (b) PR (per `field_audit_2026_w22.md` §6 decision 4).

---

## 10. Implementation checklist

- [ ] `0020_fda_core_fields_nullable.py` (event_lmd, center_cd, product_type_short, firm_legal_nam)
- [ ] `schemas/fda.py` — 4 fields nullable + docstring
- [ ] `fda.py` — guard `max(event_lmd)`; `_paginate` pacing+per-page-retry; `set_full_corpus()`; `within_batch_dedup=True`
- [ ] `cli/main.py` — `deep-rescan fda` no-dates → full corpus; one-date error
- [ ] `scripts/sql/fda/bronze/seed_completeness_gate.sql`
- [ ] tests: schema / extractor / cli
- [ ] docs: api_observations Finding + field_audit + phase-6-execution-plan FDA row
- [ ] gates: ruff / pyright / pytest / alembic heads
- [ ] (user) `alembic upgrade head` → seed → gate
