# Quarantine-recovery tooling (`recalls recover-rejected`) — rationale & execution plan

- **Status:** Design proposed 2026-06-01; not yet implemented. Code off this doc.
- **Branch:** `feature/quarantine-recovery-tool` (cut off `main` on 2026-06-01 after
  `feature/phase-6a5-historical-backfill` merged as PR #44). Cross-cutting infra/tooling,
  deliberately **off** the 6a.5 backfill branch.
- **Triggering analysis:** the 2026-06-01 FDA full-corpus seed quarantined 24 product rows
  (14 recall events) that the census (`scripts/sql/fda/bronze/explore_seed_rejections.sql`)
  proved were *genuine* recent recalls killed by a source year-typo (see §1). Recovering
  them produced a one-off, FDA-specific script that begs to be generalized.
- **Companions:**
  - `scripts/fda/recover_rejected_invariant_records.py` (the FDA one-off this generalizes)
  - `scripts/sql/fda/bronze/explore_seed_rejections.sql` (the census that justifies recovery)
  - `src/bronze/loader.py` (`BronzeLoader.load` — the load primitive reused verbatim)
  - `src/bronze/invariants.py` (`check_date_sanity` — the *shared* gate that makes this a
    cross-source need, not an FDA quirk)
  - ADR 0013 (quarantine routing), ADR 0014 (re-ingestion — the **complementary** mechanism, §5),
    ADR 0007 (content-hash dedup — what makes recovery idempotent)

---

## 0. Post-validation amendments (2026-06-01, validation workflow `wp27oa1gt`)

A 7-agent validation workflow checked every claim and design choice against the code. **Verdict:
the core thesis is sound and the key empirical claims hold** (rejected-table uniformity, datetime
introspection, lossless round-trip, watermark isolation), but the implementation approach needs the
corrections below. **§0 overrides the prose sections where they differ.**

### 0.1 PIVOT — explicit `RecoveryConfig` map, NOT the §4.2 `make_bronze_loader` refactor
§4.2's instance-method `make_bronze_loader()` is rejected: instantiating an extractor merely to read
a static loader config triggers `model_post_init` → `sa.create_engine()` + `R2LandingClient()` (heavy,
env-requiring side effects, `fda.py:207-212`). Worse, **NHTSA cannot be represented by one accessor** —
its incremental loader (11-tuple identity + `within_batch_dedup` + `allow_null_identity` +
`hash_exclude={source_recall_id}`, `nhtsa.py:461`) and its deep-rescan loader (bare `('source_recall_id',)`,
`nhtsa.py:716`) differ fundamentally. **Decision: option (b) becomes plan A.** Define an explicit
`RECOVERY_CONFIG_BY_SOURCE_NAME` map in `src/bronze/recovery.py`; each entry is a frozen
`RecoveryConfig(record_model, bronze_table, rejected_table, loader_factory)` whose `loader_factory` is a
zero-arg callable returning a `BronzeLoader` built with that source's args copied **verbatim** from its
extractor. The 8-extractor / 16-call-site refactor is **dropped from v1** (revisit only if a second
consumer emerges). The module-level `_<source>_bronze` / `_<source>_rejected` Table objects import with
zero instantiation.

### 0.2 SCOPE — 5 date-sanity sources only
Only **fda, cpsc, usda (recall), nhtsa, uscg (recall)** call `check_date_sanity`. The three non-recall
sources (`uscg_manufacturers`, `uscg_manufacturer_details`, `usda_establishments`) call only
`check_null_source_id`, so they cannot produce the date-typo class this tool recovers. The map + tests
cover exactly those 5; an unlisted source → "not implemented for source" exit-1.

### 0.3 NHTSA RecoveryConfig uses the INCREMENTAL config
NHTSA recovery uses the incremental args (11-tuple identity, `hash_exclude={source_recall_id}`,
`within_batch_dedup=True`, `allow_null_identity=True`), NOT the deep-rescan single-column config (a latent
bug). Encoded explicitly in the map.

### 0.4 Core-function corrections (§4.1)
- `datetime_field_names()` MUST treat **both** `typing.Union` and `types.UnionType` (Pydantic 2 stores
  `datetime | None` as `types.UnionType`; a `typing.Union`-only check returns ∅ for every Annotated date
  field — verified).
- `recover_quarantined()` gains a `bronze_table` param and reproduces the FDA one-off's three load-bearing
  behaviors: (a) **seed-sibling lineage** — default `extraction_timestamp` = `min(extraction_timestamp)`
  from bronze WHERE `raw_landing_path` = the run; (b) **landing_path scoping** (default = the source's
  most-recent rejection path); (c) predicate gates on `failure_stage == 'invariants'` AND the reason
  substring (reason-only would also match validate-stage rows).

### 0.5 Schema fix — `populate_by_name=True` on the USCG schemas
`UscgRecallRecord` (`uscg.py:151`) lacks `populate_by_name=True`, so reconstruct's `model_validate(dump)`
fails (dump emits field names; only validation aliases are accepted). Add it to `UscgRecallRecord`
(required — uscg is in scope) and, for consistency, to `UscgManufacturerRecord` /
`UscgManufacturerDetailRecord`. Non-breaking: production ingestion passes alias keys.

### 0.6 CLI (§4.4)
Build the engine directly from `Settings()` (no extractor instantiation). Do **not** call
`_print_run_summary` (it reads `records_fetched/loaded/...` which `RecoveryResult` lacks) — print a bespoke
`<source> recover-rejected: candidates=N inserted=M` line. Guard unknown / out-of-scope source with exit-1,
mirroring `deep-rescan`.

### 0.7 Doc corrections
- Branch is `feature/quarantine-recovery-tool` (not `-cli`); 6a.5 merged as **PR #44** — §9/§10 gating
  language is already satisfied.
- `raw_record` is `postgresql.JSONB` only on cpsc/fda; **`sa.JSON` on the other 6** → recovery uses
  dialect-agnostic JSON access (no JSONB operators). §2.1's "JSONB uniformly" corrected.
- USDA **bilingual-pairing** rejects (`check_usda_bilingual_pairing`) are a *different* class — explicitly
  NOT recoverable here (fix is re-extraction); documented so `--reason-contains` isn't misused.
- §8 risk added: persistent upstream typos **re-quarantine on every future deep-rescan** → the rejected
  table grows; "idempotent" applies to recovery *re-runs* (bronze content-hash), not rejected-table
  cleanliness across re-extractions.
- §4.4's "one `engine.begin()`" and §6's "dry-run prints ids, products, dates, firms" prose are
  **superseded by §0.6**: `recover_quarantined` reads in `engine.connect()` then inserts in a separate
  `engine.begin()` (the FDA-one-off split — shorter write-lock hold), and the CLI emits a bespoke
  `candidates=N inserted=M` summary line, not a per-record table.

### 0.8 Confirmed sound (no change)
Rejected-table 7-column uniformity ✓; `BronzeLoader.load` has zero `_source_watermarks` references, so
calling it directly never mutates a watermark for any source ✓; lossless round-trip for all 5 in-scope
schemas after §0.5 ✓; the ADR 0014 re-ingest distinction (§5) is accurate against the ADR text ✓.

---

## 1. Context — why this exists

Every extractor routes records that fail `validate_records()` or `check_invariants()` into a
`<source>_rejected` table (ADR 0013), storing the full `model_dump(mode="json")` payload in
`raw_record` (JSONB). Quarantine is usually correct — the record really is malformed. But the
2026-06-01 FDA seed surfaced a class where it is **not**: 24 rows failed the shared
`check_date_sanity` invariant (`recall_initiation_dt` > 70 years in the past), yet the census
proved they are real 2007/2012/2013 recalls (Intuitive da Vinci, HeartWare HVAD, Roche MagNA
Pure 96, …) whose *source* `recall_initiation_dt` carries a dropped-century typo
(`2013 → 0013`, one transposition `2012 → 0212`). Every other date field is intact and modern.
The invariant fired correctly on the insane year, but the consequence was 14 genuine recall
events dropped from bronze over one corrupted field.

This is **not an FDA quirk.** `check_date_sanity` is a *shared* invariant — CPSC, NHTSA, USCG,
USDA, and FDA all call it (`grep check_date_sanity src/extractors/`). The same failure mode (a
real record carrying one out-of-range date) can quarantine real data from any of them. So a
**standardized, source-agnostic recovery path** is warranted, not speculative.

**Recovery is a deliberate, human-in-the-loop operation, never automatic.** The operator first
characterizes a rejection class with a census (the `explore_seed_rejections.sql` pattern),
confirms it is genuinely recoverable, then runs recovery scoped narrowly to that class. The
tool **bypasses the invariant on purpose** — that is exactly why it can do something
re-ingestion cannot (§5).

---

## 2. Empirically verified facts (do not re-derive)

1. **The rejected table is uniform across sources.** `BronzeLoader.load` writes quarantine rows
   with a fixed column set (`loader.py:399-410`): `source_recall_id, raw_record (JSONB),
   failure_reason, failure_stage, rejected_at, raw_landing_path` (+ `id`). So one generic
   reader serves every source.
2. **Date-typed fields are derivable from the model — no per-source hardcoding.** Verified
   2026-06-01: `field.annotation` introspection yields exactly the right set per source —
   FDA → 7 fields, NHTSA → `{bgman, datea, endman, odate, rcdate}`, CPSC →
   `{last_publish_date, recall_date}`.
3. **The dumped-ISO round-trip hazard affects most sources, and the fix is uniform.**
   `model_dump(mode="json")` serializes dates to ISO 8601, but the source date validators parse
   the *native* wire format and **raise on ISO**: FDA `%m/%d/%Y`, NHTSA `%Y%m%d`, USDA/USCG
   `%Y-%m-%d` (the `T00:00:00+00:00` tail breaks `strptime`), USCG-detail `%m/%d/%Y`. Only CPSC
   (`fromisoformat`) is natively immune — and with `strict=True` even it rejects a raw ISO
   *string*. **But every validator has the `if isinstance(v, datetime): return v` passthrough
   branch.** So the universal fix is: coerce every datetime-typed field ISO-string → `datetime`,
   then `model_validate`. Proven losslessly round-trippable for FDA (the recovery script's
   `test_round_trip_identity` — re-dumping reproduces the byte-identical payload, so content
   hashes are stable).
4. **Recovery must NOT route through `<Extractor>.load_bronze`.** That method also advances the
   watermark (e.g. FDA `fda.py:316`); recovered records' dates are below the post-seed watermark,
   so routing through it would *regress* the watermark and trigger a needless incremental
   re-fetch. Recovery calls `BronzeLoader.load()` directly (no watermark mutation).
5. **Recovery is idempotent.** Content-hash dedup (ADR 0007) skips already-present rows, so
   re-running is a no-op.

---

## 3. Design

A three-layer split: a source-agnostic **core**, the source-specific **config pulled from each
extractor** (single source of truth), and a thin **CLI driver**.

```
recalls recover-rejected <source> [--landing-path KEY] [--dry-run] [--reason-contains TEXT]
        │
        ├─ registry/extractor → (record_model, bronze_table, rejected_table, loader)
        ├─ predicate          → default = the >70yr date-sanity class; override via --reason-contains
        └─ src/bronze/recovery.recover_quarantined(...)   ← generic, source-agnostic
              read rejected rows for landing_path
            → filter by is_recoverable(failure_stage, failure_reason)
            → reconstruct(model, raw_record)              ← introspected date coercion
            → loader.load(conn, records, [], landing_path, extraction_timestamp=…)   ← direct, no watermark
```

Source-specific inputs reduce to four values — `record_model`, `bronze_table`,
`rejected_table`, `loader` — **all of which already live in each extractor**. The `is_recoverable`
predicate is recovery *policy*, not source identity.

---

## 4. Code changes (file by file)

### 4.1 `src/bronze/recovery.py` (NEW) — the generic core
- `datetime_field_names(model: type[BaseModel]) -> frozenset[str]` — introspection from §2.2.
- `coerce_dumped_datetimes(raw_record, date_fields) -> dict` — ISO-string → `datetime` for those
  fields; None/`''`/non-date untouched; returns a new dict (input unmutated).
- `reconstruct(model, raw_record) -> BaseModel` — `model.model_validate(coerce_dumped_datetimes(...))`.
- `RecoveryResult` (frozen dataclass): `source, landing_path, candidates, inserted`.
- `recover_quarantined(conn, *, rejected_table, loader, model, is_recoverable, landing_path,
  extraction_timestamp=None) -> RecoveryResult` — the read→filter→reconstruct→load pipeline.
- Default predicates: `recoverable_past_date_sanity(stage, reason)` (the `>70 years in the past`
  class) and a `reason_contains(substr)` factory for `--reason-contains`.

### 4.2 Per-extractor loader-config exposure (refactor, ~8 extractors)
Each extractor's `load_bronze` currently constructs its `BronzeLoader` inline with bespoke args
(FDA `hash_exclude_fields={"rid"}`; NHTSA 11-tuple identity + `within_batch_dedup` +
`allow_null_identity`; etc.). Extract that into a reusable accessor so recovery uses the **exact**
same config:
- Add `def make_bronze_loader(self) -> BronzeLoader` (or a classmethod) to each extractor; have
  `load_bronze` call it. Mechanical, one per extractor — confirm each one's current loader args
  at build time (they differ materially per source; getting `within_batch_dedup`/identity wrong
  silently changes dedup behavior).
- Expose `RECORD_MODEL`, `BRONZE_TABLE`, `REJECTED_TABLE` as class attributes where not already
  present (the tables are module-level `_…` objects today).

### 4.3 Recovery dispatch via the ADR 0012 source registry
The CLI maps `<source>` → its extractor through the existing registry
(`src/config/source_registry`, ADR 0012 Wave 2). Confirm at build time what the registry exposes
(extractor class vs. instance) and read `make_bronze_loader()` + the three class attributes from
it. If the registry doesn't cleanly yield these, fall back to a small explicit
`{source: RecoveryConfig}` map in `recovery.py` — but prefer the registry (single dispatch table).

### 4.4 `src/cli/main.py` — `recalls recover-rejected <source>`
- Args: `source` (positional), `--landing-path` (default: most-recent rejection's path),
  `--dry-run` (reconstruct + print plan, no write), `--reason-contains` (override the default
  predicate; lets an operator recover a different characterized class).
- Builds the engine, resolves config (§4.3), runs `recover_quarantined` in one `engine.begin()`.
- Prints a plan/summary mirroring the existing `fetched/loaded/rejected` style.

### 4.5 Tests
- `tests/bronze/test_recovery.py`: `datetime_field_names` per source (FDA/NHTSA/CPSC — values
  from §2.2 as regression anchors); `coerce_dumped_datetimes` (ISO→datetime incl. year-`0013`;
  None/`''`/non-date untouched; immutability); `reconstruct` round-trip identity **per source**
  (the guard that the introspected date set is complete); `recover_quarantined` over a fake
  connection / stub loader (candidate filtering, dry-run, idempotent skip).
- `tests/cli/` : `recover-rejected` dispatch (source→config, predicate override, dry-run path).

### 4.6 Retire the FDA one-off
Once `recalls recover-rejected fda` works, `scripts/fda/recover_rejected_invariant_records.py`
is redundant. **Remove it** (the CLI command is the canonical path) — but **keep**
`scripts/sql/fda/bronze/explore_seed_rejections.sql` (the census is the prerequisite discipline,
not the recovery mechanism). Migrate its two pure-logic tests into `test_recovery.py` so coverage
isn't lost.

---

## 5. Relationship to ADR 0014 re-ingestion (complementary, NOT duplicative)

| | `recalls re-ingest` (ADR 0014, planned `scripts/re_ingest.py`) | `recalls recover-rejected` (this plan) |
|---|---|---|
| **Source of records** | R2 raw landing bytes (T0) for a date range | the `<source>_rejected` table's `raw_record` JSONB |
| **Runs `check_invariants()`?** | **Yes** — full `validate → invariants → load` | **No** — bypasses the invariant on purpose |
| **Use case** | schema drift: re-process old rows under the *current* Pydantic model | recover records the invariant gate wrongly dropped |
| **On the FDA date-typo class** | would **re-quarantine** them (same invariant fires) | **recovers** them |
| **Idempotent via** | content-hash dedup (ADR 0007) | content-hash dedup (ADR 0007) |

They share the reconstruct-and-load idea but differ on the two axes that matter (source; whether
invariants run). `re-ingest` cannot recover a quarantined-but-valid record; that is the entire
reason this tool exists. Both may share `recovery.py`'s reconstruct primitive at build time.

---

## 6. Safety & recoverability policy

- **Census first, always.** Recovery is only ever run after an operator has characterized the
  rejection class (the `explore_seed_rejections.sql` pattern) and confirmed it is genuinely
  recoverable. The tool does not decide what is recoverable; the operator does, via the predicate.
- **Narrow predicate by default.** Default scope is the single confirmed class
  (`>70 years in the past`). Broadening requires an explicit `--reason-contains`.
- **`--dry-run`** reconstructs and prints the full plan (ids, products, dates, firms) without
  writing — the standard pre-flight.
- **Non-destructive.** The source rows are **left** in `<source>_rejected` as an audit record of
  the original quarantine. (A future `--purge-recovered` flag, atomic with the insert, is a
  possible follow-up for clean DLQ semantics — deliberately out of v1 scope.)
- **No invariant change.** This tool recovers *data*; it does **not** alter `check_date_sanity`.
  Whether to move date-sanity off the hard gate (permissive-bronze, ADR 0014) is a separate
  silver-remap-era decision and is explicitly out of scope here.

---

## 7. Quality gates (the PR on the new branch)
- `ruff check` + `ruff format --check` + `pyright` clean on all changed `src/`, `tests/`, `scripts/`.
- Full `pytest` green (the per-extractor `make_bronze_loader` refactor touches widely-used modules).
- Idempotency verified: running `recover-rejected <source>` twice inserts 0 the second time.
- Per-source round-trip identity test green for every source wired into the registry.

---

## 8. Residual risks & open decisions

| Item | Disposition |
|---|---|
| **Bypassing invariants is powerful** | Mitigated by census-first discipline + narrow default predicate + `--dry-run` + non-destructive audit trail. Recovery is human-in-the-loop, never scheduled. |
| **A source's model isn't round-trippable even with date coercion** | Caught at build time by the per-source `reconstruct` round-trip identity test before that source is wired in. |
| **`make_bronze_loader` mis-set** (wrong `within_batch_dedup`/identity) | The refactor must reproduce each extractor's current loader args exactly; covered by keeping `load_bronze` calling the same accessor (so prod + recovery share one definition). |
| **Registry doesn't cleanly expose extractor config** | Fallback: explicit `{source: RecoveryConfig}` map in `recovery.py` (§4.3). Decide at build time. |
| **`--purge-recovered` DLQ cleanup** | Out of v1 scope; revisit if the both-tables (bronze + rejected) state proves confusing in practice. |

---

## 9. Sequencing
- New branch `feature/quarantine-recovery-cli`, cut off `main` **immediately after**
  `feature/phase-6a5-historical-backfill` merges (2026-06-01 user decision: "right after this one").
- **Non-overlapping with `feature/silver-field-remap`** (recovery touches `src/bronze/`,
  `src/extractors/`, `src/cli/`; the remap touches `dbt/models/silver|staging/*` only), so the two
  do not conflict; per the user instruction this branch comes first. Re-check
  `project_scope/branch_sequencing_strategy.md` at branch-cut time to confirm the hard chain
  `6a.5 → silver-field-remap → 6b/6c` is preserved (this branch slots between 6a.5 and the remap).
- **Complementary to the Phase 6 `scripts/re_ingest.py`** (ADR 0014, §5); if re-ingest is built
  first, have it import `recovery.reconstruct`.

---

## 10. Implementation checklist

> Implemented 2026-06-01 per the §0 amendments (which override §4.2/§4.3 — explicit map, not
> the `make_bronze_loader` refactor). Gates: ruff + ruff format + pyright clean; full pytest
> **926 passed, 95% coverage**.

- [x] Branch `feature/quarantine-recovery-tool` cut off `main` after 6a.5 (PR #44) merged.
- [x] `src/bronze/recovery.py` — `datetime_field_names` (types.UnionType-aware),
      `coerce_dumped_datetimes`, `reconstruct`, `RecoveryConfig`/`RecoveryResult`,
      `recover_quarantined` (engine-managed read/write split, `bronze_table` lineage),
      `recoverable_past_date_sanity` + `reason_contains` predicates.
- [x] ~~Per-extractor `make_bronze_loader()` refactor~~ → **superseded by §0.1**: explicit
      `RECOVERY_CONFIG_BY_SOURCE_NAME` map (5 sources), loader args copied verbatim with
      citations; no extractor changes.
- [x] Dispatch via the explicit map (§0.1); CLI guards unknown/out-of-scope source with exit-1.
- [x] `recalls recover-rejected <source>` CLI command (`--landing-path`, `--dry-run`,
      `--reason-contains`) — own engine from `Settings`, bespoke summary.
- [x] `tests/bronze/test_recovery.py` (33) + CLI dispatch tests (3); FDA one-off's pure-logic
      tests migrated; `recovery.py` 100% / `cli/main.py` 96%.
- [x] `populate_by_name=True` added to the three USCG schemas (§0.5).
- [x] Removed `scripts/fda/recover_rejected_invariant_records.py` + its test; kept the census SQL.
- [x] Gates: ruff / pyright / pytest green; per-source round-trip + idempotency (mock) verified.
- [ ] Version bump (`pyproject.toml`) when this lands — your call.
