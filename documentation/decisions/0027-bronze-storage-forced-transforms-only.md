# 0027 — Bronze keeps storage-forced transforms only; value-level normalization moves to silver

- **Status:** Draft
- **Date:** 2026-05-01
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0007 (extends the hashing-helper "treat as schema migration" rule to all bronze-shape changes); complements ADR 0014 (which only covered source-driven drift, not our-side normalization).

> **Acceptance criteria** (must be resolved before promoting to Accepted):
>
> 1. ~~**Confirm the storage-forced exception list** for the boolean-false sentinel
>    case in `usda_fsis_establishments_bronze.geolocation` / `.county`.~~
>    **Resolved 2026-05-01: option 3** (convert `false` → string `"false"` in
>    Pydantic; bronze column stays `TEXT`; silver does `nullif(geolocation,
>    'false')`). See "Storage-type choice" section below.
> 2. ~~**Re-baseline strategy** for the affected sources.~~
>    **Resolved 2026-05-01: full playbook at
>    `documentation/operations/re_baseline_playbook.md`**, summarized as: PR
>    template checkbox + CI guard at detection time; `change_type` column on
>    `extraction_runs` for marking; `recall_event_history` (Phase 6) joins
>    against it to filter parser-driven re-versions out of edit detection;
>    roll-forward only, no rollback.
> 3. **Migration ordering** — confirm Phase 5b.2 Step 4.5 is the right slot
>    (between cassettes and silver), gating Phase 5c.

---

## Context

### What prompted this ADR

Phase 5b.2 first extraction (2026-05-01) loaded 7,945 establishment records
into bronze. A follow-up commit added an empty-string-to-`None` normalizer
(`_FsisNullableStr`) to the establishment Pydantic schema for consistency with
the recall schema. Re-running the extractor produced 6,859 new bronze rows
(86% re-version rate) with no source-side change. Cause: the normalizer
changed the canonical record dict (`{"duns_number": ""}` → `{"duns_number":
null}`), which changed the SHA-256 content hash, which made the bronze loader
treat every previously-loaded record as a new version per ADR 0007.

This is the second observation of the pattern in the project. The FDA archive
migration (Phase 5a, Finding M) produced an identical wave, but that one was
upstream-driven — the FSIS API's data shape genuinely changed. The
establishment case was entirely our doing: the source returned the same bytes
both times.

### What was already decided

Two prior ADRs partially addressed this surface:

- **ADR 0007 line 70** states that any change to the canonical-serialization
  helper (`src/bronze/hashing.py`) "invalidates every previously-computed
  bronze hash and would cause a full re-dedup wave on the next ingest. Such
  changes are treated as schema migrations: documented in the same PR that
  makes them, accompanied by a plan for the re-dedup impact." But this is
  scoped to the hashing helper itself, not to upstream Pydantic-schema
  changes that produce the identical effect.
- **ADR 0014 line 49** prescribes the four upstream-driven schema-drift
  recovery procedures (source adds field / renames field / changes type /
  adds enum value), all routed through the planned `scripts/re_ingest.py`
  CLI in Phase 6. None of the four buckets cover our-side normalization
  changes.

### The architectural question

What kinds of transformation should happen between the raw API response and
the bronze row? Three categories exist; only the first is forced by the
storage layer:

| Category | Example | Storage-forced? |
|---|---|---|
| Type parsing | `"2026-04-27"` → `datetime(...)` for a `TIMESTAMPTZ` column | **Yes** |
| Structural validation | Required field present; no unknown keys (`extra='forbid'`) | Structural |
| Value-level normalization | `""` → `NULL`; `false`-sentinel → `NULL`; `.strip()` on whitespace | **No** |

The first two are necessary regardless of where downstream cleaning happens.
The third is a judgment call that the prior schemas (CPSC, FDA, USDA recall,
USDA establishment) all answered with "do it in bronze" — but the choice was
inherited rather than deliberated.

---

## Decision

**Bronze does only what is forced by the storage layer or required for
structural integrity. All value-level normalization moves to silver.**

### What stays in bronze

| Transform | Why |
|---|---|
| Date/datetime string → `datetime` | Postgres `TIMESTAMPTZ` cannot hold a string. |
| `"True"`/`"False"` string → `bool` | Postgres `BOOLEAN` cannot hold a string. |
| Empty/null nullable date | `TIMESTAMPTZ NULL` requires `None`, not `""`. |
| JSON serialization of nested collections | JSONB column type. |
| `extra='forbid'` strict validation | Catches schema drift at ingest (the Phase 5b.2 city-bug surface; ADR 0014). |
| Required-field validation (quarantine on miss) | Same. |
| Pydantic strict-mode type checking | Same. |

### What moves out of bronze and into silver staging

| Transform | Old home | New home | New silver expression |
|---|---|---|---|
| Empty-string → `NULL` on `Optional[str]` | `_normalize_str` in `src/schemas/usda.py`, `_FsisNullableStr` in `src/schemas/usda_establishment.py` | `dbt/models/staging/stg_<source>_recalls.sql` | `nullif(col, '')` per nullable text column |
| `"True"`/`"False"` string → bool when destination is BOOLEAN NULL but source uses `""` for missing | `_to_nullable_bool` in `src/schemas/usda.py` | Stays in bronze (storage-forced — see "What stays" above) | — |
| Per-element whitespace strip on JSONB string arrays | `_strip_list_elements` in `src/schemas/usda_establishment.py` | Silver staging | `(select jsonb_agg(trim(elem #>> '{}')) from jsonb_array_elements_text(activities) elem)` |
| Boolean-false sentinel on text columns *(establishment only)* | `_normalize_false_sentinel` in `src/schemas/usda_establishment.py` | See storage-type choice below | — |

### Storage-type choice for the boolean-false sentinel case

The establishment API returns `"geolocation": false` (JSON boolean) on records
without a populated geolocation. Our bronze column is `TEXT`. JSON `false`
cannot land in a `TEXT` column without *some* conversion — bronze is forced
to choose. Three options, pick one:

1. **Convert `false` → `NULL` in Pydantic** (status quo). Loses the
   distinction between "source returned false-sentinel" and "source returned
   `null`" (which never happens today, but could on a future API change).
   Violates the spirit of this ADR (value-level normalization in bronze) but
   is the smallest change.
2. **Change the column to `JSONB`.** Preserves type distinction perfectly.
   Silver expression becomes `case when geolocation = 'false'::jsonb then
   null else geolocation #>> '{}' end`. Adds JSONB-versus-TEXT inconsistency
   across the table for what is almost always a string value.
3. **Convert `false` → string `"false"` in Pydantic.** Bronze column stays
   `TEXT`. Preserves the source's signal in the cheapest storage. Silver
   expression becomes `nullif(geolocation, 'false')`. Detect "did the source
   flip from `false` to `null`?" via `select count(*) where geolocation =
   'false'`.

**Decided: option 3** (2026-05-01). It keeps the column type uniform, preserves
the source-signal-detection property cheaply, and the conversion is mechanical
(not value-judgment) — the only reason it exists is that the destination
column type (`TEXT`) can't hold a JSON boolean. Per-source rationale gets
documented in the schema docstring. The validator is renamed
`_coerce_false_to_text` (was `_normalize_false_sentinel`) and returns
`"false"` instead of `None` for the literal `False` input; pass-through
behavior for strings is unchanged.

---

## Consequences

### Positive

- **Lineage clarity.** Bronze content hashes change iff the source changed.
  "Did anything change at the source in this batch?" becomes a single SQL
  query against `bronze.content_hash`. The `recall_event_history` model
  (Phase 6, ADR 0022) gets clean inputs from day one — no parser-driven false
  edits to filter out via metadata columns.
- **Reversibility.** A silver-layer normalization tweak is `dbt build` away.
  The same change in bronze is a re-extract + re-load wave, plus history
  pollution.
- **Audit posture.** Bronze rows are presentable to a regulator as "what the
  source gave us" with only the storage-forced conversions disclosed.
- **Cross-source consistency** at the *policy* level: every source's bronze
  is the same kind of projection, regardless of how its API formats sentinels.
- **Future sources inherit the corrected pattern.** NHTSA (Phase 5c) and USCG
  (Phase 5d) start with this rule from day one.

### Negative

- **One-time re-baseline wave per affected source.** Per the audit, three of
  four sources re-version: FDA (medium wave), USDA recall (medium wave),
  USDA establishment (small second wave). CPSC is unaffected — its schema
  never carried empty-string normalization. Acceptable on dev; production
  gets the planned-re-baseline treatment per ADR 0007 line 70.
- **Silver staging models grow.** Each `stg_<source>_recalls.sql` view picks
  up `nullif(col, '')` wrappers per nullable text column. Wordy but
  mechanical, and the wrappers are exactly the kind of thing dbt staging
  models are *for*.
- **A small re-extract coordination burden** in Phase 7 production cron when
  silver normalizations change. Mitigated by the fact that this ADR makes
  such changes silver-local — the cron job for the source extractor is
  unaffected.

### Neutral

- The `re-ingest` CLI (Phase 6 deliverable per ADR 0014) continues to be the
  recovery mechanism for source-driven schema drift; this ADR doesn't change
  its scope.
- ADR 0007's "treat hashing-helper changes as schema migrations" rule is now
  a special case of the broader rule stated here. The ADR 0007 text remains
  correct for its narrower scope; no edit needed unless we want to
  cross-reference.

---

## Scope of the migration

Four schemas, four staging models, plus tests.

### Schemas to edit (audit completed 2026-05-01)

| File | Changes |
|---|---|
| `src/schemas/cpsc.py` | **No changes.** Audit confirmed CPSC uses plain `str \| None = Field(None, validation_alias=...)` for all optional strings — no empty-string normalizer. The only validator is `_coerce_date_string_to_utc_datetime` which is storage-forced (TIMESTAMPTZ). |
| `src/schemas/fda.py` | Drop `_normalize_str` (line 52) and `_FdaNullableStr` (line 66). Switch all `Optional[str]` fields currently typed as `_FdaNullableStr` to plain `str \| None = Field(None, ...)`. Keep `_to_int`, `_to_nullable_int`, `_to_str`, `_parse_fda_date`, `_parse_nullable_fda_date` (all storage-forced — INTEGER columns can't hold strings, TEXT columns can't hold ints, TIMESTAMPTZ can't hold strings). Update the docstring's "Empty '' normalized to None" line to reflect the new behavior (silver does it). |
| `src/schemas/usda.py` | Drop `_normalize_str`, drop `_UsdaNullableStr`. Switch all `Optional[str]` fields to plain `str \| None = Field(...)`. Keep `_to_bool`, `_to_nullable_bool`, `_parse_usda_date`, `_parse_nullable_usda_date` (storage-forced — BOOLEAN columns can't hold strings, TIMESTAMPTZ can't hold strings). |
| `src/schemas/usda_establishment.py` | Drop `_normalize_str`, drop `_strip_list_elements`. Drop `_FsisNullableStr`, `_FsisStrippedStrList`. **Rename** `_normalize_false_sentinel` → `_coerce_false_to_text` and change the body to return `"false"` instead of `None` for the literal `False` input (per the decided option 3 above). Switch all current `_FsisNullableStr` fields to plain `str \| None`. Keep date validators. |

### Staging models to edit

| File | Changes |
|---|---|
| `dbt/models/staging/stg_cpsc_recalls.sql` | **No changes.** CPSC bronze never carried empty-string sentinels — the source uses `null`/key-absent for missing values consistently. |
| `dbt/models/staging/stg_fda_recalls.sql` | `nullif(col, '')` per nullable text column. FDA returns both `null` and `""` for the same fields across records (Finding J in `documentation/fda/api_observations.md`); the wrapper normalizes them. |
| `dbt/models/staging/stg_usda_fsis_recalls.sql` | `nullif(col, '')` per nullable text column. Preserve the existing `langcode='English'` filter and latest-version ranking. |
| `dbt/models/staging/stg_usda_fsis_establishments.sql` | New file (Phase 5b.2 Step 5). Picks up `nullif(geolocation, 'false')`, `nullif(county, 'false')`, the `jsonb_array_elements_text(...) → trim(...) → jsonb_agg(...)` pattern for `activities` / `dbas`, and `nullif(col, '')` on optional text columns. |

### Tests to update

- Schema unit tests in `tests/schemas/test_*.py` — remove "empty string becomes None" expectations from the affected schemas; add a few "empty string is preserved as ''" expectations to lock in the new behavior.
- dbt staging model tests — none structurally; the existing `not_null` / `unique` tests still hold.

### Re-extract wave (revised after audit)

After the refactor PR merges to dev:

- `recalls extract cpsc` — **no wave expected.** Schema and staging are unchanged; bronze hashes don't move.
- `recalls extract fda` — re-versions every record carrying `""` in any optional text field (Finding J: FDA mixes `null` and `""` across the dataset). Estimated medium wave; exact size depends on per-field empty-string rates.
- `recalls extract usda` — re-versions records with `""` in any of the ~15 optional text fields. USDA's null-rate landscape (Finding C in `documentation/usda/recall_api_observations.md`) suggests a medium wave, similar in shape to FDA.
- `recalls extract usda_establishments` — second wave. The first re-extract on 2026-05-01 already absorbed the empty-string axis; this one absorbs the `false`-sentinel reversal (`None` → `"false"`) and the whitespace-strip removal (stripped strings → original ragged strings). Estimated ~14% (records with populated `geolocation`/`county` plus those with multi-element `activities`/`dbas`).

Each wave is a single re-extract per source; ordering doesn't matter since they're independent. The `extraction_runs` table records each wave naturally.

---

## Supporting artifacts required by the production playbook

The full playbook (`documentation/operations/re_baseline_playbook.md`) depends
on four artifacts that don't exist yet. Each is filed where it naturally
belongs in the implementation plan:

| Artifact | Where filed | When needed |
|---|---|---|
| `extraction_runs.change_type` column (Alembic migration) | Phase 5b.2 Step 4.5 alongside the schema refactor itself, since the first re-baseline event is the refactor's own re-extract wave. | With the refactor PR. |
| CLI flag `recalls extract <source> --change-type=<value>` (`src/cli/main.py`) | Phase 5b.2 Step 4.5, same PR as the migration. | With the refactor PR. |
| `.github/PULL_REQUEST_TEMPLATE.md` + CI guard workflow | Phase 7 (production CI). Listed there as a new deliverable. | Before cron turn-on. The dev refactor in 5b.2 won't have the gate yet — the user manually adds the `RE-BASELINE: yes` line in their commit/PR description as practice for the eventual gate. |
| `recall_event_history` model filters on `change_type != 'routine'` | Phase 6 / ADR 0022, listed in that ADR's implementation. | When the history model is built. |

## Implementation timing

Phase 5b.2 Step 4.5 — between cassettes (Step 4) and silver join (Step 5),
gating Phase 5c. See `project_scope/implementation_plan.md` for the slot.

Rationale:
- Cassettes (Step 4) record HTTP request/response and are unaffected by the
  bronze normalization layer; they don't need re-recording.
- Silver join (Step 5) writes a new `stg_usda_fsis_establishments.sql` view —
  doing the refactor first lets us write that view once with the new pattern
  rather than rewriting it after.
- NHTSA (Phase 5c) and USCG (Phase 5d) inherit the corrected pattern from
  day one.
