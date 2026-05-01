# Bronze re-baseline playbook

Operational reference for the four-step procedure when a PR changes the bronze
canonical-dict shape (and therefore re-versions every record on the next
extract). Architectural rationale lives in ADR 0027; this document is the
operator's checklist.

## When this applies

Any PR that modifies one of the following triggers a re-baseline wave on the
next extract for the affected source:

- A Pydantic schema in `src/schemas/<source>.py` — adding/removing a validator,
  changing a field's type, renaming a `validation_alias`, etc.
- The canonical-serialization helper in `src/bronze/hashing.py`.
- A bronze table column type that requires a corresponding Pydantic change
  (Alembic migration touching a bronze table).

PRs that don't touch these files cannot re-baseline; this playbook doesn't
apply.

## Step 1 — Detection at PR time

Two gates fire automatically on every PR:

1. **PR template checkbox.** The template asks "Does this change the bronze
   canonical dict?" with two checkboxes (yes / no). The author must check one.
2. **CI guard.** A GitHub Actions check fails the PR if any of the trigger
   files above changed and the PR body does not contain a line matching
   `RE-BASELINE: yes` or `RE-BASELINE: no`. The check is bypassable only by
   editing the PR body, which forces conscious acknowledgment.

If both fire green and the PR is marked `RE-BASELINE: no` but the change
*does* re-baseline, that's a CI hole — file as a bug, don't paper over.

## Step 2 — Pre-merge planning (when `RE-BASELINE: yes`)

The PR description must include a "Re-baseline plan" section with:

- **Affected sources** — which `<source>_recalls_bronze` table(s) re-version.
- **Estimated wave size** — rough %-of-records prediction with reasoning
  (which fields' values change, what % of records carry those values). Doesn't
  have to be precise; it has to exist.
- **Coordination note** — if any other PR in flight depends on the old shape,
  call it out.

## Step 3 — Post-merge: tag the re-extract run

After the PR merges to `main` and reaches production, the next scheduled cron
run will absorb the wave. To prevent the wave from polluting downstream
history models, tag the re-extract via the CLI flag:

```bash
recalls extract <source> --change-type=schema_rebaseline
```

Or, when the trigger was a hashing-helper change:

```bash
recalls extract <source> --change-type=hash_helper_rebaseline
```

The flag writes to `extraction_runs.change_type`. Default value is `routine`.
Allowed values: `routine`, `schema_rebaseline`, `hash_helper_rebaseline`.

For a multi-source rebaseline (e.g., a hashing-helper change), tag *each*
source's re-extract — one CLI invocation per source.

The tagged run inserts re-versioned bronze rows normally; the difference is
that downstream history models (`recall_event_history`, ADR 0022) join to
`extraction_runs.change_type` and exclude rows from non-routine runs from
their edit-detection LAG window. The new bronze rows still exist; they just
don't synthesize false "edit" events.

## Step 4 — Verification

After the tagged re-extract completes, confirm:

```sql
-- The wave landed roughly the predicted size:
select count(*) from <source>_bronze
where extraction_timestamp > '<re-extract start time>';

-- The run was tagged correctly (not 'routine'):
select source, change_type, records_inserted from extraction_runs
order by started_at desc limit 5;
```

If the tag is missing — the cron ran on the default `routine` value because
the CLI flag wasn't passed — there is no automated cleanup. Document the
incident, then accept the false-edit signals in `recall_event_history` and
note the affected `extraction_runs.run_id` so consumers can filter manually.
**Do not** retroactively update `extraction_runs.change_type`; that
contradicts the append-only audit posture (same reasoning as
`*_rejected` tables, see Phase 7 deliverable in `implementation_plan.md`).

## Step 5 — Rollback policy

**Roll forward only.** The append-only invariant on bronze (ADR 0007) means
new versions cannot be retroactively un-inserted. If the schema change itself
was wrong, ship a fix-forward PR (which causes a third wave, also tagged).

Specifically rejected approaches and why:

- **Reverting the PR + manual SQL cleanup of new bronze rows.** Destroys the
  audit trail of what we received and how we parsed it. Same posture as
  never-truncating `*_rejected` tables.
- **Using `re_ingest.py` (Phase 6 / ADR 0014) to undo.** The re-ingest CLI
  re-runs the *current* schema against R2, which means it would replay the
  buggy schema, not the previous one. Wrong tool for this job.
- **Using a database transaction to "preview" the wave.** Bronze writes are
  per-batch atomic (one transaction per `load_bronze` call), but the wave
  spans an entire extraction. Preview-then-rollback would need infrastructure
  changes that aren't worth building for a rare event.

The recovery cost of "ship the wrong schema, then fix-forward with a third
wave" is bounded: three rows per affected record in bronze, two false-edit
events in history (filtered by `change_type`), and a couple of extra MB of
storage. Cheap enough that the structural simplicity of "append-only,
no rollback" wins.

## Cross-references

- **ADR 0007** — content-hashing rationale; line 70 establishes the
  "treat hash-affecting changes as schema migrations" principle.
- **ADR 0013** — append-only quarantine posture; the same logic applies to
  bronze.
- **ADR 0027** — bronze keeps storage-forced transforms only; this playbook
  is the operational arm of that ADR.
- **`documentation/operations.md`** — broader production operations guide;
  links here for the re-baseline procedure.
