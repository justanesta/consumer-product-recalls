<!-- Thanks for the PR. Fill in the summary and the re-baseline section below. -->

## Summary

<!-- What does this change do, and why? -->

## Bronze canonical dict — re-baseline check

**Does this change the bronze canonical dict?**

A change re-baselines bronze if it touches a Pydantic schema in
`src/schemas/<source>.py` or the canonical-serialization helper
`src/bronze/hashing.py` — either re-versions every record on the next extract.
See `documentation/operations/re_baseline_playbook.md`.

- [ ] No — this PR does not change the bronze canonical dict.
- [ ] Yes — this PR re-baselines bronze (fill in the re-baseline plan below).

<!--
The `re-baseline-check` CI gate fails this PR if it touches src/schemas/*.py or
src/bronze/hashing.py and the body below is missing or unfilled. Set the value to
`yes` or `no` (lowercase).
-->

RE-BASELINE: no

<!-- If RE-BASELINE is yes, complete the plan per re_baseline_playbook.md Step 2: -->
<!--
### Re-baseline plan
- Affected sources:
- Estimated wave size:
- Coordination note:
-->

## Checklist

- [ ] `ruff check`, `ruff format --check`, `pyright`, and `pytest` pass locally.
- [ ] dbt changes verified with `dbt build` + `dbt test` against the dev Neon branch.
- [ ] Documentation updated for any new/changed schema surface (single-home rule).
