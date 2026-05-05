#!/usr/bin/env bash
#
# Probe NHTSA flat-file values for sentinels and validation-failure
# candidates BEFORE the first `recalls extract nhtsa` run.
#
# Why this matters: the bronze Pydantic schema in src/schemas/nhtsa.py
# applies storage-forced validators per ADR 0027. If a value in the live
# data fails one of those validators, the row routes to
# nhtsa_recalls_rejected — not bronze. This probe surfaces the values
# that would cause that quarantine, so we can either:
#   - update the schema to accept a known sentinel (rare; only needed if
#     the sentinel does not happen to be a valid YYYYMMDD / Yes-No /
#     CHAR(3) value), or
#   - confirm the sentinel parses cleanly into bronze (typical case —
#     ODATE's 19010101 falls here) and document silver-staging mapping
#     for the eventual stg_nhtsa_recalls.sql model.
#
# Three categories probed, matching the three classes of validators
# that can fail:
#   1. Date fields (positions 9, 10, 13, 16, 17) — _parse_nhtsa_date
#      uses datetime.strptime("%Y%m%d") and raises ValueError on
#      anything that isn't a valid YYYYMMDD.
#   2. Boolean fields (positions 28, 29) — _to_bool accepts only "Yes"
#      / "No" and raises ValueError on anything else.
#   3. FMVSS (position 19) — StringConstraints(max_length=3) rejects
#      values > 3 chars (Finding F May 2025 width reduction).
#
# Per-archive output: PRE_2010 covers the historical-seed corpus
# (1966-2009); POST_2010 covers the daily incremental path's actual
# data. Both are probed because either could surface a sentinel; the
# Step 2 schema-change decision is a union of the two.
#
# Usage:
#   ./scripts/nhtsa/probe_date_sentinels.sh
#
# Run after ./scripts/nhtsa/download_archives.sh has populated
# data/exploratory/nhtsa/. Output goes to stdout — pipe to a file if
# you want a permanent record alongside Finding H follow-up notes.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
DATA_DIR="${REPO_ROOT}/data/exploratory/nhtsa"

PRE_2010="${DATA_DIR}/FLAT_RCL_PRE_2010.zip"
POST_2010="${DATA_DIR}/FLAT_RCL_POST_2010.zip"

for f in "$PRE_2010" "$POST_2010"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing $f — run scripts/nhtsa/download_archives.sh first" >&2
    exit 1
  fi
done

# Field positions per RCL.txt (1-indexed, matching awk's $N convention).
# Names track src/extractors/nhtsa.py:_FIELD_NAMES.
DATE_FIELDS="9:bgman 10:endman 13:odate 16:rcdate 17:datea"
BOOL_FIELDS="28:do_not_drive 29:park_outside"
FMVSS_FIELD=19

# Candidate sentinel literals to count exactly. ODATE's 19010101 is the
# known case (Finding H); the others are speculative placeholders worth
# scanning for. Add more if the empirical-outlier scan below surfaces a
# new candidate.
SENTINEL_CANDIDATES="19010101 19000101 18000101 99999999 00000000 11111111"

probe_archive() {
  local archive="$1"
  local label="$2"

  echo
  echo "================================================================"
  echo "  $label  ($archive)"
  echo "================================================================"

  local total
  total=$(unzip -p "$archive" '*.txt' | wc -l)
  echo "Total rows: $total"

  echo
  echo "---- DATE FIELDS ----"
  echo "Each section: min, max, candidate-sentinel hits, non-YYYYMMDD shapes,"
  echo "and empty-value count."

  for spec in $DATE_FIELDS; do
    local pos="${spec%%:*}"
    local name="${spec##*:}"
    echo
    echo "Field $pos ($name):"

    # Min and max — sorts ASCII; YYYYMMDD ASCII-orders identically to
    # date order, so first/last lines are the empirical bounds. Empty
    # values are filtered first so they don't dominate the min slot.
    local bounds
    bounds=$(
      unzip -p "$archive" '*.txt' \
        | awk -F'\t' -v fld="$pos" '$fld != "" {print $fld}' \
        | sort -u
    )
    local min_val max_val distinct
    min_val=$(echo "$bounds" | head -1)
    max_val=$(echo "$bounds" | tail -1)
    distinct=$(echo "$bounds" | wc -l | tr -d ' ')
    echo "  min: ${min_val:-<no non-empty values>}"
    echo "  max: ${max_val:-<no non-empty values>}"
    echo "  distinct non-empty values: $distinct"

    # Candidate sentinel exact-match counts.
    for sentinel in $SENTINEL_CANDIDATES; do
      local count
      count=$(
        unzip -p "$archive" '*.txt' \
          | awk -F'\t' -v fld="$pos" -v sen="$sentinel" '$fld == sen' \
          | wc -l | tr -d ' '
      )
      if [[ "$count" -gt 0 ]]; then
        echo "  sentinel $sentinel: $count rows"
      fi
    done

    # Non-YYYYMMDD shapes — anything non-empty that isn't exactly 8
    # digits would fail strptime and quarantine the row. Show up to 5
    # examples; if this section is non-empty, it is a Step-2 blocker.
    local bad_shapes
    bad_shapes=$(
      unzip -p "$archive" '*.txt' \
        | awk -F'\t' -v fld="$pos" '$fld != "" && $fld !~ /^[0-9]{8}$/ {print $fld}' \
        | sort -u | head -5
    )
    if [[ -n "$bad_shapes" ]]; then
      echo "  ⚠ non-YYYYMMDD shapes (would QUARANTINE on first extract):"
      echo "$bad_shapes" | sed 's/^/    /'
    else
      echo "  non-YYYYMMDD shapes: none ✓"
    fi

    # Empty-value count — informational. RCDATE is the only required
    # date field; empty RCDATE would quarantine.
    local empty
    empty=$(
      unzip -p "$archive" '*.txt' \
        | awk -F'\t' -v fld="$pos" '$fld == ""' \
        | wc -l | tr -d ' '
    )
    if [[ "$name" == "rcdate" && "$empty" -gt 0 ]]; then
      echo "  ⚠ empty values: $empty rows (REQUIRED field — would QUARANTINE)"
    else
      echo "  empty values: $empty rows"
    fi
  done

  echo
  echo "---- BOOLEAN FIELDS (DO_NOT_DRIVE, PARK_OUTSIDE) ----"
  echo "Schema accepts only \"Yes\" / \"No\" / empty. Anything else quarantines."

  for spec in $BOOL_FIELDS; do
    local pos="${spec%%:*}"
    local name="${spec##*:}"
    echo
    echo "Field $pos ($name) — distinct values:"
    unzip -p "$archive" '*.txt' \
      | awk -F'\t' -v fld="$pos" '{print $fld}' \
      | sort | uniq -c | sort -rn \
      | sed 's/^/  /'
  done

  echo
  echo "---- FMVSS (field $FMVSS_FIELD) ----"
  echo "Schema enforces max_length=3. Longer values quarantine."

  local long_fmvss
  long_fmvss=$(
    unzip -p "$archive" '*.txt' \
      | awk -F'\t' -v fld="$FMVSS_FIELD" 'length($fld) > 3 {print $fld}' \
      | sort -u | head -10
  )
  if [[ -n "$long_fmvss" ]]; then
    echo "⚠ values > 3 chars (would QUARANTINE):"
    echo "$long_fmvss" | sed 's/^/  /'
  else
    echo "values > 3 chars: none ✓"
  fi

  # Length distribution for reference.
  echo
  echo "FMVSS length distribution:"
  unzip -p "$archive" '*.txt' \
    | awk -F'\t' -v fld="$FMVSS_FIELD" '{print length($fld)}' \
    | sort | uniq -c | sort -rn \
    | sed 's/^/  /'
}

probe_archive "$PRE_2010" "PRE_2010 (1966-2009)"
probe_archive "$POST_2010" "POST_2010 (2010-present)"

echo
echo "================================================================"
echo "Decision rules:"
echo "  - ANY '⚠' line → Step 2 blocker. Update schema or migration"
echo "    before running 'alembic upgrade head' / 'recalls extract nhtsa'."
echo "  - 'sentinel <date>: N rows' but no '⚠' → bronze parses cleanly;"
echo "    note the sentinel for silver staging (Step 5)."
echo "  - All sections clean → no schema change needed; proceed to"
echo "    migration + first extraction."
echo "================================================================"
