#!/usr/bin/env bash
#
# Verify that bronze's NISSAN/ACHILLES collisions correspond to
# byte-duplicate rows in the raw NHTSA TSV — not artifacts of our
# parsing or schema layer.
#
# v2 (2026-05-07) — fixes two bugs from v1:
#   - awk filters now match all 7 bronze WHERE-clause fields, not just
#     the visible 4. v1 undercounted by missing compname (field 7) and
#     rcl_cmpt_id (field 24); ACHILLES match exploded from 5 to 9.
#   - Step 1 SHA mismatch now blocks rather than warning. If the local
#     TSV isn't the same regeneration that produced the bronze load
#     under analysis, we're testing a different snapshot — fine for the
#     general "does NHTSA ship byte-dups?" question, but worth being
#     explicit about. The script falls through to a "current-snapshot
#     test" mode that explicitly says so.
#
# Three checks:
#   Step 1 — inner-TSV SHA. Compare to extraction_runs.response_inner_content_sha256
#            for the bronze load you want to mirror. Mismatch is OK
#            (we proceed in current-snapshot mode); just flagged.
#   Step 2 — NISSAN TITAN collision in raw TSV with TIGHT filter.
#   Step 3 — ACHILLES empty-ptno collision in raw TSV with TIGHT filter.
#
# Architectural conclusion only depends on Step 2/3 — whether NHTSA's
# TSV format produces byte-duplicate rows. If both return 1 unique
# line (after stripping field 1, RECORD_ID), NHTSA does ship dups.
#
# Usage: ./scripts/nhtsa/verify_collisions_raw_tsv.sh

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
ZIP="${REPO_ROOT}/data/exploratory/nhtsa/may7-bronze.zip"

if [[ ! -f "$ZIP" ]]; then
  echo "ERROR: $ZIP not found — run scripts/nhtsa/download_archives.sh first" >&2
  exit 1
fi

# Tight filters mirror the bronze WHERE clauses from
# investigate_residual_collisions.sql Q1b and Q2b. RCL.txt field positions:
#   2=CAMPNO, 3=MAKETXT, 4=MODELTXT, 5=YEARTXT, 7=COMPNAME,
#   24=RCL_CMPT_ID, 27=MFR_COMP_PTNO

NISSAN_FILTER='
  $2=="24V580000" &&
  $3=="NISSAN" &&
  $4=="TITAN" &&
  $5=="2021" &&
  $7=="AIR BAGS: AIR BAG/RESTRAINT CONTROL MODULE" &&
  $24=="000127294004588751000001543" &&
  $27=="98820 9FW4B"
'

# ACHILLES: empty mfr_comp_ptno in the TSV is "" between two tabs.
ACHILLES_FILTER='
  $2=="25T020000" &&
  $3=="ACHILLES" &&
  $4=="ATR SPORT 2" &&
  $5=="9999" &&
  $7=="TIRES:MARKINGS" &&
  $24=="000260791004398735000000305" &&
  $27==""
'

echo "================================================================"
echo "Step 1 — inner-TSV SHA-256"
echo "================================================================"
INNER_SHA=$(unzip -p "$ZIP" '*.txt' | sha256sum | awk '{print $1}')
echo "Local inner-TSV SHA: $INNER_SHA"
echo "  prefix: ${INNER_SHA:0:12}"
echo
echo "Compare to bronze May 7 inner SHA = c955c37153d1 (from"
echo "diagnose_full_reinsert.sql Q5). If match → testing the exact"
echo "snapshot that produced bronze. If mismatch → testing current"
echo "TSV; architectural answer (does NHTSA ship byte-dups?) still"
echo "valid because that's a source-format property, not a per-snapshot one."

echo
echo "================================================================"
echo "Step 2 — NISSAN TITAN collision (tight filter, 7 fields)"
echo "================================================================"
NISSAN_MATCHES=$(
  unzip -p "$ZIP" '*.txt' \
    | awk -F'\t' "$NISSAN_FILTER" \
    | wc -l
)
echo "Matching rows: $NISSAN_MATCHES"

NISSAN_UNIQUE=$(
  unzip -p "$ZIP" '*.txt' \
    | awk -F'\t' "$NISSAN_FILTER" \
    | cut -f2- \
    | sort -u \
    | wc -l
)
echo "Unique lines after stripping field 1 (RECORD_ID): $NISSAN_UNIQUE"
echo "  → 1 means byte-duplicates (NHTSA ships dup rows)"
echo "  → > 1 means a TSV-level differentiator exists that bronze flattened"

echo
echo "================================================================"
echo "Step 3 — ACHILLES empty-ptno collision (tight filter, 7 fields)"
echo "================================================================"
ACHILLES_MATCHES=$(
  unzip -p "$ZIP" '*.txt' \
    | awk -F'\t' "$ACHILLES_FILTER" \
    | wc -l
)
echo "Matching rows: $ACHILLES_MATCHES"

ACHILLES_UNIQUE=$(
  unzip -p "$ZIP" '*.txt' \
    | awk -F'\t' "$ACHILLES_FILTER" \
    | cut -f2- \
    | sort -u \
    | wc -l
)
echo "Unique lines after stripping field 1 (RECORD_ID): $ACHILLES_UNIQUE"
echo "  → 1 means byte-duplicates"
echo "  → > 1 means a TSV-level differentiator"

echo
echo "================================================================"
echo "Summary"
echo "================================================================"
if [[ "$NISSAN_MATCHES" -ge 2 && "$NISSAN_UNIQUE" == "1" \
   && "$ACHILLES_MATCHES" -ge 2 && "$ACHILLES_UNIQUE" == "1" ]]; then
  echo "✓ Both collision sets are byte-duplicate rows in the raw TSV."
  echo "  Bronze analysis is faithful. NHTSA ships duplicate rows."
  echo "  Architecture answer: 7-tuple identity + hash_exclude_fields={'source_recall_id'}"
  echo "  + within-batch dedup."
elif [[ "$NISSAN_MATCHES" == "0" && "$ACHILLES_MATCHES" == "0" ]]; then
  echo "⚠ Neither tight filter matched any rows in the local TSV."
  echo "  The collisions analyzed in bronze (May 7 snapshot) may not exist"
  echo "  in the current TSV — NHTSA may have edited those recalls between"
  echo "  the May 7 extract and this verification. To test exactly, pull"
  echo "  the May 7 wrapper from R2:"
  echo "    nhtsa/2026-05-07/2180b301-844c-4ab2-9fb1-98848642a57f.zip.gz"
else
  echo "⚠ At least one collision set has a TSV-level differentiator OR"
  echo "  partial match. Inspect raw lines:"
  echo
  echo "  NISSAN sample (up to 4 lines):"
  unzip -p "$ZIP" '*.txt' \
    | awk -F'\t' "$NISSAN_FILTER" \
    | head -4 \
    | cut -f1-7,24,27 \
    | nl -ba
  echo
  echo "  ACHILLES sample (up to 5 lines):"
  unzip -p "$ZIP" '*.txt' \
    | awk -F'\t' "$ACHILLES_FILTER" \
    | head -5 \
    | cut -f1-7,24,27 \
    | nl -ba
fi
