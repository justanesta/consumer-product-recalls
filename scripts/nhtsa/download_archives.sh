#!/usr/bin/env bash
#
# Download NHTSA flat-file archives for Phase 5c Step 1 inspection.
#
# Downloads all year-band archives, the PRE/POST 2010 historical archives,
# and the documentation PDF into data/exploratory/nhtsa/ (gitignored). Lists
# contents of each ZIP after download so inner filenames and uncompressed
# sizes are visible before drilling in with Steps 3-5 of
# project_scope/current_branch_staged_tasks.md.
#
# Total download: ~33 MB (ZIPs) + 1 MB (PDF) = ~34 MB.
# Re-running re-downloads everything (curl overwrites by default). The
# --remote-time flag preserves the server's Last-Modified as the file mtime,
# so `ls -l` on the downloaded files reflects NHTSA's stamp, not the
# download time.
#
# Usage:
#   ./scripts/nhtsa/download_archives.sh

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
TARGET_DIR="${REPO_ROOT}/data/exploratory/nhtsa"
mkdir -p "$TARGET_DIR"
cd "$TARGET_DIR"

BASE_URL="https://static.nhtsa.gov/odi/ffdd/rcl"

ARCHIVES=(
  FLAT_RCL_PRE_2010
  FLAT_RCL_POST_2010
  RCL_FROM_2000_2004
  RCL_FROM_2005_2009
  RCL_FROM_2010_2014
  RCL_FROM_2015_2019
  RCL_FROM_2020_2024
  RCL_FROM_2025_2025
  RCL_FROM_2025_2026
)

echo "Downloading to $TARGET_DIR"
echo

for f in "${ARCHIVES[@]}"; do
  echo "  ${f}.zip"
  curl --fail --location --remote-time --silent --show-error \
       --output "${f}.zip" \
       "${BASE_URL}/${f}.zip"
done

echo "  Import_Instructions_Recalls.pdf"
curl --fail --location --remote-time --silent --show-error \
     --output "Import_Instructions_Recalls.pdf" \
     "${BASE_URL}/Import_Instructions_Recalls.pdf"

echo
echo "=== Downloaded files (mtime = NHTSA's Last-Modified) ==="
ls -lh

echo
echo "=== ZIP contents ==="
for z in *.zip; do
  echo
  echo "--- $z ---"
  unzip -l "$z"
done
