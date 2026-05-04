#!/usr/bin/env bash
#
# Probe NHTSA recall data URLs to characterize watermark reliability.
#
# Hypothesis under test: every file in https://static.nhtsa.gov/odi/ffdd/rcl/
# carries the same Last-Modified timestamp because NHTSA runs a daily
# regeneration job that re-stamps every file regardless of content change.
# If true, Last-Modified is useless for incremental extraction and content
# hashing (ADR 0007) is the only reliable change-detection mechanism.
#
# Captures HEAD response headers and body sha256 for every file on every
# probe. Body sha256 is the ground-truth content fingerprint; ETag is the
# server-claimed content fingerprint; comparing them across probes tells us
# whether ETag is genuinely content-bound. Append-only JSONL, safe to run
# repeatedly. Per-probe bandwidth ~33 MB; 14-day study ~460 MB.
#
# Usage:
#   ./scripts/nhtsa/probe_watermarks.sh
#
# Run daily (manually or via cron) for >=7 days, ideally bracketing one
# real upstream content update, before drawing the verdict. See the
# `analysis snippets` block at the bottom of this file for jq one-liners
# to summarize the accumulated data.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
OUTPUT_DIR="${REPO_ROOT}/documentation/nhtsa"
OUTPUT_FILE="${OUTPUT_DIR}/watermark_probes.jsonl"

mkdir -p "$OUTPUT_DIR"

BASE_URL="https://static.nhtsa.gov/odi/ffdd/rcl"
URLS=(
  "Import_Instructions_Recalls.pdf"
  "RCL.txt"
  "RCL_Annual_Rpts.txt"
  "RCL_Qtrly_Rpts.txt"
  "FLAT_RCL_Annual_Rpts.zip"
  "FLAT_RCL_Qrtly_Rpts.zip"
  "FLAT_RCL_PRE_2010.zip"
  "FLAT_RCL_POST_2010.zip"
  "RCL_FROM_2000_2004.zip"
  "RCL_FROM_2005_2009.zip"
  "RCL_FROM_2010_2014.zip"
  "RCL_FROM_2015_2019.zip"
  "RCL_FROM_2020_2024.zip"
  "RCL_FROM_2025_2025.zip"
  "RCL_FROM_2025_2026.zip"
)

probed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "Probe at $probed_at"
printf "%-38s  %-31s  %-12s  %s\n" "FILE" "LAST-MODIFIED" "BYTES" "BODY-SHA256-PREFIX"
printf '%.0s-' {1..110}; echo

extract_header() {
  # Case-insensitive header extraction. $1 = header name, stdin = full HEAD response.
  local name="$1"
  awk -v name="$name" -F': ' '
    BEGIN { IGNORECASE = 1 }
    tolower($1) == tolower(name) { sub(/\r$/, "", $2); val = $2 }
    END { print val }
  '
}

for filename in "${URLS[@]}"; do
  url="${BASE_URL}/${filename}"

  # -s silent, -I HEAD only, -L follow redirects, --max-time bounds the request.
  # No --fail: HTTP errors should be recorded, not aborted on.
  headers=$(curl -sIL --max-time 30 "$url" 2>/dev/null || echo "")

  if [[ -z "$headers" ]]; then
    status="curl_failed"
    last_modified=""
    etag=""
    content_length=""
    content_type=""
    cache_control=""
    accept_ranges=""
    response_date=""
    x_amz_version_id=""
    x_amz_replication_status=""
  else
    # Status line of the *final* response (after any redirects)
    status=$(echo "$headers" | awk '/^HTTP\// {s=$2} END {print s}')
    last_modified=$(echo "$headers"             | extract_header "last-modified")
    etag=$(echo "$headers"                      | extract_header "etag")
    content_length=$(echo "$headers"            | extract_header "content-length")
    content_type=$(echo "$headers"              | extract_header "content-type")
    cache_control=$(echo "$headers"             | extract_header "cache-control")
    accept_ranges=$(echo "$headers"             | extract_header "accept-ranges")
    response_date=$(echo "$headers"             | extract_header "date")
    x_amz_version_id=$(echo "$headers"          | extract_header "x-amz-version-id")
    x_amz_replication_status=$(echo "$headers"  | extract_header "x-amz-replication-status")
  fi

  # Full body fetch + hash for every file. Temp file is ephemeral: written,
  # hashed, deleted in the same iteration. bytes_observed is the actual
  # on-disk size, which is reliable even when HEAD's Content-Length is
  # missing (NHTSA omits it on the data files but reports it on the PDF).
  body_sha256=""
  bytes_observed=""
  tmp=$(mktemp)
  if curl -sL --max-time 180 -o "$tmp" "$url" 2>/dev/null; then
    bytes_observed=$(stat -c %s "$tmp" 2>/dev/null || stat -f %z "$tmp" 2>/dev/null || echo "")
    body_sha256=$(sha256sum "$tmp" | awk '{print $1}')
  fi
  rm -f "$tmp"

  hash_display="${body_sha256:0:16}"
  [[ -z "$body_sha256" ]] && hash_display="(fetch failed)"

  printf "%-38s  %-31s  %-12s  %s\n" \
    "$filename" "${last_modified:-(none)}" "${bytes_observed:-?}" "$hash_display"

  jq -nc \
    --arg probed_at                "$probed_at" \
    --arg url                      "$url" \
    --arg filename                 "$filename" \
    --arg status                   "$status" \
    --arg response_date            "$response_date" \
    --arg last_modified            "$last_modified" \
    --arg etag                     "$etag" \
    --arg content_length           "$content_length" \
    --arg bytes_observed           "$bytes_observed" \
    --arg content_type             "$content_type" \
    --arg cache_control            "$cache_control" \
    --arg accept_ranges            "$accept_ranges" \
    --arg x_amz_version_id         "$x_amz_version_id" \
    --arg x_amz_replication_status "$x_amz_replication_status" \
    --arg body_sha256              "$body_sha256" \
    '{probed_at: $probed_at, filename: $filename, url: $url,
      status: $status, response_date: $response_date,
      last_modified: $last_modified, etag: $etag,
      content_length: $content_length, bytes_observed: $bytes_observed,
      content_type: $content_type, cache_control: $cache_control,
      accept_ranges: $accept_ranges,
      x_amz_version_id: $x_amz_version_id,
      x_amz_replication_status: $x_amz_replication_status,
      body_sha256: $body_sha256}' \
    >> "$OUTPUT_FILE"
done

echo
echo "Appended ${#URLS[@]} records to $OUTPUT_FILE"
echo "Total records: $(wc -l < "$OUTPUT_FILE")"

# ---------------------------------------------------------------------------
# Analysis snippets (run after >=7 days of data accumulation)
# ---------------------------------------------------------------------------
#
# 1) Distinct Last-Modified values per file (regen-stamp test):
#    Expect: one distinct value if the file is genuinely static, multiple
#    if the regen job re-stamps it.
#
#      jq -r '[.filename, .last_modified] | @tsv' \
#        documentation/nhtsa/watermark_probes.jsonl \
#        | sort -u
#
# 2) Lockstep test — do all files share the same Last-Modified per probe?
#    If yes on every probe, regen-stamp confirmed.
#
#      jq -r '[.probed_at, .last_modified] | @tsv' \
#        documentation/nhtsa/watermark_probes.jsonl \
#        | sort -u
#
# 3) Smoking gun — does Import_Instructions_Recalls.pdf advance Last-Modified
#    across days? It hasn't been edited since 2023, so any advance is proof
#    Last-Modified tracks the regen job, not content.
#
#      jq -r 'select(.filename == "Import_Instructions_Recalls.pdf")
#             | [.probed_at, .last_modified, .body_sha256] | @tsv' \
#        documentation/nhtsa/watermark_probes.jsonl
#
# 4) Body-hash stability for the rolling-window file (the one the
#    incremental extractor would actually hit):
#
#      jq -r 'select(.filename == "RCL_FROM_2025_2026.zip")
#             | [.probed_at, .last_modified, .body_sha256[:16]] | @tsv' \
#        documentation/nhtsa/watermark_probes.jsonl
