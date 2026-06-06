#!/usr/bin/env bash
#
# Chunked FDA press-release historical seed (deep-rescan).
#
# PURPOSE: drive the full ~50.5K-event `recalls deep-rescan fda_press_releases`
# sweep in resumable chunks. The extractor lands+loads bronze ONCE at the end of
# each run, so a chunk is the unit of durability AND resumability: a chunk that
# finishes durably lands its rows; a chunk that throttles costs only itself, and
# content-hash dedup makes re-running it free (idempotent). A single uncapped 50K
# sweep that throttles near the end would instead lose the whole run.
#
# WHAT IT DOES: runs chunk = `--limit N` events in recall_event_id order, reads
# `chunk_max_event_id` from the chunk's log (the deep_rescan.extract line — empty
# events land no bronze row, so the cursor is NOT recoverable from the DB), and
# feeds it as the next chunk's `--resume-after-event-id`. Stops when a chunk's
# work-list is exhausted (events == 0) or is a final partial chunk (events < N).
# Re-execs itself under `systemd-inhibit` so laptop idle/sleep cannot pause it.
#
# RUN FROM THE REPO ROOT (so `recalls` is on PATH via direnv and logs/ resolves):
#   ./scripts/fda_press_releases/seed_chunked.sh                 # fresh, from the start
#   START_CURSOR=32439 ./scripts/fda_press_releases/seed_chunked.sh   # resume after a prior chunk's chunk_max_event_id
#   LIMIT=10000 ./scripts/fda_press_releases/seed_chunked.sh     # bigger chunks (fewer hand-offs; same ~18-25h floor — pacing-bound)
#
# ENV KNOBS (all optional):
#   LIMIT             chunk size in events            (default 5000)
#   START_CURSOR      --resume-after-event-id for the FIRST chunk; empty = start  (default "")
#   CHANGE_TYPE       extraction_runs label           (default historical_seed)
#   LOG_DIR           per-chunk log directory         (default logs)
#   COOLDOWN_SECONDS  wait after a chunk failure before retrying; the iRES
#                     text/html anti-abuse throttle wants >=30 min  (default 1800)
#   MAX_RETRIES       attempts per chunk before aborting            (default 3)
#   RECALLS_BIN       CLI entrypoint                  (default recalls)
#
# NOTE: total runtime is pacing-bound (inter_event_sleep_seconds=1.0 in the
# extractor, not a CLI flag) — ~18-25h for the full corpus regardless of LIMIT.
# Most of the OLDEST events carry no press release (0 rows is expected early);
# rows appear in the newer high-event-id chunks.
#
# Ref: src/extractors/fda_press_release.py (FdaPressReleaseDeepRescanLoader),
#      project_scope/phase-6-execution-plan.md, migration 0022/0023.

set -uo pipefail

# --- Re-exec under systemd-inhibit so a closed lid / idle does not suspend the
#     seed. Guard env var prevents an infinite re-exec; falls back with a warning
#     if systemd-inhibit is unavailable (the seed still runs, just pausable). ---
if [[ -z "${SEED_INHIBITED:-}" ]]; then
    if command -v systemd-inhibit >/dev/null 2>&1; then
        export SEED_INHIBITED=1
        exec systemd-inhibit --what=sleep:idle --mode=block \
            --why="FDA press-release historical seed" "$0" "$@"
    else
        echo "warn: systemd-inhibit not found — laptop sleep will PAUSE the seed" >&2
        echo "      (still safe + idempotent, just slower wall-clock)." >&2
    fi
fi

# --- Config (override via env) ---
SOURCE="fda_press_releases"
LIMIT="${LIMIT:-5000}"
START_CURSOR="${START_CURSOR:-}"
CHANGE_TYPE="${CHANGE_TYPE:-historical_seed}"
LOG_DIR="${LOG_DIR:-logs}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-1800}"
MAX_RETRIES="${MAX_RETRIES:-3}"
RECALLS_BIN="${RECALLS_BIN:-recalls}"

# --- Pre-flight ---
command -v "$RECALLS_BIN" >/dev/null 2>&1 || {
    echo "error: '$RECALLS_BIN' not on PATH — activate the venv / direnv, run from the repo root." >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || {
    echo "error: python3 not on PATH (needed to parse chunk logs)." >&2
    exit 1
}
mkdir -p "$LOG_DIR"

# Parse "<events> <chunk_max_event_id>" from a chunk log's deep_rescan.extract
# line. chunk_max_event_id null (empty work-list) -> empty string.
extract_progress() {
    grep '"fda_press_releases.deep_rescan.extract"' "$1" 2>/dev/null | tail -1 | python3 -c '
import json, sys
line = sys.stdin.readline()
try:
    d = json.loads(line)
    cmid = d.get("chunk_max_event_id")
    print(int(d.get("events", 0)), "" if cmid is None else int(cmid))
except Exception:
    print(0, "")
'
}

cursor="$START_CURSOR"
chunk=0
total_loaded=0

echo "=== FDA press-release chunked historical seed ==="
echo "limit=$LIMIT  start_cursor=${START_CURSOR:-<from start>}  change_type=$CHANGE_TYPE"
echo "cooldown=${COOLDOWN_SECONDS}s  max_retries=$MAX_RETRIES  logs=$LOG_DIR/seed_pr_chunkNN_<ts>.log"
echo

while true; do
    chunk=$((chunk + 1))
    ts="$(date +%Y-%m-%dT%H%M%S)"
    log="$LOG_DIR/seed_pr_chunk$(printf '%02d' "$chunk")_${ts}.log"

    args=(deep-rescan "$SOURCE" --change-type "$CHANGE_TYPE" --limit "$LIMIT")
    [[ -n "$cursor" ]] && args+=(--resume-after-event-id "$cursor")

    # Run the chunk, retrying with a cooldown on failure (throttle/network).
    # Re-running the same cursor is idempotent (nothing loads until success).
    attempt=0
    while true; do
        attempt=$((attempt + 1))
        echo "[chunk $chunk | attempt $attempt] cursor=${cursor:-<start>} limit=$LIMIT -> $log"
        if "$RECALLS_BIN" "${args[@]}" >"$log" 2>&1; then
            break
        fi
        if ((attempt >= MAX_RETRIES)); then
            echo "ERROR: chunk $chunk failed after $attempt attempts (see $log)." >&2
            echo "       Resume from here with:  START_CURSOR='${cursor}' LIMIT=$LIMIT $0" >&2
            exit 1
        fi
        echo "warn: chunk $chunk failed (attempt $attempt); cooling down ${COOLDOWN_SECONDS}s before retry." >&2
        echo "      (iRES anti-abuse throttle guidance: wait >=30 min). See $log." >&2
        sleep "$COOLDOWN_SECONDS"
    done

    # Progress + cumulative loaded.
    read -r events cmid < <(extract_progress "$log")
    : "${events:=0}"
    loaded="$(grep -E "deep-rescan: fetched=" "$log" 2>/dev/null | tail -1 | grep -oE 'loaded=[0-9]+' | grep -oE '[0-9]+' | head -1)"
    : "${loaded:=0}"
    total_loaded=$((total_loaded + loaded))
    echo "    -> events=$events  chunk_max_event_id=${cmid:-<none>}  loaded=$loaded  (cumulative loaded=$total_loaded)"

    # Exhausted: empty work-list past the cursor.
    if [[ "$events" -eq 0 || -z "$cmid" ]]; then
        echo
        echo "Done — work-list exhausted at chunk $chunk. Total press-release rows loaded: $total_loaded."
        break
    fi

    # Safety: the cursor must strictly advance, else abort to avoid a loop.
    if [[ -n "$cursor" && "$cmid" == "$cursor" ]]; then
        echo "ERROR: cursor did not advance (still $cmid); aborting." >&2
        exit 1
    fi
    cursor="$cmid"

    # Final partial chunk: fewer events than the limit means nothing remains.
    if [[ "$events" -lt "$LIMIT" ]]; then
        echo
        echo "Done — final partial chunk ($events < $LIMIT) at chunk $chunk. Total press-release rows loaded: $total_loaded."
        break
    fi
done
