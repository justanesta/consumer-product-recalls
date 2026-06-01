from __future__ import annotations

import sys
from pathlib import Path

# scripts/ is not on sys.path by default; add the repo root so we can import the
# driver as a regular module for testing its pure logic.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.uscg.seed_manufacturer_details_chunked import (  # noqa: E402  — sys.path mutated
    ChunkResult,
    _chunk_command,
    decide_after_chunk,
    parse_chunk_output,
)


class TestParseChunkOutput:
    def test_parses_summary_line(self) -> None:
        out = "uscg_manufacturer_details: fetched=500 loaded=487 rejected=13\n"
        assert parse_chunk_output(out) == ChunkResult(fetched=500, loaded=487, rejected=13)

    def test_parses_summary_amid_json_log_lines(self) -> None:
        out = (
            '{"event": "extraction.started", "source": "uscg_manufacturer_details"}\n'
            '{"event": "extraction.completed", "records_loaded": 500}\n'
            "uscg_manufacturer_details: fetched=500 loaded=500 rejected=0\n"
        )
        assert parse_chunk_output(out) == ChunkResult(500, 500, 0)

    def test_takes_last_summary_when_multiple(self) -> None:
        # Defensive: if two summaries somehow appear, the final one wins.
        out = "x: fetched=1 loaded=1 rejected=0\ny: fetched=2 loaded=2 rejected=0\n"
        assert parse_chunk_output(out) == ChunkResult(2, 2, 0)

    def test_zero_counts_parse(self) -> None:
        out = "uscg_manufacturer_details: fetched=0 loaded=0 rejected=0\n"
        assert parse_chunk_output(out) == ChunkResult(0, 0, 0)

    def test_no_summary_returns_none(self) -> None:
        assert parse_chunk_output("Traceback (most recent call last): ...\n") is None

    def test_empty_output_returns_none(self) -> None:
        assert parse_chunk_output("") is None


class TestDecideAfterChunk:
    def test_full_chunk_with_progress_continues(self) -> None:
        assert decide_after_chunk(ChunkResult(500, 500, 0), limit=500) == "continue"

    def test_full_chunk_with_partial_load_continues(self) -> None:
        # Some rejects, but progress was made → keep going.
        assert decide_after_chunk(ChunkResult(500, 480, 20), limit=500) == "continue"

    def test_partial_chunk_is_done(self) -> None:
        # Fewer than the cap fetched → work-list drained.
        assert decide_after_chunk(ChunkResult(317, 317, 0), limit=500) == "done"

    def test_empty_chunk_is_done(self) -> None:
        assert decide_after_chunk(ChunkResult(0, 0, 0), limit=500) == "done"

    def test_full_chunk_loading_nothing_stalls(self) -> None:
        # A full chunk that loaded 0 → work-list not shrinking → infinite-loop guard.
        assert decide_after_chunk(ChunkResult(500, 0, 500), limit=500) == "stall"

    def test_partial_chunk_loading_nothing_is_still_done(self) -> None:
        # fetched < limit takes precedence: the list drained even if all rejected.
        assert decide_after_chunk(ChunkResult(50, 0, 50), limit=500) == "done"


class TestChunkCommand:
    def test_builds_expected_argv(self) -> None:
        assert _chunk_command(500, "historical_seed") == [
            "recalls",
            "extract",
            "uscg_manufacturer_details",
            "--limit",
            "500",
            "--change-type",
            "historical_seed",
        ]

    def test_limit_is_stringified(self) -> None:
        cmd = _chunk_command(1, "routine")
        assert "--limit" in cmd
        assert cmd[cmd.index("--limit") + 1] == "1"
