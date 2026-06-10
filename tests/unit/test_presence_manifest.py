"""Tests for the ADR 0026 per-run presence manifest (Phase 6c commit 6c.0).

Two layers:
  * the pure builder ``src.bronze.manifest.build_presence_manifest_rows`` — recall-grain
    row construction, dedup, langcode handling (no DB);
  * the ``Extractor._maybe_write_presence_manifest`` gating — only successful runs of a
    ``default_track_presence`` source with stashed passing records write the manifest.

The gating tests use ``types.SimpleNamespace`` stand-ins for bronze records (the builder
only reads ``source_recall_id`` / ``langcode`` via ``getattr``), and a ``MagicMock``
connection to assert whether the manifest insert is executed — no DB.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from pydantic import SecretStr

from src.bronze.manifest import build_presence_manifest_rows
from src.config.settings import Settings

# --- Pure builder tests (no fixtures) ---


def test_single_key_source_emits_null_langcode() -> None:
    records = [SimpleNamespace(source_recall_id="A1"), SimpleNamespace(source_recall_id="A2")]
    rows = build_presence_manifest_rows(records, run_id="r1", source="cpsc")
    assert rows == [
        {"run_id": "r1", "source": "cpsc", "source_recall_id": "A1", "langcode": None},
        {"run_id": "r1", "source": "cpsc", "source_recall_id": "A2", "langcode": None},
    ]


def test_bilingual_source_carries_langcode() -> None:
    records = [
        SimpleNamespace(source_recall_id="004-2020", langcode="English"),
        SimpleNamespace(source_recall_id="004-2020", langcode="Spanish"),
    ]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert len(rows) == 2
    assert {r["langcode"] for r in rows} == {"English", "Spanish"}
    # Same source_recall_id, distinct langcodes → both present (bilingual siblings).
    assert all(r["source_recall_id"] == "004-2020" for r in rows)


def test_dedup_collapses_repeated_identity() -> None:
    records = [
        SimpleNamespace(source_recall_id="A1", langcode="English"),
        SimpleNamespace(source_recall_id="A1", langcode="English"),  # repeat
        SimpleNamespace(source_recall_id="A1", langcode="Spanish"),
    ]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert len(rows) == 2  # (A1, English) collapsed; (A1, Spanish) distinct


def test_empty_records_yields_empty() -> None:
    assert build_presence_manifest_rows([], run_id="r1", source="usda") == []


def test_missing_source_recall_id_is_skipped() -> None:
    records = [
        SimpleNamespace(source_recall_id=None),
        SimpleNamespace(source_recall_id="A1"),
    ]
    rows = build_presence_manifest_rows(records, run_id="r1", source="usda")
    assert [r["source_recall_id"] for r in rows] == ["A1"]


def test_source_recall_id_coerced_to_str() -> None:
    # Some schemas surface numeric ids; the manifest column is text.
    records = [SimpleNamespace(source_recall_id=12345)]
    rows = build_presence_manifest_rows(records, run_id="r1", source="nhtsa")
    assert rows[0]["source_recall_id"] == "12345"


def test_langcode_field_none_ignores_langcode_attr() -> None:
    # When langcode_field is not requested, a present langcode attr is NOT carried.
    records = [SimpleNamespace(source_recall_id="A1", langcode="English")]
    rows = build_presence_manifest_rows(records, run_id="r1", source="cpsc")
    assert rows[0]["langcode"] is None


def test_source_recall_id_trimmed_to_silver_form() -> None:
    # Finding R: ~5 USDA ids carry whitespace; silver trims, so the manifest must too.
    records = [SimpleNamespace(source_recall_id=" 021-2020 ", langcode="English")]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert rows[0]["source_recall_id"] == "021-2020"


def test_trim_collapses_whitespace_variants() -> None:
    # Defensive: ' 021-2020 ' and '021-2020' dedup to one (FSIS emits consistently, but
    # the trimmed key makes a theoretical dual-form run collision-safe).
    records = [
        SimpleNamespace(source_recall_id=" 021-2020 ", langcode="English"),
        SimpleNamespace(source_recall_id="021-2020", langcode="English"),
    ]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert len(rows) == 1
    assert rows[0]["source_recall_id"] == "021-2020"


def test_whitespace_only_id_skipped() -> None:
    records = [SimpleNamespace(source_recall_id="   "), SimpleNamespace(source_recall_id="A1")]
    rows = build_presence_manifest_rows(records, run_id="r1", source="usda")
    assert [r["source_recall_id"] for r in rows] == ["A1"]


def test_langcode_field_requested_but_record_langcode_none() -> None:
    # langcode_field is asked for, but the record's langcode is None → carried as NULL,
    # not raised. ``getattr(record, langcode_field, None)`` returns None gracefully.
    records = [SimpleNamespace(source_recall_id="A1", langcode=None)]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert len(rows) == 1
    assert rows[0]["langcode"] is None


def test_langcode_field_requested_but_attr_missing_entirely() -> None:
    # The record doesn't carry the langcode attribute at all → getattr default None,
    # no AttributeError. Mixed-shape rosters don't crash the manifest builder.
    records = [SimpleNamespace(source_recall_id="A1")]  # no langcode attr
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert len(rows) == 1
    assert rows[0]["langcode"] is None


def test_records_sharing_id_with_one_null_langcode_stay_distinct() -> None:
    # (A1, "English") and (A1, None) are distinct presence keys — the None variant is not
    # collapsed into the English one.
    records = [
        SimpleNamespace(source_recall_id="A1", langcode="English"),
        SimpleNamespace(source_recall_id="A1", langcode=None),
    ]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="usda", langcode_field="langcode"
    )
    assert len(rows) == 2
    assert {r["langcode"] for r in rows} == {"English", None}


def test_recall_id_field_overrides_the_recall_key() -> None:
    # C16: NHTSA keys presence on campno, not the bronze source_recall_id (regen-unstable
    # RECORD_ID). The builder reads whichever attribute recall_id_field names; the OUTPUT column
    # stays source_recall_id (the manifest table column).
    records = [SimpleNamespace(source_recall_id="999999", campno="21V-123")]
    rows = build_presence_manifest_rows(
        records, run_id="r1", source="nhtsa", recall_id_field="campno"
    )
    assert rows == [
        {"run_id": "r1", "source": "nhtsa", "source_recall_id": "21V-123", "langcode": None}
    ]


# --- Gating tests (Extractor._maybe_write_presence_manifest) ---


@pytest.fixture
def fake_settings() -> Settings:
    return Settings(
        neon_database_url=SecretStr("postgresql://test:test@localhost/test"),
        r2_account_id="test",
        r2_access_key_id=SecretStr("test"),
        r2_secret_access_key=SecretStr("test"),
        r2_bucket_name="test",
    )


@pytest.fixture
def patch_extractor_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sa, "create_engine", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr("src.landing.r2.R2LandingClient", lambda *args, **kwargs: MagicMock())


def _usda_extractor(settings: Settings) -> Any:
    from src.extractors import usda as mod

    return mod.UsdaExtractor(base_url="https://example.test/usda", settings=settings)


def _cpsc_extractor(settings: Settings) -> Any:
    from src.extractors import cpsc as mod

    return mod.CpscExtractor(base_url="https://example.test/cpsc", settings=settings)


def test_manifest_written_for_tracked_source(
    patch_extractor_dependencies: None, fake_settings: Settings
) -> None:
    extractor = _usda_extractor(fake_settings)
    extractor._passing_records = [
        SimpleNamespace(source_recall_id="004-2020", langcode="English"),
        SimpleNamespace(source_recall_id="004-2020", langcode="Spanish"),
    ]
    conn = MagicMock()

    extractor._maybe_write_presence_manifest(conn, run_id="run-1", status="success")

    assert conn.execute.call_count == 1
    rows = conn.execute.call_args.args[1]
    assert len(rows) == 2
    assert all(r["run_id"] == "run-1" and r["source"] == "usda" for r in rows)
    assert {r["langcode"] for r in rows} == {"English", "Spanish"}


def test_manifest_skipped_for_untracked_source(
    patch_extractor_dependencies: None, fake_settings: Settings
) -> None:
    extractor = _cpsc_extractor(fake_settings)  # cpsc: default_track_presence=False
    extractor._passing_records = [SimpleNamespace(source_recall_id="A1")]
    conn = MagicMock()

    extractor._maybe_write_presence_manifest(conn, run_id="run-1", status="success")

    assert not conn.execute.called


def test_manifest_skipped_on_non_success_status(
    patch_extractor_dependencies: None, fake_settings: Settings
) -> None:
    extractor = _usda_extractor(fake_settings)
    extractor._passing_records = [SimpleNamespace(source_recall_id="A1", langcode="English")]
    conn = MagicMock()

    for status in ("aborted", "failed"):
        conn.reset_mock()
        extractor._maybe_write_presence_manifest(conn, run_id="run-1", status=status)
        assert not conn.execute.called


def test_manifest_skipped_when_no_passing_records(
    patch_extractor_dependencies: None, fake_settings: Settings
) -> None:
    extractor = _usda_extractor(fake_settings)
    extractor._passing_records = None  # run failed before validation
    conn = MagicMock()

    extractor._maybe_write_presence_manifest(conn, run_id="run-1", status="success")

    assert not conn.execute.called


def test_manifest_no_insert_on_empty_passing_records(
    patch_extractor_dependencies: None, fake_settings: Settings
) -> None:
    # 304 path: passing_records == [] → no insert (empty executemany guarded).
    extractor = _usda_extractor(fake_settings)
    extractor._passing_records = []
    conn = MagicMock()

    extractor._maybe_write_presence_manifest(conn, run_id="run-1", status="success")

    assert not conn.execute.called


def test_track_presence_sources_are_pinned() -> None:
    """Registry pin (C16 / #71): exactly USDA + NHTSA carry track_presence. The RUN that writes each
    source's manifest must enumerate the FULL corpus — that gate is pinned separately by
    test_presence_manifest_writers_are_pinned. A True for CPSC/FDA/USCG would silently produce wrong
    presence dims; this fails loudly if the set drifts."""
    from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME

    tracked = {
        name for name, c in DEDUP_CONTRACT_BY_SOURCE_NAME.items() if c.default_track_presence
    }
    assert tracked == {"usda", "nhtsa"}


def test_manifest_skipped_when_not_a_full_corpus_writer(
    patch_extractor_dependencies: None, fake_settings: Settings
) -> None:
    # C16: a track_presence source whose run is NOT a full-corpus enumeration
    # (writes_presence_manifest False) writes nothing — the NHTSA daily-POST_2010 case. Simulated
    # by flipping the flag on the USDA extractor, to avoid standing up the NHTSA loader's download
    # path.
    extractor = _usda_extractor(fake_settings)
    extractor.writes_presence_manifest = False
    extractor._passing_records = [SimpleNamespace(source_recall_id="A1", langcode="English")]
    conn = MagicMock()

    extractor._maybe_write_presence_manifest(conn, run_id="run-1", status="success")

    assert not conn.execute.called


def test_presence_manifest_writers_are_pinned() -> None:
    """C16: the manifest is written ONLY by extractors whose run enumerates the FULL corpus —
    UsdaExtractor (daily full-dump) and NhtsaDeepRescanLoader (both archives). The daily
    NhtsaExtractor pulls POST_2010 only and must NOT write (a daily manifest would mark pre-2010
    campnos as false retractions). Pinned so a refactor can't silently flip these."""
    from src.extractors.nhtsa import NhtsaDeepRescanLoader, NhtsaExtractor
    from src.extractors.usda import UsdaExtractor

    assert UsdaExtractor.model_fields["writes_presence_manifest"].default is True
    assert NhtsaDeepRescanLoader.model_fields["writes_presence_manifest"].default is True
    # The daily incremental extract is POST_2010-only (partial) — it must not write the manifest.
    assert NhtsaExtractor.model_fields["writes_presence_manifest"].default is False


def test_nhtsa_presence_keys_on_campno() -> None:
    """C16: NHTSA's manifest recall key is campno (the recall_event grain), not the regen-unstable
    RECORD_ID that bronze stores as source_recall_id; USDA uses the default source_recall_id."""
    from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME

    assert DEDUP_CONTRACT_BY_SOURCE_NAME["nhtsa"].presence_recall_id_field == "campno"
    assert DEDUP_CONTRACT_BY_SOURCE_NAME["usda"].presence_recall_id_field is None
