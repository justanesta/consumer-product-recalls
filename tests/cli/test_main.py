from __future__ import annotations

import re
from contextlib import contextmanager
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    import pytest

from src.bronze.recovery import (
    RECOVERY_CONFIG_BY_SOURCE_NAME,
    RecoveryResult,
    recoverable_past_date_sanity,
)
from src.cli.main import app

runner = CliRunner()


@contextmanager
def _patch_extractor(source: str, mock_extractor: MagicMock) -> Generator[MagicMock, None, None]:
    """Patch the routine extractor registry so ``source`` resolves to a
    MagicMock class returning ``mock_extractor`` on construction.

    Yields the mock class so tests can assert on constructor kwargs
    (``mock_cls.call_args.kwargs[...]``). On exit, the registry's original
    entry for ``source`` is restored — patch.dict's standard behavior.
    """
    mock_cls = MagicMock(return_value=mock_extractor)
    with patch.dict(
        "src.config.source_registry.EXTRACTOR_BY_SOURCE_NAME",
        {source: mock_cls},
    ):
        yield mock_cls


@contextmanager
def _patch_deep_rescan(source: str, mock_loader: MagicMock) -> Generator[MagicMock, None, None]:
    """Same as ``_patch_extractor`` but for the deep-rescan registry."""
    mock_cls = MagicMock(return_value=mock_loader)
    with patch.dict(
        "src.config.source_registry.DEEP_RESCAN_BY_SOURCE_NAME",
        {source: mock_cls},
    ):
        yield mock_cls


def test_version_command_prints_expected_string() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    # Verify the output shape (package name + version-looking suffix) without
    # pinning to a specific version number — version is read at runtime via
    # importlib.metadata, so hardcoding "0.1.0" here would force a test edit
    # on every pyproject.toml bump for no real signal.
    assert re.match(r"^consumer-product-recalls \d+\.\d+\.\d+", result.output)


def test_version_command_exits_with_zero_exit_code() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# extract command
# ---------------------------------------------------------------------------

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


def _fake_run_result(
    fetched: int = 5,
    loaded: int = 5,
    rejected_validate: int = 0,
    rejected_invariants: int = 0,
) -> MagicMock:
    r = MagicMock()
    r.records_fetched = fetched
    r.records_loaded = loaded
    r.records_rejected_validate = rejected_validate
    r.records_rejected_invariants = rejected_invariants
    return r


def test_extract_cpsc_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=10, loaded=9)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("cpsc", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "cpsc"])

    assert result.exit_code == 0
    assert "fetched=10" in result.output
    assert "loaded=9" in result.output
    assert "rejected=0" in result.output


def test_extract_cpsc_lookback_days_calls_override_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per Wave 2: --lookback-days routes through the new public
    override_watermark_lookback method rather than the CLI reaching into
    extractor._engine directly."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("cpsc", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "cpsc", "--lookback-days", "7"])

    assert result.exit_code == 0
    mock_extractor.override_watermark_lookback.assert_called_once_with(7)


def test_extract_unknown_source_exits_with_error() -> None:
    result = runner.invoke(app, ["extract", "unknown_source"])
    assert result.exit_code == 1
    assert "Unknown source" in result.output


def test_recover_rejected_out_of_scope_source_exits_1() -> None:
    # uscg_manufacturers is a real source but does not call check_date_sanity, so it is
    # intentionally absent from the recovery map — the guard fires before any DB access.
    result = runner.invoke(app, ["recover-rejected", "uscg_manufacturers"])
    assert result.exit_code == 1
    assert "not implemented" in result.output


@contextmanager
def _patch_recover(
    recovery_result: RecoveryResult,
) -> Generator[tuple[MagicMock, MagicMock], None, None]:
    """Patch the recover-rejected command's deps (Settings/engine/recover_quarantined).

    Yields (mock_recover, mock_engine) so tests can assert dispatch wiring. Settings is
    patched so no env vars are needed; the engine is a sentinel for call-arg assertions.
    """
    mock_engine = MagicMock()
    with (
        patch("src.cli.main.configure_logging"),
        patch("src.cli.main.Settings"),
        patch("src.cli.main.make_engine", return_value=mock_engine),
        patch("src.cli.main.recover_quarantined", return_value=recovery_result) as mock_recover,
    ):
        yield mock_recover, mock_engine


def test_recover_rejected_dispatches_correct_config_and_predicate() -> None:
    with _patch_recover(RecoveryResult("fda", "fda/x.json.gz", 24, 24)) as (mock_recover, engine):
        result = runner.invoke(app, ["recover-rejected", "fda"])

    assert result.exit_code == 0
    assert "candidates=24" in result.output
    assert "inserted=24" in result.output
    # Guards the source→config and default-predicate wiring (a broken dispatch that always
    # picked FDA's config or the wrong predicate would pass a bare assert_called_once()).
    mock_recover.assert_called_once_with(
        engine,
        source="fda",
        config=RECOVERY_CONFIG_BY_SOURCE_NAME["fda"],
        is_recoverable=recoverable_past_date_sanity,
        landing_path=None,
        dry_run=False,
    )


def test_recover_rejected_dry_run() -> None:
    with _patch_recover(RecoveryResult("fda", "fda/x.json.gz", 24, 0, dry_run=True)):
        result = runner.invoke(app, ["recover-rejected", "fda", "--dry-run"])

    assert result.exit_code == 0
    assert "[dry-run]" in result.output
    assert "candidates=24" in result.output


def test_recover_rejected_no_rejections() -> None:
    with _patch_recover(RecoveryResult("fda", None, 0, 0)):
        result = runner.invoke(app, ["recover-rejected", "fda"])

    assert result.exit_code == 0
    assert "no rejections found" in result.output


def test_recover_rejected_zero_candidates() -> None:
    with _patch_recover(RecoveryResult("fda", "fda/x.json.gz", 0, 0)):
        result = runner.invoke(app, ["recover-rejected", "fda"])

    assert result.exit_code == 0
    assert "0 recoverable rows" in result.output


def test_recover_rejected_partial_dedup_reports_idempotency() -> None:
    with _patch_recover(RecoveryResult("fda", "fda/x.json.gz", 24, 20)):
        result = runner.invoke(app, ["recover-rejected", "fda"])

    assert result.exit_code == 0
    assert "already present" in result.output
    assert "idempotent" in result.output


def test_recover_rejected_reason_contains_and_landing_path_wiring() -> None:
    with _patch_recover(RecoveryResult("fda", "fda/seed.json.gz", 3, 3)) as (mock_recover, _engine):
        result = runner.invoke(
            app,
            [
                "recover-rejected",
                "fda",
                "--reason-contains",
                "in the future",
                "--landing-path",
                "fda/seed.json.gz",
            ],
        )

    assert result.exit_code == 0
    call = mock_recover.call_args
    assert call.kwargs["landing_path"] == "fda/seed.json.gz"
    # --reason-contains routes a reason_contains() closure, NOT the default predicate.
    assert call.kwargs["is_recoverable"] is not recoverable_past_date_sanity


def test_extract_invalid_change_type_exits_with_error() -> None:
    result = runner.invoke(app, ["extract", "cpsc", "--change-type", "bogus"])
    assert result.exit_code == 1
    assert "Invalid --change-type" in result.output
    assert "must be one of" in result.output


def test_extract_fda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=3, loaded=3)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("fda", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "fda"])

    assert result.exit_code == 0
    assert "fda:" in result.output
    assert "fetched=3" in result.output
    assert "loaded=3" in result.output
    assert "rejected=0" in result.output


def test_extract_fda_lookback_days_calls_override_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per Wave 2: --lookback-days routes through the new public
    override_watermark_lookback method rather than the CLI reaching into
    extractor._engine directly."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("fda", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "fda", "--lookback-days", "7"])

    assert result.exit_code == 0
    mock_extractor.override_watermark_lookback.assert_called_once_with(7)


def test_extract_uscg_manufacturer_details_limit_sets_work_list_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--limit sets work_list_limit on the detail extractor (cheap dev
    validation / chunked seeding), mirroring the etag_audit post-construction
    mutation pattern."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=50, loaded=50)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("uscg_manufacturer_details", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "uscg_manufacturer_details", "--limit", "50"])

    assert result.exit_code == 0
    assert mock_extractor.work_list_limit == 50


def test_extract_limit_no_op_for_other_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """--limit on a non-work-list source surfaces an ignored-notice (parity)."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("cpsc", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "cpsc", "--limit", "50"])

    assert result.exit_code == 0
    assert "no effect" in result.output


def test_extract_limit_rejects_non_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    """--limit < 1 exits 1 with a clear message before any extraction work."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    result = runner.invoke(app, ["extract", "cpsc", "--limit", "0"])
    assert result.exit_code == 1
    assert "must be >= 1" in result.output


# ---------------------------------------------------------------------------
# deep-rescan command
# ---------------------------------------------------------------------------


def test_deep_rescan_fda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=150, loaded=149, rejected_validate=1)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_deep_rescan("fda", mock_loader),
    ):
        result = runner.invoke(
            app,
            [
                "deep-rescan",
                "fda",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-04-26",
            ],
        )

    assert result.exit_code == 0
    assert "fda deep-rescan" in result.output
    assert "fetched=150" in result.output
    assert "loaded=149" in result.output
    assert "rejected=1" in result.output
    mock_loader.set_date_range.assert_called_once()


def test_deep_rescan_unknown_source_exits_with_error() -> None:
    result = runner.invoke(
        app,
        ["deep-rescan", "unknown", "--start-date", "2026-01-01", "--end-date", "2026-04-26"],
    )
    assert result.exit_code == 1
    assert "not implemented" in result.output


# ---------------------------------------------------------------------------
# USDA dispatch — extract and deep-rescan
# ---------------------------------------------------------------------------


def test_extract_usda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=2001, loaded=2001)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("usda", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda"])

    assert result.exit_code == 0
    assert "usda:" in result.output
    assert "fetched=2001" in result.output


def test_extract_usda_etag_audit_disables_etag_on_extractor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """change_type=etag_audit must mutate extractor.etag_enabled = False
    after construction so the next request omits If-None-Match. Per the
    audit-run pattern landed 2026-05-10."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("usda", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda", "--change-type", "etag_audit"])

    assert result.exit_code == 0
    assert mock_extractor.etag_enabled is False


def test_extract_etag_audit_rejected_for_non_usda_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """change_type=etag_audit is only supported for usda + usda_establishments;
    using it on cpsc/fda/nhtsa exits with a clear error before any extraction
    work begins."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    result = runner.invoke(app, ["extract", "cpsc", "--change-type", "etag_audit"])
    assert result.exit_code == 1
    assert "etag_audit is only supported" in result.output


def test_extract_usda_lookback_days_warns_but_does_not_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=2001, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("usda", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda", "--lookback-days", "7"])

    assert result.exit_code == 0
    assert "no effect" in result.output


def test_deep_rescan_usda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=2001, loaded=12)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_deep_rescan("usda", mock_loader),
    ):
        result = runner.invoke(app, ["deep-rescan", "usda"])

    assert result.exit_code == 0
    assert "usda deep-rescan" in result.output
    assert "fetched=2001" in result.output


def test_deep_rescan_usda_ignores_date_args(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=2001, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_deep_rescan("usda", mock_loader),
    ):
        result = runner.invoke(
            app,
            [
                "deep-rescan",
                "usda",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-04-26",
            ],
        )

    assert result.exit_code == 0
    assert "ignored" in result.output


def test_deep_rescan_fda_no_dates_runs_full_corpus(monkeypatch: pytest.MonkeyPatch) -> None:
    # No dates → full-corpus historical seed (filter:"[]"); set_full_corpus() is
    # called and the summary prefix reads [full-corpus], not [None → None].
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(
        fetched=134450, loaded=134450, rejected_validate=0
    )

    with (
        patch("src.cli.main.configure_logging"),
        _patch_deep_rescan("fda", mock_loader),
    ):
        result = runner.invoke(app, ["deep-rescan", "fda"])

    assert result.exit_code == 0
    assert "fda deep-rescan [full-corpus]" in result.output
    assert "None" not in result.output
    mock_loader.set_full_corpus.assert_called_once()
    mock_loader.set_date_range.assert_not_called()


def test_deep_rescan_fda_one_date_only_exits_with_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Exactly one date is ambiguous → error (provide both, or neither).
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    with patch("src.cli.main.configure_logging"):
        result = runner.invoke(app, ["deep-rescan", "fda", "--start-date", "2026-01-01"])
    assert result.exit_code == 1
    assert "both" in result.output and "neither" in result.output


# ---------------------------------------------------------------------------
# USDA establishments dispatch — extract only (no deep-rescan path; full-dump every run)
# ---------------------------------------------------------------------------


def test_extract_usda_establishments_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=7945, loaded=7945)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("usda_establishments", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda_establishments"])

    assert result.exit_code == 0
    assert "usda_establishments:" in result.output
    assert "fetched=7945" in result.output
    assert "loaded=7945" in result.output


def test_extract_usda_establishments_lookback_days_warns_but_does_not_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=7945, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("usda_establishments", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda_establishments", "--lookback-days", "7"])

    assert result.exit_code == 0
    assert "no effect" in result.output


# ---------------------------------------------------------------------------
# NHTSA dispatch — extract (incremental) and deep-rescan
# ---------------------------------------------------------------------------


def test_extract_nhtsa_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=240126, loaded=240126)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("nhtsa", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "nhtsa"])

    assert result.exit_code == 0
    assert "nhtsa:" in result.output
    assert "fetched=240126" in result.output
    assert "loaded=240126" in result.output


def test_extract_nhtsa_lookback_days_warns_but_does_not_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NHTSA is a flat-file full-dump every run (Findings B + C);
    --lookback-days has no effect but should be accepted with a notice
    for CLI shape parity with the API-backed sources."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=240126, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("nhtsa", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "nhtsa", "--lookback-days", "7"])

    assert result.exit_code == 0
    assert "no effect" in result.output


def test_extract_nhtsa_with_valid_since_prints_dev_mode_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid --since YYYY-MM-DD must surface a dev-mode notice so the
    operator can't miss that bronze will be a date-bounded slice (an
    intentional ADR 0027 deviation for free-tier-aware dev workflows)."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=10, loaded=10)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("nhtsa", mock_extractor) as mock_cls,
    ):
        result = runner.invoke(app, ["extract", "nhtsa", "--since", "2024-01-01"])

    assert result.exit_code == 0
    assert "--since 2024-01-01 active" in result.output
    assert "dev-mode" in result.output
    # The parsed date is forwarded to the extractor as a datetime.date.
    from datetime import date as _date

    assert mock_cls.call_args.kwargs["since"] == _date(2024, 1, 1)


def test_extract_nhtsa_with_invalid_since_exits_with_error() -> None:
    """An unparseable --since exits 1 BEFORE the extractor is constructed,
    so the operator gets a clear error rather than a far-downstream stack
    trace from inside extract()."""
    result = runner.invoke(app, ["extract", "nhtsa", "--since", "not-a-date"])
    assert result.exit_code == 1
    assert "must be YYYY-MM-DD" in result.output


def test_extract_non_nhtsa_with_since_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--since`` is NHTSA-only; using it on cpsc/fda/usda must surface
    an "ignored" notice so the operator doesn't silently lose the filter
    they thought they applied."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()

    with (
        patch("src.cli.main.configure_logging"),
        _patch_extractor("cpsc", mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "cpsc", "--since", "2024-01-01"])

    assert result.exit_code == 0
    assert "--since is only honored for nhtsa" in result.output


def test_deep_rescan_nhtsa_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=322000, loaded=322000)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_deep_rescan("nhtsa", mock_loader),
    ):
        result = runner.invoke(app, ["deep-rescan", "nhtsa"])

    assert result.exit_code == 0
    assert "nhtsa deep-rescan" in result.output
    assert "fetched=322000" in result.output
    assert "loaded=322000" in result.output


def test_deep_rescan_nhtsa_ignores_date_args(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unlike FDA, NHTSA's deep-rescan archives are partitioned by DATEA
    at the source (Finding H Q2); --start-date/--end-date are accepted
    for CLI shape parity but ignored with a notice."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=322000, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        _patch_deep_rescan("nhtsa", mock_loader),
    ):
        result = runner.invoke(
            app,
            [
                "deep-rescan",
                "nhtsa",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-04-26",
            ],
        )

    assert result.exit_code == 0
    assert "ignored" in result.output


# ---------------------------------------------------------------------------
# re-ingest command — guard branches (all fire before any Settings()/DB access)
# ---------------------------------------------------------------------------


def test_reingest_unsupported_source_exits_with_error() -> None:
    # NHTSA is JSON-replay-out-of-scope → guard fires before any DB/network access.
    result = runner.invoke(
        app,
        [
            "re-ingest",
            "nhtsa",
            "--from-date",
            "2026-01-01",
            "--to-date",
            "2026-01-31",
            "--change-type",
            "schema_rebaseline",
        ],
    )
    assert result.exit_code == 1
    assert "re-ingest not supported for source: nhtsa" in result.output


def test_reingest_invalid_change_type_exits_with_error() -> None:
    result = runner.invoke(
        app,
        [
            "re-ingest",
            "fda",
            "--from-date",
            "2026-01-01",
            "--to-date",
            "2026-01-31",
            "--change-type",
            "routine",  # not a valid re-baseline label
        ],
    )
    assert result.exit_code == 1
    assert "--change-type must be one of" in result.output


def test_reingest_bad_date_format_exits_with_error() -> None:
    result = runner.invoke(
        app,
        [
            "re-ingest",
            "fda",
            "--from-date",
            "not-a-date",
            "--to-date",
            "2026-01-31",
            "--change-type",
            "schema_rebaseline",
        ],
    )
    assert result.exit_code == 1
    assert "must be YYYY-MM-DD" in result.output


def test_reingest_from_after_to_exits_with_error() -> None:
    result = runner.invoke(
        app,
        [
            "re-ingest",
            "fda",
            "--from-date",
            "2026-02-01",
            "--to-date",
            "2026-01-01",
            "--change-type",
            "schema_rebaseline",
        ],
    )
    assert result.exit_code == 1
    assert "is after" in result.output


# ---------------------------------------------------------------------------
# resolve-firms / audit-firm-rollups — CLI dispatch (mock the underlying call)
# ---------------------------------------------------------------------------


def _resolve_summary() -> MagicMock:
    summary = MagicMock()
    summary.distinct_names = 1200
    summary.rows_written = 1200
    summary.cleaned_count = 30
    summary.alias_count = 12
    summary.fei_merged = 0
    summary.fuzzy_merged = 7
    summary.fei_gated = 2
    summary.dry_run = False
    return summary


def test_resolve_firms_dispatches_with_default_options() -> None:
    summary = _resolve_summary()
    with (
        patch("src.cli.main.configure_logging"),
        patch("src.cli.main.Settings"),
        patch("src.cli.main.make_engine", return_value=MagicMock()),
        patch("src.cli.main.resolve_firm_crosswalk", return_value=summary) as mock_resolve,
    ):
        result = runner.invoke(app, ["resolve-firms"])

    assert result.exit_code == 0
    assert "distinct_names=1200" in result.output
    assert "written=1200" in result.output
    # Defaults: rollup on, fei_merge off, threshold 90.
    mock_resolve.assert_called_once()
    kwargs = mock_resolve.call_args.kwargs
    assert kwargs["dry_run"] is False
    assert kwargs["rollup"] is True
    assert kwargs["fei_merge"] is False
    assert kwargs["rollup_threshold"] == 90.0


def test_resolve_firms_forwards_flags_to_underlying_call() -> None:
    summary = _resolve_summary()
    summary.dry_run = True
    with (
        patch("src.cli.main.configure_logging"),
        patch("src.cli.main.Settings"),
        patch("src.cli.main.make_engine", return_value=MagicMock()),
        patch("src.cli.main.resolve_firm_crosswalk", return_value=summary) as mock_resolve,
    ):
        result = runner.invoke(
            app,
            ["resolve-firms", "--dry-run", "--no-rollup", "--rollup-threshold", "97"],
        )

    assert result.exit_code == 0
    assert "[dry-run]" in result.output
    kwargs = mock_resolve.call_args.kwargs
    assert kwargs["dry_run"] is True
    assert kwargs["rollup"] is False
    assert kwargs["rollup_threshold"] == 97.0


def test_audit_firm_rollups_dispatches_and_writes_csv(tmp_path: Path) -> None:
    out_path = tmp_path / "review.csv"
    review = MagicMock()
    review.canonical_name = "ACME CORP"
    review.n_members = 3
    review.min_jaccard = 0.8
    review.weakest_anchor_df = 2
    review.min_score = 99.0
    review.shared_tokens = ["acme", "corp"]
    review.members = ["ACME CORP", "ACME CORPORATION"]
    review.signature = "sig-1"

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.cli.main.Settings"),
        patch("src.cli.main.make_engine", return_value=MagicMock()),
        patch("src.cli.main.audit_rollup_clusters", return_value=[review]) as mock_audit,
    ):
        result = runner.invoke(
            app,
            [
                "audit-firm-rollups",
                "--out",
                str(out_path),
                "--reviewed-ok",
                str(tmp_path / "ok.txt"),
            ],
        )

    assert result.exit_code == 0
    assert "rollup_clusters=1" in result.output
    mock_audit.assert_called_once()
    # The ranked CSV is written to --out with a header + one data row.
    assert out_path.exists()
    lines = out_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0].startswith("risk_rank,")
    assert "ACME CORP" in lines[1]
