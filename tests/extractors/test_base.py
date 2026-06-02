from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError

from src.extractors._base import (
    _TRANSIENT_RETRY,
    AuthenticationError,
    ExtractionAbortedError,
    ExtractionResult,
    Extractor,
    QuarantineRecord,
    RateLimitError,
    TransientExtractionError,
    _is_disconnect,
)

# Pass-through side_effect: makes patched Retrying objects call fn(*args) directly.
_PASSTHROUGH = lambda fn, *a, **kw: fn(*a, **kw)  # noqa: E731


def _op_error(message: str) -> OperationalError:
    """Build a SQLAlchemy OperationalError wrapping a DBAPI error with ``message``."""
    return OperationalError("SELECT 1", None, Exception(message))


# ---------------------------------------------------------------------------
# Neon disconnect detection (_is_disconnect) + _TRANSIENT_RETRY wiring (W5)
# ---------------------------------------------------------------------------


def test_is_disconnect_true_on_libpq_server_closed_signature() -> None:
    exc = _op_error("server closed the connection unexpectedly\n\tThis probably means...")
    assert _is_disconnect(exc) is True


def test_is_disconnect_true_when_connection_invalidated_flag_set() -> None:
    exc = _op_error("a message the string-match alone would miss")
    exc.connection_invalidated = True
    assert _is_disconnect(exc) is True


def test_is_disconnect_false_on_param_overflow_operationalerror() -> None:
    # The bind-parameter overflow guarded against in bronze/loader.py is a
    # deterministic query error — it must NOT be treated as a disconnect.
    exc = _op_error("the number of query arguments cannot exceed 65535")
    assert _is_disconnect(exc) is False


def test_is_disconnect_false_on_non_operationalerror() -> None:
    assert _is_disconnect(TransientExtractionError("network")) is False
    assert _is_disconnect(ValueError("nope")) is False


def test_transient_retry_retries_connection_disconnect() -> None:
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _op_error("server closed the connection unexpectedly")
        return "ok"

    assert _TRANSIENT_RETRY(flaky) == "ok"
    assert calls["n"] == 2


def test_transient_retry_does_not_retry_non_disconnect_operationalerror() -> None:
    calls = {"n": 0}

    def boom() -> str:
        calls["n"] += 1
        raise _op_error("the number of query arguments cannot exceed 65535")

    with pytest.raises(OperationalError):
        _TRANSIENT_RETRY(boom)
    assert calls["n"] == 1


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


def test_transient_extraction_error_is_subclass_of_extraction_error() -> None:
    exc = TransientExtractionError("oops")
    assert isinstance(exc, TransientExtractionError)
    assert isinstance(exc, RuntimeError)


def test_authentication_error_is_subclass_of_extraction_error() -> None:
    exc = AuthenticationError("401")
    assert isinstance(exc, AuthenticationError)
    assert isinstance(exc, RuntimeError)


def test_rate_limit_error_default_retry_after() -> None:
    exc = RateLimitError()
    assert exc.retry_after == 60.0


def test_rate_limit_error_custom_retry_after() -> None:
    exc = RateLimitError(retry_after=120.0)
    assert exc.retry_after == 120.0


def test_rate_limit_error_message_includes_retry_after_value() -> None:
    exc = RateLimitError(retry_after=45.0)
    assert "45" in str(exc)


def test_extraction_aborted_error_stores_all_attributes() -> None:
    exc = ExtractionAbortedError("cpsc", 0.15, 0.05)
    assert exc.source == "cpsc"
    assert exc.rate == 0.15
    assert exc.threshold == 0.05


def test_extraction_aborted_error_message_includes_source_rate_threshold() -> None:
    exc = ExtractionAbortedError("fda", 0.20, 0.05)
    msg = str(exc)
    assert "fda" in msg
    assert "20" in msg
    assert "5" in msg


# ---------------------------------------------------------------------------
# QuarantineRecord
# ---------------------------------------------------------------------------


def test_quarantine_record_stores_all_fields() -> None:
    qr = QuarantineRecord(
        source_recall_id="CPSC-001",
        raw_record={"id": "CPSC-001"},
        failure_reason="missing required field",
        failure_stage="validate",
        raw_landing_path="cpsc/2024-06-01/abc.json.gz",
    )
    assert qr.source_recall_id == "CPSC-001"
    assert qr.raw_record == {"id": "CPSC-001"}
    assert qr.failure_reason == "missing required field"
    assert qr.failure_stage == "validate"
    assert qr.raw_landing_path == "cpsc/2024-06-01/abc.json.gz"


def test_quarantine_record_is_frozen() -> None:
    qr = QuarantineRecord(
        source_recall_id=None,
        raw_record={},
        failure_reason="bad",
        failure_stage="validate",
        raw_landing_path="path",
    )
    with pytest.raises((AttributeError, TypeError)):
        qr.failure_reason = "changed"  # type: ignore[misc]


def test_quarantine_record_source_recall_id_can_be_none() -> None:
    qr = QuarantineRecord(
        source_recall_id=None,
        raw_record={},
        failure_reason="no id",
        failure_stage="validate",
        raw_landing_path="path",
    )
    assert qr.source_recall_id is None


# ---------------------------------------------------------------------------
# ExtractionResult — rejection_rate computation
# ---------------------------------------------------------------------------


def test_extraction_result_rejection_rate_zero_when_no_records_fetched() -> None:
    result = ExtractionResult(
        source="cpsc",
        run_id="run-1",
        records_fetched=0,
        records_landed=0,
        records_valid=0,
        records_rejected_validate=0,
        records_rejected_invariants=0,
        records_loaded=0,
        raw_landing_path="path",
    )
    assert result.rejection_rate == 0.0


def test_extraction_result_rejection_rate_computed_correctly() -> None:
    result = ExtractionResult(
        source="cpsc",
        run_id="run-1",
        records_fetched=100,
        records_landed=100,
        records_valid=80,
        records_rejected_validate=10,
        records_rejected_invariants=10,
        records_loaded=80,
        raw_landing_path="path",
    )
    assert result.rejection_rate == pytest.approx(0.20)


def test_extraction_result_rejection_rate_uses_both_reject_counts() -> None:
    result = ExtractionResult(
        source="fda",
        run_id="run-2",
        records_fetched=50,
        records_landed=50,
        records_valid=40,
        records_rejected_validate=5,
        records_rejected_invariants=5,
        records_loaded=40,
        raw_landing_path="path",
    )
    assert result.rejection_rate == pytest.approx(0.20)


# ---------------------------------------------------------------------------
# Fake extractor for run() tests
# ---------------------------------------------------------------------------


class _FakeSchema(BaseModel):
    id: str


class FakeExtractor(Extractor[_FakeSchema]):
    source_name: str = "fake"
    rejection_threshold: float = 0.05

    # Configurable return values / side effects
    extract_result: list[dict[str, Any]] = []
    land_raw_result: str = "fake/2024-06-01/abc.json.gz"
    validate_result: tuple[list[_FakeSchema], list[QuarantineRecord]] = ([], [])
    check_invariants_result: tuple[list[_FakeSchema], list[QuarantineRecord]] = ([], [])
    load_bronze_result: int = 0

    extract_side_effect: Exception | None = None
    land_raw_side_effect: Exception | None = None
    load_bronze_side_effect: Exception | None = None

    def extract(self) -> list[dict[str, Any]]:
        if self.extract_side_effect:
            raise self.extract_side_effect
        return self.extract_result

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        if self.land_raw_side_effect:
            raise self.land_raw_side_effect
        return self.land_raw_result

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
        return self.validate_result

    def check_invariants(
        self, records: list[_FakeSchema]
    ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
        return self.check_invariants_result

    def load_bronze(
        self,
        records: list[_FakeSchema],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        if self.load_bronze_side_effect:
            raise self.load_bronze_side_effect
        return self.load_bronze_result


def _make_quarantine(n: int, stage: str = "validate") -> list[QuarantineRecord]:
    return [
        QuarantineRecord(
            source_recall_id=None,
            raw_record={"i": i},
            failure_reason="bad",
            failure_stage=stage,
            raw_landing_path="path",
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Extractor.run() — happy path
# ---------------------------------------------------------------------------


def test_run_returns_extraction_result_on_success() -> None:
    schemas = [_FakeSchema(id="1"), _FakeSchema(id="2")]
    extractor = FakeExtractor(
        extract_result=[{"id": "1"}, {"id": "2"}],
        land_raw_result="fake/2024-06-01/abc.json.gz",
        validate_result=(schemas, []),
        check_invariants_result=(schemas, []),
        load_bronze_result=2,
    )
    with (
        patch("src.extractors._base._TRANSIENT_RETRY", side_effect=_PASSTHROUGH),
        patch("src.extractors._base._R2_RETRY", side_effect=_PASSTHROUGH),
    ):
        result = extractor.run()

    assert isinstance(result, ExtractionResult)
    assert result.source == "fake"
    assert result.records_fetched == 2
    assert result.records_loaded == 2
    assert result.raw_landing_path == "fake/2024-06-01/abc.json.gz"


def test_run_result_run_id_is_a_uuid4_string() -> None:
    import uuid

    extractor = FakeExtractor(
        validate_result=([], []),
        check_invariants_result=([], []),
    )
    with (
        patch("src.extractors._base._TRANSIENT_RETRY", side_effect=_PASSTHROUGH),
        patch("src.extractors._base._R2_RETRY", side_effect=_PASSTHROUGH),
    ):
        result = extractor.run()

    uuid.UUID(result.run_id, version=4)  # raises ValueError if invalid


def test_run_calls_lifecycle_methods_in_order() -> None:
    call_log: list[str] = []

    class OrderExtractor(Extractor[_FakeSchema]):
        source_name: str = "order"

        def extract(self) -> list[dict[str, Any]]:
            call_log.append("extract")
            return []

        def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
            call_log.append("land_raw")
            return "path"

        def validate_records(
            self, raw_records: list[dict[str, Any]]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            call_log.append("validate_records")
            return [], []

        def check_invariants(
            self, records: list[_FakeSchema]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            call_log.append("check_invariants")
            return [], []

        def load_bronze(
            self,
            records: list[_FakeSchema],
            quarantined: list[QuarantineRecord],
            raw_landing_path: str,
        ) -> int:
            call_log.append("load_bronze")
            return 0

    extractor = OrderExtractor()
    with (
        patch("src.extractors._base._TRANSIENT_RETRY", side_effect=_PASSTHROUGH),
        patch("src.extractors._base._R2_RETRY", side_effect=_PASSTHROUGH),
    ):
        extractor.run()

    assert call_log == [
        "extract",
        "land_raw",
        "validate_records",
        "check_invariants",
        "load_bronze",
    ]


# ---------------------------------------------------------------------------
# Extractor.run() — rejection threshold
# ---------------------------------------------------------------------------


def test_run_raises_extraction_aborted_error_when_rejection_rate_exceeds_threshold() -> None:
    # 10 fetched, 6 rejected => 60% rejection rate > 5% threshold
    schemas: list[_FakeSchema] = []
    rejects = _make_quarantine(6)
    extractor = FakeExtractor(
        rejection_threshold=0.05,
        extract_result=[{"id": str(i)} for i in range(10)],
        validate_result=(schemas, rejects),
        check_invariants_result=(schemas, []),
        load_bronze_result=0,
    )
    with (
        patch("src.extractors._base._TRANSIENT_RETRY", side_effect=_PASSTHROUGH),
        patch("src.extractors._base._R2_RETRY", side_effect=_PASSTHROUGH),
        pytest.raises(ExtractionAbortedError) as exc_info,
    ):
        extractor.run()

    assert exc_info.value.source == "fake"
    assert exc_info.value.rate == pytest.approx(0.60)
    assert exc_info.value.threshold == pytest.approx(0.05)


def test_run_does_not_raise_when_rejection_rate_equals_threshold() -> None:
    # 100 fetched, exactly 5 rejected => 5% == threshold, should NOT abort
    schemas = [_FakeSchema(id=str(i)) for i in range(95)]
    rejects = _make_quarantine(5)
    extractor = FakeExtractor(
        rejection_threshold=0.05,
        extract_result=[{"id": str(i)} for i in range(100)],
        validate_result=(schemas, rejects),
        check_invariants_result=(schemas, []),
        load_bronze_result=95,
    )
    with (
        patch("src.extractors._base._TRANSIENT_RETRY", side_effect=_PASSTHROUGH),
        patch("src.extractors._base._R2_RETRY", side_effect=_PASSTHROUGH),
    ):
        result = extractor.run()

    assert result.rejection_rate == pytest.approx(0.05)


def test_run_combines_schema_and_invariant_rejects_for_rejection_rate() -> None:
    # 10 fetched, 3 schema rejects + 3 invariant rejects => 60% > 5% threshold
    schemas: list[_FakeSchema] = [_FakeSchema(id=str(i)) for i in range(7)]
    passing: list[_FakeSchema] = [_FakeSchema(id=str(i)) for i in range(4)]
    extractor = FakeExtractor(
        rejection_threshold=0.05,
        extract_result=[{"id": str(i)} for i in range(10)],
        validate_result=(schemas, _make_quarantine(3, "validate")),
        check_invariants_result=(passing, _make_quarantine(3, "invariants")),
        load_bronze_result=4,
    )
    with (
        patch("src.extractors._base._TRANSIENT_RETRY", side_effect=_PASSTHROUGH),
        patch("src.extractors._base._R2_RETRY", side_effect=_PASSTHROUGH),
        pytest.raises(ExtractionAbortedError) as exc_info,
    ):
        extractor.run()

    assert exc_info.value.rate == pytest.approx(0.60)


# ---------------------------------------------------------------------------
# Extractor.run() — retry delegation
# ---------------------------------------------------------------------------


def test_run_delegates_extract_to_transient_retry() -> None:
    extractor = FakeExtractor(
        validate_result=([], []),
        check_invariants_result=([], []),
    )
    transient_mock = MagicMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
    r2_mock = MagicMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))

    with (
        patch("src.extractors._base._TRANSIENT_RETRY", transient_mock),
        patch("src.extractors._base._R2_RETRY", r2_mock),
    ):
        extractor.run()

    # _TRANSIENT_RETRY is called for extract and load_bronze
    assert transient_mock.call_count == 2
    first_fn = transient_mock.call_args_list[0][0][0]
    assert first_fn.__func__.__name__ == "extract"


def test_run_delegates_land_raw_to_r2_retry() -> None:
    extractor = FakeExtractor(
        validate_result=([], []),
        check_invariants_result=([], []),
    )
    transient_mock = MagicMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
    r2_mock = MagicMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))

    with (
        patch("src.extractors._base._TRANSIENT_RETRY", transient_mock),
        patch("src.extractors._base._R2_RETRY", r2_mock),
    ):
        extractor.run()

    r2_mock.assert_called_once()
    land_fn = r2_mock.call_args[0][0]
    assert land_fn.__func__.__name__ == "land_raw"


# ---------------------------------------------------------------------------
# Operation-type subclasses — field validation
# ---------------------------------------------------------------------------


def test_rest_api_extractor_defaults() -> None:
    from src.extractors._base import RestApiExtractor

    class ConcreteRest(RestApiExtractor[_FakeSchema]):
        source_name: str = "rest"
        base_url: str = "https://api.example.com"

        def extract(self) -> list[dict[str, Any]]:
            return []

        def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
            return "path"

        def validate_records(
            self, raw_records: list[dict[str, Any]]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            return [], []

        def check_invariants(
            self, records: list[_FakeSchema]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            return [], []

        def load_bronze(
            self,
            records: list[_FakeSchema],
            quarantined: list[QuarantineRecord],
            raw_landing_path: str,
        ) -> int:
            return 0

    ext = ConcreteRest()
    assert ext.base_url == "https://api.example.com"
    assert ext.timeout_seconds == 30.0
    assert ext.rate_limit_rps is None


def test_flat_file_extractor_defaults() -> None:
    # FlatFileExtractor moved out of _base.py in Phase 5c (Step 2) — its
    # implementation grew too large for the operation-type stub list and
    # now lives in its own module alongside the helpers (download,
    # decompress, parse, capture). Tests of the helpers themselves are in
    # tests/extractors/test_flat_file.py; this remains as a sanity check
    # that the Extractor-ABC field defaults still apply.
    from src.extractors._flat_file import FlatFileExtractor

    class ConcreteFlatFile(FlatFileExtractor[_FakeSchema]):
        source_name: str = "flatfile"
        file_url: str = "https://example.com/data.zip"

        def extract(self) -> list[dict[str, Any]]:
            return []

        def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
            return "path"

        def validate_records(
            self, raw_records: list[dict[str, Any]]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            return [], []

        def check_invariants(
            self, records: list[_FakeSchema]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            return [], []

        def load_bronze(
            self,
            records: list[_FakeSchema],
            quarantined: list[QuarantineRecord],
            raw_landing_path: str,
        ) -> int:
            return 0

    ext = ConcreteFlatFile()
    assert ext.file_url == "https://example.com/data.zip"
    assert ext.timeout_seconds == 120.0


def test_html_scraping_extractor_defaults() -> None:
    # HtmlScrapingExtractor was promoted from a stub in `_base.py` to its own
    # module (`_html_scraping.py`) at Phase 5d Step 2 when USCG became its
    # first concrete user. The two abstract parse methods declared there
    # (_parse_listing_page, _parse_details_page) must be stubbed alongside
    # the original 5 lifecycle abstracts.
    from src.extractors._html_scraping import HtmlScrapingExtractor

    class ConcreteHtml(HtmlScrapingExtractor[_FakeSchema]):
        source_name: str = "html"
        start_url: str = "https://example.com/recalls"

        def extract(self) -> list[dict[str, Any]]:
            return []

        def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
            return "path"

        def validate_records(
            self, raw_records: list[dict[str, Any]]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            return [], []

        def check_invariants(
            self, records: list[_FakeSchema]
        ) -> tuple[list[_FakeSchema], list[QuarantineRecord]]:
            return [], []

        def load_bronze(
            self,
            records: list[_FakeSchema],
            quarantined: list[QuarantineRecord],
            raw_landing_path: str,
        ) -> int:
            return 0

        def _parse_listing_page(self, body: bytes, page_url: str) -> list[dict[str, Any]]:
            return []

        def _parse_details_page(self, body: bytes, page_url: str) -> dict[str, Any]:
            return {}

    ext = ConcreteHtml()
    assert ext.start_url == "https://example.com/recalls"
    assert ext.timeout_seconds == 30.0
    assert ext.scrape_delay_seconds == 1.0
