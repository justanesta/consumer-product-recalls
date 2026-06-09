from __future__ import annotations

from decimal import Decimal

from src.enrichment.quantity_crosswalk_writer import build_quantity_crosswalk_rows


def test_build_rows_maps_parse_output() -> None:
    rows = build_quantity_crosswalk_rows(["1,200 cases", "Unknown"])
    assert rows[0] == {
        "raw_quantity": "1,200 cases",
        "quantity_value": Decimal("1200"),
        "quantity_unit": "case",
        "quantity_category": "count",
        "quantity_basis": "per_product",
    }
    assert rows[1] == {
        "raw_quantity": "Unknown",
        "quantity_value": None,
        "quantity_unit": None,
        "quantity_category": None,
        "quantity_basis": "unknown",
    }


def test_build_rows_dedupes_on_raw_key() -> None:
    rows = build_quantity_crosswalk_rows(["5 units", "5 units", "5 units"])
    assert len(rows) == 1
    assert rows[0]["raw_quantity"] == "5 units"


def test_build_rows_skips_none() -> None:
    rows = build_quantity_crosswalk_rows([None, "10 bottles", None])  # type: ignore[list-item]
    assert [r["raw_quantity"] for r in rows] == ["10 bottles"]


def test_build_rows_empty() -> None:
    assert build_quantity_crosswalk_rows([]) == []
