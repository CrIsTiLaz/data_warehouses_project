from __future__ import annotations

from datetime import date

import pytest

from spark_common import infer_direction, parse_iso_date


def test_parse_iso_date_accepts_valid_values() -> None:
    assert parse_iso_date("2026-05-25", "startDate") == date(2026, 5, 25)
    assert parse_iso_date(None, "startDate") is None


def test_parse_iso_date_rejects_invalid_values() -> None:
    with pytest.raises(ValueError) as exc:
        parse_iso_date("2026/05/25", "startDate")
    assert "startDate must be an ISO date in YYYY-MM-DD format." in str(exc.value)


def test_infer_direction_returns_up_down_flat() -> None:
    assert infer_direction(100.0, 101.0) == "up"
    assert infer_direction(100.0, 99.0) == "down"
    assert infer_direction(100.0, 100.0) == "flat"
