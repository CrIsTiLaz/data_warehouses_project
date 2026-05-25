from __future__ import annotations

import pytest

from analytics import flatten_time_series_rows, forecast_next_close, summarize_time_series


def sample_points() -> list[dict[str, object]]:
    return [
        {
            "assetId": "TSLA",
            "dataSourceId": "alpha_vantage_api_v1",
            "timestamp": "2026-05-01T00:00:00Z",
            "point": {"open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000},
        },
        {
            "assetId": "TSLA",
            "dataSourceId": "alpha_vantage_api_v1",
            "timestamp": "2026-05-02T00:00:00Z",
            "point": {"open": 106.0, "high": 112.0, "low": 101.0, "close": 107.0, "volume": 1100},
        },
    ]


def test_summarize_time_series_returns_min_max_average() -> None:
    summary = summarize_time_series(sample_points())

    assert summary["close"]["min"] == 105.0
    assert summary["close"]["max"] == 107.0
    assert summary["close"]["average"] == pytest.approx(106.0)
    assert summary["volume"]["average"] == pytest.approx(1050.0)


def test_forecast_next_close_is_deterministic() -> None:
    forecast = forecast_next_close(sample_points(), window=10)

    assert forecast["basis"] == "last_2_close_values"
    assert forecast["latestTimestamp"] == "2026-05-02T00:00:00Z"
    assert forecast["latestClose"] == 107.0
    assert forecast["averageDailyChange"] == pytest.approx(2.0)
    assert forecast["forecast"]["nextPeriodClose"] == pytest.approx(109.0)
    assert forecast["forecast"]["direction"] == "up"


def test_forecast_next_close_requires_two_values() -> None:
    with pytest.raises(ValueError):
        forecast_next_close(sample_points()[:1], window=10)


def test_flatten_time_series_rows_handles_nested_shape() -> None:
    rows = flatten_time_series_rows(sample_points())

    assert len(rows) == 2
    assert rows[0]["assetId"] == "TSLA"
    assert rows[0]["timestamp"] == "2026-05-01T00:00:00Z"
    assert rows[0]["open"] == 100.0
    assert rows[0]["high"] == 110.0
    assert rows[0]["low"] == 95.0
    assert rows[0]["close"] == 105.0
    assert rows[0]["volume"] == 1000.0
