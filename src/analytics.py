from __future__ import annotations

from datetime import date
from typing import Any


def build_time_series_query(
    asset_id: str,
    data_source_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    query: dict[str, Any] = {"assetId": asset_id, "dataSourceId": data_source_id}
    timestamp_filter: dict[str, str] = {}
    if start_date is not None:
        timestamp_filter["$gte"] = f"{start_date.isoformat()}T00:00:00Z"
    if end_date is not None:
        timestamp_filter["$lte"] = f"{end_date.isoformat()}T23:59:59Z"
    if timestamp_filter:
        query["timestamp"] = timestamp_filter
    return query


def numeric_from_row(row: dict[str, Any], field: str) -> float | None:
    point = row.get("point")
    if isinstance(point, dict) and field in point:
        value = point.get(field)
    else:
        value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def summarize_time_series(points: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for field in ("open", "high", "low", "close", "volume"):
        values = [value for point in points if (value := numeric_from_row(point, field)) is not None]
        if not values:
            continue
        summary[field] = {
            "min": min(values),
            "max": max(values),
            "average": sum(values) / len(values),
        }
    return summary


def forecast_next_close(points: list[dict[str, Any]], window: int = 10) -> dict[str, Any]:
    close_points = [
        {"timestamp": str(point.get("timestamp")), "close": close}
        for point in points
        if (close := numeric_from_row(point, "close")) is not None
    ]
    if len(close_points) < 2:
        raise ValueError("At least 2 valid close values are required to compute a trend forecast.")

    sample = close_points[-window:] if window > 0 else close_points
    closes = [item["close"] for item in sample]
    average_daily_change = (closes[-1] - closes[0]) / (len(closes) - 1)
    next_close = closes[-1] + average_daily_change
    direction = "flat"
    if average_daily_change > 0:
        direction = "up"
    elif average_daily_change < 0:
        direction = "down"

    return {
        "basis": f"last_{len(sample)}_close_values",
        "latestTimestamp": sample[-1]["timestamp"],
        "latestClose": closes[-1],
        "averageDailyChange": average_daily_change,
        "forecast": {"nextPeriodClose": next_close, "direction": direction},
    }


def flatten_time_series_rows(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for point in points:
        rows.append(
            {
                "assetId": point.get("assetId"),
                "dataSourceId": point.get("dataSourceId"),
                "timestamp": point.get("timestamp"),
                "open": numeric_from_row(point, "open"),
                "high": numeric_from_row(point, "high"),
                "low": numeric_from_row(point, "low"),
                "close": numeric_from_row(point, "close"),
                "volume": numeric_from_row(point, "volume"),
            }
        )
    return rows
