from __future__ import annotations

from datetime import date


def parse_iso_date(raw: str | None, name: str) -> date | None:
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date in YYYY-MM-DD format.") from exc


def infer_direction(current_value: float, predicted_value: float, *, epsilon: float = 1e-9) -> str:
    delta = predicted_value - current_value
    if delta > epsilon:
        return "up"
    if delta < -epsilon:
        return "down"
    return "flat"
