"""
Ingest market data from Alpha Vantage (stocks/crypto) and metals.dev (precious metals) into MongoDB.

Features:
- Loads `.env` from the project directory (same folder as this file).
- Reads `MONGO_URI` or `MONGODB_ATLAS_URI`, `ALPHA_VANTAGE_API_KEY`,
  `METAL_API_KEY` (metals.dev for precious metals), and database name from
  `MONGO_DB_NAME` or `DB_NAME`.
- Handles Alpha Vantage rate-limit responses with retries.
- Applies temporal versioning to assets (deactivate old active version, insert new version).
- Bulk inserts daily time-series points with provenance (`dataSourceId`).
- CLI: `python ingestion_service.py [--symbol TSLA|AMZN|BTC|ETH|XAG|XPT] [--force] [-v]`
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
from quality import (
    TimeSeriesInsertStats,
    create_ingestion_run,
    ensure_quality_collections_and_indexes,
    finalize_ingestion_run,
    insert_valid_time_series_documents,
)
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
DATA_SOURCE_ID = "alpha_vantage_api_v1"
METAL_API_BASE_URL = "https://api.metals.dev/v1/latest"
METAL_TIMESERIES_API_BASE_URL = "https://api.metals.dev/v1/timeseries"
METAL_DATA_SOURCE_ID = "metals_dev_v1"
METAL_TIMESERIES_MAX_ATTEMPTS = 4
METAL_TIMESERIES_TIMEOUT_SECONDS = 45
METAL_TIMESERIES_BASE_SLEEP_SECONDS = 3
METAL_SYMBOL_MAP: dict[str, str] = {
    "XAG": "silver",
    "XPT": "platinum",
}
DEFAULT_DB_NAME = "acme_financial_dw"
MIN_HISTORY_DAYS = 100
# Free tier: max 5 calls/minute; stay under limit between TSLA and BTC requests.
SECONDS_BETWEEN_SYMBOL_REQUESTS = 13
# Prefer ALPHA_VANTAGE_API_KEY in .env; override in production. Rotate if this file is ever public.
_DEFAULT_ALPHA_VANTAGE_API_KEY = ""
_ENV_FILE = Path(__file__).resolve().parent / ".env"

logger = logging.getLogger(__name__)

# Supported CLI symbols -> (market_type, human label for logs)
SYMBOL_INGEST_SPECS: dict[str, tuple[str, str]] = {
    "TSLA": ("stock", "TSLA"),
    "AMZN": ("stock", "AMZN"),
    "BTC": ("crypto", "BTC"),
    "ETH": ("crypto", "ETH"),
    "XAG": ("metal", "XAG"),  # Silver / USD
    "XPT": ("metal", "XPT"),  # Platinum / USD
}

# Human-readable names for metal symbols (metals.dev returns codes only).
_METAL_NAMES: dict[str, str] = {
    "XAG": "Silver",
    "XPT": "Platinum",
    "XAU": "Gold",
    "XPD": "Palladium",
}

# Human-readable names for stock symbols not returned by the free endpoint.
_STOCK_NAMES: dict[str, str] = {
    "TSLA": "Tesla Inc",
    "AMZN": "Amazon.com Inc",
}


def _clean_connection_string(value: str | None) -> str | None:
    """Strip whitespace and a single stray trailing quote from .env typos."""
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith('"') and not s.startswith('"'):
        s = s[:-1].rstrip()
    return s or None


class IngestionError(RuntimeError):
    """Raised when ingestion cannot continue."""


@dataclass(frozen=True)
class IngestionStats:
    """Small summary object for console output."""

    asset_id: str
    asset_action: str
    inserted_points: int
    invalid_points: int = 0
    duplicate_points: int = 0
    validation_errors: tuple[str, ...] = ()
    data_source_id: str = DATA_SOURCE_ID


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def to_float(value: str | None) -> float | None:
    """Convert string numeric values to float safely."""
    if value is None:
        return None
    try:
        return float(Decimal(value))
    except Exception:
        return None


def _alpha_vantage_needs_wait(payload: dict[str, Any]) -> bool:
    """
    True when Alpha Vantage returned no usable series yet (throttle / guidance).

    Free tier often uses `Note`; call-frequency limits often use `Information`
    without `Meta Data`.
    """
    if "Note" in payload:
        return True
    if "Information" in payload and "Meta Data" not in payload:
        return True
    return False


def _alpha_vantage_is_premium_message(payload: dict[str, Any]) -> bool:
    """Detect responses that indicate the requested function requires premium access."""
    info = str(payload.get("Information", "")).lower()
    premium_markers = (
        "this is a premium endpoint",
        "premium endpoint. you may subscribe",
        "premium feature",
    )
    return any(marker in info for marker in premium_markers)


def _alpha_vantage_is_daily_quota_message(payload: dict[str, Any]) -> bool:
    """Detect free-tier daily quota exhaustion messages."""
    note = str(payload.get("Note", "")).lower()
    info = str(payload.get("Information", "")).lower()
    quota_markers = (
        "25 requests per day",
        "free api requests more sparingly",
    )
    return any(marker in note or marker in info for marker in quota_markers)


def fetch_alpha_vantage_json(
    session: requests.Session,
    params: dict[str, str],
    *,
    max_attempts: int = 8,
    base_sleep_seconds: int = 15,
) -> dict[str, Any]:
    """
    Fetch JSON from Alpha Vantage with simple retry/rate-limit handling.

    The free tier may return `Note` or `Information` (without `Meta Data`)
    when throttled or when call frequency is too high.
    """
    for attempt in range(1, max_attempts + 1):
        response = session.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if "Error Message" in payload:
            raise IngestionError(f"Alpha Vantage error: {payload['Error Message']}")

        if _alpha_vantage_is_daily_quota_message(payload):
            raise IngestionError(
                "Alpha Vantage free-tier daily quota reached (25 requests/day). "
                "Wait for quota reset or use a paid plan/API key with higher limits."
            )

        if _alpha_vantage_is_premium_message(payload):
            raise IngestionError(
                "Alpha Vantage premium endpoint returned by function "
                f"{params.get('function')!r}: {payload.get('Information')!r}"
            )

        if not _alpha_vantage_needs_wait(payload):
            return payload

        if attempt == max_attempts:
            raise IngestionError(
                "Alpha Vantage rate limit or call-frequency message persisted after retries. "
                f"Note={payload.get('Note')!r} "
                f"Information={payload.get('Information')!r}"
            )

        sleep_seconds = base_sleep_seconds * attempt
        logger.info(
            "Alpha Vantage throttle: waiting %ss before retry %s/%s",
            sleep_seconds,
            attempt + 1,
            max_attempts,
        )
        time.sleep(sleep_seconds)

    raise IngestionError("Unexpected retry termination while fetching Alpha Vantage data.")


def fetch_metals_dev_price(session: requests.Session, symbol: str, api_key: str) -> float:
    """
    Fetch latest spot price for one metal symbol from metals.dev.

    See https://metals.dev/api/latest/ — response includes ``metals.silver``, etc.
    """
    key = api_key.strip()
    if not key:
        raise IngestionError("Missing METAL_API_KEY for metal ingestion.")
    metal_key = METAL_SYMBOL_MAP.get(symbol)
    if not metal_key:
        raise IngestionError(f"No metals.dev mapping for symbol {symbol!r}.")

    response = session.get(
        METAL_API_BASE_URL,
        params={"api_key": key, "currency": "USD", "unit": "troy_oz"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    if str(payload.get("status", "")).lower() != "success":
        raise IngestionError(
            "metals.dev API returned non-success status: "
            f"status={payload.get('status')!r} message={payload.get('message')!r}"
        )

    metals = payload.get("metals")
    if not isinstance(metals, dict):
        raise IngestionError("metals.dev response missing or invalid 'metals' object.")

    raw = metals.get(metal_key)
    if raw is None:
        raise IngestionError(
            f"metals.dev response missing price for metal key {metal_key!r} (symbol {symbol})."
        )
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise IngestionError(f"metals.dev invalid price for {metal_key!r}: {raw!r}") from exc


def split_date_range(
    start_date: date, end_date: date, *, max_days: int = 30
) -> list[tuple[date, date]]:
    """Split an inclusive date range into windows of at most ``max_days`` days."""
    if max_days < 1:
        raise ValueError("max_days must be at least 1")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    chunks: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(cursor + timedelta(days=max_days - 1), end_date)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def parse_metals_dev_timeseries(symbol: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse a metals.dev timeseries payload into daily point documents."""
    metal_key = METAL_SYMBOL_MAP.get(symbol)
    if not metal_key:
        raise IngestionError(f"No metals.dev mapping for symbol {symbol!r}.")

    if str(payload.get("status", "")).lower() != "success":
        raise IngestionError(
            "metals.dev API returned non-success status: "
            f"status={payload.get('status')!r} message={payload.get('message')!r}"
        )

    rates = payload.get("rates")
    if not isinstance(rates, dict):
        raise IngestionError("metals.dev timeseries response missing or invalid 'rates' object.")

    points: list[dict[str, Any]] = []
    for date_str in sorted(rates.keys(), reverse=True):
        daily = rates[date_str]
        if not isinstance(daily, dict):
            raise IngestionError(f"metals.dev invalid daily row for {date_str!r}.")
        metals = daily.get("metals")
        if not isinstance(metals, dict):
            raise IngestionError(f"metals.dev row {date_str!r} missing 'metals' object.")

        raw = metals.get(metal_key)
        if raw is None:
            raise IngestionError(
                f"metals.dev row {date_str!r} missing price for metal key {metal_key!r}."
            )
        try:
            close = float(raw)
        except (TypeError, ValueError) as exc:
            raise IngestionError(
                f"metals.dev invalid price for {metal_key!r} on {date_str}: {raw!r}"
            ) from exc

        points.append(
            {
                "timestamp": f"{date_str}T00:00:00Z",
                "open": None,
                "high": None,
                "low": None,
                "close": close,
                "volume": None,
            }
        )
    return points


def fetch_metals_dev_timeseries(
    session: requests.Session,
    symbol: str,
    api_key: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Fetch metals.dev daily prices over an inclusive range, chunked to API limits."""
    key = api_key.strip()
    if not key:
        raise IngestionError("Missing METAL_API_KEY for metal ingestion.")
    if symbol not in METAL_SYMBOL_MAP:
        raise IngestionError(f"No metals.dev mapping for symbol {symbol!r}.")

    points: list[dict[str, Any]] = []
    for chunk_start, chunk_end in split_date_range(start_date, end_date, max_days=30):
        payload: dict[str, Any] | None = None
        last_error: Exception | None = None
        for attempt in range(1, METAL_TIMESERIES_MAX_ATTEMPTS + 1):
            try:
                response = session.get(
                    METAL_TIMESERIES_API_BASE_URL,
                    params={
                        "api_key": key,
                        "start_date": chunk_start.isoformat(),
                        "end_date": chunk_end.isoformat(),
                    },
                    timeout=METAL_TIMESERIES_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException as exc:
                last_error = exc
                if attempt == METAL_TIMESERIES_MAX_ATTEMPTS:
                    break
                sleep_seconds = METAL_TIMESERIES_BASE_SLEEP_SECONDS * attempt
                logger.warning(
                    "metals.dev timeseries retry %s/%s for %s %s..%s after error: %s "
                    "(sleeping %ss)",
                    attempt + 1,
                    METAL_TIMESERIES_MAX_ATTEMPTS,
                    symbol,
                    chunk_start.isoformat(),
                    chunk_end.isoformat(),
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        if payload is None:
            raise IngestionError(
                "metals.dev timeseries request failed for "
                f"{symbol} {chunk_start.isoformat()}..{chunk_end.isoformat()} "
                f"after {METAL_TIMESERIES_MAX_ATTEMPTS} attempts: {last_error}"
            ) from last_error

        points.extend(parse_metals_dev_timeseries(symbol, payload))

    return sorted(points, key=lambda point: point["timestamp"], reverse=True)


def parse_stock_daily_adjusted(
    payload: dict[str, Any], limit: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse TIME_SERIES_DAILY_ADJUSTED response for TSLA-like symbols."""
    meta = payload.get("Meta Data", {})
    series = payload.get("Time Series (Daily)", {})
    if not series:
        hints: list[str] = []
        for key in ("Information", "Note", "Error Message"):
            if key in payload:
                hints.append(f"{key}: {payload[key]}")
        extra = " ".join(hints) if hints else f"keys={list(payload.keys())!r}"
        raise IngestionError(f"Stock response missing 'Time Series (Daily)'. {extra}")

    ordered_dates = sorted(series.keys(), reverse=True)[:limit]
    points: list[dict[str, Any]] = []

    for date_str in ordered_dates:
        row = series[date_str]
        points.append(
            {
                "timestamp": f"{date_str}T00:00:00Z",
                "open": to_float(row.get("1. open")),
                "high": to_float(row.get("2. high")),
                "low": to_float(row.get("3. low")),
                "close": to_float(row.get("4. close")),
                "adjusted_close": to_float(row.get("5. adjusted close")),
                "volume": int(row["6. volume"]) if row.get("6. volume") else None,
                "dividend_amount": to_float(row.get("7. dividend amount")),
            }
        )

    latest_close = points[0].get("close") if points else None
    total_dividends = sum(p.get("dividend_amount") or 0.0 for p in points)
    dividend_yield = None
    if latest_close and latest_close > 0:
        dividend_yield = total_dividends / latest_close

    asset_metadata = {
        "assetId": meta.get("2. Symbol", "TSLA"),
        "symbol": meta.get("2. Symbol", "TSLA"),
        "name": "Tesla Inc",
        "instrumentClass": "Stock",
        "exchange": "NASDAQ",
        "currency": "USD",
        "attributes": {
            "dividend_yield": dividend_yield,
            "time_zone": meta.get("5. Time Zone"),
        },
    }
    return asset_metadata, points


def parse_stock_daily(
    payload: dict[str, Any], limit: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse TIME_SERIES_DAILY response for TSLA-like symbols."""
    meta = payload.get("Meta Data", {})
    series = payload.get("Time Series (Daily)", {})
    if not series:
        hints: list[str] = []
        for key in ("Information", "Note", "Error Message"):
            if key in payload:
                hints.append(f"{key}: {payload[key]}")
        extra = " ".join(hints) if hints else f"keys={list(payload.keys())!r}"
        raise IngestionError(f"Stock response missing 'Time Series (Daily)'. {extra}")

    ordered_dates = sorted(series.keys(), reverse=True)[:limit]
    points: list[dict[str, Any]] = []

    for date_str in ordered_dates:
        row = series[date_str]
        points.append(
            {
                "timestamp": f"{date_str}T00:00:00Z",
                "open": to_float(row.get("1. open")),
                "high": to_float(row.get("2. high")),
                "low": to_float(row.get("3. low")),
                "close": to_float(row.get("4. close")),
                "volume": int(row["5. volume"]) if row.get("5. volume") else None,
            }
        )

    sym = meta.get("2. Symbol", "TSLA")
    asset_metadata = {
        "assetId": sym,
        "symbol": sym,
        "name": _STOCK_NAMES.get(sym, sym),
        "instrumentClass": "Stock",
        "exchange": "NASDAQ",
        "currency": "USD",
        "attributes": {
            # Free endpoint does not provide adjusted/dividend fields.
            "dividend_yield": None,
            "time_zone": meta.get("5. Time Zone"),
        },
    }
    return asset_metadata, points


def parse_crypto_daily(
    payload: dict[str, Any], limit: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse DIGITAL_CURRENCY_DAILY response for BTC-like symbols."""
    meta = payload.get("Meta Data", {})
    series = payload.get("Time Series (Digital Currency Daily)", {})
    if not series:
        hints: list[str] = []
        for key in ("Information", "Note", "Error Message"):
            if key in payload:
                hints.append(f"{key}: {payload[key]}")
        extra = " ".join(hints) if hints else f"keys={list(payload.keys())!r}"
        raise IngestionError(
            f"Crypto response missing 'Time Series (Digital Currency Daily)'. {extra}"
        )

    ordered_dates = sorted(series.keys(), reverse=True)[:limit]
    points: list[dict[str, Any]] = []

    for date_str in ordered_dates:
        row = series[date_str]
        points.append(
            {
                "timestamp": f"{date_str}T00:00:00Z",
                "open": to_float(row.get("1a. open (USD)")),
                "high": to_float(row.get("2a. high (USD)")),
                "low": to_float(row.get("3a. low (USD)")),
                "close": to_float(row.get("4a. close (USD)")),
                "volume": to_float(row.get("5. volume")),
                "market_cap_usd": to_float(row.get("6. market cap (USD)")),
            }
        )

    latest_market_cap = points[0].get("market_cap_usd") if points else None
    asset_metadata = {
        "assetId": meta.get("2. Digital Currency Code", "BTC"),
        "symbol": meta.get("2. Digital Currency Code", "BTC"),
        "name": meta.get("3. Digital Currency Name", "Bitcoin"),
        "instrumentClass": "Crypto",
        "currency": meta.get("4. Market Code", "USD"),
        "attributes": {
            "market_cap": latest_market_cap,
            # DIGITAL_CURRENCY_DAILY does not provide max supply directly.
            "max_supply": None,
            "time_zone": meta.get("7. Time Zone"),
        },
    }
    return asset_metadata, points


def parse_fx_daily_for_crypto(
    payload: dict[str, Any], limit: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Parse FX_DAILY fallback response for BTC/USD."""
    meta = payload.get("Meta Data", {})
    series = payload.get("Time Series FX (Daily)", {})
    if not series:
        hints: list[str] = []
        for key in ("Information", "Note", "Error Message"):
            if key in payload:
                hints.append(f"{key}: {payload[key]}")
        extra = " ".join(hints) if hints else f"keys={list(payload.keys())!r}"
        raise IngestionError(f"Crypto fallback response missing 'Time Series FX (Daily)'. {extra}")

    ordered_dates = sorted(series.keys(), reverse=True)[:limit]
    points: list[dict[str, Any]] = []
    for date_str in ordered_dates:
        row = series[date_str]
        points.append(
            {
                "timestamp": f"{date_str}T00:00:00Z",
                "open": to_float(row.get("1. open")),
                "high": to_float(row.get("2. high")),
                "low": to_float(row.get("3. low")),
                "close": to_float(row.get("4. close")),
                "volume": None,
                "market_cap_usd": None,
            }
        )

    asset_metadata = {
        "assetId": meta.get("2. From Symbol", "BTC"),
        "symbol": meta.get("2. From Symbol", "BTC"),
        "name": "Bitcoin",
        "instrumentClass": "Crypto",
        "currency": meta.get("4. To Symbol", "USD"),
        "attributes": {
            "market_cap": None,
            "max_supply": None,
            "time_zone": meta.get("6. Time Zone"),
            "source_series": "FX_DAILY",
        },
    }
    return asset_metadata, points


def parse_metal_spot(
    symbol: str, price: float, *, as_of: datetime | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Build asset metadata and a single daily OHLCV-style point from a spot price.

    Uses UTC calendar date for the point timestamp. ``as_of`` is for tests only.
    """
    dt = as_of if as_of is not None else datetime.now(timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    ts = f"{date_str}T00:00:00Z"
    point: dict[str, Any] = {
        "timestamp": ts,
        "open": None,
        "high": None,
        "low": None,
        "close": float(price),
        "volume": None,
    }
    asset_metadata: dict[str, Any] = {
        "assetId": symbol,
        "symbol": symbol,
        "name": _METAL_NAMES.get(symbol, symbol),
        "instrumentClass": "Metal",
        "currency": "USD",
        "attributes": {
            "unit": "troy_ounce",
            "market": "Precious Metals",
        },
    }
    return asset_metadata, [point]


def ensure_collections(db: Database) -> None:
    """Create required collections if they do not already exist."""
    ensure_quality_collections_and_indexes(db)


def _ensure_canonical_data_source(
    data_sources: Collection,
    *,
    source_id: str,
    name: str,
    vendor_type: str,
    website: str,
) -> None:
    """
    Ensure a single canonical data source row for ``source_id``.

    Uses the oldest matching document as the survivor and deletes extras so
    legacy duplicate inserts do not accumulate.
    """
    timestamp = now_utc_iso()
    canonical = {
        "sourceId": source_id,
        "name": name,
        "vendorType": vendor_type,
        "website": website,
    }
    matches = list(data_sources.find({"sourceId": source_id}).sort("_id", 1))
    if not matches:
        data_sources.insert_one({**canonical, "created_at": timestamp})
        return

    keeper = matches[0]
    if len(matches) > 1:
        deleted = data_sources.delete_many(
            {"sourceId": source_id, "_id": {"$ne": keeper["_id"]}}
        )
        logger.info(
            "Removed %s duplicate data_sources rows for %s", deleted.deleted_count, source_id
        )

    data_sources.update_one(
        {"_id": keeper["_id"]},
        {"$set": {**canonical, "updated_at": timestamp}},
    )


def ensure_data_source(data_sources: Collection) -> None:
    """Ensure Alpha Vantage is registered as a data source."""
    _ensure_canonical_data_source(
        data_sources,
        source_id=DATA_SOURCE_ID,
        name="Alpha Vantage API",
        vendor_type="market_data",
        website="https://www.alphavantage.co/",
    )


def ensure_metals_dev_data_source(data_sources: Collection) -> None:
    """Ensure metals.dev is registered as a data source."""
    _ensure_canonical_data_source(
        data_sources,
        source_id=METAL_DATA_SOURCE_ID,
        name="Metals.dev API",
        vendor_type="market_data",
        website="https://metals.dev/",
    )


def _comparable_asset_snapshot(doc: dict[str, Any]) -> dict[str, Any]:
    """Build metadata snapshot used to detect semantic asset changes."""
    return {
        "symbol": doc.get("symbol"),
        "name": doc.get("name"),
        "instrumentClass": doc.get("instrumentClass"),
        "exchange": doc.get("exchange"),
        "currency": doc.get("currency"),
        "attributes": doc.get("attributes") or {},
    }


def upsert_versioned_asset(
    assets: Collection,
    asset_payload: dict[str, Any],
    *,
    data_source_id: str = DATA_SOURCE_ID,
    lifecycle_events: Collection | None = None,
) -> str:
    """
    Enforce temporal versioning for assets.

    Rules:
    - If asset does not exist: insert version 1 as active.
    - If active version exists and metadata changed:
      deactivate old active version(s), insert new version.
    - If unchanged: no write.
    """
    asset_id = str(asset_payload["assetId"])
    latest = assets.find_one({"assetId": asset_id}, sort=[("version", -1)])
    version_timestamp = now_utc_iso()

    if latest is None:
        assets.insert_one(
            {
                **asset_payload,
                "version": 1,
                "valid_from": version_timestamp,
                "is_active": True,
                "dataSourceId": data_source_id,
            }
        )
        return "inserted_v1"

    if _comparable_asset_snapshot(latest) == _comparable_asset_snapshot(asset_payload):
        return "unchanged"

    assets.update_many(
        {"assetId": asset_id, "is_active": True},
        {"$set": {"is_active": False, "valid_to": version_timestamp}},
    )
    previous_version = int(latest.get("version", 1))
    new_version = previous_version + 1
    assets.insert_one(
        {
            **asset_payload,
            "version": new_version,
            "valid_from": version_timestamp,
            "is_active": True,
            "dataSourceId": data_source_id,
        }
    )
    if lifecycle_events is not None:
        event_doc = {
            "eventId": f"{asset_id}:deactivated:{previous_version}->{new_version}",
            "assetId": asset_id,
            "eventType": "deactivated",
            "previousVersion": previous_version,
            "newVersion": new_version,
            "valid_to": version_timestamp,
            "newVersionValidFrom": version_timestamp,
            "recordedAt": now_utc_iso(),
            "dataSourceId": data_source_id,
        }
        lifecycle_events.update_one(
            {"eventId": event_doc["eventId"]},
            {"$setOnInsert": event_doc},
            upsert=True,
        )
    return "inserted_new_version"


def bulk_insert_time_series_points(
    time_series: Collection,
    *,
    asset_id: str,
    symbol: str,
    interval: str,
    series_type: str,
    points: list[dict[str, Any]],
    data_source_id: str = DATA_SOURCE_ID,
) -> TimeSeriesInsertStats:
    """Validate and insert missing points in bulk for one asset and interval."""
    if not points:
        return TimeSeriesInsertStats()

    timestamps = [p["timestamp"] for p in points if p.get("timestamp")]
    existing = time_series.find(
        {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            "timestamp": {"$in": timestamps},
        },
        projection={"timestamp": 1},
    )
    existing_timestamps = {row["timestamp"] for row in existing}

    docs = [
        {
            "assetId": asset_id,
            "symbol": symbol,
            "interval": interval,
            "seriesType": series_type,
            "timestamp": point["timestamp"],
            "point": point,
            "dataSourceId": data_source_id,
            "ingested_at": now_utc_iso(),
        }
        for point in points
        if point.get("timestamp") not in existing_timestamps
    ]
    if not docs:
        return TimeSeriesInsertStats(duplicates=len(existing_timestamps))

    insert_stats = insert_valid_time_series_documents(time_series, docs)
    return TimeSeriesInsertStats(
        inserted=insert_stats.inserted,
        invalid=insert_stats.invalid,
        duplicates=insert_stats.duplicates + len(existing_timestamps),
        invalid_reasons=insert_stats.invalid_reasons,
    )


def ingest_symbol_data(
    db: Database,
    session: requests.Session,
    *,
    api_key: str,
    min_days: int,
    symbol: str,
    market_type: str,
    time_series_force_refresh: bool = False,
    metal_api_key: str | None = None,
) -> IngestionStats:
    """Ingest one symbol according to market type parser."""
    assets = db["assets"]
    time_series = db["time_series"]
    lifecycle_events = db["asset_lifecycle_events"]
    data_source_id = DATA_SOURCE_ID

    if market_type == "stock":
        # Use free-tier stock endpoint directly to avoid burning a call on premium probing.
        payload = fetch_alpha_vantage_json(
            session,
            {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": "compact",
                "apikey": api_key,
            },
        )
        asset_metadata, points = parse_stock_daily(payload, min_days)
    elif market_type == "crypto":
        # Use dedicated crypto endpoint for BTC-like symbols.
        payload = fetch_alpha_vantage_json(
            session,
            {
                "function": "DIGITAL_CURRENCY_DAILY",
                "symbol": symbol,
                "market": "USD",
                "apikey": api_key,
            },
        )
        asset_metadata, points = parse_crypto_daily(payload, min_days)
    elif market_type == "metal":
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=min_days - 1)
        points = fetch_metals_dev_timeseries(
            session, symbol, metal_api_key or "", start_date, end_date
        )
        asset_metadata = {
            "assetId": symbol,
            "symbol": symbol,
            "name": _METAL_NAMES.get(symbol, symbol),
            "instrumentClass": "Metal",
            "currency": "USD",
            "attributes": {
                "unit": "troy_ounce",
                "market": "Precious Metals",
            },
        }
        data_source_id = METAL_DATA_SOURCE_ID
    else:
        raise IngestionError(f"Unsupported market_type '{market_type}'.")

    if time_series_force_refresh:
        deleted = time_series.delete_many({"symbol": symbol, "interval": "1D"})
        logger.info(
            "Force refresh: removed %s time_series rows for symbol=%s interval=1D",
            deleted.deleted_count,
            symbol,
        )

    asset_action = upsert_versioned_asset(
        assets,
        asset_metadata,
        data_source_id=data_source_id,
        lifecycle_events=lifecycle_events,
    )
    insert_stats = bulk_insert_time_series_points(
        time_series,
        asset_id=asset_metadata["assetId"],
        symbol=asset_metadata["symbol"],
        interval="1D",
        series_type="OHLCV",
        points=points,
        data_source_id=data_source_id,
    )
    return IngestionStats(
        asset_id=asset_metadata["assetId"],
        asset_action=asset_action,
        inserted_points=insert_stats.inserted,
        invalid_points=insert_stats.invalid,
        duplicate_points=insert_stats.duplicates,
        validation_errors=insert_stats.invalid_reasons,
        data_source_id=data_source_id,
    )


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest market data from Alpha Vantage into MongoDB.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        metavar="SYM",
        help=(
            "Ticker to ingest (repeat for multiple). Default: TSLA and BTC. "
            "Supported: TSLA, AMZN, BTC, ETH, XAG, XPT."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing daily (1D) time_series rows for each ingested symbol before insert.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args()


def _resolve_ingest_symbols(cli_symbols: list[str] | None) -> list[str]:
    if not cli_symbols:
        return ["TSLA", "BTC"]
    out: list[str] = []
    for raw in cli_symbols:
        key = raw.strip().upper()
        if key not in SYMBOL_INGEST_SPECS:
            supported = ", ".join(sorted(SYMBOL_INGEST_SPECS))
            raise IngestionError(f"Unsupported symbol {raw!r}. Supported: {supported}")
        if key not in out:
            out.append(key)
    return out


def main() -> None:
    """Run ingestion for configured symbols (default TSLA and BTC)."""
    args = _parse_cli_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    load_dotenv(_ENV_FILE)
    mongo_uri = _clean_connection_string(
        os.getenv("MONGO_URI") or os.getenv("MONGODB_ATLAS_URI")
    )
    api_key = (os.getenv("ALPHA_VANTAGE_API_KEY") or _DEFAULT_ALPHA_VANTAGE_API_KEY or "").strip()
    db_name = (
        os.getenv("MONGO_DB_NAME") or os.getenv("DB_NAME") or DEFAULT_DB_NAME
    ).strip()

    if not mongo_uri:
        raise IngestionError("Missing MONGO_URI (or MONGODB_ATLAS_URI) environment variable.")
    if not api_key:
        raise IngestionError(
            "Missing ALPHA_VANTAGE_API_KEY (set env or default in ingestion_service.py)."
        )

    metal_api_key = (os.getenv("METAL_API_KEY") or "").strip()
    symbols = _resolve_ingest_symbols(args.symbols)
    if any(SYMBOL_INGEST_SPECS[s][0] == "metal" for s in symbols) and not metal_api_key:
        raise IngestionError(
            "Missing METAL_API_KEY in environment (required when ingesting metal symbols)."
        )

    client = MongoClient(mongo_uri, appname="acme-financial-dw-ingestion-service")
    db = client[db_name]

    ensure_collections(db)
    ensure_data_source(db["data_sources"])
    if any(SYMBOL_INGEST_SPECS[s][0] == "metal" for s in symbols):
        ensure_metals_dev_data_source(db["data_sources"])

    run_doc = create_ingestion_run(symbols)
    stats_out: list[IngestionStats] = []
    symbol_errors: list[str] = []

    try:
        with requests.Session() as session:
            for index, sym in enumerate(symbols):
                market_type, _ = SYMBOL_INGEST_SPECS[sym]
                if index > 0:
                    logger.info(
                        "Waiting %ss before %s request (free-tier spacing)...",
                        SECONDS_BETWEEN_SYMBOL_REQUESTS,
                        sym,
                    )
                    time.sleep(SECONDS_BETWEEN_SYMBOL_REQUESTS)
                try:
                    stats = ingest_symbol_data(
                        db,
                        session,
                        api_key=api_key,
                        min_days=MIN_HISTORY_DAYS,
                        symbol=sym,
                        market_type=market_type,
                        time_series_force_refresh=bool(args.force),
                        metal_api_key=metal_api_key,
                    )
                except Exception as exc:
                    run_doc["symbolsFailed"].append(sym)
                    symbol_errors.append(f"{sym}: {exc}")
                    logger.exception("Ingestion failed for symbol=%s", sym)
                    continue

                stats_out.append(stats)
                run_doc["symbolsSucceeded"].append(sym)
                run_doc["recordsInserted"] += stats.inserted_points
                run_doc["invalidRowsSkipped"] += stats.invalid_points
                run_doc["duplicateRowsSkipped"] += stats.duplicate_points
                run_doc["validationErrors"].extend(stats.validation_errors)
                run_doc["dataSourcesTouched"].append(stats.data_source_id)
                for reason in stats.validation_errors:
                    logger.warning("Skipped invalid %s row: %s", sym, reason)

        for row in stats_out:
            logger.info(
                "%s: asset=%s, inserted_points=%s, invalid_points=%s, duplicate_points=%s",
                row.asset_id,
                row.asset_action,
                row.inserted_points,
                row.invalid_points,
                row.duplicate_points,
            )

        if symbol_errors and stats_out:
            status = "partial"
        elif symbol_errors:
            status = "failed"
        else:
            status = "success"

        error_summary = "; ".join(symbol_errors) if symbol_errors else None
        finalize_ingestion_run(
            db["ingestion_runs"],
            run_doc,
            status=status,
            error_summary=error_summary,
        )
        if symbol_errors:
            raise IngestionError(error_summary or "Ingestion failed.")
    except Exception as exc:
        if not run_doc.get("finishedAt"):
            for sym in symbols:
                if sym not in run_doc["symbolsSucceeded"] and sym not in run_doc["symbolsFailed"]:
                    run_doc["symbolsFailed"].append(sym)
            finalize_ingestion_run(
                db["ingestion_runs"],
                run_doc,
                status="failed" if not stats_out else "partial",
                error_summary=str(exc),
            )
        raise

    logger.info("Ingestion completed for database: %s", db_name)


if __name__ == "__main__":
    main()
