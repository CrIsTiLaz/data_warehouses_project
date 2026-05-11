"""
Initialize the Acme Ltd Financial Data Warehouse in MongoDB Atlas.

Rules implemented:
- Versioned insertion only (no updates/deletes).
- Flexible heterogeneous asset attributes by instrument class.
- Provenance on assets and time_series via dataSourceId.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

_ENV_FILE = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_FILE)

MONGODB_ATLAS_URI = (os.getenv("MONGO_URI") or os.getenv("MONGODB_ATLAS_URI") or "").strip()
DB_NAME = (os.getenv("MONGO_DB_NAME") or os.getenv("DB_NAME") or "acme_financial_dw").strip()


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def next_version(collection: Collection, business_key: str) -> int:
    """Compute next append-only version for an entity."""
    latest = collection.find_one(
        {"entityKey": business_key},
        sort=[("version", -1)],
        projection={"version": 1},
    )
    return 1 if latest is None else int(latest["version"]) + 1


def insert_versioned(
    collection: Collection,
    business_key: str,
    payload: dict[str, Any],
    *,
    valid_from: str | None = None,
    is_active: bool = True,
) -> Any:
    """
    Insert a new immutable versioned document.

    Existing documents are never updated or deleted.
    """
    version = next_version(collection, business_key)
    doc = {
        "entityKey": business_key,
        "version": version,
        "valid_from": valid_from or now_utc_iso(),
        "is_active": is_active,
        **payload,
    }
    result = collection.insert_one(doc)
    return result.inserted_id


def ensure_collections(db) -> None:
    """Create required collections if they do not exist."""
    required = {"assets", "data_sources", "time_series"}
    existing = set(db.list_collection_names())
    for name in sorted(required - existing):
        db.create_collection(name)


def seed_data(db) -> None:
    assets = db["assets"]
    data_sources = db["data_sources"]
    time_series = db["time_series"]

    # Keep source IDs stable and explicit for provenance.
    sources = [
        {
            "sourceId": "nasdaq_data_link",
            "name": "Nasdaq Data Link",
            "vendorType": "market_data",
            "website": "https://data.nasdaq.com/",
        },
        {
            "sourceId": "bloomberg_api",
            "name": "Bloomberg API",
            "vendorType": "market_data",
            "website": "https://www.bloomberg.com/professional/support/api-library/",
        },
        {
            "sourceId": "coinmarketcap_api",
            "name": "CoinMarketCap API",
            "vendorType": "crypto_data",
            "website": "https://coinmarketcap.com/api/",
        },
        {
            "sourceId": "ecb_reference",
            "name": "ECB Reference",
            "vendorType": "fixed_income_reference",
            "website": "https://www.ecb.europa.eu/",
        },
        {
            "sourceId": "lbma_feed",
            "name": "LBMA Feed",
            "vendorType": "metals_data",
            "website": "https://www.lbma.org.uk/",
        },
    ]

    for src in sources:
        insert_versioned(
            data_sources,
            f"source::{src['sourceId']}",
            src,
            valid_from=now_utc_iso(),
            is_active=True,
        )

    assets_seed = [
        {
            "entity_key": "asset::stock::TSLA",
            "payload": {
                "assetId": "TSLA",
                "symbol": "TSLA",
                "name": "Tesla Inc",
                "instrumentClass": "Stock",
                "exchange": "NASDAQ",
                "currency": "USD",
                "attributes": {
                    "currentPrice": 337.80,
                    "sector": "Consumer Cyclical",
                },
                "dataSourceId": "nasdaq_data_link",
            },
        },
        {
            "entity_key": "asset::stock::AMZN",
            "payload": {
                "assetId": "AMZN",
                "symbol": "AMZN",
                "name": "Amazon.com Inc",
                "instrumentClass": "Stock",
                "exchange": "NASDAQ",
                "currency": "USD",
                "attributes": {
                    "currentPrice": 183.92,
                    "sector": "Consumer Discretionary",
                },
                "dataSourceId": "nasdaq_data_link",
            },
        },
        {
            "entity_key": "asset::crypto::BTC",
            "payload": {
                "assetId": "BTC",
                "symbol": "BTC",
                "name": "Bitcoin",
                "instrumentClass": "Crypto",
                "attributes": {
                    "marketCapUSD": 1300000000000,
                    "circulatingSupply": 19700000,
                },
                "dataSourceId": "coinmarketcap_api",
            },
        },
        {
            "entity_key": "asset::crypto::ETH",
            "payload": {
                "assetId": "ETH",
                "symbol": "ETH",
                "name": "Ethereum",
                "instrumentClass": "Crypto",
                "attributes": {
                    "marketCapUSD": 420000000000,
                    "circulatingSupply": 120000000,
                },
                "dataSourceId": "coinmarketcap_api",
            },
        },
        {
            "entity_key": "asset::bond::ROS2CQA3C829",
            "payload": {
                "assetId": "ROS2CQA3C829",
                "isin": "ROS2CQA3C829",
                "name": "Romanian Government Bond",
                "instrumentClass": "Bond",
                "currency": "RON",
                "attributes": {
                    "issuer": "Romanian Government",
                    "bondType": "Sovereign",
                },
                "dataSourceId": "ecb_reference",
            },
        },
        {
            "entity_key": "asset::metal::XAG",
            "payload": {
                "assetId": "XAG",
                "symbol": "XAG",
                "name": "Silver",
                "instrumentClass": "Metal",
                "attributes": {
                    "unit": "troy_ounce",
                    "market": "Precious Metals",
                },
                "dataSourceId": "lbma_feed",
            },
        },
        {
            "entity_key": "asset::metal::XPT",
            "payload": {
                "assetId": "XPT",
                "symbol": "XPT",
                "name": "Platinum",
                "instrumentClass": "Metal",
                "attributes": {
                    "unit": "troy_ounce",
                    "market": "Precious Metals",
                },
                "dataSourceId": "lbma_feed",
            },
        },
    ]

    for item in assets_seed:
        insert_versioned(
            assets,
            item["entity_key"],
            item["payload"],
            valid_from=now_utc_iso(),
            is_active=True,
        )

    tsla_ohlcv = [
        {
            "timestamp": "2026-04-24T00:00:00Z",
            "open": 331.10,
            "high": 339.25,
            "low": 328.40,
            "close": 337.80,
            "volume": 106_450_000,
        },
        {
            "timestamp": "2026-04-25T00:00:00Z",
            "open": 337.80,
            "high": 341.50,
            "low": 334.20,
            "close": 338.95,
            "volume": 92_870_000,
        },
    ]

    insert_versioned(
        time_series,
        "timeseries::TSLA::1D",
        {
            "assetId": "TSLA",
            "symbol": "TSLA",
            "seriesType": "OHLCV",
            "interval": "1D",
            "points": tsla_ohlcv,
            "dataSourceId": "bloomberg_api",
        },
        valid_from=now_utc_iso(),
        is_active=True,
    )


def main() -> None:
    if not MONGODB_ATLAS_URI:
        raise RuntimeError("Missing MONGO_URI or MONGODB_ATLAS_URI in environment.")
    client = MongoClient(MONGODB_ATLAS_URI, appname="acme-financial-dw-setup")
    db = client[DB_NAME]

    ensure_collections(db)
    seed_data(db)

    collections = sorted(db.list_collection_names())
    print(f"Database initialized: {DB_NAME}")
    print("Collections:", ", ".join(collections))


if __name__ == "__main__":
    main()
