"""Unit tests for Alpha Vantage payload parsing (no network, no Mongo)."""

from __future__ import annotations

from datetime import date, datetime, timezone

from ingestion_service import (
    fetch_metals_dev_timeseries,
    parse_crypto_daily,
    parse_metal_spot,
    parse_metals_dev_timeseries,
    parse_stock_daily,
    split_date_range,
)


def test_parse_stock_daily_limit_and_ohlcv() -> None:
    payload = {
        "Meta Data": {
            "1. Information": "Daily Prices",
            "2. Symbol": "TSLA",
            "5. Time Zone": "US/Eastern",
        },
        "Time Series (Daily)": {
            "2026-05-01": {
                "1. open": "100.0",
                "2. high": "110.0",
                "3. low": "95.0",
                "4. close": "105.5",
                "5. volume": "1000",
            },
            "2026-05-02": {
                "1. open": "105.0",
                "2. high": "108.0",
                "3. low": "104.0",
                "4. close": "107.0",
                "5. volume": "2000",
            },
        },
    }
    meta, points = parse_stock_daily(payload, limit=1)
    assert meta["symbol"] == "TSLA"
    assert meta["instrumentClass"] == "Stock"
    assert len(points) == 1
    assert points[0]["timestamp"] == "2026-05-02T00:00:00Z"
    assert points[0]["close"] == 107.0
    assert points[0]["volume"] == 2000


def test_parse_crypto_daily_limit() -> None:
    payload = {
        "Meta Data": {
            "2. Digital Currency Code": "BTC",
            "3. Digital Currency Name": "Bitcoin",
            "4. Market Code": "USD",
            "7. Time Zone": "UTC",
        },
        "Time Series (Digital Currency Daily)": {
            "2026-05-01": {
                "1a. open (USD)": "50000",
                "2a. high (USD)": "51000",
                "3a. low (USD)": "49000",
                "4a. close (USD)": "50500",
                "5. volume": "123",
                "6. market cap (USD)": "999",
            },
            "2026-05-02": {
                "1a. open (USD)": "50500",
                "2a. high (USD)": "52000",
                "3a. low (USD)": "50000",
                "4a. close (USD)": "51500",
                "5. volume": "456",
                "6. market cap (USD)": "1000",
            },
        },
    }
    meta, points = parse_crypto_daily(payload, limit=1)
    assert meta["symbol"] == "BTC"
    assert meta["instrumentClass"] == "Crypto"
    assert len(points) == 1
    assert points[0]["timestamp"] == "2026-05-02T00:00:00Z"
    assert points[0]["close"] == 51500.0


def test_parse_stock_daily_amzn_name() -> None:
    payload = {
        "Meta Data": {"2. Symbol": "AMZN", "5. Time Zone": "US/Eastern"},
        "Time Series (Daily)": {
            "2026-05-01": {
                "1. open": "180.0",
                "2. high": "185.0",
                "3. low": "179.0",
                "4. close": "183.0",
                "5. volume": "5000",
            },
        },
    }
    meta, points = parse_stock_daily(payload, limit=5)
    assert meta["symbol"] == "AMZN"
    assert meta["name"] == "Amazon.com Inc"
    assert meta["instrumentClass"] == "Stock"
    assert len(points) == 1


def test_parse_metal_spot_xag() -> None:
    fixed = datetime(2026, 5, 2, 12, 0, 0, tzinfo=timezone.utc)
    meta, points = parse_metal_spot("XAG", 32.9, as_of=fixed)
    assert meta["symbol"] == "XAG"
    assert meta["name"] == "Silver"
    assert meta["instrumentClass"] == "Metal"
    assert meta["currency"] == "USD"
    assert meta["attributes"]["unit"] == "troy_ounce"
    assert len(points) == 1
    assert points[0]["timestamp"] == "2026-05-02T00:00:00Z"
    assert points[0]["close"] == 32.9
    assert points[0]["open"] is None
    assert points[0]["volume"] is None


def test_parse_metal_spot_xpt() -> None:
    fixed = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    meta, points = parse_metal_spot("XPT", 998.0, as_of=fixed)
    assert meta["symbol"] == "XPT"
    assert meta["name"] == "Platinum"
    assert meta["instrumentClass"] == "Metal"
    assert len(points) == 1
    assert points[0]["close"] == 998.0
    assert points[0]["timestamp"] == "2026-05-01T00:00:00Z"


def test_parse_metals_dev_timeseries_xag_points() -> None:
    payload = {
        "status": "success",
        "rates": {
            "2026-05-01": {"metals": {"silver": 32.1, "platinum": 990.0}},
            "2026-05-02": {"metals": {"silver": "32.9", "platinum": 998.0}},
        },
    }

    points = parse_metals_dev_timeseries("XAG", payload)

    assert len(points) == 2
    assert points[0]["timestamp"] == "2026-05-02T00:00:00Z"
    assert points[0]["close"] == 32.9
    assert points[0]["open"] is None
    assert points[0]["high"] is None
    assert points[0]["low"] is None
    assert points[0]["volume"] is None
    assert points[1]["timestamp"] == "2026-05-01T00:00:00Z"
    assert points[1]["close"] == 32.1


def test_split_date_range_100_days_uses_four_chunks() -> None:
    chunks = split_date_range(date(2026, 1, 1), date(2026, 4, 10), max_days=30)

    assert chunks == [
        (date(2026, 1, 1), date(2026, 1, 30)),
        (date(2026, 1, 31), date(2026, 3, 1)),
        (date(2026, 3, 2), date(2026, 3, 31)),
        (date(2026, 4, 1), date(2026, 4, 10)),
    ]


def test_fetch_metals_dev_timeseries_chunks_100_days() -> None:
    class FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class FakeSession:
        def __init__(self) -> None:
            self.params: list[dict] = []

        def get(self, url: str, *, params: dict, timeout: int) -> FakeResponse:
            self.params.append(params)
            start = params["start_date"]
            return FakeResponse(
                {
                    "status": "success",
                    "rates": {
                        start: {"metals": {"silver": 31.0, "platinum": 990.0}},
                    },
                }
            )

    session = FakeSession()

    points = fetch_metals_dev_timeseries(
        session, "XAG", "test-key", date(2026, 1, 1), date(2026, 4, 10)
    )

    assert len(session.params) == 4
    assert [(p["start_date"], p["end_date"]) for p in session.params] == [
        ("2026-01-01", "2026-01-30"),
        ("2026-01-31", "2026-03-01"),
        ("2026-03-02", "2026-03-31"),
        ("2026-04-01", "2026-04-10"),
    ]
    assert len(points) == 4
    assert points[0]["timestamp"] == "2026-04-01T00:00:00Z"
