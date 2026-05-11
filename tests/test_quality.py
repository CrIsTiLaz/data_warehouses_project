from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from pymongo.errors import BulkWriteError

from quality import (
    compute_freshness,
    create_ingestion_run,
    finalize_ingestion_run,
    insert_valid_time_series_documents,
    validate_time_series_document,
)


class InsertResult:
    def __init__(self, count: int) -> None:
        self.inserted_ids = list(range(count))


class CaptureCollection:
    def __init__(self, *, duplicate_error: bool = False) -> None:
        self.docs: list[dict[str, Any]] = []
        self.duplicate_error = duplicate_error

    def insert_one(self, doc: dict[str, Any]) -> None:
        self.docs.append(deepcopy(doc))

    def insert_many(self, docs: list[dict[str, Any]], ordered: bool) -> InsertResult:
        if self.duplicate_error:
            raise BulkWriteError(
                {
                    "nInserted": 1,
                    "writeErrors": [
                        {"index": 1, "code": 11000, "errmsg": "duplicate key error"},
                    ],
                }
            )
        self.docs.extend(deepcopy(docs))
        return InsertResult(len(docs))


class FreshnessCollection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self.rows


def valid_time_series_doc() -> dict[str, Any]:
    return {
        "assetId": "TSLA",
        "dataSourceId": "alpha_vantage_api_v1",
        "timestamp": "2026-05-02T00:00:00Z",
        "point": {"open": 100.0, "high": 110.0, "low": 95.0, "close": 105.5},
    }


def test_validation_rejects_malformed_records() -> None:
    missing_asset = valid_time_series_doc()
    missing_asset["assetId"] = ""
    bad_timestamp = valid_time_series_doc()
    bad_timestamp["timestamp"] = "2026-05-02"
    bad_price = valid_time_series_doc()
    bad_price["point"]["close"] = "105.5"

    assert validate_time_series_document(missing_asset).valid is False
    assert validate_time_series_document(bad_timestamp).valid is False
    result = validate_time_series_document(bad_price)
    assert result.valid is False
    assert result.reason == "close must be numeric when present"


def test_invalid_records_are_skipped_before_insert() -> None:
    collection = CaptureCollection()
    invalid = valid_time_series_doc()
    invalid["point"]["open"] = "not-numeric"

    stats = insert_valid_time_series_documents(collection, [valid_time_series_doc(), invalid])

    assert stats.inserted == 1
    assert stats.invalid == 1
    assert stats.duplicates == 0
    assert collection.docs == [valid_time_series_doc()]


def test_duplicate_key_errors_do_not_crash_insert() -> None:
    collection = CaptureCollection(duplicate_error=True)

    stats = insert_valid_time_series_documents(
        collection,
        [valid_time_series_doc(), {**valid_time_series_doc(), "timestamp": "2026-05-03T00:00:00Z"}],
    )

    assert stats.inserted == 1
    assert stats.duplicates == 1
    assert stats.invalid == 0


def test_ingestion_run_audit_document_written_for_success() -> None:
    collection = CaptureCollection()
    run_doc = create_ingestion_run(["TSLA"])
    run_doc["symbolsSucceeded"].append("TSLA")
    run_doc["recordsInserted"] = 10
    run_doc["dataSourcesTouched"].append("alpha_vantage_api_v1")

    finalize_ingestion_run(collection, run_doc, status="success")

    assert len(collection.docs) == 1
    saved = collection.docs[0]
    assert saved["status"] == "success"
    assert saved["symbolsSucceeded"] == ["TSLA"]
    assert saved["symbolsFailed"] == []
    assert saved["recordsInserted"] == 10
    assert saved["finishedAt"] is not None


def test_ingestion_run_audit_document_written_for_failure() -> None:
    collection = CaptureCollection()
    run_doc = create_ingestion_run(["TSLA"])
    run_doc["symbolsFailed"].append("TSLA")

    finalize_ingestion_run(collection, run_doc, status="failed", error_summary="API failed")

    saved = collection.docs[0]
    assert saved["status"] == "failed"
    assert saved["symbolsFailed"] == ["TSLA"]
    assert saved["errorSummary"] == "API failed"


def test_freshness_logic_returns_expected_status_values() -> None:
    rows = [
        {
            "_id": {"assetId": "TSLA", "dataSourceId": "alpha_vantage_api_v1"},
            "latestTimestamp": "2026-05-02T00:00:00Z",
        },
        {
            "_id": {"assetId": "BTC", "dataSourceId": "alpha_vantage_api_v1"},
            "latestTimestamp": "2026-04-30T00:00:00Z",
        },
        {
            "_id": {"assetId": "ETH", "dataSourceId": "alpha_vantage_api_v1"},
            "latestTimestamp": None,
        },
    ]

    result = compute_freshness(
        FreshnessCollection(rows),
        threshold_hours=24,
        now=datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert [row["status"] for row in result] == ["fresh", "stale", "missing"]
    assert result[0]["lagHours"] == 12
