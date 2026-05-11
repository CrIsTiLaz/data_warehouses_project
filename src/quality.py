from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

PRICE_FIELDS = ("open", "high", "low", "close")
DEFAULT_FRESHNESS_THRESHOLD_HOURS = 24


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reason: str | None = None


@dataclass(frozen=True)
class TimeSeriesInsertStats:
    inserted: int = 0
    invalid: int = 0
    duplicates: int = 0
    invalid_reasons: tuple[str, ...] = field(default_factory=tuple)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_ingestion_run(symbols_requested: list[str]) -> dict[str, Any]:
    return {
        "runId": str(uuid4()),
        "startedAt": now_utc_iso(),
        "finishedAt": None,
        "status": "running",
        "symbolsRequested": symbols_requested,
        "symbolsSucceeded": [],
        "symbolsFailed": [],
        "recordsInserted": 0,
        "invalidRowsSkipped": 0,
        "duplicateRowsSkipped": 0,
        "validationErrors": [],
        "errorSummary": None,
        "dataSourcesTouched": [],
    }


def finalize_ingestion_run(
    ingestion_runs: Collection,
    run_doc: dict[str, Any],
    *,
    status: str,
    error_summary: str | None = None,
) -> None:
    run_doc.update(
        {
            "status": status,
            "finishedAt": now_utc_iso(),
            "symbolsSucceeded": sorted(set(run_doc.get("symbolsSucceeded", []))),
            "symbolsFailed": sorted(set(run_doc.get("symbolsFailed", []))),
            "dataSourcesTouched": sorted(set(run_doc.get("dataSourcesTouched", []))),
            "errorSummary": error_summary,
        }
    )
    ingestion_runs.insert_one(dict(run_doc))


def validate_time_series_document(doc: dict[str, Any]) -> ValidationResult:
    asset_id = doc.get("assetId")
    if not isinstance(asset_id, str) or not asset_id.strip():
        return ValidationResult(False, "assetId must be a non-empty string")

    data_source_id = doc.get("dataSourceId")
    if not isinstance(data_source_id, str) or not data_source_id.strip():
        return ValidationResult(False, "dataSourceId must be a non-empty string")

    timestamp = doc.get("timestamp")
    if not isinstance(timestamp, str) or not _is_valid_utc_timestamp(timestamp):
        return ValidationResult(False, "timestamp must be a valid ISO UTC string")

    point = doc.get("point") if isinstance(doc.get("point"), dict) else doc
    for field_name in PRICE_FIELDS:
        value = point.get(field_name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int | float)):
            return ValidationResult(False, f"{field_name} must be numeric when present")

    return ValidationResult(True)


def _is_valid_utc_timestamp(value: str) -> bool:
    if not (value.endswith("Z") or value.endswith("+00:00")):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed)


def insert_valid_time_series_documents(
    time_series: Collection,
    docs: list[dict[str, Any]],
) -> TimeSeriesInsertStats:
    valid_docs: list[dict[str, Any]] = []
    invalid_reasons: list[str] = []
    for doc in docs:
        result = validate_time_series_document(doc)
        if result.valid:
            valid_docs.append(doc)
        else:
            invalid_reasons.append(result.reason or "invalid time-series document")

    if not valid_docs:
        return TimeSeriesInsertStats(
            invalid=len(invalid_reasons),
            invalid_reasons=tuple(invalid_reasons),
        )

    try:
        result = time_series.insert_many(valid_docs, ordered=False)
        inserted = len(result.inserted_ids)
        duplicates = 0
    except BulkWriteError as exc:
        details = exc.details or {}
        write_errors = details.get("writeErrors", [])
        duplicate_errors = [err for err in write_errors if err.get("code") == 11000]
        non_duplicate_errors = [err for err in write_errors if err.get("code") != 11000]
        if non_duplicate_errors:
            raise
        inserted = int(details.get("nInserted", 0))
        duplicates = len(duplicate_errors)

    return TimeSeriesInsertStats(
        inserted=inserted,
        invalid=len(invalid_reasons),
        duplicates=duplicates,
        invalid_reasons=tuple(invalid_reasons),
    )


def ensure_quality_collections_and_indexes(db: Database) -> None:
    required = {"assets", "time_series", "data_sources", "ingestion_runs"}
    existing = set(db.list_collection_names())
    for name in sorted(required - existing):
        db.create_collection(name)

    db["time_series"].create_index(
        [
            ("assetId", ASCENDING),
            ("dataSourceId", ASCENDING),
            ("timestamp", ASCENDING),
        ],
        unique=True,
        name="uq_time_series_asset_source_timestamp",
    )
    db["ingestion_runs"].create_index([("startedAt", ASCENDING)], name="ix_ingestion_runs_started")


def compute_freshness(
    time_series: Collection,
    *,
    threshold_hours: int = DEFAULT_FRESHNESS_THRESHOLD_HOURS,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    current_time = now or datetime.now(timezone.utc)
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {
            "$group": {
                "_id": {"assetId": "$assetId", "dataSourceId": "$dataSourceId"},
                "latestTimestamp": {"$first": "$timestamp"},
            }
        },
        {"$sort": {"_id.assetId": 1, "_id.dataSourceId": 1}},
    ]
    rows: list[dict[str, Any]] = []
    for row in time_series.aggregate(pipeline):
        latest_timestamp = row.get("latestTimestamp")
        lag_hours: float | None = None
        status = "missing"
        if isinstance(latest_timestamp, str) and _is_valid_utc_timestamp(latest_timestamp):
            latest_dt = datetime.fromisoformat(latest_timestamp.replace("Z", "+00:00"))
            lag_hours = (current_time - latest_dt).total_seconds() / 3600
            status = "fresh" if lag_hours <= threshold_hours else "stale"

        key = row.get("_id") or {}
        rows.append(
            {
                "assetId": key.get("assetId"),
                "dataSourceId": key.get("dataSourceId"),
                "latestTimestamp": latest_timestamp,
                "lagHours": lag_hours,
                "status": status,
            }
        )
    return rows


def detect_duplicate_risk(time_series: Collection) -> dict[str, Any]:
    pipeline = [
        {
            "$group": {
                "_id": {
                    "assetId": "$assetId",
                    "dataSourceId": "$dataSourceId",
                    "timestamp": "$timestamp",
                },
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {"$limit": 1},
    ]
    has_duplicates = next(iter(time_series.aggregate(pipeline)), None) is not None
    return {"hasDuplicates": has_duplicates, "status": "risk" if has_duplicates else "ok"}


def latest_ingestion_run(ingestion_runs: Collection) -> dict[str, Any] | None:
    return ingestion_runs.find_one({}, sort=[("startedAt", -1)])
