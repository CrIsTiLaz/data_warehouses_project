from __future__ import annotations

from datetime import date
from typing import Any

from analytics import build_time_series_query
from pymongo.database import Database
from quality import (
    DEFAULT_FRESHNESS_THRESHOLD_HOURS,
    compute_freshness,
    detect_duplicate_risk,
    latest_ingestion_run,
)


class AssetRepository:
    def __init__(self, db: Database) -> None:
        self.collection = db["assets"]

    def list_active_asset_ids(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        docs = self.collection.find(
            {"is_active": True},
            projection={"_id": 0, "assetId": 1, "version": 1},
        ).sort([("assetId", 1), ("version", -1)])

        seen: set[str] = set()
        all_ids: list[str] = []
        for doc in docs:
            asset_id = doc.get("assetId")
            if asset_id is None:
                continue
            value = str(asset_id)
            if value in seen:
                continue
            seen.add(value)
            all_ids.append(value)

        total = len(all_ids)
        page = all_ids[offset : offset + limit]
        return {
            "assetIds": page,
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_active_asset(self, asset_id: str) -> dict[str, Any] | None:
        return self.collection.find_one(
            {"assetId": asset_id, "is_active": True},
            sort=[("version", -1)],
        )


class DataSourceRepository:
    def __init__(self, db: Database) -> None:
        self.collection = db["data_sources"]

    def list_source_ids(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        docs = self.collection.find(
            {},
            projection={"_id": 0, "sourceId": 1, "version": 1},
        ).sort([("sourceId", 1), ("version", -1)])

        seen: set[str] = set()
        all_ids: list[str] = []
        for doc in docs:
            source_id = doc.get("sourceId")
            if source_id is None:
                continue
            value = str(source_id)
            if value in seen:
                continue
            seen.add(value)
            all_ids.append(value)

        total = len(all_ids)
        page = all_ids[offset : offset + limit]
        return {
            "dataSourceIds": page,
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_data_source(self, source_id: str) -> dict[str, Any] | None:
        return self.collection.find_one(
            {"sourceId": source_id},
            sort=[("version", -1)],
        )


class TimeSeriesRepository:
    def __init__(self, db: Database) -> None:
        self.collection = db["time_series"]

    def query_points(
        self,
        *,
        asset_id: str,
        data_source_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        query = build_time_series_query(asset_id, data_source_id, start_date, end_date)
        return list(self.collection.find(query).sort("timestamp", 1))


class QualityRepository:
    def __init__(self, db: Database) -> None:
        self.time_series = db["time_series"]
        self.ingestion_runs = db["ingestion_runs"]

    def total_time_series_rows(self) -> int:
        return self.time_series.count_documents({})

    def compute_freshness_rows(
        self, *, threshold_hours: int = DEFAULT_FRESHNESS_THRESHOLD_HOURS
    ) -> list[dict[str, Any]]:
        return compute_freshness(self.time_series, threshold_hours=threshold_hours)

    def duplicate_risk(self) -> dict[str, Any]:
        return detect_duplicate_risk(self.time_series)

    def latest_ingestion(self) -> dict[str, Any] | None:
        return latest_ingestion_run(self.ingestion_runs)


class AnalyticsRepository(TimeSeriesRepository):
    """Alias for explicit API boundary naming in analytics endpoints."""
