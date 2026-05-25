from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from analytics import flatten_time_series_rows, forecast_next_close, summarize_time_series
from bson import ObjectId
from pymongo.database import Database
from quality import DEFAULT_FRESHNESS_THRESHOLD_HOURS
from repositories import AssetRepository, DataSourceRepository, QualityRepository, TimeSeriesRepository


def serialize_mongo(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, list):
        return [serialize_mongo(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_mongo(item) for key, item in value.items()}
    return value


class DwhReadOnlyTools:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.assets = AssetRepository(db)
        self.data_sources = DataSourceRepository(db)
        self.time_series = TimeSeriesRepository(db)
        self.quality = QualityRepository(db)

    def registry(self) -> dict[str, Callable[..., dict[str, Any]]]:
        return {
            "list_assets": self.list_assets,
            "get_asset": self.get_asset,
            "list_data_sources": self.list_data_sources,
            "get_data_source": self.get_data_source,
            "query_time_series": self.query_time_series,
            "get_analytics_summary": self.get_analytics_summary,
            "get_analytics_forecast": self.get_analytics_forecast,
            "get_quality_freshness": self.get_quality_freshness,
            "get_quality_summary": self.get_quality_summary,
        }

    def tool_specs(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "list_assets",
                    "description": "List available active asset IDs.",
                    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_asset",
                    "description": "Get latest active metadata for one asset ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {"asset_id": {"type": "string"}},
                        "required": ["asset_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_data_sources",
                    "description": "List registered data source IDs.",
                    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_data_source",
                    "description": "Get details for one data source ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {"source_id": {"type": "string"}},
                        "required": ["source_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "query_time_series",
                    "description": "Query historical rows by asset ID, data source ID, and optional ISO date range.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "asset_id": {"type": "string"},
                            "data_source_id": {"type": "string"},
                            "start_date": {"type": ["string", "null"]},
                            "end_date": {"type": ["string", "null"]},
                        },
                        "required": ["asset_id", "data_source_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_analytics_summary",
                    "description": "Compute min/max/average metrics for selected time-series rows.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "asset_id": {"type": "string"},
                            "data_source_id": {"type": "string"},
                            "start_date": {"type": ["string", "null"]},
                            "end_date": {"type": ["string", "null"]},
                        },
                        "required": ["asset_id", "data_source_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_analytics_forecast",
                    "description": "Compute a simple deterministic next-close trend estimate.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "asset_id": {"type": "string"},
                            "data_source_id": {"type": "string"},
                            "start_date": {"type": ["string", "null"]},
                            "end_date": {"type": ["string", "null"]},
                            "window": {"type": "integer", "minimum": 2},
                        },
                        "required": ["asset_id", "data_source_id"],
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_quality_freshness",
                    "description": "Get freshness status by asset/data-source pair.",
                    "parameters": {
                        "type": "object",
                        "properties": {"threshold_hours": {"type": "integer", "minimum": 1}},
                        "additionalProperties": False,
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_quality_summary",
                    "description": "Get DWH quality summary, duplicate risk, and latest ingestion run.",
                    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
                },
            },
        ]

    def list_assets(self) -> dict[str, Any]:
        page = self.assets.list_active_asset_ids(limit=500, offset=0)
        asset_ids = page["assetIds"]
        return {
            "assetIds": asset_ids,
            "count": page["count"],
            "total": page["total"],
            "limit": page["limit"],
            "offset": page["offset"],
            "provenance": {"endpoint": "GET /assets", "collection": "assets", "filter": {"is_active": True}},
        }

    def get_asset(self, asset_id: str) -> dict[str, Any]:
        query = {"assetId": asset_id, "is_active": True}
        doc = self.assets.get_active_asset(asset_id)
        return {
            "asset": serialize_mongo(doc),
            "provenance": {"endpoint": f"GET /assets/{asset_id}", "collection": "assets", "filter": query},
        }

    def list_data_sources(self) -> dict[str, Any]:
        page = self.data_sources.list_source_ids(limit=500, offset=0)
        source_ids = page["dataSourceIds"]
        return {
            "dataSourceIds": source_ids,
            "count": page["count"],
            "total": page["total"],
            "limit": page["limit"],
            "offset": page["offset"],
            "provenance": {"endpoint": "GET /data-sources", "collection": "data_sources", "filter": {}},
        }

    def get_data_source(self, source_id: str) -> dict[str, Any]:
        query = {"sourceId": source_id}
        doc = self.data_sources.get_data_source(source_id)
        return {
            "dataSource": serialize_mongo(doc),
            "provenance": {
                "endpoint": f"GET /data-sources/{source_id}",
                "collection": "data_sources",
                "filter": query,
            },
        }

    def query_time_series(
        self,
        asset_id: str,
        data_source_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        start: date | None = None
        end: date | None = None
        query: dict[str, Any] = {"assetId": asset_id, "dataSourceId": data_source_id}
        if start_date:
            start = date.fromisoformat(start_date)
            query.setdefault("timestamp", {})["$gte"] = f"{start_date}T00:00:00Z"
        if end_date:
            end = date.fromisoformat(end_date)
            query.setdefault("timestamp", {})["$lte"] = f"{end_date}T23:59:59Z"
        points = self.time_series.query_points(
            asset_id=asset_id,
            data_source_id=data_source_id,
            start_date=start,
            end_date=end,
        )
        return {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            "count": len(points),
            "points": serialize_mongo(points),
            "provenance": {
                "endpoint": "GET /time-series",
                "collection": "time_series",
                "filter": query,
            },
        }

    def get_analytics_summary(
        self,
        asset_id: str,
        data_source_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        start: date | None = None
        end: date | None = None
        query: dict[str, Any] = {"assetId": asset_id, "dataSourceId": data_source_id}
        if start_date:
            start = date.fromisoformat(start_date)
            query.setdefault("timestamp", {})["$gte"] = f"{start_date}T00:00:00Z"
        if end_date:
            end = date.fromisoformat(end_date)
            query.setdefault("timestamp", {})["$lte"] = f"{end_date}T23:59:59Z"
        points = self.time_series.query_points(
            asset_id=asset_id,
            data_source_id=data_source_id,
            start_date=start,
            end_date=end,
        )
        summary = summarize_time_series(points)
        response: dict[str, Any] = {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            "count": len(points),
            "dateRange": {
                "start": points[0].get("timestamp") if points else None,
                "end": points[-1].get("timestamp") if points else None,
            },
            "provenance": {
                "endpoint": "GET /analytics/summary",
                "collection": "time_series",
                "filter": query,
            },
        }
        response.update(summary)
        return serialize_mongo(response)

    def get_analytics_forecast(
        self,
        asset_id: str,
        data_source_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        window: int = 10,
    ) -> dict[str, Any]:
        start: date | None = None
        end: date | None = None
        query: dict[str, Any] = {"assetId": asset_id, "dataSourceId": data_source_id}
        if start_date:
            start = date.fromisoformat(start_date)
            query.setdefault("timestamp", {})["$gte"] = f"{start_date}T00:00:00Z"
        if end_date:
            end = date.fromisoformat(end_date)
            query.setdefault("timestamp", {})["$lte"] = f"{end_date}T23:59:59Z"
        points = self.time_series.query_points(
            asset_id=asset_id,
            data_source_id=data_source_id,
            start_date=start,
            end_date=end,
        )
        response: dict[str, Any] = {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            "count": len(points),
            "provenance": {
                "endpoint": "GET /analytics/forecast",
                "collection": "time_series",
                "filter": query,
            },
        }
        try:
            response.update(forecast_next_close(points, window=window))
        except ValueError as exc:
            response["error"] = str(exc)
        return serialize_mongo(response)

    def get_analytics_spark_shape(
        self,
        asset_id: str,
        data_source_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        start: date | None = None
        end: date | None = None
        query: dict[str, Any] = {"assetId": asset_id, "dataSourceId": data_source_id}
        if start_date:
            start = date.fromisoformat(start_date)
            query.setdefault("timestamp", {})["$gte"] = f"{start_date}T00:00:00Z"
        if end_date:
            end = date.fromisoformat(end_date)
            query.setdefault("timestamp", {})["$lte"] = f"{end_date}T23:59:59Z"
        points = self.time_series.query_points(
            asset_id=asset_id,
            data_source_id=data_source_id,
            start_date=start,
            end_date=end,
        )
        rows = flatten_time_series_rows(points)
        return serialize_mongo(
            {
                "assetId": asset_id,
                "dataSourceId": data_source_id,
                "count": len(rows),
                "rows": rows,
                "provenance": {
                    "endpoint": "GET /analytics/spark-shape",
                    "collection": "time_series",
                    "filter": query,
                },
            }
        )

    def get_quality_freshness(
        self,
        threshold_hours: int = DEFAULT_FRESHNESS_THRESHOLD_HOURS,
    ) -> dict[str, Any]:
        rows = self.quality.compute_freshness_rows(threshold_hours=threshold_hours)
        return {
            "thresholdHours": threshold_hours,
            "count": len(rows),
            "items": serialize_mongo(rows),
            "provenance": {
                "endpoint": "GET /quality/freshness",
                "collection": "time_series",
                "filter": {"thresholdHours": threshold_hours},
            },
        }

    def get_quality_summary(self) -> dict[str, Any]:
        latest_run = self.quality.latest_ingestion()
        return {
            "totalTimeSeriesRows": self.quality.total_time_series_rows(),
            "duplicateRisk": self.quality.duplicate_risk(),
            "invalidRowsSkippedLatestRun": int(latest_run.get("invalidRowsSkipped", 0)) if latest_run else 0,
            "latestIngestionRun": serialize_mongo(latest_run),
            "provenance": {
                "endpoint": "GET /quality/summary",
                "collections": ["time_series", "ingestion_runs"],
                "filter": {},
            },
        }
