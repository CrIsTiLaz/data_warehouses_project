from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

from analytics import (
    flatten_time_series_rows,
    forecast_next_close,
    summarize_time_series,
)
from assistant.mcp_adapter import LocalMCPAdapter, MCPConfig, MCPUnavailableError
from assistant.models import AssistantQueryRequest, AssistantQueryResponse
from assistant.service import AssistantService
from assistant.tools import DwhReadOnlyTools
from bson import ObjectId
from quality import (
    DEFAULT_FRESHNESS_THRESHOLD_HOURS,
)
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pymongo import MongoClient
from pymongo.database import Database
from repositories import (
    AnalyticsRepository,
    AssetRepository,
    DataSourceRepository,
    QualityRepository,
    TimeSeriesRepository,
)

DEFAULT_DB_NAME = "acme_financial_dw"
_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


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


def _mongo_uri_from_env() -> str:
    load_dotenv(_ENV_FILE)
    mongo_uri = _clean_connection_string(
        os.getenv("MONGO_URI") or os.getenv("MONGODB_ATLAS_URI")
    )
    if not mongo_uri:
        raise RuntimeError("Missing MONGO_URI (or MONGODB_ATLAS_URI) environment variable.")
    return mongo_uri


def _db_name_from_env() -> str:
    load_dotenv(_ENV_FILE)
    return (os.getenv("MONGO_DB_NAME") or os.getenv("DB_NAME") or DEFAULT_DB_NAME).strip()


def serialize_mongo(value: Any) -> Any:
    """Convert Mongo-specific values into JSON-safe values."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [serialize_mongo(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_mongo(item) for key, item in value.items()}
    return value


def _parse_iso_date(raw: str | None, name: str) -> date | None:
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be an ISO date in YYYY-MM-DD format.",
        ) from exc


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    client: MongoClient | None = None
    try:
        mongo_uri = _mongo_uri_from_env()
        db_name = _db_name_from_env()
        client = MongoClient(mongo_uri, appname="acme-financial-dw-api")
        app.state.mongo_client = client
        app.state.db = client[db_name]
        yield
    finally:
        if client is not None:
            client.close()


app = FastAPI(title="Acme Financial DWH API", lifespan=lifespan)


def get_db(request: Request) -> Database:
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection is not initialized.")
    return db


def get_assistant_service(db: Database = Depends(get_db)) -> AssistantService:
    tools = DwhReadOnlyTools(db)
    adapter = LocalMCPAdapter(tools.registry(), MCPConfig.from_env(), tools.tool_specs())
    return AssistantService(adapter)


@app.get("/assets")
def list_assets(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    repo = AssetRepository(db)
    return repo.list_active_asset_ids(limit=limit, offset=offset)


@app.get("/assets/{asset_id}")
def get_asset(asset_id: str, db: Database = Depends(get_db)) -> dict[str, Any]:
    repo = AssetRepository(db)
    doc = repo.get_active_asset(asset_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id!r} was not found.")
    return serialize_mongo(doc)


@app.get("/data-sources")
def list_data_sources(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    repo = DataSourceRepository(db)
    return repo.list_source_ids(limit=limit, offset=offset)


@app.get("/data-sources/{source_id}")
def get_data_source(source_id: str, db: Database = Depends(get_db)) -> dict[str, Any]:
    repo = DataSourceRepository(db)
    doc = repo.get_data_source(source_id)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"Data source {source_id!r} was not found.",
        )
    return serialize_mongo(doc)


@app.get("/time-series")
def get_time_series(
    asset_id: str | None = Query(default=None, alias="assetId"),
    data_source_id: str | None = Query(default=None, alias="dataSourceId"),
    start_date_raw: str | None = Query(default=None, alias="startDate"),
    end_date_raw: str | None = Query(default=None, alias="endDate"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    if not asset_id:
        raise HTTPException(status_code=400, detail="assetId query parameter is required.")
    if not data_source_id:
        raise HTTPException(status_code=400, detail="dataSourceId query parameter is required.")

    start_date = _parse_iso_date(start_date_raw, "startDate")
    end_date = _parse_iso_date(end_date_raw, "endDate")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="startDate must be on or before endDate.")

    repo = TimeSeriesRepository(db)
    points = repo.query_points(
        asset_id=asset_id,
        data_source_id=data_source_id,
        start_date=start_date,
        end_date=end_date,
    )
    if not points:
        raise HTTPException(
            status_code=404,
            detail=(
                "No time-series rows found for "
                f"assetId={asset_id!r} and dataSourceId={data_source_id!r}."
            ),
        )

    return {
        "assetId": asset_id,
        "dataSourceId": data_source_id,
        "count": len(points),
        "points": serialize_mongo(points),
    }


@app.get("/quality/freshness")
def get_quality_freshness(
    threshold_hours: int = Query(
        default=DEFAULT_FRESHNESS_THRESHOLD_HOURS,
        alias="thresholdHours",
        ge=1,
    ),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    repo = QualityRepository(db)
    rows = repo.compute_freshness_rows(threshold_hours=threshold_hours)
    return {"thresholdHours": threshold_hours, "count": len(rows), "items": serialize_mongo(rows)}


@app.get("/quality/summary")
def get_quality_summary(db: Database = Depends(get_db)) -> dict[str, Any]:
    repo = QualityRepository(db)
    latest_run = repo.latest_ingestion()
    duplicate_risk = repo.duplicate_risk()
    invalid_rows = int(latest_run.get("invalidRowsSkipped", 0)) if latest_run else 0

    return {
        "totalTimeSeriesRows": repo.total_time_series_rows(),
        "duplicateRisk": duplicate_risk,
        "invalidRowsSkippedLatestRun": invalid_rows,
        "latestIngestionRun": serialize_mongo(latest_run),
    }


@app.get("/analytics/summary")
def get_analytics_summary(
    asset_id: str | None = Query(default=None, alias="assetId"),
    data_source_id: str | None = Query(default=None, alias="dataSourceId"),
    start_date_raw: str | None = Query(default=None, alias="startDate"),
    end_date_raw: str | None = Query(default=None, alias="endDate"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    if not asset_id:
        raise HTTPException(status_code=400, detail="assetId query parameter is required.")
    if not data_source_id:
        raise HTTPException(status_code=400, detail="dataSourceId query parameter is required.")

    start_date = _parse_iso_date(start_date_raw, "startDate")
    end_date = _parse_iso_date(end_date_raw, "endDate")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="startDate must be on or before endDate.")

    repo = AnalyticsRepository(db)
    points = repo.query_points(
        asset_id=asset_id,
        data_source_id=data_source_id,
        start_date=start_date,
        end_date=end_date,
    )
    if not points:
        raise HTTPException(
            status_code=404,
            detail=(
                "No time-series rows found for "
                f"assetId={asset_id!r} and dataSourceId={data_source_id!r}."
            ),
        )

    metric_summary = summarize_time_series(points)
    close_summary = metric_summary.get("close")
    if close_summary is None:
        raise HTTPException(
            status_code=404,
            detail="No valid close values found for the requested time-series selection.",
        )

    payload: dict[str, Any] = {
        "assetId": asset_id,
        "dataSourceId": data_source_id,
        "count": len(points),
        "dateRange": {"start": points[0].get("timestamp"), "end": points[-1].get("timestamp")},
        "close": close_summary,
    }
    for field_name in ("open", "high", "low", "volume"):
        if field_name in metric_summary:
            payload[field_name] = metric_summary[field_name]
    return serialize_mongo(payload)


@app.get("/analytics/forecast")
def get_analytics_forecast(
    asset_id: str | None = Query(default=None, alias="assetId"),
    data_source_id: str | None = Query(default=None, alias="dataSourceId"),
    start_date_raw: str | None = Query(default=None, alias="startDate"),
    end_date_raw: str | None = Query(default=None, alias="endDate"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    if not asset_id:
        raise HTTPException(status_code=400, detail="assetId query parameter is required.")
    if not data_source_id:
        raise HTTPException(status_code=400, detail="dataSourceId query parameter is required.")

    start_date = _parse_iso_date(start_date_raw, "startDate")
    end_date = _parse_iso_date(end_date_raw, "endDate")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="startDate must be on or before endDate.")

    repo = AnalyticsRepository(db)
    points = repo.query_points(
        asset_id=asset_id,
        data_source_id=data_source_id,
        start_date=start_date,
        end_date=end_date,
    )
    if not points:
        raise HTTPException(
            status_code=404,
            detail=(
                "No time-series rows found for "
                f"assetId={asset_id!r} and dataSourceId={data_source_id!r}."
            ),
        )

    try:
        forecast = forecast_next_close(points, window=10)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return serialize_mongo(
        {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            **forecast,
            "note": (
                "Simple deterministic trend estimate from historical close values only. "
                "Not financial advice."
            ),
        }
    )


@app.get("/analytics/spark-shape")
def get_analytics_spark_shape(
    asset_id: str | None = Query(default=None, alias="assetId"),
    data_source_id: str | None = Query(default=None, alias="dataSourceId"),
    start_date_raw: str | None = Query(default=None, alias="startDate"),
    end_date_raw: str | None = Query(default=None, alias="endDate"),
    db: Database = Depends(get_db),
) -> dict[str, Any]:
    if not asset_id:
        raise HTTPException(status_code=400, detail="assetId query parameter is required.")
    if not data_source_id:
        raise HTTPException(status_code=400, detail="dataSourceId query parameter is required.")

    start_date = _parse_iso_date(start_date_raw, "startDate")
    end_date = _parse_iso_date(end_date_raw, "endDate")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="startDate must be on or before endDate.")

    repo = AnalyticsRepository(db)
    points = repo.query_points(
        asset_id=asset_id,
        data_source_id=data_source_id,
        start_date=start_date,
        end_date=end_date,
    )
    if not points:
        raise HTTPException(
            status_code=404,
            detail=(
                "No time-series rows found for "
                f"assetId={asset_id!r} and dataSourceId={data_source_id!r}."
            ),
        )

    rows = flatten_time_series_rows(points)
    return serialize_mongo(
        {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            "count": len(rows),
            "rows": rows,
        }
    )


@app.post("/assistant/query", response_model=AssistantQueryResponse)
def query_assistant(
    payload: AssistantQueryRequest,
    service: AssistantService = Depends(get_assistant_service),
) -> AssistantQueryResponse:
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="question must not be empty.")
    try:
        return service.answer(question, payload.context)
    except MCPUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "error",
                "message": str(exc),
                "action": "Set ASSISTANT_MCP_ENABLED=true and verify MCP assistant configuration.",
            },
        ) from exc
