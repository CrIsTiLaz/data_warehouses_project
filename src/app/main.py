from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

from assistant.mcp_adapter import LocalMCPAdapter, MCPConfig, MCPUnavailableError
from assistant.models import AssistantQueryRequest, AssistantQueryResponse
from assistant.service import AssistantService
from assistant.tools import DwhReadOnlyTools
from bson import ObjectId
from quality import (
    DEFAULT_FRESHNESS_THRESHOLD_HOURS,
    compute_freshness,
    detect_duplicate_risk,
    latest_ingestion_run,
)
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pymongo import MongoClient
from pymongo.database import Database

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
def list_assets(db: Database = Depends(get_db)) -> dict[str, list[str]]:
    docs = db["assets"].find(
        {"is_active": True},
        projection={"_id": 0, "assetId": 1, "version": 1},
    ).sort([("assetId", 1), ("version", -1)])

    asset_ids: list[str] = []
    seen: set[str] = set()
    for doc in docs:
        asset_id = doc.get("assetId")
        if asset_id is not None and asset_id not in seen:
            value = str(asset_id)
            seen.add(value)
            asset_ids.append(value)
    return {"assetIds": asset_ids}


@app.get("/assets/{asset_id}")
def get_asset(asset_id: str, db: Database = Depends(get_db)) -> dict[str, Any]:
    doc = db["assets"].find_one(
        {"assetId": asset_id, "is_active": True},
        sort=[("version", -1)],
    )
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id!r} was not found.")
    return serialize_mongo(doc)


@app.get("/data-sources")
def list_data_sources(db: Database = Depends(get_db)) -> dict[str, list[str]]:
    docs = db["data_sources"].find(
        {},
        projection={"_id": 0, "sourceId": 1, "version": 1},
    ).sort([("sourceId", 1), ("version", -1)])

    source_ids: list[str] = []
    seen: set[str] = set()
    for doc in docs:
        source_id = doc.get("sourceId")
        if source_id is not None and source_id not in seen:
            value = str(source_id)
            seen.add(value)
            source_ids.append(value)
    return {"dataSourceIds": source_ids}


@app.get("/data-sources/{source_id}")
def get_data_source(source_id: str, db: Database = Depends(get_db)) -> dict[str, Any]:
    doc = db["data_sources"].find_one(
        {"sourceId": source_id},
        sort=[("version", -1)],
    )
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

    query: dict[str, Any] = {"assetId": asset_id, "dataSourceId": data_source_id}
    timestamp_filter: dict[str, str] = {}
    if start_date is not None:
        timestamp_filter["$gte"] = f"{start_date.isoformat()}T00:00:00Z"
    if end_date is not None:
        timestamp_filter["$lte"] = f"{end_date.isoformat()}T23:59:59Z"
    if timestamp_filter:
        query["timestamp"] = timestamp_filter

    points = list(db["time_series"].find(query).sort("timestamp", 1))
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
    rows = compute_freshness(db["time_series"], threshold_hours=threshold_hours)
    return {"thresholdHours": threshold_hours, "count": len(rows), "items": serialize_mongo(rows)}


@app.get("/quality/summary")
def get_quality_summary(db: Database = Depends(get_db)) -> dict[str, Any]:
    latest_run = latest_ingestion_run(db["ingestion_runs"])
    duplicate_risk = detect_duplicate_risk(db["time_series"])
    invalid_rows = int(latest_run.get("invalidRowsSkipped", 0)) if latest_run else 0

    return {
        "totalTimeSeriesRows": db["time_series"].count_documents({}),
        "duplicateRisk": duplicate_risk,
        "invalidRowsSkippedLatestRun": invalid_rows,
        "latestIngestionRun": serialize_mongo(latest_run),
    }


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
