from __future__ import annotations

from collections.abc import Iterator
from copy import deepcopy
from typing import Any

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from app.main import app, get_db


class FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def sort(self, key_or_list: str | list[tuple[str, int]], direction: int | None = None) -> "FakeCursor":
        sort_fields = key_or_list if isinstance(key_or_list, list) else [(key_or_list, direction or 1)]
        for key, sort_direction in reversed(sort_fields):
            self._docs.sort(
                key=lambda doc: doc.get(key) if doc.get(key) is not None else "",
                reverse=sort_direction == -1,
            )
        return self

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._docs)


class FakeCollection:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def find(
        self,
        query: dict[str, Any],
        projection: dict[str, int] | None = None,
    ) -> FakeCursor:
        docs = [self._project(deepcopy(doc), projection) for doc in self._docs if self._matches(doc, query)]
        return FakeCursor(docs)

    def find_one(
        self,
        query: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
        projection: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        cursor = self.find(query, projection=projection)
        if sort:
            cursor.sort(sort)
        return next(iter(cursor), None)

    def count_documents(self, query: dict[str, Any]) -> int:
        return len([doc for doc in self._docs if self._matches(doc, query)])

    def aggregate(self, pipeline: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        if any("$limit" in stage for stage in pipeline):
            seen: set[tuple[Any, Any, Any]] = set()
            for doc in self._docs:
                key = (doc.get("assetId"), doc.get("dataSourceId"), doc.get("timestamp"))
                if key in seen:
                    yield {"_id": key, "count": 2}
                    return
                seen.add(key)
            return

        latest_by_pair: dict[tuple[Any, Any], str] = {}
        for doc in self._docs:
            key = (doc.get("assetId"), doc.get("dataSourceId"))
            timestamp = doc.get("timestamp")
            if key not in latest_by_pair or timestamp > latest_by_pair[key]:
                latest_by_pair[key] = timestamp
        for asset_id, data_source_id in sorted(latest_by_pair):
            yield {
                "_id": {"assetId": asset_id, "dataSourceId": data_source_id},
                "latestTimestamp": latest_by_pair[(asset_id, data_source_id)],
            }

    def insert_one(self, doc: dict[str, Any]) -> None:
        self._docs.append(deepcopy(doc))

    def _matches(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, expected in query.items():
            actual = doc.get(key)
            if isinstance(expected, dict):
                if "$gte" in expected and actual < expected["$gte"]:
                    return False
                if "$lte" in expected and actual > expected["$lte"]:
                    return False
                continue
            if actual != expected:
                return False
        return True

    def _project(
        self,
        doc: dict[str, Any],
        projection: dict[str, int] | None,
    ) -> dict[str, Any]:
        if projection is None:
            return doc
        include = {key for key, value in projection.items() if value == 1}
        exclude_id = projection.get("_id") == 0
        if include:
            projected = {key: doc[key] for key in include if key in doc}
            if not exclude_id and "_id" in doc:
                projected["_id"] = doc["_id"]
            return projected
        return {key: value for key, value in doc.items() if projection.get(key) != 0}


class FakeDb:
    def __init__(self) -> None:
        self.collections = {
            "assets": FakeCollection(
                [
                    {
                        "_id": ObjectId("000000000000000000000001"),
                        "assetId": "TSLA",
                        "symbol": "TSLA",
                        "name": "Tesla Inc",
                        "version": 2,
                        "is_active": True,
                        "dataSourceId": "alpha_vantage_api_v1",
                    },
                    {
                        "_id": ObjectId("000000000000000000000002"),
                        "assetId": "TSLA",
                        "symbol": "TSLA",
                        "name": "Old Tesla",
                        "version": 1,
                        "is_active": False,
                    },
                    {
                        "_id": ObjectId("000000000000000000000003"),
                        "assetId": "BTC",
                        "symbol": "BTC",
                        "name": "Bitcoin",
                        "version": 1,
                        "is_active": True,
                    },
                ]
            ),
            "data_sources": FakeCollection(
                [
                    {
                        "_id": ObjectId("000000000000000000000004"),
                        "sourceId": "alpha_vantage_api_v1",
                        "name": "Alpha Vantage API",
                    },
                    {
                        "_id": ObjectId("000000000000000000000005"),
                        "sourceId": "metals_dev_v1",
                        "name": "Metals.dev API",
                    },
                ]
            ),
            "time_series": FakeCollection(
                [
                    {
                        "_id": ObjectId("000000000000000000000006"),
                        "assetId": "TSLA",
                        "dataSourceId": "alpha_vantage_api_v1",
                        "timestamp": "2026-05-02T00:00:00Z",
                        "point": {"close": 107.0},
                    },
                    {
                        "_id": ObjectId("000000000000000000000007"),
                        "assetId": "TSLA",
                        "dataSourceId": "alpha_vantage_api_v1",
                        "timestamp": "2026-05-01T00:00:00Z",
                        "point": {"close": 105.5},
                    },
                ]
            ),
            "ingestion_runs": FakeCollection(
                [
                    {
                        "_id": ObjectId("000000000000000000000008"),
                        "runId": "run-1",
                        "startedAt": "2026-05-02T01:00:00+00:00",
                        "finishedAt": "2026-05-02T01:01:00+00:00",
                        "status": "success",
                        "symbolsRequested": ["TSLA"],
                        "symbolsSucceeded": ["TSLA"],
                        "symbolsFailed": [],
                        "recordsInserted": 2,
                        "invalidRowsSkipped": 1,
                        "duplicateRowsSkipped": 0,
                        "errorSummary": None,
                        "dataSourcesTouched": ["alpha_vantage_api_v1"],
                    },
                ]
            ),
        }

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections[name]


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("MONGO_URI", "mongodb://localhost:27017")
    app.dependency_overrides[get_db] = lambda: FakeDb()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_list_assets(client: TestClient) -> None:
    response = client.get("/assets")

    assert response.status_code == 200
    assert response.json() == {"assetIds": ["BTC", "TSLA"]}


def test_get_asset(client: TestClient) -> None:
    response = client.get("/assets/TSLA")

    assert response.status_code == 200
    payload = response.json()
    assert payload["assetId"] == "TSLA"
    assert payload["version"] == 2
    assert payload["_id"] == "000000000000000000000001"


def test_get_asset_not_found(client: TestClient) -> None:
    response = client.get("/assets/MSFT")

    assert response.status_code == 404
    assert response.json()["detail"] == "Asset 'MSFT' was not found."


def test_list_data_sources(client: TestClient) -> None:
    response = client.get("/data-sources")

    assert response.status_code == 200
    assert response.json() == {"dataSourceIds": ["alpha_vantage_api_v1", "metals_dev_v1"]}


def test_get_data_source(client: TestClient) -> None:
    response = client.get("/data-sources/alpha_vantage_api_v1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sourceId"] == "alpha_vantage_api_v1"
    assert payload["name"] == "Alpha Vantage API"


def test_get_data_source_not_found(client: TestClient) -> None:
    response = client.get("/data-sources/missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Data source 'missing' was not found."


def test_get_time_series(client: TestClient) -> None:
    response = client.get(
        "/time-series",
        params={"assetId": "TSLA", "dataSourceId": "alpha_vantage_api_v1"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["assetId"] == "TSLA"
    assert payload["dataSourceId"] == "alpha_vantage_api_v1"
    assert payload["count"] == 2
    assert [point["timestamp"] for point in payload["points"]] == [
        "2026-05-01T00:00:00Z",
        "2026-05-02T00:00:00Z",
    ]


def test_get_time_series_with_date_filter(client: TestClient) -> None:
    response = client.get(
        "/time-series",
        params={
            "assetId": "TSLA",
            "dataSourceId": "alpha_vantage_api_v1",
            "startDate": "2026-05-02",
            "endDate": "2026-05-02",
        },
    )

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["points"][0]["timestamp"] == "2026-05-02T00:00:00Z"


def test_get_time_series_not_found(client: TestClient) -> None:
    response = client.get(
        "/time-series",
        params={"assetId": "BTC", "dataSourceId": "alpha_vantage_api_v1"},
    )

    assert response.status_code == 404
    assert "No time-series rows found" in response.json()["detail"]


def test_get_time_series_validation_error(client: TestClient) -> None:
    response = client.get("/time-series", params={"assetId": "TSLA"})

    assert response.status_code == 400
    assert response.json()["detail"] == "dataSourceId query parameter is required."


def test_quality_freshness_endpoint(client: TestClient) -> None:
    response = client.get("/quality/freshness", params={"thresholdHours": 24})

    assert response.status_code == 200
    payload = response.json()
    assert payload["thresholdHours"] == 24
    assert payload["count"] == 1
    assert payload["items"][0]["assetId"] == "TSLA"
    assert payload["items"][0]["dataSourceId"] == "alpha_vantage_api_v1"
    assert payload["items"][0]["latestTimestamp"] == "2026-05-02T00:00:00Z"
    assert payload["items"][0]["status"] in {"fresh", "stale"}


def test_quality_summary_endpoint(client: TestClient) -> None:
    response = client.get("/quality/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["totalTimeSeriesRows"] == 2
    assert payload["duplicateRisk"] == {"hasDuplicates": False, "status": "ok"}
    assert payload["invalidRowsSkippedLatestRun"] == 1
    assert payload["latestIngestionRun"]["status"] == "success"
