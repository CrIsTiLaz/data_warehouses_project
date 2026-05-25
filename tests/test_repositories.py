from __future__ import annotations

from collections.abc import Iterator
from copy import deepcopy
from datetime import date
from typing import Any

from repositories import AssetRepository, DataSourceRepository, TimeSeriesRepository


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

    def find(self, query: dict[str, Any], projection: dict[str, int] | None = None) -> FakeCursor:
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

    def _project(self, doc: dict[str, Any], projection: dict[str, int] | None) -> dict[str, Any]:
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
                    {"assetId": "TSLA", "version": 2, "is_active": True},
                    {"assetId": "TSLA", "version": 1, "is_active": False},
                    {"assetId": "BTC", "version": 1, "is_active": True},
                ]
            ),
            "data_sources": FakeCollection(
                [
                    {"sourceId": "alpha_vantage_api_v1", "version": 2},
                    {"sourceId": "alpha_vantage_api_v1", "version": 1},
                    {"sourceId": "metals_dev_v1", "version": 1},
                ]
            ),
            "time_series": FakeCollection(
                [
                    {
                        "assetId": "TSLA",
                        "dataSourceId": "alpha_vantage_api_v1",
                        "timestamp": "2026-05-01T00:00:00Z",
                    },
                    {
                        "assetId": "TSLA",
                        "dataSourceId": "alpha_vantage_api_v1",
                        "timestamp": "2026-05-02T00:00:00Z",
                    },
                ]
            ),
        }

    def __getitem__(self, key: str) -> FakeCollection:
        return self.collections[key]


def test_asset_repository_list_active_asset_ids_with_pagination() -> None:
    repo = AssetRepository(FakeDb())
    result = repo.list_active_asset_ids(limit=1, offset=1)

    assert result == {"assetIds": ["TSLA"], "count": 1, "total": 2, "limit": 1, "offset": 1}


def test_data_source_repository_deduplicates_latest_source_ids() -> None:
    repo = DataSourceRepository(FakeDb())
    result = repo.list_source_ids(limit=100, offset=0)

    assert result["dataSourceIds"] == ["alpha_vantage_api_v1", "metals_dev_v1"]
    assert result["total"] == 2


def test_time_series_repository_applies_date_range_filters() -> None:
    repo = TimeSeriesRepository(FakeDb())
    rows = repo.query_points(
        asset_id="TSLA",
        data_source_id="alpha_vantage_api_v1",
        start_date=date(2026, 5, 2),
        end_date=date(2026, 5, 2),
    )

    assert len(rows) == 1
    assert rows[0]["timestamp"] == "2026-05-02T00:00:00Z"
