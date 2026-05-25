from __future__ import annotations

from copy import deepcopy
from typing import Any

import ingestion_service as ingestion


class FakeAssetsCollection:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs

    def find_one(self, query: dict[str, Any], sort: list[tuple[str, int]] | None = None) -> dict[str, Any] | None:
        matches = [doc for doc in self.docs if all(doc.get(k) == v for k, v in query.items())]
        if not matches:
            return None
        if sort:
            for key, direction in reversed(sort):
                matches.sort(key=lambda doc: doc.get(key), reverse=direction == -1)
        return deepcopy(matches[0])

    def insert_one(self, doc: dict[str, Any]) -> None:
        self.docs.append(deepcopy(doc))

    def update_many(self, query: dict[str, Any], update: dict[str, Any]) -> None:
        set_payload = update.get("$set", {})
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in query.items()):
                doc.update(set_payload)


class FakeLifecycleCollection:
    def __init__(self) -> None:
        self.docs_by_id: dict[str, dict[str, Any]] = {}

    def update_one(self, query: dict[str, Any], update: dict[str, Any], upsert: bool = False) -> None:
        event_id = query.get("eventId")
        if not isinstance(event_id, str):
            return
        if event_id in self.docs_by_id:
            return
        if upsert:
            payload = update.get("$setOnInsert", {})
            self.docs_by_id[event_id] = deepcopy(payload)


def _asset_payload(*, name: str = "Tesla Inc") -> dict[str, Any]:
    return {
        "assetId": "TSLA",
        "symbol": "TSLA",
        "name": name,
        "instrumentClass": "Stock",
        "exchange": "NASDAQ",
        "currency": "USD",
        "attributes": {"dividend_yield": None, "time_zone": "US/Eastern"},
    }


def test_upsert_versioned_asset_sets_valid_to_and_records_lifecycle_event(
    monkeypatch,
) -> None:
    assets = FakeAssetsCollection(
        [
            {
                **_asset_payload(name="Tesla Old"),
                "version": 1,
                "is_active": True,
                "valid_from": "2026-05-01T00:00:00Z",
                "dataSourceId": "alpha_vantage_api_v1",
            }
        ]
    )
    lifecycle = FakeLifecycleCollection()
    monkeypatch.setattr(ingestion, "now_utc_iso", lambda: "2026-05-03T00:00:00Z")

    action = ingestion.upsert_versioned_asset(
        assets,
        _asset_payload(name="Tesla Inc"),
        data_source_id="alpha_vantage_api_v1",
        lifecycle_events=lifecycle,
    )

    assert action == "inserted_new_version"
    inactive = next(doc for doc in assets.docs if doc["version"] == 1)
    active = next(doc for doc in assets.docs if doc["version"] == 2)
    assert inactive["is_active"] is False
    assert inactive["valid_to"] == "2026-05-03T00:00:00Z"
    assert active["is_active"] is True
    assert active["valid_from"] == "2026-05-03T00:00:00Z"
    assert lifecycle.docs_by_id["TSLA:deactivated:1->2"]["valid_to"] == "2026-05-03T00:00:00Z"


def test_upsert_versioned_asset_unchanged_is_idempotent() -> None:
    payload = _asset_payload(name="Tesla Inc")
    assets = FakeAssetsCollection(
        [
            {
                **payload,
                "version": 2,
                "is_active": True,
                "valid_from": "2026-05-02T00:00:00Z",
                "dataSourceId": "alpha_vantage_api_v1",
            }
        ]
    )
    lifecycle = FakeLifecycleCollection()

    action = ingestion.upsert_versioned_asset(
        assets,
        payload,
        data_source_id="alpha_vantage_api_v1",
        lifecycle_events=lifecycle,
    )

    assert action == "unchanged"
    assert len(assets.docs) == 1
    assert lifecycle.docs_by_id == {}
