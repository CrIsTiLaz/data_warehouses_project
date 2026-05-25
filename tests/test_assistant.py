from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_assistant_service, get_db
from assistant.mcp_adapter import LLMConfig, MCPAdapter, MCPUnavailableError, OpenRouterClient
from assistant.models import AssistantQueryResponse, ToolGrounding
from assistant.service import AssistantService


class FakeAdapter(MCPAdapter):
    def __init__(self, results: dict[str, dict[str, Any]], *, unavailable: bool = False) -> None:
        self.results = results
        self.unavailable = unavailable
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def list_tools(self) -> list[str]:
        if self.unavailable:
            raise MCPUnavailableError("fake MCP unavailable")
        return sorted(self.results)

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.unavailable:
            raise MCPUnavailableError("fake MCP unavailable")
        self.calls.append((tool_name, arguments))
        return self.results[tool_name]

    def tool_specs(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": f"Fake tool {name}",
                    "parameters": {"type": "object", "properties": {}},
                },
            }
            for name in sorted(self.results)
        ]


class FakeLLMClient:
    def __init__(self, responses: list[dict[str, Any]] | None = None, *, fail: bool = False) -> None:
        self.responses = responses or []
        self.fail = fail
        self.payloads: list[dict[str, Any]] = []

    def chat_completion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.payloads.append(payload)
        if self.fail:
            raise RuntimeError("network timeout")
        return self.responses.pop(0)


def ready_llm_config() -> LLMConfig:
    return LLMConfig(
        api_key="test-key",
        model="openai/gpt-4o-mini",
        base_url="https://openrouter.ai/api/v1",
        timeout_seconds=15,
    )


class FakeService:
    def answer(
        self,
        question: str,
        context: dict[str, Any] | None = None,
    ) -> AssistantQueryResponse:
        return AssistantQueryResponse(
            answer=f"Grounded answer for: {question}",
            status="grounded",
            grounding=[
                ToolGrounding(
                    toolName="list_assets",
                    arguments={},
                    provenance={"endpoint": "GET /assets"},
                    resultSummary={"assetIds": ["TSLA"]},
                )
            ],
        )


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("MONGO_URI", "mongodb://localhost:27017")
    app.dependency_overrides[get_db] = lambda: object()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_assistant_happy_path_uses_fake_tool_results() -> None:
    adapter = FakeAdapter(
        {
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 2,
                "points": [
                    {"timestamp": "2026-05-01T00:00:00Z", "point": {"close": 105.5}},
                    {"timestamp": "2026-05-02T00:00:00Z", "point": {"close": 107.0}},
                ],
                "provenance": {
                    "endpoint": "GET /time-series",
                    "collection": "time_series",
                    "filter": {"assetId": "TSLA", "dataSourceId": "alpha_vantage_api_v1"},
                },
            }
        }
    )

    response = AssistantService(adapter).answer(
        "What is the latest close price for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "grounded"
    assert "Found 2 time-series rows" in response.answer
    assert "latest close is 107.0" in response.answer
    assert response.grounding[0].toolName == "query_time_series"
    assert adapter.calls[0][1]["asset_id"] == "TSLA"


def test_assistant_missing_data_returns_insufficient_data_without_numbers() -> None:
    adapter = FakeAdapter(
        {
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 0,
                "points": [],
                "provenance": {"endpoint": "GET /time-series"},
            }
        }
    )

    response = AssistantService(adapter).answer(
        "What is the close price for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "insufficient_data"
    assert "No data found" in response.answer
    assert "107" not in response.answer
    assert "999" not in response.answer


def test_assistant_analytics_summary_question_routes_to_summary_tool() -> None:
    adapter = FakeAdapter(
        {
            "get_analytics_summary": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 2,
                "dateRange": {"start": "2026-05-01T00:00:00Z", "end": "2026-05-02T00:00:00Z"},
                "close": {"min": 105.5, "max": 107.0, "average": 106.25},
                "provenance": {"endpoint": "GET /analytics/summary"},
            }
        }
    )

    response = AssistantService(adapter).answer(
        "What is the min, max, and average close price for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "grounded"
    assert response.grounding[0].toolName == "get_analytics_summary"
    assert "close min=105.5" in response.answer
    assert adapter.calls[0][0] == "get_analytics_summary"


def test_assistant_analytics_forecast_question_routes_to_forecast_tool() -> None:
    adapter = FakeAdapter(
        {
            "get_analytics_forecast": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 10,
                "basis": "last_10_close_values",
                "latestTimestamp": "2026-05-08T00:00:00Z",
                "latestClose": 428.35,
                "averageDailyChange": 2.15,
                "forecast": {"nextPeriodClose": 430.5, "direction": "up"},
                "provenance": {"endpoint": "GET /analytics/forecast"},
            }
        }
    )

    response = AssistantService(adapter).answer("What is the trend forecast for TSLA from alpha_vantage_api_v1?")

    assert response.status == "grounded"
    assert response.grounding[0].toolName == "get_analytics_forecast"
    assert "Next period close estimate is 430.5 (up)." in response.answer
    assert adapter.calls[0][0] == "get_analytics_forecast"


def test_assistant_ambiguity_requests_clarification() -> None:
    adapter = FakeAdapter(
        {
            "list_assets": {
                "assetIds": ["TSLA", "BTC"],
                "provenance": {"endpoint": "GET /assets"},
            },
            "list_data_sources": {
                "dataSourceIds": ["alpha_vantage_api_v1"],
                "provenance": {"endpoint": "GET /data-sources"},
            },
        }
    )

    response = AssistantService(adapter).answer("What is the latest close price?")

    assert response.status == "insufficient_data"
    assert response.clarificationNeeded == "Which assetId and dataSourceId should I use?"
    assert "Available assets include: TSLA, BTC" in response.answer
    assert [call[0] for call in adapter.calls] == ["list_assets", "list_data_sources"]


def test_assistant_mcp_unavailable_raises() -> None:
    service = AssistantService(FakeAdapter({}, unavailable=True))

    with pytest.raises(MCPUnavailableError):
        service.answer("List assets")


def test_assistant_llm_tool_call_happy_path() -> None:
    adapter = FakeAdapter(
        {
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 1,
                "points": [{"timestamp": "2026-05-02T00:00:00Z", "point": {"close": 107.0}}],
                "provenance": {"endpoint": "GET /time-series"},
            }
        }
    )
    llm = FakeLLMClient(
        [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "query_time_series",
                                        "arguments": (
                                            '{"asset_id":"TSLA",'
                                            '"data_source_id":"alpha_vantage_api_v1"}'
                                        ),
                                    },
                                }
                            ],
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "TSLA latest close from alpha_vantage_api_v1 is 107.0.",
                        }
                    }
                ]
            },
        ]
    )

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "What is the latest close price for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "grounded"
    assert response.answer == "TSLA latest close from alpha_vantage_api_v1 is 107.0."
    assert response.grounding[0].toolName == "query_time_series"
    assert adapter.calls == [
        ("query_time_series", {"asset_id": "TSLA", "data_source_id": "alpha_vantage_api_v1"})
    ]
    assert "tools" in llm.payloads[0]


def test_assistant_llm_multi_tool_call_path() -> None:
    adapter = FakeAdapter(
        {
            "list_assets": {
                "assetIds": ["TSLA"],
                "provenance": {"endpoint": "GET /assets"},
            },
            "get_asset": {
                "asset": {"assetId": "TSLA", "name": "Tesla Inc", "instrumentClass": "Stock"},
                "provenance": {"endpoint": "GET /assets/TSLA"},
            },
        }
    )
    llm = FakeLLMClient(
        [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "list_assets", "arguments": "{}"},
                                },
                                {
                                    "id": "call-2",
                                    "type": "function",
                                    "function": {"name": "get_asset", "arguments": '{"asset_id":"TSLA"}'},
                                },
                            ],
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "TSLA is Tesla Inc with instrument class Stock.",
                        }
                    }
                ]
            },
        ]
    )

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "Tell me about TSLA"
    )

    assert response.status == "grounded"
    assert response.answer == "TSLA is Tesla Inc with instrument class Stock."
    assert [item.toolName for item in response.grounding] == ["list_assets", "get_asset"]


def test_assistant_llm_runtime_error_falls_back_to_regex_planner() -> None:
    adapter = FakeAdapter(
        {
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 1,
                "points": [{"timestamp": "2026-05-02T00:00:00Z", "point": {"close": 107.0}}],
                "provenance": {"endpoint": "GET /time-series"},
            }
        }
    )
    llm = FakeLLMClient(fail=True)

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "What is the latest close price for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "grounded"
    assert "latest close is 107.0" in response.answer
    assert adapter.calls[0][0] == "query_time_series"


def test_assistant_llm_guardrail_overrides_wrong_tool_for_price_question() -> None:
    adapter = FakeAdapter(
        {
            "list_assets": {
                "assetIds": ["TSLA", "AAPL"],
                "provenance": {"endpoint": "GET /assets"},
            },
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 1,
                "points": [{"timestamp": "2026-05-02T00:00:00Z", "point": {"close": 107.0}}],
                "provenance": {"endpoint": "GET /time-series"},
            },
        }
    )
    llm = FakeLLMClient(
        [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "list_assets", "arguments": "{}"},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    )

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "What is the latest close price for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "grounded"
    assert response.grounding[0].toolName == "query_time_series"
    assert [call[0] for call in adapter.calls] == ["query_time_series"]
    assert len(llm.payloads) == 1


def test_assistant_llm_guardrail_uses_context_for_price_question() -> None:
    adapter = FakeAdapter(
        {
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 1,
                "points": [{"timestamp": "2026-05-02T00:00:00Z", "point": {"close": 107.0}}],
                "provenance": {"endpoint": "GET /time-series"},
            }
        }
    )
    llm = FakeLLMClient(
        [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "list_assets", "arguments": "{}"},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    )

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "What is the latest close price?",
        context={"assetId": "TSLA", "dataSourceId": "alpha_vantage_api_v1"},
    )

    assert response.status == "grounded"
    assert response.grounding[0].toolName == "query_time_series"
    assert adapter.calls == [
        (
            "query_time_series",
            {
                "asset_id": "TSLA",
                "data_source_id": "alpha_vantage_api_v1",
                "start_date": None,
                "end_date": None,
            },
        )
    ]


def test_assistant_llm_guardrail_clarifies_missing_price_identifiers() -> None:
    adapter = FakeAdapter(
        {
            "list_assets": {
                "assetIds": ["TSLA", "BTC"],
                "provenance": {"endpoint": "GET /assets"},
            },
            "list_data_sources": {
                "dataSourceIds": ["alpha_vantage_api_v1"],
                "provenance": {"endpoint": "GET /data-sources"},
            },
        }
    )
    llm = FakeLLMClient(
        [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {"name": "list_assets", "arguments": "{}"},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    )

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "What is the latest close price?"
    )

    assert response.status == "insufficient_data"
    assert response.clarificationNeeded == "Which assetId and dataSourceId should I use?"
    assert [call[0] for call in adapter.calls] == ["list_assets", "list_data_sources"]


def test_assistant_llm_empty_tool_result_does_not_fabricate_values() -> None:
    adapter = FakeAdapter(
        {
            "query_time_series": {
                "assetId": "TSLA",
                "dataSourceId": "alpha_vantage_api_v1",
                "count": 0,
                "points": [],
                "provenance": {"endpoint": "GET /time-series"},
            }
        }
    )
    llm = FakeLLMClient(
        [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "query_time_series",
                                        "arguments": (
                                            '{"asset_id":"TSLA",'
                                            '"data_source_id":"alpha_vantage_api_v1"}'
                                        ),
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    )

    response = AssistantService(adapter, llm_client=llm, llm_config=ready_llm_config()).answer(
        "What is the latest close for TSLA from alpha_vantage_api_v1?"
    )

    assert response.status == "insufficient_data"
    assert "No data found" in response.answer
    assert "107" not in response.answer
    assert len(llm.payloads) == 1


def test_openrouter_client_uses_chat_completions_without_exposing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {"choices": [{"message": {"content": "ok"}}]}

    def fake_post(
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, Any],
        timeout: float,
    ) -> FakeResponse:
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("assistant.mcp_adapter.httpx.post", fake_post)
    client = OpenRouterClient(ready_llm_config())

    response = client.chat_completion({"messages": [{"role": "user", "content": "hi"}]})

    assert response["choices"][0]["message"]["content"] == "ok"
    assert captured["url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer test-key"
    assert captured["json"]["model"] == "openai/gpt-4o-mini"
    assert "test-key" not in repr(response)


def test_assistant_endpoint_happy_path(client: TestClient) -> None:
    app.dependency_overrides[get_assistant_service] = lambda: FakeService()

    response = client.post("/assistant/query", json={"question": "List assets"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "grounded"
    assert payload["grounding"][0]["toolName"] == "list_assets"


def test_assistant_endpoint_mcp_unavailable_returns_503(client: TestClient) -> None:
    response = client.post("/assistant/query", json={"question": "List assets"})

    assert response.status_code == 503
    payload = response.json()["detail"]
    assert payload["status"] == "error"
    assert "ASSISTANT_MCP_ENABLED=true" in payload["action"]


def test_assistant_endpoint_partial_llm_config_returns_503(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ASSISTANT_MCP_ENABLED", "true")
    monkeypatch.setenv("LLM_MODEL", "openai/gpt-4o-mini")
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    response = client.post("/assistant/query", json={"question": "List assets"})

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "LLM_API_KEY" in detail["message"]
    assert "test-key" not in str(detail)


def test_assistant_endpoint_rejects_blank_question(client: TestClient) -> None:
    app.dependency_overrides[get_assistant_service] = lambda: FakeService()

    response = client.post("/assistant/query", json={"question": "   "})

    assert response.status_code == 400
    assert response.json()["detail"] == "question must not be empty."


def test_assistant_endpoint_rejects_overly_long_question(client: TestClient) -> None:
    response = client.post("/assistant/query", json={"question": "x" * 1001})

    assert response.status_code == 422
