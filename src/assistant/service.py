from __future__ import annotations

import re
import json
from dataclasses import dataclass
from typing import Any

from assistant.mcp_adapter import LLMConfig, MCPAdapter, MCPUnavailableError, OpenRouterClient
from assistant.models import AssistantQueryResponse, ToolGrounding

SYSTEM_PROMPT = """You are a data warehouse assistant.
Use only read-only tools for facts and numbers.
Use only tool outputs for numeric, temporal, and entity-specific claims.
If tool data is missing, say insufficient data; do not invent values.
If assetId, dataSourceId, or date range is missing for a time-series request, ask a concise clarification.
Return concise answers."""


@dataclass(frozen=True)
class PlannedToolCall:
    name: str
    arguments: dict[str, Any]


class AssistantService:
    def __init__(
        self,
        adapter: MCPAdapter,
        llm_client: OpenRouterClient | None = None,
        llm_config: LLMConfig | None = None,
    ) -> None:
        self.adapter = adapter
        self.llm_config = llm_config or LLMConfig.from_env()
        self.llm_client = llm_client

    def answer(self, question: str, context: dict[str, Any] | None = None) -> AssistantQueryResponse:
        normalized_question = question.strip()
        if not normalized_question:
            raise ValueError("question must not be empty")

        self.llm_config.validate()
        if self.llm_client is None and self.llm_config.is_ready():
            self.llm_client = OpenRouterClient(self.llm_config)
        if self.llm_client is not None and self.llm_config.is_ready():
            try:
                return self._answer_with_llm(normalized_question, context or {})
            except RuntimeError:
                pass

        return self._answer_with_fallback(normalized_question, context or {})

    def _answer_with_fallback(
        self,
        question: str,
        context: dict[str, Any],
    ) -> AssistantQueryResponse:
        plan = self._plan(question, context)
        return self._execute_plan(question, plan)

    def _execute_plan(
        self,
        question: str,
        plan: list[PlannedToolCall],
    ) -> AssistantQueryResponse:
        grounding: list[ToolGrounding] = []
        results: list[tuple[PlannedToolCall, dict[str, Any]]] = []

        try:
            self.adapter.list_tools()
            for call in plan:
                result = self.adapter.execute_tool(call.name, call.arguments)
                results.append((call, result))
                grounding.append(self._grounding_for(call, result))
        except MCPUnavailableError:
            raise

        return self._compose_answer(question, results, grounding)

    def _answer_with_llm(
        self,
        question: str,
        context: dict[str, Any],
    ) -> AssistantQueryResponse:
        if self.llm_client is None:
            raise RuntimeError("LLM client is not configured.")
        guardrail_plan = self._guardrail_plan(question, context)
        grounding: list[ToolGrounding] = []
        results: list[tuple[PlannedToolCall, dict[str, Any]]] = []
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": json.dumps({"question": question, "context": context}, sort_keys=True),
            },
        ]
        first = self.llm_client.chat_completion(
            {
                "messages": messages,
                "tools": self.adapter.tool_specs(),
                "tool_choice": "auto",
            }
        )
        message = self._first_message(first)
        raw_tool_calls = message.get("tool_calls")
        tool_calls = raw_tool_calls if isinstance(raw_tool_calls, list) else []
        if guardrail_plan and self._should_override_llm_tool_calls(guardrail_plan, tool_calls):
            return self._execute_plan(question, guardrail_plan)
        if not tool_calls:
            return self._answer_with_fallback(question, context)

        messages.append(message)
        for tool_call in tool_calls:
            function = tool_call.get("function") or {}
            tool_name = function.get("name")
            if not isinstance(tool_name, str):
                continue
            arguments = self._parse_tool_arguments(function.get("arguments"))
            result = self.adapter.execute_tool(tool_name, arguments)
            planned = PlannedToolCall(tool_name, arguments)
            results.append((planned, result))
            grounding.append(self._grounding_for(planned, result))
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", tool_name),
                    "name": tool_name,
                    "content": json.dumps(self._summarize_tool_result(result), sort_keys=True),
                }
            )

        if not results:
            return self._answer_with_fallback(question, context)

        fallback_response = self._compose_answer(question, results, grounding)
        if fallback_response.status != "grounded":
            return fallback_response

        final = self.llm_client.chat_completion(
            {
                "messages": [
                    *messages,
                    {
                        "role": "user",
                        "content": (
                            "Answer the original question using only the tool outputs above. "
                            "If the outputs do not support a numeric or factual claim, say so."
                        ),
                    },
                ],
            }
        )
        final_answer = self._first_message(final).get("content")
        if not isinstance(final_answer, str) or not final_answer.strip():
            return fallback_response
        return AssistantQueryResponse(
            answer=final_answer.strip(),
            status=fallback_response.status,
            grounding=grounding,
            clarificationNeeded=fallback_response.clarificationNeeded,
        )

    def _plan(self, question: str, context: dict[str, Any]) -> list[PlannedToolCall]:
        lower = question.lower()
        asset_id = self._extract_value(context, "assetId") or self._extract_symbol(question)
        data_source_id = self._extract_value(context, "dataSourceId") or self._extract_source_id(question)
        start_date, end_date = self._extract_dates(question, context)

        if "quality" in lower or "freshness" in lower or "stale" in lower:
            if "summary" in lower:
                return [PlannedToolCall("get_quality_summary", {})]
            return [PlannedToolCall("get_quality_freshness", {"threshold_hours": int(context.get("thresholdHours", 24))})]
        if "data source" in lower or "datasource" in lower:
            if data_source_id:
                return [PlannedToolCall("get_data_source", {"source_id": data_source_id})]
            return [PlannedToolCall("list_data_sources", {})]
        if self._is_time_series_question(lower):
            if asset_id and data_source_id:
                return [
                    PlannedToolCall(
                        "query_time_series",
                        {
                            "asset_id": asset_id,
                            "data_source_id": data_source_id,
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                    )
                ]
            return [PlannedToolCall("list_assets", {}), PlannedToolCall("list_data_sources", {})]
        if asset_id:
            return [PlannedToolCall("get_asset", {"asset_id": asset_id})]
        return [PlannedToolCall("list_assets", {})]

    def _guardrail_plan(self, question: str, context: dict[str, Any]) -> list[PlannedToolCall] | None:
        if not self._is_time_series_question(question.lower()):
            return None
        return self._plan(question, context)

    def _is_time_series_question(self, lower_question: str) -> bool:
        return any(word in lower_question for word in ("price", "close", "time series", "historical", "rows"))

    def _should_override_llm_tool_calls(
        self,
        guardrail_plan: list[PlannedToolCall],
        tool_calls: list[dict[str, Any]],
    ) -> bool:
        guardrail_names = [call.name for call in guardrail_plan]
        llm_names = {
            ((tool_call.get("function") or {}).get("name"))
            for tool_call in tool_calls
            if isinstance((tool_call.get("function") or {}).get("name"), str)
        }
        if guardrail_names == ["query_time_series"]:
            return "query_time_series" not in llm_names
        if guardrail_names == ["list_assets", "list_data_sources"]:
            return not {"list_assets", "list_data_sources"}.issubset(llm_names)
        return False

    def _compose_answer(
        self,
        question: str,
        results: list[tuple[PlannedToolCall, dict[str, Any]]],
        grounding: list[ToolGrounding],
    ) -> AssistantQueryResponse:
        if not results:
            return AssistantQueryResponse(
                answer="No assistant tools were executed.",
                status="insufficient_data",
                grounding=grounding,
                clarificationNeeded="Please ask a question about an asset, data source, time series, or quality status.",
            )

        if [call.name for call, _ in results] == ["list_assets", "list_data_sources"]:
            assets = results[0][1].get("assetIds", [])
            sources = results[1][1].get("dataSourceIds", [])
            return AssistantQueryResponse(
                answer=(
                    "I need an assetId and dataSourceId before querying time-series data. "
                    f"Available assets include: {', '.join(assets) if assets else 'none'}. "
                    f"Available data sources include: {', '.join(sources) if sources else 'none'}."
                ),
                status="insufficient_data",
                grounding=grounding,
                clarificationNeeded="Which assetId and dataSourceId should I use?",
            )

        call, result = results[-1]
        if call.name == "query_time_series":
            count = int(result.get("count", 0))
            if count == 0:
                return AssistantQueryResponse(
                    answer=(
                        "No data found for "
                        f"assetId={call.arguments.get('asset_id')} and "
                        f"dataSourceId={call.arguments.get('data_source_id')}."
                    ),
                    status="insufficient_data",
                    grounding=grounding,
                )
            points = result.get("points", [])
            latest = points[-1] if points else {}
            close = (latest.get("point") or {}).get("close")
            timestamp = latest.get("timestamp")
            answer = (
                f"Found {count} time-series rows for {result.get('assetId')} from "
                f"{result.get('dataSourceId')}. Latest timestamp is {timestamp}."
            )
            if close is not None:
                answer += f" The latest close is {close}."
            return AssistantQueryResponse(answer=answer, status="grounded", grounding=grounding)

        if call.name in {"list_assets", "list_data_sources"}:
            key = "assetIds" if call.name == "list_assets" else "dataSourceIds"
            values = result.get(key, [])
            label = "assets" if key == "assetIds" else "data sources"
            return AssistantQueryResponse(
                answer=f"Found {len(values)} {label}: {', '.join(values) if values else 'none'}.",
                status="grounded",
                grounding=grounding,
            )

        if call.name == "get_asset":
            asset = result.get("asset")
            if not asset:
                return AssistantQueryResponse(
                    answer=f"No data found for assetId={call.arguments.get('asset_id')}.",
                    status="insufficient_data",
                    grounding=grounding,
                )
            return AssistantQueryResponse(
                answer=(
                    f"{asset.get('assetId')} is {asset.get('name')} "
                    f"with instrument class {asset.get('instrumentClass')}."
                ),
                status="grounded",
                grounding=grounding,
            )

        if call.name == "get_data_source":
            source = result.get("dataSource")
            if not source:
                return AssistantQueryResponse(
                    answer=f"No data found for dataSourceId={call.arguments.get('source_id')}.",
                    status="insufficient_data",
                    grounding=grounding,
                )
            return AssistantQueryResponse(
                answer=f"{source.get('sourceId')} is {source.get('name')}.",
                status="grounded",
                grounding=grounding,
            )

        if call.name == "get_quality_freshness":
            items = result.get("items", [])
            stale = [item for item in items if item.get("status") == "stale"]
            return AssistantQueryResponse(
                answer=f"Freshness check returned {len(items)} asset/source pairs; {len(stale)} are stale.",
                status="grounded",
                grounding=grounding,
            )

        if call.name == "get_quality_summary":
            latest_run = result.get("latestIngestionRun") or {}
            return AssistantQueryResponse(
                answer=(
                    f"Quality summary shows {result.get('totalTimeSeriesRows')} time-series rows. "
                    f"Duplicate risk status is {result.get('duplicateRisk', {}).get('status')}. "
                    f"Latest ingestion run status is {latest_run.get('status')}."
                ),
                status="grounded",
                grounding=grounding,
            )

        return AssistantQueryResponse(
            answer=f"I used read-only DWH tool {call.name} for the question: {question}",
            status="grounded",
            grounding=grounding,
        )

    def _grounding_for(self, call: PlannedToolCall, result: dict[str, Any]) -> ToolGrounding:
        summary = {key: value for key, value in result.items() if key != "provenance"}
        if "points" in summary:
            summary["points"] = {"count": len(result.get("points", []))}
        return ToolGrounding(
            toolName=call.name,
            arguments=call.arguments,
            provenance=result.get("provenance", {}),
            resultSummary=summary,
        )

    def _first_message(self, response: dict[str, Any]) -> dict[str, Any]:
        choices = response.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("LLM response missing choices.")
        message = choices[0].get("message")
        if not isinstance(message, dict):
            raise RuntimeError("LLM response missing message.")
        return message

    def _parse_tool_arguments(self, raw: Any) -> dict[str, Any]:
        if raw is None or raw == "":
            return {}
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        raise RuntimeError("LLM returned malformed tool arguments.")

    def _summarize_tool_result(self, result: dict[str, Any]) -> dict[str, Any]:
        summary = {key: value for key, value in result.items() if key != "provenance"}
        if "points" in summary:
            points = result.get("points", [])
            summary["points"] = points[:3]
            summary["pointsCount"] = len(points)
            if points:
                summary["latestPoint"] = points[-1]
        return summary

    def _extract_value(self, context: dict[str, Any], key: str) -> str | None:
        value = context.get(key)
        return str(value).strip() if value else None

    def _extract_symbol(self, question: str) -> str | None:
        matches = re.findall(r"\b[A-Z0-9]{2,15}\b", question)
        ignored = {"GET", "POST", "API", "DWH", "MCP", "USD", "UTC"}
        for match in matches:
            if match not in ignored:
                return match
        return None

    def _extract_source_id(self, question: str) -> str | None:
        match = re.search(r"\b[a-z][a-z0-9_]*_(?:api|dev|feed|reference|link)(?:_v\d+)?\b", question)
        return match.group(0) if match else None

    def _extract_dates(self, question: str, context: dict[str, Any]) -> tuple[str | None, str | None]:
        dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", question)
        start_date = context.get("startDate") or (dates[0] if dates else None)
        end_date = context.get("endDate") or (dates[1] if len(dates) > 1 else None)
        return (str(start_date) if start_date else None, str(end_date) if end_date else None)
