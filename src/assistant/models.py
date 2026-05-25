from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

AssistantStatus = Literal["grounded", "insufficient_data", "error"]


class AssistantQueryRequest(BaseModel):
    question: str = Field(..., max_length=1000)
    context: dict[str, Any] | None = None


class ToolGrounding(BaseModel):
    toolName: str
    arguments: dict[str, Any]
    provenance: dict[str, Any]
    resultSummary: dict[str, Any]


class AssistantQueryResponse(BaseModel):
    answer: str
    status: AssistantStatus
    grounding: list[ToolGrounding] = Field(default_factory=list)
    clarificationNeeded: str | None = None
