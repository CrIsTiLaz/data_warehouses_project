from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx


class MCPUnavailableError(RuntimeError):
    """Raised when MCP-backed assistant execution is disabled or unavailable."""


@dataclass(frozen=True)
class MCPConfig:
    enabled: bool
    model: str
    timeout_seconds: float
    endpoint: str | None = None
    provider_api_key: str | None = None

    @classmethod
    def from_env(cls) -> "MCPConfig":
        enabled_raw = os.getenv("ASSISTANT_MCP_ENABLED", "false").strip().lower()
        timeout_raw = os.getenv("ASSISTANT_MCP_TIMEOUT_SECONDS", "10").strip()
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError:
            timeout_seconds = 10.0
        return cls(
            enabled=enabled_raw in {"1", "true", "yes", "on"},
            model=os.getenv("ASSISTANT_MODEL", "grounded-dwh-assistant").strip() or "grounded-dwh-assistant",
            timeout_seconds=timeout_seconds,
            endpoint=(os.getenv("ASSISTANT_MCP_ENDPOINT") or "").strip() or None,
            provider_api_key=(os.getenv("ASSISTANT_PROVIDER_API_KEY") or "").strip() or None,
        )


@dataclass(frozen=True)
class LLMConfig:
    api_key: str | None
    model: str | None
    base_url: str
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> "LLMConfig":
        timeout_raw = os.getenv("LLM_TIMEOUT_SECONDS", "15").strip()
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError:
            timeout_seconds = 15.0
        return cls(
            api_key=(os.getenv("LLM_API_KEY") or "").strip() or None,
            model=(os.getenv("LLM_MODEL") or "").strip() or None,
            base_url=(os.getenv("LLM_BASE_URL") or "https://openrouter.ai/api/v1").strip(),
            timeout_seconds=timeout_seconds,
        )

    def wants_llm(self) -> bool:
        return bool(self.api_key or self.model)

    def is_ready(self) -> bool:
        return bool(self.api_key and self.model)

    def validate(self) -> None:
        if not self.wants_llm():
            return
        missing: list[str] = []
        if not self.api_key:
            missing.append("LLM_API_KEY")
        if not self.model:
            missing.append("LLM_MODEL")
        if missing:
            joined = ", ".join(missing)
            raise MCPUnavailableError(
                f"OpenRouter LLM mode is partially configured. Missing: {joined}. "
                "Set both LLM_API_KEY and LLM_MODEL, or unset both to use fallback planning."
            )
        if self.timeout_seconds <= 0:
            raise MCPUnavailableError("LLM_TIMEOUT_SECONDS must be greater than zero.")


class MCPAdapter:
    def list_tools(self) -> list[str]:
        raise NotImplementedError

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def tool_specs(self) -> list[dict[str, Any]]:
        return []


class LocalMCPAdapter(MCPAdapter):
    """
    In-process MCP adapter used by the API.

    The adapter keeps MCP behind an interface while making this prototype runnable
    without hardcoding a specific desktop/client transport.
    """

    def __init__(
        self,
        tools: dict[str, Callable[..., dict[str, Any]]],
        config: MCPConfig,
        tool_specs: list[dict[str, Any]] | None = None,
    ) -> None:
        self.tools = tools
        self.config = config
        self._tool_specs = tool_specs or []

    def _ensure_available(self) -> None:
        if not self.config.enabled:
            raise MCPUnavailableError(
                "Assistant MCP is disabled. Set ASSISTANT_MCP_ENABLED=true to enable read-only assistant tools."
            )
        if self.config.timeout_seconds <= 0:
            raise MCPUnavailableError("Assistant MCP timeout must be greater than zero.")

    def list_tools(self) -> list[str]:
        self._ensure_available()
        return sorted(self.tools)

    def tool_specs(self) -> list[dict[str, Any]]:
        self._ensure_available()
        return self._tool_specs

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_available()
        tool = self.tools.get(tool_name)
        if tool is None:
            raise MCPUnavailableError(f"Unknown assistant tool: {tool_name}")
        try:
            return tool(**arguments)
        except TypeError as exc:
            raise MCPUnavailableError(f"Invalid arguments for assistant tool {tool_name}: {exc}") from exc


class OpenRouterClient:
    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        self.config.validate()

    def is_configured(self) -> bool:
        return self.config.is_ready()

    def chat_completion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.config.validate()
        if not self.config.is_ready():
            raise MCPUnavailableError("OpenRouter LLM is not configured.")
        url = self.config.base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",
            "X-Title": "Acme Financial DWH Assistant",
        }
        try:
            response = httpx.post(
                url,
                headers=headers,
                json={**payload, "model": self.config.model},
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError("OpenRouter chat completion failed.") from exc
        if not isinstance(data, dict):
            raise RuntimeError("OpenRouter returned a malformed response.")
        return data
