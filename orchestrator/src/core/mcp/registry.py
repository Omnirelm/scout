"""MCP server registry: register by name and build SDK server instances."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from agents.mcp import MCPServerSse, MCPServerStdio, MCPServerStreamableHttp
from agents.mcp.server import (
    MCPServerSseParams,
    MCPServerStdioParams,
    MCPServerStreamableHttpParams,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Literal

logger = logging.getLogger(__name__)


class McpServerConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str | None = None
    enabled: bool = False
    type: Literal["stdio", "sse", "streamable_http"]
    command: str | None = None
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    url: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float | None = None
    sse_read_timeout: float | None = None
    cache_tools_list: bool = False

    @model_validator(mode="after")
    def _transport_fields(self) -> "McpServerConfig":
        if not self.enabled:
            return self
        if self.type == "stdio" and not (self.command or "").strip():
            raise ValueError("MCP server type stdio requires non-empty command")
        if self.type in ("sse", "streamable_http") and not (self.url or "").strip():
            raise ValueError(f"MCP server type {self.type} requires non-empty url")
        return self


class McpConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    servers: dict[str, McpServerConfig] = Field(default_factory=dict)




class McpServerRegistry:
    """Stores enabled MCP server configs and builds runtime server instances."""

    def __init__(self, configs: Mapping[str, McpServerConfig] | None = None) -> None:
        self._registry: dict[str, McpServerConfig] = {}
        if configs:
            self.register_many(configs)

    def register(self, config: McpServerConfig, *, name: str | None = None) -> None:
        """Register or replace a config by normalized name."""
        normalized_name = (name or config.name or "").strip()
        if name and (config.name or "").strip() != normalized_name:
            config = config.model_copy(update={"name": normalized_name})
        name = normalized_name
        if not name:
            logger.warning("Skipping MCP server registration with empty name")
            return
        self._registry[name] = config

    def register_many(
        self,
        configs: Mapping[str, McpServerConfig],
        *,
        only_enabled: bool = True,
    ) -> None:
        """Register MCP configs from a mapping keyed by server name."""
        for name, config in configs.items():
            if only_enabled and not config.enabled:
                continue
            self.register(config, name=name)

    def get(self, name: str) -> McpServerConfig | None:
        return self._registry.get(name)

    def names(self) -> list[str]:
        return sorted(self._registry.keys())

    def resolve(self, names: Sequence[str]) -> list[McpServerConfig]:
        """Resolve declared names to known configs and warn on missing names."""
        resolved: list[McpServerConfig] = []
        for name in names:
            config = self.get(name)
            if config is None:
                logger.warning(
                    "Skill declared MCP server %r but it is not enabled or configured; skipping",
                    name,
                )
                continue
            resolved.append(config)
        return resolved

    def build_server(self, config: McpServerConfig) -> Any:
        """Build one OpenAI Agents SDK MCP server instance from config."""
        if config.type == "stdio":
            params: MCPServerStdioParams = {"command": config.command or ""}
            if config.args:
                params["args"] = list(config.args)
            if config.env:
                env = {k: v for k, v in config.env.items() if v}
                if env:
                    params["env"] = env
            return MCPServerStdio(
                params,
                name=config.name,
                cache_tools_list=config.cache_tools_list,
            )

        if config.type == "streamable_http":
            params_h: MCPServerStreamableHttpParams = {"url": config.url or ""}
            if config.headers:
                params_h["headers"] = dict(config.headers)
            if config.timeout is not None:
                params_h["timeout"] = config.timeout
            if config.sse_read_timeout is not None:
                params_h["sse_read_timeout"] = config.sse_read_timeout
            return MCPServerStreamableHttp(
                params_h,
                name=config.name,
                cache_tools_list=config.cache_tools_list,
            )

        params_s: MCPServerSseParams = {"url": config.url or ""}
        if config.headers:
            params_s["headers"] = dict(config.headers)
        return MCPServerSse(
            params_s,
            name=config.name,
            cache_tools_list=config.cache_tools_list,
        )

    def build_servers(self, names: Sequence[str]) -> list[Any]:
        return [self.build_server(config) for config in self.resolve(names)]
