"""Application wiring: logging, tools, skills, and app.state (invoked once at startup)."""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI

from src.config.settings import get_config
from src.core.skills import SkillRegistry, SkillRunner
from src.core.tools.base import ToolRegistry
from src.integrations.http.tool import HttpTool
from src.integrations.logs.base import LogExtractor
from src.integrations.logs.clickhouse import ClickHouseExtractor
from src.integrations.logs.loki import GrafanaLokiExtractor
from src.integrations.logs.opensearch import OpenSearchExtractor
from src.integrations.logs.registry import get_log_extractor
from src.core.mcp import McpServerRegistry
from src.integrations.logs.tools import (
    ClickHouseCleanQueryStringTool,
    ClickHouseFetchLogsTool,
    ClickHouseGetColumnNamesTool,
    ClickHouseGetTableNameTool,
    ClickHouseValidateQueryTool,
    GetLabelNamesTool,
    GetLabelValuesTool,
    LokiCleanQueryStringTool,
    LokiFetchLogsTool,
    LokiValidateQueryTool,
    OpenSearchCleanQueryStringTool,
    OpenSearchFetchLogsTool,
    OpenSearchGetFieldNamesTool,
    OpenSearchGetIndexNameTool,
    OpenSearchValidateQueryTool,
)

logger = logging.getLogger(__name__)


def _register_log_tools_for_extractor(
    tool_registry: ToolRegistry, extractor: LogExtractor
) -> None:
    if isinstance(extractor, GrafanaLokiExtractor):
        tool_registry.register(GetLabelNamesTool(extractor))
        tool_registry.register(GetLabelValuesTool(extractor))
        tool_registry.register(LokiCleanQueryStringTool(extractor))
        tool_registry.register(LokiFetchLogsTool(extractor))
        tool_registry.register(LokiValidateQueryTool(extractor))
    elif isinstance(extractor, OpenSearchExtractor):
        tool_registry.register(OpenSearchCleanQueryStringTool(extractor))
        tool_registry.register(OpenSearchFetchLogsTool(extractor))
        tool_registry.register(OpenSearchGetFieldNamesTool(extractor))
        tool_registry.register(OpenSearchGetIndexNameTool(extractor))
        tool_registry.register(OpenSearchValidateQueryTool(extractor))
    elif isinstance(extractor, ClickHouseExtractor):
        tool_registry.register(ClickHouseCleanQueryStringTool(extractor))
        tool_registry.register(ClickHouseFetchLogsTool(extractor))
        tool_registry.register(ClickHouseGetColumnNamesTool(extractor))
        tool_registry.register(ClickHouseGetTableNameTool(extractor))
        tool_registry.register(ClickHouseValidateQueryTool(extractor))
    else:
        logger.warning("Unsupported log extractor type: %s", type(extractor).__name__)


def wire_application(app: FastAPI) -> None:
    """
    Build registries and attach them to app.state.

    Called once from ASGI lifespan startup (before traffic). Uses cached get_config().
    """
    config = get_config()
    logging.basicConfig(
        level=config.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

    tool_registry = ToolRegistry()
    tool_registry.register(HttpTool())

    for source_name, source in config.tools.logging.items():
        if not source.enabled:
            continue
        try:
            source_for_registry = source
            if not (source_for_registry.name or "").strip():
                source_for_registry = source.model_copy(update={"name": source_name})
            extractor = get_log_extractor(
                source_for_registry.to_log_source_spec(), default_tenant_id="default"
            )
            _register_log_tools_for_extractor(tool_registry, extractor)
            logger.info(
                "Registered log tools for source %s (%s)",
                source_for_registry.name,
                source_for_registry.flavour,
            )
        except Exception:
            logger.exception(
                "Skipping log source %s (%s): initialization failed",
                source_name,
                source.flavour,
            )

    mcp_registry = McpServerRegistry(config.tools.mcp)
    for name in mcp_registry.names():
        cfg = mcp_registry.get(name)
        if cfg is None:
            continue
        logger.info("Registered MCP server: %s (%s)", cfg.name, cfg.type)

    orchestrator_root = Path(__file__).resolve().parent.parent
    skill_registry = SkillRegistry(skills_root=orchestrator_root / "skills")

    app.state.tool_registry = tool_registry
    app.state.skill_registry = skill_registry
    app.state.skill_runner = SkillRunner(
        skill_registry, tool_registry, mcp_registry=mcp_registry
    )
