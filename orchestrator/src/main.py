from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path

from fastapi import FastAPI

from src.api.router import api_router
from src.config.settings import get_settings
from src.core.skills import SkillRegistry, SkillRunner
from src.core.tools.base import ToolRegistry
from src.integrations.http.tool import HttpTool
from src.integrations.logs import (
    ClickHouseExtractor,
    GrafanaLokiExtractor,
    OpenSearchExtractor,
)
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
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    if settings.openai_api_key and not os.getenv("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = settings.openai_api_key

    loki_extractor = GrafanaLokiExtractor(
        base_url="http://localhost:3100",
        tenant_id="1",
        headers={
            "Consumer-Key": "1234567890",
        }
    )
    opensearch_extractor = OpenSearchExtractor(
        base_url="http://localhost:9200",
        index_pattern="otel-logs-*",
    )
    clickhouse_extractor = ClickHouseExtractor(
        base_url="http://localhost:8123",
        database="default",
        table="otel_logs",
    )
    
    tool_registry = ToolRegistry()
    tool_registry.register(HttpTool())
    tool_registry.register(GetLabelNamesTool(loki_extractor))
    tool_registry.register(GetLabelValuesTool(loki_extractor))
    tool_registry.register(LokiCleanQueryStringTool(loki_extractor))
    tool_registry.register(LokiFetchLogsTool(loki_extractor))
    tool_registry.register(LokiValidateQueryTool(loki_extractor))
    tool_registry.register(OpenSearchCleanQueryStringTool(opensearch_extractor))
    tool_registry.register(OpenSearchFetchLogsTool(opensearch_extractor))
    tool_registry.register(OpenSearchGetFieldNamesTool(opensearch_extractor))
    tool_registry.register(OpenSearchGetIndexNameTool(opensearch_extractor))
    tool_registry.register(OpenSearchValidateQueryTool(opensearch_extractor))
    tool_registry.register(ClickHouseCleanQueryStringTool(clickhouse_extractor))
    tool_registry.register(ClickHouseFetchLogsTool(clickhouse_extractor))
    tool_registry.register(ClickHouseGetColumnNamesTool(clickhouse_extractor))
    tool_registry.register(ClickHouseGetTableNameTool(clickhouse_extractor))
    tool_registry.register(ClickHouseValidateQueryTool(clickhouse_extractor))
    skill_registry = SkillRegistry(
        skills_root=Path(__file__).resolve().parents[1] / "skills"
    )
    app.state.tool_registry = tool_registry
    app.state.skill_registry = skill_registry
    app.state.skill_runner = SkillRunner(skill_registry, tool_registry)
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.include_router(api_router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )


if __name__ == "__main__":
    run()
