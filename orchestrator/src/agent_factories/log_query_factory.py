from agents import Agent
from pydantic import BaseModel, Field

from src.core.tools import ToolRegistry
from src.integrations.logs.clickhouse import ClickHouseExtractor
from src.integrations.logs.loki import GrafanaLokiExtractor
from src.integrations.logs.opensearch import OpenSearchExtractor
from src.integrations.logs.tools import (
    ClickHouseCleanQueryStringTool,
    ClickHouseGetColumnNamesTool,
    ClickHouseGetTableNameTool,
    ClickHouseValidateQueryTool,
    GetLabelNamesTool,
    LokiCleanQueryStringTool,
    LokiValidateQueryTool,
    OpenSearchCleanQueryStringTool,
    OpenSearchGetFieldNamesTool,
    OpenSearchGetIndexNameTool,
    OpenSearchValidateQueryTool,
)

from .instructions import get_agent_instructions, get_agent_model, get_agent_name


class QueryGeneratorAgentOutput(BaseModel):
    query: str = Field(..., description="The query to fetch logs")
    language: str = Field(..., description="The language of the query")
    error: str = Field(..., description="The error of the query generation process")


class AgentFactory(BaseModel):
    def __init__(self):
        pass

    def make_logql_generator_agent(self, loki_extractor: GrafanaLokiExtractor) -> Agent:
        """
        Create an agent for generating LogQL queries.

        Args:
            loki_extractor: Loki extractor instance
        """
        agent_key = "logql_query_generator"
        instructions = get_agent_instructions(agent_key)
        agent_name = get_agent_name(agent_key)
        model = get_agent_model(agent_key)

        registry = (
            ToolRegistry()
            .register(GetLabelNamesTool(loki_extractor))
            .register(LokiValidateQueryTool(loki_extractor))
            .register(LokiCleanQueryStringTool(loki_extractor))
        )
        tools = registry.get_function_tools(
            ["loki_get_label_names", "loki_validate_query", "loki_clean_query_string"]
        )

        return Agent(
            name=agent_name,
            model=model,
            instructions=instructions,
            tools=tools,
            output_type=QueryGeneratorAgentOutput,
        )

    def make_ppl_generator_agent(self, opensearch_extractor: OpenSearchExtractor) -> Agent:
        """
        Create an agent for generating PPL queries.

        Args:
            opensearch_extractor: PPL extractor instance
        """
        agent_key = "ppl_query_generator"
        agent_name = get_agent_name(agent_key)
        instructions = get_agent_instructions(agent_key)
        model = get_agent_model(agent_key)

        registry = (
            ToolRegistry()
            .register(OpenSearchGetFieldNamesTool(opensearch_extractor))
            .register(OpenSearchValidateQueryTool(opensearch_extractor))
            .register(OpenSearchCleanQueryStringTool(opensearch_extractor))
            .register(OpenSearchGetIndexNameTool(opensearch_extractor))
        )
        tools = registry.get_function_tools(
            ["opensearch_get_field_names", "opensearch_validate_query", "opensearch_clean_query_string", "opensearch_get_index_name"]
        )

        return Agent(
            name=agent_name,
            model=model,
            instructions=instructions,
            tools=tools,
            output_type=QueryGeneratorAgentOutput,
        )

    def make_sql_generator_agent(self, clickhouse_extractor: ClickHouseExtractor) -> Agent:
        """
        Create an agent for generating ClickHouse SQL queries against a ClickHouse log table.

        Args:
            clickhouse_extractor: ClickHouse extractor instance
        """
        agent_key = "sql_query_generator"
        agent_name = get_agent_name(agent_key)
        instructions = get_agent_instructions(agent_key)
        model = get_agent_model(agent_key)

        registry = (
            ToolRegistry()
            .register(ClickHouseGetTableNameTool(clickhouse_extractor))
            .register(ClickHouseGetColumnNamesTool(clickhouse_extractor))
            .register(ClickHouseValidateQueryTool(clickhouse_extractor))
            .register(ClickHouseCleanQueryStringTool(clickhouse_extractor))
        )
        tools = registry.get_function_tools(
            ["clickhouse_get_table_name", "clickhouse_get_column_names", "clickhouse_validate_query", "clickhouse_clean_query_string"]
        )

        return Agent(
            name=agent_name,
            model=model,
            instructions=instructions,
            tools=tools,
            output_type=QueryGeneratorAgentOutput,
        )
