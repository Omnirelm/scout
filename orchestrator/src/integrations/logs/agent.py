import datetime
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from agents import Agent, Runner, RunResult

from src.agent_factories import AgentFactory, QueryGeneratorAgentOutput

from .base import LogExtractor, LogExtractorError, LogEntry, QueryGenerationError
from .clickhouse import ClickHouseExtractor
from .loki import GrafanaLokiExtractor
from .opensearch import OpenSearchExtractor
from ...core.base import InvocationCost, extract_runner_cost

logger = logging.getLogger(__name__)


def get_agent_for_extractor(log_extractor: LogExtractor) -> Agent:
    """
    Return the proper agent for the provided log_extractor instance.

    Args:
        log_extractor: Log extractor instance

    Returns:
        Agent instance

    Raises:
        QueryGenerationError: If agent setup fails (unsupported aggregator or wrong extractor type)
    """
    factory = AgentFactory()
    agg_name = log_extractor.get_log_aggregator_name()

    if agg_name == "OPENSEARCH":
        if not isinstance(log_extractor, OpenSearchExtractor):
            raise QueryGenerationError(
                f"Agent setup failed: Expected OpenSearchExtractor, got {type(log_extractor)}"
            )
        return factory.make_ppl_generator_agent(opensearch_extractor=log_extractor)
    if agg_name == "LOKI":
        if not isinstance(log_extractor, GrafanaLokiExtractor):
            raise QueryGenerationError(
                f"Agent setup failed: Expected GrafanaLokiExtractor, got {type(log_extractor)}"
            )
        return factory.make_logql_generator_agent(loki_extractor=log_extractor)
    if agg_name == "CLICKHOUSE":
        if not isinstance(log_extractor, ClickHouseExtractor):
            raise QueryGenerationError(
                f"Agent setup failed: Expected ClickHouseExtractor, got {type(log_extractor)}"
            )
        return factory.make_sql_generator_agent(clickhouse_extractor=log_extractor)
    raise QueryGenerationError(f"Agent setup failed: Unsupported log aggregator: {agg_name}")


async def fetch_logs_with_llm(
    log_extractor: LogExtractor,
    labels: Optional[Dict[str, Any]],
    start: datetime.datetime,
    end: datetime.datetime,
    limit: int = 100,
    retries: int = 1,
    examples: Optional[Any] = None,
) -> Tuple[List[LogEntry], InvocationCost]:
    """
    Fetch logs with LLM using an agent to generate queries.
    Always fetches all logs regardless of level.

    Args:
        log_extractor: Log extractor instance
        labels: Service-identifying labels from logSource (e.g. name, namespace, service) used to scope log queries
        start: Start time for query
        end: End time for query
        limit: Maximum number of entries to return
        retries: Number of retry attempts if query generation or execution fails

    Returns:
        Tuple of (log entries, InvocationCost with per-attempt children)

    Raises:
        LogExtractorError: If all retry attempts fail
        QueryGenerationError: If agent setup fails (unsupported aggregator or wrong extractor type)
    """
    previous_query: Optional[str] = None
    previous_query_error: Optional[str] = None
    logs: List[LogEntry] = []
    output = None
    attempt_costs: List[InvocationCost] = []

    base_input: Dict[str, Any] = {
        "labels": dict(labels or {}),
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "limit": limit,
    }
    if examples is not None:
        base_input["examples"] = examples
    try:
        log_extractor.precheck()
        agent = get_agent_for_extractor(log_extractor)
    except QueryGenerationError:
        raise

    for attempt in range(1, retries + 1):
        input_json = {
            **base_input,
            "previous_query": previous_query,
            "previous_query_error": previous_query_error,
            "retry_attempt": attempt,
            "max_retries": retries,
        }
        json_input = json.dumps(input_json)

        try:
            result: RunResult = await Runner.run(
                starting_agent=agent,
                input=json_input,
            )

            attempt_cost = extract_runner_cost(result, f"log_query_generator_attempt_{attempt}")
            attempt_costs.append(attempt_cost)

            output = result.final_output_as(
                QueryGeneratorAgentOutput,
                True,
            )

            logger.info(
                "Generated query (attempt %d/%d) language=%s query=%s tokens=%d error=%s",
                attempt,
                retries,
                output.language,
                output.query,
                attempt_cost.total_tokens,
                output.error,
            )

            logs = log_extractor.fetch_logs(
                query=output.query,
                start=start,
                end=end,
                limit=limit,
            )
            logger.info("Fetched %d logs with query: %s", len(logs), output.query)
            cost = InvocationCost(label="log_query_generator", children=attempt_costs)
            return logs, cost

        except (TypeError, ValueError) as e:
            error_msg = f"Agent output parsing failed (attempt {attempt}/{retries}): {str(e)}"
            logger.warning(error_msg)
            previous_query_error = error_msg
            if attempt == retries:
                raise QueryGenerationError(
                    f"Failed to generate query after {retries} retries: Agent output parsing error - {str(e)}"
                ) from e
            continue

        except LogExtractorError as e:
            previous_query = output.query if output and hasattr(output, "query") else None
            previous_query_error = f"Query execution failed (attempt {attempt}/{retries}): {str(e)}"
            logger.warning(previous_query_error)
            if attempt == retries:
                raise
            continue

        except Exception as e:
            previous_query = output.query if output and hasattr(output, "query") else None
            previous_query_error = f"Unexpected error (attempt {attempt}/{retries}): {str(e)}"
            logger.error(previous_query_error, exc_info=True)
            if attempt == retries:
                raise QueryGenerationError(
                    f"Failed to generate query after {retries} retries: {str(e)}"
                ) from e
            continue

    cost = InvocationCost(label="log_query_generator", children=attempt_costs)
    return logs, cost
