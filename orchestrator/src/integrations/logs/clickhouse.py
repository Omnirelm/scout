"""
ClickHouse integration for log extraction.

Connects to the ClickHouse HTTP interface (default port 8123) and runs SQL queries
against the otel_logs table populated by the ClickHouse OTLP collector.
"""
from datetime import datetime, timezone
import logging
from typing import Dict, Any, List, Optional

from .base import LogExtractor, LogExtractorError, LogEntry, OAuthTokenManager, QueryGenerationError
from .parsers import detect_format, parse_json, parse_logfmt, extract_trace_id_span_id

logger = logging.getLogger(__name__)

# Default columns in the ClickHouse otel_logs table (OTLP schema).
# Used as fallback when DESCRIBE TABLE is unavailable.
OTEL_LOG_COLUMNS = [
    "Timestamp",
    "ServiceName",
    "SeverityText",
    "SeverityNumber",
    "Body",
    "TraceId",
    "SpanId",
    "ResourceAttributes",
    "LogAttributes",
    "ScopeName",
    "ScopeVersion",
    "ScopeSchemaUrl",
    "ResourceSchemaUrl",
]


class ClickHouseExtractor(LogExtractor):
    """
    ClickHouse log extractor implementation.

    Queries logs from ClickHouse via its HTTP interface using SQL.
    Designed for the default ClickHouse OTLP schema (otel_logs table), but configurable
    for any ClickHouse database and table.
    """

    def __init__(
        self,
        base_url: str,
        database: str = "default",
        table: str = "otel_logs",
        headers: Optional[Dict[str, str]] = None,
        oauth_token_manager: Optional[OAuthTokenManager] = None,
    ):
        """
        Initialize the ClickHouse extractor.

        Args:
            base_url: ClickHouse HTTP base URL (e.g. 'http://localhost:8123')
            database: ClickHouse database name (default: 'default')
            table: ClickHouse log table name (default: 'otel_logs')
            headers: Optional HTTP headers (e.g. Authorization for basic/bearer auth)
            oauth_token_manager: Optional OAuth token manager for OAuth authentication
        """
        self.database = database
        self.table = table

        merged_headers = {"Content-Type": "text/plain; charset=UTF-8"}
        if headers:
            merged_headers.update(headers)

        super().__init__(base_url, merged_headers, oauth_token_manager=oauth_token_manager)

    # -------------------------------------------------------------------------
    # LogExtractor ABC
    # -------------------------------------------------------------------------

    def get_log_aggregator_name(self) -> str:
        return "CLICKHOUSE"

    def get_query_language(self) -> str:
        return "SQL"

    def get_index_name(self) -> str:
        """Return the fully-qualified table name used as the query target."""
        return f"{self.database}.{self.table}"

    def get_label_names(self) -> List[str]:
        """Not applicable for ClickHouse (uses SQL columns, not label streams)."""
        return []

    def get_field_names(self, index: Optional[str] = None) -> List[str]:
        """
        Get column names from the ClickHouse table.

        Args:
            index: Optional fully-qualified table name (e.g. 'default.otel_logs').
                   Defaults to the configured database.table.

        Returns:
            List of column names.
        """
        target = index or self.get_index_name()
        try:
            rows = self._run_sql(f"DESCRIBE TABLE {target}")
            columns = [row.get("name", "") for row in rows if row.get("name")]
            return columns if columns else OTEL_LOG_COLUMNS
        except Exception as e:
            logger.warning("Failed to DESCRIBE TABLE %s, using default columns: %s", target, e)
            return OTEL_LOG_COLUMNS

    def validate_connection(self, timeout: float = 10.0) -> None:
        """Minimal connectivity check: run SELECT 1."""
        self._run_sql("SELECT 1", timeout=timeout)

    def precheck(self) -> bool:
        """
        Verify the table is reachable and has queryable columns.

        Raises:
            QueryGenerationError: If no columns are discoverable.
        """
        columns = self.get_field_names()
        if not columns:
            raise QueryGenerationError("Could not generate query: no columns found in ClickHouse table")
        return True

    def fetch_logs(
        self,
        query: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 100,
        **kwargs,
    ) -> List[LogEntry]:
        """
        Fetch logs from ClickHouse using a SQL query.

        The agent-generated SQL query should be a complete SELECT statement
        that includes time range and LIMIT. The start/end/limit parameters are
        provided for context but the agent embeds them directly in the SQL.

        Args:
            query: SQL SELECT statement (complete; time range and LIMIT embedded by agent)
            start: Start time (informational; agent embeds in SQL WHERE clause)
            end: End time (informational; agent embeds in SQL WHERE clause)
            limit: Max rows (informational; agent embeds in SQL LIMIT clause)

        Returns:
            List of LogEntry instances.
        """
        rows = self._run_sql(query)
        return [self._parse_log_entry(row) for row in rows]

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _run_sql(self, sql: str, timeout: float = 30.0) -> List[Dict[str, Any]]:
        """
        Execute a SQL statement against the ClickHouse HTTP interface.

        Appends 'FORMAT JSON' to the query if no FORMAT clause is present so the
        response contains a 'data' list of row objects.

        Args:
            sql: SQL statement to execute.
            timeout: Request timeout in seconds.

        Returns:
            List of row dicts from the ClickHouse JSON response.

        Raises:
            LogExtractorError: If the HTTP request fails or the response cannot be parsed.
        """
        query = sql.strip().rstrip(";")
        if "FORMAT" not in query.upper():
            query = query + " FORMAT JSON"

        url = f"{self.base_url}/"
        response = self._make_request(
            method="POST",
            url=url,
            headers=self.headers,
            data=query.encode("utf-8"),
            timeout=timeout,
        )
        try:
            result = response.json()
            return result.get("data", [])
        except Exception as e:
            raise LogExtractorError(f"Failed to parse ClickHouse response: {e}") from e

    def _parse_log_entry(self, row: Dict[str, Any]) -> LogEntry:
        """
        Parse a ClickHouse row dict into a standardized LogEntry.

        Maps ClickHouse OTLP column names to LogEntry fields:
        - Timestamp / timestamp     -> timestamp
        - Body / body               -> message
        - SeverityText / severity_text -> level
        - TraceId / trace_id        -> traceId
        - SpanId / span_id          -> spanId
        - All row fields            -> labels
        """
        # Timestamp
        timestamp_raw = row.get("Timestamp") or row.get("timestamp")
        timestamp_iso: Optional[str] = None
        if isinstance(timestamp_raw, str):
            timestamp_iso = timestamp_raw
        elif isinstance(timestamp_raw, (int, float)):
            try:
                ts = datetime.fromtimestamp(timestamp_raw, tz=timezone.utc)
                timestamp_iso = ts.isoformat()
            except Exception:
                timestamp_iso = str(timestamp_raw)

        # Log level
        level_raw = row.get("SeverityText") or row.get("severity_text")
        level = str(level_raw).upper() if level_raw else None

        # Message body
        message = str(row.get("Body") or row.get("body") or "")

        # TraceId / SpanId from dedicated columns first
        trace_id: Optional[str] = row.get("TraceId") or row.get("trace_id") or None
        span_id: Optional[str] = row.get("SpanId") or row.get("span_id") or None

        # Try to extract from structured message if not found in columns
        if (trace_id is None or span_id is None) and message:
            format_type = detect_format(message)
            if format_type == "json":
                parsed = parse_json(message)
                if parsed:
                    t, s = extract_trace_id_span_id(parsed)
                    trace_id = trace_id or t
                    span_id = span_id or s
            elif format_type == "logfmt":
                parsed = parse_logfmt(message)
                if parsed:
                    t, s = extract_trace_id_span_id(parsed)
                    trace_id = trace_id or t
                    span_id = span_id or s

        # Carry all row fields as labels/metadata
        labels = {k: v for k, v in row.items()}

        return LogEntry(
            timestamp=timestamp_iso,
            message=message,
            level=level,
            labels=labels,
            traceId=trace_id,
            spanId=span_id,
        )
