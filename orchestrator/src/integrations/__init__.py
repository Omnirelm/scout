"""
Integrations package: external datapoints and backends that agents connect to
(logs, traces, etc.).
"""

from .flavours import LogSourceFlavour, TraceSourceFlavour
from .logs import (
    LogExtractor,
    LogExtractorError,
    LogEntry,
    GrafanaLokiExtractor,
    LocalFileExtractor,
    OpenSearchExtractor,
)
from .traces import (
    TraceExtractor,
    TraceExtractorError,
    JaegerExtractor,
    GrafanaTempoExtractor,
)
from .traces.registry import get_trace_extractor
from .repository import validate_repository, parse_github_repo_url

__all__ = [
    # Flavours (shared with API)
    "LogSourceFlavour",
    "TraceSourceFlavour",
    # Logs
    "LogExtractor",
    "LogExtractorError",
    "LogEntry",
    "GrafanaLokiExtractor",
    "LocalFileExtractor",
    "OpenSearchExtractor",
    # Traces
    "TraceExtractor",
    "TraceExtractorError",
    "JaegerExtractor",
    "GrafanaTempoExtractor",
    "get_trace_extractor",
    # Repository (GitHub validation)
    "validate_repository",
    "parse_github_repo_url",
]
