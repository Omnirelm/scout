"""
Logs package for log extraction integrations.
"""

from .base import (
    LogExtractor,
    LogExtractorError,
    LogEntry,
    DedupedLogEntry,
    DedupedLogsResult,
    LogDedupeError,
    OAuthConfig,
    OAuthTokenManager,
    QueryGenerationError,
)
from .dedupe import de_dupe_logs
from .loki import GrafanaLokiExtractor
from .local_file import LocalFileExtractor
from .opensearch import OpenSearchExtractor
from .clickhouse import ClickHouseExtractor

__all__ = [
    'LogExtractor',
    'LogExtractorError',
    'LogEntry',
    'DedupedLogEntry',
    'DedupedLogsResult',
    'LogDedupeError',
    'OAuthConfig',
    'OAuthTokenManager',
    'QueryGenerationError',
    'de_dupe_logs',
    'GrafanaLokiExtractor',
    'LocalFileExtractor',
    'OpenSearchExtractor',
    'ClickHouseExtractor',
]
