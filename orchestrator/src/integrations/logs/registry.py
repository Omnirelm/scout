"""
Log extractor registry: register flavour -> factory, get extractor by config dict.
API and core use this single path to create log extractors.
"""
import logging
from typing import Any, Callable, Dict

from .base import LogExtractor
from ..flavours import LogSourceFlavour
from ..common.auth import build_headers_and_oauth_from_auth_dict
from .loki import GrafanaLokiExtractor
from .opensearch import OpenSearchExtractor
from .clickhouse import ClickHouseExtractor

logger = logging.getLogger(__name__)

# Type: (log_source_dict, tenant_id) -> LogExtractor
_LogFactory = Callable[[Dict[str, Any], str], LogExtractor]

_REGISTRY: Dict[str, _LogFactory] = {}


def register(log_flavour: str, factory: _LogFactory) -> None:
    """Register a log extractor factory for the given flavour (e.g. LOKI, OPENSEARCH)."""
    key = log_flavour.upper().strip()
    _REGISTRY[key] = factory


def get_log_extractor(log_source: Dict[str, Any], tenant_id: str) -> LogExtractor:
    """
    Create a LogExtractor from log_source config dict.

    Args:
        log_source: Dict with keys flavour and url.
                    indexPattern is OpenSearch-specific.
                    authMechanism and labels are optional.
                    labels are service-identifying key-value pairs used by the
                    log agent to scope queries; they are not used for extractor construction.
                    Same shape as API LogSource models dumped by alias.
        tenant_id: Tenant ID (used for Loki).

    Returns:
        LogExtractor instance.

    Raises:
        ValueError: If flavour is missing, url missing, or flavour not registered.
    """
    flavour = (log_source.get("flavour") or "").upper().strip()
    if not flavour:
        raise ValueError("log_source must contain 'flavour'")
    url = (log_source.get("url") or "").strip()
    if not url:
        raise ValueError("log_source must contain 'url'")

    factory = _REGISTRY.get(flavour)
    if not factory:
        raise ValueError(f"Unsupported log source flavour: {flavour}")

    return factory(log_source, tenant_id)


def _factory_opensearch(log_source: Dict[str, Any], tenant_id: str) -> LogExtractor:
    base_url = log_source.get("url", "").rstrip("/")
    index_pattern = (
        log_source.get("indexPattern")
        or log_source.get("index_pattern")
        or "logs-*"
    )
    auth = log_source.get("authMechanism") or log_source.get("auth_mechanism")
    result = build_headers_and_oauth_from_auth_dict(auth)
    return OpenSearchExtractor(
        base_url=base_url,
        index_pattern=index_pattern,
        headers=result.headers if result.headers else None,
        oauth_token_manager=result.oauth_token_manager,
    )


def _factory_loki(log_source: Dict[str, Any], tenant_id: str) -> LogExtractor:
    base_url = log_source.get("url", "").rstrip("/")
    auth = log_source.get("authMechanism") or log_source.get("auth_mechanism")
    result = build_headers_and_oauth_from_auth_dict(auth)
    return GrafanaLokiExtractor(
        base_url=base_url,
        tenant_id=tenant_id,
        headers=result.headers if result.headers else None,
        oauth_token_manager=result.oauth_token_manager,
    )


def _factory_clickhouse(log_source: Dict[str, Any], tenant_id: str) -> LogExtractor:
    base_url = log_source.get("url", "").rstrip("/")
    database = log_source.get("database") or "default"
    table = log_source.get("table") or "otel_logs"
    auth = log_source.get("authMechanism") or log_source.get("auth_mechanism")
    result = build_headers_and_oauth_from_auth_dict(auth)
    return ClickHouseExtractor(
        base_url=base_url,
        database=database,
        table=table,
        headers=result.headers if result.headers else None,
        oauth_token_manager=result.oauth_token_manager,
    )


# Register built-in extractors at module load
register(LogSourceFlavour.OPENSEARCH.value, _factory_opensearch)
register(LogSourceFlavour.LOKI.value, _factory_loki)
register(LogSourceFlavour.CLICKHOUSE.value, _factory_clickhouse)
