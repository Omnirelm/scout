"""
Trace extractor registry: register flavour -> factory, get extractor by config dict.
API and core use this single path to create trace extractors.
"""
import logging
from typing import Any, Callable, Dict, Optional

from .base import TraceExtractor
from ..flavours import TraceSourceFlavour
from ..common.auth import build_headers_and_oauth_from_auth_dict
from .jaeger import JaegerExtractor
from .tempo import GrafanaTempoExtractor

logger = logging.getLogger(__name__)

# Type: (trace_source_dict, tenant_id) -> TraceExtractor
_TraceFactory = Callable[[Dict[str, Any], str], TraceExtractor]

_REGISTRY: Dict[str, _TraceFactory] = {}


def register(trace_flavour: str, factory: _TraceFactory) -> None:
    """Register a trace extractor factory for the given flavour (e.g. JAEGER, TEMPO)."""
    key = trace_flavour.upper().strip()
    _REGISTRY[key] = factory


def get_trace_extractor(
    trace_source: Dict[str, Any], tenant_id: str
) -> TraceExtractor:
    """
    Create a TraceExtractor from trace_source config dict.

    Args:
        trace_source: Dict with keys flavour, url, authentication (optional).
                      Same shape as API TraceSource.model_dump(by_alias=True).
        tenant_id: Tenant ID for multi-tenant environments.

    Returns:
        TraceExtractor instance.

    Raises:
        ValueError: If flavour is missing, url missing, or flavour not registered.
    """
    flavour = (trace_source.get("flavour") or "").upper().strip()
    if not flavour:
        raise ValueError("trace_source must contain 'flavour'")
    url = (trace_source.get("url") or "").strip()
    if not url:
        raise ValueError("trace_source must contain 'url'")

    factory = _REGISTRY.get(flavour)
    if not factory:
        raise ValueError(f"Unsupported trace source flavour: {flavour}")

    return factory(trace_source, tenant_id)


def _factory_jaeger(trace_source: Dict[str, Any], tenant_id: str) -> TraceExtractor:
    base_url = trace_source.get("url", "").rstrip("/")
    auth = trace_source.get("authentication")
    result = build_headers_and_oauth_from_auth_dict(auth)
    # Trace extractors use headers only (OAuth not yet supported for traces)
    return JaegerExtractor(
        base_url=base_url,
        tenant_id=tenant_id,
        headers=result.headers or {},
    )


def _factory_tempo(trace_source: Dict[str, Any], tenant_id: str) -> TraceExtractor:
    base_url = trace_source.get("url", "").rstrip("/")
    auth = trace_source.get("authentication")
    result = build_headers_and_oauth_from_auth_dict(auth)
    return GrafanaTempoExtractor(
        base_url=base_url,
        tenant_id=tenant_id,
        headers=result.headers or {},
    )


# Register built-in extractors at module load
register(TraceSourceFlavour.JAEGER.value, _factory_jaeger)
register(TraceSourceFlavour.TEMPO.value, _factory_tempo)
