"""
Traces package for distributed tracing integrations.
"""

from .base import TraceExtractor, TraceExtractorError
from .tempo import GrafanaTempoExtractor
from .jaeger import JaegerExtractor

__all__ = [
    'TraceExtractor',
    'TraceExtractorError',
    'GrafanaTempoExtractor',
    'JaegerExtractor'
]
