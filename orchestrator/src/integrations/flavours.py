"""
Source flavour enums for log and trace integrations.
Defined in the integration layer so API and core share the same set.
"""
from enum import Enum


class LogSourceFlavour(str, Enum):
    OPENSEARCH = "OPENSEARCH"
    LOKI = "LOKI"
    CLICKHOUSE = "CLICKHOUSE"


class TraceSourceFlavour(str, Enum):
    JAEGER = "JAEGER"
    TEMPO = "TEMPO"
