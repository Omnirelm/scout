"""YAML integration config: logging sources and MCP servers."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

from src.core.mcp import McpConfig
from src.config.settings import get_settings
from src.integrations.logs import LoggingConfig

import yaml
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class IntegrationsConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    mcp: McpConfig = Field(default_factory=McpConfig)


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    integrations: IntegrationsConfig = Field(default_factory=IntegrationsConfig)


def load_app_config(path: str | Path) -> AppConfig:
    """Load and validate orchestrator config YAML. Missing file yields empty defaults."""
    p = Path(path)
    if not p.is_file():
        logger.warning("Integration config not found at %s; using defaults", p)
        return AppConfig()
    with p.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        return AppConfig()
    return AppConfig.model_validate(raw)


def _resolved_integration_config_path() -> Path:
    """Orchestrator project root / relative path from settings."""
    settings = get_settings()
    cfg_path = Path(settings.config_file)
    if not cfg_path.is_absolute():
        root = Path(__file__).resolve().parent.parent.parent
        cfg_path = root / cfg_path
    return cfg_path


@lru_cache
def get_app_config() -> AppConfig:
    """Process-wide integration config; cleared only via get_app_config.cache_clear()."""
    return load_app_config(_resolved_integration_config_path())
