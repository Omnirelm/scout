"""YAML integration config: logging sources and MCP servers."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

from src.core.mcp import McpConfig, McpServerConfig
from src.config.settings import get_settings
from src.integrations.logs.registry import LogSourceSpec

import yaml
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class ApiKeyCredentials(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    api_key: str = Field(alias="apiKey")
    api_key_header_name: str | None = Field(
        default=None, alias="apiKeyHeaderName"
    )


class BearerAuth(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    token: str


class BasicAuth(BaseModel):
    username: str
    password: str


class OAuthConfigInner(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    client_id: str = Field(alias="clientId")
    client_secret: str = Field(alias="clientSecret")
    token_url: str = Field(alias="tokenUrl")
    scope: str | None = None
    token_expiry_buffer: int = Field(default=60, alias="tokenExpiryBuffer")


class OAuthWrapper(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    oauth_config: OAuthConfigInner = Field(alias="oauthConfig")


class AuthConfig(BaseModel):
    """Shape matches keys expected by build_headers_and_oauth_from_auth_dict."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    api_key: ApiKeyCredentials | None = Field(default=None, alias="apiKey")
    bearer: BearerAuth | None = None
    basic: BasicAuth | None = None
    oauth: OAuthWrapper | None = None

    def model_dump_for_registry(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)


class LogSourceConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str
    enabled: bool = False
    flavour: str
    url: str
    tenant_id: str | None = Field(default=None, alias="tenantId")
    headers: dict[str, str] = Field(default_factory=dict)
    index_pattern: str | None = Field(default=None, alias="indexPattern")
    database: str | None = None
    table: str | None = None
    auth: AuthConfig | None = None

    def to_log_source_spec(self) -> LogSourceSpec:
        """Typed input for get_log_extractor (registry boundary)."""
        return LogSourceSpec(
            flavour=self.flavour,
            url=self.url,
            tenant_id=self.tenant_id,
            headers=dict(self.headers),
            index_pattern=self.index_pattern,
            database=self.database,
            table=self.table,
            auth_mechanism=(
                self.auth.model_dump_for_registry() if self.auth is not None else None
            ),
        )


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    sources: list[LogSourceConfig] = Field(default_factory=list)


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
