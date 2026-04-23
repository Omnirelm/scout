"""Logging integration configuration models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .registry import LogSourceSpec


class ApiKeyCredentials(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    api_key: str = Field(alias="apiKey")
    api_key_header_name: str | None = Field(default=None, alias="apiKeyHeaderName")


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

    name: str | None = None
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

    sources: dict[str, LogSourceConfig] = Field(default_factory=dict)
