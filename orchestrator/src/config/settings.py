import os
from functools import lru_cache
from pathlib import Path

from dynaconf import Dynaconf
from pydantic import BaseModel, ConfigDict, Field

from src.core.mcp import McpServerConfig
from src.integrations.logs.config import LogSourceConfig


class ToolsConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    logging: dict[str, LogSourceConfig] = Field(default_factory=dict)
    mcp: dict[str, McpServerConfig] = Field(default_factory=dict)


class OrchestratorConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    app_name: str = "orchestrator"
    debug: bool = False
    log_level: str = "INFO"
    config_file: str = "config.yaml"
    openai_api_key: str | None = None
    tools: ToolsConfig = Field(default_factory=ToolsConfig)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _load_config_source(path: str | Path | None = None) -> Dynaconf:
    """Private Dynaconf source for orchestrator config."""
    root = _project_root()
    settings_file = str(path) if path is not None else str(root / "config.yaml")
    return Dynaconf(
        envvar_prefix="ORCHESTRATOR",
        settings_files=[settings_file],
        environments=False,
        load_dotenv=True,
        dotenv_path=str(root / ".env"),
        merge_enabled=True,
    )


def _validate_required_runtime_values(config: OrchestratorConfig) -> None:
    missing: list[str] = []

    for source_name, source in config.tools.logging.items():
        if not source.enabled:
            continue
        if source.auth and source.auth.api_key and not source.auth.api_key.api_key:
            missing.append(f"tools.logging.{source_name}.auth.apiKey.apiKey")
        if source.auth and source.auth.bearer and not source.auth.bearer.token:
            missing.append(f"tools.logging.{source_name}.auth.bearer.token")
        if source.auth and source.auth.basic:
            if not source.auth.basic.username:
                missing.append(f"tools.logging.{source_name}.auth.basic.username")
            if not source.auth.basic.password:
                missing.append(f"tools.logging.{source_name}.auth.basic.password")

    for server_name, server in config.tools.mcp.items():
        if not server.enabled:
            continue
        if server.type == "stdio" and not (server.command or "").strip():
            missing.append(f"tools.mcp.{server_name}.command")
        if server.type in ("sse", "streamable_http") and not (server.url or "").strip():
            missing.append(f"tools.mcp.{server_name}.url")
        auth_header = server.headers.get("Authorization")
        if server.type == "streamable_http" and not (auth_header or "").strip():
            missing.append(f"tools.mcp.{server_name}.headers.Authorization")

    if missing:
        raise ValueError(
            "Missing required runtime configuration values: " + ", ".join(missing)
        )


def _apply_runtime_env(config: OrchestratorConfig) -> None:
    """Hydrate runtime env from config where downstream SDKs expect env vars."""
    if config.openai_api_key and not os.getenv("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = config.openai_api_key


def load_config(path: str | Path | None = None) -> OrchestratorConfig:
    dynasettings = _load_config_source(path)
    payload = {
        "app_name": dynasettings.get("app_name", "orchestrator"),
        "debug": dynasettings.get("debug", False),
        "log_level": dynasettings.get("log_level", "INFO"),
        "config_file": dynasettings.get("config_file", "config.yaml"),
        "openai_api_key": dynasettings.get(
            "openai_api_key", dynasettings.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
        ),
        "tools": dynasettings.get("tools", {}),
    }
    config = OrchestratorConfig.model_validate(payload)
    _validate_required_runtime_values(config)
    _apply_runtime_env(config)
    return config


@lru_cache
def get_config() -> OrchestratorConfig:
    return load_config()
