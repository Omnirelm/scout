"""Orchestrator config loader."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from src.config.settings import ToolsConfig, get_config, load_config


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tools: ToolsConfig = Field(default_factory=ToolsConfig)


def load_app_config(path: str | Path | None = None) -> AppConfig:
    cfg = load_config(path=path)
    return AppConfig.model_validate({"tools": cfg.tools.model_dump()})


@lru_cache
def get_app_config() -> AppConfig:
    return AppConfig.model_validate({"tools": get_config().tools.model_dump()})
