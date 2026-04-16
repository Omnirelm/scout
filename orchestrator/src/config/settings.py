from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ORCHESTRATOR_", env_file=".env", extra="ignore"
    )

    app_name: str = "orchestrator"
    debug: bool = False
    log_level: str = "INFO"
    openai_api_key: str | None = Field(
        default=None, validation_alias="OPENAI_API_KEY"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
