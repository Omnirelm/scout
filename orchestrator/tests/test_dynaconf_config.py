from __future__ import annotations

from pathlib import Path

import pytest

from src.config.settings import get_config, load_config


def _write_config(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _clear_config_cache() -> None:
    get_config.cache_clear()


def test_config_env_override_and_type_casting(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_config_cache()
    monkeypatch.setenv("ORCHESTRATOR_DEBUG", "true")
    monkeypatch.setenv("ORCHESTRATOR_LOG_LEVEL", "WARNING")
    monkeypatch.setenv("ORCHESTRATOR_OPENAI_API_KEY", "sk-test")

    config = get_config()

    assert config.debug is True
    assert config.log_level == "WARNING"
    assert config.openai_api_key == "sk-test"


def test_load_config_supports_nested_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
tools:
  mcp:
    github:
      enabled: true
      type: streamable_http
      url: https://example.com/mcp
      headers:
        Authorization: "Bearer test"
""".strip(),
    )

    monkeypatch.setenv(
        "ORCHESTRATOR_TOOLS__MCP__GITHUB__HEADERS__Authorization",
        "Bearer override",
    )
    monkeypatch.setenv(
        "ORCHESTRATOR_TOOLS__MCP__GITHUB__URL",
        "https://override.example/mcp",
    )

    config = load_config(config_path)

    github = config.tools.mcp["github"]
    assert github.url == "https://override.example/mcp"
    assert github.headers["Authorization"] == "Bearer override"


def test_load_config_missing_required_auth_fails(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
tools:
  mcp:
    github:
      enabled: true
      type: streamable_http
      url: https://example.com/mcp
      headers: {}
""".strip(),
    )

    with pytest.raises(ValueError) as exc:
        load_config(config_path)

    assert "headers.Authorization" in str(exc.value)
