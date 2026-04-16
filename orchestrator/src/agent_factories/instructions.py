"""Load agent name, model, and instructions from packaged agent_instructions.yaml."""

from __future__ import annotations

import importlib.resources
from functools import lru_cache
from typing import Any, Dict

import yaml

_YAML_NAME = "agent_instructions.yaml"


@lru_cache(maxsize=1)
def _agents_map() -> Dict[str, Any]:
    ref = importlib.resources.files("src.agent_factories").joinpath(_YAML_NAME)
    with ref.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping at root of {_YAML_NAME}")
    agents = data.get("agents")
    if not isinstance(agents, dict):
        raise ValueError(f"Expected 'agents' mapping in {_YAML_NAME}")
    return agents


def _entry(agent_key: str) -> Dict[str, Any]:
    agents = _agents_map()
    if agent_key not in agents:
        raise KeyError(
            f"Unknown agent key {agent_key!r}; expected one of the keys under 'agents' in {_YAML_NAME}"
        )
    entry = agents[agent_key]
    if not isinstance(entry, dict):
        raise ValueError(f"Agent {agent_key!r} must be a mapping in {_YAML_NAME}")
    return entry


def get_agent_instructions(agent_key: str) -> str:
    entry = _entry(agent_key)
    raw = entry.get("instructions")
    if raw is None or not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Agent {agent_key!r} missing non-empty 'instructions' in {_YAML_NAME}")
    return raw


def get_agent_model(agent_key: str) -> str:
    entry = _entry(agent_key)
    raw = entry.get("model")
    if raw is None or not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Agent {agent_key!r} missing non-empty 'model' in {_YAML_NAME}")
    return raw.strip()


def get_agent_name(agent_key: str) -> str:
    entry = _entry(agent_key)
    raw = entry.get("name")
    if raw is None or not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Agent {agent_key!r} missing non-empty 'name' in {_YAML_NAME}")
    return raw.strip()
