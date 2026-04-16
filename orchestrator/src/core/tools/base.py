"""
Tool registry and base tool abstraction for programmatic and agent invocation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Sequence


class ToolNotFoundError(KeyError):
    """Raised when a tool name is not registered in a ToolRegistry."""


class BaseTool(ABC):
    """A tool callable by workflows (`execute`) and by agents (`as_function_tool`)."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable snake_case identifier used for registry lookup."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what the tool does."""

    @abstractmethod
    def execute(self, **kwargs: Any) -> Any:
        """Programmatic / workflow invocation."""

    @abstractmethod
    def as_function_tool(self) -> Any:
        """Return a value suitable for `agents.Agent(..., tools=[...])`."""


class ToolRegistry:
    """Scoped registry of tools (not a singleton)."""

    def __init__(self) -> None:
        self._tools: Dict[str, BaseTool] = {}
        self._factories: Dict[str, Callable[[dict[str, Any]], BaseTool]] = {}

    def register(self, tool: BaseTool) -> ToolRegistry:
        """Register a tool by `tool.name`. Returns self for chaining."""
        self._tools[tool.name] = tool
        return self

    def get(self, name: str) -> BaseTool:
        """Return the tool registered under `name`."""
        try:
            return self._tools[name]
        except KeyError as e:
            raise ToolNotFoundError(f"Tool not found: {name!r}") from e

    def register_factory(
        self, name: str, fn: Callable[[dict[str, Any]], BaseTool]
    ) -> ToolRegistry:
        """Register a factory for context-dependent tools."""
        self._factories[name] = fn
        return self

    def resolve(self, name: str, context: dict[str, Any]) -> BaseTool:
        """Return a static tool instance or instantiate a factory."""
        if name in self._tools:
            return self._tools[name]
        factory = self._factories.get(name)
        if factory is not None:
            return factory(context)
        raise ToolNotFoundError(f"Tool not found: {name!r}")

    def list_tools(self) -> List[BaseTool]:
        """All registered tools."""
        return list(self._tools.values())

    def names(self) -> List[str]:
        """Registered tool names."""
        return list(self._tools.keys())

    def get_function_tools(self, names: Sequence[str]) -> List[Any]:
        """Build agent tool list in the given order."""
        return [self.get(n).as_function_tool() for n in names]
