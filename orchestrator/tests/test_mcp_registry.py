from src.core.mcp import McpServerConfig, McpServerRegistry


def test_registry_register_many_uses_mapping_keys_and_skips_disabled() -> None:
    registry = McpServerRegistry(
        {
            "github": McpServerConfig(
                enabled=True,
                type="streamable_http",
                url="https://example.com/mcp",
                headers={"Authorization": "Bearer token"},
            ),
            "disabled-sse": McpServerConfig(
                enabled=False,
                type="sse",
            ),
        }
    )

    assert registry.names() == ["github"]
    github = registry.get("github")
    assert github is not None
    assert github.name == "github"


def test_registry_can_include_disabled_with_override() -> None:
    registry = McpServerRegistry()
    registry.register_many(
        {
            "disabled-sse": McpServerConfig(
                enabled=False,
                type="sse",
            )
        },
        only_enabled=False,
    )

    assert registry.names() == ["disabled-sse"]
