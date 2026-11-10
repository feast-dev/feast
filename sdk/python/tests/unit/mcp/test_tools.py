"""Tool-surface tests for the features and registry sub-servers.

These go through FastMCP's in-memory client, which speaks the real MCP
protocol against the server object: tool registration, mount namespacing,
schema generation from the type hints, and argument validation are all
exercised for real. Only the Feast hop is faked.

Auth is deliberately absent here -- the in-memory transport cannot carry a
token. See ``test_auth.py`` for that.
"""

from __future__ import annotations

import httpx
import pytest

# The standalone MCP server ships behind the optional feast[mcp-server] extra.
pytest.importorskip("fastmcp", reason="feast[mcp-server] extra not installed")

from fastmcp import Client  # noqa: E402
from fastmcp.exceptions import ToolError  # noqa: E402

from tests.unit.mcp.mcp_utils import build_mcp  # noqa: E402

ONLINE_FEATURES_ARGS = {
    "features": ["driver_hourly_stats:conv_rate"],
    "entities": {"driver_id": [1001, 1002]},
}


class TestToolRegistration:
    async def test_mount_namespaces_tools_by_sub_server(self, feast_backend):
        async with Client(build_mcp()) as client:
            names = {tool.name for tool in await client.list_tools()}
        assert "features_get_online_features" in names
        assert "registry_list_feature_views" in names
        # The un-namespaced names must not leak -- clients bind to these.
        assert "get_online_features" not in names

    async def test_only_configured_sub_servers_are_mounted(self, feast_backend):
        async with Client(build_mcp(registry=False)) as client:
            names = {tool.name for tool in await client.list_tools()}
        assert any(name.startswith("features_") for name in names)
        assert not any(name.startswith("registry_") for name in names)

    async def test_every_tool_has_a_description_for_the_llm(self, feast_backend):
        async with Client(build_mcp()) as client:
            tools = await client.list_tools()
        undocumented = [
            tool.name for tool in tools if not (tool.description or "").strip()
        ]
        assert not undocumented, f"tools missing a docstring: {undocumented}"

    async def test_required_arguments_are_marked_required(self, feast_backend):
        async with Client(build_mcp()) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        schema = tools["features_get_online_features"].inputSchema
        assert set(schema["required"]) == {"features", "entities"}
        # Optional arguments carry defaults and must not be required.
        assert "full_feature_names" not in schema["required"]

    async def test_unknown_tool_is_rejected(self, feast_backend):
        async with Client(build_mcp()) as client:
            with pytest.raises(Exception):
                await client.call_tool("features_not_a_tool", {})

    async def test_missing_required_argument_is_rejected(self, feast_backend):
        async with Client(build_mcp()) as client:
            with pytest.raises(Exception):
                await client.call_tool("features_get_online_features", {})
        assert feast_backend.call_count == 0, "invalid args must not reach Feast"


class TestFeastBackendFailures:
    """Fault injection -- Feast is unreachable or answers with an error."""

    async def test_unreachable_backend_surfaces_a_clear_error(self, feast_backend):
        feast_backend.fail_with(
            httpx.ConnectError("[Errno 61] Connection refused", request=None)
        )
        async with Client(build_mcp(timeout=5.0)) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool(
                    "features_get_online_features", ONLINE_FEATURES_ARGS
                )
        assert "Connection refused" in str(excinfo.value)

    async def test_configured_timeout_reaches_the_http_client(self, feast_backend):
        """A silent hang is the failure mode; a timeout has to be configured.

        Injecting a ``ReadTimeout`` only proves the error path. This proves
        the bound exists at all -- without it, ``timeout=None`` (wait
        forever) passes every other test in this suite.
        """
        async with Client(build_mcp(timeout=5.0)) as client:
            await client.call_tool("features_get_online_features", ONLINE_FEATURES_ARGS)
        assert feast_backend.client_timeouts == [5.0]

    async def test_read_timeout_surfaces_as_an_error_not_a_hang(self, feast_backend):
        feast_backend.fail_with(httpx.ReadTimeout("timed out", request=None))
        async with Client(build_mcp(timeout=1.0)) as client:
            with pytest.raises(ToolError):
                await client.call_tool(
                    "features_get_online_features", ONLINE_FEATURES_ARGS
                )

    async def test_server_error_becomes_a_tool_error(self, feast_backend):
        feast_backend.queue(503, {"detail": "online store unavailable"})
        async with Client(build_mcp()) as client:
            with pytest.raises(ToolError, match="online store unavailable"):
                await client.call_tool(
                    "features_get_online_features", ONLINE_FEATURES_ARGS
                )
