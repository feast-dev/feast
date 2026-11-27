"""Minimal MCP client for the standalone Feast MCP server.

Connects over streamable HTTP, lists the tools the server mounted, and calls
one from each namespace. Run it after `feast mcp` is up:

    python mcp_client_demo.py                       # defaults to localhost:8000
    python mcp_client_demo.py http://localhost:8000

Set MCP_TOKEN to send a bearer token. The MCP server forwards it unchanged to
Feast, which validates it -- so this is how a Kubernetes Service Account token
reaches a cluster running with the operator's default kubernetes auth:

    MCP_TOKEN=$(kubectl create token default) python mcp_client_demo.py

Requires the same extra as the server: pip install 'feast[mcp-server]'
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from fastmcp import Client
from fastmcp.client.auth import BearerAuth

DEFAULT_URL = "http://localhost:8000"


def _unwrap(result):
    """Pull the payload out of a CallToolResult across fastmcp versions."""
    data = getattr(result, "data", None)
    if data is not None:
        return data
    content = getattr(result, "content", None)
    if content:
        # Older fastmcp versions only expose the JSON as text, so decode it to
        # match what `result.data` returns; keep non-JSON text as-is.
        payload = getattr(content[0], "text", content[0])
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                pass
        return payload
    return result


def _names(payload, key: str) -> list[str]:
    """Pull the `spec.name` of each object out of a registry listing.

    The tools return whatever the registry REST API returns -- a paginated
    envelope -- so unwrap it rather than assuming a bare list.
    """
    items = payload.get(key, []) if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        return []
    names = []
    for item in items:
        if isinstance(item, dict):
            spec = item.get("spec")
            name = spec.get("name") if isinstance(spec, dict) else item.get("name")
            if name:
                names.append(str(name))
    return names


async def main(base_url: str) -> None:
    token = os.environ.get("MCP_TOKEN")
    auth = BearerAuth(token) if token else None
    async with Client(f"{base_url.rstrip('/')}/mcp", auth=auth) as client:
        tools = await client.list_tools()
        names = sorted(tool.name for tool in tools)
        print(f"{len(names)} tools mounted:")
        for name in names:
            print(f"  - {name}")

        # A sub-server mounts only when its URL is configured, so check before
        # calling rather than assuming both namespaces are present.
        project = None
        if "registry_list_projects" in names:
            # `feast init -t local feast_demo` names the project after the
            # directory, so discover it instead of hardcoding a name.
            projects = _names(
                _unwrap(await client.call_tool("registry_list_projects", {})),
                "projects",
            )
            print(f"\nregistry_list_projects: {projects}")
            project = projects[0] if projects else None

        if project and "registry_list_feature_views" in names:
            result = await client.call_tool(
                "registry_list_feature_views", {"project": project}
            )
            # The full payload carries every field of every view; print just
            # the names so the demo output stays readable.
            views = _names(_unwrap(result), "featureViews")
            print(f"registry_list_feature_views (project={project}): {views}")

        if "features_get_online_features" in names:
            result = await client.call_tool(
                "features_get_online_features",
                {
                    "features": [
                        "driver_hourly_stats:conv_rate",
                        "driver_hourly_stats:acc_rate",
                    ],
                    "entities": {"driver_id": [1001, 1002]},
                },
            )
            print("\nfeatures_get_online_features:")
            print(_unwrap(result))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL))
