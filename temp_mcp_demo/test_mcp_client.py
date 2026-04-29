"""
Test script: connect to the running Feast MCP server and exercise all tools.

Usage:
    python test_mcp_client.py
"""

import asyncio
import json

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client


MCP_URL = "http://localhost:6566/mcp"


async def call_tool(session, name, args=None):
    """Call an MCP tool and pretty-print the result."""
    print(f"=== {name} ===")
    result = await session.call_tool(name, args or {})
    for content in result.content:
        raw = content.text
        try:
            parsed = json.loads(raw)
            print(json.dumps(parsed, indent=2))
        except (json.JSONDecodeError, AttributeError):
            print(f"  (raw) {raw}")
    if result.isError:
        print("  ** ERROR reported by server **")
    print()
    return result


async def main():
    print(f"Connecting to MCP server at {MCP_URL} ...\n")

    async with streamablehttp_client(MCP_URL) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("Connected!\n")

            # Discover available tools
            tools_result = await session.list_tools()
            print(f"Available tools ({len(tools_result.tools)}):")
            for t in tools_result.tools:
                print(f"  - {t.name}: {t.description[:80]}")
                if t.inputSchema.get("properties"):
                    for pname, pschema in t.inputSchema["properties"].items():
                        desc = pschema.get("description", "")
                        print(f"      {pname}: {desc[:60]}")
            print()

            # --- Read tools ---
            await call_tool(session, "list_feature_views")
            await call_tool(session, "list_entities")
            await call_tool(session, "list_data_sources")
            await call_tool(session, "list_feature_services")

            # --- get_online_features ---
            await call_tool(
                session,
                "get_online_features",
                {
                    "features": [
                        "customer_profile:name",
                        "customer_profile:plan_tier",
                        "customer_profile:total_spend",
                    ],
                    "entities": {"customer_id": ["C1001", "C1002", "C1003"]},
                },
            )

            # --- retrieve_online_documents (v2 text search) ---
            await call_tool(
                session,
                "retrieve_online_documents",
                {
                    "features": [
                        "knowledge_base:title",
                        "knowledge_base:content",
                        "knowledge_base:category",
                    ],
                    "query_string": "SSO",
                    "top_k": 3,
                    "api_version": 2,
                },
            )

            # --- write_to_online_store ---
            await call_tool(
                session,
                "write_to_online_store",
                {
                    "feature_view_name": "agent_memory",
                    "df": {
                        "customer_id": ["C9999"],
                        "last_topic": ["MCP test"],
                        "last_resolution": ["Verified write works"],
                        "interaction_count": [1],
                        "preferences": [""],
                        "open_issue": [""],
                        "event_timestamp": ["2026-04-13T00:00:00"],
                    },
                },
            )

            # --- verify the write by reading it back ---
            await call_tool(
                session,
                "get_online_features",
                {
                    "features": [
                        "agent_memory:last_topic",
                        "agent_memory:last_resolution",
                        "agent_memory:interaction_count",
                    ],
                    "entities": {"customer_id": ["C9999"]},
                },
            )

            print("All tools tested successfully!")


if __name__ == "__main__":
    asyncio.run(main())
