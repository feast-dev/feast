"""The functional contract of every MCP tool.

The server is a proxy, so each tool has exactly two things to get right:

1. it turns its arguments into the right Feast request -- method, path,
   query params, and JSON body, with optional arguments omitted rather than
   sent as nulls; and
2. it hands Feast's response back to the caller unmangled.

This module covers all 9 feature tools and all 13 registry tools in one
table. Structural concerns (registration, namespacing, schemas) live in
``test_tools.py``; token handling lives in ``test_auth.py``.

Without this table whole tools are invisible: routing
``materialize_incremental`` to ``/materialize``, or dropping ``query`` from
the ``/search`` body, breaks nothing that any other test asserts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

# The standalone MCP server ships behind the optional feast[mcp-server] extra.
pytest.importorskip("fastmcp", reason="feast[mcp-server] extra not installed")

from fastmcp import Client  # noqa: E402

from tests.unit.mcp.mcp_utils import (  # noqa: E402
    FEATURES_URL,
    REGISTRY_URL,
    build_mcp,
)

#: Marks a tool that sends no request body (every GET).
NO_BODY: Any = object()

#: Marks a tool whose return value is Feast's payload, not a literal.
ECHOES_PAYLOAD: Any = object()


@dataclass(frozen=True)
class ToolCase:
    """One tool invocation and the Feast request it must produce."""

    id: str
    tool: str
    args: dict
    method: str
    path: str
    params: dict = field(default_factory=dict)
    body: Any = NO_BODY
    returns: Any = ECHOES_PAYLOAD


TS_START = "2026-01-01T00:00:00"
TS_END = "2026-02-01T00:00:00"


FEATURE_CASES = [
    ToolCase(
        id="get_online_features",
        tool="features_get_online_features",
        args={
            "features": ["driver_hourly_stats:conv_rate"],
            "entities": {"driver_id": [1001]},
        },
        method="POST",
        path="/get-online-features",
        body={
            "features": ["driver_hourly_stats:conv_rate"],
            "entities": {"driver_id": [1001]},
            "full_feature_names": False,
        },
    ),
    ToolCase(
        id="get_online_features:feature_service",
        tool="features_get_online_features",
        args={
            "features": [],
            "entities": {"driver_id": [1001]},
            "feature_service": "driver_activity",
            "full_feature_names": True,
        },
        method="POST",
        path="/get-online-features",
        body={
            "features": [],
            "entities": {"driver_id": [1001]},
            "full_feature_names": True,
            "feature_service": "driver_activity",
        },
    ),
    ToolCase(
        id="search:defaults",
        tool="features_search",
        args={"features": ["docs:embedding"]},
        method="POST",
        path="/search",
        body={
            "features": ["docs:embedding"],
            "top_k": 5,
            "full_feature_names": False,
            "api_version": 2,
        },
    ),
    ToolCase(
        id="search:query_vector",
        tool="features_search",
        args={
            "features": ["docs:embedding"],
            "top_k": 3,
            "query": [0.1, 0.2],
            "distance_metric": "L2",
        },
        method="POST",
        path="/search",
        body={
            "features": ["docs:embedding"],
            "top_k": 3,
            "full_feature_names": False,
            "api_version": 2,
            "query": [0.1, 0.2],
            "distance_metric": "L2",
        },
    ),
    ToolCase(
        id="search:query_string",
        tool="features_search",
        args={"features": ["docs:embedding"], "query_string": "how do I materialize"},
        method="POST",
        path="/search",
        body={
            "features": ["docs:embedding"],
            "top_k": 5,
            "full_feature_names": False,
            "api_version": 2,
            "query_string": "how do I materialize",
        },
    ),
    ToolCase(
        id="list_vector_stores",
        tool="features_list_vector_stores",
        args={},
        method="GET",
        path="/v1/vector_stores",
    ),
    ToolCase(
        id="get_vector_store",
        tool="features_get_vector_store",
        args={"vector_store_id": "vs-1"},
        method="GET",
        path="/v1/vector_stores/vs-1",
    ),
    ToolCase(
        id="vector_store_search:text",
        tool="features_vector_store_search",
        args={"vector_store_id": "vs-1", "query": "driver stats"},
        method="POST",
        path="/v1/vector_stores/vs-1/search",
        body={"query": "driver stats", "max_num_results": 10},
    ),
    ToolCase(
        id="vector_store_search:list",
        tool="features_vector_store_search",
        args={"vector_store_id": "vs-1", "query": ["a", "b"], "max_num_results": 3},
        method="POST",
        path="/v1/vector_stores/vs-1/search",
        body={"query": ["a", "b"], "max_num_results": 3},
    ),
    ToolCase(
        id="push:defaults",
        tool="features_push",
        args={"push_source_name": "driver_stats_push", "df": {"driver_id": [1001]}},
        method="POST",
        path="/push",
        body={
            "push_source_name": "driver_stats_push",
            "df": {"driver_id": [1001]},
            "to": "online",
            "allow_registry_cache": True,
            "transform_on_write": True,
        },
        returns="ok",
    ),
    ToolCase(
        id="push:offline",
        tool="features_push",
        args={
            "push_source_name": "driver_stats_push",
            "df": {"driver_id": [1001]},
            "to": "online_and_offline",
            "allow_registry_cache": False,
            "transform_on_write": False,
        },
        method="POST",
        path="/push",
        body={
            "push_source_name": "driver_stats_push",
            "df": {"driver_id": [1001]},
            "to": "online_and_offline",
            "allow_registry_cache": False,
            "transform_on_write": False,
        },
        returns="ok",
    ),
    ToolCase(
        id="materialize:full",
        tool="features_materialize",
        args={
            "start_ts": TS_START,
            "end_ts": TS_END,
            "feature_views": ["driver_hourly_stats"],
        },
        method="POST",
        path="/materialize",
        body={
            "start_ts": TS_START,
            "end_ts": TS_END,
            "feature_views": ["driver_hourly_stats"],
        },
        returns="ok",
    ),
    ToolCase(
        id="materialize:no_args",
        tool="features_materialize",
        args={},
        method="POST",
        path="/materialize",
        body={},
        returns="ok",
    ),
    ToolCase(
        # Must NOT collapse onto /materialize -- a different endpoint entirely.
        id="materialize_incremental",
        tool="features_materialize_incremental",
        args={"end_ts": TS_END},
        method="POST",
        path="/materialize-incremental",
        body={"end_ts": TS_END},
        returns="ok",
    ),
    ToolCase(
        id="materialize_incremental:feature_views",
        tool="features_materialize_incremental",
        args={"end_ts": TS_END, "feature_views": ["driver_hourly_stats"]},
        method="POST",
        path="/materialize-incremental",
        body={"end_ts": TS_END, "feature_views": ["driver_hourly_stats"]},
        returns="ok",
    ),
    ToolCase(
        id="health",
        tool="features_health",
        args={},
        method="GET",
        path="/health",
    ),
]


REGISTRY_CASES = [
    ToolCase(
        id="list_projects",
        tool="registry_list_projects",
        args={},
        method="GET",
        path="/api/v1/projects",
    ),
    ToolCase(
        id="get_project",
        tool="registry_get_project",
        args={"name": "demo"},
        method="GET",
        path="/api/v1/projects/demo",
    ),
    ToolCase(
        id="list_entities",
        tool="registry_list_entities",
        args={"project": "demo"},
        method="GET",
        path="/api/v1/entities",
        params={"project": "demo"},
    ),
    ToolCase(
        id="get_entity",
        tool="registry_get_entity",
        args={"name": "driver", "project": "demo"},
        method="GET",
        path="/api/v1/entities/driver",
        params={"project": "demo"},
    ),
    ToolCase(
        id="list_feature_views",
        tool="registry_list_feature_views",
        args={"project": "demo"},
        method="GET",
        path="/api/v1/feature_views",
        params={"project": "demo"},
    ),
    ToolCase(
        id="list_feature_views:all_filters",
        tool="registry_list_feature_views",
        args={
            "project": "demo",
            "entity": "driver",
            "feature": "conv_rate",
            "feature_service": "driver_activity",
            "data_source": "driver_stats_source",
        },
        method="GET",
        path="/api/v1/feature_views",
        params={
            "project": "demo",
            "entity": "driver",
            "feature": "conv_rate",
            "feature_service": "driver_activity",
            "data_source": "driver_stats_source",
        },
    ),
    ToolCase(
        id="get_feature_view",
        tool="registry_get_feature_view",
        args={"name": "driver_hourly_stats", "project": "demo"},
        method="GET",
        path="/api/v1/feature_views/driver_hourly_stats",
        params={"project": "demo"},
    ),
    ToolCase(
        id="list_features",
        tool="registry_list_features",
        args={"project": "demo"},
        method="GET",
        path="/api/v1/features",
        params={"project": "demo"},
    ),
    ToolCase(
        id="list_features:feature_view",
        tool="registry_list_features",
        args={"project": "demo", "feature_view": "driver_hourly_stats"},
        method="GET",
        path="/api/v1/features",
        params={"project": "demo", "feature_view": "driver_hourly_stats"},
    ),
    ToolCase(
        id="list_feature_services",
        tool="registry_list_feature_services",
        args={"project": "demo"},
        method="GET",
        path="/api/v1/feature_services",
        params={"project": "demo"},
    ),
    ToolCase(
        id="list_feature_services:feature_view",
        tool="registry_list_feature_services",
        args={"project": "demo", "feature_view": "driver_hourly_stats"},
        method="GET",
        path="/api/v1/feature_services",
        params={"project": "demo", "feature_view": "driver_hourly_stats"},
    ),
    ToolCase(
        id="get_feature_service",
        tool="registry_get_feature_service",
        args={"name": "driver_activity", "project": "demo"},
        method="GET",
        path="/api/v1/feature_services/driver_activity",
        params={"project": "demo"},
    ),
    ToolCase(
        id="list_data_sources",
        tool="registry_list_data_sources",
        args={"project": "demo"},
        method="GET",
        path="/api/v1/data_sources",
        params={"project": "demo"},
    ),
    ToolCase(
        id="get_data_source",
        tool="registry_get_data_source",
        args={"name": "driver_stats_source", "project": "demo"},
        method="GET",
        path="/api/v1/data_sources/driver_stats_source",
        params={"project": "demo"},
    ),
    ToolCase(
        id="search_registry",
        tool="registry_search_registry",
        args={"query": "driver"},
        method="GET",
        path="/api/v1/search",
        params={"query": "driver"},
    ),
    ToolCase(
        id="search_registry:scoped",
        tool="registry_search_registry",
        args={"query": "driver", "project": "demo"},
        method="GET",
        path="/api/v1/search",
        params={"query": "driver", "project": "demo"},
    ),
    ToolCase(
        id="get_lineage:complete",
        tool="registry_get_lineage",
        args={"project": "demo"},
        method="GET",
        path="/api/v1/lineage/complete",
        params={"project": "demo"},
    ),
    ToolCase(
        id="get_lineage:object",
        tool="registry_get_lineage",
        args={
            "project": "demo",
            "object_type": "feature_view",
            "object_name": "driver_hourly_stats",
        },
        method="GET",
        path="/api/v1/lineage/objects/feature_view/driver_hourly_stats",
        params={"project": "demo"},
    ),
    ToolCase(
        # Both filters are required for the object endpoint; one alone falls
        # back to the whole graph rather than erroring.
        id="get_lineage:partial_filter_falls_back",
        tool="registry_get_lineage",
        args={"project": "demo", "object_type": "feature_view"},
        method="GET",
        path="/api/v1/lineage/complete",
        params={"project": "demo"},
    ),
]


ALL_CASES = FEATURE_CASES + REGISTRY_CASES


async def _invoke(case: ToolCase, feast_backend, payload):
    feast_backend.queue(200, payload)
    async with Client(build_mcp()) as client:
        return await client.call_tool(case.tool, case.args)


@pytest.mark.parametrize("case", ALL_CASES, ids=[c.id for c in ALL_CASES])
async def test_tool_builds_the_right_feast_request(case: ToolCase, feast_backend):
    """Arguments in, correct HTTP request out."""
    await _invoke(case, feast_backend, {"echo": case.id})

    assert feast_backend.call_count == 1, "a tool must make exactly one Feast call"
    call = feast_backend.last
    # Each sub-server must talk to its own server: the registry tools are
    # mounted against the registry URL, the feature tools against the feature
    # server, and a mount mix-up would otherwise only show up in production.
    expected_host = (
        FEATURES_URL if case.tool.startswith("features_") else REGISTRY_URL
    ).removeprefix("http://")
    assert call.host == expected_host
    assert call.method == case.method
    assert call.path == case.path
    assert call.params == case.params
    if case.body is NO_BODY:
        assert call.body is None, f"{case.tool} should not send a request body"
    else:
        assert call.body == case.body


@pytest.mark.parametrize("case", ALL_CASES, ids=[c.id for c in ALL_CASES])
async def test_tool_returns_what_feast_returned(case: ToolCase, feast_backend):
    """Feast response in, same value out -- no reshaping, no swallowing."""
    payload = {
        "metadata": {"tool": case.id},
        "results": [{"values": [1001, 0.42], "statuses": ["PRESENT", "PRESENT"]}],
    }
    result = await _invoke(case, feast_backend, payload)

    expected = payload if case.returns is ECHOES_PAYLOAD else case.returns
    assert result.data == expected


@pytest.mark.parametrize("case", ALL_CASES, ids=[c.id for c in ALL_CASES])
async def test_optional_arguments_are_omitted_not_nulled(case: ToolCase, feast_backend):
    """An argument the caller left out must not reach Feast as ``null``.

    Feast distinguishes "absent" from "null" on several of these endpoints,
    so a tool that forwards ``None`` changes the semantics Feast sees.
    """
    await _invoke(case, feast_backend, {"echo": case.id})

    call = feast_backend.last
    if isinstance(call.body, dict):
        nulls = [key for key, value in call.body.items() if value is None]
        assert not nulls, f"{case.tool} sent null body keys: {nulls}"
    assert not [k for k, v in call.params.items() if v in (None, "None")], (
        f"{case.tool} sent a None query param: {call.params}"
    )


class TestMatrixCompleteness:
    """The table must keep covering every tool the server exposes."""

    async def test_every_registered_tool_has_at_least_one_case(self, feast_backend):
        async with Client(build_mcp()) as client:
            registered = {tool.name for tool in await client.list_tools()}
        covered = {case.tool for case in ALL_CASES}
        missing = registered - covered
        assert not missing, (
            f"tools with no case in ALL_CASES: {sorted(missing)} -- "
            "add one so the tool's request shape is pinned"
        )

    async def test_no_case_targets_a_tool_that_does_not_exist(self, feast_backend):
        async with Client(build_mcp()) as client:
            registered = {tool.name for tool in await client.list_tools()}
        stale = {case.tool for case in ALL_CASES} - registered
        assert not stale, f"cases for tools that no longer exist: {sorted(stale)}"
