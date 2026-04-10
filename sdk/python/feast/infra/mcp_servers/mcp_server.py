"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast using the MCP Python SDK (FastMCP).
It exposes Feast functionality as MCP tools with LLM-friendly schemas,
avoiding the recursive $ref issues that occur with fastapi_mcp.
"""

import json
import logging
from typing import Annotated, Any, Dict, List, Optional

from pydantic import Field

from feast import utils as feast_utils
from feast.feature_store import FeatureStore
from feast.feature_view_utils import get_feature_view_from_feature_store
from feast.permissions.action import AuthzedAction
from feast.permissions.security_manager import assert_permissions

logger = logging.getLogger(__name__)

try:
    from mcp.server import FastMCP

    MCP_AVAILABLE = True
except ImportError:
    logger.warning(
        "mcp SDK is not installed. MCP support will be disabled. "
        "Install it with: pip install 'feast[mcp]'"
    )
    MCP_AVAILABLE = False
    FastMCP = None  # type: ignore[assignment, misc]


class McpTransportNotSupportedError(RuntimeError):
    pass


def _assert_read_permissions_for_features(
    store: FeatureStore, features: List[str]
) -> None:
    """Resolve feature views from feature references and assert READ_ONLINE."""
    all_feature_views, all_on_demand_feature_views = (
        feast_utils._get_feature_views_to_use(
            store.registry,
            store.project,
            list(features),
            allow_cache=True,
            hide_dummy_entity=False,
        )
    )
    for fv in all_feature_views:
        assert_permissions(resource=fv, actions=[AuthzedAction.READ_ONLINE])
    for odfv in all_on_demand_feature_views:
        assert_permissions(resource=odfv, actions=[AuthzedAction.READ_ONLINE])


def _assert_write_permissions_for_feature_view(
    store: FeatureStore,
    feature_view_name: str,
    allow_registry_cache: bool = True,
) -> None:
    """Resolve a feature view by name and assert WRITE_ONLINE."""
    resource = get_feature_view_from_feature_store(
        store, feature_view_name, allow_registry_cache=allow_registry_cache
    )
    assert_permissions(resource=resource, actions=[AuthzedAction.WRITE_ONLINE])


def create_mcp_server(store: FeatureStore, config) -> "FastMCP":
    """
    Create a FastMCP server with Feast tools registered.

    Each tool calls FeatureStore methods directly — no internal HTTP round-trip.
    The ``store`` instance is shared with the FastAPI HTTP routes.
    """
    mcp = FastMCP(
        name=getattr(config, "mcp_server_name", "feast-feature-store"),
    )

    # ── Registry introspection tools ──────────────────────────────────

    @mcp.tool(
        description=(
            "List all feature views registered in the Feast feature store. "
            "Returns name, entities, features, and tags for each feature view."
        ),
    )
    def list_feature_views(
        tags: Annotated[
            Optional[Dict[str, str]],
            Field(
                description="Optional dict of tag key-value pairs to filter feature views by."
            ),
        ] = None,
    ) -> str:
        fvs = store.list_feature_views(allow_cache=True, tags=tags)
        return json.dumps(
            [
                {
                    "name": fv.name,
                    "entities": [e.name for e in fv.entity_columns],
                    "features": [f.name for f in fv.features],
                    "tags": dict(fv.tags) if fv.tags else {},
                }
                for fv in fvs
            ]
        )

    @mcp.tool(
        description=(
            "List all entities registered in the Feast feature store. "
            "Returns name, join keys, value type, and tags for each entity."
        ),
    )
    def list_entities(
        tags: Annotated[
            Optional[Dict[str, str]],
            Field(
                description="Optional dict of tag key-value pairs to filter entities by."
            ),
        ] = None,
    ) -> str:
        entities = store.list_entities(allow_cache=True, tags=tags)
        return json.dumps(
            [
                {
                    "name": e.name,
                    "join_key": e.join_key,
                    "value_type": e.value_type.name if e.value_type else None,
                    "tags": dict(e.tags) if e.tags else {},
                }
                for e in entities
            ]
        )

    @mcp.tool(
        description=(
            "List all feature services registered in the Feast feature store. "
            "Returns name, included feature views, and tags for each."
        ),
    )
    def list_feature_services(
        tags: Annotated[
            Optional[Dict[str, str]],
            Field(
                description="Optional dict of tag key-value pairs to filter feature services by."
            ),
        ] = None,
    ) -> str:
        services = store.list_feature_services(tags=tags)
        return json.dumps(
            [
                {
                    "name": svc.name,
                    "feature_views": [p.name for p in svc.feature_view_projections],
                    "tags": dict(svc.tags) if svc.tags else {},
                }
                for svc in services
            ]
        )

    @mcp.tool(
        description=(
            "List all data sources registered in the Feast feature store. "
            "Returns name, type, and tags for each data source."
        ),
    )
    def list_data_sources(
        tags: Annotated[
            Optional[Dict[str, str]],
            Field(
                description="Optional dict of tag key-value pairs to filter data sources by."
            ),
        ] = None,
    ) -> str:
        sources = store.list_data_sources(allow_cache=True, tags=tags)
        return json.dumps(
            [
                {
                    "name": ds.name,
                    "type": type(ds).__name__,
                    "tags": dict(ds.tags) if ds.tags else {},
                }
                for ds in sources
            ]
        )

    # ── Data access tools ─────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Get online feature values for a set of entities. "
            "Provide feature references like 'feature_view:feature_name' "
            'and entity key-value pairs like {"driver_id": [1001, 1002]}.'
        ),
    )
    def get_online_features(
        features: Annotated[
            List[str],
            Field(
                description="List of feature references in 'feature_view:feature_name' format."
            ),
        ],
        entities: Annotated[
            Dict[str, List[Any]],
            Field(
                description='Entity key-value pairs, e.g. {"driver_id": [1001, 1002]}.'
            ),
        ],
        full_feature_names: Annotated[
            bool,
            Field(
                description="If true, return feature names prefixed with the feature view name."
            ),
        ] = False,
    ) -> str:
        _assert_read_permissions_for_features(store, features)
        response = store.get_online_features(
            features=features,
            entity_rows=entities,
            full_feature_names=full_feature_names,
        )
        return json.dumps(response.to_dict(), default=str)

    @mcp.tool(
        description=(
            "Retrieve the top-k most similar documents from the online store. "
            "Supports vector similarity search (provide query vector), "
            "text-based keyword search (provide query_string), or "
            "hybrid search (provide both). "
            "Set api_version=2 to enable text-based and hybrid search via query_string. "
            "Default api_version=1 supports vector search only."
        ),
    )
    def retrieve_online_documents(
        features: Annotated[
            List[str],
            Field(
                description="List of feature references in 'feature_view:feature_name' format, e.g. ['doc_fv:title', 'doc_fv:content']."
            ),
        ],
        top_k: Annotated[
            int, Field(description="Number of top results to return.")
        ] = 10,
        query: Annotated[
            Optional[List[float]],
            Field(
                description="Query embedding vector as a list of floats for vector similarity search. Required for api_version=1, optional for api_version=2."
            ),
        ] = None,
        query_string: Annotated[
            Optional[str],
            Field(
                description="Natural language text query for keyword-based document search. Only supported with api_version=2. When provided alone performs text search; when combined with query vector performs hybrid search."
            ),
        ] = None,
        distance_metric: Annotated[
            Optional[str],
            Field(
                description="Distance metric to use for retrieval, e.g. 'L2', 'COSINE'."
            ),
        ] = None,
        api_version: Annotated[
            int,
            Field(
                description="API version to use. 1 = vector search only (requires query). 2 = supports query_string for text and hybrid search."
            ),
        ] = 1,
    ) -> str:
        _assert_read_permissions_for_features(store, features)
        if api_version == 2:
            response = store.retrieve_online_documents_v2(
                features=features,
                query=query,
                top_k=top_k,
                query_string=query_string,
                distance_metric=distance_metric or "L2",
            )
        else:
            response = store.retrieve_online_documents(
                features=features,
                query=query or [],
                top_k=top_k,
                distance_metric=distance_metric or "L2",
            )
        return json.dumps(response.to_dict(), default=str)

    # ── Write tools ────────────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Write feature data directly to the online store for a given feature view. "
            "Provide the feature view name and a dictionary of column lists representing "
            "the data to write (like a DataFrame in dict form). "
            "This can be used for persisting agent memory or pushing pre-computed features."
        ),
    )
    def write_to_online_store(
        feature_view_name: Annotated[
            str, Field(description="Name of the feature view to write to.")
        ],
        df: Annotated[
            Dict[str, List[Any]],
            Field(
                description='Data to write as column-oriented dict, e.g. {"customer_id": ["C1001"], "score": [0.95]}.'
            ),
        ],
        allow_registry_cache: Annotated[
            bool, Field(description="Whether to allow using cached registry.")
        ] = True,
        transform_on_write: Annotated[
            bool,
            Field(description="Whether to apply on-demand transforms before writing."),
        ] = True,
    ) -> str:
        import pandas as pd

        _assert_write_permissions_for_feature_view(
            store, feature_view_name, allow_registry_cache
        )
        store.write_to_online_store(
            feature_view_name=feature_view_name,
            df=pd.DataFrame(df),
            allow_registry_cache=allow_registry_cache,
            transform_on_write=transform_on_write,
        )
        return json.dumps({"status": "ok"})

    # ── Materialization tools ─────────────────────────────────────────

    @mcp.tool(
        description=(
            "Materialize feature data from the offline store into the online store "
            "for a given time range. Timestamps should be ISO-8601 strings. "
            "Optionally specify feature view names to materialize."
        ),
    )
    def materialize(
        start_ts: Annotated[
            str,
            Field(
                description="Start timestamp in ISO-8601 format, e.g. '2024-01-01T00:00:00'."
            ),
        ],
        end_ts: Annotated[
            str,
            Field(
                description="End timestamp in ISO-8601 format, e.g. '2024-01-02T00:00:00'."
            ),
        ],
        feature_views: Annotated[
            Optional[List[str]],
            Field(
                description="List of feature view names to materialize. If omitted, all are materialized."
            ),
        ] = None,
    ) -> str:
        from dateutil import parser

        from feast import utils

        for fv_name in feature_views or []:
            _assert_write_permissions_for_feature_view(store, fv_name)
        start_date = utils.make_tzaware(parser.parse(start_ts))
        end_date = utils.make_tzaware(parser.parse(end_ts))
        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_views,
        )
        return json.dumps({"status": "ok"})

    @mcp.tool(
        description=(
            "Incrementally materialize new feature data from the offline store "
            "into the online store up to the given end timestamp (ISO-8601)."
        ),
    )
    def materialize_incremental(
        end_ts: Annotated[
            str,
            Field(
                description="End timestamp in ISO-8601 format, e.g. '2024-01-02T00:00:00'."
            ),
        ],
        feature_views: Annotated[
            Optional[List[str]],
            Field(
                description="List of feature view names to materialize. If omitted, all are materialized."
            ),
        ] = None,
    ) -> str:
        from dateutil import parser

        from feast import utils

        for fv_name in feature_views or []:
            _assert_write_permissions_for_feature_view(store, fv_name)
        end_date = utils.make_tzaware(parser.parse(end_ts))
        store.materialize_incremental(
            end_date=end_date,
            feature_views=feature_views,
        )
        return json.dumps({"status": "ok"})

    return mcp


def add_mcp_support_to_app(app, store: FeatureStore, config) -> Optional["FastMCP"]:
    """Add MCP support to the FastAPI app if enabled in configuration."""
    if not MCP_AVAILABLE:
        logger.warning("MCP support requested but mcp SDK is not available")
        return None

    try:
        mcp = create_mcp_server(store, config)

        base_path = getattr(config, "mcp_base_path", "/mcp")

        transport = getattr(config, "mcp_transport", "sse")
        if transport == "sse":
            mcp_app = mcp.sse_app()
            app.mount(base_path, mcp_app)
        elif transport == "http":
            mcp.settings.streamable_http_path = "/"
            mcp_app = mcp.streamable_http_app()
            app.mount(base_path, mcp_app)
            # Starlette does not propagate lifespan events to mounted
            # sub-apps, so the main app must manage the session manager.
            app.state.mcp_session_manager = mcp.session_manager
        else:
            raise McpTransportNotSupportedError(
                f"Unsupported mcp_transport={transport!r}. Expected 'sse' or 'http'."
            )

        logger.info(
            "MCP support enabled at %s endpoint (transport=%s)",
            base_path,
            transport,
        )
        logger.info(
            "MCP server: %s v%s",
            getattr(config, "mcp_server_name", "feast-feature-store"),
            getattr(config, "mcp_server_version", "1.0.0"),
        )

        return mcp

    except McpTransportNotSupportedError:
        raise
    except Exception as e:
        logger.error("Failed to initialize MCP integration: %s", e, exc_info=True)
        return None
