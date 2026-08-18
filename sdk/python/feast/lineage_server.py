# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Standalone OpenLineage lineage server.

Runs the OpenLineage consumer as an independent FastAPI application,
separate from the Feast registry or UI server.  This enables independent
scaling and deployment of the lineage subsystem.

Usage:
    feast serve_lineage --host 0.0.0.0 --port 6580
"""

import asyncio
import logging

from fastapi import FastAPI

logger = logging.getLogger(__name__)


def _build_rbac_callback(store):
    """Build a namespace-filter callback from the Feast FeatureStore.

    Returns ``None`` when RBAC / authz is not configured, which means
    "all namespaces visible" in the consumer router.
    """
    try:
        authz_cfg = getattr(store.config, "auth", None) or getattr(
            store.config, "auth_config", None
        )
        if authz_cfg is None:
            return None
    except Exception:
        return None

    ol_pydantic = getattr(store.config, "openlineage", None)
    if ol_pydantic is None:
        return None

    from feast.openlineage.config import OpenLineageConfig

    if hasattr(ol_pydantic, "to_openlineage_config"):
        ol_config = ol_pydantic.to_openlineage_config()
    elif isinstance(ol_pydantic, dict):
        ol_config = OpenLineageConfig.from_dict(ol_pydantic)
    elif isinstance(ol_pydantic, OpenLineageConfig):
        ol_config = ol_pydantic
    else:
        ol_config = None

    if ol_config is None:
        return None

    consumer_cfg = getattr(ol_config, "consumer", None)
    ns_map = getattr(consumer_cfg, "namespace_mapping", None) or {}

    def _get_allowed():
        try:
            from feast.openlineage.identity import resolve_namespace
            from feast.permissions.action import AuthzedAction
            from feast.permissions.security_manager import (
                get_security_manager,
                permitted_resources,
            )

            sm = get_security_manager()
            if sm is None:
                return None

            all_projects = store.registry.list_projects(allow_cache=True)
            if not all_projects:
                return None

            allowed_projects = permitted_resources(all_projects, AuthzedAction.DESCRIBE)
            ol_ns_config = getattr(ol_config, "namespace", "feast")
            allowed_project_names = {p.name for p in allowed_projects}

            namespaces = set()
            for p in allowed_projects:
                namespaces.add(resolve_namespace(ol_ns_config, p.name))

            for ext_ns, mapped_project in ns_map.items():
                if mapped_project in allowed_project_names:
                    namespaces.add(ext_ns)

            return list(namespaces) if namespaces else None
        except Exception:
            return None

    return _get_allowed


def create_lineage_app(store) -> FastAPI:
    """Create the standalone lineage FastAPI application."""
    from feast.openlineage.config import (
        OpenLineageConfig,
        OpenLineageConsumerConfig,
    )
    from feast.openlineage.consumer import get_consumer_router
    from feast.openlineage.processor import OpenLineageProcessor
    from feast.openlineage.store import OpenLineageStore

    ol_pydantic = getattr(store.config, "openlineage", None)
    if ol_pydantic is None:
        raise ValueError(
            "OpenLineage configuration is required. "
            "Add 'openlineage' section to feature_store.yaml."
        )

    if hasattr(ol_pydantic, "to_openlineage_config"):
        ol_config = ol_pydantic.to_openlineage_config()
    elif isinstance(ol_pydantic, dict):
        ol_config = OpenLineageConfig.from_dict(ol_pydantic)
    elif isinstance(ol_pydantic, OpenLineageConfig):
        ol_config = ol_pydantic
    else:
        consumer_dict = getattr(ol_pydantic, "consumer", None)
        if consumer_dict is None:
            raise ValueError("OpenLineage consumer configuration is required.")
        if isinstance(consumer_dict, dict):
            consumer = OpenLineageConsumerConfig.from_dict(consumer_dict)
        else:
            consumer = OpenLineageConsumerConfig(
                enabled=getattr(consumer_dict, "enabled", False),
                store_type=getattr(consumer_dict, "store_type", "sql"),
                connection_string=getattr(consumer_dict, "connection_string", None),
                api_key=getattr(consumer_dict, "api_key", None),
                namespace_mapping=getattr(consumer_dict, "namespace_mapping", None)
                or {},
                retention_days=getattr(consumer_dict, "retention_days", 30),
                retention_check_interval_hours=getattr(
                    consumer_dict, "retention_check_interval_hours", 6
                ),
            )
        ol_config = OpenLineageConfig(
            enabled=getattr(ol_pydantic, "enabled", False),
            consumer=consumer,
        )

    consumer_config = getattr(ol_config, "consumer", None)
    if consumer_config is None or not consumer_config.enabled:
        raise ValueError(
            "OpenLineage consumer must be enabled for the lineage server. "
            "Set openlineage.consumer.enabled: true in feature_store.yaml."
        )

    if consumer_config.connection_string:
        ol_store = OpenLineageStore(connection_string=consumer_config.connection_string)
    else:
        registry_config = store.config.registry
        registry_path = getattr(registry_config, "path", None)
        if isinstance(registry_config, dict):
            registry_path = registry_config.get("path")
        if not registry_path:
            raise ValueError(
                "OpenLineage consumer requires a SQL database. "
                "Set openlineage.consumer.connection_string or use a SQL registry."
            )
        ol_store = OpenLineageStore(connection_string=registry_path)

    ol_store.initialize()

    ns_mapping = consumer_config.namespace_mapping
    if isinstance(ns_mapping, type(None)):
        ns_mapping = {}

    processor = OpenLineageProcessor(
        store=ol_store,
        namespace_mapping=ns_mapping,
    )

    get_allowed_namespaces = _build_rbac_callback(store)

    consumer_router = get_consumer_router(
        config=ol_config,
        store=ol_store,
        processor=processor,
        get_allowed_namespaces=get_allowed_namespaces,
    )

    app = FastAPI(
        title="Feast OpenLineage Server",
        description="Standalone OpenLineage consumer for Feast lineage data",
        version="1.0.0",
        docs_url="/",
        redoc_url="/docs",
    )
    app.include_router(consumer_router, prefix="/api/v1")

    retention_days = getattr(consumer_config, "retention_days", 30)
    check_hours = getattr(consumer_config, "retention_check_interval_hours", 6)
    rbac_enabled = get_allowed_namespaces is not None

    @app.on_event("startup")
    async def _on_startup():
        logger.info("=" * 60)
        logger.info("Feast OpenLineage Server (standalone)")
        logger.info("-" * 60)
        logger.info(f"  Project:    {store.config.project}")
        logger.info(f"  Store type: {consumer_config.store_type}")
        logger.info(f"  RBAC:       {'enabled' if rbac_enabled else 'disabled'}")
        if retention_days > 0:
            logger.info(f"  Retention:  {retention_days}d (check every {check_hours}h)")
            asyncio.create_task(_retention_loop())
        else:
            logger.info("  Retention:  disabled")
        logger.info("  Endpoints:  POST /api/v1/lineage")
        logger.info("=" * 60)

    if retention_days > 0:

        async def _retention_loop():
            interval_s = max(check_hours, 1) * 3600
            await asyncio.sleep(30)
            while True:
                try:
                    ol_store.prune_expired(retention_days)
                    logger.info("OpenLineage retention prune completed")
                except Exception as prune_err:
                    logger.warning(f"OpenLineage retention prune failed: {prune_err}")
                await asyncio.sleep(interval_s)

    return app


def start_lineage_server(
    store,
    host: str = "0.0.0.0",
    port: int = 6580,
    tls_key_path: str = "",
    tls_cert_path: str = "",
):
    """Start the standalone OpenLineage lineage server."""
    app = create_lineage_app(store)

    ssl_kwargs: dict = {}
    scheme = "http"
    if tls_key_path and tls_cert_path:
        ssl_kwargs["ssl_keyfile"] = tls_key_path
        ssl_kwargs["ssl_certfile"] = tls_cert_path
        scheme = "https"

    import uvicorn

    logger.info(f"Starting Feast OpenLineage server on {scheme}://{host}:{port}")
    uvicorn.run(app, host=host, port=port, **ssl_kwargs)
