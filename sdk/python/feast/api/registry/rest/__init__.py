import logging
from typing import Any, Optional

from fastapi import FastAPI

from feast.api.registry.rest.compute_engines import get_compute_engine_router
from feast.api.registry.rest.data_sources import get_data_source_router
from feast.api.registry.rest.entities import get_entity_router
from feast.api.registry.rest.feature_services import get_feature_service_router
from feast.api.registry.rest.feature_views import get_feature_view_router
from feast.api.registry.rest.features import get_feature_router
from feast.api.registry.rest.label_views import get_label_view_router
from feast.api.registry.rest.lineage import get_lineage_router
from feast.api.registry.rest.metrics import get_metrics_router
from feast.api.registry.rest.monitoring import get_monitoring_router
from feast.api.registry.rest.permissions import get_permission_router
from feast.api.registry.rest.projects import get_project_router, get_registry_router
from feast.api.registry.rest.saved_datasets import get_saved_dataset_router
from feast.api.registry.rest.search import get_search_router

logger = logging.getLogger(__name__)

_ol_store_instance: Optional[Any] = None
_ol_config: Optional[Any] = None
_ol_processor: Optional[Any] = None


def get_ol_processor() -> Optional[Any]:
    """Return the global OpenLineage processor, if initialized."""
    return _ol_processor


def register_all_routes(app: FastAPI, grpc_handler, server=None, store=None):
    app.include_router(get_entity_router(grpc_handler))
    app.include_router(get_data_source_router(grpc_handler))
    app.include_router(get_feature_service_router(grpc_handler))
    app.include_router(get_feature_view_router(grpc_handler))
    app.include_router(get_feature_router(grpc_handler))
    app.include_router(get_label_view_router(grpc_handler))
    app.include_router(get_lineage_router(grpc_handler))
    app.include_router(get_permission_router(grpc_handler))
    app.include_router(get_project_router(grpc_handler))
    app.include_router(get_search_router(grpc_handler))
    app.include_router(get_metrics_router(grpc_handler, server))
    resolved_store = store or (
        server.store if server and hasattr(server, "store") else None
    )
    app.include_router(get_saved_dataset_router(grpc_handler))
    app.include_router(get_monitoring_router(grpc_handler, store=resolved_store))
    app.include_router(get_compute_engine_router(grpc_handler, store=resolved_store))

    if resolved_store:
        app.include_router(get_registry_router(resolved_store))

    _register_openlineage_consumer(app, resolved_store)


def _register_openlineage_consumer(app: FastAPI, feast_store):
    """Register the OpenLineage consumer router if consumer is enabled."""
    if feast_store is None:
        return

    try:
        ol_pydantic = getattr(feast_store.config, "openlineage", None)
        if ol_pydantic is None:
            return

        from feast.openlineage.config import (
            OpenLineageConfig,
            OpenLineageConsumerConfig,
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
                return
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
                )
            ol_config = OpenLineageConfig(
                enabled=getattr(ol_pydantic, "enabled", False),
                consumer=consumer,
            )

        consumer_config = getattr(ol_config, "consumer", None)
        if consumer_config is None or not consumer_config.enabled:
            return

        is_standalone = getattr(consumer_config, "standalone_server", False)

        from feast.openlineage.consumer import get_consumer_router
        from feast.openlineage.processor import OpenLineageProcessor
        from feast.openlineage.store import OpenLineageStore

        if consumer_config.connection_string:
            ol_store = OpenLineageStore(
                connection_string=consumer_config.connection_string
            )
        else:
            registry_config = feast_store.config.registry
            registry_path = getattr(registry_config, "path", None)
            if isinstance(registry_config, dict):
                registry_path = registry_config.get("path")
            if not registry_path:
                logger.warning("No registry path found for OpenLineage consumer store")
                return
            ol_store = OpenLineageStore(connection_string=registry_path)

        ol_store.initialize()

        import feast.api.registry.rest as _self_module

        _self_module._ol_store_instance = ol_store
        _self_module._ol_config = ol_config

        ns_mapping = consumer_config.namespace_mapping
        if isinstance(ns_mapping, type(None)):
            ns_mapping = {}

        processor = OpenLineageProcessor(
            store=ol_store,
            namespace_mapping=ns_mapping,
        )

        _self_module._ol_processor = processor

        # Wire the local processor into Feast's own OL emitter so Feast events
        # are also stored in the consumer DB automatically.
        # Skip when standalone — events will be sent via HTTP transport instead.
        if not is_standalone:
            try:
                emitter = getattr(feast_store, "_openlineage_emitter", None)
                if emitter and hasattr(emitter, "_client") and emitter._client:
                    emitter._client.set_local_processor(processor)
                    logger.info(
                        "Feast OL emitter wired to local consumer processor (eager)"
                    )
            except Exception as wire_err:
                logger.debug(f"Could not wire emitter to local processor: {wire_err}")

        def _build_allowed_namespaces_fn(fs):
            """Build a callback that derives OL namespace access from Feast RBAC.

            Uses ``permitted_resources`` with ``DESCRIBE`` on all known projects.
            The projects the current user may describe become the allowed OL
            namespaces (mapped through ``resolve_namespace``).
            External producer namespaces (e.g. ``spark://ml-team``,
            ``airflow://prod-cluster``) are included when
            ``consumer.namespace_mapping`` maps them to a Feast project the
            user is allowed to DESCRIBE.  This is the only purpose of
            ``namespace_mapping`` — it is a **read-side RBAC bridge**, not a
            routing or rewrite mechanism.  Ingest stores events as-is;
            producers own their namespace.
            """

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

                    all_projects = fs.registry.list_projects(allow_cache=True)
                    if not all_projects:
                        return None

                    allowed_projects = permitted_resources(
                        all_projects, AuthzedAction.DESCRIBE
                    )

                    ol_ns_config = getattr(ol_config, "namespace", "feast")
                    allowed_project_names = {p.name for p in allowed_projects}

                    namespaces = set()
                    for p in allowed_projects:
                        namespaces.add(resolve_namespace(ol_ns_config, p.name))

                    # Include external namespaces whose namespace_mapping
                    # target is a project the user can DESCRIBE.
                    consumer_cfg = getattr(ol_config, "consumer", None)
                    ns_map = getattr(consumer_cfg, "namespace_mapping", None) or {}
                    for ext_ns, mapped_project in ns_map.items():
                        if mapped_project in allowed_project_names:
                            namespaces.add(ext_ns)

                    return list(namespaces) if namespaces else None
                except Exception:
                    return None

            return _get_allowed

        get_allowed_namespaces = _build_allowed_namespaces_fn(feast_store)

        consumer_router = get_consumer_router(
            config=ol_config,
            store=ol_store,
            processor=processor,
            get_allowed_namespaces=get_allowed_namespaces,
        )

        app.include_router(consumer_router)

        retention_days = getattr(consumer_config, "retention_days", 30)
        check_hours = getattr(consumer_config, "retention_check_interval_hours", 6)

        mode = "standalone" if is_standalone else "embedded"
        logger.info("OpenLineage consumer endpoints registered (mode=%s)", mode)
        if not is_standalone:
            logger.info(
                "OpenLineage consumer accepting events at POST <this-server>/api/v1/lineage"
            )
        if is_standalone:
            logger.info(
                "OpenLineage retention task deferred to standalone lineage server"
            )
        elif retention_days > 0:
            logger.info(
                "OpenLineage retention pruning enabled: %dd, check every %dh",
                retention_days,
                check_hours,
            )
        else:
            logger.info("OpenLineage retention pruning disabled")

        # Start background retention pruning only when NOT standalone
        # (the standalone lineage server owns the retention task)
        if retention_days > 0 and not is_standalone:
            import asyncio

            async def _retention_loop():
                interval_s = max(check_hours, 1) * 3600
                await asyncio.sleep(30)
                while True:
                    try:
                        ol_store.prune_expired(retention_days)
                    except (ConnectionError, TimeoutError, OSError) as e:
                        logger.warning(f"OL retention prune failed (retryable): {e}")
                    except Exception as e:
                        logger.error(
                            f"OL retention prune failed (unexpected): {e}",
                            exc_info=True,
                        )
                    await asyncio.sleep(interval_s)

            @app.on_event("startup")
            async def _start_retention_task():
                asyncio.create_task(_retention_loop())

    except ImportError as e:
        logger.debug(f"OpenLineage consumer not available: {e}")
    except Exception as e:
        logger.warning(f"Failed to register OpenLineage consumer: {e}")
        import traceback

        traceback.print_exc()
