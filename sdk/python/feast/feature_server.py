import asyncio
import os
import sys
import threading
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime
from importlib import resources as importlib_resources
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import psutil
from dateutil import parser
from fastapi import (
    Depends,
    FastAPI,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.concurrency import run_in_threadpool
from fastapi.logger import logger
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from google.protobuf.json_format import MessageToDict
from prometheus_client import Gauge, start_http_server
from pydantic import BaseModel

import feast
from feast import proto_json, utils
from feast.constants import DEFAULT_FEATURE_SERVER_REGISTRY_TTL
from feast.data_source import PushMode
from feast.errors import (
    FeastError,
    FeatureViewNotFoundException,
)
from feast.feast_object import FeastObject
from feast.permissions.action import WRITE, AuthzedAction
from feast.permissions.security_manager import assert_permissions
from feast.permissions.server.rest import inject_user_details
from feast.permissions.server.utils import (
    ServerType,
    init_auth_manager,
    init_security_manager,
    str_to_auth_manager_type,
)

# Define prometheus metrics
cpu_usage_gauge = Gauge(
    "feast_feature_server_cpu_usage", "CPU usage of the Feast feature server"
)
memory_usage_gauge = Gauge(
    "feast_feature_server_memory_usage", "Memory usage of the Feast feature server"
)


# TODO: deprecate this in favor of push features
class WriteToFeatureStoreRequest(BaseModel):
    feature_view_name: str
    df: dict
    allow_registry_cache: bool = True
    transform_on_write: bool = True


class PushFeaturesRequest(BaseModel):
    push_source_name: str
    df: dict
    allow_registry_cache: bool = True
    to: str = "online"
    transform_on_write: bool = True


class MaterializeRequest(BaseModel):
    start_ts: Optional[str] = None
    end_ts: Optional[str] = None
    feature_views: Optional[List[str]] = None
    disable_event_timestamp: bool = False


class MaterializeIncrementalRequest(BaseModel):
    end_ts: str
    feature_views: Optional[List[str]] = None


class GetOnlineFeaturesRequest(BaseModel):
    entities: Dict[str, List[Any]]
    feature_service: Optional[str] = None
    features: List[str] = []
    full_feature_names: bool = False


class GetOnlineDocumentsRequest(BaseModel):
    feature_service: Optional[str] = None
    features: List[str] = []
    full_feature_names: bool = False
    top_k: Optional[int] = None
    query: Optional[List[float]] = None
    query_string: Optional[str] = None
    api_version: Optional[int] = 1


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: List[ChatMessage]


class ReadDocumentRequest(BaseModel):
    file_path: str


class SaveDocumentRequest(BaseModel):
    file_path: str
    data: dict


async def _get_features(
    request: Union[GetOnlineFeaturesRequest, GetOnlineDocumentsRequest],
    store: "feast.FeatureStore",
):
    if request.feature_service:
        feature_service = await run_in_threadpool(
            store.get_feature_service, request.feature_service, allow_cache=True
        )
        assert_permissions(
            resource=feature_service, actions=[AuthzedAction.READ_ONLINE]
        )
        features = feature_service  # type: ignore
    else:
        all_feature_views, all_on_demand_feature_views = await run_in_threadpool(
            utils._get_feature_views_to_use,
            store.registry,
            store.project,
            request.features,
            allow_cache=True,
            hide_dummy_entity=False,
        )
        for feature_view in all_feature_views:
            assert_permissions(
                resource=feature_view, actions=[AuthzedAction.READ_ONLINE]
            )
        for od_feature_view in all_on_demand_feature_views:
            assert_permissions(
                resource=od_feature_view, actions=[AuthzedAction.READ_ONLINE]
            )
        features = request.features  # type: ignore
    return features


def get_app(
    store: "feast.FeatureStore",
    registry_ttl_sec: int = DEFAULT_FEATURE_SERVER_REGISTRY_TTL,
):
    """
    Creates a FastAPI app that can be used to start a feature server.

    Args:
        store: The FeatureStore to use for serving features
        registry_ttl_sec: The TTL in seconds for the registry cache

    Returns:
        A FastAPI app

    Example:
        ```python
        from feast import FeatureStore

        store = FeatureStore(repo_path="feature_repo")
        app = get_app(store)
        ```

    The app provides the following endpoints:
    - `/get-online-features`: Get online features
    - `/retrieve-online-documents`: Retrieve online documents
    - `/push`: Push features to the feature store
    - `/write-to-online-store`: Write to the online store
    - `/health`: Health check
    - `/materialize`: Materialize features
    - `/materialize-incremental`: Materialize features incrementally
    - `/chat`: Chat UI
    - `/ws/chat`: WebSocket endpoint for chat
    MCP Support:
    - If MCP is enabled in feature server configuration, MCP endpoints will be added automatically
    """
    proto_json.patch()
    # Asynchronously refresh registry, notifying shutdown and canceling the active timer if the app is shutting down
    registry_proto = None
    shutting_down = False
    active_timer: Optional[threading.Timer] = None

    def stop_refresh():
        nonlocal shutting_down
        shutting_down = True
        if active_timer:
            active_timer.cancel()

    def async_refresh():
        if shutting_down:
            return

        store.refresh_registry()
        nonlocal registry_proto
        registry_proto = store.registry.proto()

        if registry_ttl_sec:
            nonlocal active_timer
            active_timer = threading.Timer(registry_ttl_sec, async_refresh)
            active_timer.start()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.initialize()
        async_refresh()
        yield
        stop_refresh()
        await store.close()

    app = FastAPI(lifespan=lifespan)

    @app.post(
        "/get-online-features",
        dependencies=[Depends(inject_user_details)],
    )
    async def get_online_features(request: GetOnlineFeaturesRequest) -> Dict[str, Any]:
        # Initialize parameters for FeatureStore.get_online_features(...) call
        features = await _get_features(request, store)

        read_params = dict(
            features=features,
            entity_rows=request.entities,
            full_feature_names=request.full_feature_names,
        )

        if store._get_provider().async_supported.online.read:
            response = await store.get_online_features_async(**read_params)  # type: ignore
        else:
            response = await run_in_threadpool(
                lambda: store.get_online_features(**read_params)  # type: ignore
            )

        # Convert the Protobuf object to JSON and return it
        response_dict = await run_in_threadpool(
            MessageToDict,
            response.proto,
            preserving_proto_field_name=True,
            float_precision=18,
        )
        return response_dict

    @app.post(
        "/retrieve-online-documents",
        dependencies=[Depends(inject_user_details)],
    )
    async def retrieve_online_documents(
        request: GetOnlineDocumentsRequest,
    ) -> Dict[str, Any]:
        logger.warning(
            "This endpoint is in alpha and will be moved to /get-online-features when stable."
        )
        # Initialize parameters for FeatureStore.retrieve_online_documents_v2(...) call
        features = await _get_features(request, store)

        read_params = dict(features=features, query=request.query, top_k=request.top_k)
        if request.api_version == 2 and request.query_string is not None:
            read_params["query_string"] = request.query_string

        if request.api_version == 2:
            response = await run_in_threadpool(
                lambda: store.retrieve_online_documents_v2(**read_params)  # type: ignore
            )
        else:
            response = await run_in_threadpool(
                lambda: store.retrieve_online_documents(**read_params)  # type: ignore
            )

        # Convert the Protobuf object to JSON and return it
        response_dict = await run_in_threadpool(
            MessageToDict,
            response.proto,
            preserving_proto_field_name=True,
            float_precision=18,
        )
        return response_dict

    @app.post("/push", dependencies=[Depends(inject_user_details)])
    async def push(request: PushFeaturesRequest) -> None:
        df = pd.DataFrame(request.df)
        actions = []
        if request.to == "offline":
            to = PushMode.OFFLINE
            actions = [AuthzedAction.WRITE_OFFLINE]
        elif request.to == "online":
            to = PushMode.ONLINE
            actions = [AuthzedAction.WRITE_ONLINE]
        elif request.to == "online_and_offline":
            to = PushMode.ONLINE_AND_OFFLINE
            actions = WRITE
        else:
            raise ValueError(
                f"{request.to} is not a supported push format. Please specify one of these ['online', 'offline', 'online_and_offline']."
            )

        from feast.data_source import PushSource

        all_fvs = store.list_feature_views(
            allow_cache=request.allow_registry_cache
        ) + store.list_stream_feature_views(allow_cache=request.allow_registry_cache)
        fvs_with_push_sources = {
            fv
            for fv in all_fvs
            if (
                fv.stream_source is not None
                and isinstance(fv.stream_source, PushSource)
                and fv.stream_source.name == request.push_source_name
            )
        }

        for feature_view in fvs_with_push_sources:
            assert_permissions(resource=feature_view, actions=actions)

        push_params = dict(
            push_source_name=request.push_source_name,
            df=df,
            allow_registry_cache=request.allow_registry_cache,
            to=to,
            transform_on_write=request.transform_on_write,
        )

        should_push_async = (
            store._get_provider().async_supported.online.write
            and to in [PushMode.ONLINE, PushMode.ONLINE_AND_OFFLINE]
        )
        if should_push_async:
            await store.push_async(**push_params)
        else:
            store.push(**push_params)

    async def _get_feast_object(
        feature_view_name: str, allow_registry_cache: bool
    ) -> FeastObject:
        # FIXME: this logic repeated at least 3 times in the codebase - should be centralized
        # in logging, in server and in feature_store (Python SDK)
        try:
            return await run_in_threadpool(
                store.get_feature_view,
                feature_view_name,
                allow_registry_cache=allow_registry_cache,
            )
        except FeatureViewNotFoundException:
            try:
                return await run_in_threadpool(
                    store.get_on_demand_feature_view,
                    feature_view_name,
                    allow_registry_cache=allow_registry_cache,
                )
            except FeatureViewNotFoundException:
                return await run_in_threadpool(
                    store.get_stream_feature_view,
                    feature_view_name,
                    allow_registry_cache=allow_registry_cache,
                )

    @app.post("/write-to-online-store", dependencies=[Depends(inject_user_details)])
    async def write_to_online_store(request: WriteToFeatureStoreRequest) -> None:
        df = pd.DataFrame(request.df)
        feature_view_name = request.feature_view_name
        allow_registry_cache = request.allow_registry_cache
        resource = await _get_feast_object(feature_view_name, allow_registry_cache)
        assert_permissions(resource=resource, actions=[AuthzedAction.WRITE_ONLINE])
        await run_in_threadpool(
            store.write_to_online_store,
            feature_view_name=feature_view_name,
            df=df,
            allow_registry_cache=allow_registry_cache,
            transform_on_write=request.transform_on_write,
        )

    @app.get("/health")
    async def health():
        return (
            Response(status_code=status.HTTP_200_OK)
            if registry_proto
            else Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        )

    @app.post("/chat")
    async def chat(request: ChatRequest):
        # Process the chat request
        # For now, just return dummy text
        return {"response": "This is a dummy response from the Feast feature server."}

    @app.post("/read-document")
    async def read_document_endpoint(request: ReadDocumentRequest):
        try:
            import os

            if not os.path.exists(request.file_path):
                return {"error": f"File not found: {request.file_path}"}

            with open(request.file_path, "r", encoding="utf-8") as file:
                content = file.read()

            return {"content": content, "file_path": request.file_path}
        except Exception as e:
            return {"error": str(e)}

    @app.post("/save-document")
    async def save_document_endpoint(request: SaveDocumentRequest):
        try:
            import json
            import os
            from pathlib import Path

            file_path = Path(request.file_path).resolve()
            if not str(file_path).startswith(os.getcwd()):
                return {"error": "Invalid file path"}

            base_name = file_path.stem
            labels_file = file_path.parent / f"{base_name}-labels.json"

            with open(labels_file, "w", encoding="utf-8") as file:
                json.dump(request.data, file, indent=2, ensure_ascii=False)

            return {"success": True, "saved_to": str(labels_file)}
        except Exception as e:
            return {"error": str(e)}

    @app.get("/chat")
    async def chat_ui():
        # Serve the chat UI
        static_dir_ref = importlib_resources.files(__spec__.parent) / "static/chat"  # type: ignore[name-defined, arg-type]
        with importlib_resources.as_file(static_dir_ref) as static_dir:
            with open(os.path.join(static_dir, "index.html")) as f:
                content = f.read()
        return Response(content=content, media_type="text/html")

    @app.post("/materialize", dependencies=[Depends(inject_user_details)])
    async def materialize(request: MaterializeRequest) -> None:
        for feature_view in request.feature_views or []:
            resource = await _get_feast_object(feature_view, True)
            assert_permissions(
                resource=resource,
                actions=[AuthzedAction.WRITE_ONLINE],
            )

        if request.disable_event_timestamp:
            # Query all available data and use current datetime as event timestamp
            now = datetime.now()
            start_date = datetime(
                1970, 1, 1
            )  # Beginning of time to capture all historical data
            end_date = now
        else:
            if not request.start_ts or not request.end_ts:
                raise ValueError(
                    "start_ts and end_ts are required when disable_event_timestamp is False"
                )
            start_date = utils.make_tzaware(parser.parse(request.start_ts))
            end_date = utils.make_tzaware(parser.parse(request.end_ts))

        await run_in_threadpool(
            store.materialize,
            start_date,
            end_date,
            request.feature_views,
            disable_event_timestamp=request.disable_event_timestamp,
        )

    @app.post("/materialize-incremental", dependencies=[Depends(inject_user_details)])
    async def materialize_incremental(request: MaterializeIncrementalRequest) -> None:
        for feature_view in request.feature_views or []:
            resource = await _get_feast_object(feature_view, True)
            assert_permissions(
                resource=resource,
                actions=[AuthzedAction.WRITE_ONLINE],
            )
        await run_in_threadpool(
            store.materialize_incremental,
            utils.make_tzaware(parser.parse(request.end_ts)),
            request.feature_views,
        )

    @app.exception_handler(Exception)
    async def rest_exception_handler(request: Request, exc: Exception):
        # Print the original exception on the server side
        logger.exception(traceback.format_exc())

        if isinstance(exc, FeastError):
            return JSONResponse(
                status_code=exc.http_status_code(),
                content=exc.to_error_detail(),
            )
        else:
            return JSONResponse(
                status_code=500,
                content=str(exc),
            )

    # Chat WebSocket connection manager
    class ConnectionManager:
        def __init__(self):
            self.active_connections: List[WebSocket] = []

        async def connect(self, websocket: WebSocket):
            await websocket.accept()
            self.active_connections.append(websocket)

        def disconnect(self, websocket: WebSocket):
            self.active_connections.remove(websocket)

        async def send_message(self, message: str, websocket: WebSocket):
            await websocket.send_text(message)

    manager = ConnectionManager()

    @app.websocket("/ws/chat")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            while True:
                message = await websocket.receive_text()
                # Process the received message (currently unused but kept for future implementation)
                # For now, just return dummy text
                response = f"You sent: '{message}'. This is a dummy response from the Feast feature server."

                # Stream the response word by word
                words = response.split()
                for word in words:
                    await manager.send_message(word + " ", websocket)
                    await asyncio.sleep(0.1)  # Add a small delay between words
        except WebSocketDisconnect:
            manager.disconnect(websocket)

    # Mount static files
    static_dir_ref = importlib_resources.files(__spec__.parent) / "static"  # type: ignore[name-defined, arg-type]
    with importlib_resources.as_file(static_dir_ref) as static_dir:
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # Add MCP support if enabled in feature server configuration
    _add_mcp_support_if_enabled(app, store)

    return app


def _add_mcp_support_if_enabled(app, store: "feast.FeatureStore"):
    """Add MCP support to the FastAPI app if enabled in configuration."""
    try:
        # Check if MCP is enabled in feature server config
        if (
            store.config.feature_server
            and hasattr(store.config.feature_server, "type")
            and store.config.feature_server.type == "mcp"
            and getattr(store.config.feature_server, "mcp_enabled", False)
        ):
            from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

            mcp_server = add_mcp_support_to_app(app, store, store.config.feature_server)

            if mcp_server:
                logger.info("MCP support has been enabled for the Feast feature server")
            else:
                logger.warning("MCP support was requested but could not be enabled")
        else:
            logger.debug("MCP support is not enabled in feature server configuration")
    except Exception as e:
        logger.error(f"Error checking/adding MCP support: {e}")
        # Don't fail the entire server if MCP fails to initialize


if sys.platform != "win32":
    import gunicorn.app.base

    class FeastServeApplication(gunicorn.app.base.BaseApplication):
        def __init__(self, store: "feast.FeatureStore", **options):
            self._app = get_app(
                store=store,
                registry_ttl_sec=options["registry_ttl_sec"],
            )
            self._options = options
            super().__init__()

        def load_config(self):
            for key, value in self._options.items():
                if key.lower() in self.cfg.settings and value is not None:
                    self.cfg.set(key.lower(), value)

            self.cfg.set("worker_class", "uvicorn_worker.UvicornWorker")

        def load(self):
            return self._app


def monitor_resources(self, interval: int = 5):
    """Function to monitor and update CPU and memory usage metrics."""
    logger.debug(f"Starting resource monitoring with interval {interval} seconds")
    p = psutil.Process()
    logger.debug(f"PID is {p.pid}")
    while True:
        with p.oneshot():
            cpu_usage = p.cpu_percent()
            memory_usage = p.memory_percent()
            logger.debug(f"CPU usage: {cpu_usage}%, Memory usage: {memory_usage}%")
            logger.debug(f"CPU usage: {cpu_usage}%, Memory usage: {memory_usage}%")
            cpu_usage_gauge.set(cpu_usage)
            memory_usage_gauge.set(memory_usage)
        time.sleep(interval)


def start_server(
    store: "feast.FeatureStore",
    host: str,
    port: int,
    no_access_log: bool,
    workers: int,
    keep_alive_timeout: int,
    registry_ttl_sec: int,
    tls_key_path: str,
    tls_cert_path: str,
    metrics: bool,
):
    if (tls_key_path and not tls_cert_path) or (not tls_key_path and tls_cert_path):
        raise ValueError(
            "Both key and cert file paths are required to start server in TLS mode."
        )
    if metrics:
        logger.info("Starting Prometheus Server")
        start_http_server(8000)

        logger.debug("Starting background thread to monitor CPU and memory usage")
        monitoring_thread = threading.Thread(
            target=monitor_resources, args=(5,), daemon=True
        )
        monitoring_thread.start()

    logger.debug("start_server called")
    auth_type = str_to_auth_manager_type(store.config.auth_config.type)
    logger.info(f"Auth type: {auth_type}")
    init_security_manager(auth_type=auth_type, fs=store)
    logger.debug("Security manager initialized successfully")
    init_auth_manager(
        auth_type=auth_type,
        server_type=ServerType.REST,
        auth_config=store.config.auth_config,
    )
    logger.debug("Auth manager initialized successfully")

    if sys.platform != "win32":
        options = {
            "bind": f"{host}:{port}",
            "accesslog": None if no_access_log else "-",
            "workers": workers,
            "keepalive": keep_alive_timeout,
            "registry_ttl_sec": registry_ttl_sec,
        }

        # Add SSL options if the paths exist
        if tls_key_path and tls_cert_path:
            options["keyfile"] = tls_key_path
            options["certfile"] = tls_cert_path
        FeastServeApplication(store=store, **options).run()
    else:
        import uvicorn

        app = get_app(store, registry_ttl_sec)
        if tls_key_path and tls_cert_path:
            uvicorn.run(
                app,
                host=host,
                port=port,
                access_log=(not no_access_log),
                ssl_keyfile=tls_key_path,
                ssl_certfile=tls_cert_path,
            )
        else:
            uvicorn.run(app, host=host, port=port, access_log=(not no_access_log))
