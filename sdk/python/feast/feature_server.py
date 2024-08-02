import json
import sys
import threading
import time
import traceback
from contextlib import asynccontextmanager
from typing import List, Optional

import pandas as pd
import psutil
from dateutil import parser
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.logger import logger
from google.protobuf.json_format import MessageToDict
from prometheus_client import Gauge, start_http_server
from pydantic import BaseModel

import feast
from feast import proto_json, utils
from feast.constants import DEFAULT_FEATURE_SERVER_REGISTRY_TTL
from feast.data_source import PushMode
from feast.errors import (FeatureViewNotFoundException,
                          PushSourceNotFoundException)
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


class PushFeaturesRequest(BaseModel):
    push_source_name: str
    df: dict
    allow_registry_cache: bool = True
    to: str = "online"


class MaterializeRequest(BaseModel):
    start_ts: str
    end_ts: str
    feature_views: Optional[List[str]] = None


class MaterializeIncrementalRequest(BaseModel):
    end_ts: str
    feature_views: Optional[List[str]] = None


def get_app(
    store: "feast.FeatureStore",
    registry_ttl_sec: int = DEFAULT_FEATURE_SERVER_REGISTRY_TTL,
):
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
        store.refresh_registry()
        nonlocal registry_proto
        registry_proto = store.registry.proto()
        if shutting_down:
            return
        nonlocal active_timer
        active_timer = threading.Timer(registry_ttl_sec, async_refresh)
        active_timer.start()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async_refresh()
        yield
        stop_refresh()

    app = FastAPI(lifespan=lifespan)

    async def get_body(request: Request):
        return await request.body()

    # TODO RBAC: complete the dependencies for the other endpoints
    @app.post(
        "/get-online-features",
        dependencies=[Depends(inject_user_details)],
    )
    def get_online_features(body=Depends(get_body)):
        try:
            body = json.loads(body)
            full_feature_names = body.get("full_feature_names", False)
            entity_rows = body["entities"]
            # Initialize parameters for FeatureStore.get_online_features(...) call
            if "feature_service" in body:
                feature_service = store.get_feature_service(
                    body["feature_service"], allow_cache=True
                )
                assert_permissions(
                    resource=feature_service, actions=[AuthzedAction.QUERY_ONLINE]
                )
                features = feature_service
            else:
                features = body["features"]
                all_feature_views, all_on_demand_feature_views = (
                    utils._get_feature_views_to_use(
                        store.registry,
                        store.project,
                        features,
                        allow_cache=True,
                        hide_dummy_entity=False,
                    )
                )
                for feature_view in all_feature_views:
                    assert_permissions(
                        resource=feature_view, actions=[AuthzedAction.QUERY_ONLINE]
                    )
                for od_feature_view in all_on_demand_feature_views:
                    assert_permissions(
                        resource=od_feature_view, actions=[AuthzedAction.QUERY_ONLINE]
                    )

            response_proto = store.get_online_features(
                features=features,
                entity_rows=entity_rows,
                full_feature_names=full_feature_names,
            ).proto

            # Convert the Protobuf object to JSON and return it
            return MessageToDict(
                response_proto, preserving_proto_field_name=True, float_precision=18
            )
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/push", dependencies=[Depends(inject_user_details)])
    def push(body=Depends(get_body)):
        try:
            request = PushFeaturesRequest(**json.loads(body))
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
            ) + store.list_stream_feature_views(
                allow_cache=request.allow_registry_cache
            )
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

            store.push(
                push_source_name=request.push_source_name,
                df=df,
                allow_registry_cache=request.allow_registry_cache,
                to=to,
            )
        except PushSourceNotFoundException as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/write-to-online-store", dependencies=[Depends(inject_user_details)])
    def write_to_online_store(body=Depends(get_body)):
        try:
            request = WriteToFeatureStoreRequest(**json.loads(body))
            df = pd.DataFrame(request.df)
            feature_view_name = request.feature_view_name
            allow_registry_cache = request.allow_registry_cache
            try:
                feature_view = store.get_stream_feature_view(
                    feature_view_name, allow_registry_cache=allow_registry_cache
                )
            except FeatureViewNotFoundException:
                feature_view = store.get_feature_view(
                    feature_view_name, allow_registry_cache=allow_registry_cache
                )

            assert_permissions(
                resource=feature_view, actions=[AuthzedAction.WRITE_ONLINE]
            )
            store.write_to_online_store(
                feature_view_name=feature_view_name,
                df=df,
                allow_registry_cache=allow_registry_cache,
            )
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/health")
    def health():
        return Response(status_code=status.HTTP_200_OK)

    @app.post("/materialize", dependencies=[Depends(inject_user_details)])
    def materialize(body=Depends(get_body)):
        try:
            request = MaterializeRequest(**json.loads(body))
            for feature_view in request.feature_views:
                assert_permissions(
                    resource=feature_view, actions=[AuthzedAction.WRITE_ONLINE]
                )
            store.materialize(
                utils.make_tzaware(parser.parse(request.start_ts)),
                utils.make_tzaware(parser.parse(request.end_ts)),
                request.feature_views,
            )
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/materialize-incremental", dependencies=[Depends(inject_user_details)])
    def materialize_incremental(body=Depends(get_body)):
        try:
            request = MaterializeIncrementalRequest(**json.loads(body))
            for feature_view in request.feature_views:
                assert_permissions(
                    resource=feature_view, actions=[AuthzedAction.WRITE_ONLINE]
                )
            store.materialize_incremental(
                utils.make_tzaware(parser.parse(request.end_ts)), request.feature_views
            )
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    return app


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

            self.cfg.set("worker_class", "uvicorn.workers.UvicornWorker")

        def load(self):
            return self._app


def monitor_resources(self, interval: int = 5):
    """Function to monitor and update CPU and memory usage metrics."""
    print(f"Start monitor_resources({interval})")
    p = psutil.Process()
    print(f"PID is {p.pid}")
    while True:
        with p.oneshot():
            cpu_usage = p.cpu_percent()
            memory_usage = p.memory_percent()
            print(f"cpu_usage is {cpu_usage}")
            print(f"memory_usage is {memory_usage}")
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
    metrics: bool,
):
    if metrics:
        print("Start Prometheus Server")
        start_http_server(8000)

        print("Start a background thread to monitor CPU and memory usage")
        monitoring_thread = threading.Thread(
            target=monitor_resources, args=(5,), daemon=True
        )
        monitoring_thread.start()

    print("start_server called ")
    auth_type = str_to_auth_manager_type(store.config.auth_config.type)
    print(f"auth_type is {auth_type}")
    init_security_manager(auth_type=auth_type, fs=store)
    print("init_security_manager OK ")
    init_auth_manager(
        auth_type=auth_type,
        server_type=ServerType.REST,
        auth_config=store.config.auth_config,
    )
    print("init_auth_manager OK ")

    if sys.platform != "win32":
        FeastServeApplication(
            store=store,
            bind=f"{host}:{port}",
            accesslog=None if no_access_log else "-",
            workers=workers,
            keepalive=keep_alive_timeout,
            registry_ttl_sec=registry_ttl_sec,
        ).run()
    else:
        import uvicorn

        app = get_app(store, registry_ttl_sec)
        uvicorn.run(app, host=host, port=port, access_log=(not no_access_log))
