import json
import sys
import threading
import traceback
from contextlib import asynccontextmanager
from typing import List, Optional

import pandas as pd
from dateutil import parser
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.logger import logger
from fastapi.params import Depends
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel

import feast
from feast import proto_json, utils
from feast.constants import DEFAULT_FEATURE_SERVER_REGISTRY_TTL
from feast.data_source import PushMode
from feast.errors import PushSourceNotFoundException


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

    @app.post("/get-online-features")
    def get_online_features(body=Depends(get_body)):
        try:
            features, body = _get_features_from_body(store, body)
            full_feature_names = body.get("full_feature_names", False)

            response_proto = store.get_online_features(
                features=features,
                entity_rows=body["entities"],
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

    @app.post("/push")
    def push(body=Depends(get_body)):
        try:
            request = PushFeaturesRequest(**json.loads(body))
            df = pd.DataFrame(request.df)
            if request.to == "offline":
                to = PushMode.OFFLINE
            elif request.to == "online":
                to = PushMode.ONLINE
            elif request.to == "online_and_offline":
                to = PushMode.ONLINE_AND_OFFLINE
            else:
                raise ValueError(
                    f"{request.to} is not a supported push format. Please specify one of these ['online', 'offline', 'online_and_offline']."
                )
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

    @app.post("/write-to-online-store")
    def write_to_online_store(body=Depends(get_body)):
        try:
            request = WriteToFeatureStoreRequest(**json.loads(body))
            df = pd.DataFrame(request.df)
            store.write_to_online_store(
                feature_view_name=request.feature_view_name,
                df=df,
                allow_registry_cache=request.allow_registry_cache,
            )
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/get-online-predictions")
    def get_predictions_endpoint(body=Depends(get_body)):
        try:
            features, body = _get_features_from_body(store, body)

            response_proto = store.get_online_predictions(
                prediction_feature_name=body["prediction_feature_name"],
                model_feature_name=body["model_feature_name"],
                features=features,
                entity_rows=body["entities"],
                force_recompute=body["force_recompute"],
                log_features=body["log_features"],
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

    @app.get("/health")
    def health():
        return Response(status_code=status.HTTP_200_OK)

    @app.post("/materialize")
    def materialize(body=Depends(get_body)):
        try:
            request = MaterializeRequest(**json.loads(body))
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

    @app.post("/materialize-incremental")
    def materialize_incremental(body=Depends(get_body)):
        try:
            request = MaterializeIncrementalRequest(**json.loads(body))
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


def start_server(
    store: "feast.FeatureStore",
    host: str,
    port: int,
    no_access_log: bool,
    workers: int,
    keep_alive_timeout: int,
    registry_ttl_sec: int,
):
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


def _get_features_from_body(store: "feast.FeatureStore", body):
    body = json.loads(body)
    # Initialize parameters for FeatureStore.get_online_features(...) call
    if "feature_service" in body:
        features = store.get_feature_service(body["feature_service"], allow_cache=True)
    else:
        features = body["features"]
    return features, body
