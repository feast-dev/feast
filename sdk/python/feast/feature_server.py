import json
import traceback
import warnings

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.logger import logger
from fastapi.params import Depends
from google.protobuf.json_format import MessageToDict, Parse
from pydantic import BaseModel

import feast
from feast import proto_json
from feast.data_source import PushMode
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest


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


def get_app(store: "feast.FeatureStore"):
    proto_json.patch()

    app = FastAPI()

    async def get_body(request: Request):
        return await request.body()

    @app.post("/get-online-features")
    def get_online_features(body=Depends(get_body)):
        try:
            # Validate and parse the request data into GetOnlineFeaturesRequest Protobuf object
            request_proto = GetOnlineFeaturesRequest()
            Parse(body, request_proto)

            # Initialize parameters for FeatureStore.get_online_features(...) call
            if request_proto.HasField("feature_service"):
                features = store.get_feature_service(
                    request_proto.feature_service, allow_cache=True
                )
            else:
                features = list(request_proto.features.val)

            full_feature_names = request_proto.full_feature_names

            batch_sizes = [len(v.val) for v in request_proto.entities.values()]
            num_entities = batch_sizes[0]
            if any(batch_size != num_entities for batch_size in batch_sizes):
                raise HTTPException(status_code=500, detail="Uneven number of columns")

            response_proto = store._get_online_features(
                features=features,
                entity_values=request_proto.entities,
                full_feature_names=full_feature_names,
                native_entity_values=False,
            ).proto

            # Convert the Protobuf object to JSON and return it
            return MessageToDict(  # type: ignore
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
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/write-to-online-store")
    def write_to_online_store(body=Depends(get_body)):
        warnings.warn(
            "write_to_online_store is deprecated. Please consider using /push instead",
            RuntimeWarning,
        )
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

    return app


def start_server(
    store: "feast.FeatureStore", host: str, port: int, no_access_log: bool
):
    app = get_app(store)
    uvicorn.run(app, host=host, port=port, access_log=(not no_access_log))
